package swf_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"testing"
	"time"

	strataclient "github.com/colony-2/strata-go/pkg/client"
	"github.com/colony-2/strata-go/pkg/client/story"
	"github.com/colony-2/swf-go/pkg/swf"
)

func TestTaskErrorsAreEnvelopedAndReturned(t *testing.T) {
	tests := []struct {
		name           string
		taskErr        error
		expectedKind   string
		expectAppError bool
		expectSysError bool
	}{
		{name: "app error", taskErr: swf.AppError{Payload: swf.AppErrorPayload{Message: "task app fail"}}, expectedKind: "AppError", expectAppError: true},
		{name: "system error", taskErr: swf.NewSystemError(swf.SystemErrorPayload{Message: "task system fail"}), expectedKind: "SystemError", expectSysError: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			postgresDSN, stopPG := startEmbeddedPostgres(t)
			defer stopPG()
			if err := installPGWF(ctx, postgresDSN); err != nil {
				t.Fatalf("failed to install pgwf: %v", err)
			}

			baseURL, strata := startStrata(t)
			defer strata.Shutdown()
			waitForStrataReady(t, baseURL)

			jobWorker := singleTaskJob{taskType: "err_task"}
			taskWorker := errorTaskWorker{err: tt.taskErr}

			engine := buildDirectEngine(t, postgresDSN, baseURL, strata.APIKey, func(b *swf.EngineBuilder) {
				b.PlusWorkers(jobWorker, taskWorker)
			})

			go engine.Run(ctx)

			tenantID := "tenant-task-" + tt.name
			jobKey, err := engine.StartJob(ctx, swf.StartJob{
				TenantId: tenantID,
				JobType:  jobWorker.Name(),
				Data:     swf.NewTaskDataOrPanic(map[string]interface{}{"n": 1}),
			})
			if err != nil {
				t.Fatalf("failed to start job: %v", err)
			}

			client := mustStrataClient(t, baseURL, strata.APIKey)
			key := story.Key{AnthologyID: tenantID, StoryID: jobKey.JobId}
			chap := waitForChapter(t, client, key, 1, 10*time.Second)
			var env struct {
				PayloadKind string `json:"payload_kind"`
			}
			if err := json.Unmarshal(chap.Body(), &env); err != nil {
				t.Fatalf("decode env: %v", err)
			}
			if env.PayloadKind != tt.expectedKind {
				t.Fatalf("expected chapter payload kind %s, got %s body=%s", tt.expectedKind, env.PayloadKind, string(chap.Body()))
			}

			td, gotErr := waitForJobResult(t, engine, postgresDSN, jobKey, tt.expectAppError, tt.expectSysError)
			if gotErr == nil {
				t.Fatalf("expected error, got nil")
			}
			if envTD, ok := td.(*swf.EnvelopedTaskData); !ok || envTD.Kind != tt.expectedKind {
				t.Fatalf("expected payload kind %s, got %T %+v", tt.expectedKind, td, td)
			}
		})
	}
}

func TestJobErrorsAreEnvelopedAndReturned(t *testing.T) {
	tests := []struct {
		name           string
		jobErr         error
		expectedKind   string
		expectAppError bool
		expectSysError bool
	}{
		{name: "job app error", jobErr: swf.AppError{Payload: swf.AppErrorPayload{Message: "job app fail"}}, expectedKind: "AppError", expectAppError: true},
		{name: "job system error", jobErr: swf.NewSystemError(swf.SystemErrorPayload{Message: "job system fail"}), expectedKind: "SystemError", expectSysError: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			postgresDSN, stopPG := startEmbeddedPostgres(t)
			defer stopPG()
			if err := installPGWF(ctx, postgresDSN); err != nil {
				t.Fatalf("failed to install pgwf: %v", err)
			}

			baseURL, strata := startStrata(t)
			defer strata.Shutdown()
			waitForStrataReady(t, baseURL)

			jobWorker := errorJobWorker{err: tt.jobErr}

			engine := buildDirectEngine(t, postgresDSN, baseURL, strata.APIKey, func(b *swf.EngineBuilder) {
				b.PlusWorkers(jobWorker)
			})

			go engine.Run(ctx)

			tenantID := "tenant-job-" + tt.name
			jobKey, err := engine.StartJob(ctx, swf.StartJob{
				TenantId: tenantID,
				JobType:  jobWorker.Name(),
				Data:     swf.NewTaskDataOrPanic(map[string]interface{}{"n": 1}),
			})
			if err != nil {
				t.Fatalf("failed to start job: %v", err)
			}

			client := mustStrataClient(t, baseURL, strata.APIKey)
			key := story.Key{AnthologyID: tenantID, StoryID: jobKey.JobId}
			chap := waitForChapter(t, client, key, 1, 10*time.Second)
			var env struct {
				PayloadKind string `json:"payload_kind"`
			}
			if err := json.Unmarshal(chap.Body(), &env); err != nil {
				t.Fatalf("decode env: %v", err)
			}
			if env.PayloadKind != tt.expectedKind {
				t.Fatalf("expected chapter payload kind %s, got %s", tt.expectedKind, env.PayloadKind)
			}

			td, gotErr := waitForJobResult(t, engine, postgresDSN, jobKey, tt.expectAppError, tt.expectSysError)
			if gotErr == nil {
				t.Fatalf("expected error, got nil")
			}
			if envTD, ok := td.(*swf.EnvelopedTaskData); !ok || envTD.Kind != tt.expectedKind {
				t.Fatalf("expected payload kind %s, got %T %+v", tt.expectedKind, td, td)
			}
		})
	}
}

// Helpers and worker fakes

type errorTaskWorker struct{ err error }

func (errorTaskWorker) Name() string { return "err_task" }
func (w errorTaskWorker) Run(_ swf.TaskContext, _ swf.TaskData) (swf.TaskData, error) {
	return nil, w.err
}

type singleTaskJob struct{ taskType string }

func (singleTaskJob) Name() string { return "single_task_job" }
func (j singleTaskJob) Run(ctx swf.JobContext, input swf.JobData) (swf.JobData, error) {
	_, err := ctx.DoTask(swf.RunPolicy{}, j.taskType, input)
	if err != nil {
		return nil, err
	}
	return input, nil
}

type errorJobWorker struct{ err error }

func (errorJobWorker) Name() string { return "error_job" }
func (w errorJobWorker) Run(_ swf.JobContext, _ swf.JobData) (swf.JobData, error) {
	return nil, w.err
}

func mustStrataClient(t *testing.T, baseURL, apiKey string) *strataclient.Client {
	t.Helper()
	client, err := strataclient.New(strataclient.Config{BaseURL: baseURL, APIKey: apiKey})
	if err != nil {
		t.Fatalf("failed to create strata client: %v", err)
	}
	return client
}

func waitForChapter(t *testing.T, client *strataclient.Client, key story.Key, ordinal int64, timeout time.Duration) story.Chapter {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		chap, err := client.Chapter(context.Background(), key, ordinal)
		if err == nil {
			return chap
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for chapter %d", ordinal)
	return nil
}

func waitForJobResult(t *testing.T, engine swf.SWFEngine, dsn string, jobKey swf.JobKey, expectAppError, expectSysError bool) (swf.TaskData, error) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		td, err := engine.GetJobResult(context.Background(), jobKey)
		if err == nil {
			t.Logf("job result succeeded unexpectedly: %+v", td)
			return td, nil
		}
		if err != nil {
			if expectAppError && swf.IsAppError(err) {
				return td, err
			}
			if expectSysError && swf.IsSystemError(err) {
				return td, err
			}
			t.Logf("job result not ready: %v", err)
			// Log active/archive state to help diagnose stuck jobs.
			logJobState(t, dsn, jobKey)
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for job result")
	return nil, nil
}

func logJobState(t *testing.T, dsn string, jobKey swf.JobKey) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Logf("logJobState: failed to open db: %v", err)
		return
	}
	defer db.Close()

	var active int
	_ = db.QueryRow(`SELECT COUNT(*) FROM pgwf.jobs WHERE job_id = $1`, jobKey.JobId).Scan(&active)
	var archived int
	_ = db.QueryRow(`SELECT COUNT(*) FROM pgwf.jobs_archive WHERE job_id = $1`, jobKey.JobId).Scan(&archived)
	t.Logf("job state job_id=%s active=%d archived=%d", jobKey.JobId, active, archived)
}
