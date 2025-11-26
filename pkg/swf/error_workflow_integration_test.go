package swf_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"testing"
	"time"

	strataclient "github.com/colony-2/strata/strata-go/pkg/client"
	"github.com/colony-2/strata/strata-go/pkg/client/story"
	"github.com/colony-2/swf-go/pkg/swf"
	"github.com/colony-2/swf-go/pkg/swf/impl"
	"github.com/fergusstrange/embedded-postgres"
)

func TestTaskErrorsAreEnvelopedAndReturned(t *testing.T) {
	tests := []struct {
		name         string
		taskErr      error
		expectedKind string
		expectedErr  interface{}
	}{
		{name: "app error", taskErr: swf.AppError{Payload: swf.AppErrorPayload{Message: "task app fail"}}, expectedKind: "AppError", expectedErr: &swf.AppError{}},
		{name: "system error", taskErr: swf.SystemError{Payload: swf.SystemErrorPayload{Message: "task system fail"}}, expectedKind: "SystemError", expectedErr: &swf.SystemError{}},
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

			engine, err := swf.NewEngineBuilder("tenant-task-"+tt.name).
				WithPostgresDSN(postgresDSN).
				WithStrata(baseURL).
				WithStrataAPIKey(strata.APIKey).
				PlusWorkers(jobWorker, taskWorker).
				Build(impl.Builder)
			if err != nil {
				t.Fatalf("failed to build engine: %v", err)
			}

			go engine.Run(ctx)

			jobID, err := engine.StartJob(ctx, swf.StartJob{
				JobType: jobWorker.Name(),
				Data:    &swf.SimpleTaskData{Data: swf.NewMapData(map[string]interface{}{"n": 1})},
			})
			if err != nil {
				t.Fatalf("failed to start job: %v", err)
			}

			client := mustStrataClient(t, baseURL, strata.APIKey)
			key := story.Key{AnthologyID: "tenant-task-" + tt.name, StoryID: string(jobID)}
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

			td, gotErr := waitForJobResult(t, engine, postgresDSN, jobID, tt.expectedErr)
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
		name         string
		jobErr       error
		expectedKind string
		expectedErr  interface{}
	}{
		{name: "job app error", jobErr: swf.AppError{Payload: swf.AppErrorPayload{Message: "job app fail"}}, expectedKind: "AppError", expectedErr: &swf.AppError{}},
		{name: "job system error", jobErr: swf.SystemError{Payload: swf.SystemErrorPayload{Message: "job system fail"}}, expectedKind: "SystemError", expectedErr: &swf.SystemError{}},
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

			engine, err := swf.NewEngineBuilder("tenant-job-"+tt.name).
				WithPostgresDSN(postgresDSN).
				WithStrata(baseURL).
				WithStrataAPIKey(strata.APIKey).
				PlusWorkers(jobWorker).
				Build(impl.Builder)
			if err != nil {
				t.Fatalf("failed to build engine: %v", err)
			}

			go engine.Run(ctx)

			jobID, err := engine.StartJob(ctx, swf.StartJob{
				JobType: jobWorker.Name(),
				Data:    &swf.SimpleTaskData{Data: swf.NewMapData(map[string]interface{}{"n": 1})},
			})
			if err != nil {
				t.Fatalf("failed to start job: %v", err)
			}

			client := mustStrataClient(t, baseURL, strata.APIKey)
			key := story.Key{AnthologyID: "tenant-job-" + tt.name, StoryID: string(jobID)}
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

			td, gotErr := waitForJobResult(t, engine, postgresDSN, jobID, tt.expectedErr)
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
	_, err := ctx.DoTask(j.taskType, input)
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

func startEmbeddedPostgres(t *testing.T) (string, func()) {
	t.Helper()
	pgPort := uint32(20000 + (time.Now().UnixNano() % 1000))
	postgres := embeddedpostgres.NewDatabase(
		embeddedpostgres.DefaultConfig().Port(pgPort),
	)
	if err := postgres.Start(); err != nil {
		t.Fatalf("failed to start embedded postgres: %v", err)
	}
	stop := func() { _ = postgres.Stop() }
	return fmt.Sprintf("postgres://postgres:postgres@localhost:%d/postgres?sslmode=disable", pgPort), stop
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

func waitForJobResult(t *testing.T, engine swf.SWFEngine, dsn string, jobId swf.JobId, expectedErr interface{}) (swf.TaskData, error) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		td, err := engine.GetJobResult(context.Background(), jobId)
		if err == nil {
			if expectedErr == nil {
				return td, nil
			}
			t.Logf("job result succeeded unexpectedly: %+v", td)
			return td, nil
		}
		if err != nil {
			if expectedErr == nil {
				return td, err
			}
			switch expectedErr.(type) {
			case *swf.AppError:
				var ae swf.AppError
				if errors.As(err, &ae) {
					return td, err
				}
				t.Logf("job result error not AppError yet: %v", err)
			case *swf.SystemError:
				var se swf.SystemError
				if errors.As(err, &se) {
					return td, err
				}
				t.Logf("job result error not SystemError yet: %v", err)
			default:
				// Log active/archive state to help diagnose stuck jobs.
				logJobState(t, dsn, jobId)
			}
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for job result")
	return nil, nil
}

func logJobState(t *testing.T, dsn string, jobId swf.JobId) {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Logf("logJobState: failed to open db: %v", err)
		return
	}
	defer db.Close()

	var active int
	_ = db.QueryRow(`SELECT COUNT(*) FROM pgwf.jobs WHERE job_id = $1`, jobId).Scan(&active)
	var archived int
	_ = db.QueryRow(`SELECT COUNT(*) FROM pgwf.jobs_archive WHERE job_id = $1`, jobId).Scan(&archived)
	t.Logf("job state job_id=%s active=%d archived=%d", jobId, active, archived)
}
