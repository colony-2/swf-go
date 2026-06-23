package workflow_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/workflow"
	strataclient "github.com/colony-2/strata-go/pkg/client"
	"github.com/colony-2/strata-go/pkg/client/story"
)

func TestTaskErrorsAreEnvelopedAndReturned(t *testing.T) {
	tests := []struct {
		name           string
		taskErr        error
		expectedKind   string
		expectAppError bool
		expectSysError bool
	}{
		{name: "app error", taskErr: jobdb.AppError{Payload: jobdb.AppErrorPayload{Message: "task app fail"}}, expectedKind: "AppError", expectAppError: true},
		{name: "system error", taskErr: jobdb.NewSystemError(jobdb.SystemErrorPayload{Message: "task system fail"}), expectedKind: "SystemError", expectSysError: true},
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
			tenantID := "tenant-task-" + tt.name

			engine := buildDirectEngine(t, postgresDSN, baseURL, strata.APIKey, func(b *workflow.EngineBuilder) {
				b.WithWorkerTenantId(tenantID).PlusWorkers(jobWorker, taskWorker)
			})

			go engine.Run(ctx)

			jobKey, err := engine.SubmitJob(ctx, jobdb.SubmitJob{
				TenantId: tenantID,
				JobType:  jobWorker.Name(),
				Data:     jobdb.NewTaskDataOrPanic(map[string]interface{}{"n": 1}),
			})
			if err != nil {
				t.Fatalf("failed to start job: %v", err)
			}

			td, gotErr := waitForJobResult(t, engine, postgresDSN, jobKey, tt.expectAppError, tt.expectSysError)
			if gotErr == nil {
				t.Fatalf("expected error, got nil")
			}
			if envTD, ok := td.(*jobdb.EnvelopedTaskData); !ok || envTD.Kind != tt.expectedKind {
				t.Fatalf("expected payload kind %s, got %T %+v", tt.expectedKind, td, td)
			}
			assertTaskOutcomePayloadKind(t, engine, jobKey, tt.expectedKind)
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
		{name: "job app error", jobErr: jobdb.AppError{Payload: jobdb.AppErrorPayload{Message: "job app fail"}}, expectedKind: "AppError", expectAppError: true},
		{name: "job system error", jobErr: jobdb.NewSystemError(jobdb.SystemErrorPayload{Message: "job system fail"}), expectedKind: "SystemError", expectSysError: true},
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
			tenantID := "tenant-job-" + tt.name

			engine := buildDirectEngine(t, postgresDSN, baseURL, strata.APIKey, func(b *workflow.EngineBuilder) {
				b.WithWorkerTenantId(tenantID).PlusWorkers(jobWorker)
			})

			go engine.Run(ctx)

			jobKey, err := engine.SubmitJob(ctx, jobdb.SubmitJob{
				TenantId: tenantID,
				JobType:  jobWorker.Name(),
				Data:     jobdb.NewTaskDataOrPanic(map[string]interface{}{"n": 1}),
			})
			if err != nil {
				t.Fatalf("failed to start job: %v", err)
			}

			td, gotErr := waitForJobResult(t, engine, postgresDSN, jobKey, tt.expectAppError, tt.expectSysError)
			if gotErr == nil {
				t.Fatalf("expected error, got nil")
			}
			if envTD, ok := td.(*jobdb.EnvelopedTaskData); !ok || envTD.Kind != tt.expectedKind {
				t.Fatalf("expected payload kind %s, got %T %+v", tt.expectedKind, td, td)
			}
			assertJobOutcomePayloadKind(t, engine, jobKey, tt.expectedKind)
		})
	}
}

// Helpers and worker fakes

type errorTaskWorker struct{ err error }

func (errorTaskWorker) Name() string { return "err_task" }
func (w errorTaskWorker) Run(_ workflow.TaskContext, _ jobdb.TaskData) (jobdb.TaskData, error) {
	return nil, w.err
}

type singleTaskJob struct{ taskType string }

func (singleTaskJob) Name() string { return "single_task_job" }
func (j singleTaskJob) Run(ctx workflow.JobContext, input jobdb.JobData) (jobdb.JobData, error) {
	_, err := ctx.DoTask(jobdb.RunPolicy{}, j.taskType, input)
	if err != nil {
		return nil, err
	}
	return input, nil
}

type errorJobWorker struct{ err error }

func (errorJobWorker) Name() string { return "error_job" }
func (w errorJobWorker) Run(_ workflow.JobContext, _ jobdb.JobData) (jobdb.JobData, error) {
	return nil, w.err
}

func assertTaskOutcomePayloadKind(t *testing.T, engine workflow.Engine, jobKey jobdb.JobKey, expected string) {
	t.Helper()
	run, err := engine.GetJobRun(context.Background(), jobdb.GetJobRunRequest{JobKey: jobKey})
	if err != nil {
		t.Fatalf("get job run: %v", err)
	}
	for _, attempt := range run.Attempts {
		for _, task := range attempt.Tasks {
			for _, taskAttempt := range task.Attempts {
				if taskAttempt.Outcome.PayloadKind == expected {
					return
				}
			}
		}
	}
	t.Fatalf("expected task outcome payload kind %s in job run: %+v", expected, run.Attempts)
}

func assertJobOutcomePayloadKind(t *testing.T, engine workflow.Engine, jobKey jobdb.JobKey, expected string) {
	t.Helper()
	run, err := engine.GetJobRun(context.Background(), jobdb.GetJobRunRequest{JobKey: jobKey})
	if err != nil {
		t.Fatalf("get job run: %v", err)
	}
	for _, attempt := range run.Attempts {
		if attempt.Outcome.PayloadKind == expected {
			return
		}
	}
	t.Fatalf("expected job outcome payload kind %s in job run: %+v", expected, run.Attempts)
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

func waitForJobResult(t *testing.T, engine workflow.Engine, dsn string, jobKey jobdb.JobKey, expectAppError, expectSysError bool) (jobdb.TaskData, error) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		td, err := jobResultForTest(engine, context.Background(), jobKey)
		if err == nil {
			t.Logf("job result succeeded unexpectedly: %+v", td)
			return td, nil
		}
		if err != nil {
			if expectAppError && jobdb.IsAppError(err) {
				return td, err
			}
			if expectSysError && jobdb.IsSystemError(err) {
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

func logJobState(t *testing.T, dsn string, jobKey jobdb.JobKey) {
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
