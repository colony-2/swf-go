package workflow_test

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/workflow"
	"github.com/colony-2/pgwf-go/pkg/pgwf"
)

const (
	completionSuccessJobName = "completion_success_job"
	completionAppJobName     = "completion_app_error_job"
	completionSystemJobName  = "completion_system_error_job"
	completionTimeoutJobName = "completion_timeout_job"
)

type completionSuccessWorker struct{}

func (completionSuccessWorker) Name() string { return completionSuccessJobName }
func (completionSuccessWorker) Run(ctx workflow.JobContext, input jobdb.JobData) (jobdb.JobData, error) {
	return input, nil
}

type completionAppErrorWorker struct{}

func (completionAppErrorWorker) Name() string { return completionAppJobName }
func (completionAppErrorWorker) Run(ctx workflow.JobContext, input jobdb.JobData) (jobdb.JobData, error) {
	return nil, jobdb.AppError{Payload: jobdb.AppErrorPayload{Message: "app failed"}}
}

type completionSystemErrorWorker struct{}

func (completionSystemErrorWorker) Name() string { return completionSystemJobName }
func (completionSystemErrorWorker) Run(ctx workflow.JobContext, input jobdb.JobData) (jobdb.JobData, error) {
	return nil, jobdb.SystemError{Payload: jobdb.SystemErrorPayload{Message: "system failed"}}
}

type completionTimeoutWorker struct{}

func (completionTimeoutWorker) Name() string { return completionTimeoutJobName }
func (completionTimeoutWorker) Run(ctx workflow.JobContext, input jobdb.JobData) (jobdb.JobData, error) {
	time.Sleep(200 * time.Millisecond)
	return input, nil
}

func TestCompletionStatusAndDetail(t *testing.T) {
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

	tenantID := "completion-status-tenant"
	engine := buildDirectEngine(t, postgresDSN, baseURL, strata.APIKey, func(b *workflow.EngineBuilder) {
		b.WithWorkerTenantId(tenantID).
			PlusWorkers(completionSuccessWorker{}).
			PlusWorkers(completionAppErrorWorker{}).
			PlusWorkers(completionSystemErrorWorker{}).
			PlusWorkers(completionTimeoutWorker{})
	})

	go engine.Run(ctx)

	successKey, err := engine.SubmitJob(ctx, jobdb.SubmitJob{
		TenantId: tenantID,
		JobType:  completionSuccessJobName,
		Data:     jobdb.NewTaskDataOrPanic(map[string]interface{}{"n": 1}),
	})
	if err != nil {
		t.Fatalf("start success job: %v", err)
	}
	appKey, err := engine.SubmitJob(ctx, jobdb.SubmitJob{
		TenantId: tenantID,
		JobType:  completionAppJobName,
		Data:     jobdb.NewTaskDataOrPanic(map[string]interface{}{"n": 2}),
	})
	if err != nil {
		t.Fatalf("start app error job: %v", err)
	}
	systemKey, err := engine.SubmitJob(ctx, jobdb.SubmitJob{
		TenantId: tenantID,
		JobType:  completionSystemJobName,
		Data:     jobdb.NewTaskDataOrPanic(map[string]interface{}{"n": 3}),
	})
	if err != nil {
		t.Fatalf("start system error job: %v", err)
	}
	timeoutKey, err := engine.SubmitJob(ctx, jobdb.SubmitJob{
		TenantId: tenantID,
		JobType:  completionTimeoutJobName,
		Data:     jobdb.NewTaskDataOrPanic(map[string]interface{}{"n": 4}),
		RunPolicy: jobdb.RunPolicy{
			InvocationTimeout: jobdb.AsDuration(50 * time.Millisecond),
		},
	})
	if err != nil {
		t.Fatalf("start timeout job: %v", err)
	}

	waitForCompletedStatus(t, ctx, engine, successKey)
	waitForCompletedStatus(t, ctx, engine, appKey)
	waitForCompletedStatus(t, ctx, engine, systemKey)
	waitForCompletedStatus(t, ctx, engine, timeoutKey)

	db, err := sql.Open("postgres", postgresDSN)
	if err != nil {
		t.Fatalf("open postgres: %v", err)
	}
	defer db.Close()

	assertCompletion(t, db, successKey, pgwf.CompletionStatus("success"), "")
	assertCompletion(t, db, appKey, pgwf.CompletionStatus("failed_app"), "app failed")
	assertCompletion(t, db, systemKey, pgwf.CompletionStatus("failed_system"), "system failed")
	assertCompletion(t, db, timeoutKey, pgwf.CompletionStatus("failed_timeout"), "timed out")
}

func assertCompletion(t *testing.T, db *sql.DB, key jobdb.JobKey, status pgwf.CompletionStatus, detailSubstring string) {
	t.Helper()

	job, err := pgwf.GetJob(context.Background(), db, pgwf.TenantID(key.TenantId), pgwf.JobID(key.JobId), pgwf.GetJobOptions{})
	if err != nil {
		t.Fatalf("get job %s: %v", key.String(), err)
	}
	if job.CompletionStatus == nil || *job.CompletionStatus != status {
		t.Fatalf("job %s expected completion status %q, got %v", key.String(), status, job.CompletionStatus)
	}
	if detailSubstring == "" {
		if job.CompletionDetail != nil && *job.CompletionDetail != "" {
			t.Fatalf("job %s expected empty completion detail, got %q", key.String(), *job.CompletionDetail)
		}
		return
	}
	if job.CompletionDetail == nil {
		t.Fatalf("job %s expected completion detail containing %q, got nil", key.String(), detailSubstring)
	}
	if !strings.Contains(*job.CompletionDetail, detailSubstring) {
		t.Fatalf("job %s expected completion detail to contain %q, got %q", key.String(), detailSubstring, *job.CompletionDetail)
	}
}
