package workflow_test

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/workflow"
	"github.com/lib/pq"
)

// TestJobsEventuallyComplete verifies that every job we start transitions to a completed
// archive state (surfaceable via CheckJobStatus) rather than getting stuck in the active view.
func TestJobsEventuallyComplete(t *testing.T) {
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

	tenantID := "job-status-tenant"
	engine := buildDirectEngine(t, postgresDSN, baseURL, strata.APIKey, func(b *workflow.EngineBuilder) {
		b.WithWorkerTenantId(tenantID).PlusWorkers(statusJobWorker{}, statusTaskWorker{})
	})

	go engine.Run(ctx)

	jobInputs := []int{1, 2, 3}
	jobKeys := make([]jobdb.JobKey, 0, len(jobInputs))
	for _, n := range jobInputs {
		key, err := engine.SubmitJob(ctx, jobdb.SubmitJob{
			TenantId: tenantID,
			JobType:  statusJobName,
			Data:     jobdb.NewTaskDataOrPanic(map[string]interface{}{"n": n}),
		})
		if err != nil {
			t.Fatalf("failed to start job: %v", err)
		}
		jobKeys = append(jobKeys, key)
	}

	for _, key := range jobKeys {
		waitForCompletedStatus(t, ctx, engine, key)
	}

	db, err := sql.Open("postgres", postgresDSN)
	if err != nil {
		t.Fatalf("open postgres: %v", err)
	}
	defer db.Close()

	jobIDStrings := make([]string, 0, len(jobKeys))
	for _, key := range jobKeys {
		jobIDStrings = append(jobIDStrings, key.JobId)
	}
	var archived int
	if err := db.QueryRow(`SELECT COUNT(*) FROM pgwf.jobs_archive WHERE job_id = ANY($1)`, pq.Array(jobIDStrings)).Scan(&archived); err != nil {
		t.Fatalf("count archived jobs: %v", err)
	}
	if archived != len(jobKeys) {
		t.Fatalf("expected %d archived jobs, got %d", len(jobKeys), archived)
	}
}

func waitForCompletedStatus(t *testing.T, ctx context.Context, engine workflow.Engine, jobKey jobdb.JobKey) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		status, err := jobStatusForTest(engine, ctx, jobKey)
		if err == nil && status == jobdb.JobStatusCompleted {
			return
		}
		if err != nil && !errors.Is(err, jobdb.ErrJobNotFound) {
			t.Fatalf("check status for job %s: %v", jobKey.String(), err)
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("job %s did not reach completed status", jobKey.String())
}

const (
	statusJobName  = "status_job"
	statusTaskName = "status_task"
)

type statusJobWorker struct{}

func (statusJobWorker) Name() string { return statusJobName }
func (statusJobWorker) Run(ctx workflow.JobContext, input jobdb.JobData) (jobdb.JobData, error) {
	return ctx.DoTask(jobdb.RunPolicy{}, statusTaskName, input)
}

type statusTaskWorker struct{}

func (statusTaskWorker) Name() string { return statusTaskName }
func (statusTaskWorker) Run(ctx workflow.TaskContext, input jobdb.TaskData) (jobdb.TaskData, error) {
	payload, err := input.GetData()
	if err != nil {
		return nil, err
	}
	return &jobdb.SimpleTaskData{Data: payload}, nil
}
