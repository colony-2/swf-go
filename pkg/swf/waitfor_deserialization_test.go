package swf_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/colony-2/swf-go/pkg/swf"
	"github.com/colony-2/swf-go/pkg/swf/impl"
	"github.com/lib/pq"
)

func TestWaitForDeserialization(t *testing.T) {
	ctx := context.Background()
	postgresDSN, stopPG := startEmbeddedPostgres(t)
	defer stopPG()
	if err := installPGWF(ctx, postgresDSN); err != nil {
		t.Fatalf("failed to install pgwf: %v", err)
	}

	baseURL, strata := startStrata(t)
	defer strata.Shutdown()

	engine, err := swf.NewEngineBuilder().
		WithPostgresDSN(postgresDSN).
		WithStrata(baseURL).
		WithStrataAPIKey(strata.APIKey).
		Build(impl.Builder)
	if err != nil {
		t.Fatalf("failed to build engine: %v", err)
	}

	db, err := sql.Open("postgres", postgresDSN)
	if err != nil {
		t.Fatalf("open postgres: %v", err)
	}
	defer db.Close()

	now := time.Now().UTC().Truncate(time.Second)

	// Insert a job with WaitFor dependencies
	childJobID1 := "child-job-1"
	childJobID2 := "child-job-2"
	parentJobID := "parent-job-1"

	// Insert child jobs
	_, err = db.ExecContext(ctx, `
INSERT INTO pgwf.jobs (tenant_id, job_id, next_need, wait_for, payload, singleton_key, available_at, expires_at, lease_expires_at, created_at, cancel_requested)
VALUES ('tenant-1', $1, $2, '{}'::text[], '{}'::jsonb, NULL, $3, 'infinity', '-infinity', $3, false)
`, childJobID1, "child-task", now.Add(-2*time.Minute))
	if err != nil {
		t.Fatalf("insert child job 1: %v", err)
	}

	_, err = db.ExecContext(ctx, `
INSERT INTO pgwf.jobs (tenant_id, job_id, next_need, wait_for, payload, singleton_key, available_at, expires_at, lease_expires_at, created_at, cancel_requested)
VALUES ('tenant-1', $1, $2, '{}'::text[], '{}'::jsonb, NULL, $3, 'infinity', '-infinity', $3, false)
`, childJobID2, "child-task", now.Add(-2*time.Minute))
	if err != nil {
		t.Fatalf("insert child job 2: %v", err)
	}

	// Insert parent job waiting for child jobs
	_, err = db.ExecContext(ctx, `
INSERT INTO pgwf.jobs (tenant_id, job_id, next_need, wait_for, payload, singleton_key, available_at, expires_at, lease_expires_at, created_at, cancel_requested)
VALUES ('tenant-1', $1, $2, $3, '{}'::jsonb, NULL, $4, 'infinity', '-infinity', $4, false)
`, parentJobID, "parent-task", pq.Array([]string{childJobID1, childJobID2}), now.Add(-1*time.Minute))
	if err != nil {
		t.Fatalf("insert parent job: %v", err)
	}

	// Test: List jobs and verify WaitFor is deserialized correctly
	resp, err := engine.ListJobs(ctx, swf.ListJobsRequest{
		TenantIds: []string{"tenant-1"},
	})
	if err != nil {
		t.Fatalf("ListJobs: %v", err)
	}

	var parentJob *swf.JobSummary
	for i := range resp.Jobs {
		if resp.Jobs[i].JobKey.JobId == parentJobID {
			parentJob = &resp.Jobs[i]
			break
		}
	}

	if parentJob == nil {
		t.Fatalf("parent job not found in response")
	}

	if len(parentJob.WaitFor) != 2 {
		t.Fatalf("expected 2 WaitFor dependencies, got %d", len(parentJob.WaitFor))
	}

	expectedWaitFor := map[string]bool{
		childJobID1: false,
		childJobID2: false,
	}

	for _, jobId := range parentJob.WaitFor {
		if _, ok := expectedWaitFor[jobId]; !ok {
			t.Errorf("unexpected WaitFor JobId: %s", jobId)
		}
		expectedWaitFor[jobId] = true
	}

	for jobId, found := range expectedWaitFor {
		if !found {
			t.Errorf("expected WaitFor JobId %s not found", jobId)
		}
	}

	t.Log("WaitFor deserialization test passed successfully")
}
