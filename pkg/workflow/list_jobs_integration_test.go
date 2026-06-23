package workflow_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
)

func TestListJobsRoutesByStatusAndOrdersWithUnion(t *testing.T) {
	ctx := context.Background()
	postgresDSN, stopPG := startEmbeddedPostgres(t)
	defer stopPG()
	if err := installPGWF(ctx, postgresDSN); err != nil {
		t.Fatalf("failed to install pgwf: %v", err)
	}

	baseURL, strata := startStrata(t)
	defer strata.Shutdown()

	engine := buildDirectEngine(t, postgresDSN, baseURL, strata.APIKey, nil)

	db, err := sql.Open("postgres", postgresDSN)
	if err != nil {
		t.Fatalf("open postgres: %v", err)
	}
	defer db.Close()

	now := time.Now().UTC().Truncate(time.Second)
	createdActiveA := now.Add(-1 * time.Minute)
	createdActiveB := now.Add(-2 * time.Minute)
	createdArchive := now.Add(-3 * time.Minute)

	insertJob := func(id, nextNeed string, created time.Time, cancel bool) {
		_, err := db.ExecContext(ctx, `
INSERT INTO pgwf.jobs (tenant_id, job_id, next_need, wait_for, payload, available_at, expires_at, lease_expires_at, created_at, cancel_requested)
VALUES ('test-tenant', $1, $2, '{}'::text[], '{}'::jsonb, $3, 'infinity', '-infinity', $3, $4)
`, id, nextNeed, created, cancel)
		if err != nil {
			t.Fatalf("insert job %s: %v", id, err)
		}
	}
	insertArchive := func(id, nextNeed string, created time.Time) {
		_, err := db.ExecContext(ctx, `
INSERT INTO pgwf.jobs_archive (tenant_id, job_id, next_need, wait_for, payload, metadata, created_at, expires_at, cancel_requested, archived_at)
VALUES ('test-tenant', $1, $2, '{}'::text[], '{}'::jsonb, '{}'::jsonb, $3, 'infinity', false, $4)
`, id, nextNeed, created, created)
		if err != nil {
			t.Fatalf("insert archived job %s: %v", id, err)
		}
	}

	insertJob("job-active-A", "alpha", createdActiveA, false)
	insertJob("job-active-B", "beta:task", createdActiveB, true)
	insertArchive("job-archived-C", "alpha:other", createdArchive)
	insertArchive("job-archived-D", "delta:other", createdArchive.Add(-time.Minute))

	t.Run("completed status uses archive only", func(t *testing.T) {
		resp, err := engine.ListJobs(ctx, jobdb.ListJobsRequest{
			TenantIds: []string{"test-tenant"},
			Statuses:  []jobdb.JobStatus{jobdb.JobStatusCompleted},
		})
		if err != nil {
			t.Fatalf("ListJobs: %v", err)
		}
		if len(resp.Jobs) != 2 {
			t.Fatalf("expected 2 archived jobs, got %d", len(resp.Jobs))
		}
		seen := map[string]bool{}
		for _, j := range resp.Jobs {
			seen[j.JobKey.JobId] = true
		}
		if !seen["job-archived-C"] || !seen["job-archived-D"] {
			t.Fatalf("missing archived jobs in response: %+v", resp.Jobs)
		}
	})

	t.Run("filters by job type on active", func(t *testing.T) {
		resp, err := engine.ListJobs(ctx, jobdb.ListJobsRequest{
			TenantIds: []string{"test-tenant"},
			JobTypes:  []string{"alpha"},
			Stores:    []jobdb.JobStore{jobdb.JobStoreActive},
		})
		if err != nil {
			t.Fatalf("ListJobs: %v", err)
		}
		if len(resp.Jobs) != 1 {
			t.Fatalf("expected 1 active job, got %d", len(resp.Jobs))
		}
		got := resp.Jobs[0]
		if got.JobKey.JobId != "job-active-A" || got.JobType != "alpha" {
			t.Fatalf("unexpected job %+v", got)
		}
	})

	t.Run("filters by job/task tuple", func(t *testing.T) {
		resp, err := engine.ListJobs(ctx, jobdb.ListJobsRequest{
			TenantIds: []string{"test-tenant"},
			JobTasks:  []jobdb.JobTaskFilter{{JobType: "beta", TaskType: "task"}},
		})
		if err != nil {
			t.Fatalf("ListJobs: %v", err)
		}
		if len(resp.Jobs) != 1 || resp.Jobs[0].JobKey.JobId != "job-active-B" {
			t.Fatalf("expected job-active-B from job/task filter, got %+v", resp.Jobs)
		}
	})

	t.Run("requires tenant ids", func(t *testing.T) {
		if _, err := engine.ListJobs(ctx, jobdb.ListJobsRequest{}); err == nil {
			t.Fatal("expected ListJobs to reject empty tenant ids")
		}
	})

	t.Run("filters by job ids list", func(t *testing.T) {
		resp, err := engine.ListJobs(ctx, jobdb.ListJobsRequest{
			TenantIds: []string{"test-tenant"},
			JobKeys: []jobdb.JobKey{
				{TenantId: "test-tenant", JobId: "job-active-A"},
				{TenantId: "test-tenant", JobId: "job-archived-D"},
			},
		})
		if err != nil {
			t.Fatalf("ListJobs: %v", err)
		}
		if len(resp.Jobs) != 2 {
			t.Fatalf("expected 2 jobs, got %+v", resp.Jobs)
		}
		want := map[string]bool{"job-active-A": true, "job-archived-D": true}
		for _, j := range resp.Jobs {
			if !want[j.JobKey.JobId] {
				t.Fatalf("unexpected job %s", j.JobKey.JobId)
			}
		}
	})

	t.Run("paginates newest first across union", func(t *testing.T) {
		resp, err := engine.ListJobs(ctx, jobdb.ListJobsRequest{
			TenantIds: []string{"test-tenant"},
			PageSize:  2,
		})
		if err != nil {
			t.Fatalf("ListJobs: %v", err)
		}
		if len(resp.Jobs) != 2 {
			t.Fatalf("expected 2 jobs on first page, got %d", len(resp.Jobs))
		}
		if resp.NextPageToken == "" {
			t.Fatalf("expected next page token")
		}
		if resp.Jobs[0].CreatedAt.Before(resp.Jobs[1].CreatedAt) {
			t.Fatalf("jobs not ordered by created_at desc")
		}

		resp2, err := engine.ListJobs(ctx, jobdb.ListJobsRequest{
			TenantIds: []string{"test-tenant"},
			PageSize:  2,
			PageToken: resp.NextPageToken,
		})
		if err != nil {
			t.Fatalf("ListJobs page 2: %v", err)
		}
		if len(resp2.Jobs) != 2 {
			t.Fatalf("expected 2 jobs on second page, got %d", len(resp2.Jobs))
		}
	})
}
