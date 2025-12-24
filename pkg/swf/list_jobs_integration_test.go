package swf_test

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/colony-2/swf-go/pkg/swf"
	"github.com/colony-2/swf-go/pkg/swf/impl"
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
	createdActiveA := now.Add(-1 * time.Minute)
	createdActiveB := now.Add(-2 * time.Minute)
	createdArchive := now.Add(-3 * time.Minute)

	insertJob := func(id, nextNeed string, created time.Time, cancel bool, singleton *string) {
		_, err := db.ExecContext(ctx, `
INSERT INTO pgwf.jobs (job_id, next_need, wait_for, payload, singleton_key, available_at, expires_at, lease_expires_at, created_at, cancel_requested)
VALUES ($1, $2, '{}'::text[], '{}'::jsonb, $3, $4, 'infinity', '-infinity', $4, $5)
`, id, nextNeed, singleton, created, cancel)
		if err != nil {
			t.Fatalf("insert job %s: %v", id, err)
		}
	}
	insertArchive := func(id, nextNeed string, created time.Time, singleton *string) {
		_, err := db.ExecContext(ctx, `
INSERT INTO pgwf.jobs_archive (job_id, next_need, wait_for, payload, singleton_key, created_at, expires_at, cancel_requested, archived_at)
VALUES ($1, $2, '{}'::text[], '{}'::jsonb, $3, $4, 'infinity', false, $5)
`, id, nextNeed, singleton, created, created)
		if err != nil {
			t.Fatalf("insert archived job %s: %v", id, err)
		}
	}

	sk := "sk-alpha"
	insertJob("job-active-A", "alpha", createdActiveA, false, &sk)
	insertJob("job-active-B", "beta:task", createdActiveB, true, nil)
	insertArchive("job-archived-C", "alpha:other", createdArchive, nil)
	insertArchive("job-archived-D", "delta:other", createdArchive.Add(-time.Minute), nil)

	t.Run("completed status uses archive only", func(t *testing.T) {
		resp, err := engine.ListJobs(ctx, swf.ListJobsRequest{
			Statuses: []swf.JobStatus{swf.JobStatusCompleted},
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
			if j.Payload != nil {
				t.Fatalf("expected nil payload for archive")
			}
		}
		if !seen["job-archived-C"] || !seen["job-archived-D"] {
			t.Fatalf("missing archived jobs in response: %+v", resp.Jobs)
		}
	})

	t.Run("filters by job type and singleton on active", func(t *testing.T) {
		resp, err := engine.ListJobs(ctx, swf.ListJobsRequest{
			JobTypes:      []string{"alpha"},
			SingletonKeys: []string{sk},
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
		if got.Payload == nil {
			t.Fatalf("expected payload for active job")
		}
	})

	t.Run("filters by job/task tuple", func(t *testing.T) {
		resp, err := engine.ListJobs(ctx, swf.ListJobsRequest{
			JobTasks: []swf.JobTaskFilter{{JobType: "beta", TaskType: "task"}},
		})
		if err != nil {
			t.Fatalf("ListJobs: %v", err)
		}
		if len(resp.Jobs) != 1 || resp.Jobs[0].JobKey.JobId != "job-active-B" {
			t.Fatalf("expected job-active-B from job/task filter, got %+v", resp.Jobs)
		}
	})

	t.Run("filters by job ids list", func(t *testing.T) {
		resp, err := engine.ListJobs(ctx, swf.ListJobsRequest{
			JobKeys: []swf.JobKey{
				{TenantId: "list-jobs-tenant", JobId: "job-active-A"},
				{TenantId: "list-jobs-tenant", JobId: "job-archived-D"},
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
		resp, err := engine.ListJobs(ctx, swf.ListJobsRequest{
			PageSize: 2,
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

		resp2, err := engine.ListJobs(ctx, swf.ListJobsRequest{
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
