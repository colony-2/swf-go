package directimpl

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/colony-2/pgwf-go/pkg/pgwf"
	"github.com/colony-2/swf-go/pkg/swf"
	"github.com/colony-2/swf-go/pkg/swf/internal/directtestsupport"
	_ "github.com/lib/pq"
)

func TestSubmitJobRecoversMissingPgwfRecordForExplicitJobID(t *testing.T) {
	rt, shutdown := newEmbeddedDirectRuntimeForTest(t)
	defer shutdown()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	req := swf.SubmitJobRequest{
		Job: swf.SubmitJob{
			TenantId: "tenant-submit-recover",
			JobID:    "submit-recover",
			JobType:  "manual",
			Data:     swf.NewTaskDataOrPanic(map[string]int{"n": 1}),
			Metadata: json.RawMessage(`{"queue":"blue"}`),
		},
		RequestTime: time.Now().UTC(),
	}

	handle, err := rt.SubmitJob(ctx, req)
	if err != nil {
		t.Fatalf("submit job: %v", err)
	}

	deletePgwfJobForTest(t, ctx, rt, handle.JobKey)

	if _, err := pgwf.GetJob(ctx, rt.udb, pgwf.TenantID(handle.JobKey.TenantId), pgwf.JobID(handle.JobKey.JobId), pgwf.GetJobOptions{}); !errors.Is(err, pgwf.ErrJobNotFound) {
		t.Fatalf("expected pgwf row to be deleted, got %v", err)
	}

	recovered, err := rt.SubmitJob(ctx, req)
	if err != nil {
		t.Fatalf("recover submit job: %v", err)
	}
	if recovered.JobKey != handle.JobKey {
		t.Fatalf("unexpected recovered handle %+v", recovered.JobKey)
	}

	if _, err := pgwf.GetJob(ctx, rt.udb, pgwf.TenantID(handle.JobKey.TenantId), pgwf.JobID(handle.JobKey.JobId), pgwf.GetJobOptions{}); err != nil {
		t.Fatalf("expected pgwf row after recovery: %v", err)
	}

	chapters := chapterCountForTest(t, ctx, rt, handle.JobKey)
	if chapters != 1 {
		t.Fatalf("expected exactly 1 stored chapter after recovery, got %d", chapters)
	}

	active, archived := pgwfRowCountsForTest(t, ctx, rt, handle.JobKey)
	if active != 1 || archived != 0 {
		t.Fatalf("expected one active pgwf row after recovery, got active=%d archived=%d", active, archived)
	}
}

func TestSubmitJobRejectsMetadataConflictWhenPgwfRecordMissingForExplicitJobID(t *testing.T) {
	rt, shutdown := newEmbeddedDirectRuntimeForTest(t)
	defer shutdown()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	req := swf.SubmitJobRequest{
		Job: swf.SubmitJob{
			TenantId: "tenant-submit-recover-metadata-conflict",
			JobID:    "submit-recover-metadata-conflict",
			JobType:  "manual",
			Data:     swf.NewTaskDataOrPanic(map[string]int{"n": 1}),
			Metadata: json.RawMessage(`{"queue":"blue"}`),
		},
		RequestTime: time.Now().UTC(),
	}

	handle, err := rt.SubmitJob(ctx, req)
	if err != nil {
		t.Fatalf("submit job: %v", err)
	}

	deletePgwfJobForTest(t, ctx, rt, handle.JobKey)

	_, err = rt.SubmitJob(ctx, swf.SubmitJobRequest{
		Job: swf.SubmitJob{
			TenantId: req.Job.TenantId,
			JobID:    req.Job.JobID,
			JobType:  req.Job.JobType,
			Data:     req.Job.Data,
			Metadata: json.RawMessage(`{"queue":"green"}`),
		},
		RequestTime: time.Now().UTC(),
	})
	if !errors.Is(err, swf.ErrConflict) {
		t.Fatalf("expected metadata conflict, got %v", err)
	}
	if !strings.Contains(err.Error(), "different metadata") {
		t.Fatalf("expected metadata mismatch text, got %v", err)
	}

	active, archived := pgwfRowCountsForTest(t, ctx, rt, handle.JobKey)
	if active != 0 || archived != 0 {
		t.Fatalf("expected no pgwf rows after metadata conflict, got active=%d archived=%d", active, archived)
	}

	chapters := chapterCountForTest(t, ctx, rt, handle.JobKey)
	if chapters != 1 {
		t.Fatalf("expected exactly 1 stored chapter after metadata conflict, got %d", chapters)
	}
}

func TestSubmitRestartJobRecoversMissingPgwfRecordForExplicitJobID(t *testing.T) {
	rt, shutdown := newEmbeddedDirectRuntimeForTest(t)
	defer shutdown()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	sourceReq := swf.SubmitJobRequest{
		Job: swf.SubmitJob{
			TenantId: "tenant-restart-recover",
			JobID:    "restart-source",
			JobType:  "manual",
			Data:     swf.NewTaskDataOrPanic(map[string]int{"n": 1}),
		},
		RequestTime: time.Now().UTC(),
	}
	sourceHandle, err := rt.SubmitJob(ctx, sourceReq)
	if err != nil {
		t.Fatalf("submit source job: %v", err)
	}

	lease, err := rt.GetJobLease(ctx, swf.GetJobLeaseRequest{
		JobKey:        sourceHandle.JobKey,
		WorkerID:      "restart-recovery-writer",
		Capabilities:  []string{"manual"},
		LeaseDuration: 2 * time.Second,
	})
	if err != nil {
		t.Fatalf("get source lease: %v", err)
	}
	if lease == nil {
		t.Fatal("expected source lease")
	}
	if err := rt.PutChapter(ctx, swf.PutChapterRequest{
		LeaseID: lease.LeaseID(),
		Ref: swf.ChapterRef{
			JobKey:  sourceHandle.JobKey,
			Ordinal: 1,
		},
		Chapter: swf.Chapter{
			Ordinal:   1,
			TaskType:  "manual",
			InputHash: "restart-recover-input",
			CreatedAt: time.Now().UTC(),
			Body: swf.TaskAttemptOutcomeChapter{Outcome: swf.ApplicationOutputOutcome{
				Output: swf.ApplicationOutputBytes{Data: []byte(`{"n":2}`)},
			}},
		},
	}); err != nil {
		t.Fatalf("put source chapter: %v", err)
	}
	if err := lease.Complete(ctx, swf.CompleteExecutionRequest{Status: "succeeded"}); err != nil {
		t.Fatalf("complete source lease: %v", err)
	}

	restartReq := swf.SubmitRestartJobRequest{
		Job: swf.SubmitRestartJob{
			PriorJobKey:    sourceHandle.JobKey,
			LastStepToKeep: 0,
			JobID:          "restart-recover",
		},
		RequestTime: time.Now().UTC(),
	}

	restartHandle, err := rt.SubmitRestartJob(ctx, restartReq)
	if err != nil {
		t.Fatalf("submit restart job: %v", err)
	}

	deletePgwfJobForTest(t, ctx, rt, restartHandle.JobKey)

	if _, err := pgwf.GetJob(ctx, rt.udb, pgwf.TenantID(restartHandle.JobKey.TenantId), pgwf.JobID(restartHandle.JobKey.JobId), pgwf.GetJobOptions{}); !errors.Is(err, pgwf.ErrJobNotFound) {
		t.Fatalf("expected restart pgwf row to be deleted, got %v", err)
	}

	recovered, err := rt.SubmitRestartJob(ctx, restartReq)
	if err != nil {
		t.Fatalf("recover restart job: %v", err)
	}
	if recovered.JobKey != restartHandle.JobKey {
		t.Fatalf("unexpected recovered restart handle %+v", recovered.JobKey)
	}

	if _, err := pgwf.GetJob(ctx, rt.udb, pgwf.TenantID(restartHandle.JobKey.TenantId), pgwf.JobID(restartHandle.JobKey.JobId), pgwf.GetJobOptions{}); err != nil {
		t.Fatalf("expected restart pgwf row after recovery: %v", err)
	}
}

func TestArchivedSubmitJobIsNotRecoveredAsMissingForExplicitJobID(t *testing.T) {
	rt, shutdown := newEmbeddedDirectRuntimeForTest(t)
	defer shutdown()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	req := swf.SubmitJobRequest{
		Job: swf.SubmitJob{
			TenantId: "tenant-submit-archived",
			JobID:    "submit-archived",
			JobType:  "manual",
			Data:     swf.NewTaskDataOrPanic(map[string]int{"n": 1}),
		},
		RequestTime: time.Now().UTC(),
	}

	handle, err := rt.SubmitJob(ctx, req)
	if err != nil {
		t.Fatalf("submit job: %v", err)
	}
	archiveJobForTest(t, ctx, rt, handle.JobKey, "manual")

	active, archived := pgwfRowCountsForTest(t, ctx, rt, handle.JobKey)
	if active != 0 || archived != 1 {
		t.Fatalf("expected archived-only pgwf row before repeat submit, got active=%d archived=%d", active, archived)
	}

	matching, err := rt.SubmitJob(ctx, req)
	if err != nil {
		t.Fatalf("repeat archived submit job: %v", err)
	}
	if matching.JobKey != handle.JobKey {
		t.Fatalf("unexpected matching archived handle %+v", matching.JobKey)
	}

	active, archived = pgwfRowCountsForTest(t, ctx, rt, handle.JobKey)
	if active != 0 || archived != 1 {
		t.Fatalf("expected archived-only pgwf row after repeat submit, got active=%d archived=%d", active, archived)
	}
}

func TestArchivedRestartJobIsNotRecoveredAsMissingForExplicitJobID(t *testing.T) {
	rt, shutdown := newEmbeddedDirectRuntimeForTest(t)
	defer shutdown()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	sourceReq := swf.SubmitJobRequest{
		Job: swf.SubmitJob{
			TenantId: "tenant-restart-archived",
			JobID:    "restart-archived-source",
			JobType:  "manual",
			Data:     swf.NewTaskDataOrPanic(map[string]int{"n": 1}),
		},
		RequestTime: time.Now().UTC(),
	}
	sourceHandle, err := rt.SubmitJob(ctx, sourceReq)
	if err != nil {
		t.Fatalf("submit source job: %v", err)
	}
	addSingleChapterAndArchiveJobForTest(t, ctx, rt, sourceHandle.JobKey, "manual")

	restartReq := swf.SubmitRestartJobRequest{
		Job: swf.SubmitRestartJob{
			PriorJobKey:    sourceHandle.JobKey,
			LastStepToKeep: 0,
			JobID:          "restart-archived-target",
		},
		RequestTime: time.Now().UTC(),
	}

	restartHandle, err := rt.SubmitRestartJob(ctx, restartReq)
	if err != nil {
		t.Fatalf("submit restart job: %v", err)
	}
	archiveJobForTest(t, ctx, rt, restartHandle.JobKey, "manual")

	active, archived := pgwfRowCountsForTest(t, ctx, rt, restartHandle.JobKey)
	if active != 0 || archived != 1 {
		t.Fatalf("expected archived-only pgwf row before repeat restart, got active=%d archived=%d", active, archived)
	}

	matching, err := rt.SubmitRestartJob(ctx, restartReq)
	if err != nil {
		t.Fatalf("repeat archived restart job: %v", err)
	}
	if matching.JobKey != restartHandle.JobKey {
		t.Fatalf("unexpected matching archived restart handle %+v", matching.JobKey)
	}

	active, archived = pgwfRowCountsForTest(t, ctx, rt, restartHandle.JobKey)
	if active != 0 || archived != 1 {
		t.Fatalf("expected archived-only pgwf row after repeat restart, got active=%d archived=%d", active, archived)
	}
}

func newEmbeddedDirectRuntimeForTest(t *testing.T) (*Runtime, func()) {
	t.Helper()

	dsn, stopPG, err := directtestsupport.StartEmbeddedPostgres()
	if err != nil {
		t.Fatalf("start embedded postgres: %v", err)
	}

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		stopPG()
		t.Fatalf("open postgres: %v", err)
	}

	cleanup := func() {
		_ = db.Close()
		stopPG()
	}

	setupCtx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	if err := directtestsupport.InstallPGWF(setupCtx, db); err != nil {
		cleanup()
		t.Fatalf("install pgwf: %v", err)
	}
	strata, err := directtestsupport.StartEmbeddedStrata()
	if err != nil {
		cleanup()
		t.Fatalf("start embedded strata: %v", err)
	}

	rt, err := NewFromConfig(dsn, strata.BaseURL, strata.APIKey)
	if err != nil {
		strata.Shutdown()
		cleanup()
		t.Fatalf("new direct runtime: %v", err)
	}

	return rt, func() {
		strata.Shutdown()
		cleanup()
	}
}

func deletePgwfJobForTest(t *testing.T, ctx context.Context, rt *Runtime, jobKey swf.JobKey) {
	t.Helper()
	if _, err := rt.udb.ExecContext(ctx, `DELETE FROM pgwf.jobs WHERE tenant_id = $1 AND job_id = $2`, jobKey.TenantId, jobKey.JobId); err != nil {
		t.Fatalf("delete pgwf job: %v", err)
	}
}

func archiveJobForTest(t *testing.T, ctx context.Context, rt *Runtime, jobKey swf.JobKey, capability string) {
	t.Helper()
	lease, err := rt.GetJobLease(ctx, swf.GetJobLeaseRequest{
		JobKey:        jobKey,
		WorkerID:      "archive-writer",
		Capabilities:  []string{capability},
		LeaseDuration: 2 * time.Second,
	})
	if err != nil {
		t.Fatalf("get archive lease: %v", err)
	}
	if lease == nil {
		t.Fatalf("expected lease for %s", jobKey)
	}
	if err := lease.Complete(ctx, swf.CompleteExecutionRequest{Status: "succeeded"}); err != nil {
		t.Fatalf("complete archived job: %v", err)
	}
}

func addSingleChapterAndArchiveJobForTest(t *testing.T, ctx context.Context, rt *Runtime, jobKey swf.JobKey, capability string) {
	t.Helper()
	lease, err := rt.GetJobLease(ctx, swf.GetJobLeaseRequest{
		JobKey:        jobKey,
		WorkerID:      "archive-with-chapter-writer",
		Capabilities:  []string{capability},
		LeaseDuration: 2 * time.Second,
	})
	if err != nil {
		t.Fatalf("get source lease: %v", err)
	}
	if lease == nil {
		t.Fatalf("expected source lease for %s", jobKey)
	}
	if err := rt.PutChapter(ctx, swf.PutChapterRequest{
		LeaseID: lease.LeaseID(),
		Ref: swf.ChapterRef{
			JobKey:  jobKey,
			Ordinal: 1,
		},
		Chapter: swf.Chapter{
			Ordinal:   1,
			TaskType:  capability,
			InputHash: "archive-source-input",
			CreatedAt: time.Now().UTC(),
			Body: swf.TaskAttemptOutcomeChapter{Outcome: swf.ApplicationOutputOutcome{
				Output: swf.ApplicationOutputBytes{Data: []byte(`{"n":2}`)},
			}},
		},
	}); err != nil {
		t.Fatalf("put source chapter: %v", err)
	}
	if err := lease.Complete(ctx, swf.CompleteExecutionRequest{Status: "succeeded"}); err != nil {
		t.Fatalf("complete source job: %v", err)
	}
}

func pgwfRowCountsForTest(t *testing.T, ctx context.Context, rt *Runtime, jobKey swf.JobKey) (int, int) {
	t.Helper()
	var active int
	if err := rt.udb.QueryRowContext(ctx, `SELECT COUNT(*) FROM pgwf.jobs WHERE tenant_id = $1 AND job_id = $2`, jobKey.TenantId, jobKey.JobId).Scan(&active); err != nil {
		t.Fatalf("count active pgwf rows: %v", err)
	}
	var archived int
	if err := rt.udb.QueryRowContext(ctx, `SELECT COUNT(*) FROM pgwf.jobs_archive WHERE tenant_id = $1 AND job_id = $2`, jobKey.TenantId, jobKey.JobId).Scan(&archived); err != nil {
		t.Fatalf("count archived pgwf rows: %v", err)
	}
	return active, archived
}

func chapterCountForTest(t *testing.T, ctx context.Context, rt *Runtime, jobKey swf.JobKey) int {
	t.Helper()
	chapters, err := rt.ListChapters(ctx, swf.ListChaptersRequest{
		JobKey:       jobKey,
		StartOrdinal: 0,
	})
	if err != nil {
		t.Fatalf("list chapters: %v", err)
	}
	return len(chapters)
}
