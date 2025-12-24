package impl

import (
	"context"
	"database/sql"
	"log/slog"
	"testing"
	"time"

	"github.com/colony-2/pgwf-go/pkg/pgwf"
	"github.com/segmentio/ksuid"
)

// TestAwaitUntilRecycle ensures long awaits recycle the runner and schedule wake time.
func TestAwaitUntilRecycle(t *testing.T) {
	ctx := context.Background()
	dsn, stopPG, err := StartEmbeddedPostgres()
	if err != nil {
		t.Fatalf("failed to start embedded postgres: %v", err)
	}
	defer stopPG()

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer db.Close()

	if err := InstallPGWF(ctx, db); err != nil {
		t.Fatalf("install pgwf: %v", err)
	}

	engine := swfEngineImpl{
		udb:            db,
		logger:         slog.Default(),
		awaitThreshold: 50 * time.Millisecond,
	}

	jobID := pgwf.JobID("job-" + ksuid.New().String())
	workerID := pgwf.WorkerID("worker-" + ksuid.New().String())
	nextNeed := pgwf.Capability("await:test")

	if err := pgwf.SubmitJob(ctx, db, jobID, pgwf.JobDependencies{NextNeed: nextNeed}, nil, workerID, "", time.Time{}); err != nil {
		t.Fatalf("submit job: %v", err)
	}

	lease, err := pgwf.GetWork(ctx, db, workerID, []pgwf.Capability{nextNeed})
	if err != nil {
		t.Fatalf("get work: %v", err)
	}
	if lease == nil {
		t.Fatalf("expected lease")
	}

	engine.startAwaitRecycler(ctx)

	wakeAt := time.Now().Add(2 * time.Second)
	ch := engine.AwaitUntil(lease.JobID(), lease.NextNeed(), lease, 1, 1, wakeAt)
	if ch == nil {
		t.Fatalf("await channel is nil")
	}
	select {
	case sig := <-ch:
		if sig.Kind != awaitSignalKindRecycle {
			t.Fatalf("expected recycle signal, got %v", sig)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("did not receive recycle signal")
	}

	var available time.Time
	if err := db.QueryRowContext(ctx, "select available_at from pgwf.jobs where job_id = $1", lease.JobID()).Scan(&available); err != nil {
		t.Fatalf("query available_at: %v", err)
	}
	// allow small drift between chosen wakeAt and stored available_at.
	if diff := available.Sub(wakeAt); diff < -200*time.Millisecond || diff > 2*time.Second {
		t.Fatalf("available_at not near wakeAt: wakeAt=%s available_at=%s diff=%s", wakeAt, available, diff)
	}
}
