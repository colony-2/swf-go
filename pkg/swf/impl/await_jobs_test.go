package impl

import (
	"context"
	"database/sql"
	"log/slog"
	"testing"
	"time"

	"github.com/colony-2/pgwf-go/pkg/pgwf"
	"github.com/lib/pq"
	"github.com/segmentio/ksuid"
)

func TestAwaitJobsReschedulesAndExits(t *testing.T) {
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

	engine := &swfEngineImpl{
		udb:    db,
		logger: slog.Default(),
	}

	tenantID := pgwf.TenantID("test-tenant")
	parentJobID := pgwf.JobID("parent-" + ksuid.New().String())
	childJobID := pgwf.JobID("child-" + ksuid.New().String())
	workerID := pgwf.WorkerID("worker-" + ksuid.New().String())
	parentNeed := pgwf.Capability("await:parent")
	childNeed := pgwf.Capability("await:child")

	if err := pgwf.SubmitJob(ctx, db, tenantID, childJobID, pgwf.JobDependencies{NextNeed: childNeed}, nil, nil, workerID, "", time.Time{}); err != nil {
		t.Fatalf("submit child job: %v", err)
	}
	if err := pgwf.SubmitJob(ctx, db, tenantID, parentJobID, pgwf.JobDependencies{NextNeed: parentNeed}, nil, nil, workerID, "", time.Time{}); err != nil {
		t.Fatalf("submit parent job: %v", err)
	}

	lease, err := pgwf.GetWork(ctx, db, workerID, []pgwf.Capability{parentNeed}, nil)
	if err != nil {
		t.Fatalf("get work: %v", err)
	}
	if lease == nil {
		t.Fatalf("expected lease")
	}

	r := runner{
		jobId:      lease.JobID(),
		tenantId:   string(tenantID),
		engine:     engine,
		lease:      lease,
		capability: lease.NextNeed(),
	}

	errCh := make(chan error, 1)
	postCall := make(chan struct{}, 1)
	done := make(chan struct{})
	go func() {
		defer close(done)
		if err := r.AwaitJobs(string(childJobID)); err != nil {
			errCh <- err
			return
		}
		postCall <- struct{}{}
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("await jobs did not exit")
	}

	select {
	case err := <-errCh:
		t.Fatalf("await jobs returned error: %v", err)
	default:
	}

	select {
	case <-postCall:
		t.Fatalf("expected goroutine to exit before post-call")
	case <-time.After(200 * time.Millisecond):
	}

	var waitFor []string
	var nextNeed string
	if err := db.QueryRowContext(ctx, "select wait_for, next_need from pgwf.jobs where tenant_id = $1 and job_id = $2", tenantID, parentJobID).Scan(pq.Array(&waitFor), &nextNeed); err != nil {
		t.Fatalf("query job: %v", err)
	}
	if nextNeed != string(parentNeed) {
		t.Fatalf("expected next_need %s, got %s", parentNeed, nextNeed)
	}
	if len(waitFor) != 1 || waitFor[0] != string(childJobID) {
		t.Fatalf("expected wait_for %s, got %v", childJobID, waitFor)
	}
}
