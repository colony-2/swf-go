package impl

import (
	"context"
	"database/sql"
	"errors"
	"log/slog"
	"testing"
	"time"

	"github.com/colony-2/pgwf-go/pkg/pgwf"
	"github.com/colony-2/swf-go/pkg/swf"
	"github.com/segmentio/ksuid"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

// Ensures jobRun APIs reuse an existing GORM transaction carried in context.
func TestCheckJobStatusUsesContextTransaction(t *testing.T) {
	ctx := context.Background()
	dsn, stopPG, err := StartEmbeddedPostgres()
	if err != nil {
		t.Fatalf("start postgres: %v", err)
	}
	defer stopPG()

	sqlDB, err := sql.Open("postgres", dsn)
	if err != nil {
		t.Fatalf("open sql db: %v", err)
	}
	defer sqlDB.Close()

	if err := InstallPGWF(ctx, sqlDB); err != nil {
		t.Fatalf("install pgwf: %v", err)
	}

	gdb, err := gorm.Open(postgres.Open(dsn), &gorm.Config{})
	if err != nil {
		t.Fatalf("open gorm: %v", err)
	}

	engine := swfEngineImpl{
		db:       gdb,
		udb:      sqlDB,
		workerId: "worker-tx",
		logger:   slog.Default(),
	}

	gormTx := gdb.Begin()
	if gormTx.Error != nil {
		t.Fatalf("begin tx: %v", gormTx.Error)
	}
	defer gormTx.Rollback()

	sqlTx := sqlTxFromGorm(gormTx)
	if sqlTx == nil {
		t.Fatalf("expected sql.Tx from gorm transaction")
	}

	jobKey := swf.JobKey{TenantId: "tenant-tx", JobId: "job-" + ksuid.New().String()}
	deps := pgwf.JobDependencies{NextNeed: pgwf.Capability("demo")}
	if err := pgwf.SubmitJob(ctx, sqlTx, pgwf.JobID(jobKey.JobId), deps, jobPayload{TenantId: jobKey.TenantId}, pgwf.WorkerID(engine.workerId), "", time.Time{}); err != nil {
		t.Fatalf("submit job in tx: %v", err)
	}

	ctxWithTx := swf.WithTx(ctx, gormTx)
	status, err := engine.CheckJobStatus(ctxWithTx, jobKey)
	if err != nil {
		t.Fatalf("status with tx: %v", err)
	}
	if status == "" {
		t.Fatalf("expected status when using context transaction")
	}

	_, err = engine.CheckJobStatus(ctx, jobKey)
	if !errors.Is(err, swf.ErrJobNotFound) {
		t.Fatalf("expected ErrJobNotFound outside tx, got %v", err)
	}
}
