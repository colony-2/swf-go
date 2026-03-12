package impl

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"math/rand"
	"os"
	"path/filepath"
	"time"

	"github.com/colony-2/pgwf-go/installer"
	"github.com/colony-2/pgwf-go/pkg/pgwf"
	"github.com/colony-2/strata-go/pkg/daemon"
	"github.com/colony-2/swf-go/pkg/swf"
	"github.com/fergusstrange/embedded-postgres"
)

// InstallPGWF installs the pgwf schema into the provided Postgres instance.
// Adjust the implementation if the upstream installer lives in a different package.
func InstallPGWF(ctx context.Context, db *sql.DB) error {
	inst := installer.Installer{DB: db}
	if err := inst.Apply(ctx); err != nil {
		return err
	}
	return inst.Verify(ctx)
}

// EmbeddedStrataHandle is a lightweight handle to an embedded Strata daemon.
type EmbeddedStrataHandle struct {
	BaseURL  string
	APIKey   string
	Shutdown func()
}

// StartEmbeddedStrata spins up an embedded Strata daemon for tests.
func StartEmbeddedStrata() (*EmbeddedStrataHandle, error) {
	rowDir, err := os.MkdirTemp("", "strata-rows-*")
	if err != nil {
		return nil, fmt.Errorf("create row store dir: %w", err)
	}
	blobDir, err := os.MkdirTemp("", "strata-blobs-*")
	if err != nil {
		os.RemoveAll(rowDir)
		return nil, fmt.Errorf("create blob store dir: %w", err)
	}

	cfg := daemon.Config{
		ListenAddr:             "127.0.0.1:0",
		RowStoreURI:            fmt.Sprintf("pebble://%s", filepath.ToSlash(rowDir)),
		BlobStoreURI:           fmt.Sprintf("blobfs://%s", filepath.ToSlash(blobDir)),
		MaxInlineArtifactBytes: daemon.DefaultMaxInlineArtifactBytes,
	}

	d, err := daemon.StartEmbedded(context.Background(), cfg)
	if err != nil {
		_ = os.RemoveAll(rowDir)
		_ = os.RemoveAll(blobDir)
		return nil, err
	}
	addr, err := d.Addr()
	if err != nil {
		_ = d.Shutdown(context.Background())
		_ = os.RemoveAll(rowDir)
		_ = os.RemoveAll(blobDir)
		return nil, err
	}

	return &EmbeddedStrataHandle{
		BaseURL: "http://" + addr,
		APIKey:  "test-token",
		Shutdown: func() {
			_ = d.Shutdown(context.Background())
			_ = os.RemoveAll(rowDir)
			_ = os.RemoveAll(blobDir)
		},
	}, nil
}

// StartEmbeddedPostgres spins up an embedded Postgres with isolated runtime/data/cache and returns DSN and stop func.
func StartEmbeddedPostgres() (string, func(), error) {
	pgPort := uint32(20000 + rand.Intn(1000))
	tmpDir, err := os.MkdirTemp("", "pgwf-embedded-*")
	if err != nil {
		return "", nil, fmt.Errorf("temp dir: %w", err)
	}
	runtimePath := filepath.Join(tmpDir, "runtime")
	dataPath := filepath.Join(tmpDir, "data")
	cachePath := filepath.Join(tmpDir, "cache")
	_ = os.MkdirAll(runtimePath, 0o755)
	_ = os.MkdirAll(dataPath, 0o755)
	_ = os.MkdirAll(cachePath, 0o755)

	postgres := embeddedpostgres.NewDatabase(
		embeddedpostgres.DefaultConfig().
			Port(pgPort).
			RuntimePath(runtimePath).
			DataPath(dataPath),
	)
	if err := postgres.Start(); err != nil {
		return "", nil, err
	}
	stop := func() {
		_ = postgres.Stop()
		_ = os.RemoveAll(tmpDir)
	}
	dsn := fmt.Sprintf("postgres://postgres:postgres@localhost:%d/postgres?sslmode=disable", pgPort)
	return dsn, stop, nil
}

func StartEmbeddedEngine(ctx context.Context, job swf.JobWorker, tasks ...swf.TaskWorker) (*EmbeddedEngine, error) {
	dsn, stopPG, err := StartEmbeddedPostgres()
	if err != nil {
		return nil, err
	}

	db, err := sql.Open("postgres", dsn)
	if err != nil {
		stopPG()
		return nil, err
	}
	cleanup := func() {
		_ = db.Close()
		stopPG()
	}

	// Engine setup can be noticeably slower on some macOS machines.
	// Avoid coupling bootstrap time to a short caller timeout intended for the test body.
	setupCtx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	if err := InstallPGWF(setupCtx, db); err != nil {
		cleanup()
		return nil, err
	}

	s, err := StartEmbeddedStrata()
	if err != nil {
		cleanup()
		return nil, err
	}

	b := swf.NewEngineBuilder().
		WithAwaitRecycleThreshold(5 * time.Second).
		WithPostgresDSN(dsn).
		WithStrata(s.BaseURL).
		WithStrataAPIKey(s.APIKey).
		WithLogger(slog.Default()).
		WithMaxActive(100)

	if job != nil {
		b.PlusWorkers(job)
	}

	engine, err := b.Build(Builder)

	if err != nil {
		s.Shutdown()
		cleanup()
		return nil, err
	}
	full := &EmbeddedEngine{
		SWFEngine:      engine,
		stopPG:         cleanup,
		strataShutdown: s.Shutdown,
	}

	return full, nil

}

func newRunnerForTest(engine *swfEngineImpl, lease *pgwf.Lease, ws *swf.WorkSet, ctx context.Context) *runner {
	if engine == nil {
		return &runner{}
	}
	var cap pgwf.Capability
	if lease != nil {
		cap = lease.NextNeed()
	}
	leaseAdapter := newPgwfLeaseAdapter(lease, engine.udb)
	backend := &defaultRunnerBackend{
		engine:     engine,
		lease:      leaseAdapter,
		pgwfLease:  lease,
		capability: cap,
	}
	r := &runner{
		engine:       engine,
		worker:       ws,
		storyCounter: 1,
		backend:      backend,
		lease:        leaseAdapter,
		logger:       engine.logger,
		jobPolicy:    normalizeRunPolicy(swf.RunPolicy{}),
		capability:   cap,
		ctx:          ctx,
		workerId:     engine.workerId,
		observer:     noopReplayObserver{},
	}
	if lease != nil {
		r.jobId = lease.JobID()
		r.tenantId = string(lease.TenantID())
	}
	return r
}

type EmbeddedEngine struct {
	swf.SWFEngine
	stopPG         func()
	strataShutdown func()
}

func (e *EmbeddedEngine) Shutdown() {
	e.stopPG()
	e.strataShutdown()
}
