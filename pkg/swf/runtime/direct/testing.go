package direct

import (
	"context"
	"database/sql"
	"log/slog"
	"time"

	"github.com/colony-2/swf-go/pkg/swf"
	"github.com/colony-2/swf-go/pkg/swf/internal/directtestsupport"
)

type EmbeddedEngine struct {
	swf.SWFEngine
	stopPG         func()
	strataShutdown func()
}

type EmbeddedRuntime struct {
	Runtime        *Runtime
	stopPG         func()
	strataShutdown func()
}

func (e *EmbeddedEngine) Shutdown() {
	if e == nil {
		return
	}
	e.stopPG()
	e.strataShutdown()
}

func (e *EmbeddedRuntime) Shutdown() {
	if e == nil {
		return
	}
	e.stopPG()
	e.strataShutdown()
}

func StartEmbeddedRuntime(ctx context.Context) (*EmbeddedRuntime, error) {
	dsn, stopPG, err := directtestsupport.StartEmbeddedPostgres()
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

	setupCtx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	if err := directtestsupport.InstallPGWF(setupCtx, db); err != nil {
		cleanup()
		return nil, err
	}
	s, err := directtestsupport.StartEmbeddedStrata()
	if err != nil {
		cleanup()
		return nil, err
	}

	rt, err := NewFromConfig(dsn, s.BaseURL, s.APIKey)
	if err != nil {
		s.Shutdown()
		cleanup()
		return nil, err
	}

	return &EmbeddedRuntime{
		Runtime:        rt,
		stopPG:         cleanup,
		strataShutdown: s.Shutdown,
	}, nil
}

func StartEmbeddedEngine(ctx context.Context, job swf.JobWorker, tasks ...swf.TaskWorker) (*EmbeddedEngine, error) {
	embedded, err := StartEmbeddedRuntime(ctx)
	if err != nil {
		return nil, err
	}

	b := swf.NewEngineBuilder().
		WithRuntime(embedded.Runtime).
		WithAwaitRecycleThreshold(5 * time.Second).
		WithLogger(slog.Default()).
		WithMaxActive(100)

	if job != nil {
		b.WithWorkerTenantId("default")
		b.PlusWorkers(job, tasks...)
	}
	engine, err := b.BuildEngine()
	if err != nil {
		embedded.Shutdown()
		return nil, err
	}

	return &EmbeddedEngine{
		SWFEngine:      engine,
		stopPG:         embedded.stopPG,
		strataShutdown: embedded.strataShutdown,
	}, nil
}
