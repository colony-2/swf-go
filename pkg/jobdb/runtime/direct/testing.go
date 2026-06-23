package direct

import (
	"context"
	"database/sql"
	"time"

	"github.com/colony-2/jobdb/pkg/internal/directtestsupport"
)

type EmbeddedRuntime struct {
	Runtime        *Runtime
	stopPG         func()
	strataShutdown func()
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
