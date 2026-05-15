package sqlite

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"time"

	"github.com/colony-2/swf-go/pkg/swf"
)

type EmbeddedRuntime struct {
	Runtime *Runtime
	cleanup func()
}

type EmbeddedEngine struct {
	swf.SWFEngine
	runtime *Runtime
	cleanup func()
}

func (e *EmbeddedRuntime) Shutdown() {
	if e == nil || e.Runtime == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = e.Runtime.Close(ctx)
	if e.cleanup != nil {
		e.cleanup()
	}
}

func (e *EmbeddedEngine) Shutdown() {
	if e == nil || e.runtime == nil {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_ = e.runtime.Close(ctx)
	if e.cleanup != nil {
		e.cleanup()
	}
}

func StartEmbeddedRuntime(ctx context.Context) (*EmbeddedRuntime, error) {
	dir, err := testingTempDir()
	if err != nil {
		return nil, err
	}
	rt, err := NewFromConfig(ctx, Config{
		DBPath:  filepath.Join(dir, "swf.db"),
		BlobDir: filepath.Join(dir, "blobs"),
	})
	if err != nil {
		_ = os.RemoveAll(dir)
		return nil, err
	}
	return &EmbeddedRuntime{Runtime: rt, cleanup: func() { _ = os.RemoveAll(dir) }}, nil
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
		b.PlusWorkers(job, tasks...)
	}
	engine, err := b.BuildEngine()
	if err != nil {
		embedded.Shutdown()
		return nil, err
	}
	return &EmbeddedEngine{SWFEngine: engine, runtime: embedded.Runtime, cleanup: embedded.cleanup}, nil
}

func testingTempDir() (string, error) {
	return os.MkdirTemp("", "swf-sqlite-*")
}
