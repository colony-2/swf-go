package sqlite

import (
	"context"
	"os"
	"path/filepath"
	"time"
)

type EmbeddedRuntime struct {
	Runtime *Runtime
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

func StartEmbeddedRuntime(ctx context.Context) (*EmbeddedRuntime, error) {
	dir, err := testingTempDir()
	if err != nil {
		return nil, err
	}
	rt, err := NewFromConfig(ctx, Config{
		DBPath:  filepath.Join(dir, "jobdb.db"),
		BlobDir: filepath.Join(dir, "blobs"),
	})
	if err != nil {
		_ = os.RemoveAll(dir)
		return nil, err
	}
	return &EmbeddedRuntime{Runtime: rt, cleanup: func() { _ = os.RemoveAll(dir) }}, nil
}

func testingTempDir() (string, error) {
	return os.MkdirTemp("", "jobdb-sqlite-*")
}
