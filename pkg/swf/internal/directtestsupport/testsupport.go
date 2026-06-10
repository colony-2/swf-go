package directtestsupport

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"os"
	"path/filepath"

	"github.com/colony-2/pgwf-go/installer"
	"github.com/colony-2/strata-go/pkg/daemon"
	"github.com/fergusstrange/embedded-postgres"
)

func InstallPGWF(ctx context.Context, db *sql.DB) error {
	inst := installer.Installer{DB: db}
	if err := inst.Apply(ctx); err != nil {
		return err
	}
	return inst.Verify(ctx)
}

type EmbeddedStrataHandle struct {
	BaseURL  string
	APIKey   string
	Shutdown func()
}

func StartEmbeddedStrata() (*EmbeddedStrataHandle, error) {
	rowDir, err := os.MkdirTemp("", "strata-rows-*")
	if err != nil {
		return nil, fmt.Errorf("create row store dir: %w", err)
	}
	blobDir, err := os.MkdirTemp("", "strata-blobs-*")
	if err != nil {
		_ = os.RemoveAll(rowDir)
		return nil, fmt.Errorf("create blob store dir: %w", err)
	}

	cfg := daemon.Config{
		ListenAddr:             "127.0.0.1:0",
		RowStoreURI:            fmt.Sprintf("sqlite://%s", filepath.ToSlash(filepath.Join(rowDir, "strata.db"))),
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

func StartEmbeddedPostgres() (string, func(), error) {
	pgPort, err := freeTCPPort()
	if err != nil {
		return "", nil, err
	}
	tmpDir, err := os.MkdirTemp("", "pgwf-embedded-*")
	if err != nil {
		return "", nil, fmt.Errorf("temp dir: %w", err)
	}
	runtimePath := filepath.Join(tmpDir, "runtime")
	dataPath := filepath.Join(tmpDir, "data")
	_ = os.MkdirAll(runtimePath, 0o755)
	_ = os.MkdirAll(dataPath, 0o755)

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

func freeTCPPort() (uint32, error) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return 0, fmt.Errorf("reserve postgres port: %w", err)
	}
	defer listener.Close()

	addr, ok := listener.Addr().(*net.TCPAddr)
	if !ok {
		return 0, fmt.Errorf("reserve postgres port: unexpected addr type %T", listener.Addr())
	}
	return uint32(addr.Port), nil
}
