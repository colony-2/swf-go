package sqlite

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	strataclient "github.com/colony-2/strata-go/pkg/client"
	"github.com/colony-2/strata-go/pkg/daemon/storage/blobstore"
	sqliterowstore "github.com/colony-2/strata-go/pkg/daemon/storage/sqlite"
	"github.com/colony-2/swf-go/pkg/swf"
	"github.com/segmentio/ksuid"

	_ "modernc.org/sqlite"
)

const (
	sqliteDriverName             = "sqlite"
	defaultRemoteLeaseDuration   = 30 * time.Second
	defaultSQLiteBusyTimeoutMS   = 5000
	defaultInlineArtifactMaxSize = 256
)

// Config describes a local SQLite-backed SWF runtime.
type Config struct {
	// DSN is passed to modernc.org/sqlite. If empty, DBPath is used.
	DSN string
	// DBPath is the durable SQLite database path used when DSN is empty.
	DBPath string
	// BlobDir stores large Strata artifacts. If empty, it is derived from DBPath.
	BlobDir string
	// Logger is used for runtime diagnostics.
	Logger *slog.Logger
	// WorkerID overrides the runtime's fallback worker id.
	WorkerID string
}

// Runtime is a SQLite WorkflowRuntime that composes a pgwf-like scheduler table
// with strata-go's SQLite rowstore and blobfs artifact storage.
type Runtime struct {
	db           *sql.DB
	ownsDB       bool
	strataClient *strataclient.Client
	logger       *slog.Logger
	workerID     string

	closeOnce sync.Once
	closeErr  error
}

var _ swf.WorkflowRuntime = (*Runtime)(nil)

// New builds a runtime around caller-owned SQLite and Strata client handles.
func New(db *sql.DB, strataClient *strataclient.Client, opts ...Option) (*Runtime, error) {
	if db == nil {
		return nil, fmt.Errorf("sqlite runtime: db is required")
	}
	if strataClient == nil {
		return nil, fmt.Errorf("sqlite runtime: strata client is required")
	}
	cfg := runtimeOptions{
		logger: slog.Default(),
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	if cfg.workerID == "" {
		cfg.workerID = defaultWorkerID()
	}
	rt := &Runtime{
		db:           db,
		strataClient: strataClient,
		logger:       cfg.logger,
		workerID:     cfg.workerID,
	}
	if err := migrate(ctxOrBackground(nil), db); err != nil {
		return nil, err
	}
	return rt, nil
}

// NewFromConfig opens a SQLite database, creates a strata-go embedded client
// over the same *sql.DB rowstore, and returns the composed runtime.
func NewFromConfig(ctx context.Context, cfg Config) (*Runtime, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	dsn, dbPath, err := resolveDSN(cfg)
	if err != nil {
		return nil, err
	}
	if dbPath != "" {
		if err := os.MkdirAll(filepath.Dir(dbPath), 0o755); err != nil {
			return nil, fmt.Errorf("sqlite runtime: create db dir: %w", err)
		}
	}
	blobDir, err := resolveBlobDir(cfg, dbPath)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(blobDir, 0o755); err != nil {
		return nil, fmt.Errorf("sqlite runtime: create blob dir: %w", err)
	}

	db, err := sql.Open(sqliteDriverName, dsn)
	if err != nil {
		return nil, fmt.Errorf("sqlite runtime: open db: %w", err)
	}
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	cleanupDB := true
	defer func() {
		if cleanupDB {
			_ = db.Close()
		}
	}()

	if err := configureSQLite(ctx, db); err != nil {
		return nil, err
	}
	if err := migrate(ctx, db); err != nil {
		return nil, err
	}

	rows, err := sqliterowstore.New(db)
	if err != nil {
		return nil, fmt.Errorf("sqlite runtime: create strata rowstore: %w", err)
	}
	blobs, err := blobstore.NewFS(blobDir)
	if err != nil {
		return nil, fmt.Errorf("sqlite runtime: create strata blobstore: %w", err)
	}
	strataClient, err := strataclient.NewBuilder().
		WithEmbeddedStores(rows, blobs).
		WithRetryPolicy(strataclient.RetryPolicy{MaxRetries: 0}).
		WithLogger(logger).
		Build(ctx)
	if err != nil {
		return nil, fmt.Errorf("sqlite runtime: build embedded strata client: %w", err)
	}

	workerID := cfg.WorkerID
	if workerID == "" {
		workerID = defaultWorkerID()
	}
	cleanupDB = false
	return &Runtime{
		db:           db,
		ownsDB:       true,
		strataClient: strataClient,
		logger:       logger,
		workerID:     workerID,
	}, nil
}

// Close releases resources owned by a runtime constructed with NewFromConfig.
func (r *Runtime) Close(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if r == nil {
		return nil
	}
	r.closeOnce.Do(func() {
		var errs []error
		if r.strataClient != nil {
			errs = append(errs, r.strataClient.Close(ctx))
		}
		if r.ownsDB && r.db != nil {
			errs = append(errs, r.db.Close())
		}
		r.closeErr = errors.Join(errs...)
	})
	return r.closeErr
}

type runtimeOptions struct {
	logger   *slog.Logger
	workerID string
}

// Option customizes New.
type Option func(*runtimeOptions)

func WithLogger(logger *slog.Logger) Option {
	return func(o *runtimeOptions) {
		if logger != nil {
			o.logger = logger
		}
	}
}

func WithWorkerID(workerID string) Option {
	return func(o *runtimeOptions) {
		o.workerID = workerID
	}
}

func (r *Runtime) validate() error {
	if r == nil {
		return fmt.Errorf("sqlite runtime is required")
	}
	if r.db == nil {
		return fmt.Errorf("sqlite runtime db is required")
	}
	if r.strataClient == nil {
		return fmt.Errorf("sqlite runtime strata client is required")
	}
	return nil
}

func (r *Runtime) requestWorkerID(workerID string) string {
	if workerID != "" {
		return workerID
	}
	return r.workerID
}

func defaultWorkerID() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "swf"
	}
	return fmt.Sprintf("%s:%d-%s", host, os.Getpid(), ksuid.New().String())
}

func resolveDSN(cfg Config) (dsn string, dbPath string, err error) {
	if strings.TrimSpace(cfg.DSN) != "" {
		return cfg.DSN, "", nil
	}
	path := strings.TrimSpace(cfg.DBPath)
	if path == "" {
		path = "swf.db"
	}
	abs, err := filepath.Abs(path)
	if err != nil {
		return "", "", fmt.Errorf("sqlite runtime: resolve db path: %w", err)
	}
	return abs, abs, nil
}

func resolveBlobDir(cfg Config, dbPath string) (string, error) {
	if strings.TrimSpace(cfg.BlobDir) != "" {
		return filepath.Abs(cfg.BlobDir)
	}
	if dbPath != "" {
		return dbPath + ".blobs", nil
	}
	return filepath.Abs("swf.blobs")
}

func configureSQLite(ctx context.Context, db *sql.DB) error {
	pragmas := []string{
		"PRAGMA foreign_keys = ON",
		fmt.Sprintf("PRAGMA busy_timeout = %d", defaultSQLiteBusyTimeoutMS),
		"PRAGMA journal_mode = WAL",
		"PRAGMA synchronous = NORMAL",
	}
	for _, pragma := range pragmas {
		if _, err := db.ExecContext(ctx, pragma); err != nil {
			return fmt.Errorf("sqlite runtime: %s: %w", pragma, err)
		}
	}
	return nil
}

func ctxOrBackground(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}
