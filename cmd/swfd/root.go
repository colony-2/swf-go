package main

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/colony-2/pgwf-go/installer"
	"github.com/colony-2/strata-go/pkg/daemon"
	directruntime "github.com/colony-2/swf-go/pkg/swf/runtime/direct"
	remoteruntime "github.com/colony-2/swf-go/pkg/swf/runtime/remote"
	sqliteruntime "github.com/colony-2/swf-go/pkg/swf/runtime/sqlite"
	toyruntime "github.com/colony-2/swf-go/pkg/swf/runtime/toy"
	"github.com/spf13/cobra"

	_ "github.com/lib/pq"
)

const (
	defaultListenAddr      = "127.0.0.1:9047"
	postgresDSNEnvVar      = "SWF_POSTGRES_DSN"
	sqliteDSNEnvVar        = "SWF_SQLITE_DSN"
	embeddedStrataAPIKey   = "local-dev-token"
	defaultSetupTimeout    = 45 * time.Second
	defaultShutdownTimeout = 10 * time.Second
)

var serveHTTPFunc = serveHTTP

func newRootCmd() *cobra.Command {
	var listenAddr string
	var dbPath string
	var sqliteDSN string
	var blobDir string

	cmd := &cobra.Command{
		Use:          "swfd",
		Short:        "Run local SWF runtime servers",
		SilenceUsage: true,
		Args:         cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runSQLite(cmd.Context(), listenAddr, sqliteConfigFromFlags(dbPath, sqliteDSN, blobDir))
		},
	}

	cmd.AddCommand(
		newSQLiteCmd(&listenAddr, &dbPath, &sqliteDSN, &blobDir),
		newToyCmd(&listenAddr),
		newDirectCmd(&listenAddr),
	)
	cmd.PersistentFlags().StringVar(&listenAddr, "listen", defaultListenAddr, "listen address for the HTTP API")
	cmd.PersistentFlags().StringVar(&dbPath, "db", "swf.db", "SQLite database path for the default embedded runtime")
	cmd.PersistentFlags().StringVar(&sqliteDSN, "sqlite-dsn", "", "SQLite DSN for the default embedded runtime (overrides --db and "+sqliteDSNEnvVar+")")
	cmd.PersistentFlags().StringVar(&blobDir, "blob-dir", "", "blobfs directory for large artifacts (defaults to <db>.blobs)")

	return cmd
}

func newSQLiteCmd(listenAddr *string, dbPath *string, sqliteDSN *string, blobDir *string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "sqlite",
		Short: "Run a SQLite-backed embedded workflow runtime over HTTP",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runSQLite(cmd.Context(), *listenAddr, sqliteConfigFromFlags(*dbPath, *sqliteDSN, *blobDir))
		},
	}
	return cmd
}

func newToyCmd(listenAddr *string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "toy",
		Short: "Run a toy workflow runtime over HTTP",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			return runToy(cmd.Context(), *listenAddr)
		},
	}

	return cmd
}

func newDirectCmd(listenAddr *string) *cobra.Command {
	var postgresDSN string

	cmd := &cobra.Command{
		Use:   "direct",
		Short: "Run a direct runtime with embedded Strata and postgres-backed pgwf",
		Args:  cobra.NoArgs,
		RunE: func(cmd *cobra.Command, _ []string) error {
			dsn, err := resolveRequiredString(postgresDSN, postgresDSNEnvVar, "postgres DSN")
			if err != nil {
				return err
			}

			setupCtx, cancel := context.WithTimeout(cmd.Context(), defaultSetupTimeout)
			defer cancel()

			if err := installPGWF(setupCtx, dsn); err != nil {
				return fmt.Errorf("install pgwf schema: %w", err)
			}

			strata, err := startEmbeddedStrata(setupCtx)
			if err != nil {
				return fmt.Errorf("start embedded strata: %w", err)
			}

			runtime, err := directruntime.NewFromConfig(dsn, strata.BaseURL, strata.APIKey)
			if err != nil {
				shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), defaultShutdownTimeout)
				defer shutdownCancel()
				return errors.Join(
					fmt.Errorf("build direct runtime: %w", err),
					strata.Shutdown(shutdownCtx),
				)
			}

			log.Printf("embedded strata listening at %s", strata.BaseURL)
			return serveHTTPFunc(cmd.Context(), *listenAddr, remoteruntime.NewServer(runtime), strata.Shutdown)
		},
	}

	cmd.Flags().StringVar(&postgresDSN, "postgres-dsn", "", "postgres DSN for pgwf state (overrides "+postgresDSNEnvVar+")")
	return cmd
}

func runToy(ctx context.Context, listenAddr string) error {
	runtime := toyruntime.New()
	return serveHTTPFunc(ctx, listenAddr, remoteruntime.NewServer(runtime), nil)
}

func runSQLite(ctx context.Context, listenAddr string, cfg sqliteruntime.Config) error {
	runtime, err := sqliteruntime.NewFromConfig(ctx, cfg)
	if err != nil {
		return fmt.Errorf("build SQLite runtime: %w", err)
	}
	log.Printf("using SQLite runtime")
	return serveHTTPFunc(ctx, listenAddr, remoteruntime.NewServer(runtime), runtime.Close)
}

func sqliteConfigFromFlags(dbPath string, sqliteDSN string, blobDir string) sqliteruntime.Config {
	cfg := sqliteruntime.Config{
		DBPath:  dbPath,
		BlobDir: blobDir,
	}
	if sqliteDSN != "" {
		cfg.DSN = sqliteDSN
		cfg.DBPath = ""
		return cfg
	}
	if envValue := os.Getenv(sqliteDSNEnvVar); envValue != "" {
		cfg.DSN = envValue
		cfg.DBPath = ""
	}
	return cfg
}

func serveHTTP(ctx context.Context, listenAddr string, handler http.Handler, cleanup func(context.Context) error) error {
	if ctx == nil {
		ctx = context.Background()
	}

	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return fmt.Errorf("listen on %s: %w", listenAddr, err)
	}
	defer listener.Close()

	server := &http.Server{Handler: handler}
	stopShutdown := make(chan struct{})
	defer close(stopShutdown)

	go func() {
		select {
		case <-ctx.Done():
			shutdownCtx, cancel := context.WithTimeout(context.Background(), defaultShutdownTimeout)
			defer cancel()
			_ = server.Shutdown(shutdownCtx)
		case <-stopShutdown:
		}
	}()

	log.Printf("serving runtime API on http://%s", listener.Addr().String())
	err = server.Serve(listener)
	if errors.Is(err, http.ErrServerClosed) {
		err = nil
	}

	if cleanup != nil {
		cleanupCtx, cancel := context.WithTimeout(context.Background(), defaultShutdownTimeout)
		defer cancel()
		err = errors.Join(err, cleanup(cleanupCtx))
	}

	return err
}

func resolveRequiredString(flagValue, envVar, fieldName string) (string, error) {
	if flagValue != "" {
		return flagValue, nil
	}
	if envValue := os.Getenv(envVar); envValue != "" {
		return envValue, nil
	}
	return "", fmt.Errorf("%s is required via --postgres-dsn or %s", fieldName, envVar)
}

func installPGWF(ctx context.Context, dsn string) error {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return fmt.Errorf("open postgres: %w", err)
	}
	defer db.Close()

	if err := db.PingContext(ctx); err != nil {
		return fmt.Errorf("ping postgres: %w", err)
	}

	inst := installer.Installer{DB: db}
	if err := inst.Apply(ctx); err != nil {
		return err
	}
	return inst.Verify(ctx)
}

type embeddedStrataHandle struct {
	BaseURL string
	APIKey  string
	close   func(context.Context) error
}

func (h *embeddedStrataHandle) Shutdown(ctx context.Context) error {
	if h == nil || h.close == nil {
		return nil
	}
	return h.close(ctx)
}

func startEmbeddedStrata(ctx context.Context) (*embeddedStrataHandle, error) {
	rowDir, err := os.MkdirTemp("", "swfd-strata-rows-*")
	if err != nil {
		return nil, fmt.Errorf("create row store dir: %w", err)
	}
	blobDir, err := os.MkdirTemp("", "swfd-strata-blobs-*")
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

	d, err := daemon.StartEmbedded(ctx, cfg)
	if err != nil {
		_ = os.RemoveAll(rowDir)
		_ = os.RemoveAll(blobDir)
		return nil, err
	}

	addr, err := d.Addr()
	if err != nil {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), defaultShutdownTimeout)
		defer cancel()
		_ = d.Shutdown(shutdownCtx)
		_ = os.RemoveAll(rowDir)
		_ = os.RemoveAll(blobDir)
		return nil, fmt.Errorf("resolve embedded strata address: %w", err)
	}

	return &embeddedStrataHandle{
		BaseURL: "http://" + addr,
		APIKey:  embeddedStrataAPIKey,
		close: func(ctx context.Context) error {
			return errors.Join(
				d.Shutdown(ctx),
				os.RemoveAll(rowDir),
				os.RemoveAll(blobDir),
			)
		},
	}, nil
}
