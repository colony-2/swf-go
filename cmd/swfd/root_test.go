package main

import (
	"context"
	"io"
	"net/http"
	"path/filepath"
	"testing"
)

func TestRootCommandDefaultsToSQLite(t *testing.T) {
	origServeHTTP := serveHTTPFunc
	defer func() {
		serveHTTPFunc = origServeHTTP
	}()

	called := 0
	var gotListenAddr string
	serveHTTPFunc = func(ctx context.Context, listenAddr string, _ http.Handler, cleanup func(context.Context) error) error {
		called++
		gotListenAddr = listenAddr
		if cleanup != nil {
			if err := cleanup(ctx); err != nil {
				t.Fatalf("cleanup returned error: %v", err)
			}
		}
		return nil
	}

	cmd := newRootCmd()
	cmd.SetArgs([]string{
		"--listen", "127.0.0.1:9999",
		"--db", filepath.Join(t.TempDir(), "swf.db"),
	})
	cmd.SetOut(io.Discard)
	cmd.SetErr(io.Discard)

	if err := cmd.ExecuteContext(context.Background()); err != nil {
		t.Fatalf("ExecuteContext returned error: %v", err)
	}
	if called != 1 {
		t.Fatalf("serveHTTPFunc called %d times, want 1", called)
	}
	if gotListenAddr != "127.0.0.1:9999" {
		t.Fatalf("listen address = %q, want custom root flag value", gotListenAddr)
	}
}

func TestResolveRequiredStringPrefersFlag(t *testing.T) {
	t.Setenv(postgresDSNEnvVar, "postgres://env")

	got, err := resolveRequiredString("postgres://flag", postgresDSNEnvVar, "postgres DSN")
	if err != nil {
		t.Fatalf("resolveRequiredString returned error: %v", err)
	}
	if got != "postgres://flag" {
		t.Fatalf("resolveRequiredString = %q, want flag value", got)
	}
}

func TestResolveRequiredStringFallsBackToEnv(t *testing.T) {
	t.Setenv(postgresDSNEnvVar, "postgres://env")

	got, err := resolveRequiredString("", postgresDSNEnvVar, "postgres DSN")
	if err != nil {
		t.Fatalf("resolveRequiredString returned error: %v", err)
	}
	if got != "postgres://env" {
		t.Fatalf("resolveRequiredString = %q, want env value", got)
	}
}

func TestResolveRequiredStringRequiresValue(t *testing.T) {
	t.Setenv(postgresDSNEnvVar, "")

	_, err := resolveRequiredString("", postgresDSNEnvVar, "postgres DSN")
	if err == nil {
		t.Fatal("resolveRequiredString returned nil error, want failure")
	}
	if got, want := err.Error(), "postgres DSN is required via --postgres-dsn or "+postgresDSNEnvVar; got != want {
		t.Fatalf("resolveRequiredString error = %q, want %q", got, want)
	}
}
