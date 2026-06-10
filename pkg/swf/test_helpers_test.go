package swf_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"net/http"
	"os"
	"testing"
	"time"

	strataclient "github.com/colony-2/strata-go/pkg/client"
	"github.com/colony-2/strata-go/pkg/client/core"
	"github.com/colony-2/strata-go/pkg/client/story"
	"github.com/colony-2/swf-go/pkg/swf"
	directtest "github.com/colony-2/swf-go/pkg/swf/internal/directtestsupport"
	"github.com/colony-2/swf-go/pkg/swf/internal/runtimecodec"
	directruntime "github.com/colony-2/swf-go/pkg/swf/runtime/direct"
	toyruntime "github.com/colony-2/swf-go/pkg/swf/runtime/toy"
)

// startEmbeddedPostgres launches a temporary embedded Postgres instance with isolated paths.
func startEmbeddedPostgres(t *testing.T) (string, func()) {
	t.Helper()
	dsn, stop, err := directtest.StartEmbeddedPostgres()
	if err != nil {
		t.Fatalf("failed to start embedded postgres: %v", err)
	}
	return dsn, stop
}

// installPGWF runs the pgwf schema installer against the provided DSN.
func installPGWF(ctx context.Context, dsn string) error {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return err
	}
	defer db.Close()
	return directtest.InstallPGWF(ctx, db)
}

// startStrata either uses an existing STRATA_BASE_URL or starts an embedded daemon if available.
type strataHandle struct {
	BaseURL  string
	APIKey   string
	Shutdown func()
}

func startStrata(t *testing.T) (string, *strataHandle) {
	t.Helper()
	if base := os.Getenv("STRATA_BASE_URL"); base != "" {
		apiKey := os.Getenv("STRATA_API_KEY")
		return base, &strataHandle{BaseURL: base, APIKey: apiKey, Shutdown: func() {}}
	}

	s, err := directtest.StartEmbeddedStrata()
	if err != nil {
		t.Fatalf("failed to start embedded strata: %v", err)
	}
	return s.BaseURL, &strataHandle{BaseURL: s.BaseURL, APIKey: s.APIKey, Shutdown: s.Shutdown}
}

func waitForStrataReady(t *testing.T, baseURL string) {
	t.Helper()
	client := &http.Client{Timeout: 2 * time.Second}
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		resp, err := client.Get(baseURL)
		if err == nil {
			resp.Body.Close()
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("strata not ready at %s", baseURL)
}

// waitForChapterValue polls Strata for a chapter and decodes "n" from its payload.
func waitForChapterValue(t *testing.T, client *strataclient.Client, key story.Key, ordinal int64, timeout time.Duration) int {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		chap, err := client.Chapter(context.Background(), key, ordinal)
		if err == nil {
			return decodeNumber(t, chap.Body())
		}
		if !errors.Is(err, core.ErrNotFound) {
			t.Fatalf("unexpected error fetching chapter %d: %v", ordinal, err)
		}
		time.Sleep(200 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for chapter %d", ordinal)
	return 0
}

func decodeNumber(t *testing.T, body []byte) int {
	t.Helper()
	env, err := runtimecodec.DecodeChapter(body)
	if err != nil {
		t.Fatalf("failed to decode chapter body: %v", err)
	}
	if env.PayloadKind != runtimecodec.PayloadKindApp {
		t.Fatalf("unexpected payload kind %q", env.PayloadKind)
	}

	var payload map[string]int
	if err := json.Unmarshal(env.Payload, &payload); err != nil {
		t.Fatalf("failed to decode payload: %v", err)
	}
	return payload["n"]
}

// randPort helper to avoid collisions in some legacy tests.
func randPort(base uint32) uint32 {
	return base
}

func buildDirectEngine(t *testing.T, postgresDSN, baseURL, apiKey string, configure func(*swf.EngineBuilder)) swf.SWFEngine {
	t.Helper()
	runtime, err := directruntime.NewFromConfig(postgresDSN, baseURL, apiKey)
	if err != nil {
		t.Fatalf("create direct runtime: %v", err)
	}
	builder := swf.NewEngineBuilder()
	builder.WithRuntime(runtime)
	if configure != nil {
		configure(builder)
	}
	engine, err := builder.BuildEngine()
	if err != nil {
		t.Fatalf("build engine: %v", err)
	}
	return engine
}

func buildToyEngine(t *testing.T, configure func(*swf.EngineBuilder), opts ...toyruntime.Option) (swf.SWFEngine, context.CancelFunc) {
	t.Helper()
	runtime := toyruntime.New(opts...)
	builder := swf.NewEngineBuilder().WithRuntime(runtime)
	if configure != nil {
		configure(builder)
	}
	engine, err := builder.BuildEngine()
	if err != nil {
		t.Fatalf("build toy engine: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	go engine.Run(ctx)
	return engine, cancel
}

func storyKeyForJob(jobKey swf.JobKey) story.Key {
	return story.Key{AnthologyID: jobKey.TenantId, StoryID: jobKey.JobId}
}
