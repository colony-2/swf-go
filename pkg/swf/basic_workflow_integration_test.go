package swf_test

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"net/http"
	"os"
	"testing"
	"time"

	strataclient "github.com/colony-2/strata/strata-go/pkg/client"
	"github.com/colony-2/strata/strata-go/pkg/client/core"
	"github.com/colony-2/strata/strata-go/pkg/client/story"
	"github.com/colony-2/swf-go/pkg/swf"
	"github.com/colony-2/swf-go/pkg/swf/impl"
	"github.com/fergusstrange/embedded-postgres"
	_ "github.com/lib/pq"
)

// TestBasicWorkflowIntegration exercises the simplest end-to-end flow:
// one job worker shared across two engines, with tasks split between them.
func TestBasicWorkflowIntegration(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start embedded Postgres on a random high port to avoid conflicts.
	pgPort := uint32(15432 + rand.Intn(1000))
	postgres := embeddedpostgres.NewDatabase(
		embeddedpostgres.DefaultConfig().Port(pgPort),
	)
	if err := postgres.Start(); err != nil {
		t.Fatalf("failed to start embedded postgres: %v", err)
	}
	defer func() {
		_ = postgres.Stop()
	}()

	postgresDSN := fmt.Sprintf("postgres://postgres:postgres@localhost:%d/postgres?sslmode=disable", pgPort)
	if err := installPGWF(ctx, postgresDSN); err != nil {
		t.Fatalf("failed to install pgwf schema: %v", err)
	}

	baseURL, strata := startStrata(t)
	defer strata.Shutdown()
	waitForStrataReady(t, baseURL)

	tenantID := "test-tenant"

	engine1, err := swf.NewEngineBuilder(tenantID).
		WithPostgresDSN(postgresDSN).
		WithStrata(baseURL).
		WithStrataAPIKey(strata.APIKey).
		PlusWorkers(pipeJob{}, addOneTask{}).
		Build(impl.Builder)
	if err != nil {
		t.Fatalf("failed to build engine1: %v", err)
	}

	engine2, err := swf.NewEngineBuilder(tenantID).
		WithPostgresDSN(postgresDSN).
		WithStrata(baseURL).
		WithStrataAPIKey(strata.APIKey).
		PlusWorkers(pipeJob{}, doubleTask{}).
		Build(impl.Builder)
	if err != nil {
		t.Fatalf("failed to build engine2: %v", err)
	}

	go engine1.Run(ctx)
	go engine2.Run(ctx)
	go userInputWatcher(ctx, t, engine1)

	initial := &swf.SimpleTaskData{
		Data: swf.NewMapData(map[string]interface{}{"n": 1}),
	}
	jobID, err := engine1.StartJob(ctx, swf.StartJob{
		JobType: pipeJobName,
		Data:    initial,
	})
	if err != nil {
		t.Fatalf("failed to start job: %v", err)
	}

	strataClient, err := strataclient.New(strataclient.Config{BaseURL: baseURL, APIKey: strata.APIKey})
	if err != nil {
		t.Fatalf("failed to create strata client: %v", err)
	}
	key := story.Key{AnthologyID: tenantID, StoryID: string(jobID)}

	// Expect five task chapters (ordinals 1-5) plus the final job output at ordinal 5.
	// Steps: t1(+1), t2(*2), userInput(+3), t1(+1), t2(*2) starting from 1 -> 2,4,7,8,16.
	expecteds := []int{2, 4, 7, 8, 16}
	for idx, expected := range expecteds {
		ordinal := int64(idx + 1) // job data is ordinal 0
		got := waitForChapterValue(t, strataClient, key, ordinal, 30*time.Second)
		if got != expected {
			t.Fatalf("ordinal %d: want %d, got %d", ordinal, expected, got)
		}
	}
}

// installPGWF runs the pgwf schema installer against the provided DSN.
func installPGWF(ctx context.Context, dsn string) error {
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return err
	}
	defer db.Close()

	// NOTE: adjust if the installer signature differs in pgwf-go.
	return impl.InstallPGWF(ctx, db)
}

// startStrata either uses an existing STRATA_BASE_URL or starts an embedded daemon if available.
type strataHandle struct {
	BaseURL  string
	APIKey   string
	Shutdown func()
}

func startStrata(t *testing.T) (string, *strataHandle) {
	if base := os.Getenv("STRATA_BASE_URL"); base != "" {
		apiKey := os.Getenv("STRATA_API_KEY")
		return base, &strataHandle{BaseURL: base, APIKey: apiKey, Shutdown: func() {}}
	}

	s, err := impl.StartEmbeddedStrata()
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
	var payload map[string]int
	if err := json.Unmarshal(body, &payload); err != nil {
		t.Fatalf("failed to decode chapter body: %v", err)
	}
	return payload["n"]
}

// --- Workers used in the integration scenario ---

const pipeJobName = "pipe"

type pipeJob struct{}

func (pipeJob) Name() string { return pipeJobName }

func (pipeJob) Run(ctx swf.JobContext, data swf.JobData) (swf.JobData, error) {
	current := taskNumber(data)
	payload := &swf.SimpleTaskData{Data: swf.NewMapData(map[string]interface{}{"n": current})}

	steps := []string{addOneTaskName, doubleTaskName, userInputTaskName, addOneTaskName, doubleTaskName}
	var err error
	var out swf.TaskData = payload
	for _, step := range steps {
		out, err = ctx.DoTask(step, out)
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

const addOneTaskName = "t1"

type addOneTask struct{}

func (addOneTask) Name() string { return addOneTaskName }

func (addOneTask) Run(_ swf.TaskContext, input swf.TaskData) (swf.TaskData, error) {
	n := taskNumber(input)
	return &swf.SimpleTaskData{Data: swf.NewMapData(map[string]interface{}{"n": n + 1})}, nil
}

const doubleTaskName = "t2"

type doubleTask struct{}

func (doubleTask) Name() string { return doubleTaskName }

func (doubleTask) Run(_ swf.TaskContext, input swf.TaskData) (swf.TaskData, error) {
	n := taskNumber(input)
	return &swf.SimpleTaskData{Data: swf.NewMapData(map[string]interface{}{"n": n * 2})}, nil
}

const userInputTaskName = "userInput"

// userInputWatcher completes externally-handled tasks that no engine claims.
func userInputWatcher(ctx context.Context, t *testing.T, engine swf.SWFEngine) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		handles, err := engine.FindTasksWaitingForCapability(ctx, pipeJobName, userInputTaskName)
		if err != nil {
			// If the database is shutting down or context will end soon, just back off.
			select {
			case <-ctx.Done():
				return
			case <-time.After(100 * time.Millisecond):
				continue
			}
		}
		for _, h := range handles {
			data, err := h.Data()
			if err != nil {
				t.Fatalf("watcher failed to get data: %v", err)
			}
			n := taskNumber(data)
			output := &swf.SimpleTaskData{Data: swf.NewMapData(map[string]interface{}{"n": n + 3})}
			if err := h.Finish(ctx, output); err != nil {
				t.Fatalf("watcher failed to finish task: %v", err)
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func taskNumber(td swf.TaskData) int {
	data, err := td.GetData()
	if err != nil {
		return 0
	}
	bytes, err := data.ToBytes()
	if err != nil {
		return 0
	}
	var payload map[string]int
	if err := json.Unmarshal(bytes, &payload); err != nil {
		return 0
	}
	return payload["n"]
}
