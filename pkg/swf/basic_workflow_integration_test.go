package swf_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"sync"
	"testing"
	"time"

	strataclient "github.com/colony-2/strata-go/pkg/client"
	"github.com/colony-2/strata-go/pkg/client/story"
	"github.com/colony-2/swf-go/pkg/swf"
	"github.com/colony-2/swf-go/pkg/swf/impl"
	_ "github.com/lib/pq"
)

// TestBasicWorkflowIntegration exercises the simplest end-to-end flow:
// one job worker shared across two engines, with tasks split between them.
func TestBasicWorkflowIntegration(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start embedded Postgres on a random high port to avoid conflicts.
	postgresDSN, stopPG := startEmbeddedPostgres(t)
	defer stopPG()
	if err := installPGWF(ctx, postgresDSN); err != nil {
		t.Fatalf("failed to install pgwf schema: %v", err)
	}

	baseURL, strata := startStrata(t)
	defer strata.Shutdown()
	waitForStrataReady(t, baseURL)

	tenantID := "test-tenant"
	logCapture := newCaptureHandler()
	logger := slog.New(logCapture)

	engine1, err := swf.NewEngineBuilder().
		WithPostgresDSN(postgresDSN).
		WithStrata(baseURL).
		WithStrataAPIKey(strata.APIKey).
		WithLogger(logger).
		PlusWorkers(pipeJob{}, addOneTask{}).
		Build(impl.Builder)
	if err != nil {
		t.Fatalf("failed to build engine1: %v", err)
	}

	engine2, err := swf.NewEngineBuilder().
		WithPostgresDSN(postgresDSN).
		WithStrata(baseURL).
		WithStrataAPIKey(strata.APIKey).
		WithLogger(logger).
		PlusWorkers(pipeJob{}, doubleTask{}).
		Build(impl.Builder)
	if err != nil {
		t.Fatalf("failed to build engine2: %v", err)
	}

	go engine1.Run(ctx)
	go engine2.Run(ctx)
	go userInputWatcher(ctx, t, engine1)

	initial := swf.NewTaskDataOrPanic(map[string]interface{}{"n": 1})
	jobKey, err := engine1.StartJob(ctx, swf.StartJob{
		TenantId: tenantID,
		JobType:  pipeJobName,
		Data:     initial,
	})
	if err != nil {
		t.Fatalf("failed to start job: %v", err)
	}

	strataClient, err := strataclient.New(strataclient.Config{BaseURL: baseURL, APIKey: strata.APIKey})
	if err != nil {
		t.Fatalf("failed to create strata client: %v", err)
	}
	key := story.Key{AnthologyID: jobKey.TenantId, StoryID: jobKey.JobId}

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

	if errs := logCapture.Errors(); errs > 0 {
		t.Fatalf("saw %d error log(s) during run", errs)
	}
}

// --- Workers used in the integration scenario ---

const pipeJobName = "pipe"

type pipeJob struct{}

func (pipeJob) Name() string { return pipeJobName }

func (pipeJob) Run(ctx swf.JobContext, data swf.JobData) (swf.JobData, error) {
	current := taskNumber(data)
	payload := swf.NewTaskDataOrPanic(map[string]interface{}{"n": current})
	if ctx.Logger() != nil {
		ctx.Logger().Info("starting job", "current", current)
	}

	steps := []string{addOneTaskName, doubleTaskName, userInputTaskName, addOneTaskName, doubleTaskName}
	var err error
	var out = payload
	for _, step := range steps {
		out, err = ctx.DoTask(swf.RunPolicy{}, step, out)
		if err != nil {
			return nil, err
		}
		if ctx.Logger() != nil {
			ctx.Logger().Info("completed task", "task", step, "value", taskNumber(out))
		}
	}
	return out, nil
}

const addOneTaskName = "t1"

type addOneTask struct{}

func (addOneTask) Name() string { return addOneTaskName }

func (addOneTask) Run(ctx swf.TaskContext, input swf.TaskData) (swf.TaskData, error) {
	n := taskNumber(input)
	if ctx.Logger != nil {
		ctx.Logger.Info("t1 add", "input", n)
	}
	return swf.NewTaskDataOrPanic(map[string]interface{}{"n": n + 1}), nil
}

const doubleTaskName = "t2"

type doubleTask struct{}

func (doubleTask) Name() string { return doubleTaskName }

func (doubleTask) Run(ctx swf.TaskContext, input swf.TaskData) (swf.TaskData, error) {
	n := taskNumber(input)
	if ctx.Logger != nil {
		ctx.Logger.Info("t2 double", "input", n)
	}
	return swf.NewTaskDataOrPanic(map[string]interface{}{"n": n * 2}), nil
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
			output := swf.NewTaskDataOrPanic(map[string]interface{}{"n": n + 3})
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
	var payload map[string]int
	if err := json.Unmarshal(data, &payload); err != nil {
		return 0
	}
	return payload["n"]
}

type captureHandler struct {
	mu      sync.Mutex
	records []slog.Record
}

func newCaptureHandler() *captureHandler {
	return &captureHandler{records: make([]slog.Record, 0)}
}

func (h *captureHandler) Enabled(context.Context, slog.Level) bool {
	return true
}

func (h *captureHandler) Handle(ctx context.Context, r slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.records = append(h.records, r.Clone())
	return nil
}

func (h *captureHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return h
}

func (h *captureHandler) WithGroup(name string) slog.Handler {
	return h
}

func (h *captureHandler) Errors() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	count := 0
	for _, r := range h.records {
		if r.Level >= slog.LevelError {
			count++
		}
	}
	return count
}
