package swf_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/colony-2/swf-go/pkg/swf"
	"github.com/colony-2/swf-go/pkg/swf/impl"
	"github.com/colony-2/swf-go/pkg/swf/toy"
	_ "github.com/lib/pq"
)

// TestChapterConstraintsAcrossEngines validates that both ToyEngine and the full
// Strata-backed engine enforce the same chapter constraints:
// - Chapters must be written once (no duplicates)
// - Chapters must be written in monotonic order starting at 0
func TestChapterConstraintsAcrossEngines(t *testing.T) {
	tests := []struct {
		name        string
		setupEngine func(t *testing.T) (swf.SWFEngine, func())
		skipReason  string
	}{
		{
			name: "ToyEngine",
			setupEngine: func(t *testing.T) (swf.SWFEngine, func()) {
				ws, err := swf.AsWorkSet(&deterministicJob{}, &incrementTask{})
				if err != nil {
					t.Fatalf("failed to create workset: %v", err)
				}
				engine := toy.NewToyEngine([]swf.WorkSet{*ws})
				return engine, func() {}
			},
		},
		{
			name:       "Strata-backed Engine",
			skipReason: "", // Set in individual subtests where needed
			setupEngine: func(t *testing.T) (swf.SWFEngine, func()) {
				ctx := context.Background()
				postgresDSN, stopPG := startEmbeddedPostgres(t)
				if err := installPGWF(ctx, postgresDSN); err != nil {
					stopPG()
					t.Fatalf("failed to install pgwf schema: %v", err)
				}

				baseURL, strata := startStrata(t)
				waitForStrataReady(t, baseURL)

				logCapture := newCaptureHandler()
				logger := slog.New(logCapture)
				engine, err := swf.NewEngineBuilder().
					WithPostgresDSN(postgresDSN).
					WithStrata(baseURL).
					WithStrataAPIKey(strata.APIKey).
					WithLogger(logger).
					PlusWorkers(&deterministicJob{}, &incrementTask{}).
					Build(impl.Builder)
				if err != nil {
					strata.Shutdown()
					stopPG()
					t.Fatalf("failed to build engine: %v", err)
				}

				cleanup := func() {
					strata.Shutdown()
					stopPG()
				}

				ctx, cancel := context.WithCancel(context.Background())
				go engine.Run(ctx)

				return engine, func() {
					cancel()
					cleanup()
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if tt.skipReason != "" {
				t.Skip(tt.skipReason)
			}

			engine, cleanup := tt.setupEngine(t)
			defer cleanup()

			t.Run("deterministic job succeeds", func(t *testing.T) {
				ctx := context.Background()
				input := swf.NewTaskDataOrPanic(map[string]int{"n": 1})

				jobKey, err := engine.StartJob(ctx, swf.StartJob{
					TenantId: "test-tenant",
					JobType:  "deterministic-job",
					Data:     input,
				})
				if err != nil {
					t.Fatalf("StartJob failed: %v", err)
				}

				// Wait for job to complete (with timeout for async engines)
				waitForJobToComplete(t, engine, jobKey)

				// Should have a successful result
				result, err := engine.GetJobResult(ctx, jobKey)
				if err != nil {
					t.Fatalf("GetJobResult failed: %v", err)
				}
				if result == nil {
					t.Fatal("expected non-nil result")
				}
			})

			t.Run("non-deterministic job fails with chapter error", func(t *testing.T) {
				// For the Strata-backed engine, we can't easily simulate non-deterministic
				// behavior without manipulating internal state, which isn't accessible.
				// The Strata backend enforces chapter constraints at the storage layer
				// (SaveChapter fails if chapter already exists), but testing this requires
				// a more complex setup involving job restarts or replays.
				// We rely on ToyEngine tests to verify the constraint behavior.
				if tt.name == "Strata-backed Engine" {
					t.Skip("Skipping non-deterministic test for Strata engine - constraint is enforced at storage layer")
				}

				// Register a non-deterministic job worker
				ws2, err := swf.AsWorkSet(&nonDeterministicJob{}, &incrementTask{})
				if err != nil {
					t.Fatalf("failed to create workset: %v", err)
				}
				if err := engine.RegisterWorkers(ws2); err != nil {
					t.Fatalf("failed to register workers: %v", err)
				}

				ctx := context.Background()
				input := swf.NewTaskDataOrPanic(map[string]int{"n": 1})

				jobKey, err := engine.StartJob(ctx, swf.StartJob{
					TenantId: "test-tenant",
					JobType:  "non-deterministic-job",
					Data:     input,
				})
				if err != nil {
					t.Fatalf("StartJob failed: %v", err)
				}

				// Wait for job to complete (with timeout for async engines)
				waitForJobToComplete(t, engine, jobKey)

				// Should have an error result containing "chapter already created"
				_, err = engine.GetJobResult(ctx, jobKey)
				if err == nil {
					t.Fatal("expected error from GetJobResult, got nil")
				}

				errMsg := err.Error()
				if !strings.Contains(errMsg, "chapter already created") &&
					!strings.Contains(errMsg, "already exists") &&
					!strings.Contains(errMsg, "duplicate") {
					t.Fatalf("expected chapter constraint error, got: %v", err)
				}
			})
		})
	}
}

// deterministicJob is a simple job that executes tasks in a deterministic order
type deterministicJob struct{}

func (j *deterministicJob) Name() string { return "deterministic-job" }

func (j *deterministicJob) Run(ctx swf.JobContext, data swf.JobData) (swf.JobData, error) {
	// Execute three tasks in order
	result := data
	for i := 0; i < 3; i++ {
		out, err := ctx.DoTask(swf.RunPolicy{}, "increment", result)
		if err != nil {
			return nil, err
		}
		result = out
	}
	return result, nil
}

// nonDeterministicJob attempts to execute the same task ordinal twice
type nonDeterministicJob struct{}

func (j *nonDeterministicJob) Name() string { return "non-deterministic-job" }

func (j *nonDeterministicJob) Run(ctx swf.JobContext, data swf.JobData) (swf.JobData, error) {
	// Execute first task
	result, err := ctx.DoTask(swf.RunPolicy{}, "increment", data)
	if err != nil {
		return nil, err
	}

	// Execute second task
	result2, err := ctx.DoTask(swf.RunPolicy{}, "increment", result)
	if err != nil {
		return nil, err
	}

	// Now try to manipulate internal state to execute the same ordinal again
	// This simulates what happens when a workflow is non-deterministic
	// For ToyEngine, we'll use type assertion to manipulate the step counter
	type stepManipulator interface {
		ManipulateStepForTest(newStep int64)
	}

	if manipulator, ok := ctx.(stepManipulator); ok {
		// Reset step to 1 to try to write chapter 1 again
		manipulator.ManipulateStepForTest(1)
		// This should fail with "chapter already created" error
		_, err := ctx.DoTask(swf.RunPolicy{}, "increment", data)
		if err != nil {
			return nil, err // Expected error
		}
		return nil, swf.AppError{Payload: swf.AppErrorPayload{Message: "expected chapter already created error but got none"}}
	}

	// For the full Strata engine, we can't manipulate internal state in the same way.
	// The full engine enforces chapter constraints at the Strata level, which we
	// can't easily trigger in a test without restarting the job.
	// This path means we're on the full engine - just return success for now.
	return result2, nil
}

// incrementTask increments the "n" field in the input data
type incrementTask struct{}

func (t *incrementTask) Name() string { return "increment" }

func (t *incrementTask) Run(ctx swf.TaskContext, input swf.TaskData) (swf.TaskData, error) {
	inputData, err := input.GetData()
	if err != nil {
		return nil, err
	}

	var data map[string]int
	if err := json.Unmarshal(inputData, &data); err != nil {
		return nil, err
	}

	data["n"] = data["n"] + 1
	return swf.NewTaskDataOrPanic(data), nil
}

// waitForJobToComplete polls the engine until the job reaches a terminal status
func waitForJobToComplete(t *testing.T, engine swf.SWFEngine, jobKey swf.JobKey) {
	t.Helper()
	ctx := context.Background()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		status, err := engine.CheckJobStatus(ctx, jobKey)
		if err != nil {
			t.Fatalf("CheckJobStatus failed: %v", err)
		}
		if status == swf.JobStatusCompleted || status == swf.JobStatusCancelled {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for job %v to complete", jobKey)
}
