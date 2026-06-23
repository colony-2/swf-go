package workflow_test

import (
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/workflow"
	_ "github.com/lib/pq"
)

// TestChapterConstraintsAcrossEngines validates that both the toy and direct
// Strata-backed engine enforce the same chapter constraints:
// - Chapters must be written once (no duplicates)
// - Chapters must be written in monotonic order starting at 0
func TestChapterConstraintsAcrossEngines(t *testing.T) {
	tests := []struct {
		name        string
		setupEngine func(t *testing.T) (workflow.Engine, func())
		skipReason  string
	}{
		{
			name: "toy",
			setupEngine: func(t *testing.T) (workflow.Engine, func()) {
				engine, cancel := buildToyEngine(t, func(b *workflow.EngineBuilder) {
					b.WithWorkerTenantId("test-tenant").PlusWorkers(&deterministicJob{}, &incrementTask{})
				})
				return engine, func() { cancel() }
			},
		},
		{
			name:       "direct",
			skipReason: "", // Set in individual subtests where needed
			setupEngine: func(t *testing.T) (workflow.Engine, func()) {
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
				engine := buildDirectEngine(t, postgresDSN, baseURL, strata.APIKey, func(b *workflow.EngineBuilder) {
					b.WithLogger(logger).WithWorkerTenantId("test-tenant").PlusWorkers(&deterministicJob{}, &incrementTask{})
				})

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
				input := jobdb.NewTaskDataOrPanic(map[string]int{"n": 1})

				jobKey, err := engine.SubmitJob(ctx, jobdb.SubmitJob{
					TenantId: "test-tenant",
					JobType:  "deterministic-job",
					Data:     input,
				})
				if err != nil {
					t.Fatalf("SubmitJob failed: %v", err)
				}

				// Wait for job to complete (with timeout for async engines)
				waitForJobToComplete(t, engine, jobKey)

				// Should have a successful result
				result, err := jobResultForTest(engine, ctx, jobKey)
				if err != nil {
					t.Fatalf("GetJobResult failed: %v", err)
				}
				if result == nil {
					t.Fatal("expected non-nil result")
				}
			})

			t.Run("non-deterministic job fails with determinism error", func(t *testing.T) {
				// Register a non-deterministic job worker
				ws2, err := workflow.AsWorkSet(&nonDeterministicJob{}, &incrementTask{})
				if err != nil {
					t.Fatalf("failed to create workset: %v", err)
				}
				if err := engine.RegisterWorkers(ws2); err != nil {
					t.Fatalf("failed to register workers: %v", err)
				}

				ctx := context.Background()
				input := jobdb.NewTaskDataOrPanic(map[string]int{"n": 1})

				jobKey, err := engine.SubmitJob(ctx, jobdb.SubmitJob{
					TenantId: "test-tenant",
					JobType:  "non-deterministic-job",
					Data:     input,
				})
				if err != nil {
					t.Fatalf("SubmitJob failed: %v", err)
				}

				// Wait for job to complete (with timeout for async engines)
				waitForJobToComplete(t, engine, jobKey)

				// Should have an error result indicating the replay diverged.
				_, err = jobResultForTest(engine, ctx, jobKey)
				if err == nil {
					t.Fatal("expected error from GetJobResult, got nil")
				}

				errMsg := err.Error()
				if !strings.Contains(errMsg, "not deterministic") &&
					!strings.Contains(errMsg, "input hash mismatch") &&
					!strings.Contains(errMsg, "workflow was not deterministic") {
					t.Fatalf("expected determinism error, got: %v", err)
				}
			})
		})
	}
}

// deterministicJob is a simple job that executes tasks in a deterministic order
type deterministicJob struct{}

func (j *deterministicJob) Name() string { return "deterministic-job" }

func (j *deterministicJob) Run(ctx workflow.JobContext, data jobdb.JobData) (jobdb.JobData, error) {
	// Execute three tasks in order
	result := data
	for i := 0; i < 3; i++ {
		out, err := ctx.DoTask(jobdb.RunPolicy{}, "increment", result)
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

func (j *nonDeterministicJob) Run(ctx workflow.JobContext, data jobdb.JobData) (jobdb.JobData, error) {
	// Execute first task
	result, err := ctx.DoTask(jobdb.RunPolicy{}, "increment", data)
	if err != nil {
		return nil, err
	}

	// Now try to manipulate internal state to execute the same ordinal again
	// This simulates what happens when a workflow is non-deterministic
	// The shared worker runner exposes a small test hook that allows this test
	// to force a duplicate ordinal through the actual runtime-backed engine.
	type stepManipulator interface {
		ManipulateStepForTest(newStep int64)
	}

	if manipulator, ok := ctx.(stepManipulator); ok {
		// Reset step to 1 to try to write chapter 1 again
		manipulator.ManipulateStepForTest(1)
		// Re-run the same ordinal with different input so the shared runner
		// reports a determinism mismatch instead of replaying the cached step.
		conflictingInput := jobdb.NewTaskDataOrPanic(map[string]int{"n": 99})
		_, err := ctx.DoTask(jobdb.RunPolicy{}, "increment", conflictingInput)
		if err != nil {
			return nil, err // Expected error
		}
		return nil, jobdb.AppError{Payload: jobdb.AppErrorPayload{Message: "expected determinism error but got none"}}
	}
	return result, nil
}

// incrementTask increments the "n" field in the input data
type incrementTask struct{}

func (t *incrementTask) Name() string { return "increment" }

func (t *incrementTask) Run(ctx workflow.TaskContext, input jobdb.TaskData) (jobdb.TaskData, error) {
	inputData, err := input.GetData()
	if err != nil {
		return nil, err
	}

	var data map[string]int
	if err := json.Unmarshal(inputData, &data); err != nil {
		return nil, err
	}

	data["n"] = data["n"] + 1
	return jobdb.NewTaskDataOrPanic(data), nil
}

// waitForJobToComplete polls the engine until the job reaches a terminal status
func waitForJobToComplete(t *testing.T, engine workflow.Engine, jobKey jobdb.JobKey) {
	t.Helper()
	ctx := context.Background()
	deadline := time.Now().Add(30 * time.Second)
	for time.Now().Before(deadline) {
		status, err := jobStatusForTest(engine, ctx, jobKey)
		if err != nil {
			t.Fatalf("CheckJobStatus failed: %v", err)
		}
		if status == jobdb.JobStatusCompleted || status == jobdb.JobStatusCancelled {
			return
		}
		time.Sleep(100 * time.Millisecond)
	}
	t.Fatalf("timeout waiting for job %v to complete", jobKey)
}
