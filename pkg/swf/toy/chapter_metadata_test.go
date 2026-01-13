package toy

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/colony-2/swf-go/pkg/swf"
)

// TestChapterMetadataPreservationOnExternalTaskCompletion tests that when external
// task workers complete tasks via TaskHandle.Finish(), the chapter metadata is properly
// preserved. This is critical for job determinism when jobs use cached chapters.
//
// Bug scenario without proper metadata preservation:
// 1. Job runs with external task completion (TaskHandle.Finish)
// 2. Task chapter is saved without proper metadata (no RunPolicy, no InputRef, no Attempt)
// 3. Same job re-executes (e.g., after crash/restart or replay)
// 4. Job reads cached task chapter
// 5. Job result is saved with metadata
// 6. Later, if job is replayed again, cached job result doesn't have matching metadata
// 7. Determinism check fails: "workflow was not deterministic: input hash mismatch" or metadata mismatch
func TestChapterMetadataPreservationOnExternalTaskCompletion(t *testing.T) {
	ctx := context.Background()
	tenantId := "test-tenant"

	// Create job that does: external task -> returns result
	// This simulates a workflow that needs external approval/completion
	// NOTE: We intentionally don't include the "approval" task worker,
	// so the task will wait for external completion via TaskHandle.Finish
	jobWorker := &externalTaskJob{taskName: "approval"}

	ws := mustWorkSet(jobWorker) // NO task worker for "approval" - it's external!
	engine := NewToyEngine([]swf.WorkSet{ws})

	retryPolicy := swf.RunPolicy{
		Retry: swf.RetryPolicy{
			MaximumAttempts:    3,
			BackoffCoefficient: 2.0,
			InitialInterval:    swf.Duration(time.Second),
		},
	}

	input := swf.NewTaskDataOrPanic(map[string]int{"n": 1})

	// Start the job - it will block waiting for external task completion
	var wg sync.WaitGroup
	wg.Add(1)
	var jobKey swf.JobKey
	var startErr error
	go func() {
		defer wg.Done()
		jobKey, startErr = engine.StartJob(ctx, swf.StartJob{
			TenantId:  tenantId,
			JobType:   ws.JobWorker.Name(),
			Data:      input,
			RunPolicy: retryPolicy,
		})
	}()

	// Wait for external task to appear
	var handles []swf.TaskHandle
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		var err error
		handles, err = engine.FindTasksWaitingForCapability(ctx, ws.JobWorker.Name(), "approval", nil)
		if err != nil {
			t.Fatalf("FindTasksWaitingForCapability: %v", err)
		}
		if len(handles) > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if len(handles) != 1 {
		t.Fatalf("Expected 1 external task handle, got %d", len(handles))
	}

	// Complete the external task via TaskHandle.Finish()
	// BUG: Without the fix, this doesn't preserve metadata
	approvalOutput := swf.NewTaskDataOrPanic(map[string]int{"n": 42})
	err := handles[0].Finish(ctx, approvalOutput)
	if err != nil {
		t.Fatalf("Failed to finish external task: %v", err)
	}

	// Wait for job to complete
	wg.Wait()
	if startErr != nil {
		t.Fatalf("StartJob failed: %v", startErr)
	}

	status, err := engine.CheckJobStatus(ctx, jobKey)
	if err != nil {
		t.Fatalf("CheckJobStatus failed: %v", err)
	}
	if status != swf.JobStatusCompleted {
		t.Fatalf("Expected job to complete, got status %s", status)
	}

	result, err := engine.GetJobResult(ctx, jobKey)
	if err != nil {
		t.Fatalf("GetJobResult failed: %v", err)
	}
	if extractNumber(result) != 42 {
		t.Fatalf("Expected result 42, got %d", extractNumber(result))
	}

	t.Logf("First execution completed successfully")

	// Now simulate a replay/restart scenario
	// The toy engine already has cached chapters from the first run
	// Re-execute the SAME job (same input, same job type) to use cached results
	// This tests determinism: cached chapters should have proper metadata

	t.Logf("Re-executing job to test cached chapter determinism...")

	wg.Add(1)
	var jobKey2 swf.JobKey
	var startErr2 error
	go func() {
		defer wg.Done()
		jobKey2, startErr2 = engine.StartJob(ctx, swf.StartJob{
			TenantId:  tenantId,
			JobType:   ws.JobWorker.Name(),
			Data:      input, // Same input as before
			RunPolicy: retryPolicy,
		})
	}()

	// The second execution should use cached task result
	// But it may still pause if the cache lookup fails or metadata is wrong
	// Wait a bit to see if it needs external completion again
	time.Sleep(100 * time.Millisecond)

	handles2, err := engine.FindTasksWaitingForCapability(ctx, ws.JobWorker.Name(), "approval", nil)
	if err != nil {
		t.Fatalf("FindTasksWaitingForCapability on replay: %v", err)
	}

	if len(handles2) > 0 {
		// Task is waiting again - complete it
		t.Logf("Task waiting on replay, completing again")
		err = handles2[0].Finish(ctx, approvalOutput)
		if err != nil {
			t.Fatalf("Failed to finish external task on replay: %v", err)
		}
	}

	// Wait for second execution to complete
	wg.Wait()
	if startErr2 != nil {
		// Check if it's a determinism error
		if errors.Is(startErr2, swf.ErrWorkflowNotDeterministic) {
			t.Fatalf("BUG DETECTED: Determinism error on replay: %v\nThis indicates TaskHandle.Finish() didn't preserve chapter metadata properly", startErr2)
		}
		t.Fatalf("Second StartJob failed: %v", startErr2)
	}

	status2, err := engine.CheckJobStatus(ctx, jobKey2)
	if err != nil {
		t.Fatalf("CheckJobStatus on replay failed: %v", err)
	}
	if status2 != swf.JobStatusCompleted {
		t.Fatalf("Expected replayed job to complete, got status %s", status2)
	}

	result2, err := engine.GetJobResult(ctx, jobKey2)
	if err != nil {
		// Check if it's a determinism error in result retrieval
		if errors.Is(err, swf.ErrWorkflowNotDeterministic) {
			t.Fatalf("BUG DETECTED: Determinism error getting result: %v\nThis indicates TaskHandle.Finish() didn't preserve chapter metadata properly", err)
		}
		t.Fatalf("GetJobResult on replay failed: %v", err)
	}
	if extractNumber(result2) != 42 {
		t.Fatalf("Expected replayed result 42, got %d", extractNumber(result2))
	}

	t.Logf("SUCCESS: Job replay completed with proper determinism - metadata was preserved correctly")
}

// externalTaskJob is a simple job that does one external task
type externalTaskJob struct {
	taskName string
}

func (j *externalTaskJob) Name() string { return "external-job" }

func (j *externalTaskJob) Run(ctx swf.JobContext, data swf.JobData) (swf.JobData, error) {
	// Do the external task
	result, err := ctx.DoTask(swf.RunPolicy{}, j.taskName, data)
	if err != nil {
		return nil, err
	}
	return result, nil
}
