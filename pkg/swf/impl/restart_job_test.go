package impl

import (
	"context"
	"encoding/json"
	"sync/atomic"
	"testing"
	"time"

	"github.com/colony-2/swf-go/pkg/swf"
)

func TestRestartJobWithoutInitialChapterProvided(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var runs atomic.Int32
	jobWorker := &countingJobWorker{name: "restart-job-no-input", counter: &runs}

	embedded, err := StartEmbeddedEngine(ctx, jobWorker)
	if err != nil {
		t.Fatalf("start embedded engine: %v", err)
	}
	defer embedded.Shutdown()

	engine := embedded.SWFEngine
	go engine.Run(ctx)

	origInput := swf.NewTaskDataOrPanic(map[string]string{"hello": "world"})
	jobKey, err := engine.StartJob(ctx, swf.StartJob{
		TenantId: "tenant-2",
		JobType:  jobWorker.Name(),
		Data:     origInput,
	})
	if err != nil {
		t.Fatalf("start job: %v", err)
	}

	if err := swf.WaitForJobToComplete(ctx, 10*time.Second, jobKey, engine); err != nil {
		t.Fatalf("wait for job: %v", err)
	}

	if got := runs.Load(); got != 1 {
		t.Fatalf("expected 1 job execution, got %d", got)
	}

	restartKey, err := engine.RestartJob(ctx, swf.RestartJob{
		PriorJobKey:    jobKey,
		LastStepToKeep: 0, // keep the initial chapter and restart from step 1
	})
	if err != nil {
		t.Fatalf("restart job without data: %v", err)
	}

	if err := swf.WaitForJobToComplete(ctx, 10*time.Second, restartKey, engine); err != nil {
		t.Fatalf("wait for restarted job: %v", err)
	}

	if got := runs.Load(); got != 2 {
		t.Fatalf("expected job to re-execute using cloned initial chapter, got %d runs", got)
	}

	result, err := engine.GetJobResult(ctx, restartKey)
	if err != nil {
		t.Fatalf("get restart result: %v", err)
	}
	payload, err := result.GetData()
	if err != nil {
		t.Fatalf("decode restart result: %v", err)
	}

	var decoded map[string]any
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatalf("unmarshal restart result: %v", err)
	}

	if decoded["hello"] != "world" || decoded["executed"] != true {
		t.Fatalf("unexpected restart result payload when reusing initial chapter: %#v", decoded)
	}
}

func TestRestartJobWithNewInitialDataOptional(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var runs atomic.Int32
	jobWorker := &countingJobWorker{name: "restart-job-with-input", counter: &runs}

	embedded, err := StartEmbeddedEngine(ctx, jobWorker)
	if err != nil {
		t.Fatalf("start embedded engine: %v", err)
	}
	defer embedded.Shutdown()

	engine := embedded.SWFEngine
	go engine.Run(ctx)

	origInput := swf.NewTaskDataOrPanic(map[string]string{"hello": "world"})
	jobKey, err := engine.StartJob(ctx, swf.StartJob{
		TenantId: "tenant-1",
		JobType:  jobWorker.Name(),
		Data:     origInput,
	})
	if err != nil {
		t.Fatalf("start job: %v", err)
	}
	if err := swf.WaitForJobToComplete(ctx, 10*time.Second, jobKey, engine); err != nil {
		t.Fatalf("wait for job: %v", err)
	}

	newInput := swf.NewTaskDataOrPanic(map[string]string{"hello": "again"})
	restartKey, err := engine.RestartJob(ctx, swf.RestartJob{
		PriorJobKey:    jobKey,
		LastStepToKeep: 0,
		ExtraTaskInput: newInput,
		ExtraTaskOutput: swf.NewTaskDataOrPanic(map[string]any{
			"hello":    "again",
			"executed": true,
		}),
	})
	if err != nil {
		t.Fatalf("restart job: %v", err)
	}
	if err := swf.WaitForJobToComplete(ctx, 10*time.Second, restartKey, engine); err != nil {
		t.Fatalf("wait for restarted job: %v", err)
	}

	// Job should replay cached steps; worker may re-run but result should reflect provided chapter.
	if got := runs.Load(); got < 1 {
		t.Fatalf("expected at least one job execution; runs=%d", got)
	}

	result, err := engine.GetJobResult(ctx, restartKey)
	if err != nil {
		t.Fatalf("get restart result: %v", err)
	}
	payload, err := result.GetData()
	if err != nil {
		t.Fatalf("decode restart result: %v", err)
	}

	var decoded map[string]any
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatalf("unmarshal restart result: %v", err)
	}
	if decoded["hello"] != "again" {
		t.Fatalf("unexpected restart result payload: %#v", decoded)
	}
}

func TestRestartJobRejectsNegativeLastStepToKeep(t *testing.T) {
	ctx := context.Background()
	engine := &swfEngineImpl{}
	_, err := engine.RestartJob(ctx, swf.RestartJob{
		PriorJobKey:    swf.JobKey{TenantId: "t1", JobId: "job1"},
		LastStepToKeep: -1,
	})
	if err == nil {
		t.Fatalf("expected restart to fail with negative LastStepToKeep")
	}
}

func TestRestartJobRejectsMidJobRetryBoundary(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var runs atomic.Int32
	jobWorker := &failThenSucceedJobWorker{name: "restart-mid-retry", failAttempts: 1, counter: &runs}

	embedded, err := StartEmbeddedEngine(ctx, jobWorker)
	if err != nil {
		t.Fatalf("start embedded engine: %v", err)
	}
	defer embedded.Shutdown()

	engine := embedded.SWFEngine
	go engine.Run(ctx)

	input := swf.NewTaskDataOrPanic(map[string]string{"hello": "world"})
	jobKey, err := engine.StartJob(ctx, swf.StartJob{
		TenantId: "tenant-retry",
		JobType:  jobWorker.Name(),
		Data:     input,
		RunPolicy: swf.RunPolicy{
			Retry: swf.RetryPolicy{MaximumAttempts: 2},
		},
	})
	if err != nil {
		t.Fatalf("start job: %v", err)
	}

	if err := swf.WaitForJobToComplete(ctx, 10*time.Second, jobKey, engine); err != nil {
		t.Fatalf("wait for job: %v", err)
	}
	if got := runs.Load(); got != 2 {
		t.Fatalf("expected 2 job attempts (fail then succeed), got %d", got)
	}

	// LastStepToKeep before attempt 2 (ordinal 2) should be rejected because next is attempt 2.
	if _, err := engine.RestartJob(ctx, swf.RestartJob{
		PriorJobKey:    jobKey,
		LastStepToKeep: 1,
	}); err == nil {
		t.Fatalf("expected restart to fail when LastStepToKeep slices into retry chain")
	}
}

func TestRestartJobRejectsWhenNextChapterMissing(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var runs atomic.Int32
	jobWorker := &countingJobWorker{name: "restart-missing-next", counter: &runs}

	embedded, err := StartEmbeddedEngine(ctx, jobWorker)
	if err != nil {
		t.Fatalf("start embedded engine: %v", err)
	}
	defer embedded.Shutdown()

	engine := embedded.SWFEngine
	go engine.Run(ctx)

	input := swf.NewTaskDataOrPanic(map[string]string{"hello": "world"})
	jobKey, err := engine.StartJob(ctx, swf.StartJob{
		TenantId: "tenant-missing-next",
		JobType:  jobWorker.Name(),
		Data:     input,
	})
	if err != nil {
		t.Fatalf("start job: %v", err)
	}

	if err := swf.WaitForJobToComplete(ctx, 10*time.Second, jobKey, engine); err != nil {
		t.Fatalf("wait for job: %v", err)
	}

	if _, err := engine.RestartJob(ctx, swf.RestartJob{
		PriorJobKey:    jobKey,
		LastStepToKeep: 1, // next ordinal 2 is missing
	}); err == nil {
		t.Fatalf("expected restart to fail when next chapter is missing")
	}
}
