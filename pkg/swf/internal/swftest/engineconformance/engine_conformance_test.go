package engineconformance_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/colony-2/swf-go/pkg/swf"
	swftest "github.com/colony-2/swf-go/pkg/swf/internal/swftest"
)

func TestRestartJobWithoutExtraOutputAcrossBuiltInRuntimes(t *testing.T) {
	ws := swftest.MustWorkSet(t,
		swftest.SequenceJob{Steps: []string{swftest.AddOneTaskName, swftest.DoubleTaskName}},
		swftest.AddOneTask{},
		swftest.DoubleTask{},
	)

	for _, harness := range swftest.BuiltInRuntimeHarnesses() {
		harness := harness
		t.Run(harness.Name, func(t *testing.T) {
			built := harness.New(t, ws)
			defer built.Shutdown(t)

			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()

			origKey, err := built.Engine.StartJob(ctx, swf.StartJob{
				TenantId: "tenant-restart-" + harness.Name,
				JobType:  swftest.SequenceJobName,
				Data:     swftest.NumberTaskData(1),
			})
			if err != nil {
				t.Fatalf("start job: %v", err)
			}
			swftest.WaitForEngineStatus(t, ctx, built.Engine, origKey, swf.JobStatusCompleted)

			restartKey, err := built.Engine.RestartJob(ctx, swf.RestartJob{
				PriorJobKey:    origKey,
				LastStepToKeep: 0,
			})
			if err != nil {
				t.Fatalf("restart job: %v", err)
			}
			swftest.WaitForEngineStatus(t, ctx, built.Engine, restartKey, swf.JobStatusCompleted)

			result, err := built.Engine.GetJobResult(ctx, restartKey)
			if err != nil {
				t.Fatalf("get restart result: %v", err)
			}
			if got := swftest.MustDecodeNumberTaskData(t, result); got != 4 {
				t.Fatalf("unexpected restart result: got %d want 4", got)
			}
		})
	}
}

func TestGetJobRunCompletedAcrossBuiltInRuntimes(t *testing.T) {
	ws := swftest.MustWorkSet(t,
		swftest.SequenceJob{Steps: []string{swftest.AddOneTaskName, swftest.DoubleTaskName}},
		swftest.AddOneTask{},
		swftest.DoubleTask{},
	)

	for _, harness := range swftest.BuiltInRuntimeHarnesses() {
		harness := harness
		t.Run(harness.Name, func(t *testing.T) {
			built := harness.New(t, ws)
			defer built.Shutdown(t)

			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()

			jobKey, err := built.Engine.StartJob(ctx, swf.StartJob{
				TenantId: "tenant-run-complete-" + harness.Name,
				JobType:  swftest.SequenceJobName,
				Data:     swftest.NumberTaskData(1),
			})
			if err != nil {
				t.Fatalf("start job: %v", err)
			}
			swftest.WaitForEngineStatus(t, ctx, built.Engine, jobKey, swf.JobStatusCompleted)

			resp, err := built.Engine.GetJobRun(ctx, swf.GetJobRunRequest{
				JobKey:               jobKey,
				IncludeInputs:        true,
				IncludeOutputs:       true,
				IncludeAttemptInputs: true,
			})
			if err != nil {
				t.Fatalf("get job run: %v", err)
			}
			if resp.Job.Status != swf.JobStatusCompleted {
				t.Fatalf("expected completed status, got %s", resp.Job.Status)
			}
			if resp.Start.Input == nil {
				t.Fatal("expected start input")
			}
			if got := swftest.MustDecodeNumberTaskIO(t, resp.Start.Input); got != 1 {
				t.Fatalf("unexpected start input: %d", got)
			}
			if len(resp.Attempts) != 1 {
				t.Fatalf("expected 1 job attempt, got %d", len(resp.Attempts))
			}
			if len(resp.Attempts[0].Tasks) != 2 {
				t.Fatalf("expected 2 task runs, got %d", len(resp.Attempts[0].Tasks))
			}
			if resp.Attempts[0].Tasks[0].TaskType != swftest.AddOneTaskName || resp.Attempts[0].Tasks[1].TaskType != swftest.DoubleTaskName {
				t.Fatalf("unexpected task types: %s, %s", resp.Attempts[0].Tasks[0].TaskType, resp.Attempts[0].Tasks[1].TaskType)
			}
			if got := swftest.MustDecodeNumberTaskIO(t, resp.Attempts[0].Tasks[0].Attempts[0].Output); got != 2 {
				t.Fatalf("unexpected add output: %d", got)
			}
			if got := swftest.MustDecodeNumberTaskIO(t, resp.Attempts[0].Tasks[1].Attempts[0].Output); got != 4 {
				t.Fatalf("unexpected double output: %d", got)
			}
			if resp.Attempts[0].Output == nil {
				t.Fatal("expected job output")
			}
			if got := swftest.MustDecodeNumberTaskIO(t, resp.Attempts[0].Output); got != 4 {
				t.Fatalf("unexpected job output: %d", got)
			}

			output, err := resp.GetOutput(built.Engine, jobKey.TenantId)
			if err != nil {
				t.Fatalf("GetOutput failed: %v", err)
			}
			if got := swftest.MustDecodeNumberTaskData(t, output); got != 4 {
				t.Fatalf("unexpected GetOutput result: %d", got)
			}
		})
	}
}

func TestGetJobRunLazilyLoadsOutputAcrossBuiltInRuntimes(t *testing.T) {
	ws := swftest.MustWorkSet(t,
		swftest.SequenceJob{Steps: []string{swftest.AddOneTaskName, swftest.DoubleTaskName}},
		swftest.AddOneTask{},
		swftest.DoubleTask{},
	)

	for _, harness := range swftest.BuiltInRuntimeHarnesses() {
		harness := harness
		t.Run(harness.Name, func(t *testing.T) {
			built := harness.New(t, ws)
			defer built.Shutdown(t)

			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()

			jobKey, err := built.Engine.StartJob(ctx, swf.StartJob{
				TenantId: "tenant-run-output-" + harness.Name,
				JobType:  swftest.SequenceJobName,
				Data:     swftest.NumberTaskData(1),
			})
			if err != nil {
				t.Fatalf("start job: %v", err)
			}
			swftest.WaitForEngineStatus(t, ctx, built.Engine, jobKey, swf.JobStatusCompleted)

			resp, err := built.Engine.GetJobRun(ctx, swf.GetJobRunRequest{
				JobKey:         jobKey,
				IncludeOutputs: false,
			})
			if err != nil {
				t.Fatalf("get job run: %v", err)
			}

			output, err := resp.GetOutput(built.Engine, jobKey.TenantId)
			if err != nil {
				t.Fatalf("GetOutput failed: %v", err)
			}
			if got := swftest.MustDecodeNumberTaskData(t, output); got != 4 {
				t.Fatalf("unexpected GetOutput result: %d", got)
			}
		})
	}
}

func TestGetJobRunGetOutputFailedAcrossBuiltInRuntimes(t *testing.T) {
	ws := swftest.MustWorkSet(t, swftest.FailingJob{})

	for _, harness := range swftest.BuiltInRuntimeHarnesses() {
		harness := harness
		t.Run(harness.Name, func(t *testing.T) {
			built := harness.New(t, ws)
			defer built.Shutdown(t)

			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()

			jobKey, err := built.Engine.StartJob(ctx, swf.StartJob{
				TenantId: "tenant-run-failed-" + harness.Name,
				JobType:  swftest.FailingJobName,
				Data:     swftest.NumberTaskData(1),
			})
			if err != nil {
				t.Fatalf("start job: %v", err)
			}
			swftest.WaitForEngineStatus(t, ctx, built.Engine, jobKey, swf.JobStatusCompleted)

			resp, err := built.Engine.GetJobRun(ctx, swf.GetJobRunRequest{JobKey: jobKey})
			if err != nil {
				t.Fatalf("get job run: %v", err)
			}
			if _, err := resp.GetOutput(built.Engine, jobKey.TenantId); !errors.Is(err, swf.ErrJobFailed) {
				t.Fatalf("expected ErrJobFailed, got %v", err)
			} else if !strings.Contains(err.Error(), "intentional failure") {
				t.Fatalf("expected failure message, got %v", err)
			}
		})
	}
}

func TestGetJobRunGetOutputCancelledAcrossBuiltInRuntimes(t *testing.T) {
	ws := swftest.MustWorkSet(t, swftest.SequenceJob{Steps: []string{swftest.MissingTaskName}})

	for _, harness := range swftest.BuiltInRuntimeHarnesses() {
		harness := harness
		t.Run(harness.Name, func(t *testing.T) {
			built := harness.New(t, ws)
			defer built.Shutdown(t)

			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()

			jobKey := swf.JobKey{
				TenantId: "tenant-run-cancelled-" + harness.Name,
				JobId:    "cancelled-job",
			}
			done := swftest.MustStartJobAsync(t, built.Engine, swf.StartJob{
				TenantId: jobKey.TenantId,
				JobType:  swftest.SequenceJobName,
				JobID:    jobKey.JobId,
				Data:     swftest.NumberTaskData(1),
			})

			_ = swftest.WaitForTaskHandle(t, ctx, built.Engine, swftest.SequenceJobName, swftest.MissingTaskName, []string{jobKey.TenantId})

			if err := built.Engine.CancelJob(ctx, swf.CancelJob{JobKey: jobKey}); err != nil {
				t.Fatalf("cancel job: %v", err)
			}
			if err := <-done; err != nil {
				t.Fatalf("async start failed: %v", err)
			}
			swftest.WaitForEngineStatus(t, ctx, built.Engine, jobKey, swf.JobStatusCancelled)

			resp, err := built.Engine.GetJobRun(ctx, swf.GetJobRunRequest{JobKey: jobKey})
			if err != nil {
				t.Fatalf("get job run: %v", err)
			}
			if _, err := resp.GetOutput(built.Engine, jobKey.TenantId); !errors.Is(err, swf.ErrJobCancelled) {
				t.Fatalf("expected ErrJobCancelled, got %v", err)
			}
		})
	}
}

func TestGetJobRunPendingRuntimeAcrossBuiltInRuntimes(t *testing.T) {
	ws := swftest.MustWorkSet(t, swftest.SequenceJob{Steps: []string{swftest.MissingTaskName}})

	for _, harness := range swftest.BuiltInRuntimeHarnesses() {
		harness := harness
		t.Run(harness.Name, func(t *testing.T) {
			built := harness.New(t, ws)
			defer built.Shutdown(t)

			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()

			jobKey := swf.JobKey{
				TenantId: "tenant-run-pending-" + harness.Name,
				JobId:    "pending-runtime",
			}
			done := swftest.MustStartJobAsync(t, built.Engine, swf.StartJob{
				TenantId: jobKey.TenantId,
				JobType:  swftest.SequenceJobName,
				JobID:    jobKey.JobId,
				Data:     swftest.NumberTaskData(1),
			})

			handle := swftest.WaitForTaskHandle(t, ctx, built.Engine, swftest.SequenceJobName, swftest.MissingTaskName, []string{jobKey.TenantId})

			resp, err := built.Engine.GetJobRun(ctx, swf.GetJobRunRequest{
				JobKey:               jobKey,
				IncludeInputs:        true,
				IncludeAttemptInputs: true,
			})
			if err != nil {
				t.Fatalf("get job run: %v", err)
			}
			if len(resp.Attempts) != 1 {
				t.Fatalf("expected 1 job attempt, got %d", len(resp.Attempts))
			}
			if len(resp.Attempts[0].Tasks) != 1 {
				t.Fatalf("expected 1 task run, got %d", len(resp.Attempts[0].Tasks))
			}
			task := resp.Attempts[0].Tasks[0]
			if len(task.Attempts) != 1 {
				t.Fatalf("expected 1 task attempt, got %d", len(task.Attempts))
			}
			attempt := task.Attempts[0]
			if attempt.State == "" {
				t.Fatal("expected runtime state")
			}
			swftest.ExpectJobTypeFromNextNeed(t, attempt.Runtime.NextNeed, swftest.SequenceJobName)
			swftest.ExpectTaskSuffix(t, *attempt.Runtime.NextNeed, ":"+swftest.MissingTaskName)
			if attempt.Input == nil {
				t.Fatal("expected runtime input")
			}
			if got := swftest.MustDecodeNumberTaskIO(t, attempt.Input); got != 1 {
				t.Fatalf("unexpected runtime input: %d", got)
			}
			if _, err := resp.GetOutput(built.Engine, jobKey.TenantId); !errors.Is(err, swf.ErrJobNotComplete) {
				t.Fatalf("expected ErrJobNotComplete, got %v", err)
			}

			if err := handle.Finish(ctx, swftest.NumberTaskData(2)); err != nil {
				t.Fatalf("finish task: %v", err)
			}
			if err := <-done; err != nil {
				t.Fatalf("async start failed: %v", err)
			}
			swftest.WaitForEngineStatus(t, ctx, built.Engine, jobKey, swf.JobStatusCompleted)
		})
	}
}
