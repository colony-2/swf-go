package engineconformance_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/colony-2/swf-go/pkg/swf"
	swftest "github.com/colony-2/swf-go/pkg/swf/internal/swftest"
)

type artifactPassthroughJob struct{}

func (artifactPassthroughJob) Name() string { return "artifact-passthrough-job" }

func (artifactPassthroughJob) Run(ctx swf.JobContext, input swf.JobData) (swf.JobData, error) {
	return ctx.DoTask(swf.RunPolicy{}, "artifact-passthrough-task", input)
}

type artifactPassthroughTask struct{}

func (artifactPassthroughTask) Name() string { return "artifact-passthrough-task" }

func (artifactPassthroughTask) Run(_ swf.TaskContext, _ swf.TaskData) (swf.TaskData, error) {
	return &swf.SimpleTaskData{
		Data: []byte(`{"ok":true}`),
		Artifacts: []swf.Artifact{
			swf.NewArtifactFromBytes("trace.txt", []byte("artifact-passthrough")),
		},
	}, nil
}

type failedChildJob struct {
	started chan<- struct{}
	release <-chan struct{}
}

func (failedChildJob) Name() string { return "failed-child" }

func (j failedChildJob) Run(_ swf.JobContext, _ swf.JobData) (swf.JobData, error) {
	if j.started != nil {
		close(j.started)
	}
	<-j.release
	return nil, swf.AppError{Payload: swf.AppErrorPayload{Message: "child failed", Level: "error"}}
}

type awaitFailedChildJob struct {
	engine  swf.SWFEngine
	childID string
}

func (awaitFailedChildJob) Name() string { return "await-failed-child" }

func (j *awaitFailedChildJob) Run(ctx swf.JobContext, data swf.JobData) (swf.JobData, error) {
	if err := ctx.AwaitJobs(j.childID); err != nil {
		return nil, err
	}
	result, err := j.engine.GetJobResult(context.Background(), swf.JobKey{
		TenantId: ctx.GetJobKey().TenantId,
		JobId:    j.childID,
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

type awaitFailedChildViaRunOutputJob struct {
	engine swf.SWFEngine
}

func (awaitFailedChildViaRunOutputJob) Name() string { return "await-failed-child-run-output" }

func (j *awaitFailedChildViaRunOutputJob) Run(ctx swf.JobContext, data swf.JobData) (swf.JobData, error) {
	childKey, err := j.engine.StartJob(context.Background(), swf.StartJob{
		TenantId: ctx.GetJobKey().TenantId,
		JobType:  "failed-child",
		JobID:    ctx.GetJobKey().JobId + "-child",
		Data:     data,
	})
	if err != nil {
		return nil, err
	}
	if err := ctx.AwaitJobs(childKey.JobId); err != nil {
		return nil, err
	}
	run, err := j.engine.GetJobRun(context.Background(), swf.GetJobRunRequest{
		JobKey:           childKey,
		IncludeOutputs:   true,
		IncludeArtifacts: true,
	})
	if err != nil {
		return nil, err
	}
	return run.GetOutput(j.engine, childKey.TenantId)
}

type childRunOutputRetryParentJob struct {
	branchRuns *atomic.Int32
}

func (childRunOutputRetryParentJob) Name() string { return "child-run-output-retry-parent" }

func (j childRunOutputRetryParentJob) Run(ctx swf.JobContext, data swf.JobData) (swf.JobData, error) {
	started, err := ctx.DoTask(swf.RunPolicy{}, "child-run-output-start", data)
	if err != nil {
		return nil, err
	}
	_, err = ctx.DoTask(swf.RunPolicy{
		Retry: swf.RetryPolicy{
			MaximumAttempts:    1,
			BackoffCoefficient: 1,
		},
	}, "child-run-output-finish", started)
	if err != nil {
		if errors.Is(err, swf.ErrJobFailed) {
			return nil, err
		}
		if j.branchRuns != nil {
			j.branchRuns.Add(1)
		}
		return ctx.DoTask(swf.RunPolicy{}, "child-run-output-unexpected-branch", data)
	}
	return data, nil
}

type childRunOutputStartTask struct {
	engine *swf.SWFEngine
}

func (childRunOutputStartTask) Name() string { return "child-run-output-start" }

func (t childRunOutputStartTask) Run(ctx swf.TaskContext, data swf.TaskData) (swf.TaskData, error) {
	if t.engine == nil || *t.engine == nil {
		return nil, errors.New("engine not configured")
	}
	childKey, err := (*t.engine).StartJob(context.Background(), swf.StartJob{
		TenantId:  ctx.JobKey.TenantId,
		JobType:   "child-run-output-failing-child",
		Data:      data,
		RunPolicy: swf.DefaultRunPolicy(),
	})
	if err != nil {
		return nil, err
	}
	return swf.NewTaskDataOrPanic(map[string]string{"job_id": childKey.JobId}), nil
}

type childRunOutputFinishTask struct {
	engine *swf.SWFEngine
}

func (childRunOutputFinishTask) Name() string { return "child-run-output-finish" }

func (t childRunOutputFinishTask) Run(ctx swf.TaskContext, data swf.TaskData) (swf.TaskData, error) {
	if t.engine == nil || *t.engine == nil {
		return nil, errors.New("engine not configured")
	}
	raw, err := data.GetData()
	if err != nil {
		return nil, err
	}
	var payload map[string]string
	if err := json.Unmarshal(raw, &payload); err != nil {
		return nil, err
	}
	childID := payload["job_id"]
	if err := ctx.AwaitJobs(childID); err != nil {
		return nil, err
	}
	childKey := swf.JobKey{TenantId: ctx.JobKey.TenantId, JobId: childID}
	run, err := (*t.engine).GetJobRun(context.Background(), swf.GetJobRunRequest{
		JobKey:           childKey,
		IncludeOutputs:   true,
		IncludeArtifacts: true,
	})
	if err != nil {
		return nil, err
	}
	return run.GetOutput(*t.engine, childKey.TenantId)
}

type childRunOutputUnexpectedBranchTask struct{}

func (childRunOutputUnexpectedBranchTask) Name() string { return "child-run-output-unexpected-branch" }

func (childRunOutputUnexpectedBranchTask) Run(_ swf.TaskContext, data swf.TaskData) (swf.TaskData, error) {
	return data, nil
}

type childRunOutputFailingChildJob struct{}

func (childRunOutputFailingChildJob) Name() string { return "child-run-output-failing-child" }

func (childRunOutputFailingChildJob) Run(ctx swf.JobContext, data swf.JobData) (swf.JobData, error) {
	return ctx.DoTask(swf.RunPolicy{
		Retry: swf.RetryPolicy{
			MaximumAttempts:    1,
			BackoffCoefficient: 1,
		},
	}, "child-run-output-fail-task", data)
}

type childRunOutputFailTask struct{}

func (childRunOutputFailTask) Name() string { return "child-run-output-fail-task" }

func (childRunOutputFailTask) Run(_ swf.TaskContext, _ swf.TaskData) (swf.TaskData, error) {
	time.Sleep(150 * time.Millisecond)
	return nil, swf.AppError{Payload: swf.AppErrorPayload{Message: "child failed", Level: "error"}}
}

func TestArtifactPassthroughAcrossBuiltInRuntimes(t *testing.T) {
	ws := swftest.MustWorkSet(t, artifactPassthroughJob{}, artifactPassthroughTask{})

	for _, harness := range swftest.BuiltInRuntimeHarnesses() {
		harness := harness
		t.Run(harness.Name, func(t *testing.T) {
			built := harness.New(t, ws)
			defer built.Shutdown(t)

			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()

			jobKey, err := built.Engine.StartJob(ctx, swf.StartJob{
				TenantId: "tenant-artifact-passthrough-" + harness.Name,
				JobType:  ws.JobWorker.Name(),
				JobID:    "artifact-passthrough",
				Data:     swftest.NumberTaskData(1),
			})
			if err != nil {
				t.Fatalf("start job: %v", err)
			}

			done := make(chan error, 1)
			go func() {
				done <- swf.WaitForJobToComplete(ctx, 3*time.Second, jobKey, built.Engine)
			}()

			select {
			case err := <-done:
				if err != nil {
					t.Fatalf("wait for completion: %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("artifact passthrough workflow deadlocked")
			}

			result, err := built.Engine.GetJobResult(ctx, jobKey)
			if err != nil {
				t.Fatalf("get job result: %v", err)
			}
			artifacts, err := result.GetArtifacts()
			if err != nil {
				t.Fatalf("get result artifacts: %v", err)
			}
			if len(artifacts) != 1 {
				t.Fatalf("expected 1 artifact, got %d", len(artifacts))
			}
			data, err := artifacts[0].Bytes(ctx)
			if err != nil {
				t.Fatalf("read artifact bytes: %v", err)
			}
			if string(data) != "artifact-passthrough" {
				t.Fatalf("unexpected artifact bytes %q", string(data))
			}
		})
	}
}

func TestAwaitFailedChildReplayAcrossBuiltInRuntimes(t *testing.T) {
	for _, harness := range swftest.BuiltInRuntimeHarnesses() {
		harness := harness
		t.Run(harness.Name, func(t *testing.T) {
			childStarted := make(chan struct{})
			releaseChild := make(chan struct{})
			parent := &awaitFailedChildJob{childID: "failed-child-job"}
			child := failedChildJob{started: childStarted, release: releaseChild}

			childWS := swftest.MustWorkSet(t, child)
			parentWS := swftest.MustWorkSet(t, parent)
			built := harness.New(t, childWS, parentWS)
			defer built.Shutdown(t)
			parent.engine = built.Engine

			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()

			childKey, err := built.Engine.StartJob(ctx, swf.StartJob{
				TenantId: "tenant-await-failed-child-" + harness.Name,
				JobType:  child.Name(),
				JobID:    parent.childID,
				Data:     swftest.NumberTaskData(1),
			})
			if err != nil {
				t.Fatalf("start child: %v", err)
			}
			parentKey, err := built.Engine.StartJob(ctx, swf.StartJob{
				TenantId: childKey.TenantId,
				JobType:  parent.Name(),
				JobID:    "parent-await-failed-child",
				Data:     swftest.NumberTaskData(1),
			})
			if err != nil {
				t.Fatalf("start parent: %v", err)
			}

			select {
			case <-childStarted:
			case <-ctx.Done():
				t.Fatalf("child did not start: %v", ctx.Err())
			}

			swftest.WaitForEngineStatus(t, ctx, built.Engine, parentKey, swf.JobStatusPendingJobs)
			close(releaseChild)
			swftest.WaitForEngineStatus(t, ctx, built.Engine, childKey, swf.JobStatusCompleted)
			swftest.WaitForEngineStatus(t, ctx, built.Engine, parentKey, swf.JobStatusCompleted)

			_, err = built.Engine.GetJobResult(ctx, parentKey)
			if err == nil {
				t.Fatal("expected parent result to fail")
			}
			if !strings.Contains(err.Error(), "child failed") {
				t.Fatalf("unexpected parent error %v", err)
			}

			_, replayErr := built.Engine.ReplayJobRun(ctx, swf.ReplayRunRequest{JobKey: parentKey})
			if replayErr == nil {
				t.Fatal("expected replay to surface child failure")
			}
			if errors.Is(replayErr, swf.ErrWorkflowNotDeterministic) || strings.Contains(replayErr.Error(), "workflow was not deterministic") {
				t.Fatalf("unexpected replay determinism error: %v", replayErr)
			}
			if !strings.Contains(replayErr.Error(), "child failed") {
				t.Fatalf("unexpected replay error %v", replayErr)
			}
		})
	}
}

func TestToyAwaitFailedChildViaGetJobRunOutputCompletes(t *testing.T) {
	var toyHarness swftest.RuntimeHarness
	for _, harness := range swftest.BuiltInRuntimeHarnesses() {
		if harness.Name == "toy" {
			toyHarness = harness
			break
		}
	}
	if toyHarness.Name == "" {
		t.Fatal("toy runtime harness not found")
	}

	childStarted := make(chan struct{})
	releaseChild := make(chan struct{})
	parent := &awaitFailedChildViaRunOutputJob{}
	child := failedChildJob{started: childStarted, release: releaseChild}

	childWS := swftest.MustWorkSet(t, child)
	parentWS := swftest.MustWorkSet(t, parent)
	built := toyHarness.New(t, childWS, parentWS)
	defer built.Shutdown(t)
	parent.engine = built.Engine

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	parentKey, err := built.Engine.StartJob(ctx, swf.StartJob{
		TenantId: "tenant-await-failed-child-run-output",
		JobType:  parent.Name(),
		JobID:    "parent-await-failed-child-run-output",
		Data:     swftest.NumberTaskData(1),
	})
	if err != nil {
		t.Fatalf("start parent: %v", err)
	}

	select {
	case <-childStarted:
	case <-ctx.Done():
		t.Fatalf("child did not start: %v", ctx.Err())
	}

	swftest.WaitForEngineStatus(t, ctx, built.Engine, parentKey, swf.JobStatusPendingJobs)
	close(releaseChild)
	swftest.WaitForEngineStatus(t, ctx, built.Engine, parentKey, swf.JobStatusCompleted)

	_, err = built.Engine.GetJobResult(ctx, parentKey)
	if err == nil {
		t.Fatal("expected parent result to fail")
	}
	if !strings.Contains(err.Error(), "child failed") {
		t.Fatalf("unexpected parent error %v", err)
	}

	_, replayErr := built.Engine.ReplayJobRun(ctx, swf.ReplayRunRequest{JobKey: parentKey})
	if replayErr == nil {
		t.Fatal("expected replay to surface child failure")
	}
	if errors.Is(replayErr, swf.ErrWorkflowNotDeterministic) || strings.Contains(replayErr.Error(), "workflow was not deterministic") {
		t.Fatalf("unexpected replay determinism error: %v", replayErr)
	}
	if !strings.Contains(replayErr.Error(), "child failed") {
		t.Fatalf("unexpected replay error %v", replayErr)
	}
}

func TestGetJobRunOutputErrorShapeStableAcrossBuiltInRuntimes(t *testing.T) {
	for _, harness := range swftest.BuiltInRuntimeHarnesses() {
		harness := harness
		t.Run(harness.Name, func(t *testing.T) {
			branchRuns := &atomic.Int32{}
			parent := childRunOutputRetryParentJob{branchRuns: branchRuns}
			var engineRef swf.SWFEngine
			parentWS := swftest.MustWorkSet(t, parent, childRunOutputStartTask{engine: &engineRef}, childRunOutputFinishTask{engine: &engineRef}, childRunOutputUnexpectedBranchTask{})
			childWS := swftest.MustWorkSet(t, childRunOutputFailingChildJob{}, childRunOutputFailTask{})

			built := harness.New(t, parentWS, childWS)
			defer built.Shutdown(t)
			engineRef = built.Engine

			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()

			jobKey, err := built.Engine.StartJob(ctx, swf.StartJob{
				TenantId:  "tenant-job-run-output-shape-" + harness.Name,
				JobType:   parent.Name(),
				Data:      swftest.NumberTaskData(1),
				RunPolicy: swf.DefaultRunPolicy(),
			})
			if err != nil {
				t.Fatalf("start parent: %v", err)
			}

			swftest.WaitForEngineStatus(t, ctx, built.Engine, jobKey, swf.JobStatusCompleted)

			if branchRuns.Load() != 0 {
				t.Fatalf("unexpected replay branch executed %d times", branchRuns.Load())
			}

			_, err = built.Engine.GetJobResult(ctx, jobKey)
			if err == nil {
				t.Fatal("expected parent result to fail")
			}
			if !errors.Is(err, swf.ErrJobFailed) && !strings.Contains(err.Error(), "child failed") {
				t.Fatalf("unexpected parent error %v", err)
			}
		})
	}
}

func TestToyGetJobRunIncludesFailedTaskAttemptForChildOutputFailure(t *testing.T) {
	var toyHarness swftest.RuntimeHarness
	for _, harness := range swftest.BuiltInRuntimeHarnesses() {
		if harness.Name == "toy" {
			toyHarness = harness
			break
		}
	}
	if toyHarness.Name == "" {
		t.Fatal("toy runtime harness not found")
	}

	branchRuns := &atomic.Int32{}
	parent := childRunOutputRetryParentJob{branchRuns: branchRuns}
	var engineRef swf.SWFEngine
	parentWS := swftest.MustWorkSet(t, parent, childRunOutputStartTask{engine: &engineRef}, childRunOutputFinishTask{engine: &engineRef}, childRunOutputUnexpectedBranchTask{})
	childWS := swftest.MustWorkSet(t, childRunOutputFailingChildJob{}, childRunOutputFailTask{})

	built := toyHarness.New(t, parentWS, childWS)
	defer built.Shutdown(t)
	engineRef = built.Engine

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	jobKey, err := built.Engine.StartJob(ctx, swf.StartJob{
		TenantId:  "tenant-toy-run-projection",
		JobType:   parent.Name(),
		Data:      swftest.NumberTaskData(1),
		RunPolicy: swf.DefaultRunPolicy(),
	})
	if err != nil {
		t.Fatalf("start parent: %v", err)
	}
	swftest.WaitForEngineStatus(t, ctx, built.Engine, jobKey, swf.JobStatusCompleted)

	run, err := built.Engine.GetJobRun(ctx, swf.GetJobRunRequest{
		JobKey:           jobKey,
		IncludeInputs:    true,
		IncludeOutputs:   true,
		IncludeArtifacts: true,
	})
	if err != nil {
		t.Fatalf("get job run: %v", err)
	}

	var found bool
	for _, attempt := range run.Attempts {
		for _, task := range attempt.Tasks {
			if task.TaskType != "child-run-output-finish" {
				continue
			}
			for _, taskAttempt := range task.Attempts {
				if taskAttempt.Outcome.Status == swf.TaskOutcomeStatusFailed {
					found = true
					if taskAttempt.Outcome.PayloadKind != "AppError" {
						t.Fatalf("unexpected payload kind %q", taskAttempt.Outcome.PayloadKind)
					}
					if taskAttempt.Outcome.Error == nil || !strings.Contains(taskAttempt.Outcome.Error.Message, "child failed") {
						t.Fatalf("unexpected task error %+v", taskAttempt.Outcome.Error)
					}
				}
			}
		}
	}
	if !found {
		t.Fatal("expected failed finish task attempt in toy job run")
	}
}
