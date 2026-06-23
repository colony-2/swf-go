package engineconformance_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	jobdbtest "github.com/colony-2/jobdb/pkg/workflow/internal/jobdbtest"
	"github.com/colony-2/jobdb/pkg/workflow"
)

type artifactPassthroughJob struct{}

func (artifactPassthroughJob) Name() string { return "artifact-passthrough-job" }

func (artifactPassthroughJob) Run(ctx workflow.JobContext, input jobdb.JobData) (jobdb.JobData, error) {
	return ctx.DoTask(jobdb.RunPolicy{}, "artifact-passthrough-task", input)
}

type artifactPassthroughTask struct{}

func (artifactPassthroughTask) Name() string { return "artifact-passthrough-task" }

func (artifactPassthroughTask) Run(_ workflow.TaskContext, _ jobdb.TaskData) (jobdb.TaskData, error) {
	return &jobdb.SimpleTaskData{
		Data: []byte(`{"ok":true}`),
		Artifacts: []jobdb.Artifact{
			jobdb.NewArtifactFromBytes("trace.txt", []byte("artifact-passthrough")),
		},
	}, nil
}

type failedChildJob struct {
	started chan<- struct{}
	release <-chan struct{}
}

func (failedChildJob) Name() string { return "failed-child" }

func (j failedChildJob) Run(_ workflow.JobContext, _ jobdb.JobData) (jobdb.JobData, error) {
	if j.started != nil {
		close(j.started)
	}
	<-j.release
	return nil, jobdb.AppError{Payload: jobdb.AppErrorPayload{Message: "child failed", Level: "error"}}
}

type awaitFailedChildJob struct {
	engine  workflow.Engine
	childID string
}

func (awaitFailedChildJob) Name() string { return "await-failed-child" }

func (j *awaitFailedChildJob) Run(ctx workflow.JobContext, data jobdb.JobData) (jobdb.JobData, error) {
	if err := ctx.AwaitJobs(j.childID); err != nil {
		return nil, err
	}
	result, err := jobResultForTest(j.engine, context.Background(), jobdb.JobKey{
		TenantId: ctx.GetJobKey().TenantId,
		JobId:    j.childID,
	})
	if err != nil {
		return nil, err
	}
	return result, nil
}

type awaitFailedChildViaRunOutputJob struct {
	engine workflow.Engine
}

func (awaitFailedChildViaRunOutputJob) Name() string { return "await-failed-child-run-output" }

func (j *awaitFailedChildViaRunOutputJob) Run(ctx workflow.JobContext, data jobdb.JobData) (jobdb.JobData, error) {
	childKey, err := j.engine.SubmitJob(context.Background(), jobdb.SubmitJob{
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
	run, err := j.engine.GetJobRun(context.Background(), jobdb.GetJobRunRequest{
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

func (j childRunOutputRetryParentJob) Run(ctx workflow.JobContext, data jobdb.JobData) (jobdb.JobData, error) {
	started, err := ctx.DoTask(jobdb.RunPolicy{}, "child-run-output-start", data)
	if err != nil {
		return nil, err
	}
	_, err = ctx.DoTask(jobdb.RunPolicy{
		Retry: jobdb.RetryPolicy{
			MaximumAttempts:    1,
			BackoffCoefficient: 1,
		},
	}, "child-run-output-finish", started)
	if err != nil {
		if errors.Is(err, jobdb.ErrJobFailed) {
			return nil, err
		}
		if j.branchRuns != nil {
			j.branchRuns.Add(1)
		}
		return ctx.DoTask(jobdb.RunPolicy{}, "child-run-output-unexpected-branch", data)
	}
	return data, nil
}

type childRunOutputStartTask struct {
	engine *workflow.Engine
}

func (childRunOutputStartTask) Name() string { return "child-run-output-start" }

func (t childRunOutputStartTask) Run(ctx workflow.TaskContext, data jobdb.TaskData) (jobdb.TaskData, error) {
	if t.engine == nil || *t.engine == nil {
		return nil, errors.New("engine not configured")
	}
	childKey, err := (*t.engine).SubmitJob(context.Background(), jobdb.SubmitJob{
		TenantId:  ctx.JobKey.TenantId,
		JobType:   "child-run-output-failing-child",
		Data:      data,
		RunPolicy: jobdb.DefaultRunPolicy(),
	})
	if err != nil {
		return nil, err
	}
	return jobdb.NewTaskDataOrPanic(map[string]string{"job_id": childKey.JobId}), nil
}

type childRunOutputFinishTask struct {
	engine *workflow.Engine
}

func (childRunOutputFinishTask) Name() string { return "child-run-output-finish" }

func (t childRunOutputFinishTask) Run(ctx workflow.TaskContext, data jobdb.TaskData) (jobdb.TaskData, error) {
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
	childKey := jobdb.JobKey{TenantId: ctx.JobKey.TenantId, JobId: childID}
	run, err := (*t.engine).GetJobRun(context.Background(), jobdb.GetJobRunRequest{
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

func (childRunOutputUnexpectedBranchTask) Run(_ workflow.TaskContext, data jobdb.TaskData) (jobdb.TaskData, error) {
	return data, nil
}

type childRunOutputFailingChildJob struct{}

func (childRunOutputFailingChildJob) Name() string { return "child-run-output-failing-child" }

func (childRunOutputFailingChildJob) Run(ctx workflow.JobContext, data jobdb.JobData) (jobdb.JobData, error) {
	return ctx.DoTask(jobdb.RunPolicy{
		Retry: jobdb.RetryPolicy{
			MaximumAttempts:    1,
			BackoffCoefficient: 1,
		},
	}, "child-run-output-fail-task", data)
}

type childRunOutputFailTask struct{}

func (childRunOutputFailTask) Name() string { return "child-run-output-fail-task" }

func (childRunOutputFailTask) Run(_ workflow.TaskContext, _ jobdb.TaskData) (jobdb.TaskData, error) {
	time.Sleep(150 * time.Millisecond)
	return nil, jobdb.AppError{Payload: jobdb.AppErrorPayload{Message: "child failed", Level: "error"}}
}

func TestArtifactPassthroughAcrossBuiltInRuntimes(t *testing.T) {
	ws := jobdbtest.MustWorkSet(t, artifactPassthroughJob{}, artifactPassthroughTask{})

	for _, harness := range jobdbtest.BuiltInRuntimeHarnesses() {
		harness := harness
		t.Run(harness.Name, func(t *testing.T) {
			built := harness.New(t, ws)
			defer built.Shutdown(t)

			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()

			jobKey, err := built.Engine.SubmitJob(ctx, jobdb.SubmitJob{
				TenantId: built.WorkerTenantID,
				JobType:  ws.JobWorker.Name(),
				JobID:    "artifact-passthrough",
				Data:     jobdbtest.NumberTaskData(1),
			})
			if err != nil {
				t.Fatalf("start job: %v", err)
			}

			done := make(chan error, 1)
			go func() {
				done <- workflow.WaitForJobToComplete(ctx, 3*time.Second, jobKey, built.Engine)
			}()

			select {
			case err := <-done:
				if err != nil {
					t.Fatalf("wait for completion: %v", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("artifact passthrough workflow deadlocked")
			}

			result, err := jobResultForTest(built.Engine, ctx, jobKey)
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
	for _, harness := range jobdbtest.BuiltInRuntimeHarnesses() {
		harness := harness
		t.Run(harness.Name, func(t *testing.T) {
			childStarted := make(chan struct{})
			releaseChild := make(chan struct{})
			parent := &awaitFailedChildJob{childID: "failed-child-job"}
			child := failedChildJob{started: childStarted, release: releaseChild}

			childWS := jobdbtest.MustWorkSet(t, child)
			parentWS := jobdbtest.MustWorkSet(t, parent)
			built := harness.New(t, childWS, parentWS)
			defer built.Shutdown(t)
			parent.engine = built.Engine

			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()

			childKey, err := built.Engine.SubmitJob(ctx, jobdb.SubmitJob{
				TenantId: built.WorkerTenantID,
				JobType:  child.Name(),
				JobID:    parent.childID,
				Data:     jobdbtest.NumberTaskData(1),
			})
			if err != nil {
				t.Fatalf("start child: %v", err)
			}
			parentKey, err := built.Engine.SubmitJob(ctx, jobdb.SubmitJob{
				TenantId: childKey.TenantId,
				JobType:  parent.Name(),
				JobID:    "parent-await-failed-child",
				Data:     jobdbtest.NumberTaskData(1),
			})
			if err != nil {
				t.Fatalf("start parent: %v", err)
			}

			select {
			case <-childStarted:
			case <-ctx.Done():
				t.Fatalf("child did not start: %v", ctx.Err())
			}

			jobdbtest.WaitForEngineStatus(t, ctx, built.Engine, parentKey, jobdb.JobStatusPendingJobs)
			close(releaseChild)
			jobdbtest.WaitForEngineStatus(t, ctx, built.Engine, childKey, jobdb.JobStatusCompleted)
			jobdbtest.WaitForEngineStatus(t, ctx, built.Engine, parentKey, jobdb.JobStatusCompleted)

			_, err = jobResultForTest(built.Engine, ctx, parentKey)
			if err == nil {
				t.Fatal("expected parent result to fail")
			}
			if !strings.Contains(err.Error(), "child failed") {
				t.Fatalf("unexpected parent error %v", err)
			}

			_, replayErr := built.Engine.ReplayJobRun(ctx, workflow.ReplayRunRequest{JobKey: parentKey})
			if replayErr == nil {
				t.Fatal("expected replay to surface child failure")
			}
			if errors.Is(replayErr, jobdb.ErrWorkflowNotDeterministic) || strings.Contains(replayErr.Error(), "workflow was not deterministic") {
				t.Fatalf("unexpected replay determinism error: %v", replayErr)
			}
			if !strings.Contains(replayErr.Error(), "child failed") {
				t.Fatalf("unexpected replay error %v", replayErr)
			}
		})
	}
}

func TestToyAwaitFailedChildViaGetJobRunOutputCompletes(t *testing.T) {
	var toyHarness jobdbtest.RuntimeHarness
	for _, harness := range jobdbtest.BuiltInRuntimeHarnesses() {
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

	childWS := jobdbtest.MustWorkSet(t, child)
	parentWS := jobdbtest.MustWorkSet(t, parent)
	built := toyHarness.New(t, childWS, parentWS)
	defer built.Shutdown(t)
	parent.engine = built.Engine

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	parentKey, err := built.Engine.SubmitJob(ctx, jobdb.SubmitJob{
		TenantId: built.WorkerTenantID,
		JobType:  parent.Name(),
		JobID:    "parent-await-failed-child-run-output",
		Data:     jobdbtest.NumberTaskData(1),
	})
	if err != nil {
		t.Fatalf("start parent: %v", err)
	}

	select {
	case <-childStarted:
	case <-ctx.Done():
		t.Fatalf("child did not start: %v", ctx.Err())
	}

	jobdbtest.WaitForEngineStatus(t, ctx, built.Engine, parentKey, jobdb.JobStatusPendingJobs)
	close(releaseChild)
	jobdbtest.WaitForEngineStatus(t, ctx, built.Engine, parentKey, jobdb.JobStatusCompleted)

	_, err = jobResultForTest(built.Engine, ctx, parentKey)
	if err == nil {
		t.Fatal("expected parent result to fail")
	}
	if !strings.Contains(err.Error(), "child failed") {
		t.Fatalf("unexpected parent error %v", err)
	}

	_, replayErr := built.Engine.ReplayJobRun(ctx, workflow.ReplayRunRequest{JobKey: parentKey})
	if replayErr == nil {
		t.Fatal("expected replay to surface child failure")
	}
	if errors.Is(replayErr, jobdb.ErrWorkflowNotDeterministic) || strings.Contains(replayErr.Error(), "workflow was not deterministic") {
		t.Fatalf("unexpected replay determinism error: %v", replayErr)
	}
	if !strings.Contains(replayErr.Error(), "child failed") {
		t.Fatalf("unexpected replay error %v", replayErr)
	}
}

func TestGetJobRunOutputErrorShapeStableAcrossBuiltInRuntimes(t *testing.T) {
	for _, harness := range jobdbtest.BuiltInRuntimeHarnesses() {
		harness := harness
		t.Run(harness.Name, func(t *testing.T) {
			branchRuns := &atomic.Int32{}
			parent := childRunOutputRetryParentJob{branchRuns: branchRuns}
			var engineRef workflow.Engine
			parentWS := jobdbtest.MustWorkSet(t, parent, childRunOutputStartTask{engine: &engineRef}, childRunOutputFinishTask{engine: &engineRef}, childRunOutputUnexpectedBranchTask{})
			childWS := jobdbtest.MustWorkSet(t, childRunOutputFailingChildJob{}, childRunOutputFailTask{})

			built := harness.New(t, parentWS, childWS)
			defer built.Shutdown(t)
			engineRef = built.Engine

			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()

			jobKey, err := built.Engine.SubmitJob(ctx, jobdb.SubmitJob{
				TenantId:  built.WorkerTenantID,
				JobType:   parent.Name(),
				Data:      jobdbtest.NumberTaskData(1),
				RunPolicy: jobdb.DefaultRunPolicy(),
			})
			if err != nil {
				t.Fatalf("start parent: %v", err)
			}

			jobdbtest.WaitForEngineStatus(t, ctx, built.Engine, jobKey, jobdb.JobStatusCompleted)

			if branchRuns.Load() != 0 {
				t.Fatalf("unexpected replay branch executed %d times", branchRuns.Load())
			}

			_, err = jobResultForTest(built.Engine, ctx, jobKey)
			if err == nil {
				t.Fatal("expected parent result to fail")
			}
			if !errors.Is(err, jobdb.ErrJobFailed) && !strings.Contains(err.Error(), "child failed") {
				t.Fatalf("unexpected parent error %v", err)
			}
		})
	}
}

func TestToyGetJobRunIncludesFailedTaskAttemptForChildOutputFailure(t *testing.T) {
	var toyHarness jobdbtest.RuntimeHarness
	for _, harness := range jobdbtest.BuiltInRuntimeHarnesses() {
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
	var engineRef workflow.Engine
	parentWS := jobdbtest.MustWorkSet(t, parent, childRunOutputStartTask{engine: &engineRef}, childRunOutputFinishTask{engine: &engineRef}, childRunOutputUnexpectedBranchTask{})
	childWS := jobdbtest.MustWorkSet(t, childRunOutputFailingChildJob{}, childRunOutputFailTask{})

	built := toyHarness.New(t, parentWS, childWS)
	defer built.Shutdown(t)
	engineRef = built.Engine

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	jobKey, err := built.Engine.SubmitJob(ctx, jobdb.SubmitJob{
		TenantId:  built.WorkerTenantID,
		JobType:   parent.Name(),
		Data:      jobdbtest.NumberTaskData(1),
		RunPolicy: jobdb.DefaultRunPolicy(),
	})
	if err != nil {
		t.Fatalf("start parent: %v", err)
	}
	jobdbtest.WaitForEngineStatus(t, ctx, built.Engine, jobKey, jobdb.JobStatusCompleted)

	run, err := built.Engine.GetJobRun(ctx, jobdb.GetJobRunRequest{
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
				if taskAttempt.Outcome.Status == jobdb.TaskOutcomeStatusFailed {
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
