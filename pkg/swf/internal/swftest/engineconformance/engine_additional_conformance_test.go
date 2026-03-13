package engineconformance_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/colony-2/swf-go/pkg/swf"
	swftest "github.com/colony-2/swf-go/pkg/swf/internal/swftest"
)

type customIDJob struct{}

func (customIDJob) Name() string { return "custom-id-job" }
func (customIDJob) Run(_ swf.JobContext, data swf.JobData) (swf.JobData, error) {
	return data, nil
}

type awaitChildJob struct {
	started chan<- struct{}
	release <-chan struct{}
}

func (awaitChildJob) Name() string { return "await-child" }
func (j awaitChildJob) Run(_ swf.JobContext, data swf.JobData) (swf.JobData, error) {
	if j.started != nil {
		close(j.started)
	}
	<-j.release
	return data, nil
}

type awaitParentJob struct {
	childJobID string
}

func (awaitParentJob) Name() string { return "await-parent" }
func (j awaitParentJob) Run(ctx swf.JobContext, data swf.JobData) (swf.JobData, error) {
	if err := ctx.AwaitJobs(j.childJobID); err != nil {
		return nil, err
	}
	return data, nil
}

type awaitTaskJob struct{}

func (awaitTaskJob) Name() string { return "await-task-parent" }
func (awaitTaskJob) Run(ctx swf.JobContext, data swf.JobData) (swf.JobData, error) {
	return ctx.DoTask(swf.RunPolicy{}, "await-task", data)
}

type awaitTaskWorker struct {
	childJobID string
}

func (awaitTaskWorker) Name() string { return "await-task" }
func (t awaitTaskWorker) Run(ctx swf.TaskContext, data swf.TaskData) (swf.TaskData, error) {
	if err := ctx.AwaitJobs(t.childJobID); err != nil {
		return nil, err
	}
	return data, nil
}

type pendingJob struct{}

func (pendingJob) Name() string { return "pending-job" }
func (pendingJob) Run(ctx swf.JobContext, data swf.JobData) (swf.JobData, error) {
	return ctx.DoTask(swf.RunPolicy{}, "pending-task", data)
}

type transformPendingJob struct{}

func (transformPendingJob) Name() string { return "transform-pending-job" }
func (transformPendingJob) Run(ctx swf.JobContext, data swf.JobData) (swf.JobData, error) {
	first, err := ctx.DoTask(swf.RunPolicy{}, swftest.AddOneTaskName, data)
	if err != nil {
		return nil, err
	}
	n, err := numberFromTaskData(first)
	if err != nil {
		return nil, err
	}
	transformed := swftest.NumberTaskData(n + 100)
	return ctx.DoTask(swf.RunPolicy{}, "pending-task", transformed)
}

type externalApprovalJob struct{}

func (externalApprovalJob) Name() string { return "external-approval-job" }
func (externalApprovalJob) Run(ctx swf.JobContext, data swf.JobData) (swf.JobData, error) {
	return ctx.DoTask(swf.RunPolicy{}, "approval", data)
}

type retryJob struct {
	attempts int
}

func (retryJob) Name() string { return "retry-job" }
func (j *retryJob) Run(_ swf.JobContext, data swf.JobData) (swf.JobData, error) {
	j.attempts++
	if j.attempts == 1 {
		return nil, swf.AppError{Payload: swf.AppErrorPayload{Message: "retry me", Level: "error"}}
	}
	return data, nil
}

type retryTaskJob struct{}

func (retryTaskJob) Name() string { return "retry-task-job" }
func (retryTaskJob) Run(ctx swf.JobContext, data swf.JobData) (swf.JobData, error) {
	return ctx.DoTask(swf.RunPolicy{
		Retry: swf.RetryPolicy{
			MaximumAttempts:    3,
			BackoffCoefficient: 1,
		},
	}, "retry-task", data)
}

type retryTask struct {
	attempts int
}

func (retryTask) Name() string { return "retry-task" }
func (t *retryTask) Run(_ swf.TaskContext, data swf.TaskData) (swf.TaskData, error) {
	t.attempts++
	if t.attempts == 1 {
		return nil, swf.AppError{Payload: swf.AppErrorPayload{Message: "retry task", Level: "error"}}
	}
	return data, nil
}

func numberFromTaskData(data swf.TaskData) (int, error) {
	raw, err := data.GetData()
	if err != nil {
		return 0, err
	}
	payload := map[string]int{}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return 0, err
	}
	return payload["n"], nil
}

func TestCustomJobIDAcrossBuiltInRuntimes(t *testing.T) {
	ws := swftest.MustWorkSet(t, customIDJob{})

	for _, harness := range swftest.BuiltInRuntimeHarnesses() {
		harness := harness
		t.Run(harness.Name, func(t *testing.T) {
			built := harness.New(t, ws)
			defer built.Shutdown(t)

			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()

			jobKey, err := built.Engine.StartJob(ctx, swf.StartJob{
				TenantId: "tenant-custom-" + harness.Name,
				JobType:  ws.JobWorker.Name(),
				JobID:    "custom-job-id",
				Data:     swftest.NumberTaskData(7),
			})
			if err != nil {
				t.Fatalf("start job: %v", err)
			}
			if jobKey.JobId != "custom-job-id" {
				t.Fatalf("unexpected job key %+v", jobKey)
			}
			swftest.WaitForEngineStatus(t, ctx, built.Engine, jobKey, swf.JobStatusCompleted)
		})
	}
}

func TestRestartValidationAcrossBuiltInRuntimes(t *testing.T) {
	for _, harness := range swftest.BuiltInRuntimeHarnesses() {
		harness := harness
		t.Run(harness.Name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()

			t.Run("negative-last-step", func(t *testing.T) {
				ws := swftest.MustWorkSet(t, customIDJob{})
				built := harness.New(t, ws)
				defer built.Shutdown(t)

				if _, err := built.Engine.RestartJob(ctx, swf.RestartJob{
					PriorJobKey:    swf.JobKey{TenantId: "tenant", JobId: "missing"},
					LastStepToKeep: -1,
				}); err == nil {
					t.Fatal("expected restart to fail")
				}
			})

			t.Run("missing-next-chapter", func(t *testing.T) {
				ws := swftest.MustWorkSet(t, customIDJob{})
				built := harness.New(t, ws)
				defer built.Shutdown(t)

				jobKey, err := built.Engine.StartJob(ctx, swf.StartJob{
					TenantId: "tenant-missing-next-" + harness.Name,
					JobType:  ws.JobWorker.Name(),
					Data:     swftest.NumberTaskData(1),
				})
				if err != nil {
					t.Fatalf("start job: %v", err)
				}
				swftest.WaitForEngineStatus(t, ctx, built.Engine, jobKey, swf.JobStatusCompleted)

				if _, err := built.Engine.RestartJob(ctx, swf.RestartJob{
					PriorJobKey:    jobKey,
					LastStepToKeep: 1,
				}); err == nil {
					t.Fatal("expected restart to fail when next chapter is missing")
				}
			})

			t.Run("retry-boundary", func(t *testing.T) {
				job := &retryTaskJob{}
				task := &retryTask{}
				ws := swftest.MustWorkSet(t, job, task)
				built := harness.New(t, ws)
				defer built.Shutdown(t)

				jobKey, err := built.Engine.StartJob(ctx, swf.StartJob{
					TenantId: "tenant-retry-boundary-" + harness.Name,
					JobType:  job.Name(),
					Data:     swftest.NumberTaskData(1),
				})
				if err != nil {
					t.Fatalf("start job: %v", err)
				}
				swftest.WaitForEngineStatus(t, ctx, built.Engine, jobKey, swf.JobStatusCompleted)

				if _, err := built.Engine.RestartJob(ctx, swf.RestartJob{
					PriorJobKey:    jobKey,
					LastStepToKeep: 1,
				}); err == nil {
					t.Fatal("expected restart to fail when slicing into retry chain")
				}
			})
		})
	}
}

func TestAwaitJobsAcrossBuiltInRuntimes(t *testing.T) {
	for _, harness := range swftest.BuiltInRuntimeHarnesses() {
		harness := harness
		t.Run(harness.Name, func(t *testing.T) {
			childStarted := make(chan struct{})
			releaseChild := make(chan struct{})
			t.Cleanup(func() {
				select {
				case <-releaseChild:
				default:
					close(releaseChild)
				}
			})

			tenantID := "tenant-await-" + harness.Name
			childJobID := "child"
			parentJobID := "parent"
			childWorker := awaitChildJob{started: childStarted, release: releaseChild}
			parentWorker := awaitParentJob{childJobID: childJobID}
			built := harness.New(t, swftest.MustWorkSet(t, childWorker), swftest.MustWorkSet(t, parentWorker))
			defer built.Shutdown(t)

			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()

			childDone := swftest.MustStartJobAsync(t, built.Engine, swf.StartJob{
				TenantId: tenantID,
				JobType:  childWorker.Name(),
				JobID:    childJobID,
				Data:     swftest.NumberTaskData(1),
			})
			select {
			case <-childStarted:
			case <-ctx.Done():
				t.Fatalf("child did not start: %v", ctx.Err())
			}

			parentDone := swftest.MustStartJobAsync(t, built.Engine, swf.StartJob{
				TenantId: tenantID,
				JobType:  parentWorker.Name(),
				JobID:    parentJobID,
				Data:     swftest.NumberTaskData(2),
			})

			swftest.WaitForEngineStatus(t, ctx, built.Engine, swf.JobKey{TenantId: tenantID, JobId: parentJobID}, swf.JobStatusPendingJobs)
			resp, err := built.Engine.ListJobs(ctx, swf.ListJobsRequest{TenantIds: []string{tenantID}, JobKeys: []swf.JobKey{{TenantId: tenantID, JobId: parentJobID}}})
			if err != nil {
				t.Fatalf("list jobs: %v", err)
			}
			if len(resp.Jobs) != 1 || len(resp.Jobs[0].WaitFor) != 1 || resp.Jobs[0].WaitFor[0] != childJobID {
				t.Fatalf("unexpected wait_for response %+v", resp.Jobs)
			}

			close(releaseChild)
			if err := <-childDone; err != nil {
				t.Fatalf("child failed: %v", err)
			}
			if err := <-parentDone; err != nil {
				t.Fatalf("parent failed: %v", err)
			}
			swftest.WaitForEngineStatus(t, ctx, built.Engine, swf.JobKey{TenantId: tenantID, JobId: parentJobID}, swf.JobStatusCompleted)
		})
	}
}

func TestTaskContextAwaitJobsAcrossBuiltInRuntimes(t *testing.T) {
	for _, harness := range swftest.BuiltInRuntimeHarnesses() {
		harness := harness
		t.Run(harness.Name, func(t *testing.T) {
			childStarted := make(chan struct{})
			releaseChild := make(chan struct{})
			t.Cleanup(func() {
				select {
				case <-releaseChild:
				default:
					close(releaseChild)
				}
			})

			tenantID := "tenant-await-task-" + harness.Name
			childJobID := "child"
			childWorker := awaitChildJob{started: childStarted, release: releaseChild}
			taskWorker := awaitTaskWorker{childJobID: childJobID}
			parentWorker := awaitTaskJob{}
			built := harness.New(t, swftest.MustWorkSet(t, childWorker), swftest.MustWorkSet(t, parentWorker, taskWorker))
			defer built.Shutdown(t)

			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()

			childDone := swftest.MustStartJobAsync(t, built.Engine, swf.StartJob{
				TenantId: tenantID,
				JobType:  childWorker.Name(),
				JobID:    childJobID,
				Data:     swftest.NumberTaskData(1),
			})
			select {
			case <-childStarted:
			case <-ctx.Done():
				t.Fatalf("child did not start: %v", ctx.Err())
			}

			parentKey := swf.JobKey{TenantId: tenantID, JobId: "parent"}
			parentDone := swftest.MustStartJobAsync(t, built.Engine, swf.StartJob{
				TenantId: tenantID,
				JobType:  parentWorker.Name(),
				JobID:    parentKey.JobId,
				Data:     swftest.NumberTaskData(2),
			})

			swftest.WaitForEngineStatus(t, ctx, built.Engine, parentKey, swf.JobStatusPendingJobs)
			close(releaseChild)
			if err := <-childDone; err != nil {
				t.Fatalf("child failed: %v", err)
			}
			if err := <-parentDone; err != nil {
				t.Fatalf("parent failed: %v", err)
			}
			swftest.WaitForEngineStatus(t, ctx, built.Engine, parentKey, swf.JobStatusCompleted)
		})
	}
}

func TestPendingTaskHandlesAcrossBuiltInRuntimes(t *testing.T) {
	for _, harness := range swftest.BuiltInRuntimeHarnesses() {
		harness := harness
		t.Run(harness.Name, func(t *testing.T) {
			ws := swftest.MustWorkSet(t, transformPendingJob{}, swftest.AddOneTask{})
			built := harness.New(t, ws)
			defer built.Shutdown(t)

			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()

			jobKey := swf.JobKey{TenantId: "tenant-pending-" + harness.Name, JobId: "pending"}
			done := swftest.MustStartJobAsync(t, built.Engine, swf.StartJob{
				TenantId: jobKey.TenantId,
				JobType:  ws.JobWorker.Name(),
				JobID:    jobKey.JobId,
				Data:     swftest.NumberTaskData(10),
			})

			handle := swftest.WaitForTaskHandle(t, ctx, built.Engine, ws.JobWorker.Name(), "pending-task", []string{jobKey.TenantId})
			waiting, err := built.Engine.GetWaitingTask(ctx, jobKey)
			if err != nil {
				t.Fatalf("get waiting task: %v", err)
			}
			data, err := waiting.Data()
			if err != nil {
				t.Fatalf("waiting data: %v", err)
			}
			if got := swftest.MustDecodeNumberTaskData(t, data); got != 11 {
				t.Fatalf("expected previous step output 11, got %d", got)
			}

			resp, err := built.Engine.ListJobs(ctx, swf.ListJobsRequest{
				TenantIds: []string{jobKey.TenantId},
				JobKeys:   []swf.JobKey{jobKey},
			})
			if err != nil {
				t.Fatalf("list jobs: %v", err)
			}
			if len(resp.Jobs) != 1 {
				t.Fatalf("expected 1 job summary, got %d", len(resp.Jobs))
			}
			if resp.Jobs[0].TaskWaitOutput == nil || *resp.Jobs[0].TaskWaitOutput != handle.TaskOrdinalToComplete() {
				t.Fatalf("unexpected task wait output %+v", resp.Jobs[0].TaskWaitOutput)
			}
			if resp.Jobs[0].TaskWaitInput == nil || *resp.Jobs[0].TaskWaitInput != handle.TaskOrdinalToComplete()-1 {
				t.Fatalf("unexpected task wait input %+v", resp.Jobs[0].TaskWaitInput)
			}

			if err := handle.Finish(ctx, swftest.NumberTaskData(200)); err != nil {
				t.Fatalf("finish waiting task: %v", err)
			}
			if err := <-done; err != nil {
				t.Fatalf("start job async: %v", err)
			}
			swftest.WaitForEngineStatus(t, ctx, built.Engine, jobKey, swf.JobStatusCompleted)
		})
	}
}

func TestReplayAfterExternalTaskCompletionAcrossBuiltInRuntimes(t *testing.T) {
	for _, harness := range swftest.BuiltInRuntimeHarnesses() {
		harness := harness
		t.Run(harness.Name, func(t *testing.T) {
			ws := swftest.MustWorkSet(t, externalApprovalJob{})
			built := harness.New(t, ws)
			defer built.Shutdown(t)

			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()

			jobKey := swf.JobKey{TenantId: "tenant-external-" + harness.Name, JobId: "approval"}
			done := swftest.MustStartJobAsync(t, built.Engine, swf.StartJob{
				TenantId: jobKey.TenantId,
				JobType:  ws.JobWorker.Name(),
				JobID:    jobKey.JobId,
				Data:     swftest.NumberTaskData(42),
				RunPolicy: swf.RunPolicy{
					Retry: swf.RetryPolicy{MaximumAttempts: 3, BackoffCoefficient: 1},
				},
			})

			handle := swftest.WaitForTaskHandle(t, ctx, built.Engine, ws.JobWorker.Name(), "approval", []string{jobKey.TenantId})
			if err := handle.Finish(ctx, swftest.NumberTaskData(42)); err != nil {
				t.Fatalf("finish approval: %v", err)
			}
			if err := <-done; err != nil {
				t.Fatalf("start job async: %v", err)
			}
			swftest.WaitForEngineStatus(t, ctx, built.Engine, jobKey, swf.JobStatusCompleted)

			replayed, err := built.Engine.ReplayJobRun(ctx, swf.ReplayRunRequest{JobKey: jobKey})
			if err != nil {
				t.Fatalf("replay job run: %v", err)
			}
			if got := swftest.MustDecodeNumberTaskData(t, replayed); got != 42 {
				t.Fatalf("unexpected replayed output %d", got)
			}
		})
	}
}

func TestGetJobRunRetryRepresentationOnDirectRuntime(t *testing.T) {
	for _, harness := range swftest.BuiltInRuntimeHarnesses() {
		harness := harness
		if harness.Name != "direct" {
			continue
		}
		t.Run(harness.Name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()

			t.Run("job-retry", func(t *testing.T) {
				job := &retryJob{}
				ws := swftest.MustWorkSet(t, job)
				built := harness.New(t, ws)
				defer built.Shutdown(t)

				jobKey, err := built.Engine.StartJob(ctx, swf.StartJob{
					TenantId: "tenant-job-run-job-" + harness.Name,
					JobType:  job.Name(),
					Data:     swftest.NumberTaskData(1),
					RunPolicy: swf.RunPolicy{
						Retry: swf.RetryPolicy{MaximumAttempts: 3, BackoffCoefficient: 1},
					},
				})
				if err != nil {
					t.Fatalf("start job: %v", err)
				}
				swftest.WaitForEngineStatus(t, ctx, built.Engine, jobKey, swf.JobStatusCompleted)

				resp, err := built.Engine.GetJobRun(ctx, swf.GetJobRunRequest{JobKey: jobKey, IncludeOutputs: true})
				if err != nil {
					t.Fatalf("get job run: %v", err)
				}
				if len(resp.Attempts) != 2 {
					t.Fatalf("expected 2 job attempts, got %d", len(resp.Attempts))
				}
			})

			t.Run("task-retry", func(t *testing.T) {
				job := &retryTaskJob{}
				task := &retryTask{}
				ws := swftest.MustWorkSet(t, job, task)
				built := harness.New(t, ws)
				defer built.Shutdown(t)

				jobKey, err := built.Engine.StartJob(ctx, swf.StartJob{
					TenantId: "tenant-job-run-task-" + harness.Name,
					JobType:  job.Name(),
					Data:     swftest.NumberTaskData(1),
				})
				if err != nil {
					t.Fatalf("start job: %v", err)
				}
				swftest.WaitForEngineStatus(t, ctx, built.Engine, jobKey, swf.JobStatusCompleted)

				resp, err := built.Engine.GetJobRun(ctx, swf.GetJobRunRequest{JobKey: jobKey, IncludeOutputs: true})
				if err != nil {
					t.Fatalf("get job run: %v", err)
				}
				if len(resp.Attempts) != 1 || len(resp.Attempts[0].Tasks) != 1 || len(resp.Attempts[0].Tasks[0].Attempts) != 2 {
					t.Fatalf("unexpected task retry representation %+v", resp.Attempts)
				}
			})
		})
	}
}

type echoTask struct{}

func (echoTask) Name() string { return "echo" }
func (echoTask) Run(_ swf.TaskContext, data swf.TaskData) (swf.TaskData, error) {
	return data, nil
}

type singleEchoJob struct {
	runs *int
}

func (singleEchoJob) Name() string { return "single-echo" }
func (j singleEchoJob) Run(ctx swf.JobContext, data swf.JobData) (swf.JobData, error) {
	if j.runs != nil {
		*j.runs = *j.runs + 1
	}
	return ctx.DoTask(swf.RunPolicy{}, "echo", data)
}

func TestGetJobRunSynthesizedNextAttemptOnDirectRuntime(t *testing.T) {
	for _, harness := range swftest.BuiltInRuntimeHarnesses() {
		harness := harness
		if harness.Name != "direct" {
			continue
		}
		t.Run(harness.Name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()

			job := &retryJob{}
			ws := swftest.MustWorkSet(t, job)
			built := harness.New(t, ws)
			defer built.Shutdown(t)

			jobKey, err := built.Engine.StartJob(ctx, swf.StartJob{
				TenantId: "tenant-synth-next-" + harness.Name,
				JobType:  job.Name(),
				Data:     swftest.NumberTaskData(1),
				RunPolicy: swf.RunPolicy{
					Retry: swf.RetryPolicy{
						MaximumAttempts:    2,
						BackoffCoefficient: 1,
						InitialInterval:    swf.Duration(5 * time.Second),
					},
				},
			})
			if err != nil {
				t.Fatalf("start job: %v", err)
			}

			var resp swf.GetJobRunResponse
			deadline := time.Now().Add(2 * time.Second)
			for time.Now().Before(deadline) {
				resp, err = built.Engine.GetJobRun(ctx, swf.GetJobRunRequest{JobKey: jobKey})
				if err != nil {
					t.Fatalf("get job run: %v", err)
				}
				if len(resp.Attempts) > 0 && resp.Attempts[0].Outcome.Status == swf.TaskOutcomeStatusFailed {
					break
				}
				time.Sleep(20 * time.Millisecond)
			}

			if len(resp.Attempts) != 2 {
				t.Fatalf("expected synthesized next attempt, got %d attempts", len(resp.Attempts))
			}
			if resp.Attempts[1].Output != nil {
				t.Fatalf("expected synthesized pending attempt without output")
			}
			if resp.Job.Status == swf.JobStatusCompleted {
				t.Fatalf("expected job to remain pending during retry backoff")
			}
		})
	}
}

func TestRestartWithExtraOutputDeterminismErrorOnDirectRuntime(t *testing.T) {
	for _, harness := range swftest.BuiltInRuntimeHarnesses() {
		harness := harness
		if harness.Name != "direct" {
			continue
		}
		t.Run(harness.Name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()

			runs := 0
			ws := swftest.MustWorkSet(t, singleEchoJob{runs: &runs}, echoTask{})
			built := harness.New(t, ws)
			defer built.Shutdown(t)

			origInput := swf.NewTaskDataOrPanic(map[string]string{"hello": "world"})
			jobKey, err := built.Engine.StartJob(ctx, swf.StartJob{
				TenantId: "tenant-restart-extra-" + harness.Name,
				JobType:  ws.JobWorker.Name(),
				Data:     origInput,
			})
			if err != nil {
				t.Fatalf("start job: %v", err)
			}
			swftest.WaitForEngineStatus(t, ctx, built.Engine, jobKey, swf.JobStatusCompleted)

			newInput := swf.NewTaskDataOrPanic(map[string]string{"hello": "again"})
			restartKey, err := built.Engine.RestartJob(ctx, swf.RestartJob{
				PriorJobKey:     jobKey,
				LastStepToKeep:  0,
				ExtraTaskInput:  newInput,
				ExtraTaskOutput: newInput,
			})
			if err != nil {
				t.Fatalf("restart job: %v", err)
			}
			swftest.WaitForEngineStatus(t, ctx, built.Engine, restartKey, swf.JobStatusCompleted)

			if runs < 1 {
				t.Fatalf("expected at least one initial job execution")
			}
			if _, err := built.Engine.GetJobResult(ctx, restartKey); err == nil || !errors.Is(err, swf.ErrWorkflowNotDeterministic) && !strings.Contains(err.Error(), "workflow was not deterministic") {
				t.Fatalf("expected determinism error from restart result, got %v", err)
			}
		})
	}
}

func TestCancelledJobReturnsCancelledOutputAcrossBuiltInRuntimes(t *testing.T) {
	for _, harness := range swftest.BuiltInRuntimeHarnesses() {
		harness := harness
		t.Run(harness.Name, func(t *testing.T) {
			ws := swftest.MustWorkSet(t, pendingJob{})
			built := harness.New(t, ws)
			defer built.Shutdown(t)

			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()

			jobKey := swf.JobKey{TenantId: "tenant-cancel-" + harness.Name, JobId: "cancelled"}
			done := swftest.MustStartJobAsync(t, built.Engine, swf.StartJob{
				TenantId: jobKey.TenantId,
				JobType:  ws.JobWorker.Name(),
				JobID:    jobKey.JobId,
				Data:     swftest.NumberTaskData(1),
			})
			_ = swftest.WaitForTaskHandle(t, ctx, built.Engine, ws.JobWorker.Name(), "pending-task", []string{jobKey.TenantId})

			if err := built.Engine.CancelJob(ctx, swf.CancelJob{JobKey: jobKey}); err != nil {
				t.Fatalf("cancel job: %v", err)
			}
			if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
				t.Fatalf("async start: %v", err)
			}
			swftest.WaitForEngineStatus(t, ctx, built.Engine, jobKey, swf.JobStatusCancelled)
		})
	}
}
