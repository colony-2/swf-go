package engineconformance_test

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/workflow"
	jobdbtest "github.com/colony-2/jobdb/pkg/workflow/internal/jobdbtest"
)

type customIDJob struct{}

func (customIDJob) Name() string { return "custom-id-job" }
func (customIDJob) Run(_ workflow.JobContext, data jobdb.JobData) (jobdb.JobData, error) {
	return data, nil
}

type awaitChildJob struct {
	started chan<- struct{}
	release <-chan struct{}
}

func (awaitChildJob) Name() string { return "await-child" }
func (j awaitChildJob) Run(_ workflow.JobContext, data jobdb.JobData) (jobdb.JobData, error) {
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
func (j awaitParentJob) Run(ctx workflow.JobContext, data jobdb.JobData) (jobdb.JobData, error) {
	if err := ctx.AwaitJobs(j.childJobID); err != nil {
		return nil, err
	}
	return data, nil
}

type awaitTaskJob struct{}

func (awaitTaskJob) Name() string { return "await-task-parent" }
func (awaitTaskJob) Run(ctx workflow.JobContext, data jobdb.JobData) (jobdb.JobData, error) {
	return ctx.DoTask(jobdb.RunPolicy{}, "await-task", data)
}

type awaitTaskWorker struct {
	childJobID string
}

func (awaitTaskWorker) Name() string { return "await-task" }
func (t awaitTaskWorker) Run(ctx workflow.TaskContext, data jobdb.TaskData) (jobdb.TaskData, error) {
	if err := ctx.AwaitJobs(t.childJobID); err != nil {
		return nil, err
	}
	return data, nil
}

type pendingJob struct{}

func (pendingJob) Name() string { return "pending-job" }
func (pendingJob) Run(ctx workflow.JobContext, data jobdb.JobData) (jobdb.JobData, error) {
	return ctx.DoTask(jobdb.RunPolicy{}, "pending-task", data)
}

type transformPendingJob struct{}

func (transformPendingJob) Name() string { return "transform-pending-job" }
func (transformPendingJob) Run(ctx workflow.JobContext, data jobdb.JobData) (jobdb.JobData, error) {
	first, err := ctx.DoTask(jobdb.RunPolicy{}, jobdbtest.AddOneTaskName, data)
	if err != nil {
		return nil, err
	}
	n, err := numberFromTaskData(first)
	if err != nil {
		return nil, err
	}
	transformed := jobdbtest.NumberTaskData(n + 100)
	return ctx.DoTask(jobdb.RunPolicy{}, "pending-task", transformed)
}

type externalApprovalJob struct{}

func (externalApprovalJob) Name() string { return "external-approval-job" }
func (externalApprovalJob) Run(ctx workflow.JobContext, data jobdb.JobData) (jobdb.JobData, error) {
	return ctx.DoTask(jobdb.RunPolicy{}, "approval", data)
}

type retryJob struct {
	attempts int
}

func (retryJob) Name() string { return "retry-job" }
func (j *retryJob) Run(_ workflow.JobContext, data jobdb.JobData) (jobdb.JobData, error) {
	j.attempts++
	if j.attempts == 1 {
		return nil, jobdb.AppError{Payload: jobdb.AppErrorPayload{Message: "retry me", Level: "error"}}
	}
	return data, nil
}

type retryTaskJob struct{}

func (retryTaskJob) Name() string { return "retry-task-job" }
func (retryTaskJob) Run(ctx workflow.JobContext, data jobdb.JobData) (jobdb.JobData, error) {
	return ctx.DoTask(jobdb.RunPolicy{
		Retry: jobdb.RetryPolicy{
			MaximumAttempts:    3,
			BackoffCoefficient: 1,
		},
	}, "retry-task", data)
}

type retryTask struct {
	attempts int
}

func (retryTask) Name() string { return "retry-task" }
func (t *retryTask) Run(_ workflow.TaskContext, data jobdb.TaskData) (jobdb.TaskData, error) {
	t.attempts++
	if t.attempts == 1 {
		return nil, jobdb.AppError{Payload: jobdb.AppErrorPayload{Message: "retry task", Level: "error"}}
	}
	return data, nil
}

func numberFromTaskData(data jobdb.TaskData) (int, error) {
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
	ws := jobdbtest.MustWorkSet(t, customIDJob{})

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
				JobID:    "custom-job-id",
				Data:     jobdbtest.NumberTaskData(7),
			})
			if err != nil {
				t.Fatalf("start job: %v", err)
			}
			if jobKey.JobId != "custom-job-id" {
				t.Fatalf("unexpected job key %+v", jobKey)
			}
			jobdbtest.WaitForEngineStatus(t, ctx, built.Engine, jobKey, jobdb.JobStatusCompleted)
		})
	}
}

func TestRestartValidationAcrossBuiltInRuntimes(t *testing.T) {
	for _, harness := range jobdbtest.BuiltInRuntimeHarnesses() {
		harness := harness
		t.Run(harness.Name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()

			t.Run("negative-last-step", func(t *testing.T) {
				ws := jobdbtest.MustWorkSet(t, customIDJob{})
				built := harness.New(t, ws)
				defer built.Shutdown(t)

				if _, err := built.Engine.SubmitRestartJob(ctx, jobdb.SubmitRestartJob{
					PriorJobKey:    jobdb.JobKey{TenantId: "tenant", JobId: "missing"},
					LastStepToKeep: -1,
				}); err == nil {
					t.Fatal("expected restart to fail")
				}
			})

			t.Run("missing-next-chapter", func(t *testing.T) {
				ws := jobdbtest.MustWorkSet(t, customIDJob{})
				built := harness.New(t, ws)
				defer built.Shutdown(t)

				jobKey, err := built.Engine.SubmitJob(ctx, jobdb.SubmitJob{
					TenantId: built.WorkerTenantID,
					JobType:  ws.JobWorker.Name(),
					Data:     jobdbtest.NumberTaskData(1),
				})
				if err != nil {
					t.Fatalf("start job: %v", err)
				}
				jobdbtest.WaitForEngineStatus(t, ctx, built.Engine, jobKey, jobdb.JobStatusCompleted)

				if _, err := built.Engine.SubmitRestartJob(ctx, jobdb.SubmitRestartJob{
					PriorJobKey:    jobKey,
					LastStepToKeep: 1,
				}); err == nil {
					t.Fatal("expected restart to fail when next chapter is missing")
				}
			})

			t.Run("retry-boundary", func(t *testing.T) {
				job := &retryTaskJob{}
				task := &retryTask{}
				ws := jobdbtest.MustWorkSet(t, job, task)
				built := harness.New(t, ws)
				defer built.Shutdown(t)

				jobKey, err := built.Engine.SubmitJob(ctx, jobdb.SubmitJob{
					TenantId: built.WorkerTenantID,
					JobType:  job.Name(),
					Data:     jobdbtest.NumberTaskData(1),
				})
				if err != nil {
					t.Fatalf("start job: %v", err)
				}
				jobdbtest.WaitForEngineStatus(t, ctx, built.Engine, jobKey, jobdb.JobStatusCompleted)

				if _, err := built.Engine.SubmitRestartJob(ctx, jobdb.SubmitRestartJob{
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
	for _, harness := range jobdbtest.BuiltInRuntimeHarnesses() {
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

			childJobID := "child"
			parentJobID := "parent"
			childWorker := awaitChildJob{started: childStarted, release: releaseChild}
			parentWorker := awaitParentJob{childJobID: childJobID}
			built := harness.New(t, jobdbtest.MustWorkSet(t, childWorker), jobdbtest.MustWorkSet(t, parentWorker))
			defer built.Shutdown(t)
			tenantID := built.WorkerTenantID

			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()

			childDone := jobdbtest.MustStartJobAsync(t, built.Engine, jobdb.SubmitJob{
				TenantId: tenantID,
				JobType:  childWorker.Name(),
				JobID:    childJobID,
				Data:     jobdbtest.NumberTaskData(1),
			})
			select {
			case <-childStarted:
			case <-ctx.Done():
				t.Fatalf("child did not start: %v", ctx.Err())
			}

			parentDone := jobdbtest.MustStartJobAsync(t, built.Engine, jobdb.SubmitJob{
				TenantId: tenantID,
				JobType:  parentWorker.Name(),
				JobID:    parentJobID,
				Data:     jobdbtest.NumberTaskData(2),
			})

			jobdbtest.WaitForEngineStatus(t, ctx, built.Engine, jobdb.JobKey{TenantId: tenantID, JobId: parentJobID}, jobdb.JobStatusPendingJobs)
			resp, err := built.Engine.ListJobs(ctx, jobdb.ListJobsRequest{TenantIds: []string{tenantID}, JobKeys: []jobdb.JobKey{{TenantId: tenantID, JobId: parentJobID}}})
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
			jobdbtest.WaitForEngineStatus(t, ctx, built.Engine, jobdb.JobKey{TenantId: tenantID, JobId: parentJobID}, jobdb.JobStatusCompleted)
		})
	}
}

func TestTaskContextAwaitJobsAcrossBuiltInRuntimes(t *testing.T) {
	for _, harness := range jobdbtest.BuiltInRuntimeHarnesses() {
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

			childJobID := "child"
			childWorker := awaitChildJob{started: childStarted, release: releaseChild}
			taskWorker := awaitTaskWorker{childJobID: childJobID}
			parentWorker := awaitTaskJob{}
			built := harness.New(t, jobdbtest.MustWorkSet(t, childWorker), jobdbtest.MustWorkSet(t, parentWorker, taskWorker))
			defer built.Shutdown(t)
			tenantID := built.WorkerTenantID

			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()

			childDone := jobdbtest.MustStartJobAsync(t, built.Engine, jobdb.SubmitJob{
				TenantId: tenantID,
				JobType:  childWorker.Name(),
				JobID:    childJobID,
				Data:     jobdbtest.NumberTaskData(1),
			})
			select {
			case <-childStarted:
			case <-ctx.Done():
				t.Fatalf("child did not start: %v", ctx.Err())
			}

			parentKey := jobdb.JobKey{TenantId: tenantID, JobId: "parent"}
			parentDone := jobdbtest.MustStartJobAsync(t, built.Engine, jobdb.SubmitJob{
				TenantId: tenantID,
				JobType:  parentWorker.Name(),
				JobID:    parentKey.JobId,
				Data:     jobdbtest.NumberTaskData(2),
			})

			jobdbtest.WaitForEngineStatus(t, ctx, built.Engine, parentKey, jobdb.JobStatusPendingJobs)
			close(releaseChild)
			if err := <-childDone; err != nil {
				t.Fatalf("child failed: %v", err)
			}
			if err := <-parentDone; err != nil {
				t.Fatalf("parent failed: %v", err)
			}
			jobdbtest.WaitForEngineStatus(t, ctx, built.Engine, parentKey, jobdb.JobStatusCompleted)
		})
	}
}

func TestPendingTaskHandlesAcrossBuiltInRuntimes(t *testing.T) {
	for _, harness := range jobdbtest.BuiltInRuntimeHarnesses() {
		harness := harness
		t.Run(harness.Name, func(t *testing.T) {
			ws := jobdbtest.MustWorkSet(t, transformPendingJob{}, jobdbtest.AddOneTask{})
			built := harness.New(t, ws)
			defer built.Shutdown(t)

			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()

			jobKey := jobdb.JobKey{TenantId: built.WorkerTenantID, JobId: "pending"}
			done := jobdbtest.MustStartJobAsync(t, built.Engine, jobdb.SubmitJob{
				TenantId: jobKey.TenantId,
				JobType:  ws.JobWorker.Name(),
				JobID:    jobKey.JobId,
				Data:     jobdbtest.NumberTaskData(10),
			})

			handle := jobdbtest.WaitForTaskHandle(t, ctx, built.Engine, ws.JobWorker.Name(), "pending-task", []string{jobKey.TenantId})
			waiting, err := built.Engine.GetWaitingTask(ctx, jobKey)
			if err != nil {
				t.Fatalf("get waiting task: %v", err)
			}
			data, err := waiting.Data()
			if err != nil {
				t.Fatalf("waiting data: %v", err)
			}
			if got := jobdbtest.MustDecodeNumberTaskData(t, data); got != 11 {
				t.Fatalf("expected previous step output 11, got %d", got)
			}

			resp, err := built.Engine.ListJobs(ctx, jobdb.ListJobsRequest{
				TenantIds: []string{jobKey.TenantId},
				JobKeys:   []jobdb.JobKey{jobKey},
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
			if resp.Jobs[0].NextNeed == nil || *resp.Jobs[0].NextNeed != ws.JobWorker.Name()+":pending-task" {
				t.Fatalf("unexpected next need %+v", resp.Jobs[0].NextNeed)
			}
			if resp.Jobs[0].TaskWaitNext == nil || *resp.Jobs[0].TaskWaitNext != ws.JobWorker.Name() {
				t.Fatalf("unexpected task wait resume need %+v", resp.Jobs[0].TaskWaitNext)
			}
			if resp.Jobs[0].TaskWaitInputHash == nil || *resp.Jobs[0].TaskWaitInputHash == "" {
				t.Fatalf("expected task wait input hash")
			}

			if err := handle.Finish(ctx, jobdbtest.NumberTaskData(200)); err != nil {
				t.Fatalf("finish waiting task: %v", err)
			}
			if err := <-done; err != nil {
				t.Fatalf("start job async: %v", err)
			}
			jobdbtest.WaitForEngineStatus(t, ctx, built.Engine, jobKey, jobdb.JobStatusCompleted)
		})
	}
}

func TestReplayAfterExternalTaskCompletionAcrossBuiltInRuntimes(t *testing.T) {
	for _, harness := range jobdbtest.BuiltInRuntimeHarnesses() {
		harness := harness
		t.Run(harness.Name, func(t *testing.T) {
			ws := jobdbtest.MustWorkSet(t, externalApprovalJob{})
			built := harness.New(t, ws)
			defer built.Shutdown(t)

			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()

			jobKey := jobdb.JobKey{TenantId: built.WorkerTenantID, JobId: "approval"}
			done := jobdbtest.MustStartJobAsync(t, built.Engine, jobdb.SubmitJob{
				TenantId: jobKey.TenantId,
				JobType:  ws.JobWorker.Name(),
				JobID:    jobKey.JobId,
				Data:     jobdbtest.NumberTaskData(42),
				RunPolicy: jobdb.RunPolicy{
					Retry: jobdb.RetryPolicy{MaximumAttempts: 3, BackoffCoefficient: 1},
				},
			})

			handle := jobdbtest.WaitForTaskHandle(t, ctx, built.Engine, ws.JobWorker.Name(), "approval", []string{jobKey.TenantId})
			if err := handle.Finish(ctx, jobdbtest.NumberTaskData(42)); err != nil {
				t.Fatalf("finish approval: %v", err)
			}
			if err := <-done; err != nil {
				t.Fatalf("start job async: %v", err)
			}
			jobdbtest.WaitForEngineStatus(t, ctx, built.Engine, jobKey, jobdb.JobStatusCompleted)

			replayed, err := built.Engine.ReplayJobRun(ctx, workflow.ReplayRunRequest{JobKey: jobKey})
			if err != nil {
				t.Fatalf("replay job run: %v", err)
			}
			if got := jobdbtest.MustDecodeNumberTaskData(t, replayed); got != 42 {
				t.Fatalf("unexpected replayed output %d", got)
			}
		})
	}
}

func TestGetJobRunRetryRepresentationOnDirectRuntime(t *testing.T) {
	for _, harness := range jobdbtest.BuiltInRuntimeHarnesses() {
		harness := harness
		if harness.Name != "direct" {
			continue
		}
		t.Run(harness.Name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()

			t.Run("job-retry", func(t *testing.T) {
				job := &retryJob{}
				ws := jobdbtest.MustWorkSet(t, job)
				built := harness.New(t, ws)
				defer built.Shutdown(t)

				jobKey, err := built.Engine.SubmitJob(ctx, jobdb.SubmitJob{
					TenantId: built.WorkerTenantID,
					JobType:  job.Name(),
					Data:     jobdbtest.NumberTaskData(1),
					RunPolicy: jobdb.RunPolicy{
						Retry: jobdb.RetryPolicy{MaximumAttempts: 3, BackoffCoefficient: 1},
					},
				})
				if err != nil {
					t.Fatalf("start job: %v", err)
				}
				jobdbtest.WaitForEngineStatus(t, ctx, built.Engine, jobKey, jobdb.JobStatusCompleted)

				resp, err := built.Engine.GetJobRun(ctx, jobdb.GetJobRunRequest{JobKey: jobKey, IncludeOutputs: true})
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
				ws := jobdbtest.MustWorkSet(t, job, task)
				built := harness.New(t, ws)
				defer built.Shutdown(t)

				jobKey, err := built.Engine.SubmitJob(ctx, jobdb.SubmitJob{
					TenantId: built.WorkerTenantID,
					JobType:  job.Name(),
					Data:     jobdbtest.NumberTaskData(1),
				})
				if err != nil {
					t.Fatalf("start job: %v", err)
				}
				jobdbtest.WaitForEngineStatus(t, ctx, built.Engine, jobKey, jobdb.JobStatusCompleted)

				resp, err := built.Engine.GetJobRun(ctx, jobdb.GetJobRunRequest{JobKey: jobKey, IncludeOutputs: true})
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
func (echoTask) Run(_ workflow.TaskContext, data jobdb.TaskData) (jobdb.TaskData, error) {
	return data, nil
}

type singleEchoJob struct {
	runs *int
}

func (singleEchoJob) Name() string { return "single-echo" }
func (j singleEchoJob) Run(ctx workflow.JobContext, data jobdb.JobData) (jobdb.JobData, error) {
	if j.runs != nil {
		*j.runs = *j.runs + 1
	}
	return ctx.DoTask(jobdb.RunPolicy{}, "echo", data)
}

func TestGetJobRunSynthesizedNextAttemptOnDirectRuntime(t *testing.T) {
	for _, harness := range jobdbtest.BuiltInRuntimeHarnesses() {
		harness := harness
		if harness.Name != "direct" {
			continue
		}
		t.Run(harness.Name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()

			job := &retryJob{}
			ws := jobdbtest.MustWorkSet(t, job)
			built := harness.New(t, ws)
			defer built.Shutdown(t)

			jobKey, err := built.Engine.SubmitJob(ctx, jobdb.SubmitJob{
				TenantId: built.WorkerTenantID,
				JobType:  job.Name(),
				Data:     jobdbtest.NumberTaskData(1),
				RunPolicy: jobdb.RunPolicy{
					Retry: jobdb.RetryPolicy{
						MaximumAttempts:    2,
						BackoffCoefficient: 1,
						InitialInterval:    jobdb.Duration(5 * time.Second),
					},
				},
			})
			if err != nil {
				t.Fatalf("start job: %v", err)
			}

			var resp jobdb.GetJobRunResponse
			deadline := time.Now().Add(2 * time.Second)
			for time.Now().Before(deadline) {
				resp, err = built.Engine.GetJobRun(ctx, jobdb.GetJobRunRequest{JobKey: jobKey})
				if err != nil {
					t.Fatalf("get job run: %v", err)
				}
				if len(resp.Attempts) > 0 && resp.Attempts[0].Outcome.Status == jobdb.TaskOutcomeStatusFailed {
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
			if resp.Job.Status == jobdb.JobStatusCompleted {
				t.Fatalf("expected job to remain pending during retry backoff")
			}
		})
	}
}

func TestRestartWithExtraOutputDeterminismErrorOnDirectRuntime(t *testing.T) {
	for _, harness := range jobdbtest.BuiltInRuntimeHarnesses() {
		harness := harness
		if harness.Name != "direct" {
			continue
		}
		t.Run(harness.Name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()

			runs := 0
			ws := jobdbtest.MustWorkSet(t, singleEchoJob{runs: &runs}, echoTask{})
			built := harness.New(t, ws)
			defer built.Shutdown(t)

			origInput := jobdb.NewTaskDataOrPanic(map[string]string{"hello": "world"})
			jobKey, err := built.Engine.SubmitJob(ctx, jobdb.SubmitJob{
				TenantId: built.WorkerTenantID,
				JobType:  ws.JobWorker.Name(),
				Data:     origInput,
			})
			if err != nil {
				t.Fatalf("start job: %v", err)
			}
			jobdbtest.WaitForEngineStatus(t, ctx, built.Engine, jobKey, jobdb.JobStatusCompleted)

			newInput := jobdb.NewTaskDataOrPanic(map[string]string{"hello": "again"})
			restartKey, err := built.Engine.SubmitRestartJob(ctx, jobdb.SubmitRestartJob{
				PriorJobKey:     jobKey,
				LastStepToKeep:  0,
				ExtraTaskInput:  newInput,
				ExtraTaskOutput: newInput,
			})
			if err != nil {
				t.Fatalf("restart job: %v", err)
			}
			jobdbtest.WaitForEngineStatus(t, ctx, built.Engine, restartKey, jobdb.JobStatusCompleted)

			if runs < 1 {
				t.Fatalf("expected at least one initial job execution")
			}
			if _, err := jobResultForTest(built.Engine, ctx, restartKey); err == nil || !errors.Is(err, jobdb.ErrWorkflowNotDeterministic) && !strings.Contains(err.Error(), "workflow was not deterministic") {
				t.Fatalf("expected determinism error from restart result, got %v", err)
			}
		})
	}
}

func TestCancelledJobReturnsCancelledOutputAcrossBuiltInRuntimes(t *testing.T) {
	for _, harness := range jobdbtest.BuiltInRuntimeHarnesses() {
		harness := harness
		t.Run(harness.Name, func(t *testing.T) {
			ws := jobdbtest.MustWorkSet(t, pendingJob{})
			built := harness.New(t, ws)
			defer built.Shutdown(t)

			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()

			jobKey := jobdb.JobKey{TenantId: built.WorkerTenantID, JobId: "cancelled"}
			done := jobdbtest.MustStartJobAsync(t, built.Engine, jobdb.SubmitJob{
				TenantId: jobKey.TenantId,
				JobType:  ws.JobWorker.Name(),
				JobID:    jobKey.JobId,
				Data:     jobdbtest.NumberTaskData(1),
			})
			_ = jobdbtest.WaitForTaskHandle(t, ctx, built.Engine, ws.JobWorker.Name(), "pending-task", []string{jobKey.TenantId})

			if err := built.Engine.CancelJob(ctx, jobdb.CancelJob{JobKey: jobKey}); err != nil {
				t.Fatalf("cancel job: %v", err)
			}
			if err := <-done; err != nil && !errors.Is(err, context.Canceled) {
				t.Fatalf("async start: %v", err)
			}
			jobdbtest.WaitForEngineStatus(t, ctx, built.Engine, jobKey, jobdb.JobStatusCancelled)
		})
	}
}

func TestEngineMetadataFilteredWorkersAndManualJobLeaseAcrossBuiltInRuntimes(t *testing.T) {
	metaFilter, err := jobdb.Metadata().EqualFilter("queue", "blue")
	if err != nil {
		t.Fatalf("build metadata filter: %v", err)
	}
	ws := jobdbtest.MustWorkSetWithOptions(t, customIDJob{}, workflow.WorkRegistrationOptions{MetadataFilter: metaFilter})

	for _, harness := range jobdbtest.BuiltInRuntimeHarnesses() {
		harness := harness
		t.Run(harness.Name, func(t *testing.T) {
			built := harness.New(t, ws)
			defer built.Shutdown(t)

			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()

			blueKey, err := built.Engine.SubmitJob(ctx, jobdb.SubmitJob{
				TenantId: built.WorkerTenantID,
				JobType:  ws.JobWorker.Name(),
				JobID:    "blue",
				Data:     jobdbtest.NumberTaskData(1),
				Metadata: json.RawMessage(`{"queue":"blue"}`),
			})
			if err != nil {
				t.Fatalf("submit blue job: %v", err)
			}
			greenKey, err := built.Engine.SubmitJob(ctx, jobdb.SubmitJob{
				TenantId: blueKey.TenantId,
				JobType:  ws.JobWorker.Name(),
				JobID:    "green",
				Data:     jobdbtest.NumberTaskData(2),
				Metadata: json.RawMessage(`{"queue":"green"}`),
			})
			if err != nil {
				t.Fatalf("submit green job: %v", err)
			}

			jobdbtest.WaitForEngineStatus(t, ctx, built.Engine, blueKey, jobdb.JobStatusCompleted)

			greenJob, err := built.Engine.GetJob(ctx, greenKey)
			if err != nil {
				t.Fatalf("get green job: %v", err)
			}
			if greenJob.Status == jobdb.JobStatusCompleted {
				t.Fatalf("expected green job to remain unprocessed by metadata-filtered worker")
			}

			lease, err := built.Engine.GetJobLease(ctx, jobdb.GetJobLeaseRequest{
				JobKey:        greenKey,
				WorkerID:      "manual-worker",
				Capabilities:  []string{ws.JobWorker.Name()},
				LeaseDuration: time.Second,
			})
			if err != nil {
				t.Fatalf("manual job lease: %v", err)
			}
			if lease == nil {
				t.Fatal("expected manual job lease")
			}
			if lease.Job().JobKey != greenKey {
				t.Fatalf("unexpected leased job %+v", lease.Job().JobKey)
			}
			completeLeaseForTest(t, ctx, lease, 1)
			jobdbtest.WaitForEngineStatus(t, ctx, built.Engine, greenKey, jobdb.JobStatusCompleted)
		})
	}
}

func TestFindTasksWaitingWithMetadataFilterAcrossBuiltInRuntimes(t *testing.T) {
	ws := jobdbtest.MustWorkSet(t, pendingJob{})
	metaFilter, err := jobdb.Metadata().EqualFilter("rank", 1)
	if err != nil {
		t.Fatalf("build metadata filter: %v", err)
	}

	for _, harness := range jobdbtest.BuiltInRuntimeHarnesses() {
		harness := harness
		t.Run(harness.Name, func(t *testing.T) {
			built := harness.New(t, ws)
			defer built.Shutdown(t)

			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()

			blueKey, err := built.Engine.SubmitJob(ctx, jobdb.SubmitJob{
				TenantId: built.WorkerTenantID,
				JobType:  ws.JobWorker.Name(),
				JobID:    "blue-pending",
				Data:     jobdbtest.NumberTaskData(3),
				Metadata: json.RawMessage(`{"rank":1}`),
			})
			if err != nil {
				t.Fatalf("submit blue pending job: %v", err)
			}
			if _, err := built.Engine.SubmitJob(ctx, jobdb.SubmitJob{
				TenantId: blueKey.TenantId,
				JobType:  ws.JobWorker.Name(),
				JobID:    "green-pending",
				Data:     jobdbtest.NumberTaskData(4),
				Metadata: json.RawMessage(`{"rank":2}`),
			}); err != nil {
				t.Fatalf("submit green pending job: %v", err)
			}

			var handles []workflow.TaskHandle
			deadline := time.Now().Add(5 * time.Second)
			for time.Now().Before(deadline) {
				handles, err = built.Engine.FindTasksWaiting(ctx, workflow.FindTasksWaitingRequest{
					JobType:        ws.JobWorker.Name(),
					TaskType:       "pending-task",
					TenantIds:      []string{blueKey.TenantId},
					MetadataFilter: metaFilter,
					Limit:          1,
				})
				if err != nil {
					t.Fatalf("find tasks waiting: %v", err)
				}
				if len(handles) > 0 {
					break
				}
				time.Sleep(20 * time.Millisecond)
			}
			if len(handles) != 1 {
				t.Fatalf("expected 1 filtered pending handle, got %d", len(handles))
			}
			if handles[0].JobKey() != blueKey {
				t.Fatalf("unexpected filtered handle job %+v", handles[0].JobKey())
			}
		})
	}
}
