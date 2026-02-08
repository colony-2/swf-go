package impl

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/colony-2/swf-go/pkg/swf"
)

func TestGetJobRunCompleted(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	jobCounter := atomic.Int32{}
	jobWorker := &taskCallingJobWorker{
		name:    "job-run-complete",
		counter: &jobCounter,
	}
	taskWorker := &echoTaskWorker{}
	ws := initWorkset(jobWorker, taskWorker)
	jobWorker.workset = ws

	embedded, err := StartEmbeddedEngine(ctx, nil)
	if err != nil {
		t.Fatalf("start embedded engine: %v", err)
	}
	defer embedded.Shutdown()

	engine := embedded.SWFEngine.(*swfEngineImpl)
	if err := engine.RegisterWorkers(ws); err != nil {
		t.Fatalf("register workers: %v", err)
	}
	go engine.Run(ctx)

	jobInput := swf.NewTaskDataOrPanic(map[string]string{"job": "input"})
	jobKey, err := engine.StartJob(ctx, swf.StartJob{
		TenantId: "test-tenant",
		JobType:  jobWorker.Name(),
		Data:     jobInput,
	})
	if err != nil {
		t.Fatalf("start job: %v", err)
	}

	if err := swf.WaitForJobToComplete(ctx, 10*time.Second, jobKey, engine); err != nil {
		t.Fatalf("wait for job complete: %v", err)
	}

	resp, err := engine.GetJobRun(ctx, swf.GetJobRunRequest{JobKey: jobKey})
	if err != nil {
		t.Fatalf("get job run: %v", err)
	}
	if resp.Job.JobType != jobWorker.Name() {
		t.Fatalf("unexpected job type: %s", resp.Job.JobType)
	}
	if resp.Start.Input == nil {
		t.Fatalf("missing start input")
	}
	if got := extractFieldString(t, resp.Start.Input.Data, "job"); got != "input" {
		t.Fatalf("unexpected start input: %s", got)
	}
	if len(resp.Tasks) != 2 {
		t.Fatalf("expected 2 task runs, got %d", len(resp.Tasks))
	}
	if resp.Tasks[0].TaskType != "echo" || resp.Tasks[1].TaskType != "echo" {
		t.Fatalf("unexpected task types: %s, %s", resp.Tasks[0].TaskType, resp.Tasks[1].TaskType)
	}
	if got := extractFieldString(t, resp.Tasks[0].Attempts[0].Output.Data, "task"); got != "1" {
		t.Fatalf("unexpected task 1 output: %s", got)
	}
	if got := extractFieldString(t, resp.Tasks[1].Attempts[0].Output.Data, "task"); got != "2" {
		t.Fatalf("unexpected task 2 output: %s", got)
	}
	if resp.Result == nil || resp.Result.Output == nil {
		t.Fatalf("missing job result")
	}
	if got := extractFieldString(t, resp.Result.Output.Data, "job"); got != "input" {
		t.Fatalf("unexpected job result output: %s", got)
	}
	if len(resp.JobAttempts) != 1 {
		t.Fatalf("expected 1 job attempt, got %d", len(resp.JobAttempts))
	}

	out, err := resp.GetOutput(engine, jobKey.TenantId)
	if err != nil {
		t.Fatalf("GetOutput failed: %v", err)
	}
	if got := extractFieldString(t, mustData(t, out), "job"); got != "input" {
		t.Fatalf("unexpected job output: %s", got)
	}
}

func TestGetJobRunPendingRuntime(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	taskType := "missing-task"
	jobWorker := &taskCallingJobWorkerSimple{
		name:     "job-run-pending",
		taskType: taskType,
	}
	jobWorker.workset = initWorkset(jobWorker)

	embedded, err := StartEmbeddedEngine(ctx, nil)
	if err != nil {
		t.Fatalf("start embedded engine: %v", err)
	}
	defer embedded.Shutdown()

	engine := embedded.SWFEngine.(*swfEngineImpl)
	if err := engine.RegisterWorkers(jobWorker.workset); err != nil {
		t.Fatalf("register workers: %v", err)
	}
	go engine.Run(ctx)

	jobInput := swf.NewTaskDataOrPanic(map[string]string{"form": "hello"})
	jobKey, err := engine.StartJob(ctx, swf.StartJob{
		TenantId: "test-tenant",
		JobType:  jobWorker.Name(),
		Data:     jobInput,
	})
	if err != nil {
		t.Fatalf("start job: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		handles, err := engine.FindTasksWaitingForCapability(ctx, jobWorker.Name(), taskType, []string{jobKey.TenantId})
		if err != nil {
			t.Fatalf("find waiting tasks: %v", err)
		}
		if len(handles) > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	resp, err := engine.GetJobRun(ctx, swf.GetJobRunRequest{
		JobKey:               jobKey,
		IncludeInputs:        true,
		IncludeAttemptInputs: true,
	})
	if err != nil {
		t.Fatalf("get job run: %v", err)
	}
	if resp.Result != nil {
		t.Fatalf("expected nil result for pending job")
	}
	if len(resp.Tasks) != 1 {
		t.Fatalf("expected 1 task run, got %d", len(resp.Tasks))
	}
	task := resp.Tasks[0]
	expectedCapability := jobWorker.Name() + ":" + taskType
	if task.TaskType != expectedCapability {
		t.Fatalf("unexpected task type: %s", task.TaskType)
	}
	if len(task.Attempts) != 1 {
		t.Fatalf("expected 1 attempt, got %d", len(task.Attempts))
	}
	attempt := task.Attempts[0]
	if attempt.Ordinal != 1 {
		t.Fatalf("expected runtime ordinal 1, got %d", attempt.Ordinal)
	}
	if attempt.State == "" {
		t.Fatalf("expected runtime state")
	}
	if attempt.Runtime == nil || attempt.Runtime.NextNeed == nil {
		t.Fatalf("expected runtime next_need")
	}
	if *attempt.Runtime.NextNeed != expectedCapability {
		t.Fatalf("unexpected next_need: %s", *attempt.Runtime.NextNeed)
	}
	if attempt.Input == nil {
		t.Fatalf("expected runtime input")
	}
	if got := extractFieldString(t, attempt.Input.Data, "form"); got != "hello" {
		t.Fatalf("unexpected runtime input: %s", got)
	}

	if _, err := resp.GetOutput(engine, jobKey.TenantId); !errors.Is(err, swf.ErrJobNotComplete) {
		t.Fatalf("expected ErrJobNotComplete, got %v", err)
	}
}

func TestGetJobRunGetOutputFailed(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	jobWorker := &failingJobWorker{name: "job-run-failed"}
	ws := initWorkset(jobWorker)

	embedded, err := StartEmbeddedEngine(ctx, nil)
	if err != nil {
		t.Fatalf("start embedded engine: %v", err)
	}
	defer embedded.Shutdown()

	engine := embedded.SWFEngine.(*swfEngineImpl)
	if err := engine.RegisterWorkers(ws); err != nil {
		t.Fatalf("register workers: %v", err)
	}
	go engine.Run(ctx)

	jobInput := swf.NewTaskDataOrPanic(map[string]string{"job": "input"})
	jobKey, err := engine.StartJob(ctx, swf.StartJob{
		TenantId: "test-tenant",
		JobType:  jobWorker.Name(),
		Data:     jobInput,
	})
	if err != nil {
		t.Fatalf("start job: %v", err)
	}

	if err := swf.WaitForJobToComplete(ctx, 10*time.Second, jobKey, engine); err != nil {
		t.Fatalf("wait for job complete: %v", err)
	}

	resp, err := engine.GetJobRun(ctx, swf.GetJobRunRequest{JobKey: jobKey})
	if err != nil {
		t.Fatalf("get job run: %v", err)
	}
	if _, err := resp.GetOutput(engine, jobKey.TenantId); !errors.Is(err, swf.ErrJobFailed) {
		t.Fatalf("expected ErrJobFailed, got %v", err)
	} else if err == nil || !strings.Contains(err.Error(), "intentional failure") {
		t.Fatalf("expected error message to include failure text, got %v", err)
	}
}

func TestGetJobRunGetOutputCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	taskType := "missing-task"
	jobWorker := &taskCallingJobWorkerSimple{
		name:     "job-run-cancelled",
		taskType: taskType,
	}
	jobWorker.workset = initWorkset(jobWorker)

	embedded, err := StartEmbeddedEngine(ctx, nil)
	if err != nil {
		t.Fatalf("start embedded engine: %v", err)
	}
	defer embedded.Shutdown()

	engine := embedded.SWFEngine.(*swfEngineImpl)
	if err := engine.RegisterWorkers(jobWorker.workset); err != nil {
		t.Fatalf("register workers: %v", err)
	}
	go engine.Run(ctx)

	jobInput := swf.NewTaskDataOrPanic(map[string]string{"form": "hello"})
	jobKey, err := engine.StartJob(ctx, swf.StartJob{
		TenantId: "test-tenant",
		JobType:  jobWorker.Name(),
		Data:     jobInput,
	})
	if err != nil {
		t.Fatalf("start job: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		handles, err := engine.FindTasksWaitingForCapability(ctx, jobWorker.Name(), taskType, []string{jobKey.TenantId})
		if err != nil {
			t.Fatalf("find waiting tasks: %v", err)
		}
		if len(handles) > 0 {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	if err := engine.CancelJob(ctx, swf.CancelJob{JobKey: jobKey}); err != nil {
		t.Fatalf("cancel job: %v", err)
	}

	deadline = time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		status, err := engine.CheckJobStatus(ctx, jobKey)
		if err != nil {
			t.Fatalf("check job status: %v", err)
		}
		if status == swf.JobStatusCancelled {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	resp, err := engine.GetJobRun(ctx, swf.GetJobRunRequest{JobKey: jobKey})
	if err != nil {
		t.Fatalf("get job run: %v", err)
	}
	if _, err := resp.GetOutput(engine, jobKey.TenantId); !errors.Is(err, swf.ErrJobCancelled) {
		t.Fatalf("expected ErrJobCancelled, got %v", err)
	}
}

func TestGetJobRunJobRetryRepresentation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var attemptCount atomic.Int32
	jobWorker := &failThenSucceedJobWorker{
		name:         "job-run-retry",
		failAttempts: 1,
		counter:      &attemptCount,
	}
	jobWorker.workset = initWorkset(jobWorker)

	embedded, err := StartEmbeddedEngine(ctx, nil)
	if err != nil {
		t.Fatalf("start embedded engine: %v", err)
	}
	defer embedded.Shutdown()

	engine := embedded.SWFEngine.(*swfEngineImpl)
	if err := engine.RegisterWorkers(jobWorker.workset); err != nil {
		t.Fatalf("register workers: %v", err)
	}
	go engine.Run(ctx)

	jobKey, err := engine.StartJob(ctx, swf.StartJob{
		TenantId: "test-tenant",
		JobType:  jobWorker.Name(),
		Data:     swf.NewTaskDataOrPanic(map[string]string{"job": "retry"}),
		RunPolicy: swf.RunPolicy{
			Retry: swf.RetryPolicy{
				MaximumAttempts:    2,
				BackoffCoefficient: 1.0,
				InitialInterval:    swf.Duration(10 * time.Millisecond),
			},
		},
	})
	if err != nil {
		t.Fatalf("start job: %v", err)
	}

	if err := swf.WaitForJobToComplete(ctx, 10*time.Second, jobKey, engine); err != nil {
		t.Fatalf("wait for job complete: %v", err)
	}
	if attemptCount.Load() != 2 {
		t.Fatalf("expected 2 attempts, got %d", attemptCount.Load())
	}

	resp, err := engine.GetJobRun(ctx, swf.GetJobRunRequest{JobKey: jobKey})
	if err != nil {
		t.Fatalf("get job run: %v", err)
	}
	if len(resp.JobAttempts) != 2 {
		t.Fatalf("expected 2 job attempts, got %d", len(resp.JobAttempts))
	}
	if resp.JobAttempts[0].Attempt != 1 || resp.JobAttempts[1].Attempt != 2 {
		t.Fatalf("unexpected attempt numbers: %d, %d", resp.JobAttempts[0].Attempt, resp.JobAttempts[1].Attempt)
	}
	if resp.JobAttempts[0].Outcome.Status != swf.TaskOutcomeStatusFailed {
		t.Fatalf("expected first attempt to fail, got %s", resp.JobAttempts[0].Outcome.Status)
	}
	if resp.JobAttempts[1].Outcome.Status != swf.TaskOutcomeStatusSucceeded {
		t.Fatalf("expected second attempt to succeed, got %s", resp.JobAttempts[1].Outcome.Status)
	}
	if resp.Result == nil {
		t.Fatalf("expected job result")
	}
	if resp.Result.Attempt != 2 {
		t.Fatalf("expected result attempt 2, got %d", resp.Result.Attempt)
	}
}

func TestGetJobRunTaskRetryRepresentation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var taskAttemptCount atomic.Int32
	taskWorker := &failThenSucceedTaskWorker{
		name:         "job-run-retry-task",
		failAttempts: 1,
		counter:      &taskAttemptCount,
	}
	jobWorker := &taskCallingJobWorkerSimple{
		name:     "job-run-retry-task-job",
		taskType: taskWorker.Name(),
		taskPolicy: swf.RunPolicy{
			Retry: swf.RetryPolicy{
				MaximumAttempts:    2,
				BackoffCoefficient: 1.0,
				InitialInterval:    swf.Duration(10 * time.Millisecond),
			},
		},
	}
	ws := initWorkset(jobWorker, taskWorker)
	jobWorker.workset = ws

	embedded, err := StartEmbeddedEngine(ctx, nil)
	if err != nil {
		t.Fatalf("start embedded engine: %v", err)
	}
	defer embedded.Shutdown()

	engine := embedded.SWFEngine.(*swfEngineImpl)
	if err := engine.RegisterWorkers(ws); err != nil {
		t.Fatalf("register workers: %v", err)
	}
	go engine.Run(ctx)

	jobKey, err := engine.StartJob(ctx, swf.StartJob{
		TenantId: "test-tenant",
		JobType:  jobWorker.Name(),
		Data:     swf.NewTaskDataOrPanic(map[string]string{"task": "retry"}),
	})
	if err != nil {
		t.Fatalf("start job: %v", err)
	}

	if err := swf.WaitForJobToComplete(ctx, 10*time.Second, jobKey, engine); err != nil {
		t.Fatalf("wait for job complete: %v", err)
	}
	if taskAttemptCount.Load() != 2 {
		t.Fatalf("expected 2 task attempts, got %d", taskAttemptCount.Load())
	}

	resp, err := engine.GetJobRun(ctx, swf.GetJobRunRequest{JobKey: jobKey})
	if err != nil {
		t.Fatalf("get job run: %v", err)
	}
	if len(resp.Tasks) != 1 {
		t.Fatalf("expected 1 task run, got %d", len(resp.Tasks))
	}
	taskRun := resp.Tasks[0]
	if taskRun.TaskType != taskWorker.Name() {
		t.Fatalf("unexpected task type: %s", taskRun.TaskType)
	}
	if len(taskRun.Attempts) != 2 {
		t.Fatalf("expected 2 task attempts, got %d", len(taskRun.Attempts))
	}
	if taskRun.Attempts[0].Attempt != 1 || taskRun.Attempts[1].Attempt != 2 {
		t.Fatalf("unexpected task attempt numbers: %d, %d", taskRun.Attempts[0].Attempt, taskRun.Attempts[1].Attempt)
	}
	if taskRun.Attempts[0].Outcome.Status != swf.TaskOutcomeStatusFailed {
		t.Fatalf("expected first task attempt to fail, got %s", taskRun.Attempts[0].Outcome.Status)
	}
	if taskRun.Attempts[1].Outcome.Status != swf.TaskOutcomeStatusSucceeded {
		t.Fatalf("expected second task attempt to succeed, got %s", taskRun.Attempts[1].Outcome.Status)
	}
}

func extractFieldString(t *testing.T, data json.RawMessage, key string) string {
	t.Helper()
	var payload map[string]string
	if err := json.Unmarshal(data, &payload); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	return payload[key]
}

func mustData(t *testing.T, data swf.TaskData) json.RawMessage {
	t.Helper()
	bytes, err := data.GetData()
	if err != nil {
		t.Fatalf("get data: %v", err)
	}
	return bytes
}

type failingJobWorker struct {
	name string
}

func (w *failingJobWorker) Name() string { return w.name }

func (w *failingJobWorker) Run(ctx swf.JobContext, data swf.JobData) (swf.JobData, error) {
	return nil, errors.New("intentional failure")
}
