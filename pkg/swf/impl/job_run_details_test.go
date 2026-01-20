package impl

import (
	"context"
	"encoding/json"
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
}

func extractFieldString(t *testing.T, data json.RawMessage, key string) string {
	t.Helper()
	var payload map[string]string
	if err := json.Unmarshal(data, &payload); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	return payload[key]
}
