package swftest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/colony-2/swf-go/pkg/swf"
	directruntime "github.com/colony-2/swf-go/pkg/swf/runtime/direct"
	toyruntime "github.com/colony-2/swf-go/pkg/swf/runtime/toy"
)

type RuntimeHarness struct {
	Name                   string
	SupportsLeases         bool
	SupportsRuntimeStorage bool
	StartsWorkerLoop       bool
	New                    func(t *testing.T, workers ...swf.WorkSet) *BuiltRuntimeHarness
}

type BuiltRuntimeHarness struct {
	Name    string
	Runtime swf.WorkflowRuntime
	Engine  swf.SWFEngine

	cancel   context.CancelFunc
	runDone  <-chan struct{}
	shutdown func()
}

func (h *BuiltRuntimeHarness) Shutdown(t *testing.T) {
	t.Helper()
	if h == nil {
		return
	}
	if h.cancel != nil {
		h.cancel()
	}
	if h.runDone != nil {
		select {
		case <-h.runDone:
		case <-time.After(5 * time.Second):
			t.Fatalf("%s engine did not stop after cancellation", h.Name)
		}
	}
	if h.shutdown != nil {
		h.shutdown()
	}
}

func BuiltInRuntimeHarnesses() []RuntimeHarness {
	return []RuntimeHarness{
		{
			Name:                   "toy",
			SupportsLeases:         false,
			SupportsRuntimeStorage: true,
			StartsWorkerLoop:       true,
			New:                    newToyHarness,
		},
		{
			Name:                   "direct",
			SupportsLeases:         true,
			SupportsRuntimeStorage: true,
			StartsWorkerLoop:       true,
			New:                    newDirectHarness,
		},
	}
}

func MustWorkSet(t *testing.T, job swf.JobWorker, tasks ...swf.TaskWorker) swf.WorkSet {
	t.Helper()
	ws, err := swf.AsWorkSet(job, tasks...)
	if err != nil {
		t.Fatalf("build workset: %v", err)
	}
	return *ws
}

func WaitForEngineStatus(t *testing.T, ctx context.Context, engine swf.SWFEngine, jobKey swf.JobKey, want swf.JobStatus) {
	t.Helper()
	waitForStatus(t, ctx, func(ctx context.Context) (swf.JobStatus, error) {
		return engine.CheckJobStatus(ctx, jobKey)
	}, want)
}

func WaitForRuntimeStatus(t *testing.T, ctx context.Context, runtime swf.WorkflowRuntime, jobKey swf.JobKey, want swf.JobStatus) {
	t.Helper()
	waitForStatus(t, ctx, func(ctx context.Context) (swf.JobStatus, error) {
		return runtime.CheckJobStatus(ctx, jobKey)
	}, want)
}

func WaitForTaskHandle(t *testing.T, ctx context.Context, engine swf.SWFEngine, jobType string, taskType string, tenantIDs []string) swf.TaskHandle {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		handles, err := engine.FindTasksWaitingForCapability(ctx, jobType, taskType, tenantIDs)
		if err == nil && len(handles) > 0 {
			return handles[0]
		}
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("find waiting tasks: %v", err)
		}
		select {
		case <-ctx.Done():
			t.Fatalf("wait for task handle: %v", ctx.Err())
		case <-time.After(20 * time.Millisecond):
		}
	}
	t.Fatalf("timed out waiting for %s:%s task handle", jobType, taskType)
	return nil
}

func MustDecodeNumberTaskData(t *testing.T, data swf.TaskData) int {
	t.Helper()
	if data == nil {
		t.Fatal("missing task data")
	}
	raw, err := data.GetData()
	if err != nil {
		t.Fatalf("get task data: %v", err)
	}
	return decodeNumber(t, raw)
}

func MustDecodeNumberTaskIO(t *testing.T, data *swf.TaskIO) int {
	t.Helper()
	if data == nil {
		t.Fatal("missing task io")
	}
	return decodeNumber(t, data.Data)
}

func MustStartJobAsync(t *testing.T, engine swf.SWFEngine, start swf.StartJob) <-chan error {
	t.Helper()
	done := make(chan error, 1)
	go func() {
		_, err := engine.StartJob(context.Background(), start)
		done <- err
	}()
	return done
}

func ExpectJobTypeFromNextNeed(t *testing.T, nextNeed *string, wantJobType string) {
	t.Helper()
	if nextNeed == nil {
		t.Fatal("missing runtime next_need")
	}
	if got := swf.JobTypeFromNextNeed(*nextNeed); got != wantJobType {
		t.Fatalf("unexpected next_need job type: got %q want %q", got, wantJobType)
	}
}

func ExpectTaskSuffix(t *testing.T, value string, wantSuffix string) {
	t.Helper()
	if !strings.HasSuffix(value, wantSuffix) {
		t.Fatalf("unexpected task value %q, want suffix %q", value, wantSuffix)
	}
}

func NumberTaskData(n int) swf.TaskData {
	return swf.NewTaskDataOrPanic(map[string]int{"n": n})
}

const (
	SequenceJobName = "seq"
	AddOneTaskName  = "add"
	DoubleTaskName  = "double"
	MissingTaskName = "missing"
	FailingJobName  = "failing"
)

type SequenceJob struct {
	Steps []string
}

func (SequenceJob) Name() string { return SequenceJobName }

func (j SequenceJob) Run(ctx swf.JobContext, data swf.JobData) (swf.JobData, error) {
	var (
		out swf.TaskData = data
		err error
	)
	for _, step := range j.Steps {
		out, err = ctx.DoTask(swf.RunPolicy{}, step, out)
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

type AddOneTask struct{}

func (AddOneTask) Name() string { return AddOneTaskName }

func (AddOneTask) Run(_ swf.TaskContext, input swf.TaskData) (swf.TaskData, error) {
	n, err := numberFromTaskData(input)
	if err != nil {
		return nil, err
	}
	return NumberTaskData(n + 1), nil
}

type DoubleTask struct{}

func (DoubleTask) Name() string { return DoubleTaskName }

func (DoubleTask) Run(_ swf.TaskContext, input swf.TaskData) (swf.TaskData, error) {
	n, err := numberFromTaskData(input)
	if err != nil {
		return nil, err
	}
	return NumberTaskData(n * 2), nil
}

type FailingJob struct{}

func (FailingJob) Name() string { return FailingJobName }

func (FailingJob) Run(_ swf.JobContext, _ swf.JobData) (swf.JobData, error) {
	return nil, errors.New("intentional failure")
}

func decodeNumber(t *testing.T, raw json.RawMessage) int {
	t.Helper()
	payload := map[string]int{}
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatalf("decode numeric payload: %v", err)
	}
	return payload["n"]
}

func waitForStatus(t *testing.T, ctx context.Context, check func(context.Context) (swf.JobStatus, error), want swf.JobStatus) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		status, err := check(ctx)
		if err == nil && status == want {
			return
		}
		if err != nil && !errors.Is(err, swf.ErrJobNotFound) && !errors.Is(err, context.Canceled) {
			t.Fatalf("check status: %v", err)
		}
		select {
		case <-ctx.Done():
			t.Fatalf("wait for status: %v", ctx.Err())
		case <-time.After(20 * time.Millisecond):
		}
	}
	t.Fatalf("job did not reach status %s", want)
}

func newToyHarness(t *testing.T, workers ...swf.WorkSet) *BuiltRuntimeHarness {
	t.Helper()
	runtime := toyruntime.New()
	return buildHarness(t, "toy", runtime, true, func() {}, workers...)
}

func newDirectHarness(t *testing.T, workers ...swf.WorkSet) *BuiltRuntimeHarness {
	t.Helper()
	embedded, err := directruntime.StartEmbeddedRuntime(context.Background())
	if err != nil {
		t.Fatalf("start embedded direct runtime: %v", err)
	}
	return buildHarness(t, "direct", embedded.Runtime, true, embedded.Shutdown, workers...)
}

func buildHarness(t *testing.T, name string, runtime swf.WorkflowRuntime, startLoop bool, shutdown func(), workers ...swf.WorkSet) *BuiltRuntimeHarness {
	t.Helper()
	builder := swf.NewEngineBuilder().WithRuntime(runtime)
	for _, ws := range workers {
		tasks := make([]swf.TaskWorker, 0, len(ws.TaskWorkers))
		for _, task := range ws.TaskWorkers {
			tasks = append(tasks, task)
		}
		builder.PlusWorkers(ws.JobWorker, tasks...)
	}
	engine, err := builder.BuildEngine()
	if err != nil {
		shutdown()
		t.Fatalf("build %s engine: %v", name, err)
	}

	var (
		cancel  context.CancelFunc
		runDone <-chan struct{}
	)
	if startLoop && len(workers) > 0 {
		runCtx, runCancel := context.WithCancel(context.Background())
		cancel = runCancel
		done := make(chan struct{})
		runDone = done
		go func() {
			defer close(done)
			engine.Run(runCtx)
		}()
	}

	return &BuiltRuntimeHarness{
		Name:     name,
		Runtime:  runtime,
		Engine:   engine,
		cancel:   cancel,
		runDone:  runDone,
		shutdown: shutdown,
	}
}

func numberFromTaskData(data swf.TaskData) (int, error) {
	if data == nil {
		return 0, fmt.Errorf("missing task data")
	}
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
