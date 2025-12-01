package toy

import (
	"context"
	"encoding/json"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/colony-2/swf-go/pkg/swf"
)

func TestToyEngineRunsJobInline(t *testing.T) {
	ws := mustWorkSet(sequenceJob{steps: []string{"add", "double"}}, addOneTask{}, doubleTask{})
	engine := NewToyEngine([]swf.WorkSet{ws})

	input := swf.NewTaskDataOrPanic(map[string]int{"n": 1})
	jobID, err := engine.StartJob(context.Background(), swf.StartJob{
		JobType: ws.JobWorker.Name(),
		Data:    input,
	})
	if err != nil {
		t.Fatalf("StartJob failed: %v", err)
	}

	status, err := engine.CheckJobStatus(context.Background(), jobID)
	if err != nil {
		t.Fatalf("CheckJobStatus failed: %v", err)
	}
	if status != swf.JobStatusCompleted {
		t.Fatalf("expected status %s, got %s", swf.JobStatusCompleted, status)
	}

	result, err := engine.GetJobResult(context.Background(), jobID)
	if err != nil {
		t.Fatalf("GetJobResult failed: %v", err)
	}
	if got := extractNumber(result); got != 4 {
		t.Fatalf("unexpected result value, want 4 got %d", got)
	}
}

func TestToyEngineErrorsOnMissingTaskWorker(t *testing.T) {
	ws := mustWorkSet(sequenceJob{steps: []string{"missing"}}, addOneTask{})
	engine := NewToyEngine([]swf.WorkSet{ws})

	jobID, err := engine.StartJob(context.Background(), swf.StartJob{
		JobType: ws.JobWorker.Name(),
		Data:    swf.NewTaskDataOrPanic(map[string]int{"n": 1}),
	})
	if err != nil {
		t.Fatalf("StartJob failed: %v", err)
	}

	status, err := engine.CheckJobStatus(context.Background(), jobID)
	if err != nil {
		t.Fatalf("CheckJobStatus failed: %v", err)
	}
	if status != swf.JobStatusCompleted {
		t.Fatalf("expected status %s, got %s", swf.JobStatusCompleted, status)
	}

	_, err = engine.GetJobResult(context.Background(), jobID)
	if err == nil {
		t.Fatalf("expected GetJobResult to fail for missing task worker")
	}
}

func TestToyEngineCancelJob(t *testing.T) {
	ws := mustWorkSet(sequenceJob{steps: []string{"slow"}}, slowTask{sleep: 500 * time.Millisecond})
	jobID := swf.JobId("cancel-me")
	engine := NewToyEngine([]swf.WorkSet{ws}, WithJobIDGenerator(func() (swf.JobId, error) {
		return jobID, nil
	}))

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, err := engine.StartJob(context.Background(), swf.StartJob{
			JobType: ws.JobWorker.Name(),
			Data:    swf.NewTaskDataOrPanic(map[string]int{"n": 1}),
		})
		if err != nil {
			t.Errorf("StartJob failed: %v", err)
		}
	}()

	time.Sleep(50 * time.Millisecond)
	if err := engine.CancelJob(context.Background(), swf.CancelJob{JobId: jobID}); err != nil {
		t.Fatalf("CancelJob failed: %v", err)
	}

	wg.Wait()

	status, err := engine.CheckJobStatus(context.Background(), jobID)
	if err != nil {
		t.Fatalf("CheckJobStatus failed: %v", err)
	}
	if status != swf.JobStatusCancelled {
		t.Fatalf("expected status %s, got %s", swf.JobStatusCancelled, status)
	}

	if _, err := engine.GetJobResult(context.Background(), jobID); err == nil {
		t.Fatalf("expected GetJobResult to fail for cancelled job")
	}
}

func TestFindTasksWaitingForCapabilityEmpty(t *testing.T) {
	engine := NewToyEngine(nil)
	handles, err := engine.FindTasksWaitingForCapability(context.Background(), "job", "task")
	if err != nil {
		t.Fatalf("FindTasksWaitingForCapability returned error: %v", err)
	}
	if len(handles) != 0 {
		t.Fatalf("expected no pending tasks, got %d", len(handles))
	}
}

// --- Helpers and stub workers ---

type sequenceJob struct {
	steps []string
}

func (sequenceJob) Name() string { return "seq" }

func (j sequenceJob) Run(ctx swf.JobContext, data swf.JobData) (swf.JobData, error) {
	current := data
	for _, step := range j.steps {
		out, err := ctx.DoTask(swf.RunPolicy{}, step, current)
		if err != nil {
			return nil, err
		}
		current = out
	}
	return current, nil
}

type addOneTask struct{}

func (addOneTask) Name() string { return "add" }

func (addOneTask) Run(ctx swf.TaskContext, input swf.TaskData) (swf.TaskData, error) {
	return swf.NewTaskDataOrPanic(map[string]int{"n": extractNumber(input) + 1}), nil
}

type doubleTask struct{}

func (doubleTask) Name() string { return "double" }

func (doubleTask) Run(ctx swf.TaskContext, input swf.TaskData) (swf.TaskData, error) {
	return swf.NewTaskDataOrPanic(map[string]int{"n": extractNumber(input) * 2}), nil
}

type slowTask struct {
	sleep time.Duration
}

func (slowTask) Name() string { return "slow" }

func (s slowTask) Run(ctx swf.TaskContext, input swf.TaskData) (swf.TaskData, error) {
	if err := ctx.AwaitDuration(swf.Duration(s.sleep)); err != nil && !errors.Is(err, context.Canceled) {
		return nil, err
	}
	return swf.NewTaskDataOrPanic(map[string]int{"n": extractNumber(input)}), nil
}

func mustWorkSet(job swf.JobWorker, tasks ...swf.TaskWorker) swf.WorkSet {
	ws, err := swf.AsWorkSet(job, tasks...)
	if err != nil {
		panic(err)
	}
	return *ws
}

func extractNumber(td swf.TaskData) int {
	data, err := td.GetData()
	if err != nil {
		return 0
	}
	var payload map[string]int
	if err := json.Unmarshal(data, &payload); err != nil {
		return 0
	}
	return payload["n"]
}
