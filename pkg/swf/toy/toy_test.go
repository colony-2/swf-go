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

func TestToyEngineCustomJobID(t *testing.T) {
	ws := mustWorkSet(sequenceJob{steps: []string{"add", "double"}}, addOneTask{}, doubleTask{})
	engine := NewToyEngine([]swf.WorkSet{ws})

	customID := "my-custom-job-id"
	tenantID := "test-tenant"
	input := swf.NewTaskDataOrPanic(map[string]int{"n": 1})
	jobKey, err := engine.StartJob(context.Background(), swf.StartJob{
		TenantId: tenantID,
		JobType:  ws.JobWorker.Name(),
		JobID:    customID,
		Data:     input,
	})
	if err != nil {
		t.Fatalf("StartJob failed: %v", err)
	}
	expectedKey := swf.JobKey{TenantId: tenantID, JobId: customID}
	if jobKey != expectedKey {
		t.Fatalf("expected job key %v, got %v", expectedKey, jobKey)
	}

	status, err := engine.CheckJobStatus(context.Background(), jobKey)
	if err != nil {
		t.Fatalf("CheckJobStatus failed: %v", err)
	}
	if status != swf.JobStatusCompleted {
		t.Fatalf("expected status %s, got %s", swf.JobStatusCompleted, status)
	}

	result, err := engine.GetJobResult(context.Background(), jobKey)
	if err != nil {
		t.Fatalf("GetJobResult failed: %v", err)
	}
	if got := extractNumber(result); got != 4 {
		t.Fatalf("unexpected result value, want 4 got %d", got)
	}
}

func TestToyEngineRunsJobInline(t *testing.T) {
	ws := mustWorkSet(sequenceJob{steps: []string{"add", "double"}}, addOneTask{}, doubleTask{})
	engine := NewToyEngine([]swf.WorkSet{ws})

	input := swf.NewTaskDataOrPanic(map[string]int{"n": 1})
	jobKey, err := engine.StartJob(context.Background(), swf.StartJob{
		TenantId: "test-tenant",
		JobType:  ws.JobWorker.Name(),
		Data:     input,
	})
	if err != nil {
		t.Fatalf("StartJob failed: %v", err)
	}

	status, err := engine.CheckJobStatus(context.Background(), jobKey)
	if err != nil {
		t.Fatalf("CheckJobStatus failed: %v", err)
	}
	if status != swf.JobStatusCompleted {
		t.Fatalf("expected status %s, got %s", swf.JobStatusCompleted, status)
	}

	result, err := engine.GetJobResult(context.Background(), jobKey)
	if err != nil {
		t.Fatalf("GetJobResult failed: %v", err)
	}
	if got := extractNumber(result); got != 4 {
		t.Fatalf("unexpected result value, want 4 got %d", got)
	}
}

func TestToyEnginePendingOnMissingTaskWorker(t *testing.T) {
	ws := mustWorkSet(sequenceJob{steps: []string{"missing"}}, addOneTask{})
	jobKey := swf.JobKey{TenantId: "test-tenant", JobId: "pending-missing"}
	engine := NewToyEngine([]swf.WorkSet{ws}, WithJobIDGenerator(func(tenantId string) (swf.JobKey, error) {
		return jobKey, nil
	}))

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, _ = engine.StartJob(context.Background(), swf.StartJob{
			TenantId: "test-tenant",
			JobType:  ws.JobWorker.Name(),
			Data:     swf.NewTaskDataOrPanic(map[string]int{"n": 1}),
			// keep deterministic ID to query status later
		})
	}()

	// Await pending handle for missing task.
	var handles []swf.TaskHandle
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		var err error
		handles, err = engine.FindTasksWaitingForCapability(context.Background(), ws.JobWorker.Name(), "missing")
		if err != nil {
			t.Fatalf("FindTasksWaitingForCapability: %v", err)
		}
		if len(handles) > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if len(handles) != 1 {
		t.Fatalf("expected 1 pending handle, got %d", len(handles))
	}

	// Complete the pending task.
	err := handles[0].Finish(context.Background(), swf.NewTaskDataOrPanic(map[string]int{"n": 2}))
	if err != nil {
		t.Fatalf("Finish failed: %v", err)
	}
	wg.Wait()

	status, err := engine.CheckJobStatus(context.Background(), jobKey)
	if err != nil {
		t.Fatalf("CheckJobStatus failed: %v", err)
	}
	if status != swf.JobStatusCompleted {
		t.Fatalf("expected status %s, got %s", swf.JobStatusCompleted, status)
	}

	result, err := engine.GetJobResult(context.Background(), jobKey)
	if err != nil {
		t.Fatalf("GetJobResult failed: %v", err)
	}
	if extractNumber(result) != 2 {
		t.Fatalf("unexpected result payload")
	}
}

func TestToyEngineCancelJob(t *testing.T) {
	ws := mustWorkSet(sequenceJob{steps: []string{"slow"}}, slowTask{sleep: 500 * time.Millisecond})
	jobKey := swf.JobKey{TenantId: "test-tenant", JobId: "cancel-me"}
	engine := NewToyEngine([]swf.WorkSet{ws}, WithJobIDGenerator(func(tenantId string) (swf.JobKey, error) {
		return jobKey, nil
	}))

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		_, err := engine.StartJob(context.Background(), swf.StartJob{
			TenantId: "test-tenant",
			JobType:  ws.JobWorker.Name(),
			Data:     swf.NewTaskDataOrPanic(map[string]int{"n": 1}),
		})
		if err != nil {
			t.Errorf("StartJob failed: %v", err)
		}
	}()

	time.Sleep(50 * time.Millisecond)
	if err := engine.CancelJob(context.Background(), swf.CancelJob{JobKey: jobKey}); err != nil {
		t.Fatalf("CancelJob failed: %v", err)
	}

	wg.Wait()

	status, err := engine.CheckJobStatus(context.Background(), jobKey)
	if err != nil {
		t.Fatalf("CheckJobStatus failed: %v", err)
	}
	if status != swf.JobStatusCancelled {
		t.Fatalf("expected status %s, got %s", swf.JobStatusCancelled, status)
	}

	if _, err := engine.GetJobResult(context.Background(), jobKey); err == nil {
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

func TestListJobsToyEngine(t *testing.T) {
	now := time.Now().UTC()
	engine := &ToyEngine{
		workers:    make(map[string]swf.WorkSet),
		jobRecords: make(map[swf.JobKey]*jobRecord),
	}

	jobReadyKey := swf.JobKey{TenantId: "test-tenant", JobId: "job-ready"}
	jobCancelledKey := swf.JobKey{TenantId: "test-tenant", JobId: "job-cancelled"}
	jobCompletedKey := swf.JobKey{TenantId: "test-tenant", JobId: "job-completed"}

	engine.jobRecords[jobReadyKey] = &jobRecord{
		status:    swf.JobStatusReady,
		jobType:   "alpha",
		createdAt: now.Add(-1 * time.Minute),
		payload:   []byte(`{"p":1}`),
	}
	engine.jobRecords[jobCancelledKey] = &jobRecord{
		status:    swf.JobStatusCancelled,
		jobType:   "alpha",
		createdAt: now.Add(-30 * time.Second),
		cancelled: true,
		singleton: ptrString("sk"),
		payload:   []byte(`{"p":2}`),
	}
	archivedAt := now.Add(-2 * time.Minute)
	engine.jobRecords[jobCompletedKey] = &jobRecord{
		status:    swf.JobStatusCompleted,
		jobType:   "beta",
		createdAt: now.Add(-2 * time.Minute),
		archived:  &archivedAt,
	}

	t.Run("completed status routes to archived store", func(t *testing.T) {
		resp, err := engine.ListJobs(context.Background(), swf.ListJobsRequest{
			Statuses: []swf.JobStatus{swf.JobStatusCompleted},
		})
		if err != nil {
			t.Fatalf("ListJobs: %v", err)
		}
		if len(resp.Jobs) != 1 || resp.Jobs[0].JobKey != jobCompletedKey {
			t.Fatalf("expected archived job-completed, got %+v", resp.Jobs)
		}
	})

	t.Run("filters by job type and singleton", func(t *testing.T) {
		resp, err := engine.ListJobs(context.Background(), swf.ListJobsRequest{
			JobTypes:      []string{"alpha"},
			SingletonKeys: []string{"sk"},
		})
		if err != nil {
			t.Fatalf("ListJobs: %v", err)
		}
		if len(resp.Jobs) != 1 || resp.Jobs[0].JobKey != jobCancelledKey {
			t.Fatalf("expected job-cancelled, got %+v", resp.Jobs)
		}
		if !resp.Jobs[0].CancelRequested {
			t.Fatalf("expected cancel requested true")
		}
	})

	t.Run("paginates by created_at desc", func(t *testing.T) {
		resp, err := engine.ListJobs(context.Background(), swf.ListJobsRequest{PageSize: 2})
		if err != nil {
			t.Fatalf("ListJobs: %v", err)
		}
		if len(resp.Jobs) != 2 {
			t.Fatalf("expected 2 jobs, got %d", len(resp.Jobs))
		}
		if resp.NextPageToken == "" {
			t.Fatalf("expected next page token")
		}
		if resp.Jobs[0].JobKey != jobCancelledKey {
			t.Fatalf("expected newest job-cancelled first, got %v", resp.Jobs[0].JobKey)
		}

		resp2, err := engine.ListJobs(context.Background(), swf.ListJobsRequest{
			PageSize:  2,
			PageToken: resp.NextPageToken,
		})
		if err != nil {
			t.Fatalf("ListJobs page 2: %v", err)
		}
		if len(resp2.Jobs) != 1 || resp2.Jobs[0].JobKey != jobCompletedKey {
			t.Fatalf("expected final archived job, got %+v", resp2.Jobs)
		}
	})
}

func TestPendingTaskCompletion(t *testing.T) {
	jobWorker := simpleJobWorker{name: "pending-job", task: "needs-external"}
	ws := mustWorkSet(jobWorker, dummyTask{name: "other"})

	engine := NewToyEngine([]swf.WorkSet{ws})

	input := swf.NewTaskDataOrPanic(map[string]int{"n": 1})
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = engine.StartJob(context.Background(), swf.StartJob{
			TenantId: "test-tenant",
			JobType:  ws.JobWorker.Name(),
			Data:     input,
		})
	}()

	// Wait for pending handle to appear.
	var handles []swf.TaskHandle
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		var err error
		handles, err = engine.FindTasksWaitingForCapability(context.Background(), jobWorker.name, jobWorker.task)
		if err != nil {
			t.Fatalf("FindTasksWaitingForCapability: %v", err)
		}
		if len(handles) > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if len(handles) != 1 {
		t.Fatalf("expected 1 pending handle, got %d", len(handles))
	}

	handleByID, err := engine.GetWaitingTask(context.Background(), handles[0].JobKey())
	if err != nil {
		t.Fatalf("GetWaitingTask: %v", err)
	}

	err = handleByID.Finish(context.Background(), swf.NewTaskDataOrPanic(map[string]int{"n": 5}))
	if err != nil {
		t.Fatalf("Finish failed: %v", err)
	}
	<-done

	status, err := engine.CheckJobStatus(context.Background(), handles[0].JobKey())
	if err != nil {
		t.Fatalf("CheckJobStatus: %v", err)
	}
	if status != swf.JobStatusCompleted {
		t.Fatalf("expected completed status, got %s", status)
	}

	resp, err := engine.ListJobs(context.Background(), swf.ListJobsRequest{
		JobTasks: []swf.JobTaskFilter{{JobType: jobWorker.name, TaskType: jobWorker.task}},
	})
	if err != nil {
		t.Fatalf("ListJobs with job/task filter: %v", err)
	}
	if len(resp.Jobs) != 1 || resp.Jobs[0].JobKey != handles[0].JobKey() {
		t.Fatalf("expected job from job/task filter, got %+v", resp.Jobs)
	}

	respIDs, err := engine.ListJobs(context.Background(), swf.ListJobsRequest{
		JobKeys: []swf.JobKey{handles[0].JobKey()},
	})
	if err != nil {
		t.Fatalf("ListJobs with job ids: %v", err)
	}
	if len(respIDs.Jobs) != 1 || respIDs.Jobs[0].JobKey != handles[0].JobKey() {
		t.Fatalf("expected job via id filter, got %+v", respIDs.Jobs)
	}
}

func TestJobSummaryPendingStepMatchesHandle(t *testing.T) {
	ws := mustWorkSet(sequenceJob{steps: []string{"add", "missing"}}, addOneTask{})
	jobKey := swf.JobKey{TenantId: "test-tenant", JobId: "multi-step-pending"}
	engine := NewToyEngine([]swf.WorkSet{ws}, WithJobIDGenerator(func(tenantId string) (swf.JobKey, error) { return jobKey, nil }))

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = engine.StartJob(context.Background(), swf.StartJob{
			TenantId: "test-tenant",
			JobType:  ws.JobWorker.Name(),
			Data:     swf.NewTaskDataOrPanic(map[string]int{"n": 1}),
		})
	}()

	var handles []swf.TaskHandle
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		var err error
		handles, err = engine.FindTasksWaitingForCapability(context.Background(), ws.JobWorker.Name(), "missing")
		if err != nil {
			t.Fatalf("FindTasksWaitingForCapability: %v", err)
		}
		if len(handles) > 0 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if len(handles) != 1 {
		t.Fatalf("expected 1 pending handle, got %d", len(handles))
	}

	handle := handles[0]
	expectedOutputOrdinal := handle.TaskOrdinalToComplete()
	if expectedOutputOrdinal <= 0 {
		t.Fatalf("TaskOrdinalToComplete should be positive, got %d", expectedOutputOrdinal)
	}
	expectedInputOrdinal := expectedOutputOrdinal - 1

	resp, err := engine.ListJobs(context.Background(), swf.ListJobsRequest{
		JobKeys: []swf.JobKey{jobKey},
	})
	if err != nil {
		t.Fatalf("ListJobs: %v", err)
	}
	if len(resp.Jobs) != 1 {
		t.Fatalf("expected 1 job in summary, got %d", len(resp.Jobs))
	}
	summary := resp.Jobs[0]
	if summary.TaskWaitInput == nil || *summary.TaskWaitInput != expectedInputOrdinal {
		t.Fatalf("TaskWaitInput mismatch, want %d got %v", expectedInputOrdinal, summary.TaskWaitInput)
	}
	if summary.TaskWaitOutput == nil || *summary.TaskWaitOutput != expectedOutputOrdinal {
		t.Fatalf("TaskWaitOutput mismatch, want %d got %v", expectedOutputOrdinal, summary.TaskWaitOutput)
	}

	if err := handle.Finish(context.Background(), swf.NewTaskDataOrPanic(map[string]int{"n": 3})); err != nil {
		t.Fatalf("Finish: %v", err)
	}
	<-done
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

func ptrString(s string) *string {
	return &s
}

type simpleJobWorker struct {
	name string
	task string
}

func (j simpleJobWorker) Name() string { return j.name }
func (j simpleJobWorker) Run(ctx swf.JobContext, input swf.JobData) (swf.JobData, error) {
	return ctx.DoTask(swf.RunPolicy{}, j.task, input)
}

type dummyTask struct {
	name string
}

func (d dummyTask) Name() string { return d.name }
func (dummyTask) Run(ctx swf.TaskContext, input swf.TaskData) (swf.TaskData, error) {
	return input, nil
}
