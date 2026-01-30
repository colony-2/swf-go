package toy

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
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

func TestToyEngineRestartReexecutesWhenNoExtraOutput(t *testing.T) {
	ws := mustWorkSet(sequenceJob{steps: []string{"add", "double"}}, addOneTask{}, doubleTask{})
	engine := NewToyEngine([]swf.WorkSet{ws})

	input := swf.NewTaskDataOrPanic(map[string]int{"n": 1})
	origKey, err := engine.StartJob(context.Background(), swf.StartJob{
		TenantId: "tenant-restart",
		JobType:  ws.JobWorker.Name(),
		Data:     input,
	})
	if err != nil {
		t.Fatalf("StartJob failed: %v", err)
	}

	restartKey, err := engine.RestartJob(context.Background(), swf.RestartJob{
		PriorJobKey:    origKey,
		LastStepToKeep: 0,
	})
	if err != nil {
		t.Fatalf("RestartJob failed: %v", err)
	}

	status, err := engine.CheckJobStatus(context.Background(), restartKey)
	if err != nil {
		t.Fatalf("CheckJobStatus failed: %v", err)
	}
	if status != swf.JobStatusCompleted {
		t.Fatalf("expected completed status, got %s", status)
	}

	result, err := engine.GetJobResult(context.Background(), restartKey)
	if err != nil {
		t.Fatalf("GetJobResult failed: %v", err)
	}
	if got := extractNumber(result); got != 4 {
		t.Fatalf("unexpected restart result value, want 4 got %d", got)
	}
}

func TestToyEngineRestartWithExtraOutputSkipsExecution(t *testing.T) {
	ws := mustWorkSet(sequenceJob{steps: []string{"add", "double"}}, addOneTask{}, doubleTask{})
	engine := NewToyEngine([]swf.WorkSet{ws})

	input := swf.NewTaskDataOrPanic(map[string]int{"n": 1})
	origKey, err := engine.StartJob(context.Background(), swf.StartJob{
		TenantId: "tenant-restart-extra",
		JobType:  ws.JobWorker.Name(),
		Data:     input,
	})
	if err != nil {
		t.Fatalf("StartJob failed: %v", err)
	}

	extraOut := swf.NewTaskDataOrPanic(map[string]int{"n": 10})
	restartKey, err := engine.RestartJob(context.Background(), swf.RestartJob{
		PriorJobKey:     origKey,
		LastStepToKeep:  0,
		ExtraTaskOutput: extraOut,
	})
	if err != nil {
		t.Fatalf("RestartJob failed: %v", err)
	}

	status, err := engine.CheckJobStatus(context.Background(), restartKey)
	if err != nil {
		t.Fatalf("CheckJobStatus failed: %v", err)
	}
	if status != swf.JobStatusCompleted {
		t.Fatalf("expected completed status, got %s", status)
	}

	result, err := engine.GetJobResult(context.Background(), restartKey)
	if err != nil {
		t.Fatalf("GetJobResult failed: %v", err)
	}
	if got := extractNumber(result); got != 10 {
		t.Fatalf("unexpected restart result value, want 10 got %d", got)
	}
}

func TestToyEngineRestartRejectsMidRetryBoundary(t *testing.T) {
	ws := mustWorkSet(sequenceJob{steps: []string{"add", "double"}}, addOneTask{}, doubleTask{})
	engine := NewToyEngine([]swf.WorkSet{ws})

	input := swf.NewTaskDataOrPanic(map[string]int{"n": 1})
	jobKey, err := engine.StartJob(context.Background(), swf.StartJob{
		TenantId: "tenant-retry",
		JobType:  ws.JobWorker.Name(),
		Data:     input,
	})
	if err != nil {
		t.Fatalf("StartJob failed: %v", err)
	}

	// Simulate a retry chain by marking the next chapter as attempt 2.
	record := engine.getJobRecord(jobKey)
	if record == nil {
		t.Fatalf("job record not found")
	}
	record.mu.Lock()
	if chap := record.chapters[1]; chap != nil {
		chap.Attempt = 2
	} else {
		record.mu.Unlock()
		t.Fatalf("expected chapter 1 to exist")
	}
	record.mu.Unlock()

	if _, err := engine.RestartJob(context.Background(), swf.RestartJob{
		PriorJobKey:    jobKey,
		LastStepToKeep: 0, // next ordinal 1 now marked attempt 2
	}); err == nil {
		t.Fatalf("expected restart to fail when slicing into retry chain")
	}
}

func TestToyEngineRestartRejectsWhenNextMissing(t *testing.T) {
	ws := mustWorkSet(sequenceJob{steps: []string{"add", "double"}}, addOneTask{}, doubleTask{})
	engine := NewToyEngine([]swf.WorkSet{ws})

	input := swf.NewTaskDataOrPanic(map[string]int{"n": 1})
	jobKey, err := engine.StartJob(context.Background(), swf.StartJob{
		TenantId: "tenant-missing-next",
		JobType:  ws.JobWorker.Name(),
		Data:     input,
	})
	if err != nil {
		t.Fatalf("StartJob failed: %v", err)
	}

	if _, err := engine.RestartJob(context.Background(), swf.RestartJob{
		PriorJobKey:    jobKey,
		LastStepToKeep: 3, // next ordinal 4 does not exist
	}); err == nil {
		t.Fatalf("expected restart to fail when next chapter is missing")
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
		handles, err = engine.FindTasksWaitingForCapability(context.Background(), ws.JobWorker.Name(), "missing", nil)
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

func TestToyEngineAwaitJobs(t *testing.T) {
	tenantID := "test-tenant"
	childJobID := "child-job"
	parentJobID := "parent-job"

	childStarted := make(chan struct{})
	releaseChild := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-releaseChild:
		default:
			close(releaseChild)
		}
	})

	childWorker := blockingJobWorker{name: "child-worker", started: childStarted, release: releaseChild}
	parentWorker := awaitJobsParentWorker{name: "parent-worker", waitFor: []string{childJobID}}
	childWS := mustWorkSet(childWorker)
	parentWS := mustWorkSet(parentWorker)
	engine := NewToyEngine([]swf.WorkSet{childWS, parentWS})

	childDone := make(chan error, 1)
	go func() {
		_, err := engine.StartJob(context.Background(), swf.StartJob{
			TenantId: tenantID,
			JobType:  childWorker.Name(),
			JobID:    childJobID,
			Data:     swf.NewTaskDataOrPanic(map[string]int{"n": 1}),
		})
		childDone <- err
	}()

	select {
	case <-childStarted:
	case <-time.After(2 * time.Second):
		t.Fatalf("child job did not start")
	}

	parentDone := make(chan error, 1)
	go func() {
		_, err := engine.StartJob(context.Background(), swf.StartJob{
			TenantId: tenantID,
			JobType:  parentWorker.Name(),
			JobID:    parentJobID,
			Data:     swf.NewTaskDataOrPanic(map[string]int{"n": 2}),
		})
		parentDone <- err
	}()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		status, err := engine.CheckJobStatus(context.Background(), swf.JobKey{TenantId: tenantID, JobId: parentJobID})
		if err == nil && status == swf.JobStatusPendingJobs {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	status, err := engine.CheckJobStatus(context.Background(), swf.JobKey{TenantId: tenantID, JobId: parentJobID})
	if err != nil {
		t.Fatalf("CheckJobStatus failed: %v", err)
	}
	if status != swf.JobStatusPendingJobs {
		t.Fatalf("expected parent status %s, got %s", swf.JobStatusPendingJobs, status)
	}

	resp, err := engine.ListJobs(context.Background(), swf.ListJobsRequest{TenantIds: []string{tenantID}})
	if err != nil {
		t.Fatalf("ListJobs failed: %v", err)
	}
	found := false
	for _, job := range resp.Jobs {
		if job.JobKey.JobId == parentJobID {
			found = true
			if len(job.WaitFor) != 1 || job.WaitFor[0] != childJobID {
				t.Fatalf("expected wait_for %s, got %v", childJobID, job.WaitFor)
			}
		}
	}
	if !found {
		t.Fatalf("parent job not found in ListJobs response")
	}

	close(releaseChild)

	select {
	case err := <-childDone:
		if err != nil {
			t.Fatalf("child job error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("child job did not finish")
	}

	select {
	case err := <-parentDone:
		if err != nil {
			t.Fatalf("parent job error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("parent job did not finish")
	}

	finalStatus, err := engine.CheckJobStatus(context.Background(), swf.JobKey{TenantId: tenantID, JobId: parentJobID})
	if err != nil {
		t.Fatalf("CheckJobStatus failed: %v", err)
	}
	if finalStatus != swf.JobStatusCompleted {
		t.Fatalf("expected parent status %s, got %s", swf.JobStatusCompleted, finalStatus)
	}
}

func TestToyEngineTaskContextAwaitJobs(t *testing.T) {
	tenantID := "test-tenant"
	childJobID := "child-job-task"
	parentJobID := "parent-job-task"

	childStarted := make(chan struct{})
	releaseChild := make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-releaseChild:
		default:
			close(releaseChild)
		}
	})

	childWorker := blockingJobWorker{name: "child-worker-task", started: childStarted, release: releaseChild}
	taskWorker := awaitJobsTaskWorker{name: "await-task", waitFor: []string{childJobID}}
	parentWorker := simpleJobWorker{name: "parent-task-worker", task: taskWorker.Name()}
	engine := NewToyEngine([]swf.WorkSet{
		mustWorkSet(childWorker),
		mustWorkSet(parentWorker, taskWorker),
	})

	childDone := make(chan error, 1)
	go func() {
		_, err := engine.StartJob(context.Background(), swf.StartJob{
			TenantId: tenantID,
			JobType:  childWorker.Name(),
			JobID:    childJobID,
			Data:     swf.NewTaskDataOrPanic(map[string]int{"n": 1}),
		})
		childDone <- err
	}()

	select {
	case <-childStarted:
	case <-time.After(2 * time.Second):
		t.Fatalf("child job did not start")
	}

	parentDone := make(chan error, 1)
	go func() {
		_, err := engine.StartJob(context.Background(), swf.StartJob{
			TenantId: tenantID,
			JobType:  parentWorker.Name(),
			JobID:    parentJobID,
			Data:     swf.NewTaskDataOrPanic(map[string]int{"n": 2}),
		})
		parentDone <- err
	}()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		status, err := engine.CheckJobStatus(context.Background(), swf.JobKey{TenantId: tenantID, JobId: parentJobID})
		if err == nil && status == swf.JobStatusPendingJobs {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	status, err := engine.CheckJobStatus(context.Background(), swf.JobKey{TenantId: tenantID, JobId: parentJobID})
	if err != nil {
		t.Fatalf("CheckJobStatus failed: %v", err)
	}
	if status != swf.JobStatusPendingJobs {
		t.Fatalf("expected parent status %s, got %s", swf.JobStatusPendingJobs, status)
	}

	resp, err := engine.ListJobs(context.Background(), swf.ListJobsRequest{TenantIds: []string{tenantID}})
	if err != nil {
		t.Fatalf("ListJobs failed: %v", err)
	}
	found := false
	for _, job := range resp.Jobs {
		if job.JobKey.JobId == parentJobID {
			found = true
			if len(job.WaitFor) != 1 || job.WaitFor[0] != childJobID {
				t.Fatalf("expected wait_for %s, got %v", childJobID, job.WaitFor)
			}
		}
	}
	if !found {
		t.Fatalf("parent job not found in ListJobs response")
	}

	close(releaseChild)

	select {
	case err := <-childDone:
		if err != nil {
			t.Fatalf("child job error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("child job did not finish")
	}

	select {
	case err := <-parentDone:
		if err != nil {
			t.Fatalf("parent job error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatalf("parent job did not finish")
	}

	finalStatus, err := engine.CheckJobStatus(context.Background(), swf.JobKey{TenantId: tenantID, JobId: parentJobID})
	if err != nil {
		t.Fatalf("CheckJobStatus failed: %v", err)
	}
	if finalStatus != swf.JobStatusCompleted {
		t.Fatalf("expected parent status %s, got %s", swf.JobStatusCompleted, finalStatus)
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

func TestToyEngineGetJobRunCompleted(t *testing.T) {
	ws := mustWorkSet(sequenceJob{steps: []string{"add", "double"}}, addOneTask{}, doubleTask{})
	engine := NewToyEngine([]swf.WorkSet{ws})

	jobKey, err := engine.StartJob(context.Background(), swf.StartJob{
		TenantId: "test-tenant",
		JobType:  ws.JobWorker.Name(),
		Data:     swf.NewTaskDataOrPanic(map[string]int{"n": 1}),
	})
	if err != nil {
		t.Fatalf("StartJob failed: %v", err)
	}

	resp, err := engine.GetJobRun(context.Background(), swf.GetJobRunRequest{JobKey: jobKey})
	if err != nil {
		t.Fatalf("GetJobRun failed: %v", err)
	}
	if resp.Job.Status != swf.JobStatusCompleted {
		t.Fatalf("expected completed status, got %s", resp.Job.Status)
	}
	if resp.Start.Input == nil {
		t.Fatalf("expected start input")
	}
	if got := extractNumberFromIO(t, resp.Start.Input); got != 1 {
		t.Fatalf("unexpected start input: %d", got)
	}
	if len(resp.Tasks) != 2 {
		t.Fatalf("expected 2 tasks, got %d", len(resp.Tasks))
	}
	if resp.Tasks[0].TaskType != "add" || resp.Tasks[1].TaskType != "double" {
		t.Fatalf("unexpected task types: %s, %s", resp.Tasks[0].TaskType, resp.Tasks[1].TaskType)
	}
	if got := extractNumberFromIO(t, resp.Tasks[0].Attempts[0].Output); got != 2 {
		t.Fatalf("unexpected add output: %d", got)
	}
	if got := extractNumberFromIO(t, resp.Tasks[1].Attempts[0].Output); got != 4 {
		t.Fatalf("unexpected double output: %d", got)
	}
	if resp.Result == nil || resp.Result.Output == nil {
		t.Fatalf("expected job result output")
	}
	if got := extractNumberFromIO(t, resp.Result.Output); got != 4 {
		t.Fatalf("unexpected job result output: %d", got)
	}
}

func TestToyEngineGetJobRunPendingRuntime(t *testing.T) {
	ws := mustWorkSet(sequenceJob{steps: []string{"missing"}})
	jobKey := swf.JobKey{TenantId: "test-tenant", JobId: "pending-runtime"}
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
		})
	}()

	var handles []swf.TaskHandle
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		var err error
		handles, err = engine.FindTasksWaitingForCapability(context.Background(), ws.JobWorker.Name(), "missing", nil)
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

	resp, err := engine.GetJobRun(context.Background(), swf.GetJobRunRequest{JobKey: jobKey})
	if err != nil {
		t.Fatalf("GetJobRun failed: %v", err)
	}
	if resp.Result != nil {
		t.Fatalf("expected nil result for pending job")
	}
	if len(resp.Tasks) != 1 {
		t.Fatalf("expected 1 task run, got %d", len(resp.Tasks))
	}
	task := resp.Tasks[0]
	if task.TaskType != "missing" {
		t.Fatalf("unexpected task type: %s", task.TaskType)
	}
	if len(task.Attempts) != 1 {
		t.Fatalf("expected 1 attempt, got %d", len(task.Attempts))
	}
	attempt := task.Attempts[0]
	if attempt.State != swf.TaskAttemptStateReady {
		t.Fatalf("expected READY state, got %s", attempt.State)
	}
	if attempt.Runtime == nil || attempt.Runtime.NextNeed == nil || *attempt.Runtime.NextNeed != "seq:missing" {
		t.Fatalf("expected runtime next_need seq:missing")
	}
	if attempt.Input == nil {
		t.Fatalf("expected runtime input")
	}
	if got := extractNumberFromIO(t, attempt.Input); got != 1 {
		t.Fatalf("unexpected runtime input: %d", got)
	}

	err = handles[0].Finish(context.Background(), swf.NewTaskDataOrPanic(map[string]int{"n": 2}))
	if err != nil {
		t.Fatalf("Finish failed: %v", err)
	}
	wg.Wait()
}

func extractNumberFromIO(t *testing.T, io *swf.TaskIO) int {
	t.Helper()
	if io == nil {
		t.Fatalf("missing task io")
	}
	var payload map[string]int
	if err := json.Unmarshal(io.Data, &payload); err != nil {
		t.Fatalf("unmarshal task io: %v", err)
	}
	return payload["n"]
}

func TestFindTasksWaitingForCapabilityEmpty(t *testing.T) {
	engine := NewToyEngine(nil)
	handles, err := engine.FindTasksWaitingForCapability(context.Background(), "job", "task", nil)
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
			TenantIds: []string{"test-tenant"},
			Statuses:  []swf.JobStatus{swf.JobStatusCompleted},
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
			TenantIds:     []string{"test-tenant"},
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
		resp, err := engine.ListJobs(context.Background(), swf.ListJobsRequest{
			TenantIds: []string{"test-tenant"},
			PageSize:  2,
		})
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
			TenantIds: []string{"test-tenant"},
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
		handles, err = engine.FindTasksWaitingForCapability(context.Background(), jobWorker.name, jobWorker.task, nil)
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
		TenantIds: []string{"test-tenant"},
		JobTasks:  []swf.JobTaskFilter{{JobType: jobWorker.name, TaskType: jobWorker.task}},
	})
	if err != nil {
		t.Fatalf("ListJobs with job/task filter: %v", err)
	}
	if len(resp.Jobs) != 1 || resp.Jobs[0].JobKey != handles[0].JobKey() {
		t.Fatalf("expected job from job/task filter, got %+v", resp.Jobs)
	}

	respIDs, err := engine.ListJobs(context.Background(), swf.ListJobsRequest{
		TenantIds: []string{"test-tenant"},
		JobKeys:   []swf.JobKey{handles[0].JobKey()},
	})
	if err != nil {
		t.Fatalf("ListJobs with job ids: %v", err)
	}
	if len(respIDs.Jobs) != 1 || respIDs.Jobs[0].JobKey != handles[0].JobKey() {
		t.Fatalf("expected job via id filter, got %+v", respIDs.Jobs)
	}
}

func TestGetWaitingTaskReturnsCorrectStepData(t *testing.T) {
	// This test verifies that GetWaitingTask returns the OUTPUT from the previous step,
	// NOT the INPUT that was passed to DoTask (which might have been modified by the job).
	//
	// Job flow: input(n=1) → add(+1)=2 → double(*2)=4 → missing(pending)
	// Expected: handle.Data() returns n=4 (output from step 2), NOT n=1 (from step 0)

	ws := mustWorkSet(sequenceJob{steps: []string{"add", "double", "missing"}}, addOneTask{}, doubleTask{})
	jobKey := swf.JobKey{TenantId: "test-tenant", JobId: "test-step-data"}
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

	// Wait for pending task "missing" to appear
	var handles []swf.TaskHandle
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		var err error
		handles, err = engine.FindTasksWaitingForCapability(context.Background(), ws.JobWorker.Name(), "missing", nil)
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

	handle, err := engine.GetWaitingTask(context.Background(), jobKey)
	if err != nil {
		t.Fatalf("GetWaitingTask: %v", err)
	}

	data, err := handle.Data()
	if err != nil {
		t.Fatalf("handle.Data(): %v", err)
	}

	actualValue := extractNumber(data)
	expectedValue := 4 // (1 + 1) * 2 = 4
	if actualValue != expectedValue {
		t.Fatalf("handle.Data() returned wrong step data: expected n=%d (output from step 2), got n=%d",
			expectedValue, actualValue)
	}

	if err := handle.Finish(context.Background(), swf.NewTaskDataOrPanic(map[string]int{"n": 100})); err != nil {
		t.Fatalf("Finish: %v", err)
	}
	<-done
}

func TestGetWaitingTaskReturnsOutputNotInput(t *testing.T) {
	// This test explicitly verifies the distinction between:
	// - OUTPUT of the previous step (what's persisted)
	// - INPUT to the current step (what the job passed to DoTask)
	// These can be DIFFERENT if the job modifies data between steps.
	//
	// Job flow:
	//   step 0: n=10
	//   step 1: add(n=10) → n=11
	//   Job transforms: n=11 + 100 = n=111
	//   step 2: missing(n=111) - pending
	//
	// GetWaitingTask should return n=11 (persisted output from step 1),
	// NOT n=111 (the transformed input passed to step 2)

	jobWithTransform := &jobWorkerWithTransform{missingTask: "missing"}
	ws := mustWorkSet(jobWithTransform, addOneTask{})
	jobKey := swf.JobKey{TenantId: "test-tenant", JobId: "test-transform"}
	engine := NewToyEngine([]swf.WorkSet{ws}, WithJobIDGenerator(func(tenantId string) (swf.JobKey, error) { return jobKey, nil }))

	done := make(chan struct{})
	go func() {
		defer close(done)
		_, _ = engine.StartJob(context.Background(), swf.StartJob{
			TenantId: "test-tenant",
			JobType:  ws.JobWorker.Name(),
			Data:     swf.NewTaskDataOrPanic(map[string]int{"n": 10}),
		})
	}()

	// Wait for pending task
	var handles []swf.TaskHandle
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		var err error
		handles, err = engine.FindTasksWaitingForCapability(context.Background(), ws.JobWorker.Name(), "missing", nil)
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

	handle, err := engine.GetWaitingTask(context.Background(), jobKey)
	if err != nil {
		t.Fatalf("GetWaitingTask: %v", err)
	}

	data, err := handle.Data()
	if err != nil {
		t.Fatalf("handle.Data(): %v", err)
	}

	actualValue := extractNumber(data)
	expectedValue := 11 // OUTPUT from step 1: 10 + 1 = 11
	wrongValue := 111   // INPUT to step 2: (10 + 1) + 100 = 111

	if actualValue == wrongValue {
		t.Fatalf("BUG: handle.Data() returned the INPUT to the pending task (n=%d) instead of the OUTPUT from the previous step (n=%d)",
			wrongValue, expectedValue)
	}
	if actualValue != expectedValue {
		t.Fatalf("handle.Data() returned unexpected value: expected n=%d (output from step 1), got n=%d",
			expectedValue, actualValue)
	}

	if err := handle.Finish(context.Background(), swf.NewTaskDataOrPanic(map[string]int{"n": 200})); err != nil {
		t.Fatalf("Finish: %v", err)
	}
	<-done
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
		handles, err = engine.FindTasksWaitingForCapability(context.Background(), ws.JobWorker.Name(), "missing", nil)
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
		TenantIds: []string{"test-tenant"},
		JobKeys:   []swf.JobKey{jobKey},
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

func TestChapterConstraintEnforcement(t *testing.T) {
	// Test that ToyEngine enforces chapter constraints similar to Strata:
	// - Chapters must be written once
	// - Chapters must be written in monotonic order starting at 0

	t.Run("chapters are tracked in order", func(t *testing.T) {
		ws := mustWorkSet(sequenceJob{steps: []string{"add", "double", "add"}}, addOneTask{}, doubleTask{})
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

		// Verify the job completed successfully
		status, err := engine.CheckJobStatus(context.Background(), jobKey)
		if err != nil {
			t.Fatalf("CheckJobStatus failed: %v", err)
		}
		if status != swf.JobStatusCompleted {
			t.Fatalf("expected status %s, got %s", swf.JobStatusCompleted, status)
		}

		// Verify chapters were written in order: 0 (job input), 1 (add), 2 (double), 3 (add)
		engine.mu.Lock()
		record := engine.jobRecords[jobKey]
		engine.mu.Unlock()

		if record == nil {
			t.Fatalf("job record not found")
		}

		record.mu.Lock()
		defer record.mu.Unlock()

		expectedChapters := []int64{0, 1, 2, 3}
		for _, ordinal := range expectedChapters {
			if record.chapters[ordinal] == nil {
				t.Errorf("expected chapter %d to be written", ordinal)
			}
		}

		// Verify no unexpected chapters were written
		if len(record.chapters) != len(expectedChapters) {
			t.Errorf("expected %d chapters, got %d", len(expectedChapters), len(record.chapters))
		}
	})

	t.Run("duplicate chapter error with non-deterministic job", func(t *testing.T) {
		// This job worker will try to execute the same task with the same ordinal twice
		// by manually manipulating the step counter
		nonDeterministicJob := &nonDeterministicJobWorker{}
		ws := mustWorkSet(nonDeterministicJob, addOneTask{})
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

		// The job should have completed with an error
		status, err := engine.CheckJobStatus(context.Background(), jobKey)
		if err != nil {
			t.Fatalf("CheckJobStatus failed: %v", err)
		}
		if status != swf.JobStatusCompleted {
			t.Fatalf("expected status %s, got %s", swf.JobStatusCompleted, status)
		}

		// Verify the result contains the duplicate chapter error
		_, err = engine.GetJobResult(context.Background(), jobKey)
		if err == nil {
			t.Fatal("expected error from GetJobResult, got nil")
		}
		errMsg := err.Error()
		if !containsSubstring(errMsg, "chapter already created") {
			t.Fatalf("expected 'chapter already created' error, got: %v", err)
		}
	})
}

// nonDeterministicJobWorker simulates a job that tries to write the same chapter twice
type nonDeterministicJobWorker struct{}

func (nonDeterministicJobWorker) Name() string { return "non-deterministic" }

func (j nonDeterministicJobWorker) Run(ctx swf.JobContext, data swf.JobData) (swf.JobData, error) {
	// Execute first task normally
	result, err := ctx.DoTask(swf.RunPolicy{}, "add", data)
	if err != nil {
		return nil, err
	}

	// Try to manipulate internal state to execute the same ordinal again
	// This simulates what happens when a workflow is non-deterministic
	if jc, ok := ctx.(*toyJobContext); ok {
		// Save current step
		savedStep := jc.step
		// Reset step to previous value to try to write same chapter again
		jc.step = savedStep - 1
		// This should fail with "chapter already created" error
		_, err := ctx.DoTask(swf.RunPolicy{}, "add", data)
		if err != nil {
			// Expected error - return it so the job completes with error
			return nil, err
		}
		return nil, errors.New("expected chapter already created error but got none")
	}

	return result, nil
}

func containsSubstring(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
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

// jobWorkerWithTransform modifies data between tasks to test that
// GetWaitingTask returns the OUTPUT from the previous step, not the INPUT to the current step
type jobWorkerWithTransform struct {
	missingTask string
}

func (j *jobWorkerWithTransform) Name() string { return "transform-job" }

func (j *jobWorkerWithTransform) Run(ctx swf.JobContext, data swf.JobData) (swf.JobData, error) {
	// Execute first task
	out1, err := ctx.DoTask(swf.RunPolicy{}, "add", data)
	if err != nil {
		return nil, err
	}

	// Transform the output before passing to next task
	// This simulates job-level data transformation between tasks
	n := extractNumber(out1)
	transformed := swf.NewTaskDataOrPanic(map[string]int{"n": n + 100})

	// Execute second task with transformed data
	out2, err := ctx.DoTask(swf.RunPolicy{}, j.missingTask, transformed)
	if err != nil {
		return nil, err
	}

	return out2, nil
}

type simpleJobWorker struct {
	name string
	task string
}

func (j simpleJobWorker) Name() string { return j.name }
func (j simpleJobWorker) Run(ctx swf.JobContext, input swf.JobData) (swf.JobData, error) {
	return ctx.DoTask(swf.RunPolicy{}, j.task, input)
}

type blockingJobWorker struct {
	name    string
	started chan<- struct{}
	release <-chan struct{}
}

func (b blockingJobWorker) Name() string { return b.name }
func (b blockingJobWorker) Run(ctx swf.JobContext, input swf.JobData) (swf.JobData, error) {
	if b.started != nil {
		close(b.started)
	}
	<-b.release
	return input, nil
}

type awaitJobsParentWorker struct {
	name    string
	waitFor []string
}

func (a awaitJobsParentWorker) Name() string { return a.name }
func (a awaitJobsParentWorker) Run(ctx swf.JobContext, input swf.JobData) (swf.JobData, error) {
	if err := ctx.AwaitJobs(a.waitFor...); err != nil {
		return nil, err
	}
	return input, nil
}

type awaitJobsTaskWorker struct {
	name    string
	waitFor []string
}

func (a awaitJobsTaskWorker) Name() string { return a.name }
func (a awaitJobsTaskWorker) Run(ctx swf.TaskContext, input swf.TaskData) (swf.TaskData, error) {
	if err := ctx.AwaitJobs(a.waitFor...); err != nil {
		return nil, err
	}
	return input, nil
}

type dummyTask struct {
	name string
}

func (d dummyTask) Name() string { return d.name }
func (dummyTask) Run(ctx swf.TaskContext, input swf.TaskData) (swf.TaskData, error) {
	return input, nil
}

// Test artifact cleanup behavior
func TestToyEngineArtifactCleanup(t *testing.T) {
	t.Run("cleans up job input artifacts", func(t *testing.T) {
		cleanupCalled := false
		art := swf.NewArtifact("test.txt", func() (io.ReadCloser, int64, error) {
			return io.NopCloser(bytes.NewReader([]byte("test data"))), 9, nil
		}, func() error {
			cleanupCalled = true
			return nil
		})

		ws := mustWorkSet(simplePassThroughJob{}, dummyTask{name: "pass"})
		engine := NewToyEngine([]swf.WorkSet{ws})

		input := &swf.SimpleTaskData{
			Data:      []byte(`{"key":"value"}`),
			Artifacts: []swf.Artifact{art},
		}

		jobKey, err := engine.StartJob(context.Background(), swf.StartJob{
			TenantId: "test-tenant",
			JobType:  ws.JobWorker.Name(),
			Data:     input,
		})
		if err != nil {
			t.Fatalf("StartJob failed: %v", err)
		}

		status, _ := engine.CheckJobStatus(context.Background(), jobKey)
		if status != swf.JobStatusCompleted {
			t.Fatalf("expected job to complete, got status %s", status)
		}

		if !cleanupCalled {
			t.Errorf("expected artifact cleanup to be called")
		}
	})

	t.Run("cleans up task output artifacts", func(t *testing.T) {
		cleanupCalled := false
		art := swf.NewArtifact("output.txt", func() (io.ReadCloser, int64, error) {
			return io.NopCloser(bytes.NewReader([]byte("output data"))), 11, nil
		}, func() error {
			cleanupCalled = true
			return nil
		})

		taskWithArtifact := artifactProducingTask{artifact: art}
		ws := mustWorkSet(simpleJobWorker{name: "artifact-job", task: "produce"}, taskWithArtifact)
		engine := NewToyEngine([]swf.WorkSet{ws})

		input := swf.NewTaskDataOrPanic(map[string]string{"key": "value"})
		jobKey, err := engine.StartJob(context.Background(), swf.StartJob{
			TenantId: "test-tenant",
			JobType:  ws.JobWorker.Name(),
			Data:     input,
		})
		if err != nil {
			t.Fatalf("StartJob failed: %v", err)
		}

		status, _ := engine.CheckJobStatus(context.Background(), jobKey)
		if status != swf.JobStatusCompleted {
			t.Fatalf("expected job to complete, got status %s", status)
		}

		if !cleanupCalled {
			t.Errorf("expected task output artifact cleanup to be called")
		}
	})

	t.Run("materializes artifacts to memory", func(t *testing.T) {
		// Create a temporary file artifact
		tempFile, err := os.CreateTemp("", "test-*.txt")
		if err != nil {
			t.Fatalf("failed to create temp file: %v", err)
		}
		tempPath := tempFile.Name()
		testData := []byte("file content")
		tempFile.Write(testData)
		tempFile.Close()

		art, err := swf.NewArtifactFromFile("file.txt", tempPath)
		if err != nil {
			t.Fatalf("failed to create artifact: %v", err)
		}

		// Job that reads artifact and verifies it's in memory
		verifyJob := &artifactVerifyJob{expectedData: testData}
		ws := mustWorkSet(verifyJob)
		engine := NewToyEngine([]swf.WorkSet{ws})

		input := &swf.SimpleTaskData{
			Data:      []byte(`{}`),
			Artifacts: []swf.Artifact{art},
		}

		jobKey, err := engine.StartJob(context.Background(), swf.StartJob{
			TenantId: "test-tenant",
			JobType:  ws.JobWorker.Name(),
			Data:     input,
		})
		if err != nil {
			t.Fatalf("StartJob failed: %v", err)
		}

		status, _ := engine.CheckJobStatus(context.Background(), jobKey)
		if status != swf.JobStatusCompleted {
			t.Fatalf("expected job to complete, got status %s", status)
		}

		result, err := engine.GetJobResult(context.Background(), jobKey)
		if err != nil {
			t.Fatalf("GetJobResult failed: %v", err)
		}

		var resultData map[string]bool
		resultBytes, _ := result.GetData()
		json.Unmarshal(resultBytes, &resultData)
		if !resultData["verified"] {
			t.Errorf("artifact was not properly materialized to memory")
		}

		// Verify original file was cleaned up
		if _, err := os.Stat(tempPath); !os.IsNotExist(err) {
			t.Errorf("expected temp file to be cleaned up, but it still exists")
		}
	})

	t.Run("cleans up artifacts on job failure", func(t *testing.T) {
		cleanupCalled := false
		art := swf.NewArtifact("test.txt", func() (io.ReadCloser, int64, error) {
			return io.NopCloser(bytes.NewReader([]byte("test data"))), 9, nil
		}, func() error {
			cleanupCalled = true
			return nil
		})

		ws := mustWorkSet(failingJob{}, dummyTask{name: "dummy"})
		engine := NewToyEngine([]swf.WorkSet{ws})

		input := &swf.SimpleTaskData{
			Data:      []byte(`{}`),
			Artifacts: []swf.Artifact{art},
		}

		jobKey, err := engine.StartJob(context.Background(), swf.StartJob{
			TenantId: "test-tenant",
			JobType:  ws.JobWorker.Name(),
			Data:     input,
		})
		if err != nil {
			t.Fatalf("StartJob failed: %v", err)
		}

		status, _ := engine.CheckJobStatus(context.Background(), jobKey)
		if status != swf.JobStatusCompleted {
			t.Fatalf("expected job to complete (with error), got status %s", status)
		}

		if !cleanupCalled {
			t.Errorf("expected artifact cleanup to be called even on failure")
		}
	})

	t.Run("cleans up external task completion artifacts", func(t *testing.T) {
		cleanupCalled := false
		art := swf.NewArtifact("external.txt", func() (io.ReadCloser, int64, error) {
			return io.NopCloser(bytes.NewReader([]byte("external data"))), 13, nil
		}, func() error {
			cleanupCalled = true
			return nil
		})

		jobWorker := simpleJobWorker{name: "external-job", task: "external-task"}
		ws := mustWorkSet(jobWorker, dummyTask{name: "other"})
		engine := NewToyEngine([]swf.WorkSet{ws})

		done := make(chan struct{})
		go func() {
			defer close(done)
			_, _ = engine.StartJob(context.Background(), swf.StartJob{
				TenantId: "test-tenant",
				JobType:  ws.JobWorker.Name(),
				Data:     swf.NewTaskDataOrPanic(map[string]int{"n": 1}),
			})
		}()

		// Wait for pending task
		var handles []swf.TaskHandle
		deadline := time.Now().Add(2 * time.Second)
		for time.Now().Before(deadline) {
			var err error
			handles, err = engine.FindTasksWaitingForCapability(context.Background(), jobWorker.name, jobWorker.task, nil)
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

		// Complete with artifact
		completionData := &swf.SimpleTaskData{
			Data:      []byte(`{"result":"done"}`),
			Artifacts: []swf.Artifact{art},
		}
		err := handles[0].Finish(context.Background(), completionData)
		if err != nil {
			t.Fatalf("Finish failed: %v", err)
		}

		<-done

		if !cleanupCalled {
			t.Errorf("expected external task artifact cleanup to be called")
		}
	})
}

// Helper job workers for artifact tests
type simplePassThroughJob struct{}

func (simplePassThroughJob) Name() string { return "pass-through" }
func (simplePassThroughJob) Run(ctx swf.JobContext, input swf.JobData) (swf.JobData, error) {
	return input, nil
}

type artifactProducingTask struct {
	artifact swf.Artifact
}

func (artifactProducingTask) Name() string { return "produce" }
func (t artifactProducingTask) Run(ctx swf.TaskContext, input swf.TaskData) (swf.TaskData, error) {
	return &swf.SimpleTaskData{
		Data:      []byte(`{"produced":true}`),
		Artifacts: []swf.Artifact{t.artifact},
	}, nil
}

type artifactVerifyJob struct {
	expectedData []byte
}

func (artifactVerifyJob) Name() string { return "verify" }
func (j *artifactVerifyJob) Run(ctx swf.JobContext, input swf.JobData) (swf.JobData, error) {
	artifacts, err := input.GetArtifacts()
	if err != nil {
		return nil, err
	}
	if len(artifacts) != 1 {
		return swf.NewTaskDataOrPanic(map[string]bool{"verified": false}), nil
	}

	// Verify artifact is in memory (bytesArtifact type)
	data, err := artifacts[0].Bytes(context.Background())
	if err != nil {
		return nil, err
	}

	verified := bytes.Equal(data, j.expectedData)
	return swf.NewTaskDataOrPanic(map[string]bool{"verified": verified}), nil
}

type failingJob struct{}

func (failingJob) Name() string { return "failing" }
func (failingJob) Run(ctx swf.JobContext, input swf.JobData) (swf.JobData, error) {
	return nil, errors.New("intentional failure")
}
