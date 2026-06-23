package workflow

import (
	"context"
	"errors"
	"testing"
	"time"
)

type runJobIfLeaseableStubRuntime struct {
	leaseReq  GetJobLeaseRequest
	leaseResp ExecutionLease
	leaseErr  error
	jobResp   JobInfo
	jobErr    error
	listResp  ListJobsResponse
	listErr   error
}

func (r *runJobIfLeaseableStubRuntime) SubmitJob(context.Context, SubmitJobRequest) (JobHandle, error) {
	return JobHandle{}, errors.New("unexpected SubmitJob call")
}

func (r *runJobIfLeaseableStubRuntime) SubmitRestartJob(context.Context, SubmitRestartJobRequest) (JobHandle, error) {
	return JobHandle{}, errors.New("unexpected SubmitRestartJob call")
}

func (r *runJobIfLeaseableStubRuntime) CancelJob(context.Context, CancelJobRequest) error {
	return errors.New("unexpected CancelJob call")
}

func (r *runJobIfLeaseableStubRuntime) PollWork(context.Context, PollWorkRequest) ([]ExecutionLease, error) {
	return nil, errors.New("unexpected PollWork call")
}

func (r *runJobIfLeaseableStubRuntime) GetJobLease(_ context.Context, req GetJobLeaseRequest) (ExecutionLease, error) {
	r.leaseReq = req
	return r.leaseResp, r.leaseErr
}

func (r *runJobIfLeaseableStubRuntime) CompleteTaskIfWaiting(context.Context, CompleteTaskIfWaitingRequest) error {
	return errors.New("unexpected CompleteTaskIfWaiting call")
}

func (r *runJobIfLeaseableStubRuntime) GetJob(context.Context, JobKey) (JobInfo, error) {
	return r.jobResp, r.jobErr
}

func (r *runJobIfLeaseableStubRuntime) ListJobs(context.Context, ListJobsRequest) (ListJobsResponse, error) {
	return r.listResp, r.listErr
}

func (r *runJobIfLeaseableStubRuntime) GetChapter(context.Context, ChapterRef) (Chapter, error) {
	return Chapter{}, errors.New("unexpected GetChapter call")
}

func (r *runJobIfLeaseableStubRuntime) ListChapters(context.Context, ListChaptersRequest) ([]Chapter, error) {
	return nil, errors.New("unexpected ListChapters call")
}

func (r *runJobIfLeaseableStubRuntime) PutChapter(context.Context, PutChapterRequest) error {
	return errors.New("unexpected PutChapter call")
}

func (r *runJobIfLeaseableStubRuntime) OpenArtifact(context.Context, ArtifactRef) (ArtifactReader, error) {
	return nil, errors.New("unexpected OpenArtifact call")
}

type runJobIfLeaseableTestJob struct {
	name string
}

func (j runJobIfLeaseableTestJob) Name() string { return j.name }

func (j runJobIfLeaseableTestJob) Run(_ JobContext, data JobData) (JobData, error) {
	return data, nil
}

type runJobIfLeaseableTestTask struct {
	name string
}

func (t runJobIfLeaseableTestTask) Name() string { return t.name }

func (t runJobIfLeaseableTestTask) Run(_ TaskContext, data TaskData) (TaskData, error) {
	return data, nil
}

type runJobIfLeaseableLeaseRuntime struct {
	*runnerTestRuntime
	leaseReq  GetJobLeaseRequest
	leaseResp ExecutionLease
	leaseErr  error
}

func (r *runJobIfLeaseableLeaseRuntime) GetJobLease(_ context.Context, req GetJobLeaseRequest) (ExecutionLease, error) {
	r.leaseReq = req
	return r.leaseResp, r.leaseErr
}

type blockingJobRunListener struct {
	started chan<- struct{}
	unblock <-chan struct{}
	ended   chan<- struct{}
}

func (l blockingJobRunListener) OnJobStart(JobStartEvent) {
	select {
	case l.started <- struct{}{}:
	default:
	}
	<-l.unblock
}

func (blockingJobRunListener) OnTaskStart(TaskStartEvent) {}
func (blockingJobRunListener) OnTaskEnd(TaskEndEvent)     {}
func (l blockingJobRunListener) OnJobEnd(JobEndEvent) {
	select {
	case l.ended <- struct{}{}:
	default:
	}
}

func TestGetJobForRunBuildsLeaseRequest(t *testing.T) {
	jobKey := JobKey{TenantId: "tenant-a", JobId: "job-a"}
	runtime := &runJobIfLeaseableStubRuntime{
		jobResp: JobInfo{Status: JobStatusActive},
	}

	runnable, err := GetJobForRun(context.Background(), runtime, GetJobForRunRequest{
		JobKey:        jobKey,
		JobWorker:     runJobIfLeaseableTestJob{name: "lease-job"},
		TaskWorkers:   []TaskWorker{runJobIfLeaseableTestTask{name: "task-b"}, runJobIfLeaseableTestTask{name: "task-a"}},
		WorkerID:      "worker-123",
		LeaseDuration: 3,
	})
	if err != nil {
		t.Fatalf("get job for run: %v", err)
	}
	if runnable.LeaseAcquired() {
		t.Fatal("expected no lease to be acquired")
	}
	outcome, ok := runnable.Outcome()
	if !ok {
		t.Fatal("expected cached outcome")
	}
	if outcome.Status != JobRunNotLeaseable {
		t.Fatalf("unexpected outcome status %q", outcome.Status)
	}
	if outcome.JobStatus == nil || *outcome.JobStatus != JobStatusActive {
		t.Fatalf("unexpected outcome job status %+v", outcome.JobStatus)
	}

	if runtime.leaseReq.JobKey != jobKey {
		t.Fatalf("unexpected lease request job key %+v", runtime.leaseReq.JobKey)
	}
	if runtime.leaseReq.WorkerID != "worker-123" {
		t.Fatalf("unexpected lease request worker ID %q", runtime.leaseReq.WorkerID)
	}
	if runtime.leaseReq.LeaseDuration != 3 {
		t.Fatalf("unexpected lease duration %s", runtime.leaseReq.LeaseDuration)
	}
	wantCaps := []string{"lease-job", "lease-job:task-a", "lease-job:task-b"}
	if len(runtime.leaseReq.Capabilities) != len(wantCaps) {
		t.Fatalf("unexpected capabilities %+v", runtime.leaseReq.Capabilities)
	}
	for i, capability := range wantCaps {
		if runtime.leaseReq.Capabilities[i] != capability {
			t.Fatalf("unexpected capabilities %+v", runtime.leaseReq.Capabilities)
		}
	}
}

func TestGetJobForRunReturnsCompletedWithoutLeaseForTerminalJob(t *testing.T) {
	jobKey := JobKey{TenantId: "tenant-complete", JobId: "job-complete"}
	output := NewTaskDataOrPanic(map[string]any{"ok": true})
	runtime := &runJobIfLeaseableStubRuntime{
		jobResp: JobInfo{
			Status: JobStatusCompleted,
			Data:   output,
		},
	}

	runnable, err := GetJobForRun(context.Background(), runtime, GetJobForRunRequest{
		JobKey:    jobKey,
		JobWorker: runJobIfLeaseableTestJob{name: "lease-job"},
	})
	if err != nil {
		t.Fatalf("get job for run: %v", err)
	}
	if runnable.LeaseAcquired() {
		t.Fatal("expected no lease to be acquired")
	}
	outcome, err := runnable.Run(nil)
	if err != nil {
		t.Fatalf("run cached runnable: %v", err)
	}
	if outcome.Status != JobRunCompleted {
		t.Fatalf("unexpected outcome status %q", outcome.Status)
	}
	if outcome.Output == nil {
		t.Fatal("expected completed output")
	}
}

func TestGetJobForRunReportsSuspendedMissingCapabilityWithoutLease(t *testing.T) {
	jobKey := JobKey{TenantId: "tenant-suspended", JobId: "job-suspended"}
	runtime := &runJobIfLeaseableStubRuntime{
		jobResp: JobInfo{Status: JobStatusReady},
		listResp: ListJobsResponse{
			Jobs: []JobSummary{{
				JobKey:   jobKey,
				Status:   JobStatusReady,
				JobType:  "lease-job",
				NextNeed: strPtr("lease-job:missing"),
			}},
		},
	}

	runnable, err := GetJobForRun(context.Background(), runtime, GetJobForRunRequest{
		JobKey:    jobKey,
		JobWorker: runJobIfLeaseableTestJob{name: "lease-job"},
	})
	if err != nil {
		t.Fatalf("get job for run: %v", err)
	}
	if runnable.LeaseAcquired() {
		t.Fatal("expected no lease to be acquired")
	}
	outcome, ok := runnable.Outcome()
	if !ok {
		t.Fatal("expected cached outcome")
	}
	if outcome.Status != JobRunSuspended {
		t.Fatalf("unexpected outcome status %q", outcome.Status)
	}
	if outcome.MissingCapability == nil || *outcome.MissingCapability != "lease-job:missing" {
		t.Fatalf("unexpected missing capability %+v", outcome.MissingCapability)
	}
	if outcome.NextNeed == nil || *outcome.NextNeed != "lease-job:missing" {
		t.Fatalf("unexpected next need %+v", outcome.NextNeed)
	}
}

func TestJobRunnableRunDoesNotBlockOnListener(t *testing.T) {
	jobKey := JobKey{TenantId: "tenant-runnable", JobId: "job-runnable"}
	runtime := &runJobIfLeaseableLeaseRuntime{
		runnerTestRuntime: newRunnerTestRuntime(),
	}
	runtime.leaseResp = &fakeExecutionLease{
		runtime:    runtime.runnerTestRuntime,
		job:        JobHandle{JobKey: jobKey},
		capability: "lease-job",
	}
	runtime.checkJobStatusHook = func(key JobKey) (JobStatus, error) {
		runtime.mu.Lock()
		defer runtime.mu.Unlock()
		if summary, ok := runtime.activeJobs[key]; ok {
			return summary.Status, nil
		}
		chapters := runtime.chapters[key]
		for _, chapter := range chapters {
			if chapterIs(chapter, chapterTypeJobAttemptOutcome) {
				return JobStatusCompleted, nil
			}
		}
		return "", ErrJobNotFound
	}
	seedJobStartForTest(t, runtime.runnerTestRuntime, jobKey, "lease-job", NewTaskDataOrPanic(map[string]any{"ok": true}), RunPolicy{})

	runnable, err := GetJobForRun(context.Background(), runtime, GetJobForRunRequest{
		JobKey:    jobKey,
		JobWorker: runJobIfLeaseableTestJob{name: "lease-job"},
		WorkerID:  "worker-runnable",
	})
	if err != nil {
		t.Fatalf("get job for run: %v", err)
	}
	if !runnable.LeaseAcquired() {
		t.Fatal("expected runnable to acquire a lease")
	}

	started := make(chan struct{}, 1)
	unblock := make(chan struct{})
	ended := make(chan struct{}, 1)
	outcomeCh := make(chan JobRunOutcome, 1)
	errCh := make(chan error, 1)
	go func() {
		outcome, err := runnable.Run(blockingJobRunListener{
			started: started,
			unblock: unblock,
			ended:   ended,
		})
		outcomeCh <- outcome
		errCh <- err
	}()

	select {
	case <-started:
	case <-time.After(2 * time.Second):
		t.Fatal("listener did not receive job start")
	}

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("run job runnable: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("run should complete even while listener is blocked")
	}

	outcome := <-outcomeCh
	if outcome.Status != JobRunCompleted {
		t.Fatalf("unexpected outcome status %q", outcome.Status)
	}

	close(unblock)
	select {
	case <-ended:
	case <-time.After(2 * time.Second):
		t.Fatal("listener did not finish after being unblocked")
	}
}
