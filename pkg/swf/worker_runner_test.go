package swf

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"sort"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type runnerTestRuntime struct {
	mu sync.Mutex

	chapters   map[JobKey]map[int64]StoredChapter
	artifacts  map[ArtifactRef][]byte
	activeJobs map[JobKey]JobSummary

	putChapterHook     func(PutChapterRequest) error
	checkJobStatusHook func(JobKey) (JobStatus, error)
	listJobsHook       func(ListJobsRequest) (ListJobsResponse, error)
}

type runnerTestJobInfoData struct {
	taskData TaskData
	err      error
}

func (d *runnerTestJobInfoData) GetData() (Data, error) {
	if d.taskData == nil {
		return nil, d.err
	}
	data, err := d.taskData.GetData()
	if err != nil {
		return data, err
	}
	return data, d.err
}

func (d *runnerTestJobInfoData) GetDataOrPanic() Data {
	data, err := d.GetData()
	if err != nil {
		panic(err)
	}
	return data
}

func (d *runnerTestJobInfoData) GetArtifacts() ([]Artifact, error) {
	if d.taskData == nil {
		return nil, d.err
	}
	return d.taskData.GetArtifacts()
}

func (d *runnerTestJobInfoData) TaskDataResult() (TaskData, error) {
	return d.taskData, d.err
}

func newRunnerTestRuntime() *runnerTestRuntime {
	return &runnerTestRuntime{
		chapters:   make(map[JobKey]map[int64]StoredChapter),
		artifacts:  make(map[ArtifactRef][]byte),
		activeJobs: make(map[JobKey]JobSummary),
	}
}

func (r *runnerTestRuntime) SubmitJob(context.Context, SubmitJobRequest) (JobHandle, error) {
	return JobHandle{}, errors.New("unexpected SubmitJob call")
}

func (r *runnerTestRuntime) SubmitRestartJob(context.Context, SubmitRestartJobRequest) (JobHandle, error) {
	return JobHandle{}, errors.New("unexpected SubmitRestartJob call")
}

func (r *runnerTestRuntime) CancelJob(context.Context, CancelJobRequest) error {
	return errors.New("unexpected CancelJob call")
}

func (r *runnerTestRuntime) PollWork(context.Context, PollWorkRequest) ([]ExecutionLease, error) {
	return nil, nil
}

func (r *runnerTestRuntime) GetJobLease(context.Context, GetJobLeaseRequest) (ExecutionLease, error) {
	return nil, errors.New("unexpected GetJobLease call")
}

func (r *runnerTestRuntime) CompleteTaskIfWaiting(context.Context, CompleteTaskIfWaitingRequest) error {
	return errors.New("unexpected CompleteTaskIfWaiting call")
}

func (r *runnerTestRuntime) GetJob(_ context.Context, jobKey JobKey) (JobInfo, error) {
	status, err := r.checkJobStatus(jobKey)
	if err != nil {
		return JobInfo{}, err
	}
	job := JobInfo{
		Status: status,
		Data:   &runnerTestJobInfoData{err: ErrJobNotComplete},
	}
	if status != JobStatusCompleted && status != JobStatusCancelled {
		return job, nil
	}

	r.mu.Lock()
	byOrdinal := r.chapters[jobKey]
	r.mu.Unlock()
	if len(byOrdinal) == 0 {
		return job, nil
	}
	var latest StoredChapter
	haveLatest := false
	for _, chapter := range byOrdinal {
		if !haveLatest || chapter.Ordinal > latest.Ordinal {
			latest = cloneStoredChapterForTest(chapter)
			haveLatest = true
		}
	}
	if !haveLatest {
		return job, nil
	}
	td, payloadErr := storedChapterToTaskData(r, jobKey, latest)
	job.Data = &runnerTestJobInfoData{taskData: td, err: payloadErr}
	return job, nil
}

func (r *runnerTestRuntime) GetJobRun(context.Context, GetJobRunRequest) (GetJobRunResponse, error) {
	return GetJobRunResponse{}, errors.New("unexpected GetJobRun call")
}

func (r *runnerTestRuntime) ListJobs(_ context.Context, req ListJobsRequest) (ListJobsResponse, error) {
	if r.listJobsHook != nil {
		return r.listJobsHook(req)
	}
	r.mu.Lock()
	defer r.mu.Unlock()

	jobs := make([]JobSummary, 0, len(r.activeJobs))
	for key, summary := range r.activeJobs {
		if len(req.TenantIds) > 0 && !containsTenant(req.TenantIds, key.TenantId) {
			continue
		}
		if len(req.JobKeys) > 0 && !containsJobKey(req.JobKeys, key) {
			continue
		}
		jobs = append(jobs, summary)
	}
	return ListJobsResponse{Jobs: jobs}, nil
}

func (r *runnerTestRuntime) ListChapters(_ context.Context, req ListChaptersRequest) ([]StoredChapter, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	byOrdinal := r.chapters[req.JobKey]
	if byOrdinal == nil {
		return nil, ErrJobNotFound
	}
	ordinals := make([]int64, 0, len(byOrdinal))
	for ordinal := range byOrdinal {
		ordinals = append(ordinals, ordinal)
	}
	sort.Slice(ordinals, func(i, j int) bool { return ordinals[i] < ordinals[j] })
	out := make([]StoredChapter, 0, len(ordinals))
	for _, ordinal := range ordinals {
		if ordinal < req.StartOrdinal {
			continue
		}
		if req.EndOrdinal != nil && ordinal > *req.EndOrdinal {
			break
		}
		out = append(out, cloneStoredChapterForTest(byOrdinal[ordinal]))
	}
	return out, nil
}

func (r *runnerTestRuntime) checkJobStatus(jobKey JobKey) (JobStatus, error) {
	if r.checkJobStatusHook != nil {
		return r.checkJobStatusHook(jobKey)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	summary, ok := r.activeJobs[jobKey]
	if !ok {
		return "", ErrJobNotFound
	}
	return summary.Status, nil
}

func (r *runnerTestRuntime) GetChapter(_ context.Context, ref ChapterRef) (StoredChapter, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	byOrdinal := r.chapters[ref.JobKey]
	if byOrdinal == nil {
		return StoredChapter{}, ErrChapterNotFound
	}
	chapter, ok := byOrdinal[ref.Ordinal]
	if !ok {
		return StoredChapter{}, ErrChapterNotFound
	}
	return cloneStoredChapterForTest(chapter), nil
}

func (r *runnerTestRuntime) PutChapter(_ context.Context, req PutChapterRequest) error {
	if r.putChapterHook != nil {
		if err := r.putChapterHook(req); err != nil {
			return err
		}
	}

	chapter := req.Chapter
	if len(req.ArtifactUploads) > 0 {
		stored := make([]StoredArtifact, 0, len(req.ArtifactUploads))
		r.mu.Lock()
		for _, item := range req.ArtifactUploads {
			reader, err := item.Open()
			if err != nil {
				r.mu.Unlock()
				return err
			}
			data, err := io.ReadAll(reader)
			_ = reader.Close()
			if err != nil {
				r.mu.Unlock()
				return err
			}
			digest, err := computeSha256(bytes.NewReader(data))
			if err != nil {
				r.mu.Unlock()
				return err
			}
			ref := ArtifactRef{
				JobKey:  req.Ref.JobKey,
				Ordinal: req.Ref.Ordinal,
				Name:    item.Name,
				Digest:  digest,
			}
			r.artifacts[ref] = append([]byte(nil), data...)
			stored = append(stored, StoredArtifact{
				Name:   item.Name,
				Digest: digest,
				Size:   int64(len(data)),
			})
		}
		r.mu.Unlock()
		chapter.Artifacts = stored
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.chapters[req.Ref.JobKey] == nil {
		r.chapters[req.Ref.JobKey] = make(map[int64]StoredChapter)
	}
	if _, exists := r.chapters[req.Ref.JobKey][req.Ref.Ordinal]; exists {
		return errors.New("chapter already created")
	}
	r.chapters[req.Ref.JobKey][req.Ref.Ordinal] = cloneStoredChapterForTest(chapter)
	if chapter.ChapterType == chapterTypeJobAttemptOutcome {
		delete(r.activeJobs, req.Ref.JobKey)
	}
	return nil
}

func (r *runnerTestRuntime) OpenArtifact(_ context.Context, ref ArtifactRef) (ArtifactReader, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	data, ok := r.artifacts[ref]
	if !ok {
		return nil, errors.New("artifact not found")
	}
	return runnerTestArtifactReader{name: ref.Name, data: append([]byte(nil), data...)}, nil
}

type runnerTestArtifactReader struct {
	name string
	data []byte
}

func (r runnerTestArtifactReader) Open() (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(r.data)), nil
}

func (r runnerTestArtifactReader) Size() int64  { return int64(len(r.data)) }
func (r runnerTestArtifactReader) Name() string { return r.name }

type fakeExecutionLease struct {
	leaseID    string
	job        JobHandle
	capability string
	payload    json.RawMessage

	mu                 sync.Mutex
	keepAliveCalls     int
	stopKeepAliveCalls int
	completeCalls      []CompleteExecutionRequest
	rescheduleCalls    []RescheduleExecutionRequest
	completeErr        error
	rescheduleErr      error
}

func (l *fakeExecutionLease) LeaseID() string { return l.leaseID }
func (l *fakeExecutionLease) Job() JobHandle     { return l.job }
func (l *fakeExecutionLease) Capability() string { return l.capability }
func (l *fakeExecutionLease) Payload() json.RawMessage {
	return append(json.RawMessage(nil), l.payload...)
}

func (l *fakeExecutionLease) KeepAlive(context.Context) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.keepAliveCalls++
	return nil
}

func (l *fakeExecutionLease) StopKeepAlive() {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.stopKeepAliveCalls++
}

func (l *fakeExecutionLease) Complete(_ context.Context, req CompleteExecutionRequest) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.completeCalls = append(l.completeCalls, req)
	return l.completeErr
}

func (l *fakeExecutionLease) Reschedule(_ context.Context, req RescheduleExecutionRequest) error {
	l.mu.Lock()
	defer l.mu.Unlock()
	req.Payload = append(json.RawMessage(nil), req.Payload...)
	l.rescheduleCalls = append(l.rescheduleCalls, req)
	return l.rescheduleErr
}

func (l *fakeExecutionLease) snapshot() (int, int, []CompleteExecutionRequest, []RescheduleExecutionRequest) {
	l.mu.Lock()
	defer l.mu.Unlock()
	complete := append([]CompleteExecutionRequest(nil), l.completeCalls...)
	reschedule := append([]RescheduleExecutionRequest(nil), l.rescheduleCalls...)
	return l.keepAliveCalls, l.stopKeepAliveCalls, complete, reschedule
}

func seedJobStartForTest(t *testing.T, runtime *runnerTestRuntime, jobKey JobKey, jobType string, input TaskData, policy RunPolicy) {
	t.Helper()
	raw, err := input.GetData()
	if err != nil {
		t.Fatalf("get input data: %v", err)
	}
	inputHash, err := computeInputHash(context.Background(), input)
	if err != nil {
		t.Fatalf("compute input hash: %v", err)
	}
	meta := chapterMeta{
		Version:   envelopeVersion,
		Ordinal:   0,
		TaskType:  jobType,
		WorkerID:  "seed",
		CreatedAt: time.Now().UTC(),
		InputHash: inputHash,
		Attempt:   1,
		RunPolicy: &policy,
	}
	metaJSON, err := json.Marshal(meta)
	if err != nil {
		t.Fatalf("marshal metadata: %v", err)
	}
	runtime.mu.Lock()
	runtime.chapters[jobKey] = map[int64]StoredChapter{
		0: {
			Ordinal:     0,
			TaskType:    jobType,
			ChapterType: chapterTypeJobStart,
			PayloadKind: payloadKindApp,
			InputHash:   inputHash,
			CreatedAt:   meta.CreatedAt,
			Metadata:    metaJSON,
			Data:        append(json.RawMessage(nil), raw...),
		},
	}
	runtime.activeJobs[jobKey] = JobSummary{
		JobKey:  jobKey,
		Status:  JobStatusReady,
		JobType: jobType,
	}
	runtime.mu.Unlock()
}

func cloneStoredChapterForTest(ch StoredChapter) StoredChapter {
	out := ch
	out.Metadata = append(json.RawMessage(nil), ch.Metadata...)
	out.Data = append(json.RawMessage(nil), ch.Data...)
	if len(ch.Artifacts) > 0 {
		out.Artifacts = append([]StoredArtifact(nil), ch.Artifacts...)
	}
	return out
}

func containsTenant(items []string, want string) bool {
	for _, item := range items {
		if item == want {
			return true
		}
	}
	return false
}

func containsJobKey(items []JobKey, want JobKey) bool {
	for _, item := range items {
		if item == want {
			return true
		}
	}
	return false
}

func mustWorkSetForRunnerTest(t *testing.T, job JobWorker, tasks ...TaskWorker) *WorkSet {
	t.Helper()
	ws, err := AsWorkSet(job, tasks...)
	if err != nil {
		t.Fatalf("build workset: %v", err)
	}
	return ws
}

type countingJobWorker struct {
	name    string
	counter *atomic.Int32
}

func (w countingJobWorker) Name() string { return w.name }

func (w countingJobWorker) Run(_ JobContext, data JobData) (JobData, error) {
	w.counter.Add(1)
	return data, nil
}

type countingTaskWorker struct {
	name    string
	counter *atomic.Int32
}

func (w countingTaskWorker) Name() string { return w.name }

func (w countingTaskWorker) Run(_ TaskContext, data TaskData) (TaskData, error) {
	w.counter.Add(1)
	return data, nil
}

type singleTaskJob struct {
	name     string
	taskType string
}

func (j singleTaskJob) Name() string { return j.name }

func (j singleTaskJob) Run(ctx JobContext, data JobData) (JobData, error) {
	return ctx.DoTask(RunPolicy{}, j.taskType, data)
}

type failThenSucceedJob struct {
	name         string
	counter      *atomic.Int32
	failAttempts int32
}

func (j failThenSucceedJob) Name() string { return j.name }

func (j failThenSucceedJob) Run(_ JobContext, data JobData) (JobData, error) {
	attempt := j.counter.Add(1)
	if attempt <= j.failAttempts {
		return nil, AppError{Payload: AppErrorPayload{Message: "retry me", Level: "error"}}
	}
	return data, nil
}

type failThenSucceedTask struct {
	name         string
	counter      *atomic.Int32
	failAttempts int32
}

func (t failThenSucceedTask) Name() string { return t.name }

func (t failThenSucceedTask) Run(_ TaskContext, data TaskData) (TaskData, error) {
	attempt := t.counter.Add(1)
	if attempt <= t.failAttempts {
		return nil, AppError{Payload: AppErrorPayload{Message: "retry me", Level: "error"}}
	}
	return data, nil
}

type awaitJobsJob struct {
	name    string
	waitFor []string
}

func (j awaitJobsJob) Name() string { return j.name }

func (j awaitJobsJob) Run(ctx JobContext, data JobData) (JobData, error) {
	if err := ctx.AwaitJobs(j.waitFor...); err != nil {
		return nil, err
	}
	return data, nil
}

type awaitJobsTask struct {
	name    string
	waitFor []string
}

func (t awaitJobsTask) Name() string { return t.name }

func (t awaitJobsTask) Run(ctx TaskContext, data TaskData) (TaskData, error) {
	if err := ctx.AwaitJobs(t.waitFor...); err != nil {
		return nil, err
	}
	return data, nil
}

type awaitDurationJob struct {
	name string
	wait time.Duration
}

func (j awaitDurationJob) Name() string { return j.name }

func (j awaitDurationJob) Run(ctx JobContext, data JobData) (JobData, error) {
	if err := ctx.AwaitDuration(Duration(j.wait)); err != nil {
		return nil, err
	}
	return data, nil
}

type taskTimeoutJob struct {
	name     string
	taskType string
	policy   RunPolicy
}

func (j taskTimeoutJob) Name() string { return j.name }

func (j taskTimeoutJob) Run(ctx JobContext, data JobData) (JobData, error) {
	return ctx.DoTask(j.policy, j.taskType, data)
}

type replayObserverRecorder struct {
	jobStarts  []JobStartEvent
	jobEnds    []JobEndEvent
	taskStarts []TaskStartEvent
	taskEnds   []TaskEndEvent
}

func (r *replayObserverRecorder) OnJobStart(evt JobStartEvent) {
	r.jobStarts = append(r.jobStarts, evt)
}
func (r *replayObserverRecorder) OnJobEnd(evt JobEndEvent) { r.jobEnds = append(r.jobEnds, evt) }
func (r *replayObserverRecorder) OnTaskStart(evt TaskStartEvent) {
	r.taskStarts = append(r.taskStarts, evt)
}
func (r *replayObserverRecorder) OnTaskEnd(evt TaskEndEvent) { r.taskEnds = append(r.taskEnds, evt) }

func runRunnerAsync(ctx context.Context, runner *workerRunner) (<-chan struct{}, <-chan error) {
	done := make(chan struct{})
	errCh := make(chan error, 1)
	go func() {
		defer close(done)
		_, err := runner.DoJob(ctx)
		errCh <- err
	}()
	return done, errCh
}

func TestWorkerRunnerJobRestartUsesCache(t *testing.T) {
	runtime := newRunnerTestRuntime()
	jobKey := JobKey{TenantId: "tenant", JobId: "job-cache"}
	var count atomic.Int32
	job := countingJobWorker{name: "job-cache", counter: &count}
	ws := mustWorkSetForRunnerTest(t, job)
	input := NewTaskDataOrPanic(map[string]string{"ok": "yes"})
	seedJobStartForTest(t, runtime, jobKey, job.Name(), input, RunPolicy{})

	lease1 := &fakeExecutionLease{job: JobHandle{JobKey: jobKey}, capability: job.Name(), payload: json.RawMessage(`{}`)}
	runner1 := newWorkerRunner(runtime, ws, lease1, workerRunnerOptions{JobPolicy: RunPolicy{}})
	if _, err := runner1.DoJob(context.Background()); err != nil {
		t.Fatalf("first do job: %v", err)
	}

	lease2 := &fakeExecutionLease{job: JobHandle{JobKey: jobKey}, capability: job.Name(), payload: json.RawMessage(`{}`)}
	runner2 := newWorkerRunner(runtime, ws, lease2, workerRunnerOptions{JobPolicy: RunPolicy{}})
	if _, err := runner2.DoJob(context.Background()); err != nil {
		t.Fatalf("second do job: %v", err)
	}

	if got := count.Load(); got != 1 {
		t.Fatalf("expected 1 execution, got %d", got)
	}
}

func TestWorkerRunnerTaskRestartUsesCache(t *testing.T) {
	runtime := newRunnerTestRuntime()
	jobKey := JobKey{TenantId: "tenant", JobId: "task-cache"}
	var count atomic.Int32
	job := singleTaskJob{name: "task-cache", taskType: "count"}
	task := countingTaskWorker{name: "count", counter: &count}
	ws := mustWorkSetForRunnerTest(t, job, task)
	input := NewTaskDataOrPanic(map[string]int{"n": 1})
	seedJobStartForTest(t, runtime, jobKey, job.Name(), input, RunPolicy{})

	lease1 := &fakeExecutionLease{job: JobHandle{JobKey: jobKey}, capability: job.Name(), payload: json.RawMessage(`{}`)}
	runner1 := newWorkerRunner(runtime, ws, lease1, workerRunnerOptions{JobPolicy: RunPolicy{}})
	if _, err := runner1.DoJob(context.Background()); err != nil {
		t.Fatalf("first do job: %v", err)
	}

	lease2 := &fakeExecutionLease{job: JobHandle{JobKey: jobKey}, capability: job.Name(), payload: json.RawMessage(`{}`)}
	runner2 := newWorkerRunner(runtime, ws, lease2, workerRunnerOptions{JobPolicy: RunPolicy{}})
	if _, err := runner2.DoJob(context.Background()); err != nil {
		t.Fatalf("second do job: %v", err)
	}

	if got := count.Load(); got != 1 {
		t.Fatalf("expected 1 task execution, got %d", got)
	}
}

func TestWorkerRunnerJobRetryWithFailures(t *testing.T) {
	runtime := newRunnerTestRuntime()
	jobKey := JobKey{TenantId: "tenant", JobId: "job-retry"}
	var count atomic.Int32
	job := failThenSucceedJob{name: "job-retry", counter: &count, failAttempts: 1}
	ws := mustWorkSetForRunnerTest(t, job)
	input := NewTaskDataOrPanic(map[string]int{"n": 1})
	policy := RunPolicy{Retry: RetryPolicy{MaximumAttempts: 3, BackoffCoefficient: 1}}
	seedJobStartForTest(t, runtime, jobKey, job.Name(), input, policy)

	lease := &fakeExecutionLease{job: JobHandle{JobKey: jobKey}, capability: job.Name(), payload: json.RawMessage(`{}`)}
	runner := newWorkerRunner(runtime, ws, lease, workerRunnerOptions{JobPolicy: policy})
	if _, err := runner.DoJob(context.Background()); err != nil {
		t.Fatalf("do job: %v", err)
	}

	if got := count.Load(); got != 2 {
		t.Fatalf("expected 2 attempts, got %d", got)
	}
	ch1, err := runtime.GetChapter(context.Background(), ChapterRef{JobKey: jobKey, Ordinal: 1})
	if err != nil {
		t.Fatalf("chapter 1: %v", err)
	}
	if ch1.PayloadKind != payloadKindAppError {
		t.Fatalf("expected app error chapter, got %s", ch1.PayloadKind)
	}
	ch2, err := runtime.GetChapter(context.Background(), ChapterRef{JobKey: jobKey, Ordinal: 2})
	if err != nil {
		t.Fatalf("chapter 2: %v", err)
	}
	if ch2.PayloadKind != payloadKindApp {
		t.Fatalf("expected success chapter, got %s", ch2.PayloadKind)
	}
}

func TestWorkerRunnerTaskRetryWithFailures(t *testing.T) {
	runtime := newRunnerTestRuntime()
	jobKey := JobKey{TenantId: "tenant", JobId: "task-retry"}
	var count atomic.Int32
	task := failThenSucceedTask{name: "retry-task", counter: &count, failAttempts: 1}
	job := taskTimeoutJob{
		name:     "task-retry",
		taskType: task.Name(),
		policy:   RunPolicy{Retry: RetryPolicy{MaximumAttempts: 3, BackoffCoefficient: 1}},
	}
	ws := mustWorkSetForRunnerTest(t, job, task)
	input := NewTaskDataOrPanic(map[string]int{"n": 1})
	seedJobStartForTest(t, runtime, jobKey, job.Name(), input, RunPolicy{})

	lease := &fakeExecutionLease{job: JobHandle{JobKey: jobKey}, capability: job.Name(), payload: json.RawMessage(`{}`)}
	runner := newWorkerRunner(runtime, ws, lease, workerRunnerOptions{JobPolicy: RunPolicy{}})
	if _, err := runner.DoJob(context.Background()); err != nil {
		t.Fatalf("do job: %v", err)
	}

	if got := count.Load(); got != 2 {
		t.Fatalf("expected 2 task attempts, got %d", got)
	}
	ch1, err := runtime.GetChapter(context.Background(), ChapterRef{JobKey: jobKey, Ordinal: 1})
	if err != nil {
		t.Fatalf("chapter 1: %v", err)
	}
	if ch1.PayloadKind != payloadKindAppError {
		t.Fatalf("expected app error chapter, got %s", ch1.PayloadKind)
	}
	ch2, err := runtime.GetChapter(context.Background(), ChapterRef{JobKey: jobKey, Ordinal: 2})
	if err != nil {
		t.Fatalf("chapter 2: %v", err)
	}
	if ch2.PayloadKind != payloadKindApp {
		t.Fatalf("expected success chapter, got %s", ch2.PayloadKind)
	}
}

func TestWorkerRunnerAwaitJobsReschedulesAndExits(t *testing.T) {
	runtime := newRunnerTestRuntime()
	parent := JobKey{TenantId: "tenant", JobId: "parent"}
	child := JobKey{TenantId: "tenant", JobId: "child"}
	runtime.activeJobs[child] = JobSummary{JobKey: child, Status: JobStatusReady, JobType: "child"}

	job := awaitJobsJob{name: "parent-job", waitFor: []string{child.JobId}}
	ws := mustWorkSetForRunnerTest(t, job)
	seedJobStartForTest(t, runtime, parent, job.Name(), NewTaskDataOrPanic(map[string]int{"n": 1}), RunPolicy{})

	lease := &fakeExecutionLease{job: JobHandle{JobKey: parent}, capability: job.Name(), payload: json.RawMessage(`{}`)}
	runner := newWorkerRunner(runtime, ws, lease, workerRunnerOptions{JobPolicy: RunPolicy{}})
	done, errCh := runRunnerAsync(context.Background(), runner)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("runner did not exit")
	}
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	default:
	}

	_, _, _, reschedules := lease.snapshot()
	if len(reschedules) != 1 {
		t.Fatalf("expected 1 reschedule, got %d", len(reschedules))
	}
	if got := reschedules[0].WaitForJobIDs; len(got) != 1 || got[0] != child.JobId {
		t.Fatalf("unexpected wait_for %+v", got)
	}
}

func TestWorkerRunnerTaskAwaitJobsReschedulesAndExits(t *testing.T) {
	runtime := newRunnerTestRuntime()
	parent := JobKey{TenantId: "tenant", JobId: "parent-task"}
	child := JobKey{TenantId: "tenant", JobId: "child-task"}
	runtime.activeJobs[child] = JobSummary{JobKey: child, Status: JobStatusReady, JobType: "child"}

	task := awaitJobsTask{name: "await-task", waitFor: []string{child.JobId}}
	job := singleTaskJob{name: "parent-task", taskType: task.Name()}
	ws := mustWorkSetForRunnerTest(t, job, task)
	seedJobStartForTest(t, runtime, parent, job.Name(), NewTaskDataOrPanic(map[string]int{"n": 1}), RunPolicy{})

	lease := &fakeExecutionLease{job: JobHandle{JobKey: parent}, capability: job.Name(), payload: json.RawMessage(`{}`)}
	runner := newWorkerRunner(runtime, ws, lease, workerRunnerOptions{JobPolicy: RunPolicy{}})
	done, errCh := runRunnerAsync(context.Background(), runner)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("runner did not exit")
	}
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	default:
	}

	_, _, _, reschedules := lease.snapshot()
	if len(reschedules) != 1 {
		t.Fatalf("expected 1 reschedule, got %d", len(reschedules))
	}
	if got := reschedules[0].WaitForJobIDs; len(got) != 1 || got[0] != child.JobId {
		t.Fatalf("unexpected wait_for %+v", got)
	}
}

func TestWorkerRunnerAwaitDurationRecycleReschedulesAndExits(t *testing.T) {
	runtime := newRunnerTestRuntime()
	jobKey := JobKey{TenantId: "tenant", JobId: "await-duration"}
	job := awaitDurationJob{name: "await-job", wait: 2 * time.Second}
	ws := mustWorkSetForRunnerTest(t, job)
	seedJobStartForTest(t, runtime, jobKey, job.Name(), NewTaskDataOrPanic(map[string]int{"n": 1}), RunPolicy{})

	lease := &fakeExecutionLease{job: JobHandle{JobKey: jobKey}, capability: job.Name(), payload: json.RawMessage(`{}`)}
	runner := newWorkerRunner(runtime, ws, lease, workerRunnerOptions{JobPolicy: RunPolicy{}, AwaitThreshold: 50 * time.Millisecond})
	done, errCh := runRunnerAsync(context.Background(), runner)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("runner did not exit")
	}
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	default:
	}

	_, _, _, reschedules := lease.snapshot()
	if len(reschedules) != 1 {
		t.Fatalf("expected 1 reschedule, got %d", len(reschedules))
	}
	if reschedules[0].WaitUntil == nil {
		t.Fatal("expected wait_until to be set")
	}
}

func TestWorkerRunnerRescheduleSetsAlternateNeedFromInvocationTimeout(t *testing.T) {
	runtime := newRunnerTestRuntime()
	jobKey := JobKey{TenantId: "tenant", JobId: "alternate-need"}
	missingTask := "missing"
	job := taskTimeoutJob{
		name:     "alternate-need",
		taskType: missingTask,
		policy:   RunPolicy{InvocationTimeout: AsDuration(2 * time.Second)},
	}
	ws := mustWorkSetForRunnerTest(t, job)
	seedJobStartForTest(t, runtime, jobKey, job.Name(), NewTaskDataOrPanic(map[string]int{"n": 1}), RunPolicy{})

	lease := &fakeExecutionLease{job: JobHandle{JobKey: jobKey}, capability: job.Name(), payload: json.RawMessage(`{}`)}
	runner := newWorkerRunner(runtime, ws, lease, workerRunnerOptions{JobPolicy: RunPolicy{}})
	done, errCh := runRunnerAsync(context.Background(), runner)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("runner did not exit")
	}
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	default:
	}

	_, _, _, reschedules := lease.snapshot()
	if len(reschedules) != 1 {
		t.Fatalf("expected 1 reschedule, got %d", len(reschedules))
	}
	if reschedules[0].AlternateNeed != job.Name() {
		t.Fatalf("expected alternate need %q, got %q", job.Name(), reschedules[0].AlternateNeed)
	}
	if reschedules[0].AlternateAfter == nil || time.Duration(*reschedules[0].AlternateAfter) != 2*time.Second {
		t.Fatalf("unexpected alternate after %+v", reschedules[0].AlternateAfter)
	}
}

func TestWorkerRunnerLeaseLossOnMissingTaskExitsWithoutFailure(t *testing.T) {
	runtime := newRunnerTestRuntime()
	jobKey := JobKey{TenantId: "tenant", JobId: "lease-loss-missing-task"}
	job := taskTimeoutJob{
		name:     "lease-loss-missing-task",
		taskType: "missing",
	}
	ws := mustWorkSetForRunnerTest(t, job)
	seedJobStartForTest(t, runtime, jobKey, job.Name(), NewTaskDataOrPanic(map[string]int{"n": 1}), RunPolicy{})

	lease := &fakeExecutionLease{
		job:           JobHandle{JobKey: jobKey},
		capability:    job.Name(),
		payload:       json.RawMessage(`{}`),
		rescheduleErr: ErrExecutionLeaseLost,
	}
	runner := newWorkerRunner(runtime, ws, lease, workerRunnerOptions{JobPolicy: RunPolicy{}})
	done, errCh := runRunnerAsync(context.Background(), runner)
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("runner did not exit")
	}
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("unexpected error: %v", err)
		}
	default:
	}

	_, _, completeCalls, reschedules := lease.snapshot()
	if len(reschedules) != 1 {
		t.Fatalf("expected 1 reschedule attempt, got %d", len(reschedules))
	}
	if len(completeCalls) != 0 {
		t.Fatalf("expected no complete calls, got %d", len(completeCalls))
	}
	if _, err := runtime.GetChapter(context.Background(), ChapterRef{JobKey: jobKey, Ordinal: 1}); !errors.Is(err, ErrChapterNotFound) {
		t.Fatalf("expected no persisted chapter, got %v", err)
	}
}

func TestWorkerRunnerIgnoresLeaseLossOnComplete(t *testing.T) {
	runtime := newRunnerTestRuntime()
	jobKey := JobKey{TenantId: "tenant", JobId: "lease-loss-complete"}
	job := countingJobWorker{name: "lease-loss-complete", counter: &atomic.Int32{}}
	ws := mustWorkSetForRunnerTest(t, job)
	seedJobStartForTest(t, runtime, jobKey, job.Name(), NewTaskDataOrPanic(map[string]int{"n": 1}), RunPolicy{})

	lease := &fakeExecutionLease{
		job:         JobHandle{JobKey: jobKey},
		capability:  job.Name(),
		payload:     json.RawMessage(`{}`),
		completeErr: ErrExecutionLeaseLost,
	}
	runner := newWorkerRunner(runtime, ws, lease, workerRunnerOptions{JobPolicy: RunPolicy{}})
	if _, err := runner.DoJob(context.Background()); err != nil {
		t.Fatalf("do job: %v", err)
	}

	_, _, completeCalls, _ := lease.snapshot()
	if len(completeCalls) != 1 {
		t.Fatalf("expected 1 complete call, got %d", len(completeCalls))
	}
	if _, err := runtime.GetChapter(context.Background(), ChapterRef{JobKey: jobKey, Ordinal: 1}); err != nil {
		t.Fatalf("expected persisted output chapter: %v", err)
	}
}

func TestWorkerRunnerDoesNotCompleteLeaseOnPersistFailure(t *testing.T) {
	runtime := newRunnerTestRuntime()
	runtime.putChapterHook = func(req PutChapterRequest) error {
		if req.Chapter.ChapterType == chapterTypeJobAttemptOutcome {
			return errors.New("save failed")
		}
		return nil
	}
	jobKey := JobKey{TenantId: "tenant", JobId: "persist-failure"}
	job := countingJobWorker{name: "persist-failure", counter: &atomic.Int32{}}
	ws := mustWorkSetForRunnerTest(t, job)
	seedJobStartForTest(t, runtime, jobKey, job.Name(), NewTaskDataOrPanic(map[string]int{"n": 1}), RunPolicy{})

	lease := &fakeExecutionLease{job: JobHandle{JobKey: jobKey}, capability: job.Name(), payload: json.RawMessage(`{}`)}
	runner := newWorkerRunner(runtime, ws, lease, workerRunnerOptions{JobPolicy: RunPolicy{}})
	if _, err := runner.DoJob(context.Background()); err == nil {
		t.Fatal("expected persist error")
	}

	_, _, completeCalls, _ := lease.snapshot()
	if len(completeCalls) != 0 {
		t.Fatalf("expected no complete calls, got %d", len(completeCalls))
	}
}

func TestWorkerRunnerStopsKeepAliveOnExit(t *testing.T) {
	runtime := newRunnerTestRuntime()
	jobKey := JobKey{TenantId: "tenant", JobId: "keepalive-stop"}
	job := countingJobWorker{name: "keepalive-stop", counter: &atomic.Int32{}}
	ws := mustWorkSetForRunnerTest(t, job)
	seedJobStartForTest(t, runtime, jobKey, job.Name(), NewTaskDataOrPanic(map[string]int{"n": 1}), RunPolicy{})

	lease := &fakeExecutionLease{job: JobHandle{JobKey: jobKey}, capability: job.Name(), payload: json.RawMessage(`{}`)}
	runner := newWorkerRunner(runtime, ws, lease, workerRunnerOptions{JobPolicy: RunPolicy{}})
	if _, err := runner.DoJob(context.Background()); err != nil {
		t.Fatalf("do job: %v", err)
	}

	keepAliveCalls, stopCalls, _, _ := lease.snapshot()
	if keepAliveCalls != 1 {
		t.Fatalf("expected 1 keepalive call, got %d", keepAliveCalls)
	}
	if stopCalls != 1 {
		t.Fatalf("expected 1 stop keepalive call, got %d", stopCalls)
	}
}

func TestReplayObserverUsesCachedChapterTimes(t *testing.T) {
	runtime := newRunnerTestRuntime()
	jobKey := JobKey{TenantId: "tenant", JobId: "replay-times"}
	job := singleTaskJob{name: "replay-times", taskType: "echo"}
	task := countingTaskWorker{name: "echo", counter: &atomic.Int32{}}
	ws := mustWorkSetForRunnerTest(t, job, task)
	seedJobStartForTest(t, runtime, jobKey, job.Name(), NewTaskDataOrPanic(map[string]int{"n": 1}), RunPolicy{})

	lease := &fakeExecutionLease{job: JobHandle{JobKey: jobKey}, capability: job.Name(), payload: json.RawMessage(`{}`)}
	runner := newWorkerRunner(runtime, ws, lease, workerRunnerOptions{JobPolicy: RunPolicy{}})
	if _, err := runner.DoJob(context.Background()); err != nil {
		t.Fatalf("do job: %v", err)
	}

	engine, err := newWorkerEngine(runtime, []WorkSet{*ws}, RuntimeBuildOptions{})
	if err != nil {
		t.Fatalf("new worker engine: %v", err)
	}
	observer := &replayObserverRecorder{}
	if _, err := engine.ReplayJobRun(context.Background(), ReplayRunRequest{JobKey: jobKey, Observer: observer}); err != nil {
		t.Fatalf("replay: %v", err)
	}
	if len(observer.jobStarts) != 1 || len(observer.jobEnds) != 1 || len(observer.taskStarts) != 1 || len(observer.taskEnds) != 1 {
		t.Fatalf("unexpected replay events: %+v", observer)
	}

	startChapter, _ := runtime.GetChapter(context.Background(), ChapterRef{JobKey: jobKey, Ordinal: 0})
	taskChapter, _ := runtime.GetChapter(context.Background(), ChapterRef{JobKey: jobKey, Ordinal: 1})
	jobChapter, _ := runtime.GetChapter(context.Background(), ChapterRef{JobKey: jobKey, Ordinal: 2})
	startMeta, _ := storedChapterMeta(startChapter)
	taskMeta, _ := storedChapterMeta(taskChapter)
	jobMeta, _ := storedChapterMeta(jobChapter)
	if !observer.jobStarts[0].At.Equal(startMeta.CreatedAt) {
		t.Fatalf("job start mismatch: got %v want %v", observer.jobStarts[0].At, startMeta.CreatedAt)
	}
	if !observer.taskStarts[0].At.Equal(metaStartAt(taskMeta)) {
		t.Fatalf("task start mismatch: got %v want %v", observer.taskStarts[0].At, metaStartAt(taskMeta))
	}
	if !observer.taskEnds[0].At.Equal(metaEndAt(taskMeta)) {
		t.Fatalf("task end mismatch: got %v want %v", observer.taskEnds[0].At, metaEndAt(taskMeta))
	}
	if !observer.jobEnds[0].At.Equal(metaEndAt(jobMeta)) {
		t.Fatalf("job end mismatch: got %v want %v", observer.jobEnds[0].At, metaEndAt(jobMeta))
	}
}
