package workflow

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/segmentio/ksuid"
)

type claimedJobRunOptions struct {
	Logger         *slog.Logger
	WorkerID       string
	AwaitThreshold time.Duration
	Observer       ReplayObserver
}

type JobRunStatus string

const (
	JobRunNotLeaseable JobRunStatus = "NOT_LEASEABLE"
	JobRunCompleted    JobRunStatus = "COMPLETED"
	JobRunFailed       JobRunStatus = "FAILED"
	JobRunSuspended    JobRunStatus = "SUSPENDED"
)

type GetJobForRunRequest struct {
	JobKey        JobKey
	JobWorker     JobWorker
	TaskWorkers   []TaskWorker
	WorkerID      string
	LeaseDuration time.Duration
	Logger        *slog.Logger

	// If the job awaits longer than this, the lease is rescheduled instead of
	// sleeping inline. Zero uses the existing default behavior.
	AwaitThreshold time.Duration
}

type JobRunOutcome struct {
	Status        JobRunStatus
	LeaseAcquired bool

	// Set when Status == COMPLETED.
	Output JobData

	// Set when Status == FAILED.
	JobError error

	// Best-effort job state details, mainly useful when Status == SUSPENDED
	// or NOT_LEASEABLE.
	JobStatus         *JobStatus
	NextNeed          *string
	WaitForJobIDs     []string
	MissingCapability *string
}

type JobRunListener interface {
	OnJobStart(event JobStartEvent)
	OnTaskStart(event TaskStartEvent)
	OnTaskEnd(event TaskEndEvent)
	OnJobEnd(event JobEndEvent)
}

type JobRunnable struct {
	ctx                   context.Context
	runtime               WorkflowRuntime
	workset               *WorkSet
	lease                 ExecutionLease
	workerID              string
	logger                *slog.Logger
	awaitThreshold        time.Duration
	supportedCapabilities map[string]struct{}
	jobKey                JobKey

	mu      sync.Mutex
	running bool
	runDone bool
	outcome *JobRunOutcome
	runErr  error
}

func GetJobForRun(ctx context.Context, runtime WorkflowRuntime, req GetJobForRunRequest) (*JobRunnable, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if runtime == nil {
		return nil, fmt.Errorf("workflow runtime is required")
	}
	if req.JobWorker == nil {
		return nil, fmt.Errorf("job worker is required")
	}

	workset, err := AsWorkSet(req.JobWorker, req.TaskWorkers...)
	if err != nil {
		return nil, err
	}

	workerID, err := normalizeRequestedWorkerID(req.WorkerID)
	if err != nil {
		return nil, err
	}

	capabilities := workSetCapabilities(workset)
	capabilitySet := make(map[string]struct{}, len(capabilities))
	for _, capability := range capabilities {
		capabilitySet[capability] = struct{}{}
	}

	lease, err := runtime.GetJobLease(ctx, GetJobLeaseRequest{
		JobKey:        req.JobKey,
		WorkerID:      workerID,
		Capabilities:  capabilities,
		LeaseDuration: req.LeaseDuration,
	})
	if err != nil {
		return nil, err
	}
	if lease == nil {
		outcome, err := classifyJobRunWithoutLease(ctx, runtime, req.JobKey, capabilitySet)
		if err != nil {
			return nil, err
		}
		return &JobRunnable{
			ctx:                   ctx,
			runtime:               runtime,
			workset:               workset,
			workerID:              workerID,
			logger:                req.Logger,
			awaitThreshold:        req.AwaitThreshold,
			supportedCapabilities: capabilitySet,
			jobKey:                req.JobKey,
			runDone:               true,
			outcome:               cloneJobRunOutcomePtr(&outcome),
		}, nil
	}

	jobKey := req.JobKey
	if leaseKey := lease.Job().JobKey; leaseKey != (JobKey{}) {
		jobKey = leaseKey
	}
	return &JobRunnable{
		ctx:                   ctx,
		runtime:               runtime,
		workset:               workset,
		lease:                 lease,
		workerID:              workerID,
		logger:                req.Logger,
		awaitThreshold:        req.AwaitThreshold,
		supportedCapabilities: capabilitySet,
		jobKey:                jobKey,
	}, nil
}

func (r *JobRunnable) JobKey() JobKey {
	if r == nil {
		return JobKey{}
	}
	return r.jobKey
}

func (r *JobRunnable) LeaseAcquired() bool {
	if r == nil {
		return false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.outcome != nil {
		return r.outcome.LeaseAcquired
	}
	return r.lease != nil
}

func (r *JobRunnable) Outcome() (JobRunOutcome, bool) {
	if r == nil {
		return JobRunOutcome{}, false
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.outcome == nil {
		return JobRunOutcome{}, false
	}
	return cloneJobRunOutcome(*r.outcome), true
}

func (r *JobRunnable) Run(listener JobRunListener) (JobRunOutcome, error) {
	if r == nil {
		return JobRunOutcome{}, fmt.Errorf("job runnable is nil")
	}

	r.mu.Lock()
	if r.runDone {
		outcome := JobRunOutcome{}
		if r.outcome != nil {
			outcome = cloneJobRunOutcome(*r.outcome)
		}
		err := r.runErr
		r.mu.Unlock()
		return outcome, err
	}
	if r.running {
		r.mu.Unlock()
		return JobRunOutcome{}, fmt.Errorf("job runnable is already running")
	}
	r.running = true
	lease := r.lease
	ctx := r.ctx
	runtime := r.runtime
	workset := r.workset
	workerID := r.workerID
	logger := r.logger
	awaitThreshold := r.awaitThreshold
	jobKey := r.jobKey
	supportedCapabilities := cloneStringSet(r.supportedCapabilities)
	r.mu.Unlock()

	var observer ReplayObserver
	var asyncListener *asyncJobRunListener
	if listener != nil {
		asyncListener = newAsyncJobRunListener(listener)
		observer = asyncListener
	}

	output, runErr := runClaimedJobLease(ctx, runtime, workset, lease, claimedJobRunOptions{
		Logger:         logger,
		WorkerID:       workerID,
		AwaitThreshold: awaitThreshold,
		Observer:       observer,
	})
	if asyncListener != nil {
		asyncListener.Close()
	}

	outcome, err := classifyJobRunAfterRun(ctx, runtime, jobKey, supportedCapabilities, output, runErr)

	r.mu.Lock()
	r.running = false
	r.runDone = true
	if err == nil {
		r.outcome = cloneJobRunOutcomePtr(&outcome)
	}
	r.runErr = err
	r.mu.Unlock()

	return outcome, err
}

func normalizeRequestedWorkerID(workerID string) (string, error) {
	if workerID != "" {
		return workerID, nil
	}
	return newRuntimeWorkerID()
}

func newRuntimeWorkerID() (string, error) {
	host, err := os.Hostname()
	if err != nil {
		return "", err
	}
	return fmt.Sprintf("%s:%d-%s", host, os.Getpid(), ksuid.New().String()), nil
}

func workSetCapabilities(workset *WorkSet) []string {
	if workset == nil || workset.JobWorker == nil {
		return nil
	}
	capabilities := make([]string, 0, len(workset.TaskWorkers)+1)
	jobType := workset.JobWorker.Name()
	capabilities = append(capabilities, jobType)
	taskTypes := make([]string, 0, len(workset.TaskWorkers))
	for taskType := range workset.TaskWorkers {
		taskTypes = append(taskTypes, taskType)
	}
	sort.Strings(taskTypes)
	for _, taskType := range taskTypes {
		capabilities = append(capabilities, workerCapability(jobType, taskType))
	}
	return capabilities
}

func runClaimedJobLease(ctx context.Context, runtime WorkflowRuntime, workset *WorkSet, lease ExecutionLease, opts claimedJobRunOptions) (JobData, error) {
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}

	payload := workerJobPayload{}
	if raw := lease.Payload(); len(raw) > 0 {
		if err := json.Unmarshal(raw, &payload); err != nil {
			logger.Warn("failed to decode job payload", "job", lease.Job().JobKey, "error", err)
		}
	}
	payload.RunPolicy = normalizeRunPolicy(payload.RunPolicy)

	runner := newWorkerRunner(runtime, workset, lease, workerRunnerOptions{
		Logger:         logger.With("job", lease.Job().JobKey.String(), "capability", lease.Capability()),
		JobPolicy:      payload.RunPolicy,
		WorkerID:       opts.WorkerID,
		Observer:       opts.Observer,
		AwaitThreshold: opts.AwaitThreshold,
	})
	return runner.DoJob(ctx)
}

func classifyJobRunWithoutLease(ctx context.Context, runtime WorkflowRuntime, jobKey JobKey, supportedCapabilities map[string]struct{}) (JobRunOutcome, error) {
	job, err := runtime.GetJob(ctx, jobKey)
	if err != nil {
		return JobRunOutcome{}, err
	}
	if job.Status == JobStatusCompleted || job.Status == JobStatusCancelled {
		return classifyJobRunTerminal(job, nil)
	}

	outcome := JobRunOutcome{
		Status: JobRunNotLeaseable,
	}
	applyJobRunStatus(&outcome, job.Status)
	applyJobRunSummary(ctx, runtime, jobKey, supportedCapabilities, &outcome)

	if job.Status == JobStatusPendingJobs || job.Status == JobStatusAwaitingFuture || outcome.MissingCapability != nil {
		outcome.Status = JobRunSuspended
	}
	return outcome, nil
}

func classifyJobRunAfterRun(ctx context.Context, runtime WorkflowRuntime, jobKey JobKey, supportedCapabilities map[string]struct{}, directOutput JobData, runErr error) (JobRunOutcome, error) {
	job, err := runtime.GetJob(ctx, jobKey)
	if err != nil {
		return JobRunOutcome{}, err
	}
	if job.Status == JobStatusCompleted || job.Status == JobStatusCancelled {
		outcome, err := classifyJobRunTerminal(job, directOutput)
		if err != nil {
			return JobRunOutcome{}, err
		}
		outcome.LeaseAcquired = true
		return outcome, nil
	}
	if runErr != nil {
		return JobRunOutcome{}, fmt.Errorf("job %s did not reach a terminal state after execution: status=%s: %w", jobKey, job.Status, runErr)
	}

	outcome := JobRunOutcome{
		Status:        JobRunSuspended,
		LeaseAcquired: true,
	}
	applyJobRunStatus(&outcome, job.Status)
	applyJobRunSummary(ctx, runtime, jobKey, supportedCapabilities, &outcome)
	return outcome, nil
}

func classifyJobRunTerminal(job JobInfo, directOutput JobData) (JobRunOutcome, error) {
	outcome := JobRunOutcome{}
	applyJobRunStatus(&outcome, job.Status)

	if job.Status == JobStatusCancelled {
		outcome.Status = JobRunFailed
		outcome.JobError = ErrJobCancelled
		return outcome, nil
	}
	if job.Status != JobStatusCompleted {
		return JobRunOutcome{}, fmt.Errorf("job is not terminal: %s", job.Status)
	}

	if job.Data == nil {
		if directOutput != nil {
			outcome.Status = JobRunCompleted
			outcome.Output = directOutput
			return outcome, nil
		}
		return JobRunOutcome{}, fmt.Errorf("completed job result is missing")
	}
	if _, err := job.Data.GetData(); err != nil {
		outcome.Status = JobRunFailed
		outcome.JobError = err
		return outcome, nil
	}

	outcome.Status = JobRunCompleted
	outcome.Output = job.Data
	return outcome, nil
}

func applyJobRunStatus(outcome *JobRunOutcome, status JobStatus) {
	if outcome == nil {
		return
	}
	statusCopy := status
	outcome.JobStatus = &statusCopy
}

func applyJobRunSummary(ctx context.Context, runtime WorkflowRuntime, jobKey JobKey, supportedCapabilities map[string]struct{}, outcome *JobRunOutcome) {
	if outcome == nil {
		return
	}
	summary, err := getJobSummaryForJobRun(ctx, runtime, jobKey)
	if err != nil {
		return
	}

	applyJobRunStatus(outcome, summary.Status)
	outcome.NextNeed = cloneStringPtr(summary.NextNeed)
	outcome.WaitForJobIDs = append([]string(nil), summary.WaitFor...)
	if summary.NextNeed == nil {
		return
	}
	if _, ok := supportedCapabilities[*summary.NextNeed]; ok {
		return
	}
	outcome.MissingCapability = cloneStringPtr(summary.NextNeed)
}

func getJobSummaryForJobRun(ctx context.Context, runtime WorkflowRuntime, jobKey JobKey) (*JobSummary, error) {
	resp, err := runtime.ListJobs(ctx, ListJobsRequest{
		TenantIds: []string{jobKey.TenantId},
		JobKeys:   []JobKey{jobKey},
		PageSize:  1,
	})
	if err != nil {
		return nil, err
	}
	for _, job := range resp.Jobs {
		if job.JobKey != jobKey {
			continue
		}
		summary := job
		return &summary, nil
	}
	return nil, ErrJobNotFound
}

func cloneJobRunOutcome(outcome JobRunOutcome) JobRunOutcome {
	cloned := outcome
	cloned.JobStatus = cloneJobStatusPtr(outcome.JobStatus)
	cloned.NextNeed = cloneStringPtr(outcome.NextNeed)
	cloned.WaitForJobIDs = append([]string(nil), outcome.WaitForJobIDs...)
	cloned.MissingCapability = cloneStringPtr(outcome.MissingCapability)
	return cloned
}

func cloneJobRunOutcomePtr(outcome *JobRunOutcome) *JobRunOutcome {
	if outcome == nil {
		return nil
	}
	cloned := cloneJobRunOutcome(*outcome)
	return &cloned
}

func cloneJobStatusPtr(status *JobStatus) *JobStatus {
	if status == nil {
		return nil
	}
	cloned := *status
	return &cloned
}

func cloneStringSet(values map[string]struct{}) map[string]struct{} {
	if len(values) == 0 {
		return nil
	}
	cloned := make(map[string]struct{}, len(values))
	for key := range values {
		cloned[key] = struct{}{}
	}
	return cloned
}

type asyncJobRunListener struct {
	listener JobRunListener
	mu       sync.Mutex
	cond     *sync.Cond
	queue    []func()
	closed   bool
}

func newAsyncJobRunListener(listener JobRunListener) *asyncJobRunListener {
	if listener == nil {
		return nil
	}
	out := &asyncJobRunListener{listener: listener}
	out.cond = sync.NewCond(&out.mu)
	go out.loop()
	return out
}

func (l *asyncJobRunListener) Close() {
	if l == nil {
		return
	}
	l.mu.Lock()
	l.closed = true
	l.cond.Broadcast()
	l.mu.Unlock()
}

func (l *asyncJobRunListener) OnJobStart(event JobStartEvent) {
	if l == nil {
		return
	}
	evt := event
	l.enqueue(func() {
		l.listener.OnJobStart(evt)
	})
}

func (l *asyncJobRunListener) OnTaskStart(event TaskStartEvent) {
	if l == nil {
		return
	}
	evt := event
	l.enqueue(func() {
		l.listener.OnTaskStart(evt)
	})
}

func (l *asyncJobRunListener) OnTaskEnd(event TaskEndEvent) {
	if l == nil {
		return
	}
	evt := event
	l.enqueue(func() {
		l.listener.OnTaskEnd(evt)
	})
}

func (l *asyncJobRunListener) OnJobEnd(event JobEndEvent) {
	if l == nil {
		return
	}
	evt := event
	l.enqueue(func() {
		l.listener.OnJobEnd(evt)
	})
}

func (l *asyncJobRunListener) enqueue(fn func()) {
	if l == nil || fn == nil {
		return
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.closed {
		return
	}
	l.queue = append(l.queue, fn)
	l.cond.Signal()
}

func (l *asyncJobRunListener) loop() {
	for {
		l.mu.Lock()
		for len(l.queue) == 0 && !l.closed {
			l.cond.Wait()
		}
		if len(l.queue) == 0 && l.closed {
			l.mu.Unlock()
			return
		}
		fn := l.queue[0]
		l.queue = l.queue[1:]
		l.mu.Unlock()
		func() {
			defer func() {
				_ = recover()
			}()
			fn()
		}()
	}
}
