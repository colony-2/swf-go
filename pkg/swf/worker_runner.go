package swf

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	goruntime "runtime"
	"sync"
	"sync/atomic"
	"time"
)

type workerRunnerOptions struct {
	Logger         *slog.Logger
	JobPolicy      RunPolicy
	WorkerID       string
	JobKey         JobKey
	Observer       ReplayObserver
	Replay         bool
	AwaitThreshold time.Duration
}

type workerRunner struct {
	runtime WorkflowRuntime
	worker  *WorkSet
	lease   ExecutionLease
	logger  *slog.Logger

	jobPolicy      RunPolicy
	workerID       string
	observer       ReplayObserver
	replay         bool
	awaitThreshold time.Duration

	ctx          context.Context
	jobKey       JobKey
	storyCounter int64

	currentInvocationDeadline time.Time
	currentTotalDeadline      time.Time
	currentInvocationLimit    time.Duration
	currentTotalLimit         time.Duration
	currentInputRef           *InputReference
	currentKind               string
	currentJobAttemptStartAt  time.Time
	lastTaskEndAt             *time.Time
	rescheduled               atomic.Bool
}

type jobExecutionConfig struct {
	retryCfg          RetryPolicy
	invocationTimeout time.Duration
	totalTimeout      time.Duration
	inputRef          *InputReference
	totalDeadline     time.Time
}

type jobResult struct {
	output JobData
	err    error
}

type taskResult struct {
	output TaskData
	err    error
}

func newWorkerRunner(runtime WorkflowRuntime, ws *WorkSet, lease ExecutionLease, opts workerRunnerOptions) *workerRunner {
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	jobKey := JobKey{}
	if lease != nil {
		jobKey = lease.Job().JobKey
	} else if opts.JobKey != (JobKey{}) {
		jobKey = opts.JobKey
	}
	return &workerRunner{
		runtime:        runtime,
		worker:         ws,
		lease:          lease,
		logger:         logger,
		jobPolicy:      normalizeRunPolicy(opts.JobPolicy),
		workerID:       opts.WorkerID,
		observer:       opts.Observer,
		replay:         opts.Replay,
		awaitThreshold: opts.AwaitThreshold,
		jobKey:         jobKey,
		storyCounter:   1,
	}
}

func (r *workerRunner) GetJobKey() JobKey {
	return r.jobKey
}

func (r *workerRunner) Logger() *slog.Logger {
	return r.logger
}

// ManipulateStepForTest is a test hook used by determinism tests to force
// duplicate/changing ordinals against the shared worker runner.
func (r *workerRunner) ManipulateStepForTest(newStep int64) {
	r.storyCounter = newStep
}

func (r *workerRunner) observerOrNoop() ReplayObserver {
	if r.observer != nil {
		return r.observer
	}
	return noopReplayObserver{}
}

func (r *workerRunner) emitJobStart(attempt int, input JobData, at time.Time) {
	r.observerOrNoop().OnJobStart(JobStartEvent{
		JobKey:        r.GetJobKey(),
		AttemptNumber: attempt,
		Input:         input,
		At:            at,
	})
}

func (r *workerRunner) emitJobEnd(attempt int, output JobData, err error, at time.Time) {
	r.observerOrNoop().OnJobEnd(JobEndEvent{
		JobKey:        r.GetJobKey(),
		AttemptNumber: attempt,
		Output:        output,
		Err:           err,
		At:            at,
	})
}

func (r *workerRunner) emitTaskStart(taskType string, ordinal int64, attempt int, input TaskData, at time.Time) {
	r.observerOrNoop().OnTaskStart(TaskStartEvent{
		JobKey:        r.GetJobKey(),
		TaskType:      taskType,
		Ordinal:       ordinal,
		AttemptNumber: attempt,
		Input:         input,
		At:            at,
	})
}

func (r *workerRunner) emitTaskEnd(taskType string, ordinal int64, attempt int, output TaskData, err error, at time.Time) {
	r.lastTaskEndAt = &at
	r.observerOrNoop().OnTaskEnd(TaskEndEvent{
		JobKey:        r.GetJobKey(),
		TaskType:      taskType,
		Ordinal:       ordinal,
		AttemptNumber: attempt,
		Output:        output,
		Err:           err,
		At:            at,
	})
}

func (r *workerRunner) fallbackTaskTime() time.Time {
	if r.lastTaskEndAt != nil {
		return *r.lastTaskEndAt
	}
	if !r.currentJobAttemptStartAt.IsZero() {
		return r.currentJobAttemptStartAt
	}
	return time.Now().UTC()
}

func panicToAppError(rec interface{}) error {
	return &AppError{Payload: AppErrorPayload{Message: fmt.Sprintf("panic: %v", rec), Level: "error"}}
}

func prematureCloseOut() {
	goruntime.Goexit()
}

func metaStartAt(meta chapterMeta) time.Time {
	if meta.StartedAt != nil {
		return meta.StartedAt.UTC()
	}
	return meta.CreatedAt
}

func metaEndAt(meta chapterMeta) time.Time {
	if meta.FinishedAt != nil {
		return meta.FinishedAt.UTC()
	}
	return meta.CreatedAt
}

func (r *workerRunner) getChapter(ctx context.Context, ordinal int64) (StoredChapter, chapterMeta, error) {
	chapter, err := r.runtime.GetChapter(ctx, ChapterRef{
		JobKey:  r.GetJobKey(),
		Ordinal: ordinal,
	})
	if err != nil {
		return StoredChapter{}, chapterMeta{}, err
	}
	meta, err := storedChapterMeta(chapter)
	if err != nil {
		return StoredChapter{}, chapterMeta{}, err
	}
	return chapter, meta, nil
}

func (r *workerRunner) taskTotalDeadline(ctx context.Context, ordinal int64, totalTimeout time.Duration) (time.Time, error) {
	if totalTimeout <= 0 {
		return time.Time{}, nil
	}
	startOrdinal := ordinal - 1
	if startOrdinal < 0 {
		startOrdinal = 0
	}
	_, meta, err := r.getChapter(ctx, startOrdinal)
	if err != nil {
		return time.Time{}, err
	}
	return meta.CreatedAt.Add(totalTimeout), nil
}

func (r *workerRunner) jobTotalDeadline(meta chapterMeta, totalTimeout time.Duration) time.Time {
	if totalTimeout <= 0 {
		return time.Time{}
	}
	return meta.CreatedAt.Add(totalTimeout)
}

func (r *workerRunner) awaitUntil(wakeAt time.Time, ordinal int64, attempt int, kind string, inputRef *InputReference, invocationDeadline time.Time, totalDeadline time.Time, invocationLimit time.Duration, totalLimit time.Duration) error {
	now := time.Now()
	if !totalDeadline.IsZero() && now.After(totalDeadline) {
		return NewTimeoutError(kind, totalLimit, TimeoutScopeTotal, inputRef, false)
	}
	if !invocationDeadline.IsZero() && now.After(invocationDeadline) {
		return NewTimeoutError(kind, invocationLimit, TimeoutScopeInvocation, inputRef, true)
	}
	if !totalDeadline.IsZero() && wakeAt.After(totalDeadline) {
		wakeAt = totalDeadline
	}
	if !invocationDeadline.IsZero() && wakeAt.After(invocationDeadline) {
		wakeAt = invocationDeadline
	}
	if wakeAt.IsZero() || !wakeAt.After(now) {
		return nil
	}
	if r.replay {
		return ReplayCacheMissError{
			JobKey:  r.GetJobKey(),
			Ordinal: ordinal,
			Attempt: attempt,
			Reason:  ReplayCacheMissAwaitNotReady,
		}
	}
	wait := time.Until(wakeAt)
	threshold := r.awaitThreshold
	if threshold <= 0 {
		threshold = 5 * time.Minute
	}
	if r.lease != nil && wait > threshold {
		if err := r.lease.Reschedule(context.TODO(), RescheduleExecutionRequest{
			NextNeed:  r.lease.Capability(),
			WaitUntil: &wakeAt,
			Payload:   r.lease.Payload(),
		}); err != nil {
			if IsExecutionLeaseLost(err) {
				r.rescheduled.Store(true)
				prematureCloseOut()
			}
			return err
		}
		r.rescheduled.Store(true)
		prematureCloseOut()
	}

	timer := time.NewTimer(wait)
	defer timer.Stop()
	select {
	case <-timer.C:
	case <-r.ctx.Done():
		return r.ctx.Err()
	}
	now = time.Now()
	if !totalDeadline.IsZero() && (now.After(totalDeadline) || now.Equal(totalDeadline)) {
		return NewTimeoutError(kind, totalLimit, TimeoutScopeTotal, inputRef, false)
	}
	if !invocationDeadline.IsZero() && (now.After(invocationDeadline) || now.Equal(invocationDeadline)) {
		return NewTimeoutError(kind, invocationLimit, TimeoutScopeInvocation, inputRef, true)
	}
	return nil
}

func (r *workerRunner) awaitJobsComplete(ctx context.Context, jobIds []string) (bool, error) {
	if len(jobIds) == 0 {
		return true, nil
	}
	for _, id := range jobIds {
		if id == "" {
			return false, fmt.Errorf("jobId cannot be empty")
		}
		job, err := r.runtime.GetJob(ctx, JobKey{TenantId: r.GetJobKey().TenantId, JobId: id})
		if errors.Is(err, ErrJobNotFound) {
			continue
		}
		if err != nil {
			return false, err
		}
		if job.Status != JobStatusCompleted && job.Status != JobStatusCancelled {
			return false, nil
		}
	}
	return true, nil
}

func (r *workerRunner) AwaitJobs(jobIds ...string) error {
	if len(jobIds) == 0 {
		return fmt.Errorf("at least one jobId is required")
	}
	if r.replay {
		complete, err := r.awaitJobsComplete(context.Background(), jobIds)
		if err != nil {
			return err
		}
		if complete {
			return nil
		}
		return ReplayCacheMissError{
			JobKey:  r.GetJobKey(),
			Ordinal: r.storyCounter,
			Attempt: 1,
			Reason:  ReplayCacheMissAwaitJobsPending,
		}
	}
	complete, err := r.awaitJobsComplete(context.Background(), jobIds)
	if err != nil {
		return err
	}
	if complete {
		return nil
	}
	if r.lease == nil {
		return fmt.Errorf("awaiting jobs requires an execution lease")
	}
	if err := r.lease.Reschedule(context.TODO(), RescheduleExecutionRequest{
		NextNeed:      r.lease.Capability(),
		WaitForJobIDs: append([]string(nil), jobIds...),
		Payload:       r.lease.Payload(),
	}); err != nil {
		if IsExecutionLeaseLost(err) {
			r.rescheduled.Store(true)
			prematureCloseOut()
		}
		return err
	}
	r.rescheduled.Store(true)
	prematureCloseOut()
	return nil
}

func (r *workerRunner) AwaitDuration(waitFor Duration) error {
	wait := waitFor.ToDuration()
	if wait <= 0 {
		return nil
	}
	kind := r.currentKind
	if kind == "" {
		kind = "task"
	}
	return r.awaitUntil(time.Now().Add(wait), r.storyCounter, 0, kind, r.currentInputRef, r.currentInvocationDeadline, r.currentTotalDeadline, r.currentInvocationLimit, r.currentTotalLimit)
}

func (r *workerRunner) loadInitialChapterAndPolicy(ctx context.Context) (TaskData, StoredChapter, chapterMeta, error) {
	chapter, meta, err := r.getChapter(ctx, 0)
	if err != nil {
		return nil, StoredChapter{}, chapterMeta{}, err
	}
	if chapter.ChapterType != chapterTypeJobStart {
		return nil, StoredChapter{}, chapterMeta{}, fmt.Errorf("%w: unexpected chapter type %q at ordinal 0", ErrWorkflowNotDeterministic, chapter.ChapterType)
	}
	if meta.RunPolicy != nil {
		r.jobPolicy = mergeRunPolicy(*meta.RunPolicy, r.jobPolicy)
	}
	inputData, err := storedChapterToTaskData(r.runtime, r.GetJobKey(), chapter)
	if err != nil {
		return nil, StoredChapter{}, chapterMeta{}, err
	}
	return inputData, chapter, meta, nil
}

func (r *workerRunner) setupJobExecutionConfig(ctx context.Context, inputData TaskData, meta chapterMeta) (jobExecutionConfig, error) {
	retryCfg := r.jobPolicy.Retry
	invocationTimeout := durationPtrToDuration(r.jobPolicy.InvocationTimeout)
	totalTimeout := durationPtrToDuration(r.jobPolicy.TotalTimeout)
	inputHash, err := computeInputHash(ctx, inputData)
	if err != nil {
		return jobExecutionConfig{}, fmt.Errorf("failed to hash job input: %w", err)
	}
	inputRef := &InputReference{Ordinal: 0, Hash: inputHash}
	return jobExecutionConfig{
		retryCfg:          retryCfg,
		invocationTimeout: invocationTimeout,
		totalTimeout:      totalTimeout,
		inputRef:          inputRef,
		totalDeadline:     r.jobTotalDeadline(meta, totalTimeout),
	}, nil
}

func (r *workerRunner) checkTotalTimeoutExceeded(totalDeadline time.Time, totalTimeout time.Duration, inputRef *InputReference) error {
	if !totalDeadline.IsZero() && time.Now().After(totalDeadline) {
		return NewTimeoutError("job", totalTimeout, TimeoutScopeTotal, inputRef, false)
	}
	return nil
}

func (r *workerRunner) setupAttemptDeadlines(invocationTimeout time.Duration, totalDeadline time.Time, totalTimeout time.Duration, inputRef *InputReference) time.Time {
	now := time.Now()
	attemptInvocationDeadline := time.Time{}
	if invocationTimeout > 0 {
		attemptInvocationDeadline = now.Add(invocationTimeout)
	}
	r.currentInvocationDeadline = attemptInvocationDeadline
	r.currentTotalDeadline = totalDeadline
	r.currentInvocationLimit = invocationTimeout
	r.currentTotalLimit = totalTimeout
	r.currentInputRef = inputRef
	r.currentKind = "job"
	return attemptInvocationDeadline
}

func (r *workerRunner) executeJobWorkerAsync(inputData TaskData) chan jobResult {
	resultCh := make(chan jobResult, 1)
	go func() {
		var output JobData
		var jobErr error
		defer func() {
			if rec := recover(); rec != nil {
				jobErr = panicToAppError(rec)
			}
			jobErr = normalizeComparableError(jobErr)
			resultCh <- jobResult{output: output, err: jobErr}
		}()
		output, jobErr = r.worker.JobWorker.Run(r, inputData)
	}()
	return resultCh
}

func (r *workerRunner) waitForJobResultWithDeadline(resultCh chan jobResult, attemptInvocationDeadline, totalDeadline time.Time, invocationTimeout, totalTimeout time.Duration, inputRef *InputReference) (JobData, error) {
	deadline := attemptInvocationDeadline
	if deadline.IsZero() || (!totalDeadline.IsZero() && totalDeadline.Before(deadline)) {
		deadline = totalDeadline
	}

	if deadline.IsZero() {
		res := <-resultCh
		return res.output, res.err
	}

	timer := time.NewTimer(time.Until(deadline))
	defer timer.Stop()
	select {
	case res := <-resultCh:
		return res.output, res.err
	case <-timer.C:
		if !totalDeadline.IsZero() && deadline.Equal(totalDeadline) {
			return nil, NewTimeoutError("job", totalTimeout, TimeoutScopeTotal, inputRef, false)
		}
		return nil, NewTimeoutError("job", invocationTimeout, TimeoutScopeInvocation, inputRef, true)
	}
}

func (r *workerRunner) validatePostExecutionTimeouts(jobErr error, attemptInvocationDeadline, totalDeadline time.Time, invocationTimeout, totalTimeout time.Duration, inputRef *InputReference) error {
	if jobErr != nil {
		return jobErr
	}
	now := time.Now()
	if !totalDeadline.IsZero() && now.After(totalDeadline) {
		return NewTimeoutError("job", totalTimeout, TimeoutScopeTotal, inputRef, false)
	}
	if !attemptInvocationDeadline.IsZero() && now.After(attemptInvocationDeadline) {
		return NewTimeoutError("job", invocationTimeout, TimeoutScopeInvocation, inputRef, true)
	}
	return nil
}

func (r *workerRunner) completeLease(ctx context.Context, err error) {
	if r.lease == nil || r.replay {
		return
	}
	status, detail := completionStatusAndDetail(err)
	if completeErr := r.lease.Complete(ctx, CompleteExecutionRequest{
		Status: status,
		Detail: detail,
	}); completeErr != nil && !IsExecutionLeaseLost(completeErr) {
		r.logger.Warn("complete lease failed", "error", completeErr, "job", r.GetJobKey())
	}
}

func (r *workerRunner) currentLeaseID() string {
	if r.lease == nil {
		return ""
	}
	return r.lease.LeaseID()
}

func (r *workerRunner) shouldCheckPrerequisites() bool {
	return !r.replay && r.lease != nil
}

type prereqFailedError struct {
	app AppError
}

func newPrereqFailedError(message string) error {
	return &prereqFailedError{
		app: AppError{Payload: AppErrorPayload{Message: message, Level: "error"}},
	}
}

func (e *prereqFailedError) Error() string      { return e.app.Error() }
func (e *prereqFailedError) NonRetryable() bool { return true }
func (e *prereqFailedError) Unwrap() error      { return e.app }

func (r *workerRunner) checkPrerequisites(ctx context.Context, meta chapterMeta) error {
	for _, prereq := range meta.Prerequisites {
		if prereq.Condition != JobPrereqSuccess {
			continue
		}
		key := JobKey{TenantId: r.GetJobKey().TenantId, JobId: prereq.JobID}
		job, err := r.runtime.GetJob(ctx, key)
		if err != nil {
			return fmt.Errorf("check prerequisite job %s: %w", prereq.JobID, err)
		}
		if job.Status != JobStatusCompleted {
			return newPrereqFailedError(fmt.Sprintf("prerequisite job %s not completed", prereq.JobID))
		}
		if job.Data == nil {
			return newPrereqFailedError(fmt.Sprintf("prerequisite job %s did not expose result data", prereq.JobID))
		}
		if _, err := job.Data.GetData(); err != nil {
			msg := fmt.Sprintf("prerequisite job %s did not succeed", prereq.JobID)
			if err.Error() != "" {
				msg += ": " + err.Error()
			}
			return newPrereqFailedError(msg)
		}
	}
	return nil
}

func (r *workerRunner) failJobPrerequisites(ctx context.Context, inputData JobData, config jobExecutionConfig, err error) (JobData, error) {
	attempt := 1
	startAt := time.Now().UTC()
	r.emitJobStart(attempt, inputData, startAt)

	ordinal := r.storyCounter
	r.storyCounter++

	payload, artifacts, payloadKind, prepErr := r.prepareJobResultPayload(nil, err, config.inputRef)
	if prepErr != nil {
		return nil, prepErr
	}
	if _, saveErr := r.persistJobOutcome(ctx, ordinal, payload, artifacts, payloadKind, config.inputRef.Hash, attempt, config.inputRef, &startAt, &startAt); saveErr != nil {
		return nil, saveErr
	}
	cleanupArtifacts(artifacts, r.logger)
	if inputArtifacts, _ := inputData.GetArtifacts(); len(inputArtifacts) > 0 {
		cleanupArtifacts(inputArtifacts, r.logger)
	}
	r.completeLease(ctx, err)
	r.emitJobEnd(attempt, nil, err, startAt)
	return nil, err
}

func (r *workerRunner) persistJobOutcome(ctx context.Context, ordinal int64, payload json.RawMessage, artifacts []Artifact, payloadKind string, inputHash string, attempt int, inputRef *InputReference, startedAt *time.Time, finishedAt *time.Time) (TaskData, error) {
	meta := chapterMeta{
		Attempt:    attempt,
		InputRef:   inputRef,
		StartedAt:  startedAt,
		FinishedAt: finishedAt,
	}
	return persistTaskDataChapter(ctx, r.runtime, r.currentLeaseID(), ChapterRef{
		JobKey:  r.GetJobKey(),
		Ordinal: ordinal,
	}, r.worker.JobWorker.Name(), chapterTypeJobAttemptOutcome, payloadKind, inputHash, time.Now().UTC(), meta, payload, artifacts)
}

func (r *workerRunner) checkCachedJobResult(ctx context.Context, ordinal int64, inputHash string, retryCfg RetryPolicy, totalDeadline time.Time, totalTimeout time.Duration, inputRef *InputReference) (JobData, int, bool, bool, error, error, *time.Time, *time.Time) {
	chapter, meta, err := r.getChapter(ctx, ordinal)
	if errors.Is(err, ErrChapterNotFound) {
		return nil, 1, false, false, nil, nil, nil, nil
	}
	if err != nil {
		return nil, 0, false, false, nil, err, nil, nil
	}
	if chapter.ChapterType != chapterTypeJobAttemptOutcome {
		return nil, 0, false, false, nil, fmt.Errorf("%w: unexpected chapter type %q at ordinal %d", ErrWorkflowNotDeterministic, chapter.ChapterType, ordinal), nil, nil
	}
	if meta.InputHash != "" && meta.InputHash != inputHash {
		return nil, 0, false, false, nil, fmt.Errorf("%w: ordinal %d job result input hash mismatch", ErrWorkflowNotDeterministic, ordinal), nil, nil
	}
	if !totalDeadline.IsZero() && time.Now().After(totalDeadline) {
		return nil, 0, false, false, nil, NewTimeoutError("job", totalTimeout, TimeoutScopeTotal, inputRef, false), nil, nil
	}

	priorAttempt := meta.Attempt
	if priorAttempt <= 0 {
		priorAttempt = 1
	}
	nextAttempt := priorAttempt + 1
	endAt := metaEndAt(meta)

	output, payloadErr := storedChapterToTaskData(r.runtime, r.GetJobKey(), chapter)
	if payloadErr == nil {
		return output, nextAttempt, true, true, nil, nil, &endAt, &endAt
	}

	if !isRetryable(payloadErr, retryCfg) || priorAttempt >= int(retryCfg.MaximumAttempts) {
		return nil, nextAttempt, true, true, payloadErr, payloadErr, &endAt, &endAt
	}

	backoff := computeBackoff(retryCfg, priorAttempt)
	if backoff > 0 {
		wakeAt := meta.CreatedAt.Add(backoff)
		if time.Now().Before(wakeAt) {
			if err := r.awaitUntil(wakeAt, ordinal, priorAttempt, "job", inputRef, time.Time{}, totalDeadline, 0, totalTimeout); err != nil {
				return nil, 0, false, false, nil, err, nil, nil
			}
		}
	}
	return nil, nextAttempt, true, false, payloadErr, nil, &endAt, &endAt
}

func (r *workerRunner) prepareJobResultPayload(output JobData, originalErr error, inputRef *InputReference) (json.RawMessage, []Artifact, string, error) {
	artifacts := []Artifact{}
	if originalErr != nil {
		payload, kind, err := errorPayloadFromError(originalErr, inputRef)
		if err != nil {
			return nil, nil, "", err
		}
		if output != nil {
			if arts, artsErr := output.GetArtifacts(); artsErr == nil {
				artifacts = arts
			}
		}
		return payload, artifacts, kind, nil
	}
	if output == nil {
		raw, _ := json.Marshal(SystemErrorPayload{Message: "missing job output", InputRef: inputRef})
		return json.RawMessage(raw), artifacts, payloadKindSystemError, nil
	}
	dataBytes, err := output.GetData()
	if err != nil {
		return nil, nil, "", err
	}
	artifacts, err = output.GetArtifacts()
	if err != nil {
		return nil, nil, "", err
	}
	return dataBytes, artifacts, payloadKindApp, nil
}

func (r *workerRunner) DoTask(policy RunPolicy, taskType string, data TaskData) (TaskData, error) {
	ctx := r.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	inputHash, err := computeInputHash(ctx, data)
	if err != nil {
		return nil, fmt.Errorf("compute input hash: %w", err)
	}
	effectivePolicy := normalizeRunPolicy(mergeRunPolicy(policy, r.jobPolicy))
	retryCfg := effectivePolicy.Retry
	invocationTimeout := durationPtrToDuration(effectivePolicy.InvocationTimeout)
	totalTimeout := durationPtrToDuration(effectivePolicy.TotalTimeout)
	maxAttempts := int(retryCfg.MaximumAttempts)
	attempt := 1
	var nextAttemptStartAt *time.Time

	for {
		startAt := time.Now().UTC()
		if nextAttemptStartAt != nil {
			startAt = *nextAttemptStartAt
			nextAttemptStartAt = nil
		}
		ordinal := r.storyCounter
		r.storyCounter++

		inputRef := &InputReference{Ordinal: ordinal - 1, Hash: inputHash}
		if inputRef.Ordinal < 0 {
			inputRef.Ordinal = 0
		}

		totalDeadline, err := r.taskTotalDeadline(ctx, ordinal, totalTimeout)
		if err != nil && !errors.Is(err, ErrChapterNotFound) {
			return nil, fmt.Errorf("compute total deadline: %w", err)
		}

		chapter, meta, err := r.getChapter(ctx, ordinal)
		if err == nil {
			if chapter.ChapterType != chapterTypeTaskAttemptOutcome && chapter.ChapterType != chapterTypeRestartExtra {
				return nil, fmt.Errorf("%w: unexpected chapter type %q at ordinal %d", ErrWorkflowNotDeterministic, chapter.ChapterType, ordinal)
			}
			r.emitTaskStart(taskType, ordinal, attempt, data, metaStartAt(meta))
			if chapter.ChapterType == chapterTypeRestartExtra && r.shouldCheckPrerequisites() && len(meta.Prerequisites) > 0 {
				if err := r.checkPrerequisites(ctx, meta); err != nil {
					r.emitTaskEnd(taskType, ordinal, attempt, nil, err, metaEndAt(meta))
					return nil, err
				}
			}
			if meta.InputHash == "" {
				return nil, fmt.Errorf("%w: ordinal %d task %s missing input hash", ErrMissingInputHash, ordinal, taskType)
			}
			if meta.InputHash != inputHash {
				return nil, TaskInputMismatchError{
					TaskType:          taskType,
					Ordinal:           ordinal,
					CachedInputHash:   meta.InputHash,
					ComputedInputHash: inputHash,
					CachedInput:       meta.Input,
					Meta: TaskDeterminismMeta{
						Ordinal:       meta.Ordinal,
						TaskType:      meta.TaskType,
						WorkerID:      meta.WorkerID,
						CreatedAt:     meta.CreatedAt,
						Attempt:       meta.Attempt,
						MaxAttempts:   meta.MaxAttempts,
						NextAttemptAt: meta.NextAttemptAt,
						BackoffMillis: meta.BackoffMillis,
						Retryable:     meta.Retryable,
						InputHash:     meta.InputHash,
						InputRef:      meta.InputRef,
						RunPolicy:     meta.RunPolicy,
						InputPayload:  meta.Input,
						Version:       meta.Version,
					},
				}
			}
			td, payloadErr := storedChapterToTaskData(r.runtime, r.GetJobKey(), chapter)
			if payloadErr == nil {
				r.emitTaskEnd(taskType, ordinal, attempt, td, nil, metaEndAt(meta))
				return td, nil
			}
			priorAttempt := meta.Attempt
			if priorAttempt <= 0 {
				priorAttempt = 1
			}
			if !isRetryable(payloadErr, retryCfg) || priorAttempt >= maxAttempts {
				r.emitTaskEnd(taskType, ordinal, attempt, nil, payloadErr, metaEndAt(meta))
				return nil, payloadErr
			}
			r.emitTaskEnd(taskType, ordinal, attempt, nil, payloadErr, metaEndAt(meta))
			backoff := computeBackoff(retryCfg, priorAttempt)
			endAt := metaEndAt(meta)
			nextAttemptStartAt = &endAt
			if backoff > 0 {
				if err := r.awaitUntil(meta.CreatedAt.Add(backoff), ordinal, priorAttempt, "task", inputRef, time.Time{}, totalDeadline, 0, totalTimeout); err != nil {
					return nil, err
				}
			}
			attempt = priorAttempt + 1
			continue
		}
		if err != nil && !errors.Is(err, ErrChapterNotFound) {
			return nil, fmt.Errorf("failed to get chapter %d: %w", ordinal, err)
		}

		worker, local := r.worker.TaskWorkers[taskType]
		if !local {
			if r.lease == nil || r.replay {
				return nil, ReplayCacheMissError{
					JobKey:   r.GetJobKey(),
					TaskType: taskType,
					Ordinal:  ordinal,
					Attempt:  attempt,
					Reason:   ReplayCacheMissTaskResultMissing,
				}
			}
			inputOrdinal := ordinal - 1
			if inputOrdinal < 0 {
				inputOrdinal = 0
			}
			req := RescheduleExecutionRequest{
				NextNeed: workerCapability(r.worker.JobWorker.Name(), taskType),
				Payload: mustMarshalJSON(workerJobPayload{
					RunPolicy: r.jobPolicy,
					TaskWait: &workerTaskWait{
						InputStep:  inputOrdinal,
						OutputStep: ordinal,
						Next:       r.worker.JobWorker.Name(),
						InputHash:  inputHash,
					},
				}),
			}
			if invocationTimeout > 0 {
				req.AlternateNeed = r.worker.JobWorker.Name()
				req.AlternateAfter = &invocationTimeout
			}
			if err := r.lease.Reschedule(context.TODO(), req); err != nil {
				if IsExecutionLeaseLost(err) {
					r.rescheduled.Store(true)
					prematureCloseOut()
				}
				return nil, fmt.Errorf("failed to reschedule job: %w", err)
			}
			r.rescheduled.Store(true)
			prematureCloseOut()
		}

		r.emitTaskStart(taskType, ordinal, attempt, data, startAt)
		attemptStartAt := startAt
		now := time.Now()
		if !totalDeadline.IsZero() && now.After(totalDeadline) {
			err := NewTimeoutError("task", totalTimeout, TimeoutScopeTotal, inputRef, false)
			r.emitTaskEnd(taskType, ordinal, attempt, nil, err, startAt)
			return nil, err
		}

		attemptInvocationDeadline := time.Time{}
		if invocationTimeout > 0 {
			attemptInvocationDeadline = now.Add(invocationTimeout)
		}

		exitCh := make(chan struct{})
		var exitOnce sync.Once
		resultCh := make(chan taskResult, 1)
		go func(attemptNum int) {
			var output TaskData
			var taskErr error
			func() {
				defer func() {
					if r.rescheduled.Load() {
						exitOnce.Do(func() { close(exitCh) })
					}
					if rec := recover(); rec != nil {
						taskErr = panicToAppError(rec)
					}
					taskErr = normalizeComparableError(taskErr)
				}()
				output, taskErr = worker.Run(NewTaskContext(
					r.GetJobKey(),
					ordinal,
					r.logger.With("task", taskType, "step", ordinal, "attempt", attemptNum),
					func(wakeAt time.Time) error {
						return r.awaitUntil(wakeAt, ordinal, attemptNum, "task", inputRef, attemptInvocationDeadline, totalDeadline, invocationTimeout, totalTimeout)
					},
					func(jobIds ...string) error {
						wasRescheduled := r.rescheduled.Load()
						if err := r.AwaitJobs(jobIds...); err != nil {
							return err
						}
						if !wasRescheduled && r.rescheduled.Load() {
							exitOnce.Do(func() { close(exitCh) })
							goruntime.Goexit()
						}
						return nil
					},
				), data)
			}()
			resultCh <- taskResult{output: output, err: taskErr}
		}(attempt)

		deadline := attemptInvocationDeadline
		if deadline.IsZero() || (!totalDeadline.IsZero() && totalDeadline.Before(deadline)) {
			deadline = totalDeadline
		}

		var output TaskData
		var taskErr error
		if deadline.IsZero() {
			select {
			case res := <-resultCh:
				output, taskErr = res.output, res.err
			case <-exitCh:
				prematureCloseOut()
			}
		} else {
			timer := time.NewTimer(time.Until(deadline))
			select {
			case res := <-resultCh:
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				output, taskErr = res.output, res.err
			case <-exitCh:
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				prematureCloseOut()
			case <-timer.C:
				if !totalDeadline.IsZero() && deadline.Equal(totalDeadline) {
					taskErr = NewTimeoutError("task", totalTimeout, TimeoutScopeTotal, inputRef, false)
				} else {
					taskErr = NewTimeoutError("task", invocationTimeout, TimeoutScopeInvocation, inputRef, true)
				}
			}
		}

		now = time.Now()
		if taskErr == nil {
			if !totalDeadline.IsZero() && now.After(totalDeadline) {
				taskErr = NewTimeoutError("task", totalTimeout, TimeoutScopeTotal, inputRef, false)
			} else if !attemptInvocationDeadline.IsZero() && now.After(attemptInvocationDeadline) {
				taskErr = NewTimeoutError("task", invocationTimeout, TimeoutScopeInvocation, inputRef, true)
			}
		}

		var (
			payload     json.RawMessage
			payloadKind = payloadKindApp
			artifacts   []Artifact
			dataBytes   Data
		)
		originalErr := taskErr
		if taskErr != nil {
			var tdErr error
			payload, payloadKind, tdErr = errorPayloadFromError(taskErr, inputRef)
			if tdErr != nil {
				return nil, tdErr
			}
			if output != nil {
				artifacts, _ = output.GetArtifacts()
			}
		} else {
			dataBytes, err = output.GetData()
			if err != nil {
				return nil, err
			}
			payload = dataBytes
			artifacts, err = output.GetArtifacts()
			if err != nil {
				return nil, err
			}
			if _, err := validateOutputArtifacts(ctx, artifacts); err != nil {
				return nil, err
			}
		}

		finishedAt := time.Now().UTC()
		inputPayload := json.RawMessage(nil)
		if TaskInputStorageEnabled() {
			inputData, err := data.GetData()
			if err != nil {
				return nil, err
			}
			inputPayload = append(json.RawMessage(nil), inputData...)
		}
		meta = chapterMeta{
			Attempt:    attempt,
			InputRef:   inputRef,
			Input:      inputPayload,
			StartedAt:  &attemptStartAt,
			FinishedAt: &finishedAt,
		}
		persistedOutput, err := persistTaskDataChapter(context.TODO(), r.runtime, r.currentLeaseID(), ChapterRef{
			JobKey:  r.GetJobKey(),
			Ordinal: ordinal,
		}, taskType, chapterTypeTaskAttemptOutcome, payloadKind, inputHash, time.Now().UTC(), meta, payload, artifacts)
		if err != nil {
			return nil, err
		}
		cleanupArtifacts(artifacts, r.logger)

		if originalErr == nil {
			if inputArtifacts, _ := data.GetArtifacts(); len(inputArtifacts) > 0 {
				cleanupArtifacts(inputArtifacts, r.logger)
			}
			r.emitTaskEnd(taskType, ordinal, attempt, persistedOutput, nil, finishedAt)
			return persistedOutput, nil
		}

		if retryable := isRetryable(originalErr, retryCfg); retryable && attempt < maxAttempts {
			if inputArtifacts, _ := data.GetArtifacts(); len(inputArtifacts) > 0 {
				cleanupArtifacts(inputArtifacts, r.logger)
			}
			backoff := computeBackoff(retryCfg, attempt)
			r.emitTaskEnd(taskType, ordinal, attempt, nil, originalErr, finishedAt)
			nextAttemptStartAt = &finishedAt
			attempt++
			if backoff > 0 {
				if err := r.awaitUntil(time.Now().UTC().Add(backoff), ordinal, attempt-1, "task", inputRef, time.Time{}, totalDeadline, 0, totalTimeout); err != nil {
					return nil, err
				}
			}
			continue
		}

		if inputArtifacts, _ := data.GetArtifacts(); len(inputArtifacts) > 0 {
			cleanupArtifacts(inputArtifacts, r.logger)
		}
		r.emitTaskEnd(taskType, ordinal, attempt, nil, originalErr, finishedAt)
		return nil, originalErr
	}
}

func (r *workerRunner) DoJob(ctx context.Context) (JobData, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	r.ctx = ctx

	inputData, _, meta, err := r.loadInitialChapterAndPolicy(ctx)
	if err != nil {
		return nil, err
	}
	config, err := r.setupJobExecutionConfig(ctx, inputData, meta)
	if err != nil {
		return nil, err
	}

	if r.shouldCheckPrerequisites() {
		if err := r.checkPrerequisites(ctx, meta); err != nil {
			return r.failJobPrerequisites(ctx, inputData, config, err)
		}
	}

	maxAttempts := int(config.retryCfg.MaximumAttempts)
	attempt := 1
	initialStartAt := meta.CreatedAt
	var nextAttemptStartAt *time.Time

	if r.lease != nil && !r.replay {
		_ = r.lease.KeepAlive(ctx)
		defer r.lease.StopKeepAlive()
	}

	for {
		startAt := time.Now().UTC()
		if attempt == 1 {
			startAt = initialStartAt
		}
		if nextAttemptStartAt != nil {
			startAt = *nextAttemptStartAt
			nextAttemptStartAt = nil
		}
		r.currentJobAttemptStartAt = startAt
		r.lastTaskEndAt = nil
		r.emitJobStart(attempt, inputData, startAt)

		if err := r.checkTotalTimeoutExceeded(config.totalDeadline, config.totalTimeout, config.inputRef); err != nil {
			payload, artifacts, payloadKind, prepErr := r.prepareJobResultPayload(nil, err, config.inputRef)
			if prepErr != nil {
				return nil, prepErr
			}
			ordinal := r.storyCounter
			r.storyCounter++
			if _, saveErr := r.persistJobOutcome(ctx, ordinal, payload, artifacts, payloadKind, config.inputRef.Hash, attempt, config.inputRef, nil, nil); saveErr != nil {
				return nil, saveErr
			}
			cleanupArtifacts(artifacts, r.logger)
			if inputArtifacts, _ := inputData.GetArtifacts(); len(inputArtifacts) > 0 {
				cleanupArtifacts(inputArtifacts, r.logger)
			}
			r.completeLease(ctx, err)
			r.emitJobEnd(attempt, nil, err, startAt)
			return nil, err
		}

		var (
			outputCached JobData
			nextAttempt  int
			cached       bool
			terminal     bool
			priorErr     error
			cachedErr    error
			cachedEndAt  *time.Time
			nextStartAt  *time.Time
		)
		if cachedChapter, _, err := r.getChapter(ctx, r.storyCounter); err == nil {
			if cachedChapter.ChapterType == chapterTypeJobAttemptOutcome {
				outputCached, nextAttempt, cached, terminal, priorErr, cachedErr, cachedEndAt, nextStartAt = r.checkCachedJobResult(ctx, r.storyCounter, config.inputRef.Hash, config.retryCfg, config.totalDeadline, config.totalTimeout, config.inputRef)
				if cachedErr != nil {
					return nil, cachedErr
				}
				if cached {
					at := startAt
					if cachedEndAt != nil {
						at = *cachedEndAt
					}
					if terminal {
						r.completeLease(ctx, priorErr)
						r.emitJobEnd(attempt, outputCached, priorErr, at)
						if priorErr != nil {
							return nil, priorErr
						}
						return outputCached, nil
					}
					r.emitJobEnd(attempt, nil, priorErr, at)
					nextAttemptStartAt = nextStartAt
					attempt = nextAttempt
					continue
				}
			}
		} else if !errors.Is(err, ErrChapterNotFound) {
			return nil, err
		}

		attemptInvocationDeadline := r.setupAttemptDeadlines(config.invocationTimeout, config.totalDeadline, config.totalTimeout, config.inputRef)
		attemptStartAt := startAt
		resultCh := r.executeJobWorkerAsync(inputData)
		output, jobErr := r.waitForJobResultWithDeadline(resultCh, attemptInvocationDeadline, config.totalDeadline, config.invocationTimeout, config.totalTimeout, config.inputRef)
		if output == nil && jobErr == nil {
			r.emitJobEnd(attempt, nil, nil, attemptStartAt)
			return nil, nil
		}
		jobErr = r.validatePostExecutionTimeouts(jobErr, attemptInvocationDeadline, config.totalDeadline, config.invocationTimeout, config.totalTimeout, config.inputRef)
		attemptFinishedAt := time.Now().UTC()
		if isReplayCacheMiss(jobErr) {
			attemptFinishedAt = r.fallbackTaskTime()
		}

		ordinal := r.storyCounter
		r.storyCounter++
		outputCached, nextAttempt, cached, terminal, priorErr, cachedErr, cachedEndAt, nextStartAt = r.checkCachedJobResult(ctx, ordinal, config.inputRef.Hash, config.retryCfg, config.totalDeadline, config.totalTimeout, config.inputRef)
		if cachedErr != nil {
			return nil, cachedErr
		}
		if cached {
			if terminal {
				r.completeLease(ctx, priorErr)
				at := attemptFinishedAt
				if cachedEndAt != nil {
					at = *cachedEndAt
				}
				r.emitJobEnd(attempt, outputCached, priorErr, at)
				if priorErr != nil {
					return nil, priorErr
				}
				return outputCached, nil
			}
			at := attemptFinishedAt
			if cachedEndAt != nil {
				at = *cachedEndAt
			}
			r.emitJobEnd(attempt, nil, priorErr, at)
			nextAttemptStartAt = nextStartAt
			attempt = nextAttempt
			continue
		}

		payload, artifacts, payloadKind, err := r.prepareJobResultPayload(output, jobErr, config.inputRef)
		if err != nil {
			return nil, err
		}
		if _, err := r.persistJobOutcome(ctx, ordinal, payload, artifacts, payloadKind, config.inputRef.Hash, attempt, config.inputRef, &attemptStartAt, &attemptFinishedAt); err != nil {
			return nil, err
		}
		cleanupArtifacts(artifacts, r.logger)

		if jobErr == nil {
			if inputArtifacts, _ := inputData.GetArtifacts(); len(inputArtifacts) > 0 {
				cleanupArtifacts(inputArtifacts, r.logger)
			}
			r.completeLease(ctx, nil)
			r.emitJobEnd(attempt, output, nil, attemptFinishedAt)
			return output, nil
		}

		if retryable := isRetryable(jobErr, config.retryCfg); !retryable || attempt >= maxAttempts {
			if inputArtifacts, _ := inputData.GetArtifacts(); len(inputArtifacts) > 0 {
				cleanupArtifacts(inputArtifacts, r.logger)
			}
			r.completeLease(ctx, jobErr)
			r.emitJobEnd(attempt, nil, jobErr, attemptFinishedAt)
			return nil, jobErr
		}

		if inputArtifacts, _ := inputData.GetArtifacts(); len(inputArtifacts) > 0 {
			cleanupArtifacts(inputArtifacts, r.logger)
		}
		backoff := computeBackoff(config.retryCfg, attempt)
		r.emitJobEnd(attempt, nil, jobErr, attemptFinishedAt)
		attempt++
		nextAttemptStartAt = &attemptFinishedAt
		if backoff > 0 {
			if err := r.awaitUntil(time.Now().UTC().Add(backoff), ordinal, attempt-1, "job", config.inputRef, time.Time{}, config.totalDeadline, 0, config.totalTimeout); err != nil {
				return nil, err
			}
		}
	}
}

func completionStatusAndDetail(err error) (string, string) {
	if err == nil {
		return "success", ""
	}
	if errors.Is(err, ErrJobCancelled) || errors.Is(err, context.Canceled) {
		return "cancelled", err.Error()
	}
	var te TimeoutError
	if errors.As(err, &te) {
		return "failed_timeout", messageOrFallback(te.Payload.Message, err)
	}
	var ae AppError
	if errors.As(err, &ae) {
		return "failed_app", messageOrFallback(ae.Payload.Message, err)
	}
	var se SystemError
	if errors.As(err, &se) {
		return "failed_system", messageOrFallback(se.Payload.Message, err)
	}
	return "failed_system", messageOrFallback("", err)
}

func messageOrFallback(message string, err error) string {
	if message != "" {
		return message
	}
	if err != nil {
		return err.Error()
	}
	return ""
}

func isReplayCacheMiss(err error) bool {
	var miss ReplayCacheMissError
	return errors.As(err, &miss)
}

type noopReplayObserver struct{}

func (noopReplayObserver) OnJobStart(JobStartEvent)   {}
func (noopReplayObserver) OnTaskStart(TaskStartEvent) {}
func (noopReplayObserver) OnTaskEnd(TaskEndEvent)     {}
func (noopReplayObserver) OnJobEnd(JobEndEvent)       {}

func mustMarshalJSON(v any) json.RawMessage {
	raw, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return raw
}
