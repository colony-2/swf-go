package impl

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"runtime"
	"time"

	"github.com/colony-2/pgwf-go/pkg/pgwf"
	"github.com/colony-2/strata-go/pkg/client/core"
	"github.com/colony-2/strata-go/pkg/client/story"
	"github.com/colony-2/swf-go/pkg/swf"
)

type runner struct {
	jobId        pgwf.JobID
	tenantId     string
	worker       *swf.WorkSet
	storyCounter int64
	engine       *swfEngineImpl
	lease        *pgwf.Lease
	logger       *slog.Logger
	jobPolicy    swf.RunPolicy
	capability   pgwf.Capability
	ctx          context.Context
	// current attempt bookkeeping for job-level Await/Spawn paths.
	currentInvocationDeadline time.Time
	currentTotalDeadline      time.Time
	currentInvocationLimit    time.Duration
	currentTotalLimit         time.Duration
	currentInputRef           *swf.InputReference
	currentKind               string
}

func (r *runner) GetJobKey() swf.JobKey {
	return swf.JobKey{
		TenantId: r.tenantId,
		JobId:    string(r.jobId),
	}
}

func notificationJobID(child swf.JobKey) pgwf.JobID {
	return pgwf.JobID(fmt.Sprintf("%s-notify", child.JobId))
}

func durationPtrToDuration(d *swf.Duration) time.Duration {
	if d == nil {
		return 0
	}
	return time.Duration(*d)
}

type asyncChildSpawn struct {
	ChildJobID        string `json:"child_job_id"`
	JobType           string `json:"job_type"`
	InputHash         string `json:"input_hash,omitempty"`
	NotificationJobID string `json:"notification_job_id,omitempty"`
}

func panicToAppError(rec interface{}) error {
	return swf.AppError{Payload: swf.AppErrorPayload{Message: fmt.Sprintf("panic: %v", rec), Level: "error"}}
}

func (r *runner) taskTotalDeadline(ctx context.Context, key story.Key, ordinal int64, totalTimeout time.Duration) (time.Time, error) {
	if totalTimeout <= 0 {
		return time.Time{}, nil
	}
	startOrdinal := ordinal - 1
	if startOrdinal < 0 {
		startOrdinal = 0
	}
	chap, err := r.engine.strata.Chapter(ctx, key, startOrdinal)
	if err != nil {
		return time.Time{}, err
	}
	env, decErr := decodeChapterEnvelope(chap.Body())
	if decErr != nil {
		return time.Time{}, decErr
	}
	return env.Meta.CreatedAt.Add(totalTimeout), nil
}

func (r *runner) jobTotalDeadline(env0 chapterEnvelope, totalTimeout time.Duration) time.Time {
	if totalTimeout <= 0 {
		return time.Time{}
	}
	return env0.Meta.CreatedAt.Add(totalTimeout)
}

func (r *runner) awaitUntil(wakeAt time.Time, ordinal int64, attempt int, kind string, inputRef *swf.InputReference, invocationDeadline time.Time, totalDeadline time.Time, invocationLimit time.Duration, totalLimit time.Duration) error {
	now := time.Now()
	if !totalDeadline.IsZero() && now.After(totalDeadline) {
		return swf.NewTimeoutError(kind, totalLimit, swf.TimeoutScopeTotal, inputRef, false)
	}
	if !invocationDeadline.IsZero() && now.After(invocationDeadline) {
		return swf.NewTimeoutError(kind, invocationLimit, swf.TimeoutScopeInvocation, inputRef, true)
	}
	if !totalDeadline.IsZero() && wakeAt.After(totalDeadline) {
		wakeAt = totalDeadline
	}
	if !invocationDeadline.IsZero() && wakeAt.After(invocationDeadline) {
		wakeAt = invocationDeadline
	}
	if wakeAt.IsZero() || time.Now().After(wakeAt) {
		return nil
	}
	ctx := r.ctx

	ch := r.engine.AwaitUntil(r.jobId, r.capability, r.lease, ordinal, attempt, wakeAt)
	if ch == nil {
		prematureCloseOut()
		return nil
	}

	// Clear any stale signal before waiting.
	select {
	case <-ch:
	default:
	}

	select {
	case sig := <-ch:
		if sig.Kind == awaitSignalKindRecycle {
			prematureCloseOut()
		}
	case <-ctx.Done():
		prematureCloseOut()
		return ctx.Err()
	}
	now = time.Now()
	if !totalDeadline.IsZero() && (now.After(totalDeadline) || now.Equal(totalDeadline)) {
		return swf.NewTimeoutError(kind, totalLimit, swf.TimeoutScopeTotal, inputRef, false)
	}
	if !invocationDeadline.IsZero() && (now.After(invocationDeadline) || now.Equal(invocationDeadline)) {
		return swf.NewTimeoutError(kind, invocationLimit, swf.TimeoutScopeInvocation, inputRef, true)
	}
	return nil
}

func (r *runner) awaitChild(ctx context.Context, childJobKey swf.JobKey, ordinal int64, notificationJobID pgwf.JobID, kind string, inputRef *swf.InputReference, invocationDeadline time.Time, totalDeadline time.Time, invocationLimit time.Duration, totalLimit time.Duration) (swf.TaskData, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	for {
		if td, done, err := r.engine.jobResultIfComplete(ctx, childJobKey); err != nil {
			return nil, err
		} else if done {
			return td, nil
		}

		if err := r.engine.ensureNotificationJob(ctx, notificationJobID, pgwf.JobID(childJobKey.JobId), r.jobId, ordinal); err != nil {
			return nil, err
		}

		ch := r.engine.AwaitChild(r.jobId, r.capability, r.lease, ordinal, pgwf.JobID(childJobKey.JobId), notificationJobID)
		if ch == nil {
			prematureCloseOut()
			return nil, nil
		}

		select {
		case <-ch:
		default:
		}

		select {
		case sig := <-ch:
			if sig.Kind == awaitSignalKindRecycle {
				prematureCloseOut()
			}
		case <-ctx.Done():
			prematureCloseOut()
			return nil, ctx.Err()
		}
		now := time.Now()
		if !totalDeadline.IsZero() && (now.After(totalDeadline) || now.Equal(totalDeadline)) {
			return nil, swf.NewTimeoutError(kind, totalLimit, swf.TimeoutScopeTotal, inputRef, false)
		}
		if !invocationDeadline.IsZero() && (now.After(invocationDeadline) || now.Equal(invocationDeadline)) {
			return nil, swf.NewTimeoutError(kind, invocationLimit, swf.TimeoutScopeInvocation, inputRef, true)
		}
	}
}

func (r *runner) DoTask(policy swf.RunPolicy, taskType string, data swf.TaskData) (swf.TaskData, error) {
	ordinal := r.storyCounter
	r.storyCounter++
	ctx := r.ctx
	if ctx == nil {
		ctx = context.Background()
	}

	inputHash, err := computeInputHash(ctx, data)
	if err != nil {
		return nil, fmt.Errorf("compute input hash: %w", err)
	}
	inputRef := &swf.InputReference{Ordinal: ordinal - 1}
	if inputRef.Ordinal < 0 {
		inputRef.Ordinal = 0
	}
	inputRef.Hash = inputHash

	basePolicy := r.jobPolicy
	effectivePolicy := normalizeRunPolicy(mergeRunPolicy(policy, basePolicy))
	retryCfg := effectivePolicy.Retry
	invocationTimeout := durationPtrToDuration(effectivePolicy.InvocationTimeout)
	totalTimeout := durationPtrToDuration(effectivePolicy.TotalTimeout)
	maxAttempts := int(retryCfg.MaximumAttempts)
	attempt := 1

	key := r.GetJobKey().ToStoryKey()
	totalDeadline, err := r.taskTotalDeadline(ctx, key, ordinal, totalTimeout)
	if err != nil {
		return nil, fmt.Errorf("compute total deadline: %w", err)
	}
	chap, err := r.engine.strata.Chapter(ctx, key, ordinal)
	if err == nil {
		env, decErr := decodeChapterEnvelope(chap.Body())
		if decErr != nil {
			return nil, fmt.Errorf("%w: decode cached chapter: %v", swf.ErrWorkflowNotDeterministic, decErr)
		}
		if env.Meta.InputHash == "" {
			return nil, fmt.Errorf("%w: ordinal %d task %s missing input hash", swf.ErrMissingInputHash, ordinal, taskType)
		}
		if env.Meta.InputHash != inputHash {
			return nil, fmt.Errorf("%w: ordinal %d task %s", swf.ErrWorkflowNotDeterministic, ordinal, taskType)
		}
		if !totalDeadline.IsZero() && time.Now().After(totalDeadline) {
			return nil, swf.NewTimeoutError("task", totalTimeout, swf.TimeoutScopeTotal, inputRef, false)
		}
		priorAttempt := env.Meta.Attempt
		if priorAttempt > 0 {
			attempt = priorAttempt + 1
		}

		td, payloadErr := envelopeToTaskData(env, chap.Artifacts())
		if payloadErr != nil {
			retryable := isRetryable(payloadErr, retryCfg)
			if !retryable || priorAttempt >= maxAttempts {
				return nil, payloadErr
			}
			backoff := time.Duration(0)
			if priorAttempt > 0 {
				backoff = computeBackoff(retryCfg, priorAttempt)
			}
			if backoff > 0 {
				wakeAt := env.Meta.CreatedAt.Add(backoff)
				if time.Now().Before(wakeAt) {
					if err := r.awaitUntil(wakeAt, ordinal, priorAttempt, "task", inputRef, time.Time{}, totalDeadline, 0, totalTimeout); err != nil {
						return nil, err
					}
				}
			}
		} else {
			return td, nil
		}
	} else if !errors.Is(err, core.ErrNotFound) {
		return nil, fmt.Errorf("failed to get chapter %d: %w", ordinal, err)
	}

	worker, capabilityExistsLocally := r.worker.TaskWorkers[taskType]
	if !capabilityExistsLocally {
		inputOrdinal := ordinal - 1
		if inputOrdinal < 0 {
			inputOrdinal = 0
		}

		err = r.lease.Reschedule(context.TODO(), r.engine.udb, pgwf.JobDependencies{
			NextNeed: pgwf.Capability(r.worker.JobWorker.Name() + ":" + taskType),
			WaitFor:  nil,
		}, jobPayload{
			RunPolicy: r.jobPolicy,
			TaskWait: &taskWait{
				InputStep:  inputOrdinal,
				OutputStep: ordinal,
				Next:       r.worker.JobWorker.Name(),
			},
		})

		if err != nil {
			return nil, fmt.Errorf("failed to reschedule job: %w", err)
		}

		prematureCloseOut()
		return nil, nil
	}

	for {
		now := time.Now()
		if !totalDeadline.IsZero() && now.After(totalDeadline) {
			return nil, swf.NewTimeoutError("task", totalTimeout, swf.TimeoutScopeTotal, inputRef, false)
		}
		attemptInvocationDeadline := time.Time{}
		if invocationTimeout > 0 {
			attemptInvocationDeadline = now.Add(invocationTimeout)
		}

		type taskResult struct {
			output swf.TaskData
			err    error
		}
		resultCh := make(chan taskResult, 1)
		go func(attemptNum int) {
			var output swf.TaskData
			var taskErr error
			func() {
				defer func() {
					if rec := recover(); rec != nil {
						taskErr = panicToAppError(rec)
					}
				}()
				output, taskErr = worker.Run(
					swf.NewTaskContext(
						r.GetJobKey(),
						ordinal,
						r.logger.With("task", taskType, "step", ordinal, "attempt", attemptNum),
						func(wakeAt time.Time) error {
							return r.awaitUntil(wakeAt, ordinal, attemptNum, "task", inputRef, attemptInvocationDeadline, totalDeadline, invocationTimeout, totalTimeout)
						},
						func(jobType string, td swf.TaskData) (*swf.Future, error) {
							return r.spawnAsyncWithDeadlines(jobType, td, attemptInvocationDeadline, totalDeadline, invocationTimeout, totalTimeout, inputRef)
						},
					),
					data,
				)
			}()
			resultCh <- taskResult{output: output, err: taskErr}
		}(attempt)

		deadline := attemptInvocationDeadline
		if deadline.IsZero() || (!totalDeadline.IsZero() && totalDeadline.Before(deadline)) {
			deadline = totalDeadline
		}

		var output swf.TaskData
		var taskErr error
		if deadline.IsZero() {
			res := <-resultCh
			output, taskErr = res.output, res.err
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
			case <-timer.C:
				if !totalDeadline.IsZero() && deadline.Equal(totalDeadline) {
					taskErr = swf.NewTimeoutError("task", totalTimeout, swf.TimeoutScopeTotal, inputRef, false)
				} else {
					taskErr = swf.NewTimeoutError("task", invocationTimeout, swf.TimeoutScopeInvocation, inputRef, true)
				}
			}
		}

		now = time.Now()
		if taskErr == nil {
			if !totalDeadline.IsZero() && now.After(totalDeadline) {
				taskErr = swf.NewTimeoutError("task", totalTimeout, swf.TimeoutScopeTotal, inputRef, false)
			} else if !attemptInvocationDeadline.IsZero() && now.After(attemptInvocationDeadline) {
				taskErr = swf.NewTimeoutError("task", invocationTimeout, swf.TimeoutScopeInvocation, inputRef, true)
			}
		}

		payloadKind := payloadKindApp
		originalErr := taskErr
		var payload json.RawMessage
		artifacts := []swf.Artifact{}
		if taskErr != nil {
			var tdErr error
			payload, payloadKind, tdErr = errorPayloadFromError(taskErr, inputRef)
			if tdErr != nil {
				return nil, tdErr
			}
		} else {
			// success
			dataBytes, err := output.GetData()
			if err != nil {
				return nil, err
			}
			payload = dataBytes
			artifacts, err = output.GetArtifacts()
			if err != nil {
				return nil, err
			}
		}

		retryable := isRetryable(originalErr, retryCfg)
		now = time.Now().UTC()
		backoff := time.Duration(0)
		if originalErr != nil && retryable && attempt < maxAttempts {
			backoff = computeBackoff(retryCfg, attempt)
		}
		meta := chapterMetadata{
			Attempt:  attempt,
			InputRef: inputRef,
		}

		chap, err := payloadToChapter(payload, artifacts, ordinal, taskType, r.engine.workerId, payloadKind, inputHash, now, meta)
		if err != nil {
			return nil, err
		}

		err = r.engine.strata.SaveChapter(context.TODO(), key, chap)
		if err != nil {
			return nil, err
		}

		if originalErr == nil {
			return output, nil
		}
		if retryable && attempt < maxAttempts {
			attempt++
			if backoff > 0 {
				if err := r.awaitUntil(now.Add(backoff), ordinal, attempt-1, "task", inputRef, time.Time{}, totalDeadline, 0, totalTimeout); err != nil {
					return nil, err
				}
			}
			continue
		}
		return nil, originalErr
	}
}

func prematureCloseOut() {
	// do any finalization
	runtime.Goexit()
}

var _ swf.JobContext = &runner{}

type RunError struct {
	Err error
}

func (r *runner) getChapter(ordinal int64) (story.Chapter, error) {
	return r.engine.strata.Chapter(context.TODO(), r.GetJobKey().ToStoryKey(), ordinal)
}

func (r *runner) Logger() *slog.Logger {
	return r.logger
}

func (r *runner) AwaitDuration(waitFor swf.Duration) error {
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

func (r *runner) SpawnAsync(jobType string, data swf.TaskData) (*swf.Future, error) {
	return r.spawnAsyncWithDeadlines(jobType, data, r.currentInvocationDeadline, r.currentTotalDeadline, r.currentInvocationLimit, r.currentTotalLimit, r.currentInputRef)
}

func (r *runner) spawnAsyncWithDeadlines(jobType string, data swf.TaskData, invocationDeadline time.Time, totalDeadline time.Time, invocationLimit time.Duration, totalLimit time.Duration, inputRef *swf.InputReference) (*swf.Future, error) {
	if jobType == "" {
		return nil, fmt.Errorf("job type is required")
	}
	ctx := r.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	ordinal := r.storyCounter
	r.storyCounter++

	childJobKey := swf.JobKey{
		TenantId: r.tenantId,
		JobId:    fmt.Sprintf("%s-%d", r.jobId, ordinal),
	}
	notifyJobID := notificationJobID(childJobKey)

	inputHash, err := computeInputHash(ctx, data)
	if err != nil {
		return nil, fmt.Errorf("compute child input hash: %w", err)
	}

	key := r.GetJobKey().ToStoryKey()
	if cached, err := r.engine.strata.Chapter(ctx, key, ordinal); err == nil {
		if env, decErr := decodeChapterEnvelope(cached.Body()); decErr == nil && env.PayloadKind == payloadKindAppChildJob {
			var existing asyncChildSpawn
			if unmarshalErr := json.Unmarshal(env.Payload, &existing); unmarshalErr == nil {
				if existing.ChildJobID != "" {
					childJobKey = swf.JobKey{TenantId: r.tenantId, JobId: existing.ChildJobID}
				}
				if existing.NotificationJobID != "" {
					notifyJobID = pgwf.JobID(existing.NotificationJobID)
				}
				if existing.InputHash != "" && existing.InputHash != inputHash {
					return nil, fmt.Errorf("%w: async child input mismatch at ordinal %d", swf.ErrWorkflowNotDeterministic, ordinal)
				}
				if existing.JobType != "" && existing.JobType != jobType {
					return nil, fmt.Errorf("%w: async child job type mismatch at ordinal %d", swf.ErrWorkflowNotDeterministic, ordinal)
				}
			}
		}
	} else if !errors.Is(err, core.ErrNotFound) {
		return nil, err
	} else {
		// record the spawn metadata before submitting the child job
		spawn := asyncChildSpawn{
			ChildJobID:        childJobKey.JobId,
			JobType:           jobType,
			InputHash:         inputHash,
			NotificationJobID: string(notifyJobID),
		}
		raw, err := json.Marshal(spawn)
		if err != nil {
			return nil, err
		}
		now := time.Now().UTC()
		chap, err := payloadToChapter(json.RawMessage(raw), nil, ordinal, jobType, r.engine.workerId, payloadKindAppChildJob, inputHash, now, chapterMetadata{})
		if err != nil {
			return nil, err
		}
		if err := r.engine.strata.SaveChapter(context.TODO(), key, chap); err != nil {
			return nil, err
		}
	}

	// ensure the child story exists
	childKey := childJobKey.ToStoryKey()
	if _, err := r.engine.strata.Chapter(ctx, childKey, 0); err != nil {
		if !errors.Is(err, core.ErrNotFound) {
			return nil, err
		}
		now := time.Now().UTC()
		co, err := taskDataToCreatOptions(data, 0, jobType, r.engine.workerId, payloadKindApp, inputHash, now, chapterMetadata{Attempt: 1})
		if err != nil {
			return nil, err
		}
		if _, err := r.engine.strata.CreateStory(ctx, childKey, co); err != nil {
			return nil, err
		}
	}

	runPolicy := normalizeRunPolicy(r.jobPolicy)
	if err := r.engine.ensureChildAndNotificationJobs(ctx, pgwf.JobID(childJobKey.JobId), notifyJobID, jobType, runPolicy, r.GetJobKey(), ordinal); err != nil {
		return nil, err
	}

	return swf.NewFuture(childJobKey, func(waitCtx context.Context) (swf.TaskData, error) {
		return r.awaitChild(waitCtx, childJobKey, ordinal, notifyJobID, "task", inputRef, invocationDeadline, totalDeadline, invocationLimit, totalLimit)
	}), nil
}

func (r *runner) Run(ctx context.Context, lease *pgwf.Lease) {
	if ctx == nil {
		ctx = context.Background()
	}
	r.ctx = ctx
	if r.engine != nil {
		defer r.engine.resetAwaitState(r.jobId)
	}
	_ = lease.WithKeepAlive(r.engine.udb)

	key := r.GetJobKey().ToStoryKey()
	chap0, err := r.getChapter(0)
	if err != nil {
		r.logger.Error("failed to get initial chapter", "error", err)
		return
	}
	env0, err := decodeChapterEnvelope(chap0.Body())
	if err != nil {
		r.logger.Error("failed to decode initial chapter", "error", err)
		return
	}
	if env0.Meta.RunPolicy != nil {
		r.jobPolicy = mergeRunPolicy(*env0.Meta.RunPolicy, r.jobPolicy)
	}
	r.jobPolicy = normalizeRunPolicy(r.jobPolicy)
	inputData, err := envelopeToTaskData(env0, chap0.Artifacts())
	if err != nil {
		r.logger.Error("failed to decode initial chapter payload", "error", err)
		return
	}

	retryCfg := r.jobPolicy.Retry
	invocationTimeout := durationPtrToDuration(r.jobPolicy.InvocationTimeout)
	totalTimeout := durationPtrToDuration(r.jobPolicy.TotalTimeout)

	inputHash, err := computeInputHash(ctx, inputData)
	if err != nil {
		r.logger.Error("failed to hash job input", "error", err)
		return
	}
	inputRef := &swf.InputReference{Ordinal: 0, Hash: inputHash}
	totalDeadline := r.jobTotalDeadline(env0, totalTimeout)

	for {
		now := time.Now()
		if !totalDeadline.IsZero() && now.After(totalDeadline) {
			jobErr := swf.NewTimeoutError("job", totalTimeout, swf.TimeoutScopeTotal, inputRef, false)
			r.logger.Error("job total timeout", "error", jobErr)
			return
		}
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

		var output swf.JobData
		var jobErr error
		type jobResult struct {
			output swf.JobData
			err    error
		}
		resultCh := make(chan jobResult, 1)
		go func() {
			defer func() {
				if rec := recover(); rec != nil {
					jobErr = panicToAppError(rec)
				}
			}()
			output, jobErr = r.worker.JobWorker.Run(r, inputData)
			resultCh <- jobResult{output: output, err: jobErr}
		}()

		deadline := attemptInvocationDeadline
		if deadline.IsZero() || (!totalDeadline.IsZero() && totalDeadline.Before(deadline)) {
			deadline = totalDeadline
		}
		if deadline.IsZero() {
			res := <-resultCh
			output, jobErr = res.output, res.err
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
				output, jobErr = res.output, res.err
			case <-timer.C:
				if !totalDeadline.IsZero() && deadline.Equal(totalDeadline) {
					jobErr = swf.NewTimeoutError("job", totalTimeout, swf.TimeoutScopeTotal, inputRef, false)
				} else {
					jobErr = swf.NewTimeoutError("job", invocationTimeout, swf.TimeoutScopeInvocation, inputRef, true)
				}
			}
		}

		now = time.Now()
		if jobErr == nil {
			if !totalDeadline.IsZero() && now.After(totalDeadline) {
				jobErr = swf.NewTimeoutError("job", totalTimeout, swf.TimeoutScopeTotal, inputRef, false)
			} else if !attemptInvocationDeadline.IsZero() && now.After(attemptInvocationDeadline) {
				jobErr = swf.NewTimeoutError("job", invocationTimeout, swf.TimeoutScopeInvocation, inputRef, true)
			}
		}

		ordinal := r.storyCounter
		r.storyCounter++

		attempt := 1
		maxAttempts := int(retryCfg.MaximumAttempts)
		if cached, err := r.engine.strata.Chapter(ctx, key, ordinal); err == nil {
			env, decErr := decodeChapterEnvelope(cached.Body())
			if decErr != nil {
				r.logger.Error("decode cached job result", "error", decErr)
				return
			}
			if env.Meta.InputHash != "" && env.Meta.InputHash != inputHash {
				r.logger.Error("job run not deterministic", "ordinal", ordinal)
				return
			}
			priorAttempt := env.Meta.Attempt
			if priorAttempt > 0 {
				attempt = priorAttempt + 1
			}
			_, payloadErr := envelopeToTaskData(env, cached.Artifacts())
			if payloadErr == nil {
				_ = lease.Complete(ctx, r.engine.udb)
				return
			}
			retryable := isRetryable(payloadErr, retryCfg)
			if !retryable || priorAttempt >= maxAttempts {
				_ = lease.Complete(ctx, r.engine.udb)
				return
			}
			backoff := time.Duration(0)
			if priorAttempt > 0 {
				backoff = computeBackoff(retryCfg, priorAttempt)
			}
			if backoff > 0 {
				wakeAt := env.Meta.CreatedAt.Add(backoff)
				if time.Now().Before(wakeAt) {
					if err := r.awaitUntil(wakeAt, ordinal, priorAttempt, "job", inputRef, time.Time{}, totalDeadline, 0, totalTimeout); err != nil {
						r.logger.Error("job await failed", "error", err)
						return
					}
				}
			}
		} else if err != nil && !errors.Is(err, core.ErrNotFound) {
			r.logger.Error("failed to check cached job attempt", "error", err)
			return
		}

		if jobErr != nil {
			r.logger.Error("job worker run failed", "error", jobErr, "attempt", attempt)
		}

		payloadKind := payloadKindApp
		originalErr := jobErr
		var payload json.RawMessage
		artifacts := []swf.Artifact{}
		if originalErr != nil {
			var tdErr error
			payload, payloadKind, tdErr = errorPayloadFromError(originalErr, inputRef)
			if tdErr != nil {
				r.logger.Error("failed to marshal error payload", "error", tdErr)
				return
			}
		} else {
			if output == nil {
				raw, _ := json.Marshal(swf.SystemErrorPayload{Message: "missing job output", InputRef: inputRef})
				payload = raw
				payloadKind = payloadKindSystemError
			} else {
				dataBytes, err := output.GetData()
				if err != nil {
					r.logger.Error("failed to get job output data", "error", err)
					return
				}
				payload = dataBytes
				artifacts, err = output.GetArtifacts()
				if err != nil {
					r.logger.Error("failed to get job output artifacts", "error", err)
					return
				}
			}
		}

		retryable := isRetryable(originalErr, retryCfg)
		now = time.Now().UTC()
		backoff := time.Duration(0)
		if originalErr != nil && retryable && attempt < maxAttempts {
			backoff = computeBackoff(retryCfg, attempt)
		}
		meta := chapterMetadata{
			Attempt:  attempt,
			InputRef: inputRef,
		}

		chap, err := payloadToChapter(payload, artifacts, ordinal, r.worker.JobWorker.Name(), r.engine.workerId, payloadKind, inputHash, now, meta)
		if err != nil {
			r.logger.Error("failed to build chapter", "error", err)
			return
		}

		err = r.engine.strata.SaveChapter(context.TODO(), key, chap)
		if err != nil {
			r.logger.Error("failed to save chapter", "error", err)
			return
		}

		err = lease.Complete(ctx, r.engine.udb)
		if err != nil {
			r.logger.Error("failed to complete lease", "error", err)
		}

		if originalErr == nil {
			return
		}
		if retryable && attempt < maxAttempts {
			attempt++
			if backoff > 0 {
				if err := r.awaitUntil(now.Add(backoff), ordinal, attempt-1, "job", inputRef, time.Time{}, totalDeadline, 0, totalTimeout); err != nil {
					r.logger.Error("job await failed", "error", err)
					return
				}
			}
			continue
		}
		return
	}
}
