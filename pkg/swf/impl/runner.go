package impl

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"runtime"
	"sync"
	"time"

	"github.com/colony-2/pgwf-go/pkg/pgwf"
	strata "github.com/colony-2/strata-go/pkg/client/artifact"
	"github.com/colony-2/strata-go/pkg/client/core"
	"github.com/colony-2/strata-go/pkg/client/story"
	"github.com/colony-2/swf-go/pkg/swf"
	swfinternal "github.com/colony-2/swf-go/pkg/swf/internal"
)

type runner struct {
	jobId        pgwf.JobID
	tenantId     string
	worker       *swf.WorkSet
	storyCounter int64
	backend      swfinternal.RunnerBackend
	lease        swfinternal.Lease
	logger       *slog.Logger
	jobPolicy    swf.RunPolicy
	capability   pgwf.Capability
	workerId     string
	observer     swf.ReattemptObserver
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

func durationPtrToDuration(d *swf.Duration) time.Duration {
	if d == nil {
		return 0
	}
	return time.Duration(*d)
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
	chap, err := r.backend.GetChapter(ctx, key, startOrdinal)
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
	if ctx == nil {
		ctx = context.Background()
	}
	info := swfinternal.AwaitInfo{
		JobKey:  r.GetJobKey(),
		Ordinal: ordinal,
		Attempt: attempt,
	}
	if err := r.backend.AwaitUntil(ctx, wakeAt, info); err != nil {
		return err
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

func (r *runner) DoTask(policy swf.RunPolicy, taskType string, data swf.TaskData) (swf.TaskData, error) {
	ctx := r.ctx
	if ctx == nil {
		ctx = context.Background()
	}

	inputHash, err := computeInputHash(ctx, data)
	if err != nil {
		return nil, fmt.Errorf("compute input hash: %w", err)
	}

	// Debug logging for task input hash computation
	inputData, _ := data.GetData()
	inputArtifacts, _ := data.GetArtifacts()
	r.logger.Debug("computed task input hash",
		"taskType", taskType,
		"inputHash", inputHash,
		"dataLength", len(inputData),
		"artifactCount", len(inputArtifacts))

	basePolicy := r.jobPolicy
	effectivePolicy := normalizeRunPolicy(mergeRunPolicy(policy, basePolicy))
	retryCfg := effectivePolicy.Retry
	invocationTimeout := durationPtrToDuration(effectivePolicy.InvocationTimeout)
	totalTimeout := durationPtrToDuration(effectivePolicy.TotalTimeout)
	maxAttempts := int(retryCfg.MaximumAttempts)

	key := r.GetJobKey().ToStoryKey()
	attempt := 1

	// Main retry loop - each attempt gets a new ordinal (chapters are write-once)
	for {
		// Get ordinal for this attempt
		ordinal := r.storyCounter
		r.storyCounter++

		inputRef := &swf.InputReference{Ordinal: ordinal - 1}
		if inputRef.Ordinal < 0 {
			inputRef.Ordinal = 0
		}
		inputRef.Hash = inputHash

		totalDeadline, err := r.taskTotalDeadline(ctx, key, ordinal, totalTimeout)
		if err != nil {
			return nil, fmt.Errorf("compute total deadline: %w", err)
		}

		// CACHE-FIRST: Check if we already have a result at this ordinal
		chap, err := r.backend.GetChapter(ctx, key, ordinal)
		if err == nil {
			// Cached result exists
			env, decErr := decodeChapterEnvelope(chap.Body())
			if decErr != nil {
				return nil, fmt.Errorf("%w: decode cached chapter: %v", swf.ErrWorkflowNotDeterministic, decErr)
			}
			if env.ChapterType != chapterTypeTaskAttemptOutcome && env.ChapterType != chapterTypeRestartExtra {
				return nil, fmt.Errorf("%w: unexpected chapter type %q at ordinal %d", swf.ErrWorkflowNotDeterministic, env.ChapterType, ordinal)
			}

			r.logger.Debug("checking cached task result",
				"taskType", taskType,
				"ordinal", ordinal,
				"cachedInputHash", env.Meta.InputHash,
				"computedInputHash", inputHash,
				"hashMatch", env.Meta.InputHash == inputHash)

			if env.Meta.InputHash == "" {
				return nil, fmt.Errorf("%w: ordinal %d task %s missing input hash", swf.ErrMissingInputHash, ordinal, taskType)
			}
			if env.Meta.InputHash != inputHash {
				r.logger.Error("task input hash mismatch",
					"taskType", taskType,
					"ordinal", ordinal,
					"cachedInputHash", env.Meta.InputHash,
					"computedInputHash", inputHash)
				artifacts := convertStrataArtifacts(chap.Artifacts(), key.StoryID, ordinal)
				cachedOutput, cachedOutputErr := envelopeToTaskData(env, artifacts)
				meta := swf.TaskDeterminismMeta{
					Ordinal:       env.Meta.Ordinal,
					TaskType:      env.Meta.TaskType,
					WorkerID:      env.Meta.WorkerID,
					CreatedAt:     env.Meta.CreatedAt,
					Attempt:       env.Meta.Attempt,
					MaxAttempts:   env.Meta.MaxAttempts,
					NextAttemptAt: env.Meta.NextAttemptAt,
					BackoffMillis: env.Meta.BackoffMillis,
					Retryable:     env.Meta.Retryable,
					InputHash:     env.Meta.InputHash,
					InputRef:      env.Meta.InputRef,
					RunPolicy:     env.Meta.RunPolicy,
					InputPayload:  env.Meta.Input,
					Version:       env.Meta.Version,
				}
				return nil, swf.TaskInputMismatchError{
					TaskType:          taskType,
					Ordinal:           ordinal,
					CachedInputHash:   env.Meta.InputHash,
					ComputedInputHash: inputHash,
					CachedInput:       env.Meta.Input,
					CachedOutput:      cachedOutput,
					CachedOutputErr:   cachedOutputErr,
					Meta:              meta,
				}
			}
			if !totalDeadline.IsZero() && time.Now().After(totalDeadline) {
				return nil, swf.NewTimeoutError("task", totalTimeout, swf.TimeoutScopeTotal, inputRef, false)
			}

			// Try to decode result
			artifacts := convertStrataArtifacts(chap.Artifacts(), key.StoryID, ordinal)
			td, payloadErr := envelopeToTaskData(env, artifacts)
			if payloadErr == nil {
				// Cached success - return immediately
				return td, nil
			}

			// Cached error - check if retryable
			priorAttempt := env.Meta.Attempt
			if priorAttempt > 0 {
				attempt = priorAttempt + 1
			}
			retryable := isRetryable(payloadErr, retryCfg)
			if !retryable || priorAttempt >= maxAttempts {
				// Non-retryable or max attempts - return error
				return nil, payloadErr
			}

			// Retryable error - wait backoff and continue to next iteration (new ordinal)
			r.emitTaskReattemptBoundary(taskType, ordinal, priorAttempt, payloadErr, r.storyCounter, priorAttempt+1)
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
			// Continue to next iteration for retry (new ordinal, with incremented attempt)
			continue
		} else if !errors.Is(err, core.ErrNotFound) {
			return nil, fmt.Errorf("failed to get chapter %d: %w", ordinal, err)
		}

		// No cached result - need to execute
		worker, capabilityExistsLocally := r.worker.TaskWorkers[taskType]
		if !capabilityExistsLocally {
			inputOrdinal := ordinal - 1
			if inputOrdinal < 0 {
				inputOrdinal = 0
			}

			deps := pgwf.JobDependencies{
				NextNeed: pgwf.Capability(r.worker.JobWorker.Name() + ":" + taskType),
				WaitFor:  nil,
			}
			if invocationTimeout > 0 {
				deps.Alternate = &pgwf.AlternateNext{
					Need:  pgwf.Capability(r.worker.JobWorker.Name()),
					After: invocationTimeout,
				}
			}

			err = r.lease.Reschedule(context.TODO(), deps, jobPayload{
				RunPolicy: r.jobPolicy,
				TaskWait: &taskWait{
					InputStep:  inputOrdinal,
					OutputStep: ordinal,
					Next:       r.worker.JobWorker.Name(), // use only the job type for next need as we can't determine here what the next need is.
					InputHash:  inputHash,
				},
			})

			if err != nil {
				return nil, fmt.Errorf("failed to reschedule job: %w", err)
			}

			prematureCloseOut()
			panic("unreachable")
		}

		// Execute task
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
		exitCh := make(chan struct{})
		var exitOnce sync.Once
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
						func(jobIds ...string) error {
							info := swfinternal.AwaitInfo{
								JobKey:  r.GetJobKey(),
								Ordinal: ordinal,
								Attempt: attemptNum,
							}
							rescheduled, err := r.backend.AwaitJobs(ctx, jobIds, info)
							if err != nil {
								return err
							}
							if !rescheduled {
								return nil
							}
							exitOnce.Do(func() {
								close(exitCh)
							})
							runtime.Goexit()
							return nil
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
		var dataBytes swf.Data
		var outputArtifactDigests []string
		if taskErr != nil {
			var tdErr error
			payload, payloadKind, tdErr = errorPayloadFromError(taskErr, inputRef)
			if tdErr != nil {
				return nil, tdErr
			}
			// Extract artifacts even on error
			if output != nil {
				artifacts, err = output.GetArtifacts()
				if err != nil {
					r.logger.Warn("Failed to extract artifacts from error case",
						"error", err, "taskType", taskType, "ordinal", ordinal)
					artifacts = []swf.Artifact{}
				}
			}
		} else {
			// success
			dataBytes, err = output.GetData()
			if err != nil {
				return nil, err
			}
			payload = dataBytes
			artifacts, err = output.GetArtifacts()
			if err != nil {
				return nil, err
			}
			outputArtifactDigests, err = validateOutputArtifacts(ctx, artifacts)
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
		inputPayload := json.RawMessage(nil)
		if swf.TaskInputStorageEnabled() {
			inputData, err := data.GetData()
			if err != nil {
				return nil, err
			}
			inputPayload = append(json.RawMessage(nil), inputData...)
		}
		meta := chapterMetadata{
			Attempt:      attempt,
			InputRef:     inputRef,
			InputPayload: inputPayload,
		}

		chap, err = payloadToChapter(payload, artifacts, ordinal, taskType, r.workerId, chapterTypeTaskAttemptOutcome, payloadKind, inputHash, now, meta)
		if err != nil {
			return nil, err
		}

		err = r.backend.SaveChapter(context.TODO(), key, chap)
		if err != nil {
			return nil, err
		}
		assignArtifactKeys(artifacts, r.GetJobKey().JobId, ordinal)

		returnedOutput := output
		if originalErr == nil {
			returnedOutput, err = r.backend.AfterSaveTaskOutput(output, dataBytes, artifacts, outputArtifactDigests, key, ordinal, r.logger)
			if err != nil {
				return nil, err
			}
		}

		// Cleanup output artifacts after successful save
		cleanupArtifacts(context.TODO(), artifacts, r.logger)

		if originalErr == nil {
			// Success - cleanup input artifacts and return
			inputArtifacts, _ := data.GetArtifacts()
			cleanupArtifacts(context.TODO(), inputArtifacts, r.logger)
			return returnedOutput, nil
		}

		// Error - check if should retry
		if retryable && attempt < maxAttempts {
			// Cleanup input artifacts before retry
			inputArtifacts, _ := data.GetArtifacts()
			cleanupArtifacts(context.TODO(), inputArtifacts, r.logger)

			if backoff > 0 {
				if err := r.awaitUntil(now.Add(backoff), ordinal, attempt, "task", inputRef, time.Time{}, totalDeadline, 0, totalTimeout); err != nil {
					return nil, err
				}
			}
			r.emitTaskReattemptBoundary(taskType, ordinal, attempt, originalErr, r.storyCounter, attempt+1)
			// Increment attempt for next iteration
			attempt++
			// Continue to next iteration (new ordinal, incremented attempt)
			continue
		}

		// Max attempts or non-retryable - cleanup input artifacts and return error
		inputArtifacts, _ := data.GetArtifacts()
		cleanupArtifacts(context.TODO(), inputArtifacts, r.logger)
		return nil, originalErr
	}
}

// cleanupArtifacts calls Cleanup() on each artifact and logs any errors.
// Cleanup errors do not fail the workflow.
func cleanupArtifacts(ctx context.Context, artifacts []swf.Artifact, logger *slog.Logger) {
	for _, art := range artifacts {
		if err := art.Cleanup(); err != nil {
			logger.Warn("artifact cleanup failed", "name", art.Name(), "error", err)
		} else {
			logger.Debug("artifact cleaned up", "name", art.Name())
		}
	}
}

// convertStrataArtifacts converts strata artifacts to swf artifacts
func convertStrataArtifacts(strataArts []strata.Artifact, jobID string, ordinal int64) []swf.Artifact {
	artifacts := make([]swf.Artifact, 0, len(strataArts))
	for _, a := range strataArts {
		artifacts = append(artifacts, swf.FromStrataArtifact(a))
	}
	assignArtifactKeys(artifacts, jobID, ordinal)
	return artifacts
}

func prematureCloseOut() {
	// do any finalization
	runtime.Goexit()
}

var _ swf.JobContext = &runner{}

type RunError struct {
	Err error
}

func (r *runner) observerOrNoop() swf.ReattemptObserver {
	if r.observer != nil {
		return r.observer
	}
	return noopReattemptObserver{}
}

func (r *runner) emitTaskReattemptBoundary(taskType string, prevOrdinal int64, prevAttempt int, prevErr error, nextOrdinal int64, nextAttempt int) {
	obs := r.observerOrNoop()
	obs.OnTaskReattemptBoundary(swf.TaskReattemptBoundary{
		JobKey:                 r.GetJobKey(),
		TaskType:               taskType,
		PreviousAttemptOrdinal: prevOrdinal,
		PreviousAttemptNumber:  prevAttempt,
		PreviousAttemptError:   prevErr,
		NextAttemptOrdinal:     nextOrdinal,
		NextAttemptNumber:      nextAttempt,
	})
}

func (r *runner) emitJobReattemptBoundary(prevOrdinal int64, prevAttempt int, prevErr error, nextOrdinal int64, nextAttempt int) {
	obs := r.observerOrNoop()
	obs.OnJobReattemptBoundary(swf.JobReattemptBoundary{
		JobKey:                 r.GetJobKey(),
		PreviousAttemptOrdinal: prevOrdinal,
		PreviousAttemptNumber:  prevAttempt,
		PreviousAttemptError:   prevErr,
		NextAttemptOrdinal:     nextOrdinal,
		NextAttemptNumber:      nextAttempt,
	})
}

func (r *runner) getChapter(ordinal int64) (story.Chapter, error) {
	return r.backend.GetChapter(context.TODO(), r.GetJobKey().ToStoryKey(), ordinal)
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

func (r *runner) AwaitJobs(jobIds ...string) error {
	ctx := r.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	info := swfinternal.AwaitInfo{
		JobKey: r.GetJobKey(),
	}
	rescheduled, err := r.backend.AwaitJobs(ctx, jobIds, info)
	if err != nil {
		return err
	}
	if rescheduled {
		prematureCloseOut()
	}
	return nil
}

// jobExecutionConfig holds the configuration for a job execution attempt
type jobExecutionConfig struct {
	retryCfg          swf.RetryPolicy
	invocationTimeout time.Duration
	totalTimeout      time.Duration
	inputRef          *swf.InputReference
	totalDeadline     time.Time
}

// loadInitialChapterAndPolicy loads chapter 0, decodes it, merges run policy, and returns input data
func (r *runner) loadInitialChapterAndPolicy() (swf.TaskData, chapterEnvelope, error) {
	chap0, err := r.getChapter(0)
	if err != nil {
		return nil, chapterEnvelope{}, fmt.Errorf("failed to get initial chapter: %w", err)
	}
	env0, err := decodeChapterEnvelope(chap0.Body())
	if err != nil {
		return nil, chapterEnvelope{}, fmt.Errorf("failed to decode initial chapter: %w", err)
	}
	if env0.ChapterType != chapterTypeJobStart {
		return nil, chapterEnvelope{}, fmt.Errorf("%w: unexpected chapter type %q at ordinal 0", swf.ErrWorkflowNotDeterministic, env0.ChapterType)
	}
	if env0.Meta.RunPolicy != nil {
		r.jobPolicy = mergeRunPolicy(*env0.Meta.RunPolicy, r.jobPolicy)
	}
	r.jobPolicy = normalizeRunPolicy(r.jobPolicy)
	artifacts := convertStrataArtifacts(chap0.Artifacts(), r.GetJobKey().JobId, chap0.Ordinal())
	inputData, err := envelopeToTaskData(env0, artifacts)
	if err != nil {
		return nil, chapterEnvelope{}, fmt.Errorf("failed to decode initial chapter payload: %w", err)
	}
	return inputData, env0, nil
}

// setupJobExecutionConfig computes retry config, timeouts, input hash, and deadlines
func (r *runner) setupJobExecutionConfig(ctx context.Context, inputData swf.TaskData, env0 chapterEnvelope) (jobExecutionConfig, error) {
	retryCfg := r.jobPolicy.Retry
	invocationTimeout := durationPtrToDuration(r.jobPolicy.InvocationTimeout)
	totalTimeout := durationPtrToDuration(r.jobPolicy.TotalTimeout)

	inputHash, err := computeInputHash(ctx, inputData)
	if err != nil {
		return jobExecutionConfig{}, fmt.Errorf("failed to hash job input: %w", err)
	}

	// Debug logging for job input hash computation
	jobInputData, _ := inputData.GetData()
	jobInputArtifacts, _ := inputData.GetArtifacts()
	r.logger.Debug("computed job input hash",
		"inputHash", inputHash,
		"dataLength", len(jobInputData),
		"artifactCount", len(jobInputArtifacts))

	inputRef := &swf.InputReference{Ordinal: 0, Hash: inputHash}
	totalDeadline := r.jobTotalDeadline(env0, totalTimeout)

	return jobExecutionConfig{
		retryCfg:          retryCfg,
		invocationTimeout: invocationTimeout,
		totalTimeout:      totalTimeout,
		inputRef:          inputRef,
		totalDeadline:     totalDeadline,
	}, nil
}

// checkTotalTimeoutExceeded checks if the total deadline has been exceeded
func (r *runner) checkTotalTimeoutExceeded(totalDeadline time.Time, totalTimeout time.Duration, inputRef *swf.InputReference) error {
	now := time.Now()
	if !totalDeadline.IsZero() && now.After(totalDeadline) {
		return swf.NewTimeoutError("job", totalTimeout, swf.TimeoutScopeTotal, inputRef, false)
	}
	return nil
}

// setupAttemptDeadlines sets the invocation deadline and stores current execution state
func (r *runner) setupAttemptDeadlines(invocationTimeout time.Duration, totalDeadline time.Time, totalTimeout time.Duration, inputRef *swf.InputReference) time.Time {
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

type jobResult struct {
	output swf.JobData
	err    error
}

// executeJobWorkerAsync runs the job worker in a goroutine with panic recovery
func (r *runner) executeJobWorkerAsync(inputData swf.TaskData) chan jobResult {
	resultCh := make(chan jobResult, 1)
	go func() {
		var output swf.JobData
		var jobErr error
		defer func() {
			if rec := recover(); rec != nil {
				jobErr = panicToAppError(rec)
			}
			resultCh <- jobResult{output: output, err: jobErr}
		}()
		output, jobErr = r.worker.JobWorker.Run(r, inputData)
	}()
	return resultCh
}

// waitForJobResultWithDeadline waits for job result, applying invocation and total deadlines
func (r *runner) waitForJobResultWithDeadline(resultCh chan jobResult, attemptInvocationDeadline, totalDeadline time.Time, invocationTimeout, totalTimeout time.Duration, inputRef *swf.InputReference) (swf.JobData, error) {
	deadline := attemptInvocationDeadline
	if deadline.IsZero() || (!totalDeadline.IsZero() && totalDeadline.Before(deadline)) {
		deadline = totalDeadline
	}

	var output swf.JobData
	var jobErr error
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
	return output, jobErr
}

// validatePostExecutionTimeouts checks if timeouts were exceeded after job execution
func (r *runner) validatePostExecutionTimeouts(jobErr error, attemptInvocationDeadline, totalDeadline time.Time, invocationTimeout, totalTimeout time.Duration, inputRef *swf.InputReference) error {
	if jobErr != nil {
		return jobErr
	}
	now := time.Now()
	if !totalDeadline.IsZero() && now.After(totalDeadline) {
		return swf.NewTimeoutError("job", totalTimeout, swf.TimeoutScopeTotal, inputRef, false)
	}
	if !attemptInvocationDeadline.IsZero() && now.After(attemptInvocationDeadline) {
		return swf.NewTimeoutError("job", invocationTimeout, swf.TimeoutScopeInvocation, inputRef, true)
	}
	return nil
}

// checkCachedJobResult checks if a cached job result exists and handles retry logic
// Returns: (output, nextAttempt, priorAttempt, cached, terminal, priorErr, err)
func (r *runner) checkCachedJobResult(ctx context.Context, key story.Key, ordinal int64, inputHash string, retryCfg swf.RetryPolicy, totalDeadline time.Time, totalTimeout time.Duration, inputRef *swf.InputReference) (swf.JobData, int, int, bool, bool, error, error) {
	maxAttempts := int(retryCfg.MaximumAttempts)
	cached, err := r.backend.GetJobAttemptOutcome(ctx, key, ordinal)
	if errors.Is(err, core.ErrNotFound) {
		// No cached result, need to execute
		return nil, 1, 0, false, false, nil, nil
	}
	if err != nil {
		return nil, 0, 0, false, false, nil, fmt.Errorf("failed to get chapter %d: %w", ordinal, err)
	}

	// Found cached result
	env, decErr := decodeChapterEnvelope(cached.Body())
	if decErr != nil {
		return nil, 0, 0, false, false, nil, fmt.Errorf("%w: decode cached chapter: %v", swf.ErrWorkflowNotDeterministic, decErr)
	}
	if env.ChapterType != chapterTypeJobAttemptOutcome {
		return nil, 0, 0, false, false, nil, fmt.Errorf("%w: unexpected chapter type %q at ordinal %d", swf.ErrWorkflowNotDeterministic, env.ChapterType, ordinal)
	}

	r.logger.Debug("checking cached job result",
		"ordinal", ordinal,
		"cachedInputHash", env.Meta.InputHash,
		"computedInputHash", inputHash,
		"hashMatch", env.Meta.InputHash == inputHash)

	if env.Meta.InputHash != "" && env.Meta.InputHash != inputHash {
		r.logger.Error("job result input hash mismatch",
			"ordinal", ordinal,
			"cachedInputHash", env.Meta.InputHash,
			"computedInputHash", inputHash)
		return nil, 0, 0, false, false, nil, fmt.Errorf("%w: ordinal %d job result input hash mismatch", swf.ErrWorkflowNotDeterministic, ordinal)
	}
	if !totalDeadline.IsZero() && time.Now().After(totalDeadline) {
		return nil, 0, 0, false, false, nil, swf.NewTimeoutError("job", totalTimeout, swf.TimeoutScopeTotal, inputRef, false)
	}

	priorAttempt := env.Meta.Attempt
	nextAttempt := priorAttempt + 1
	if priorAttempt <= 0 {
		nextAttempt = 1
	}

	// Try to decode the result
	artifacts := convertStrataArtifacts(cached.Artifacts(), key.StoryID, cached.Ordinal())
	output, payloadErr := envelopeToTaskData(env, artifacts)
	if payloadErr == nil {
		// Cached success - terminal
		return output, nextAttempt, priorAttempt, true, true, nil, nil
	}

	// Cached error - check if retryable
	retryable := isRetryable(payloadErr, retryCfg)
	if !retryable || priorAttempt >= maxAttempts {
		// Non-retryable or max attempts - terminal
		return nil, nextAttempt, priorAttempt, true, true, payloadErr, payloadErr
	}

	// Retryable error - need to wait backoff then retry
	backoff := time.Duration(0)
	if priorAttempt > 0 {
		backoff = computeBackoff(retryCfg, priorAttempt)
	}
	if backoff > 0 {
		wakeAt := env.Meta.CreatedAt.Add(backoff)
		if time.Now().Before(wakeAt) {
			if err := r.awaitUntil(wakeAt, ordinal, priorAttempt, "job", inputRef, time.Time{}, totalDeadline, 0, totalTimeout); err != nil {
				return nil, 0, 0, false, false, nil, err
			}
		}
	}

	// After backoff, need to retry (not terminal)
	return nil, nextAttempt, priorAttempt, true, false, payloadErr, nil
}

// prepareJobResultPayload converts job output or error into a payload and artifacts
func (r *runner) prepareJobResultPayload(output swf.JobData, originalErr error, inputRef *swf.InputReference) (json.RawMessage, []swf.Artifact, string, error) {
	artifacts := []swf.Artifact{}
	kind := payloadKindApp

	if originalErr != nil {
		payload, errKind, tdErr := errorPayloadFromError(originalErr, inputRef)
		if tdErr != nil {
			return nil, nil, "", fmt.Errorf("failed to marshal error payload: %w", tdErr)
		}
		// Extract artifacts even on error
		if output != nil {
			var err error
			artifacts, err = output.GetArtifacts()
			if err != nil {
				r.logger.Warn("Failed to extract artifacts from job error case", "error", err)
				artifacts = []swf.Artifact{}
			}
		}
		return payload, artifacts, errKind, nil
	}

	if output == nil {
		raw, _ := json.Marshal(swf.SystemErrorPayload{Message: "missing job output", InputRef: inputRef})
		return json.RawMessage(raw), artifacts, payloadKindSystemError, nil
	}

	dataBytes, err := output.GetData()
	if err != nil {
		return nil, nil, "", fmt.Errorf("failed to get job output data: %w", err)
	}
	artifacts, err = output.GetArtifacts()
	if err != nil {
		return nil, nil, "", fmt.Errorf("failed to get job output artifacts: %w", err)
	}
	return dataBytes, artifacts, kind, nil
}

// saveJobChapter saves the job result chapter
func (r *runner) saveJobChapter(key story.Key, payload json.RawMessage, artifacts []swf.Artifact, ordinal int64, workerName, inputHash string, kind string, attempt int, inputRef *swf.InputReference) error {
	now := time.Now().UTC()
	meta := chapterMetadata{
		Attempt:  attempt,
		InputRef: inputRef,
	}

	chap, err := payloadToChapter(payload, artifacts, ordinal, workerName, r.workerId, chapterTypeJobAttemptOutcome, kind, inputHash, now, meta)
	if err != nil {
		return fmt.Errorf("failed to build chapter: %w", err)
	}

	err = r.backend.SaveChapter(context.TODO(), key, chap)
	if err != nil {
		return fmt.Errorf("failed to save chapter: %w", err)
	}

	assignArtifactKeys(artifacts, key.StoryID, ordinal)

	return nil
}

// DoJob executes the job worker with retry logic, timeout handling, and result persistence
// Follows cache-first pattern: checks for cached result before executing
// Each retry attempt gets a new ordinal (chapters are write-once)
func (r *runner) DoJob(ctx context.Context) (swf.JobData, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	r.ctx = ctx
	if r.lease != nil {
		defer r.lease.StopKeepAlive()
		_ = r.lease.KeepAlive(ctx)
	}

	// Load initial chapter and setup job policy
	inputData, env0, err := r.loadInitialChapterAndPolicy()
	if err != nil {
		r.logger.Error(err.Error())
		return nil, err
	}

	// Setup execution configuration with timeouts and deadlines
	config, err := r.setupJobExecutionConfig(ctx, inputData, env0)
	if err != nil {
		r.logger.Error(err.Error())
		return nil, err
	}

	key := r.GetJobKey().ToStoryKey()
	maxAttempts := int(config.retryCfg.MaximumAttempts)
	attempt := 1

	// Main retry loop - each attempt gets a new ordinal (chapters are write-once)
	for {
		// Check if total timeout has been exceeded
		if err := r.checkTotalTimeoutExceeded(config.totalDeadline, config.totalTimeout, config.inputRef); err != nil {
			r.logger.Error("job total timeout", "error", err)
			jobResultOrdinal := r.storyCounter
			for {
				chap, chapErr := r.backend.GetChapter(ctx, key, jobResultOrdinal)
				if chapErr == nil {
					env, decErr := decodeChapterEnvelope(chap.Body())
					if decErr != nil {
						r.logger.Error("decode cached job result failed", "error", decErr)
						return nil, decErr
					}
					if env.Meta.TaskType == r.worker.JobWorker.Name() {
						artifacts := convertStrataArtifacts(chap.Artifacts(), key.StoryID, chap.Ordinal())
						if env.PayloadKind == payloadKindApp {
							inputArtifacts, _ := inputData.GetArtifacts()
							cleanupArtifacts(context.TODO(), inputArtifacts, r.logger)
							_ = r.lease.Complete(ctx)
							return nil, nil
						}
						_, payloadErr := envelopeToTaskData(env, artifacts)
						if payloadErr == nil {
							inputArtifacts, _ := inputData.GetArtifacts()
							cleanupArtifacts(context.TODO(), inputArtifacts, r.logger)
							_ = r.lease.Complete(ctx)
							return nil, nil
						}
						retryable := isRetryable(payloadErr, config.retryCfg)
						if !retryable {
							inputArtifacts, _ := inputData.GetArtifacts()
							cleanupArtifacts(context.TODO(), inputArtifacts, r.logger)
							_ = r.lease.Complete(ctx)
							return nil, payloadErr
						}
						if env.Meta.Attempt > 0 {
							attempt = env.Meta.Attempt + 1
						}
					}
					jobResultOrdinal++
					continue
				}
				if !errors.Is(chapErr, core.ErrNotFound) {
					r.logger.Error("failed to check cached job result", "error", chapErr)
					return nil, chapErr
				}
				break
			}
			r.storyCounter = jobResultOrdinal + 1

			payload, artifacts, payloadKind, prepErr := r.prepareJobResultPayload(nil, err, config.inputRef)
			if prepErr != nil {
				r.logger.Error(prepErr.Error())
				return nil, prepErr
			}
			if saveErr := r.saveJobChapter(key, payload, artifacts, jobResultOrdinal, r.worker.JobWorker.Name(), config.inputRef.Hash, payloadKind, attempt, config.inputRef); saveErr != nil {
				r.logger.Error(saveErr.Error())
				return nil, saveErr
			}
			if len(artifacts) > 0 {
				cleanupArtifacts(context.TODO(), artifacts, r.logger)
			}
			inputArtifacts, _ := inputData.GetArtifacts()
			cleanupArtifacts(context.TODO(), inputArtifacts, r.logger)
			_ = r.lease.Complete(ctx)
			return nil, err
		}

		// Setup deadlines for this attempt
		attemptInvocationDeadline := r.setupAttemptDeadlines(config.invocationTimeout, config.totalDeadline, config.totalTimeout, config.inputRef)

		// Execute job worker asynchronously (may call DoTask which will use storyCounter)
		resultCh := r.executeJobWorkerAsync(inputData)

		// Wait for result with deadline enforcement
		output, jobErr := r.waitForJobResultWithDeadline(resultCh, attemptInvocationDeadline, config.totalDeadline, config.invocationTimeout, config.totalTimeout, config.inputRef)

		// Check if the job was rescheduled (e.g., task needs to run on different engine)
		// In this case, output=nil and jobErr=nil, and the job should exit without saving
		if output == nil && jobErr == nil {
			// Job was rescheduled - lease was already updated by DoTask
			// Exit without saving job result
			return nil, nil
		}

		// Validate timeouts after execution
		jobErr = r.validatePostExecutionTimeouts(jobErr, attemptInvocationDeadline, config.totalDeadline, config.invocationTimeout, config.totalTimeout, config.inputRef)

		if jobErr != nil {
			r.logger.Error("job worker run failed", "error", jobErr, "attempt", attempt)
		}

		// NOW get the ordinal for the job result (after tasks have executed)
		jobResultOrdinal := r.storyCounter
		r.storyCounter++

		// Check if we already have a cached job result at this ordinal
		// (e.g., if we crashed after saving the result but before completing the lease)
		outputCached, nextAttempt, priorAttempt, cached, terminal, priorErr, err := r.checkCachedJobResult(ctx, key, jobResultOrdinal, config.inputRef.Hash, config.retryCfg, config.totalDeadline, config.totalTimeout, config.inputRef)
		if err != nil {
			r.logger.Error("check cached job result failed", "error", err)
			return nil, err
		}

		if cached {
			// Found cached job result - use it instead of fresh execution result
			if terminal {
				// Terminal result (success or non-retryable error) - complete and return
				_ = r.lease.Complete(ctx)
				if priorErr != nil {
					return nil, priorErr
				}
				return outputCached, nil
			}
			// Retryable error - update attempt number and retry
			r.emitJobReattemptBoundary(jobResultOrdinal, priorAttempt, priorErr, r.storyCounter, nextAttempt)
			attempt = nextAttempt
			// Continue to next iteration for retry
			continue
		}

		// No cached result - prepare and save the fresh execution result
		payload, artifacts, payloadKind, err := r.prepareJobResultPayload(output, jobErr, config.inputRef)
		if err != nil {
			r.logger.Error(err.Error())
			return nil, err
		}

		// Save the execution result at this ordinal (write-once)
		if err := r.saveJobChapter(key, payload, artifacts, jobResultOrdinal, r.worker.JobWorker.Name(), config.inputRef.Hash, payloadKind, attempt, config.inputRef); err != nil {
			r.logger.Error(err.Error())
			return nil, err
		}
		if len(artifacts) > 0 {
			cleanupArtifacts(context.TODO(), artifacts, r.logger)
		}

		// Determine if we're done or need to retry
		if jobErr == nil {
			// Success - cleanup input artifacts, complete lease and return
			inputArtifacts, _ := inputData.GetArtifacts()
			cleanupArtifacts(context.TODO(), inputArtifacts, r.logger)
			_ = r.lease.Complete(ctx)
			return output, nil
		}

		retryable := isRetryable(jobErr, config.retryCfg)
		if !retryable || attempt >= maxAttempts {
			// Non-retryable or max attempts reached - cleanup input artifacts, complete lease and return
			inputArtifacts, _ := inputData.GetArtifacts()
			cleanupArtifacts(context.TODO(), inputArtifacts, r.logger)
			_ = r.lease.Complete(ctx)
			return nil, jobErr
		}

		// Retryable error and under max attempts - cleanup input artifacts, wait backoff and retry with new ordinal
		inputArtifacts, _ := inputData.GetArtifacts()
		cleanupArtifacts(context.TODO(), inputArtifacts, r.logger)

		backoff := computeBackoff(config.retryCfg, attempt)
		r.emitJobReattemptBoundary(jobResultOrdinal, attempt, jobErr, r.storyCounter, attempt+1)
		attempt++
		if backoff > 0 {
			now := time.Now().UTC()
			if err := r.awaitUntil(now.Add(backoff), jobResultOrdinal, attempt-1, "job", config.inputRef, time.Time{}, config.totalDeadline, 0, config.totalTimeout); err != nil {
				r.logger.Error("job await failed", "error", err)
				return nil, err
			}
		}
		// Loop back - will check for next cached job result or execute again
	}
}
