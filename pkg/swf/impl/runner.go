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

		if err := r.engine.ensureNotificationJob(ctx, notificationJobID, pgwf.JobID(childJobKey.JobId), r.GetJobKey(), ordinal); err != nil {
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
		chap, err := r.engine.strata.Chapter(ctx, key, ordinal)
		if err == nil {
			// Cached result exists
			env, decErr := decodeChapterEnvelope(chap.Body())
			if decErr != nil {
				return nil, fmt.Errorf("%w: decode cached chapter: %v", swf.ErrWorkflowNotDeterministic, decErr)
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
				return nil, fmt.Errorf("%w: ordinal %d task %s", swf.ErrWorkflowNotDeterministic, ordinal, taskType)
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

			err = r.lease.Reschedule(context.TODO(), r.engine.udb, pgwf.JobDependencies{
				NextNeed: pgwf.Capability(r.worker.JobWorker.Name() + ":" + taskType),
				WaitFor:  nil,
			}, jobPayload{
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
							rescheduled, err := r.rescheduleAwaitJobs(jobIds...)
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

		chap, err = payloadToChapter(payload, artifacts, ordinal, taskType, r.engine.workerId, payloadKind, inputHash, now, meta)
		if err != nil {
			return nil, err
		}

		err = r.engine.strata.SaveChapter(context.TODO(), key, chap)
		if err != nil {
			return nil, err
		}
		assignArtifactKeys(artifacts, r.GetJobKey().JobId, ordinal)

		returnedOutput := output
		if originalErr == nil {
			returnedOutput, err = wrapOutputArtifactsWithFallback(output, dataBytes, artifacts, outputArtifactDigests, key, ordinal, r.engine.strata, r.logger)
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

func (r *runner) rescheduleAwaitJobs(jobIds ...string) (bool, error) {
	if len(jobIds) == 0 {
		return false, fmt.Errorf("at least one jobId is required")
	}
	if r.engine == nil || r.engine.udb == nil {
		return false, fmt.Errorf("engine is not available")
	}
	if r.lease == nil {
		return false, fmt.Errorf("lease is not available")
	}
	capability := r.capability
	if capability == "" {
		capability = r.lease.NextNeed()
	}
	if capability == "" {
		return false, fmt.Errorf("capability is required")
	}
	completed, err := r.awaitJobsComplete(jobIds...)
	if err != nil {
		return false, err
	}
	if completed {
		return false, nil
	}
	waitFor := make([]pgwf.JobID, 0, len(jobIds))
	for _, id := range jobIds {
		if id == "" {
			return false, fmt.Errorf("jobId cannot be empty")
		}
		waitFor = append(waitFor, pgwf.JobID(id))
	}
	payload := r.lease.Payload()
	if len(payload) == 0 {
		payload = json.RawMessage(`{}`)
	}
	deps := pgwf.JobDependencies{
		NextNeed: capability,
		WaitFor:  waitFor,
	}
	if err := r.lease.Reschedule(context.TODO(), r.engine.udb, deps, payload); err != nil {
		return false, err
	}
	return true, nil
}

func (r *runner) awaitJobsComplete(jobIds ...string) (bool, error) {
	if r.engine == nil {
		return false, fmt.Errorf("engine is not available")
	}
	tenantId := r.tenantId
	if tenantId == "" {
		return false, fmt.Errorf("tenantId is required")
	}
	jobKeys := make([]swf.JobKey, 0, len(jobIds))
	jobIDSet := make(map[string]struct{}, len(jobIds))
	for _, id := range jobIds {
		if id == "" {
			return false, fmt.Errorf("jobId cannot be empty")
		}
		jobKeys = append(jobKeys, swf.JobKey{TenantId: tenantId, JobId: id})
		jobIDSet[id] = struct{}{}
	}
	ctx := r.ctx
	if ctx == nil {
		ctx = context.Background()
	}
	pageToken := ""
	for {
		resp, err := r.engine.ListJobs(ctx, swf.ListJobsRequest{
			TenantIds: []string{tenantId},
			Stores:    []swf.JobStore{swf.JobStoreActive},
			JobKeys:   jobKeys,
			PageSize:  swf.MaxListJobsPageSize,
			PageToken: pageToken,
		})
		if err != nil {
			return false, err
		}
		for _, job := range resp.Jobs {
			if _, ok := jobIDSet[job.JobKey.JobId]; ok {
				return false, nil
			}
		}
		if resp.NextPageToken == "" {
			return true, nil
		}
		pageToken = resp.NextPageToken
	}
}

func (r *runner) AwaitJobs(jobIds ...string) error {
	rescheduled, err := r.rescheduleAwaitJobs(jobIds...)
	if err != nil {
		return err
	}
	if !rescheduled {
		return nil
	}
	prematureCloseOut()
	return nil
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

	// Debug logging for async child input hash computation
	childInputData, _ := data.GetData()
	childInputArtifacts, _ := data.GetArtifacts()
	r.logger.Debug("computed async child input hash",
		"childJobType", jobType,
		"ordinal", ordinal,
		"inputHash", inputHash,
		"dataLength", len(childInputData),
		"artifactCount", len(childInputArtifacts))

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

				r.logger.Debug("checking cached async child spawn",
					"childJobType", jobType,
					"ordinal", ordinal,
					"cachedInputHash", existing.InputHash,
					"computedInputHash", inputHash,
					"hashMatch", existing.InputHash == inputHash)

				if existing.InputHash != "" && existing.InputHash != inputHash {
					r.logger.Error("async child input hash mismatch",
						"childJobType", jobType,
						"ordinal", ordinal,
						"cachedInputHash", existing.InputHash,
						"computedInputHash", inputHash)
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
// Returns: (output, attempt, cached, terminal, error)
// - output: the cached result if successful
// - attempt: the next attempt number
// - cached: true if a cached result was found
// - terminal: true if this is a final result (success or non-retryable error)
// - error: any error encountered
func (r *runner) checkCachedJobResult(ctx context.Context, key story.Key, ordinal int64, inputHash string, retryCfg swf.RetryPolicy, totalDeadline time.Time, totalTimeout time.Duration, inputRef *swf.InputReference) (swf.JobData, int, bool, bool, error) {
	maxAttempts := int(retryCfg.MaximumAttempts)
	cached, err := r.engine.strata.Chapter(ctx, key, ordinal)
	if errors.Is(err, core.ErrNotFound) {
		// No cached result, need to execute
		return nil, 1, false, false, nil
	}
	if err != nil {
		return nil, 0, false, false, fmt.Errorf("failed to get chapter %d: %w", ordinal, err)
	}

	// Found cached result
	env, decErr := decodeChapterEnvelope(cached.Body())
	if decErr != nil {
		return nil, 0, false, false, fmt.Errorf("%w: decode cached chapter: %v", swf.ErrWorkflowNotDeterministic, decErr)
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
		return nil, 0, false, false, fmt.Errorf("%w: ordinal %d job result input hash mismatch", swf.ErrWorkflowNotDeterministic, ordinal)
	}
	if !totalDeadline.IsZero() && time.Now().After(totalDeadline) {
		return nil, 0, false, false, swf.NewTimeoutError("job", totalTimeout, swf.TimeoutScopeTotal, inputRef, false)
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
		return output, nextAttempt, true, true, nil
	}

	// Cached error - check if retryable
	retryable := isRetryable(payloadErr, retryCfg)
	if !retryable || priorAttempt >= maxAttempts {
		// Non-retryable or max attempts - terminal
		return nil, nextAttempt, true, true, payloadErr
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
				return nil, 0, false, false, err
			}
		}
	}

	// After backoff, need to retry (not terminal)
	return nil, nextAttempt, true, false, nil
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

	chap, err := payloadToChapter(payload, artifacts, ordinal, workerName, r.engine.workerId, kind, inputHash, now, meta)
	if err != nil {
		return fmt.Errorf("failed to build chapter: %w", err)
	}

	err = r.engine.strata.SaveChapter(context.TODO(), key, chap)
	if err != nil {
		return fmt.Errorf("failed to save chapter: %w", err)
	}

	assignArtifactKeys(artifacts, key.StoryID, ordinal)

	return nil
}

// DoJob executes the job worker with retry logic, timeout handling, and result persistence
// Follows cache-first pattern: checks for cached result before executing
// Each retry attempt gets a new ordinal (chapters are write-once)
func (r *runner) DoJob(ctx context.Context, lease *pgwf.Lease) {
	if ctx == nil {
		ctx = context.Background()
	}
	r.ctx = ctx
	if r.engine != nil {
		defer r.engine.resetAwaitState(r.jobId)
	}
	_ = lease.WithKeepAlive(r.engine.udb)

	// Load initial chapter and setup job policy
	inputData, env0, err := r.loadInitialChapterAndPolicy()
	if err != nil {
		r.logger.Error(err.Error())
		return
	}

	// Setup execution configuration with timeouts and deadlines
	config, err := r.setupJobExecutionConfig(ctx, inputData, env0)
	if err != nil {
		r.logger.Error(err.Error())
		return
	}

	key := r.GetJobKey().ToStoryKey()
	maxAttempts := int(config.retryCfg.MaximumAttempts)
	attempt := 1

	// Main retry loop - each attempt gets a new ordinal (chapters are write-once)
	for {
		// Check if total timeout has been exceeded
		if err := r.checkTotalTimeoutExceeded(config.totalDeadline, config.totalTimeout, config.inputRef); err != nil {
			r.logger.Error("job total timeout", "error", err)
			_ = lease.Complete(ctx, r.engine.udb)
			return
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
			return
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
		_, nextAttempt, cached, terminal, err := r.checkCachedJobResult(ctx, key, jobResultOrdinal, config.inputRef.Hash, config.retryCfg, config.totalDeadline, config.totalTimeout, config.inputRef)
		if err != nil {
			r.logger.Error("check cached job result failed", "error", err)
			_ = lease.Complete(ctx, r.engine.udb)
			return
		}

		if cached {
			// Found cached job result - use it instead of fresh execution result
			if terminal {
				// Terminal result (success or non-retryable error) - complete and return
				_ = lease.Complete(ctx, r.engine.udb)
				return
			}
			// Retryable error - update attempt number and retry
			attempt = nextAttempt
			// Continue to next iteration for retry
			continue
		}

		// No cached result - prepare and save the fresh execution result
		payload, artifacts, payloadKind, err := r.prepareJobResultPayload(output, jobErr, config.inputRef)
		if err != nil {
			r.logger.Error(err.Error())
			_ = lease.Complete(ctx, r.engine.udb)
			return
		}

		// Save the execution result at this ordinal (write-once)
		if err := r.saveJobChapter(key, payload, artifacts, jobResultOrdinal, r.worker.JobWorker.Name(), config.inputRef.Hash, payloadKind, attempt, config.inputRef); err != nil {
			r.logger.Error(err.Error())
			_ = lease.Complete(ctx, r.engine.udb)
			return
		}
		if len(artifacts) > 0 {
			cleanupArtifacts(context.TODO(), artifacts, r.logger)
		}

		// Determine if we're done or need to retry
		if jobErr == nil {
			// Success - cleanup input artifacts, complete lease and return
			inputArtifacts, _ := inputData.GetArtifacts()
			cleanupArtifacts(context.TODO(), inputArtifacts, r.logger)
			_ = lease.Complete(ctx, r.engine.udb)
			return
		}

		retryable := isRetryable(jobErr, config.retryCfg)
		if !retryable || attempt >= maxAttempts {
			// Non-retryable or max attempts reached - cleanup input artifacts, complete lease and return
			inputArtifacts, _ := inputData.GetArtifacts()
			cleanupArtifacts(context.TODO(), inputArtifacts, r.logger)
			_ = lease.Complete(ctx, r.engine.udb)
			return
		}

		// Retryable error and under max attempts - cleanup input artifacts, wait backoff and retry with new ordinal
		inputArtifacts, _ := inputData.GetArtifacts()
		cleanupArtifacts(context.TODO(), inputArtifacts, r.logger)

		backoff := computeBackoff(config.retryCfg, attempt)
		attempt++
		if backoff > 0 {
			now := time.Now().UTC()
			if err := r.awaitUntil(now.Add(backoff), jobResultOrdinal, attempt-1, "job", config.inputRef, time.Time{}, config.totalDeadline, 0, config.totalTimeout); err != nil {
				r.logger.Error("job await failed", "error", err)
				_ = lease.Complete(ctx, r.engine.udb)
				return
			}
		}
		// Loop back - will check for next cached job result or execute again
	}
}
