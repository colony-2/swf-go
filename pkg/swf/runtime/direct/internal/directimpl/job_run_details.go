package directimpl

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/colony-2/pgwf-go/pkg/pgwf"
	"github.com/colony-2/strata-go/pkg/client/artifact"
	"github.com/colony-2/strata-go/pkg/client/pagination"
	"github.com/colony-2/strata-go/pkg/client/story"
	"github.com/colony-2/swf-go/pkg/swf"
)

const defaultJobRunChaptersPageSize = 200

type jobRunAccessor interface {
	pgwfDB(ctx context.Context) pgwf.DB
	loadStory(ctx context.Context, key story.Key) (story.Story, error)
	loadChapter(ctx context.Context, key story.Key, ordinal int64) (story.Chapter, error)
}

func (r *Runtime) GetJobRun(ctx context.Context, req swf.GetJobRunRequest) (swf.GetJobRunResponse, error) {
	return getJobRun(ctx, r, req)
}

func getJobRun(ctx context.Context, accessor jobRunAccessor, req swf.GetJobRunRequest) (swf.GetJobRunResponse, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := req.JobKey.Validate(); err != nil {
		return swf.GetJobRunResponse{}, err
	}

	includeInputs, includeOutputs, includeArtifacts, includeAttemptInputs := normalizeGetJobRunOptions(req)

	statusInfo, err := pgwf.GetJobStatus(ctx, accessor.pgwfDB(ctx), pgwf.TenantID(req.JobKey.TenantId), pgwf.JobID(req.JobKey.JobId))
	if errors.Is(err, pgwf.ErrJobNotFound) {
		return swf.GetJobRunResponse{}, swf.ErrJobNotFound
	}
	if err != nil {
		return swf.GetJobRunResponse{}, fmt.Errorf("failed to get job status: %w", err)
	}
	runPolicy := swf.RunPolicy{}
	if len(statusInfo.Payload) > 0 {
		var payload jobPayload
		if err := json.Unmarshal(statusInfo.Payload, &payload); err == nil {
			runPolicy = payload.RunPolicy
		}
	}
	retryPolicy := normalizeRunPolicy(runPolicy).Retry

	jobStatus := convertPgwfStatusToSwf(statusInfo.Status, statusInfo.CancelRequested, statusInfo.ArchivedAt)
	resp := swf.GetJobRunResponse{
		Job: swf.JobRunSummary{
			JobKey:     req.JobKey,
			Status:     jobStatus,
			CreatedAt:  statusInfo.CreatedAt,
			ArchivedAt: statusInfo.ArchivedAt,
			Metadata:   statusInfo.Metadata,
		},
	}

	st, err := accessor.loadStory(ctx, storyKeyForJob(req.JobKey))
	if err != nil {
		return swf.GetJobRunResponse{}, fmt.Errorf("failed to load story: %w", err)
	}

	iter, err := st.Chapters(ctx, story.ChaptersOptions{
		PageSize:  defaultJobRunChaptersPageSize,
		Direction: story.DirectionForward,
	})
	if err != nil {
		return swf.GetJobRunResponse{}, fmt.Errorf("failed to list chapters: %w", err)
	}

	var (
		jobType              string
		startSet             bool
		startInputRef        *swf.InputReference
		attempts             []swf.JobAttempt
		attemptIndex               = map[int]int{}
		activeAttempt              = 1
		currentAttempt             = 0
		currentRunIdx              = -1
		lastOrdinal          int64 = -1
		lastCompletedAttempt int
		lastCompletedOutcome swf.TaskOutcome
	)

	ensureAttempt := func(num int) int {
		if num <= 0 {
			num = 1
		}
		if idx, ok := attemptIndex[num]; ok {
			return idx
		}
		attempt := swf.JobAttempt{
			Attempt:  num,
			InputRef: startInputRef,
		}
		if num == 1 && startSet {
			attempt.CreatedAt = resp.Start.CreatedAt
			attempt.WorkerID = resp.Start.WorkerID
		}
		attempts = append(attempts, attempt)
		idx := len(attempts) - 1
		attemptIndex[num] = idx
		return idx
	}

	for iter.HasNext() {
		chap, err := iter.Next(ctx)
		if errors.Is(err, pagination.ErrNoMoreItems) {
			break
		}
		if err != nil {
			return swf.GetJobRunResponse{}, fmt.Errorf("failed to iterate chapters: %w", err)
		}

		lastOrdinal = chap.Ordinal()
		env, decErr := decodeChapterEnvelope(chap.Body())
		if decErr != nil {
			return swf.GetJobRunResponse{}, fmt.Errorf("%w: decode chapter: %v", swf.ErrWorkflowNotDeterministic, decErr)
		}

		if chap.Ordinal() == 0 && !startSet {
			if env.ChapterType != chapterTypeJobStart {
				return swf.GetJobRunResponse{}, fmt.Errorf("%w: unexpected chapter type %q at ordinal 0", swf.ErrWorkflowNotDeterministic, env.ChapterType)
			}
			jobType = env.Meta.TaskType
			resp.Job.JobType = jobType
			startInputRef = &swf.InputReference{Ordinal: 0, Hash: env.Meta.InputHash}
			startInput, err := buildTaskIOFromPayload(ctx, env.Payload, chap.Artifacts(), req.JobKey.JobId, chap.Ordinal(), includeInputs, includeArtifacts)
			if err != nil {
				return swf.GetJobRunResponse{}, err
			}
			if !includeInputs {
				startInput = nil
			}
			resp.Start = swf.JobStart{
				Ordinal:   chap.Ordinal(),
				WorkerID:  env.Meta.WorkerID,
				CreatedAt: env.Meta.CreatedAt,
				Input:     startInput,
			}
			startSet = true
			_ = ensureAttempt(1)
			continue
		}

		attempt, err := buildTaskAttempt(ctx, accessor, storyKeyForJob(req.JobKey), chap, env, includeInputs, includeOutputs || env.ChapterType == chapterTypeJobAttemptOutcome, includeArtifacts, includeAttemptInputs)
		if err != nil {
			return swf.GetJobRunResponse{}, err
		}

		if env.ChapterType == chapterTypeJobAttemptOutcome {
			attemptNum := attempt.Attempt
			if attemptNum <= 0 {
				attemptNum = 1
			}
			idx := ensureAttempt(attemptNum)
			attempts[idx].Ordinal = attempt.Ordinal
			attempts[idx].Attempt = attemptNum
			attempts[idx].WorkerID = attempt.WorkerID
			attempts[idx].CreatedAt = attempt.CreatedAt
			attempts[idx].InputRef = attempt.InputRef
			attempts[idx].Output = attempt.Output
			attempts[idx].Outcome = attempt.Outcome
			activeAttempt = attemptNum + 1
			lastCompletedAttempt = attemptNum
			lastCompletedOutcome = attempt.Outcome
			currentAttempt = 0
			currentRunIdx = -1
			continue
		}

		if env.ChapterType != chapterTypeTaskAttemptOutcome && env.ChapterType != chapterTypeRestartExtra {
			return swf.GetJobRunResponse{}, fmt.Errorf("%w: unexpected chapter type %q at ordinal %d", swf.ErrWorkflowNotDeterministic, env.ChapterType, chap.Ordinal())
		}
		idx := ensureAttempt(activeAttempt)
		if currentAttempt != activeAttempt {
			currentAttempt = activeAttempt
			currentRunIdx = -1
		}
		if currentRunIdx == -1 || attempt.Attempt <= 1 || attempts[idx].Tasks[currentRunIdx].TaskType != env.Meta.TaskType {
			attempts[idx].Tasks = append(attempts[idx].Tasks, swf.TaskRun{
				TaskRunID: fmt.Sprintf("%s:%d", env.Meta.TaskType, chap.Ordinal()),
				TaskType:  env.Meta.TaskType,
				Attempts:  []swf.TaskAttempt{},
			})
			currentRunIdx = len(attempts[idx].Tasks) - 1
		}
		attempts[idx].Tasks[currentRunIdx].Attempts = append(attempts[idx].Tasks[currentRunIdx].Attempts, attempt)
	}

	if resp.Job.JobType == "" {
		resp.Job.JobType = swf.JobTypeFromNextNeed(statusInfo.NextNeed)
	}

	currentAttemptNum := 0
	if statusInfo.ArchivedAt == nil && resp.Job.Status != swf.JobStatusCancelled {
		if lastCompletedAttempt == 0 {
			currentAttemptNum = 1
		} else if shouldSynthesizeNextAttempt(lastCompletedAttempt, lastCompletedOutcome, retryPolicy) {
			currentAttemptNum = lastCompletedAttempt + 1
		}
	}
	if currentAttemptNum > 0 {
		_ = ensureAttempt(currentAttemptNum)
	}

	if statusInfo.ArchivedAt == nil {
		runtimeRun, ok, err := buildRuntimeTaskRun(ctx, accessor, storyKeyForJob(req.JobKey), statusInfo, lastOrdinal, includeInputs, includeArtifacts, includeAttemptInputs)
		if err != nil {
			return swf.GetJobRunResponse{}, err
		}
		if ok && currentAttemptNum > 0 {
			idx := ensureAttempt(currentAttemptNum)
			attempts[idx].Tasks = append(attempts[idx].Tasks, runtimeRun)
		}
	}

	if !includeOutputs && len(attempts) > 0 {
		latest := latestJobAttempt(attempts)
		for i := range attempts {
			if attempts[i].Attempt != latest.Attempt || attempts[i].Ordinal != latest.Ordinal {
				attempts[i].Output = nil
			}
		}
	}

	resp.Attempts = attempts
	return resp, nil
}

func normalizeGetJobRunOptions(req swf.GetJobRunRequest) (bool, bool, bool, bool) {
	if !req.IncludeInputs && !req.IncludeOutputs && !req.IncludeArtifacts && !req.IncludeAttemptInputs {
		return true, true, true, false
	}
	return req.IncludeInputs, req.IncludeOutputs, req.IncludeArtifacts, req.IncludeAttemptInputs
}

func latestJobAttempt(attempts []swf.JobAttempt) swf.JobAttempt {
	best := attempts[0]
	for i := 1; i < len(attempts); i++ {
		attempt := attempts[i]
		if attempt.Attempt > best.Attempt || (attempt.Attempt == best.Attempt && attempt.Ordinal > best.Ordinal) {
			best = attempt
		}
	}
	return best
}

func shouldSynthesizeNextAttempt(lastAttempt int, outcome swf.TaskOutcome, policy swf.RetryPolicy) bool {
	if lastAttempt <= 0 {
		return false
	}
	if outcome.Status != swf.TaskOutcomeStatusFailed {
		return false
	}
	if policy.MaximumAttempts <= 0 {
		policy = normalizeRetryPolicy(policy)
	}
	if lastAttempt >= int(policy.MaximumAttempts) {
		return false
	}
	if outcome.Error == nil {
		return true
	}
	switch outcome.Error.Kind {
	case swf.TaskErrorKindTimeout:
		if outcome.Error.Retryable == nil {
			return false
		}
		return *outcome.Error.Retryable
	case swf.TaskErrorKindSystem:
		if outcome.Error.Retryable == nil {
			return true
		}
		return *outcome.Error.Retryable
	case swf.TaskErrorKindApp:
		return true
	default:
		return true
	}
}

func buildTaskAttempt(ctx context.Context, accessor jobRunAccessor, key story.Key, chap story.Chapter, env chapterEnvelope, includeInputs, includeOutputs, includeArtifacts, includeAttemptInputs bool) (swf.TaskAttempt, error) {
	attemptNum := env.Meta.Attempt
	if attemptNum <= 0 {
		attemptNum = 1
	}

	output, err := buildTaskIOFromPayload(ctx, env.Payload, chap.Artifacts(), key.StoryID, chap.Ordinal(), includeOutputs, includeArtifacts)
	if err != nil {
		return swf.TaskAttempt{}, err
	}
	if !includeOutputs {
		output = nil
	}

	var input *swf.TaskIO
	if includeInputs {
		if includeAttemptInputs && env.Meta.InputRef != nil {
			resolved, err := resolveInputRef(ctx, accessor, key, env.Meta.InputRef, includeArtifacts)
			if err != nil {
				return swf.TaskAttempt{}, err
			}
			input = resolved
		} else if env.Meta.Input != nil {
			input = &swf.TaskIO{Data: append([]byte(nil), env.Meta.Input...)}
		}
	}

	outcome, err := outcomeFromEnvelope(env)
	if err != nil {
		return swf.TaskAttempt{}, err
	}

	state := outcome.Status
	if outcome.Status == "" {
		state = swf.TaskAttemptStateSucceeded
	}

	return swf.TaskAttempt{
		Ordinal:       chap.Ordinal(),
		Attempt:       attemptNum,
		WorkerID:      env.Meta.WorkerID,
		CreatedAt:     env.Meta.CreatedAt,
		InputHash:     env.Meta.InputHash,
		InputRef:      env.Meta.InputRef,
		RunPolicy:     env.Meta.RunPolicy,
		Retryable:     env.Meta.Retryable,
		MaxAttempts:   intPtr(env.Meta.MaxAttempts),
		NextAttemptAt: env.Meta.NextAttemptAt,
		BackoffMillis: int64Ptr(env.Meta.BackoffMillis),
		Input:         input,
		Output:        output,
		State:         state,
		Outcome:       outcome,
	}, nil
}

func buildRuntimeTaskRun(ctx context.Context, accessor jobRunAccessor, key story.Key, status *pgwf.JobStatusInfo, lastOrdinal int64, includeInputs, includeArtifacts, includeAttemptInputs bool) (swf.TaskRun, bool, error) {
	if status == nil || status.NextNeed == "" {
		return swf.TaskRun{}, false, nil
	}

	state, runtime := runtimeStateFromStatus(status)
	if state == "" {
		return swf.TaskRun{}, false, nil
	}

	runtimeOrdinal := lastOrdinal + 1
	if runtimeOrdinal < 0 {
		runtimeOrdinal = 0
	}

	var input *swf.TaskIO
	var inputRef *swf.InputReference
	if lastOrdinal >= 0 {
		inputRef = &swf.InputReference{Ordinal: lastOrdinal}
	}
	if includeInputs {
		if includeAttemptInputs && inputRef != nil {
			resolved, err := resolveInputRef(ctx, accessor, key, inputRef, includeArtifacts)
			if err != nil {
				return swf.TaskRun{}, false, err
			}
			input = resolved
		}
	}

	attempt := swf.TaskAttempt{
		Ordinal:  runtimeOrdinal,
		Attempt:  1,
		InputRef: inputRef,
		Input:    input,
		State:    state,
		Runtime:  runtime,
	}

	run := swf.TaskRun{
		TaskRunID: fmt.Sprintf("%s:%d", status.NextNeed, runtimeOrdinal),
		TaskType:  status.NextNeed,
		Attempts:  []swf.TaskAttempt{attempt},
	}
	return run, true, nil
}

func runtimeStateFromStatus(status *pgwf.JobStatusInfo) (string, *swf.TaskRuntime) {
	if status == nil {
		return "", nil
	}
	runtime := &swf.TaskRuntime{
		NextNeed:       strPtr(status.NextNeed),
		AvailableAt:    timePtr(status.AvailableAt),
		WaitFor:        status.WaitFor,
		LeaseOwner:     status.LeaseID,
		LeaseExpiresAt: status.LeaseExpiresAt,
	}

	now := time.Now().UTC()
	switch status.Status {
	case pgwf.JobStatusReady:
		if !status.AvailableAt.After(now) {
			return swf.TaskAttemptStateReady, runtime
		}
		return swf.TaskAttemptStateWaiting, runtime
	case pgwf.JobStatusActive:
		return swf.TaskAttemptStateLeased, runtime
	case pgwf.JobStatusAwaitingFuture, pgwf.JobStatusPendingJobs:
		return swf.TaskAttemptStateWaiting, runtime
	default:
		return swf.TaskAttemptStateWaiting, runtime
	}
}

func resolveInputRef(ctx context.Context, accessor jobRunAccessor, key story.Key, ref *swf.InputReference, includeArtifacts bool) (*swf.TaskIO, error) {
	if ref == nil {
		return nil, nil
	}
	chap, err := accessor.loadChapter(ctx, key, ref.Ordinal)
	if err != nil {
		return nil, err
	}
	env, err := decodeChapterEnvelope(chap.Body())
	if err != nil {
		return nil, fmt.Errorf("%w: decode input chapter: %v", swf.ErrWorkflowNotDeterministic, err)
	}
	return buildTaskIOFromPayload(ctx, env.Payload, chap.Artifacts(), key.StoryID, ref.Ordinal, true, includeArtifacts)
}

func buildTaskIOFromPayload(ctx context.Context, payload json.RawMessage, artifacts []artifact.Artifact, jobID string, ordinal int64, includeData bool, includeArtifacts bool) (*swf.TaskIO, error) {
	if !includeData && !includeArtifacts {
		return nil, nil
	}
	out := &swf.TaskIO{}
	if includeData && payload != nil {
		out.Data = append([]byte(nil), payload...)
	}
	if includeArtifacts {
		infos, err := buildArtifactInfos(ctx, artifacts, jobID, ordinal)
		if err != nil {
			return nil, err
		}
		out.Artifacts = infos
	}
	if out.Data == nil && len(out.Artifacts) == 0 {
		return nil, nil
	}
	return out, nil
}

func buildArtifactInfos(ctx context.Context, artifacts []artifact.Artifact, jobID string, ordinal int64) ([]swf.ArtifactInfo, error) {
	if len(artifacts) == 0 {
		return nil, nil
	}
	out := make([]swf.ArtifactInfo, 0, len(artifacts))
	for _, art := range artifacts {
		if art == nil {
			continue
		}
		sha, err := art.Sha256(ctx)
		if err != nil {
			return nil, err
		}
		var key *swf.ArtifactKey
		if jobID != "" && ordinal >= 0 && art.Name() != "" {
			k := swf.ArtifactKey{
				JobId:       jobID,
				TaskOrdinal: ordinal,
				Name:        art.Name(),
				SizeBytes:   art.SizeBytes(),
			}
			key = &k
		}
		out = append(out, swf.ArtifactInfo{
			ID:          art.ID(),
			Name:        art.Name(),
			ContentType: art.ContentType(),
			SizeBytes:   art.SizeBytes(),
			Sha256:      sha,
			Key:         key,
		})
	}
	return out, nil
}

func outcomeFromEnvelope(env chapterEnvelope) (swf.TaskOutcome, error) {
	outcome := swf.TaskOutcome{
		PayloadKind: env.PayloadKind,
	}

	switch env.PayloadKind {
	case payloadKindApp:
		outcome.Status = swf.TaskOutcomeStatusSucceeded
		return outcome, nil
	case payloadKindAppError:
		var p swf.AppErrorPayload
		if err := json.Unmarshal(env.Payload, &p); err != nil {
			return swf.TaskOutcome{}, err
		}
		outcome.Status = swf.TaskOutcomeStatusFailed
		outcome.Error = &swf.TaskError{
			Kind:       swf.TaskErrorKindApp,
			Message:    p.Message,
			Level:      p.Level,
			Attrs:      p.Attrs,
			InputRef:   p.InputRef,
			Stacktrace: p.Stacktrace,
		}
		return outcome, nil
	case payloadKindSystemError:
		var p swf.SystemErrorPayload
		if err := json.Unmarshal(env.Payload, &p); err != nil {
			return swf.TaskOutcome{}, err
		}
		outcome.Status = swf.TaskOutcomeStatusFailed
		outcome.Error = &swf.TaskError{
			Kind:       swf.TaskErrorKindSystem,
			Message:    p.Message,
			Component:  p.Component,
			Code:       p.Code,
			Retryable:  boolPtr(p.Retryable),
			InputRef:   p.InputRef,
			Stacktrace: p.Stacktrace,
		}
		return outcome, nil
	case payloadKindTimeout:
		var p swf.TimeoutPayload
		if err := json.Unmarshal(env.Payload, &p); err != nil {
			return swf.TaskOutcome{}, err
		}
		outcome.Status = swf.TaskOutcomeStatusFailed
		outcome.Error = &swf.TaskError{
			Kind:      swf.TaskErrorKindTimeout,
			Message:   p.Message,
			Component: p.Component,
			Code:      p.Code,
			Retryable: boolPtr(p.Retryable),
			Scope:     p.Scope,
			After:     &p.After,
			InputRef:  p.InputRef,
		}
		return outcome, nil
	default:
		outcome.Status = swf.TaskOutcomeStatusFailed
		outcome.Error = &swf.TaskError{
			Kind:    swf.TaskErrorKindSystem,
			Message: fmt.Sprintf("unsupported payload kind %q", env.PayloadKind),
			Code:    "unknown_payload_kind",
		}
		return outcome, nil
	}
}

func boolPtr(v bool) *bool {
	val := v
	return &val
}

func intPtr(v int) *int {
	if v <= 0 {
		return nil
	}
	val := v
	return &val
}

func int64Ptr(v int64) *int64 {
	if v <= 0 {
		return nil
	}
	val := v
	return &val
}

func timePtr(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	cp := t
	return &cp
}

func strPtr(s string) *string {
	if s == "" {
		return nil
	}
	return &s
}
