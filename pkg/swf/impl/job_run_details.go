package impl

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

func (s *swfEngineImpl) GetJobRun(ctx context.Context, req swf.GetJobRunRequest) (swf.GetJobRunResponse, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := req.JobKey.Validate(); err != nil {
		return swf.GetJobRunResponse{}, err
	}

	includeInputs, includeOutputs, includeArtifacts, includeAttemptInputs := normalizeGetJobRunOptions(req)

	statusInfo, err := pgwf.GetJobStatus(ctx, s.pgwfDB(ctx), pgwf.TenantID(req.JobKey.TenantId), pgwf.JobID(req.JobKey.JobId))
	if errors.Is(err, pgwf.ErrJobNotFound) {
		return swf.GetJobRunResponse{}, swf.ErrJobNotFound
	}
	if err != nil {
		return swf.GetJobRunResponse{}, fmt.Errorf("failed to get job status: %w", err)
	}

	jobStatus := convertPgwfStatusToSwf(statusInfo.Status, statusInfo.CancelRequested, statusInfo.ArchivedAt)
	resp := swf.GetJobRunResponse{
		Job: swf.JobRunSummary{
			JobKey:     req.JobKey,
			Status:     jobStatus,
			CreatedAt:  statusInfo.CreatedAt,
			ArchivedAt: statusInfo.ArchivedAt,
		},
	}

	st, err := s.strata.Story(ctx, req.JobKey.ToStoryKey())
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
		jobType     string
		startSet    bool
		tasks       []swf.TaskRun
		jobRuns     []swf.JobAttempt
		lastOrdinal int64 = -1
		currentRun  *swf.TaskRun
	)

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
			jobType = env.Meta.TaskType
			resp.Job.JobType = jobType
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
			continue
		}

		attempt, err := buildTaskAttempt(ctx, s, req.JobKey.ToStoryKey(), chap, env, includeInputs, includeOutputs || env.Meta.TaskType == jobType, includeArtifacts, includeAttemptInputs)
		if err != nil {
			return swf.GetJobRunResponse{}, err
		}

		if env.Meta.TaskType == jobType {
			jobRuns = append(jobRuns, swf.JobAttempt{
				Ordinal:   attempt.Ordinal,
				Attempt:   attempt.Attempt,
				WorkerID:  attempt.WorkerID,
				CreatedAt: attempt.CreatedAt,
				InputRef:  attempt.InputRef,
				Output:    attempt.Output,
				Outcome:   attempt.Outcome,
			})
			continue
		}

		if currentRun == nil || env.Meta.Attempt <= 1 || currentRun.TaskType != env.Meta.TaskType {
			newRun := swf.TaskRun{
				TaskRunID: fmt.Sprintf("%s:%d", env.Meta.TaskType, chap.Ordinal()),
				TaskType:  env.Meta.TaskType,
				Attempts:  []swf.TaskAttempt{},
			}
			tasks = append(tasks, newRun)
			currentRun = &tasks[len(tasks)-1]
		}
		currentRun.Attempts = append(currentRun.Attempts, attempt)
	}

	resp.Tasks = tasks
	resp.JobAttempts = jobRuns

	if resp.Job.JobType == "" {
		resp.Job.JobType = swf.JobTypeFromNextNeed(statusInfo.NextNeed)
	}

	if len(jobRuns) > 0 {
		latest := latestJobAttempt(jobRuns)
		resp.Result = &latest
		if !includeOutputs {
			for i := 0; i < len(jobRuns)-1; i++ {
				jobRuns[i].Output = nil
			}
			resp.JobAttempts = jobRuns
		}
	}

	if statusInfo.ArchivedAt == nil {
		runtimeRun, ok, err := buildRuntimeTaskRun(ctx, s, req.JobKey.ToStoryKey(), statusInfo, lastOrdinal, includeInputs, includeArtifacts, includeAttemptInputs)
		if err != nil {
			return swf.GetJobRunResponse{}, err
		}
		if ok {
			resp.Tasks = append(resp.Tasks, runtimeRun)
		}
	}

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

func buildTaskAttempt(ctx context.Context, s *swfEngineImpl, key story.Key, chap story.Chapter, env chapterEnvelope, includeInputs, includeOutputs, includeArtifacts, includeAttemptInputs bool) (swf.TaskAttempt, error) {
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
			resolved, err := resolveInputRef(ctx, s, key, env.Meta.InputRef, includeArtifacts)
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

func buildRuntimeTaskRun(ctx context.Context, s *swfEngineImpl, key story.Key, status *pgwf.JobStatusInfo, lastOrdinal int64, includeInputs, includeArtifacts, includeAttemptInputs bool) (swf.TaskRun, bool, error) {
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
			resolved, err := resolveInputRef(ctx, s, key, inputRef, includeArtifacts)
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

func resolveInputRef(ctx context.Context, s *swfEngineImpl, key story.Key, ref *swf.InputReference, includeArtifacts bool) (*swf.TaskIO, error) {
	if ref == nil {
		return nil, nil
	}
	chap, err := s.strata.Chapter(ctx, key, ref.Ordinal)
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
	case payloadKindApp, payloadKindAppChildJob:
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
