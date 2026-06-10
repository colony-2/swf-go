package swf

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"
)

func getJobRunFromRuntime(ctx context.Context, runtime WorkflowRuntime, req GetJobRunRequest) (GetJobRunResponse, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := req.JobKey.Validate(); err != nil {
		return GetJobRunResponse{}, err
	}

	includeInputs, includeOutputs, includeArtifacts, includeAttemptInputs := normalizeGetJobRunOptions(req)

	job, err := loadJobSummary(ctx, runtime, req.JobKey)
	if err != nil {
		return GetJobRunResponse{}, err
	}
	chapters, err := runtime.ListChapters(ctx, ListChaptersRequest{JobKey: req.JobKey})
	if err != nil {
		return GetJobRunResponse{}, err
	}
	sort.Slice(chapters, func(i, j int) bool { return chapters[i].Ordinal < chapters[j].Ordinal })

	resp := GetJobRunResponse{
		Job: JobRunSummary{
			JobKey:     req.JobKey,
			JobType:    job.JobType,
			Status:     job.Status,
			CreatedAt:  job.CreatedAt,
			ArchivedAt: job.ArchivedAt,
			Metadata:   append(json.RawMessage(nil), job.Metadata...),
		},
	}

	chapterByOrdinal := make(map[int64]Chapter, len(chapters))
	for _, chapter := range chapters {
		chapterByOrdinal[chapter.Ordinal] = cloneStoredJobRunChapter(chapter)
	}

	var (
		jobType              = job.JobType
		retryPolicy          = normalizeRunPolicy(RunPolicy{}).Retry
		startSet             bool
		startInputRef        *InputReference
		attempts             []JobAttempt
		attemptIndex               = map[int]int{}
		activeAttempt              = 1
		currentAttempt             = 0
		currentRunIdx              = -1
		lastOrdinal          int64 = -1
		lastCompletedAttempt int
		lastCompletedOutcome TaskOutcome
	)

	ensureAttempt := func(num int) int {
		if num <= 0 {
			num = 1
		}
		if idx, ok := attemptIndex[num]; ok {
			return idx
		}
		attempt := JobAttempt{
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

	for _, chapter := range chapters {
		if chapter.Ordinal > lastOrdinal {
			lastOrdinal = chapter.Ordinal
		}

		meta, err := chapterMetaFromChapter(chapter)
		if err != nil {
			return GetJobRunResponse{}, fmt.Errorf("%w: decode chapter metadata: %v", ErrWorkflowNotDeterministic, err)
		}

		if chapter.Ordinal == 0 && !startSet {
			if !chapterIs(chapter, chapterTypeJobStart) {
				got, _ := chapterType(chapter)
				return GetJobRunResponse{}, fmt.Errorf("%w: unexpected chapter type %q at ordinal 0", ErrWorkflowNotDeterministic, got)
			}
			if meta.TaskType != "" {
				jobType = meta.TaskType
			}
			resp.Job.JobType = jobType
			startInputRef = &InputReference{Ordinal: 0, Hash: meta.InputHash}
			startInput, err := buildStoredTaskIO(req.JobKey, chapter, includeInputs, includeArtifacts)
			if err != nil {
				return GetJobRunResponse{}, err
			}
			if !includeInputs {
				startInput = nil
			}
			resp.Start = JobStart{
				Ordinal:   chapter.Ordinal,
				WorkerID:  meta.WorkerID,
				CreatedAt: meta.CreatedAt,
				Input:     startInput,
			}
			if meta.RunPolicy != nil {
				retryPolicy = normalizeRunPolicy(*meta.RunPolicy).Retry
			} else if runPolicy, ok := runPolicyFromJobSummary(job); ok {
				retryPolicy = normalizeRunPolicy(runPolicy).Retry
			}
			startSet = true
			_ = ensureAttempt(1)
			continue
		}

		isJobAttemptOutcome := chapterIs(chapter, chapterTypeJobAttemptOutcome)
		attempt, err := buildStoredTaskAttempt(req.JobKey, chapter, meta, chapterByOrdinal, includeInputs, includeOutputs || isJobAttemptOutcome, includeArtifacts, includeAttemptInputs)
		if err != nil {
			return GetJobRunResponse{}, err
		}

		if isJobAttemptOutcome {
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

		if !chapterIs(chapter, chapterTypeTaskAttemptOutcome) && !chapterIs(chapter, chapterTypeRestartExtra) {
			got, _ := chapterType(chapter)
			return GetJobRunResponse{}, fmt.Errorf("%w: unexpected chapter type %q at ordinal %d", ErrWorkflowNotDeterministic, got, chapter.Ordinal)
		}
		idx := ensureAttempt(activeAttempt)
		if currentAttempt != activeAttempt {
			currentAttempt = activeAttempt
			currentRunIdx = -1
		}
		if currentRunIdx == -1 || attempt.Attempt <= 1 || attempts[idx].Tasks[currentRunIdx].TaskType != meta.TaskType {
			attempts[idx].Tasks = append(attempts[idx].Tasks, TaskRun{
				TaskRunID: fmt.Sprintf("%s:%d", meta.TaskType, chapter.Ordinal),
				TaskType:  meta.TaskType,
				Attempts:  []TaskAttempt{},
			})
			currentRunIdx = len(attempts[idx].Tasks) - 1
		}
		attempts[idx].Tasks[currentRunIdx].Attempts = append(attempts[idx].Tasks[currentRunIdx].Attempts, attempt)
	}

	if resp.Job.JobType == "" {
		resp.Job.JobType = jobType
	}

	currentAttemptNum := 0
	if job.ArchivedAt == nil && resp.Job.Status != JobStatusCancelled {
		if lastCompletedAttempt == 0 {
			currentAttemptNum = 1
		} else if shouldSynthesizeNextAttempt(lastCompletedAttempt, lastCompletedOutcome, retryPolicy) {
			currentAttemptNum = lastCompletedAttempt + 1
		}
	}
	if currentAttemptNum > 0 {
		_ = ensureAttempt(currentAttemptNum)
	}

	if job.ArchivedAt == nil {
		runtimeRun, ok, err := buildRuntimeTaskRunFromSummary(job, lastOrdinal, chapterByOrdinal, includeInputs, includeArtifacts, includeAttemptInputs)
		if err != nil {
			return GetJobRunResponse{}, err
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

func loadJobSummary(ctx context.Context, runtime WorkflowRuntime, jobKey JobKey) (JobSummary, error) {
	pageToken := ""
	for {
		resp, err := runtime.ListJobs(ctx, ListJobsRequest{
			TenantIds: []string{jobKey.TenantId},
			JobKeys:   []JobKey{jobKey},
			PageSize:  MaxListJobsPageSize,
			PageToken: pageToken,
		})
		if err != nil {
			return JobSummary{}, err
		}
		for _, job := range resp.Jobs {
			if job.JobKey == jobKey {
				return cloneJobSummary(job), nil
			}
		}
		if resp.NextPageToken == "" {
			return JobSummary{}, ErrJobNotFound
		}
		pageToken = resp.NextPageToken
	}
}

func normalizeGetJobRunOptions(req GetJobRunRequest) (bool, bool, bool, bool) {
	if !req.IncludeInputs && !req.IncludeOutputs && !req.IncludeArtifacts && !req.IncludeAttemptInputs {
		return true, true, true, false
	}
	return req.IncludeInputs, req.IncludeOutputs, req.IncludeArtifacts, req.IncludeAttemptInputs
}

func buildStoredTaskAttempt(
	jobKey JobKey,
	chapter Chapter,
	meta chapterMeta,
	chapterByOrdinal map[int64]Chapter,
	includeInputs bool,
	includeOutputs bool,
	includeArtifacts bool,
	includeAttemptInputs bool,
) (TaskAttempt, error) {
	attemptNum := meta.Attempt
	if attemptNum <= 0 {
		attemptNum = 1
	}

	output, err := buildStoredTaskIO(jobKey, chapter, includeOutputs, includeArtifacts)
	if err != nil {
		return TaskAttempt{}, err
	}
	if !includeOutputs {
		output = nil
	}

	var input *TaskIO
	if includeInputs {
		if includeAttemptInputs && meta.InputRef != nil {
			resolved, err := resolveStoredInputRef(jobKey, chapterByOrdinal, meta.InputRef, includeArtifacts)
			if err != nil {
				return TaskAttempt{}, err
			}
			input = resolved
		} else if len(meta.Input) > 0 {
			input = &TaskIO{Data: append(json.RawMessage(nil), meta.Input...)}
		}
	}

	outcome, err := outcomeFromChapter(chapter)
	if err != nil {
		return TaskAttempt{}, err
	}
	state := outcome.Status
	if state == "" {
		state = TaskAttemptStateSucceeded
	}

	return TaskAttempt{
		Ordinal:       chapter.Ordinal,
		Attempt:       attemptNum,
		WorkerID:      meta.WorkerID,
		CreatedAt:     meta.CreatedAt,
		InputHash:     meta.InputHash,
		InputRef:      meta.InputRef,
		RunPolicy:     meta.RunPolicy,
		Retryable:     meta.Retryable,
		MaxAttempts:   intPtr(meta.MaxAttempts),
		NextAttemptAt: cloneTimePtr(meta.NextAttemptAt),
		BackoffMillis: int64Ptr(meta.BackoffMillis),
		Input:         input,
		Output:        output,
		State:         state,
		Outcome:       outcome,
	}, nil
}

func buildRuntimeTaskRunFromSummary(
	job JobSummary,
	lastOrdinal int64,
	chapterByOrdinal map[int64]Chapter,
	includeInputs bool,
	includeArtifacts bool,
	includeAttemptInputs bool,
) (TaskRun, bool, error) {
	currentNeed := currentNeedFromJobSummary(job)
	if currentNeed == "" {
		return TaskRun{}, false, nil
	}

	state, runtime := runtimeStateFromJobSummary(job, currentNeed)
	if state == "" {
		return TaskRun{}, false, nil
	}

	runtimeOrdinal := lastOrdinal + 1
	if runtimeOrdinal < 0 {
		runtimeOrdinal = 0
	}

	var input *TaskIO
	var inputRef *InputReference
	if lastOrdinal >= 0 {
		inputRef = &InputReference{Ordinal: lastOrdinal}
	}
	if includeInputs && includeAttemptInputs && inputRef != nil {
		resolved, err := resolveStoredInputRef(job.JobKey, chapterByOrdinal, inputRef, includeArtifacts)
		if err != nil {
			return TaskRun{}, false, err
		}
		input = resolved
	}

	return TaskRun{
		TaskRunID: fmt.Sprintf("%s:%d", currentNeed, runtimeOrdinal),
		TaskType:  currentNeed,
		Attempts: []TaskAttempt{{
			Ordinal:  runtimeOrdinal,
			Attempt:  1,
			InputRef: inputRef,
			Input:    input,
			State:    state,
			Runtime:  runtime,
		}},
	}, true, nil
}

func runtimeStateFromJobSummary(job JobSummary, currentNeed string) (string, *TaskRuntime) {
	if currentNeed == "" {
		return "", nil
	}

	runtime := &TaskRuntime{
		NextNeed:       strPtr(currentNeed),
		AvailableAt:    timePtr(job.AvailableAt),
		WaitFor:        append([]string(nil), job.WaitFor...),
		LeaseExpiresAt: cloneTimePtr(job.LeaseExpiresAt),
	}

	now := time.Now().UTC()
	switch job.Status {
	case JobStatusReady:
		if !job.AvailableAt.After(now) {
			return TaskAttemptStateReady, runtime
		}
		return TaskAttemptStateWaiting, runtime
	case JobStatusActive:
		return TaskAttemptStateLeased, runtime
	case JobStatusAwaitingFuture, JobStatusPendingJobs:
		return TaskAttemptStateWaiting, runtime
	default:
		return "", nil
	}
}

func currentNeedFromJobSummary(job JobSummary) string {
	if job.NextNeed != nil && *job.NextNeed != "" {
		return *job.NextNeed
	}
	return job.JobType
}

func runPolicyFromJobSummary(job JobSummary) (RunPolicy, bool) {
	if len(job.Payload) == 0 {
		return RunPolicy{}, false
	}
	var payload workerJobPayload
	if err := json.Unmarshal(job.Payload, &payload); err != nil {
		return RunPolicy{}, false
	}
	return payload.RunPolicy, true
}

func resolveStoredInputRef(jobKey JobKey, chapterByOrdinal map[int64]Chapter, ref *InputReference, includeArtifacts bool) (*TaskIO, error) {
	if ref == nil {
		return nil, nil
	}
	chapter, ok := chapterByOrdinal[ref.Ordinal]
	if !ok {
		return nil, ErrChapterNotFound
	}
	return buildStoredTaskIO(jobKey, chapter, true, includeArtifacts)
}

func buildStoredTaskIO(jobKey JobKey, chapter Chapter, includeData bool, includeArtifacts bool) (*TaskIO, error) {
	if !includeData && !includeArtifacts {
		return nil, nil
	}
	out := &TaskIO{}
	if includeData {
		_, data, err := chapterPayload(chapter)
		if err != nil {
			return nil, err
		}
		if data != nil {
			out.Data = append(json.RawMessage(nil), data...)
		}
	}
	if includeArtifacts {
		out.Artifacts = buildStoredArtifactInfos(jobKey, chapter)
	}
	if out.Data == nil && len(out.Artifacts) == 0 {
		return nil, nil
	}
	return out, nil
}

func buildStoredArtifactInfos(jobKey JobKey, chapter Chapter) []ArtifactInfo {
	if len(chapter.Artifacts) == 0 {
		return nil
	}
	out := make([]ArtifactInfo, 0, len(chapter.Artifacts))
	for _, art := range chapter.Artifacts {
		key := ArtifactKey{
			JobId:       jobKey.JobId,
			TaskOrdinal: chapter.Ordinal,
			Name:        art.Name,
			SizeBytes:   art.Size,
		}
		out = append(out, ArtifactInfo{
			Name:      art.Name,
			SizeBytes: art.Size,
			Sha256:    art.Digest,
			Key:       &key,
		})
	}
	return out
}

func outcomeFromChapter(chapter Chapter) (TaskOutcome, error) {
	payloadKind, data, err := chapterPayload(chapter)
	if err != nil {
		return TaskOutcome{}, err
	}
	outcome := TaskOutcome{
		PayloadKind: payloadKind,
	}

	switch payloadKind {
	case payloadKindApp:
		outcome.Status = TaskOutcomeStatusSucceeded
		return outcome, nil
	case payloadKindAppError:
		var p AppErrorPayload
		if err := json.Unmarshal(data, &p); err != nil {
			return TaskOutcome{}, err
		}
		outcome.Status = TaskOutcomeStatusFailed
		outcome.Error = &TaskError{
			Kind:       TaskErrorKindApp,
			Message:    p.Message,
			Level:      p.Level,
			Attrs:      cloneAttrs(p.Attrs),
			InputRef:   p.InputRef,
			Stacktrace: append([]string(nil), p.Stacktrace...),
		}
		return outcome, nil
	case payloadKindSystemError:
		var p SystemErrorPayload
		if err := json.Unmarshal(data, &p); err != nil {
			return TaskOutcome{}, err
		}
		outcome.Status = TaskOutcomeStatusFailed
		outcome.Error = &TaskError{
			Kind:       TaskErrorKindSystem,
			Message:    p.Message,
			Component:  p.Component,
			Code:       p.Code,
			Retryable:  boolPtr(p.Retryable),
			InputRef:   p.InputRef,
			Stacktrace: append([]string(nil), p.Stacktrace...),
		}
		return outcome, nil
	case payloadKindTimeout:
		var p TimeoutPayload
		if err := json.Unmarshal(data, &p); err != nil {
			return TaskOutcome{}, err
		}
		outcome.Status = TaskOutcomeStatusFailed
		outcome.Error = &TaskError{
			Kind:      TaskErrorKindTimeout,
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
		outcome.Status = TaskOutcomeStatusFailed
		outcome.Error = &TaskError{
			Kind:    TaskErrorKindSystem,
			Message: fmt.Sprintf("unsupported payload kind %q", payloadKind),
			Code:    "unknown_payload_kind",
		}
		return outcome, nil
	}
}

func shouldSynthesizeNextAttempt(lastAttempt int, outcome TaskOutcome, policy RetryPolicy) bool {
	if lastAttempt <= 0 || outcome.Status != TaskOutcomeStatusFailed {
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
	case TaskErrorKindTimeout:
		if outcome.Error.Retryable == nil {
			return false
		}
		return *outcome.Error.Retryable
	case TaskErrorKindSystem:
		if outcome.Error.Retryable == nil {
			return true
		}
		return *outcome.Error.Retryable
	case TaskErrorKindApp:
		return true
	default:
		return true
	}
}

func cloneStoredJobRunChapter(chapter Chapter) Chapter {
	cloned := chapter
	cloned.Body = cloneChapterBody(chapter.Body)
	cloned.Metadata = cloneChapterMetadata(chapter.Metadata)
	if len(chapter.Artifacts) > 0 {
		cloned.Artifacts = append([]StoredArtifact(nil), chapter.Artifacts...)
	}
	return cloned
}

func cloneJobSummary(job JobSummary) JobSummary {
	cloned := job
	cloned.WaitFor = append([]string(nil), job.WaitFor...)
	cloned.Payload = append(json.RawMessage(nil), job.Payload...)
	cloned.Metadata = append(json.RawMessage(nil), job.Metadata...)
	cloned.ExpiresAt = cloneTimePtr(job.ExpiresAt)
	cloned.LeaseExpiresAt = cloneTimePtr(job.LeaseExpiresAt)
	cloned.TaskWaitInput = cloneInt64Ptr(job.TaskWaitInput)
	cloned.TaskWaitOutput = cloneInt64Ptr(job.TaskWaitOutput)
	cloned.TaskWaitInputHash = cloneStringPtr(job.TaskWaitInputHash)
	cloned.TaskWaitNext = cloneStringPtr(job.TaskWaitNext)
	cloned.NextNeed = cloneStringPtr(job.NextNeed)
	return cloned
}

func cloneTimePtr(src *time.Time) *time.Time {
	if src == nil {
		return nil
	}
	value := *src
	return &value
}

func cloneInt64Ptr(src *int64) *int64 {
	if src == nil {
		return nil
	}
	value := *src
	return &value
}

func cloneStringPtr(src *string) *string {
	if src == nil {
		return nil
	}
	value := *src
	return &value
}

func boolPtr(v bool) *bool {
	value := v
	return &value
}

func intPtr(v int) *int {
	if v <= 0 {
		return nil
	}
	value := v
	return &value
}

func int64Ptr(v int64) *int64 {
	if v <= 0 {
		return nil
	}
	value := v
	return &value
}

func timePtr(t time.Time) *time.Time {
	if t.IsZero() {
		return nil
	}
	value := t
	return &value
}

func strPtr(s string) *string {
	if s == "" {
		return nil
	}
	value := s
	return &value
}
