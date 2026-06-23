package workflow

import (
	"context"
	"errors"
	"fmt"
	"time"
)

type ReplayCacheMissReason string

const (
	ReplayCacheMissTaskResultMissing ReplayCacheMissReason = "task_result_missing"
	ReplayCacheMissJobResultMissing  ReplayCacheMissReason = "job_result_missing"
	ReplayCacheMissAwaitNotReady     ReplayCacheMissReason = "await_not_ready"
	ReplayCacheMissAwaitJobsPending  ReplayCacheMissReason = "await_jobs_pending"
)

// ReplayCacheMissError indicates replay could not proceed due to missing cached data.
type ReplayCacheMissError struct {
	JobKey   JobKey
	TaskType string
	Ordinal  int64
	Attempt  int
	Reason   ReplayCacheMissReason
}

func (e ReplayCacheMissError) Error() string {
	reason := string(e.Reason)
	if reason == "" {
		reason = "cache_miss"
	}
	if e.TaskType != "" {
		return fmt.Sprintf("replay cache miss: %s (task=%s ordinal=%d attempt=%d)", reason, e.TaskType, e.Ordinal, e.Attempt)
	}
	return fmt.Sprintf("replay cache miss: %s (ordinal=%d attempt=%d)", reason, e.Ordinal, e.Attempt)
}

// ErrReplayShouldNeverMutate signals replay attempted to mutate state.
var ErrReplayShouldNeverMutate = errors.New("replay run should never mutate state")

type replayReadOnlyRuntime struct {
	runtime WorkflowRuntime
}

func newReplayReadOnlyRuntime(runtime WorkflowRuntime) WorkflowRuntime {
	return replayReadOnlyRuntime{runtime: runtime}
}

func replayMutationError(operation string) error {
	return fmt.Errorf("%w: %s", ErrReplayShouldNeverMutate, operation)
}

func (r replayReadOnlyRuntime) SubmitJob(context.Context, SubmitJobRequest) (JobHandle, error) {
	return JobHandle{}, replayMutationError("submit job")
}

func (r replayReadOnlyRuntime) SubmitRestartJob(context.Context, SubmitRestartJobRequest) (JobHandle, error) {
	return JobHandle{}, replayMutationError("submit restart job")
}

func (r replayReadOnlyRuntime) CancelJob(context.Context, CancelJobRequest) error {
	return replayMutationError("cancel job")
}

func (r replayReadOnlyRuntime) PollWork(context.Context, PollWorkRequest) ([]ExecutionLease, error) {
	return nil, replayMutationError("poll work")
}

func (r replayReadOnlyRuntime) GetJobLease(context.Context, GetJobLeaseRequest) (ExecutionLease, error) {
	return nil, replayMutationError("get job lease")
}

func (r replayReadOnlyRuntime) CompleteTaskIfWaiting(context.Context, CompleteTaskIfWaitingRequest) error {
	return replayMutationError("complete waiting task")
}

func (r replayReadOnlyRuntime) UpsertSchedule(context.Context, UpsertScheduleRequest) (ScheduleInfo, error) {
	return ScheduleInfo{}, replayMutationError("upsert schedule")
}

func (r replayReadOnlyRuntime) GetSchedule(ctx context.Context, key ScheduleKey) (ScheduleInfo, error) {
	return r.runtime.GetSchedule(ctx, key)
}

func (r replayReadOnlyRuntime) ListSchedules(ctx context.Context, req ListSchedulesRequest) (ListSchedulesResponse, error) {
	return r.runtime.ListSchedules(ctx, req)
}

func (r replayReadOnlyRuntime) PauseSchedule(context.Context, ScheduleMutationRequest) (ScheduleInfo, error) {
	return ScheduleInfo{}, replayMutationError("pause schedule")
}

func (r replayReadOnlyRuntime) ResumeSchedule(context.Context, ScheduleMutationRequest) (ScheduleInfo, error) {
	return ScheduleInfo{}, replayMutationError("resume schedule")
}

func (r replayReadOnlyRuntime) ArchiveSchedule(context.Context, ScheduleMutationRequest) (ScheduleInfo, error) {
	return ScheduleInfo{}, replayMutationError("archive schedule")
}

func (r replayReadOnlyRuntime) TriggerSchedule(context.Context, TriggerScheduleRequest) (JobHandle, error) {
	return JobHandle{}, replayMutationError("trigger schedule")
}

func (r replayReadOnlyRuntime) ListScheduleRuns(ctx context.Context, req ListScheduleRunsRequest) (ListScheduleRunsResponse, error) {
	return r.runtime.ListScheduleRuns(ctx, req)
}

func (r replayReadOnlyRuntime) GetJob(ctx context.Context, jobKey JobKey) (JobInfo, error) {
	return r.runtime.GetJob(ctx, jobKey)
}

func (r replayReadOnlyRuntime) ListJobs(ctx context.Context, req ListJobsRequest) (ListJobsResponse, error) {
	return r.runtime.ListJobs(ctx, req)
}

func (r replayReadOnlyRuntime) GetChapter(ctx context.Context, ref ChapterRef) (Chapter, error) {
	return r.runtime.GetChapter(ctx, ref)
}

func (r replayReadOnlyRuntime) ListChapters(ctx context.Context, req ListChaptersRequest) ([]Chapter, error) {
	return r.runtime.ListChapters(ctx, req)
}

func (r replayReadOnlyRuntime) PutChapter(context.Context, PutChapterRequest) error {
	return replayMutationError("put chapter")
}

func (r replayReadOnlyRuntime) OpenArtifact(ctx context.Context, ref ArtifactRef) (ArtifactReader, error) {
	return r.runtime.OpenArtifact(ctx, ref)
}

// ReplayObserver receives lifecycle events during replay.
type ReplayObserver interface {
	OnJobStart(event JobStartEvent)
	OnTaskStart(event TaskStartEvent)
	OnTaskEnd(event TaskEndEvent)
	OnJobEnd(event JobEndEvent)
}

type JobStartEvent struct {
	JobKey        JobKey
	AttemptNumber int
	Input         JobData
	At            time.Time
}

type TaskStartEvent struct {
	JobKey        JobKey
	TaskType      string
	Ordinal       int64
	AttemptNumber int
	Input         TaskData
	At            time.Time
}

type TaskEndEvent struct {
	JobKey        JobKey
	TaskType      string
	Ordinal       int64
	AttemptNumber int
	Output        TaskData
	Err           error
	At            time.Time
}

type JobEndEvent struct {
	JobKey        JobKey
	AttemptNumber int
	Output        JobData
	Err           error
	At            time.Time
}

// ReplayRunRequest describes a cache-only job replay.
type ReplayRunRequest struct {
	JobKey   JobKey
	Observer ReplayObserver // optional

	// Optional job worker override for replay-time instrumentation.
	// If nil, the registered job worker is used.
	JobWorker JobWorker
}
