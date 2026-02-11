package swf

import (
	"errors"
	"fmt"
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
}

type TaskStartEvent struct {
	JobKey        JobKey
	TaskType      string
	Ordinal       int64
	AttemptNumber int
	Input         TaskData
}

type TaskEndEvent struct {
	JobKey        JobKey
	TaskType      string
	Ordinal       int64
	AttemptNumber int
	Output        TaskData
	Err           error
}

type JobEndEvent struct {
	JobKey        JobKey
	AttemptNumber int
	Output        JobData
	Err           error
}

// ReplayRunRequest describes a cache-only job replay.
type ReplayRunRequest struct {
	JobKey   JobKey
	Observer ReplayObserver // optional

	// Optional job worker override for replay-time instrumentation.
	// If nil, the registered job worker is used.
	JobWorker JobWorker
}
