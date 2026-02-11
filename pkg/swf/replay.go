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

// ReattemptObserver receives retry boundary events.
type ReattemptObserver interface {
	OnTaskReattemptBoundary(event TaskReattemptBoundary)
	OnJobReattemptBoundary(event JobReattemptBoundary)
}

type TaskReattemptBoundary struct {
	JobKey                 JobKey
	TaskType               string
	PreviousAttemptOrdinal int64
	PreviousAttemptNumber  int
	PreviousAttemptError   error
	NextAttemptOrdinal     int64
	NextAttemptNumber      int
}

type JobReattemptBoundary struct {
	JobKey                 JobKey
	PreviousAttemptOrdinal int64
	PreviousAttemptNumber  int
	PreviousAttemptError   error
	NextAttemptOrdinal     int64
	NextAttemptNumber      int
}

// ReplayRunRequest describes a cache-only job replay.
type ReplayRunRequest struct {
	JobKey   JobKey
	Observer ReattemptObserver // optional
}
