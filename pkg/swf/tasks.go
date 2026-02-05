package swf

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"
)

type taskRunApi interface {
	// FindTasksWaitingForCapability returns task handles for jobs waiting for the given capability.
	// If tenantIds is non-empty, only tasks from those tenants are returned.
	// If tenantIds is empty, all tasks are returned.
	FindTasksWaitingForCapability(ctx context.Context, jobType string, taskType string, tenantIds []string) ([]TaskHandle, error)
	// GetWaitingTask returns a task handle if the job is currently ready/pending that capability.
	GetWaitingTask(ctx context.Context, key JobKey) (TaskHandle, error)
}

type TaskContext struct {
	JobKey JobKey
	Step   int64
	Logger *slog.Logger
	// await is set by the runner so AwaitDuration can be engine-directed.
	await     func(wakeAt time.Time) error
	awaitJobs func(jobIds ...string) error
}

// AwaitDuration pauses task execution for the specified duration.
// The engine may override this to reschedule work or recycle runners.
func (tc TaskContext) AwaitDuration(waitFor Duration) error {
	// zero/negative waits should fall through without blocking.
	wait := waitFor.ToDuration()
	if wait <= 0 {
		return nil
	}
	wakeAt := time.Now().Add(wait)
	if tc.await != nil {
		return tc.await(wakeAt)
	}
	time.Sleep(time.Until(wakeAt))
	return nil
}

// AwaitJobs waits for the provided job IDs to complete.
func (tc TaskContext) AwaitJobs(jobIds ...string) error {
	if tc.awaitJobs == nil {
		return fmt.Errorf("awaiting jobs not supported in this context")
	}
	return tc.awaitJobs(jobIds...)
}

// NewTaskContext builds a task context with an optional await handler.
func NewTaskContext(jobKey JobKey, step int64, logger *slog.Logger, await func(time.Time) error, awaitJobs func(...string) error) TaskContext {
	return TaskContext{
		JobKey:    jobKey,
		Step:      step,
		Logger:    logger,
		await:     await,
		awaitJobs: awaitJobs,
	}
}

type Worker interface {
	worker()
}

type TaskHandle interface {
	JobKey() JobKey
	Data() (TaskData, error)
	Finish(ctx context.Context, taskData TaskData) error
	TaskOrdinalToComplete() int64
	TaskType() string
	CreatedAt() time.Time
	Metadata() json.RawMessage
}

type TaskCompletion struct {
	JobKey JobKey
	Step   int64
	Error  error
}
