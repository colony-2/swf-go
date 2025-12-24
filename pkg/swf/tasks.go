package swf

import (
	"context"
	"fmt"
	"log/slog"
	"time"
)

type taskRunApi interface {
	FindTasksWaitingForCapability(ctx context.Context, jobType string, taskType string) ([]TaskHandle, error)
	// GetWaitingTask returns a task handle if the job is currently ready/pending that capability.
	GetWaitingTask(ctx context.Context, key JobKey) (TaskHandle, error)
}

type TaskContext struct {
	JobKey JobKey
	Step   int64
	Logger *slog.Logger
	// await is set by the runner so AwaitDuration can be engine-directed.
	await      func(wakeAt time.Time) error
	spawnAsync func(jobType string, data TaskData) (*Future, error)
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

// SpawnAsync launches a child job asynchronously.
func (tc TaskContext) SpawnAsync(jobType string, data TaskData) (*Future, error) {
	if tc.spawnAsync == nil {
		return nil, fmt.Errorf("async spawning not supported in this context")
	}
	return tc.spawnAsync(jobType, data)
}

// NewTaskContext builds a task context with an optional await handler.
func NewTaskContext(jobKey JobKey, step int64, logger *slog.Logger, await func(time.Time) error, spawn func(string, TaskData) (*Future, error)) TaskContext {
	return TaskContext{
		JobKey:     jobKey,
		Step:       step,
		Logger:     logger,
		await:      await,
		spawnAsync: spawn,
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
}

type TaskCompletion struct {
	JobKey JobKey
	Step   int64
	Error  error
}
