package swf

import (
	"context"
	"log/slog"
	"time"
)

type taskRunApi interface {
	FindTasksWaitingForCapability(ctx context.Context, jobType string, taskType string) ([]TaskHandle, error)
}

type TaskContext struct {
	JobId  JobId
	Step   int64
	Logger *slog.Logger
	// await is set by the runner so AwaitDuration can be engine-directed.
	await func(wakeAt time.Time) error
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

// NewTaskContext builds a task context with an optional await handler.
func NewTaskContext(jobId JobId, step int64, logger *slog.Logger, await func(time.Time) error) TaskContext {
	return TaskContext{
		JobId:  jobId,
		Step:   step,
		Logger: logger,
		await:  await,
	}
}

type Worker interface {
	worker()
}

type TaskHandle interface {
	JobId() JobId
	Data() (TaskData, error)
	Finish(ctx context.Context, taskData TaskData) error
}

type TaskCompletion struct {
	JobId JobId
	Step  int64
	Error error
}
