package workflow

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"
)

type taskRunApi interface {
	// FindTasksWaitingForCapability returns task handles for jobs waiting for the given capability.
	// tenantIds must contain at least one tenant ID.
	FindTasksWaitingForCapability(ctx context.Context, jobType string, taskType string, tenantIds []string) ([]TaskHandle, error)
	// FindTasksWaiting returns task handles for jobs waiting for the given capability and optional metadata filter.
	FindTasksWaiting(ctx context.Context, req FindTasksWaitingRequest) ([]TaskHandle, error)
	// GetWaitingTask returns a task handle if the job is currently ready/pending that capability.
	GetWaitingTask(ctx context.Context, key JobKey) (TaskHandle, error)
}

type FindTasksWaitingRequest struct {
	JobType        string
	TaskType       string
	TenantIds      []string
	MetadataFilter MetadataFilter
	Limit          int
}

type TaskContext struct {
	JobKey JobKey
	Step   int64
	Logger *slog.Logger
	// await is set by the runner so AwaitDuration can be engine-directed.
	await            func(wakeAt time.Time) error
	awaitJobs        func(jobIds ...string) error
	submitJob        func(context.Context, SubmitJob) (JobKey, error)
	submitRestartJob func(context.Context, SubmitRestartJob) (JobKey, error)
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

// SubmitJob starts a child job using the current job lease.
func (tc TaskContext) SubmitJob(ctx context.Context, submit SubmitJob) (JobKey, error) {
	if tc.submitJob == nil {
		return JobKey{}, fmt.Errorf("submitting jobs not supported in this context")
	}
	return tc.submitJob(ctx, submit)
}

// SubmitRestartJob starts a child restart job using the current job lease.
func (tc TaskContext) SubmitRestartJob(ctx context.Context, restart SubmitRestartJob) (JobKey, error) {
	if tc.submitRestartJob == nil {
		return JobKey{}, fmt.Errorf("submitting restart jobs not supported in this context")
	}
	return tc.submitRestartJob(ctx, restart)
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

func newTaskContextWithLeaseActions(
	jobKey JobKey,
	step int64,
	logger *slog.Logger,
	await func(time.Time) error,
	awaitJobs func(...string) error,
	submitJob func(context.Context, SubmitJob) (JobKey, error),
	submitRestartJob func(context.Context, SubmitRestartJob) (JobKey, error),
) TaskContext {
	tc := NewTaskContext(jobKey, step, logger, await, awaitJobs)
	tc.submitJob = submitJob
	tc.submitRestartJob = submitRestartJob
	return tc
}

type Worker interface {
	worker()
}

type TaskWorker interface {
	Name() string
	Run(context TaskContext, input TaskData) (TaskData, error)
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
