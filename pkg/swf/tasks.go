package swf

import (
	"context"
	"time"
)

type taskRunApi interface {
	FindTasksWaitingForCapability(ctx context.Context, capability Capability) ([]TaskHandle, error)
	RegisterTaskWorkers(workers ...TaskWorker) error
	GetTaskData(ctx context.Context, jobId JobId, step int64) (TaskData, error)
}

type TaskContext interface {
	JobId() JobId
	Step() int64
}

type TaskWorker interface {
	Name() string
	Run(context TaskContext, input TaskData) (TaskData, error)
}

type TaskHandle interface {
	Data() (TaskData, error)
	JobId() JobId
	Finish(ctx context.Context, taskData TaskData, capability Capability, waitFor []JobId, wait time.Duration) error
}

type TaskCompletion struct {
	JobId JobId
	Step  int64
	Error error
}
