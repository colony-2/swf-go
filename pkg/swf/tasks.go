package swf

import (
	"context"
)

type taskRunApi interface {
	FindTasksWaitingForCapability(ctx context.Context, jobType string, taskType string) ([]TaskHandle, error)
}

type TaskContext struct {
	JobId JobId
	Step  int64
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
