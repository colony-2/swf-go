package swf

import "context"

type StartJob struct {
	RetryPolicy  RetryPolicy
	JobId        JobId
	Data         JobData
	Dependencies Dependencies
}

type RestartJob struct {
	PriorJobId     JobId
	LastStepToKeep int64

	RetryPolicy     RetryPolicy
	NewJobId        JobId
	DataForNextTask TaskData
	Dependencies    Dependencies
}

type CancelJob struct {
	JobId  JobId
	Reason string
}

type JobStatus struct {
	JobId JobId
	Step  int64
}

type JobData TaskData

type JobContext interface {
	jobRunApi
	GetJobId() JobId
	DoTask(retryPolicy RetryPolicy, taskType string, data TaskData) (TaskData, error)
}

type JobOutcome interface {
	jobOutcome()
}

type JobOutcomeSuccess JobData

func (os *JobOutcomeSuccess) jobOutcome() {}

type JobOutcomeFailure string

func (of *JobOutcomeFailure) jobOutcome() {}

type JobOutcomeSuspend struct {
	Dependencies Dependencies
}

func (s *JobOutcomeSuspend) jobOutcome() {}

type JobWorker interface {
	Name() string
	Run(JobContext, StartJob) (JobOutcome, error)
}

type jobRunApi interface {
	StartJob(ctx context.Context, start StartJob) error
	RestartJob(ctx context.Context, restart RestartJob) error
	CancelJob(ctx context.Context, cancel CancelJob) error
	RegisterJobWorkers(workers ...JobWorker) error
}
