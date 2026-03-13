package swf

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"regexp"
	"time"
)

// StartJob defines the parameters for starting a new workflow job.
// If JobID is provided, it will be used as the job identifier; otherwise, a new unique ID will be generated.
type StartJob struct {
	TenantId      string            // REQUIRED: Tenant for this job
	JobType       string            // The type of job to start (must match a registered JobWorker name)
	JobID         string            // Optional job identifier. If empty, a new unique ID will be generated using ksuid
	SingletonKey  string            // Optional key to ensure only one job with this key runs at a time
	Data          JobData           // Input data for the job
	RunPolicy     RunPolicy         // Runtime policy for retries, timeouts, etc.
	Metadata      json.RawMessage   // Optional metadata persisted with the job in pgwf
	Prerequisites []JobPrerequisite // Optional prerequisites that must complete before this job starts
}

type RestartJob struct {
	PriorJobKey     JobKey
	LastStepToKeep  int64
	JobID           string            // optional override for new job id
	ExtraTaskInput  TaskData          // optional input used to compute hash for ExtraTaskOutput
	ExtraTaskOutput TaskData          // optional cached task/job output to append at LastStepToKeep+1
	Prerequisites   []JobPrerequisite // Optional prerequisites that must complete before this job starts
}

// JobPrereqCondition defines how a prerequisite is evaluated.
type JobPrereqCondition string

const (
	JobPrereqComplete JobPrereqCondition = "complete" // job must be archived (any outcome)
	JobPrereqSuccess  JobPrereqCondition = "success"  // job must be archived + succeeded
)

// JobPrerequisite declares a dependency on another job.
type JobPrerequisite struct {
	JobID     string             // required; same tenant as the parent job
	Condition JobPrereqCondition // required; default to JobPrereqComplete if empty
}

type CancelJob struct {
	JobKey JobKey
	Reason string
}

type JobStatus string

const (
	JobStatusReady          JobStatus = "READY"
	JobStatusExpired        JobStatus = "EXPIRED"
	JobStatusPendingJobs    JobStatus = "PENDING_JOBS"
	JobStatusAwaitingFuture JobStatus = "AWAITING_FUTURE"
	JobStatusActive         JobStatus = "ACTIVE"
	JobStatusCrashConcern   JobStatus = "CRASH_CONCERN"
	JobStatusCancelled      JobStatus = "CANCELLED"
	JobStatusCompleted      JobStatus = "COMPLETED"
)

type JobData TaskData

type JobContext interface {
	//jobRunApi
	GetJobKey() JobKey
	Logger() *slog.Logger
	//RunChildJobSync(ctx context.Context, childJob StartJob) (JobKey, error)
	DoTask(policy RunPolicy, taskType string, data TaskData) (TaskData, error)
	AwaitDuration(waitFor Duration) error
	AwaitJobs(jobIds ...string) error
}

type JobWorker interface {
	Name() string
	Run(JobContext, JobData) (JobData, error)
}

type jobRunApi interface {
	StartJob(ctx context.Context, start StartJob) (JobKey, error)
	RestartJob(ctx context.Context, restart RestartJob) (JobKey, error)
	CancelJob(ctx context.Context, cancel CancelJob) error
	CheckJobStatus(ctx context.Context, jobKey JobKey) (JobStatus, error)
	GetJobResult(ctx context.Context, jobKey JobKey) (TaskData, error)
	GetJobRun(ctx context.Context, req GetJobRunRequest) (GetJobRunResponse, error)
	ReplayJobRun(ctx context.Context, req ReplayRunRequest) (JobData, error)
}

type EngineBuilder struct {
	workers      map[string]WorkSet
	maxActive    int
	logger       *slog.Logger
	awaitRecycle time.Duration
	runtime      WorkflowRuntime
}

type WorkSet struct {
	JobWorker   JobWorker
	TaskWorkers map[string]TaskWorker
}

func NewEngineBuilder() *EngineBuilder {
	return &EngineBuilder{
		workers:      make(map[string]WorkSet),
		maxActive:    4,
		logger:       slog.Default(),
		awaitRecycle: 5 * time.Minute,
	}
}

func (e *EngineBuilder) WithRuntime(runtime WorkflowRuntime) *EngineBuilder {
	e.runtime = runtime
	return e
}

func (e *EngineBuilder) WithMaxActive(maxActive int) *EngineBuilder {
	e.maxActive = maxActive
	return e
}

func (e *EngineBuilder) WithLogger(logger *slog.Logger) *EngineBuilder {
	if logger != nil {
		e.logger = logger
	}
	return e
}

// WithAwaitRecycleThreshold configures how far in the future a wait must be before recycling the runner.
func (e *EngineBuilder) WithAwaitRecycleThreshold(d time.Duration) *EngineBuilder {
	if d > 0 {
		e.awaitRecycle = d
	}
	return e
}

func AsWorkSet(jobWorker JobWorker, taskWorkers ...TaskWorker) (*WorkSet, error) {
	namePattern := regexp.MustCompile(`^[A-Za-z0-9_-]+$`)
	if !namePattern.MatchString(jobWorker.Name()) {
		fmt.Println(jobWorker.Name())
		return nil, fmt.Errorf("invalid job worker name %s", jobWorker.Name())
	}

	tasks := make(map[string]TaskWorker)
	for _, tw := range taskWorkers {

		if _, ok := tasks[tw.Name()]; ok {
			if !namePattern.MatchString(tw.Name()) {
				return nil, fmt.Errorf("invalid task worker name %s", tw.Name())
			}

			return nil, fmt.Errorf("task worker with name %s already registered", tw.Name())
		}
		tasks[tw.Name()] = tw
	}

	return &WorkSet{
		JobWorker:   jobWorker,
		TaskWorkers: tasks,
	}, nil

}

func (e *EngineBuilder) PlusWorkers(jobWorker JobWorker, taskWorkers ...TaskWorker) *EngineBuilder {

	if _, ok := e.workers[jobWorker.Name()]; ok {
		panic("job worker with name " + jobWorker.Name() + " already registered")
	}

	ws, err := AsWorkSet(jobWorker, taskWorkers...)
	if err != nil {
		panic(err)
	}
	e.workers[jobWorker.Name()] = *ws
	return e
}

func (b *EngineBuilder) BuildEngine() (SWFEngine, error) {
	if b.runtime == nil {
		return nil, fmt.Errorf("workflow runtime is required")
	}

	ws := make([]WorkSet, len(b.workers))
	i := 0
	for _, v := range b.workers {
		ws[i] = v
		i++
	}

	workerEngine, err := newWorkerEngine(b.runtime, ws, RuntimeBuildOptions{
		Logger:                b.logger,
		MaxActive:             b.maxActive,
		AwaitRecycleThreshold: b.awaitRecycle,
	})
	if err != nil {
		return nil, err
	}
	return newRuntimeEngine(b.runtime, workerEngine), nil
}
