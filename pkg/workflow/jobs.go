package workflow

import (
	"context"
	"fmt"
	"log/slog"
	"regexp"
	"time"
)

type JobContext interface {
	GetJobKey() JobKey
	Logger() *slog.Logger
	DoTask(policy RunPolicy, taskType string, data TaskData) (TaskData, error)
	AwaitDuration(waitFor Duration) error
	AwaitJobs(jobIds ...string) error
	SubmitJob(ctx context.Context, submit SubmitJob) (JobKey, error)
	SubmitRestartJob(ctx context.Context, restart SubmitRestartJob) (JobKey, error)
}

type JobWorker interface {
	Name() string
	Run(JobContext, JobData) (JobData, error)
}

type jobRunApi interface {
	SubmitJob(ctx context.Context, submit SubmitJob) (JobKey, error)
	SubmitRestartJob(ctx context.Context, restart SubmitRestartJob) (JobKey, error)
	CancelJob(ctx context.Context, cancel CancelJob) error
	GetJob(ctx context.Context, jobKey JobKey) (JobInfo, error)
	GetJobRun(ctx context.Context, req GetJobRunRequest) (GetJobRunResponse, error)
	ReplayJobRun(ctx context.Context, req ReplayRunRequest) (JobData, error)
}

type EngineBuilder struct {
	workers      map[string]WorkSet
	maxActive    int
	logger       *slog.Logger
	awaitRecycle time.Duration
	pollTenantId string
	runtime      WorkflowRuntime
}

type WorkRegistrationOptions struct {
	MetadataFilter MetadataFilter
}

type WorkSet struct {
	JobWorker   JobWorker
	TaskWorkers map[string]TaskWorker
	Options     WorkRegistrationOptions

	metadataEquals []MetadataPredicate
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

// WithWorkerTenantId configures the tenant polled by the worker loop.
// Engines with registered workers must set this before BuildEngine.
func (e *EngineBuilder) WithWorkerTenantId(tenantId string) *EngineBuilder {
	e.pollTenantId = tenantId
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
	return AsWorkSetWithOptions(jobWorker, WorkRegistrationOptions{}, taskWorkers...)
}

func AsWorkSetWithOptions(jobWorker JobWorker, opts WorkRegistrationOptions, taskWorkers ...TaskWorker) (*WorkSet, error) {
	namePattern := regexp.MustCompile(`^[A-Za-z0-9_-]+$`)
	if !namePattern.MatchString(jobWorker.Name()) {
		fmt.Println(jobWorker.Name())
		return nil, fmt.Errorf("invalid job worker name %s", jobWorker.Name())
	}
	predicates, err := MetadataPredicates(opts.MetadataFilter)
	if err != nil {
		return nil, err
	}
	if _, err := metadataPredicateSignature(predicates); err != nil {
		return nil, err
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
		JobWorker:      jobWorker,
		TaskWorkers:    tasks,
		Options:        opts,
		metadataEquals: predicates,
	}, nil
}

func (e *EngineBuilder) PlusWorkers(jobWorker JobWorker, taskWorkers ...TaskWorker) *EngineBuilder {
	return e.PlusWorkersWithOptions(jobWorker, WorkRegistrationOptions{}, taskWorkers...)
}

func (e *EngineBuilder) PlusWorkersWithOptions(jobWorker JobWorker, opts WorkRegistrationOptions, taskWorkers ...TaskWorker) *EngineBuilder {
	if _, ok := e.workers[jobWorker.Name()]; ok {
		panic("job worker with name " + jobWorker.Name() + " already registered")
	}

	ws, err := AsWorkSetWithOptions(jobWorker, opts, taskWorkers...)
	if err != nil {
		panic(err)
	}
	e.workers[jobWorker.Name()] = *ws
	return e
}

func (b *EngineBuilder) BuildEngine() (Engine, error) {
	if b.runtime == nil {
		return nil, fmt.Errorf("workflow runtime is required")
	}
	runtime := b.runtime

	ws := make([]WorkSet, len(b.workers))
	i := 0
	for _, v := range b.workers {
		ws[i] = v
		i++
	}

	workerEngine, err := newWorkerEngine(runtime, ws, RuntimeBuildOptions{
		Logger:                b.logger,
		MaxActive:             b.maxActive,
		AwaitRecycleThreshold: b.awaitRecycle,
		PollTenantId:          b.pollTenantId,
	})
	if err != nil {
		return nil, err
	}
	return newRuntimeEngine(runtime, workerEngine), nil
}
