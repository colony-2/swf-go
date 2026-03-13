package swf

import (
	"context"
	"fmt"
	"time"
)

type workerEngineAPI interface {
	ReplayJobRun(ctx context.Context, req ReplayRunRequest) (JobData, error)
	FindTasksWaitingForCapability(ctx context.Context, jobType string, taskType string, tenantIds []string) ([]TaskHandle, error)
	GetWaitingTask(ctx context.Context, key JobKey) (TaskHandle, error)
	Run(ctx context.Context)
	RegisterWorkers(workset *WorkSet) error
	GetArtifact(tenantId string, key ArtifactKey) (Artifact, error)
}

type runtimeEngine struct {
	runtime WorkflowRuntime
	worker  workerEngineAPI
}

func newRuntimeEngine(runtime WorkflowRuntime, worker workerEngineAPI) SWFEngine {
	if worker == nil {
		return nil
	}
	return &runtimeEngine{
		runtime: runtime,
		worker:  worker,
	}
}

func (e *runtimeEngine) StartJob(ctx context.Context, start StartJob) (JobKey, error) {
	handle, err := e.runtime.StartJob(ctx, StartJobRequest{
		Job:         start,
		RequestTime: nowUTC(),
	})
	if err != nil {
		return JobKey{}, err
	}
	return handle.JobKey, nil
}

func (e *runtimeEngine) RestartJob(ctx context.Context, restart RestartJob) (JobKey, error) {
	handle, err := e.runtime.RestartJob(ctx, RestartJobRequest{
		Job:         restart,
		RequestTime: nowUTC(),
	})
	if err != nil {
		return JobKey{}, err
	}
	return handle.JobKey, nil
}

func (e *runtimeEngine) CancelJob(ctx context.Context, cancel CancelJob) error {
	return e.runtime.CancelJob(ctx, CancelJobRequest{
		JobKey: cancel.JobKey,
		Reason: cancel.Reason,
	})
}

func (e *runtimeEngine) CheckJobStatus(ctx context.Context, jobKey JobKey) (JobStatus, error) {
	return e.runtime.CheckJobStatus(ctx, jobKey)
}

func (e *runtimeEngine) GetJobResult(ctx context.Context, jobKey JobKey) (TaskData, error) {
	return e.runtime.GetJobResult(ctx, jobKey)
}

func (e *runtimeEngine) GetJobRun(ctx context.Context, req GetJobRunRequest) (GetJobRunResponse, error) {
	return e.runtime.GetJobRun(ctx, req)
}

func (e *runtimeEngine) ReplayJobRun(ctx context.Context, req ReplayRunRequest) (JobData, error) {
	return e.worker.ReplayJobRun(ctx, req)
}

func (e *runtimeEngine) ListJobs(ctx context.Context, req ListJobsRequest) (ListJobsResponse, error) {
	return e.runtime.ListJobs(ctx, req)
}

func (e *runtimeEngine) FindTasksWaitingForCapability(ctx context.Context, jobType string, taskType string, tenantIds []string) ([]TaskHandle, error) {
	return e.worker.FindTasksWaitingForCapability(ctx, jobType, taskType, tenantIds)
}

func (e *runtimeEngine) GetWaitingTask(ctx context.Context, key JobKey) (TaskHandle, error) {
	return e.worker.GetWaitingTask(ctx, key)
}

func (e *runtimeEngine) Run(ctx context.Context) {
	e.worker.Run(ctx)
}

func (e *runtimeEngine) RegisterWorkers(workset *WorkSet) error {
	return e.worker.RegisterWorkers(workset)
}

func (e *runtimeEngine) GetArtifact(tenantId string, key ArtifactKey) (Artifact, error) {
	if tenantId == "" {
		return nil, fmt.Errorf("tenantId is required")
	}
	if err := key.Validate(); err != nil {
		return nil, err
	}
	return e.worker.GetArtifact(tenantId, key)
}

func nowUTC() time.Time {
	return time.Now().UTC()
}
