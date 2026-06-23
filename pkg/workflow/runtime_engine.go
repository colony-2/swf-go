package workflow

import (
	"context"
	"fmt"
	"time"
)

type workerEngineAPI interface {
	ReplayJobRun(ctx context.Context, req ReplayRunRequest) (JobData, error)
	Run(ctx context.Context)
	RegisterWorkers(workset *WorkSet) error
	GetArtifact(tenantId string, key ArtifactKey) (Artifact, error)
}

type runtimeEngine struct {
	runtime WorkflowRuntime
	worker  workerEngineAPI
}

func newRuntimeEngine(runtime WorkflowRuntime, worker workerEngineAPI) Engine {
	if worker == nil {
		return nil
	}
	return &runtimeEngine{
		runtime: runtime,
		worker:  worker,
	}
}

func (e *runtimeEngine) SubmitJob(ctx context.Context, submit SubmitJob) (JobKey, error) {
	handle, err := e.runtime.SubmitJob(ctx, SubmitJobRequest{
		Job:         submit,
		RequestTime: nowUTC(),
	})
	if err != nil {
		return JobKey{}, err
	}
	return handle.JobKey, nil
}

func (e *runtimeEngine) SubmitRestartJob(ctx context.Context, restart SubmitRestartJob) (JobKey, error) {
	handle, err := e.runtime.SubmitRestartJob(ctx, SubmitRestartJobRequest{
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

func (e *runtimeEngine) GetJob(ctx context.Context, jobKey JobKey) (JobInfo, error) {
	return e.runtime.GetJob(ctx, jobKey)
}

func (e *runtimeEngine) GetJobRun(ctx context.Context, req GetJobRunRequest) (GetJobRunResponse, error) {
	return GetJobRun(ctx, e.runtime, req)
}

func (e *runtimeEngine) ReplayJobRun(ctx context.Context, req ReplayRunRequest) (JobData, error) {
	return e.worker.ReplayJobRun(ctx, req)
}

func (e *runtimeEngine) ListJobs(ctx context.Context, req ListJobsRequest) (ListJobsResponse, error) {
	return e.runtime.ListJobs(ctx, req)
}

func (e *runtimeEngine) UpsertSchedule(ctx context.Context, req UpsertScheduleRequest) (ScheduleInfo, error) {
	if req.RequestTime.IsZero() {
		req.RequestTime = nowUTC()
	}
	return e.runtime.UpsertSchedule(ctx, req)
}

func (e *runtimeEngine) GetSchedule(ctx context.Context, key ScheduleKey) (ScheduleInfo, error) {
	return e.runtime.GetSchedule(ctx, key)
}

func (e *runtimeEngine) ListSchedules(ctx context.Context, req ListSchedulesRequest) (ListSchedulesResponse, error) {
	return e.runtime.ListSchedules(ctx, req)
}

func (e *runtimeEngine) PauseSchedule(ctx context.Context, req ScheduleMutationRequest) (ScheduleInfo, error) {
	if req.RequestTime.IsZero() {
		req.RequestTime = nowUTC()
	}
	return e.runtime.PauseSchedule(ctx, req)
}

func (e *runtimeEngine) ResumeSchedule(ctx context.Context, req ScheduleMutationRequest) (ScheduleInfo, error) {
	if req.RequestTime.IsZero() {
		req.RequestTime = nowUTC()
	}
	return e.runtime.ResumeSchedule(ctx, req)
}

func (e *runtimeEngine) ArchiveSchedule(ctx context.Context, req ScheduleMutationRequest) (ScheduleInfo, error) {
	if req.RequestTime.IsZero() {
		req.RequestTime = nowUTC()
	}
	return e.runtime.ArchiveSchedule(ctx, req)
}

func (e *runtimeEngine) TriggerSchedule(ctx context.Context, req TriggerScheduleRequest) (JobHandle, error) {
	if req.RequestTime.IsZero() {
		req.RequestTime = nowUTC()
	}
	return e.runtime.TriggerSchedule(ctx, req)
}

func (e *runtimeEngine) ListScheduleRuns(ctx context.Context, req ListScheduleRunsRequest) (ListScheduleRunsResponse, error) {
	return e.runtime.ListScheduleRuns(ctx, req)
}

func (e *runtimeEngine) FindTasksWaitingForCapability(ctx context.Context, jobType string, taskType string, tenantIds []string) ([]TaskHandle, error) {
	return e.FindTasksWaiting(ctx, FindTasksWaitingRequest{
		JobType:   jobType,
		TaskType:  taskType,
		TenantIds: tenantIds,
	})
}

func (e *runtimeEngine) FindTasksWaiting(ctx context.Context, req FindTasksWaitingRequest) ([]TaskHandle, error) {
	return findWaitingTasksFromRuntime(ctx, e.runtime, req)
}

func (e *runtimeEngine) GetWaitingTask(ctx context.Context, key JobKey) (TaskHandle, error) {
	return getWaitingTaskFromRuntime(ctx, e.runtime, key)
}

func (e *runtimeEngine) GetJobLease(ctx context.Context, req GetJobLeaseRequest) (ExecutionLease, error) {
	return e.runtime.GetJobLease(ctx, req)
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
