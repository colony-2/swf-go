package workflow

import (
	"context"
	"errors"
	"fmt"
	"time"
)

type Engine interface {
	jobRunApi
	taskRunApi
	jobLeaseApi
	loopWorkerApi
	jobsListApi
	schedulesApi

	RegisterWorkers(workset *WorkSet) error
	GetArtifact(tenantId string, key ArtifactKey) (Artifact, error)
}

type jobsListApi interface {
	ListJobs(ctx context.Context, req ListJobsRequest) (ListJobsResponse, error)
}

type jobLeaseApi interface {
	GetJobLease(ctx context.Context, req GetJobLeaseRequest) (ExecutionLease, error)
}

type schedulesApi interface {
	UpsertSchedule(ctx context.Context, req UpsertScheduleRequest) (ScheduleInfo, error)
	GetSchedule(ctx context.Context, key ScheduleKey) (ScheduleInfo, error)
	ListSchedules(ctx context.Context, req ListSchedulesRequest) (ListSchedulesResponse, error)
	PauseSchedule(ctx context.Context, req ScheduleMutationRequest) (ScheduleInfo, error)
	ResumeSchedule(ctx context.Context, req ScheduleMutationRequest) (ScheduleInfo, error)
	ArchiveSchedule(ctx context.Context, req ScheduleMutationRequest) (ScheduleInfo, error)
	TriggerSchedule(ctx context.Context, req TriggerScheduleRequest) (JobHandle, error)
	ListScheduleRuns(ctx context.Context, req ListScheduleRunsRequest) (ListScheduleRunsResponse, error)
}

func WaitForJobToComplete(ctx context.Context, timeout time.Duration, jobKey JobKey, engine Engine) error {
	pollInterval := 200 * time.Millisecond
	pollCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-pollCtx.Done():
			if errors.Is(pollCtx.Err(), context.DeadlineExceeded) {
				return fmt.Errorf("job %s did not complete within the specified timeout of %s", jobKey, timeout)
			}
			return fmt.Errorf("polling for job %s stopped unexpectedly: %v", jobKey, pollCtx.Err())

		case <-ticker.C:
			// Time to check the status
			job, err := engine.GetJob(ctx, jobKey)
			if err != nil {
				return fmt.Errorf("failed to check status for job %s: %v", jobKey, err)
			}

			if job.Status == JobStatusCompleted {
				return nil
			}

		}
	}
}
