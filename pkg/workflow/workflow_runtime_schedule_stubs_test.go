package workflow

import (
	"context"
	"errors"
)

var errUnexpectedScheduleRuntimeCall = errors.New("unexpected schedule runtime call")

func (r *fakeWorkflowRuntime) UpsertSchedule(context.Context, UpsertScheduleRequest) (ScheduleInfo, error) {
	return ScheduleInfo{}, errUnexpectedScheduleRuntimeCall
}

func (r *fakeWorkflowRuntime) GetSchedule(context.Context, ScheduleKey) (ScheduleInfo, error) {
	return ScheduleInfo{}, errUnexpectedScheduleRuntimeCall
}

func (r *fakeWorkflowRuntime) ListSchedules(context.Context, ListSchedulesRequest) (ListSchedulesResponse, error) {
	return ListSchedulesResponse{}, errUnexpectedScheduleRuntimeCall
}

func (r *fakeWorkflowRuntime) PauseSchedule(context.Context, ScheduleMutationRequest) (ScheduleInfo, error) {
	return ScheduleInfo{}, errUnexpectedScheduleRuntimeCall
}

func (r *fakeWorkflowRuntime) ResumeSchedule(context.Context, ScheduleMutationRequest) (ScheduleInfo, error) {
	return ScheduleInfo{}, errUnexpectedScheduleRuntimeCall
}

func (r *fakeWorkflowRuntime) ArchiveSchedule(context.Context, ScheduleMutationRequest) (ScheduleInfo, error) {
	return ScheduleInfo{}, errUnexpectedScheduleRuntimeCall
}

func (r *fakeWorkflowRuntime) TriggerSchedule(context.Context, TriggerScheduleRequest) (JobHandle, error) {
	return JobHandle{}, errUnexpectedScheduleRuntimeCall
}

func (r *fakeWorkflowRuntime) ListScheduleRuns(context.Context, ListScheduleRunsRequest) (ListScheduleRunsResponse, error) {
	return ListScheduleRunsResponse{}, errUnexpectedScheduleRuntimeCall
}

func (r *runnerTestRuntime) UpsertSchedule(context.Context, UpsertScheduleRequest) (ScheduleInfo, error) {
	return ScheduleInfo{}, errUnexpectedScheduleRuntimeCall
}

func (r *runnerTestRuntime) GetSchedule(context.Context, ScheduleKey) (ScheduleInfo, error) {
	return ScheduleInfo{}, errUnexpectedScheduleRuntimeCall
}

func (r *runnerTestRuntime) ListSchedules(context.Context, ListSchedulesRequest) (ListSchedulesResponse, error) {
	return ListSchedulesResponse{}, errUnexpectedScheduleRuntimeCall
}

func (r *runnerTestRuntime) PauseSchedule(context.Context, ScheduleMutationRequest) (ScheduleInfo, error) {
	return ScheduleInfo{}, errUnexpectedScheduleRuntimeCall
}

func (r *runnerTestRuntime) ResumeSchedule(context.Context, ScheduleMutationRequest) (ScheduleInfo, error) {
	return ScheduleInfo{}, errUnexpectedScheduleRuntimeCall
}

func (r *runnerTestRuntime) ArchiveSchedule(context.Context, ScheduleMutationRequest) (ScheduleInfo, error) {
	return ScheduleInfo{}, errUnexpectedScheduleRuntimeCall
}

func (r *runnerTestRuntime) TriggerSchedule(context.Context, TriggerScheduleRequest) (JobHandle, error) {
	return JobHandle{}, errUnexpectedScheduleRuntimeCall
}

func (r *runnerTestRuntime) ListScheduleRuns(context.Context, ListScheduleRunsRequest) (ListScheduleRunsResponse, error) {
	return ListScheduleRunsResponse{}, errUnexpectedScheduleRuntimeCall
}

func (r *runJobIfLeaseableStubRuntime) UpsertSchedule(context.Context, UpsertScheduleRequest) (ScheduleInfo, error) {
	return ScheduleInfo{}, errUnexpectedScheduleRuntimeCall
}

func (r *runJobIfLeaseableStubRuntime) GetSchedule(context.Context, ScheduleKey) (ScheduleInfo, error) {
	return ScheduleInfo{}, errUnexpectedScheduleRuntimeCall
}

func (r *runJobIfLeaseableStubRuntime) ListSchedules(context.Context, ListSchedulesRequest) (ListSchedulesResponse, error) {
	return ListSchedulesResponse{}, errUnexpectedScheduleRuntimeCall
}

func (r *runJobIfLeaseableStubRuntime) PauseSchedule(context.Context, ScheduleMutationRequest) (ScheduleInfo, error) {
	return ScheduleInfo{}, errUnexpectedScheduleRuntimeCall
}

func (r *runJobIfLeaseableStubRuntime) ResumeSchedule(context.Context, ScheduleMutationRequest) (ScheduleInfo, error) {
	return ScheduleInfo{}, errUnexpectedScheduleRuntimeCall
}

func (r *runJobIfLeaseableStubRuntime) ArchiveSchedule(context.Context, ScheduleMutationRequest) (ScheduleInfo, error) {
	return ScheduleInfo{}, errUnexpectedScheduleRuntimeCall
}

func (r *runJobIfLeaseableStubRuntime) TriggerSchedule(context.Context, TriggerScheduleRequest) (JobHandle, error) {
	return JobHandle{}, errUnexpectedScheduleRuntimeCall
}

func (r *runJobIfLeaseableStubRuntime) ListScheduleRuns(context.Context, ListScheduleRunsRequest) (ListScheduleRunsResponse, error) {
	return ListScheduleRunsResponse{}, errUnexpectedScheduleRuntimeCall
}
