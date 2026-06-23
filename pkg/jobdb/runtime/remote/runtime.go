package remote

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/runtimeapi"
)

type Runtime struct {
	raw    *runtimeapi.Client
	client *runtimeapi.ClientWithResponses
}

func New(baseURL string, httpClient *http.Client) (*Runtime, error) {
	opts := make([]runtimeapi.ClientOption, 0, 1)
	if httpClient != nil {
		opts = append(opts, runtimeapi.WithHTTPClient(httpClient))
	}
	raw, err := runtimeapi.NewClient(baseURL, opts...)
	if err != nil {
		return nil, err
	}
	client, err := runtimeapi.NewClientWithResponses(baseURL, opts...)
	if err != nil {
		return nil, err
	}
	return &Runtime{
		raw:    raw,
		client: client,
	}, nil
}

func (r *Runtime) SubmitJob(ctx context.Context, req jobdb.SubmitJobRequest) (jobdb.JobHandle, error) {
	if req.Job.TenantId == "" {
		return jobdb.JobHandle{}, fmt.Errorf("tenantId is required")
	}
	data, err := taskDataToAPIWrite(ctx, jobdb.TaskData(req.Job.Data))
	if err != nil {
		return jobdb.JobHandle{}, err
	}
	metadata, err := metadataJSONToAPI(req.Job.Metadata)
	if err != nil {
		return jobdb.JobHandle{}, err
	}
	runPolicy, err := runPolicyToAPI(req.Job.RunPolicy)
	if err != nil {
		return jobdb.JobHandle{}, err
	}
	body := runtimeapi.SubmitJobRequest{
		Job: runtimeapi.SubmitJob{
			AvailableAt:   cloneTime(req.Job.AvailableAt),
			Data:          data,
			JobType:       req.Job.JobType,
			Metadata:      metadata,
			Prerequisites: toAPIPrerequisites(req.Job.Prerequisites),
			RunPolicy:     runPolicy,
			Schema:        jobSchemaSelectorToAPI(req.Job.Schema),
		},
		RequestTime: timePtr(req.RequestTime),
		WorkerId:    stringPtrOrNil(req.WorkerID),
	}
	if req.Job.JobID != "" {
		resp, err := r.client.PutJobWithResponse(ctx, req.Job.TenantId, req.Job.JobID, body)
		if err != nil {
			return jobdb.JobHandle{}, err
		}
		if resp.StatusCode() != http.StatusOK || resp.JSON200 == nil {
			return jobdb.JobHandle{}, explicitJobCreateError("put job", resp.StatusCode(), resp.Body, resp.JSON409)
		}
		return fromAPIJobHandle(*resp.JSON200), nil
	}
	resp, err := r.client.SubmitJobWithResponse(ctx, req.Job.TenantId, body)
	if err != nil {
		return jobdb.JobHandle{}, err
	}
	if resp.StatusCode() != http.StatusOK || resp.JSON200 == nil {
		return jobdb.JobHandle{}, responseError("submit job", resp.StatusCode(), resp.Body, nil)
	}
	return fromAPIJobHandle(*resp.JSON200), nil
}

func (r *Runtime) SubmitRestartJob(ctx context.Context, req jobdb.SubmitRestartJobRequest) (jobdb.JobHandle, error) {
	if req.Job.PriorJobKey.TenantId == "" {
		return jobdb.JobHandle{}, fmt.Errorf("prior job tenantId is required")
	}
	body := runtimeapi.SubmitRestartJobRequest{
		Job: runtimeapi.SubmitRestartJob{
			LastStepToKeep: req.Job.LastStepToKeep,
			PriorJobKey:    toAPIJobKey(req.Job.PriorJobKey),
			Prerequisites:  toAPIPrerequisites(req.Job.Prerequisites),
			Schema:         jobSchemaSelectorToAPI(req.Job.Schema),
		},
		RequestTime: timePtr(req.RequestTime),
		WorkerId:    stringPtrOrNil(req.WorkerID),
	}
	if req.Job.ExtraTaskInput != nil {
		input, err := taskDataToAPIWrite(ctx, req.Job.ExtraTaskInput)
		if err != nil {
			return jobdb.JobHandle{}, err
		}
		body.Job.ExtraTaskInput = &input
	}
	if req.Job.ExtraTaskOutput != nil {
		output, err := taskDataToAPIWrite(ctx, req.Job.ExtraTaskOutput)
		if err != nil {
			return jobdb.JobHandle{}, err
		}
		body.Job.ExtraTaskOutput = &output
	}
	if req.Job.JobID != "" {
		resp, err := r.client.PutRestartJobWithResponse(ctx, req.Job.PriorJobKey.TenantId, req.Job.JobID, body)
		if err != nil {
			return jobdb.JobHandle{}, err
		}
		if resp.StatusCode() != http.StatusOK || resp.JSON200 == nil {
			return jobdb.JobHandle{}, explicitJobCreateError("put restart job", resp.StatusCode(), resp.Body, resp.JSON409)
		}
		return fromAPIJobHandle(*resp.JSON200), nil
	}
	resp, err := r.client.SubmitRestartJobWithResponse(ctx, req.Job.PriorJobKey.TenantId, body)
	if err != nil {
		return jobdb.JobHandle{}, err
	}
	if resp.StatusCode() != http.StatusOK || resp.JSON200 == nil {
		return jobdb.JobHandle{}, responseError("submit restart job", resp.StatusCode(), resp.Body, nil)
	}
	return fromAPIJobHandle(*resp.JSON200), nil
}

func (r *Runtime) CancelJob(ctx context.Context, req jobdb.CancelJobRequest) error {
	resp, err := r.client.CancelJobWithResponse(ctx, req.JobKey.TenantId, req.JobKey.JobId, runtimeapi.CancelJobRequest{
		Reason:   stringPtrOrNil(req.Reason),
		WorkerId: stringPtrOrNil(req.WorkerID),
	})
	if err != nil {
		return err
	}
	if resp.StatusCode() == http.StatusNoContent {
		return nil
	}
	return responseError("cancel job", resp.StatusCode(), resp.Body, jobdb.ErrJobNotFound)
}

func (r *Runtime) PollWork(ctx context.Context, req jobdb.PollWorkRequest) ([]jobdb.ExecutionLease, error) {
	if req.TenantId == "" {
		return nil, fmt.Errorf("tenantId is required for PollWork")
	}
	return r.pollWorkOnce(ctx, req)
}

func (r *Runtime) pollWorkOnce(ctx context.Context, req jobdb.PollWorkRequest) ([]jobdb.ExecutionLease, error) {
	metadataEquals, err := metadataPredicatesToAPI(predicatesFilter(req.MetadataEquals))
	if err != nil {
		return nil, err
	}
	body := runtimeapi.PollWorkRequest{
		TenantId:       req.TenantId,
		WorkerId:       req.WorkerID,
		Capabilities:   append([]string(nil), req.Capabilities...),
		Limit:          req.Limit,
		LongPollUntil:  req.LongPollUntil,
		LeaseDuration:  toAPIDurationPointer(req.LeaseDuration),
		MetadataEquals: metadataEquals,
	}
	resp, err := r.client.PollWorkWithResponse(ctx, body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode() != http.StatusOK || resp.JSON200 == nil {
		return nil, responseError("poll work", resp.StatusCode(), resp.Body, nil)
	}
	out := make([]jobdb.ExecutionLease, 0, len(resp.JSON200.Leases))
	for _, lease := range resp.JSON200.Leases {
		converted, err := r.executionLeaseFromAPI(lease)
		if err != nil {
			return nil, err
		}
		out = append(out, converted)
	}
	return out, nil
}

func (r *Runtime) GetJobLease(ctx context.Context, req jobdb.GetJobLeaseRequest) (jobdb.ExecutionLease, error) {
	resp, err := r.client.GetJobLeaseWithResponse(ctx, req.JobKey.TenantId, req.JobKey.JobId, runtimeapi.GetJobLeaseRequest{
		Capabilities:  append([]string(nil), req.Capabilities...),
		LeaseDuration: toAPIDurationPointer(req.LeaseDuration),
		WorkerId:      stringPtrOrNil(req.WorkerID),
	})
	if err != nil {
		return nil, err
	}
	if resp.StatusCode() != http.StatusOK || resp.JSON200 == nil {
		return nil, responseError("get job lease", resp.StatusCode(), resp.Body, nil)
	}
	if resp.JSON200.Lease == nil {
		return nil, nil
	}
	return r.executionLeaseFromAPI(*resp.JSON200.Lease)
}

func (r *Runtime) CompleteTaskIfWaiting(ctx context.Context, req jobdb.CompleteTaskIfWaitingRequest) error {
	data, err := taskDataToAPIWrite(ctx, req.Data)
	if err != nil {
		return err
	}
	body := runtimeapi.CommitChapterIfWaitingRequest{
		Capability:    stringPtrOrNil(req.Capability),
		Data:          data,
		InputHash:     stringPtrOrNil(req.InputHash),
		InputOrdinal:  int64Ptr(req.InputOrdinal),
		OutputOrdinal: int64Ptr(req.OutputOrdinal),
		ResumeNeed:    stringPtrOrNil(req.ResumeNeed),
	}
	resp, err := r.client.CommitChapterIfWaitingWithResponse(ctx, req.JobKey.TenantId, req.JobKey.JobId, req.OutputOrdinal, body)
	if err != nil {
		return err
	}
	if resp.StatusCode() == http.StatusNoContent {
		return nil
	}
	return responseErrorWithConflict("complete task if waiting", resp.StatusCode(), resp.Body, jobdb.ErrJobNotFound, jobdb.ErrConflict)
}

func (r *Runtime) GetJob(ctx context.Context, jobKey jobdb.JobKey) (jobdb.JobInfo, error) {
	resp, err := r.client.GetJobWithResponse(ctx, jobKey.TenantId, jobKey.JobId)
	if err != nil {
		return jobdb.JobInfo{}, err
	}
	if resp.StatusCode() != http.StatusOK || resp.JSON200 == nil {
		return jobdb.JobInfo{}, responseError("get job", resp.StatusCode(), resp.Body, jobdb.ErrJobNotFound)
	}
	return jobInfoFromAPI(r, jobKey, *resp.JSON200)
}

func (r *Runtime) ListJobs(ctx context.Context, req jobdb.ListJobsRequest) (jobdb.ListJobsResponse, error) {
	if len(req.TenantIds) != 1 || req.TenantIds[0] == "" {
		return jobdb.ListJobsResponse{}, fmt.Errorf("exactly one tenantId is required")
	}
	metadataPredicates, err := metadataPredicatesToAPI(req.MetadataFilter)
	if err != nil {
		return jobdb.ListJobsResponse{}, err
	}
	body := runtimeapi.ListJobsRequest{
		CreatedAfter:       req.CreatedAfter,
		CreatedBefore:      req.CreatedBefore,
		MetadataPredicates: metadataPredicates,
		PageToken:          stringPtrOrNil(req.PageToken),
		PageSize:           intPtr(req.PageSize),
	}
	if len(req.JobKeys) > 0 {
		jobKeys := make([]runtimeapi.JobKey, 0, len(req.JobKeys))
		for _, jobKey := range req.JobKeys {
			jobKeys = append(jobKeys, toAPIJobKey(jobKey))
		}
		body.JobKeys = &jobKeys
	}
	if len(req.JobTasks) > 0 {
		jobTasks := make([]runtimeapi.JobTaskFilter, 0, len(req.JobTasks))
		for _, task := range req.JobTasks {
			jobTasks = append(jobTasks, runtimeapi.JobTaskFilter{
				JobType:  task.JobType,
				TaskType: task.TaskType,
			})
		}
		body.JobTasks = &jobTasks
	}
	if len(req.JobTypes) > 0 {
		jobTypes := append([]string(nil), req.JobTypes...)
		body.JobTypes = &jobTypes
	}
	if len(req.Statuses) > 0 {
		statuses := make([]runtimeapi.JobStatus, 0, len(req.Statuses))
		for _, status := range req.Statuses {
			statuses = append(statuses, runtimeapi.JobStatus(status))
		}
		body.Statuses = &statuses
	}
	if len(req.Stores) > 0 {
		stores := make([]runtimeapi.JobStore, 0, len(req.Stores))
		for _, store := range req.Stores {
			stores = append(stores, runtimeapi.JobStore(store))
		}
		body.Stores = &stores
	}

	resp, err := r.client.ListJobsWithResponse(ctx, req.TenantIds[0], body)
	if err != nil {
		return jobdb.ListJobsResponse{}, err
	}
	if resp.StatusCode() != http.StatusOK || resp.JSON200 == nil {
		return jobdb.ListJobsResponse{}, responseError("list jobs", resp.StatusCode(), resp.Body, nil)
	}
	out := jobdb.ListJobsResponse{
		NextPageToken: stringValue(resp.JSON200.NextPageToken),
	}
	for _, job := range resp.JSON200.Jobs {
		converted, err := jobSummaryFromAPI(job)
		if err != nil {
			return jobdb.ListJobsResponse{}, err
		}
		out.Jobs = append(out.Jobs, converted)
	}
	return out, nil
}

func (r *Runtime) RegisterJobSchema(ctx context.Context, req jobdb.RegisterJobSchemaRequest) (jobdb.JobSchemaInfo, error) {
	if req.TenantId == "" {
		return jobdb.JobSchemaInfo{}, fmt.Errorf("tenantId is required")
	}
	body := runtimeapi.RegisterJobSchemaRequest{
		Schema: runtimeapi.JobSchemaDocument(cloneRawMessage(req.Schema)),
	}
	resp, err := r.client.RegisterJobSchemaWithResponse(ctx, req.TenantId, body)
	if err != nil {
		return jobdb.JobSchemaInfo{}, err
	}
	if resp.StatusCode() != http.StatusOK || resp.JSON200 == nil {
		return jobdb.JobSchemaInfo{}, responseError("register job schema", resp.StatusCode(), resp.Body, jobdb.ErrConflict)
	}
	return jobSchemaInfoFromAPI(*resp.JSON200), nil
}

func (r *Runtime) GetJobSchema(ctx context.Context, key jobdb.JobSchemaKey) (jobdb.JobSchemaInfo, error) {
	if err := key.Validate(); err != nil {
		return jobdb.JobSchemaInfo{}, err
	}
	resp, err := r.client.GetJobSchemaWithResponse(ctx, key.TenantId, runtimeapi.JobSchemaHash(key.SchemaHash))
	if err != nil {
		return jobdb.JobSchemaInfo{}, err
	}
	if resp.StatusCode() != http.StatusOK || resp.JSON200 == nil {
		return jobdb.JobSchemaInfo{}, responseError("get job schema", resp.StatusCode(), resp.Body, jobdb.ErrJobSchemaNotFound)
	}
	return jobSchemaInfoFromAPI(*resp.JSON200), nil
}

func (r *Runtime) ListJobSchemas(ctx context.Context, req jobdb.ListJobSchemasRequest) (jobdb.ListJobSchemasResponse, error) {
	if req.TenantId == "" {
		return jobdb.ListJobSchemasResponse{}, fmt.Errorf("tenantId is required")
	}
	params := &runtimeapi.ListJobSchemasParams{}
	if req.State != "" {
		state := runtimeapi.JobSchemaListState(req.State)
		params.State = &state
	}
	resp, err := r.client.ListJobSchemasWithResponse(ctx, req.TenantId, params)
	if err != nil {
		return jobdb.ListJobSchemasResponse{}, err
	}
	if resp.StatusCode() != http.StatusOK || resp.JSON200 == nil {
		return jobdb.ListJobSchemasResponse{}, responseError("list job schemas", resp.StatusCode(), resp.Body, nil)
	}
	out := jobdb.ListJobSchemasResponse{
		Schemas: make([]jobdb.JobSchemaInfo, 0, len(resp.JSON200.Schemas)),
	}
	for _, schema := range resp.JSON200.Schemas {
		out.Schemas = append(out.Schemas, jobSchemaInfoFromAPI(schema))
	}
	return out, nil
}

func (r *Runtime) ArchiveJobSchema(ctx context.Context, key jobdb.JobSchemaKey) (jobdb.JobSchemaInfo, error) {
	if err := key.Validate(); err != nil {
		return jobdb.JobSchemaInfo{}, err
	}
	resp, err := r.client.ArchiveJobSchemaWithResponse(ctx, key.TenantId, runtimeapi.JobSchemaHash(key.SchemaHash))
	if err != nil {
		return jobdb.JobSchemaInfo{}, err
	}
	if resp.StatusCode() != http.StatusOK || resp.JSON200 == nil {
		return jobdb.JobSchemaInfo{}, responseError("archive job schema", resp.StatusCode(), resp.Body, jobdb.ErrJobSchemaNotFound)
	}
	return jobSchemaInfoFromAPI(*resp.JSON200), nil
}

func (r *Runtime) UpsertSchedule(ctx context.Context, req jobdb.UpsertScheduleRequest) (jobdb.ScheduleInfo, error) {
	target, err := scheduleTargetToAPI(ctx, req.Target)
	if err != nil {
		return jobdb.ScheduleInfo{}, err
	}
	body := runtimeapi.UpsertScheduleRequest{
		ExpectedGeneration: req.ExpectedGeneration,
		Paused:             boolPtr(req.Paused),
		RequestTime:        timePtr(req.RequestTime),
		Target:             target,
		Trigger:            scheduleTriggerToAPI(req.Trigger),
		WorkerId:           stringPtrOrNil(req.WorkerID),
	}
	if req.OverlapPolicy != "" {
		policy := runtimeapi.ScheduleOverlapPolicy(req.OverlapPolicy)
		body.OverlapPolicy = &policy
	}
	if req.FailurePolicy != (jobdb.ScheduleFailurePolicy{}) {
		policy := scheduleFailurePolicyToAPI(req.FailurePolicy)
		body.FailurePolicy = &policy
	}
	resp, err := r.client.UpsertScheduleWithResponse(ctx, req.TenantId, req.ScheduleId, body)
	if err != nil {
		return jobdb.ScheduleInfo{}, err
	}
	if resp.StatusCode() != http.StatusOK || resp.JSON200 == nil {
		return jobdb.ScheduleInfo{}, responseErrorWithConflict("upsert schedule", resp.StatusCode(), resp.Body, jobdb.ErrJobNotFound, jobdb.ErrConflict)
	}
	return scheduleInfoFromAPI(*resp.JSON200)
}

func (r *Runtime) GetSchedule(ctx context.Context, key jobdb.ScheduleKey) (jobdb.ScheduleInfo, error) {
	resp, err := r.client.GetScheduleWithResponse(ctx, key.TenantId, key.ScheduleId)
	if err != nil {
		return jobdb.ScheduleInfo{}, err
	}
	if resp.StatusCode() != http.StatusOK || resp.JSON200 == nil {
		return jobdb.ScheduleInfo{}, responseError("get schedule", resp.StatusCode(), resp.Body, jobdb.ErrJobNotFound)
	}
	return scheduleInfoFromAPI(*resp.JSON200)
}

func (r *Runtime) ListSchedules(ctx context.Context, req jobdb.ListSchedulesRequest) (jobdb.ListSchedulesResponse, error) {
	if req.TenantId == "" {
		return jobdb.ListSchedulesResponse{}, fmt.Errorf("tenantId is required")
	}
	body := runtimeapi.ListSchedulesRequest{
		PageSize:  intPtr(req.PageSize),
		PageToken: stringPtrOrNil(req.PageToken),
	}
	if len(req.ScheduleIds) > 0 {
		values := append([]string(nil), req.ScheduleIds...)
		body.ScheduleIds = &values
	}
	if len(req.States) > 0 {
		values := make([]runtimeapi.ScheduleState, 0, len(req.States))
		for _, state := range req.States {
			values = append(values, runtimeapi.ScheduleState(state))
		}
		body.States = &values
	}
	if len(req.TargetJobTypes) > 0 {
		values := append([]string(nil), req.TargetJobTypes...)
		body.TargetJobTypes = &values
	}
	resp, err := r.client.ListSchedulesWithResponse(ctx, req.TenantId, body)
	if err != nil {
		return jobdb.ListSchedulesResponse{}, err
	}
	if resp.StatusCode() != http.StatusOK || resp.JSON200 == nil {
		return jobdb.ListSchedulesResponse{}, responseError("list schedules", resp.StatusCode(), resp.Body, nil)
	}
	out := jobdb.ListSchedulesResponse{
		NextPageToken: stringValue(resp.JSON200.NextPageToken),
	}
	for _, schedule := range resp.JSON200.Schedules {
		converted, err := scheduleInfoFromAPI(schedule)
		if err != nil {
			return jobdb.ListSchedulesResponse{}, err
		}
		out.Schedules = append(out.Schedules, converted)
	}
	return out, nil
}

func (r *Runtime) PauseSchedule(ctx context.Context, req jobdb.ScheduleMutationRequest) (jobdb.ScheduleInfo, error) {
	body := runtimeapi.ScheduleMutationRequest{
		ExpectedGeneration: req.ExpectedGeneration,
		RequestTime:        timePtr(req.RequestTime),
		WorkerId:           stringPtrOrNil(req.WorkerID),
	}
	resp, err := r.client.PauseScheduleWithResponse(ctx, req.ScheduleKey.TenantId, req.ScheduleKey.ScheduleId, body)
	if err != nil {
		return jobdb.ScheduleInfo{}, err
	}
	if resp.StatusCode() != http.StatusOK || resp.JSON200 == nil {
		return jobdb.ScheduleInfo{}, responseErrorWithConflict("pause schedule", resp.StatusCode(), resp.Body, jobdb.ErrJobNotFound, jobdb.ErrConflict)
	}
	return scheduleInfoFromAPI(*resp.JSON200)
}

func (r *Runtime) ResumeSchedule(ctx context.Context, req jobdb.ScheduleMutationRequest) (jobdb.ScheduleInfo, error) {
	body := runtimeapi.ScheduleMutationRequest{
		ExpectedGeneration: req.ExpectedGeneration,
		RequestTime:        timePtr(req.RequestTime),
		WorkerId:           stringPtrOrNil(req.WorkerID),
	}
	resp, err := r.client.ResumeScheduleWithResponse(ctx, req.ScheduleKey.TenantId, req.ScheduleKey.ScheduleId, body)
	if err != nil {
		return jobdb.ScheduleInfo{}, err
	}
	if resp.StatusCode() != http.StatusOK || resp.JSON200 == nil {
		return jobdb.ScheduleInfo{}, responseErrorWithConflict("resume schedule", resp.StatusCode(), resp.Body, jobdb.ErrJobNotFound, jobdb.ErrConflict)
	}
	return scheduleInfoFromAPI(*resp.JSON200)
}

func (r *Runtime) ArchiveSchedule(ctx context.Context, req jobdb.ScheduleMutationRequest) (jobdb.ScheduleInfo, error) {
	body := runtimeapi.ScheduleMutationRequest{
		ExpectedGeneration: req.ExpectedGeneration,
		RequestTime:        timePtr(req.RequestTime),
		WorkerId:           stringPtrOrNil(req.WorkerID),
	}
	resp, err := r.client.ArchiveScheduleWithResponse(ctx, req.ScheduleKey.TenantId, req.ScheduleKey.ScheduleId, body)
	if err != nil {
		return jobdb.ScheduleInfo{}, err
	}
	if resp.StatusCode() != http.StatusOK || resp.JSON200 == nil {
		return jobdb.ScheduleInfo{}, responseErrorWithConflict("archive schedule", resp.StatusCode(), resp.Body, jobdb.ErrJobNotFound, jobdb.ErrConflict)
	}
	return scheduleInfoFromAPI(*resp.JSON200)
}

func (r *Runtime) TriggerSchedule(ctx context.Context, req jobdb.TriggerScheduleRequest) (jobdb.JobHandle, error) {
	body := runtimeapi.TriggerScheduleRequest{
		RequestId:   stringPtrOrNil(req.RequestID),
		RequestTime: timePtr(req.RequestTime),
		WorkerId:    stringPtrOrNil(req.WorkerID),
	}
	resp, err := r.client.TriggerScheduleWithResponse(ctx, req.ScheduleKey.TenantId, req.ScheduleKey.ScheduleId, body)
	if err != nil {
		return jobdb.JobHandle{}, err
	}
	if resp.StatusCode() != http.StatusOK || resp.JSON200 == nil {
		return jobdb.JobHandle{}, responseErrorWithConflict("trigger schedule", resp.StatusCode(), resp.Body, jobdb.ErrJobNotFound, jobdb.ErrConflict)
	}
	return fromAPIJobHandle(*resp.JSON200), nil
}

func (r *Runtime) ListScheduleRuns(ctx context.Context, req jobdb.ListScheduleRunsRequest) (jobdb.ListScheduleRunsResponse, error) {
	body := runtimeapi.ListScheduleRunsRequest{
		PageSize:        intPtr(req.PageSize),
		PageToken:       stringPtrOrNil(req.PageToken),
		ScheduledAfter:  cloneTime(req.ScheduledAfter),
		ScheduledBefore: cloneTime(req.ScheduledBefore),
	}
	if len(req.Statuses) > 0 {
		statuses := make([]runtimeapi.JobStatus, 0, len(req.Statuses))
		for _, status := range req.Statuses {
			statuses = append(statuses, runtimeapi.JobStatus(status))
		}
		body.Statuses = &statuses
	}
	resp, err := r.client.ListScheduleRunsWithResponse(ctx, req.ScheduleKey.TenantId, req.ScheduleKey.ScheduleId, body)
	if err != nil {
		return jobdb.ListScheduleRunsResponse{}, err
	}
	if resp.StatusCode() != http.StatusOK || resp.JSON200 == nil {
		return jobdb.ListScheduleRunsResponse{}, responseError("list schedule runs", resp.StatusCode(), resp.Body, jobdb.ErrJobNotFound)
	}
	out := jobdb.ListScheduleRunsResponse{
		NextPageToken: stringValue(resp.JSON200.NextPageToken),
	}
	for _, run := range resp.JSON200.Runs {
		converted, err := scheduleRunSummaryFromAPI(run)
		if err != nil {
			return jobdb.ListScheduleRunsResponse{}, err
		}
		out.Runs = append(out.Runs, converted)
	}
	return out, nil
}

func (r *Runtime) GetChapter(ctx context.Context, ref jobdb.ChapterRef) (jobdb.Chapter, error) {
	resp, err := r.client.GetChapterWithResponse(ctx, ref.JobKey.TenantId, ref.JobKey.JobId, ref.Ordinal)
	if err != nil {
		return jobdb.Chapter{}, err
	}
	if resp.StatusCode() != http.StatusOK || resp.JSON200 == nil {
		return jobdb.Chapter{}, responseError("get chapter", resp.StatusCode(), resp.Body, jobdb.ErrChapterNotFound)
	}
	return fromAPIStoredChapter(*resp.JSON200)
}

func (r *Runtime) ListChapters(ctx context.Context, req jobdb.ListChaptersRequest) ([]jobdb.Chapter, error) {
	params := &runtimeapi.ListChaptersParams{
		StartOrdinal: req.StartOrdinal,
	}
	if req.EndOrdinal != nil {
		end := runtimeapi.EndOrdinal(*req.EndOrdinal)
		params.EndOrdinal = &end
	}
	resp, err := r.client.ListChaptersWithResponse(ctx, req.JobKey.TenantId, req.JobKey.JobId, params)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode() != http.StatusOK || resp.JSON200 == nil {
		return nil, responseError("list chapters", resp.StatusCode(), resp.Body, jobdb.ErrChapterNotFound)
	}
	out := make([]jobdb.Chapter, 0, len(resp.JSON200.Chapters))
	for _, chapter := range resp.JSON200.Chapters {
		converted, err := fromAPIStoredChapter(chapter)
		if err != nil {
			return nil, err
		}
		out = append(out, converted)
	}
	return out, nil
}

func (r *Runtime) PutChapter(ctx context.Context, req jobdb.PutChapterRequest) error {
	if req.LeaseID == "" {
		return fmt.Errorf("leaseId is required")
	}
	if req.LeaseToken == "" {
		return fmt.Errorf("leaseToken is required")
	}
	body, err := runtimeChapterToAddRequest(ctx, req.Chapter, req.ArtifactUploads)
	if err != nil {
		return err
	}
	resp, err := r.client.AddChapterWithLeaseWithResponse(
		ctx,
		req.Ref.JobKey.TenantId,
		req.Ref.JobKey.JobId,
		req.LeaseID,
		&runtimeapi.AddChapterWithLeaseParams{XJobDBLeaseToken: req.LeaseToken},
		body,
	)
	if err != nil {
		return err
	}
	if resp.StatusCode() == http.StatusNoContent {
		return nil
	}
	return responseErrorWithConflict("put chapter", resp.StatusCode(), resp.Body, jobdb.ErrJobNotFound, jobdb.ErrConflict)
}

func (r *Runtime) OpenArtifact(ctx context.Context, ref jobdb.ArtifactRef) (jobdb.ArtifactReader, error) {
	resp, err := r.raw.OpenArtifact(ctx, ref.JobKey.TenantId, ref.JobKey.JobId, ref.Ordinal, ref.Name)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, responseError("open artifact", resp.StatusCode, body, nil)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	return &remoteArtifactReader{
		name: ref.Name,
		size: int64(len(body)),
		body: body,
	}, nil
}

type remoteExecutionLease struct {
	runtime     *Runtime
	leaseID     string
	jobKey      jobdb.JobKey
	capability  string
	schemaHash  string
	payloadJSON json.RawMessage
	mu          sync.RWMutex
	leaseToken  string
}

func (l *remoteExecutionLease) LeaseID() string      { return l.leaseID }
func (l *remoteExecutionLease) Job() jobdb.JobHandle { return jobdb.JobHandle{JobKey: l.jobKey} }
func (l *remoteExecutionLease) Capability() string   { return l.capability }
func (l *remoteExecutionLease) LeaseSchemaHash() string {
	return l.schemaHash
}
func (l *remoteExecutionLease) StopKeepAlive() {}
func (l *remoteExecutionLease) LeaseToken() string {
	l.mu.RLock()
	defer l.mu.RUnlock()
	return l.leaseToken
}
func (l *remoteExecutionLease) Payload() json.RawMessage {
	return append(json.RawMessage(nil), l.payloadJSON...)
}
func (l *remoteExecutionLease) KeepAlive(ctx context.Context) error {
	resp, err := l.runtime.client.KeepAliveLeaseWithResponse(
		ctx,
		l.jobKey.TenantId,
		l.jobKey.JobId,
		l.leaseID,
		&runtimeapi.KeepAliveLeaseParams{XJobDBLeaseToken: l.LeaseToken()},
	)
	if err != nil {
		return err
	}
	if resp.StatusCode() == http.StatusOK && resp.JSON200 != nil {
		l.mu.Lock()
		l.leaseToken = resp.JSON200.LeaseToken
		l.mu.Unlock()
		return nil
	}
	return responseError("keep lease alive", resp.StatusCode(), resp.Body, jobdb.ErrExecutionLeaseLost)
}

func (l *remoteExecutionLease) Complete(ctx context.Context, req jobdb.CompleteExecutionRequest) error {
	if req.Chapter == nil {
		return fmt.Errorf("complete lease requires final chapter")
	}
	chapterWrite, err := runtimeChapterToAddRequest(ctx, *req.Chapter, req.ArtifactUploads)
	if err != nil {
		return err
	}
	body := runtimeapi.CompleteExecutionRequest{
		Status:          req.Status,
		Detail:          stringPtrOrNil(req.Detail),
		Chapter:         chapterWrite.Chapter,
		ArtifactUploads: chapterWrite.ArtifactUploads,
	}
	resp, err := l.runtime.client.CompleteJobWithLeaseWithResponse(
		ctx,
		l.jobKey.TenantId,
		l.jobKey.JobId,
		l.leaseID,
		&runtimeapi.CompleteJobWithLeaseParams{XJobDBLeaseToken: l.LeaseToken()},
		body,
	)
	if err != nil {
		return err
	}
	if resp.StatusCode() == http.StatusNoContent {
		return nil
	}
	return responseError("complete job with lease", resp.StatusCode(), resp.Body, jobdb.ErrExecutionLeaseLost)
}

func optionalArtifactWrites(items []runtimeapi.ArtifactWrite) *[]runtimeapi.ArtifactWrite {
	if len(items) == 0 {
		return nil
	}
	out := append([]runtimeapi.ArtifactWrite(nil), items...)
	return &out
}

func (l *remoteExecutionLease) Reschedule(ctx context.Context, req jobdb.RescheduleExecutionRequest) error {
	var payload *runtimeapi.SchedulerPayload
	var err error
	if len(req.Payload) > 0 {
		payload, err = schedulerPayloadOptionalToAPI(req.Payload)
		if err != nil {
			return err
		}
	}
	body := runtimeapi.RescheduleExecutionRequest{
		AlternateAfter: toAPIStdDurationValue(req.AlternateAfter),
		AlternateNeed:  stringPtrOrNil(req.AlternateNeed),
		NextNeed:       stringPtrOrNil(req.NextNeed),
		Payload:        payload,
		WaitUntil:      req.WaitUntil,
	}
	if len(req.WaitForJobIDs) > 0 {
		waitFor := append([]string(nil), req.WaitForJobIDs...)
		body.WaitForJobIds = &waitFor
	}
	resp, err := l.runtime.client.RescheduleJobWithLeaseWithResponse(
		ctx,
		l.jobKey.TenantId,
		l.jobKey.JobId,
		l.leaseID,
		&runtimeapi.RescheduleJobWithLeaseParams{XJobDBLeaseToken: l.LeaseToken()},
		body,
	)
	if err != nil {
		return err
	}
	if resp.StatusCode() == http.StatusNoContent {
		return nil
	}
	return responseError("reschedule job with lease", resp.StatusCode(), resp.Body, jobdb.ErrExecutionLeaseLost)
}

func (r *Runtime) executionLeaseFromAPI(lease runtimeapi.ExecutionLease) (jobdb.ExecutionLease, error) {
	payload, err := schedulerPayloadFromAPI(lease.Payload)
	if err != nil {
		return nil, err
	}
	return &remoteExecutionLease{
		runtime:     r,
		leaseID:     lease.LeaseId,
		jobKey:      fromAPIJobKey(lease.Job.JobKey),
		capability:  lease.Capability,
		schemaHash:  stringValue(lease.SchemaHash),
		payloadJSON: payload,
		leaseToken:  lease.LeaseToken,
	}, nil
}

type remoteArtifactReader struct {
	name string
	size int64
	body []byte
}

func (r *remoteArtifactReader) Open() (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(append([]byte(nil), r.body...))), nil
}

func (r *remoteArtifactReader) Size() int64 { return r.size }

func (r *remoteArtifactReader) Name() string { return r.name }

func toAPIPrerequisites(prereqs []jobdb.JobPrerequisite) *[]runtimeapi.JobPrerequisite {
	if len(prereqs) == 0 {
		return nil
	}
	out := make([]runtimeapi.JobPrerequisite, 0, len(prereqs))
	for _, prereq := range prereqs {
		out = append(out, runtimeapi.JobPrerequisite{
			Condition: runtimeapi.JobPrerequisiteCondition(prereq.Condition),
			JobId:     prereq.JobID,
		})
	}
	return &out
}

func responseError(operation string, status int, body []byte, sentinel error) error {
	message := strings.TrimSpace(string(body))
	if message == "" {
		message = http.StatusText(status)
	}
	switch status {
	case http.StatusNotFound:
		if sentinel != nil {
			return fmt.Errorf("%w: %s", sentinel, message)
		}
	case http.StatusConflict:
		if sentinel != nil {
			return fmt.Errorf("%w: %s", sentinel, message)
		}
	case http.StatusBadRequest:
		if strings.Contains(message, jobdb.ErrJobSchemaValidation.Error()) {
			return fmt.Errorf("%w: %s", jobdb.ErrJobSchemaValidation, message)
		}
		return fmt.Errorf("%s: %s", operation, message)
	}
	if sentinel != nil && (status == http.StatusNotFound || status == http.StatusConflict) {
		return fmt.Errorf("%w: %s", sentinel, message)
	}
	return fmt.Errorf("%s: http %d: %s", operation, status, message)
}

func responseErrorWithConflict(operation string, status int, body []byte, notFoundSentinel error, conflictSentinel error) error {
	message := strings.TrimSpace(string(body))
	if message == "" {
		message = http.StatusText(status)
	}
	switch status {
	case http.StatusBadRequest:
		if strings.Contains(message, jobdb.ErrJobSchemaValidation.Error()) {
			return fmt.Errorf("%w: %s", jobdb.ErrJobSchemaValidation, message)
		}
		return fmt.Errorf("%s: %s", operation, message)
	case http.StatusNotFound:
		if notFoundSentinel != nil {
			return fmt.Errorf("%w: %s", notFoundSentinel, message)
		}
	case http.StatusConflict:
		if conflictSentinel != nil {
			return fmt.Errorf("%w: %s", conflictSentinel, message)
		}
	}
	return fmt.Errorf("%s: http %d: %s", operation, status, message)
}

func explicitJobCreateError(operation string, status int, body []byte, conflict *runtimeapi.ErrorResponse) error {
	message := strings.TrimSpace(string(body))
	if conflict != nil && conflict.Message != "" {
		message = conflict.Message
	}
	if message == "" {
		message = http.StatusText(status)
	}
	switch status {
	case http.StatusBadRequest:
		if strings.Contains(message, jobdb.ErrJobSchemaValidation.Error()) {
			return fmt.Errorf("%w: %s", jobdb.ErrJobSchemaValidation, message)
		}
		return fmt.Errorf("%s: %s", operation, message)
	case http.StatusConflict:
		if conflict != nil && conflict.Code == runtimeapi.ExistingJobMismatch {
			return jobdb.NewExistingJobMismatchError(message)
		}
		return fmt.Errorf("%w: %s", jobdb.ErrConflict, message)
	default:
		return fmt.Errorf("%s: http %d: %s", operation, status, message)
	}
}

func predicatesFilter(predicates []jobdb.MetadataPredicate) jobdb.MetadataFilter {
	if len(predicates) == 0 {
		return nil
	}
	filter := jobdb.Metadata()
	for _, predicate := range predicates {
		if len(predicate.Path) == 0 || len(predicate.Values) == 0 {
			continue
		}
		var clause jobdb.MetadataFilter
		for idx, value := range predicate.Values {
			eq, err := jobdb.Metadata().EqualFilter(jobdb.FieldName(predicate.Path[0]), value)
			if err != nil {
				continue
			}
			if idx == 0 {
				clause = eq
				continue
			}
			next, err := clause.OrFilter(eq)
			if err != nil {
				continue
			}
			clause = next
		}
		if clause == nil {
			continue
		}
		next, err := filter.AndFilter(clause)
		if err != nil {
			continue
		}
		filter = next
	}
	return filter
}

func stringValue(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

func int64Ptr(value int64) *int64 {
	return &value
}

func intPtr(value int) *int {
	if value == 0 {
		return nil
	}
	return &value
}

func timePtr(value time.Time) *time.Time {
	if value.IsZero() {
		return nil
	}
	v := value.UTC()
	return &v
}
