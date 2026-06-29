package remote

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/leaseauth"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/runtimeapi"
)

type leaseOperationRuntime interface {
	KeepAliveLeaseByID(ctx context.Context, jobKey jobdb.JobKey, leaseID string, workerID string, leaseDuration time.Duration) error
	CompleteJobWithLeaseByID(ctx context.Context, jobKey jobdb.JobKey, leaseID string, workerID string, req jobdb.CompleteExecutionRequest) error
	RescheduleJobWithLeaseByID(ctx context.Context, jobKey jobdb.JobKey, leaseID string, workerID string, req jobdb.RescheduleExecutionRequest) error
	SubmitJobWithLeaseByID(ctx context.Context, parentJobKey jobdb.JobKey, leaseID string, workerID string, req jobdb.SubmitJobRequest) (jobdb.JobHandle, error)
	SubmitRestartJobWithLeaseByID(ctx context.Context, parentJobKey jobdb.JobKey, leaseID string, workerID string, req jobdb.SubmitRestartJobRequest) (jobdb.JobHandle, error)
}

type leaseRenewalRuntime interface {
	KeepAliveLeaseByIDWithExpiry(ctx context.Context, jobKey jobdb.JobKey, leaseID string, workerID string, leaseDuration time.Duration) (time.Time, error)
}

type proxyServer struct {
	runtime        jobdb.WorkflowRuntime
	leaseOps       leaseOperationRuntime
	schemaRegistry jobdb.JobSchemaRegistry
	tokens         *leaseTokenSigner
}

func NewServer(runtime jobdb.WorkflowRuntime) http.Handler {
	server := &proxyServer{
		runtime:        runtime,
		leaseOps:       runtimeLeaseOps(runtime),
		schemaRegistry: runtimeSchemaRegistry(runtime),
		tokens:         newLeaseTokenSigner(),
	}
	strict := runtimeapi.NewStrictHandlerWithOptions(server, nil, runtimeapi.StrictHTTPServerOptions{
		RequestErrorHandlerFunc: func(w http.ResponseWriter, _ *http.Request, err error) {
			http.Error(w, err.Error(), http.StatusBadRequest)
		},
		ResponseErrorHandlerFunc: func(w http.ResponseWriter, _ *http.Request, err error) {
			if errors.Is(err, jobdb.ErrExistingJobMismatch) {
				writeAPIError(w, http.StatusConflict, runtimeapi.ErrorResponse{
					Code:    runtimeapi.ExistingJobMismatch,
					Message: err.Error(),
				})
				return
			}
			status := http.StatusInternalServerError
			var statusErr *httpStatusError
			if errors.As(err, &statusErr) {
				status = statusErr.status
			} else {
				switch {
				case errors.Is(err, jobdb.ErrJobNotFound), errors.Is(err, jobdb.ErrChapterNotFound):
					status = http.StatusNotFound
				case errors.Is(err, jobdb.ErrJobSchemaNotFound):
					status = http.StatusNotFound
				case errors.Is(err, jobdb.ErrExecutionLeaseLost), errors.Is(err, jobdb.ErrConflict):
					status = http.StatusConflict
				case errors.Is(err, jobdb.ErrJobSchemaArchived):
					status = http.StatusConflict
				case errors.Is(err, jobdb.ErrJobSchemaValidation):
					status = http.StatusBadRequest
				}
			}
			http.Error(w, err.Error(), status)
		},
	})
	router := chi.NewRouter()
	handler := runtimeapi.HandlerFromMux(strict, router)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method == http.MethodPost && r.URL.Path == "/v1/jobs/poll" {
			if err := rejectLegacyPollWorkFields(r); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
		}
		handler.ServeHTTP(w, r)
	})
}

func rejectLegacyPollWorkFields(r *http.Request) error {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		return err
	}
	r.Body = io.NopCloser(bytes.NewReader(body))

	fields := make(map[string]json.RawMessage)
	if err := json.Unmarshal(body, &fields); err != nil {
		return nil
	}
	if _, ok := fields["tenantIds"]; ok {
		return fmt.Errorf("tenantIds is not supported; use tenantId")
	}
	return nil
}

func (s *proxyServer) PollWork(ctx context.Context, request runtimeapi.PollWorkRequestObject) (runtimeapi.PollWorkResponseObject, error) {
	if request.Body == nil {
		return nil, badRequest("poll work body is required")
	}
	req := jobdb.PollWorkRequest{
		TenantId:      request.Body.TenantId,
		WorkerID:      request.Body.WorkerId,
		Capabilities:  append([]string(nil), request.Body.Capabilities...),
		Limit:         request.Body.Limit,
		LongPollUntil: request.Body.LongPollUntil,
	}
	if req.TenantId == "" {
		return nil, badRequest("tenantId is required")
	}
	var err error
	req.LeaseDuration, err = fromAPIDurationPointer(request.Body.LeaseDuration)
	if err != nil {
		return nil, badRequest(err.Error())
	}
	req.MetadataEquals, err = metadataPredicatesFromAPI(request.Body.MetadataEquals)
	if err != nil {
		return nil, badRequest(err.Error())
	}
	leases, err := s.runtime.PollWork(ctx, req)
	if err != nil {
		return nil, err
	}
	out := make([]runtimeapi.ExecutionLease, 0, len(leases))
	for _, lease := range leases {
		model, err := s.toAPIExecutionLease(lease, req.LeaseDuration)
		if err != nil {
			return nil, err
		}
		out = append(out, model)
	}
	return runtimeapi.PollWork200JSONResponse{Leases: out}, nil
}

func (s *proxyServer) SubmitJob(ctx context.Context, request runtimeapi.SubmitJobRequestObject) (runtimeapi.SubmitJobResponseObject, error) {
	if request.Body == nil {
		return nil, badRequest("submit job body is required")
	}
	req, err := submitJobRequestFromAPI(*request.Body, request.TenantId, "")
	if err != nil {
		return nil, badRequest(err.Error())
	}
	handle, err := s.runtime.SubmitJob(ctx, req)
	if err != nil {
		return nil, err
	}
	return runtimeapi.SubmitJob200JSONResponse(toAPIJobHandle(handle)), nil
}

func (s *proxyServer) PutJob(ctx context.Context, request runtimeapi.PutJobRequestObject) (runtimeapi.PutJobResponseObject, error) {
	if request.Body == nil {
		return nil, badRequest("submit job body is required")
	}
	req, err := submitJobRequestFromAPI(*request.Body, request.TenantId, request.JobId)
	if err != nil {
		return nil, badRequest(err.Error())
	}
	handle, err := s.runtime.SubmitJob(ctx, req)
	if err != nil {
		return nil, err
	}
	return runtimeapi.PutJob200JSONResponse(toAPIJobHandle(handle)), nil
}

func submitJobRequestFromAPI(body runtimeapi.SubmitJobRequest, tenantID string, jobID string) (jobdb.SubmitJobRequest, error) {
	data, err := taskDataFromAPIWrite(body.Job.Data)
	if err != nil {
		return jobdb.SubmitJobRequest{}, err
	}
	metadata, err := metadataAPIToJSON(body.Job.Metadata)
	if err != nil {
		return jobdb.SubmitJobRequest{}, err
	}
	runPolicy, err := runPolicyFromAPI(body.Job.RunPolicy)
	if err != nil {
		return jobdb.SubmitJobRequest{}, err
	}
	return jobdb.SubmitJobRequest{
		Job: jobdb.SubmitJob{
			AvailableAt:   cloneTime(body.Job.AvailableAt),
			TenantId:      tenantID,
			JobID:         jobID,
			JobType:       body.Job.JobType,
			Data:          jobdb.JobData(data),
			RunPolicy:     runPolicy,
			Metadata:      metadata,
			Prerequisites: fromAPIPrerequisites(body.Job.Prerequisites),
			Schema:        jobSchemaSelectorFromAPI(body.Job.Schema),
		},
		RequestTime: derefTime(body.RequestTime),
		WorkerID:    stringValue(body.WorkerId),
	}, nil
}

func (s *proxyServer) ListJobs(ctx context.Context, request runtimeapi.ListJobsRequestObject) (runtimeapi.ListJobsResponseObject, error) {
	if request.Body == nil {
		return nil, badRequest("list jobs body is required")
	}
	metadataFilter, err := metadataFilterFromAPI(request.Body.MetadataPredicates)
	if err != nil {
		return nil, badRequest(err.Error())
	}
	req := jobdb.ListJobsRequest{
		TenantIds:      []string{request.TenantId},
		MetadataFilter: metadataFilter,
		CreatedAfter:   request.Body.CreatedAfter,
		CreatedBefore:  request.Body.CreatedBefore,
		PageSize:       derefInt(request.Body.PageSize),
		PageToken:      stringValue(request.Body.PageToken),
	}
	if request.Body.ParentJobIds != nil {
		req.ParentJobIDs = append(req.ParentJobIDs, (*request.Body.ParentJobIds)...)
	}
	if request.Body.RootOnly != nil {
		req.RootOnly = *request.Body.RootOnly
	}
	if request.Body.JobKeys != nil {
		for _, jobKey := range *request.Body.JobKeys {
			req.JobKeys = append(req.JobKeys, fromAPIJobKey(jobKey))
		}
	}
	if request.Body.JobTasks != nil {
		for _, jobTask := range *request.Body.JobTasks {
			req.JobTasks = append(req.JobTasks, jobdb.JobTaskFilter{
				JobType:  jobTask.JobType,
				TaskType: jobTask.TaskType,
			})
		}
	}
	if request.Body.JobTypes != nil {
		req.JobTypes = append(req.JobTypes, (*request.Body.JobTypes)...)
	}
	if request.Body.Statuses != nil {
		for _, status := range *request.Body.Statuses {
			req.Statuses = append(req.Statuses, jobdb.JobStatus(status))
		}
	}
	if request.Body.Stores != nil {
		for _, store := range *request.Body.Stores {
			req.Stores = append(req.Stores, jobdb.JobStore(store))
		}
	}
	resp, err := s.runtime.ListJobs(ctx, req)
	if err != nil {
		return nil, err
	}
	jobs := make([]runtimeapi.JobSummary, 0, len(resp.Jobs))
	for _, job := range resp.Jobs {
		converted, err := jobSummaryToAPI(job)
		if err != nil {
			return nil, err
		}
		jobs = append(jobs, converted)
	}
	out := runtimeapi.ListJobsResponse{
		Jobs: jobs,
	}
	if resp.NextPageToken != "" {
		out.NextPageToken = stringPtr(resp.NextPageToken)
	}
	return runtimeapi.ListJobs200JSONResponse(out), nil
}

func (s *proxyServer) RegisterJobSchema(ctx context.Context, request runtimeapi.RegisterJobSchemaRequestObject) (runtimeapi.RegisterJobSchemaResponseObject, error) {
	if request.Body == nil {
		return nil, badRequest("register job schema body is required")
	}
	registry, err := s.requireSchemaRegistry()
	if err != nil {
		return nil, err
	}
	info, err := registry.RegisterJobSchema(ctx, jobdb.RegisterJobSchemaRequest{
		TenantId: request.TenantId,
		Schema:   cloneRawMessage(request.Body.Schema),
	})
	if errors.Is(err, jobdb.ErrConflict) {
		return runtimeapi.RegisterJobSchema409JSONResponse{
			Code:    runtimeapi.Conflict,
			Message: err.Error(),
		}, nil
	}
	if err != nil {
		return nil, err
	}
	return runtimeapi.RegisterJobSchema200JSONResponse(jobSchemaInfoToAPI(info)), nil
}

func (s *proxyServer) GetJobSchema(ctx context.Context, request runtimeapi.GetJobSchemaRequestObject) (runtimeapi.GetJobSchemaResponseObject, error) {
	registry, err := s.requireSchemaRegistry()
	if err != nil {
		return nil, err
	}
	info, err := registry.GetJobSchema(ctx, jobdb.JobSchemaKey{
		TenantId:   request.TenantId,
		SchemaHash: request.SchemaHash,
	})
	if err != nil {
		return nil, err
	}
	return runtimeapi.GetJobSchema200JSONResponse(jobSchemaInfoToAPI(info)), nil
}

func (s *proxyServer) ListJobSchemas(ctx context.Context, request runtimeapi.ListJobSchemasRequestObject) (runtimeapi.ListJobSchemasResponseObject, error) {
	registry, err := s.requireSchemaRegistry()
	if err != nil {
		return nil, err
	}
	req := jobdb.ListJobSchemasRequest{TenantId: request.TenantId}
	if request.Params.State != nil {
		req.State = jobdb.JobSchemaListState(*request.Params.State)
	}
	resp, err := registry.ListJobSchemas(ctx, req)
	if err != nil {
		return nil, err
	}
	out := runtimeapi.ListJobSchemasResponse{
		Schemas: make([]runtimeapi.JobSchemaInfo, 0, len(resp.Schemas)),
	}
	for _, schema := range resp.Schemas {
		out.Schemas = append(out.Schemas, jobSchemaInfoToAPI(schema))
	}
	return runtimeapi.ListJobSchemas200JSONResponse(out), nil
}

func (s *proxyServer) ArchiveJobSchema(ctx context.Context, request runtimeapi.ArchiveJobSchemaRequestObject) (runtimeapi.ArchiveJobSchemaResponseObject, error) {
	registry, err := s.requireSchemaRegistry()
	if err != nil {
		return nil, err
	}
	info, err := registry.ArchiveJobSchema(ctx, jobdb.JobSchemaKey{
		TenantId:   request.TenantId,
		SchemaHash: request.SchemaHash,
	})
	if err != nil {
		return nil, err
	}
	return runtimeapi.ArchiveJobSchema200JSONResponse(jobSchemaInfoToAPI(info)), nil
}

func (s *proxyServer) UpsertSchedule(ctx context.Context, request runtimeapi.UpsertScheduleRequestObject) (runtimeapi.UpsertScheduleResponseObject, error) {
	if request.Body == nil {
		return nil, badRequest("upsert schedule body is required")
	}
	trigger, err := scheduleTriggerFromAPI(request.Body.Trigger)
	if err != nil {
		return nil, badRequest(err.Error())
	}
	target, err := scheduleTargetFromAPI(request.Body.Target)
	if err != nil {
		return nil, badRequest(err.Error())
	}
	var overlap jobdb.ScheduleOverlapPolicy
	if request.Body.OverlapPolicy != nil {
		overlap = jobdb.ScheduleOverlapPolicy(*request.Body.OverlapPolicy)
	}
	info, err := s.runtime.UpsertSchedule(ctx, jobdb.UpsertScheduleRequest{
		TenantId:           request.TenantId,
		ScheduleId:         request.ScheduleId,
		Trigger:            trigger,
		Target:             target,
		OverlapPolicy:      overlap,
		FailurePolicy:      scheduleFailurePolicyFromAPI(request.Body.FailurePolicy),
		Paused:             boolValue(request.Body.Paused),
		ExpectedGeneration: request.Body.ExpectedGeneration,
		RequestTime:        derefTime(request.Body.RequestTime),
		WorkerID:           stringValue(request.Body.WorkerId),
	})
	if err != nil {
		return nil, err
	}
	converted, err := scheduleInfoToAPI(ctx, info)
	if err != nil {
		return nil, err
	}
	return runtimeapi.UpsertSchedule200JSONResponse(converted), nil
}

func (s *proxyServer) GetSchedule(ctx context.Context, request runtimeapi.GetScheduleRequestObject) (runtimeapi.GetScheduleResponseObject, error) {
	info, err := s.runtime.GetSchedule(ctx, jobdb.ScheduleKey{TenantId: request.TenantId, ScheduleId: request.ScheduleId})
	if err != nil {
		return nil, err
	}
	converted, err := scheduleInfoToAPI(ctx, info)
	if err != nil {
		return nil, err
	}
	return runtimeapi.GetSchedule200JSONResponse(converted), nil
}

func (s *proxyServer) ListSchedules(ctx context.Context, request runtimeapi.ListSchedulesRequestObject) (runtimeapi.ListSchedulesResponseObject, error) {
	if request.Body == nil {
		return nil, badRequest("list schedules body is required")
	}
	req := jobdb.ListSchedulesRequest{
		TenantId:       request.TenantId,
		PageSize:       derefInt(request.Body.PageSize),
		PageToken:      stringValue(request.Body.PageToken),
		ScheduleIds:    cloneStringSlice(request.Body.ScheduleIds),
		TargetJobTypes: cloneStringSlice(request.Body.TargetJobTypes),
	}
	if request.Body.States != nil {
		for _, state := range *request.Body.States {
			req.States = append(req.States, jobdb.ScheduleState(state))
		}
	}
	resp, err := s.runtime.ListSchedules(ctx, req)
	if err != nil {
		return nil, err
	}
	schedules := make([]runtimeapi.ScheduleInfo, 0, len(resp.Schedules))
	for _, schedule := range resp.Schedules {
		converted, err := scheduleInfoToAPI(ctx, schedule)
		if err != nil {
			return nil, err
		}
		schedules = append(schedules, converted)
	}
	out := runtimeapi.ListSchedulesResponse{Schedules: schedules}
	if resp.NextPageToken != "" {
		out.NextPageToken = stringPtr(resp.NextPageToken)
	}
	return runtimeapi.ListSchedules200JSONResponse(out), nil
}

func (s *proxyServer) PauseSchedule(ctx context.Context, request runtimeapi.PauseScheduleRequestObject) (runtimeapi.PauseScheduleResponseObject, error) {
	info, err := s.runtime.PauseSchedule(ctx, scheduleMutationRequestFromAPI(request.TenantId, request.ScheduleId, request.Body))
	if err != nil {
		return nil, err
	}
	converted, err := scheduleInfoToAPI(ctx, info)
	if err != nil {
		return nil, err
	}
	return runtimeapi.PauseSchedule200JSONResponse(converted), nil
}

func (s *proxyServer) ResumeSchedule(ctx context.Context, request runtimeapi.ResumeScheduleRequestObject) (runtimeapi.ResumeScheduleResponseObject, error) {
	info, err := s.runtime.ResumeSchedule(ctx, scheduleMutationRequestFromAPI(request.TenantId, request.ScheduleId, request.Body))
	if err != nil {
		return nil, err
	}
	converted, err := scheduleInfoToAPI(ctx, info)
	if err != nil {
		return nil, err
	}
	return runtimeapi.ResumeSchedule200JSONResponse(converted), nil
}

func (s *proxyServer) ArchiveSchedule(ctx context.Context, request runtimeapi.ArchiveScheduleRequestObject) (runtimeapi.ArchiveScheduleResponseObject, error) {
	info, err := s.runtime.ArchiveSchedule(ctx, scheduleMutationRequestFromAPI(request.TenantId, request.ScheduleId, request.Body))
	if err != nil {
		return nil, err
	}
	converted, err := scheduleInfoToAPI(ctx, info)
	if err != nil {
		return nil, err
	}
	return runtimeapi.ArchiveSchedule200JSONResponse(converted), nil
}

func (s *proxyServer) TriggerSchedule(ctx context.Context, request runtimeapi.TriggerScheduleRequestObject) (runtimeapi.TriggerScheduleResponseObject, error) {
	req := jobdb.TriggerScheduleRequest{
		ScheduleKey: jobdb.ScheduleKey{TenantId: request.TenantId, ScheduleId: request.ScheduleId},
	}
	if request.Body != nil {
		req.RequestID = stringValue(request.Body.RequestId)
		req.RequestTime = derefTime(request.Body.RequestTime)
		req.WorkerID = stringValue(request.Body.WorkerId)
	}
	handle, err := s.runtime.TriggerSchedule(ctx, req)
	if err != nil {
		return nil, err
	}
	return runtimeapi.TriggerSchedule200JSONResponse(toAPIJobHandle(handle)), nil
}

func (s *proxyServer) ListScheduleRuns(ctx context.Context, request runtimeapi.ListScheduleRunsRequestObject) (runtimeapi.ListScheduleRunsResponseObject, error) {
	if request.Body == nil {
		return nil, badRequest("list schedule runs body is required")
	}
	req := jobdb.ListScheduleRunsRequest{
		ScheduleKey:     jobdb.ScheduleKey{TenantId: request.TenantId, ScheduleId: request.ScheduleId},
		ScheduledAfter:  cloneTime(request.Body.ScheduledAfter),
		ScheduledBefore: cloneTime(request.Body.ScheduledBefore),
		PageSize:        derefInt(request.Body.PageSize),
		PageToken:       stringValue(request.Body.PageToken),
	}
	if request.Body.Statuses != nil {
		for _, status := range *request.Body.Statuses {
			req.Statuses = append(req.Statuses, jobdb.JobStatus(status))
		}
	}
	resp, err := s.runtime.ListScheduleRuns(ctx, req)
	if err != nil {
		return nil, err
	}
	runs := make([]runtimeapi.ScheduleRunSummary, 0, len(resp.Runs))
	for _, run := range resp.Runs {
		converted, err := scheduleRunSummaryToAPI(run)
		if err != nil {
			return nil, err
		}
		runs = append(runs, converted)
	}
	out := runtimeapi.ListScheduleRunsResponse{Runs: runs}
	if resp.NextPageToken != "" {
		out.NextPageToken = stringPtr(resp.NextPageToken)
	}
	return runtimeapi.ListScheduleRuns200JSONResponse(out), nil
}

func (s *proxyServer) SubmitRestartJob(ctx context.Context, request runtimeapi.SubmitRestartJobRequestObject) (runtimeapi.SubmitRestartJobResponseObject, error) {
	if request.Body == nil {
		return nil, badRequest("submit restart job body is required")
	}
	req, err := submitRestartJobRequestFromAPI(*request.Body, request.TenantId, "")
	if err != nil {
		return nil, badRequest(err.Error())
	}
	handle, err := s.runtime.SubmitRestartJob(ctx, req)
	if err != nil {
		return nil, err
	}
	return runtimeapi.SubmitRestartJob200JSONResponse(toAPIJobHandle(handle)), nil
}

func (s *proxyServer) PutRestartJob(ctx context.Context, request runtimeapi.PutRestartJobRequestObject) (runtimeapi.PutRestartJobResponseObject, error) {
	if request.Body == nil {
		return nil, badRequest("submit restart job body is required")
	}
	req, err := submitRestartJobRequestFromAPI(*request.Body, request.TenantId, request.JobId)
	if err != nil {
		return nil, badRequest(err.Error())
	}
	handle, err := s.runtime.SubmitRestartJob(ctx, req)
	if err != nil {
		return nil, err
	}
	return runtimeapi.PutRestartJob200JSONResponse(toAPIJobHandle(handle)), nil
}

func submitRestartJobRequestFromAPI(body runtimeapi.SubmitRestartJobRequest, tenantID string, jobID string) (jobdb.SubmitRestartJobRequest, error) {
	job := jobdb.SubmitRestartJob{
		PriorJobKey:    fromAPIJobKey(body.Job.PriorJobKey),
		LastStepToKeep: body.Job.LastStepToKeep,
		JobID:          jobID,
		Prerequisites:  fromAPIPrerequisites(body.Job.Prerequisites),
		Schema:         jobSchemaSelectorFromAPI(body.Job.Schema),
	}
	if job.PriorJobKey.TenantId != "" && job.PriorJobKey.TenantId != tenantID {
		return jobdb.SubmitRestartJobRequest{}, fmt.Errorf("priorJobKey tenantId must match path tenantId")
	}
	job.PriorJobKey.TenantId = tenantID
	if body.Job.ExtraTaskInput != nil {
		data, err := taskDataFromAPIWrite(*body.Job.ExtraTaskInput)
		if err != nil {
			return jobdb.SubmitRestartJobRequest{}, err
		}
		job.ExtraTaskInput = data
	}
	if body.Job.ExtraTaskOutput != nil {
		data, err := taskDataFromAPIWrite(*body.Job.ExtraTaskOutput)
		if err != nil {
			return jobdb.SubmitRestartJobRequest{}, err
		}
		job.ExtraTaskOutput = data
	}
	return jobdb.SubmitRestartJobRequest{
		Job:         job,
		RequestTime: derefTime(body.RequestTime),
		WorkerID:    stringValue(body.WorkerId),
	}, nil
}

func (s *proxyServer) GetJob(ctx context.Context, request runtimeapi.GetJobRequestObject) (runtimeapi.GetJobResponseObject, error) {
	jobKey := jobdb.JobKey{TenantId: request.TenantId, JobId: request.JobId}
	job, err := s.runtime.GetJob(ctx, jobKey)
	if err != nil {
		return nil, err
	}
	converted, err := jobInfoToAPI(ctx, job)
	if err != nil {
		return nil, err
	}
	return runtimeapi.GetJob200JSONResponse(converted), nil
}

func (s *proxyServer) CancelJob(ctx context.Context, request runtimeapi.CancelJobRequestObject) (runtimeapi.CancelJobResponseObject, error) {
	if request.Body == nil {
		return nil, badRequest("cancel job body is required")
	}
	err := s.runtime.CancelJob(ctx, jobdb.CancelJobRequest{
		JobKey: jobdb.JobKey{
			TenantId: request.TenantId,
			JobId:    request.JobId,
		},
		Reason:   stringValue(request.Body.Reason),
		WorkerID: stringValue(request.Body.WorkerId),
	})
	if err != nil {
		return nil, err
	}
	return runtimeapi.CancelJob204Response{}, nil
}

func (s *proxyServer) ListChapters(ctx context.Context, request runtimeapi.ListChaptersRequestObject) (runtimeapi.ListChaptersResponseObject, error) {
	jobKey := jobdb.JobKey{TenantId: request.TenantId, JobId: request.JobId}
	var endOrdinal *int64
	if request.Params.EndOrdinal != nil {
		value := int64(*request.Params.EndOrdinal)
		endOrdinal = &value
	}
	chapters, err := s.runtime.ListChapters(ctx, jobdb.ListChaptersRequest{
		JobKey:       jobKey,
		StartOrdinal: request.Params.StartOrdinal,
		EndOrdinal:   endOrdinal,
	})
	if err != nil {
		return nil, err
	}
	out := make([]runtimeapi.ChapterRecord, 0, len(chapters))
	for _, chapter := range chapters {
		converted, err := toAPIStoredChapter(ctx, chapter)
		if err != nil {
			return nil, err
		}
		out = append(out, converted)
	}
	return runtimeapi.ListChapters200JSONResponse(runtimeapi.ListChaptersResponse{Chapters: out}), nil
}

func (s *proxyServer) GetChapter(ctx context.Context, request runtimeapi.GetChapterRequestObject) (runtimeapi.GetChapterResponseObject, error) {
	chapter, err := s.runtime.GetChapter(ctx, jobdb.ChapterRef{
		JobKey: jobdb.JobKey{
			TenantId: request.TenantId,
			JobId:    request.JobId,
		},
		Ordinal: request.Ordinal,
	})
	if err != nil {
		return nil, err
	}
	converted, err := toAPIStoredChapter(ctx, chapter)
	if err != nil {
		return nil, err
	}
	return runtimeapi.GetChapter200JSONResponse(converted), nil
}

func (s *proxyServer) OpenArtifact(ctx context.Context, request runtimeapi.OpenArtifactRequestObject) (runtimeapi.OpenArtifactResponseObject, error) {
	reader, err := s.runtime.OpenArtifact(ctx, jobdb.ArtifactRef{
		JobKey: jobdb.JobKey{
			TenantId: request.TenantId,
			JobId:    request.JobId,
		},
		Ordinal: request.Ordinal,
		Name:    request.Name,
	})
	if err != nil {
		return nil, err
	}
	rc, err := reader.Open()
	if err != nil {
		return nil, err
	}
	return runtimeapi.OpenArtifact200ApplicationoctetStreamResponse{
		Body:          rc,
		ContentLength: reader.Size(),
	}, nil
}

func (s *proxyServer) CommitChapterIfWaiting(ctx context.Context, request runtimeapi.CommitChapterIfWaitingRequestObject) (runtimeapi.CommitChapterIfWaitingResponseObject, error) {
	if request.Body == nil {
		return nil, badRequest("commit chapter body is required")
	}
	if request.Body.OutputOrdinal != nil && *request.Body.OutputOrdinal != request.Ordinal {
		return nil, badRequest("outputOrdinal must match the path ordinal")
	}
	data, err := taskDataFromAPIWrite(request.Body.Data)
	if err != nil {
		return nil, badRequest(err.Error())
	}
	err = s.runtime.CompleteTaskIfWaiting(ctx, jobdb.CompleteTaskIfWaitingRequest{
		JobKey: jobdb.JobKey{
			TenantId: request.TenantId,
			JobId:    request.JobId,
		},
		Capability:    stringValue(request.Body.Capability),
		ResumeNeed:    stringValue(request.Body.ResumeNeed),
		InputOrdinal:  derefInt64(request.Body.InputOrdinal),
		OutputOrdinal: request.Ordinal,
		InputHash:     stringValue(request.Body.InputHash),
		Data:          data,
	})
	if err != nil {
		return nil, err
	}
	return runtimeapi.CommitChapterIfWaiting204Response{}, nil
}

func (s *proxyServer) GetJobLease(ctx context.Context, request runtimeapi.GetJobLeaseRequestObject) (runtimeapi.GetJobLeaseResponseObject, error) {
	if request.Body == nil {
		return nil, badRequest("get job lease body is required")
	}
	leaseDuration, err := fromAPIDurationPointer(request.Body.LeaseDuration)
	if err != nil {
		return nil, badRequest(err.Error())
	}
	lease, err := s.runtime.GetJobLease(ctx, jobdb.GetJobLeaseRequest{
		JobKey: jobdb.JobKey{
			TenantId: request.TenantId,
			JobId:    request.JobId,
		},
		WorkerID:      stringValue(request.Body.WorkerId),
		Capabilities:  append([]string(nil), request.Body.Capabilities...),
		LeaseDuration: leaseDuration,
	})
	if err != nil {
		return nil, err
	}
	out := runtimeapi.GetJobLeaseResponse{}
	if lease != nil {
		converted, err := s.toAPIExecutionLease(lease, leaseDuration)
		if err != nil {
			return nil, err
		}
		out.Lease = &converted
	}
	return runtimeapi.GetJobLease200JSONResponse(out), nil
}

func (s *proxyServer) AddChapterWithLease(ctx context.Context, request runtimeapi.AddChapterWithLeaseRequestObject) (runtimeapi.AddChapterWithLeaseResponseObject, error) {
	if request.Body == nil {
		return nil, badRequest("add chapter body is required")
	}
	jobKey := jobdb.JobKey{TenantId: request.TenantId, JobId: request.JobId}
	claims, err := s.validatedLeaseClaims(request.Params.XJobDBLeaseToken, jobKey, request.LeaseId)
	if err != nil {
		return nil, err
	}
	chapter, uploads, err := writableChapterToRuntimeChapter(request.Body.Chapter, optionalRequestArtifactWrites(request.Body.ArtifactUploads))
	if err != nil {
		return nil, badRequest(err.Error())
	}
	if chapter.Ordinal < 0 {
		return nil, badRequest("chapter ordinal must be >= 0")
	}
	ctx = leaseauth.WithClaims(ctx, claims.leaseAuthClaims())
	err = s.runtime.PutChapter(ctx, jobdb.PutChapterRequest{
		LeaseID:    request.LeaseId,
		LeaseToken: request.Params.XJobDBLeaseToken,
		Ref: jobdb.ChapterRef{
			JobKey:  jobKey,
			Ordinal: chapter.Ordinal,
		},
		Chapter:         chapter,
		ArtifactUploads: uploads,
	})
	if err != nil {
		return nil, err
	}
	return runtimeapi.AddChapterWithLease204Response{}, nil
}

func (s *proxyServer) CompleteJobWithLease(ctx context.Context, request runtimeapi.CompleteJobWithLeaseRequestObject) (runtimeapi.CompleteJobWithLeaseResponseObject, error) {
	if request.Body == nil {
		return nil, badRequest("complete job body is required")
	}
	jobKey := jobdb.JobKey{TenantId: request.TenantId, JobId: request.JobId}
	claims, err := s.validatedLeaseClaims(request.Params.XJobDBLeaseToken, jobKey, request.LeaseId)
	if err != nil {
		return nil, err
	}
	ops, err := s.requireLeaseOps()
	if err != nil {
		return nil, err
	}
	chapter, uploads, err := writableChapterToRuntimeChapter(request.Body.Chapter, optionalRequestArtifactWrites(request.Body.ArtifactUploads))
	if err != nil {
		return nil, badRequest(err.Error())
	}
	err = ops.CompleteJobWithLeaseByID(ctx, jobKey, request.LeaseId, claims.WorkerID, jobdb.CompleteExecutionRequest{
		Status:          request.Body.Status,
		Detail:          stringValue(request.Body.Detail),
		Chapter:         &chapter,
		ArtifactUploads: uploads,
	})
	if err != nil {
		return nil, err
	}
	return runtimeapi.CompleteJobWithLease204Response{}, nil
}

func optionalRequestArtifactWrites(items *[]runtimeapi.ArtifactWrite) []runtimeapi.ArtifactWrite {
	if items == nil {
		return nil
	}
	return *items
}

func (s *proxyServer) KeepAliveLease(ctx context.Context, request runtimeapi.KeepAliveLeaseRequestObject) (runtimeapi.KeepAliveLeaseResponseObject, error) {
	jobKey := jobdb.JobKey{TenantId: request.TenantId, JobId: request.JobId}
	claims, err := s.validatedLeaseClaims(request.Params.XJobDBLeaseToken, jobKey, request.LeaseId)
	if err != nil {
		return nil, err
	}
	ops, err := s.requireLeaseOps()
	if err != nil {
		return nil, err
	}
	leaseDuration := claims.leaseDuration()
	leaseExpiresAt := time.Now().UTC().Add(leaseDuration)
	if renewal, ok := ops.(leaseRenewalRuntime); ok {
		var err error
		leaseExpiresAt, err = renewal.KeepAliveLeaseByIDWithExpiry(ctx, jobKey, request.LeaseId, claims.WorkerID, leaseDuration)
		if err != nil {
			return nil, err
		}
	} else {
		if err := ops.KeepAliveLeaseByID(ctx, jobKey, request.LeaseId, claims.WorkerID, leaseDuration); err != nil {
			return nil, err
		}
	}
	token, err := s.tokens.mintForLeaseExpiry(jobKey, request.LeaseId, claims.WorkerID, claims.SchemaHash, leaseExpiresAt, leaseDuration)
	if err != nil {
		return nil, err
	}
	return runtimeapi.KeepAliveLease200JSONResponse{
		LeaseToken: token,
	}, nil
}

func (s *proxyServer) RescheduleJobWithLease(ctx context.Context, request runtimeapi.RescheduleJobWithLeaseRequestObject) (runtimeapi.RescheduleJobWithLeaseResponseObject, error) {
	if request.Body == nil {
		return nil, badRequest("reschedule job body is required")
	}
	payload, err := schedulerPayloadPointerFromAPI(request.Body.Payload)
	if err != nil {
		return nil, badRequest(err.Error())
	}
	alternateAfter, err := fromAPIStdDurationValue(request.Body.AlternateAfter)
	if err != nil {
		return nil, badRequest(err.Error())
	}
	jobKey := jobdb.JobKey{TenantId: request.TenantId, JobId: request.JobId}
	claims, err := s.validatedLeaseClaims(request.Params.XJobDBLeaseToken, jobKey, request.LeaseId)
	if err != nil {
		return nil, err
	}
	ops, err := s.requireLeaseOps()
	if err != nil {
		return nil, err
	}
	err = ops.RescheduleJobWithLeaseByID(ctx, jobKey, request.LeaseId, claims.WorkerID, jobdb.RescheduleExecutionRequest{
		AlternateAfter: alternateAfter,
		AlternateNeed:  stringValue(request.Body.AlternateNeed),
		NextNeed:       stringValue(request.Body.NextNeed),
		Payload:        payload,
		WaitUntil:      request.Body.WaitUntil,
		WaitForJobIDs:  cloneStringSlice(request.Body.WaitForJobIds),
	})
	if err != nil {
		return nil, err
	}
	return runtimeapi.RescheduleJobWithLease204Response{}, nil
}

func (s *proxyServer) SubmitJobWithLease(ctx context.Context, request runtimeapi.SubmitJobWithLeaseRequestObject) (runtimeapi.SubmitJobWithLeaseResponseObject, error) {
	if request.Body == nil {
		return nil, badRequest("submit job body is required")
	}
	jobKey := jobdb.JobKey{TenantId: request.TenantId, JobId: request.ParentJobId}
	claims, err := s.validatedLeaseClaims(request.Params.XJobDBLeaseToken, jobKey, request.LeaseId)
	if err != nil {
		return nil, err
	}
	ops, err := s.requireLeaseOps()
	if err != nil {
		return nil, err
	}
	req, err := submitJobRequestFromAPI(*request.Body, request.TenantId, "")
	if err != nil {
		return nil, badRequest(err.Error())
	}
	ctx = leaseauth.WithClaims(ctx, claims.leaseAuthClaims())
	handle, err := ops.SubmitJobWithLeaseByID(ctx, jobKey, request.LeaseId, claims.WorkerID, req)
	if err != nil {
		return nil, err
	}
	return runtimeapi.SubmitJobWithLease200JSONResponse(toAPIJobHandle(handle)), nil
}

func (s *proxyServer) PutJobWithLease(ctx context.Context, request runtimeapi.PutJobWithLeaseRequestObject) (runtimeapi.PutJobWithLeaseResponseObject, error) {
	if request.Body == nil {
		return nil, badRequest("submit job body is required")
	}
	jobKey := jobdb.JobKey{TenantId: request.TenantId, JobId: request.ParentJobId}
	claims, err := s.validatedLeaseClaims(request.Params.XJobDBLeaseToken, jobKey, request.LeaseId)
	if err != nil {
		return nil, err
	}
	ops, err := s.requireLeaseOps()
	if err != nil {
		return nil, err
	}
	req, err := submitJobRequestFromAPI(*request.Body, request.TenantId, request.ChildJobId)
	if err != nil {
		return nil, badRequest(err.Error())
	}
	ctx = leaseauth.WithClaims(ctx, claims.leaseAuthClaims())
	handle, err := ops.SubmitJobWithLeaseByID(ctx, jobKey, request.LeaseId, claims.WorkerID, req)
	if err != nil {
		return nil, err
	}
	return runtimeapi.PutJobWithLease200JSONResponse(toAPIJobHandle(handle)), nil
}

func (s *proxyServer) SubmitRestartJobWithLease(ctx context.Context, request runtimeapi.SubmitRestartJobWithLeaseRequestObject) (runtimeapi.SubmitRestartJobWithLeaseResponseObject, error) {
	if request.Body == nil {
		return nil, badRequest("submit restart job body is required")
	}
	jobKey := jobdb.JobKey{TenantId: request.TenantId, JobId: request.ParentJobId}
	claims, err := s.validatedLeaseClaims(request.Params.XJobDBLeaseToken, jobKey, request.LeaseId)
	if err != nil {
		return nil, err
	}
	ops, err := s.requireLeaseOps()
	if err != nil {
		return nil, err
	}
	req, err := submitRestartJobRequestFromAPI(*request.Body, request.TenantId, "")
	if err != nil {
		return nil, badRequest(err.Error())
	}
	ctx = leaseauth.WithClaims(ctx, claims.leaseAuthClaims())
	handle, err := ops.SubmitRestartJobWithLeaseByID(ctx, jobKey, request.LeaseId, claims.WorkerID, req)
	if err != nil {
		return nil, err
	}
	return runtimeapi.SubmitRestartJobWithLease200JSONResponse(toAPIJobHandle(handle)), nil
}

func (s *proxyServer) PutRestartJobWithLease(ctx context.Context, request runtimeapi.PutRestartJobWithLeaseRequestObject) (runtimeapi.PutRestartJobWithLeaseResponseObject, error) {
	if request.Body == nil {
		return nil, badRequest("submit restart job body is required")
	}
	jobKey := jobdb.JobKey{TenantId: request.TenantId, JobId: request.ParentJobId}
	claims, err := s.validatedLeaseClaims(request.Params.XJobDBLeaseToken, jobKey, request.LeaseId)
	if err != nil {
		return nil, err
	}
	ops, err := s.requireLeaseOps()
	if err != nil {
		return nil, err
	}
	req, err := submitRestartJobRequestFromAPI(*request.Body, request.TenantId, request.ChildJobId)
	if err != nil {
		return nil, badRequest(err.Error())
	}
	ctx = leaseauth.WithClaims(ctx, claims.leaseAuthClaims())
	handle, err := ops.SubmitRestartJobWithLeaseByID(ctx, jobKey, request.LeaseId, claims.WorkerID, req)
	if err != nil {
		return nil, err
	}
	return runtimeapi.PutRestartJobWithLease200JSONResponse(toAPIJobHandle(handle)), nil
}

func (s *proxyServer) toAPIExecutionLease(lease jobdb.ExecutionLease, requestedDuration time.Duration) (runtimeapi.ExecutionLease, error) {
	payload, err := schedulerPayloadToAPI(lease.Payload())
	if err != nil {
		return runtimeapi.ExecutionLease{}, err
	}
	token, err := s.tokens.mintForLease(lease, leaseTokenTTL(requestedDuration))
	if err != nil {
		return runtimeapi.ExecutionLease{}, err
	}
	return runtimeapi.ExecutionLease{
		Capability: lease.Capability(),
		Job:        toAPIJobHandle(lease.Job()),
		LeaseId:    lease.LeaseID(),
		LeaseToken: token,
		Payload:    payload,
		SchemaHash: schemaHashPtr(leaseSchemaHash(lease)),
	}, nil
}

func metadataPredicatesFromAPI(predicates *[]runtimeapi.MetadataPredicate) ([]jobdb.MetadataPredicate, error) {
	if predicates == nil {
		return nil, nil
	}
	out := make([]jobdb.MetadataPredicate, 0, len(*predicates))
	for _, predicate := range *predicates {
		values := make([]any, 0, len(predicate.Values))
		for _, value := range predicate.Values {
			converted, err := metadataAPIValueToAny(value)
			if err != nil {
				return nil, err
			}
			values = append(values, converted)
		}
		out = append(out, jobdb.MetadataPredicate{
			Path:   append([]string(nil), predicate.Path...),
			Values: values,
		})
	}
	return out, nil
}

func fromAPIPrerequisites(prereqs *[]runtimeapi.JobPrerequisite) []jobdb.JobPrerequisite {
	if prereqs == nil {
		return nil
	}
	out := make([]jobdb.JobPrerequisite, 0, len(*prereqs))
	for _, prereq := range *prereqs {
		out = append(out, jobdb.JobPrerequisite{
			Condition: jobdb.JobPrereqCondition(prereq.Condition),
			JobID:     prereq.JobId,
		})
	}
	return out
}

func scheduleMutationRequestFromAPI(tenantID string, scheduleID string, body *runtimeapi.ScheduleMutationRequest) jobdb.ScheduleMutationRequest {
	req := jobdb.ScheduleMutationRequest{
		ScheduleKey: jobdb.ScheduleKey{TenantId: tenantID, ScheduleId: scheduleID},
	}
	if body == nil {
		return req
	}
	req.ExpectedGeneration = body.ExpectedGeneration
	req.RequestTime = derefTime(body.RequestTime)
	req.WorkerID = stringValue(body.WorkerId)
	return req
}

type httpStatusError struct {
	status int
	err    error
}

func (e *httpStatusError) Error() string {
	if e.err == nil {
		return http.StatusText(e.status)
	}
	return e.err.Error()
}

func (e *httpStatusError) Unwrap() error { return e.err }

func badRequest(message string) error {
	return &httpStatusError{
		status: http.StatusBadRequest,
		err:    errors.New(message),
	}
}

func runtimeLeaseOps(runtime jobdb.WorkflowRuntime) leaseOperationRuntime {
	if runtime == nil {
		return nil
	}
	ops, _ := runtime.(leaseOperationRuntime)
	return ops
}

func runtimeSchemaRegistry(runtime jobdb.WorkflowRuntime) jobdb.JobSchemaRegistry {
	if runtime == nil {
		return nil
	}
	registry, _ := runtime.(jobdb.JobSchemaRegistry)
	return registry
}

func (s *proxyServer) requireLeaseOps() (leaseOperationRuntime, error) {
	if s == nil || s.leaseOps == nil {
		return nil, errors.New("runtime does not support tokenized lease operations")
	}
	return s.leaseOps, nil
}

func (s *proxyServer) requireSchemaRegistry() (jobdb.JobSchemaRegistry, error) {
	if s == nil || s.schemaRegistry == nil {
		return nil, &httpStatusError{
			status: http.StatusNotImplemented,
			err:    errors.New("runtime does not support job schema registry operations"),
		}
	}
	return s.schemaRegistry, nil
}

func (s *proxyServer) validatedLeaseClaims(token string, jobKey jobdb.JobKey, leaseID string) (leaseTokenClaims, error) {
	claims, err := s.tokens.validateAndParse(token, jobKey, leaseID, time.Now().UTC())
	if err != nil {
		return leaseTokenClaims{}, leaseTokenValidationError(err)
	}
	return claims, nil
}

func writeAPIError(w http.ResponseWriter, status int, payload runtimeapi.ErrorResponse) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(payload)
}

func derefInt(value *int) int {
	if value == nil {
		return 0
	}
	return *value
}

func derefInt64(value *int64) int64 {
	if value == nil {
		return 0
	}
	return *value
}

func derefTime(value *time.Time) time.Time {
	if value == nil {
		return time.Time{}
	}
	return value.UTC()
}

func cloneStringSlice(values *[]string) []string {
	if values == nil {
		return nil
	}
	return append([]string(nil), (*values)...)
}
