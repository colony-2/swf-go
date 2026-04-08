package remote

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"

	"github.com/colony-2/swf-go/pkg/swf"
	"github.com/colony-2/swf-go/pkg/swf/internal/runtimeapi"
)

type leaseOperationRuntime interface {
	KeepAliveLeaseByID(ctx context.Context, jobKey swf.JobKey, leaseID string, workerID string, leaseDuration time.Duration) error
	CompleteJobWithLeaseByID(ctx context.Context, jobKey swf.JobKey, leaseID string, workerID string, req swf.CompleteExecutionRequest) error
	RescheduleJobWithLeaseByID(ctx context.Context, jobKey swf.JobKey, leaseID string, workerID string, req swf.RescheduleExecutionRequest) error
}

type proxyServer struct {
	runtime  swf.WorkflowRuntime
	leaseOps leaseOperationRuntime
	tokens   *leaseTokenSigner
}

func NewServer(runtime swf.WorkflowRuntime) http.Handler {
	server := &proxyServer{
		runtime:  runtime,
		leaseOps: runtimeLeaseOps(runtime),
		tokens:   newLeaseTokenSigner(),
	}
	strict := runtimeapi.NewStrictHandlerWithOptions(server, nil, runtimeapi.StrictHTTPServerOptions{
		RequestErrorHandlerFunc: func(w http.ResponseWriter, _ *http.Request, err error) {
			http.Error(w, err.Error(), http.StatusBadRequest)
		},
		ResponseErrorHandlerFunc: func(w http.ResponseWriter, _ *http.Request, err error) {
			if errors.Is(err, swf.ErrExistingJobMismatch) {
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
				case errors.Is(err, swf.ErrJobNotFound), errors.Is(err, swf.ErrChapterNotFound):
					status = http.StatusNotFound
				case errors.Is(err, swf.ErrExecutionLeaseLost), errors.Is(err, swf.ErrConflict):
					status = http.StatusConflict
				}
			}
			http.Error(w, err.Error(), status)
		},
	})
	router := chi.NewRouter()
	return runtimeapi.HandlerFromMux(strict, router)
}

func (s *proxyServer) PollWork(ctx context.Context, request runtimeapi.PollWorkRequestObject) (runtimeapi.PollWorkResponseObject, error) {
	if request.Body == nil {
		return nil, badRequest("poll work body is required")
	}
	req := swf.PollWorkRequest{
		WorkerID:      request.Body.WorkerId,
		Capabilities:  append([]string(nil), request.Body.Capabilities...),
		Limit:         request.Body.Limit,
		LongPollUntil: request.Body.LongPollUntil,
	}
	if request.Body.TenantIds != nil {
		req.TenantIds = append([]string(nil), (*request.Body.TenantIds)...)
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
	data, err := taskDataFromAPIWrite(request.Body.Job.Data)
	if err != nil {
		return nil, badRequest(err.Error())
	}
	metadata, err := unmarshalJSONValueOptional(request.Body.Job.Metadata)
	if err != nil {
		return nil, badRequest(err.Error())
	}
	runPolicy, err := runPolicyFromAPI(request.Body.Job.RunPolicy)
	if err != nil {
		return nil, badRequest(err.Error())
	}
	handle, err := s.runtime.SubmitJob(ctx, swf.SubmitJobRequest{
		Job: swf.SubmitJob{
			TenantId:      request.TenantId,
			JobType:       request.Body.Job.JobType,
			Data:          swf.JobData(data),
			RunPolicy:     runPolicy,
			Metadata:      metadata,
			Prerequisites: fromAPIPrerequisites(request.Body.Job.Prerequisites),
		},
		RequestTime: derefTime(request.Body.RequestTime),
		WorkerID:    stringValue(request.Body.WorkerId),
	})
	if err != nil {
		return nil, err
	}
	return runtimeapi.SubmitJob200JSONResponse(toAPIJobHandle(handle)), nil
}

func (s *proxyServer) PutJob(ctx context.Context, request runtimeapi.PutJobRequestObject) (runtimeapi.PutJobResponseObject, error) {
	if request.Body == nil {
		return nil, badRequest("submit job body is required")
	}
	data, err := taskDataFromAPIWrite(request.Body.Job.Data)
	if err != nil {
		return nil, badRequest(err.Error())
	}
	metadata, err := unmarshalJSONValueOptional(request.Body.Job.Metadata)
	if err != nil {
		return nil, badRequest(err.Error())
	}
	runPolicy, err := runPolicyFromAPI(request.Body.Job.RunPolicy)
	if err != nil {
		return nil, badRequest(err.Error())
	}
	handle, err := s.runtime.SubmitJob(ctx, swf.SubmitJobRequest{
		Job: swf.SubmitJob{
			TenantId:      request.TenantId,
			JobID:         request.JobId,
			JobType:       request.Body.Job.JobType,
			Data:          swf.JobData(data),
			RunPolicy:     runPolicy,
			Metadata:      metadata,
			Prerequisites: fromAPIPrerequisites(request.Body.Job.Prerequisites),
		},
		RequestTime: derefTime(request.Body.RequestTime),
		WorkerID:    stringValue(request.Body.WorkerId),
	})
	if err != nil {
		return nil, err
	}
	return runtimeapi.PutJob200JSONResponse(toAPIJobHandle(handle)), nil
}

func (s *proxyServer) ListJobs(ctx context.Context, request runtimeapi.ListJobsRequestObject) (runtimeapi.ListJobsResponseObject, error) {
	if request.Body == nil {
		return nil, badRequest("list jobs body is required")
	}
	metadataFilter, err := metadataFilterFromAPI(request.Body.MetadataPredicates)
	if err != nil {
		return nil, badRequest(err.Error())
	}
	req := swf.ListJobsRequest{
		TenantIds:      []string{request.TenantId},
		MetadataFilter: metadataFilter,
		CreatedAfter:   request.Body.CreatedAfter,
		CreatedBefore:  request.Body.CreatedBefore,
		PageSize:       derefInt(request.Body.PageSize),
		PageToken:      stringValue(request.Body.PageToken),
	}
	if request.Body.JobKeys != nil {
		for _, jobKey := range *request.Body.JobKeys {
			req.JobKeys = append(req.JobKeys, fromAPIJobKey(jobKey))
		}
	}
	if request.Body.JobTasks != nil {
		for _, jobTask := range *request.Body.JobTasks {
			req.JobTasks = append(req.JobTasks, swf.JobTaskFilter{
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
			req.Statuses = append(req.Statuses, swf.JobStatus(status))
		}
	}
	if request.Body.Stores != nil {
		for _, store := range *request.Body.Stores {
			req.Stores = append(req.Stores, swf.JobStore(store))
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

func (s *proxyServer) SubmitRestartJob(ctx context.Context, request runtimeapi.SubmitRestartJobRequestObject) (runtimeapi.SubmitRestartJobResponseObject, error) {
	if request.Body == nil {
		return nil, badRequest("submit restart job body is required")
	}
	job := swf.SubmitRestartJob{
		PriorJobKey:    fromAPIJobKey(request.Body.Job.PriorJobKey),
		LastStepToKeep: request.Body.Job.LastStepToKeep,
		Prerequisites:  fromAPIPrerequisites(request.Body.Job.Prerequisites),
	}
	if job.PriorJobKey.TenantId != request.TenantId {
		return nil, badRequest("priorJobKey tenantId must match path tenantId")
	}
	if request.Body.Job.ExtraTaskInput != nil {
		data, err := taskDataFromAPIWrite(*request.Body.Job.ExtraTaskInput)
		if err != nil {
			return nil, badRequest(err.Error())
		}
		job.ExtraTaskInput = data
	}
	if request.Body.Job.ExtraTaskOutput != nil {
		data, err := taskDataFromAPIWrite(*request.Body.Job.ExtraTaskOutput)
		if err != nil {
			return nil, badRequest(err.Error())
		}
		job.ExtraTaskOutput = data
	}
	handle, err := s.runtime.SubmitRestartJob(ctx, swf.SubmitRestartJobRequest{
		Job:         job,
		RequestTime: derefTime(request.Body.RequestTime),
		WorkerID:    stringValue(request.Body.WorkerId),
	})
	if err != nil {
		return nil, err
	}
	return runtimeapi.SubmitRestartJob200JSONResponse(toAPIJobHandle(handle)), nil
}

func (s *proxyServer) PutRestartJob(ctx context.Context, request runtimeapi.PutRestartJobRequestObject) (runtimeapi.PutRestartJobResponseObject, error) {
	if request.Body == nil {
		return nil, badRequest("submit restart job body is required")
	}
	job := swf.SubmitRestartJob{
		PriorJobKey:    fromAPIJobKey(request.Body.Job.PriorJobKey),
		LastStepToKeep: request.Body.Job.LastStepToKeep,
		JobID:          request.JobId,
		Prerequisites:  fromAPIPrerequisites(request.Body.Job.Prerequisites),
	}
	if job.PriorJobKey.TenantId != request.TenantId {
		return nil, badRequest("priorJobKey tenantId must match path tenantId")
	}
	if request.Body.Job.ExtraTaskInput != nil {
		data, err := taskDataFromAPIWrite(*request.Body.Job.ExtraTaskInput)
		if err != nil {
			return nil, badRequest(err.Error())
		}
		job.ExtraTaskInput = data
	}
	if request.Body.Job.ExtraTaskOutput != nil {
		data, err := taskDataFromAPIWrite(*request.Body.Job.ExtraTaskOutput)
		if err != nil {
			return nil, badRequest(err.Error())
		}
		job.ExtraTaskOutput = data
	}
	handle, err := s.runtime.SubmitRestartJob(ctx, swf.SubmitRestartJobRequest{
		Job:         job,
		RequestTime: derefTime(request.Body.RequestTime),
		WorkerID:    stringValue(request.Body.WorkerId),
	})
	if err != nil {
		return nil, err
	}
	return runtimeapi.PutRestartJob200JSONResponse(toAPIJobHandle(handle)), nil
}

func (s *proxyServer) GetJob(ctx context.Context, request runtimeapi.GetJobRequestObject) (runtimeapi.GetJobResponseObject, error) {
	jobKey := swf.JobKey{TenantId: request.TenantId, JobId: request.JobId}
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
	err := s.runtime.CancelJob(ctx, swf.CancelJobRequest{
		JobKey: swf.JobKey{
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
	jobKey := swf.JobKey{TenantId: request.TenantId, JobId: request.JobId}
	var endOrdinal *int64
	if request.Params.EndOrdinal != nil {
		value := int64(*request.Params.EndOrdinal)
		endOrdinal = &value
	}
	chapters, err := s.runtime.ListChapters(ctx, swf.ListChaptersRequest{
		JobKey:       jobKey,
		StartOrdinal: request.Params.StartOrdinal,
		EndOrdinal:   endOrdinal,
	})
	if err != nil {
		return nil, err
	}
	out := make([]runtimeapi.StoredChapter, 0, len(chapters))
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
	chapter, err := s.runtime.GetChapter(ctx, swf.ChapterRef{
		JobKey: swf.JobKey{
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
	reader, err := s.runtime.OpenArtifact(ctx, swf.ArtifactRef{
		JobKey: swf.JobKey{
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
	err = s.runtime.CompleteTaskIfWaiting(ctx, swf.CompleteTaskIfWaitingRequest{
		JobKey: swf.JobKey{
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
	lease, err := s.runtime.GetJobLease(ctx, swf.GetJobLeaseRequest{
		JobKey: swf.JobKey{
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
	jobKey := swf.JobKey{TenantId: request.TenantId, JobId: request.JobId}
	if _, err := s.validatedLeaseClaims(request.Params.XSWFLeaseToken, jobKey, request.LeaseId); err != nil {
		return nil, err
	}
	chapter, uploads, err := writableChapterToRuntimeChapter(request.Body.Chapter)
	if err != nil {
		return nil, badRequest(err.Error())
	}
	if chapter.Ordinal < 0 {
		return nil, badRequest("chapter ordinal must be >= 0")
	}
	err = s.runtime.PutChapter(ctx, swf.PutChapterRequest{
		LeaseID:    request.LeaseId,
		LeaseToken: request.Params.XSWFLeaseToken,
		Ref: swf.ChapterRef{
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
	jobKey := swf.JobKey{TenantId: request.TenantId, JobId: request.JobId}
	claims, err := s.validatedLeaseClaims(request.Params.XSWFLeaseToken, jobKey, request.LeaseId)
	if err != nil {
		return nil, err
	}
	ops, err := s.requireLeaseOps()
	if err != nil {
		return nil, err
	}
	err = ops.CompleteJobWithLeaseByID(ctx, jobKey, request.LeaseId, claims.WorkerID, swf.CompleteExecutionRequest{
		Status: request.Body.Status,
		Detail: stringValue(request.Body.Detail),
	})
	if err != nil {
		return nil, err
	}
	return runtimeapi.CompleteJobWithLease204Response{}, nil
}

func (s *proxyServer) KeepAliveLease(ctx context.Context, request runtimeapi.KeepAliveLeaseRequestObject) (runtimeapi.KeepAliveLeaseResponseObject, error) {
	jobKey := swf.JobKey{TenantId: request.TenantId, JobId: request.JobId}
	claims, err := s.validatedLeaseClaims(request.Params.XSWFLeaseToken, jobKey, request.LeaseId)
	if err != nil {
		return nil, err
	}
	ops, err := s.requireLeaseOps()
	if err != nil {
		return nil, err
	}
	if err := ops.KeepAliveLeaseByID(ctx, jobKey, request.LeaseId, claims.WorkerID, claims.leaseDuration()); err != nil {
		return nil, err
	}
	token, err := s.tokens.mint(jobKey, request.LeaseId, claims.WorkerID, claims.leaseDuration())
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
	payload, err := unmarshalJSONValueOptional(request.Body.Payload)
	if err != nil {
		return nil, badRequest(err.Error())
	}
	alternateAfter, err := fromAPIStdDurationValue(request.Body.AlternateAfter)
	if err != nil {
		return nil, badRequest(err.Error())
	}
	jobKey := swf.JobKey{TenantId: request.TenantId, JobId: request.JobId}
	claims, err := s.validatedLeaseClaims(request.Params.XSWFLeaseToken, jobKey, request.LeaseId)
	if err != nil {
		return nil, err
	}
	ops, err := s.requireLeaseOps()
	if err != nil {
		return nil, err
	}
	err = ops.RescheduleJobWithLeaseByID(ctx, jobKey, request.LeaseId, claims.WorkerID, swf.RescheduleExecutionRequest{
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

func (s *proxyServer) toAPIExecutionLease(lease swf.ExecutionLease, requestedDuration time.Duration) (runtimeapi.ExecutionLease, error) {
	payload, err := marshalJSONValueOptional(lease.Payload())
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
	}, nil
}

func metadataPredicatesFromAPI(predicates *[]runtimeapi.MetadataPredicate) ([]swf.MetadataPredicate, error) {
	if predicates == nil {
		return nil, nil
	}
	out := make([]swf.MetadataPredicate, 0, len(*predicates))
	for _, predicate := range *predicates {
		values := make([]any, len(predicate.Values))
		copy(values, predicate.Values)
		out = append(out, swf.MetadataPredicate{
			Path:   append([]string(nil), predicate.Path...),
			Values: values,
		})
	}
	return out, nil
}

func fromAPIPrerequisites(prereqs *[]runtimeapi.JobPrerequisite) []swf.JobPrerequisite {
	if prereqs == nil {
		return nil
	}
	out := make([]swf.JobPrerequisite, 0, len(*prereqs))
	for _, prereq := range *prereqs {
		out = append(out, swf.JobPrerequisite{
			Condition: swf.JobPrereqCondition(prereq.Condition),
			JobID:     prereq.JobId,
		})
	}
	return out
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

func runtimeLeaseOps(runtime swf.WorkflowRuntime) leaseOperationRuntime {
	if runtime == nil {
		return nil
	}
	ops, _ := runtime.(leaseOperationRuntime)
	return ops
}

func (s *proxyServer) requireLeaseOps() (leaseOperationRuntime, error) {
	if s == nil || s.leaseOps == nil {
		return nil, errors.New("runtime does not support tokenized lease operations")
	}
	return s.leaseOps, nil
}

func (s *proxyServer) validatedLeaseClaims(token string, jobKey swf.JobKey, leaseID string) (leaseTokenClaims, error) {
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
