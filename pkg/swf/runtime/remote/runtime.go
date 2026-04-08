package remote

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/colony-2/swf-go/pkg/swf"
	"github.com/colony-2/swf-go/pkg/swf/internal/runtimeapi"
)

type Runtime struct {
	raw    *runtimeapi.Client
	client *runtimeapi.ClientWithResponses

	mu           sync.Mutex
	knownTenants map[string]struct{}
	pollCursor   int
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
		raw:          raw,
		client:       client,
		knownTenants: make(map[string]struct{}),
	}, nil
}

func (r *Runtime) SubmitJob(ctx context.Context, req swf.SubmitJobRequest) (swf.JobHandle, error) {
	if req.Job.TenantId == "" {
		return swf.JobHandle{}, fmt.Errorf("tenantId is required")
	}
	data, err := taskDataToAPIWrite(ctx, swf.TaskData(req.Job.Data))
	if err != nil {
		return swf.JobHandle{}, err
	}
	metadata, err := marshalJSONValueOptional(req.Job.Metadata)
	if err != nil {
		return swf.JobHandle{}, err
	}
	runPolicy, err := runPolicyToAPI(req.Job.RunPolicy)
	if err != nil {
		return swf.JobHandle{}, err
	}
	body := runtimeapi.SubmitJobRequest{
		Job: runtimeapi.SubmitJob{
			Data:          data,
			JobType:       req.Job.JobType,
			Metadata:      metadata,
			Prerequisites: toAPIPrerequisites(req.Job.Prerequisites),
			RunPolicy:     runPolicy,
		},
		RequestTime: timePtr(req.RequestTime),
		WorkerId:    stringPtrOrNil(req.WorkerID),
	}
	if req.Job.JobID != "" {
		resp, err := r.client.PutJobWithResponse(ctx, req.Job.TenantId, req.Job.JobID, body)
		if err != nil {
			return swf.JobHandle{}, err
		}
		if resp.StatusCode() != http.StatusOK || resp.JSON200 == nil {
			return swf.JobHandle{}, explicitJobCreateError("put job", resp.StatusCode(), resp.Body, resp.JSON409)
		}
		handle := fromAPIJobHandle(*resp.JSON200)
		r.rememberTenant(handle.JobKey.TenantId)
		return handle, nil
	}
	resp, err := r.client.SubmitJobWithResponse(ctx, req.Job.TenantId, body)
	if err != nil {
		return swf.JobHandle{}, err
	}
	if resp.StatusCode() != http.StatusOK || resp.JSON200 == nil {
		return swf.JobHandle{}, responseError("submit job", resp.StatusCode(), resp.Body, nil)
	}
	handle := fromAPIJobHandle(*resp.JSON200)
	r.rememberTenant(handle.JobKey.TenantId)
	return handle, nil
}

func (r *Runtime) SubmitRestartJob(ctx context.Context, req swf.SubmitRestartJobRequest) (swf.JobHandle, error) {
	if req.Job.PriorJobKey.TenantId == "" {
		return swf.JobHandle{}, fmt.Errorf("prior job tenantId is required")
	}
	body := runtimeapi.SubmitRestartJobRequest{
		Job: runtimeapi.SubmitRestartJob{
			LastStepToKeep: req.Job.LastStepToKeep,
			PriorJobKey:    toAPIJobKey(req.Job.PriorJobKey),
			Prerequisites:  toAPIPrerequisites(req.Job.Prerequisites),
		},
		RequestTime: timePtr(req.RequestTime),
		WorkerId:    stringPtrOrNil(req.WorkerID),
	}
	if req.Job.ExtraTaskInput != nil {
		input, err := taskDataToAPIWrite(ctx, req.Job.ExtraTaskInput)
		if err != nil {
			return swf.JobHandle{}, err
		}
		body.Job.ExtraTaskInput = &input
	}
	if req.Job.ExtraTaskOutput != nil {
		output, err := taskDataToAPIWrite(ctx, req.Job.ExtraTaskOutput)
		if err != nil {
			return swf.JobHandle{}, err
		}
		body.Job.ExtraTaskOutput = &output
	}
	if req.Job.JobID != "" {
		resp, err := r.client.PutRestartJobWithResponse(ctx, req.Job.PriorJobKey.TenantId, req.Job.JobID, body)
		if err != nil {
			return swf.JobHandle{}, err
		}
		if resp.StatusCode() != http.StatusOK || resp.JSON200 == nil {
			return swf.JobHandle{}, explicitJobCreateError("put restart job", resp.StatusCode(), resp.Body, resp.JSON409)
		}
		handle := fromAPIJobHandle(*resp.JSON200)
		r.rememberTenant(handle.JobKey.TenantId)
		return handle, nil
	}
	resp, err := r.client.SubmitRestartJobWithResponse(ctx, req.Job.PriorJobKey.TenantId, body)
	if err != nil {
		return swf.JobHandle{}, err
	}
	if resp.StatusCode() != http.StatusOK || resp.JSON200 == nil {
		return swf.JobHandle{}, responseError("submit restart job", resp.StatusCode(), resp.Body, nil)
	}
	handle := fromAPIJobHandle(*resp.JSON200)
	r.rememberTenant(handle.JobKey.TenantId)
	return handle, nil
}

func (r *Runtime) CancelJob(ctx context.Context, req swf.CancelJobRequest) error {
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
	return responseError("cancel job", resp.StatusCode(), resp.Body, swf.ErrJobNotFound)
}

func (r *Runtime) PollWork(ctx context.Context, req swf.PollWorkRequest) ([]swf.ExecutionLease, error) {
	if len(req.TenantIds) > 1 {
		return nil, fmt.Errorf("at most one tenantId may be supplied for PollWork")
	}
	if len(req.TenantIds) == 1 && req.TenantIds[0] == "" {
		return nil, fmt.Errorf("tenantId must be non-empty when supplied for PollWork")
	}
	if len(req.TenantIds) == 0 {
		known := r.knownTenantOrder()
		if len(known) == 0 {
			// Some runtimes intentionally reject tenant-less polling. When the
			// client has not yet observed any tenant IDs, treat that as "no work
			// known yet" rather than sending an invalid request and triggering
			// worker-loop backoff.
			return nil, nil
		}
		return r.pollWorkKnownTenants(ctx, req, known)
	}
	return r.pollWorkOnce(ctx, req)
}

func (r *Runtime) pollWorkOnce(ctx context.Context, req swf.PollWorkRequest) ([]swf.ExecutionLease, error) {
	metadataEquals, err := metadataPredicatesToAPI(predicatesFilter(req.MetadataEquals))
	if err != nil {
		return nil, err
	}
	body := runtimeapi.PollWorkRequest{
		WorkerId:       req.WorkerID,
		Capabilities:   append([]string(nil), req.Capabilities...),
		Limit:          req.Limit,
		LongPollUntil:  req.LongPollUntil,
		LeaseDuration:  toAPIDurationPointer(req.LeaseDuration),
		MetadataEquals: metadataEquals,
	}
	if len(req.TenantIds) == 1 {
		tenantIDs := append([]string(nil), req.TenantIds...)
		body.TenantIds = &tenantIDs
	}
	resp, err := r.client.PollWorkWithResponse(ctx, body)
	if err != nil {
		return nil, err
	}
	if resp.StatusCode() != http.StatusOK || resp.JSON200 == nil {
		return nil, responseError("poll work", resp.StatusCode(), resp.Body, nil)
	}
	out := make([]swf.ExecutionLease, 0, len(resp.JSON200.Leases))
	for _, lease := range resp.JSON200.Leases {
		converted, err := r.executionLeaseFromAPI(lease)
		if err != nil {
			return nil, err
		}
		out = append(out, converted)
	}
	return out, nil
}

func (r *Runtime) pollWorkKnownTenants(ctx context.Context, req swf.PollWorkRequest, tenants []string) ([]swf.ExecutionLease, error) {
	var firstErr error
	for _, tenantID := range tenants {
		pollReq := req
		pollReq.TenantIds = []string{tenantID}
		leases, err := r.pollWorkOnce(ctx, pollReq)
		if err != nil {
			if firstErr == nil {
				firstErr = err
			}
			continue
		}
		if len(leases) > 0 {
			return leases, nil
		}
	}
	if firstErr != nil {
		return nil, firstErr
	}
	return nil, nil
}

func (r *Runtime) GetJobLease(ctx context.Context, req swf.GetJobLeaseRequest) (swf.ExecutionLease, error) {
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
	r.rememberTenant(req.JobKey.TenantId)
	return r.executionLeaseFromAPI(*resp.JSON200.Lease)
}

func (r *Runtime) rememberTenant(tenantID string) {
	if tenantID == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.knownTenants[tenantID] = struct{}{}
}

func (r *Runtime) knownTenantOrder() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	if len(r.knownTenants) == 0 {
		return nil
	}
	tenants := make([]string, 0, len(r.knownTenants))
	for tenantID := range r.knownTenants {
		tenants = append(tenants, tenantID)
	}
	sort.Strings(tenants)
	if len(tenants) == 1 {
		return tenants
	}
	start := r.pollCursor % len(tenants)
	r.pollCursor = (r.pollCursor + 1) % len(tenants)
	return append(append([]string(nil), tenants[start:]...), tenants[:start]...)
}

func (r *Runtime) CompleteTaskIfWaiting(ctx context.Context, req swf.CompleteTaskIfWaitingRequest) error {
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
	return responseErrorWithConflict("complete task if waiting", resp.StatusCode(), resp.Body, swf.ErrJobNotFound, swf.ErrConflict)
}

func (r *Runtime) GetJob(ctx context.Context, jobKey swf.JobKey) (swf.JobInfo, error) {
	resp, err := r.client.GetJobWithResponse(ctx, jobKey.TenantId, jobKey.JobId)
	if err != nil {
		return swf.JobInfo{}, err
	}
	if resp.StatusCode() != http.StatusOK || resp.JSON200 == nil {
		return swf.JobInfo{}, responseError("get job", resp.StatusCode(), resp.Body, swf.ErrJobNotFound)
	}
	return jobInfoFromAPI(r, jobKey, *resp.JSON200)
}

func (r *Runtime) ListJobs(ctx context.Context, req swf.ListJobsRequest) (swf.ListJobsResponse, error) {
	if len(req.TenantIds) != 1 || req.TenantIds[0] == "" {
		return swf.ListJobsResponse{}, fmt.Errorf("exactly one tenantId is required")
	}
	metadataPredicates, err := metadataPredicatesToAPI(req.MetadataFilter)
	if err != nil {
		return swf.ListJobsResponse{}, err
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
		return swf.ListJobsResponse{}, err
	}
	if resp.StatusCode() != http.StatusOK || resp.JSON200 == nil {
		return swf.ListJobsResponse{}, responseError("list jobs", resp.StatusCode(), resp.Body, nil)
	}
	out := swf.ListJobsResponse{
		NextPageToken: stringValue(resp.JSON200.NextPageToken),
	}
	for _, job := range resp.JSON200.Jobs {
		converted, err := jobSummaryFromAPI(job)
		if err != nil {
			return swf.ListJobsResponse{}, err
		}
		out.Jobs = append(out.Jobs, converted)
	}
	return out, nil
}

func (r *Runtime) GetChapter(ctx context.Context, ref swf.ChapterRef) (swf.StoredChapter, error) {
	resp, err := r.client.GetChapterWithResponse(ctx, ref.JobKey.TenantId, ref.JobKey.JobId, ref.Ordinal)
	if err != nil {
		return swf.StoredChapter{}, err
	}
	if resp.StatusCode() != http.StatusOK || resp.JSON200 == nil {
		return swf.StoredChapter{}, responseError("get chapter", resp.StatusCode(), resp.Body, swf.ErrChapterNotFound)
	}
	return fromAPIStoredChapter(*resp.JSON200)
}

func (r *Runtime) ListChapters(ctx context.Context, req swf.ListChaptersRequest) ([]swf.StoredChapter, error) {
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
		return nil, responseError("list chapters", resp.StatusCode(), resp.Body, swf.ErrChapterNotFound)
	}
	out := make([]swf.StoredChapter, 0, len(resp.JSON200.Chapters))
	for _, chapter := range resp.JSON200.Chapters {
		converted, err := fromAPIStoredChapter(chapter)
		if err != nil {
			return nil, err
		}
		out = append(out, converted)
	}
	return out, nil
}

func (r *Runtime) PutChapter(ctx context.Context, req swf.PutChapterRequest) error {
	if req.LeaseID == "" {
		return fmt.Errorf("leaseId is required")
	}
	if req.LeaseToken == "" {
		return fmt.Errorf("leaseToken is required")
	}
	chapter, err := runtimeChapterToWritable(ctx, req.Chapter, req.ArtifactUploads)
	if err != nil {
		return err
	}
	resp, err := r.client.AddChapterWithLeaseWithResponse(
		ctx,
		req.Ref.JobKey.TenantId,
		req.Ref.JobKey.JobId,
		req.LeaseID,
		&runtimeapi.AddChapterWithLeaseParams{XSWFLeaseToken: req.LeaseToken},
		runtimeapi.AddChapterRequest{Chapter: chapter},
	)
	if err != nil {
		return err
	}
	if resp.StatusCode() == http.StatusNoContent {
		return nil
	}
	return responseErrorWithConflict("put chapter", resp.StatusCode(), resp.Body, swf.ErrJobNotFound, swf.ErrConflict)
}

func (r *Runtime) OpenArtifact(ctx context.Context, ref swf.ArtifactRef) (swf.ArtifactReader, error) {
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
	jobKey      swf.JobKey
	capability  string
	payloadJSON json.RawMessage
	mu          sync.RWMutex
	leaseToken  string
}

func (l *remoteExecutionLease) LeaseID() string    { return l.leaseID }
func (l *remoteExecutionLease) Job() swf.JobHandle { return swf.JobHandle{JobKey: l.jobKey} }
func (l *remoteExecutionLease) Capability() string { return l.capability }
func (l *remoteExecutionLease) StopKeepAlive()     {}
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
		&runtimeapi.KeepAliveLeaseParams{XSWFLeaseToken: l.LeaseToken()},
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
	return responseError("keep lease alive", resp.StatusCode(), resp.Body, swf.ErrExecutionLeaseLost)
}

func (l *remoteExecutionLease) Complete(ctx context.Context, req swf.CompleteExecutionRequest) error {
	body := runtimeapi.CompleteExecutionRequest{
		Status: req.Status,
		Detail: stringPtrOrNil(req.Detail),
	}
	resp, err := l.runtime.client.CompleteJobWithLeaseWithResponse(
		ctx,
		l.jobKey.TenantId,
		l.jobKey.JobId,
		l.leaseID,
		&runtimeapi.CompleteJobWithLeaseParams{XSWFLeaseToken: l.LeaseToken()},
		body,
	)
	if err != nil {
		return err
	}
	if resp.StatusCode() == http.StatusNoContent {
		return nil
	}
	return responseError("complete job with lease", resp.StatusCode(), resp.Body, swf.ErrExecutionLeaseLost)
}

func (l *remoteExecutionLease) Reschedule(ctx context.Context, req swf.RescheduleExecutionRequest) error {
	var payload interface{}
	var err error
	if len(req.Payload) > 0 {
		payload, err = marshalJSONValueRequired(req.Payload)
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
		&runtimeapi.RescheduleJobWithLeaseParams{XSWFLeaseToken: l.LeaseToken()},
		body,
	)
	if err != nil {
		return err
	}
	if resp.StatusCode() == http.StatusNoContent {
		return nil
	}
	return responseError("reschedule job with lease", resp.StatusCode(), resp.Body, swf.ErrExecutionLeaseLost)
}

func (r *Runtime) executionLeaseFromAPI(lease runtimeapi.ExecutionLease) (swf.ExecutionLease, error) {
	payload, err := unmarshalJSONValueOptional(lease.Payload)
	if err != nil {
		return nil, err
	}
	return &remoteExecutionLease{
		runtime:     r,
		leaseID:     lease.LeaseId,
		jobKey:      fromAPIJobKey(lease.Job.JobKey),
		capability:  lease.Capability,
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

func toAPIPrerequisites(prereqs []swf.JobPrerequisite) *[]runtimeapi.JobPrerequisite {
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
		return fmt.Errorf("%s: %s", operation, message)
	case http.StatusConflict:
		if conflict != nil && conflict.Code == runtimeapi.ExistingJobMismatch {
			return swf.NewExistingJobMismatchError(message)
		}
		return fmt.Errorf("%w: %s", swf.ErrConflict, message)
	default:
		return fmt.Errorf("%s: http %d: %s", operation, status, message)
	}
}

func predicatesFilter(predicates []swf.MetadataPredicate) swf.MetadataFilter {
	if len(predicates) == 0 {
		return nil
	}
	filter := swf.Metadata()
	for _, predicate := range predicates {
		if len(predicate.Path) == 0 || len(predicate.Values) == 0 {
			continue
		}
		var clause swf.MetadataFilter
		for idx, value := range predicate.Values {
			eq, err := swf.Metadata().EqualFilter(swf.FieldName(predicate.Path[0]), value)
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
