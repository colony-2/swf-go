package toyimpl

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/colony-2/swf-go/pkg/swf"
	"github.com/segmentio/ksuid"
)

// JobIDGenerator allows overriding how job IDs are created.
type JobIDGenerator func(tenantId string) (swf.JobKey, error)

// Option configures the ToyEngine.
type Option func(*ToyEngine)

// WithLogger sets a logger for ToyEngine.
func WithLogger(logger *slog.Logger) Option {
	return func(e *ToyEngine) {
		if logger != nil {
			e.logger = logger
		}
	}
}

// WithJobIDGenerator overrides the default job ID generator.
func WithJobIDGenerator(gen JobIDGenerator) Option {
	return func(e *ToyEngine) {
		if gen != nil {
			e.idGenerator = gen
		}
	}
}

// ToyEngine holds the in-memory state backing the toy WorkflowRuntime.
type ToyEngine struct {
	mu               sync.Mutex
	jobRecords       map[swf.JobKey]*jobRecord
	runtimeChapters  map[swf.JobKey]map[int64]swf.Chapter
	runtimeArtifacts map[runtimeArtifactKey][]byte
	idGenerator      JobIDGenerator
	logger           *slog.Logger
}

type runtimeArtifactKey struct {
	jobKey  swf.JobKey
	ordinal int64
	name    string
}

type jobRecord struct {
	mu          sync.Mutex
	status      swf.JobStatus
	result      swf.TaskData
	err         error
	cancelled   bool
	finished    time.Time
	jobType     string
	createdAt   time.Time
	archived    *time.Time
	payload     []byte
	metadata    json.RawMessage
	capability  string
	step        int64
	waitFor     []string
	availableAt time.Time
	leased      bool
	leaseID     string
	chapters    map[int64]*toyChapter
}

type toyChapter struct {
	TaskType  string
	CreatedAt time.Time
	Input     swf.TaskData
	Output    swf.TaskData
	Attempt   int
	Err       error
}

type normalizedMetadataPredicate struct {
	Path       []string
	ValuesJSON []string
}

func normalizeMetadataPredicates(predicates []swf.MetadataPredicate) ([]normalizedMetadataPredicate, error) {
	if len(predicates) == 0 {
		return nil, nil
	}
	normalized := make([]normalizedMetadataPredicate, 0, len(predicates))
	for i, predicate := range predicates {
		if len(predicate.Path) == 0 {
			return nil, fmt.Errorf("metadata predicate %d path is required", i)
		}
		for _, segment := range predicate.Path {
			if segment == "" {
				return nil, fmt.Errorf("metadata predicate %d path contains empty segment", i)
			}
		}
		if len(predicate.Values) == 0 {
			return nil, fmt.Errorf("metadata predicate %d values are required", i)
		}
		valuesJSON := make([]string, 0, len(predicate.Values))
		for _, value := range predicate.Values {
			if value == nil {
				return nil, fmt.Errorf("metadata predicate %d values cannot contain nil", i)
			}
			valueJSON, err := encodeMetadataPredicateValue(value)
			if err != nil {
				return nil, fmt.Errorf("metadata predicate %d values invalid: %w", i, err)
			}
			valuesJSON = append(valuesJSON, valueJSON)
		}
		normalized = append(normalized, normalizedMetadataPredicate{
			Path:       predicate.Path,
			ValuesJSON: valuesJSON,
		})
	}
	return normalized, nil
}

func encodeMetadataPredicateValue(value any) (string, error) {
	switch v := value.(type) {
	case json.RawMessage:
		if !json.Valid(v) {
			return "", fmt.Errorf("metadata predicate value must be valid JSON")
		}
		return string(v), nil
	case []byte:
		if !json.Valid(v) {
			return "", fmt.Errorf("metadata predicate value must be valid JSON")
		}
		return string(v), nil
	default:
		encoded, err := json.Marshal(v)
		if err != nil {
			return "", fmt.Errorf("metadata predicate value must be JSON-serializable: %w", err)
		}
		return string(encoded), nil
	}
}

func metadataValueAtPath(root any, path []string) (any, bool) {
	if len(path) == 0 {
		return nil, false
	}
	current := root
	for _, segment := range path {
		obj, ok := current.(map[string]any)
		if !ok {
			return nil, false
		}
		next, ok := obj[segment]
		if !ok {
			return nil, false
		}
		current = next
	}
	return current, true
}

func metadataMatches(raw json.RawMessage, predicates []normalizedMetadataPredicate) (bool, error) {
	if len(predicates) == 0 {
		return true, nil
	}
	if len(raw) == 0 {
		raw = json.RawMessage(`{}`)
	}
	var metadata any
	if err := json.Unmarshal(raw, &metadata); err != nil {
		return false, fmt.Errorf("metadata must be valid JSON object: %w", err)
	}
	for _, predicate := range predicates {
		value, ok := metadataValueAtPath(metadata, predicate.Path)
		if !ok {
			return false, nil
		}
		valueJSON, err := encodeMetadataPredicateValue(value)
		if err != nil {
			return false, err
		}
		matched := false
		for _, candidate := range predicate.ValuesJSON {
			if valueJSON == candidate {
				matched = true
				break
			}
		}
		if !matched {
			return false, nil
		}
	}
	return true, nil
}

// NewToyEngine constructs the in-memory state backing the toy runtime.
func NewToyEngine(opts ...Option) *ToyEngine {
	engine := &ToyEngine{
		jobRecords:       make(map[swf.JobKey]*jobRecord),
		runtimeChapters:  make(map[swf.JobKey]map[int64]swf.Chapter),
		runtimeArtifacts: make(map[runtimeArtifactKey][]byte),
		idGenerator: func(tenantId string) (swf.JobKey, error) {
			return swf.JobKey{TenantId: tenantId, JobId: ksuid.New().String()}, nil
		},
		logger: slog.Default(),
	}
	for _, opt := range opts {
		opt(engine)
	}
	return engine
}

// GetJobRun returns a simplified job run view for the ToyEngine.
func (e *ToyEngine) GetJobRun(ctx context.Context, req swf.GetJobRunRequest) (swf.GetJobRunResponse, error) {
	e.mu.Lock()
	record, ok := e.jobRecords[req.JobKey]
	e.mu.Unlock()
	if !ok {
		return swf.GetJobRunResponse{}, fmt.Errorf("job %s not found", req.JobKey)
	}

	includeInputs, includeOutputs, includeArtifacts, _ := normalizeToyJobRunOptions(req)

	resp := swf.GetJobRunResponse{
		Job: swf.JobRunSummary{
			JobKey:     req.JobKey,
			JobType:    record.jobType,
			Status:     record.status,
			CreatedAt:  record.createdAt,
			ArchivedAt: record.archived,
		},
	}

	record.mu.Lock()
	if len(record.metadata) > 0 {
		metadataCopy := make([]byte, len(record.metadata))
		copy(metadataCopy, record.metadata)
		resp.Job.Metadata = metadataCopy
	}
	chapters := make(map[int64]*toyChapter, len(record.chapters))
	for ord, chap := range record.chapters {
		chapters[ord] = chap
	}
	capability := record.capability
	pendingStep := record.step
	status := record.status
	finished := record.finished
	result := record.result
	jobErr := record.err
	record.mu.Unlock()

	startSet := false
	if includeInputs {
		if chap := chapters[0]; chap != nil {
			input, err := buildToyTaskIO(ctx, chap.Input, req.JobKey.JobId, 0, includeInputs, includeArtifacts)
			if err != nil {
				return swf.GetJobRunResponse{}, err
			}
			resp.Start = swf.JobStart{
				Ordinal:   0,
				CreatedAt: chap.CreatedAt,
				Input:     input,
			}
			startSet = true
		}
	}
	attempts := []swf.JobAttempt{}
	attemptIndex := map[int]int{}
	ensureAttempt := func(num int) int {
		if num <= 0 {
			num = 1
		}
		if idx, ok := attemptIndex[num]; ok {
			return idx
		}
		attempt := swf.JobAttempt{Attempt: num}
		if num == 1 && startSet {
			attempt.CreatedAt = resp.Start.CreatedAt
		}
		attempts = append(attempts, attempt)
		idx := len(attempts) - 1
		attemptIndex[num] = idx
		return idx
	}
	_ = ensureAttempt(1)

	ordinals := make([]int64, 0, len(chapters))
	for ord := range chapters {
		if ord > 0 {
			ordinals = append(ordinals, ord)
		}
	}
	sort.Slice(ordinals, func(i, j int) bool { return ordinals[i] < ordinals[j] })

	for _, ord := range ordinals {
		chap := chapters[ord]
		if chap == nil || (chap.Output == nil && chap.Err == nil) {
			continue
		}

		input, err := buildToyTaskIO(ctx, chap.Input, req.JobKey.JobId, ord, includeInputs, includeArtifacts)
		if err != nil {
			return swf.GetJobRunResponse{}, err
		}
		output, err := buildToyTaskIO(ctx, chap.Output, req.JobKey.JobId, ord, includeOutputs, includeArtifacts)
		if err != nil {
			return swf.GetJobRunResponse{}, err
		}

		state := swf.TaskAttemptStateSucceeded
		outcome := swf.TaskOutcome{
			Status:      swf.TaskOutcomeStatusSucceeded,
			PayloadKind: "App",
		}
		if chap.Err != nil {
			state = swf.TaskAttemptStateFailed
			outcome = toyTaskOutcomeFromError(chap.Err)
		}

		attempt := swf.TaskAttempt{
			Ordinal: ord,
			Attempt: 1,
			Input:   input,
			Output:  output,
			State:   state,
			Outcome: outcome,
		}

		idx := ensureAttempt(1)
		attempts[idx].Tasks = append(attempts[idx].Tasks, swf.TaskRun{
			TaskRunID: fmt.Sprintf("%s:%d", chap.TaskType, ord),
			TaskType:  chap.TaskType,
			Attempts:  []swf.TaskAttempt{attempt},
		})
	}

	lastOrdinal := int64(0)
	if len(ordinals) > 0 {
		lastOrdinal = ordinals[len(ordinals)-1]
	}

	if status == swf.JobStatusCompleted && (result != nil || jobErr != nil) {
		output, err := buildToyTaskIO(ctx, result, req.JobKey.JobId, lastOrdinal+1, true, includeArtifacts)
		if err != nil {
			return swf.GetJobRunResponse{}, err
		}
		outcome := swf.TaskOutcome{
			Status:      swf.TaskOutcomeStatusSucceeded,
			PayloadKind: "App",
		}
		if jobErr != nil {
			outcome = toyTaskOutcomeFromError(jobErr)
		}
		idx := ensureAttempt(1)
		attempts[idx].Ordinal = lastOrdinal + 1
		attempts[idx].Attempt = 1
		attempts[idx].CreatedAt = finished
		attempts[idx].Output = output
		attempts[idx].Outcome = outcome
	}

	if status != swf.JobStatusCompleted && capability != "" {
		taskType := extractTaskType(capability)
		input := (*swf.TaskIO)(nil)
		if includeInputs {
			if chap := chapters[pendingStep]; chap != nil {
				loaded, err := buildToyTaskIO(ctx, chap.Input, req.JobKey.JobId, pendingStep, includeInputs, includeArtifacts)
				if err != nil {
					return swf.GetJobRunResponse{}, err
				}
				input = loaded
			}
		}
		state := swf.TaskAttemptStateWaiting
		if status == swf.JobStatusReady {
			state = swf.TaskAttemptStateReady
		} else if status == swf.JobStatusActive {
			state = swf.TaskAttemptStateRunning
		}

		idx := ensureAttempt(1)
		attempts[idx].Tasks = append(attempts[idx].Tasks, swf.TaskRun{
			TaskRunID: fmt.Sprintf("%s:%d", taskType, pendingStep),
			TaskType:  taskType,
			Attempts: []swf.TaskAttempt{
				{
					Ordinal: pendingStep,
					Attempt: 1,
					Input:   input,
					State:   state,
					Runtime: &swf.TaskRuntime{NextNeed: &capability},
				},
			},
		})
	}

	resp.Attempts = attempts
	return resp, nil
}

func normalizeToyJobRunOptions(req swf.GetJobRunRequest) (bool, bool, bool, bool) {
	if !req.IncludeInputs && !req.IncludeOutputs && !req.IncludeArtifacts && !req.IncludeAttemptInputs {
		return true, true, true, false
	}
	return req.IncludeInputs, req.IncludeOutputs, req.IncludeArtifacts, req.IncludeAttemptInputs
}

func toyTaskOutcomeFromError(err error) swf.TaskOutcome {
	outcome := swf.TaskOutcome{
		Status:      swf.TaskOutcomeStatusFailed,
		PayloadKind: "AppError",
		Error: &swf.TaskError{
			Kind:    swf.TaskErrorKindApp,
			Message: err.Error(),
		},
	}

	var jobFailed *swf.JobFailedError
	if errors.As(err, &jobFailed) && jobFailed.Cause != nil {
		err = jobFailed.Cause
	}

	var timeoutErr swf.TimeoutError
	if errors.As(err, &timeoutErr) {
		outcome.PayloadKind = "Timeout"
		outcome.Error = &swf.TaskError{
			Kind:      swf.TaskErrorKindTimeout,
			Message:   timeoutErr.Payload.Message,
			Retryable: boolPtr(timeoutErr.Payload.Retryable),
			Scope:     timeoutErr.Payload.Scope,
			After:     durationPtr(timeoutErr.Payload.After),
			InputRef:  timeoutErr.Payload.InputRef,
			Component: timeoutErr.Payload.Component,
			Code:      timeoutErr.Payload.Code,
		}
		return outcome
	}

	var systemErr swf.SystemError
	if errors.As(err, &systemErr) {
		outcome.PayloadKind = "SystemError"
		outcome.Error = &swf.TaskError{
			Kind:       swf.TaskErrorKindSystem,
			Message:    systemErr.Payload.Message,
			Component:  systemErr.Payload.Component,
			Code:       systemErr.Payload.Code,
			Retryable:  boolPtr(systemErr.Payload.Retryable),
			InputRef:   systemErr.Payload.InputRef,
			Stacktrace: append([]string(nil), systemErr.Payload.Stacktrace...),
		}
		return outcome
	}

	var appErr swf.AppError
	if errors.As(err, &appErr) {
		outcome.Error = &swf.TaskError{
			Kind:       swf.TaskErrorKindApp,
			Message:    appErr.Payload.Message,
			Level:      appErr.Payload.Level,
			Attrs:      cloneToyAttrs(appErr.Payload.Attrs),
			InputRef:   appErr.Payload.InputRef,
			Stacktrace: append([]string(nil), appErr.Payload.Stacktrace...),
		}
		return outcome
	}

	return outcome
}

func boolPtr(v bool) *bool {
	return &v
}

func durationPtr(v swf.Duration) *swf.Duration {
	return &v
}

func buildToyTaskIO(ctx context.Context, data swf.TaskData, jobID string, ordinal int64, includeData bool, includeArtifacts bool) (*swf.TaskIO, error) {
	if data == nil || (!includeData && !includeArtifacts) {
		return nil, nil
	}
	out := &swf.TaskIO{}
	if includeData {
		bytes, err := data.GetData()
		if err != nil {
			return nil, err
		}
		out.Data = append([]byte(nil), bytes...)
	}
	if includeArtifacts {
		arts, err := data.GetArtifacts()
		if err != nil {
			return nil, err
		}
		infos, err := buildToyArtifactInfos(ctx, arts, jobID, ordinal)
		if err != nil {
			return nil, err
		}
		out.Artifacts = infos
	}
	if out.Data == nil && len(out.Artifacts) == 0 {
		return nil, nil
	}
	return out, nil
}

func cloneToyAttrs(attrs map[string]interface{}) map[string]interface{} {
	if len(attrs) == 0 {
		return nil
	}
	out := make(map[string]interface{}, len(attrs))
	for key, value := range attrs {
		out[key] = value
	}
	return out
}

func buildToyArtifactInfos(ctx context.Context, artifacts []swf.Artifact, jobID string, ordinal int64) ([]swf.ArtifactInfo, error) {
	if len(artifacts) == 0 {
		return nil, nil
	}
	out := make([]swf.ArtifactInfo, 0, len(artifacts))
	for _, art := range artifacts {
		if art == nil {
			continue
		}
		sha, err := art.Sha256(ctx)
		if err != nil {
			return nil, err
		}
		var key *swf.ArtifactKey
		if k, err := art.ArtifactKey(); err == nil {
			key = &k
		} else if jobID != "" && ordinal >= 0 && art.Name() != "" {
			k := swf.ArtifactKey{
				JobId:       jobID,
				TaskOrdinal: ordinal,
				Name:        art.Name(),
				SizeBytes:   art.Size(),
			}
			key = &k
		}
		out = append(out, swf.ArtifactInfo{
			Name:        art.Name(),
			ContentType: "application/octet-stream",
			SizeBytes:   art.Size(),
			Sha256:      sha,
			Key:         key,
		})
	}
	return out, nil
}

func extractTaskType(capability string) string {
	for i := len(capability) - 1; i >= 0; i-- {
		if capability[i] == ':' {
			return capability[i+1:]
		}
	}
	return capability
}

func containsStore(stores []swf.JobStore, store swf.JobStore) bool {
	for _, s := range stores {
		if s == store {
			return true
		}
	}
	return false
}

func containsString(values []string, target string) bool {
	for _, v := range values {
		if v == target {
			return true
		}
	}
	return false
}

// ListJobs returns in-memory job summaries ordered by created_at desc then job_id desc.
func (e *ToyEngine) ListJobs(ctx context.Context, req swf.ListJobsRequest) (swf.ListJobsResponse, error) {
	// Validate that TenantIds is provided - matches real engine behavior
	if len(req.TenantIds) == 0 {
		return swf.ListJobsResponse{}, fmt.Errorf("tenant_ids is required for ListJobs")
	}

	pageSize := req.PageSize
	if pageSize <= 0 {
		pageSize = swf.DefaultListJobsPageSize
	} else if pageSize > swf.MaxListJobsPageSize {
		pageSize = swf.MaxListJobsPageSize
	}

	var (
		activeStatuses []swf.JobStatus
		includeActive  bool
		includeArchive bool
	)
	if len(req.Statuses) > 0 {
		for _, st := range req.Statuses {
			switch st {
			case swf.JobStatusCompleted:
				includeArchive = true
			case swf.JobStatusReady, swf.JobStatusExpired, swf.JobStatusPendingJobs, swf.JobStatusAwaitingFuture, swf.JobStatusActive, swf.JobStatusCrashConcern, swf.JobStatusCancelled:
				includeActive = true
				activeStatuses = append(activeStatuses, st)
			default:
				return swf.ListJobsResponse{}, fmt.Errorf("unknown status %q", st)
			}
		}
	} else {
		if len(req.Stores) == 0 {
			includeActive, includeArchive = true, true
		} else {
			for _, store := range req.Stores {
				switch store {
				case swf.JobStoreActive:
					includeActive = true
				case swf.JobStoreArchived:
					includeArchive = true
				default:
					return swf.ListJobsResponse{}, fmt.Errorf("unknown store %q", store)
				}
			}
		}
	}

	if !includeActive && !includeArchive {
		return swf.ListJobsResponse{}, nil
	}

	rawPredicates, err := swf.MetadataPredicates(req.MetadataFilter)
	if err != nil {
		return swf.ListJobsResponse{}, err
	}
	metadataPredicates, err := normalizeMetadataPredicates(rawPredicates)
	if err != nil {
		return swf.ListJobsResponse{}, err
	}

	var (
		cursorTime time.Time
		cursorJob  string
		hasCursor  bool
	)
	if req.PageToken != "" {
		createdAt, jobKey, err := swf.DecodeListJobsPageToken(req.PageToken)
		if err != nil {
			return swf.ListJobsResponse{}, err
		}
		cursorTime = createdAt
		cursorJob = jobKey.String()
		hasCursor = true
	}

	jobTypeAllowed := func(jt string) bool {
		if len(req.JobTypes) == 0 {
			return true
		}
		for _, expect := range req.JobTypes {
			if jt == expect {
				return true
			}
		}
		return false
	}

	tenantAllowed := func(tenantId string) bool {
		if len(req.TenantIds) == 0 {
			return true
		}
		for _, tid := range req.TenantIds {
			if tid == tenantId {
				return true
			}
		}
		return false
	}

	jobKeyAllowed := func(key swf.JobKey) bool {
		if len(req.JobKeys) == 0 {
			return true
		}
		for _, expect := range req.JobKeys {
			if expect == key {
				return true
			}
		}
		return false
	}

	jobTaskAllowed := func(rec *jobRecord) bool {
		if len(req.JobTasks) == 0 {
			return true
		}
		if rec.capability == "" {
			return false
		}
		for _, pair := range req.JobTasks {
			if pair.JobType == "" || pair.TaskType == "" {
				continue
			}
			if rec.capability == pair.JobType+":"+pair.TaskType {
				return true
			}
		}
		return false
	}

	statusAllowed := func(st swf.JobStatus) bool {
		if len(req.Statuses) == 0 {
			return true
		}
		for _, expect := range req.Statuses {
			if st == expect {
				return true
			}
		}
		return false
	}

	records := make([]swf.JobSummary, 0)
	e.mu.Lock()
	for key, rec := range e.jobRecords {
		rec.mu.Lock()
		status := rec.status
		store := swf.JobStoreActive
		if status == swf.JobStatusCompleted {
			store = swf.JobStoreArchived
		}

		// Filter by tenant - must match one of the requested tenants
		if !tenantAllowed(key.TenantId) {
			rec.mu.Unlock()
			continue
		}

		if status == swf.JobStatusCompleted && !includeArchive {
			rec.mu.Unlock()
			continue
		}
		if status != swf.JobStatusCompleted && !includeActive {
			rec.mu.Unlock()
			continue
		}
		if len(req.Stores) > 0 && !containsStore(req.Stores, store) {
			rec.mu.Unlock()
			continue
		}
		if !statusAllowed(status) {
			rec.mu.Unlock()
			continue
		}
		if !jobKeyAllowed(key) {
			rec.mu.Unlock()
			continue
		}
		if !jobTypeAllowed(rec.jobType) {
			rec.mu.Unlock()
			continue
		}
		if !jobTaskAllowed(rec) {
			rec.mu.Unlock()
			continue
		}
		if req.CreatedAfter != nil && rec.createdAt.Before(*req.CreatedAfter) {
			rec.mu.Unlock()
			continue
		}
		if req.CreatedBefore != nil && rec.createdAt.After(*req.CreatedBefore) {
			rec.mu.Unlock()
			continue
		}
		if len(metadataPredicates) > 0 {
			match, err := metadataMatches(rec.metadata, metadataPredicates)
			if err != nil {
				rec.mu.Unlock()
				e.mu.Unlock()
				return swf.ListJobsResponse{}, err
			}
			if !match {
				rec.mu.Unlock()
				continue
			}
		}

		payloadCopy := json.RawMessage(nil)
		if len(rec.payload) > 0 {
			payloadCopy = make([]byte, len(rec.payload))
			copy(payloadCopy, rec.payload)
		}
		metadataCopy := json.RawMessage(nil)
		if len(rec.metadata) > 0 {
			metadataCopy = make([]byte, len(rec.metadata))
			copy(metadataCopy, rec.metadata)
		}
		summary := swf.JobSummary{
			JobKey:            key,
			Status:            status,
			JobType:           rec.jobType,
			NextNeed:          cloneString(rec.capability),
			WaitFor:           append([]string(nil), rec.waitFor...),
			AvailableAt:       rec.createdAt,
			ExpiresAt:         nil,
			LeaseExpiresAt:    nil,
			CancelRequested:   rec.cancelled,
			CreatedAt:         rec.createdAt,
			ArchivedAt:        rec.archived,
			Payload:           payloadCopy,
			Metadata:          metadataCopy,
			TaskWaitInput:     nil,
			TaskWaitOutput:    nil,
			TaskWaitInputHash: nil,
			TaskWaitNext:      nil,
		}
		if wait, err := extractWorkerTaskWait(payloadCopy); err == nil && wait != nil {
			summary.TaskWaitInput = &wait.InputStep
			summary.TaskWaitOutput = &wait.OutputStep
			summary.TaskWaitInputHash = cloneStringPtr(&wait.InputHash)
			summary.TaskWaitNext = cloneStringPtr(&wait.Next)
		}
		rec.mu.Unlock()
		records = append(records, summary)
	}
	e.mu.Unlock()

	sort.Slice(records, func(i, j int) bool {
		if records[i].CreatedAt.Equal(records[j].CreatedAt) {
			return records[i].JobKey.String() > records[j].JobKey.String()
		}
		return records[i].CreatedAt.After(records[j].CreatedAt)
	})

	filtered := make([]swf.JobSummary, 0, len(records))
	for _, r := range records {
		if hasCursor {
			if r.CreatedAt.After(cursorTime) {
				continue
			}
			if r.CreatedAt.Equal(cursorTime) && r.JobKey.String() >= cursorJob {
				continue
			}
		}
		filtered = append(filtered, r)
	}

	nextToken := ""
	if len(filtered) > pageSize {
		last := filtered[pageSize-1]
		if tok, err := swf.EncodeListJobsPageToken(last.CreatedAt, last.JobKey); err == nil {
			nextToken = tok
		}
		filtered = filtered[:pageSize]
	}

	return swf.ListJobsResponse{Jobs: filtered, NextPageToken: nextToken}, nil
}

func (e *ToyEngine) getJobRecord(key swf.JobKey) *jobRecord {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.jobRecords[key]
}

// materializeArtifacts converts artifacts to in-memory copies and cleans up originals.
// This ensures the toy engine has stable artifact data independent of external resources
// and properly exercises cleanup paths to help catch bugs early.
func materializeArtifacts(ctx context.Context, artifacts []swf.Artifact, logger *slog.Logger) ([]swf.Artifact, error) {
	if len(artifacts) == 0 {
		return artifacts, nil
	}

	materialized := make([]swf.Artifact, 0, len(artifacts))
	for _, art := range artifacts {
		// Read artifact bytes
		data, err := art.Bytes(ctx)
		if err != nil {
			// Cleanup artifacts processed so far
			cleanupArtifacts(ctx, materialized, logger)
			// Cleanup remaining original artifacts
			cleanupArtifacts(ctx, artifacts, logger)
			return nil, err
		}

		// Create in-memory copy
		memArt := swf.NewArtifactFromBytes(art.Name(), data)
		materialized = append(materialized, memArt)
	}

	// Cleanup original artifacts now that we have materialized copies
	cleanupArtifacts(ctx, artifacts, logger)

	return materialized, nil
}

func assignToyArtifactKeys(artifacts []swf.Artifact, jobID string, ordinal int64) {
	if jobID == "" || ordinal < 0 {
		return
	}
	for _, art := range artifacts {
		if art == nil || art.Name() == "" {
			continue
		}
		swf.AssignArtifactKey(art, swf.ArtifactKey{
			JobId:       jobID,
			TaskOrdinal: ordinal,
			Name:        art.Name(),
			SizeBytes:   art.Size(),
		})
	}
}

// cleanupArtifacts calls Cleanup() on each artifact and logs any errors.
// Cleanup errors do not fail the workflow.
func cleanupArtifacts(ctx context.Context, artifacts []swf.Artifact, logger *slog.Logger) {
	for _, art := range artifacts {
		if err := art.Cleanup(); err != nil {
			logger.Warn("artifact cleanup failed", "name", art.Name(), "error", err)
		} else {
			logger.Debug("artifact cleaned up", "name", art.Name())
		}
	}
}

func (e *ToyEngine) OpenStoredArtifact(ctx context.Context, ref swf.ArtifactRef) (swf.ArtifactReader, error) {
	e.mu.Lock()
	data, ok := e.runtimeArtifacts[runtimeArtifactKey{
		jobKey:  ref.JobKey,
		ordinal: ref.Ordinal,
		name:    ref.Name,
	}]
	e.mu.Unlock()
	if ok {
		art := swf.NewArtifactFromBytes(ref.Name, data)
		return toyArtifactReader{art: art}, nil
	}
	return nil, fmt.Errorf("artifact %s not found for job %s ordinal %d", ref.Name, ref.JobKey.JobId, ref.Ordinal)
}

type toyArtifactReader struct {
	art swf.Artifact
}

func (r toyArtifactReader) Open() (io.ReadCloser, error) {
	return r.art.Open()
}

func (r toyArtifactReader) Size() int64 {
	return r.art.Size()
}

func (r toyArtifactReader) Name() string {
	return r.art.Name()
}

func cloneStringPtr(src *string) *string {
	if src == nil {
		return nil
	}
	value := *src
	return &value
}

func cloneString(src string) *string {
	if src == "" {
		return nil
	}
	value := src
	return &value
}
