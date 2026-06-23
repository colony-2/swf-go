package toyimpl

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"sort"
	"time"

	"github.com/colony-2/jobdb/pkg/internal/runtimecodec"
	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/jobmetadata"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/jobschema"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/leaseauth"
	"github.com/segmentio/ksuid"
)

const defaultToyRemoteLeaseDuration = 30 * time.Second

type workerJobPayload struct {
	RunPolicy jobdb.RunPolicy `json:"run_policy,omitempty"`
	TaskWait  *workerTaskWait `json:"task_wait,omitempty"`
}

type workerTaskWait struct {
	InputStep  int64  `json:"in"`
	OutputStep int64  `json:"out"`
	Next       string `json:"next"`
	InputHash  string `json:"input_hash,omitempty"`
}

// Runtime is an in-memory WorkflowRuntime used by the shared jobdb engine.
type Runtime struct {
	engine *ToyEngine
}

var _ jobdb.WorkflowRuntime = (*Runtime)(nil)

func New(opts ...Option) *Runtime {
	engine := NewToyEngine(opts...)
	return &Runtime{engine: engine}
}

func (r *Runtime) SubmitJob(ctx context.Context, req jobdb.SubmitJobRequest) (jobdb.JobHandle, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return jobdb.JobHandle{}, err
	}
	var jobKey jobdb.JobKey
	if req.Job.JobID != "" {
		jobKey = jobdb.JobKey{TenantId: req.Job.TenantId, JobId: req.Job.JobID}
	} else {
		var err error
		jobKey, err = r.engine.idGenerator(req.Job.TenantId)
		if err != nil {
			return jobdb.JobHandle{}, err
		}
	}
	if err := jobKey.Validate(); err != nil {
		return jobdb.JobHandle{}, err
	}
	if err := jobdb.ValidateApplicationMetadata(req.Job.Metadata); err != nil {
		return jobdb.JobHandle{}, err
	}
	schemaHash, err := jobschema.ResolveActiveForNewJob(ctx, r, req.Job.TenantId, req.Job.Schema)
	if err != nil {
		return jobdb.JobHandle{}, err
	}
	storedMetadata, err := jobdb.BuildJobMetadataEnvelope(req.Job.Metadata, jobdb.RuntimeJobMetadata{SchemaHash: schemaHash})
	if err != nil {
		return jobdb.JobHandle{}, err
	}
	if req.Job.JobID != "" {
		inputHash, err := jobdbInputHash(ctx, req.Job.Data)
		if err != nil {
			return jobdb.JobHandle{}, err
		}
		if handle, ok, err := r.existingEquivalentJob(jobKey, req.Job, inputHash, schemaHash); ok || err != nil {
			return handle, err
		}
	}

	jobData, storedArtifacts, err := r.materializeTaskData(ctx, jobKey, 0, req.Job.Data)
	if err != nil {
		return jobdb.JobHandle{}, err
	}
	inputHash, err := jobdbInputHash(ctx, jobData)
	if err != nil {
		return jobdb.JobHandle{}, err
	}
	now := time.Now().UTC()
	runPolicy := req.Job.RunPolicy
	metadata, err := marshalChapterMetadata(map[string]any{
		"version":    1,
		"ordinal":    int64(0),
		"task_type":  req.Job.JobType,
		"created_at": now,
		"input_hash": inputHash,
		"attempt":    1,
		"run_policy": runPolicy,
		"prereqs":    req.Job.Prerequisites,
	})
	if err != nil {
		return jobdb.JobHandle{}, err
	}
	payload, err := jobData.GetData()
	if err != nil {
		return jobdb.JobHandle{}, err
	}
	stored := jobdb.Chapter{
		Ordinal:   0,
		TaskType:  req.Job.JobType,
		InputHash: inputHash,
		CreatedAt: now,
		Metadata:  metadata,
		Body:      jobdb.JobStartChapter{Input: jobdb.ApplicationInputBytes{Data: append([]byte(nil), payload...)}},
		Artifacts: storedArtifacts,
	}
	if err := jobschema.ValidateChapter(ctx, r, jobdb.JobSchemaKey{TenantId: jobKey.TenantId, SchemaHash: schemaHash}, stored); err != nil {
		return jobdb.JobHandle{}, err
	}

	payloadJSON, err := json.Marshal(workerJobPayload{
		RunPolicy: runPolicy,
	})
	if err != nil {
		return jobdb.JobHandle{}, err
	}

	availableAt := now
	if req.Job.AvailableAt != nil {
		availableAt = req.Job.AvailableAt.UTC()
	}
	status := jobdb.JobStatusReady
	if availableAt.After(now) {
		status = jobdb.JobStatusAwaitingFuture
	}
	record := &jobRecord{
		status:      status,
		jobType:     req.Job.JobType,
		createdAt:   now,
		metadata:    cloneJSON(storedMetadata),
		payload:     payloadJSON,
		capability:  req.Job.JobType,
		chapters:    make(map[int64]*toyChapter),
		availableAt: availableAt,
	}
	record.chapters[0] = &toyChapter{
		TaskType:  req.Job.JobType,
		CreatedAt: now,
		Input:     jobData,
		Output:    jobData,
		Attempt:   1,
	}

	r.engine.mu.Lock()
	r.engine.jobRecords[jobKey] = record
	if r.engine.runtimeChapters[jobKey] == nil {
		r.engine.runtimeChapters[jobKey] = make(map[int64]jobdb.Chapter)
	}
	r.engine.runtimeChapters[jobKey][0] = stored
	r.engine.mu.Unlock()

	return jobdb.JobHandle{JobKey: jobKey}, nil
}

func (r *Runtime) existingEquivalentJob(jobKey jobdb.JobKey, job jobdb.SubmitJob, inputHash string, schemaHash string) (jobdb.JobHandle, bool, error) {
	r.engine.mu.Lock()
	defer r.engine.mu.Unlock()

	record, ok := r.engine.jobRecords[jobKey]
	if !ok {
		return jobdb.JobHandle{}, false, nil
	}
	start, ok := r.engine.runtimeChapters[jobKey][0]
	if !ok {
		return jobdb.JobHandle{}, false, jobdb.NewExistingJobMismatchError(fmt.Sprintf("job %s already exists without start chapter", jobKey))
	}
	if start.TaskType != job.JobType {
		return jobdb.JobHandle{}, false, jobdb.NewExistingJobMismatchError(fmt.Sprintf("job %s already exists with different job type", jobKey))
	}
	if start.InputHash != inputHash {
		return jobdb.JobHandle{}, false, jobdb.NewExistingJobMismatchError(fmt.Sprintf("job %s already exists with different input", jobKey))
	}
	storedMetadata, err := jobdb.BuildJobMetadataEnvelope(job.Metadata, jobdb.RuntimeJobMetadata{SchemaHash: schemaHash})
	if err != nil {
		return jobdb.JobHandle{}, false, err
	}
	if !bytes.Equal(record.metadata, storedMetadata) {
		return jobdb.JobHandle{}, false, jobdb.NewExistingJobMismatchError(fmt.Sprintf("job %s already exists with different metadata", jobKey))
	}
	runPolicy, err := extractRunPolicyFromMetadata(start.Metadata)
	if err != nil {
		return jobdb.JobHandle{}, false, err
	}
	if !reflect.DeepEqual(runPolicy, job.RunPolicy) {
		return jobdb.JobHandle{}, false, jobdb.NewExistingJobMismatchError(fmt.Sprintf("job %s already exists with different run policy", jobKey))
	}
	prereqs, err := extractPrerequisitesFromMetadata(start.Metadata)
	if err != nil {
		return jobdb.JobHandle{}, false, err
	}
	if !reflect.DeepEqual(prereqs, job.Prerequisites) {
		return jobdb.JobHandle{}, false, jobdb.NewExistingJobMismatchError(fmt.Sprintf("job %s already exists with different prerequisites", jobKey))
	}
	return jobdb.JobHandle{JobKey: jobKey}, true, nil
}

func (r *Runtime) SubmitRestartJob(ctx context.Context, req jobdb.SubmitRestartJobRequest) (jobdb.JobHandle, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if req.Job.LastStepToKeep < 0 {
		return jobdb.JobHandle{}, fmt.Errorf("LastStepToKeep must be >= 0 for restart")
	}
	prior := r.engine.getJobRecord(req.Job.PriorJobKey)
	if prior == nil {
		return jobdb.JobHandle{}, jobdb.ErrJobNotFound
	}
	var jobKey jobdb.JobKey
	if req.Job.JobID != "" {
		jobKey = jobdb.JobKey{TenantId: req.Job.PriorJobKey.TenantId, JobId: req.Job.JobID}
	} else {
		genKey, err := r.engine.idGenerator(req.Job.PriorJobKey.TenantId)
		if err != nil {
			return jobdb.JobHandle{}, err
		}
		jobKey = genKey
	}
	if err := jobKey.Validate(); err != nil {
		return jobdb.JobHandle{}, err
	}
	schemaHash, err := jobschema.ResolveActiveForNewJob(ctx, r, req.Job.PriorJobKey.TenantId, req.Job.Schema)
	if err != nil {
		return jobdb.JobHandle{}, err
	}
	storedMetadata, err := jobdb.BuildJobMetadataEnvelope(nil, jobdb.RuntimeJobMetadata{SchemaHash: schemaHash})
	if err != nil {
		return jobdb.JobHandle{}, err
	}
	if err := jobschema.Prime(ctx, r, jobdb.JobSchemaKey{TenantId: jobKey.TenantId, SchemaHash: schemaHash}); err != nil {
		return jobdb.JobHandle{}, err
	}

	r.engine.mu.Lock()
	defer r.engine.mu.Unlock()

	sourceChapters := r.engine.runtimeChapters[req.Job.PriorJobKey]
	if sourceChapters == nil {
		return jobdb.JobHandle{}, jobdb.ErrJobNotFound
	}
	nextOrdinal := req.Job.LastStepToKeep + 1
	nextChapter, ok := sourceChapters[nextOrdinal]
	if !ok {
		return jobdb.JobHandle{}, fmt.Errorf("LastStepToKeep %d invalid: no chapter at ordinal %d", req.Job.LastStepToKeep, nextOrdinal)
	}
	nextAttempt, err := extractAttemptFromMetadata(nextChapter.Metadata)
	if err != nil {
		return jobdb.JobHandle{}, err
	}
	if nextAttempt > 1 {
		taskType := nextChapter.TaskType
		if taskType == "" {
			if chap0, ok := sourceChapters[0]; ok {
				taskType = chap0.TaskType
			}
		}
		return jobdb.JobHandle{}, fmt.Errorf("LastStepToKeep %d cuts into retry chain: next ordinal %d is attempt %d of %s", req.Job.LastStepToKeep, nextOrdinal, nextAttempt, taskType)
	}
	targetChapters := make(map[int64]jobdb.Chapter)
	for ordinal, chapter := range sourceChapters {
		if ordinal > req.Job.LastStepToKeep {
			continue
		}
		targetChapters[ordinal] = cloneChapter(chapter)
	}
	if _, ok := targetChapters[0]; !ok {
		return jobdb.JobHandle{}, jobdb.ErrJobNotFound
	}
	for _, chapter := range targetChapters {
		if err := jobschema.ValidateChapter(ctx, r, jobdb.JobSchemaKey{TenantId: jobKey.TenantId, SchemaHash: schemaHash}, chapter); err != nil {
			return jobdb.JobHandle{}, err
		}
	}
	if req.Job.JobID != "" {
		if handle, ok, err := r.existingEquivalentRestartJob(jobKey, req.Job, targetChapters, storedMetadata); ok || err != nil {
			return handle, err
		}
	}

	chap0 := targetChapters[0]
	jobType := chap0.TaskType
	meta, err := extractRunPolicyFromMetadata(chap0.Metadata)
	if err != nil {
		return jobdb.JobHandle{}, err
	}
	payloadJSON, err := json.Marshal(workerJobPayload{RunPolicy: meta})
	if err != nil {
		return jobdb.JobHandle{}, err
	}

	record := &jobRecord{
		status:      jobdb.JobStatusReady,
		jobType:     jobType,
		createdAt:   time.Now().UTC(),
		metadata:    cloneJSON(storedMetadata),
		payload:     payloadJSON,
		capability:  jobType,
		chapters:    make(map[int64]*toyChapter),
		availableAt: time.Now().UTC(),
	}
	for ordinal, chapter := range targetChapters {
		td, decodeErr := r.taskDataFromChapter(jobKey, chapter)
		if decodeErr != nil {
			return jobdb.JobHandle{}, decodeErr
		}
		record.chapters[ordinal] = &toyChapter{
			TaskType:  chapter.TaskType,
			CreatedAt: chapter.CreatedAt,
			Input:     td,
			Output:    td,
			Attempt:   1,
		}
	}
	r.engine.jobRecords[jobKey] = record
	r.engine.runtimeChapters[jobKey] = targetChapters
	return jobdb.JobHandle{JobKey: jobKey}, nil
}

func (r *Runtime) existingEquivalentRestartJob(jobKey jobdb.JobKey, job jobdb.SubmitRestartJob, expected map[int64]jobdb.Chapter, expectedMetadata json.RawMessage) (jobdb.JobHandle, bool, error) {
	record, ok := r.engine.jobRecords[jobKey]
	if !ok {
		return jobdb.JobHandle{}, false, nil
	}
	if record == nil {
		return jobdb.JobHandle{}, false, jobdb.NewExistingJobMismatchError(fmt.Sprintf("job %s already exists without state", jobKey))
	}
	if !bytes.Equal(record.metadata, expectedMetadata) {
		return jobdb.JobHandle{}, false, jobdb.NewExistingJobMismatchError(fmt.Sprintf("job %s already exists with different metadata", jobKey))
	}
	existing := r.engine.runtimeChapters[jobKey]
	if existing == nil {
		return jobdb.JobHandle{}, false, jobdb.NewExistingJobMismatchError(fmt.Sprintf("job %s already exists without runtime chapters", jobKey))
	}
	for ordinal := int64(0); ordinal <= job.LastStepToKeep; ordinal++ {
		want, ok := expected[ordinal]
		if !ok {
			return jobdb.JobHandle{}, false, jobdb.NewExistingJobMismatchError(fmt.Sprintf("job %s is missing expected restart chapter %d", jobKey, ordinal))
		}
		got, ok := existing[ordinal]
		if !ok {
			return jobdb.JobHandle{}, false, jobdb.NewExistingJobMismatchError(fmt.Sprintf("job %s already exists with different restart history at ordinal %d", jobKey, ordinal))
		}
		if !reflect.DeepEqual(cloneChapter(got), cloneChapter(want)) {
			return jobdb.JobHandle{}, false, jobdb.NewExistingJobMismatchError(fmt.Sprintf("job %s already exists with different restart history at ordinal %d", jobKey, ordinal))
		}
	}
	if next, ok := existing[job.LastStepToKeep+1]; ok && runtimecodec.ChapterIs(next, runtimecodec.ChapterTypeRestartExtra) && job.ExtraTaskOutput == nil {
		return jobdb.JobHandle{}, false, jobdb.NewExistingJobMismatchError(fmt.Sprintf("job %s already exists with restart extra output that was not requested", jobKey))
	}
	return jobdb.JobHandle{JobKey: jobKey}, true, nil
}

func (r *Runtime) CancelJob(ctx context.Context, req jobdb.CancelJobRequest) error {
	record := r.engine.getJobRecord(req.JobKey)
	if record == nil {
		return jobdb.ErrJobNotFound
	}
	record.mu.Lock()
	defer record.mu.Unlock()
	record.cancelled = true
	record.status = jobdb.JobStatusCancelled
	record.err = context.Canceled
	now := time.Now().UTC()
	record.archived = &now
	record.leased = false
	record.leaseID = ""
	return nil
}

func (r *Runtime) PollWork(ctx context.Context, req jobdb.PollWorkRequest) ([]jobdb.ExecutionLease, error) {
	if req.LeaseDuration < 0 {
		return nil, fmt.Errorf("lease duration must be >= 0")
	}
	if req.TenantId == "" {
		return nil, fmt.Errorf("tenant_id is required for PollWork")
	}
	metadataPredicates, err := normalizeMetadataPredicates(req.MetadataEquals)
	if err != nil {
		return nil, err
	}

	r.engine.mu.Lock()

	limit := req.Limit
	if limit <= 0 {
		limit = 1
	}
	capSet := make(map[string]struct{}, len(req.Capabilities))
	for _, capability := range req.Capabilities {
		if capability != "" {
			capSet[capability] = struct{}{}
		}
	}
	now := time.Now().UTC()
	out := make([]jobdb.ExecutionLease, 0, limit)
	for key, record := range r.engine.jobRecords {
		if key.TenantId != req.TenantId {
			continue
		}
		record.mu.Lock()
		r.advanceRecordStateLocked(key.TenantId, now, record)
		if record.leased || record.status != jobdb.JobStatusReady {
			record.mu.Unlock()
			continue
		}
		if _, ok := capSet[record.capability]; !ok {
			record.mu.Unlock()
			continue
		}
		if len(metadataPredicates) > 0 {
			match, err := metadataMatches(record.metadata, metadataPredicates)
			if err != nil {
				record.mu.Unlock()
				r.engine.mu.Unlock()
				return nil, err
			}
			if !match {
				record.mu.Unlock()
				continue
			}
		}
		record.leased = true
		record.status = jobdb.JobStatusActive
		record.leaseID = ksuid.New().String()
		payload := cloneJSON(record.payload)
		out = append(out, &runtimeLease{
			runtime:    r,
			jobKey:     key,
			leaseID:    record.leaseID,
			capability: record.capability,
			payload:    payload,
			expiresAt:  now.Add(toyLeaseDurationOrDefault(req.LeaseDuration)),
			duration:   toyLeaseDurationOrDefault(req.LeaseDuration),
			schemaHash: jobmetadata.SchemaHashFromStoredMetadata(record.metadata),
		})
		record.mu.Unlock()
		if len(out) >= limit {
			break
		}
	}
	r.engine.mu.Unlock()
	filtered := out[:0]
	for _, item := range out {
		lease, ok := item.(*runtimeLease)
		if !ok {
			filtered = append(filtered, item)
			continue
		}
		ok, err := r.preflightScheduleLease(ctx, lease)
		if err != nil {
			return nil, err
		}
		if ok {
			filtered = append(filtered, lease)
		}
	}
	return filtered, nil
}

func (r *Runtime) GetJobLease(ctx context.Context, req jobdb.GetJobLeaseRequest) (jobdb.ExecutionLease, error) {
	if req.LeaseDuration < 0 {
		return nil, fmt.Errorf("lease duration must be >= 0")
	}

	capSet := make(map[string]struct{}, len(req.Capabilities))
	for _, capability := range req.Capabilities {
		if capability != "" {
			capSet[capability] = struct{}{}
		}
	}
	if len(capSet) == 0 {
		return nil, fmt.Errorf("at least one capability is required")
	}

	r.engine.mu.Lock()

	record := r.engine.jobRecords[req.JobKey]
	if record == nil {
		r.engine.mu.Unlock()
		return nil, nil
	}

	now := time.Now().UTC()
	record.mu.Lock()
	r.advanceRecordStateLocked(req.JobKey.TenantId, now, record)
	if record.leased || record.status != jobdb.JobStatusReady {
		record.mu.Unlock()
		r.engine.mu.Unlock()
		return nil, nil
	}
	if _, ok := capSet[record.capability]; !ok {
		record.mu.Unlock()
		r.engine.mu.Unlock()
		return nil, nil
	}

	record.leased = true
	record.status = jobdb.JobStatusActive
	record.leaseID = ksuid.New().String()
	lease := &runtimeLease{
		runtime:    r,
		jobKey:     req.JobKey,
		leaseID:    record.leaseID,
		capability: record.capability,
		payload:    cloneJSON(record.payload),
		expiresAt:  now.Add(toyLeaseDurationOrDefault(req.LeaseDuration)),
		duration:   toyLeaseDurationOrDefault(req.LeaseDuration),
		schemaHash: jobmetadata.SchemaHashFromStoredMetadata(record.metadata),
	}
	record.mu.Unlock()
	r.engine.mu.Unlock()
	ok, err := r.preflightScheduleLease(ctx, lease)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}
	return lease, nil
}

type jobInfoTaskData struct {
	taskData jobdb.TaskData
	err      error
}

func (d *jobInfoTaskData) GetData() (jobdb.Data, error) {
	if d.taskData == nil {
		return nil, d.err
	}
	data, err := d.taskData.GetData()
	if err != nil {
		return data, err
	}
	return data, d.err
}

func (d *jobInfoTaskData) GetDataOrPanic() jobdb.Data {
	data, err := d.GetData()
	if err != nil {
		panic(err)
	}
	return data
}

func (d *jobInfoTaskData) GetArtifacts() ([]jobdb.Artifact, error) {
	if d.taskData == nil {
		return nil, d.err
	}
	return d.taskData.GetArtifacts()
}

func (d *jobInfoTaskData) TaskDataResult() (jobdb.TaskData, error) {
	return d.taskData, d.err
}

func (r *Runtime) GetJob(ctx context.Context, jobKey jobdb.JobKey) (jobdb.JobInfo, error) {
	record := r.engine.getJobRecord(jobKey)
	if record == nil {
		return jobdb.JobInfo{}, jobdb.ErrJobNotFound
	}
	record.mu.Lock()
	defer record.mu.Unlock()
	job := jobdb.JobInfo{
		Status:     record.status,
		Data:       &jobInfoTaskData{err: jobdb.ErrJobNotComplete},
		SchemaHash: jobmetadata.SchemaHashFromStoredMetadata(record.metadata),
	}
	if record.status == jobdb.JobStatusCompleted || record.status == jobdb.JobStatusCancelled {
		job.Data = &jobInfoTaskData{taskData: record.result, err: record.err}
	}
	return job, nil
}

func (r *Runtime) GetJobRun(ctx context.Context, req jobdb.GetJobRunRequest) (jobdb.GetJobRunResponse, error) {
	return r.engine.GetJobRun(ctx, req)
}

func (r *Runtime) ListJobs(ctx context.Context, req jobdb.ListJobsRequest) (jobdb.ListJobsResponse, error) {
	return r.engine.ListJobs(ctx, req)
}

func (r *Runtime) GetChapter(ctx context.Context, ref jobdb.ChapterRef) (jobdb.Chapter, error) {
	r.engine.mu.Lock()
	defer r.engine.mu.Unlock()
	chapters := r.engine.runtimeChapters[ref.JobKey]
	if chapters == nil {
		return jobdb.Chapter{}, jobdb.ErrChapterNotFound
	}
	chapter, ok := chapters[ref.Ordinal]
	if !ok {
		return jobdb.Chapter{}, jobdb.ErrChapterNotFound
	}
	return cloneChapter(chapter), nil
}

func (r *Runtime) ListChapters(ctx context.Context, req jobdb.ListChaptersRequest) ([]jobdb.Chapter, error) {
	if req.StartOrdinal < 0 {
		return nil, fmt.Errorf("start ordinal must be >= 0")
	}
	r.engine.mu.Lock()
	defer r.engine.mu.Unlock()

	chapters := r.engine.runtimeChapters[req.JobKey]
	if chapters == nil {
		return nil, jobdb.ErrJobNotFound
	}

	ordinals := make([]int64, 0, len(chapters))
	for ordinal := range chapters {
		ordinals = append(ordinals, ordinal)
	}
	sort.Slice(ordinals, func(i, j int) bool { return ordinals[i] < ordinals[j] })

	out := make([]jobdb.Chapter, 0, len(ordinals))
	for _, ordinal := range ordinals {
		if ordinal < req.StartOrdinal {
			continue
		}
		if req.EndOrdinal != nil && ordinal > *req.EndOrdinal {
			break
		}
		out = append(out, cloneChapter(chapters[ordinal]))
	}
	return out, nil
}

func (r *Runtime) PutChapter(ctx context.Context, req jobdb.PutChapterRequest) error {
	if req.LeaseID == "" {
		return fmt.Errorf("lease id is required for PutChapter")
	}
	if req.Ref.Ordinal < 0 {
		return fmt.Errorf("chapter ordinal must be >= 0")
	}
	if req.Chapter.Ordinal != req.Ref.Ordinal {
		return fmt.Errorf("chapter ordinal %d does not match target ordinal %d", req.Chapter.Ordinal, req.Ref.Ordinal)
	}
	record := r.engine.getJobRecord(req.Ref.JobKey)
	if record == nil {
		return jobdb.ErrJobNotFound
	}
	if authorized, err := leaseauth.Authorize(ctx, req.Ref.JobKey, req.LeaseID); err != nil {
		return err
	} else if !authorized {
		record.mu.Lock()
		if record.leaseID != req.LeaseID {
			record.mu.Unlock()
			return jobdb.ErrExecutionLeaseLost
		}
		record.mu.Unlock()
	}

	chapter, err := r.prepareChapterWrite(ctx, req)
	if err != nil {
		return err
	}
	schemaHash := ""
	if claims, ok := leaseauth.ClaimsFromContext(ctx); ok && leaseauth.Matches(claims, req.Ref.JobKey, req.LeaseID) {
		schemaHash = claims.SchemaHash
	}
	if schemaHash == "" {
		record.mu.Lock()
		schemaHash = jobmetadata.SchemaHashFromStoredMetadata(record.metadata)
		record.mu.Unlock()
	}
	if err := jobschema.ValidateChapter(ctx, r, jobdb.JobSchemaKey{TenantId: req.Ref.JobKey.TenantId, SchemaHash: schemaHash}, chapter); err != nil {
		return err
	}
	return r.storeRuntimeChapter(req.Ref.JobKey, req.Ref.Ordinal, chapter)
}

func (r *Runtime) storeRuntimeChapter(jobKey jobdb.JobKey, ordinal int64, chapter jobdb.Chapter) error {
	chapterType, err := runtimecodec.ChapterType(chapter)
	if err != nil {
		return err
	}
	payloadKind, payload, err := runtimecodec.ChapterPayload(chapter)
	if err != nil {
		return err
	}

	r.engine.mu.Lock()
	chapters := r.engine.runtimeChapters[jobKey]
	if chapters == nil {
		r.engine.mu.Unlock()
		return jobdb.ErrJobNotFound
	}
	if ordinal < 0 {
		r.engine.mu.Unlock()
		return fmt.Errorf("chapter ordinal must be >= 0")
	}
	if chapter.Ordinal != ordinal {
		r.engine.mu.Unlock()
		return fmt.Errorf("chapter ordinal %d does not match target ordinal %d", chapter.Ordinal, ordinal)
	}
	if _, exists := chapters[ordinal]; exists {
		r.engine.mu.Unlock()
		return fmt.Errorf("%w: chapter ordinal %d already exists", jobdb.ErrConflict, ordinal)
	}
	if ordinal > 0 {
		if _, exists := chapters[ordinal-1]; !exists {
			r.engine.mu.Unlock()
			return fmt.Errorf("%w: chapter ordinal %d is not appendable", jobdb.ErrConflict, ordinal)
		}
	}
	chapters[ordinal] = cloneChapter(chapter)
	record := r.engine.jobRecords[jobKey]
	r.engine.mu.Unlock()

	if record == nil {
		return nil
	}
	td, err := r.taskDataFromChapter(jobKey, chapter)
	if err != nil && chapterType != runtimecodec.ChapterTypeJobAttemptOutcome && td == nil {
		return err
	}
	meta, _ := extractAttemptFromMetadata(chapter.Metadata)
	record.mu.Lock()
	if chapterType != runtimecodec.ChapterTypeJobAttemptOutcome {
		record.chapters[ordinal] = &toyChapter{
			TaskType:  chapter.TaskType,
			CreatedAt: chapter.CreatedAt,
			Input:     td,
			Output:    td,
			Attempt:   meta,
			Err:       err,
		}
	}
	if chapterType == runtimecodec.ChapterTypeJobAttemptOutcome {
		record.result = td
		if td == nil {
			record.result = &jobdb.EnvelopedTaskData{
				SimpleTaskData: jobdb.SimpleTaskData{
					Data: append([]byte(nil), payload...),
				},
				Kind: payloadKind,
			}
		}
		_, resultErr := r.taskDataFromChapter(jobKey, chapter)
		record.err = resultErr
	}
	record.mu.Unlock()
	return nil
}

func (r *Runtime) OpenArtifact(ctx context.Context, ref jobdb.ArtifactRef) (jobdb.ArtifactReader, error) {
	return r.engine.OpenStoredArtifact(ctx, ref)
}

func (r *Runtime) materializeTaskData(ctx context.Context, jobKey jobdb.JobKey, ordinal int64, data jobdb.TaskData) (jobdb.TaskData, []jobdb.StoredArtifact, error) {
	if data == nil {
		return nil, nil, nil
	}
	raw, err := data.GetData()
	if err != nil {
		return nil, nil, err
	}
	artifacts, err := data.GetArtifacts()
	if err != nil {
		return nil, nil, err
	}
	materialized, err := materializeArtifacts(ctx, artifacts, r.engine.logger)
	if err != nil {
		return nil, nil, err
	}
	assignToyArtifactKeys(materialized, jobKey.JobId, ordinal)
	stored := make([]jobdb.StoredArtifact, 0, len(materialized))
	for _, art := range materialized {
		if art == nil {
			continue
		}
		bytes, err := art.Bytes(ctx)
		if err != nil {
			return nil, nil, err
		}
		digest, err := art.Sha256(ctx)
		if err != nil {
			return nil, nil, err
		}
		r.engine.mu.Lock()
		r.engine.runtimeArtifacts[runtimeArtifactKey{jobKey: jobKey, ordinal: ordinal, name: art.Name()}] = append([]byte(nil), bytes...)
		r.engine.mu.Unlock()
		stored = append(stored, jobdb.StoredArtifact{Name: art.Name(), Digest: digest, Size: art.Size()})
	}
	return &jobdb.SimpleTaskData{Data: raw, Artifacts: materialized}, stored, nil
}

func (r *Runtime) prepareChapterWrite(ctx context.Context, req jobdb.PutChapterRequest) (jobdb.Chapter, error) {
	chapter := req.Chapter
	if len(req.ArtifactUploads) == 0 {
		if len(chapter.Artifacts) > 0 {
			return jobdb.Chapter{}, fmt.Errorf("put chapter with artifact descriptors but no artifact uploads")
		}
		return chapter, nil
	}

	stored := make([]jobdb.StoredArtifact, 0, len(req.ArtifactUploads))
	for _, item := range req.ArtifactUploads {
		if item.Open == nil {
			return jobdb.Chapter{}, fmt.Errorf("artifact %q missing opener", item.Name)
		}
		rc, err := item.Open()
		if err != nil {
			return jobdb.Chapter{}, err
		}
		data, err := io.ReadAll(rc)
		_ = rc.Close()
		if err != nil {
			return jobdb.Chapter{}, err
		}
		art := jobdb.NewArtifactFromBytes(item.Name, data)
		digest, err := art.Sha256(ctx)
		if err != nil {
			return jobdb.Chapter{}, err
		}
		r.engine.mu.Lock()
		r.engine.runtimeArtifacts[runtimeArtifactKey{
			jobKey:  req.Ref.JobKey,
			ordinal: req.Ref.Ordinal,
			name:    item.Name,
		}] = append([]byte(nil), data...)
		r.engine.mu.Unlock()
		stored = append(stored, jobdb.StoredArtifact{
			Name:   item.Name,
			Digest: digest,
			Size:   int64(len(data)),
		})
	}
	if err := validateStoredArtifactDescriptors(chapter.Artifacts, stored); err != nil {
		return jobdb.Chapter{}, err
	}
	chapter.Artifacts = stored
	return chapter, nil
}

func validateStoredArtifactDescriptors(existing []jobdb.StoredArtifact, computed []jobdb.StoredArtifact) error {
	if len(existing) == 0 {
		return nil
	}
	if len(existing) != len(computed) {
		return fmt.Errorf("chapter artifact metadata count %d does not match uploads %d", len(existing), len(computed))
	}
	for i := range existing {
		if existing[i].Name != computed[i].Name {
			return fmt.Errorf("chapter artifact %d name %q does not match uploaded artifact %q", i, existing[i].Name, computed[i].Name)
		}
		if existing[i].Size != 0 && existing[i].Size != computed[i].Size {
			return fmt.Errorf("chapter artifact %q size %d does not match uploaded size %d", existing[i].Name, existing[i].Size, computed[i].Size)
		}
		if existing[i].Digest != "" && existing[i].Digest != computed[i].Digest {
			return fmt.Errorf("chapter artifact %q digest %q does not match uploaded digest %q", existing[i].Name, existing[i].Digest, computed[i].Digest)
		}
	}
	return nil
}

func (r *Runtime) taskDataFromChapter(jobKey jobdb.JobKey, chapter jobdb.Chapter) (jobdb.TaskData, error) {
	artifacts := make([]jobdb.Artifact, 0, len(chapter.Artifacts))
	for _, art := range chapter.Artifacts {
		artifacts = append(artifacts, jobdb.NewArtifact(art.Name, func() (io.ReadCloser, int64, error) {
			reader, err := r.OpenArtifact(context.Background(), jobdb.ArtifactRef{
				JobKey:  jobKey,
				Ordinal: chapter.Ordinal,
				Name:    art.Name,
				Digest:  art.Digest,
			})
			if err != nil {
				return nil, 0, err
			}
			rc, err := reader.Open()
			return rc, art.Size, err
		}, func() error { return nil }))
	}
	payloadKind, payloadData, err := runtimecodec.ChapterPayload(chapter)
	if err != nil {
		return nil, err
	}
	switch payloadKind {
	case runtimecodec.PayloadKindApp:
		return &jobdb.EnvelopedTaskData{
			SimpleTaskData: jobdb.SimpleTaskData{
				Data:      append([]byte(nil), payloadData...),
				Artifacts: artifacts,
			},
			Kind: payloadKind,
		}, nil
	case runtimecodec.PayloadKindTimeout:
		var payload jobdb.TimeoutPayload
		if err := json.Unmarshal(payloadData, &payload); err != nil {
			return nil, err
		}
		return &jobdb.EnvelopedTaskData{
			SimpleTaskData: jobdb.SimpleTaskData{Data: append([]byte(nil), payloadData...), Artifacts: artifacts},
			Kind:           payloadKind,
		}, jobdb.TimeoutError{Payload: payload}
	case runtimecodec.PayloadKindAppError:
		var payload jobdb.AppErrorPayload
		if err := json.Unmarshal(payloadData, &payload); err != nil {
			return nil, err
		}
		if jobFailedErr, ok := decodeToyJobFailedAppError(payload); ok {
			return &jobdb.EnvelopedTaskData{
				SimpleTaskData: jobdb.SimpleTaskData{Data: append([]byte(nil), payloadData...), Artifacts: artifacts},
				Kind:           payloadKind,
			}, jobFailedErr
		}
		return &jobdb.EnvelopedTaskData{
			SimpleTaskData: jobdb.SimpleTaskData{Data: append([]byte(nil), payloadData...), Artifacts: artifacts},
			Kind:           payloadKind,
		}, jobdb.AppError{Payload: payload}
	case runtimecodec.PayloadKindSystemError:
		var payload jobdb.SystemErrorPayload
		if err := json.Unmarshal(payloadData, &payload); err != nil {
			return nil, err
		}
		return &jobdb.EnvelopedTaskData{
			SimpleTaskData: jobdb.SimpleTaskData{Data: append([]byte(nil), payloadData...), Artifacts: artifacts},
			Kind:           payloadKind,
		}, jobdb.SystemError{Payload: payload}
	default:
		return &jobdb.EnvelopedTaskData{
			SimpleTaskData: jobdb.SimpleTaskData{Data: append([]byte(nil), payloadData...), Artifacts: artifacts},
			Kind:           payloadKind,
		}, nil
	}
}

func (r *Runtime) advanceRecordStateLocked(tenantId string, now time.Time, record *jobRecord) {
	if record.status == jobdb.JobStatusAwaitingFuture && !record.availableAt.IsZero() && !record.availableAt.After(now) {
		record.status = jobdb.JobStatusReady
	}
	if record.status == jobdb.JobStatusPendingJobs && len(record.waitFor) > 0 {
		complete := true
		for _, jobID := range record.waitFor {
			dep := r.engine.jobRecords[jobdb.JobKey{TenantId: tenantId, JobId: jobID}]
			if dep == nil {
				continue
			}
			dep.mu.Lock()
			status := dep.status
			dep.mu.Unlock()
			if status != jobdb.JobStatusCompleted && status != jobdb.JobStatusCancelled {
				complete = false
				break
			}
		}
		if complete {
			if !record.availableAt.IsZero() && record.availableAt.After(now) {
				record.status = jobdb.JobStatusAwaitingFuture
			} else {
				record.status = jobdb.JobStatusReady
			}
			record.waitFor = nil
		}
	}
}

func (r *Runtime) completeLease(ctx context.Context, jobKey jobdb.JobKey, leaseID string, req jobdb.CompleteExecutionRequest) error {
	if req.Chapter == nil {
		return fmt.Errorf("complete lease requires final chapter")
	}
	if !runtimecodec.ChapterIs(*req.Chapter, runtimecodec.ChapterTypeJobAttemptOutcome) {
		return fmt.Errorf("complete lease chapter must be %s", runtimecodec.ChapterTypeJobAttemptOutcome)
	}
	record := r.engine.getJobRecord(jobKey)
	if record == nil {
		return jobdb.ErrJobNotFound
	}
	record.mu.Lock()
	if record.leaseID != leaseID {
		record.mu.Unlock()
		return jobdb.ErrExecutionLeaseLost
	}
	schemaHash := jobmetadata.SchemaHashFromStoredMetadata(record.metadata)
	record.mu.Unlock()

	if _, err := r.ensureCompletionChapter(ctx, jobKey, leaseID, schemaHash, req); err != nil {
		return err
	}

	record.mu.Lock()
	defer record.mu.Unlock()
	if record.leaseID != leaseID {
		return jobdb.ErrExecutionLeaseLost
	}
	record.leased = false
	record.leaseID = ""
	now := time.Now().UTC()
	record.archived = &now
	record.completionDetail = req.Detail
	switch req.Status {
	case "cancelled":
		record.status = jobdb.JobStatusCancelled
		record.cancelled = true
		record.err = jobdb.ErrJobCancelled
	default:
		record.status = jobdb.JobStatusCompleted
	}
	return nil
}

func (r *Runtime) ensureCompletionChapter(ctx context.Context, jobKey jobdb.JobKey, leaseID string, schemaHash string, req jobdb.CompleteExecutionRequest) (jobdb.Chapter, error) {
	ref := jobdb.ChapterRef{JobKey: jobKey, Ordinal: req.Chapter.Ordinal}
	if ref.Ordinal < 0 {
		return jobdb.Chapter{}, fmt.Errorf("chapter ordinal must be >= 0")
	}
	if existing, ok := r.existingRuntimeChapter(jobKey, ref.Ordinal); ok {
		candidate := *req.Chapter
		if len(req.ArtifactUploads) > 0 {
			prepared, err := r.prepareChapterWrite(ctx, jobdb.PutChapterRequest{
				LeaseID:         leaseID,
				Ref:             ref,
				Chapter:         *req.Chapter,
				ArtifactUploads: req.ArtifactUploads,
			})
			if err != nil {
				return jobdb.Chapter{}, err
			}
			candidate = prepared
		}
		if !reflect.DeepEqual(cloneChapter(existing), cloneChapter(candidate)) {
			return jobdb.Chapter{}, fmt.Errorf("%w: chapter ordinal %d already exists with different contents", jobdb.ErrConflict, ref.Ordinal)
		}
		return existing, nil
	}

	chapter, err := r.prepareChapterWrite(ctx, jobdb.PutChapterRequest{
		LeaseID:         leaseID,
		Ref:             ref,
		Chapter:         *req.Chapter,
		ArtifactUploads: req.ArtifactUploads,
	})
	if err != nil {
		return jobdb.Chapter{}, err
	}
	if err := jobschema.ValidateChapter(ctx, r, jobdb.JobSchemaKey{TenantId: jobKey.TenantId, SchemaHash: schemaHash}, chapter); err != nil {
		return jobdb.Chapter{}, err
	}
	if err := r.storeRuntimeChapter(jobKey, ref.Ordinal, chapter); err != nil {
		return jobdb.Chapter{}, err
	}
	return chapter, nil
}

func (r *Runtime) existingRuntimeChapter(jobKey jobdb.JobKey, ordinal int64) (jobdb.Chapter, bool) {
	r.engine.mu.Lock()
	defer r.engine.mu.Unlock()
	chapters := r.engine.runtimeChapters[jobKey]
	if chapters == nil {
		return jobdb.Chapter{}, false
	}
	chapter, ok := chapters[ordinal]
	if !ok {
		return jobdb.Chapter{}, false
	}
	return cloneChapter(chapter), true
}

func (r *Runtime) rescheduleLease(jobKey jobdb.JobKey, leaseID string, req jobdb.RescheduleExecutionRequest) error {
	record := r.engine.getJobRecord(jobKey)
	if record == nil {
		return jobdb.ErrJobNotFound
	}
	record.mu.Lock()
	defer record.mu.Unlock()
	if record.leaseID != leaseID {
		return jobdb.ErrExecutionLeaseLost
	}
	record.leased = false
	record.leaseID = ""
	record.capability = req.NextNeed
	record.payload = cloneJSON(req.Payload)
	record.waitFor = append([]string(nil), req.WaitForJobIDs...)
	record.availableAt = time.Time{}
	record.step = 0
	if wait, err := extractWorkerTaskWait(record.payload); err == nil && wait != nil {
		record.step = wait.OutputStep
		if _, exists := record.chapters[wait.OutputStep]; !exists {
			var input jobdb.TaskData
			if prev := record.chapters[wait.InputStep]; prev != nil {
				if prev.Output != nil {
					input = prev.Output
				} else {
					input = prev.Input
				}
			}
			record.chapters[wait.OutputStep] = &toyChapter{
				TaskType:  extractTaskType(req.NextNeed),
				CreatedAt: time.Now().UTC(),
				Input:     input,
				Attempt:   1,
			}
		}
	}
	switch {
	case len(req.WaitForJobIDs) > 0:
		record.status = jobdb.JobStatusPendingJobs
	case req.WaitUntil != nil && req.WaitUntil.After(time.Now().UTC()):
		record.status = jobdb.JobStatusAwaitingFuture
		record.availableAt = req.WaitUntil.UTC()
	default:
		record.status = jobdb.JobStatusReady
	}
	return nil
}

type runtimeLease struct {
	runtime    *Runtime
	jobKey     jobdb.JobKey
	leaseID    string
	capability string
	payload    json.RawMessage
	expiresAt  time.Time
	duration   time.Duration
	schemaHash string
}

func (l *runtimeLease) LeaseID() string          { return l.leaseID }
func (l *runtimeLease) Job() jobdb.JobHandle     { return jobdb.JobHandle{JobKey: l.jobKey} }
func (l *runtimeLease) Capability() string       { return l.capability }
func (l *runtimeLease) Payload() json.RawMessage { return append(json.RawMessage(nil), l.payload...) }
func (l *runtimeLease) LeaseExpiry() time.Time   { return l.expiresAt }
func (l *runtimeLease) LeaseSchemaHash() string  { return l.schemaHash }
func (l *runtimeLease) KeepAlive(context.Context) error {
	l.expiresAt = time.Now().UTC().Add(toyLeaseDurationOrDefault(l.duration))
	return nil
}
func (l *runtimeLease) StopKeepAlive() {}
func (l *runtimeLease) Complete(ctx context.Context, req jobdb.CompleteExecutionRequest) error {
	return l.runtime.completeLease(ctx, l.jobKey, l.leaseID, req)
}
func (l *runtimeLease) Reschedule(ctx context.Context, req jobdb.RescheduleExecutionRequest) error {
	return l.runtime.rescheduleLease(l.jobKey, l.leaseID, req)
}

func toyLeaseDurationOrDefault(d time.Duration) time.Duration {
	if d <= 0 {
		return defaultToyRemoteLeaseDuration
	}
	return d
}

type runtimeTaskHandle struct {
	runtime   *Runtime
	jobKey    jobdb.JobKey
	payload   json.RawMessage
	metadata  json.RawMessage
	wait      workerTaskWait
	taskType  string
	createdAt time.Time
}

func (h *runtimeTaskHandle) JobKey() jobdb.JobKey         { return h.jobKey }
func (h *runtimeTaskHandle) TaskOrdinalToComplete() int64 { return h.wait.OutputStep }
func (h *runtimeTaskHandle) TaskType() string             { return h.taskType }
func (h *runtimeTaskHandle) CreatedAt() time.Time         { return h.createdAt }
func (h *runtimeTaskHandle) Metadata() json.RawMessage    { return cloneJSON(h.metadata) }

func (h *runtimeTaskHandle) Data() (jobdb.TaskData, error) {
	ref := jobdb.ChapterRef{JobKey: h.jobKey, Ordinal: h.wait.InputStep}
	chapter, err := h.runtime.GetChapter(context.Background(), ref)
	if err != nil {
		return nil, err
	}
	return h.runtime.taskDataFromChapter(h.jobKey, chapter)
}

func (h *runtimeTaskHandle) Finish(ctx context.Context, taskData jobdb.TaskData) error {
	return h.runtime.CompleteTaskIfWaiting(ctx, jobdb.CompleteTaskIfWaitingRequest{
		JobKey:        h.jobKey,
		Capability:    jobdb.JobTypeFromNextNeed(h.wait.Next) + ":" + h.taskType,
		ResumeNeed:    h.wait.Next,
		InputOrdinal:  h.wait.InputStep,
		OutputOrdinal: h.wait.OutputStep,
		InputHash:     h.wait.InputHash,
		Data:          taskData,
	})
}

func (r *Runtime) CompleteTaskIfWaiting(ctx context.Context, req jobdb.CompleteTaskIfWaitingRequest) error {
	record := r.engine.getJobRecord(req.JobKey)
	if record == nil {
		return jobdb.ErrJobNotFound
	}
	payloadInfo := workerJobPayload{}

	record.mu.Lock()
	payload := cloneJSON(record.payload)
	currentCapability := record.capability
	record.mu.Unlock()

	_ = json.Unmarshal(payload, &payloadInfo)
	wait, err := extractWorkerTaskWait(payload)
	if err != nil {
		return err
	}
	if wait == nil {
		return fmt.Errorf("%w: job is not waiting on an external task", jobdb.ErrConflict)
	}
	if req.Capability != "" && currentCapability != req.Capability {
		return fmt.Errorf("%w: waiting capability %q does not match requested capability %q", jobdb.ErrConflict, currentCapability, req.Capability)
	}
	if req.ResumeNeed != "" && wait.Next != req.ResumeNeed {
		return fmt.Errorf("%w: resume need %q does not match requested resume need %q", jobdb.ErrConflict, wait.Next, req.ResumeNeed)
	}
	if req.InputOrdinal != 0 && wait.InputStep != req.InputOrdinal {
		return fmt.Errorf("%w: waiting input ordinal %d does not match requested input ordinal %d", jobdb.ErrConflict, wait.InputStep, req.InputOrdinal)
	}
	if wait.OutputStep != req.OutputOrdinal {
		return fmt.Errorf("%w: waiting output ordinal %d does not match requested output ordinal %d", jobdb.ErrConflict, wait.OutputStep, req.OutputOrdinal)
	}
	if req.InputHash != "" && wait.InputHash != req.InputHash {
		return fmt.Errorf("%w: waiting input hash does not match requested input hash", jobdb.ErrConflict)
	}

	td, storedArtifacts, err := r.materializeTaskData(ctx, req.JobKey, wait.OutputStep, req.Data)
	if err != nil {
		return err
	}
	dataPayload, err := td.GetData()
	if err != nil {
		return err
	}
	metadata, err := marshalChapterMetadata(map[string]any{
		"version":    1,
		"ordinal":    wait.OutputStep,
		"task_type":  extractTaskType(currentCapability),
		"created_at": time.Now().UTC(),
		"input_hash": wait.InputHash,
		"attempt":    1,
		"run_policy": payloadInfo.RunPolicy,
	})
	if err != nil {
		return err
	}
	taskType := extractTaskType(currentCapability)
	if taskType == "" || taskType == currentCapability {
		return fmt.Errorf("task type not found in capability")
	}
	chapter := jobdb.Chapter{
		Ordinal:   wait.OutputStep,
		TaskType:  taskType,
		InputHash: wait.InputHash,
		CreatedAt: time.Now().UTC(),
		Metadata:  metadata,
		Body: jobdb.TaskAttemptOutcomeChapter{Outcome: jobdb.ApplicationOutputOutcome{
			Output: jobdb.ApplicationOutputBytes{Data: append([]byte(nil), dataPayload...)},
		}},
		Artifacts: storedArtifacts,
	}
	record.mu.Lock()
	schemaHash := jobmetadata.SchemaHashFromStoredMetadata(record.metadata)
	record.mu.Unlock()
	if err := jobschema.ValidateChapter(ctx, r, jobdb.JobSchemaKey{TenantId: req.JobKey.TenantId, SchemaHash: schemaHash}, chapter); err != nil {
		return err
	}
	if err := r.storeRuntimeChapter(req.JobKey, wait.OutputStep, chapter); err != nil {
		return err
	}
	record = r.engine.getJobRecord(req.JobKey)
	if record == nil {
		return jobdb.ErrJobNotFound
	}
	record.mu.Lock()
	resumeNeed := wait.Next
	if req.ResumeNeed != "" {
		resumeNeed = req.ResumeNeed
	}
	record.capability = resumeNeed
	record.payload = mustMarshalWorkerPayload(workerJobPayload{RunPolicy: payloadInfo.RunPolicy})
	record.status = jobdb.JobStatusReady
	record.leased = false
	record.leaseID = ""
	record.step = wait.OutputStep
	record.mu.Unlock()
	return nil
}

func extractWorkerTaskWait(payload json.RawMessage) (*workerTaskWait, error) {
	if len(payload) == 0 {
		return nil, nil
	}
	var parsed workerJobPayload
	if err := json.Unmarshal(payload, &parsed); err != nil {
		return nil, err
	}
	return parsed.TaskWait, nil
}

func extractRunPolicyFromMetadata(metadata jobdb.ChapterMetadata) (jobdb.RunPolicy, error) {
	raw, err := runtimecodec.ChapterMetadataToJSON(metadata)
	if err != nil {
		return jobdb.RunPolicy{}, err
	}
	if len(raw) == 0 {
		return jobdb.RunPolicy{}, nil
	}
	var payload struct {
		RunPolicy jobdb.RunPolicy `json:"run_policy"`
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return jobdb.RunPolicy{}, err
	}
	return payload.RunPolicy, nil
}

func extractPrerequisitesFromMetadata(metadata jobdb.ChapterMetadata) ([]jobdb.JobPrerequisite, error) {
	raw, err := runtimecodec.ChapterMetadataToJSON(metadata)
	if err != nil {
		return nil, err
	}
	if len(raw) == 0 {
		return nil, nil
	}
	var payload struct {
		Prereqs []jobdb.JobPrerequisite `json:"prereqs"`
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return nil, err
	}
	return append([]jobdb.JobPrerequisite(nil), payload.Prereqs...), nil
}

func extractAttemptFromMetadata(metadata jobdb.ChapterMetadata) (int, error) {
	raw, err := runtimecodec.ChapterMetadataToJSON(metadata)
	if err != nil {
		return 0, err
	}
	if len(raw) == 0 {
		return 1, nil
	}
	var payload struct {
		Attempt int `json:"attempt"`
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return 0, err
	}
	if payload.Attempt == 0 {
		return 1, nil
	}
	return payload.Attempt, nil
}

func marshalChapterMetadata(v map[string]any) (jobdb.ChapterMetadata, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return jobdb.ChapterMetadata{}, err
	}
	return runtimecodec.ChapterMetadataFromJSON(raw)
}

func mustMarshalWorkerPayload(v workerJobPayload) json.RawMessage {
	raw, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return raw
}

func jobdbInputHash(ctx context.Context, data jobdb.TaskData) (string, error) {
	if data == nil {
		return "", nil
	}
	raw, err := data.GetData()
	if err != nil {
		return "", err
	}
	artifacts, err := data.GetArtifacts()
	if err != nil {
		return "", err
	}
	parts := make([]string, 0, len(artifacts))
	for _, art := range artifacts {
		if art == nil {
			continue
		}
		hash, err := art.Sha256(ctx)
		if err != nil {
			return "", err
		}
		parts = append(parts, art.Name()+"|"+hash)
	}
	sort.Strings(parts)
	h := sha256.New()
	_, _ = h.Write(raw)
	for _, part := range parts {
		_, _ = h.Write([]byte(part))
	}
	return fmt.Sprintf("%x", h.Sum(nil)), nil
}

func cloneChapter(chapter jobdb.Chapter) jobdb.Chapter {
	return runtimecodec.CloneChapter(chapter)
}

func cloneJSON(raw json.RawMessage) json.RawMessage {
	if len(raw) == 0 {
		return nil
	}
	out := make([]byte, len(raw))
	copy(out, raw)
	return out
}

func decodeToyJobFailedAppError(payload jobdb.AppErrorPayload) (error, bool) {
	attrs := payload.Attrs
	if len(attrs) == 0 {
		return nil, false
	}
	raw, ok := attrs["_jobdb_job_failed"]
	if !ok {
		return nil, false
	}
	marked, ok := raw.(bool)
	if !ok || !marked {
		return nil, false
	}

	switch kind, _ := attrs["_jobdb_job_failed_kind"].(string); kind {
	case jobdb.TaskErrorKindTimeout:
		return &jobdb.JobFailedError{Cause: &jobdb.TimeoutError{Payload: jobdb.TimeoutPayload{
			Scope:     toyAttrString(attrs, "_jobdb_job_failed_scope"),
			After:     toyAttrDuration(attrs, "_jobdb_job_failed_after"),
			Retryable: toyAttrBool(attrs, "_jobdb_job_failed_retryable"),
			InputRef:  payload.InputRef,
			Component: toyAttrString(attrs, "_jobdb_job_failed_component"),
			Code:      toyAttrString(attrs, "_jobdb_job_failed_code"),
			Message:   payload.Message,
		}}}, true
	case jobdb.TaskErrorKindSystem:
		return &jobdb.JobFailedError{Cause: &jobdb.SystemError{Payload: jobdb.SystemErrorPayload{
			Message:    payload.Message,
			Component:  toyAttrString(attrs, "_jobdb_job_failed_component"),
			Code:       toyAttrString(attrs, "_jobdb_job_failed_code"),
			Retryable:  toyAttrBool(attrs, "_jobdb_job_failed_retryable"),
			InputRef:   payload.InputRef,
			Stacktrace: append([]string(nil), payload.Stacktrace...),
		}}}, true
	default:
		return &jobdb.JobFailedError{Cause: &jobdb.AppError{Payload: jobdb.AppErrorPayload{
			Message:    payload.Message,
			Level:      payload.Level,
			Attrs:      toyStripJobFailedAttrs(attrs),
			InputRef:   payload.InputRef,
			Stacktrace: append([]string(nil), payload.Stacktrace...),
		}}}, true
	}
}

func toyStripJobFailedAttrs(attrs map[string]interface{}) map[string]interface{} {
	if len(attrs) == 0 {
		return nil
	}
	out := make(map[string]interface{}, len(attrs))
	for key, value := range attrs {
		switch key {
		case "_jobdb_job_failed", "_jobdb_job_failed_kind", "_jobdb_job_failed_code", "_jobdb_job_failed_component", "_jobdb_job_failed_retryable", "_jobdb_job_failed_scope", "_jobdb_job_failed_after":
			continue
		default:
			out[key] = value
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func toyAttrString(attrs map[string]interface{}, key string) string {
	value, _ := attrs[key].(string)
	return value
}

func toyAttrBool(attrs map[string]interface{}, key string) bool {
	value, _ := attrs[key].(bool)
	return value
}

func toyAttrDuration(attrs map[string]interface{}, key string) jobdb.Duration {
	value, _ := attrs[key].(string)
	if value == "" {
		return 0
	}
	d, err := time.ParseDuration(value)
	if err != nil {
		return 0
	}
	return jobdb.Duration(d)
}
