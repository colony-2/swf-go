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

	"github.com/colony-2/swf-go/pkg/swf"
	"github.com/colony-2/swf-go/pkg/swf/internal/runtimecodec"
	"github.com/segmentio/ksuid"
)

type workerJobPayload struct {
	RunPolicy swf.RunPolicy   `json:"run_policy,omitempty"`
	TaskWait  *workerTaskWait `json:"task_wait,omitempty"`
}

type workerTaskWait struct {
	InputStep  int64  `json:"in"`
	OutputStep int64  `json:"out"`
	Next       string `json:"next"`
	InputHash  string `json:"input_hash,omitempty"`
}

// Runtime is an in-memory WorkflowRuntime used by the shared SWF engine.
type Runtime struct {
	engine *ToyEngine
}

var _ swf.WorkflowRuntime = (*Runtime)(nil)

func New(opts ...Option) *Runtime {
	engine := NewToyEngine(opts...)
	return &Runtime{engine: engine}
}

func (r *Runtime) SubmitJob(ctx context.Context, req swf.SubmitJobRequest) (swf.JobHandle, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return swf.JobHandle{}, err
	}
	var jobKey swf.JobKey
	if req.Job.JobID != "" {
		jobKey = swf.JobKey{TenantId: req.Job.TenantId, JobId: req.Job.JobID}
	} else {
		var err error
		jobKey, err = r.engine.idGenerator(req.Job.TenantId)
		if err != nil {
			return swf.JobHandle{}, err
		}
	}
	if err := jobKey.Validate(); err != nil {
		return swf.JobHandle{}, err
	}
	if req.Job.JobID != "" {
		inputHash, err := swfInputHash(ctx, req.Job.Data)
		if err != nil {
			return swf.JobHandle{}, err
		}
		if handle, ok, err := r.existingEquivalentJob(jobKey, req.Job, inputHash); ok || err != nil {
			return handle, err
		}
	}

	jobData, storedArtifacts, err := r.materializeTaskData(ctx, jobKey, 0, req.Job.Data)
	if err != nil {
		return swf.JobHandle{}, err
	}
	inputHash, err := swfInputHash(ctx, jobData)
	if err != nil {
		return swf.JobHandle{}, err
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
		return swf.JobHandle{}, err
	}
	payload, err := jobData.GetData()
	if err != nil {
		return swf.JobHandle{}, err
	}
	stored := swf.Chapter{
		Ordinal:   0,
		TaskType:  req.Job.JobType,
		InputHash: inputHash,
		CreatedAt: now,
		Metadata:  metadata,
		Body:      swf.JobStartChapter{Input: swf.ApplicationInputBytes{Data: append([]byte(nil), payload...)}},
		Artifacts: storedArtifacts,
	}

	payloadJSON, err := json.Marshal(workerJobPayload{
		RunPolicy: runPolicy,
	})
	if err != nil {
		return swf.JobHandle{}, err
	}

	record := &jobRecord{
		status:      swf.JobStatusReady,
		jobType:     req.Job.JobType,
		createdAt:   now,
		metadata:    cloneJSON(req.Job.Metadata),
		payload:     payloadJSON,
		capability:  req.Job.JobType,
		chapters:    make(map[int64]*toyChapter),
		availableAt: now,
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
		r.engine.runtimeChapters[jobKey] = make(map[int64]swf.Chapter)
	}
	r.engine.runtimeChapters[jobKey][0] = stored
	r.engine.mu.Unlock()

	return swf.JobHandle{JobKey: jobKey}, nil
}

func (r *Runtime) existingEquivalentJob(jobKey swf.JobKey, job swf.SubmitJob, inputHash string) (swf.JobHandle, bool, error) {
	r.engine.mu.Lock()
	defer r.engine.mu.Unlock()

	record, ok := r.engine.jobRecords[jobKey]
	if !ok {
		return swf.JobHandle{}, false, nil
	}
	start, ok := r.engine.runtimeChapters[jobKey][0]
	if !ok {
		return swf.JobHandle{}, false, swf.NewExistingJobMismatchError(fmt.Sprintf("job %s already exists without start chapter", jobKey))
	}
	if start.TaskType != job.JobType {
		return swf.JobHandle{}, false, swf.NewExistingJobMismatchError(fmt.Sprintf("job %s already exists with different job type", jobKey))
	}
	if start.InputHash != inputHash {
		return swf.JobHandle{}, false, swf.NewExistingJobMismatchError(fmt.Sprintf("job %s already exists with different input", jobKey))
	}
	if !bytes.Equal(record.metadata, job.Metadata) {
		return swf.JobHandle{}, false, swf.NewExistingJobMismatchError(fmt.Sprintf("job %s already exists with different metadata", jobKey))
	}
	runPolicy, err := extractRunPolicyFromMetadata(start.Metadata)
	if err != nil {
		return swf.JobHandle{}, false, err
	}
	if !reflect.DeepEqual(runPolicy, job.RunPolicy) {
		return swf.JobHandle{}, false, swf.NewExistingJobMismatchError(fmt.Sprintf("job %s already exists with different run policy", jobKey))
	}
	prereqs, err := extractPrerequisitesFromMetadata(start.Metadata)
	if err != nil {
		return swf.JobHandle{}, false, err
	}
	if !reflect.DeepEqual(prereqs, job.Prerequisites) {
		return swf.JobHandle{}, false, swf.NewExistingJobMismatchError(fmt.Sprintf("job %s already exists with different prerequisites", jobKey))
	}
	return swf.JobHandle{JobKey: jobKey}, true, nil
}

func (r *Runtime) SubmitRestartJob(ctx context.Context, req swf.SubmitRestartJobRequest) (swf.JobHandle, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if req.Job.LastStepToKeep < 0 {
		return swf.JobHandle{}, fmt.Errorf("LastStepToKeep must be >= 0 for restart")
	}
	prior := r.engine.getJobRecord(req.Job.PriorJobKey)
	if prior == nil {
		return swf.JobHandle{}, swf.ErrJobNotFound
	}
	var jobKey swf.JobKey
	if req.Job.JobID != "" {
		jobKey = swf.JobKey{TenantId: req.Job.PriorJobKey.TenantId, JobId: req.Job.JobID}
	} else {
		genKey, err := r.engine.idGenerator(req.Job.PriorJobKey.TenantId)
		if err != nil {
			return swf.JobHandle{}, err
		}
		jobKey = genKey
	}

	r.engine.mu.Lock()
	defer r.engine.mu.Unlock()

	sourceChapters := r.engine.runtimeChapters[req.Job.PriorJobKey]
	if sourceChapters == nil {
		return swf.JobHandle{}, swf.ErrJobNotFound
	}
	nextOrdinal := req.Job.LastStepToKeep + 1
	nextChapter, ok := sourceChapters[nextOrdinal]
	if !ok {
		return swf.JobHandle{}, fmt.Errorf("LastStepToKeep %d invalid: no chapter at ordinal %d", req.Job.LastStepToKeep, nextOrdinal)
	}
	nextAttempt, err := extractAttemptFromMetadata(nextChapter.Metadata)
	if err != nil {
		return swf.JobHandle{}, err
	}
	if nextAttempt > 1 {
		taskType := nextChapter.TaskType
		if taskType == "" {
			if chap0, ok := sourceChapters[0]; ok {
				taskType = chap0.TaskType
			}
		}
		return swf.JobHandle{}, fmt.Errorf("LastStepToKeep %d cuts into retry chain: next ordinal %d is attempt %d of %s", req.Job.LastStepToKeep, nextOrdinal, nextAttempt, taskType)
	}
	targetChapters := make(map[int64]swf.Chapter)
	for ordinal, chapter := range sourceChapters {
		if ordinal > req.Job.LastStepToKeep {
			continue
		}
		targetChapters[ordinal] = cloneChapter(chapter)
	}
	if _, ok := targetChapters[0]; !ok {
		return swf.JobHandle{}, swf.ErrJobNotFound
	}
	if req.Job.JobID != "" {
		if handle, ok, err := r.existingEquivalentRestartJob(jobKey, req.Job, targetChapters); ok || err != nil {
			return handle, err
		}
	}

	chap0 := targetChapters[0]
	jobType := chap0.TaskType
	meta, err := extractRunPolicyFromMetadata(chap0.Metadata)
	if err != nil {
		return swf.JobHandle{}, err
	}
	payloadJSON, err := json.Marshal(workerJobPayload{RunPolicy: meta})
	if err != nil {
		return swf.JobHandle{}, err
	}

	record := &jobRecord{
		status:      swf.JobStatusReady,
		jobType:     jobType,
		createdAt:   time.Now().UTC(),
		payload:     payloadJSON,
		capability:  jobType,
		chapters:    make(map[int64]*toyChapter),
		availableAt: time.Now().UTC(),
	}
	for ordinal, chapter := range targetChapters {
		td, decodeErr := r.taskDataFromChapter(jobKey, chapter)
		if decodeErr != nil {
			return swf.JobHandle{}, decodeErr
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
	return swf.JobHandle{JobKey: jobKey}, nil
}

func (r *Runtime) existingEquivalentRestartJob(jobKey swf.JobKey, job swf.SubmitRestartJob, expected map[int64]swf.Chapter) (swf.JobHandle, bool, error) {
	record, ok := r.engine.jobRecords[jobKey]
	if !ok {
		return swf.JobHandle{}, false, nil
	}
	if record == nil {
		return swf.JobHandle{}, false, swf.NewExistingJobMismatchError(fmt.Sprintf("job %s already exists without state", jobKey))
	}
	existing := r.engine.runtimeChapters[jobKey]
	if existing == nil {
		return swf.JobHandle{}, false, swf.NewExistingJobMismatchError(fmt.Sprintf("job %s already exists without runtime chapters", jobKey))
	}
	for ordinal := int64(0); ordinal <= job.LastStepToKeep; ordinal++ {
		want, ok := expected[ordinal]
		if !ok {
			return swf.JobHandle{}, false, swf.NewExistingJobMismatchError(fmt.Sprintf("job %s is missing expected restart chapter %d", jobKey, ordinal))
		}
		got, ok := existing[ordinal]
		if !ok {
			return swf.JobHandle{}, false, swf.NewExistingJobMismatchError(fmt.Sprintf("job %s already exists with different restart history at ordinal %d", jobKey, ordinal))
		}
		if !reflect.DeepEqual(cloneChapter(got), cloneChapter(want)) {
			return swf.JobHandle{}, false, swf.NewExistingJobMismatchError(fmt.Sprintf("job %s already exists with different restart history at ordinal %d", jobKey, ordinal))
		}
	}
	if next, ok := existing[job.LastStepToKeep+1]; ok && runtimecodec.ChapterIs(next, runtimecodec.ChapterTypeRestartExtra) && job.ExtraTaskOutput == nil {
		return swf.JobHandle{}, false, swf.NewExistingJobMismatchError(fmt.Sprintf("job %s already exists with restart extra output that was not requested", jobKey))
	}
	return swf.JobHandle{JobKey: jobKey}, true, nil
}

func (r *Runtime) CancelJob(ctx context.Context, req swf.CancelJobRequest) error {
	record := r.engine.getJobRecord(req.JobKey)
	if record == nil {
		return swf.ErrJobNotFound
	}
	record.mu.Lock()
	defer record.mu.Unlock()
	record.cancelled = true
	record.status = swf.JobStatusCancelled
	record.err = context.Canceled
	now := time.Now().UTC()
	record.archived = &now
	record.leased = false
	record.leaseID = ""
	return nil
}

func (r *Runtime) PollWork(ctx context.Context, req swf.PollWorkRequest) ([]swf.ExecutionLease, error) {
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
	defer r.engine.mu.Unlock()

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
	out := make([]swf.ExecutionLease, 0, limit)
	for key, record := range r.engine.jobRecords {
		if key.TenantId != req.TenantId {
			continue
		}
		record.mu.Lock()
		r.advanceRecordStateLocked(key.TenantId, now, record)
		if record.leased || record.status != swf.JobStatusReady {
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
				return nil, err
			}
			if !match {
				record.mu.Unlock()
				continue
			}
		}
		record.leased = true
		record.status = swf.JobStatusActive
		record.leaseID = ksuid.New().String()
		payload := cloneJSON(record.payload)
		out = append(out, &runtimeLease{
			runtime:    r,
			jobKey:     key,
			leaseID:    record.leaseID,
			capability: record.capability,
			payload:    payload,
		})
		record.mu.Unlock()
		if len(out) >= limit {
			break
		}
	}
	return out, nil
}

func (r *Runtime) GetJobLease(ctx context.Context, req swf.GetJobLeaseRequest) (swf.ExecutionLease, error) {
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
	defer r.engine.mu.Unlock()

	record := r.engine.jobRecords[req.JobKey]
	if record == nil {
		return nil, nil
	}

	now := time.Now().UTC()
	record.mu.Lock()
	defer record.mu.Unlock()
	r.advanceRecordStateLocked(req.JobKey.TenantId, now, record)
	if record.leased || record.status != swf.JobStatusReady {
		return nil, nil
	}
	if _, ok := capSet[record.capability]; !ok {
		return nil, nil
	}

	record.leased = true
	record.status = swf.JobStatusActive
	record.leaseID = ksuid.New().String()
	return &runtimeLease{
		runtime:    r,
		jobKey:     req.JobKey,
		leaseID:    record.leaseID,
		capability: record.capability,
		payload:    cloneJSON(record.payload),
	}, nil
}

type jobInfoTaskData struct {
	taskData swf.TaskData
	err      error
}

func (d *jobInfoTaskData) GetData() (swf.Data, error) {
	if d.taskData == nil {
		return nil, d.err
	}
	data, err := d.taskData.GetData()
	if err != nil {
		return data, err
	}
	return data, d.err
}

func (d *jobInfoTaskData) GetDataOrPanic() swf.Data {
	data, err := d.GetData()
	if err != nil {
		panic(err)
	}
	return data
}

func (d *jobInfoTaskData) GetArtifacts() ([]swf.Artifact, error) {
	if d.taskData == nil {
		return nil, d.err
	}
	return d.taskData.GetArtifacts()
}

func (d *jobInfoTaskData) TaskDataResult() (swf.TaskData, error) {
	return d.taskData, d.err
}

func (r *Runtime) GetJob(ctx context.Context, jobKey swf.JobKey) (swf.JobInfo, error) {
	record := r.engine.getJobRecord(jobKey)
	if record == nil {
		return swf.JobInfo{}, swf.ErrJobNotFound
	}
	record.mu.Lock()
	defer record.mu.Unlock()
	job := swf.JobInfo{
		Status: record.status,
		Data:   &jobInfoTaskData{err: swf.ErrJobNotComplete},
	}
	if record.status == swf.JobStatusCompleted || record.status == swf.JobStatusCancelled {
		job.Data = &jobInfoTaskData{taskData: record.result, err: record.err}
	}
	return job, nil
}

func (r *Runtime) GetJobRun(ctx context.Context, req swf.GetJobRunRequest) (swf.GetJobRunResponse, error) {
	return r.engine.GetJobRun(ctx, req)
}

func (r *Runtime) ListJobs(ctx context.Context, req swf.ListJobsRequest) (swf.ListJobsResponse, error) {
	return r.engine.ListJobs(ctx, req)
}

func (r *Runtime) GetChapter(ctx context.Context, ref swf.ChapterRef) (swf.Chapter, error) {
	r.engine.mu.Lock()
	defer r.engine.mu.Unlock()
	chapters := r.engine.runtimeChapters[ref.JobKey]
	if chapters == nil {
		return swf.Chapter{}, swf.ErrChapterNotFound
	}
	chapter, ok := chapters[ref.Ordinal]
	if !ok {
		return swf.Chapter{}, swf.ErrChapterNotFound
	}
	return cloneChapter(chapter), nil
}

func (r *Runtime) ListChapters(ctx context.Context, req swf.ListChaptersRequest) ([]swf.Chapter, error) {
	if req.StartOrdinal < 0 {
		return nil, fmt.Errorf("start ordinal must be >= 0")
	}
	r.engine.mu.Lock()
	defer r.engine.mu.Unlock()

	chapters := r.engine.runtimeChapters[req.JobKey]
	if chapters == nil {
		return nil, swf.ErrJobNotFound
	}

	ordinals := make([]int64, 0, len(chapters))
	for ordinal := range chapters {
		ordinals = append(ordinals, ordinal)
	}
	sort.Slice(ordinals, func(i, j int) bool { return ordinals[i] < ordinals[j] })

	out := make([]swf.Chapter, 0, len(ordinals))
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

func (r *Runtime) PutChapter(ctx context.Context, req swf.PutChapterRequest) error {
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
		return swf.ErrJobNotFound
	}
	record.mu.Lock()
	if record.leaseID != req.LeaseID {
		record.mu.Unlock()
		return swf.ErrExecutionLeaseLost
	}
	record.mu.Unlock()

	chapter, err := r.prepareChapterWrite(ctx, req)
	if err != nil {
		return err
	}
	return r.storeRuntimeChapter(req.Ref.JobKey, req.Ref.Ordinal, chapter)
}

func (r *Runtime) storeRuntimeChapter(jobKey swf.JobKey, ordinal int64, chapter swf.Chapter) error {
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
		return swf.ErrJobNotFound
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
		return fmt.Errorf("%w: chapter ordinal %d already exists", swf.ErrConflict, ordinal)
	}
	if ordinal > 0 {
		if _, exists := chapters[ordinal-1]; !exists {
			r.engine.mu.Unlock()
			return fmt.Errorf("%w: chapter ordinal %d is not appendable", swf.ErrConflict, ordinal)
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
			record.result = &swf.EnvelopedTaskData{
				SimpleTaskData: swf.SimpleTaskData{
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

func (r *Runtime) OpenArtifact(ctx context.Context, ref swf.ArtifactRef) (swf.ArtifactReader, error) {
	return r.engine.OpenStoredArtifact(ctx, ref)
}

func (r *Runtime) FindTasksWaitingForCapability(ctx context.Context, jobType string, taskType string, tenantIds []string) ([]swf.TaskHandle, error) {
	resp, err := r.ListJobs(ctx, swf.ListJobsRequest{
		TenantIds: tenantIds,
		Statuses:  []swf.JobStatus{swf.JobStatusReady},
		JobTasks:  []swf.JobTaskFilter{{JobType: jobType, TaskType: taskType}},
		PageSize:  swf.MaxListJobsPageSize,
	})
	if err != nil {
		return nil, err
	}
	handles := make([]swf.TaskHandle, 0, len(resp.Jobs))
	for _, job := range resp.Jobs {
		record := r.engine.getJobRecord(job.JobKey)
		if record == nil {
			continue
		}
		record.mu.Lock()
		payload := cloneJSON(record.payload)
		metadata := cloneJSON(record.metadata)
		createdAt := record.createdAt
		record.mu.Unlock()
		wait, err := extractWorkerTaskWait(payload)
		if err != nil || wait == nil {
			continue
		}
		handles = append(handles, &runtimeTaskHandle{
			runtime:   r,
			jobKey:    job.JobKey,
			payload:   payload,
			metadata:  metadata,
			wait:      *wait,
			taskType:  taskType,
			createdAt: createdAt,
		})
	}
	return handles, nil
}

func (r *Runtime) GetWaitingTask(ctx context.Context, key swf.JobKey) (swf.TaskHandle, error) {
	record := r.engine.getJobRecord(key)
	if record == nil {
		return nil, swf.ErrJobNotFound
	}
	record.mu.Lock()
	payload := cloneJSON(record.payload)
	metadata := cloneJSON(record.metadata)
	createdAt := record.createdAt
	currentCapability := record.capability
	record.mu.Unlock()

	wait, err := extractWorkerTaskWait(payload)
	if err != nil {
		return nil, err
	}
	if wait == nil {
		return nil, swf.ErrJobNotFound
	}
	taskType := extractTaskType(currentCapability)
	return &runtimeTaskHandle{
		runtime:   r,
		jobKey:    key,
		payload:   payload,
		metadata:  metadata,
		wait:      *wait,
		taskType:  taskType,
		createdAt: createdAt,
	}, nil
}

func (r *Runtime) materializeTaskData(ctx context.Context, jobKey swf.JobKey, ordinal int64, data swf.TaskData) (swf.TaskData, []swf.StoredArtifact, error) {
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
	stored := make([]swf.StoredArtifact, 0, len(materialized))
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
		stored = append(stored, swf.StoredArtifact{Name: art.Name(), Digest: digest, Size: art.Size()})
	}
	return &swf.SimpleTaskData{Data: raw, Artifacts: materialized}, stored, nil
}

func (r *Runtime) prepareChapterWrite(ctx context.Context, req swf.PutChapterRequest) (swf.Chapter, error) {
	chapter := req.Chapter
	if len(req.ArtifactUploads) == 0 {
		return chapter, nil
	}

	stored := make([]swf.StoredArtifact, 0, len(req.ArtifactUploads))
	for _, item := range req.ArtifactUploads {
		if item.Open == nil {
			return swf.Chapter{}, fmt.Errorf("artifact %q missing opener", item.Name)
		}
		rc, err := item.Open()
		if err != nil {
			return swf.Chapter{}, err
		}
		data, err := io.ReadAll(rc)
		_ = rc.Close()
		if err != nil {
			return swf.Chapter{}, err
		}
		art := swf.NewArtifactFromBytes(item.Name, data)
		digest, err := art.Sha256(ctx)
		if err != nil {
			return swf.Chapter{}, err
		}
		r.engine.mu.Lock()
		r.engine.runtimeArtifacts[runtimeArtifactKey{
			jobKey:  req.Ref.JobKey,
			ordinal: req.Ref.Ordinal,
			name:    item.Name,
		}] = append([]byte(nil), data...)
		r.engine.mu.Unlock()
		stored = append(stored, swf.StoredArtifact{
			Name:   item.Name,
			Digest: digest,
			Size:   int64(len(data)),
		})
	}
	if err := validateStoredArtifactDescriptors(chapter.Artifacts, stored); err != nil {
		return swf.Chapter{}, err
	}
	chapter.Artifacts = stored
	return chapter, nil
}

func validateStoredArtifactDescriptors(existing []swf.StoredArtifact, computed []swf.StoredArtifact) error {
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

func (r *Runtime) taskDataFromChapter(jobKey swf.JobKey, chapter swf.Chapter) (swf.TaskData, error) {
	artifacts := make([]swf.Artifact, 0, len(chapter.Artifacts))
	for _, art := range chapter.Artifacts {
		artifacts = append(artifacts, swf.NewArtifact(art.Name, func() (io.ReadCloser, int64, error) {
			reader, err := r.OpenArtifact(context.Background(), swf.ArtifactRef{
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
		return &swf.EnvelopedTaskData{
			SimpleTaskData: swf.SimpleTaskData{
				Data:      append([]byte(nil), payloadData...),
				Artifacts: artifacts,
			},
			Kind: payloadKind,
		}, nil
	case runtimecodec.PayloadKindTimeout:
		var payload swf.TimeoutPayload
		if err := json.Unmarshal(payloadData, &payload); err != nil {
			return nil, err
		}
		return &swf.EnvelopedTaskData{
			SimpleTaskData: swf.SimpleTaskData{Data: append([]byte(nil), payloadData...), Artifacts: artifacts},
			Kind:           payloadKind,
		}, swf.TimeoutError{Payload: payload}
	case runtimecodec.PayloadKindAppError:
		var payload swf.AppErrorPayload
		if err := json.Unmarshal(payloadData, &payload); err != nil {
			return nil, err
		}
		if jobFailedErr, ok := decodeToyJobFailedAppError(payload); ok {
			return &swf.EnvelopedTaskData{
				SimpleTaskData: swf.SimpleTaskData{Data: append([]byte(nil), payloadData...), Artifacts: artifacts},
				Kind:           payloadKind,
			}, jobFailedErr
		}
		return &swf.EnvelopedTaskData{
			SimpleTaskData: swf.SimpleTaskData{Data: append([]byte(nil), payloadData...), Artifacts: artifacts},
			Kind:           payloadKind,
		}, swf.AppError{Payload: payload}
	case runtimecodec.PayloadKindSystemError:
		var payload swf.SystemErrorPayload
		if err := json.Unmarshal(payloadData, &payload); err != nil {
			return nil, err
		}
		return &swf.EnvelopedTaskData{
			SimpleTaskData: swf.SimpleTaskData{Data: append([]byte(nil), payloadData...), Artifacts: artifacts},
			Kind:           payloadKind,
		}, swf.SystemError{Payload: payload}
	default:
		return &swf.EnvelopedTaskData{
			SimpleTaskData: swf.SimpleTaskData{Data: append([]byte(nil), payloadData...), Artifacts: artifacts},
			Kind:           payloadKind,
		}, nil
	}
}

func (r *Runtime) advanceRecordStateLocked(tenantId string, now time.Time, record *jobRecord) {
	if record.status == swf.JobStatusAwaitingFuture && !record.availableAt.IsZero() && !record.availableAt.After(now) {
		record.status = swf.JobStatusReady
	}
	if record.status == swf.JobStatusPendingJobs && len(record.waitFor) > 0 {
		complete := true
		for _, jobID := range record.waitFor {
			dep := r.engine.jobRecords[swf.JobKey{TenantId: tenantId, JobId: jobID}]
			if dep == nil {
				continue
			}
			dep.mu.Lock()
			status := dep.status
			dep.mu.Unlock()
			if status != swf.JobStatusCompleted && status != swf.JobStatusCancelled {
				complete = false
				break
			}
		}
		if complete {
			record.status = swf.JobStatusReady
			record.waitFor = nil
		}
	}
}

func (r *Runtime) completeLease(jobKey swf.JobKey, leaseID string, req swf.CompleteExecutionRequest) error {
	record := r.engine.getJobRecord(jobKey)
	if record == nil {
		return swf.ErrJobNotFound
	}
	record.mu.Lock()
	defer record.mu.Unlock()
	if record.leaseID != leaseID {
		return swf.ErrExecutionLeaseLost
	}
	record.leased = false
	record.leaseID = ""
	now := time.Now().UTC()
	record.archived = &now
	switch req.Status {
	case "cancelled":
		record.status = swf.JobStatusCancelled
		record.cancelled = true
		record.err = swf.ErrJobCancelled
	default:
		record.status = swf.JobStatusCompleted
	}
	return nil
}

func (r *Runtime) rescheduleLease(jobKey swf.JobKey, leaseID string, req swf.RescheduleExecutionRequest) error {
	record := r.engine.getJobRecord(jobKey)
	if record == nil {
		return swf.ErrJobNotFound
	}
	record.mu.Lock()
	defer record.mu.Unlock()
	if record.leaseID != leaseID {
		return swf.ErrExecutionLeaseLost
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
			var input swf.TaskData
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
		record.status = swf.JobStatusPendingJobs
	case req.WaitUntil != nil && req.WaitUntil.After(time.Now().UTC()):
		record.status = swf.JobStatusAwaitingFuture
		record.availableAt = req.WaitUntil.UTC()
	default:
		record.status = swf.JobStatusReady
	}
	return nil
}

type runtimeLease struct {
	runtime    *Runtime
	jobKey     swf.JobKey
	leaseID    string
	capability string
	payload    json.RawMessage
}

func (l *runtimeLease) LeaseID() string          { return l.leaseID }
func (l *runtimeLease) Job() swf.JobHandle       { return swf.JobHandle{JobKey: l.jobKey} }
func (l *runtimeLease) Capability() string       { return l.capability }
func (l *runtimeLease) Payload() json.RawMessage { return append(json.RawMessage(nil), l.payload...) }
func (l *runtimeLease) KeepAlive(context.Context) error {
	return nil
}
func (l *runtimeLease) StopKeepAlive() {}
func (l *runtimeLease) Complete(ctx context.Context, req swf.CompleteExecutionRequest) error {
	return l.runtime.completeLease(l.jobKey, l.leaseID, req)
}
func (l *runtimeLease) Reschedule(ctx context.Context, req swf.RescheduleExecutionRequest) error {
	return l.runtime.rescheduleLease(l.jobKey, l.leaseID, req)
}

type runtimeTaskHandle struct {
	runtime   *Runtime
	jobKey    swf.JobKey
	payload   json.RawMessage
	metadata  json.RawMessage
	wait      workerTaskWait
	taskType  string
	createdAt time.Time
}

func (h *runtimeTaskHandle) JobKey() swf.JobKey           { return h.jobKey }
func (h *runtimeTaskHandle) TaskOrdinalToComplete() int64 { return h.wait.OutputStep }
func (h *runtimeTaskHandle) TaskType() string             { return h.taskType }
func (h *runtimeTaskHandle) CreatedAt() time.Time         { return h.createdAt }
func (h *runtimeTaskHandle) Metadata() json.RawMessage    { return cloneJSON(h.metadata) }

func (h *runtimeTaskHandle) Data() (swf.TaskData, error) {
	ref := swf.ChapterRef{JobKey: h.jobKey, Ordinal: h.wait.InputStep}
	chapter, err := h.runtime.GetChapter(context.Background(), ref)
	if err != nil {
		return nil, err
	}
	return h.runtime.taskDataFromChapter(h.jobKey, chapter)
}

func (h *runtimeTaskHandle) Finish(ctx context.Context, taskData swf.TaskData) error {
	return h.runtime.CompleteTaskIfWaiting(ctx, swf.CompleteTaskIfWaitingRequest{
		JobKey:        h.jobKey,
		Capability:    swf.JobTypeFromNextNeed(h.wait.Next) + ":" + h.taskType,
		ResumeNeed:    h.wait.Next,
		InputOrdinal:  h.wait.InputStep,
		OutputOrdinal: h.wait.OutputStep,
		InputHash:     h.wait.InputHash,
		Data:          taskData,
	})
}

func (r *Runtime) CompleteTaskIfWaiting(ctx context.Context, req swf.CompleteTaskIfWaitingRequest) error {
	record := r.engine.getJobRecord(req.JobKey)
	if record == nil {
		return swf.ErrJobNotFound
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
		return fmt.Errorf("%w: job is not waiting on an external task", swf.ErrConflict)
	}
	if req.Capability != "" && currentCapability != req.Capability {
		return fmt.Errorf("%w: waiting capability %q does not match requested capability %q", swf.ErrConflict, currentCapability, req.Capability)
	}
	if req.ResumeNeed != "" && wait.Next != req.ResumeNeed {
		return fmt.Errorf("%w: resume need %q does not match requested resume need %q", swf.ErrConflict, wait.Next, req.ResumeNeed)
	}
	if req.InputOrdinal != 0 && wait.InputStep != req.InputOrdinal {
		return fmt.Errorf("%w: waiting input ordinal %d does not match requested input ordinal %d", swf.ErrConflict, wait.InputStep, req.InputOrdinal)
	}
	if wait.OutputStep != req.OutputOrdinal {
		return fmt.Errorf("%w: waiting output ordinal %d does not match requested output ordinal %d", swf.ErrConflict, wait.OutputStep, req.OutputOrdinal)
	}
	if req.InputHash != "" && wait.InputHash != req.InputHash {
		return fmt.Errorf("%w: waiting input hash does not match requested input hash", swf.ErrConflict)
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
	if err := r.storeRuntimeChapter(req.JobKey, wait.OutputStep, swf.Chapter{
		Ordinal:   wait.OutputStep,
		TaskType:  taskType,
		InputHash: wait.InputHash,
		CreatedAt: time.Now().UTC(),
		Metadata:  metadata,
		Body: swf.TaskAttemptOutcomeChapter{Outcome: swf.ApplicationOutputOutcome{
			Output: swf.ApplicationOutputBytes{Data: append([]byte(nil), dataPayload...)},
		}},
		Artifacts: storedArtifacts,
	}); err != nil {
		return err
	}
	record = r.engine.getJobRecord(req.JobKey)
	if record == nil {
		return swf.ErrJobNotFound
	}
	record.mu.Lock()
	resumeNeed := wait.Next
	if req.ResumeNeed != "" {
		resumeNeed = req.ResumeNeed
	}
	record.capability = resumeNeed
	record.payload = mustMarshalWorkerPayload(workerJobPayload{RunPolicy: payloadInfo.RunPolicy})
	record.status = swf.JobStatusReady
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

func extractRunPolicyFromMetadata(metadata swf.ChapterMetadata) (swf.RunPolicy, error) {
	raw, err := runtimecodec.ChapterMetadataToJSON(metadata)
	if err != nil {
		return swf.RunPolicy{}, err
	}
	if len(raw) == 0 {
		return swf.RunPolicy{}, nil
	}
	var payload struct {
		RunPolicy swf.RunPolicy `json:"run_policy"`
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return swf.RunPolicy{}, err
	}
	return payload.RunPolicy, nil
}

func extractPrerequisitesFromMetadata(metadata swf.ChapterMetadata) ([]swf.JobPrerequisite, error) {
	raw, err := runtimecodec.ChapterMetadataToJSON(metadata)
	if err != nil {
		return nil, err
	}
	if len(raw) == 0 {
		return nil, nil
	}
	var payload struct {
		Prereqs []swf.JobPrerequisite `json:"prereqs"`
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return nil, err
	}
	return append([]swf.JobPrerequisite(nil), payload.Prereqs...), nil
}

func extractAttemptFromMetadata(metadata swf.ChapterMetadata) (int, error) {
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

func marshalChapterMetadata(v map[string]any) (swf.ChapterMetadata, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return swf.ChapterMetadata{}, err
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

func swfInputHash(ctx context.Context, data swf.TaskData) (string, error) {
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

func cloneChapter(chapter swf.Chapter) swf.Chapter {
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

func decodeToyJobFailedAppError(payload swf.AppErrorPayload) (error, bool) {
	attrs := payload.Attrs
	if len(attrs) == 0 {
		return nil, false
	}
	raw, ok := attrs["_swf_job_failed"]
	if !ok {
		return nil, false
	}
	marked, ok := raw.(bool)
	if !ok || !marked {
		return nil, false
	}

	switch kind, _ := attrs["_swf_job_failed_kind"].(string); kind {
	case swf.TaskErrorKindTimeout:
		return &swf.JobFailedError{Cause: &swf.TimeoutError{Payload: swf.TimeoutPayload{
			Scope:     toyAttrString(attrs, "_swf_job_failed_scope"),
			After:     toyAttrDuration(attrs, "_swf_job_failed_after"),
			Retryable: toyAttrBool(attrs, "_swf_job_failed_retryable"),
			InputRef:  payload.InputRef,
			Component: toyAttrString(attrs, "_swf_job_failed_component"),
			Code:      toyAttrString(attrs, "_swf_job_failed_code"),
			Message:   payload.Message,
		}}}, true
	case swf.TaskErrorKindSystem:
		return &swf.JobFailedError{Cause: &swf.SystemError{Payload: swf.SystemErrorPayload{
			Message:    payload.Message,
			Component:  toyAttrString(attrs, "_swf_job_failed_component"),
			Code:       toyAttrString(attrs, "_swf_job_failed_code"),
			Retryable:  toyAttrBool(attrs, "_swf_job_failed_retryable"),
			InputRef:   payload.InputRef,
			Stacktrace: append([]string(nil), payload.Stacktrace...),
		}}}, true
	default:
		return &swf.JobFailedError{Cause: &swf.AppError{Payload: swf.AppErrorPayload{
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
		case "_swf_job_failed", "_swf_job_failed_kind", "_swf_job_failed_code", "_swf_job_failed_component", "_swf_job_failed_retryable", "_swf_job_failed_scope", "_swf_job_failed_after":
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

func toyAttrDuration(attrs map[string]interface{}, key string) swf.Duration {
	value, _ := attrs[key].(string)
	if value == "" {
		return 0
	}
	d, err := time.ParseDuration(value)
	if err != nil {
		return 0
	}
	return swf.Duration(d)
}
