package toyimpl

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"sort"
	"time"

	"github.com/colony-2/swf-go/pkg/swf"
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

func (r *Runtime) StartJob(ctx context.Context, req swf.StartJobRequest) (swf.JobHandle, error) {
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
	metaJSON, err := marshalChapterMetadata(map[string]any{
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
	stored := swf.StoredChapter{
		Ordinal:     0,
		TaskType:    req.Job.JobType,
		ChapterType: "JobStart",
		PayloadKind: "App",
		InputHash:   inputHash,
		CreatedAt:   now,
		Metadata:    metaJSON,
		Data:        append(json.RawMessage(nil), payload...),
		Artifacts:   storedArtifacts,
	}

	payloadJSON, err := json.Marshal(workerJobPayload{
		RunPolicy: runPolicy,
	})
	if err != nil {
		return swf.JobHandle{}, err
	}
	var singleton *string
	if req.Job.SingletonKey != "" {
		singleton = &req.Job.SingletonKey
	}

	record := &jobRecord{
		status:      swf.JobStatusReady,
		jobType:     req.Job.JobType,
		singleton:   singleton,
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
		r.engine.runtimeChapters[jobKey] = make(map[int64]swf.StoredChapter)
	}
	r.engine.runtimeChapters[jobKey][0] = stored
	r.engine.mu.Unlock()

	return swf.JobHandle{JobKey: jobKey}, nil
}

func (r *Runtime) RestartJob(ctx context.Context, req swf.RestartJobRequest) (swf.JobHandle, error) {
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
	targetChapters := make(map[int64]swf.StoredChapter)
	for ordinal, chapter := range sourceChapters {
		if ordinal > req.Job.LastStepToKeep {
			continue
		}
		targetChapters[ordinal] = cloneStoredChapter(chapter)
	}
	if _, ok := targetChapters[0]; !ok {
		return swf.JobHandle{}, swf.ErrJobNotFound
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
		td, decodeErr := r.taskDataFromStoredChapter(jobKey, chapter)
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

func (r *Runtime) CheckJobStatus(ctx context.Context, jobKey swf.JobKey) (swf.JobStatus, error) {
	record := r.engine.getJobRecord(jobKey)
	if record == nil {
		return "", swf.ErrJobNotFound
	}
	record.mu.Lock()
	defer record.mu.Unlock()
	return record.status, nil
}

func (r *Runtime) GetJobResult(ctx context.Context, jobKey swf.JobKey) (swf.TaskData, error) {
	record := r.engine.getJobRecord(jobKey)
	if record == nil {
		return nil, swf.ErrJobNotFound
	}
	record.mu.Lock()
	defer record.mu.Unlock()
	if record.status != swf.JobStatusCompleted {
		return nil, swf.ErrJobNotComplete
	}
	return record.result, record.err
}

func (r *Runtime) GetJobRun(ctx context.Context, req swf.GetJobRunRequest) (swf.GetJobRunResponse, error) {
	return r.engine.GetJobRun(ctx, req)
}

func (r *Runtime) ListJobs(ctx context.Context, req swf.ListJobsRequest) (swf.ListJobsResponse, error) {
	return r.engine.ListJobs(ctx, req)
}

func (r *Runtime) GetChapter(ctx context.Context, ref swf.ChapterRef) (swf.StoredChapter, error) {
	r.engine.mu.Lock()
	defer r.engine.mu.Unlock()
	chapters := r.engine.runtimeChapters[ref.JobKey]
	if chapters == nil {
		return swf.StoredChapter{}, swf.ErrChapterNotFound
	}
	chapter, ok := chapters[ref.Ordinal]
	if !ok {
		return swf.StoredChapter{}, swf.ErrChapterNotFound
	}
	return cloneStoredChapter(chapter), nil
}

func (r *Runtime) PutChapter(ctx context.Context, req swf.PutChapterRequest) error {
	r.engine.mu.Lock()
	if r.engine.runtimeChapters[req.Ref.JobKey] == nil {
		r.engine.runtimeChapters[req.Ref.JobKey] = make(map[int64]swf.StoredChapter)
	}
	r.engine.runtimeChapters[req.Ref.JobKey][req.Ref.Ordinal] = cloneStoredChapter(req.Chapter)
	record := r.engine.jobRecords[req.Ref.JobKey]
	r.engine.mu.Unlock()

	if record == nil {
		return nil
	}
	td, err := r.taskDataFromStoredChapter(req.Ref.JobKey, req.Chapter)
	if err != nil {
		if req.Chapter.ChapterType != "JobAttemptOutcome" {
			return err
		}
	}
	meta, _ := extractAttemptFromMetadata(req.Chapter.Metadata)
	record.mu.Lock()
	if req.Chapter.ChapterType != "JobAttemptOutcome" {
		record.chapters[req.Ref.Ordinal] = &toyChapter{
			TaskType:  req.Chapter.TaskType,
			CreatedAt: req.Chapter.CreatedAt,
			Input:     td,
			Output:    td,
			Attempt:   meta,
		}
	}
	if req.Chapter.ChapterType == "JobAttemptOutcome" {
		record.result = td
		if td == nil {
			record.result = &swf.EnvelopedTaskData{
				SimpleTaskData: swf.SimpleTaskData{
					Data: append([]byte(nil), req.Chapter.Data...),
				},
				Kind: req.Chapter.PayloadKind,
			}
		}
		_, resultErr := r.taskDataFromStoredChapter(req.Ref.JobKey, req.Chapter)
		record.err = resultErr
	}
	record.mu.Unlock()
	return nil
}

func (r *Runtime) OpenArtifact(ctx context.Context, ref swf.ArtifactRef) (swf.ArtifactReader, error) {
	return r.engine.OpenStoredArtifact(ctx, ref)
}

func (r *Runtime) PutArtifacts(ctx context.Context, req swf.PutArtifactsRequest) ([]swf.StoredArtifact, error) {
	return r.engine.PutStoredArtifacts(ctx, req)
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
	record.mu.Unlock()

	wait, err := extractWorkerTaskWait(payload)
	if err != nil {
		return nil, err
	}
	if wait == nil {
		return nil, swf.ErrJobNotFound
	}
	taskType := extractTaskType(wait.Next)
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

func (r *Runtime) taskDataFromStoredChapter(jobKey swf.JobKey, chapter swf.StoredChapter) (swf.TaskData, error) {
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
	switch chapter.PayloadKind {
	case "App":
		return &swf.EnvelopedTaskData{
			SimpleTaskData: swf.SimpleTaskData{
				Data:      append([]byte(nil), chapter.Data...),
				Artifacts: artifacts,
			},
			Kind: chapter.PayloadKind,
		}, nil
	case "Timeout":
		var payload swf.TimeoutPayload
		if err := json.Unmarshal(chapter.Data, &payload); err != nil {
			return nil, err
		}
		return &swf.EnvelopedTaskData{
			SimpleTaskData: swf.SimpleTaskData{Data: append([]byte(nil), chapter.Data...), Artifacts: artifacts},
			Kind:           chapter.PayloadKind,
		}, swf.TimeoutError{Payload: payload}
	case "AppError":
		var payload swf.AppErrorPayload
		if err := json.Unmarshal(chapter.Data, &payload); err != nil {
			return nil, err
		}
		return &swf.EnvelopedTaskData{
			SimpleTaskData: swf.SimpleTaskData{Data: append([]byte(nil), chapter.Data...), Artifacts: artifacts},
			Kind:           chapter.PayloadKind,
		}, swf.AppError{Payload: payload}
	case "SystemError":
		var payload swf.SystemErrorPayload
		if err := json.Unmarshal(chapter.Data, &payload); err != nil {
			return nil, err
		}
		return &swf.EnvelopedTaskData{
			SimpleTaskData: swf.SimpleTaskData{Data: append([]byte(nil), chapter.Data...), Artifacts: artifacts},
			Kind:           chapter.PayloadKind,
		}, swf.SystemError{Payload: payload}
	default:
		return &swf.EnvelopedTaskData{
			SimpleTaskData: swf.SimpleTaskData{Data: append([]byte(nil), chapter.Data...), Artifacts: artifacts},
			Kind:           chapter.PayloadKind,
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

func (l *runtimeLease) Job() swf.JobHandle       { return swf.JobHandle{JobKey: l.jobKey} }
func (l *runtimeLease) Capability() string       { return l.capability }
func (l *runtimeLease) Payload() json.RawMessage { return append(json.RawMessage(nil), l.payload...) }
func (l *runtimeLease) KeepAlive(context.Context) error {
	return nil
}
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
	return h.runtime.taskDataFromStoredChapter(h.jobKey, chapter)
}

func (h *runtimeTaskHandle) Finish(ctx context.Context, taskData swf.TaskData) error {
	record := h.runtime.engine.getJobRecord(h.jobKey)
	if record == nil {
		return swf.ErrJobNotFound
	}
	payloadInfo := workerJobPayload{}
	_ = json.Unmarshal(h.payload, &payloadInfo)

	td, storedArtifacts, err := h.runtime.materializeTaskData(ctx, h.jobKey, h.wait.OutputStep, taskData)
	if err != nil {
		return err
	}
	payload, err := td.GetData()
	if err != nil {
		return err
	}
	metaJSON, err := marshalChapterMetadata(map[string]any{
		"version":    1,
		"ordinal":    h.wait.OutputStep,
		"task_type":  h.taskType,
		"created_at": time.Now().UTC(),
		"input_hash": h.wait.InputHash,
		"attempt":    1,
		"run_policy": payloadInfo.RunPolicy,
	})
	if err != nil {
		return err
	}
	if err := h.runtime.PutChapter(ctx, swf.PutChapterRequest{
		Ref: swf.ChapterRef{JobKey: h.jobKey, Ordinal: h.wait.OutputStep},
		Chapter: swf.StoredChapter{
			Ordinal:     h.wait.OutputStep,
			TaskType:    h.taskType,
			ChapterType: "TaskAttemptOutcome",
			PayloadKind: "App",
			InputHash:   h.wait.InputHash,
			CreatedAt:   time.Now().UTC(),
			Metadata:    metaJSON,
			Data:        append(json.RawMessage(nil), payload...),
			Artifacts:   storedArtifacts,
		},
	}); err != nil {
		return err
	}
	record.mu.Lock()
	record.capability = h.wait.Next
	record.payload = mustMarshalWorkerPayload(workerJobPayload{RunPolicy: payloadInfo.RunPolicy})
	record.status = swf.JobStatusReady
	record.leased = false
	record.leaseID = ""
	record.step = h.wait.OutputStep
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

func extractRunPolicyFromMetadata(raw json.RawMessage) (swf.RunPolicy, error) {
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

func extractAttemptFromMetadata(raw json.RawMessage) (int, error) {
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

func marshalChapterMetadata(v map[string]any) (json.RawMessage, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, err
	}
	return json.RawMessage(raw), nil
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

func cloneStoredChapter(chapter swf.StoredChapter) swf.StoredChapter {
	out := chapter
	out.Data = cloneJSON(chapter.Data)
	out.Metadata = cloneJSON(chapter.Metadata)
	if len(chapter.Artifacts) > 0 {
		out.Artifacts = append([]swf.StoredArtifact(nil), chapter.Artifacts...)
	}
	return out
}

func cloneJSON(raw json.RawMessage) json.RawMessage {
	if len(raw) == 0 {
		return nil
	}
	out := make([]byte, len(raw))
	copy(out, raw)
	return out
}
