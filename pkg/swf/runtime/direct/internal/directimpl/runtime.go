package directimpl

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"time"

	"github.com/colony-2/pgwf-go/pkg/pgwf"
	strataclient "github.com/colony-2/strata-go/pkg/client"
	strataartifact "github.com/colony-2/strata-go/pkg/client/artifact"
	"github.com/colony-2/strata-go/pkg/client/core"
	"github.com/colony-2/strata-go/pkg/client/pagination"
	"github.com/colony-2/strata-go/pkg/client/story"
	"github.com/colony-2/swf-go/pkg/swf"
	"github.com/segmentio/ksuid"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

// Runtime is the direct in-process WorkflowRuntime backed by strata + pgwf.
type Runtime struct {
	db           *gorm.DB
	udb          *sql.DB
	strataClient *strataclient.Client
	logger       *slog.Logger
	workerID     string
}

var _ swf.WorkflowRuntime = (*Runtime)(nil)

func New(db *gorm.DB, strataClient *strataclient.Client) *Runtime {
	var udb *sql.DB
	if db != nil {
		udb, _ = db.DB()
	}
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "swf"
	}
	return &Runtime{
		db:           db,
		udb:          udb,
		strataClient: strataClient,
		logger:       slog.Default(),
		workerID:     fmt.Sprintf("%s:%d-%s", host, os.Getpid(), ksuid.New().String()),
	}
}

func NewFromConfig(postgresDSN, strataBaseURL, strataAPIKey string) (*Runtime, error) {
	if postgresDSN == "" {
		return nil, fmt.Errorf("postgres DSN is required")
	}
	if strataBaseURL == "" {
		return nil, fmt.Errorf("strata base URL is required")
	}
	if strataAPIKey == "" {
		return nil, fmt.Errorf("strata API key is required")
	}

	db, err := gorm.Open(postgres.Open(postgresDSN), &gorm.Config{})
	if err != nil {
		return nil, fmt.Errorf("failed to connect to postgres: %w", err)
	}
	strataClient, err := strataclient.New(strataclient.Config{
		BaseURL: strataBaseURL,
		APIKey:  strataAPIKey,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create strata client: %w", err)
	}
	return New(db, strataClient), nil
}

func (r *Runtime) SubmitJob(ctx context.Context, req swf.SubmitJobRequest) (swf.JobHandle, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := r.validate(); err != nil {
		return swf.JobHandle{}, err
	}

	jobID := req.Job.JobID
	if jobID == "" {
		jobID = ksuid.New().String()
	}
	jobKey := swf.JobKey{
		TenantId: req.Job.TenantId,
		JobId:    jobID,
	}
	prereqs, waitFor, err := normalizePrerequisites(jobKey, req.Job.Prerequisites)
	if err != nil {
		return swf.JobHandle{}, err
	}
	taskData := swf.TaskData(req.Job.Data)
	inputHash, err := computeInputHash(ctx, taskData)
	if err != nil {
		return swf.JobHandle{}, err
	}
	jobPolicy := normalizeRunPolicy(req.Job.RunPolicy)
	co, err := taskDataToCreatOptions(taskData, 0, req.Job.JobType, r.requestWorkerID(req.WorkerID), chapterTypeJobStart, payloadKindApp, inputHash, time.Now().UTC(), chapterMetadata{
		Attempt:       1,
		RunPolicy:     &jobPolicy,
		Prerequisites: prereqs,
	})
	if err != nil {
		return swf.JobHandle{}, err
	}
	if _, err := r.strataClient.CreateStory(ctx, storyKeyForJob(jobKey), co); err != nil {
		return swf.JobHandle{}, err
	}
	if artifacts, _ := taskData.GetArtifacts(); len(artifacts) > 0 {
		assignArtifactKeys(artifacts, jobKey.JobId, 0)
		for _, art := range artifacts {
			if cleanupErr := art.Cleanup(); cleanupErr != nil {
				r.logger.Warn("failed to cleanup job input artifact", "artifact", art.Name(), "error", cleanupErr)
			}
		}
	}
	if err := r.startJob(ctx, jobKey, req.Job.JobType, req.Job.SingletonKey, req.Job.Metadata, waitFor, jobPayload{RunPolicy: jobPolicy}, req.WorkerID); err != nil {
		return swf.JobHandle{}, err
	}
	return swf.JobHandle{JobKey: jobKey}, nil
}

func (r *Runtime) SubmitRestartJob(ctx context.Context, req swf.SubmitRestartJobRequest) (swf.JobHandle, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := r.validate(); err != nil {
		return swf.JobHandle{}, err
	}
	job := req.Job
	if job.LastStepToKeep < 0 {
		return swf.JobHandle{}, fmt.Errorf("LastStepToKeep must be >= 0 for restart")
	}

	jobKey := swf.JobKey{
		TenantId: job.PriorJobKey.TenantId,
		JobId:    job.JobID,
	}
	if jobKey.JobId == "" {
		jobKey.JobId = ksuid.New().String()
	}
	prereqs, waitFor, err := normalizePrerequisites(jobKey, job.Prerequisites)
	if err != nil {
		return swf.JobHandle{}, err
	}
	sourceJob := storyKeyForJob(job.PriorJobKey)
	targetJob := storyKeyForJob(jobKey)

	chap0, err := r.strataClient.Chapter(ctx, sourceJob, 0)
	if err != nil {
		return swf.JobHandle{}, fmt.Errorf("load source initial chapter: %w", err)
	}
	env0, err := decodeChapterEnvelope(chap0.Body())
	if err != nil {
		return swf.JobHandle{}, fmt.Errorf("decode source initial chapter: %w", err)
	}
	jobType := env0.Meta.TaskType
	jobPolicy := swf.RunPolicy{}
	if env0.Meta.RunPolicy != nil {
		jobPolicy = normalizeRunPolicy(*env0.Meta.RunPolicy)
	}

	nextOrdinal := job.LastStepToKeep + 1
	nextChap, err := r.strataClient.Chapter(ctx, sourceJob, nextOrdinal)
	if err != nil {
		return swf.JobHandle{}, fmt.Errorf("LastStepToKeep %d invalid: no chapter at ordinal %d: %w", job.LastStepToKeep, nextOrdinal, err)
	}
	nextEnv, err := decodeChapterEnvelope(nextChap.Body())
	if err != nil {
		return swf.JobHandle{}, fmt.Errorf("decode source chapter %d: %w", nextOrdinal, err)
	}
	nextAttempt := nextEnv.Meta.Attempt
	if nextAttempt == 0 {
		nextAttempt = 1
	}
	if nextAttempt > 1 {
		return swf.JobHandle{}, fmt.Errorf("LastStepToKeep %d cuts into retry chain: next ordinal %d is attempt %d of %s", job.LastStepToKeep, nextOrdinal, nextAttempt, nextEnv.Meta.TaskType)
	}

	createOptions := story.CreateOptions{RequestID: ksuid.New().String()}
	if job.ExtraTaskOutput != nil {
		hashInput := job.ExtraTaskInput
		if hashInput == nil {
			hashInput = swf.NewTaskDataOrPanic(map[string]any{})
		}
		inputHash, err := computeInputHash(ctx, hashInput)
		if err != nil {
			return swf.JobHandle{}, err
		}
		inputRef := &swf.InputReference{Ordinal: job.LastStepToKeep, Hash: inputHash}
		createOptions, err = taskDataToCreatOptions(job.ExtraTaskOutput, job.LastStepToKeep+1, restartExtraTaskType, r.requestWorkerID(req.WorkerID), chapterTypeRestartExtra, payloadKindApp, inputHash, time.Now().UTC(), chapterMetadata{
			Attempt:       1,
			InputRef:      inputRef,
			Prerequisites: prereqs,
		})
		if err != nil {
			return swf.JobHandle{}, err
		}
	}

	if _, err := r.strataClient.CloneStory(ctx, sourceJob, story.CloneOptions{
		DestinationKey: targetJob,
		LastOrdinal:    job.LastStepToKeep,
		CreateOptions:  createOptions,
	}); err != nil {
		return swf.JobHandle{}, err
	}
	if err := r.startJob(ctx, jobKey, jobType, "", nil, waitFor, jobPayload{RunPolicy: jobPolicy}, req.WorkerID); err != nil {
		return swf.JobHandle{}, err
	}
	return swf.JobHandle{JobKey: jobKey}, nil
}

func (r *Runtime) startJob(ctx context.Context, jobKey swf.JobKey, jobType string, singletonKey string, metadata json.RawMessage, waitFor []pgwf.JobID, payload jobPayload, workerID string) error {
	if ctx == nil {
		ctx = context.Background()
	}
	return pgwf.SubmitJob(ctx, r.pgwfDB(ctx), pgwf.TenantID(jobKey.TenantId), pgwf.JobID(jobKey.JobId), pgwf.JobDependencies{
		NextNeed: pgwf.Capability(jobType),
		WaitFor:  waitFor,
	}, payload, metadata, pgwf.WorkerID(r.requestWorkerID(workerID)), singletonKey, time.Time{})
}

func (r *Runtime) CancelJob(ctx context.Context, req swf.CancelJobRequest) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := r.validate(); err != nil {
		return err
	}
	return pgwf.CancelJob(ctx, r.pgwfDB(ctx), pgwf.TenantID(req.JobKey.TenantId), pgwf.JobID(req.JobKey.JobId), pgwf.WorkerID(r.requestWorkerID(req.WorkerID)), req.Reason)
}

func (r *Runtime) PollWork(ctx context.Context, req swf.PollWorkRequest) ([]swf.ExecutionLease, error) {
	if err := r.validate(); err != nil {
		return nil, err
	}
	caps := make([]pgwf.Capability, 0, len(req.Capabilities))
	for _, capName := range req.Capabilities {
		if capName == "" {
			continue
		}
		caps = append(caps, pgwf.Capability(capName))
	}
	if len(caps) == 0 {
		return nil, nil
	}
	limit := req.Limit
	if limit <= 0 {
		limit = 1
	}
	opts := pgwf.GetWorkOptions{
		MetadataEquals: concreteMetadataPredicatesToPgwf(req.MetadataEquals),
	}
	if req.LeaseDuration != 0 {
		opts.LeaseSeconds = durationToLeaseSeconds(req.LeaseDuration)
	}

	out := make([]swf.ExecutionLease, 0, limit)
	for i := 0; i < limit; i++ {
		lease, err := pgwf.GetWorkWithOptions(ctx, r.udb, pgwf.WorkerID(r.requestWorkerID(req.WorkerID)), caps, opts)
		if err != nil {
			return nil, err
		}
		if lease == nil {
			break
		}
		out = append(out, &executionLease{lease: lease, udb: r.udb})
	}
	return out, nil
}

func (r *Runtime) GetJobLease(ctx context.Context, req swf.GetJobLeaseRequest) (swf.ExecutionLease, error) {
	if err := r.validate(); err != nil {
		return nil, err
	}
	caps := make([]pgwf.Capability, 0, len(req.Capabilities))
	for _, capName := range req.Capabilities {
		if capName == "" {
			continue
		}
		caps = append(caps, pgwf.Capability(capName))
	}

	opts := pgwf.GetJobLeaseOptions{}
	if req.LeaseDuration != 0 {
		opts.LeaseSeconds = durationToLeaseSeconds(req.LeaseDuration)
	}

	lease, err := pgwf.GetJobLeaseWithOptions(
		ctx,
		r.udb,
		pgwf.TenantID(req.JobKey.TenantId),
		pgwf.JobID(req.JobKey.JobId),
		pgwf.WorkerID(r.requestWorkerID(req.WorkerID)),
		caps,
		opts,
	)
	if err != nil {
		return nil, err
	}
	if lease == nil {
		return nil, nil
	}
	return &executionLease{lease: lease, udb: r.udb}, nil
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
	status, err := pgwf.GetJobStatus(ctx, r.pgwfDB(ctx), pgwf.TenantID(jobKey.TenantId), pgwf.JobID(jobKey.JobId))
	if errors.Is(err, pgwf.ErrJobNotFound) {
		return swf.JobInfo{}, swf.ErrJobNotFound
	}
	if err != nil {
		return swf.JobInfo{}, fmt.Errorf("failed to get job status: %w", err)
	}
	job := swf.JobInfo{
		Status: convertPgwfStatusToSwf(status.Status, status.CancelRequested, status.ArchivedAt),
		Data:   &jobInfoTaskData{err: swf.ErrJobNotComplete},
	}
	if status.ArchivedAt == nil {
		return job, nil
	}

	st, err := r.strataClient.Story(ctx, storyKeyForJob(jobKey))
	if err != nil {
		return swf.JobInfo{}, err
	}
	chap, err := st.GetLastChapter(ctx)
	if err != nil {
		return swf.JobInfo{}, err
	}
	td, payloadErr := chapterToTaskData(chap, jobKey)
	job.Data = &jobInfoTaskData{taskData: td, err: payloadErr}
	return job, nil
}

func (r *Runtime) ListJobs(ctx context.Context, req swf.ListJobsRequest) (swf.ListJobsResponse, error) {
	if len(req.TenantIds) == 0 {
		return swf.ListJobsResponse{}, fmt.Errorf("tenant_ids is required for ListJobs")
	}

	metadataPredicates, err := metadataPredicatesToPgwf(req.MetadataFilter)
	if err != nil {
		return swf.ListJobsResponse{}, err
	}

	opts := pgwf.ListJobsOptions{
		TenantIDs:       append([]string(nil), req.TenantIds...),
		Statuses:        convertSwfStatusesToPgwf(req.Statuses),
		JobTypePatterns: buildJobTypePatterns(req.JobTypes, req.JobTasks),
		IncludeArchived: shouldIncludeArchived(req.Stores, req.Statuses),
		MetadataEquals:  metadataPredicates,
		CreatedAfter:    req.CreatedAfter,
		CreatedBefore:   req.CreatedBefore,
		Limit:           normalizePageSize(req.PageSize),
		Cursor:          req.PageToken,
		SortBy:          pgwf.SortByCreatedAt,
		SortOrder:       pgwf.SortDesc,
	}
	if len(req.SingletonKeys) > 0 {
		opts.SingletonKey = req.SingletonKeys[0]
	}

	result, err := pgwf.ListJobs(ctx, r.pgwfDB(ctx), opts)
	if err != nil {
		return swf.ListJobsResponse{}, fmt.Errorf("failed to list jobs: %w", err)
	}

	requestedStatuses := make(map[swf.JobStatus]bool, len(req.Statuses))
	for _, st := range req.Statuses {
		requestedStatuses[st] = true
	}
	requestedJobKeys := make(map[swf.JobKey]bool, len(req.JobKeys))
	for _, jk := range req.JobKeys {
		requestedJobKeys[jk] = true
	}

	jobs := make([]swf.JobSummary, 0, len(result.Jobs))
	for _, job := range result.Jobs {
		jobKey := swf.JobKey{TenantId: job.TenantID, JobId: job.JobID}
		if len(requestedJobKeys) > 0 && !requestedJobKeys[jobKey] {
			continue
		}
		swfStatus := convertPgwfStatusToSwf(job.Status, job.CancelRequested, job.ArchivedAt)
		if len(requestedStatuses) > 0 && !requestedStatuses[swfStatus] {
			continue
		}
		summary := swf.JobSummary{
			JobKey:          jobKey,
			Status:          swfStatus,
			JobType:         swf.JobTypeFromNextNeed(job.NextNeed),
			NextNeed:        strPtr(job.NextNeed),
			SingletonKey:    job.SingletonKey,
			WaitFor:         job.WaitFor,
			AvailableAt:     job.AvailableAt,
			ExpiresAt:       job.ExpiresAt,
			LeaseExpiresAt:  job.LeaseExpiresAt,
			CancelRequested: job.CancelRequested,
			CreatedAt:       job.CreatedAt,
			ArchivedAt:      job.ArchivedAt,
			Metadata:        job.Metadata,
		}
		if strings.Contains(job.NextNeed, ":") {
			details, detailErr := pgwf.GetJob(ctx, r.pgwfDB(ctx), pgwf.TenantID(job.TenantID), pgwf.JobID(job.JobID), pgwf.GetJobOptions{IncludePayload: true})
			if detailErr != nil {
				return swf.ListJobsResponse{}, fmt.Errorf("failed to get job details: %w", detailErr)
			}
			summary.Payload = details.Payload
			if tw, waitErr := extractTaskWaitFromRaw(details.Payload); waitErr == nil && tw != nil {
				summary.TaskWaitInput = &tw.InputStep
				summary.TaskWaitOutput = &tw.OutputStep
				summary.TaskWaitInputHash = strPtr(tw.InputHash)
				summary.TaskWaitNext = &tw.Next
			}
		}
		jobs = append(jobs, summary)
	}

	return swf.ListJobsResponse{
		Jobs:          jobs,
		NextPageToken: result.NextCursor,
	}, nil
}

func (r *Runtime) FindTasksWaitingForCapability(ctx context.Context, jobType string, taskType string, tenantIds []string) ([]swf.TaskHandle, error) {
	jobs, err := pgwf.FindJobs(ctx, r.pgwfDB(ctx), pgwf.FindJobsOptions{
		TenantIDs: tenantIds,
		Status:    pgwf.JobStatusReady,
		NextNeed:  jobType + ":" + taskType,
		Limit:     1000,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to find jobs: %w", err)
	}

	handles := make([]swf.TaskHandle, 0, len(jobs))
	for _, j := range jobs {
		details, err := pgwf.GetJob(ctx, r.pgwfDB(ctx), pgwf.TenantID(j.TenantID), pgwf.JobID(j.JobID), pgwf.GetJobOptions{IncludePayload: true})
		if err != nil {
			return nil, fmt.Errorf("failed to get job details: %w", err)
		}
		tw, err := extractTaskWaitFromRaw(details.Payload)
		if err != nil {
			return nil, err
		}
		handles = append(handles, &taskHandleImpl{
			jobID:         j.JobID,
			tenantId:      j.TenantID,
			payload:       details.Payload,
			metadata:      details.Metadata,
			inputOrdinal:  tw.InputStep,
			outputOrdinal: tw.OutputStep,
			runtime:       r,
			nextNeed:      pgwf.Capability(tw.Next),
			taskType:      taskType,
			createdAt:     j.CreatedAt,
		})
	}
	return handles, nil
}

func (r *Runtime) GetWaitingTask(ctx context.Context, key swf.JobKey) (swf.TaskHandle, error) {
	job, err := pgwf.GetJob(ctx, r.pgwfDB(ctx), pgwf.TenantID(key.TenantId), pgwf.JobID(key.JobId), pgwf.GetJobOptions{IncludePayload: true})
	if errors.Is(err, pgwf.ErrJobNotFound) {
		return nil, swf.ErrJobNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get job: %w", err)
	}
	if job.Status != pgwf.JobStatusReady {
		return nil, swf.ErrJobNotFound
	}

	tw, err := extractTaskWaitFromRaw(job.Payload)
	if err != nil {
		return nil, err
	}
	return &taskHandleImpl{
		jobID:         job.JobID,
		tenantId:      key.TenantId,
		payload:       job.Payload,
		metadata:      job.Metadata,
		inputOrdinal:  tw.InputStep,
		outputOrdinal: tw.OutputStep,
		runtime:       r,
		nextNeed:      pgwf.Capability(tw.Next),
		taskType:      taskTypeFromCapability(job.NextNeed),
		createdAt:     job.CreatedAt,
	}, nil
}

func (r *Runtime) GetChapter(ctx context.Context, ref swf.ChapterRef) (swf.StoredChapter, error) {
	if err := r.validate(); err != nil {
		return swf.StoredChapter{}, err
	}
	chapter, err := r.strataClient.Chapter(ctx, StoryKeyForJob(ref.JobKey), ref.Ordinal)
	if err != nil {
		if errors.Is(err, core.ErrNotFound) {
			return swf.StoredChapter{}, swf.ErrChapterNotFound
		}
		return swf.StoredChapter{}, err
	}
	return StoredChapterFromStoryChapter(chapter)
}

func (r *Runtime) ListChapters(ctx context.Context, req swf.ListChaptersRequest) ([]swf.StoredChapter, error) {
	if err := r.validate(); err != nil {
		return nil, err
	}
	if req.StartOrdinal < 0 {
		return nil, fmt.Errorf("start ordinal must be >= 0")
	}
	st, err := r.loadStory(ctx, storyKeyForJob(req.JobKey))
	if err != nil {
		if errors.Is(err, core.ErrNotFound) {
			return nil, swf.ErrJobNotFound
		}
		return nil, err
	}
	iter, err := st.Chapters(ctx, story.ChaptersOptions{
		PageSize:  defaultJobRunChaptersPageSize,
		Direction: story.DirectionForward,
	})
	if err != nil {
		return nil, err
	}

	out := make([]swf.StoredChapter, 0)
	for iter.HasNext() {
		chapter, err := iter.Next(ctx)
		if errors.Is(err, pagination.ErrNoMoreItems) {
			break
		}
		if err != nil {
			return nil, err
		}
		stored, err := StoredChapterFromStoryChapter(chapter)
		if err != nil {
			return nil, err
		}
		if stored.Ordinal < req.StartOrdinal {
			continue
		}
		if req.EndOrdinal != nil && stored.Ordinal > *req.EndOrdinal {
			break
		}
		out = append(out, stored)
	}
	return out, nil
}

func (r *Runtime) PutChapter(ctx context.Context, req swf.PutChapterRequest) error {
	if err := r.validate(); err != nil {
		return err
	}
	chapter, attached, err := r.prepareChapterWrite(ctx, req)
	if err != nil {
		return err
	}
	body, err := EncodeStoredChapter(chapter)
	if err != nil {
		return err
	}
	builder := story.NewChapter().WithOrdinal(req.Ref.Ordinal).WithBytes(body)
	for _, art := range attached {
		builder.AddArtifact(art)
	}
	return r.strataClient.SaveChapter(ctx, StoryKeyForJob(req.Ref.JobKey), builder)
}

func (r *Runtime) OpenArtifact(ctx context.Context, ref swf.ArtifactRef) (swf.ArtifactReader, error) {
	if err := r.validate(); err != nil {
		return nil, err
	}
	chapter, err := r.loadChapter(ctx, storyKeyForJob(ref.JobKey), ref.Ordinal)
	if err != nil {
		return nil, err
	}
	var descriptor *strataartifact.Descriptor
	for _, existing := range chapter.Artifacts() {
		if existing == nil || existing.Name() != ref.Name {
			continue
		}
		digest, _ := existing.Sha256(ctx)
		if ref.Digest != "" && digest != "" && ref.Digest != digest {
			continue
		}
		descriptor = &strataartifact.Descriptor{
			Name:        existing.Name(),
			ContentType: existing.ContentType(),
			SizeBytes:   existing.SizeBytes(),
			Sha256:      digest,
		}
		break
	}
	if descriptor == nil {
		return nil, fmt.Errorf("artifact %s not found for job %s ordinal %d", ref.Name, ref.JobKey.JobId, ref.Ordinal)
	}
	art := strataartifact.FromRemote(
		*descriptor,
		strataartifact.Locator{
			AnthologyID: ref.JobKey.TenantId,
			StoryID:     ref.JobKey.JobId,
			Ordinal:     ref.Ordinal,
			Name:        descriptor.Name,
		},
		r.strataClient.Core(),
	)
	return artifactReader{art: FromStrataArtifactForRuntime(art)}, nil
}

func (r *Runtime) validate() error {
	if r == nil {
		return fmt.Errorf("runtime is required")
	}
	if r.db == nil || r.udb == nil {
		return fmt.Errorf("db is required")
	}
	if r.strataClient == nil {
		return fmt.Errorf("strata client is required")
	}
	return nil
}

func (r *Runtime) dbFromCtx(ctx context.Context) *gorm.DB {
	if ctx == nil {
		ctx = context.Background()
	}
	if tx, ok := swf.TxFromCtx(ctx); ok && tx != nil {
		return tx.WithContext(ctx)
	}
	return r.db.WithContext(ctx)
}

func (r *Runtime) sqlTxFromCtx(ctx context.Context) *sql.Tx {
	if ctx == nil {
		return nil
	}
	if tx, ok := swf.TxFromCtx(ctx); ok && tx != nil {
		return sqlTxFromGorm(tx)
	}
	return nil
}

func (r *Runtime) pgwfDB(ctx context.Context) pgwf.DB {
	if tx := r.sqlTxFromCtx(ctx); tx != nil {
		return tx
	}
	return r.udb
}

func (r *Runtime) requestWorkerID(workerID string) string {
	if workerID != "" {
		return workerID
	}
	return r.workerID
}

func (r *Runtime) loadStory(ctx context.Context, key story.Key) (story.Story, error) {
	return r.strataClient.Story(ctx, key)
}

func (r *Runtime) loadChapter(ctx context.Context, key story.Key, ordinal int64) (story.Chapter, error) {
	return r.strataClient.Chapter(ctx, key, ordinal)
}

func (r *Runtime) prepareChapterWrite(ctx context.Context, req swf.PutChapterRequest) (swf.StoredChapter, []strataartifact.Artifact, error) {
	chapter := req.Chapter
	if len(req.ArtifactUploads) == 0 {
		if len(chapter.Artifacts) > 0 {
			return swf.StoredChapter{}, nil, fmt.Errorf("put chapter with artifact descriptors but no artifact uploads")
		}
		return chapter, nil, nil
	}

	stored := make([]swf.StoredArtifact, 0, len(req.ArtifactUploads))
	attached := make([]strataartifact.Artifact, 0, len(req.ArtifactUploads))
	for _, item := range req.ArtifactUploads {
		if item.Open == nil {
			return swf.StoredChapter{}, nil, fmt.Errorf("artifact %q is missing opener", item.Name)
		}
		reader, err := item.Open()
		if err != nil {
			return swf.StoredChapter{}, nil, err
		}
		data, err := io.ReadAll(reader)
		_ = reader.Close()
		if err != nil {
			return swf.StoredChapter{}, nil, err
		}
		art := swf.NewArtifactFromBytes(item.Name, data)
		digest, err := art.Sha256(ctx)
		if err != nil {
			return swf.StoredChapter{}, nil, err
		}
		stored = append(stored, swf.StoredArtifact{
			Name:   item.Name,
			Digest: digest,
			Size:   int64(len(data)),
		})
		attached = append(attached, ToStrataArtifactForRuntime(art))
	}
	if err := validateChapterArtifactDescriptors(chapter.Artifacts, stored); err != nil {
		return swf.StoredChapter{}, nil, err
	}
	chapter.Artifacts = stored
	return chapter, attached, nil
}

func validateChapterArtifactDescriptors(existing []swf.StoredArtifact, computed []swf.StoredArtifact) error {
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

type artifactReader struct {
	art swf.Artifact
}

func (a artifactReader) Open() (io.ReadCloser, error) { return a.art.Open() }
func (a artifactReader) Size() int64                  { return a.art.Size() }
func (a artifactReader) Name() string                 { return a.art.Name() }

type executionLease struct {
	lease *pgwf.Lease
	udb   *sql.DB
}

func (l *executionLease) Job() swf.JobHandle {
	return swf.JobHandle{
		JobKey: swf.JobKey{
			TenantId: string(l.lease.TenantID()),
			JobId:    string(l.lease.JobID()),
		},
	}
}

func (l *executionLease) Capability() string {
	return string(l.lease.NextNeed())
}

func (l *executionLease) Payload() json.RawMessage {
	return l.lease.Payload()
}

func (l *executionLease) KeepAlive(ctx context.Context) error {
	_ = l.lease.WithKeepAlive(l.udb)
	return nil
}

func (l *executionLease) StopKeepAlive() {
	stopLeaseKeepAlive(l.lease)
}

func (l *executionLease) Complete(ctx context.Context, req swf.CompleteExecutionRequest) error {
	status := completionStatusSuccess
	switch req.Status {
	case "", "success", "succeeded":
		status = completionStatusSuccess
	case "failed_app":
		status = completionStatusFailedApp
	case "failed_system":
		status = completionStatusFailedSystem
	case "failed_timeout":
		status = completionStatusFailedTimeout
	case "cancelled":
		status = completionStatusCancelled
	default:
		status = pgwf.CompletionStatus(req.Status)
	}
	err := l.lease.CompleteWithStatus(ctx, l.udb, status, req.Detail)
	if err != nil {
		if errors.Is(err, pgwf.ErrLeaseMismatch) || errors.Is(err, pgwf.ErrLeaseExpired) {
			return swf.ErrExecutionLeaseLost
		}
	}
	return err
}

func (l *executionLease) Reschedule(ctx context.Context, req swf.RescheduleExecutionRequest) error {
	deps := pgwf.JobDependencies{
		NextNeed: pgwf.Capability(req.NextNeed),
	}
	if req.WaitUntil != nil {
		deps.AvailableAt = req.WaitUntil.UTC()
	}
	if len(req.WaitForJobIDs) > 0 {
		waitFor := make([]pgwf.JobID, 0, len(req.WaitForJobIDs))
		for _, id := range req.WaitForJobIDs {
			if id != "" {
				waitFor = append(waitFor, pgwf.JobID(id))
			}
		}
		deps.WaitFor = waitFor
	}
	if req.AlternateNeed != "" {
		after := time.Duration(0)
		if req.AlternateAfter != nil {
			after = time.Duration(*req.AlternateAfter)
		}
		deps.Alternate = &pgwf.AlternateNext{
			Need:  pgwf.Capability(req.AlternateNeed),
			After: after,
		}
	}
	payload := req.Payload
	if len(payload) == 0 {
		payload = json.RawMessage(`{}`)
	}
	err := l.lease.Reschedule(ctx, l.udb, deps, payload)
	if err != nil {
		if errors.Is(err, pgwf.ErrLeaseMismatch) || errors.Is(err, pgwf.ErrLeaseExpired) {
			return swf.ErrExecutionLeaseLost
		}
	}
	return err
}
