package directimpl

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"time"

	"github.com/colony-2/jobdb/pkg/internal/runtimecodec"
	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/chapterstore"
	chapterartifact "github.com/colony-2/jobdb/pkg/jobdb/internal/chapterstore/artifact"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/chapterstore/blobstore"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/chapterstore/core"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/chapterstore/pagination"
	postgresrowstore "github.com/colony-2/jobdb/pkg/jobdb/internal/chapterstore/postgres"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/chapterstore/story"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/jobmetadata"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/jobschema"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/leaseauth"
	"github.com/colony-2/pgwf-go/pkg/pgwf"
	"github.com/segmentio/ksuid"
	pgdriver "gorm.io/driver/postgres"
	"gorm.io/gorm"
)

// Config describes a direct Postgres-backed JobDB runtime.
type Config struct {
	PostgresDSN            string
	BlobStoreURI           string
	MaxInlineArtifactBytes int64
	Logger                 *slog.Logger
}

// Runtime is the direct in-process WorkflowRuntime backed by Postgres records.
type Runtime struct {
	db           *gorm.DB
	udb          *sql.DB
	chapterStore *chapterstore.Store
	logger       *slog.Logger
	workerID     string
}

var _ jobdb.WorkflowRuntime = (*Runtime)(nil)

func New(db *gorm.DB, cfg Config) (*Runtime, error) {
	if db == nil {
		return nil, fmt.Errorf("db is required")
	}
	rows, err := postgresrowstore.New(db)
	if err != nil {
		return nil, fmt.Errorf("create chapter rowstore: %w", err)
	}
	blobs, err := blobstore.OpenURI(resolveBlobStoreURI(cfg.BlobStoreURI))
	if err != nil {
		return nil, fmt.Errorf("create chapter blobstore: %w", err)
	}
	chapterStore, err := chapterstore.New(rows, blobs, chapterstore.Config{
		MaxInlineArtifactBytes: cfg.MaxInlineArtifactBytes,
		Logger:                 cfg.Logger,
	})
	if err != nil {
		return nil, err
	}
	return newRuntime(db, chapterStore, cfg), nil
}

func newRuntime(db *gorm.DB, chapterStore *chapterstore.Store, cfg Config) *Runtime {
	var udb *sql.DB
	if db != nil {
		udb, _ = db.DB()
	}
	host, err := os.Hostname()
	if err != nil || host == "" {
		host = "jobdb"
	}
	rt := &Runtime{
		db:           db,
		udb:          udb,
		chapterStore: chapterStore,
		logger:       cfg.logger(),
		workerID:     fmt.Sprintf("%s:%d-%s", host, os.Getpid(), ksuid.New().String()),
	}
	if udb != nil {
		if err := migrateSchedules(context.Background(), udb); err != nil {
			rt.logger.Warn("failed to migrate schedule tables", "error", err)
		}
		if err := migrateJobSchemas(context.Background(), udb); err != nil {
			rt.logger.Warn("failed to migrate schema tables", "error", err)
		}
	}
	return rt
}

func NewFromConfig(cfg Config) (*Runtime, error) {
	if cfg.PostgresDSN == "" {
		return nil, fmt.Errorf("postgres DSN is required")
	}

	db, err := gorm.Open(pgdriver.Open(cfg.PostgresDSN), &gorm.Config{})
	if err != nil {
		return nil, fmt.Errorf("failed to connect to postgres: %w", err)
	}
	rt, err := New(db, cfg)
	if err != nil {
		return nil, err
	}
	if err := migrateSchedules(context.Background(), rt.udb); err != nil {
		return nil, err
	}
	return rt, nil
}

func (c Config) logger() *slog.Logger {
	if c.Logger != nil {
		return c.Logger
	}
	return slog.Default()
}

func resolveBlobStoreURI(uri string) string {
	if strings.TrimSpace(uri) != "" {
		return uri
	}
	path, err := filepath.Abs("jobdb-blobs")
	if err != nil {
		path = "jobdb-blobs"
	}
	return "blobfs://" + filepath.ToSlash(path)
}

func (r *Runtime) SubmitJob(ctx context.Context, req jobdb.SubmitJobRequest) (jobdb.JobHandle, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := r.validate(); err != nil {
		return jobdb.JobHandle{}, err
	}

	jobID := req.Job.JobID
	if jobID == "" {
		jobID = ksuid.New().String()
	}
	jobKey := jobdb.JobKey{
		TenantId: req.Job.TenantId,
		JobId:    jobID,
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
	prereqs, waitFor, err := normalizePrerequisites(jobKey, req.Job.Prerequisites)
	if err != nil {
		return jobdb.JobHandle{}, err
	}
	taskData := jobdb.TaskData(req.Job.Data)
	inputHash, err := computeInputHash(ctx, taskData)
	if err != nil {
		return jobdb.JobHandle{}, err
	}
	jobPolicy := normalizeRunPolicy(req.Job.RunPolicy)
	co, err := taskDataToCreatOptions(taskData, 0, req.Job.JobType, r.requestWorkerID(req.WorkerID), chapterTypeJobStart, payloadKindApp, inputHash, time.Now().UTC(), chapterMetadata{
		Attempt:       1,
		RunPolicy:     &jobPolicy,
		Metadata:      metadataForStartChapter(req.Job.Metadata),
		Prerequisites: prereqs,
	})
	if err != nil {
		return jobdb.JobHandle{}, err
	}
	initialStoredChapter, err := ChapterFromStoryChapter(co.InitialChapter)
	if err != nil {
		return jobdb.JobHandle{}, err
	}
	if err := jobschema.ValidateFirstChapter(ctx, r, jobdb.JobSchemaKey{TenantId: jobKey.TenantId, SchemaHash: schemaHash}, initialStoredChapter); err != nil {
		return jobdb.JobHandle{}, err
	}
	if _, err := r.chapterStore.CreateStory(ctx, storyKeyForJob(jobKey), co); err != nil {
		if req.Job.JobID != "" && errors.Is(err, core.ErrConflict) {
			if handle, handled, reconcileErr := r.reconcileExistingSubmitJob(ctx, req, jobKey, inputHash, prereqs, waitFor, jobPolicy, schemaHash); handled || reconcileErr != nil {
				return handle, reconcileErr
			}
		}
		return jobdb.JobHandle{}, err
	}
	if artifacts, _ := taskData.GetArtifacts(); len(artifacts) > 0 {
		assignArtifactKeys(artifacts, jobKey.JobId, 0)
		for _, art := range artifacts {
			if cleanupErr := art.Cleanup(); cleanupErr != nil {
				r.logger.Warn("failed to cleanup job input artifact", "artifact", art.Name(), "error", cleanupErr)
			}
		}
	}
	if err := r.ensureSubmittedJobRecord(ctx, jobKey, req.Job.JobType, storedMetadata, waitFor, jobPayload{RunPolicy: jobPolicy}, req.WorkerID, req.Job.AvailableAt); err != nil {
		return jobdb.JobHandle{}, err
	}
	return jobdb.JobHandle{JobKey: jobKey}, nil
}

func (r *Runtime) SubmitRestartJob(ctx context.Context, req jobdb.SubmitRestartJobRequest) (jobdb.JobHandle, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := r.validate(); err != nil {
		return jobdb.JobHandle{}, err
	}
	job := req.Job
	if job.LastStepToKeep < 0 {
		return jobdb.JobHandle{}, fmt.Errorf("LastStepToKeep must be >= 0 for restart")
	}

	jobKey := jobdb.JobKey{
		TenantId: job.PriorJobKey.TenantId,
		JobId:    job.JobID,
	}
	if jobKey.JobId == "" {
		jobKey.JobId = ksuid.New().String()
	}
	if err := jobKey.Validate(); err != nil {
		return jobdb.JobHandle{}, err
	}
	schemaHash, err := jobschema.ResolveActiveForNewJob(ctx, r, job.PriorJobKey.TenantId, job.Schema)
	if err != nil {
		return jobdb.JobHandle{}, err
	}
	storedMetadata, err := jobdb.BuildJobMetadataEnvelope(nil, jobdb.RuntimeJobMetadata{SchemaHash: schemaHash})
	if err != nil {
		return jobdb.JobHandle{}, err
	}
	prereqs, waitFor, err := normalizePrerequisites(jobKey, job.Prerequisites)
	if err != nil {
		return jobdb.JobHandle{}, err
	}
	sourceJob := storyKeyForJob(job.PriorJobKey)
	targetJob := storyKeyForJob(jobKey)

	chap0, err := r.chapterStore.Chapter(ctx, sourceJob, 0)
	if err != nil {
		return jobdb.JobHandle{}, fmt.Errorf("load source initial chapter: %w", err)
	}
	env0, err := decodeChapterEnvelope(chap0.Body())
	if err != nil {
		return jobdb.JobHandle{}, fmt.Errorf("decode source initial chapter: %w", err)
	}
	jobType := env0.Meta.TaskType
	jobPolicy := jobdb.RunPolicy{}
	if env0.Meta.RunPolicy != nil {
		jobPolicy = normalizeRunPolicy(*env0.Meta.RunPolicy)
	}

	nextOrdinal := job.LastStepToKeep + 1
	nextChap, err := r.chapterStore.Chapter(ctx, sourceJob, nextOrdinal)
	if err != nil {
		return jobdb.JobHandle{}, fmt.Errorf("LastStepToKeep %d invalid: no chapter at ordinal %d: %w", job.LastStepToKeep, nextOrdinal, err)
	}
	nextEnv, err := decodeChapterEnvelope(nextChap.Body())
	if err != nil {
		return jobdb.JobHandle{}, fmt.Errorf("decode source chapter %d: %w", nextOrdinal, err)
	}
	nextAttempt := nextEnv.Meta.Attempt
	if nextAttempt == 0 {
		nextAttempt = 1
	}
	if nextAttempt > 1 {
		return jobdb.JobHandle{}, fmt.Errorf("LastStepToKeep %d cuts into retry chain: next ordinal %d is attempt %d of %s", job.LastStepToKeep, nextOrdinal, nextAttempt, nextEnv.Meta.TaskType)
	}

	extra, err := buildRestartExtraExpectation(ctx, job, prereqs)
	if err != nil {
		return jobdb.JobHandle{}, err
	}

	createOptions := story.CreateOptions{RequestID: ksuid.New().String()}
	if job.ExtraTaskOutput != nil {
		hashInput := job.ExtraTaskInput
		if hashInput == nil {
			hashInput = jobdb.NewTaskDataOrPanic(map[string]any{})
		}
		inputHash, err := computeInputHash(ctx, hashInput)
		if err != nil {
			return jobdb.JobHandle{}, err
		}
		inputRef := &jobdb.InputReference{Ordinal: job.LastStepToKeep, Hash: inputHash}
		createOptions, err = taskDataToCreatOptions(job.ExtraTaskOutput, job.LastStepToKeep+1, restartExtraTaskType, r.requestWorkerID(req.WorkerID), chapterTypeRestartExtra, payloadKindApp, inputHash, time.Now().UTC(), chapterMetadata{
			Attempt:       1,
			InputRef:      inputRef,
			Prerequisites: prereqs,
		})
		if err != nil {
			return jobdb.JobHandle{}, err
		}
	}
	if schemaHash != "" {
		for ordinal := int64(0); ordinal <= job.LastStepToKeep; ordinal++ {
			sourceChapter, err := r.chapterStore.Chapter(ctx, sourceJob, ordinal)
			if err != nil {
				return jobdb.JobHandle{}, err
			}
			storedChapter, err := ChapterFromStoryChapter(sourceChapter)
			if err != nil {
				return jobdb.JobHandle{}, err
			}
			var validationErr error
			if ordinal == 0 {
				validationErr = jobschema.ValidateFirstChapter(ctx, r, jobdb.JobSchemaKey{TenantId: jobKey.TenantId, SchemaHash: schemaHash}, storedChapter)
			} else {
				validationErr = jobschema.ValidateOrdinaryChapter(ctx, r, jobdb.JobSchemaKey{TenantId: jobKey.TenantId, SchemaHash: schemaHash}, storedChapter)
			}
			if validationErr != nil {
				return jobdb.JobHandle{}, validationErr
			}
		}
		if createOptions.InitialChapter != nil {
			storedChapter, err := ChapterFromStoryChapter(createOptions.InitialChapter)
			if err != nil {
				return jobdb.JobHandle{}, err
			}
			if err := jobschema.ValidateOrdinaryChapter(ctx, r, jobdb.JobSchemaKey{TenantId: jobKey.TenantId, SchemaHash: schemaHash}, storedChapter); err != nil {
				return jobdb.JobHandle{}, err
			}
		}
	}

	if _, err := r.chapterStore.CloneStory(ctx, sourceJob, story.CloneOptions{
		DestinationKey: targetJob,
		LastOrdinal:    job.LastStepToKeep,
		CreateOptions:  createOptions,
	}); err != nil {
		if job.JobID != "" && errors.Is(err, core.ErrConflict) {
			if handle, handled, reconcileErr := r.reconcileExistingRestartJob(ctx, req, jobKey, prereqs, waitFor, jobType, jobPolicy, extra, storedMetadata); handled || reconcileErr != nil {
				return handle, reconcileErr
			}
		}
		return jobdb.JobHandle{}, err
	}
	if err := r.ensureSubmittedJobRecord(ctx, jobKey, jobType, storedMetadata, waitFor, jobPayload{RunPolicy: jobPolicy}, req.WorkerID, nil); err != nil {
		return jobdb.JobHandle{}, err
	}
	return jobdb.JobHandle{JobKey: jobKey}, nil
}

func (r *Runtime) startJob(ctx context.Context, jobKey jobdb.JobKey, jobType string, metadata json.RawMessage, waitFor []pgwf.JobID, payload jobPayload, workerID string, availableAt *time.Time) error {
	if ctx == nil {
		ctx = context.Background()
	}
	payloadJSON, err := encodeJobPayload(payload)
	if err != nil {
		return err
	}
	return pgwf.SubmitJob(ctx, r.pgwfDB(ctx), pgwf.TenantID(jobKey.TenantId), pgwf.JobID(jobKey.JobId), pgwf.JobDependencies{
		NextNeed:    pgwf.Capability(jobType),
		WaitFor:     waitFor,
		AvailableAt: optionalAvailableAt(availableAt),
	}, payloadJSON, metadata, pgwf.WorkerID(r.requestWorkerID(workerID)), time.Time{})
}

func optionalAvailableAt(availableAt *time.Time) time.Time {
	if availableAt == nil {
		return time.Time{}
	}
	return availableAt.UTC()
}

func (r *Runtime) CancelJob(ctx context.Context, req jobdb.CancelJobRequest) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := r.validate(); err != nil {
		return err
	}
	return pgwf.CancelJob(ctx, r.pgwfDB(ctx), pgwf.TenantID(req.JobKey.TenantId), pgwf.JobID(req.JobKey.JobId), pgwf.WorkerID(r.requestWorkerID(req.WorkerID)), req.Reason)
}

func (r *Runtime) PollWork(ctx context.Context, req jobdb.PollWorkRequest) ([]jobdb.ExecutionLease, error) {
	if err := r.validate(); err != nil {
		return nil, err
	}
	if req.TenantId == "" {
		return nil, fmt.Errorf("tenant_id is required for PollWork")
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
		TenantIDs:      []pgwf.TenantID{pgwf.TenantID(req.TenantId)},
	}
	if req.LeaseDuration != 0 {
		opts.LeaseSeconds = durationToLeaseSeconds(req.LeaseDuration)
	}

	out := make([]jobdb.ExecutionLease, 0, limit)
	workerID := r.requestWorkerID(req.WorkerID)
	attemptBudget := limit * 4
	if attemptBudget < 8 {
		attemptBudget = 8
	}
	for len(out) < limit && attemptBudget > 0 {
		attemptBudget--
		lease, err := pgwf.GetWorkWithOptions(ctx, r.udb, pgwf.WorkerID(workerID), caps, opts)
		if err != nil {
			return nil, err
		}
		if lease == nil {
			break
		}
		wrapped := &executionLease{runtime: r, lease: lease, udb: r.udb, workerID: workerID}
		ok, err := r.preflightScheduleLease(ctx, wrapped)
		if err != nil {
			return nil, err
		}
		if ok {
			out = append(out, wrapped)
		}
	}
	return out, nil
}

func (r *Runtime) GetJobLease(ctx context.Context, req jobdb.GetJobLeaseRequest) (jobdb.ExecutionLease, error) {
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
	wrapped := &executionLease{runtime: r, lease: lease, udb: r.udb, workerID: r.requestWorkerID(req.WorkerID)}
	ok, err := r.preflightScheduleLease(ctx, wrapped)
	if err != nil {
		return nil, err
	}
	if !ok {
		return nil, nil
	}
	return wrapped, nil
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
	status, err := pgwf.GetJobStatus(ctx, r.pgwfDB(ctx), pgwf.TenantID(jobKey.TenantId), pgwf.JobID(jobKey.JobId))
	if errors.Is(err, pgwf.ErrJobNotFound) {
		return jobdb.JobInfo{}, jobdb.ErrJobNotFound
	}
	if err != nil {
		return jobdb.JobInfo{}, fmt.Errorf("failed to get job status: %w", err)
	}
	job := jobdb.JobInfo{
		Status:     convertPgwfStatusToJobDB(status.Status, status.CancelRequested, status.ArchivedAt),
		Data:       &jobInfoTaskData{err: jobdb.ErrJobNotComplete},
		SchemaHash: jobmetadata.SchemaHashFromStoredMetadata(status.Metadata),
	}
	if status.ArchivedAt == nil {
		return job, nil
	}

	st, err := r.chapterStore.Story(ctx, storyKeyForJob(jobKey))
	if err != nil {
		return jobdb.JobInfo{}, err
	}
	chap, err := st.GetLastChapter(ctx)
	if err != nil {
		return jobdb.JobInfo{}, err
	}
	td, payloadErr := chapterToTaskData(chap, jobKey)
	job.Data = &jobInfoTaskData{taskData: td, err: payloadErr}
	return job, nil
}

func (r *Runtime) ListJobs(ctx context.Context, req jobdb.ListJobsRequest) (jobdb.ListJobsResponse, error) {
	if len(req.TenantIds) == 0 {
		return jobdb.ListJobsResponse{}, fmt.Errorf("tenant_ids is required for ListJobs")
	}

	metadataPredicates, err := metadataPredicatesToPgwf(req.MetadataFilter)
	if err != nil {
		return jobdb.ListJobsResponse{}, err
	}

	opts := pgwf.ListJobsOptions{
		TenantIDs:       append([]string(nil), req.TenantIds...),
		Statuses:        convertJobDBStatusesToPgwf(req.Statuses),
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
	result, err := pgwf.ListJobs(ctx, r.pgwfDB(ctx), opts)
	if err != nil {
		return jobdb.ListJobsResponse{}, fmt.Errorf("failed to list jobs: %w", err)
	}

	requestedStatuses := make(map[jobdb.JobStatus]bool, len(req.Statuses))
	for _, st := range req.Statuses {
		requestedStatuses[st] = true
	}
	requestedJobKeys := make(map[jobdb.JobKey]bool, len(req.JobKeys))
	for _, jk := range req.JobKeys {
		requestedJobKeys[jk] = true
	}

	jobs := make([]jobdb.JobSummary, 0, len(result.Jobs))
	for _, job := range result.Jobs {
		jobKey := jobdb.JobKey{TenantId: job.TenantID, JobId: job.JobID}
		if len(requestedJobKeys) > 0 && !requestedJobKeys[jobKey] {
			continue
		}
		jobdbStatus := convertPgwfStatusToJobDB(job.Status, job.CancelRequested, job.ArchivedAt)
		if len(requestedStatuses) > 0 && !requestedStatuses[jobdbStatus] {
			continue
		}
		summary := jobdb.JobSummary{
			JobKey:          jobKey,
			Status:          jobdbStatus,
			JobType:         jobdb.JobTypeFromNextNeed(job.NextNeed),
			NextNeed:        strPtr(job.NextNeed),
			WaitFor:         job.WaitFor,
			AvailableAt:     job.AvailableAt,
			ExpiresAt:       job.ExpiresAt,
			LeaseExpiresAt:  job.LeaseExpiresAt,
			CancelRequested: job.CancelRequested,
			CreatedAt:       job.CreatedAt,
			ArchivedAt:      job.ArchivedAt,
			Metadata:        jobdb.StripRuntimeMetadata(job.Metadata),
			SchemaHash:      jobmetadata.SchemaHashFromStoredMetadata(job.Metadata),
		}
		if strings.Contains(job.NextNeed, ":") {
			details, detailErr := pgwf.GetJob(ctx, r.pgwfDB(ctx), pgwf.TenantID(job.TenantID), pgwf.JobID(job.JobID), pgwf.GetJobOptions{IncludePayload: true})
			if detailErr != nil {
				return jobdb.ListJobsResponse{}, fmt.Errorf("failed to get job details: %w", detailErr)
			}
			summary.Payload = jobPayloadVisibleJSON(details.Payload)
			if tw, waitErr := extractTaskWaitFromRaw(details.Payload); waitErr == nil && tw != nil {
				summary.TaskWaitInput = &tw.InputStep
				summary.TaskWaitOutput = &tw.OutputStep
				summary.TaskWaitInputHash = strPtr(tw.InputHash)
				summary.TaskWaitNext = &tw.Next
			}
		}
		jobs = append(jobs, summary)
	}

	return jobdb.ListJobsResponse{
		Jobs:          jobs,
		NextPageToken: result.NextCursor,
	}, nil
}

func (r *Runtime) GetChapter(ctx context.Context, ref jobdb.ChapterRef) (jobdb.Chapter, error) {
	if err := r.validate(); err != nil {
		return jobdb.Chapter{}, err
	}
	chapter, err := r.chapterStore.Chapter(ctx, StoryKeyForJob(ref.JobKey), ref.Ordinal)
	if err != nil {
		if errors.Is(err, core.ErrNotFound) {
			return jobdb.Chapter{}, jobdb.ErrChapterNotFound
		}
		return jobdb.Chapter{}, err
	}
	return ChapterFromStoryChapter(chapter)
}

func (r *Runtime) ListChapters(ctx context.Context, req jobdb.ListChaptersRequest) ([]jobdb.Chapter, error) {
	if err := r.validate(); err != nil {
		return nil, err
	}
	if req.StartOrdinal < 0 {
		return nil, fmt.Errorf("start ordinal must be >= 0")
	}
	st, err := r.loadStory(ctx, storyKeyForJob(req.JobKey))
	if err != nil {
		if errors.Is(err, core.ErrNotFound) {
			return nil, jobdb.ErrJobNotFound
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

	out := make([]jobdb.Chapter, 0)
	for iter.HasNext() {
		chapter, err := iter.Next(ctx)
		if errors.Is(err, pagination.ErrNoMoreItems) {
			break
		}
		if err != nil {
			return nil, err
		}
		stored, err := ChapterFromStoryChapter(chapter)
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

func (r *Runtime) PutChapter(ctx context.Context, req jobdb.PutChapterRequest) error {
	if err := r.validate(); err != nil {
		return err
	}
	if req.LeaseID == "" {
		return fmt.Errorf("lease id is required for PutChapter")
	}
	if req.Ref.Ordinal < 0 {
		return fmt.Errorf("chapter ordinal must be >= 0")
	}
	if req.Chapter.Ordinal != req.Ref.Ordinal {
		return fmt.Errorf("chapter ordinal %d does not match target ordinal %d", req.Chapter.Ordinal, req.Ref.Ordinal)
	}
	schemaHash := ""
	if claims, ok := leaseauth.ClaimsFromContext(ctx); ok && leaseauth.Matches(claims, req.Ref.JobKey, req.LeaseID) {
		schemaHash = claims.SchemaHash
	}
	if authorized, err := leaseauth.Authorize(ctx, req.Ref.JobKey, req.LeaseID); err != nil {
		return err
	} else if !authorized {
		job, err := pgwf.GetJob(ctx, r.pgwfDB(ctx), pgwf.TenantID(req.Ref.JobKey.TenantId), pgwf.JobID(req.Ref.JobKey.JobId), pgwf.GetJobOptions{})
		if err != nil {
			if errors.Is(err, pgwf.ErrJobNotFound) {
				return jobdb.ErrJobNotFound
			}
			return err
		}
		if job.LeaseID == nil || *job.LeaseID != req.LeaseID {
			return jobdb.ErrExecutionLeaseLost
		}
		schemaHash = jobmetadata.SchemaHashFromStoredMetadata(job.Metadata)
	}
	if err := r.ensureNextVisibleChapterOrdinal(ctx, req.Ref.JobKey, req.Ref.Ordinal); err != nil {
		return err
	}
	chapter, attached, err := r.prepareChapterWrite(ctx, req)
	if err != nil {
		return err
	}
	if err := jobschema.ValidateOrdinaryChapter(ctx, r, jobdb.JobSchemaKey{TenantId: req.Ref.JobKey.TenantId, SchemaHash: schemaHash}, chapter); err != nil {
		return err
	}
	body, err := EncodeChapter(chapter)
	if err != nil {
		return err
	}
	builder := story.NewChapter().WithOrdinal(req.Ref.Ordinal).WithBytes(body)
	for _, art := range attached {
		builder.AddArtifact(art)
	}
	err = r.chapterStore.SaveChapter(ctx, StoryKeyForJob(req.Ref.JobKey), builder)
	if err != nil {
		if errors.Is(err, core.ErrConflict) {
			return fmt.Errorf("%w: chapter ordinal %d already exists or is not appendable", jobdb.ErrConflict, req.Ref.Ordinal)
		}
		return err
	}
	return nil
}

func (r *Runtime) ensureCompletionChapter(ctx context.Context, jobKey jobdb.JobKey, leaseID string, workerID string, req jobdb.CompleteExecutionRequest) error {
	if req.Chapter == nil {
		return fmt.Errorf("complete lease requires final chapter")
	}
	if !runtimecodec.ChapterIs(*req.Chapter, runtimecodec.ChapterTypeJobAttemptOutcome) {
		return fmt.Errorf("complete lease chapter must be %s", runtimecodec.ChapterTypeJobAttemptOutcome)
	}
	if req.Chapter.Ordinal < 0 {
		return fmt.Errorf("chapter ordinal must be >= 0")
	}
	detail, err := pgwf.GetJob(ctx, r.pgwfDB(ctx), pgwf.TenantID(jobKey.TenantId), pgwf.JobID(jobKey.JobId), pgwf.GetJobOptions{})
	if err != nil {
		if errors.Is(err, pgwf.ErrJobNotFound) {
			return jobdb.ErrJobNotFound
		}
		return err
	}
	if detail.LeaseID == nil || *detail.LeaseID != leaseID {
		return jobdb.ErrExecutionLeaseLost
	}
	if detail.LeaseExpiresAt == nil || !detail.LeaseExpiresAt.After(time.Now().UTC()) {
		return jobdb.ErrExecutionLeaseLost
	}
	ref := jobdb.ChapterRef{JobKey: jobKey, Ordinal: req.Chapter.Ordinal}
	if existing, ok, err := r.existingCompletionChapter(ctx, ref); err != nil {
		return err
	} else if ok {
		candidate := *req.Chapter
		if len(req.ArtifactUploads) > 0 {
			prepared, _, err := r.prepareChapterWrite(ctx, jobdb.PutChapterRequest{
				LeaseID:         leaseID,
				Ref:             ref,
				Chapter:         *req.Chapter,
				ArtifactUploads: req.ArtifactUploads,
			})
			if err != nil {
				return err
			}
			candidate = prepared
		}
		same, err := sameRuntimeChapter(existing, candidate)
		if err != nil {
			return err
		}
		if !same {
			return fmt.Errorf("%w: chapter ordinal %d already exists with different contents", jobdb.ErrConflict, ref.Ordinal)
		}
		return nil
	}
	if err := r.ensureNextVisibleChapterOrdinal(ctx, jobKey, ref.Ordinal); err != nil {
		return err
	}
	chapter, attached, err := r.prepareChapterWrite(ctx, jobdb.PutChapterRequest{
		LeaseID:         leaseID,
		Ref:             ref,
		Chapter:         *req.Chapter,
		ArtifactUploads: req.ArtifactUploads,
	})
	if err != nil {
		return err
	}
	schemaHash := jobmetadata.SchemaHashFromStoredMetadata(detail.Metadata)
	if err := jobschema.ValidateLastChapter(ctx, r, jobdb.JobSchemaKey{TenantId: jobKey.TenantId, SchemaHash: schemaHash}, chapter); err != nil {
		return err
	}
	body, err := EncodeChapter(chapter)
	if err != nil {
		return err
	}
	builder := story.NewChapter().WithOrdinal(ref.Ordinal).WithBytes(body)
	for _, art := range attached {
		builder.AddArtifact(art)
	}
	err = r.chapterStore.SaveChapter(ctx, StoryKeyForJob(jobKey), builder)
	if err != nil {
		if errors.Is(err, core.ErrConflict) {
			return fmt.Errorf("%w: chapter ordinal %d already exists or is not appendable", jobdb.ErrConflict, ref.Ordinal)
		}
		return err
	}
	return nil
}

func (r *Runtime) existingCompletionChapter(ctx context.Context, ref jobdb.ChapterRef) (jobdb.Chapter, bool, error) {
	chapter, err := r.loadChapter(ctx, storyKeyForJob(ref.JobKey), ref.Ordinal)
	if err != nil {
		if errors.Is(err, core.ErrNotFound) {
			return jobdb.Chapter{}, false, nil
		}
		return jobdb.Chapter{}, false, err
	}
	stored, err := ChapterFromStoryChapter(chapter)
	if err != nil {
		return jobdb.Chapter{}, false, err
	}
	return stored, true, nil
}

func sameRuntimeChapter(left jobdb.Chapter, right jobdb.Chapter) (bool, error) {
	leftBody, err := EncodeChapter(left)
	if err != nil {
		return false, err
	}
	rightBody, err := EncodeChapter(right)
	if err != nil {
		return false, err
	}
	if !bytes.Equal(leftBody, rightBody) {
		return false, nil
	}
	return reflect.DeepEqual(normalizeStoredArtifacts(left.Artifacts), normalizeStoredArtifacts(right.Artifacts)), nil
}

func normalizeStoredArtifacts(in []jobdb.StoredArtifact) []jobdb.StoredArtifact {
	out := append([]jobdb.StoredArtifact(nil), in...)
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out
}

func (r *Runtime) OpenArtifact(ctx context.Context, ref jobdb.ArtifactRef) (jobdb.ArtifactReader, error) {
	if err := r.validate(); err != nil {
		return nil, err
	}
	chapter, err := r.loadChapter(ctx, storyKeyForJob(ref.JobKey), ref.Ordinal)
	if err != nil {
		return nil, err
	}
	var found chapterartifact.Artifact
	for _, existing := range chapter.Artifacts() {
		if existing == nil || existing.Name() != ref.Name {
			continue
		}
		digest, _ := existing.Sha256(ctx)
		if ref.Digest != "" && digest != "" && ref.Digest != digest {
			continue
		}
		found = existing
		break
	}
	if found == nil {
		return nil, fmt.Errorf("artifact %s not found for job %s ordinal %d", ref.Name, ref.JobKey.JobId, ref.Ordinal)
	}
	return artifactReader{art: FromChapterArtifactForRuntime(found)}, nil
}

func (r *Runtime) validate() error {
	if r == nil {
		return fmt.Errorf("runtime is required")
	}
	if r.db == nil || r.udb == nil {
		return fmt.Errorf("db is required")
	}
	if r.chapterStore == nil {
		return fmt.Errorf("chapter store is required")
	}
	return nil
}

func (r *Runtime) dbFromCtx(ctx context.Context) *gorm.DB {
	if ctx == nil {
		ctx = context.Background()
	}
	if tx, ok := jobdb.TxFromCtx(ctx); ok && tx != nil {
		return tx.WithContext(ctx)
	}
	return r.db.WithContext(ctx)
}

func (r *Runtime) sqlTxFromCtx(ctx context.Context) *sql.Tx {
	if ctx == nil {
		return nil
	}
	if tx, ok := jobdb.TxFromCtx(ctx); ok && tx != nil {
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
	return r.chapterStore.Story(ctx, key)
}

func (r *Runtime) loadChapter(ctx context.Context, key story.Key, ordinal int64) (story.Chapter, error) {
	return r.chapterStore.Chapter(ctx, key, ordinal)
}

func (r *Runtime) ensureNextVisibleChapterOrdinal(ctx context.Context, jobKey jobdb.JobKey, ordinal int64) error {
	st, err := r.loadStory(ctx, storyKeyForJob(jobKey))
	if err != nil {
		if errors.Is(err, core.ErrNotFound) {
			return jobdb.ErrJobNotFound
		}
		return err
	}
	last, err := st.GetLastChapter(ctx)
	if err != nil {
		if errors.Is(err, core.ErrNotFound) {
			if ordinal == 0 {
				return nil
			}
			return fmt.Errorf("%w: chapter ordinal %d is not appendable; expected 0", jobdb.ErrConflict, ordinal)
		}
		return err
	}
	lastOrdinal := last.Ordinal()
	switch {
	case ordinal <= lastOrdinal:
		return fmt.Errorf("%w: chapter ordinal %d already exists", jobdb.ErrConflict, ordinal)
	case ordinal != lastOrdinal+1:
		return fmt.Errorf("%w: chapter ordinal %d is not appendable; expected %d", jobdb.ErrConflict, ordinal, lastOrdinal+1)
	default:
		return nil
	}
}

func (r *Runtime) prepareChapterWrite(ctx context.Context, req jobdb.PutChapterRequest) (jobdb.Chapter, []chapterartifact.Artifact, error) {
	chapter := req.Chapter
	if len(req.ArtifactUploads) == 0 {
		if len(chapter.Artifacts) > 0 {
			return jobdb.Chapter{}, nil, fmt.Errorf("put chapter with artifact descriptors but no artifact uploads")
		}
		return chapter, nil, nil
	}

	stored := make([]jobdb.StoredArtifact, 0, len(req.ArtifactUploads))
	attached := make([]chapterartifact.Artifact, 0, len(req.ArtifactUploads))
	for _, item := range req.ArtifactUploads {
		if item.Open == nil {
			return jobdb.Chapter{}, nil, fmt.Errorf("artifact %q is missing opener", item.Name)
		}
		reader, err := item.Open()
		if err != nil {
			return jobdb.Chapter{}, nil, err
		}
		data, err := io.ReadAll(reader)
		_ = reader.Close()
		if err != nil {
			return jobdb.Chapter{}, nil, err
		}
		art := jobdb.NewArtifactFromBytes(item.Name, data)
		digest, err := art.Sha256(ctx)
		if err != nil {
			return jobdb.Chapter{}, nil, err
		}
		stored = append(stored, jobdb.StoredArtifact{
			Name:   item.Name,
			Digest: digest,
			Size:   int64(len(data)),
		})
		attached = append(attached, ToChapterArtifactForRuntime(art))
	}
	if err := validateChapterArtifactDescriptors(chapter.Artifacts, stored); err != nil {
		return jobdb.Chapter{}, nil, err
	}
	chapter.Artifacts = stored
	return chapter, attached, nil
}

func validateChapterArtifactDescriptors(existing []jobdb.StoredArtifact, computed []jobdb.StoredArtifact) error {
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
	art jobdb.Artifact
}

func (a artifactReader) Open() (io.ReadCloser, error) { return a.art.Open() }
func (a artifactReader) Size() int64                  { return a.art.Size() }
func (a artifactReader) Name() string                 { return a.art.Name() }

type executionLease struct {
	runtime    *Runtime
	lease      *pgwf.Lease
	udb        *sql.DB
	workerID   string
	schemaHash string
}

func (l *executionLease) LeaseID() string {
	return l.lease.LeaseID()
}

func (l *executionLease) Job() jobdb.JobHandle {
	return jobdb.JobHandle{
		JobKey: jobdb.JobKey{
			TenantId: string(l.lease.TenantID()),
			JobId:    string(l.lease.JobID()),
		},
	}
}

func (l *executionLease) Capability() string {
	return string(l.lease.NextNeed())
}

func (l *executionLease) Payload() json.RawMessage {
	return jobPayloadVisibleJSON(l.lease.Payload())
}

func (l *executionLease) LeaseWorkerID() string {
	return l.workerID
}

func (l *executionLease) LeaseExpiry() time.Time {
	return l.lease.LeaseExpiry()
}

func (l *executionLease) LeaseSchemaHash() string {
	return l.schemaHash
}

func (l *executionLease) KeepAlive(ctx context.Context) error {
	_ = l.lease.WithKeepAlive(l.udb)
	return nil
}

func (l *executionLease) StopKeepAlive() {
	stopLeaseKeepAlive(l.lease)
}

func (l *executionLease) Complete(ctx context.Context, req jobdb.CompleteExecutionRequest) error {
	if l.runtime == nil {
		return fmt.Errorf("runtime is required for lease completion")
	}
	if err := l.runtime.ensureCompletionChapter(ctx, l.Job().JobKey, l.LeaseID(), l.workerID, req); err != nil {
		return err
	}
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
			return jobdb.ErrExecutionLeaseLost
		}
	}
	return err
}

func (l *executionLease) Reschedule(ctx context.Context, req jobdb.RescheduleExecutionRequest) error {
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
	storedPayload, err := jobPayloadFromVisibleJSON(payload)
	if err != nil {
		return err
	}
	payloadJSON, err := encodeJobPayload(storedPayload)
	if err != nil {
		return err
	}
	err = l.lease.Reschedule(ctx, l.udb, deps, payloadJSON)
	if err != nil {
		if errors.Is(err, pgwf.ErrLeaseMismatch) || errors.Is(err, pgwf.ErrLeaseExpired) {
			return jobdb.ErrExecutionLeaseLost
		}
	}
	return err
}
