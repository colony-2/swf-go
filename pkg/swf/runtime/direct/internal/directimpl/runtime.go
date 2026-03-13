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
	"strings"
	"sync"
	"time"

	"github.com/colony-2/pgwf-go/pkg/pgwf"
	strataclient "github.com/colony-2/strata-go/pkg/client"
	strataartifact "github.com/colony-2/strata-go/pkg/client/artifact"
	"github.com/colony-2/strata-go/pkg/client/core"
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

	pendingMu        sync.Mutex
	pendingArtifacts map[pendingArtifactKey]pendingArtifact
}

var _ swf.WorkflowRuntime = (*Runtime)(nil)

type pendingArtifactKey struct {
	jobKey  swf.JobKey
	ordinal int64
	name    string
	digest  string
}

type pendingArtifact struct {
	size int64
	art  swf.Artifact
}

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
		db:               db,
		udb:              udb,
		strataClient:     strataClient,
		logger:           slog.Default(),
		workerID:         fmt.Sprintf("%s:%d-%s", host, os.Getpid(), ksuid.New().String()),
		pendingArtifacts: make(map[pendingArtifactKey]pendingArtifact),
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

func (r *Runtime) StartJob(ctx context.Context, req swf.StartJobRequest) (swf.JobHandle, error) {
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

func (r *Runtime) RestartJob(ctx context.Context, req swf.RestartJobRequest) (swf.JobHandle, error) {
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

	out := make([]swf.ExecutionLease, 0, limit)
	for i := 0; i < limit; i++ {
		lease, err := pgwf.GetWork(ctx, r.udb, pgwf.WorkerID(r.requestWorkerID(req.WorkerID)), caps, nil)
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

func (r *Runtime) CheckJobStatus(ctx context.Context, jobKey swf.JobKey) (swf.JobStatus, error) {
	status, err := pgwf.GetJobStatus(ctx, r.pgwfDB(ctx), pgwf.TenantID(jobKey.TenantId), pgwf.JobID(jobKey.JobId))
	if errors.Is(err, pgwf.ErrJobNotFound) {
		return "", swf.ErrJobNotFound
	}
	if err != nil {
		return "", fmt.Errorf("failed to get job status: %w", err)
	}
	return convertPgwfStatusToSwf(status.Status, status.CancelRequested, status.ArchivedAt), nil
}

func (r *Runtime) GetJobResult(ctx context.Context, jobKey swf.JobKey) (swf.TaskData, error) {
	isArchived, err := pgwf.IsJobArchived(ctx, r.pgwfDB(ctx), pgwf.TenantID(jobKey.TenantId), pgwf.JobID(jobKey.JobId))
	if err != nil {
		return nil, fmt.Errorf("failed to check if job is archived: %w", err)
	}
	if !isArchived {
		return nil, swf.ErrJobNotComplete
	}

	st, err := r.strataClient.Story(ctx, storyKeyForJob(jobKey))
	if err != nil {
		return nil, err
	}
	chap, err := st.GetLastChapter(ctx)
	if err != nil {
		return nil, err
	}
	td, payloadErr := chapterToTaskData(chap, jobKey)
	if payloadErr != nil {
		return td, payloadErr
	}
	return td, nil
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
	requestedJobIDs := make(map[string]bool, len(req.JobKeys))
	for _, jk := range req.JobKeys {
		requestedJobIDs[jk.JobId] = true
	}

	jobs := make([]swf.JobSummary, 0, len(result.Jobs))
	for _, job := range result.Jobs {
		if len(requestedJobIDs) > 0 && !requestedJobIDs[job.JobID] {
			continue
		}
		swfStatus := convertPgwfStatusToSwf(job.Status, job.CancelRequested, job.ArchivedAt)
		if len(requestedStatuses) > 0 && !requestedStatuses[swfStatus] {
			continue
		}
		summary := swf.JobSummary{
			JobKey:          swf.JobKey{TenantId: job.TenantID, JobId: job.JobID},
			Status:          swfStatus,
			JobType:         swf.JobTypeFromNextNeed(job.NextNeed),
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

func (r *Runtime) PutChapter(ctx context.Context, req swf.PutChapterRequest) error {
	if err := r.validate(); err != nil {
		return err
	}
	body, err := EncodeStoredChapter(req.Chapter)
	if err != nil {
		return err
	}
	builder := story.NewChapter().WithOrdinal(req.Ref.Ordinal).WithBytes(body)
	attached, err := r.takePendingArtifacts(req.Ref.JobKey, req.Ref.Ordinal, req.Chapter.Artifacts)
	if err != nil {
		return err
	}
	for _, art := range attached {
		builder.AddArtifact(art)
	}
	return r.strataClient.SaveChapter(ctx, StoryKeyForJob(req.Ref.JobKey), builder)
}

func (r *Runtime) OpenArtifact(ctx context.Context, ref swf.ArtifactRef) (swf.ArtifactReader, error) {
	if err := r.validate(); err != nil {
		return nil, err
	}
	art := strataartifact.FromRemote(
		strataartifact.Descriptor{
			Name:        ref.Name,
			ContentType: "application/octet-stream",
			SizeBytes:   0,
			Sha256:      ref.Digest,
		},
		strataartifact.Locator{
			AnthologyID: ref.JobKey.TenantId,
			StoryID:     ref.JobKey.JobId,
			Ordinal:     ref.Ordinal,
			Name:        ref.Name,
		},
		r.strataClient.Core(),
	)
	return artifactReader{art: FromStrataArtifactForRuntime(art)}, nil
}

func (r *Runtime) PutArtifacts(ctx context.Context, req swf.PutArtifactsRequest) ([]swf.StoredArtifact, error) {
	if err := r.validate(); err != nil {
		return nil, err
	}
	out := make([]swf.StoredArtifact, 0, len(req.Items))
	for _, item := range req.Items {
		if item.Open == nil {
			return nil, fmt.Errorf("artifact %q is missing opener", item.Name)
		}
		reader, err := item.Open()
		if err != nil {
			return nil, err
		}
		data, err := io.ReadAll(reader)
		_ = reader.Close()
		if err != nil {
			return nil, err
		}
		art := swf.NewArtifact(item.Name, func() (io.ReadCloser, int64, error) {
			return io.NopCloser(bytes.NewReader(data)), int64(len(data)), nil
		}, func() error { return nil })
		digest, err := art.Sha256(ctx)
		if err != nil {
			return nil, err
		}
		key := pendingArtifactKey{
			jobKey:  req.JobKey,
			ordinal: req.Ordinal,
			name:    item.Name,
			digest:  digest,
		}
		r.pendingMu.Lock()
		r.pendingArtifacts[key] = pendingArtifact{
			size: int64(len(data)),
			art:  art,
		}
		r.pendingMu.Unlock()
		out = append(out, swf.StoredArtifact{
			Name:   item.Name,
			Digest: digest,
			Size:   int64(len(data)),
		})
	}
	return out, nil
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

func (r *Runtime) takePendingArtifacts(jobKey swf.JobKey, ordinal int64, refs []swf.StoredArtifact) ([]strataartifact.Artifact, error) {
	attached := make([]strataartifact.Artifact, 0, len(refs))
	for _, ref := range refs {
		key := pendingArtifactKey{
			jobKey:  jobKey,
			ordinal: ordinal,
			name:    ref.Name,
			digest:  ref.Digest,
		}
		r.pendingMu.Lock()
		pending, ok := r.pendingArtifacts[key]
		if ok {
			delete(r.pendingArtifacts, key)
		}
		r.pendingMu.Unlock()
		if ok {
			attached = append(attached, ToStrataArtifactForRuntime(pending.art))
			continue
		}
		attached = append(attached, strataartifact.FromRemote(
			strataartifact.Descriptor{
				Name:        ref.Name,
				ContentType: "application/octet-stream",
				SizeBytes:   ref.Size,
				Sha256:      ref.Digest,
			},
			strataartifact.Locator{
				AnthologyID: jobKey.TenantId,
				StoryID:     jobKey.JobId,
				Ordinal:     ordinal,
				Name:        ref.Name,
			},
			r.strataClient.Core(),
		))
	}
	return attached, nil
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
