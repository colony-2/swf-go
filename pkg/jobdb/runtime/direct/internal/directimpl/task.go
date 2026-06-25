package directimpl

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/chapterstore/core"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/chapterstore/story"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/jobmetadata"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/jobschema"
	"github.com/colony-2/pgwf-go/pkg/pgwf"
)

type taskHandleImpl struct {
	jobID         string
	tenantId      string
	payload       json.RawMessage
	metadata      json.RawMessage
	inputOrdinal  int64
	outputOrdinal int64
	inputChapter  story.Chapter
	runtime       *Runtime
	nextNeed      pgwf.Capability
	taskType      string
	createdAt     time.Time
}

func (h *taskHandleImpl) TaskOrdinalToComplete() int64 {
	return h.outputOrdinal
}

func (h *taskHandleImpl) TaskType() string {
	return h.taskType
}

func (h *taskHandleImpl) CreatedAt() time.Time {
	return h.createdAt
}

func (h *taskHandleImpl) Metadata() json.RawMessage {
	if h.metadata == nil {
		return json.RawMessage(`{}`)
	}
	cpy := make(json.RawMessage, len(h.metadata))
	copy(cpy, h.metadata)
	return cpy
}

func (h *taskHandleImpl) chapter() (story.Chapter, error) {
	if h.inputChapter == nil {
		jobKey := h.JobKey()
		if h.runtime == nil {
			return nil, fmt.Errorf("task handle backend is nil")
		}
		chapter, err := h.runtime.chapterStore.Chapter(context.TODO(), storyKeyForJob(jobKey), h.inputOrdinal)
		if err != nil {
			return nil, err
		}

		h.inputChapter = chapter
	}
	return h.inputChapter, nil
}

func (h *taskHandleImpl) Data() (jobdb.TaskData, error) {
	c, err := h.chapter()
	if err != nil {
		return nil, err
	}
	return chapterToTaskData(c, h.JobKey())
}

func (h *taskHandleImpl) JobKey() jobdb.JobKey {
	return jobdb.JobKey{
		TenantId: h.tenantId,
		JobId:    h.jobID,
	}
}

func (h *taskHandleImpl) Finish(ctx context.Context, taskData jobdb.TaskData) error {
	tw, err := extractTaskWaitFromRaw(h.payload)
	if err != nil {
		return err
	}
	if tw == nil {
		return nil
	}
	return h.runtime.CompleteTaskIfWaiting(ctx, jobdb.CompleteTaskIfWaitingRequest{
		JobKey:        h.JobKey(),
		Capability:    jobdb.JobTypeFromNextNeed(string(h.nextNeed)) + ":" + h.taskType,
		ResumeNeed:    string(h.nextNeed),
		InputOrdinal:  tw.InputStep,
		OutputOrdinal: tw.OutputStep,
		InputHash:     tw.InputHash,
		Data:          taskData,
	})
}

func (r *Runtime) CompleteTaskIfWaiting(ctx context.Context, req jobdb.CompleteTaskIfWaitingRequest) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := r.validate(); err != nil {
		return err
	}
	jobKey := req.JobKey
	job, err := pgwf.GetJob(ctx, r.pgwfDB(ctx), pgwf.TenantID(jobKey.TenantId), pgwf.JobID(jobKey.JobId), pgwf.GetJobOptions{IncludePayload: true})
	if errors.Is(err, pgwf.ErrJobNotFound) {
		return jobdb.ErrJobNotFound
	}
	if err != nil {
		return fmt.Errorf("failed to load job: %w", err)
	}
	tw, err := extractTaskWaitFromRaw(job.Payload)
	if err != nil {
		return err
	}
	if tw == nil {
		return fmt.Errorf("%w: job is not waiting on an external task", jobdb.ErrConflict)
	}
	if req.Capability != "" && job.NextNeed != req.Capability {
		return fmt.Errorf("%w: waiting capability %q does not match requested capability %q", jobdb.ErrConflict, job.NextNeed, req.Capability)
	}
	if req.ResumeNeed != "" && tw.Next != req.ResumeNeed {
		return fmt.Errorf("%w: resume need %q does not match requested resume need %q", jobdb.ErrConflict, tw.Next, req.ResumeNeed)
	}
	if req.InputOrdinal != 0 && tw.InputStep != req.InputOrdinal {
		return fmt.Errorf("%w: waiting input ordinal %d does not match requested input ordinal %d", jobdb.ErrConflict, tw.InputStep, req.InputOrdinal)
	}
	if tw.OutputStep != req.OutputOrdinal {
		return fmt.Errorf("%w: waiting output ordinal %d does not match requested output ordinal %d", jobdb.ErrConflict, tw.OutputStep, req.OutputOrdinal)
	}
	if req.InputHash != "" && tw.InputHash != req.InputHash {
		return fmt.Errorf("%w: waiting input hash does not match requested input hash", jobdb.ErrConflict)
	}

	var inputChapter story.Chapter
	if tw.InputStep > 0 {
		inputChapter, err = r.chapterStore.Chapter(ctx, storyKeyForJob(jobKey), tw.InputStep)
		if err != nil {
			return fmt.Errorf("failed to load input chapter: %w", err)
		}
	}

	payload, err := decodeJobPayload(job.Payload)
	if err != nil {
		return err
	}

	meta := chapterMetadata{}
	if inputChapter != nil {
		if env, decErr := decodeChapterEnvelope(inputChapter.Body()); decErr == nil {
			if env.Meta.Attempt > 0 {
				meta.Attempt = env.Meta.Attempt
			}
			if env.Meta.MaxAttempts > 0 {
				meta.MaxAttempts = env.Meta.MaxAttempts
			}
			if env.Meta.NextAttemptAt != nil {
				meta.NextAttemptAt = env.Meta.NextAttemptAt
			}
			if env.Meta.BackoffMillis > 0 {
				meta.BackoffMillis = env.Meta.BackoffMillis
			}
			if env.Meta.Retryable != nil {
				meta.Retryable = env.Meta.Retryable
			}
			if env.Meta.InputRef != nil {
				meta.InputRef = env.Meta.InputRef
			}
		}
	}
	if payload.RunPolicy.Retry.MaximumAttempts > 0 {
		meta.RunPolicy = &payload.RunPolicy
	}

	workerID := r.workerID
	taskType := taskTypeFromCapability(job.NextNeed)
	if req.Capability != "" {
		taskType = taskTypeFromCapability(req.Capability)
	}
	if taskType == "" || taskType == job.NextNeed || (req.Capability != "" && taskType == req.Capability) {
		return fmt.Errorf("task type not found in capability")
	}
	chap, err := taskDataToChapter(req.Data, tw.OutputStep, taskType, workerID, chapterTypeTaskAttemptOutcome, payloadKindApp, tw.InputHash, time.Now().UTC(), meta)
	if err != nil {
		return err
	}
	storedChapter, err := ChapterFromStoryChapter(chap)
	if err != nil {
		return err
	}
	if err := jobschema.ValidateOrdinaryChapter(ctx, r, jobdb.JobSchemaKey{TenantId: jobKey.TenantId, SchemaHash: jobmetadata.SchemaHashFromStoredMetadata(job.Metadata)}, storedChapter); err != nil {
		return err
	}
	if err := r.ensureNextVisibleChapterOrdinal(ctx, jobKey, tw.OutputStep); err != nil {
		return err
	}
	err = r.chapterStore.SaveChapter(ctx, storyKeyForJob(jobKey), chap)
	if err != nil {
		if errors.Is(err, core.ErrConflict) {
			return fmt.Errorf("%w: output chapter %d already exists or is not appendable", jobdb.ErrConflict, tw.OutputStep)
		}
		return err
	}
	artifacts, _ := req.Data.GetArtifacts()
	assignArtifactKeys(artifacts, jobKey.JobId, tw.OutputStep)
	tenantID := pgwf.TenantID(jobKey.TenantId)
	resumeNeed := tw.Next
	if req.ResumeNeed != "" {
		resumeNeed = req.ResumeNeed
	}

	resumePayload, err := encodeJobPayload(jobPayload{RunPolicy: payload.RunPolicy})
	if err != nil {
		return err
	}

	err = pgwf.RescheduleUnheldJob(
		ctx,
		r.pgwfDB(ctx),
		tenantID,
		pgwf.JobID(jobKey.JobId),
		pgwf.WorkerID(workerID), pgwf.JobDependencies{NextNeed: pgwf.Capability(resumeNeed)},
		resumePayload)
	if err != nil {
		switch {
		case errors.Is(err, pgwf.ErrJobNotFound):
			return jobdb.ErrJobNotFound
		case errors.Is(err, pgwf.ErrLeaseMismatch), errors.Is(err, pgwf.ErrLeaseExpired):
			return fmt.Errorf("%w: job is no longer in a commit-if-waiting state", jobdb.ErrConflict)
		default:
			return err
		}
	}
	return nil
}

func chapterToTaskData(chapter story.Chapter, jobKey jobdb.JobKey) (jobdb.TaskData, error) {
	artifacts := make([]jobdb.Artifact, 0, len(chapter.Artifacts()))
	for _, a := range chapter.Artifacts() {
		artifacts = append(artifacts, fromChapterArtifact(a))
	}
	assignArtifactKeys(artifacts, jobKey.JobId, chapter.Ordinal())

	env, err := decodeChapterEnvelope(chapter.Body())
	if err != nil {
		return nil, err
	}

	return envelopeToTaskData(env, artifacts)
}
