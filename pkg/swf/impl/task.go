package impl

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/colony-2/pgwf-go/pkg/pgwf"
	"github.com/colony-2/strata-go/pkg/client/story"
	"github.com/colony-2/swf-go/pkg/swf"
)

type taskHandleImpl struct {
	jobID         string
	tenantId      string
	payload       json.RawMessage
	metadata      json.RawMessage
	inputOrdinal  int64
	outputOrdinal int64
	inputChapter  story.Chapter
	engine        *swfEngineImpl
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
		chapter, err := h.engine.strata.Chapter(context.TODO(), jobKey.ToStoryKey(), h.inputOrdinal)
		if err != nil {
			return nil, err
		}

		h.inputChapter = chapter
	}
	return h.inputChapter, nil
}

func (h *taskHandleImpl) Data() (swf.TaskData, error) {
	c, err := h.chapter()
	if err != nil {
		return nil, err
	}
	return chapterToTaskData(c, h.JobKey())
}

func (h *taskHandleImpl) JobKey() swf.JobKey {
	return swf.JobKey{
		TenantId: h.tenantId,
		JobId:    h.jobID,
	}
}

func (h *taskHandleImpl) Finish(ctx context.Context, taskData swf.TaskData) error {
	if ctx == nil {
		ctx = context.Background()
	}
	// Load input chapter if not already set (e.g., when created by FindTasksWaitingForCapability)
	if h.inputChapter == nil && h.inputOrdinal > 0 {
		jobKey := h.JobKey()
		ch, err := h.engine.strata.Chapter(ctx, jobKey.ToStoryKey(), h.inputOrdinal)
		if err != nil {
			return fmt.Errorf("failed to load input chapter: %w", err)
		}
		h.inputChapter = ch
	}

	// Compute input hash from input chapter
	tw, err := extractTaskWaitFromRaw(h.payload)
	if err != nil || tw.InputHash == "" {
		return fmt.Errorf("input hash not found in payload")
	}
	inputHash := tw.InputHash

	// Extract metadata from payload and input chapter
	var payload jobPayload
	_ = json.Unmarshal(h.payload, &payload)

	// Build chapter metadata from input chapter and payload
	meta := chapterMetadata{}
	if h.inputChapter != nil {
		if env, decErr := decodeChapterEnvelope(h.inputChapter.Body()); decErr == nil {
			// Preserve attempt information from input
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
	// Include RunPolicy from payload
	if payload.RunPolicy.Retry.MaximumAttempts > 0 {
		meta.RunPolicy = &payload.RunPolicy
	}

	chap, err := taskDataToChapter(taskData, h.outputOrdinal, h.taskType, h.engine.workerId, payloadKindApp, inputHash, time.Now().UTC(), meta)
	if err != nil {
		return err
	}
	jobKey := h.JobKey()
	err = h.engine.strata.SaveChapter(ctx, jobKey.ToStoryKey(), chap)
	if err != nil {
		return err
	}
	artifacts, _ := taskData.GetArtifacts()
	assignArtifactKeys(artifacts, jobKey.JobId, h.outputOrdinal)
	tenantID := pgwf.TenantID(h.tenantId)
	return pgwf.RescheduleUnheldJob(
		ctx,
		h.engine.pgwfDB(ctx),
		tenantID,
		pgwf.JobID(h.jobID),
		pgwf.WorkerID(h.engine.workerId), pgwf.JobDependencies{NextNeed: h.nextNeed},
		jobPayload{RunPolicy: payload.RunPolicy})
}

var _ swf.TaskHandle = &taskHandleImpl{}

func chapterToTaskData(chapter story.Chapter, jobKey swf.JobKey) (swf.TaskData, error) {
	artifacts := make([]swf.Artifact, 0, len(chapter.Artifacts()))
	for _, a := range chapter.Artifacts() {
		artifacts = append(artifacts, swf.FromStrataArtifact(a))
	}
	assignArtifactKeys(artifacts, jobKey.JobId, chapter.Ordinal())

	env, err := decodeChapterEnvelope(chapter.Body())
	if err != nil {
		return nil, err
	}

	return envelopeToTaskData(env, artifacts)
}
