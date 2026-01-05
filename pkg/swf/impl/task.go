package impl

import (
	"context"
	"encoding/json"
	"time"

	"github.com/colony-2/pgwf-go/pkg/pgwf"
	"github.com/colony-2/strata-go/pkg/client/story"
	"github.com/colony-2/swf-go/pkg/swf"
)

type taskHandleImpl struct {
	jobID         string
	tenantId      string
	payload       json.RawMessage
	inputOrdinal  int64
	outputOrdinal int64
	inputChapter  story.Chapter
	engine        *swfEngineImpl
	nextNeed      pgwf.Capability
	taskType      string
}

func (h *taskHandleImpl) TaskOrdinalToComplete() int64 {
	return h.outputOrdinal
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
	return chapterToTaskData(c)
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
	// write the story.
	inputTD, err := chapterToTaskData(h.inputChapter)
	if err != nil {
		return err
	}
	inputHash, err := computeInputHash(ctx, inputTD)
	if err != nil {
		return err
	}

	chap, err := taskDataToChapter(taskData, h.outputOrdinal, h.taskType, h.engine.workerId, payloadKindApp, inputHash, time.Now().UTC(), chapterMetadata{})
	if err != nil {
		return err
	}
	jobKey := h.JobKey()
	err = h.engine.strata.SaveChapter(context.TODO(), jobKey.ToStoryKey(), chap)
	if err != nil {
		return err
	}
	var payload jobPayload
	_ = json.Unmarshal(h.payload, &payload)
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

func chapterToTaskData(chapter story.Chapter) (swf.TaskData, error) {
	artifacts := make([]swf.Artifact, 0, len(chapter.Artifacts()))
	for _, a := range chapter.Artifacts() {
		artifacts = append(artifacts, swf.FromStrataArtifact(a))
	}

	env, err := decodeChapterEnvelope(chapter.Body())
	if err != nil {
		return nil, err
	}

	return envelopeToTaskData(env, artifacts)
}
