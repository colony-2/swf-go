package impl

import (
	"context"

	"github.com/colony-2/pgwf-go/pkg/pgwf"
	"github.com/colony-2/strata/strata-go/pkg/client/story"
	"github.com/colony-2/swf-go/pkg/swf"
)

type taskHandleImpl struct {
	job           Job
	inputOrdinal  int64
	outputOrdinal int64
	inputChapter  story.Chapter
	engine        *swfEngineImpl
	nextNeed      pgwf.Capability
}

func (h *taskHandleImpl) chapter() (story.Chapter, error) {
	if h.inputChapter == nil {
		chapter, err := h.engine.strata.Chapter(context.TODO(), story.Key{AnthologyID: h.engine.tenantId, StoryID: string(h.JobId())}, h.inputOrdinal)
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
	return chapterToTaskData(c), nil
}

func (h *taskHandleImpl) JobId() swf.JobId {
	return swf.JobId(h.job.JobID)
}

func (h *taskHandleImpl) Finish(ctx context.Context, taskData swf.TaskData) error {
	// write the story.
	chap, err := taskDataToChapter(taskData, h.outputOrdinal)
	if err != nil {
		return err
	}
	err = h.engine.strata.SaveChapter(context.TODO(), story.Key{
		AnthologyID: h.engine.tenantId,
		StoryID:     h.job.JobID,
	}, chap)
	if err != nil {
		return err
	}
	return pgwf.RescheduleUnheldJob(
		ctx,
		h.engine.udb,
		pgwf.JobID(h.job.JobID),
		pgwf.WorkerID(h.engine.workerId), pgwf.JobDependencies{NextNeed: h.nextNeed},
		nil)
}

var _ swf.TaskHandle = &taskHandleImpl{}

func chapterToTaskData(chapter story.Chapter) swf.TaskData {
	artifacts := make([]swf.Artifact, 0, len(chapter.Artifacts()))
	for _, a := range chapter.Artifacts() {
		artifacts = append(artifacts, a)
	}

	data := swf.NewBytesData(chapter.Body())
	task := swf.SimpleTaskData{
		Data:      data,
		Artifacts: artifacts,
	}
	return &task

}
