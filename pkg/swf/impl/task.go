package impl

import (
	"context"
	"fmt"
	"time"

	"github.com/colony-2/pgwf-go/pkg/pgwf"
	"github.com/colony-2/strata/strata-go/pkg/client/story"
	"github.com/colony-2/swf-go/pkg/swf"
)

type taskHandleImpl struct {
	engine       *swfEngineImpl
	job          Job
	story        *story.Story
	inputChapter story.Chapter
}

func (t taskHandleImpl) chapter() (story.Chapter, error) {
	if t.inputChapter == nil {
		chapter, err := t.engine.strata.Chapter(context.TODO(), story.Key{AnthologyID: t.engine.tenantId, StoryID: string(t.JobId())}, -1)
		if err != nil {
			return nil, err
		}

		t.inputChapter = chapter
	}
	return t.inputChapter, nil
}

func (t taskHandleImpl) Data() (swf.TaskData, error) {
	c, err := t.chapter()
	if err != nil {
		return nil, err
	}
	return chapterToTaskData(c), nil
}

func (t taskHandleImpl) JobId() swf.JobId {
	return swf.JobId(t.job.JobID)
}

func (t taskHandleImpl) Finish(ctx context.Context, taskOutput swf.TaskData, nextNeed swf.Capability, waitFor []swf.JobId, wait time.Duration) error {
	c, err := t.chapter()
	if err != nil {
		return err
	}
	ord := c.Ordinal()
	newChapter, err := taskDataToChapter(taskOutput, ord+1)
	if err != nil {
		return err
	}

	err = t.engine.strata.SaveChapter(ctx, story.Key{AnthologyID: t.engine.tenantId, StoryID: string(t.job.JobID)}, newChapter)
	if err != nil {
		return fmt.Errorf("failed to save chapter: %w", err)
	}

	waits := make([]pgwf.JobID, len(waitFor))

	for i, j := range waitFor {
		waits[i] = pgwf.JobID(j)
	}

	availableAt := time.Time{}
	if wait > 0 {
		availableAt = time.Now().Add(wait)
	}
	dep := pgwf.JobDependencies{
		NextNeed:     pgwf.Capability(nextNeed),
		WaitFor:      waits,
		SingletonKey: t.job.SingletonKey,
		AvailableAt:  availableAt,
	}
	return pgwf.RescheduleUnheldJob(ctx, t.engine.udb, pgwf.JobID(t.JobId()), pgwf.WorkerID(t.engine.workerId), dep)
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
