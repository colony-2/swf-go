package impl

import (
	"context"
	"errors"
	"fmt"
	"runtime"

	"github.com/colony-2/pgwf-go/pkg/pgwf"
	"github.com/colony-2/strata/strata-go/pkg/client/core"
	"github.com/colony-2/strata/strata-go/pkg/client/story"
	"github.com/colony-2/swf-go/pkg/swf"
)

type runner struct {
	jobId        pgwf.JobID
	worker       *swf.WorkSet
	storyCounter int64
	engine       *swfEngineImpl
	lease        *pgwf.Lease
}

func (r *runner) GetJobId() swf.JobId {
	return swf.JobId(r.jobId)
}

func (r *runner) DoTask(taskType string, data swf.TaskData) (swf.TaskData, error) {
	ordinal := r.storyCounter
	r.storyCounter++

	chap, err := r.engine.strata.Chapter(context.TODO(), story.Key{AnthologyID: r.engine.tenantId, StoryID: string(r.jobId)}, ordinal)

	if err == nil {
		return chapterToTaskData(chap), nil
	}

	if !errors.Is(err, core.ErrNotFound) {
		return nil, fmt.Errorf("failed to get chapter %d: %w", ordinal, err)
	}

	worker, capabilityExistsLocally := r.worker.TaskWorkers[taskType]

	if !capabilityExistsLocally {
		inputOrdinal := ordinal - 1
		if inputOrdinal < 0 {
			inputOrdinal = 0
		}

		err = r.lease.Reschedule(context.TODO(), r.engine.udb, pgwf.JobDependencies{
			NextNeed: pgwf.Capability(r.worker.JobWorker.Name() + ":" + taskType),
			WaitFor:  nil,
		}, taskWait{
			InputStep:  inputOrdinal,
			OutputStep: ordinal,
			Next:       r.worker.JobWorker.Name(),
		})

		if err != nil {
			return nil, fmt.Errorf("failed to reschedule job: %w", err)
		}

		prematureCloseOut()
		return nil, nil
	}

	output, err := worker.Run(swf.TaskContext{
		JobId: r.GetJobId(),
		Step:  r.storyCounter,
	}, data)

	if err != nil {
		return nil, err
	}
	chap, err = taskDataToChapter(output, ordinal)

	if err != nil {
		return nil, err
	}

	err = r.engine.strata.SaveChapter(context.TODO(), story.Key{
		AnthologyID: r.engine.tenantId,
		StoryID:     string(r.GetJobId()),
	}, chap)

	if err != nil {
		return nil, err
	}

	return output, nil

}

func prematureCloseOut() {
	// do any finalization
	runtime.Goexit()
}

var _ swf.JobContext = &runner{}

type RunError struct {
	Err error
}

func (r *runner) getChapter(ordinal int64) (story.Chapter, error) {
	return r.engine.strata.Chapter(context.TODO(), story.Key{AnthologyID: r.engine.tenantId, StoryID: string(r.jobId)}, ordinal)
}

func (r *runner) Run(ctx context.Context, lease *pgwf.Lease) {
	_ = lease.WithKeepAlive(r.engine.udb)

	chap, err := r.getChapter(0)
	if err != nil {
		fmt.Println(err)
		return
	}
	output, err := r.worker.JobWorker.Run(r, chapterToTaskData(chap))

	if err != nil {
		fmt.Println(err)
		return
	}

	ordinal := r.storyCounter
	r.storyCounter++

	chap, err = taskDataToChapter(output, ordinal)

	if err != nil {
		fmt.Println(err)
		return
	}

	err = r.engine.strata.SaveChapter(context.TODO(), story.Key{
		AnthologyID: r.engine.tenantId,
		StoryID:     string(r.GetJobId()),
	}, chap)

	if err != nil {
		fmt.Println(err)
	}

	err = lease.Complete(ctx, r.engine.udb)
	if err != nil {
		fmt.Println(err)
	}

}
