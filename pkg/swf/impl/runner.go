package impl

import (
	"context"
	"errors"
	"fmt"
	"runtime"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/colony-2/pgwf-go/pkg/pgwf"
	"github.com/colony-2/strata/strata-go/pkg/client/core"
	"github.com/colony-2/strata/strata-go/pkg/client/story"
	"github.com/colony-2/swf-go/pkg/swf"
	"github.com/segmentio/ksuid"
)

type runner struct {
	incoming     chan inmsg
	outgoing     chan outmsg
	jobWorker    swf.JobWorker
	jobStart     swf.StartJob
	story        story.Story
	taskCounter  int64
	capabilities map[string]swf.TaskWorker
	engine       *swfEngineImpl
}

func (r *runner) GetJobId() swf.JobId {
	return r.jobStart.JobId
}

func (r *runner) DoTask(retryPolicy swf.RetryPolicy, taskType string, data swf.TaskData) (swf.TaskData, error) {
	r.taskCounter++
	chap, err := r.story.Chapter(context.TODO(), r.taskCounter)

	if err == nil {
		return chapterToTaskData(chap), nil
	}

	if !errors.Is(core.ErrNotFound, err) {
		return nil, fmt.Errorf("failed to get chapter %d: %w", r.taskCounter, err)
	}

	if worker, ok := r.capabilities[taskType]; !ok {
		// two new jobs: job to do the task, job to inform us that first job is done.
		taskJobId := pgwf.JobID(ksuid.New().String())

		tx, err := r.engine.udb.Begin()
		if err != nil {
			return nil, err
		}
		pgwf.SubmitJob(context.TODO(), tx, taskJobId)

	}
	//

	// if chapter exists, get it.
	// if not, determine if the task can be run locally. if not pause until chapter is available.
	// run task locally.
	// if can't run locally, generate a new job id and save it to the journal
	// start a new pgwf job with the generated id. that job is an input job
	//

	// task is a story entry
	//
	//create a new pgwf sub-job and wait for it to complete. we will create the remote job as a child of this job. the remote job will write against this story. we will start the new job
	// the subjob's id must be a determinsitic id so that if the parent dies just after starting it, we realized that we already created it.

}

type SingleTask struct {
	story   string
	chapter int64
	notify  string
}

var _ swf.JobContext = &runner{}

func (r *runner) Run() {
	r.jobWorker.Run()
}

type inmsg interface {
	inmsg()
}

type outmsg interface {
}

func (r *runner) nextMessage() inmsg {
	msgI := <-r.incoming
	switch msg := msgI.(type) {
	case exitRoutine:

		runtime.Goexit()
		return nil
	default:
		panic(fmt.Sprintf("unknown message type %s", msg))
	}
}

type taskComplete struct {
	data swf.TaskData
}

func (e taskComplete) inmsg() {}

type exitRoutine struct {
}

func (e exitRoutine) inmsg() {}

type notify struct {
	JobId swf.JobId
}

func (e notify) inmsg() {}

func (r *runner) Kill() {
	r.incoming <- exitRoutine{}
}

func (r *runner) incomingNotification(JobId swf.JobId) {
	// look i we care about notification. if so, act on it.
	r.incoming <- notify{JobId}
}

func (s *swfEngineImpl) listen(ctx context.Context) <-chan *pgwf.Lease {
	capabilities := make([]pgwf.Capability, 0, len(s.jobWorkers)+len(s.taskWorkers)+1)
	for _, w := range s.jobWorkers {
		capabilities = append(capabilities, pgwf.Capability(w.Name()))
	}
	for _, w := range s.taskWorkers {
		capabilities = append(capabilities, pgwf.Capability(w.Name()))
	}
	capabilities = append(capabilities, pgwf.Capability(NOTIFY_PREFIX+s.workerId))

	ch := make(chan *pgwf.Lease)
	go func() {
		defer close(ch)
		b := backoff.NewExponentialBackOff()
		b.InitialInterval = 0
		b.MaxInterval = time.Second * 30
		for {
			item, err := pgwf.GetWork(ctx, s.udb, pgwf.WorkerID(s.workerId), capabilities)
			if err == nil {
				b.Reset()
				select {
				case ch <- item:
				case <-ctx.Done():
					return
				}
			}

			select {
			case <-time.After(b.NextBackOff()):
			case <-ctx.Done():
				return
			}
		}
	}()
	return ch
}
