package impl

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/colony-2/pgwf-go/pkg/pgwf"
	strataclient "github.com/colony-2/strata/strata-go/pkg/client"
	"github.com/colony-2/strata/strata-go/pkg/client/story"
	"github.com/colony-2/swf-go/pkg/swf"
	"github.com/google/uuid"
	"github.com/segmentio/ksuid"
	"gorm.io/gorm"
)

type swfEngineImpl struct {
	tenantId        string
	strata          *strataclient.Client
	db              *gorm.DB
	udb             *sql.DB
	workers         map[pgwf.Capability]*swf.WorkSet
	workerId        string
	runners         map[string]runner
	activeWorkLimit int
}

func taskDataToChapter(jobData swf.TaskData, ordinal int64) (story.Chapter, error) {

	var chapBuilder *story.ChapterBuilder

	if jobData != nil {
		data, err := jobData.GetData()
		if err != nil {
			return nil, err
		}
		bytes, err := data.ToBytes()
		if err != nil {
			return nil, err
		}

		chapBuilder = story.NewChapter().WithOrdinal(ordinal).WithBytes(bytes)
		artifacts, err := jobData.GetArtifacts()
		if err != nil {
			return nil, err
		}
		// loop over artifacts and add them to the story
		for _, v := range artifacts {
			chapBuilder.AddArtifact(v)
		}

	}

	return chapBuilder, nil
}

func taskDataToCreatOptions(jobData swf.TaskData, ordinal int64) (story.CreateOptions, error) {
	chap, err := taskDataToChapter(jobData, ordinal)
	if err != nil {
		return story.CreateOptions{}, err
	}

	co := story.CreateOptions{
		RequestID:      uuid.New().String(),
		InitialChapter: chap,
	}
	return co, nil
}

func (s *swfEngineImpl) StartJob(ctx context.Context, job swf.StartJob) (swf.JobId, error) {
	jobId := swf.JobId(ksuid.New().String())
	key := story.Key{
		AnthologyID: s.tenantId,
		StoryID:     string(jobId),
	}
	taskData := swf.TaskData(job.Data)
	co, err := taskDataToCreatOptions(taskData, 0)
	if err != nil {
		return "", err
	}
	_, err = s.strata.CreateStory(context.TODO(), key, co)
	if err != nil {
		return "", err
	}

	return jobId, s.startJob(jobId, job.JobType, job.SingletonKey)
}

func (s *swfEngineImpl) startJob(jobId swf.JobId, jobType string, singletonKey string) error {
	dep := pgwf.JobDependencies{
		NextNeed: pgwf.Capability(jobType),
	}
	return pgwf.SubmitJob(context.TODO(), s.udb, pgwf.JobID(jobId), dep, nil, pgwf.WorkerID(s.workerId), singletonKey, time.Time{})
}

func (s *swfEngineImpl) RestartJob(ctx context.Context, job swf.RestartJob) (swf.JobId, error) {
	jobId := swf.JobId(ksuid.New().String())
	sourceJob := story.Key{
		AnthologyID: s.tenantId,
		StoryID:     string(job.PriorJobId),
	}

	targetJob := story.Key{
		AnthologyID: s.tenantId,
		StoryID:     string(jobId),
	}

	createOptions, err := taskDataToCreatOptions(job.Data, job.LastStepToKeep+1)
	if err != nil {
		return "", err
	}

	cloneOptions := story.CloneOptions{
		DestinationKey: targetJob,
		LastOrdinal:    job.LastStepToKeep,
		CreateOptions:  createOptions,
	}
	_, err = s.strata.CloneStory(context.TODO(), sourceJob, cloneOptions)

	if err != nil {
		return "", err
	}
	return jobId, s.startJob(jobId, job.JobType, job.SingletonKey)
}

func (s *swfEngineImpl) CancelJob(ctx context.Context, job swf.CancelJob) error {
	return pgwf.CancelJob(ctx, s.udb, pgwf.JobID(job.JobId), pgwf.WorkerID(s.workerId), job.Reason)
}

type taskWait struct {
	InputStep  int64  `json:"in"`
	OutputStep int64  `json:"out"`
	Next       string `json:"nex	t"`
}

func (s *swfEngineImpl) FindTasksWaitingForCapability(ctx context.Context, jobType string, taskType string) ([]swf.TaskHandle, error) {
	var jobs []Job
	err := s.db.Where(&Job{NextNeed: jobType + ":" + taskType, Status: "READY"}).Find(&jobs).Error
	if err != nil {
		return nil, err
	}

	handles := make([]swf.TaskHandle, 0, len(jobs))
	for _, j := range jobs {
		tw := taskWait{}
		err = json.Unmarshal(j.Payload, &tw)
		if err != nil {
			return nil, err
		}
		th := taskHandleImpl{
			job:           j,
			inputOrdinal:  tw.InputStep,
			outputOrdinal: tw.OutputStep,
			engine:        s,
			nextNeed:      pgwf.Capability(tw.Next),
		}
		handles = append(handles, &th)
	}

	return handles, nil
}

var _ swf.SWFEngine = &swfEngineImpl{}

var Builder swf.Builder = func(tenantId string, db *gorm.DB, strataClient *strataclient.Client, workers []swf.WorkSet) (swf.SWFEngine, error) {
	underlying, err := db.DB()
	if err != nil {
		return nil, err
	}
	host, err := os.Hostname()
	if err != nil {
		return nil, err
	}

	// create a map of capabilities to workers (each task maps to the workset of the parent job. this way we avoid string splitting on each job to find things.
	capMap := make(map[pgwf.Capability]*swf.WorkSet)
	for i := range workers {
		w := &workers[i]
		for _, c := range w.Capabilities {
			capMap[c] = w
		}
		capMap[pgwf.Capability(w.JobWorker.Name())] = w
	}

	workerId := fmt.Sprintf("%s:%d-%s", host, os.Getppid(), ksuid.New().String())
	f := swfEngineImpl{
		tenantId: tenantId,
		strata:   strataClient,
		db:       db,
		workers:  capMap,
		workerId: workerId,
		udb:      underlying,
	}

	return &f, nil
}

func (s *swfEngineImpl) Run(ctx context.Context) {
	caps := make([]pgwf.Capability, 0, len(s.workers))
	for k, v := range s.workers {
		caps = append(caps, v.Capabilities...)
		caps = append(caps, pgwf.Capability(k))
	}
	go func() {
		b := backoff.NewExponentialBackOff()
		b.MaxInterval = time.Second * 30
		for {
			lease, err := pgwf.GetWork(ctx, s.udb, pgwf.WorkerID(s.workerId), caps)
			if err == nil {
				if lease != nil {
					b.Reset()
					go s.runSomething(context.Background(), lease)
					continue // let's try again without a backoff.
				}
				// no work right now; fall through to backoff
			}
			select {
			case <-ctx.Done():
				return
			case <-time.After(b.NextBackOff()):
			}
		}
	}()
}

// runs inside goroutine for a specific lease.
func (s *swfEngineImpl) runSomething(ctx context.Context, lease *swf.Lease) {
	capability := lease.NextNeed()
	workSet, ok := s.workers[capability]
	if !ok {
		// this should never happen. we don't want to crash so we'll just let the lease expire
		log.Printf("no workset found for capability %s. this shouldn't happen", capability)
		return
	}

	runner := runner{
		jobId:        lease.JobID(),
		worker:       workSet,
		storyCounter: 1,
		engine:       s,
		lease:        lease,
	}
	runner.Run(ctx, lease)
}

var _ swf.SWFEngine = &swfEngineImpl{}
