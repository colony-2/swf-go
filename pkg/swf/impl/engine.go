package impl

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/colony-2/pgwf-go/pkg/pgwf"
	strataclient "github.com/colony-2/strata/strata-go/pkg/client"
	"github.com/colony-2/strata/strata-go/pkg/client/story"
	"github.com/colony-2/swf-go/pkg/swf"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

type swfEngineImpl struct {
	tenantId     string
	strata       *strataclient.Client
	db           *gorm.DB
	udb          *sql.DB
	jobWorkers   map[string]swf.JobWorker
	taskWorkers  map[string]swf.TaskWorker
	capabilities []swf.Capability
	workerId     string
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

func (s *swfEngineImpl) StartJob(ctx context.Context, job swf.StartJob) error {
	key := story.Key{
		AnthologyID: s.tenantId,
		StoryID:     string(job.JobId),
	}
	taskData := swf.TaskData(job.Data)
	co, err := taskDataToCreatOptions(taskData, 0)
	if err != nil {
		return err
	}
	_, err = s.strata.CreateStory(context.TODO(), key, co)

	return s.startJob(job.JobId, job.Dependencies)
}

func (s *swfEngineImpl) startJob(jobId swf.JobId, dependencies swf.Dependencies) error {
	return pgwf.SubmitJob(context.TODO(), s.udb, pgwf.JobID(jobId), pgwf.JobDependencies(dependencies), pgwf.WorkerID(s.workerId))
}

func (s *swfEngineImpl) RestartJob(ctx context.Context, job swf.RestartJob) error {
	sourceJob := story.Key{
		AnthologyID: s.tenantId,
		StoryID:     string(job.PriorJobId),
	}

	targetJob := story.Key{
		AnthologyID: s.tenantId,
		StoryID:     string(job.NewJobId),
	}

	createOptions, err := taskDataToCreatOptions(job.DataForNextTask, job.LastStepToKeep+1)
	if err != nil {
		return err
	}

	cloneOptions := story.CloneOptions{
		DestinationKey: targetJob,
		LastOrdinal:    job.LastStepToKeep,
		CreateOptions:  createOptions,
	}
	_, err = s.strata.CloneStory(context.TODO(), sourceJob, cloneOptions)

	if err != nil {
		return err
	}
	return s.startJob(job.NewJobId, job.Dependencies)
}

func (s *swfEngineImpl) CancelJob(ctx context.Context, job swf.CancelJob) error {
	return pgwf.CancelJob(ctx, s.udb, pgwf.JobID(job.JobId), pgwf.WorkerID(s.workerId), job.Reason)
}

func (s *swfEngineImpl) RegisterJobWorkers(workers ...swf.JobWorker) error {
	// insert each in s.JobWorkers, failing if any already exist
	for _, runner := range workers {
		if _, ok := s.jobWorkers[runner.Name()]; ok {
			return fmt.Errorf("job worker with name %s already registered", runner.Name())
		}
		s.jobWorkers[runner.Name()] = runner
	}
	return nil
}

func (s *swfEngineImpl) RegisterTaskWorkers(workers ...swf.TaskWorker) error {
	// insert each in s.JobWorkers, failing if any already exist
	for _, runner := range workers {
		if _, ok := s.jobWorkers[runner.Name()]; ok {
			return fmt.Errorf("job worker with name %s already registered", runner.Name())
		}
		s.taskWorkers[runner.Name()] = runner
	}
	return nil
}

func (s *swfEngineImpl) FindTasksWaitingForCapability(ctx context.Context, capability swf.Capability) ([]swf.TaskHandle, error) {
	var jobs []Job
	err := s.db.Where(&Job{NextNeed: string(capability), Status: "READY"}).Find(&jobs).Error
	if err != nil {
		return nil, err
	}

	handles := make([]swf.TaskHandle, 0, len(jobs))
	for _, j := range jobs {
		th := taskHandleImpl{
			engine: s,
			job:    j,
		}
		handles = append(handles, th)
	}

	return handles, nil
}

func (s swfEngineImpl) GetTaskData(ctx context.Context, jobId swf.JobId, step int64) (swf.TaskData, error) {
	chapter, err := s.strata.Chapter(context.TODO(), story.Key{AnthologyID: s.tenantId, StoryID: string(jobId)}, step)
	if err != nil {
		return nil, err
	}

	return chapterToTaskData(chapter), nil
}

func NewSWFEngine(tenantId string, workerId string, db *gorm.DB, strataClient *strataclient.Client) (swf.SWFEngine, error) {
	underlying, err := db.DB()
	if err != nil {
		return nil, err
	}

	f := swfEngineImpl{
		tenantId:     tenantId,
		strata:       strataClient,
		db:           db,
		jobWorkers:   make(map[string]swf.JobWorker),
		taskWorkers:  make(map[string]swf.TaskWorker),
		capabilities: make([]swf.Capability, 0),
		workerId:     workerId,
		udb:          underlying,
	}

	return &f, nil
}

var _ swf.SWFEngine = &swfEngineImpl{}
