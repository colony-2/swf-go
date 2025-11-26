package swf

import (
	"context"
	"fmt"
	"log"
	"regexp"

	"github.com/colony-2/pgwf-go/pkg/pgwf"
	strataclient "github.com/colony-2/strata/strata-go/pkg/client"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

type StartJob struct {
	JobType      string
	SingletonKey string
	Data         JobData
}

type RestartJob struct {
	PriorJobId     JobId
	LastStepToKeep int64
	StartJob
}

type CancelJob struct {
	JobId  JobId
	Reason string
}

type JobStatus struct {
	JobId JobId
	Step  int64
}

type JobData TaskData

type JobContext interface {
	//jobRunApi
	GetJobId() JobId
	//RunChildJobSync(ctx context.Context, childJob StartJob) (JobId, error)
	DoTask(taskType string, data TaskData) (TaskData, error)
}

type JobWorker interface {
	Name() string
	Run(JobContext, JobData) (JobData, error)
}

type jobRunApi interface {
	StartJob(ctx context.Context, start StartJob) (JobId, error)
	RestartJob(ctx context.Context, restart RestartJob) (JobId, error)
	CancelJob(ctx context.Context, cancel CancelJob) error
}

type EngineBuilder struct {
	workers     map[string]WorkSet
	tenantId    string
	maxActive   int
	strataURI   string
	postgresDSN string
}

type WorkSet struct {
	JobWorker    JobWorker
	TaskWorkers  map[string]TaskWorker
	Capabilities []pgwf.Capability
}

func NewEngineBuilder(tenantId string) *EngineBuilder {
	return &EngineBuilder{
		workers:   make(map[string]WorkSet),
		tenantId:  tenantId,
		maxActive: 4,
	}
}

func (e *EngineBuilder) WithPostgresDSN(dsn string) *EngineBuilder {
	e.postgresDSN = dsn
	return e
}

func (e *EngineBuilder) WithMaxActive(maxActive int) *EngineBuilder {
	e.maxActive = maxActive
	return e
}

func (e *EngineBuilder) WithStrata(uri string) *EngineBuilder {
	e.strataURI = uri
	return e
}

func (e *EngineBuilder) PlusWorkers(jobWorker JobWorker, taskWorkers ...TaskWorker) *EngineBuilder {
	namePattern := regexp.MustCompile(`^[A-Za-z0-9_-]+$`)
	if !namePattern.MatchString(jobWorker.Name()) {
		panic(fmt.Sprintf("invalid job worker name %s", jobWorker.Name()))
	}
	if _, ok := e.workers[jobWorker.Name()]; ok {
		panic("job worker with name " + jobWorker.Name() + " already registered")
	}

	tasks := make(map[string]TaskWorker)
	capabilities := make([]pgwf.Capability, len(taskWorkers))
	for _, tw := range taskWorkers {

		if _, ok := tasks[tw.Name()]; ok {
			if !namePattern.MatchString(tw.Name()) {
				panic(fmt.Sprintf("invalid task worker name %s", tw.Name()))
			}
			panic("task worker with name " + tw.Name() + " already registered")
		}
		tasks[tw.Name()] = tw
		capabilities = append(capabilities, pgwf.Capability(jobWorker.Name()+":"+tw.Name()))
	}

	workerSet := WorkSet{
		JobWorker:    jobWorker,
		TaskWorkers:  tasks,
		Capabilities: capabilities,
	}
	e.workers[jobWorker.Name()] = workerSet
	return e
}

func (b *EngineBuilder) Build(builder Builder) (SWFEngine, error) {
	if b.strataURI == "" {
		return nil, fmt.Errorf("strata URI is required")
	}

	if b.postgresDSN == "" {
		return nil, fmt.Errorf("postgres DSN is required")
	}

	if len(b.workers) == 0 {
		return nil, fmt.Errorf("at least one job worker must be registered")
	}

	if b.tenantId == "" {
		return nil, fmt.Errorf("tenant ID is required")
	}

	log.Printf("Building engine with workers: %+v", b.workers)
	sclient, err := strataclient.New(strataclient.Config{
		BaseURL: b.strataURI,
	})
	if err != nil {
		return nil, fmt.Errorf("failed to create strata client: %w", err)
	}

	db, err := gorm.Open(postgres.Open(b.postgresDSN), &gorm.Config{})
	if err != nil {
		return nil, fmt.Errorf("failed to connect to postgres: %w", err)
	}

	ws := make([]WorkSet, len(b.workers))
	for _, v := range b.workers {
		ws = append(ws, v)
	}
	return builder(b.tenantId, db, sclient, ws)
}

type Builder func(tenantId string, db *gorm.DB, strataClient *strataclient.Client, workers []WorkSet) (SWFEngine, error)
