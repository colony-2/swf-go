package toy

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/colony-2/swf-go/pkg/swf"
	"github.com/segmentio/ksuid"
)

// JobIDGenerator allows overriding how job IDs are created.
type JobIDGenerator func() (swf.JobId, error)

// Option configures the ToyEngine.
type Option func(*ToyEngine)

// WithLogger sets a logger for ToyEngine.
func WithLogger(logger *slog.Logger) Option {
	return func(e *ToyEngine) {
		if logger != nil {
			e.logger = logger
		}
	}
}

// WithJobIDGenerator overrides the default job ID generator.
func WithJobIDGenerator(gen JobIDGenerator) Option {
	return func(e *ToyEngine) {
		if gen != nil {
			e.idGenerator = gen
		}
	}
}

// ToyEngine is a synchronous, in-memory SWFEngine implementation.
// It executes jobs immediately on the caller goroutine with no persistence.
type ToyEngine struct {
	mu          sync.Mutex
	workers     map[string]swf.WorkSet
	jobRecords  map[swf.JobId]*jobRecord
	idGenerator JobIDGenerator
	logger      *slog.Logger
}

type jobRecord struct {
	mu        sync.Mutex
	status    swf.JobStatus
	result    swf.TaskData
	err       error
	cancelled bool
	cancel    context.CancelFunc
	started   time.Time
	finished  time.Time
	jobType   string
}

// NewToyEngine constructs a ToyEngine with the provided worksets.
func NewToyEngine(workers []swf.WorkSet, opts ...Option) *ToyEngine {
	engine := &ToyEngine{
		workers:     make(map[string]swf.WorkSet),
		jobRecords:  make(map[swf.JobId]*jobRecord),
		idGenerator: func() (swf.JobId, error) { return swf.JobId(ksuid.New().String()), nil },
		logger:      slog.Default(),
	}
	for _, ws := range workers {
		engine.workers[ws.JobWorker.Name()] = ws
	}
	for _, opt := range opts {
		opt(engine)
	}
	return engine
}

// RegisterWorkers adds a new workset. Duplicate job names error.
func (e *ToyEngine) RegisterWorkers(ws *swf.WorkSet) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if _, ok := e.workers[ws.JobWorker.Name()]; ok {
		return fmt.Errorf("worker %s already registered", ws.JobWorker.Name())
	}
	e.workers[ws.JobWorker.Name()] = *ws
	return nil
}

// Run is unsupported for ToyEngine.
func (e *ToyEngine) Run(context.Context) {
	if e.logger != nil {
		e.logger.Error("Run called on ToyEngine; unsupported")
	}
}

// StartJob executes the job synchronously on the caller goroutine.
func (e *ToyEngine) StartJob(ctx context.Context, start swf.StartJob) (swf.JobId, error) {
	ws, ok := e.getWorkSet(start.JobType)
	if !ok {
		return "", fmt.Errorf("job worker %s not registered", start.JobType)
	}
	jobID, err := e.idGenerator()
	if err != nil {
		return "", err
	}
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return "", err
		}
	}
	runCtx, cancel := context.WithCancel(context.Background())
	record := &jobRecord{
		status:  swf.JobStatusReady,
		cancel:  cancel,
		jobType: start.JobType,
	}
	e.setJobRecord(jobID, record)
	e.runJob(runCtx, jobID, ws, swf.JobData(start.Data))
	return jobID, nil
}

// RestartJob executes like StartJob but with the provided restart data.
func (e *ToyEngine) RestartJob(ctx context.Context, restart swf.RestartJob) (swf.JobId, error) {
	return e.StartJob(ctx, swf.StartJob{
		JobType:      restart.JobType,
		SingletonKey: restart.SingletonKey,
		Data:         restart.Data,
		RunPolicy:    restart.RunPolicy,
	})
}

// CancelJob attempts to cancel an active job.
func (e *ToyEngine) CancelJob(ctx context.Context, cancel swf.CancelJob) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	record, ok := e.jobRecords[cancel.JobId]
	if !ok {
		return fmt.Errorf("job %s not found", cancel.JobId)
	}
	record.mu.Lock()
	defer record.mu.Unlock()
	if record.status != swf.JobStatusCompleted && record.status != swf.JobStatusCancelled {
		record.cancelled = true
		if record.cancel != nil {
			record.cancel()
		}
		record.status = swf.JobStatusCancelled
		if record.err == nil {
			record.err = context.Canceled
		}
	}
	return nil
}

// CheckJobStatus returns the current status of the job.
func (e *ToyEngine) CheckJobStatus(ctx context.Context, jobId swf.JobId) (swf.JobStatus, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	record, ok := e.jobRecords[jobId]
	if !ok {
		return "", fmt.Errorf("job %s not found", jobId)
	}
	record.mu.Lock()
	defer record.mu.Unlock()
	return record.status, nil
}

// GetJobResult returns the final result or error for a completed job.
func (e *ToyEngine) GetJobResult(ctx context.Context, jobId swf.JobId) (swf.TaskData, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	record, ok := e.jobRecords[jobId]
	if !ok {
		return nil, fmt.Errorf("job %s not found", jobId)
	}
	record.mu.Lock()
	defer record.mu.Unlock()
	if record.status != swf.JobStatusCompleted {
		return nil, fmt.Errorf("job %s not completed", jobId)
	}
	return record.result, record.err
}

// FindTasksWaitingForCapability returns no pending tasks in v1.
func (e *ToyEngine) FindTasksWaitingForCapability(ctx context.Context, jobType string, taskType string) ([]swf.TaskHandle, error) {
	return []swf.TaskHandle{}, nil
}

func (e *ToyEngine) getWorkSet(jobType string) (swf.WorkSet, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	ws, ok := e.workers[jobType]
	return ws, ok
}

func (e *ToyEngine) setJobRecord(id swf.JobId, record *jobRecord) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.jobRecords[id] = record
}

func (e *ToyEngine) runJob(ctx context.Context, jobID swf.JobId, ws swf.WorkSet, data swf.JobData) {
	record := e.getJobRecord(jobID)
	if record == nil {
		return
	}
	record.mu.Lock()
	record.started = time.Now()
	record.status = swf.JobStatusActive
	record.mu.Unlock()

	var (
		result swf.JobData
		err    error
	)

	defer func() {
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v", r)
		}
		record.mu.Lock()
		defer record.mu.Unlock()
		if record.cancelled {
			record.status = swf.JobStatusCancelled
			if record.err == nil {
				record.err = ctx.Err()
			}
			record.finished = time.Now()
			return
		}
		record.status = swf.JobStatusCompleted
		record.result = result
		record.err = err
		record.finished = time.Now()
	}()

	jc := &toyJobContext{
		engine:   e,
		jobID:    jobID,
		logger:   e.logger,
		workSet:  ws,
		step:     1,
		cancelCh: ctx.Done(),
		record:   record,
	}
	result, err = ws.JobWorker.Run(jc, data)
}

func (e *ToyEngine) getJobRecord(id swf.JobId) *jobRecord {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.jobRecords[id]
}

type toyJobContext struct {
	engine   *ToyEngine
	jobID    swf.JobId
	logger   *slog.Logger
	workSet  swf.WorkSet
	step     int64
	cancelCh <-chan struct{}
	record   *jobRecord
}

func (c *toyJobContext) GetJobId() swf.JobId {
	return c.jobID
}

func (c *toyJobContext) Logger() *slog.Logger {
	if c.logger != nil {
		return c.logger
	}
	return slog.Default()
}

func (c *toyJobContext) DoTask(_ swf.RunPolicy, taskType string, data swf.TaskData) (swf.TaskData, error) {
	select {
	case <-c.cancelCh:
		c.markCancelled()
		return nil, context.Canceled
	default:
	}

	taskWorker, ok := c.workSet.TaskWorkers[taskType]
	if !ok {
		return nil, fmt.Errorf("task worker %s not registered for job %s", taskType, c.workSet.JobWorker.Name())
	}

	await := func(wakeAt time.Time) error {
		sleep := time.Until(wakeAt)
		if sleep <= 0 {
			return nil
		}
		timer := time.NewTimer(sleep)
		defer timer.Stop()
		select {
		case <-timer.C:
			return nil
		case <-c.cancelCh:
			c.markCancelled()
			return context.Canceled
		}
	}

	tc := swf.NewTaskContext(c.jobID, c.step, c.Logger(), await, nil)
	output, err := taskWorker.Run(tc, data)
	if err != nil {
		return nil, err
	}
	c.step++
	return output, nil
}

func (c *toyJobContext) AwaitDuration(waitFor swf.Duration) error {
	d := waitFor.ToDuration()
	if d <= 0 {
		return nil
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-c.cancelCh:
		c.markCancelled()
		return context.Canceled
	}
}

func (c *toyJobContext) SpawnAsync(jobType string, data swf.TaskData) (*swf.Future, error) {
	return nil, fmt.Errorf("SpawnAsync not supported in ToyEngine")
}

func (c *toyJobContext) markCancelled() {
	c.record.mu.Lock()
	defer c.record.mu.Unlock()
	c.record.cancelled = true
	if c.record.status != swf.JobStatusCancelled {
		c.record.status = swf.JobStatusCancelled
	}
	if c.record.err == nil {
		c.record.err = context.Canceled
	}
}
