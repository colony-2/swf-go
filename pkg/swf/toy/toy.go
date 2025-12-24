package toy

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sort"
	"sync"
	"time"

	"github.com/colony-2/swf-go/pkg/swf"
	"github.com/segmentio/ksuid"
)

// JobIDGenerator allows overriding how job IDs are created.
type JobIDGenerator func(tenantId string) (swf.JobKey, error)

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
	jobRecords  map[swf.JobKey]*jobRecord
	pending     map[string][]*pendingTask
	idGenerator JobIDGenerator
	logger      *slog.Logger
}

type jobRecord struct {
	mu         sync.Mutex
	status     swf.JobStatus
	result     swf.TaskData
	err        error
	cancelled  bool
	cancel     context.CancelFunc
	started    time.Time
	finished   time.Time
	jobType    string
	singleton  *string
	createdAt  time.Time
	archived   *time.Time
	payload    []byte
	capability string
	step       int64
}

type pendingTask struct {
	jobKey     swf.JobKey
	data       swf.TaskData
	capability string
	step       int64
	done       chan pendingResult
}

type pendingResult struct {
	data swf.TaskData
	err  error
}

// NewToyEngine constructs a ToyEngine with the provided worksets.
func NewToyEngine(workers []swf.WorkSet, opts ...Option) *ToyEngine {
	engine := &ToyEngine{
		workers:    make(map[string]swf.WorkSet),
		jobRecords: make(map[swf.JobKey]*jobRecord),
		pending:    make(map[string][]*pendingTask),
		idGenerator: func(tenantId string) (swf.JobKey, error) {
			return swf.JobKey{TenantId: tenantId, JobId: ksuid.New().String()}, nil
		},
		logger: slog.Default(),
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
func (e *ToyEngine) StartJob(ctx context.Context, start swf.StartJob) (swf.JobKey, error) {
	ws, ok := e.getWorkSet(start.JobType)
	if !ok {
		return swf.JobKey{}, fmt.Errorf("job worker %s not registered", start.JobType)
	}
	// Use provided JobID if present, otherwise generate a new one
	var jobKey swf.JobKey
	if start.JobID != "" {
		jobKey = swf.JobKey{TenantId: start.TenantId, JobId: start.JobID}
	} else {
		var err error
		jobKey, err = e.idGenerator(start.TenantId)
		if err != nil {
			return swf.JobKey{}, err
		}
	}
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return swf.JobKey{}, err
		}
	}
	runCtx, cancel := context.WithCancel(context.Background())
	payloadBytes, _ := start.Data.GetData()
	var singletonPtr *string
	if start.SingletonKey != "" {
		singletonPtr = &start.SingletonKey
	}
	record := &jobRecord{
		status:    swf.JobStatusReady,
		cancel:    cancel,
		jobType:   start.JobType,
		singleton: singletonPtr,
		createdAt: time.Now(),
		payload:   payloadBytes,
	}
	e.setJobRecord(jobKey, record)
	e.runJob(runCtx, jobKey, ws, swf.JobData(start.Data))
	return jobKey, nil
}

// RestartJob executes like StartJob but with the provided restart data.
func (e *ToyEngine) RestartJob(ctx context.Context, restart swf.RestartJob) (swf.JobKey, error) {
	return e.StartJob(ctx, swf.StartJob{
		TenantId:     restart.TenantId,
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
	record, ok := e.jobRecords[cancel.JobKey]
	if !ok {
		return fmt.Errorf("job %s not found", cancel.JobKey)
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
func (e *ToyEngine) CheckJobStatus(ctx context.Context, jobKey swf.JobKey) (swf.JobStatus, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	record, ok := e.jobRecords[jobKey]
	if !ok {
		return "", fmt.Errorf("job %s not found", jobKey)
	}
	record.mu.Lock()
	defer record.mu.Unlock()
	return record.status, nil
}

// GetJobResult returns the final result or error for a completed job.
func (e *ToyEngine) GetJobResult(ctx context.Context, jobKey swf.JobKey) (swf.TaskData, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	record, ok := e.jobRecords[jobKey]
	if !ok {
		return nil, fmt.Errorf("job %s not found", jobKey)
	}
	record.mu.Lock()
	defer record.mu.Unlock()
	if record.status != swf.JobStatusCompleted {
		return nil, fmt.Errorf("job %s not completed", jobKey)
	}
	return record.result, record.err
}

// FindTasksWaitingForCapability returns pending task handles for a capability.
func (e *ToyEngine) FindTasksWaitingForCapability(ctx context.Context, jobType string, taskType string) ([]swf.TaskHandle, error) {
	capability := jobType + ":" + taskType
	e.mu.Lock()
	defer e.mu.Unlock()
	pending := e.pending[capability]
	handles := make([]swf.TaskHandle, 0, len(pending))
	for _, p := range pending {
		handles = append(handles, &pendingHandle{engine: e, task: p})
	}
	return handles, nil
}

// GetWaitingTask returns a pending handle for the given job key if present.
func (e *ToyEngine) GetWaitingTask(ctx context.Context, key swf.JobKey) (swf.TaskHandle, error) {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, queue := range e.pending {
		for _, p := range queue {
			if p.jobKey == key {
				return &pendingHandle{engine: e, task: p}, nil
			}
		}
	}
	return nil, swf.ErrJobNotFound
}

func containsStore(stores []swf.JobStore, store swf.JobStore) bool {
	for _, s := range stores {
		if s == store {
			return true
		}
	}
	return false
}

func containsString(values []string, target string) bool {
	for _, v := range values {
		if v == target {
			return true
		}
	}
	return false
}

// pendingHandle exposes a pending task so callers can complete it.
type pendingHandle struct {
	engine *ToyEngine
	task   *pendingTask
}

func (h *pendingHandle) JobKey() swf.JobKey {
	return h.task.jobKey
}

func (h *pendingHandle) Data() (swf.TaskData, error) {
	return h.task.data, nil
}

func (h *pendingHandle) TaskOrdinalToComplete() int64 {
	return h.task.step
}

func (h *pendingHandle) Finish(ctx context.Context, taskData swf.TaskData) error {
	if ctx != nil {
		if err := ctx.Err(); err != nil {
			return err
		}
	}
	h.engine.mu.Lock()
	queue := h.engine.pending[h.task.capability]
	idx := -1
	for i, p := range queue {
		if p == h.task {
			idx = i
			break
		}
	}
	if idx == -1 {
		h.engine.mu.Unlock()
		return fmt.Errorf("pending task already finished")
	}
	// remove from queue
	h.engine.pending[h.task.capability] = append(queue[:idx], queue[idx+1:]...)
	h.engine.mu.Unlock()

	select {
	case h.task.done <- pendingResult{data: taskData}:
		return nil
	default:
		return fmt.Errorf("pending task already resolved")
	}
}

// ListJobs returns in-memory job summaries ordered by created_at desc then job_id desc.
func (e *ToyEngine) ListJobs(ctx context.Context, req swf.ListJobsRequest) (swf.ListJobsResponse, error) {
	pageSize := req.PageSize
	if pageSize <= 0 {
		pageSize = swf.DefaultListJobsPageSize
	} else if pageSize > swf.MaxListJobsPageSize {
		pageSize = swf.MaxListJobsPageSize
	}

	var (
		activeStatuses []swf.JobStatus
		includeActive  bool
		includeArchive bool
	)
	if len(req.Statuses) > 0 {
		for _, st := range req.Statuses {
			switch st {
			case swf.JobStatusCompleted:
				includeArchive = true
			case swf.JobStatusReady, swf.JobStatusExpired, swf.JobStatusPendingJobs, swf.JobStatusAwaitingFuture, swf.JobStatusActive, swf.JobStatusCrashConcern, swf.JobStatusCancelled:
				includeActive = true
				activeStatuses = append(activeStatuses, st)
			default:
				return swf.ListJobsResponse{}, fmt.Errorf("unknown status %q", st)
			}
		}
	} else {
		if len(req.Stores) == 0 {
			includeActive, includeArchive = true, true
		} else {
			for _, store := range req.Stores {
				switch store {
				case swf.JobStoreActive:
					includeActive = true
				case swf.JobStoreArchived:
					includeArchive = true
				default:
					return swf.ListJobsResponse{}, fmt.Errorf("unknown store %q", store)
				}
			}
		}
	}

	if !includeActive && !includeArchive {
		return swf.ListJobsResponse{}, nil
	}

	var (
		cursorTime time.Time
		cursorJob  string
		hasCursor  bool
	)
	if req.PageToken != "" {
		createdAt, jobKey, err := swf.DecodeListJobsPageToken(req.PageToken)
		if err != nil {
			return swf.ListJobsResponse{}, err
		}
		cursorTime = createdAt
		cursorJob = jobKey.String()
		hasCursor = true
	}

	jobTypeAllowed := func(jt string) bool {
		if len(req.JobTypes) == 0 {
			return true
		}
		for _, expect := range req.JobTypes {
			if jt == expect {
				return true
			}
		}
		return false
	}

	jobKeyAllowed := func(key swf.JobKey) bool {
		if len(req.JobKeys) == 0 {
			return true
		}
		for _, expect := range req.JobKeys {
			if expect == key {
				return true
			}
		}
		return false
	}

	jobTaskAllowed := func(jobKey swf.JobKey, rec *jobRecord) bool {
		if len(req.JobTasks) == 0 {
			return true
		}
		// Build capability set for this job from pending queues.
		caps := make(map[string]struct{})
		for capName, queue := range e.pending {
			for _, p := range queue {
				if p.jobKey == jobKey {
					caps[capName] = struct{}{}
				}
			}
		}
		// If no current pending capabilities, fall back to the last seen capability on the record (e.g., after completion).
		if len(caps) == 0 {
			if rec.capability != "" {
				caps[rec.capability] = struct{}{}
			} else {
				return false
			}
		}
		for _, pair := range req.JobTasks {
			if pair.JobType == "" || pair.TaskType == "" {
				continue
			}
			if _, ok := caps[pair.JobType+":"+pair.TaskType]; ok {
				return true
			}
		}
		return false
	}

	statusAllowed := func(st swf.JobStatus) bool {
		if len(req.Statuses) == 0 {
			return true
		}
		for _, expect := range req.Statuses {
			if st == expect {
				return true
			}
		}
		return false
	}

	records := make([]swf.JobSummary, 0)
	e.mu.Lock()
	for key, rec := range e.jobRecords {
		rec.mu.Lock()
		status := rec.status
		store := swf.JobStoreActive
		if status == swf.JobStatusCompleted {
			store = swf.JobStoreArchived
		}

		if status == swf.JobStatusCompleted && !includeArchive {
			rec.mu.Unlock()
			continue
		}
		if status != swf.JobStatusCompleted && !includeActive {
			rec.mu.Unlock()
			continue
		}
		if len(req.Stores) > 0 && !containsStore(req.Stores, store) {
			rec.mu.Unlock()
			continue
		}
		if !statusAllowed(status) {
			rec.mu.Unlock()
			continue
		}
		if !jobKeyAllowed(key) {
			rec.mu.Unlock()
			continue
		}
		if rec.singleton != nil && len(req.SingletonKeys) > 0 && !containsString(req.SingletonKeys, *rec.singleton) {
			rec.mu.Unlock()
			continue
		}
		if len(req.SingletonKeys) > 0 && rec.singleton == nil {
			rec.mu.Unlock()
			continue
		}
		if !jobTypeAllowed(rec.jobType) {
			rec.mu.Unlock()
			continue
		}
		if !jobTaskAllowed(key, rec) {
			rec.mu.Unlock()
			continue
		}
		if req.CreatedAfter != nil && rec.createdAt.Before(*req.CreatedAfter) {
			rec.mu.Unlock()
			continue
		}
		if req.CreatedBefore != nil && rec.createdAt.After(*req.CreatedBefore) {
			rec.mu.Unlock()
			continue
		}

		payloadCopy := json.RawMessage(nil)
		if len(rec.payload) > 0 {
			payloadCopy = make([]byte, len(rec.payload))
			copy(payloadCopy, rec.payload)
		}
		summary := swf.JobSummary{
			JobKey:          key,
			Status:          status,
			JobType:         rec.jobType,
			SingletonKey:    rec.singleton,
			WaitFor:         []swf.JobKey{},
			AvailableAt:     rec.createdAt,
			ExpiresAt:       nil,
			LeaseExpiresAt:  nil,
			CancelRequested: rec.cancelled,
			CreatedAt:       rec.createdAt,
			ArchivedAt:      rec.archived,
			Payload:         payloadCopy,
			TaskWaitInput:   nil,
			TaskWaitOutput:  nil,
			TaskWaitNext:    nil,
		}
		if rec.capability != "" {
			summary.TaskWaitNext = &rec.capability
			step := rec.step
			if step > 0 {
				input := step - 1
				summary.TaskWaitInput = &input
				summary.TaskWaitOutput = &step
			}
		}
		rec.mu.Unlock()
		records = append(records, summary)
	}
	e.mu.Unlock()

	sort.Slice(records, func(i, j int) bool {
		if records[i].CreatedAt.Equal(records[j].CreatedAt) {
			return records[i].JobKey.String() > records[j].JobKey.String()
		}
		return records[i].CreatedAt.After(records[j].CreatedAt)
	})

	filtered := make([]swf.JobSummary, 0, len(records))
	for _, r := range records {
		if hasCursor {
			if r.CreatedAt.After(cursorTime) {
				continue
			}
			if r.CreatedAt.Equal(cursorTime) && r.JobKey.String() >= cursorJob {
				continue
			}
		}
		filtered = append(filtered, r)
	}

	nextToken := ""
	if len(filtered) > pageSize {
		last := filtered[pageSize-1]
		if tok, err := swf.EncodeListJobsPageToken(last.CreatedAt, last.JobKey); err == nil {
			nextToken = tok
		}
		filtered = filtered[:pageSize]
	}

	return swf.ListJobsResponse{Jobs: filtered, NextPageToken: nextToken}, nil
}

func (e *ToyEngine) getWorkSet(jobType string) (swf.WorkSet, bool) {
	e.mu.Lock()
	defer e.mu.Unlock()
	ws, ok := e.workers[jobType]
	return ws, ok
}

func (e *ToyEngine) setJobRecord(key swf.JobKey, record *jobRecord) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.jobRecords[key] = record
}

func (e *ToyEngine) runJob(ctx context.Context, jobKey swf.JobKey, ws swf.WorkSet, data swf.JobData) {
	record := e.getJobRecord(jobKey)
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
		if record.status == swf.JobStatusCompleted {
			finished := record.finished
			record.archived = &finished
		}
	}()

	jc := &toyJobContext{
		engine:   e,
		jobKey:   jobKey,
		logger:   e.logger,
		workSet:  ws,
		step:     1,
		cancelCh: ctx.Done(),
		record:   record,
	}
	result, err = ws.JobWorker.Run(jc, data)
}

func (e *ToyEngine) getJobRecord(key swf.JobKey) *jobRecord {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.jobRecords[key]
}

type toyJobContext struct {
	engine   *ToyEngine
	jobKey   swf.JobKey
	logger   *slog.Logger
	workSet  swf.WorkSet
	step     int64
	cancelCh <-chan struct{}
	record   *jobRecord
}

func (c *toyJobContext) GetJobKey() swf.JobKey {
	return c.jobKey
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
		return c.awaitExternalCompletion(taskType, data)
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

	tc := swf.NewTaskContext(c.jobKey, c.step, c.Logger(), await, nil)
	output, err := taskWorker.Run(tc, data)
	if err != nil {
		return nil, err
	}
	c.step++
	return output, nil
}

func (c *toyJobContext) awaitExternalCompletion(taskType string, data swf.TaskData) (swf.TaskData, error) {
	capability := c.workSet.JobWorker.Name() + ":" + taskType
	pending := &pendingTask{
		jobKey:     c.jobKey,
		data:       data,
		capability: capability,
		step:       c.step,
		done:       make(chan pendingResult, 1),
	}

	c.engine.mu.Lock()
	c.engine.pending[capability] = append(c.engine.pending[capability], pending)
	c.record.mu.Lock()
	if c.record.status != swf.JobStatusCancelled {
		c.record.status = swf.JobStatusPendingJobs
	}
	c.record.capability = capability
	c.record.step = pending.step
	c.record.mu.Unlock()
	c.engine.mu.Unlock()

	select {
	case res := <-pending.done:
		c.record.mu.Lock()
		if !c.record.cancelled {
			c.record.status = swf.JobStatusActive
		}
		c.record.mu.Unlock()
		if res.err != nil {
			return nil, res.err
		}
		c.step++
		return res.data, nil
	case <-c.cancelCh:
		c.engine.removePending(pending)
		c.markCancelled()
		return nil, context.Canceled
	}
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

func (e *ToyEngine) removePending(task *pendingTask) {
	e.mu.Lock()
	defer e.mu.Unlock()
	queue := e.pending[task.capability]
	for i, p := range queue {
		if p == task {
			e.pending[task.capability] = append(queue[:i], queue[i+1:]...)
			return
		}
	}
}
