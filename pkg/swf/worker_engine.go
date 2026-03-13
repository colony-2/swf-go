package swf

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/segmentio/ksuid"
)

type workerEngine struct {
	runtime        WorkflowRuntime
	logger         *slog.Logger
	workerID       string
	maxActive      int
	awaitThreshold time.Duration

	mu           sync.RWMutex
	workers      map[string]*WorkSet
	capabilities []string
}

func newWorkerEngine(runtime WorkflowRuntime, workers []WorkSet, opts RuntimeBuildOptions) (workerEngineAPI, error) {
	host, err := os.Hostname()
	if err != nil {
		return nil, err
	}
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	engine := &workerEngine{
		runtime:        runtime,
		logger:         logger,
		workerID:       fmt.Sprintf("%s:%d-%s", host, os.Getpid(), ksuid.New().String()),
		maxActive:      opts.MaxActive,
		awaitThreshold: opts.AwaitRecycleThreshold,
		workers:        make(map[string]*WorkSet),
	}
	for i := range workers {
		ws := workers[i]
		if err := engine.RegisterWorkers(&ws); err != nil {
			return nil, err
		}
	}
	return engine, nil
}

func (e *workerEngine) RegisterWorkers(workset *WorkSet) error {
	if workset == nil {
		return fmt.Errorf("workset is nil")
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	jobType := workset.JobWorker.Name()
	if _, ok := e.workers[jobType]; ok {
		return fmt.Errorf("worker %s already registered", jobType)
	}
	clone := *workset
	e.workers[jobType] = &clone
	e.refreshCapabilitiesLocked()
	return nil
}

func (e *workerEngine) refreshCapabilitiesLocked() {
	caps := make([]string, 0, len(e.workers)*2)
	for jobType, ws := range e.workers {
		caps = append(caps, jobType)
		for taskType := range ws.TaskWorkers {
			caps = append(caps, workerCapability(jobType, taskType))
		}
	}
	e.capabilities = caps
}

func (e *workerEngine) capabilitiesSnapshot() []string {
	e.mu.RLock()
	defer e.mu.RUnlock()
	caps := make([]string, len(e.capabilities))
	copy(caps, e.capabilities)
	return caps
}

func (e *workerEngine) workSetForCapability(capability string) (*WorkSet, bool) {
	e.mu.RLock()
	defer e.mu.RUnlock()
	jobType := JobTypeFromNextNeed(capability)
	ws, ok := e.workers[jobType]
	return ws, ok
}

func (e *workerEngine) ReplayJobRun(ctx context.Context, req ReplayRunRequest) (JobData, error) {
	return replayRuntimeJob(ctx, e.runtime, e.lookupReplayWorkSet(req))
}

func (e *workerEngine) lookupReplayWorkSet(req ReplayRunRequest) *replayWorkSet {
	e.mu.RLock()
	defer e.mu.RUnlock()
	return &replayWorkSet{
		engine:    e,
		jobKey:    req.JobKey,
		jobWorker: req.JobWorker,
		observer:  req.Observer,
	}
}

func (e *workerEngine) FindTasksWaitingForCapability(ctx context.Context, jobType string, taskType string, tenantIds []string) ([]TaskHandle, error) {
	runtime, ok := e.runtime.(waitingTaskRuntime)
	if !ok {
		return nil, fmt.Errorf("workflow runtime does not support waiting task inspection")
	}
	return runtime.FindTasksWaitingForCapability(ctx, jobType, taskType, tenantIds)
}

func (e *workerEngine) GetWaitingTask(ctx context.Context, key JobKey) (TaskHandle, error) {
	runtime, ok := e.runtime.(waitingTaskRuntime)
	if !ok {
		return nil, fmt.Errorf("workflow runtime does not support waiting task inspection")
	}
	return runtime.GetWaitingTask(ctx, key)
}

func (e *workerEngine) GetArtifact(tenantId string, key ArtifactKey) (Artifact, error) {
	ref := ChapterRef{
		JobKey: JobKey{
			TenantId: tenantId,
			JobId:    key.JobId,
		},
		Ordinal: key.TaskOrdinal,
	}
	chapter, err := e.runtime.GetChapter(context.Background(), ref)
	if err != nil {
		return nil, err
	}
	for _, art := range chapter.Artifacts {
		if art.Name != key.Name {
			continue
		}
		return newRuntimeBackedArtifact(e.runtime, ArtifactRef{
			JobKey:  ref.JobKey,
			Ordinal: ref.Ordinal,
			Name:    art.Name,
			Digest:  art.Digest,
		}, art.Size), nil
	}
	return nil, fmt.Errorf("artifact %s not found for job %s ordinal %d", key.Name, key.JobId, key.TaskOrdinal)
}

func (e *workerEngine) Run(ctx context.Context) {
	limit := e.maxActive
	if limit <= 0 {
		limit = 1
	}
	var wg sync.WaitGroup
	var workerSeq atomic.Uint64
	active := make(chan struct{}, limit)
	leaseDone := make(chan struct{}, limit)

	backoff := 50 * time.Millisecond
	const maxBackoff = 30 * time.Second

	for {
		for len(active) >= limit {
			select {
			case <-ctx.Done():
				wg.Wait()
				return
			case <-leaseDone:
			}
		}

		workerID := e.nextWorkerID(workerSeq.Add(1))
		leases, err := e.runtime.PollWork(ctx, PollWorkRequest{
			WorkerID:     workerID,
			Capabilities: e.capabilitiesSnapshot(),
			Limit:        1,
		})
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			e.logger.Error("poll work failed", "worker", workerID, "error", err)
			if !sleepWithContext(ctx, backoff) {
				break
			}
			backoff = minDuration(backoff*2, maxBackoff)
			continue
		}
		if len(leases) == 0 {
			select {
			case <-leaseDone:
				backoff = 50 * time.Millisecond
				continue
			default:
			}
			if !sleepWithContext(ctx, backoff) {
				break
			}
			backoff = minDuration(backoff*2, maxBackoff)
			continue
		}

		backoff = 50 * time.Millisecond
		active <- struct{}{}
		wg.Add(1)
		go func(lease ExecutionLease, workerID string) {
			defer wg.Done()
			defer func() {
				<-active
				select {
				case leaseDone <- struct{}{}:
				default:
				}
			}()
			e.runLease(ctx, lease, workerID)
		}(leases[0], workerID)
	}

	wg.Wait()
}

func (e *workerEngine) nextWorkerID(seq uint64) string {
	if e.maxActive <= 1 && seq <= 1 {
		return e.workerID
	}
	return fmt.Sprintf("%s/%d", e.workerID, seq)
}

func sleepWithContext(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}

func (e *workerEngine) runLease(ctx context.Context, lease ExecutionLease, workerID string) {
	workset, ok := e.workSetForCapability(lease.Capability())
	if !ok {
		e.logger.Error("no workset found for capability", "capability", lease.Capability(), "job", lease.Job().JobKey)
		return
	}

	payload := workerJobPayload{}
	if raw := lease.Payload(); len(raw) > 0 {
		if err := json.Unmarshal(raw, &payload); err != nil {
			e.logger.Warn("failed to decode job payload", "job", lease.Job().JobKey, "error", err)
		}
	}
	payload.RunPolicy = normalizeRunPolicy(payload.RunPolicy)

	runner := newWorkerRunner(e.runtime, workset, lease, workerRunnerOptions{
		Logger:         e.logger.With("job", lease.Job().JobKey.String(), "capability", lease.Capability()),
		JobPolicy:      payload.RunPolicy,
		WorkerID:       workerID,
		AwaitThreshold: e.awaitThreshold,
	})
	_, _ = runner.DoJob(ctx)
}

type replayWorkSet struct {
	engine    *workerEngine
	jobKey    JobKey
	jobWorker JobWorker
	observer  ReplayObserver
}

func replayRuntimeJob(ctx context.Context, runtime WorkflowRuntime, spec *replayWorkSet) (JobData, error) {
	if spec == nil || spec.engine == nil {
		return nil, fmt.Errorf("replay workset is required")
	}
	jobKey := spec.jobKey
	chapter, err := runtime.GetChapter(ctx, ChapterRef{JobKey: jobKey, Ordinal: 0})
	if err != nil {
		if err == ErrChapterNotFound {
			return nil, ErrJobNotFound
		}
		return nil, err
	}
	meta, err := storedChapterMeta(chapter)
	if err != nil {
		return nil, err
	}
	jobType := meta.TaskType
	if jobType == "" {
		jobType = chapter.TaskType
	}
	ws, ok := spec.engine.workSetForCapability(jobType)
	if !ok {
		return nil, fmt.Errorf("job worker %s not registered", jobType)
	}
	if spec.jobWorker != nil {
		ws = &WorkSet{
			JobWorker:   spec.jobWorker,
			TaskWorkers: ws.TaskWorkers,
		}
	}

	runner := newWorkerRunner(runtime, ws, nil, workerRunnerOptions{
		JobKey:         jobKey,
		Logger:         spec.engine.logger.With("job", jobKey.String(), "capability", jobType),
		WorkerID:       spec.engine.workerID,
		Observer:       spec.observer,
		Replay:         true,
		AwaitThreshold: spec.engine.awaitThreshold,
	})
	return runner.DoJob(ctx)
}
