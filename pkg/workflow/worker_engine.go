package workflow

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

type workerEngine struct {
	runtime        WorkflowRuntime
	logger         *slog.Logger
	workerID       string
	maxActive      int
	awaitThreshold time.Duration
	pollTenantId   string

	mu           sync.RWMutex
	workers      map[string]*WorkSet
	capabilities []string
	pollGroups   []pollGroup
}

type pollGroup struct {
	capabilities   []string
	metadataEquals []MetadataPredicate
}

func newWorkerEngine(runtime WorkflowRuntime, workers []WorkSet, opts RuntimeBuildOptions) (workerEngineAPI, error) {
	workerID, err := newRuntimeWorkerID()
	if err != nil {
		return nil, err
	}
	if len(workers) > 0 && opts.PollTenantId == "" {
		return nil, fmt.Errorf("worker poll tenantId is required when workers are registered")
	}
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	engine := &workerEngine{
		runtime:        runtime,
		logger:         logger,
		workerID:       workerID,
		maxActive:      opts.MaxActive,
		awaitThreshold: opts.AwaitRecycleThreshold,
		pollTenantId:   opts.PollTenantId,
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
	if e.pollTenantId == "" {
		return fmt.Errorf("worker poll tenantId is required when workers are registered")
	}
	clone := *workset
	predicates, err := MetadataPredicates(clone.Options.MetadataFilter)
	if err != nil {
		return err
	}
	if _, err := metadataPredicateSignature(predicates); err != nil {
		return err
	}
	clone.metadataEquals = predicates
	e.workers[jobType] = &clone
	e.refreshCapabilitiesLocked()
	return nil
}

func (e *workerEngine) refreshCapabilitiesLocked() {
	caps := make([]string, 0, len(e.workers)*2)
	groupMap := make(map[string]*pollGroup)
	groupOrder := make([]string, 0, len(e.workers))
	for jobType, ws := range e.workers {
		caps = append(caps, jobType)
		signature, err := metadataPredicateSignature(ws.metadataEquals)
		if err != nil {
			signature = ""
		}
		group := groupMap[signature]
		if group == nil {
			group = &pollGroup{
				metadataEquals: cloneMetadataPredicates(ws.metadataEquals),
			}
			groupMap[signature] = group
			groupOrder = append(groupOrder, signature)
		}
		group.capabilities = append(group.capabilities, jobType)
		for taskType := range ws.TaskWorkers {
			capability := workerCapability(jobType, taskType)
			caps = append(caps, capability)
			group.capabilities = append(group.capabilities, capability)
		}
	}
	e.capabilities = caps
	sort.Strings(e.capabilities)
	groups := make([]pollGroup, 0, len(groupOrder))
	for _, signature := range groupOrder {
		group := groupMap[signature]
		if group == nil {
			continue
		}
		sort.Strings(group.capabilities)
		groups = append(groups, pollGroup{
			capabilities:   append([]string(nil), group.capabilities...),
			metadataEquals: cloneMetadataPredicates(group.metadataEquals),
		})
	}
	sort.Slice(groups, func(i, j int) bool {
		if len(groups[i].metadataEquals) == len(groups[j].metadataEquals) {
			return strings.Join(groups[i].capabilities, ",") < strings.Join(groups[j].capabilities, ",")
		}
		return len(groups[i].metadataEquals) < len(groups[j].metadataEquals)
	})
	e.pollGroups = groups
}

func (e *workerEngine) capabilitiesSnapshot() []string {
	e.mu.RLock()
	defer e.mu.RUnlock()
	caps := make([]string, len(e.capabilities))
	copy(caps, e.capabilities)
	return caps
}

func (e *workerEngine) pollGroupsSnapshot() []pollGroup {
	e.mu.RLock()
	defer e.mu.RUnlock()
	out := make([]pollGroup, 0, len(e.pollGroups))
	for _, group := range e.pollGroups {
		out = append(out, pollGroup{
			capabilities:   append([]string(nil), group.capabilities...),
			metadataEquals: cloneMetadataPredicates(group.metadataEquals),
		})
	}
	return out
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
		foundLease := false
		hadPollError := false
		for _, group := range e.pollGroupsSnapshot() {
			leases, err := e.runtime.PollWork(ctx, PollWorkRequest{
				TenantId:       e.pollTenantId,
				WorkerID:       workerID,
				Capabilities:   group.capabilities,
				Limit:          1,
				MetadataEquals: cloneMetadataPredicates(group.metadataEquals),
			})
			if err != nil {
				if ctx.Err() != nil {
					hadPollError = false
					break
				}
				hadPollError = true
				e.logger.Error("poll work failed", "worker", workerID, "error", err)
				continue
			}
			if len(leases) == 0 {
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
			foundLease = true
			break
		}
		if ctx.Err() != nil {
			break
		}
		if hadPollError {
			if !sleepWithContext(ctx, backoff) {
				break
			}
			backoff = minDuration(backoff*2, maxBackoff)
			continue
		}
		if !foundLease {
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

func cloneMetadataPredicates(predicates []MetadataPredicate) []MetadataPredicate {
	if len(predicates) == 0 {
		return nil
	}
	out := make([]MetadataPredicate, 0, len(predicates))
	for _, predicate := range predicates {
		out = append(out, MetadataPredicate{
			Path:   append([]string(nil), predicate.Path...),
			Values: append([]any(nil), predicate.Values...),
		})
	}
	return out
}

func (e *workerEngine) runLease(ctx context.Context, lease ExecutionLease, workerID string) {
	workset, ok := e.workSetForCapability(lease.Capability())
	if !ok {
		e.logger.Error("no workset found for capability", "capability", lease.Capability(), "job", lease.Job().JobKey)
		return
	}
	_, _ = runClaimedJobLease(ctx, e.runtime, workset, lease, claimedJobRunOptions{
		Logger:         e.logger,
		WorkerID:       workerID,
		AwaitThreshold: e.awaitThreshold,
	})
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
	replayRuntime := newReplayReadOnlyRuntime(runtime)
	jobKey := spec.jobKey
	chapter, err := replayRuntime.GetChapter(ctx, ChapterRef{JobKey: jobKey, Ordinal: 0})
	if err != nil {
		if err == ErrChapterNotFound {
			return nil, ErrJobNotFound
		}
		return nil, err
	}
	meta, err := chapterMetaFromChapter(chapter)
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

	runner := newWorkerRunner(replayRuntime, ws, nil, workerRunnerOptions{
		JobKey:         jobKey,
		Logger:         spec.engine.logger.With("job", jobKey.String(), "capability", jobType),
		WorkerID:       spec.engine.workerID,
		Observer:       spec.observer,
		Replay:         true,
		AwaitThreshold: spec.engine.awaitThreshold,
	})
	return runner.DoJob(ctx)
}
