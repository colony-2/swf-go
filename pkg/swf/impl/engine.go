package impl

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/cenkalti/backoff"
	"github.com/colony-2/pgwf-go/pkg/pgwf"
	strataclient "github.com/colony-2/strata-go/pkg/client"
	"github.com/colony-2/strata-go/pkg/client/story"
	"github.com/colony-2/swf-go/pkg/swf"
	"github.com/google/uuid"
	"github.com/segmentio/ksuid"
	"gorm.io/gorm"
)

type awaitSignalKind int

const (
	awaitSignalKindWake awaitSignalKind = iota
	awaitSignalKindRecycle
)

type awaitSignal struct {
	Kind       awaitSignalKind
	ChildJobID pgwf.JobID
}

type asyncNotificationPayload struct {
	ParentJobID  pgwf.JobID `json:"parent_job_id"`
	AwaitOrdinal int64      `json:"await_ordinal"`
	ChildJobID   pgwf.JobID `json:"child_job_id"`
}

type awaitState struct {
	ch                chan awaitSignal
	timer             *time.Timer
	wakeAt            time.Time
	lease             *pgwf.Lease
	capability        pgwf.Capability
	ordinal           int64
	attempt           int
	recycled          bool
	childJobID        pgwf.JobID
	notificationJobID pgwf.JobID
	started           time.Time
}

type jobPayload struct {
	RunPolicy   swf.RunPolicy             `json:"run_policy,omitempty"`
	TaskWait    *taskWait                 `json:"task_wait,omitempty"`
	AsyncNotify *asyncNotificationPayload `json:"async_notify,omitempty"`
}

func notificationCapability(workerId string) pgwf.Capability {
	return pgwf.Capability(fmt.Sprintf("NOTIFICATION-%s", workerId))
}

func (s *swfEngineImpl) isNotificationCapability(cap pgwf.Capability) bool {
	return cap == notificationCapability(s.workerId)
}

type swfEngineImpl struct {
	strata          *strataclient.Client
	db              *gorm.DB
	udb             *sql.DB
	workers         map[pgwf.Capability]*swf.WorkSet
	workersMu       sync.RWMutex
	capabilities    []pgwf.Capability
	workerId        string
	runners         map[string]runner
	activeWorkLimit int
	logger          *slog.Logger
	awaitMu         sync.Mutex
	awaits          map[pgwf.JobID]*awaitState
	awaitThreshold  time.Duration
	awaitRecycler   sync.Once
}

func (s *swfEngineImpl) dbFromCtx(ctx context.Context) *gorm.DB {
	if ctx == nil {
		ctx = context.Background()
	}
	if tx, ok := swf.TxFromCtx(ctx); ok && tx != nil {
		return tx.WithContext(ctx)
	}
	return s.db.WithContext(ctx)
}

func (s *swfEngineImpl) sqlTxFromCtx(ctx context.Context) *sql.Tx {
	if ctx == nil {
		return nil
	}
	if tx, ok := swf.TxFromCtx(ctx); ok && tx != nil {
		return sqlTxFromGorm(tx)
	}
	return nil
}

func (s *swfEngineImpl) pgwfDB(ctx context.Context) pgwf.DB {
	if tx := s.sqlTxFromCtx(ctx); tx != nil {
		return tx
	}
	return s.udb
}

func sqlTxFromGorm(db *gorm.DB) *sql.Tx {
	if db == nil {
		return nil
	}
	if db.Statement != nil {
		if tx, ok := db.Statement.ConnPool.(*sql.Tx); ok && tx != nil {
			return tx
		}
	}
	if tx, ok := db.ConnPool.(*sql.Tx); ok && tx != nil {
		return tx
	}
	return nil
}

func (s *swfEngineImpl) refreshCapabilitiesLocked() {
	seen := make(map[pgwf.Capability]struct{})
	caps := make([]pgwf.Capability, 0, len(s.workers)+1)
	for capKey, ws := range s.workers {
		if _, ok := seen[capKey]; !ok {
			caps = append(caps, capKey)
			seen[capKey] = struct{}{}
		}
		for _, c := range ws.Capabilities {
			if _, ok := seen[c]; !ok {
				caps = append(caps, c)
				seen[c] = struct{}{}
			}
		}
	}
	notif := notificationCapability(s.workerId)
	if _, ok := seen[notif]; !ok {
		caps = append(caps, notif)
	}
	s.capabilities = caps
}

func (s *swfEngineImpl) refreshCapabilities() {
	s.workersMu.Lock()
	defer s.workersMu.Unlock()
	s.refreshCapabilitiesLocked()
}

func (s *swfEngineImpl) addWorkSetLocked(workset *swf.WorkSet) {
	if s.workers == nil {
		s.workers = make(map[pgwf.Capability]*swf.WorkSet)
	}
	for _, c := range workset.Capabilities {
		s.workers[c] = workset
	}
	s.workers[pgwf.Capability(workset.JobWorker.Name())] = workset
}

func (s *swfEngineImpl) capabilitiesSnapshot() []pgwf.Capability {
	s.workersMu.RLock()
	defer s.workersMu.RUnlock()
	caps := make([]pgwf.Capability, len(s.capabilities))
	copy(caps, s.capabilities)
	return caps
}

func (s *swfEngineImpl) workSetFor(capability pgwf.Capability) (*swf.WorkSet, bool) {
	s.workersMu.RLock()
	defer s.workersMu.RUnlock()
	ws, ok := s.workers[capability]
	return ws, ok
}

func (s *swfEngineImpl) RegisterWorkers(workset *swf.WorkSet) error {
	if workset == nil {
		return fmt.Errorf("workset is nil")
	}

	jobCap := pgwf.Capability(workset.JobWorker.Name())

	s.workersMu.Lock()
	defer s.workersMu.Unlock()

	if _, ok := s.workers[jobCap]; ok {
		return fmt.Errorf("worker %s already registered", workset.JobWorker.Name())
	}
	for _, cap := range workset.Capabilities {
		if existing, ok := s.workers[cap]; ok {
			return fmt.Errorf("capability %s already registered for worker %s", cap, existing.JobWorker.Name())
		}
	}

	s.addWorkSetLocked(workset)
	s.refreshCapabilitiesLocked()
	return nil
}

type chapterMetadata struct {
	Attempt       int
	MaxAttempts   int
	NextAttemptAt *time.Time
	BackoffMillis int64
	Retryable     *bool
	InputRef      *swf.InputReference
	RunPolicy     *swf.RunPolicy
}

func taskDataToChapter(jobData swf.TaskData, ordinal int64, taskType string, workerId string, payloadKind string, inputHash string, createdAt time.Time, meta chapterMetadata) (story.Chapter, error) {
	if jobData == nil {
		return nil, fmt.Errorf("task data is required")
	}

	data, err := jobData.GetData()
	if err != nil {
		return nil, err
	}
	artifacts, err := jobData.GetArtifacts()
	if err != nil {
		return nil, err
	}
	return payloadToChapter(data, artifacts, ordinal, taskType, workerId, payloadKind, inputHash, createdAt, meta)
}

func taskDataToCreatOptions(jobData swf.TaskData, ordinal int64, taskType string, workerId string, payloadKind string, inputHash string, createdAt time.Time, meta chapterMetadata) (story.CreateOptions, error) {
	chap, err := taskDataToChapter(jobData, ordinal, taskType, workerId, payloadKind, inputHash, createdAt, meta)
	if err != nil {
		return story.CreateOptions{}, err
	}

	co := story.CreateOptions{
		RequestID:      uuid.New().String(),
		InitialChapter: chap,
	}
	return co, nil
}

func (s *swfEngineImpl) StartJob(ctx context.Context, job swf.StartJob) (swf.JobKey, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	// Use provided JobID if present, otherwise generate a new one
	jobId := job.JobID
	if jobId == "" {
		jobId = ksuid.New().String()
	}
	jobKey := swf.JobKey{
		TenantId: job.TenantId,
		JobId:    jobId,
	}
	key := jobKey.ToStoryKey()
	taskData := swf.TaskData(job.Data)
	inputHash, err := computeInputHash(ctx, taskData)
	if err != nil {
		return swf.JobKey{}, err
	}
	now := time.Now().UTC()
	jobPolicy := job.RunPolicy
	jobPolicy = normalizeRunPolicy(jobPolicy)
	co, err := taskDataToCreatOptions(taskData, 0, job.JobType, s.workerId, payloadKindApp, inputHash, now, chapterMetadata{
		Attempt: 1,
	})
	if err != nil {
		return swf.JobKey{}, err
	}
	_, err = s.strata.CreateStory(ctx, key, co)
	if err != nil {
		return swf.JobKey{}, err
	}

	return jobKey, s.startJob(ctx, jobKey, job.JobType, job.SingletonKey, jobPayload{RunPolicy: jobPolicy})
}

func (s *swfEngineImpl) startJob(ctx context.Context, jobKey swf.JobKey, jobType string, singletonKey string, payload jobPayload) error {
	if ctx == nil {
		ctx = context.Background()
	}
	dep := pgwf.JobDependencies{
		NextNeed: pgwf.Capability(jobType),
	}
	tenantID := pgwf.TenantID(jobKey.TenantId)
	return pgwf.SubmitJob(ctx, s.pgwfDB(ctx), tenantID, pgwf.JobID(jobKey.JobId), dep, payload, pgwf.WorkerID(s.workerId), singletonKey, time.Time{})
}

func (s *swfEngineImpl) RestartJob(ctx context.Context, job swf.RestartJob) (swf.JobKey, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	jobKey := swf.JobKey{
		TenantId: job.TenantId,
		JobId:    ksuid.New().String(),
	}
	sourceJob := job.PriorJobKey.ToStoryKey()
	targetJob := jobKey.ToStoryKey()

	inputHash, err := computeInputHash(ctx, job.Data)
	if err != nil {
		return swf.JobKey{}, err
	}
	now := time.Now().UTC()
	jobPolicy := job.RunPolicy
	jobPolicy = normalizeRunPolicy(jobPolicy)
	createOptions, err := taskDataToCreatOptions(job.Data, job.LastStepToKeep+1, job.JobType, s.workerId, payloadKindApp, inputHash, now, chapterMetadata{
		Attempt: 1,
	})
	if err != nil {
		return swf.JobKey{}, err
	}

	cloneOptions := story.CloneOptions{
		DestinationKey: targetJob,
		LastOrdinal:    job.LastStepToKeep,
		CreateOptions:  createOptions,
	}
	_, err = s.strata.CloneStory(ctx, sourceJob, cloneOptions)

	if err != nil {
		return swf.JobKey{}, err
	}
	return jobKey, s.startJob(ctx, jobKey, job.JobType, job.SingletonKey, jobPayload{RunPolicy: jobPolicy})
}

func (s *swfEngineImpl) CancelJob(ctx context.Context, job swf.CancelJob) error {
	if ctx == nil {
		ctx = context.Background()
	}
	tenantID := pgwf.TenantID(job.JobKey.TenantId)
	return pgwf.CancelJob(ctx, s.pgwfDB(ctx), tenantID, pgwf.JobID(job.JobKey.JobId), pgwf.WorkerID(s.workerId), job.Reason)
}

func (s *swfEngineImpl) SetAwaitThreshold(d time.Duration) {
	if d > 0 {
		s.awaitThreshold = d
	}
}

func (s *swfEngineImpl) resetAwaitState(jobID pgwf.JobID) {
	s.awaitMu.Lock()
	defer s.awaitMu.Unlock()
	if s.awaits == nil {
		return
	}
	if st, ok := s.awaits[jobID]; ok {
		if st.timer != nil {
			if !st.timer.Stop() {
				select {
				case <-st.timer.C:
				default:
				}
			}
			st.timer = nil
		}
		for len(st.ch) > 0 {
			<-st.ch
		}
	}
}

func (s *swfEngineImpl) awaitState(jobID pgwf.JobID) *awaitState {
	s.awaitMu.Lock()
	defer s.awaitMu.Unlock()
	if s.awaits == nil {
		s.awaits = make(map[pgwf.JobID]*awaitState)
	}
	state, ok := s.awaits[jobID]
	if !ok {
		state = &awaitState{ch: make(chan awaitSignal, 1)}
		s.awaits[jobID] = state
	}
	return state
}

func (s *swfEngineImpl) sendAwait(jobID pgwf.JobID, sig awaitSignal) {
	s.awaitMu.Lock()
	state, ok := s.awaits[jobID]
	if !ok {
		s.awaitMu.Unlock()
		return
	}
	if sig.Kind == awaitSignalKindRecycle {
		state.recycled = true
	}
	ch := state.ch
	s.awaitMu.Unlock()
	select {
	case ch <- sig:
	default:
	}
}

// AwaitUntil chooses whether to block in-memory or recycle the runner until wakeAt.
func (s *swfEngineImpl) AwaitUntil(jobID pgwf.JobID, capability pgwf.Capability, lease *pgwf.Lease, ordinal int64, attempt int, wakeAt time.Time) <-chan awaitSignal {
	if lease == nil || capability == "" {
		return nil
	}

	state := s.awaitState(jobID)
	s.awaitMu.Lock()
	if state.timer != nil {
		if !state.timer.Stop() {
			select {
			case <-state.timer.C:
			default:
			}
		}
	}
	state.wakeAt = wakeAt
	state.lease = lease
	state.capability = capability
	state.ordinal = ordinal
	state.attempt = attempt
	state.recycled = false
	state.childJobID = ""
	state.notificationJobID = ""
	state.started = time.Now()
	waitFor := time.Until(wakeAt)
	if waitFor < 0 {
		waitFor = 0
	}
	state.timer = time.AfterFunc(waitFor, func() {
		s.awaitMu.Lock()
		st, ok := s.awaits[jobID]
		alreadyRecycle := ok && st.recycled
		s.awaitMu.Unlock()
		if ok && !alreadyRecycle {
			s.sendAwait(jobID, awaitSignal{Kind: awaitSignalKindWake})
		}
	})
	ch := state.ch
	s.awaitMu.Unlock()
	return ch
}

// AwaitChild registers a child-job await using the same await channel.
func (s *swfEngineImpl) AwaitChild(jobID pgwf.JobID, capability pgwf.Capability, lease *pgwf.Lease, ordinal int64, childJobID pgwf.JobID, notificationJobID pgwf.JobID) <-chan awaitSignal {
	if lease == nil || capability == "" {
		return nil
	}
	state := s.awaitState(jobID)
	s.awaitMu.Lock()
	if state.timer != nil {
		if !state.timer.Stop() {
			select {
			case <-state.timer.C:
			default:
			}
		}
		state.timer = nil
	}
	state.wakeAt = time.Time{}
	state.lease = lease
	state.capability = capability
	state.ordinal = ordinal
	state.attempt = 0
	state.childJobID = childJobID
	state.notificationJobID = notificationJobID
	state.recycled = false
	state.started = time.Now()
	ch := state.ch
	s.awaitMu.Unlock()
	return ch
}

func (s *swfEngineImpl) startAwaitRecycler(ctx context.Context) {
	s.awaitRecycler.Do(func() {
		go s.awaitRecycleLoop(ctx)
	})
}

func (s *swfEngineImpl) awaitRecycleLoop(ctx context.Context) {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			s.recycleLongWaits()
		}
	}
}

func (s *swfEngineImpl) recycleLongWaits() {
	now := time.Now()
	threshold := s.awaitThreshold
	if threshold <= 0 {
		threshold = 5 * time.Minute
	}
	type pending struct {
		jobID pgwf.JobID
		st    *awaitState
	}
	var items []pending

	s.awaitMu.Lock()
	for jobID, st := range s.awaits {
		if st == nil || st.recycled || st.lease == nil {
			continue
		}
		if st.childJobID != "" {
			if !st.started.IsZero() && now.Sub(st.started) > threshold {
				items = append(items, pending{jobID: jobID, st: st})
			}
			continue
		}
		if st.wakeAt.IsZero() {
			continue
		}
		if st.wakeAt.Sub(now) > threshold {
			items = append(items, pending{jobID: jobID, st: st})
		}
	}
	s.awaitMu.Unlock()

	for _, item := range items {
		s.recycleAwait(item.jobID, item.st)
	}
}

func (s *swfEngineImpl) recycleAwait(jobID pgwf.JobID, st *awaitState) {
	if st == nil || st.lease == nil {
		return
	}
	payload := st.lease.Payload()
	if len(payload) == 0 {
		payload = json.RawMessage(`{}`)
	}
	deps := pgwf.JobDependencies{
		NextNeed: st.capability,
	}
	if st.childJobID != "" {
		deps.WaitFor = []pgwf.JobID{st.childJobID}
	} else {
		deps.AvailableAt = st.wakeAt
	}
	if err := st.lease.Reschedule(context.TODO(), s.udb, deps, payload); err != nil {
		s.logger.Warn("await recycle reschedule failed", "jobId", jobID, "ordinal", st.ordinal, "attempt", st.attempt, "error", err)
		return
	}
	s.awaitMu.Lock()
	if st.timer != nil {
		if !st.timer.Stop() {
			select {
			case <-st.timer.C:
			default:
			}
		}
	}
	st.recycled = true
	s.awaitMu.Unlock()
	s.sendAwait(jobID, awaitSignal{Kind: awaitSignalKindRecycle, ChildJobID: st.childJobID})
}

// payloadToChapter builds a chapter from raw payload JSON and artifacts, bypassing TaskData.
func payloadToChapter(payload json.RawMessage, artifacts []swf.Artifact, ordinal int64, taskType string, workerId string, payloadKind string, inputHash string, createdAt time.Time, metaOpts chapterMetadata) (story.Chapter, error) {
	if payload == nil {
		return nil, fmt.Errorf("payload is required")
	}
	if inputHash == "" {
		return nil, fmt.Errorf("input hash is required")
	}
	meta := chapterMeta{
		Version:   envelopeVersion,
		Ordinal:   ordinal,
		TaskType:  taskType,
		WorkerID:  workerId,
		CreatedAt: createdAt,
		InputHash: inputHash,
	}
	if metaOpts.Attempt > 0 {
		meta.Attempt = metaOpts.Attempt
	}
	if metaOpts.MaxAttempts > 0 {
		meta.MaxAttempts = metaOpts.MaxAttempts
	}
	if metaOpts.NextAttemptAt != nil {
		meta.NextAttemptAt = metaOpts.NextAttemptAt
	}
	if metaOpts.BackoffMillis > 0 {
		meta.BackoffMillis = metaOpts.BackoffMillis
	}
	if metaOpts.Retryable != nil {
		meta.Retryable = metaOpts.Retryable
	}
	if metaOpts.InputRef != nil {
		meta.InputRef = metaOpts.InputRef
	}
	if metaOpts.RunPolicy != nil {
		meta.RunPolicy = metaOpts.RunPolicy
	}

	envBytes, err := buildChapterEnvelope(meta, payloadKind, payload)
	if err != nil {
		return nil, err
	}

	chapBuilder := story.NewChapter().WithOrdinal(ordinal).WithBytes(envBytes)
	for _, v := range artifacts {
		chapBuilder.AddArtifact(swf.ToStrataArtifact(v))
	}
	return chapBuilder, nil
}

func convertPgwfStatusToSwf(status pgwf.JobStatus, cancelRequested bool, archivedAt *time.Time) swf.JobStatus {
	if archivedAt != nil {
		if cancelRequested {
			return swf.JobStatusCancelled
		}
		return swf.JobStatusCompleted
	}

	switch status {
	case pgwf.JobStatusReady:
		return swf.JobStatusReady
	case pgwf.JobStatusActive:
		return swf.JobStatusActive
	case pgwf.JobStatusCancelled:
		return swf.JobStatusCancelled
	case pgwf.JobStatusAwaitingFuture:
		return swf.JobStatusAwaitingFuture
	case pgwf.JobStatusPendingJobs:
		return swf.JobStatusPendingJobs
	case pgwf.JobStatusCrashConcern:
		return swf.JobStatusCrashConcern
	case pgwf.JobStatusExpired:
		return swf.JobStatusExpired
	default:
		return swf.JobStatusReady
	}
}

func convertSwfStatusesToPgwf(statuses []swf.JobStatus) []pgwf.JobStatus {
	result := make([]pgwf.JobStatus, 0, len(statuses))
	for _, st := range statuses {
		switch st {
		case swf.JobStatusCompleted, swf.JobStatusCancelled:
			continue
		default:
			result = append(result, pgwf.JobStatus(st))
		}
	}
	return result
}

func shouldIncludeArchived(stores []swf.JobStore, statuses []swf.JobStatus) bool {
	if len(stores) > 0 {
		for _, store := range stores {
			if store == swf.JobStoreArchived {
				return true
			}
		}
		return false
	}

	for _, st := range statuses {
		if st == swf.JobStatusCompleted || st == swf.JobStatusCancelled {
			return true
		}
	}

	return len(statuses) == 0
}

func buildJobTypePatterns(jobTypes []string, jobTasks []swf.JobTaskFilter) []string {
	patterns := make([]string, 0, len(jobTypes)*2+len(jobTasks))
	for _, jt := range jobTypes {
		patterns = append(patterns, jt, jt+":%")
	}
	for _, task := range jobTasks {
		if task.JobType != "" && task.TaskType != "" {
			patterns = append(patterns, task.JobType+":"+task.TaskType)
		}
	}
	return patterns
}

func normalizePageSize(pageSize int) int {
	if pageSize <= 0 {
		return swf.DefaultListJobsPageSize
	}
	if pageSize > swf.MaxListJobsPageSize {
		return swf.MaxListJobsPageSize
	}
	return pageSize
}

func (s *swfEngineImpl) CheckJobStatus(ctx context.Context, jobKey swf.JobKey) (swf.JobStatus, error) {
	status, err := pgwf.GetJobStatus(ctx, s.pgwfDB(ctx),
		pgwf.TenantID(jobKey.TenantId),
		pgwf.JobID(jobKey.JobId))
	if errors.Is(err, pgwf.ErrJobNotFound) {
		return "", swf.ErrJobNotFound
	}
	if err != nil {
		return "", fmt.Errorf("failed to get job status: %w", err)
	}

	return convertPgwfStatusToSwf(status.Status, status.CancelRequested, status.ArchivedAt), nil
}

func (s *swfEngineImpl) GetJobResult(ctx context.Context, jobKey swf.JobKey) (swf.TaskData, error) {
	isArchived, err := pgwf.IsJobArchived(ctx, s.pgwfDB(ctx),
		pgwf.TenantID(jobKey.TenantId),
		pgwf.JobID(jobKey.JobId))
	if err != nil {
		return nil, fmt.Errorf("failed to check if job is archived: %w", err)
	}
	if !isArchived {
		return nil, swf.ErrJobNotComplete
	}

	key := jobKey.ToStoryKey()
	st, err := s.strata.Story(ctx, key)
	if err != nil {
		return nil, err
	}
	chap, err := st.GetLastChapter(ctx)
	if err != nil {
		return nil, err
	}
	td, payloadErr := chapterToTaskData(chap)
	if payloadErr != nil {
		return td, payloadErr
	}
	return td, nil
}

func (s *swfEngineImpl) jobResultIfComplete(ctx context.Context, jobKey swf.JobKey) (swf.TaskData, bool, error) {
	isArchived, err := pgwf.IsJobArchived(ctx, s.pgwfDB(ctx),
		pgwf.TenantID(jobKey.TenantId),
		pgwf.JobID(jobKey.JobId))
	if err != nil {
		return nil, false, fmt.Errorf("failed to check if job is archived: %w", err)
	}
	if !isArchived {
		return nil, false, nil
	}
	td, err := s.GetJobResult(ctx, jobKey)
	return td, err == nil, err
}

func (s *swfEngineImpl) ensureNotificationJob(ctx context.Context, notificationJobID pgwf.JobID, childJobID pgwf.JobID, parentJobKey swf.JobKey, awaitOrdinal int64) error {
	pgdb := s.pgwfDB(ctx)
	tenantID := pgwf.TenantID(parentJobKey.TenantId)
	deps := pgwf.JobDependencies{
		NextNeed: notificationCapability(s.workerId),
		WaitFor:  []pgwf.JobID{childJobID},
	}
	payload := jobPayload{
		AsyncNotify: &asyncNotificationPayload{
			ParentJobID:  pgwf.JobID(parentJobKey.JobId),
			AwaitOrdinal: awaitOrdinal,
			ChildJobID:   childJobID,
		},
	}
	if err := pgwf.RescheduleUnheldJob(ctx, pgdb, tenantID, notificationJobID, pgwf.WorkerID(s.workerId), deps, payload); err == nil {
		return nil
	} else if errors.Is(err, pgwf.ErrJobNotFound) {
		if err := pgwf.SubmitJob(ctx, pgdb, tenantID, notificationJobID, deps, payload, pgwf.WorkerID(s.workerId), "", time.Time{}); err == nil || errors.Is(err, pgwf.ErrDependencyViolation) {
			return nil
		}
	}
	// If the job is leased or otherwise not ready, defer; an existing notification job will eventually fire.
	return nil
}

func (s *swfEngineImpl) ListJobs(ctx context.Context, req swf.ListJobsRequest) (swf.ListJobsResponse, error) {
	patterns := buildJobTypePatterns(req.JobTypes, req.JobTasks)

	tenantIDs := make([]string, len(req.TenantIds))
	copy(tenantIDs, req.TenantIds)

	pgwfStatuses := convertSwfStatusesToPgwf(req.Statuses)
	includeArchived := shouldIncludeArchived(req.Stores, req.Statuses)

	opts := pgwf.ListJobsOptions{
		TenantIDs:       tenantIDs,
		Statuses:        pgwfStatuses,
		JobTypePatterns: patterns,
		IncludeArchived: includeArchived,
		CreatedAfter:    req.CreatedAfter,
		CreatedBefore:   req.CreatedBefore,
		Limit:           normalizePageSize(req.PageSize),
		Cursor:          req.PageToken,
		SortBy:          pgwf.SortByCreatedAt,
		SortOrder:       pgwf.SortDesc,
	}

	if len(req.SingletonKeys) > 0 {
		opts.SingletonKey = req.SingletonKeys[0]
	}

	result, err := pgwf.ListJobs(ctx, s.pgwfDB(ctx), opts)
	if err != nil {
		return swf.ListJobsResponse{}, fmt.Errorf("failed to list jobs: %w", err)
	}

	// Build filter maps for client-side filtering
	// pgwf.ListJobs doesn't support job ID filtering, so we do it client-side.
	// Also, when user requests only archive statuses, pgwf returns both active and archived
	// because we set IncludeArchived=true with Statuses=[], so we filter by status too.
	requestedStatuses := make(map[swf.JobStatus]bool)
	for _, st := range req.Statuses {
		requestedStatuses[st] = true
	}
	hasRequestedStatuses := len(requestedStatuses) > 0

	requestedJobIDs := make(map[string]bool)
	for _, jk := range req.JobKeys {
		requestedJobIDs[jk.JobId] = true
	}
	hasRequestedJobIDs := len(requestedJobIDs) > 0

	jobs := make([]swf.JobSummary, 0, len(result.Jobs))
	for _, job := range result.Jobs {
		// Filter by job ID if requested
		if hasRequestedJobIDs && !requestedJobIDs[job.JobID] {
			continue
		}

		swfStatus := convertPgwfStatusToSwf(job.Status, job.CancelRequested, job.ArchivedAt)

		// Filter by status if requested
		if hasRequestedStatuses && !requestedStatuses[swfStatus] {
			continue
		}

		jobs = append(jobs, swf.JobSummary{
			JobKey:          swf.JobKey{TenantId: job.TenantID, JobId: job.JobID},
			Status:          swfStatus,
			JobType:         swf.JobTypeFromNextNeed(job.NextNeed),
			SingletonKey:    job.SingletonKey,
			WaitFor:         job.WaitFor,
			AvailableAt:     job.AvailableAt,
			ExpiresAt:       job.ExpiresAt,
			LeaseExpiresAt:  job.LeaseExpiresAt,
			CancelRequested: job.CancelRequested,
			CreatedAt:       job.CreatedAt,
			ArchivedAt:      job.ArchivedAt,
			Payload:         nil,
		})
	}

	return swf.ListJobsResponse{
		Jobs:          jobs,
		NextPageToken: result.NextCursor,
	}, nil
}

func (s *swfEngineImpl) ensureChildAndNotificationJobs(ctx context.Context, childJobID pgwf.JobID, notificationJobID pgwf.JobID, jobType string, runPolicy swf.RunPolicy, parentJobKey swf.JobKey, awaitOrdinal int64) error {
	sqlTx := s.sqlTxFromCtx(ctx)
	if sqlTx == nil {
		sqlTx = s.sqlTxFromCtx(ctx)
	}

	isArchived, err := pgwf.IsJobArchived(ctx, s.pgwfDB(ctx),
		pgwf.TenantID(parentJobKey.TenantId),
		childJobID)
	if err != nil {
		return fmt.Errorf("failed to check if child job is archived: %w", err)
	}
	if isArchived {
		return nil
	}

	jobExistence, err := pgwf.CheckJobExistsWithTenant(ctx, s.pgwfDB(ctx), childJobID, pgwf.TenantID(parentJobKey.TenantId))
	if errors.Is(err, pgwf.ErrTenantMismatch) {
		return fmt.Errorf("child job %s belongs to different tenant, cannot be awaited by job from tenant %s", childJobID, parentJobKey.TenantId)
	}
	if err != nil && !errors.Is(err, pgwf.ErrJobNotFound) {
		return fmt.Errorf("failed to check if child job exists: %w", err)
	}

	if jobExistence != nil && jobExistence.Exists {
		return s.ensureNotificationJob(ctx, notificationJobID, childJobID, parentJobKey, awaitOrdinal)
	}

	runPolicy = normalizeRunPolicy(runPolicy)

	if sqlTx != nil {
		return s.submitChildAndNotify(ctx, sqlTx, childJobID, notificationJobID, jobType, runPolicy, parentJobKey, awaitOrdinal)
	}

	tx, err := s.udb.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	committed := false
	defer func() {
		if !committed {
			_ = tx.Rollback()
		}
	}()

	if err := s.submitChildAndNotify(ctx, tx, childJobID, notificationJobID, jobType, runPolicy, parentJobKey, awaitOrdinal); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return err
	}
	committed = true
	return nil
}

func (s *swfEngineImpl) submitChildAndNotify(ctx context.Context, tx *sql.Tx, childJobID pgwf.JobID, notificationJobID pgwf.JobID, jobType string, runPolicy swf.RunPolicy, parentJobKey swf.JobKey, awaitOrdinal int64) error {
	tenantID := pgwf.TenantID(parentJobKey.TenantId)
	childDeps := pgwf.JobDependencies{NextNeed: pgwf.Capability(jobType)}
	if err := pgwf.SubmitJob(ctx, tx, tenantID, childJobID, childDeps, jobPayload{RunPolicy: runPolicy}, pgwf.WorkerID(s.workerId), "", time.Time{}); err != nil {
		return err
	}

	notifyDeps := pgwf.JobDependencies{
		NextNeed: notificationCapability(s.workerId),
		WaitFor:  []pgwf.JobID{childJobID},
	}
	notifyPayload := jobPayload{
		AsyncNotify: &asyncNotificationPayload{
			ParentJobID:  pgwf.JobID(parentJobKey.JobId),
			AwaitOrdinal: awaitOrdinal,
			ChildJobID:   childJobID,
		},
	}
	return pgwf.SubmitJob(ctx, tx, tenantID, notificationJobID, notifyDeps, notifyPayload, pgwf.WorkerID(s.workerId), "", time.Time{})
}

type taskWait struct {
	InputStep  int64  `json:"in"`
	OutputStep int64  `json:"out"`
	Next       string `json:"next"`
}

func (s *swfEngineImpl) FindTasksWaitingForCapability(ctx context.Context, jobType string, taskType string, tenantIds []string) ([]swf.TaskHandle, error) {
	capability := jobType + ":" + taskType

	opts := pgwf.FindJobsOptions{
		TenantIDs: tenantIds,
		Status:    pgwf.JobStatusReady,
		NextNeed:  capability,
		Limit:     1000,
	}

	jobs, err := pgwf.FindJobs(ctx, s.pgwfDB(ctx), opts)
	if err != nil {
		return nil, fmt.Errorf("failed to find jobs: %w", err)
	}

	handles := make([]swf.TaskHandle, 0, len(jobs))
	for _, j := range jobs {
		details, err := pgwf.GetJob(ctx, s.pgwfDB(ctx), pgwf.TenantID(j.TenantID), pgwf.JobID(j.JobID), pgwf.GetJobOptions{IncludePayload: true})
		if err != nil {
			return nil, fmt.Errorf("failed to get job details: %w", err)
		}

		tw, err := extractTaskWaitFromRaw(details.Payload)
		if err != nil {
			return nil, err
		}

		th := taskHandleImpl{
			jobID:         j.JobID,
			tenantId:      j.TenantID,
			payload:       details.Payload,
			inputOrdinal:  tw.InputStep,
			outputOrdinal: tw.OutputStep,
			engine:        s,
			nextNeed:      pgwf.Capability(tw.Next),
			taskType:      taskType,
		}
		handles = append(handles, &th)
	}

	return handles, nil
}

func extractTaskWaitFromRaw(payloadJSON json.RawMessage) (*taskWait, error) {
	var payload jobPayload
	if err := json.Unmarshal(payloadJSON, &payload); err == nil {
		if payload.TaskWait != nil {
			return payload.TaskWait, nil
		}
	}

	var legacy taskWait
	if err := json.Unmarshal(payloadJSON, &legacy); err != nil {
		return nil, err
	}
	return &legacy, nil
}

func taskTypeFromCapability(cap string) string {
	parts := strings.SplitN(cap, ":", 2)
	if len(parts) == 2 {
		return parts[1]
	}
	return cap
}

func (s *swfEngineImpl) GetWaitingTask(ctx context.Context, key swf.JobKey) (swf.TaskHandle, error) {
	job, err := pgwf.GetJob(ctx, s.pgwfDB(ctx),
		pgwf.TenantID(key.TenantId),
		pgwf.JobID(key.JobId),
		pgwf.GetJobOptions{IncludePayload: true})
	if errors.Is(err, pgwf.ErrJobNotFound) {
		return nil, swf.ErrJobNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("failed to get job: %w", err)
	}

	if job.Status != pgwf.JobStatusReady {
		return nil, swf.ErrJobNotFound
	}

	tw, err := extractTaskWaitFromRaw(job.Payload)
	if err != nil {
		return nil, err
	}
	th := &taskHandleImpl{
		jobID:         job.JobID,
		tenantId:      key.TenantId,
		payload:       job.Payload,
		inputOrdinal:  tw.InputStep,
		outputOrdinal: tw.OutputStep,
		engine:        s,
		nextNeed:      pgwf.Capability(tw.Next),
		taskType:      taskTypeFromCapability(tw.Next),
	}
	return th, nil
}

var _ swf.SWFEngine = &swfEngineImpl{}

var Builder swf.Builder = func(db *gorm.DB, strataClient *strataclient.Client, workers []swf.WorkSet, logger *slog.Logger) (swf.SWFEngine, error) {
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
	if logger == nil {
		logger = slog.Default()
	}
	f := swfEngineImpl{
		strata:         strataClient,
		db:             db,
		workers:        capMap,
		workerId:       workerId,
		udb:            underlying,
		logger:         logger,
		awaitThreshold: 5 * time.Minute,
	}

	f.refreshCapabilities()

	return &f, nil
}

func (s *swfEngineImpl) Run(ctx context.Context) {
	s.startAwaitRecycler(ctx)
	s.refreshCapabilities()
	go func() {
		b := backoff.NewExponentialBackOff()
		b.MaxInterval = time.Second * 30
		for {
			caps := s.capabilitiesSnapshot()
			lease, err := pgwf.GetWork(ctx, s.udb, pgwf.WorkerID(s.workerId), caps, nil)
			if err == nil {
				if lease != nil {
					b.Reset()
					go s.runSomething(ctx, lease)
					continue // let's try again without a backoff.
				}
				// no work right now; fall through to backoff
			}
			if err != nil {
				s.logger.Error("get work failed", "error", err)
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
	if s.isNotificationCapability(capability) {
		s.handleNotification(ctx, lease)
		return
	}
	workSet, ok := s.workSetFor(capability)
	if !ok {
		// this should never happen. we don't want to crash so we'll just let the lease expire
		s.logger.Error("no workset found for capability", "capability", capability)
		return
	}

	payload := jobPayload{}
	if raw := lease.Payload(); len(raw) > 0 {
		if err := json.Unmarshal(raw, &payload); err != nil {
			s.logger.Warn("failed to decode job payload", "jobId", lease.JobID(), "error", err)
		}
	}
	payload.RunPolicy = normalizeRunPolicy(payload.RunPolicy)

	s.resetAwaitState(lease.JobID())
	runner := runner{
		jobId:        lease.JobID(),
		tenantId:     string(lease.TenantID()),
		worker:       workSet,
		storyCounter: 1,
		engine:       s,
		lease:        lease,
		logger:       s.logger.With("jobId", lease.JobID(), "capability", capability),
		jobPolicy:    payload.RunPolicy,
		capability:   capability,
	}
	runner.DoJob(ctx, lease)
}

func (s *swfEngineImpl) handleNotification(ctx context.Context, lease *swf.Lease) {
	payload := jobPayload{}
	if raw := lease.Payload(); len(raw) > 0 {
		_ = json.Unmarshal(raw, &payload)
	}
	notify := payload.AsyncNotify
	if notify != nil {
		s.sendAwait(notify.ParentJobID, awaitSignal{Kind: awaitSignalKindWake, ChildJobID: notify.ChildJobID})
	}
	if err := lease.Complete(ctx, s.udb); err != nil {
		s.logger.Warn("notification completion failed", "jobId", lease.JobID(), "error", err)
	}
}

var _ swf.SWFEngine = &swfEngineImpl{}
