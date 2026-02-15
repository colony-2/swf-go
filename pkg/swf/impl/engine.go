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
	"github.com/colony-2/strata-go/pkg/client/core"
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
	Kind awaitSignalKind
}

type awaitState struct {
	ch         chan awaitSignal
	timer      *time.Timer
	wakeAt     time.Time
	lease      *pgwf.Lease
	capability pgwf.Capability
	ordinal    int64
	attempt    int
	recycled   bool
}

type jobPayload struct {
	RunPolicy swf.RunPolicy `json:"run_policy,omitempty"`
	TaskWait  *taskWait     `json:"task_wait,omitempty"`
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
	InputPayload  json.RawMessage
	StartedAt     *time.Time
	FinishedAt    *time.Time
	Prerequisites []swf.JobPrerequisite
}

func taskDataToChapter(jobData swf.TaskData, ordinal int64, taskType string, workerId string, chapterType string, payloadKind string, inputHash string, createdAt time.Time, meta chapterMetadata) (story.Chapter, error) {
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
	return payloadToChapter(data, artifacts, ordinal, taskType, workerId, chapterType, payloadKind, inputHash, createdAt, meta)
}

func taskDataToCreatOptions(jobData swf.TaskData, ordinal int64, taskType string, workerId string, chapterType string, payloadKind string, inputHash string, createdAt time.Time, meta chapterMetadata) (story.CreateOptions, error) {
	chap, err := taskDataToChapter(jobData, ordinal, taskType, workerId, chapterType, payloadKind, inputHash, createdAt, meta)
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
	prereqs, waitFor, err := normalizePrerequisites(jobKey, job.Prerequisites)
	if err != nil {
		return swf.JobKey{}, err
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
	co, err := taskDataToCreatOptions(taskData, 0, job.JobType, s.workerId, chapterTypeJobStart, payloadKindApp, inputHash, now, chapterMetadata{
		Attempt: 1,
		Prerequisites: prereqs,
	})
	if err != nil {
		return swf.JobKey{}, err
	}
	_, err = s.strata.CreateStory(ctx, key, co)
	if err != nil {
		return swf.JobKey{}, err
	}

	artifacts, _ := taskData.GetArtifacts()
	assignArtifactKeys(artifacts, jobKey.JobId, 0)

	// Cleanup input artifacts after successful storage
	for _, art := range artifacts {
		if cleanupErr := art.Cleanup(); cleanupErr != nil {
			s.logger.Warn("Failed to cleanup job input artifact", "artifact", art.Name(), "error", cleanupErr)
		}
	}

	return jobKey, s.startJob(ctx, jobKey, job.JobType, job.SingletonKey, job.Metadata, waitFor, jobPayload{RunPolicy: jobPolicy})
}

func (s *swfEngineImpl) startJob(ctx context.Context, jobKey swf.JobKey, jobType string, singletonKey string, metadata json.RawMessage, waitFor []pgwf.JobID, payload jobPayload) error {
	if ctx == nil {
		ctx = context.Background()
	}
	dep := pgwf.JobDependencies{
		NextNeed: pgwf.Capability(jobType),
		WaitFor:  waitFor,
	}
	tenantID := pgwf.TenantID(jobKey.TenantId)
	return pgwf.SubmitJob(ctx, s.pgwfDB(ctx), tenantID, pgwf.JobID(jobKey.JobId), dep, payload, metadata, pgwf.WorkerID(s.workerId), singletonKey, time.Time{})
}

func (s *swfEngineImpl) RestartJob(ctx context.Context, job swf.RestartJob) (swf.JobKey, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if job.LastStepToKeep < 0 {
		return swf.JobKey{}, fmt.Errorf("LastStepToKeep must be >= 0 for restart")
	}
	jobKey := swf.JobKey{
		TenantId: job.PriorJobKey.TenantId,
		JobId:    job.JobID,
	}
	if jobKey.JobId == "" {
		jobKey.JobId = ksuid.New().String()
	}
	prereqs, waitFor, err := normalizePrerequisites(jobKey, job.Prerequisites)
	if err != nil {
		return swf.JobKey{}, err
	}
	sourceJob := job.PriorJobKey.ToStoryKey()
	targetJob := jobKey.ToStoryKey()

	// Load initial chapter to recover job type, run policy, and input hash.
	chap0, err := s.strata.Chapter(ctx, sourceJob, 0)
	if err != nil {
		return swf.JobKey{}, fmt.Errorf("load source initial chapter: %w", err)
	}
	env0, err := decodeChapterEnvelope(chap0.Body())
	if err != nil {
		return swf.JobKey{}, fmt.Errorf("decode source initial chapter: %w", err)
	}
	jobType := env0.Meta.TaskType

	runPolicy := env0.Meta.RunPolicy
	jobPolicy := swf.RunPolicy{}
	if runPolicy != nil {
		jobPolicy = normalizeRunPolicy(*runPolicy)
	}

	// Validate LastStepToKeep is on a task/job boundary:
	// the next chapter (LastStepToKeep+1) must exist and be the first attempt.
	nextOrdinal := job.LastStepToKeep + 1
	nextChap, err := s.strata.Chapter(ctx, sourceJob, nextOrdinal)
	if err != nil {
		return swf.JobKey{}, fmt.Errorf("LastStepToKeep %d invalid: no chapter at ordinal %d: %w", job.LastStepToKeep, nextOrdinal, err)
	}
	nextEnv, err := decodeChapterEnvelope(nextChap.Body())
	if err != nil {
		return swf.JobKey{}, fmt.Errorf("decode source chapter %d: %w", nextOrdinal, err)
	}
	nextAttempt := nextEnv.Meta.Attempt
	if nextAttempt == 0 {
		nextAttempt = 1
	}
	if nextAttempt > 1 {
		return swf.JobKey{}, fmt.Errorf("LastStepToKeep %d cuts into retry chain: next ordinal %d is attempt %d of %s", job.LastStepToKeep, nextOrdinal, nextAttempt, nextEnv.Meta.TaskType)
	}

	createOptions := story.CreateOptions{RequestID: uuid.New().String()}
	if job.ExtraTaskOutput != nil {
		// Hash based on provided input or empty input.
		hashInput := job.ExtraTaskInput
		if hashInput == nil {
			hashInput = swf.NewTaskDataOrPanic(map[string]any{})
		}
		inputHash, err := computeInputHash(ctx, hashInput)
		if err != nil {
			return swf.JobKey{}, err
		}
		inputRef := &swf.InputReference{Ordinal: job.LastStepToKeep, Hash: inputHash}
		meta := chapterMetadata{
			Attempt:       1,
			InputRef:      inputRef,
			Prerequisites: prereqs,
		}

		// Store provided output as the next chapter after LastStepToKeep.
		createOptions, err = taskDataToCreatOptions(job.ExtraTaskOutput, job.LastStepToKeep+1, restartExtraTaskType, s.workerId, chapterTypeRestartExtra, payloadKindApp, inputHash, time.Now().UTC(), meta)
		if err != nil {
			return swf.JobKey{}, err
		}
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
	return jobKey, s.startJob(ctx, jobKey, jobType, "", nil, waitFor, jobPayload{RunPolicy: jobPolicy})
}

func normalizePrerequisites(jobKey swf.JobKey, prereqs []swf.JobPrerequisite) ([]swf.JobPrerequisite, []pgwf.JobID, error) {
	if len(prereqs) == 0 {
		return nil, nil, nil
	}
	seen := make(map[string]struct{}, len(prereqs))
	normalized := make([]swf.JobPrerequisite, 0, len(prereqs))
	waitFor := make([]pgwf.JobID, 0, len(prereqs))
	for _, p := range prereqs {
		if strings.TrimSpace(p.JobID) == "" {
			return nil, nil, fmt.Errorf("prerequisite job id is required")
		}
		if p.JobID == jobKey.JobId {
			return nil, nil, fmt.Errorf("prerequisite job id cannot reference self")
		}
		if _, ok := seen[p.JobID]; ok {
			continue
		}
		seen[p.JobID] = struct{}{}
		if p.Condition == "" {
			p.Condition = swf.JobPrereqComplete
		}
		switch p.Condition {
		case swf.JobPrereqComplete, swf.JobPrereqSuccess:
		default:
			return nil, nil, fmt.Errorf("invalid prerequisite condition %q", p.Condition)
		}
		normalized = append(normalized, p)
		waitFor = append(waitFor, pgwf.JobID(p.JobID))
	}
	return normalized, waitFor, nil
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
	deps.AvailableAt = st.wakeAt
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
	s.sendAwait(jobID, awaitSignal{Kind: awaitSignalKindRecycle})
}

// payloadToChapter builds a chapter from raw payload JSON and artifacts, bypassing TaskData.
func payloadToChapter(payload json.RawMessage, artifacts []swf.Artifact, ordinal int64, taskType string, workerId string, chapterType string, payloadKind string, inputHash string, createdAt time.Time, metaOpts chapterMetadata) (story.Chapter, error) {
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
	if metaOpts.InputPayload != nil {
		meta.Input = metaOpts.InputPayload
	}
	if metaOpts.StartedAt != nil {
		meta.StartedAt = metaOpts.StartedAt
	}
	if metaOpts.FinishedAt != nil {
		meta.FinishedAt = metaOpts.FinishedAt
	}
	if len(metaOpts.Prerequisites) > 0 {
		meta.Prerequisites = metaOpts.Prerequisites
	}

	envBytes, err := buildChapterEnvelope(meta, chapterType, payloadKind, payload)
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
	td, payloadErr := chapterToTaskData(chap, jobKey)
	if payloadErr != nil {
		return td, payloadErr
	}
	return td, nil
}

func (s *swfEngineImpl) ReplayJobRun(ctx context.Context, req swf.ReplayRunRequest) (swf.JobData, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := req.JobKey.Validate(); err != nil {
		return nil, err
	}

	key := req.JobKey.ToStoryKey()
	chap0, err := s.strata.Chapter(ctx, key, 0)
	if err != nil {
		if errors.Is(err, core.ErrNotFound) {
			return nil, swf.ErrJobNotFound
		}
		return nil, err
	}
	env0, err := decodeChapterEnvelope(chap0.Body())
	if err != nil {
		return nil, err
	}
	jobType := env0.Meta.TaskType
	if jobType == "" {
		return nil, fmt.Errorf("missing job type in initial chapter")
	}

	ws, ok := s.workSetFor(pgwf.Capability(jobType))
	if !ok {
		return nil, fmt.Errorf("job worker %s not registered", jobType)
	}

	backend := &replayRunnerBackend{engine: s}
	observer := req.Observer
	if observer == nil {
		observer = noopReplayObserver{}
	}

	r := runner{
		jobId:        pgwf.JobID(req.JobKey.JobId),
		tenantId:     req.JobKey.TenantId,
		engine:       nil,
		worker:       ws,
		storyCounter: 1,
		backend:      backend,
		lease:        replayLease{},
		logger:       s.logger.With("jobId", req.JobKey.JobId, "capability", jobType),
		jobPolicy:    normalizeRunPolicy(swf.RunPolicy{}),
		capability:   pgwf.Capability(jobType),
		workerId:     s.workerId,
		observer:     observer,
	}

	if req.JobWorker != nil {
		r.worker = &swf.WorkSet{
			JobWorker:   req.JobWorker,
			TaskWorkers: ws.TaskWorkers,
		}
	}

	output, runErr := r.DoJob(ctx)
	if runErr != nil {
		return nil, runErr
	}
	return output, nil
}

func (s *swfEngineImpl) GetArtifact(tenantId string, key swf.ArtifactKey) (swf.Artifact, error) {
	if tenantId == "" {
		return nil, fmt.Errorf("tenantId is required")
	}
	if err := key.Validate(); err != nil {
		return nil, err
	}
	storyKey := story.Key{
		AnthologyID: tenantId,
		StoryID:     key.JobId,
	}
	chap, err := s.strata.Chapter(context.Background(), storyKey, key.TaskOrdinal)
	if err != nil {
		return nil, err
	}
	for _, art := range chap.Artifacts() {
		if art != nil && art.Name() == key.Name {
			swfArt := swf.FromStrataArtifact(art)
			swf.AssignArtifactKey(swfArt, key)
			return swfArt, nil
		}
	}
	return nil, fmt.Errorf("artifact %s not found for job %s ordinal %d", key.Name, key.JobId, key.TaskOrdinal)
}

func (s *swfEngineImpl) ListJobs(ctx context.Context, req swf.ListJobsRequest) (swf.ListJobsResponse, error) {
	// Validate that TenantIds is provided - pgwf requires it
	if len(req.TenantIds) == 0 {
		return swf.ListJobsResponse{}, fmt.Errorf("tenant_ids is required for ListJobs")
	}

	patterns := buildJobTypePatterns(req.JobTypes, req.JobTasks)

	tenantIDs := make([]string, len(req.TenantIds))
	copy(tenantIDs, req.TenantIds)

	pgwfStatuses := convertSwfStatusesToPgwf(req.Statuses)
	includeArchived := shouldIncludeArchived(req.Stores, req.Statuses)

	metadataPredicates, err := swf.PgwfMetadataPredicates(req.MetadataFilter)
	if err != nil {
		return swf.ListJobsResponse{}, err
	}

	opts := pgwf.ListJobsOptions{
		TenantIDs:       tenantIDs,
		Statuses:        pgwfStatuses,
		JobTypePatterns: patterns,
		IncludeArchived: includeArchived,
		MetadataEquals:  metadataPredicates,
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
			Metadata:        job.Metadata,
		})
	}

	return swf.ListJobsResponse{
		Jobs:          jobs,
		NextPageToken: result.NextCursor,
	}, nil
}

type taskWait struct {
	InputStep  int64  `json:"in"`
	OutputStep int64  `json:"out"`
	Next       string `json:"next"`
	InputHash  string `json:"input_hash,omitempty"`
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
			metadata:      details.Metadata,
			inputOrdinal:  tw.InputStep,
			outputOrdinal: tw.OutputStep,
			engine:        s,
			nextNeed:      pgwf.Capability(tw.Next),
			taskType:      taskType,
			createdAt:     j.CreatedAt,
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
		metadata:      job.Metadata,
		inputOrdinal:  tw.InputStep,
		outputOrdinal: tw.OutputStep,
		engine:        s,
		nextNeed:      pgwf.Capability(tw.Next),
		taskType:      job.NextNeed,
		createdAt:     job.CreatedAt,
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
	leaseAdapter := newPgwfLeaseAdapter(lease, s.udb)
	backend := &defaultRunnerBackend{
		engine:     s,
		lease:      leaseAdapter,
		pgwfLease:  lease,
		capability: capability,
	}
	runner := runner{
		jobId:        lease.JobID(),
		tenantId:     string(lease.TenantID()),
		engine:       s,
		worker:       workSet,
		storyCounter: 1,
		backend:      backend,
		lease:        leaseAdapter,
		logger:       s.logger.With("jobId", lease.JobID(), "capability", capability),
		jobPolicy:    payload.RunPolicy,
		capability:   capability,
		workerId:     s.workerId,
		observer:     noopReplayObserver{},
	}
	runner.DoJob(ctx)
	s.resetAwaitState(lease.JobID())
}

var _ swf.SWFEngine = &swfEngineImpl{}
