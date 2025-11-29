package impl

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
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
	tenantId        string
	strata          *strataclient.Client
	db              *gorm.DB
	udb             *sql.DB
	workers         map[pgwf.Capability]*swf.WorkSet
	workerId        string
	runners         map[string]runner
	activeWorkLimit int
	logger          *slog.Logger
	awaitMu         sync.Mutex
	awaits          map[pgwf.JobID]*awaitState
	awaitThreshold  time.Duration
	awaitRecycler   sync.Once
}

func (s *swfEngineImpl) RegisterWorkers(job swf.JobWorker, tasks ...swf.TaskWorker) error {
	_, ok := s.workers[pgwf.Capability(job.Name())]
	if ok {
		return fmt.Errorf("worker %s already registered", job.Name())
	}

	set, err := swf.AsWorkSet(job, tasks...)
	if err != nil {
		return err
	}
	s.workers[pgwf.Capability(job.Name())] = set
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
	bytes, err := data.ToBytes()
	if err != nil {
		return nil, err
	}
	artifacts, err := jobData.GetArtifacts()
	if err != nil {
		return nil, err
	}
	return payloadToChapter(json.RawMessage(bytes), artifacts, ordinal, taskType, workerId, payloadKind, inputHash, createdAt, meta)
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

func (s *swfEngineImpl) StartJob(ctx context.Context, job swf.StartJob) (swf.JobId, error) {
	jobId := swf.JobId(ksuid.New().String())
	key := story.Key{
		AnthologyID: s.tenantId,
		StoryID:     string(jobId),
	}
	taskData := swf.TaskData(job.Data)
	inputHash, err := computeInputHash(ctx, taskData)
	if err != nil {
		return "", err
	}
	now := time.Now().UTC()
	jobPolicy := job.RunPolicy
	jobPolicy = normalizeRunPolicy(jobPolicy)
	co, err := taskDataToCreatOptions(taskData, 0, job.JobType, s.workerId, payloadKindApp, inputHash, now, chapterMetadata{
		Attempt: 1,
	})
	if err != nil {
		return "", err
	}
	_, err = s.strata.CreateStory(context.TODO(), key, co)
	if err != nil {
		return "", err
	}

	return jobId, s.startJob(jobId, job.JobType, job.SingletonKey, jobPayload{RunPolicy: jobPolicy})
}

func (s *swfEngineImpl) startJob(jobId swf.JobId, jobType string, singletonKey string, payload jobPayload) error {
	dep := pgwf.JobDependencies{
		NextNeed: pgwf.Capability(jobType),
	}
	return pgwf.SubmitJob(context.TODO(), s.udb, pgwf.JobID(jobId), dep, payload, pgwf.WorkerID(s.workerId), singletonKey, time.Time{})
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

	inputHash, err := computeInputHash(ctx, job.Data)
	if err != nil {
		return "", err
	}
	now := time.Now().UTC()
	jobPolicy := job.RunPolicy
	jobPolicy = normalizeRunPolicy(jobPolicy)
	createOptions, err := taskDataToCreatOptions(job.Data, job.LastStepToKeep+1, job.JobType, s.workerId, payloadKindApp, inputHash, now, chapterMetadata{
		Attempt: 1,
	})
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
	return jobId, s.startJob(jobId, job.JobType, job.SingletonKey, jobPayload{RunPolicy: jobPolicy})
}

func (s *swfEngineImpl) CancelJob(ctx context.Context, job swf.CancelJob) error {
	return pgwf.CancelJob(ctx, s.udb, pgwf.JobID(job.JobId), pgwf.WorkerID(s.workerId), job.Reason)
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
		chapBuilder.AddArtifact(v)
	}
	return chapBuilder, nil
}

func (s *swfEngineImpl) CheckJobStatus(ctx context.Context, jobId swf.JobId) (swf.JobStatus, error) {
	var job Job
	err := s.db.WithContext(ctx).First(&job, "job_id = ?", string(jobId)).Error
	if err == nil {
		return swf.JobStatus(job.Status), nil
	}
	if !errors.Is(err, gorm.ErrRecordNotFound) {
		return "", err
	}

	var archived archivedJob
	err = s.db.WithContext(ctx).First(&archived, "job_id = ?", string(jobId)).Error
	if err == nil {
		return swf.JobStatusCompleted, nil
	}
	if errors.Is(err, gorm.ErrRecordNotFound) {
		return "", swf.ErrJobNotFound
	}
	return "", err
}

func (s *swfEngineImpl) GetJobResult(ctx context.Context, jobId swf.JobId) (swf.TaskData, error) {
	// Ensure job is complete (archived) before returning a result.
	var archived int64
	if err := s.db.WithContext(ctx).
		Table("pgwf.jobs_archive").
		Where("job_id = ?", string(jobId)).
		Count(&archived).Error; err != nil {
		return nil, err
	}
	if archived == 0 {
		return nil, swf.ErrJobNotComplete
	}

	key := story.Key{AnthologyID: s.tenantId, StoryID: string(jobId)}
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

func (s *swfEngineImpl) jobResultIfComplete(ctx context.Context, jobId swf.JobId) (swf.TaskData, bool, error) {
	var archived int64
	if err := s.db.WithContext(ctx).
		Table("pgwf.jobs_archive").
		Where("job_id = ?", string(jobId)).
		Count(&archived).Error; err != nil {
		return nil, false, err
	}
	if archived == 0 {
		return nil, false, nil
	}
	td, err := s.GetJobResult(ctx, jobId)
	return td, err == nil, err
}

func (s *swfEngineImpl) ensureNotificationJob(ctx context.Context, notificationJobID pgwf.JobID, childJobID pgwf.JobID, parentJobID pgwf.JobID, awaitOrdinal int64) error {
	deps := pgwf.JobDependencies{
		NextNeed: notificationCapability(s.workerId),
		WaitFor:  []pgwf.JobID{childJobID},
	}
	payload := jobPayload{
		AsyncNotify: &asyncNotificationPayload{
			ParentJobID:  parentJobID,
			AwaitOrdinal: awaitOrdinal,
			ChildJobID:   childJobID,
		},
	}
	if err := pgwf.RescheduleUnheldJob(ctx, s.udb, notificationJobID, pgwf.WorkerID(s.workerId), deps, payload); err == nil {
		return nil
	} else if errors.Is(err, pgwf.ErrJobNotFound) {
		if err := pgwf.SubmitJob(ctx, s.udb, notificationJobID, deps, payload, pgwf.WorkerID(s.workerId), "", time.Time{}); err == nil || errors.Is(err, pgwf.ErrDependencyViolation) {
			return nil
		}
	}
	// If the job is leased or otherwise not ready, defer; an existing notification job will eventually fire.
	return nil
}

func (s *swfEngineImpl) ensureChildAndNotificationJobs(ctx context.Context, childJobID pgwf.JobID, notificationJobID pgwf.JobID, jobType string, runPolicy swf.RunPolicy, parentJobID swf.JobId, awaitOrdinal int64) error {
	var archived int64
	if err := s.db.WithContext(ctx).
		Table("pgwf.jobs_archive").
		Where("job_id = ?", string(childJobID)).
		Count(&archived).Error; err != nil {
		return err
	}
	if archived > 0 {
		return nil
	}

	var existing Job
	if err := s.db.WithContext(ctx).Where("job_id = ?", string(childJobID)).First(&existing).Error; err == nil {
		return s.ensureNotificationJob(ctx, notificationJobID, childJobID, pgwf.JobID(parentJobID), awaitOrdinal)
	} else if !errors.Is(err, gorm.ErrRecordNotFound) {
		return err
	}

	runPolicy = normalizeRunPolicy(runPolicy)

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

	childDeps := pgwf.JobDependencies{NextNeed: pgwf.Capability(jobType)}
	if err := pgwf.SubmitJob(ctx, tx, childJobID, childDeps, jobPayload{RunPolicy: runPolicy}, pgwf.WorkerID(s.workerId), "", time.Time{}); err != nil {
		return err
	}

	notifyDeps := pgwf.JobDependencies{
		NextNeed: notificationCapability(s.workerId),
		WaitFor:  []pgwf.JobID{childJobID},
	}
	notifyPayload := jobPayload{
		AsyncNotify: &asyncNotificationPayload{
			ParentJobID:  pgwf.JobID(parentJobID),
			AwaitOrdinal: awaitOrdinal,
			ChildJobID:   childJobID,
		},
	}
	if err := pgwf.SubmitJob(ctx, tx, notificationJobID, notifyDeps, notifyPayload, pgwf.WorkerID(s.workerId), "", time.Time{}); err != nil {
		return err
	}

	if err := tx.Commit(); err != nil {
		return err
	}
	committed = true
	return nil
}

type taskWait struct {
	InputStep  int64  `json:"in"`
	OutputStep int64  `json:"out"`
	Next       string `json:"next"`
}

func (s *swfEngineImpl) FindTasksWaitingForCapability(ctx context.Context, jobType string, taskType string) ([]swf.TaskHandle, error) {
	var jobs []Job
	err := s.db.Where(&Job{NextNeed: jobType + ":" + taskType, Status: "READY"}).Find(&jobs).Error
	if err != nil {
		return nil, err
	}

	handles := make([]swf.TaskHandle, 0, len(jobs))
	for _, j := range jobs {
		var payload jobPayload
		var tw *taskWait
		if err = json.Unmarshal(j.Payload, &payload); err == nil {
			tw = payload.TaskWait
		}
		if tw == nil {
			legacy := taskWait{}
			if err = json.Unmarshal(j.Payload, &legacy); err != nil {
				return nil, err
			}
			tw = &legacy
		}
		th := taskHandleImpl{
			job:           j,
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

var _ swf.SWFEngine = &swfEngineImpl{}

var Builder swf.Builder = func(tenantId string, db *gorm.DB, strataClient *strataclient.Client, workers []swf.WorkSet, logger *slog.Logger) (swf.SWFEngine, error) {
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
		tenantId:       tenantId,
		strata:         strataClient,
		db:             db,
		workers:        capMap,
		workerId:       workerId,
		udb:            underlying,
		logger:         logger,
		awaitThreshold: 5 * time.Minute,
	}

	return &f, nil
}

func (s *swfEngineImpl) Run(ctx context.Context) {
	s.startAwaitRecycler(ctx)
	caps := make([]pgwf.Capability, 0, len(s.workers))
	for k, v := range s.workers {
		caps = append(caps, v.Capabilities...)
		caps = append(caps, pgwf.Capability(k))
	}
	caps = append(caps, notificationCapability(s.workerId))
	go func() {
		b := backoff.NewExponentialBackOff()
		b.MaxInterval = time.Second * 30
		for {
			lease, err := pgwf.GetWork(ctx, s.udb, pgwf.WorkerID(s.workerId), caps)
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
	workSet, ok := s.workers[capability]
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
		worker:       workSet,
		storyCounter: 1,
		engine:       s,
		lease:        lease,
		logger:       s.logger.With("jobId", lease.JobID(), "capability", capability),
		jobPolicy:    payload.RunPolicy,
		capability:   capability,
	}
	runner.Run(ctx, lease)
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
