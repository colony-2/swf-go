package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	strataartifact "github.com/colony-2/strata-go/pkg/client/artifact"
	"github.com/colony-2/strata-go/pkg/client/core"
	"github.com/colony-2/strata-go/pkg/client/pagination"
	"github.com/colony-2/strata-go/pkg/client/story"
	"github.com/colony-2/swf-go/pkg/swf"
	"github.com/google/uuid"
	"github.com/segmentio/ksuid"
)

func (r *Runtime) SubmitJob(ctx context.Context, req swf.SubmitJobRequest) (swf.JobHandle, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := r.validate(); err != nil {
		return swf.JobHandle{}, err
	}
	jobID := req.Job.JobID
	if jobID == "" {
		jobID = ksuid.New().String()
	}
	jobKey := swf.JobKey{TenantId: req.Job.TenantId, JobId: jobID}
	if err := jobKey.Validate(); err != nil {
		return swf.JobHandle{}, err
	}
	sctx := strataContext(ctx)
	prereqs, waitFor, err := normalizePrerequisites(jobKey, req.Job.Prerequisites)
	if err != nil {
		return swf.JobHandle{}, err
	}
	taskData := swf.TaskData(req.Job.Data)
	inputHash, err := computeInputHash(ctx, taskData)
	if err != nil {
		return swf.JobHandle{}, err
	}
	jobPolicy := normalizeRunPolicy(req.Job.RunPolicy)
	initialChapter, err := taskDataToChapter(taskData, 0, req.Job.JobType, r.requestWorkerID(req.WorkerID), chapterTypeJobStart, payloadKindApp, inputHash, time.Now().UTC(), chapterMetadata{
		Attempt:       1,
		RunPolicy:     &jobPolicy,
		Metadata:      metadataForStartChapter(req.Job.Metadata),
		Prerequisites: prereqs,
	})
	if err != nil {
		return swf.JobHandle{}, err
	}
	if _, err := r.strataClient.CreateStory(sctx, storyKeyForJob(jobKey), story.CreateOptions{RequestID: uuid.New().String(), InitialChapter: initialChapter}); err != nil {
		if req.Job.JobID != "" && errors.Is(err, core.ErrConflict) {
			if handle, handled, reconcileErr := r.reconcileExistingSubmitJob(ctx, req, jobKey, inputHash, prereqs, waitFor, jobPolicy); handled || reconcileErr != nil {
				return handle, reconcileErr
			}
		}
		return swf.JobHandle{}, err
	}
	if artifacts, _ := taskData.GetArtifacts(); len(artifacts) > 0 {
		assignArtifactKeys(artifacts, jobKey.JobId, 0)
		cleanupArtifacts(artifacts, r.logger)
	}
	if err := r.ensureSubmittedJobRecord(ctx, jobKey, req.Job.JobType, req.Job.Metadata, waitFor, jobPayload{RunPolicy: jobPolicy}, req.WorkerID); err != nil {
		return swf.JobHandle{}, err
	}
	return swf.JobHandle{JobKey: jobKey}, nil
}

func (r *Runtime) SubmitRestartJob(ctx context.Context, req swf.SubmitRestartJobRequest) (swf.JobHandle, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := r.validate(); err != nil {
		return swf.JobHandle{}, err
	}
	job := req.Job
	if job.LastStepToKeep < 0 {
		return swf.JobHandle{}, fmt.Errorf("LastStepToKeep must be >= 0 for restart")
	}
	jobKey := swf.JobKey{TenantId: job.PriorJobKey.TenantId, JobId: job.JobID}
	if jobKey.JobId == "" {
		jobKey.JobId = ksuid.New().String()
	}
	if err := jobKey.Validate(); err != nil {
		return swf.JobHandle{}, err
	}
	sctx := strataContext(ctx)
	prereqs, waitFor, err := normalizePrerequisites(jobKey, job.Prerequisites)
	if err != nil {
		return swf.JobHandle{}, err
	}
	sourceJob := storyKeyForJob(job.PriorJobKey)
	targetJob := storyKeyForJob(jobKey)

	chap0, err := r.strataClient.Chapter(sctx, sourceJob, 0)
	if err != nil {
		return swf.JobHandle{}, fmt.Errorf("load source initial chapter: %w", err)
	}
	env0, err := decodeChapterEnvelope(chap0.Body())
	if err != nil {
		return swf.JobHandle{}, fmt.Errorf("decode source initial chapter: %w", err)
	}
	jobType := env0.Meta.TaskType
	jobPolicy := swf.RunPolicy{}
	if env0.Meta.RunPolicy != nil {
		jobPolicy = normalizeRunPolicy(*env0.Meta.RunPolicy)
	}

	nextOrdinal := job.LastStepToKeep + 1
	nextChap, err := r.strataClient.Chapter(sctx, sourceJob, nextOrdinal)
	if err != nil {
		return swf.JobHandle{}, fmt.Errorf("LastStepToKeep %d invalid: no chapter at ordinal %d: %w", job.LastStepToKeep, nextOrdinal, err)
	}
	nextEnv, err := decodeChapterEnvelope(nextChap.Body())
	if err != nil {
		return swf.JobHandle{}, fmt.Errorf("decode source chapter %d: %w", nextOrdinal, err)
	}
	nextAttempt := nextEnv.Meta.Attempt
	if nextAttempt == 0 {
		nextAttempt = 1
	}
	if nextAttempt > 1 {
		return swf.JobHandle{}, fmt.Errorf("LastStepToKeep %d cuts into retry chain: next ordinal %d is attempt %d of %s", job.LastStepToKeep, nextOrdinal, nextAttempt, nextEnv.Meta.TaskType)
	}

	extra, err := buildRestartExtraExpectation(ctx, job, prereqs)
	if err != nil {
		return swf.JobHandle{}, err
	}
	createOptions := story.CreateOptions{RequestID: ksuid.New().String()}
	if job.ExtraTaskOutput != nil {
		hashInput := job.ExtraTaskInput
		if hashInput == nil {
			hashInput = swf.NewTaskDataOrPanic(map[string]any{})
		}
		inputHash, err := computeInputHash(ctx, hashInput)
		if err != nil {
			return swf.JobHandle{}, err
		}
		inputRef := &swf.InputReference{Ordinal: job.LastStepToKeep, Hash: inputHash}
		createOptions, err = taskDataToCreateOptions(job.ExtraTaskOutput, job.LastStepToKeep+1, restartExtraTaskType, r.requestWorkerID(req.WorkerID), chapterTypeRestartExtra, payloadKindApp, inputHash, time.Now().UTC(), chapterMetadata{
			Attempt:       1,
			InputRef:      inputRef,
			Prerequisites: prereqs,
		})
		if err != nil {
			return swf.JobHandle{}, err
		}
	}

	if _, err := r.strataClient.CloneStory(sctx, sourceJob, story.CloneOptions{
		DestinationKey: targetJob,
		LastOrdinal:    job.LastStepToKeep,
		CreateOptions:  createOptions,
	}); err != nil {
		if job.JobID != "" && errors.Is(err, core.ErrConflict) {
			if handle, handled, reconcileErr := r.reconcileExistingRestartJob(ctx, req, jobKey, prereqs, waitFor, jobType, jobPolicy, extra); handled || reconcileErr != nil {
				return handle, reconcileErr
			}
		}
		return swf.JobHandle{}, err
	}
	if job.ExtraTaskOutput != nil {
		if artifacts, _ := job.ExtraTaskOutput.GetArtifacts(); len(artifacts) > 0 {
			assignArtifactKeys(artifacts, jobKey.JobId, job.LastStepToKeep+1)
			cleanupArtifacts(artifacts, r.logger)
		}
	}
	if err := r.ensureSubmittedJobRecord(ctx, jobKey, jobType, nil, waitFor, jobPayload{RunPolicy: jobPolicy}, req.WorkerID); err != nil {
		return swf.JobHandle{}, err
	}
	return swf.JobHandle{JobKey: jobKey}, nil
}

func (r *Runtime) CancelJob(ctx context.Context, req swf.CancelJobRequest) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := r.validate(); err != nil {
		return err
	}
	now := time.Now().UTC()
	result, err := r.db.ExecContext(ctx, `
UPDATE swf_jobs
SET cancel_requested = 1,
	archived_at_ns = COALESCE(archived_at_ns, ?),
	completion_status = COALESCE(completion_status, 'cancelled'),
	completion_detail = ?,
	lease_id = NULL,
	lease_worker_id = NULL,
	lease_expires_at_ns = NULL,
	updated_at_ns = ?
WHERE tenant_id = ? AND job_id = ?`,
		timeToNS(now), req.Reason, timeToNS(now), req.JobKey.TenantId, req.JobKey.JobId)
	if err != nil {
		return err
	}
	n, _ := result.RowsAffected()
	if n == 0 {
		return swf.ErrJobNotFound
	}
	return nil
}

func (r *Runtime) PollWork(ctx context.Context, req swf.PollWorkRequest) ([]swf.ExecutionLease, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := r.validate(); err != nil {
		return nil, err
	}
	if req.LeaseDuration < 0 {
		return nil, fmt.Errorf("lease duration must be >= 0")
	}
	if req.TenantId == "" {
		return nil, fmt.Errorf("tenant_id is required for PollWork")
	}
	if len(req.Capabilities) == 0 {
		return nil, nil
	}
	for {
		leases, err := r.pollOnce(ctx, req)
		if err != nil || len(leases) > 0 || req.LongPollUntil == nil || !time.Now().Before(*req.LongPollUntil) {
			return leases, err
		}
		timer := time.NewTimer(50 * time.Millisecond)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}
}

func (r *Runtime) pollOnce(ctx context.Context, req swf.PollWorkRequest) ([]swf.ExecutionLease, error) {
	metadataPredicates, err := normalizeMetadataPredicates(req.MetadataEquals)
	if err != nil {
		return nil, err
	}
	limit := req.Limit
	if limit <= 0 {
		limit = 1
	}
	capSet := make(map[string]struct{}, len(req.Capabilities))
	for _, capability := range req.Capabilities {
		if capability != "" {
			capSet[capability] = struct{}{}
		}
	}
	if len(capSet) == 0 {
		return nil, nil
	}
	var out []swf.ExecutionLease
	err = r.withTx(ctx, func(tx *sql.Tx) error {
		rows, err := tx.QueryContext(ctx, `SELECT `+jobColumns+` FROM swf_jobs WHERE archived_at_ns IS NULL ORDER BY created_at_ns ASC, job_id ASC`)
		if err != nil {
			return err
		}
		candidates := make([]jobRow, 0)
		for rows.Next() {
			row, err := scanJobRow(rows)
			if err != nil {
				_ = rows.Close()
				return err
			}
			candidates = append(candidates, row)
		}
		if err := rows.Close(); err != nil {
			return err
		}
		if err := rows.Err(); err != nil {
			return err
		}

		now := time.Now().UTC()
		workerID := r.requestWorkerID(req.WorkerID)
		for _, row := range candidates {
			if row.tenantID != req.TenantId {
				continue
			}
			if row.cancelRequested {
				continue
			}
			need, altFired := effectiveNextNeed(row, now)
			if _, ok := capSet[need]; !ok {
				continue
			}
			if row.leaseID.Valid && row.leaseID.String != "" && row.leaseExpiresAtNS.Valid && timeFromNS(row.leaseExpiresAtNS.Int64).After(now) {
				continue
			}
			waitFor, err := decodeWaitFor(row.waitForRaw)
			if err != nil {
				return err
			}
			ready, err := dependenciesReady(ctx, tx, row.tenantID, waitFor)
			if err != nil {
				return err
			}
			if !ready {
				continue
			}
			if available := timeFromNS(row.availableAtNS); !available.IsZero() && available.After(now) {
				continue
			}
			if len(metadataPredicates) > 0 {
				match, err := metadataMatches(row.metadata, metadataPredicates)
				if err != nil {
					return err
				}
				if !match {
					continue
				}
			}
			leaseID := ksuid.New().String()
			expires := now.Add(leaseDurationOrDefault(req.LeaseDuration))
			nextNeed := row.nextNeed
			altNeed := any(nil)
			altAt := any(nil)
			if altFired {
				nextNeed = need
			} else {
				if row.alternateNeed.Valid {
					altNeed = row.alternateNeed.String
				}
				if row.alternateAtNS.Valid {
					altAt = row.alternateAtNS.Int64
				}
			}
			result, err := tx.ExecContext(ctx, `
UPDATE swf_jobs
SET next_need = ?, lease_id = ?, lease_worker_id = ?, lease_expires_at_ns = ?,
	alternate_need = ?, alternate_at_ns = ?, updated_at_ns = ?
WHERE tenant_id = ? AND job_id = ?`,
				nextNeed, leaseID, workerID, timeToNS(expires), altNeed, altAt, timeToNS(now), row.tenantID, row.jobID)
			if err != nil {
				return err
			}
			n, _ := result.RowsAffected()
			if n == 0 {
				continue
			}
			out = append(out, &executionLease{
				runtime:    r,
				jobKey:     swf.JobKey{TenantId: row.tenantID, JobId: row.jobID},
				leaseID:    leaseID,
				workerID:   workerID,
				capability: nextNeed,
				payload:    cloneBytes(row.payload),
				duration:   leaseDurationOrDefault(req.LeaseDuration),
			})
			if len(out) >= limit {
				break
			}
		}
		return nil
	})
	return out, err
}

func (r *Runtime) GetJobLease(ctx context.Context, req swf.GetJobLeaseRequest) (swf.ExecutionLease, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := r.validate(); err != nil {
		return nil, err
	}
	if req.LeaseDuration < 0 {
		return nil, fmt.Errorf("lease duration must be >= 0")
	}
	capSet := make(map[string]struct{}, len(req.Capabilities))
	for _, capability := range req.Capabilities {
		if capability != "" {
			capSet[capability] = struct{}{}
		}
	}
	if len(capSet) == 0 {
		return nil, fmt.Errorf("at least one capability is required")
	}
	var lease swf.ExecutionLease
	err := r.withTx(ctx, func(tx *sql.Tx) error {
		row, err := r.loadJobRowTx(ctx, tx, req.JobKey)
		if errors.Is(err, swf.ErrJobNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		now := time.Now().UTC()
		status, err := statusFromRow(ctx, tx, row, now)
		if err != nil {
			return err
		}
		if status != swf.JobStatusReady && status != swf.JobStatusCrashConcern {
			return nil
		}
		need, altFired := effectiveNextNeed(row, now)
		if _, ok := capSet[need]; !ok {
			return nil
		}
		leaseID := ksuid.New().String()
		workerID := r.requestWorkerID(req.WorkerID)
		expires := now.Add(leaseDurationOrDefault(req.LeaseDuration))
		nextNeed := row.nextNeed
		altNeed := any(nil)
		altAt := any(nil)
		if altFired {
			nextNeed = need
		} else {
			if row.alternateNeed.Valid {
				altNeed = row.alternateNeed.String
			}
			if row.alternateAtNS.Valid {
				altAt = row.alternateAtNS.Int64
			}
		}
		_, err = tx.ExecContext(ctx, `
UPDATE swf_jobs
SET next_need = ?, lease_id = ?, lease_worker_id = ?, lease_expires_at_ns = ?,
	alternate_need = ?, alternate_at_ns = ?, updated_at_ns = ?
WHERE tenant_id = ? AND job_id = ?`,
			nextNeed, leaseID, workerID, timeToNS(expires), altNeed, altAt, timeToNS(now), row.tenantID, row.jobID)
		if err != nil {
			return err
		}
		lease = &executionLease{
			runtime:    r,
			jobKey:     req.JobKey,
			leaseID:    leaseID,
			workerID:   workerID,
			capability: nextNeed,
			payload:    cloneBytes(row.payload),
			duration:   leaseDurationOrDefault(req.LeaseDuration),
		}
		return nil
	})
	return lease, err
}

type jobInfoTaskData struct {
	taskData swf.TaskData
	err      error
}

func (d *jobInfoTaskData) GetData() (swf.Data, error) {
	if d.taskData == nil {
		return nil, d.err
	}
	data, err := d.taskData.GetData()
	if err != nil {
		return data, err
	}
	return data, d.err
}

func (d *jobInfoTaskData) GetDataOrPanic() swf.Data {
	data, err := d.GetData()
	if err != nil {
		panic(err)
	}
	return data
}

func (d *jobInfoTaskData) GetArtifacts() ([]swf.Artifact, error) {
	if d.taskData == nil {
		return nil, d.err
	}
	return d.taskData.GetArtifacts()
}

func (d *jobInfoTaskData) TaskDataResult() (swf.TaskData, error) {
	return d.taskData, d.err
}

func (r *Runtime) GetJob(ctx context.Context, jobKey swf.JobKey) (swf.JobInfo, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := r.validate(); err != nil {
		return swf.JobInfo{}, err
	}
	row, err := r.loadJobRow(ctx, jobKey)
	if err != nil {
		return swf.JobInfo{}, err
	}
	status, err := statusFromRow(ctx, r.db, row, time.Now().UTC())
	if err != nil {
		return swf.JobInfo{}, err
	}
	job := swf.JobInfo{
		Status: status,
		Data:   &jobInfoTaskData{err: swf.ErrJobNotComplete},
	}
	if row.archivedAtNS.Valid {
		st, err := r.strataClient.Story(strataContext(ctx), storyKeyForJob(jobKey))
		if err != nil {
			return swf.JobInfo{}, err
		}
		chap, err := st.GetLastChapter(strataContext(ctx))
		if err != nil {
			return swf.JobInfo{}, err
		}
		td, payloadErr := chapterToTaskData(chap, jobKey)
		job.Data = &jobInfoTaskData{taskData: td, err: payloadErr}
	}
	return job, nil
}

func (r *Runtime) ListJobs(ctx context.Context, req swf.ListJobsRequest) (swf.ListJobsResponse, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := r.validate(); err != nil {
		return swf.ListJobsResponse{}, err
	}
	if len(req.TenantIds) == 0 {
		return swf.ListJobsResponse{}, fmt.Errorf("tenant_ids is required for ListJobs")
	}
	pageSize := req.PageSize
	if pageSize <= 0 {
		pageSize = swf.DefaultListJobsPageSize
	} else if pageSize > swf.MaxListJobsPageSize {
		pageSize = swf.MaxListJobsPageSize
	}
	rawPredicates, err := swf.MetadataPredicates(req.MetadataFilter)
	if err != nil {
		return swf.ListJobsResponse{}, err
	}
	metadataPredicates, err := normalizeMetadataPredicates(rawPredicates)
	if err != nil {
		return swf.ListJobsResponse{}, err
	}
	var cursorTime time.Time
	var cursorJob string
	hasCursor := false
	if req.PageToken != "" {
		createdAt, jobKey, err := swf.DecodeListJobsPageToken(req.PageToken)
		if err != nil {
			return swf.ListJobsResponse{}, err
		}
		cursorTime = createdAt
		cursorJob = jobKey.String()
		hasCursor = true
	}
	rows, err := r.db.QueryContext(ctx, `SELECT `+jobColumns+` FROM swf_jobs ORDER BY created_at_ns DESC, job_id DESC`)
	if err != nil {
		return swf.ListJobsResponse{}, err
	}
	all := make([]jobRow, 0)
	for rows.Next() {
		row, err := scanJobRow(rows)
		if err != nil {
			_ = rows.Close()
			return swf.ListJobsResponse{}, err
		}
		all = append(all, row)
	}
	if err := rows.Close(); err != nil {
		return swf.ListJobsResponse{}, err
	}
	if err := rows.Err(); err != nil {
		return swf.ListJobsResponse{}, err
	}

	tenantAllowed := stringSet(req.TenantIds)
	statusAllowed := statusSet(req.Statuses)
	jobKeyAllowed := jobKeySet(req.JobKeys)
	jobTypeAllowed := stringSet(req.JobTypes)
	includeActive, includeArchive, err := listStoreSelection(req)
	if err != nil {
		return swf.ListJobsResponse{}, err
	}

	now := time.Now().UTC()
	out := make([]swf.JobSummary, 0)
	for _, row := range all {
		key := swf.JobKey{TenantId: row.tenantID, JobId: row.jobID}
		if len(tenantAllowed) > 0 && !tenantAllowed[row.tenantID] {
			continue
		}
		status, err := statusFromRow(ctx, r.db, row, now)
		if err != nil {
			return swf.ListJobsResponse{}, err
		}
		archived := row.archivedAtNS.Valid
		if archived && !includeArchive {
			continue
		}
		if !archived && !includeActive {
			continue
		}
		if len(statusAllowed) > 0 && !statusAllowed[status] {
			continue
		}
		if len(jobKeyAllowed) > 0 && !jobKeyAllowed[key] {
			continue
		}
		if len(jobTypeAllowed) > 0 && !jobTypeAllowed[row.jobType] {
			continue
		}
		if len(req.JobTasks) > 0 && !jobTaskMatches(row.nextNeed, req.JobTasks) {
			continue
		}
		createdAt := timeFromNS(row.createdAtNS)
		if req.CreatedAfter != nil && createdAt.Before(*req.CreatedAfter) {
			continue
		}
		if req.CreatedBefore != nil && createdAt.After(*req.CreatedBefore) {
			continue
		}
		if len(metadataPredicates) > 0 {
			match, err := metadataMatches(row.metadata, metadataPredicates)
			if err != nil {
				return swf.ListJobsResponse{}, err
			}
			if !match {
				continue
			}
		}
		if hasCursor {
			if createdAt.After(cursorTime) {
				continue
			}
			if createdAt.Equal(cursorTime) && key.String() >= cursorJob {
				continue
			}
		}
		waitFor, err := decodeWaitFor(row.waitForRaw)
		if err != nil {
			return swf.ListJobsResponse{}, err
		}
		nextNeed, _ := effectiveNextNeed(row, now)
		summary := swf.JobSummary{
			JobKey:          key,
			Status:          status,
			JobType:         row.jobType,
			NextNeed:        cloneString(nextNeed),
			WaitFor:         waitFor,
			AvailableAt:     timeFromNS(row.availableAtNS),
			LeaseExpiresAt:  nullTimeFromNS(row.leaseExpiresAtNS),
			CancelRequested: row.cancelRequested,
			CreatedAt:       createdAt,
			ArchivedAt:      nullTimeFromNS(row.archivedAtNS),
			Payload:         jobPayloadVisibleJSON(row.payload),
			Metadata:        cloneJSON(row.metadata),
		}
		if tw, waitErr := extractTaskWaitFromRaw(row.payload); waitErr == nil && tw != nil {
			summary.TaskWaitInput = &tw.InputStep
			summary.TaskWaitOutput = &tw.OutputStep
			summary.TaskWaitInputHash = cloneString(tw.InputHash)
			summary.TaskWaitNext = cloneString(tw.Next)
		}
		out = append(out, summary)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].CreatedAt.Equal(out[j].CreatedAt) {
			return out[i].JobKey.String() > out[j].JobKey.String()
		}
		return out[i].CreatedAt.After(out[j].CreatedAt)
	})
	nextToken := ""
	if len(out) > pageSize {
		last := out[pageSize-1]
		if tok, err := swf.EncodeListJobsPageToken(last.CreatedAt, last.JobKey); err == nil {
			nextToken = tok
		}
		out = out[:pageSize]
	}
	return swf.ListJobsResponse{Jobs: out, NextPageToken: nextToken}, nil
}

func (r *Runtime) GetChapter(ctx context.Context, ref swf.ChapterRef) (swf.StoredChapter, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := r.validate(); err != nil {
		return swf.StoredChapter{}, err
	}
	chapter, err := r.strataClient.Chapter(strataContext(ctx), storyKeyForJob(ref.JobKey), ref.Ordinal)
	if err != nil {
		if errors.Is(err, core.ErrNotFound) {
			return swf.StoredChapter{}, swf.ErrChapterNotFound
		}
		return swf.StoredChapter{}, err
	}
	return storedChapterFromStoryChapter(chapter)
}

func (r *Runtime) ListChapters(ctx context.Context, req swf.ListChaptersRequest) ([]swf.StoredChapter, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := r.validate(); err != nil {
		return nil, err
	}
	if req.StartOrdinal < 0 {
		return nil, fmt.Errorf("start ordinal must be >= 0")
	}
	st, err := r.strataClient.Story(strataContext(ctx), storyKeyForJob(req.JobKey))
	if err != nil {
		if errors.Is(err, core.ErrNotFound) {
			return nil, swf.ErrJobNotFound
		}
		return nil, err
	}
	iter, err := st.Chapters(strataContext(ctx), story.ChaptersOptions{PageSize: 100, Direction: story.DirectionForward})
	if err != nil {
		return nil, err
	}
	out := make([]swf.StoredChapter, 0)
	for iter.HasNext() {
		chapter, err := iter.Next(strataContext(ctx))
		if errors.Is(err, pagination.ErrNoMoreItems) {
			break
		}
		if err != nil {
			return nil, err
		}
		stored, err := storedChapterFromStoryChapter(chapter)
		if err != nil {
			return nil, err
		}
		if stored.Ordinal < req.StartOrdinal {
			continue
		}
		if req.EndOrdinal != nil && stored.Ordinal > *req.EndOrdinal {
			break
		}
		out = append(out, stored)
	}
	return out, nil
}

func (r *Runtime) PutChapter(ctx context.Context, req swf.PutChapterRequest) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := r.validate(); err != nil {
		return err
	}
	if req.LeaseID == "" {
		return fmt.Errorf("lease id is required for PutChapter")
	}
	if req.Ref.Ordinal < 0 {
		return fmt.Errorf("chapter ordinal must be >= 0")
	}
	if req.Chapter.Ordinal != req.Ref.Ordinal {
		return fmt.Errorf("chapter ordinal %d does not match target ordinal %d", req.Chapter.Ordinal, req.Ref.Ordinal)
	}
	if err := r.validateLease(ctx, req.Ref.JobKey, req.LeaseID, ""); err != nil {
		return err
	}
	if err := r.ensureNextVisibleChapterOrdinal(ctx, req.Ref.JobKey, req.Ref.Ordinal); err != nil {
		return err
	}
	chapter, attached, err := r.prepareChapterWrite(ctx, req)
	if err != nil {
		return err
	}
	body, err := encodeStoredChapter(chapter)
	if err != nil {
		return err
	}
	builder := story.NewChapter().WithOrdinal(req.Ref.Ordinal).WithBytes(body)
	for _, art := range attached {
		builder.AddArtifact(art)
	}
	err = r.strataClient.SaveChapter(strataContext(ctx), storyKeyForJob(req.Ref.JobKey), builder)
	if err != nil {
		if errors.Is(err, core.ErrConflict) {
			return fmt.Errorf("%w: chapter ordinal %d already exists or is not appendable", swf.ErrConflict, req.Ref.Ordinal)
		}
		return err
	}
	return nil
}

func (r *Runtime) OpenArtifact(ctx context.Context, ref swf.ArtifactRef) (swf.ArtifactReader, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := r.validate(); err != nil {
		return nil, err
	}
	chapter, err := r.strataClient.Chapter(strataContext(ctx), storyKeyForJob(ref.JobKey), ref.Ordinal)
	if err != nil {
		if errors.Is(err, core.ErrNotFound) {
			return nil, swf.ErrChapterNotFound
		}
		return nil, err
	}
	var descriptor *strataartifact.Descriptor
	for _, existing := range chapter.Artifacts() {
		if existing == nil || existing.Name() != ref.Name {
			continue
		}
		digest, _ := existing.Sha256(ctx)
		if ref.Digest != "" && digest != "" && ref.Digest != digest {
			continue
		}
		descriptor = &strataartifact.Descriptor{
			Name:        existing.Name(),
			ContentType: existing.ContentType(),
			SizeBytes:   existing.SizeBytes(),
			Sha256:      digest,
		}
		break
	}
	if descriptor == nil {
		return nil, fmt.Errorf("artifact %s not found for job %s ordinal %d", ref.Name, ref.JobKey.JobId, ref.Ordinal)
	}
	art := strataartifact.FromRemote(*descriptor, strataartifact.Locator{
		AnthologyID: ref.JobKey.TenantId,
		StoryID:     ref.JobKey.JobId,
		Ordinal:     ref.Ordinal,
		Name:        descriptor.Name,
	}, r.strataClient.Core())
	return artifactReader{art: fromStrataArtifact(art)}, nil
}

func (r *Runtime) CompleteTaskIfWaiting(ctx context.Context, req swf.CompleteTaskIfWaitingRequest) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := r.validate(); err != nil {
		return err
	}
	jobKey := req.JobKey
	row, err := r.loadJobRow(ctx, jobKey)
	if err != nil {
		return err
	}
	tw, err := extractTaskWaitFromRaw(row.payload)
	if err != nil {
		return err
	}
	if tw == nil {
		return fmt.Errorf("%w: job is not waiting on an external task", swf.ErrConflict)
	}
	currentCapability := row.nextNeed
	if req.Capability != "" && currentCapability != req.Capability {
		return fmt.Errorf("%w: waiting capability %q does not match requested capability %q", swf.ErrConflict, currentCapability, req.Capability)
	}
	if req.ResumeNeed != "" && tw.Next != req.ResumeNeed {
		return fmt.Errorf("%w: resume need %q does not match requested resume need %q", swf.ErrConflict, tw.Next, req.ResumeNeed)
	}
	if req.InputOrdinal != 0 && tw.InputStep != req.InputOrdinal {
		return fmt.Errorf("%w: waiting input ordinal %d does not match requested input ordinal %d", swf.ErrConflict, tw.InputStep, req.InputOrdinal)
	}
	if tw.OutputStep != req.OutputOrdinal {
		return fmt.Errorf("%w: waiting output ordinal %d does not match requested output ordinal %d", swf.ErrConflict, tw.OutputStep, req.OutputOrdinal)
	}
	if req.InputHash != "" && tw.InputHash != req.InputHash {
		return fmt.Errorf("%w: waiting input hash does not match requested input hash", swf.ErrConflict)
	}

	var inputChapter story.Chapter
	if tw.InputStep > 0 {
		inputChapter, err = r.strataClient.Chapter(strataContext(ctx), storyKeyForJob(jobKey), tw.InputStep)
		if err != nil {
			return fmt.Errorf("failed to load input chapter: %w", err)
		}
	}
	payload, err := decodeJobPayload(row.payload)
	if err != nil {
		return err
	}
	meta := chapterMetadata{}
	if inputChapter != nil {
		if env, decErr := decodeChapterEnvelope(inputChapter.Body()); decErr == nil {
			meta.Attempt = env.Meta.Attempt
			meta.MaxAttempts = env.Meta.MaxAttempts
			meta.NextAttemptAt = env.Meta.NextAttemptAt
			meta.BackoffMillis = env.Meta.BackoffMillis
			meta.Retryable = env.Meta.Retryable
			meta.InputRef = env.Meta.InputRef
		}
	}
	if payload.RunPolicy.Retry.MaximumAttempts > 0 {
		meta.RunPolicy = &payload.RunPolicy
	}
	taskType := taskTypeFromCapability(currentCapability)
	if req.Capability != "" {
		taskType = taskTypeFromCapability(req.Capability)
	}
	if taskType == "" || taskType == currentCapability || (req.Capability != "" && taskType == req.Capability) {
		return fmt.Errorf("task type not found in capability")
	}
	chapter, err := taskDataToChapter(req.Data, tw.OutputStep, taskType, r.workerID, chapterTypeTaskAttemptOutcome, payloadKindApp, tw.InputHash, time.Now().UTC(), meta)
	if err != nil {
		return err
	}
	if err := r.ensureNextVisibleChapterOrdinal(ctx, jobKey, tw.OutputStep); err != nil {
		return err
	}
	err = r.strataClient.SaveChapter(strataContext(ctx), storyKeyForJob(jobKey), chapter)
	if err != nil {
		if errors.Is(err, core.ErrConflict) {
			return fmt.Errorf("%w: output chapter %d already exists or is not appendable", swf.ErrConflict, tw.OutputStep)
		}
		return err
	}
	artifacts, _ := req.Data.GetArtifacts()
	assignArtifactKeys(artifacts, jobKey.JobId, tw.OutputStep)
	resumeNeed := tw.Next
	if req.ResumeNeed != "" {
		resumeNeed = req.ResumeNeed
	}
	resumePayload, err := encodeJobPayload(jobPayload{RunPolicy: payload.RunPolicy})
	if err != nil {
		return err
	}
	waitFor, err := encodeWaitFor(nil)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	result, err := r.db.ExecContext(ctx, `
UPDATE swf_jobs
SET next_need = ?, payload = ?, wait_for = ?, available_at_ns = ?,
	lease_id = NULL, lease_worker_id = NULL, lease_expires_at_ns = NULL,
	alternate_need = NULL, alternate_at_ns = NULL, updated_at_ns = ?
WHERE tenant_id = ? AND job_id = ? AND archived_at_ns IS NULL AND next_need = ?`,
		resumeNeed, resumePayload, waitFor, timeToNS(now), timeToNS(now), jobKey.TenantId, jobKey.JobId, currentCapability)
	if err != nil {
		return err
	}
	n, _ := result.RowsAffected()
	if n == 0 {
		return fmt.Errorf("%w: job is no longer in a commit-if-waiting state", swf.ErrConflict)
	}
	return nil
}

func (r *Runtime) ensureNextVisibleChapterOrdinal(ctx context.Context, jobKey swf.JobKey, ordinal int64) error {
	st, err := r.strataClient.Story(strataContext(ctx), storyKeyForJob(jobKey))
	if err != nil {
		if errors.Is(err, core.ErrNotFound) {
			return swf.ErrJobNotFound
		}
		return err
	}
	last, err := st.GetLastChapter(strataContext(ctx))
	if err != nil {
		if errors.Is(err, core.ErrNotFound) {
			if ordinal == 0 {
				return nil
			}
			return fmt.Errorf("%w: chapter ordinal %d is not appendable; expected 0", swf.ErrConflict, ordinal)
		}
		return err
	}
	lastOrdinal := last.Ordinal()
	switch {
	case ordinal <= lastOrdinal:
		return fmt.Errorf("%w: chapter ordinal %d already exists", swf.ErrConflict, ordinal)
	case ordinal != lastOrdinal+1:
		return fmt.Errorf("%w: chapter ordinal %d is not appendable; expected %d", swf.ErrConflict, ordinal, lastOrdinal+1)
	default:
		return nil
	}
}

func (r *Runtime) prepareChapterWrite(ctx context.Context, req swf.PutChapterRequest) (swf.StoredChapter, []strataartifact.Artifact, error) {
	chapter := req.Chapter
	if len(req.ArtifactUploads) == 0 {
		if len(chapter.Artifacts) > 0 {
			return swf.StoredChapter{}, nil, fmt.Errorf("put chapter with artifact descriptors but no artifact uploads")
		}
		return chapter, nil, nil
	}
	stored := make([]swf.StoredArtifact, 0, len(req.ArtifactUploads))
	attached := make([]strataartifact.Artifact, 0, len(req.ArtifactUploads))
	for _, item := range req.ArtifactUploads {
		if item.Open == nil {
			return swf.StoredChapter{}, nil, fmt.Errorf("artifact %q is missing opener", item.Name)
		}
		reader, err := item.Open()
		if err != nil {
			return swf.StoredChapter{}, nil, err
		}
		data, err := io.ReadAll(reader)
		_ = reader.Close()
		if err != nil {
			return swf.StoredChapter{}, nil, err
		}
		art := swf.NewArtifactFromBytes(item.Name, data)
		digest, err := art.Sha256(ctx)
		if err != nil {
			return swf.StoredChapter{}, nil, err
		}
		stored = append(stored, swf.StoredArtifact{Name: item.Name, Digest: digest, Size: int64(len(data))})
		attached = append(attached, toStrataArtifact(art))
	}
	if err := validateChapterArtifactDescriptors(chapter.Artifacts, stored); err != nil {
		return swf.StoredChapter{}, nil, err
	}
	chapter.Artifacts = stored
	return chapter, attached, nil
}

type artifactReader struct {
	art swf.Artifact
}

func (a artifactReader) Open() (io.ReadCloser, error) { return a.art.Open() }
func (a artifactReader) Size() int64                  { return a.art.Size() }
func (a artifactReader) Name() string                 { return a.art.Name() }

func stringSet(values []string) map[string]bool {
	if len(values) == 0 {
		return nil
	}
	out := make(map[string]bool, len(values))
	for _, value := range values {
		out[value] = true
	}
	return out
}

func statusSet(values []swf.JobStatus) map[swf.JobStatus]bool {
	if len(values) == 0 {
		return nil
	}
	out := make(map[swf.JobStatus]bool, len(values))
	for _, value := range values {
		out[value] = true
	}
	return out
}

func jobKeySet(values []swf.JobKey) map[swf.JobKey]bool {
	if len(values) == 0 {
		return nil
	}
	out := make(map[swf.JobKey]bool, len(values))
	for _, value := range values {
		out[value] = true
	}
	return out
}

func listStoreSelection(req swf.ListJobsRequest) (includeActive bool, includeArchive bool, err error) {
	if len(req.Statuses) > 0 {
		for _, status := range req.Statuses {
			switch status {
			case swf.JobStatusCompleted, swf.JobStatusCancelled:
				includeArchive = true
				if status == swf.JobStatusCancelled {
					includeActive = true
				}
			case swf.JobStatusReady, swf.JobStatusExpired, swf.JobStatusPendingJobs, swf.JobStatusAwaitingFuture, swf.JobStatusActive, swf.JobStatusCrashConcern:
				includeActive = true
			default:
				return false, false, fmt.Errorf("unknown status %q", status)
			}
		}
		return includeActive, includeArchive, nil
	}
	if len(req.Stores) == 0 {
		return true, true, nil
	}
	for _, store := range req.Stores {
		switch store {
		case swf.JobStoreActive:
			includeActive = true
		case swf.JobStoreArchived:
			includeArchive = true
		default:
			return false, false, fmt.Errorf("unknown store %q", store)
		}
	}
	return includeActive, includeArchive, nil
}

func jobTaskMatches(nextNeed string, tasks []swf.JobTaskFilter) bool {
	if len(tasks) == 0 {
		return true
	}
	for _, task := range tasks {
		if task.JobType == "" || task.TaskType == "" {
			continue
		}
		if nextNeed == workerCapability(task.JobType, task.TaskType) {
			return true
		}
	}
	return false
}

func buildRestartExtraExpectation(ctx context.Context, job swf.SubmitRestartJob, prereqs []swf.JobPrerequisite) (restartExtraExpectation, error) {
	if job.ExtraTaskOutput == nil {
		return restartExtraExpectation{}, nil
	}
	hashInput := job.ExtraTaskInput
	if hashInput == nil {
		hashInput = swf.NewTaskDataOrPanic(map[string]any{})
	}
	inputHash, err := computeInputHash(ctx, hashInput)
	if err != nil {
		return restartExtraExpectation{}, err
	}
	payload, err := job.ExtraTaskOutput.GetData()
	if err != nil {
		return restartExtraExpectation{}, err
	}
	artifacts, err := taskDataArtifacts(ctx, job.ExtraTaskOutput)
	if err != nil {
		return restartExtraExpectation{}, err
	}
	return restartExtraExpectation{
		Present:       true,
		InputHash:     inputHash,
		InputRef:      &swf.InputReference{Ordinal: job.LastStepToKeep, Hash: inputHash},
		Payload:       append(json.RawMessage(nil), payload...),
		Artifacts:     artifacts,
		Prerequisites: normalizePrereqSlice(prereqs),
	}, nil
}

type restartExtraExpectation struct {
	Present       bool
	InputHash     string
	InputRef      *swf.InputReference
	Payload       json.RawMessage
	Artifacts     []artifactFingerprint
	Prerequisites []swf.JobPrerequisite
}

func taskDataArtifacts(ctx context.Context, data swf.TaskData) ([]artifactFingerprint, error) {
	if data == nil {
		return nil, nil
	}
	artifacts, err := data.GetArtifacts()
	if err != nil {
		return nil, err
	}
	out := make([]artifactFingerprint, 0, len(artifacts))
	for _, art := range artifacts {
		if art == nil {
			continue
		}
		digest, err := art.Sha256(ctx)
		if err != nil {
			return nil, err
		}
		out = append(out, artifactFingerprint{Name: art.Name(), Digest: digest, Size: art.Size()})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

func containsNotFound(err error) bool {
	return errors.Is(err, core.ErrNotFound) || strings.Contains(strings.ToLower(err.Error()), "not found")
}
