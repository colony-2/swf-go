package sqlite

import (
	"context"
	"encoding/json"
	"errors"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/strata-go/pkg/client/core"
)

func (r *Runtime) preflightScheduleLease(ctx context.Context, lease *executionLease, metadata json.RawMessage) (bool, error) {
	occ, hasSchedule, err := jobdb.ExtractScheduleOccurrenceMetadata(metadata)
	if err != nil {
		return false, err
	}
	if !hasSchedule {
		return true, nil
	}
	chapterCount, err := r.storyChapterCount(ctx, lease.jobKey)
	if err != nil {
		return false, err
	}
	if chapterCount > 1 {
		return true, nil
	}
	row, found, err := r.loadScheduleRow(ctx, jobdb.ScheduleKey{TenantId: lease.jobKey.TenantId, ScheduleId: occ.ScheduleId})
	if err != nil {
		return false, err
	}
	if !found {
		return r.cancelScheduledLease(ctx, lease, cancellationForOccurrence(occ, "schedule_missing", "schedule missing before app start", 0, ""))
	}
	if row.state == jobdb.ScheduleStatePaused {
		return r.cancelScheduledLease(ctx, lease, cancellationForOccurrence(occ, "schedule_paused", "schedule paused before app start", row.generation, row.specHash))
	}
	if row.state == jobdb.ScheduleStateArchived {
		return r.cancelScheduledLease(ctx, lease, cancellationForOccurrence(occ, "schedule_archived", "schedule archived before app start", row.generation, row.specHash))
	}
	if row.generation != occ.Generation {
		return r.cancelScheduledLease(ctx, lease, cancellationForOccurrence(occ, "schedule_generation_mismatch", "schedule generation changed before app start", row.generation, row.specHash))
	}
	if row.specHash != occ.SpecHash {
		return r.cancelScheduledLease(ctx, lease, cancellationForOccurrence(occ, "schedule_spec_mismatch", "schedule spec changed before app start", row.generation, row.specHash))
	}
	if row.trigger.EndAt != nil && occ.ScheduledAt.After(row.trigger.EndAt.UTC()) {
		return r.cancelScheduledLease(ctx, lease, cancellationForOccurrence(occ, "schedule_ended", "schedule ended before app start", row.generation, row.specHash))
	}
	failureBits := occ.FailureHistory.Bits
	windowSize := row.failurePolicy.WindowSize
	if windowSize <= 0 {
		windowSize = occ.FailureHistory.WindowSize
	}
	if occ.PreviousJobId != "" {
		failureBits = jobdb.AppendScheduleFailureBit(failureBits, r.previousJobSucceeded(ctx, lease.jobKey.TenantId, occ.PreviousJobId), windowSize)
	}
	if jobdb.ScheduleFailurePolicyViolated(failureBits, row.failurePolicy) {
		return r.cancelScheduledLease(ctx, lease, cancellationForOccurrence(occ, "failure_policy", "schedule failure policy blocked this occurrence", row.generation, row.specHash))
	}
	nextFireAt, err := jobdb.NextScheduleFire(row.trigger, time.Now().UTC())
	if err != nil {
		return false, err
	}
	if nextFireAt != nil {
		if _, err := r.submitScheduledOccurrence(ctx, row, *nextFireAt, lease.jobKey.JobId, failureBits, false, lease.workerID); err != nil {
			return false, err
		}
	}
	return true, nil
}

func (r *Runtime) storyChapterCount(ctx context.Context, jobKey jobdb.JobKey) (int64, error) {
	st, err := r.strataClient.Story(strataContext(ctx), storyKeyForJob(jobKey))
	if err != nil {
		if errors.Is(err, core.ErrNotFound) {
			return 0, jobdb.ErrJobNotFound
		}
		return 0, err
	}
	return st.ChapterCount(), nil
}

func (r *Runtime) previousJobSucceeded(ctx context.Context, tenantID string, jobID string) bool {
	row, err := r.loadJobRow(ctx, jobdb.JobKey{TenantId: tenantID, JobId: jobID})
	if err != nil {
		return false
	}
	return row.archivedAtNS.Valid && row.completionStatus.Valid && row.completionStatus.String == "success"
}

func (r *Runtime) cancelScheduledLease(ctx context.Context, lease *executionLease, detail scheduleCancelDetail) (bool, error) {
	raw, err := json.Marshal(detail)
	if err != nil {
		return false, err
	}
	chapter := schedulePreflightCancelChapter(lease.Capability(), detail)
	if err := lease.Complete(ctx, jobdb.CompleteExecutionRequest{Status: "cancelled", Detail: string(raw), Chapter: &chapter}); err != nil {
		return false, err
	}
	return false, nil
}

func schedulePreflightCancelChapter(taskType string, detail scheduleCancelDetail) jobdb.Chapter {
	return jobdb.Chapter{
		Ordinal:   1,
		TaskType:  taskType,
		CreatedAt: time.Now().UTC(),
		Body: jobdb.JobAttemptOutcomeChapter{Outcome: jobdb.SystemErrorOutcome{Error: jobdb.SystemErrorPayload{
			Message:   detail.Message,
			Component: "jobdb.schedule_preflight",
			Code:      detail.ReasonCode,
		}}},
	}
}

type scheduleCancelDetail struct {
	Kind               string    `json:"kind"`
	Status             string    `json:"status"`
	ReasonCode         string    `json:"reasonCode"`
	Message            string    `json:"message,omitempty"`
	ScheduleId         string    `json:"scheduleId,omitempty"`
	ExpectedGeneration int64     `json:"expectedGeneration,omitempty"`
	ActualGeneration   int64     `json:"actualGeneration,omitempty"`
	ExpectedSpecHash   string    `json:"expectedSpecHash,omitempty"`
	ActualSpecHash     string    `json:"actualSpecHash,omitempty"`
	ScheduledAt        time.Time `json:"scheduledAt,omitempty"`
}

func cancellationForOccurrence(occ jobdb.ScheduleOccurrenceMetadata, reason string, message string, actualGeneration int64, actualSpecHash string) scheduleCancelDetail {
	return scheduleCancelDetail{
		Kind:               "schedule_preflight_outcome",
		Status:             "cancelled",
		ReasonCode:         reason,
		Message:            message,
		ScheduleId:         occ.ScheduleId,
		ExpectedGeneration: occ.Generation,
		ActualGeneration:   actualGeneration,
		ExpectedSpecHash:   occ.SpecHash,
		ActualSpecHash:     actualSpecHash,
		ScheduledAt:        occ.ScheduledAt,
	}
}

func scheduleReasonFromCompletionDetail(detail string) string {
	if detail == "" {
		return ""
	}
	var payload struct {
		ReasonCode string `json:"reasonCode"`
	}
	if err := json.Unmarshal([]byte(detail), &payload); err != nil {
		return ""
	}
	return payload.ReasonCode
}
