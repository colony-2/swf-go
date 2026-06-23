package toyimpl

import (
	"context"
	"encoding/json"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
)

func (r *Runtime) preflightScheduleLease(ctx context.Context, lease *runtimeLease) (bool, error) {
	r.engine.mu.Lock()
	record := r.engine.jobRecords[lease.jobKey]
	if record == nil {
		r.engine.mu.Unlock()
		return false, jobdb.ErrJobNotFound
	}
	metadata := cloneJSON(record.metadata)
	chapterCount := int64(len(r.engine.runtimeChapters[lease.jobKey]))
	r.engine.mu.Unlock()
	occ, hasSchedule, err := jobdb.ExtractScheduleOccurrenceMetadata(metadata)
	if err != nil {
		return false, err
	}
	if !hasSchedule {
		return true, nil
	}
	if chapterCount > 1 {
		return true, nil
	}
	info, err := r.GetSchedule(ctx, jobdb.ScheduleKey{TenantId: lease.jobKey.TenantId, ScheduleId: occ.ScheduleId})
	if err != nil {
		return r.cancelScheduledLease(ctx, lease, cancellationForOccurrence(occ, "schedule_missing", "schedule missing before app start", 0, ""))
	}
	switch info.State {
	case jobdb.ScheduleStatePaused:
		return r.cancelScheduledLease(ctx, lease, cancellationForOccurrence(occ, "schedule_paused", "schedule paused before app start", info.Generation, info.SpecHash))
	case jobdb.ScheduleStateArchived:
		return r.cancelScheduledLease(ctx, lease, cancellationForOccurrence(occ, "schedule_archived", "schedule archived before app start", info.Generation, info.SpecHash))
	}
	if info.Generation != occ.Generation {
		return r.cancelScheduledLease(ctx, lease, cancellationForOccurrence(occ, "schedule_generation_mismatch", "schedule generation changed before app start", info.Generation, info.SpecHash))
	}
	if info.SpecHash != occ.SpecHash {
		return r.cancelScheduledLease(ctx, lease, cancellationForOccurrence(occ, "schedule_spec_mismatch", "schedule spec changed before app start", info.Generation, info.SpecHash))
	}
	failureBits := occ.FailureHistory.Bits
	windowSize := info.FailurePolicy.WindowSize
	if windowSize <= 0 {
		windowSize = occ.FailureHistory.WindowSize
	}
	if occ.PreviousJobId != "" {
		failureBits = jobdb.AppendScheduleFailureBit(failureBits, r.previousJobSucceeded(lease.jobKey.TenantId, occ.PreviousJobId), windowSize)
	}
	if jobdb.ScheduleFailurePolicyViolated(failureBits, info.FailurePolicy) {
		return r.cancelScheduledLease(ctx, lease, cancellationForOccurrence(occ, "failure_policy", "schedule failure policy blocked this occurrence", info.Generation, info.SpecHash))
	}
	nextFireAt, err := jobdb.NextScheduleFire(info.Trigger, time.Now().UTC())
	if err != nil {
		return false, err
	}
	if nextFireAt != nil {
		if _, err := r.submitScheduledOccurrence(ctx, info, *nextFireAt, lease.jobKey.JobId, failureBits, false, ""); err != nil {
			return false, err
		}
	}
	return true, nil
}

func (r *Runtime) previousJobSucceeded(tenantID string, jobID string) bool {
	r.engine.mu.Lock()
	record := r.engine.jobRecords[jobdb.JobKey{TenantId: tenantID, JobId: jobID}]
	r.engine.mu.Unlock()
	if record == nil {
		return false
	}
	record.mu.Lock()
	defer record.mu.Unlock()
	return record.status == jobdb.JobStatusCompleted && !record.cancelled
}

func (r *Runtime) cancelScheduledLease(ctx context.Context, lease *runtimeLease, detail scheduleCancelDetail) (bool, error) {
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
