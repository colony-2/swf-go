package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
)

type executionLease struct {
	runtime    *Runtime
	jobKey     jobdb.JobKey
	leaseID    string
	workerID   string
	capability string
	payload    []byte
	duration   time.Duration
	expiresAt  time.Time
	schemaHash string
}

func (l *executionLease) LeaseID() string { return l.leaseID }

func (l *executionLease) LeaseWorkerID() string { return l.workerID }

func (l *executionLease) LeaseExpiry() time.Time { return l.expiresAt }

func (l *executionLease) LeaseSchemaHash() string { return l.schemaHash }

func (l *executionLease) Job() jobdb.JobHandle {
	return jobdb.JobHandle{JobKey: l.jobKey}
}

func (l *executionLease) Capability() string { return l.capability }

func (l *executionLease) Payload() json.RawMessage {
	return jobPayloadVisibleJSON(l.payload)
}

func (l *executionLease) KeepAlive(ctx context.Context) error {
	expiresAt, err := l.runtime.KeepAliveLeaseByIDWithExpiry(ctx, l.jobKey, l.leaseID, l.workerID, l.duration)
	if err != nil {
		return err
	}
	l.expiresAt = expiresAt
	return nil
}

func (l *executionLease) StopKeepAlive() {}

func (l *executionLease) Complete(ctx context.Context, req jobdb.CompleteExecutionRequest) error {
	return l.runtime.CompleteJobWithLeaseByID(ctx, l.jobKey, l.leaseID, l.workerID, req)
}

func (l *executionLease) Reschedule(ctx context.Context, req jobdb.RescheduleExecutionRequest) error {
	return l.runtime.RescheduleJobWithLeaseByID(ctx, l.jobKey, l.leaseID, l.workerID, req)
}

func (l *executionLease) SubmitJob(ctx context.Context, req jobdb.SubmitJobRequest) (jobdb.JobHandle, error) {
	return l.runtime.SubmitJobWithLeaseByID(ctx, l.jobKey, l.leaseID, l.workerID, req)
}

func (l *executionLease) SubmitRestartJob(ctx context.Context, req jobdb.SubmitRestartJobRequest) (jobdb.JobHandle, error) {
	return l.runtime.SubmitRestartJobWithLeaseByID(ctx, l.jobKey, l.leaseID, l.workerID, req)
}

func (r *Runtime) KeepAliveLeaseByID(ctx context.Context, jobKey jobdb.JobKey, leaseID string, workerID string, leaseDuration time.Duration) error {
	_, err := r.KeepAliveLeaseByIDWithExpiry(ctx, jobKey, leaseID, workerID, leaseDuration)
	return err
}

func (r *Runtime) KeepAliveLeaseByIDWithExpiry(ctx context.Context, jobKey jobdb.JobKey, leaseID string, workerID string, leaseDuration time.Duration) (time.Time, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := r.validate(); err != nil {
		return time.Time{}, err
	}
	if leaseID == "" || workerID == "" {
		return time.Time{}, jobdb.ErrExecutionLeaseLost
	}
	expires := time.Now().UTC().Add(leaseDurationOrDefault(leaseDuration))
	err := r.withTx(ctx, func(tx *sql.Tx) error {
		row, err := r.loadJobRowTx(ctx, tx, jobKey)
		if err != nil {
			return err
		}
		if err := validateLeaseRow(row, leaseID, workerID, time.Now().UTC()); err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `
UPDATE jobdb_jobs
SET lease_expires_at_ns = ?, updated_at_ns = ?
WHERE tenant_id = ? AND job_id = ? AND lease_id = ?`,
			timeToNS(expires), timeToNS(time.Now().UTC()), jobKey.TenantId, jobKey.JobId, leaseID)
		return err
	})
	if err != nil {
		return time.Time{}, err
	}
	return expires, nil
}

func (r *Runtime) CompleteJobWithLeaseByID(ctx context.Context, jobKey jobdb.JobKey, leaseID string, workerID string, req jobdb.CompleteExecutionRequest) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := r.validate(); err != nil {
		return err
	}
	if leaseID == "" || workerID == "" {
		return jobdb.ErrExecutionLeaseLost
	}
	if err := r.ensureCompletionChapter(ctx, jobKey, leaseID, workerID, req); err != nil {
		return err
	}
	status := completionStatusFromRequest(req.Status)
	now := time.Now().UTC()
	return r.withTx(ctx, func(tx *sql.Tx) error {
		row, err := r.loadJobRowTx(ctx, tx, jobKey)
		if err != nil {
			return err
		}
		if err := validateLeaseRow(row, leaseID, workerID, now); err != nil {
			return err
		}
		cancelRequested := 0
		if status == "cancelled" {
			cancelRequested = 1
		}
		_, err = tx.ExecContext(ctx, `
UPDATE jobdb_jobs
SET archived_at_ns = ?, completion_status = ?, completion_detail = ?,
	cancel_requested = CASE WHEN ? = 1 THEN 1 ELSE cancel_requested END,
	lease_id = NULL, lease_worker_id = NULL, lease_expires_at_ns = NULL,
	alternate_need = NULL, alternate_at_ns = NULL, updated_at_ns = ?
WHERE tenant_id = ? AND job_id = ?`,
			timeToNS(now), status, nullableString(req.Detail), cancelRequested, timeToNS(now), jobKey.TenantId, jobKey.JobId)
		return err
	})
}

func (r *Runtime) RescheduleJobWithLeaseByID(ctx context.Context, jobKey jobdb.JobKey, leaseID string, workerID string, req jobdb.RescheduleExecutionRequest) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := r.validate(); err != nil {
		return err
	}
	if leaseID == "" || workerID == "" {
		return jobdb.ErrExecutionLeaseLost
	}
	if req.NextNeed == "" {
		return fmt.Errorf("next capability is required")
	}
	if req.AlternateAfter != nil && *req.AlternateAfter < 0 {
		return fmt.Errorf("alternate after must be non-negative")
	}
	if req.AlternateNeed == "" && req.AlternateAfter != nil && *req.AlternateAfter > 0 {
		return fmt.Errorf("alternate capability is required when after is set")
	}
	payload := req.Payload
	if len(payload) == 0 {
		payload = json.RawMessage(`{}`)
	}
	storedPayload, err := jobPayloadFromVisibleJSON(payload)
	if err != nil {
		return err
	}
	payloadBytes, err := encodeJobPayload(storedPayload)
	if err != nil {
		return err
	}
	waitFor, err := encodeWaitFor(req.WaitForJobIDs)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	availableAt := now
	if req.WaitUntil != nil {
		availableAt = req.WaitUntil.UTC()
	}
	var alternateNeed any
	var alternateAt any
	if req.AlternateNeed != "" {
		after := time.Duration(0)
		if req.AlternateAfter != nil {
			after = *req.AlternateAfter
		}
		alternateNeed = req.AlternateNeed
		alternateAt = timeToNS(now.Add(after))
	}
	return r.withTx(ctx, func(tx *sql.Tx) error {
		row, err := r.loadJobRowTx(ctx, tx, jobKey)
		if err != nil {
			return err
		}
		if err := validateLeaseRow(row, leaseID, workerID, now); err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `
UPDATE jobdb_jobs
SET next_need = ?, payload = ?, wait_for = ?, available_at_ns = ?,
	lease_id = NULL, lease_worker_id = NULL, lease_expires_at_ns = NULL,
	alternate_need = ?, alternate_at_ns = ?, updated_at_ns = ?
WHERE tenant_id = ? AND job_id = ?`,
			req.NextNeed, payloadBytes, waitFor, timeToNS(availableAt), alternateNeed, alternateAt, timeToNS(now), jobKey.TenantId, jobKey.JobId)
		return err
	})
}

func (r *Runtime) SubmitJobWithLeaseByID(ctx context.Context, parentJobKey jobdb.JobKey, leaseID string, workerID string, req jobdb.SubmitJobRequest) (jobdb.JobHandle, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := r.validate(); err != nil {
		return jobdb.JobHandle{}, err
	}
	if leaseID == "" || workerID == "" {
		return jobdb.JobHandle{}, jobdb.ErrExecutionLeaseLost
	}
	if _, err := r.validateLease(ctx, parentJobKey, leaseID, workerID); err != nil {
		return jobdb.JobHandle{}, err
	}
	req.Job.TenantId = parentJobKey.TenantId
	return r.submitJobWithParent(ctx, req, parentJobKey.JobId)
}

func (r *Runtime) SubmitRestartJobWithLeaseByID(ctx context.Context, parentJobKey jobdb.JobKey, leaseID string, workerID string, req jobdb.SubmitRestartJobRequest) (jobdb.JobHandle, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := r.validate(); err != nil {
		return jobdb.JobHandle{}, err
	}
	if leaseID == "" || workerID == "" {
		return jobdb.JobHandle{}, jobdb.ErrExecutionLeaseLost
	}
	if _, err := r.validateLease(ctx, parentJobKey, leaseID, workerID); err != nil {
		return jobdb.JobHandle{}, err
	}
	if req.Job.PriorJobKey.TenantId != "" && req.Job.PriorJobKey.TenantId != parentJobKey.TenantId {
		return jobdb.JobHandle{}, fmt.Errorf("prior job tenantId must match parent tenantId")
	}
	req.Job.PriorJobKey.TenantId = parentJobKey.TenantId
	return r.submitRestartJobWithParent(ctx, req, parentJobKey.JobId)
}

func (r *Runtime) validateLease(ctx context.Context, jobKey jobdb.JobKey, leaseID string, workerID string) (jobRow, error) {
	row, err := r.loadJobRow(ctx, jobKey)
	if err != nil {
		return jobRow{}, err
	}
	return row, validateLeaseRow(row, leaseID, workerID, time.Now().UTC())
}

func validateLeaseRow(row jobRow, leaseID string, workerID string, now time.Time) error {
	if row.archivedAtNS.Valid {
		return jobdb.ErrExecutionLeaseLost
	}
	if !row.leaseID.Valid || row.leaseID.String == "" || row.leaseID.String != leaseID {
		return jobdb.ErrExecutionLeaseLost
	}
	if workerID != "" && (!row.leaseWorkerID.Valid || row.leaseWorkerID.String != workerID) {
		return jobdb.ErrExecutionLeaseLost
	}
	if !row.leaseExpiresAtNS.Valid || !timeFromNS(row.leaseExpiresAtNS.Int64).After(now) {
		return jobdb.ErrExecutionLeaseLost
	}
	return nil
}

func classifyLeaseMutationError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, jobdb.ErrJobNotFound) || errors.Is(err, jobdb.ErrExecutionLeaseLost) {
		return err
	}
	return err
}

func nullableString(value string) any {
	if value == "" {
		return nil
	}
	return value
}
