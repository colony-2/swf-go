package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/colony-2/swf-go/pkg/swf"
)

type executionLease struct {
	runtime    *Runtime
	jobKey     swf.JobKey
	leaseID    string
	workerID   string
	capability string
	payload    []byte
	duration   time.Duration
}

func (l *executionLease) LeaseID() string { return l.leaseID }

func (l *executionLease) LeaseWorkerID() string { return l.workerID }

func (l *executionLease) Job() swf.JobHandle {
	return swf.JobHandle{JobKey: l.jobKey}
}

func (l *executionLease) Capability() string { return l.capability }

func (l *executionLease) Payload() json.RawMessage {
	return jobPayloadVisibleJSON(l.payload)
}

func (l *executionLease) KeepAlive(ctx context.Context) error {
	return l.runtime.KeepAliveLeaseByID(ctx, l.jobKey, l.leaseID, l.workerID, l.duration)
}

func (l *executionLease) StopKeepAlive() {}

func (l *executionLease) Complete(ctx context.Context, req swf.CompleteExecutionRequest) error {
	return l.runtime.CompleteJobWithLeaseByID(ctx, l.jobKey, l.leaseID, l.workerID, req)
}

func (l *executionLease) Reschedule(ctx context.Context, req swf.RescheduleExecutionRequest) error {
	return l.runtime.RescheduleJobWithLeaseByID(ctx, l.jobKey, l.leaseID, l.workerID, req)
}

func (r *Runtime) KeepAliveLeaseByID(ctx context.Context, jobKey swf.JobKey, leaseID string, workerID string, leaseDuration time.Duration) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := r.validate(); err != nil {
		return err
	}
	if leaseID == "" || workerID == "" {
		return swf.ErrExecutionLeaseLost
	}
	expires := time.Now().UTC().Add(leaseDurationOrDefault(leaseDuration))
	return r.withTx(ctx, func(tx *sql.Tx) error {
		row, err := r.loadJobRowTx(ctx, tx, jobKey)
		if err != nil {
			return err
		}
		if err := validateLeaseRow(row, leaseID, workerID, time.Now().UTC()); err != nil {
			return err
		}
		_, err = tx.ExecContext(ctx, `
UPDATE swf_jobs
SET lease_expires_at_ns = ?, updated_at_ns = ?
WHERE tenant_id = ? AND job_id = ? AND lease_id = ?`,
			timeToNS(expires), timeToNS(time.Now().UTC()), jobKey.TenantId, jobKey.JobId, leaseID)
		return err
	})
}

func (r *Runtime) CompleteJobWithLeaseByID(ctx context.Context, jobKey swf.JobKey, leaseID string, workerID string, req swf.CompleteExecutionRequest) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := r.validate(); err != nil {
		return err
	}
	if leaseID == "" || workerID == "" {
		return swf.ErrExecutionLeaseLost
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
UPDATE swf_jobs
SET archived_at_ns = ?, completion_status = ?, completion_detail = ?,
	cancel_requested = CASE WHEN ? = 1 THEN 1 ELSE cancel_requested END,
	lease_id = NULL, lease_worker_id = NULL, lease_expires_at_ns = NULL,
	alternate_need = NULL, alternate_at_ns = NULL, updated_at_ns = ?
WHERE tenant_id = ? AND job_id = ?`,
			timeToNS(now), status, nullableString(req.Detail), cancelRequested, timeToNS(now), jobKey.TenantId, jobKey.JobId)
		return err
	})
}

func (r *Runtime) RescheduleJobWithLeaseByID(ctx context.Context, jobKey swf.JobKey, leaseID string, workerID string, req swf.RescheduleExecutionRequest) error {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := r.validate(); err != nil {
		return err
	}
	if leaseID == "" || workerID == "" {
		return swf.ErrExecutionLeaseLost
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
UPDATE swf_jobs
SET next_need = ?, payload = ?, wait_for = ?, available_at_ns = ?,
	lease_id = NULL, lease_worker_id = NULL, lease_expires_at_ns = NULL,
	alternate_need = ?, alternate_at_ns = ?, updated_at_ns = ?
WHERE tenant_id = ? AND job_id = ?`,
			req.NextNeed, payloadBytes, waitFor, timeToNS(availableAt), alternateNeed, alternateAt, timeToNS(now), jobKey.TenantId, jobKey.JobId)
		return err
	})
}

func (r *Runtime) validateLease(ctx context.Context, jobKey swf.JobKey, leaseID string, workerID string) error {
	row, err := r.loadJobRow(ctx, jobKey)
	if err != nil {
		return err
	}
	return validateLeaseRow(row, leaseID, workerID, time.Now().UTC())
}

func validateLeaseRow(row jobRow, leaseID string, workerID string, now time.Time) error {
	if row.archivedAtNS.Valid {
		return swf.ErrExecutionLeaseLost
	}
	if !row.leaseID.Valid || row.leaseID.String == "" || row.leaseID.String != leaseID {
		return swf.ErrExecutionLeaseLost
	}
	if workerID != "" && (!row.leaseWorkerID.Valid || row.leaseWorkerID.String != workerID) {
		return swf.ErrExecutionLeaseLost
	}
	if !row.leaseExpiresAtNS.Valid || !timeFromNS(row.leaseExpiresAtNS.Int64).After(now) {
		return swf.ErrExecutionLeaseLost
	}
	return nil
}

func classifyLeaseMutationError(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, swf.ErrJobNotFound) || errors.Is(err, swf.ErrExecutionLeaseLost) {
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
