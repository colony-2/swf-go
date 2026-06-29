package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/colony-2/jobdb/pkg/internal/runtimecodec"
	"github.com/colony-2/jobdb/pkg/jobdb"
)

const jobColumns = `
tenant_id, job_id, job_type, next_need, payload, metadata, parent_job_id, wait_for,
available_at_ns, created_at_ns, updated_at_ns, archived_at_ns,
cancel_requested, completion_status, completion_detail,
lease_id, lease_worker_id, lease_expires_at_ns, alternate_need, alternate_at_ns
`

type jobRow struct {
	tenantID         string
	jobID            string
	jobType          string
	nextNeed         string
	payload          []byte
	metadata         json.RawMessage
	parentJobID      sql.NullString
	waitForRaw       []byte
	availableAtNS    int64
	createdAtNS      int64
	updatedAtNS      int64
	archivedAtNS     sql.NullInt64
	cancelRequested  bool
	completionStatus sql.NullString
	completionDetail sql.NullString
	leaseID          sql.NullString
	leaseWorkerID    sql.NullString
	leaseExpiresAtNS sql.NullInt64
	alternateNeed    sql.NullString
	alternateAtNS    sql.NullInt64
}

func scanJobRow(scanner interface{ Scan(dest ...any) error }) (jobRow, error) {
	var row jobRow
	var cancelRequested int
	var payload []byte
	var metadata []byte
	var waitFor []byte
	if err := scanner.Scan(
		&row.tenantID,
		&row.jobID,
		&row.jobType,
		&row.nextNeed,
		&payload,
		&metadata,
		&row.parentJobID,
		&waitFor,
		&row.availableAtNS,
		&row.createdAtNS,
		&row.updatedAtNS,
		&row.archivedAtNS,
		&cancelRequested,
		&row.completionStatus,
		&row.completionDetail,
		&row.leaseID,
		&row.leaseWorkerID,
		&row.leaseExpiresAtNS,
		&row.alternateNeed,
		&row.alternateAtNS,
	); err != nil {
		return jobRow{}, err
	}
	row.payload = cloneBytes(payload)
	row.metadata = append(json.RawMessage(nil), metadata...)
	row.waitForRaw = cloneBytes(waitFor)
	row.cancelRequested = cancelRequested != 0
	return row, nil
}

func (r *Runtime) loadJobRow(ctx context.Context, jobKey jobdb.JobKey) (jobRow, error) {
	row, err := scanJobRow(r.db.QueryRowContext(ctx, `SELECT `+jobColumns+` FROM jobdb_jobs WHERE tenant_id = ? AND job_id = ?`, jobKey.TenantId, jobKey.JobId))
	if err == sql.ErrNoRows {
		return jobRow{}, jobdb.ErrJobNotFound
	}
	return row, err
}

func (r *Runtime) loadJobRowTx(ctx context.Context, tx *sql.Tx, jobKey jobdb.JobKey) (jobRow, error) {
	row, err := scanJobRow(tx.QueryRowContext(ctx, `SELECT `+jobColumns+` FROM jobdb_jobs WHERE tenant_id = ? AND job_id = ?`, jobKey.TenantId, jobKey.JobId))
	if err == sql.ErrNoRows {
		return jobRow{}, jobdb.ErrJobNotFound
	}
	return row, err
}

func (r *Runtime) insertJobRecord(ctx context.Context, jobKey jobdb.JobKey, jobType string, metadata json.RawMessage, waitFor []string, payload jobPayload, workerID string, availableAt *time.Time) error {
	payloadBytes, err := encodeJobPayload(payload)
	if err != nil {
		return err
	}
	waitBytes, err := encodeWaitFor(waitFor)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	leaseableAt := now
	if availableAt != nil {
		leaseableAt = availableAt.UTC()
	}
	_, err = r.db.ExecContext(ctx, `
INSERT INTO jobdb_jobs (
	tenant_id, job_id, job_type, next_need, payload, metadata, parent_job_id, wait_for,
	available_at_ns, created_at_ns, updated_at_ns
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		jobKey.TenantId,
		jobKey.JobId,
		jobType,
		jobType,
		payloadBytes,
		cloneJSON(metadata),
		parentJobIDFromMetadata(metadata),
		waitBytes,
		timeToNS(leaseableAt),
		timeToNS(now),
		timeToNS(now),
	)
	if err != nil {
		return fmt.Errorf("sqlite runtime: insert job: %w", err)
	}
	return nil
}

func parentJobIDFromMetadata(metadata json.RawMessage) any {
	parentJobID, ok, err := jobdb.ExtractParentJobID(metadata)
	if err != nil || !ok {
		return nil
	}
	return parentJobID
}

func (r *Runtime) ensureSubmittedJobRecord(ctx context.Context, jobKey jobdb.JobKey, jobType string, metadata json.RawMessage, waitFor []string, payload jobPayload, workerID string, availableAt *time.Time) error {
	if err := r.insertJobRecord(ctx, jobKey, jobType, metadata, waitFor, payload, workerID, availableAt); err == nil {
		return nil
	}
	row, err := r.loadJobRow(ctx, jobKey)
	if err != nil {
		return err
	}
	if !jsonObjectsEqual(row.metadata, metadata) {
		return jobdb.NewExistingJobMismatchError(fmt.Sprintf("job %s already exists with different metadata", jobKey))
	}
	if availableAt != nil && !timeFromNS(row.availableAtNS).Equal(availableAt.UTC()) {
		return jobdb.NewExistingJobMismatchError(fmt.Sprintf("job %s already exists with different availableAt", jobKey))
	}
	return nil
}

func (r *Runtime) withTx(ctx context.Context, fn func(*sql.Tx) error) error {
	tx, err := r.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}

func decodeWaitFor(raw []byte) ([]string, error) {
	return runtimecodec.DecodeWaitForJobs(raw)
}

func encodeWaitFor(waitFor []string) ([]byte, error) {
	return runtimecodec.EncodeWaitForJobs(waitFor)
}

func timeToNS(t time.Time) int64 {
	if t.IsZero() {
		return 0
	}
	return t.UTC().UnixNano()
}

func timeFromNS(ns int64) time.Time {
	if ns == 0 {
		return time.Time{}
	}
	return time.Unix(0, ns).UTC()
}

func nullTimeFromNS(v sql.NullInt64) *time.Time {
	if !v.Valid || v.Int64 == 0 {
		return nil
	}
	t := timeFromNS(v.Int64)
	return &t
}

func cloneJSON(raw json.RawMessage) json.RawMessage {
	if raw == nil {
		return nil
	}
	return append(json.RawMessage(nil), raw...)
}

func cloneBytes(raw []byte) []byte {
	if raw == nil {
		return nil
	}
	return append([]byte(nil), raw...)
}

func cloneString(value string) *string {
	v := value
	return &v
}

func statusFromRow(ctx context.Context, tx queryer, row jobRow, now time.Time) (jobdb.JobStatus, error) {
	if row.archivedAtNS.Valid {
		if row.cancelRequested || (row.completionStatus.Valid && row.completionStatus.String == "cancelled") {
			return jobdb.JobStatusCancelled, nil
		}
		return jobdb.JobStatusCompleted, nil
	}
	if row.cancelRequested {
		return jobdb.JobStatusCancelled, nil
	}
	if row.leaseID.Valid && row.leaseID.String != "" {
		if row.leaseExpiresAtNS.Valid && timeFromNS(row.leaseExpiresAtNS.Int64).After(now) {
			return jobdb.JobStatusActive, nil
		}
		return jobdb.JobStatusCrashConcern, nil
	}
	waitFor, err := decodeWaitFor(row.waitForRaw)
	if err != nil {
		return "", err
	}
	ready, err := dependenciesReady(ctx, tx, row.tenantID, waitFor)
	if err != nil {
		return "", err
	}
	if !ready {
		return jobdb.JobStatusPendingJobs, nil
	}
	if available := timeFromNS(row.availableAtNS); !available.IsZero() && available.After(now) {
		return jobdb.JobStatusAwaitingFuture, nil
	}
	return jobdb.JobStatusReady, nil
}

type queryer interface {
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

func dependenciesReady(ctx context.Context, q queryer, tenantID string, waitFor []string) (bool, error) {
	if len(waitFor) == 0 {
		return true, nil
	}
	for _, id := range waitFor {
		if id == "" {
			continue
		}
		var archivedAt sql.NullInt64
		err := q.QueryRowContext(ctx, `SELECT archived_at_ns FROM jobdb_jobs WHERE tenant_id = ? AND job_id = ?`, tenantID, id).Scan(&archivedAt)
		if err == sql.ErrNoRows {
			continue
		}
		if err != nil {
			return false, err
		}
		if !archivedAt.Valid {
			return false, nil
		}
	}
	return true, nil
}

func effectiveNextNeed(row jobRow, now time.Time) (need string, alternateFired bool) {
	if row.alternateNeed.Valid && row.alternateNeed.String != "" && row.alternateAtNS.Valid {
		if !timeFromNS(row.alternateAtNS.Int64).After(now) {
			return row.alternateNeed.String, true
		}
	}
	return row.nextNeed, false
}

func leaseDurationOrDefault(d time.Duration) time.Duration {
	if d <= 0 {
		return defaultRemoteLeaseDuration
	}
	return d
}
