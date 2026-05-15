package sqlite

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/colony-2/swf-go/pkg/swf"
)

const jobColumns = `
tenant_id, job_id, job_type, next_need, payload, metadata, wait_for,
available_at_ns, created_at_ns, updated_at_ns, archived_at_ns,
cancel_requested, completion_status, completion_detail,
lease_id, lease_worker_id, lease_expires_at_ns, alternate_need, alternate_at_ns
`

type jobRow struct {
	tenantID         string
	jobID            string
	jobType          string
	nextNeed         string
	payload          json.RawMessage
	metadata         json.RawMessage
	waitForJSON      string
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
	if err := scanner.Scan(
		&row.tenantID,
		&row.jobID,
		&row.jobType,
		&row.nextNeed,
		&payload,
		&metadata,
		&row.waitForJSON,
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
	row.payload = append(json.RawMessage(nil), payload...)
	row.metadata = append(json.RawMessage(nil), metadata...)
	row.cancelRequested = cancelRequested != 0
	if len(row.payload) == 0 {
		row.payload = json.RawMessage(`{}`)
	}
	if strings.TrimSpace(row.waitForJSON) == "" {
		row.waitForJSON = "[]"
	}
	return row, nil
}

func (r *Runtime) loadJobRow(ctx context.Context, jobKey swf.JobKey) (jobRow, error) {
	row, err := scanJobRow(r.db.QueryRowContext(ctx, `SELECT `+jobColumns+` FROM swf_jobs WHERE tenant_id = ? AND job_id = ?`, jobKey.TenantId, jobKey.JobId))
	if err == sql.ErrNoRows {
		return jobRow{}, swf.ErrJobNotFound
	}
	return row, err
}

func (r *Runtime) loadJobRowTx(ctx context.Context, tx *sql.Tx, jobKey swf.JobKey) (jobRow, error) {
	row, err := scanJobRow(tx.QueryRowContext(ctx, `SELECT `+jobColumns+` FROM swf_jobs WHERE tenant_id = ? AND job_id = ?`, jobKey.TenantId, jobKey.JobId))
	if err == sql.ErrNoRows {
		return jobRow{}, swf.ErrJobNotFound
	}
	return row, err
}

func (r *Runtime) insertJobRecord(ctx context.Context, jobKey swf.JobKey, jobType string, metadata json.RawMessage, waitFor []string, payload jobPayload, workerID string) error {
	payloadJSON, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	waitJSON, err := json.Marshal(waitFor)
	if err != nil {
		return err
	}
	now := time.Now().UTC()
	_, err = r.db.ExecContext(ctx, `
INSERT INTO swf_jobs (
	tenant_id, job_id, job_type, next_need, payload, metadata, wait_for,
	available_at_ns, created_at_ns, updated_at_ns
) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		jobKey.TenantId,
		jobKey.JobId,
		jobType,
		jobType,
		payloadJSON,
		cloneJSON(metadata),
		string(waitJSON),
		timeToNS(now),
		timeToNS(now),
		timeToNS(now),
	)
	if err != nil {
		return fmt.Errorf("sqlite runtime: insert job: %w", err)
	}
	return nil
}

func (r *Runtime) ensureSubmittedJobRecord(ctx context.Context, jobKey swf.JobKey, jobType string, metadata json.RawMessage, waitFor []string, payload jobPayload, workerID string) error {
	if err := r.insertJobRecord(ctx, jobKey, jobType, metadata, waitFor, payload, workerID); err == nil {
		return nil
	}
	row, err := r.loadJobRow(ctx, jobKey)
	if err != nil {
		return err
	}
	if !jsonObjectsEqual(row.metadata, metadata) {
		return swf.NewExistingJobMismatchError(fmt.Sprintf("job %s already exists with different metadata", jobKey))
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

func decodeWaitFor(raw string) ([]string, error) {
	if strings.TrimSpace(raw) == "" {
		return nil, nil
	}
	var out []string
	if err := json.Unmarshal([]byte(raw), &out); err != nil {
		return nil, err
	}
	return out, nil
}

func encodeWaitFor(waitFor []string) (string, error) {
	if len(waitFor) == 0 {
		return "[]", nil
	}
	clean := make([]string, 0, len(waitFor))
	for _, id := range waitFor {
		if id != "" {
			clean = append(clean, id)
		}
	}
	raw, err := json.Marshal(clean)
	if err != nil {
		return "", err
	}
	return string(raw), nil
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

func cloneString(value string) *string {
	v := value
	return &v
}

func statusFromRow(ctx context.Context, tx queryer, row jobRow, now time.Time) (swf.JobStatus, error) {
	if row.archivedAtNS.Valid {
		if row.cancelRequested || (row.completionStatus.Valid && row.completionStatus.String == "cancelled") {
			return swf.JobStatusCancelled, nil
		}
		return swf.JobStatusCompleted, nil
	}
	if row.cancelRequested {
		return swf.JobStatusCancelled, nil
	}
	if row.leaseID.Valid && row.leaseID.String != "" {
		if row.leaseExpiresAtNS.Valid && timeFromNS(row.leaseExpiresAtNS.Int64).After(now) {
			return swf.JobStatusActive, nil
		}
		return swf.JobStatusCrashConcern, nil
	}
	waitFor, err := decodeWaitFor(row.waitForJSON)
	if err != nil {
		return "", err
	}
	ready, err := dependenciesReady(ctx, tx, row.tenantID, waitFor)
	if err != nil {
		return "", err
	}
	if !ready {
		return swf.JobStatusPendingJobs, nil
	}
	if available := timeFromNS(row.availableAtNS); !available.IsZero() && available.After(now) {
		return swf.JobStatusAwaitingFuture, nil
	}
	return swf.JobStatusReady, nil
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
		err := q.QueryRowContext(ctx, `SELECT archived_at_ns FROM swf_jobs WHERE tenant_id = ? AND job_id = ?`, tenantID, id).Scan(&archivedAt)
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
