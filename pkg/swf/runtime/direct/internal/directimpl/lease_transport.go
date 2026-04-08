package directimpl

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/lib/pq"

	"github.com/colony-2/pgwf-go/pkg/pgwf"
	"github.com/colony-2/swf-go/pkg/swf"
)

const (
	extendLeaseByIDStmt     = `SELECT pgwf.extend_lease($1, $2, $3, $4, $5)`
	rescheduleLeaseByIDStmt = `
SELECT job_id, next_need, wait_for, available_at
FROM pgwf.reschedule_job($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11)
`
	completeLeaseByIDStmt             = `SELECT pgwf.complete_job($1, $2, $3, $4, $5, $6)`
	defaultRemoteLeaseRenewalDuration = 30 * time.Second
)

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
	leaseSeconds := durationToLeaseSeconds(leaseDuration)
	if leaseSeconds <= 0 {
		leaseSeconds = durationToLeaseSeconds(defaultRemoteLeaseRenewalDuration)
	}
	row := r.pgwfDB(ctx).QueryRowContext(
		ctx,
		extendLeaseByIDStmt,
		jobKey.TenantId,
		jobKey.JobId,
		leaseID,
		workerID,
		leaseSeconds,
	)
	var newExpiry time.Time
	if err := row.Scan(&newExpiry); err != nil {
		return directLeaseMutationError(err)
	}
	return nil
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
	status := completionStatusSuccess
	switch req.Status {
	case "", "success", "succeeded":
		status = completionStatusSuccess
	case "failed_app":
		status = completionStatusFailedApp
	case "failed_system":
		status = completionStatusFailedSystem
	case "failed_timeout":
		status = completionStatusFailedTimeout
	case "cancelled":
		status = completionStatusCancelled
	default:
		status = pgwf.CompletionStatus(req.Status)
	}
	row := r.pgwfDB(ctx).QueryRowContext(
		ctx,
		completeLeaseByIDStmt,
		jobKey.TenantId,
		jobKey.JobId,
		leaseID,
		workerID,
		string(status),
		sqlNullString(req.Detail),
	)
	var ok bool
	if err := row.Scan(&ok); err != nil {
		return directLeaseMutationError(err)
	}
	if !ok {
		return swf.ErrJobNotFound
	}
	return nil
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

	deps := pgwf.JobDependencies{
		NextNeed: pgwf.Capability(req.NextNeed),
	}
	if req.WaitUntil != nil {
		deps.AvailableAt = req.WaitUntil.UTC()
	}
	if len(req.WaitForJobIDs) > 0 {
		waitFor := make([]pgwf.JobID, 0, len(req.WaitForJobIDs))
		for _, id := range req.WaitForJobIDs {
			if id != "" {
				waitFor = append(waitFor, pgwf.JobID(id))
			}
		}
		deps.WaitFor = waitFor
	}
	if req.AlternateNeed != "" {
		after := time.Duration(0)
		if req.AlternateAfter != nil {
			after = time.Duration(*req.AlternateAfter)
		}
		deps.Alternate = &pgwf.AlternateNext{
			Need:  pgwf.Capability(req.AlternateNeed),
			After: after,
		}
	}
	if err := validateRemoteLeaseDependencies(deps); err != nil {
		return err
	}

	payload := req.Payload
	if len(payload) == 0 {
		payload = json.RawMessage(`{}`)
	}
	altNeedArg, altAfterArg, altSet := remoteRescheduleAlternateArgs(deps)
	row := r.pgwfDB(ctx).QueryRowContext(
		ctx,
		rescheduleLeaseByIDStmt,
		jobKey.TenantId,
		jobKey.JobId,
		leaseID,
		workerID,
		string(deps.NextNeed),
		pq.Array(jobIDsToStrings(deps.WaitFor)),
		remoteOptionalTime(deps.AvailableAt),
		payload,
		altNeedArg,
		altAfterArg,
		altSet,
	)
	var (
		id        string
		need      string
		waits     pq.StringArray
		available time.Time
	)
	if err := row.Scan(&id, &need, (*pq.StringArray)(&waits), &available); err != nil {
		return directLeaseMutationError(err)
	}
	return nil
}

func directLeaseMutationError(err error) error {
	if err == nil {
		return nil
	}
	switch {
	case errors.Is(err, pgwf.ErrLeaseMismatch), errors.Is(err, pgwf.ErrLeaseExpired):
		return swf.ErrExecutionLeaseLost
	case errors.Is(err, pgwf.ErrJobNotFound):
		return swf.ErrJobNotFound
	}
	message := strings.ToLower(err.Error())
	switch {
	case strings.Contains(message, "not currently leased"),
		strings.Contains(message, "actively leased"),
		strings.Contains(message, "active lease not found"),
		strings.Contains(message, "has expired"):
		return swf.ErrExecutionLeaseLost
	case strings.Contains(message, "not found"):
		return swf.ErrJobNotFound
	default:
		return err
	}
}

func jobIDsToStrings(ids []pgwf.JobID) []string {
	if len(ids) == 0 {
		return nil
	}
	out := make([]string, 0, len(ids))
	for _, id := range ids {
		if id == "" {
			continue
		}
		out = append(out, string(id))
	}
	return out
}

func sqlNullString(value string) any {
	if value == "" {
		return nil
	}
	return value
}

func remoteRescheduleAlternateArgs(d pgwf.JobDependencies) (need any, after any, set bool) {
	if d.Alternate == nil {
		return nil, nil, false
	}
	if d.Alternate.Need == "" && d.Alternate.After == 0 {
		return nil, nil, true
	}
	return string(d.Alternate.Need), remoteDurationToSecondsArg(d.Alternate.After), true
}

func validateRemoteLeaseDependencies(d pgwf.JobDependencies) error {
	if d.NextNeed == "" {
		return fmt.Errorf("next capability is required")
	}
	if d.Alternate != nil {
		if d.Alternate.After < 0 {
			return fmt.Errorf("alternate after must be non-negative")
		}
		if d.Alternate.Need == "" && d.Alternate.After > 0 {
			return fmt.Errorf("alternate capability is required when after is set")
		}
	}
	return nil
}

func remoteDurationToSecondsArg(d time.Duration) any {
	if d < 0 {
		return nil
	}
	seconds := int((d + time.Second - 1) / time.Second)
	if seconds < 0 {
		seconds = 0
	}
	return seconds
}

func remoteOptionalTime(t time.Time) any {
	if t.IsZero() {
		return nil
	}
	return t
}
