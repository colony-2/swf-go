package toyimpl

import (
	"context"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
)

func (r *Runtime) KeepAliveLeaseByID(ctx context.Context, jobKey jobdb.JobKey, leaseID string, workerID string, leaseDuration time.Duration) error {
	_, err := r.KeepAliveLeaseByIDWithExpiry(ctx, jobKey, leaseID, workerID, leaseDuration)
	return err
}

func (r *Runtime) KeepAliveLeaseByIDWithExpiry(_ context.Context, jobKey jobdb.JobKey, leaseID string, _ string, leaseDuration time.Duration) (time.Time, error) {
	record := r.engine.getJobRecord(jobKey)
	if record == nil {
		return time.Time{}, jobdb.ErrJobNotFound
	}
	record.mu.Lock()
	defer record.mu.Unlock()
	if record.leaseID != leaseID {
		return time.Time{}, jobdb.ErrExecutionLeaseLost
	}
	return time.Now().UTC().Add(toyLeaseDurationOrDefault(leaseDuration)), nil
}

func (r *Runtime) CompleteJobWithLeaseByID(ctx context.Context, jobKey jobdb.JobKey, leaseID string, _ string, req jobdb.CompleteExecutionRequest) error {
	return r.completeLease(ctx, jobKey, leaseID, req)
}

func (r *Runtime) RescheduleJobWithLeaseByID(_ context.Context, jobKey jobdb.JobKey, leaseID string, _ string, req jobdb.RescheduleExecutionRequest) error {
	return r.rescheduleLease(jobKey, leaseID, req)
}
