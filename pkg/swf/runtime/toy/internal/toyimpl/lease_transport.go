package toyimpl

import (
	"context"
	"time"

	"github.com/colony-2/swf-go/pkg/swf"
)

func (r *Runtime) KeepAliveLeaseByID(_ context.Context, jobKey swf.JobKey, leaseID string, _ string, _ time.Duration) error {
	record := r.engine.getJobRecord(jobKey)
	if record == nil {
		return swf.ErrJobNotFound
	}
	record.mu.Lock()
	defer record.mu.Unlock()
	if record.leaseID != leaseID {
		return swf.ErrExecutionLeaseLost
	}
	return nil
}

func (r *Runtime) CompleteJobWithLeaseByID(_ context.Context, jobKey swf.JobKey, leaseID string, _ string, req swf.CompleteExecutionRequest) error {
	return r.completeLease(jobKey, leaseID, req)
}

func (r *Runtime) RescheduleJobWithLeaseByID(_ context.Context, jobKey swf.JobKey, leaseID string, _ string, req swf.RescheduleExecutionRequest) error {
	return r.rescheduleLease(jobKey, leaseID, req)
}
