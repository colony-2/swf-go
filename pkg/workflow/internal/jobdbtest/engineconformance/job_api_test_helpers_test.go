package engineconformance_test

import (
	"context"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
)

type testJobGetter interface {
	GetJob(context.Context, jobdb.JobKey) (jobdb.JobInfo, error)
}

func jobStatusForTest(getter testJobGetter, ctx context.Context, jobKey jobdb.JobKey) (jobdb.JobStatus, error) {
	job, err := getter.GetJob(ctx, jobKey)
	return job.Status, err
}

func jobResultForTest(getter testJobGetter, ctx context.Context, jobKey jobdb.JobKey) (jobdb.TaskData, error) {
	job, err := getter.GetJob(ctx, jobKey)
	if err != nil {
		return nil, err
	}
	return jobdb.ExtractTaskDataResult(job.Data)
}

func completeLeaseForTest(t *testing.T, ctx context.Context, lease jobdb.ExecutionLease, ordinal int64) {
	t.Helper()
	chapter := jobdb.Chapter{
		Ordinal:   ordinal,
		TaskType:  lease.Capability(),
		CreatedAt: time.Now().UTC(),
		Body: jobdb.JobAttemptOutcomeChapter{Outcome: jobdb.ApplicationOutputOutcome{
			Output: jobdb.ApplicationOutputBytes{Data: []byte(`{"ok":true}`)},
		}},
	}
	if err := lease.Complete(ctx, jobdb.CompleteExecutionRequest{
		Status:  "succeeded",
		Chapter: &chapter,
	}); err != nil {
		t.Fatalf("complete lease: %v", err)
	}
}
