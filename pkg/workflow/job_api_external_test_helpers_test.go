package workflow_test

import (
	"context"

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
