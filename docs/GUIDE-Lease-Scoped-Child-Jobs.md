# Lease-Scoped Child Jobs

JobDB can now record when a running job starts another job. A child job is
created through the parent job's active execution lease, and JobDB stores the
parent job ID in internal job metadata.

Use this when a worker wants durable fan-out from inside a job or task and
downstream tools need to list the jobs started by that parent.

## Start a Child Job from a Workflow

Inside a job worker, call `SubmitJob` on `workflow.JobContext`:

```go
func (w MyJob) Run(ctx workflow.JobContext, input jobdb.JobData) (jobdb.JobData, error) {
	childKey, err := ctx.SubmitJob(context.Background(), jobdb.SubmitJob{
		JobID:   "parent-123/fetch-profile",
		JobType: "fetch-profile",
		Data:    &jobdb.SimpleTaskData{Data: []byte(`{"userId":"u-123"}`)},
	})
	if err != nil {
		return nil, err
	}

	if err := ctx.AwaitJobs(childKey.JobId); err != nil {
		return nil, err
	}
	return input, nil
}
```

Inside a task worker, call the same methods on `workflow.TaskContext`:

```go
func (t MyTask) Run(ctx workflow.TaskContext, input jobdb.TaskData) (jobdb.TaskData, error) {
	childKey, err := ctx.SubmitJob(context.Background(), jobdb.SubmitJob{
		JobID:   "parent-123/task-child",
		JobType: "task-child",
		Data:    input,
	})
	if err != nil {
		return nil, err
	}
	return &jobdb.SimpleTaskData{Data: []byte(`{"childJobId":"` + childKey.JobId + `"}`)}, nil
}
```

`SubmitRestartJob` is also available on both context objects:

```go
restartKey, err := ctx.SubmitRestartJob(context.Background(), jobdb.SubmitRestartJob{
	JobID:          "parent-123/replay-child",
	PriorJobKey:    jobdb.JobKey{JobId: "previous-child-run"},
	LastStepToKeep: 3,
})
```

The parent tenant is inferred from the active lease. Jobs cannot be started
cross-tenant through this API.

## Start a Child Job from a Lease

Lower-level runtime users can call the lease directly:

```go
lease, err := runtime.GetJobLease(ctx, jobdb.GetJobLeaseRequest{
	JobKey:       parentKey,
	WorkerID:     "worker-1",
	Capabilities: []string{"parent-job"},
})
if err != nil || lease == nil {
	// handle unavailable work
}

child, err := lease.SubmitJob(ctx, jobdb.SubmitJobRequest{
	Job: jobdb.SubmitJob{
		JobID:   "child-id",
		JobType: "child-job",
		Data:    &jobdb.SimpleTaskData{Data: []byte(`{}`)},
	},
})
```

The same `SubmitJobRequest` and `SubmitRestartJobRequest` shapes are used as
top-level submissions. There are no separate child request types.

## Listing Children

`JobSummary.ParentJobID` is set for jobs created through a lease.

List direct children of a parent:

```go
children, err := runtime.ListJobs(ctx, jobdb.ListJobsRequest{
	TenantIds:    []string{tenantID},
	ParentJobIDs: []string{parentJobID},
	PageSize:     100,
})
```

List only root jobs, excluding child jobs:

```go
roots, err := runtime.ListJobs(ctx, jobdb.ListJobsRequest{
	TenantIds: []string{tenantID},
	RootOnly:  true,
	PageSize:  100,
})
```

`ParentJobIDs` returns direct children only. It does not recursively return the
full descendant tree. Supplying both `ParentJobIDs` and `RootOnly` is invalid.

## Idempotency

Use explicit child job IDs when a worker might retry the same child submission.
Equivalent explicit-ID requests return the existing job. If an existing job with
that ID has different durable start state, the runtime returns
`ErrExistingJobMismatch`.

For generated IDs, each successful call creates a new child job.

## Remote Runtime API

The remote runtime uses the same request bodies as normal submit and restart
operations, but the parent job lease is encoded in the path and protected by the
lease token header:

```text
POST /v1/tenants/{tenantId}/jobs/{parentJobId}/leases/{leaseId}/jobs
PUT  /v1/tenants/{tenantId}/jobs/{parentJobId}/leases/{leaseId}/jobs/{childJobId}

POST /v1/tenants/{tenantId}/jobs/{parentJobId}/leases/{leaseId}/jobs/restarts
PUT  /v1/tenants/{tenantId}/jobs/{parentJobId}/leases/{leaseId}/jobs/{childJobId}/restart
```

Clients should usually prefer the Go runtime or workflow APIs. Direct HTTP
callers must send `X-JobDB-Lease-Token`.

## Internal Metadata

Parent tracking is stored as JobDB internal metadata and exposed through
`JobSummary.ParentJobID` plus `ListJobsRequest.ParentJobIDs` and `RootOnly`.
Consumers should not read or write the internal metadata envelope directly.
