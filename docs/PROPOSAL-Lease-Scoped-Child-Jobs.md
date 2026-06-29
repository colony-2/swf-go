# Proposal: Lease-Scoped Child Job Starts

## Summary

JobDB currently lets any client submit jobs through the base lifecycle APIs:

- `WorkflowRuntime.SubmitJob`
- `WorkflowRuntime.SubmitRestartJob`
- `POST /v1/tenants/{tenantId}/jobs`
- `POST /v1/tenants/{tenantId}/jobs/restarts`

Those APIs create independent root jobs. When job worker code starts another
job, JobDB does not inherently record that the new job was started by the
currently executing job.

Add lease-scoped job start APIs parallel to the base start APIs. These new APIs
reuse the existing job start request shapes, but can only be called while
holding a live execution lease for the parent job. On success, the runtime
creates the child job and stores the parent job ID in runtime-owned internal job
metadata.

## Goals

- Add first-class parent/child job provenance for jobs started by workers.
- Require a valid parent execution lease for parented job creation.
- Keep root job submission APIs unchanged.
- Reuse existing `SubmitJob`, `SubmitJobRequest`, `SubmitRestartJob`, and
  `SubmitRestartJobRequest` shapes.
- Expose parent filtering and parent projection fields through `ListJobs`.
- Add direct Go bindings on the lease and workflow context objects.

## Non-Goals

- Do not make all job starts lease-scoped. Root submissions remain valid.
- Do not add a separate child-job request type in Go.
- Do not add worker ID, lease ID, start kind, start ID, or start timestamp to
  parent metadata.
- Do not make parent completion depend on child completion. Parent code should
  still call `AwaitJobs` when it wants to wait for children.
- Do not introduce automatic cancellation cascading from parent to child in the
  first version.
- Do not preserve backward compatibility for old databases. New databases after
  this change should be created with the new schema/metadata expectations.

## Model

A child job is a normal job with one durable parent pointer:

```go
type RuntimeJobMetadata struct {
	Schedule    *ScheduleOccurrenceMetadata `json:"schedule,omitempty"`
	SchemaHash  string                      `json:"schemaHash,omitempty"`
	ParentJobID string                      `json:"parentJobId,omitempty"`
}
```

Stored metadata shape:

```json
{
  "app": {"queue": "blue"},
  "internal": {
    "schemaHash": "sha256:...",
    "parentJobId": "parent-1"
  }
}
```

The parent pointer is recorded on the child. The parent job does not need a
mutable child list in its job record.

Child jobs are same-tenant only. Because the lease path already identifies the
parent as `/v1/tenants/{tenantId}/jobs/{parentJobId}/leases/{leaseId}/...`, the
stored parent field only needs the parent job ID. If a child submit request
contains a tenant ID, it must be empty or equal to the path tenant.

## Go Runtime API

Extend `ExecutionLease` with start methods that reuse the existing runtime
request shapes:

```go
type ExecutionLease interface {
	LeaseID() string
	Job() JobHandle
	Capability() string
	Payload() json.RawMessage
	KeepAlive(ctx context.Context) error
	StopKeepAlive()
	Complete(ctx context.Context, req CompleteExecutionRequest) error
	Reschedule(ctx context.Context, req RescheduleExecutionRequest) error

	SubmitJob(ctx context.Context, req SubmitJobRequest) (JobHandle, error)
	SubmitRestartJob(ctx context.Context, req SubmitRestartJobRequest) (JobHandle, error)
}
```

For remote/server adapters that operate by lease ID, add transport methods
parallel to the existing `CompleteJobWithLeaseByID` and
`RescheduleJobWithLeaseByID` methods:

```go
type leaseJobSubmitter interface {
	SubmitJobWithLeaseByID(
		ctx context.Context,
		parentJobKey JobKey,
		leaseID string,
		workerID string,
		req SubmitJobRequest,
	) (JobHandle, error)

	SubmitRestartJobWithLeaseByID(
		ctx context.Context,
		parentJobKey JobKey,
		leaseID string,
		workerID string,
		req SubmitRestartJobRequest,
	) (JobHandle, error)
}
```

Example low-level usage:

```go
lease, err := runtime.GetJobLease(ctx, jobdb.GetJobLeaseRequest{
	JobKey:       parentKey,
	WorkerID:     "worker-1",
	Capabilities: []string{"parent-job"},
})
if err != nil {
	return err
}
if lease == nil {
	return nil
}

child, err := lease.SubmitJob(ctx, jobdb.SubmitJobRequest{
	Job: jobdb.SubmitJob{
		JobType: "child-job",
		JobID:   "parent-1-child-1",
		Data:    jobdb.NewTaskDataOrPanic(map[string]any{"n": 1}),
	},
})
if err != nil {
	return err
}
```

## Workflow Bindings

Add job start methods directly to the workflow context objects. Do not use an
optional side interface for discovery.

```go
type JobContext interface {
	GetJobKey() JobKey
	Logger() *slog.Logger
	DoTask(policy RunPolicy, taskType string, data TaskData) (TaskData, error)
	AwaitDuration(waitFor Duration) error
	AwaitJobs(jobIds ...string) error
	SubmitJob(ctx context.Context, submit SubmitJob) (JobKey, error)
	SubmitRestartJob(ctx context.Context, restart SubmitRestartJob) (JobKey, error)
}
```

If task workers should also be allowed to start jobs, add the same methods to
`TaskContext`:

```go
func (tc TaskContext) SubmitJob(ctx context.Context, submit SubmitJob) (JobKey, error)
func (tc TaskContext) SubmitRestartJob(ctx context.Context, restart SubmitRestartJob) (JobKey, error)
```

Example workflow usage:

```go
func (j ParentJob) Run(ctx workflow.JobContext, input jobdb.JobData) (jobdb.JobData, error) {
	child, err := ctx.SubmitJob(
		context.Background(),
		jobdb.SubmitJob{
			JobType: "child-job",
			JobID:   "parent-1-child-1",
			Data:    jobdb.NewTaskDataOrPanic(map[string]any{"n": 1}),
		},
	)
	if err != nil {
		return nil, err
	}

	if err := ctx.AwaitJobs(child.JobId); err != nil {
		return nil, err
	}
	return jobdb.NewTaskData(map[string]any{"child": child.JobId})
}
```

The context implementation should call the current execution lease. If no live
lease is available, it should return an error rather than falling back to root
submission.

## OpenAPI Additions

Add lease-scoped paths parallel to the base submit paths. Each operation
requires `X-JobDB-Lease-Token` and validates the path lease before creating the
child.

```text
POST /v1/tenants/{tenantId}/jobs/{parentJobId}/leases/{leaseId}/jobs
PUT  /v1/tenants/{tenantId}/jobs/{parentJobId}/leases/{leaseId}/jobs/{childJobId}

POST /v1/tenants/{tenantId}/jobs/{parentJobId}/leases/{leaseId}/jobs/restarts
PUT  /v1/tenants/{tenantId}/jobs/{parentJobId}/leases/{leaseId}/jobs/{childJobId}/restart
```

Suggested operation IDs:

- `submitJobWithLease`
- `putJobWithLease`
- `submitRestartJobWithLease`
- `putRestartJobWithLease`

Requests reuse existing schemas:

- submit and put job with lease: `SubmitJobRequest`
- submit and put restart with lease: `SubmitRestartJobRequest`

Responses match the base APIs:

- `200 JobHandle`: child submitted or equivalent explicit-ID request
  confirmed.
- `409 ErrorResponse`: lease lost, parent mismatch, or existing child durable
  state does not match the request.
- `404 ErrorResponse`: parent job or restart source job not found.

## List Jobs Fields

Add a parent projection field:

```go
type JobSummary struct {
	// existing fields...
	ParentJobID string
}
```

`ParentJobID` is empty for root jobs.

Add parent filters:

```go
type ListJobsRequest struct {
	// existing fields...
	ParentJobIDs []string
	RootOnly     bool
}
```

Filter semantics:

- `ParentJobIDs` returns direct children of any listed parent job ID in the
  requested tenant set.
- `RootOnly` returns jobs with no parent job ID.
- Supplying both `ParentJobIDs` and `RootOnly` is invalid.
- Parent filters compose with existing tenant, status, store, job type,
  metadata, created-time, and pagination filters.
- Parent filtering is for direct children only. Descendant traversal should be a
  separate API if needed.

OpenAPI schema additions:

```yaml
ListJobsRequest:
  properties:
    parentJobIds:
      type: array
      items:
        type: string
    rootOnly:
      type: boolean

JobSummary:
  properties:
    parentJobId:
      type: string
      description: Empty or absent for root jobs.
```

## Runtime Semantics

Lease-scoped job submit must behave as one logical operation:

1. Validate the parent lease identity and lease token.
2. Normalize the child tenant to the path tenant.
3. Apply the same validation as the base submit or restart API.
4. Store `internal.parentJobId` in the child job metadata envelope.
5. Create the child job and initial/restart chapters.

For child restarts, `SubmitRestartJob.PriorJobKey.TenantId` must match the path
tenant.

If the parent lease is stale, expired, missing, or mismatched, return
`ErrExecutionLeaseLost` and do not create a child.

Explicit child `JobID` keeps the same idempotency rules as base explicit-ID
submit and restart, plus the stored `internal.parentJobId` must match the path
parent job ID. If the destination job exists with matching durable child state
but a conflicting parent job ID, return `ErrExistingJobMismatch`.

Generated child job IDs behave like generated root job IDs: retrying after an
unknown client-side failure can create a second child. Callers that need
idempotent recovery should provide an explicit child `JobID`.

## Storage Plan

Parent provenance is runtime-owned internal job metadata, not application
metadata. JobDB already has this envelope; the change is to add
`RuntimeJobMetadata.ParentJobID`.

Shared metadata helper changes:

- Extend `RuntimeJobMetadata` with `ParentJobID`.
- Update `runtimeJobMetadataEmpty` and `storedInternalMetadataKnown` to include
  `parentJobId`.
- Continue using `BuildJobMetadataEnvelope(appMetadata, RuntimeJobMetadata{...})`
  for all job creation paths.
- Add `ExtractParentJobID(raw json.RawMessage) (string, bool, error)` for list
  projection, filters, idempotency checks, and tests.

SQLite runtime:

- New databases should include a `parent_job_id TEXT` column on `jobdb_jobs`.
  No compatibility migration is required.
- Source of truth remains the metadata envelope. The column is a query
  projection populated from `internal.parentJobId` on insert.
- List filtering can use `parent_job_id` directly.

Direct/Postgres runtime:

- Source of truth is pgwf job metadata JSONB containing the same metadata
  envelope.
- Do not route parent filters through the public `MetadataFilter` helper, since
  that helper intentionally prepends `app` and only exposes application
  metadata.
- For the initial implementation, decode `internal.parentJobId` from metadata
  returned by pgwf listing and apply parent filters in JobDB code.
- Do not add pgwf JSONB expression indexes in this change. Indexed parent
  listing can be added later as a targeted optimization.

Toy runtime:

- Source of truth is the existing in-memory job record metadata envelope.
- Decode `internal.parentJobId` for `JobSummary.ParentJobID`.
- Apply `ParentJobIDs` and `RootOnly` filters while scanning records.

Remote runtime:

- No independent storage. It sends lease-scoped job submit requests to the
  server and receives parent projections through `ListJobs`.
- The generated OpenAPI model should include `parentJobIds`, `rootOnly`, and
  `parentJobId`.

## Implementation Steps

1. Add `ParentJobID` to `RuntimeJobMetadata`, `JobSummary`, and
   `ListJobsRequest`.
2. Add `SubmitJob` and `SubmitRestartJob` methods to `ExecutionLease`
   implementations.
3. Add runtime transport methods by lease ID for direct, SQLite, toy, and remote
   server adapters.
4. Add OpenAPI paths reusing `SubmitJobRequest` and `SubmitRestartJobRequest`,
   then regenerate `pkg/jobdb/internal/runtimeapi`.
5. Implement remote client bindings and remote server handlers.
6. Add `internal.parentJobId` metadata construction/extraction and the new
   SQLite `parent_job_id` column for new databases.
7. Thread parent projection through `JobSummary` conversion and list filtering.
8. Add `SubmitJob` and `SubmitRestartJob` directly to workflow context objects.
9. Update `api/jobdb.public.txt` snapshots.
10. Document downstream usage in `docs/GUIDE-Lease-Scoped-Child-Jobs.md`.

## Tests

Conformance tests should cover every runtime:

- Lease-scoped submit succeeds only with a live lease.
- Lease-scoped restart succeeds only with a live lease.
- Stale, expired, missing, or mismatched lease fails without creating a child.
- Child job stores `internal.parentJobId` and lists with `ParentJobID`.
- Child job has the same start chapter, artifacts, schema validation, run
  policy, prerequisites, and available-at behavior as base submit.
- Explicit child job ID is idempotent for an equivalent child request and fails
  for a different child or parent job ID.
- `ListJobs` with `ParentJobIDs` returns direct children only.
- `ListJobs` with `RootOnly` excludes child jobs.
- Parent filters compose with status, stores, job types, metadata filters, and
  pagination.
- Remote child submit sends and validates `X-JobDB-Lease-Token`.
- Workflow `JobContext.SubmitJob` can submit a child and then `AwaitJobs` that
  child.

## Open Questions

- Should a later API support descendant queries or cancellation propagation?
