# Specification: Lease Completion With Final Chapter

Status: Draft

## Problem

The worker currently records final job completion with two runtime mutations:

1. `WorkflowRuntime.PutChapter` writes a `JobAttemptOutcome` chapter and its
   artifacts.
2. `ExecutionLease.Complete` completes the leased job with `Status` and
   `Detail`.

For the remote runtime this is also two HTTP operations:
`/leases/{leaseId}/add_chapter` followed by `/leases/{leaseId}/complete`.
That creates a failure window where the final chapter and terminal job state can
diverge. It also makes lease validation, artifact handling, and completion
status recording happen across separate API boundaries.

Final job completion should be one lease-scoped operation. The complete-lease
API should accept the final chapter and artifact bytes, and the workflow runner
should use that API when recording job completion.

## Goals

- Allow `ExecutionLease.Complete` to complete the job through one logical lease
  operation that writes the final `JobAttemptOutcome` chapter with artifacts.
- Make the remote complete endpoint accept the same chapter and artifact write
  payload currently sent to the add-chapter endpoint.
- Keep `PutChapter` for non-terminal chapter writes such as task attempt
  outcomes and restart-extra chapters.
- Reject lease completion requests that do not include a final chapter.
- Make workflow job completion use the combined complete-lease path.
- Make completion retry-safe when a backend writes the final chapter before it
  marks the job row terminal.

## Non-Goals

- Change the visible chapter schema or artifact descriptor format.
- Change task attempt recording. Task outcomes should continue to use
  `PutChapter`.
- Remove the add-chapter endpoint. It remains the API for non-terminal chapter
  writes.
- Introduce standalone durable artifact writes.
- Preserve the old complete-without-chapter pattern.

## Runtime Contract

Extend `jobdb.CompleteExecutionRequest`:

```go
type CompleteExecutionRequest struct {
    Status string
    Detail string

    Chapter         *Chapter
    ArtifactUploads []ArtifactUpload
}
```

`Chapter` is required. A request with `Chapter == nil` is invalid and must not
complete the job.

- The chapter must be a `JobAttemptOutcomeChapter`.
- The target job comes from the lease, not from the request body.
- `Chapter.Ordinal` is the target ordinal and must satisfy the same append-only
  rules as `PutChapter`.
- For a fresh chapter write, artifact descriptors are materialized from
  `ArtifactUploads`. If `Chapter.Artifacts` is non-empty, it must describe
  `ArtifactUploads` exactly: same names, sizes, and SHA-256 digests after upload
  bytes are read.
- For an idempotent retry where the chapter already exists and matches the
  request, `ArtifactUploads` may be empty because the artifact bytes are already
  durable. If uploads are provided on that retry, they must still match
  `Chapter.Artifacts`.
- Duplicate artifact names are invalid.
- `Status` and `Detail` remain the scheduler completion fields. The runtime
  should not infer or rewrite them from the chapter body in this change.

The combined operation must be retry-safe:

- If lease validation fails, return `ErrExecutionLeaseLost`; do not write a
  chapter, artifacts, or terminal job state.
- If the target chapter ordinal is empty, validate the chapter, schema, artifact
  materialization, and ordinal rules before writing it.
- If the target chapter ordinal already contains an identical chapter, treat the
  chapter write as already applied and continue completing the job.
- If the target chapter ordinal already contains a different chapter, return
  `ErrConflict` and do not complete the job.
- If chapter validation, schema validation, artifact materialization, or
  non-idempotent ordinal checks fail, return the same errors `PutChapter` would
  return and do not complete the job.
- On success, the final chapter is visible, attached artifacts are readable, and
  the job is terminal with the requested completion status.
- `GetJob`, `GetJobRun`, chapter listing, and artifact reads must observe a
  consistent completed job: a completed workflow job with a final chapter must
  expose output data and artifacts from that same chapter.

Backends may implement this with a single database/storage transaction where
available, but a transaction across the chapter store and job row is not
required. A backend that has to write internally in two steps should write the
chapter first and then mark the job terminal. If a process dies after the
chapter commit and before the terminal row update, the lease eventually expires,
another worker can acquire the job, submit the same final-completion request,
observe the identical existing chapter, and finish the terminal row update
successfully.

The workflow runner must not paper over backend limitations by making separate
public runtime calls. Retry safety belongs inside the complete-lease operation.

## Remote API Contract

Update `openapi/jobdb-runtime.yaml`:

```yaml
CompleteExecutionRequest:
  type: object
  required: [status, chapter]
  additionalProperties: false
  properties:
    status:
      type: string
    detail:
      type: string
    chapter:
      $ref: '#/components/schemas/ChapterRecord'
    artifactUploads:
      type: array
      description: |
        Artifact payloads attached to the final chapter written as part of
        lease completion. Missing and empty arrays are both treated as no
        uploads; artifacts do not exist as durable resources unless the
        complete operation succeeds.
      items:
        $ref: '#/components/schemas/ArtifactWrite'
```

`POST /v1/tenants/{tenantId}/jobs/{jobId}/leases/{leaseId}/complete` remains the
endpoint. It now requires `chapter`; `artifactUploads` is optional. Missing and
empty arrays both mean no uploads and are valid for idempotent retries of an
already-written final chapter. The existing `add_chapter` endpoint remains for
non-terminal chapter writes and follows the same optional `artifactUploads`
encoding rule.

The generated runtime API must be regenerated after the OpenAPI change.

## Workflow Runner Changes

Replace the final job completion sequence in `pkg/workflow/worker_runner.go`:

- Stop calling `persistJobOutcome` followed by `completeLease` for final job
  results.
- Add a helper that prepares the final `JobAttemptOutcome` chapter and artifact
  uploads without committing them.
- Call `lease.Complete` with `Status`, `Detail`, `Chapter`, and
  `ArtifactUploads`.
- If a final job outcome chapter is already present for an otherwise
  uncompleted job, call `lease.Complete` with that stored chapter and an empty
  `ArtifactUploads` list. This covers the crash window where a previous worker
  wrote the chapter but did not mark the job terminal.
- Only call `markStoryOrdinalConsumed` after the combined complete operation
  succeeds.
- Keep local artifact cleanup after the complete call returns, matching current
  ownership semantics.
- Keep `PutChapter` for task attempts inside `DoTask`.

The shared chapter-building code should be factored out of
`persistTaskDataChapter` so both paths use the same artifact digesting,
metadata encoding, chapter body construction, and artifact-key assignment rules.

## Runtime Implementation Notes

- `pkg/jobdb/runtime/remote`: encode and decode the new complete request fields.
  `remoteExecutionLease.Complete` must call only the complete endpoint for final
  job completion.
- `pkg/jobdb/runtime/remote/server.go`: pass the decoded chapter and artifact
  uploads through `CompleteJobWithLeaseByID`.
- `pkg/jobdb/runtime/toy`: validate the lease, store the chapter, then mark the
  record completed. If the same chapter already exists, skip the write and
  complete the record.
- `pkg/jobdb/runtime/sqlite`: perform lease validation, artifact/chapter
  storage, schema validation, and job-row completion in one SQLite transaction
  when practical. If this remains split across stores, the existing identical
  final chapter must be accepted as an idempotent retry.
- `pkg/jobdb/runtime/direct`: add a combined direct/pgwf operation or shared
  idempotent completion path. Do not implement workflow completion by calling
  direct `PutChapter` and then direct `Complete`.

## Tests

Add runtime conformance coverage:

- `lease.Complete` with a `JobAttemptOutcome` chapter and artifact completes the
  job, stores the chapter, and makes the artifact readable.
- Completing with a non-appendable final ordinal, or with an already-used final
  ordinal whose existing chapter differs from the request, fails with
  `ErrConflict` and does not complete the job.
- Completing with invalid artifact metadata fails and does not complete the job.
- Completing with an expired or mismatched lease fails with
  `ErrExecutionLeaseLost` and writes nothing.
- Completing without a chapter fails and does not complete the job.
- Completing after an identical final chapter is already present but the job row
  is not terminal succeeds and marks the job terminal.

Add workflow runner coverage:

- Successful job completion sends the final chapter and artifacts via
  `CompleteExecutionRequest` and does not call `PutChapter` for the final job
  outcome.
- Job error completion, including error artifacts, uses the same combined
  complete path.
- A worker that finds a cached final job outcome chapter on a non-terminal job
  retries completion through `CompleteExecutionRequest` without writing a second
  chapter.
- If combined completion fails, the runner does not consume the story ordinal and
  returns/logs behavior equivalent to the current complete failure path.

Add remote coverage:

- The remote client sends `chapter` on the complete endpoint and sends
  `artifactUploads` only when uploads are present.
- The remote server validates the lease token and forwards the combined request
  to the underlying runtime.
- No workflow completion test depends on an `add_chapter` request for the final
  job chapter.

## Acceptance Criteria

- Workflow final job completion is represented by one `lease.Complete` call.
- A remote workflow job completion uses one complete HTTP request, not an
  add-chapter request followed by complete.
- The complete API rejects requests that omit the final chapter.
- A crash after internal final chapter write and before terminal row update is
  recoverable by a later worker retrying the same complete operation.
- Final chapter, artifacts, and terminal job state cannot permanently diverge
  through the public runtime API.
