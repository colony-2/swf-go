# Workflow Runtime Guarantees Reference

This document is the normative behavioral reference for [`openapi/swf-runtime.yaml`](../openapi/swf-runtime.yaml).

Its purpose is to make the runtime contract implementation-independent. A runtime may be backed by `pgwf` + `strata` or by some other queue and storage stack, but it must preserve the same externally visible rules.

These rules are derived from the current SWF runtime boundary, the direct runtime's use of `pgwf` for job state and leases, the direct runtime's use of `strata` for chapter storage, and the existing SWF conformance and integration tests.

## Model

Each job is one logical state machine.

At any point in time, a job has logical state that includes things like:

- Whether it is leaseable, leased, waiting, completed, or cancelled.
- What capability it is currently waiting to run or resume.
- What externally visible progress has already been committed.

The visible chapter log is part of that logical job state. Chapters are the durable externally visible progress history for the job.

Other aspects of job state, such as current lease ownership, waiting-task guards, completion status, or internal coordination markers, may or may not be represented as chapters internally.

An implementation may realize the single logical job using one store, multiple stores, hidden intermediate chapters, hidden records, or some other internal structure. That decomposition is not part of the API contract.

The contract is defined by observable behavior of the single logical job, not by the storage layout chosen by a particular implementation.

## Coordination Model

All job-mutating operations must behave as if they share one job-scoped exclusivity domain.

The allowed coordination modes are:

- Lease-based coordination: `pollWork`, `getJobLease`, `keepalive`, `complete`, `reschedule`, and `add_chapter` operate on a live execution lease.
- Short-lock coordination: `commit-if-waiting` is the lease-less completion path for externally completed tasks.

The important rule is that these paths are not independent. A runtime must behave as if they compete in the same job lock space.

In particular:

- A job must not be simultaneously owned by two live execution leases.
- `commit-if-waiting` must not be able to race successfully alongside a conflicting live lease mutation of the same job.
- A losing conflicting writer must fail. It must not produce a second successful state transition.

## Chapter Log Guarantees

For every job, the chapter stream must satisfy all of the following:

- Chapter ordinals are zero-based integers.
- Ordinal `0` is the initial job chapter.
- Ordinal `0` is written exactly once for a given job.
- Every later successful chapter commit consumes the next logical position in the stream.
- The visible chapter sequence is contiguous from `0` through the highest committed ordinal.
- A chapter ordinal may be committed at most once.
- Once committed, a chapter is immutable. Its payload bytes, metadata, payload kind, chapter type, and attached artifacts must not change.
- The visible chapter order is monotonic. Runtimes must not allow backfilling older ordinals after later ordinals are visible.
- The runtime contract is append-only. There is no supported chapter delete or rewrite operation.

Retries do not rewrite an earlier chapter. A retry attempt is represented by a later chapter with later ordinal and updated attempt metadata.

The spec only constrains the externally visible chapter log. It does not require every internal state transition to appear as a visible chapter.

## Chapter Write Requirements

There are only two legal ways to commit a new chapter:

- `add_chapter` with a currently valid execution lease.
- `commit-if-waiting` while the job is still waiting on the described external task.

Runtimes must reject all other chapter-write patterns.

Additional requirements:

- The target ordinal is single-assignment. If multiple callers race for the same ordinal, at most one write may become visible.
- A duplicate or conflicting write must not succeed as a second visible commit, even if the caller repeated the same bytes.
- If the request carries both a path ordinal and a chapter body ordinal, they must refer to the same ordinal.
- A chapter write without the required coordination must fail. It must never silently create an unowned chapter.

## Waiting Task Completion Guarantees

When SWF suspends a job on an external task, the runtime records a waiting slot with:

- The current task capability.
- The resume capability.
- The input chapter ordinal.
- The output chapter ordinal to be written by the external completion.
- The deterministic input hash for that task invocation.

`commit-if-waiting` is defined against that waiting slot.

The operation must obey these rules:

- It may commit the task output chapter only if the job is still waiting on the described slot.
- `capability`, `resumeNeed`, `inputOrdinal`, `outputOrdinal`, and `inputHash` are guards. If a supplied guard does not match the current wait slot, the runtime must not apply the completion.
- A guard mismatch is a conflict error, not a partial mutation.
- If the wait slot has already been satisfied by another actor, the runtime must not write a second output chapter for the same ordinal.
- On success, the output chapter becomes visible at `outputOrdinal` and the job is advanced back to `resumeNeed`.

`commit-if-waiting` is not a silent no-op API. A conflicting or stale completion attempt must fail with a conflict response rather than returning success.

## Lease Guarantees

Execution leases are the normal ownership mechanism for job mutation.

Every conforming implementation must preserve these rules:

- At most one live lease may exist for a job at a time.
- A leased job is not leaseable again until the lease is completed, rescheduled, or lost.
- Queue-based leasing (`pollWork`) and targeted leasing (`getJobLease`) share the same lease space and must obey the same exclusion rules.
- `keepalive` extends an existing lease; it does not create a new lease or transfer ownership.
- `complete`, `reschedule`, and `add_chapter` require the current live lease.
- After a lease is completed, rescheduled, expired, or otherwise lost, further use of that lease token must fail.

For the remote API, `ExecutionLease.leaseToken` is the transport credential for
lease mutations. A server must reject missing, expired, stale, or mismatched
lease tokens before applying `keepalive`, `complete`, `reschedule`, or
`add_chapter`. `keepalive` returns a fresh lease token for the renewed lease.
Lease loss maps to conflict semantics. The current adapter uses HTTP `409` for
`ErrExecutionLeaseLost`.

## Artifact Guarantees

Artifacts are chapter-scoped commit data.

That means:

- Artifact bytes are written as part of the parent chapter commit.
- Artifacts do not exist as independently durable workflow resources before the parent chapter commit succeeds.
- A committed chapter's artifact set is immutable.
- If the request includes artifact descriptors, they must match the uploaded bytes by name, and by size and digest when those fields are supplied.
- Artifact reads are defined only for committed chapters.

The API intentionally does not expose a standalone durable artifact-upload operation.

## Restart Guarantees

A restart clones a prefix of an earlier run into a new job and then resumes execution from that copied history.

The restart boundary must obey these rules:

- `lastStepToKeep` must be `>= 0`.
- The source job must contain a chapter at `lastStepToKeep + 1`.
- A restart boundary must not cut into the middle of a retry chain.
- In practice, the chapter immediately after the kept prefix must represent attempt `1` of its logical step.

If `extraTaskOutput` is supplied, it is appended as the next chapter after the kept prefix and participates in the normal determinism rules. It is not a mutable patch to an earlier chapter.

## Explicit Job ID Guarantees

When the caller supplies an explicit destination job ID for submit or restart, creation is defined against that logical job identity rather than against an auto-generated ID.

The runtime must obey all of the following:

- An equivalent repeat of the same explicit-ID submit or restart request must return success for the existing job.
- For submit, equivalence is defined by the durable initial job chapter and any immutable create-time metadata the runtime persists alongside it.
- For restart, equivalence is defined by the copied source prefix through `lastStepToKeep`, plus the restart-extra chapter when one is requested.
- If the durable job history matches the request but an internal runtime coordination record is missing, the runtime must recreate the missing record and return success.
- That recovery behavior is only valid while the durable history still reflects the initial submitted or restarted shape. If later durable progress is visible but the internal coordination record is missing, the runtime must fail rather than silently manufacturing a new initial state.
- If the target job ID already exists but the durable state does not match the request, the runtime must fail with a distinguishable conflict outcome.

## Schedule Guarantees

Schedules are runtime-owned recurring job definitions, not worker-owned
controller jobs.

The runtime must obey all of the following:

- A schedule row is the authoritative control-plane state for schedule ID,
  state, generation, spec hash, trigger, target start spec, overlap policy, and
  failure policy.
- The schedule target is job-start-like: job type, input task data including
  artifacts, run policy, and app metadata.
- Scheduled occurrences are ordinary app jobs with deterministic job IDs,
  delayed leaseability, and hidden runtime-owned schedule metadata in the
  scheduler job record.
- Public app metadata APIs expose only app metadata. Runtime-owned
  `internal.schedule` metadata must not be visible to app code or accepted from
  public submit APIs.
- For an unstarted scheduled occurrence, the runtime performs schedule preflight
  before returning a lease to app code.
- An occurrence is considered started for schedule-preflight purposes once its
  visible chapter count is greater than one. After that point, later retries run
  under normal job lease semantics even if the parent schedule changes.
- With serial overlap policy, the next occurrence may be submitted before the
  current occurrence starts, but it must not be leaseable until the previous
  occurrence is terminal.
- A stale, paused, archived, ended, generation-mismatched, spec-mismatched, or
  failure-policy-blocked occurrence must be completed as `CANCELLED` with a
  durable schedule cancellation detail rather than being returned to app code.

## Read Guarantees

Read-side operations must preserve the following behavior:

- `listChapters` returns chapters in ascending ordinal order.
- `listChapters` applies an inclusive ordinal range.
- Asking for an `endOrdinal` above the current max is allowed and returns the available suffix.
- Asking for a `startOrdinal` above the current max returns an empty list.
- `getChapter` returns exactly one committed chapter by ordinal or a not-found error.
- `openArtifact` reads bytes from a committed chapter attachment.

## Error Semantics

The current remote adapter normalizes runtime outcomes this way:

- HTTP `400`: invalid request shape or semantic validation failure.
- HTTP `404`: missing job or missing chapter.
- HTTP `409`: lease lost or conflicting execution ownership.

Additional operation-specific behavior:

- `getJobLease` returns HTTP `200` with `lease: null` when no targeted lease is available.
- `commit-if-waiting` returns HTTP `204` only when the completion is applied.
- `commit-if-waiting` returns HTTP `409` when the job is no longer waiting on the described slot or when the guards do not match.
- `add_chapter` returns HTTP `409` when the target ordinal already exists or is not appendable in the visible chapter sequence.
- Explicit-ID submit and restart return HTTP `409` with error code `existing_job_mismatch` when the destination job already exists with different durable state.

For chapter writes specifically, the important interoperability rule is not the exact error code. It is that a second conflicting write must never become a second successful visible commit.

## Explicit Non-Guarantees

The current contract does not promise all possible transactional properties.

In particular:

- It does not require any particular internal decomposition or transaction protocol for maintaining the single logical job state.
- It does not guarantee that a transport failure means the operation had no side effects.

What is guaranteed is the steady-state correctness model:

- Chapter assignment is single-write.
- Job mutation is serialized through one logical coordination domain.
- Leases are exclusive.
- Waiting-task completion is conditional.

Any new runtime implementation should match those guarantees and should not add behaviors that weaken them.
