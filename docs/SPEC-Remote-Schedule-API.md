# Proposal: Remote Schedule API

## Status

Proposed | Updated: 2026-06-18

## Summary

Add schedules as first-class resources at the SWF remote REST API layer.

Schedules are stored as durable schedule manifests. Scheduled occurrences are
normal application jobs. There is no schedule controller job type and no
`swfd`-owned tenant worker responsible for advancing schedules.

Instead, SWF gains runtime-internal schedule metadata on jobs. When an engine
leases a scheduled occurrence, the engine performs schedule preflight before app
code sees the job:

1. verify the parent schedule manifest still allows this occurrence
2. evaluate serial/failure policy using hidden runtime metadata
3. submit the next occurrence idempotently
4. record a durable schedule preflight marker
5. run app code normally

This keeps scheduling progress tied to ordinary SWF job execution. Tenants only
run their normal app workers.

This uses existing pgwf and Strata concepts plus two runtime-level additions:

- delayed submit through `SubmitJob.AvailableAt`
- hidden engine metadata and system chapters that are not visible to app code or
  app replay

## Core Model

### Schedule Manifest

Each schedule has one durable manifest keyed by tenant and schedule ID.

The manifest is the authoritative control-plane record for:

- schedule ID
- desired state
- generation
- spec hash
- trigger
- target job shape
- serial/failure policy
- creation/update history

The manifest is not a worker-owned job that has to be leased or parked. Schedule
API mutations append schedule-system history using a runtime/internal compare
and append primitive, or an equivalent schedule manifest primitive. Children do
not mutate the manifest.

Suggested manifest identity:

```text
tenantId + scheduleId == schedule manifest key
```

It can be backed by an internal SWF job/story for storage reuse, but it must not
require a job worker or an active lease to process schedule commands.

### Schedule States

The manifest stores desired control state:

```text
ACTIVE
PAUSED
ARCHIVED
```

An API may also report a derived effective state:

```text
FAILURE_PAUSED
```

`FAILURE_PAUSED` is derived from the occurrence chain when a scheduled job is
cancelled during preflight because the hidden failure window violates the
failure policy. It is not written by a child job into the parent manifest. A
manual resume writes a new manifest generation and files a new first occurrence
with an empty failure window.

### Occurrence Jobs

Every scheduled occurrence is the actual target app job. There are no occurrence
controller jobs.

A scheduled occurrence is submitted with:

- `JobType`, input, run policy, and app metadata from the schedule target
- deterministic explicit `JobID`
- `AvailableAt = scheduledAt`
- serial prerequisite, when applicable
- public/index schedule metadata for schedule APIs
- hidden runtime schedule metadata for engine preflight

The app worker receives only the normal job input and app metadata. It cannot
read or forge hidden runtime schedule metadata.

### Engine Schedule Preflight

When an engine leases a job that contains hidden schedule metadata, it runs
preflight before invoking app code.

For an occurrence `J_k`, hidden metadata contains:

```json
{
  "schedule": {
    "tenantId": "tenant-a",
    "scheduleId": "daily-cleanup",
    "manifestKey": {
      "tenantId": "tenant-a",
      "jobId": "daily-cleanup"
    },
    "generation": 8,
    "specHash": "sha256:...",
    "scheduledAt": "2026-06-17T13:00:00Z",
    "previousJobId": "swfsched_daily-cleanup_run_20260617T120000000000000Z",
    "failureBits": "101101",
    "failureWindowSize": 10
  }
}
```

`failureBits` contains completed outcomes through `J_{k-2}`. The most recent
previous job, `J_{k-1}`, is fetched during `J_k` preflight, appended to the
window, and carried into `J_{k+1}`.

Preflight algorithm:

1. If this job already has a durable successful schedule preflight marker, skip
   validation and continue to app execution. This preserves normal retries after
   a job has started.
2. Load the schedule manifest.
3. If the manifest is paused, archived, missing, or generation/spec do not
   match, record a schedule cancellation system chapter and complete the job as
   `CANCELLED`.
4. If `previousJobId` is present, load that terminal job and append one outcome
   bit to the hidden failure window.
5. Evaluate failure policy. If violated, record a schedule cancellation system
   chapter with reason `failure_policy` and complete the job as `CANCELLED`.
6. Compute the next fire time.
7. Submit `J_{k+1}` idempotently using the schedule target shape from the
   validated manifest.
8. Record a durable successful schedule preflight marker on `J_k`.
9. Invoke app code normally.

The next occurrence is submitted before app code runs so the schedule chain does
not depend on the current app job succeeding. Because v1 uses serial semantics,
the next occurrence waits for the current one to become terminal before it can
start.

If the process crashes after submitting `J_{k+1}` but before recording the
preflight marker, retrying `J_k` preflight repeats the same deterministic submit
and converges.

If the process crashes after recording the marker but before app code runs,
retrying `J_k` does not re-check schedule state. The occurrence has already
passed the "not yet started" boundary and should run/retry normally.

### System Chapters

SWF should add system chapters or an equivalent internal history namespace for
engine-owned events. System chapters must be:

- hidden from app `JobData`, app metadata, and deterministic app replay
- visible to runtime/admin inspection and schedule run projection
- excluded from app task ordinal semantics

Schedule preflight uses system chapters for:

- successful schedule preflight marker
- scheduled cancellation before app start

Scheduled cancellation chapter payload:

```json
{
  "kind": "schedule_preflight_outcome",
  "status": "cancelled",
  "reasonCode": "schedule_paused",
  "message": "schedule paused before app start",
  "scheduleId": "daily-cleanup",
  "expectedGeneration": 8,
  "actualGeneration": 9,
  "scheduledAt": "2026-06-17T13:00:00Z"
}
```

Recommended reason codes:

```text
schedule_missing
schedule_paused
schedule_archived
schedule_generation_mismatch
schedule_spec_mismatch
failure_policy
```

The job terminal state for these cases is `CANCELLED`. The cancellation reason
should also be copied into completion detail where the runtime supports it, but
the system chapter is the durable, portable explanation.

## Delayed Submit And Serial Chaining

SWF submit supports delayed initial leaseability:

```go
type SubmitJob struct {
    AvailableAt *time.Time
}
```

Remote wire field:

```json
{
  "availableAt": "2026-06-17T13:00:00Z"
}
```

Semantics: a job is not leaseable before `availableAt`, and is also not
leaseable until prerequisites are satisfied.

V1 uses serial schedule semantics. The next occurrence is submitted before the
current app job starts, but with a prerequisite on the current job:

```go
SubmitJob{
    JobID:       "swfsched_daily-cleanup_run_20260617T140000000000000Z",
    AvailableAt: nextFireAt,
    Prerequisites: []swf.JobPrerequisite{
        {JobID: currentJobID, Condition: swf.JobPrereqComplete},
    },
}
```

Use `complete`, not `success`, so a failed occurrence still allows the next
occurrence to start, evaluate the failure window, and either continue the chain
or stop it with a scheduled cancellation.

## Trigger Timing

V1 distinguishes calendar triggers from interval triggers when a job starts
late.

Calendar or cron triggers are expression anchored. If a daily-noon occurrence
starts two days late at 3pm, the next occurrence is the next noon after the
actual preflight time.

Interval triggers are fixed-delay from actual preflight time. If an every-12h
occurrence starts 4 hours late, the next occurrence is 12 hours after the actual
preflight time.

This avoids catch-up storms in v1 while preserving intuitive calendar behavior.
Backfill and more advanced catch-up policies can be added later as explicit
features.

## Failure Policy

Failure policy is intentionally narrow in v1:

```json
{
  "minSuccessPercent": 80,
  "windowSize": 10,
  "maxSequentialFailures": 3
}
```

The occurrence chain carries a compact binary window in hidden runtime metadata:

- `1` means the prior target job completed successfully
- `0` means the prior target job failed, was cancelled, or otherwise did not
  produce successful app output

For occurrence `J_k`, the hidden window contains outcomes through `J_{k-2}`.
During preflight, the engine loads `J_{k-1}`, appends its outcome, trims to
`windowSize`, and evaluates both configured conditions:

- success percentage over the available window is at least `minSuccessPercent`
- sequential failures are fewer than `maxSequentialFailures`

If either condition fails, `J_k` is cancelled before app start with a
`failure_policy` system chapter and no successor is submitted.

This means failure policy is enforced when the next scheduled occurrence wakes
up. It does not require an out-of-band health process. Immediate failure status
at the moment `J_{k-1}` completes would require a separate terminal hook and is
not part of v1.

## Schedule API

### Upsert Schedule

```text
PUT /v1/tenants/{tenantId}/schedules/{scheduleId}
```

Creates or updates the schedule manifest.

Request:

```json
{
  "trigger": {
    "kind": "cron",
    "expression": "0 12 * * *",
    "timezone": "UTC",
    "startAt": "2026-06-17T00:00:00Z",
    "endAt": null
  },
  "target": {
    "jobType": "daily_cleanup",
    "data": { "bucket": "reports" },
    "runPolicy": {},
    "metadata": { "owner": "analytics" }
  },
  "overlapPolicy": "serial",
  "failurePolicy": {
    "minSuccessPercent": 80,
    "windowSize": 10,
    "maxSequentialFailures": 3
  },
  "paused": false,
  "expectedGeneration": 7
}
```

Response:

```json
{
  "schedule": {
    "tenantId": "tenant-a",
    "scheduleId": "daily-cleanup",
    "manifestKey": {
      "tenantId": "tenant-a",
      "jobId": "daily-cleanup"
    },
    "state": "ACTIVE",
    "effectiveState": "ACTIVE",
    "generation": 8,
    "specHash": "sha256:...",
    "nextFireAt": "2026-06-17T13:00:00Z",
    "nextJobKey": {
      "tenantId": "tenant-a",
      "jobId": "swfsched_daily-cleanup_run_20260617T130000000000000Z"
    },
    "updatedAt": "2026-06-17T12:00:00Z"
  }
}
```

Semantics:

- If the manifest does not exist, create it at generation `1`.
- If `expectedGeneration` is present and does not match, return `409`.
- Updating an archived schedule returns `409`.
- Create/update increments generation and resets the hidden failure window.
- If the resulting state is active, the schedule API submits the first
  occurrence for that generation.
- Existing future jobs from older generations do not need to be found or
  cancelled. When they wake, preflight detects the generation/spec mismatch,
  writes a scheduled cancellation system chapter, and completes them as
  `CANCELLED`.

### Get Schedule

```text
GET /v1/tenants/{tenantId}/schedules/{scheduleId}
```

Loads the manifest and projects state. It may derive `effectiveState` from the
latest scheduled occurrence system outcome, for example `FAILURE_PAUSED` when
the chain stopped due to failure policy.

### List Schedules

```text
POST /v1/tenants/{tenantId}/schedules/query
```

Projects schedules from manifest records tagged with schedule index metadata.

Request:

```json
{
  "scheduleIds": ["daily-cleanup"],
  "states": ["ACTIVE", "PAUSED"],
  "targetJobTypes": ["daily_cleanup"],
  "pageSize": 100,
  "pageToken": ""
}
```

### Pause Schedule

```text
POST /v1/tenants/{tenantId}/schedules/{scheduleId}/pause
```

Updates the manifest to `PAUSED` and increments generation.

Pause only affects occurrences that have not yet passed schedule preflight. A
job that already recorded successful schedule preflight continues and retries
normally, even if the schedule is paused later.

Existing future occurrences may be left in place. They will cancel themselves
before app start when they wake and observe the manifest no longer matches.

### Resume Schedule

```text
POST /v1/tenants/{tenantId}/schedules/{scheduleId}/resume
```

Updates the manifest to `ACTIVE`, increments generation, resets the hidden
failure window, and files the first occurrence for the new generation.

### Archive Schedule

```text
POST /v1/tenants/{tenantId}/schedules/{scheduleId}/archive
```

Updates the manifest to `ARCHIVED` and increments generation. Future
not-yet-started occurrences cancel themselves during preflight. Already-started
occurrences continue normally.

### Trigger Schedule Now

```text
POST /v1/tenants/{tenantId}/schedules/{scheduleId}/trigger
```

Creates a manual occurrence immediately. Manual triggers should carry hidden
schedule metadata if they must honor current manifest generation/state before
app start. They should not automatically join the recurring serial chain unless
the API explicitly requests that behavior.

### Backfill Schedule

```text
POST /v1/tenants/{tenantId}/schedules/{scheduleId}/backfills
```

Backfill is not required for v1. A later version can submit bounded batches of
scheduled app jobs using the same hidden metadata and deterministic IDs.

### List Schedule Runs

```text
POST /v1/tenants/{tenantId}/schedules/{scheduleId}/runs/query
```

Lists scheduled app jobs by schedule index metadata and system outcomes.

Request:

```json
{
  "scheduledAfter": "2026-06-17T00:00:00Z",
  "scheduledBefore": "2026-06-18T00:00:00Z",
  "statuses": ["READY", "ACTIVE", "COMPLETED", "CANCELLED"],
  "pageSize": 10,
  "pageToken": ""
}
```

Runs cancelled by schedule preflight should expose a schedule cancellation
reason from the system chapter.

## Metadata

### App Metadata

The app target metadata from the schedule spec is passed to the app job in the
same way as normal job metadata.

Reserved `swf_` schedule fields must not be accepted from user-provided target
metadata.

### Schedule Index Metadata

Use top-level fields so existing metadata filtering can find manifests and runs.
These fields are for schedule APIs and runtime inspection, not for app worker
logic.

Manifest index metadata:

```json
{
  "swf_kind": "schedule_manifest",
  "swf_schedule_id": "daily-cleanup",
  "swf_schedule_generation": 8,
  "swf_schedule_target_job_type": "daily_cleanup"
}
```

Occurrence index metadata:

```json
{
  "swf_kind": "schedule_tick",
  "swf_schedule_id": "daily-cleanup",
  "swf_schedule_generation": 8,
  "swf_scheduled_at": "2026-06-17T13:00:00Z",
  "swf_schedule_run_id": "20260617T130000000000000Z",
  "swf_schedule_manual": false,
  "swf_schedule_backfill_id": ""
}
```

### Hidden Runtime Metadata

Hidden runtime metadata is persisted with the job but not exposed to app code or
public app-facing metadata APIs.

It drives engine preflight and carries the serial failure window. Only trusted
runtime/schedule APIs may set it.

## Deterministic Job IDs

Schedule IDs should be URL-safe and job-ID-safe in v1:

```text
[A-Za-z0-9_-]+
```

Suggested IDs:

```text
<scheduleId>                                      # manifest key
swfsched_<scheduleId>_g<generation>_run_<fireTime> # recurring occurrence
swfsched_<scheduleId>_manual_<requestId>         # manual occurrence
swfsched_<scheduleId>_backfill_<requestId>_<fireTime>
```

Include generation in recurring occurrence IDs so an updated schedule can file a
new occurrence at the same fire time as an older stale generation. Use a
nanosecond-precision UTC fire time in IDs:

```text
YYYYMMDDThhmmssnnnnnnnnnZ
```

## State Ownership

No new external durable store is required. A runtime may back the manifest and
hidden occurrence metadata with internal tables or internal system history.

Logical state ownership:

```text
schedule identity             schedule manifest key
schedule spec/generation      manifest system history
desired state                 manifest system history
next due occurrence           deterministic job ID + availableAt
serial chain                  occurrence prerequisites
failure window                hidden runtime metadata on occurrence jobs
preflight result              system chapters on occurrence jobs
target run result             normal job terminal state and app chapters
schedule/run projections      manifest/run metadata plus system chapters
```

Any in-memory cache, timer, or cursor is an optimization only. Correctness must
survive process restart because due occurrences are ordinary jobs in the runtime.

## Transaction Model

No cross-job transaction is required.

The schedule API mutates only the manifest. Occurrence jobs read the manifest
and submit successors idempotently.

Cross-job side effects converge through deterministic explicit job IDs:

- submitting the first occurrence twice converges
- submitting the same successor twice converges
- stale generations cancel themselves during preflight

Crash cases:

- Crash after successor submit and before preflight marker: retry submits the
  same successor and then records the marker.
- Crash after preflight marker and before app invocation: retry skips schedule
  validation and runs app code.
- Crash during app execution: normal SWF retry semantics apply; schedule state
  is not revalidated.

## Overlap Policy

V1 supports only:

```text
serial
```

Serial means the next occurrence may be submitted early but is not leaseable
until both:

- its own `availableAt` has passed
- the previous occurrence is terminal

Future versions can add `allow`, `skip`, or `cancel_previous`.

## Error Semantics

- `400`: invalid trigger, target, policy, schedule ID, or request shape
- `404`: schedule manifest not found
- `409`: expected generation mismatch, archived schedule update, invalid state
  transition, or explicit job ID conflict with a different shape
- `200`: idempotent success or synchronous projection success

## Security And Isolation

- Schedules are tenant-scoped.
- Scheduled jobs are submitted only into the schedule's tenant.
- App code cannot see or set hidden runtime schedule metadata.
- Public submit APIs must reject hidden runtime metadata unless the caller is a
  trusted runtime/schedule component.
- User target metadata cannot override reserved `swf_` schedule fields.
- Schedule preflight must happen before app code, and successful preflight must
  be recorded durably before app code so retries preserve the started boundary.

## Open Questions

- Should the manifest be represented as a reserved SWF job/story or as a
  first-class runtime manifest record backed by the same storage?
- What exact runtime primitive should append manifest system history with
  compare-and-set semantics?
- Should `GET /schedules/{id}` return `state: FAILURE_PAUSED`, or keep
  `state: ACTIVE` and expose `effectiveState: FAILURE_PAUSED`?
- How much of schedule index metadata should be visible through general
  `ListJobs` versus only schedule APIs?
- Should completion detail be normalized so scheduled cancellations are visible
  even without reading system chapters?

## Recommendation

Use a schedule manifest for control-plane state and make recurring occurrences
ordinary app jobs. Add hidden runtime schedule metadata and system chapters so
the SWF engine can preflight scheduled jobs, submit the next occurrence, and
record clear cancellation reasons without exposing schedule internals to app
code.

This removes controller-worker ownership from `swfd`, avoids tenant-managed
schedule workers, and keeps schedule progress inside normal SWF execution.
