# Remote Schedule API Reference

## Status

Current reference | Updated: 2026-06-21

## Summary

Add schedules as first-class resources at the SWF remote REST API layer.

Schedules are stored as durable rows in `swf_schedules`. Scheduled occurrences
are normal application jobs. There is no schedule controller job type and no
`swfd`-owned tenant worker responsible for advancing schedules.

Instead, SWF gains hidden scheduler-side schedule metadata on occurrence jobs
and the runtime server performs schedule administration inside the lease
acquisition path. When `PollWork` or `GetJobLease` acquires a candidate
scheduled occurrence from pgwf or another scheduler backend, the server performs
schedule preflight before a lease is returned to any app worker:

1. identify scheduled occurrences from scheduler-side metadata
2. read Strata story metadata for scheduled occurrences only
3. if the story has more than the start chapter, skip schedule validation and
   return the lease
4. otherwise verify the parent schedule row, evaluate serial/failure policy, and
   submit the next occurrence idempotently
5. return the lease to the worker only if the occurrence is allowed to run

If the occurrence is stale, paused, archived, or blocked by failure policy, the
server records the schedule outcome, completes the job as `CANCELLED`, and does
not include that lease in the response. This keeps scheduling progress tied to
ordinary SWF job lease attempts while keeping the REST API standalone: tenants
only run normal app workers, and clients do not need schedule-specific lease
logic.

This uses the existing scheduler/lease store plus two runtime-level additions:

- delayed submit through `SubmitJob.AvailableAt`
- hidden scheduler-side runtime metadata that is not visible to app code or app
  replay

The schedule lease path does not read Strata chapter bodies. It may read Strata
story metadata for scheduled jobs to get the current chapter count. All other
data needed to decide whether to return, cancel, or advance a scheduled
occurrence lives in pgwf or the pgwf-like scheduler database used by the
runtime.

## Core Model

### Schedule Row

Each schedule has one durable row keyed by tenant and schedule ID.

The schedule row is the authoritative control-plane record for:

- schedule ID
- desired state
- generation
- spec hash
- trigger
- target job shape
- serial/failure policy
- creation/update history

The schedule row is not a worker-owned job, a pgwf job, or a Strata story.
Schedule API mutations update the schedule table using ordinary runtime
database concurrency controls. Children do not mutate the schedule row.

Suggested table identity:

```text
swf_schedules(tenant_id, schedule_id)
```

Both Postgres/direct and SQLite runtimes should expose the same logical table.
The table is separate from pgwf job tables, SQLite scheduler job tables, and
Strata chapter tables.

Minimal columns:

```text
tenant_id
schedule_id
state
generation
spec_hash
trigger_json
target_json
target_job_type
overlap_policy
failure_policy_json
next_fire_at
next_job_id
created_at
updated_at
```

`target_json` stores the durable job-start-like target spec: target job type,
raw application data, target artifact descriptors/bytes, run policy, and app
metadata. The schedule spec hash includes the target data and artifact
fingerprints. Occurrence submission uses this stored target snapshot; it does
not reopen client-local files or depend on the original upsert request body.

A runtime may also keep a companion `swf_schedule_events` table for update
history and admin inspection, but lease preflight reads the current
`swf_schedules` row directly.

### Schedule States

The schedule row stores desired control state:

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
failure policy. It is not written by a child job into the parent schedule row. A
manual resume writes a new schedule generation and files a new first occurrence
with an empty failure window.

### Occurrence Jobs

Every scheduled occurrence is the actual target app job. There are no occurrence
controller jobs.

A scheduled occurrence is submitted with:

- `JobType`, input, target artifact snapshot, run policy, and app metadata from
  the schedule target
- deterministic explicit `JobID`
- `AvailableAt = scheduledAt`
- serial prerequisite, when applicable
- hidden scheduler-side schedule metadata for schedule APIs and server-side
  lease preflight

The app worker receives only the normal job input and app metadata. It cannot
read or forge hidden scheduler-side schedule metadata.

### Server-Side Lease Preflight

Schedule preflight is server-side runtime behavior, not SDK or worker behavior.
It is implemented in the lease-acquisition path for both broad polling and
targeted leasing:

- `PollWork`: acquire a candidate low-level lease, run schedule preflight if the
  job has hidden scheduler-side schedule metadata, and return the lease only
  after preflight succeeds.
- `GetJobLease`: apply the same preflight to the targeted job. If the targeted
  occurrence is cancelled by preflight, return `lease: null`.
- Embedded/direct runtimes should use the same runtime preflight code in their
  `PollWork` and `GetJobLease` implementations. The worker runner must not be
  the component that enforces schedules.

The low-level scheduler lease is internal until preflight succeeds, unless the
occurrence has already crossed the started boundary indicated by Strata story
metadata. App workers never receive a lease for a scheduled occurrence that is
both unstarted and invalid under the current schedule row.

For an occurrence `J_k`, hidden metadata contains:

```json
{
  "app": {
    "owner": "analytics"
  },
  "internal": {
    "schedule": {
      "kind": "schedule_tick",
      "scheduleId": "daily-cleanup",
      "generation": 8,
      "specHash": "sha256:...",
      "scheduledAt": "2026-06-17T13:00:00Z",
      "runId": "20260617T130000000000000Z",
      "manual": false,
      "previousJobId": "swfsched_daily-cleanup_g8_run_20260617T120000000000000Z",
      "failureHistory": {
        "bits": "101101",
        "windowSize": 10
      }
    }
  }
}
```

The stored pgwf/SQLite job metadata object is an envelope. `app` is the
application metadata object. `internal` is the runtime-owned namespace.
App-facing metadata APIs return only `app`.

`failureHistory.bits` contains completed outcomes through `J_{k-2}`. The most
recent previous job, `J_{k-1}`, is fetched during `J_k` preflight, appended to
the window, and carried into `J_{k+1}`.

Preflight algorithm:

1. Acquire a candidate low-level scheduler lease.
2. If the job has no `internal.schedule` metadata, return the lease normally.
3. Read Strata story metadata for this job and inspect the chapter count. Do not
   read chapter bodies.
4. If `chapter_count > 1`, app execution has already written at least one
   post-start chapter. Skip schedule validation and return the lease.
5. Load the `swf_schedules` row.
6. If the schedule is paused, archived, missing, past `endAt`, or
   generation/spec do not match, record a scheduler-side schedule cancellation
   outcome, complete the job as `CANCELLED`, and do not return the lease.
7. If `previousJobId` is present, load that terminal job and append one outcome
   bit to the hidden failure window.
8. Evaluate failure policy. If violated, record a scheduler-side schedule
   cancellation outcome with reason `failure_policy`, complete the job as
   `CANCELLED`, and do not submit a successor.
9. Compute the next fire time.
10. Submit `J_{k+1}` idempotently using the schedule target shape from the
   validated schedule row.
11. Return the lease to the app worker. App code runs normally after that point.

The next occurrence is submitted before the lease is returned to app code, so
the schedule chain does not depend on the current app job succeeding or on a
client doing follow-up administrative work. Because v1 uses serial semantics,
the next occurrence waits for the current one to become terminal before it can
start.

If the process crashes after submitting `J_{k+1}` but before app execution
writes a second chapter, retrying `J_k` preflight repeats the same deterministic
submit and converges if the schedule is still valid.

If the process crashes before app execution writes a second chapter and the
schedule is paused, archived, updated, or failure-paused before the next lease
attempt, `J_k` may be cancelled on revalidation. V1 accepts this edge case to
avoid a mutable per-job preflight marker and any new scheduler mechanism.

If app execution has written a second chapter, retrying `J_k` skips schedule
validation even if the schedule changed later. The occurrence has clearly
started from the app's perspective and should run/retry normally.

If the process crashes after acquiring the low-level lease but before recording
any schedule outcome, the lease eventually expires and another lease attempt
repeats preflight.

### Scheduled Occurrence Metadata And Started Boundary

SWF should add immutable scheduler-side metadata for scheduled occurrences. This
metadata lives with pgwf or the pgwf-like scheduler tables, not in the app
chapter stream. It must be readable in the lease path before opening any chapter
body.

The metadata is:

- hidden from app `JobData`, app metadata, and deterministic app replay
- visible to runtime/admin inspection and schedule run projection
- excluded from app task ordinal semantics
- authoritative for identifying a job as a scheduled occurrence

Suggested pgwf/SQLite job metadata shape:

```json
{
  "app": {
    "owner": "analytics",
    "_swf": "ordinary app metadata is allowed"
  },
  "internal": {
    "schedule": {
      "kind": "schedule_tick",
      "scheduleId": "daily-cleanup",
      "generation": 8,
      "specHash": "sha256:...",
      "scheduledAt": "2026-06-17T13:00:00Z",
      "runId": "20260617T130000000000000Z",
      "manual": false,
      "previousJobId": "swfsched_daily-cleanup_g8_run_20260617T120000000000000Z",
      "failureHistory": {
        "bits": "101101",
        "windowSize": 10
      }
    }
  }
}
```

In the Postgres/direct runtime, this is stored in the pgwf job record's
`metadata` JSONB column. In SQLite, this is stored in the SQLite scheduler job
record's `metadata` column. There is no Postgres sidecar table for occurrence
metadata.

The `internal` object is runtime-owned metadata and is hidden from app metadata
APIs. Public submit APIs treat user-provided metadata as the `app` payload, so
there are no reserved app metadata keys. Public `GetJob` / `ListJobs` / lease
responses return only the `app` object. Runtime/admin schedule APIs may expose a
projected schedule view derived from `internal`.

The important constraint is locality: lease acquisition reads schedule identity
and failure-window metadata from the scheduler database. The only Strata read in
the scheduled lease path is a story metadata read for chapter count.

Started-boundary rule:

```text
chapter_count > 1
```

Each job story starts with chapter `0`. Once chapter count is greater than one,
app execution has written at least one post-start chapter. At that point the
runtime skips parent schedule validation and returns the lease normally.

When `chapter_count == 1`, the runtime treats the occurrence as not yet started
for schedule purposes and may revalidate or cancel it before app code resumes.
This means a crash after successor submission but before the second chapter can
lead to revalidation and cancellation on the next lease attempt.

Scheduled cancellation detail payload:

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
schedule_ended
schedule_generation_mismatch
schedule_spec_mismatch
failure_policy
```

The job terminal state for these cases is `CANCELLED`. The cancellation reason
should be copied into completion detail. That completion detail is the durable,
portable explanation.

The server writes completion detail while holding the candidate lease, then
completes the occurrence under that lease as `CANCELLED`. It should not rely on
an out-of-band public cancel call for schedule preflight outcomes.

Admin APIs may project scheduled cancellation outcomes as system events or
system chapters, but those projections are not part of the lease decision path.

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
    JobID:       "swfsched_daily-cleanup_g8_run_20260617T140000000000000Z",
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

The occurrence chain carries a compact binary window in hidden scheduler-side
metadata:

- `1` means the prior target job completed successfully
- `0` means the prior target job failed, was cancelled, or otherwise did not
  produce successful app output

For occurrence `J_k`, the hidden window contains outcomes through `J_{k-2}`.
During preflight, the runtime server loads `J_{k-1}`, appends its outcome,
trims to `windowSize`, and evaluates both configured conditions:

- success percentage over the available window is at least `minSuccessPercent`
- sequential failures are fewer than `maxSequentialFailures`

If either condition fails, `J_k` is cancelled before app start with a
`failure_policy` completion detail and no successor is submitted.

This means failure policy is enforced when the next scheduled occurrence wakes
up. It does not require an out-of-band health process. Immediate failure status
at the moment `J_{k-1}` completes would require a separate terminal hook and is
not part of v1.

## Schedule API

### Upsert Schedule

```text
PUT /v1/tenants/{tenantId}/schedules/{scheduleId}
```

Creates or updates the schedule row.

`target.data` uses the same `TaskDataWrite` shape as `SubmitJob.data`. Artifact
uploads in the target are persisted into the schedule target snapshot, and
returned schedule projections include the stored target as normal task data.

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
    "data": {
      "data": { "bucket": "reports" },
      "artifacts": []
    },
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
  "tenantId": "tenant-a",
  "scheduleId": "daily-cleanup",
  "scheduleKey": {
    "tenantId": "tenant-a",
    "scheduleId": "daily-cleanup"
  },
  "state": "ACTIVE",
  "effectiveState": "ACTIVE",
  "generation": 8,
  "specHash": "sha256:...",
  "trigger": {
    "kind": "cron",
    "expression": "0 12 * * *",
    "timezone": "UTC",
    "startAt": "2026-06-17T00:00:00Z"
  },
  "target": {
    "jobType": "daily_cleanup",
    "data": {
      "data": { "bucket": "reports" },
      "artifacts": []
    },
    "runPolicy": {},
    "metadata": { "owner": "analytics" }
  },
  "overlapPolicy": "serial",
  "failurePolicy": {
    "minSuccessPercent": 80,
    "windowSize": 10,
    "maxSequentialFailures": 3
  },
  "nextFireAt": "2026-06-17T13:00:00Z",
  "nextJobKey": {
    "tenantId": "tenant-a",
    "jobId": "swfsched_daily-cleanup_g8_run_20260617T130000000000000Z"
  },
  "createdAt": "2026-06-17T12:00:00Z",
  "updatedAt": "2026-06-17T12:00:00Z"
}
```

Semantics:

- If the schedule row does not exist, create it at generation `1`.
- If `expectedGeneration` is present and does not match, return `409`.
- Updating an archived schedule returns `409`.
- Create/update increments generation and resets the hidden failure window.
- If the resulting state is active, the server-side schedule API submits the
  first occurrence for that generation with hidden scheduler-side metadata.
- Existing future jobs from older generations do not need to be found or
  cancelled. When they wake, preflight detects the generation/spec mismatch,
  writes a scheduled cancellation completion detail, and completes them as
  `CANCELLED`.

The public job submit APIs are not responsible for materializing scheduled
occurrences and must not accept hidden scheduler-side schedule metadata from
ordinary clients.

### Get Schedule

```text
GET /v1/tenants/{tenantId}/schedules/{scheduleId}
```

Loads the schedule row and projects state. It may derive `effectiveState` from
the latest scheduled occurrence terminal detail, for example `FAILURE_PAUSED`
when the chain stopped due to failure policy.

### List Schedules

```text
POST /v1/tenants/{tenantId}/schedules/query
```

Projects schedules from indexed `swf_schedules` rows.

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

Updates the schedule row to `PAUSED` and increments generation.

Pause only affects occurrences whose story still has only the start chapter. A
job whose story has more than one chapter continues and retries normally, even
if the schedule is paused later.

Existing future occurrences may be left in place. They will cancel themselves
before app start when they wake and observe the schedule row no longer matches.

### Resume Schedule

```text
POST /v1/tenants/{tenantId}/schedules/{scheduleId}/resume
```

Updates the schedule row to `ACTIVE`, increments generation, resets the hidden
failure window, and files the first occurrence for the new generation.

### Archive Schedule

```text
POST /v1/tenants/{tenantId}/schedules/{scheduleId}/archive
```

Updates the schedule row to `ARCHIVED` and increments generation. Future
occurrences whose stories still have only the start chapter cancel themselves
during preflight. Occurrences with more than one chapter continue normally.

### Trigger Schedule Now

```text
POST /v1/tenants/{tenantId}/schedules/{scheduleId}/trigger
```

Creates a manual occurrence immediately. Manual triggers should carry hidden
scheduler-side schedule metadata if they must honor current schedule
generation/state before app start. They should not automatically join the
recurring serial chain unless the API explicitly requests that behavior.

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

Lists scheduled app jobs by internal schedule metadata and terminal
status/detail.

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
reason from completion detail.

## Lease API Semantics

Schedule administration is part of the runtime server's lease path. The REST
client and app worker protocol stay unchanged.

For `POST /v1/jobs/poll`:

- The server may acquire and consume one or more internal scheduled leases while
  trying to satisfy the poll request.
- A scheduled occurrence that fails preflight is completed as `CANCELLED` and is
  omitted from the `leases` array.
- The server should continue polling candidates until it fills the requested
  limit, no work is available, the long-poll deadline expires, or an internal
  administrative attempt budget is reached.
- Returning an empty `leases` array can therefore mean either no app-runnable
  work exists or only non-runnable scheduled occurrences were consumed.

For `GET /v1/tenants/{tenantId}/jobs/{jobId}/lease`:

- If the targeted job is unscheduled and leaseable, behavior is unchanged.
- If the targeted scheduled occurrence passes preflight, return its lease.
- If the targeted scheduled occurrence is cancelled by preflight, return
  `200` with `lease: null`, matching the existing "not leaseable" shape.
- If schedule preflight cannot complete because of a storage/runtime error,
  fail closed: do not return the app lease. Return an error for targeted lease
  and either return an error or continue to another candidate for poll,
  depending on whether the failure is isolated to one candidate.

This is the key boundary for REST independence: a generic HTTP worker that only
knows how to poll, run, keep alive, and complete jobs gets correct schedule
behavior without embedding schedule logic.

## Metadata

### App Metadata

The app target metadata from the schedule spec is stored under the scheduler
job metadata envelope's `app` field and is passed to the app job in the same
way as normal job metadata. App metadata owns its full key space, including
keys such as `swf_`, `_swf`, `app`, or `internal`.

Public app-facing metadata is the envelope's `app` object. Public metadata
filters are translated to `app.<field>` in pgwf/SQLite.

### Schedule Run Metadata

Schedule rows are found through `swf_schedules`, not through public job
metadata. Schedule list filters should use indexed table columns such as
`tenant_id`, `schedule_id`, `state`, `generation`, and target job type.

Scheduled occurrences are identified by the runtime-owned `internal.schedule`
namespace in the scheduler job record. Schedule-run queries may read that
internal namespace from pgwf/SQLite job metadata; public `ListJobs` metadata
filters only apply to the envelope's `app` object.

### Hidden Scheduler Runtime Metadata

Hidden runtime metadata is persisted inside the scheduler job record's metadata
field. It is not stored in the first chapter and is not exposed to app code or
public app-facing metadata APIs.

This metadata drives server-side lease preflight and carries the serial failure
window. It is immutable after occurrence submission. Only trusted
runtime/schedule APIs may set it.

Storage rule:

- Postgres/direct runtime: store it in the pgwf job record's `metadata` JSONB
  column under the envelope's `internal` key.
- SQLite runtime: store it in the SQLite scheduler job record's `metadata`
  column under the envelope's `internal` key.
- Remote runtime: the HTTP client never sends or receives this field. `swfd`
  reads and writes it locally while handling schedule APIs and lease APIs.

The lease path may read pgwf/SQLite scheduler metadata and the `swf_schedules`
row. It must not read Strata chapters to decide whether to return a lease.

A scheduled occurrence is not fully submitted until both the scheduler job row
and its `internal.schedule` metadata are durable. With pgwf and SQLite this should
be a single scheduler-row insert/update. `internal.schedule` is the scheduled
occurrence marker; a job without that internal namespace is treated as ordinary
app work.

## Deterministic Job IDs

Schedule IDs should be URL-safe and job-ID-safe in v1:

```text
[A-Za-z0-9_-]+
```

Suggested IDs:

```text
<scheduleId>                                      # schedule table key
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

No external scheduler service is required, but schedules are a first-class
runtime storage resource. Postgres/direct and SQLite runtimes should both own
dedicated schedule tables separate from pgwf job rows, SQLite scheduler job
rows, and Strata chapters.

Logical state ownership:

```text
schedule identity             swf_schedules(tenant_id, schedule_id)
schedule spec/generation      swf_schedules row
target start spec             swf_schedules target_json
desired state                 swf_schedules row
next due occurrence           swf_schedules next_fire_at/next_job_id + availableAt
serial chain                  occurrence prerequisites
failure window                hidden scheduler-side metadata on occurrence jobs
preflight execution           runtime server lease path
started boundary              Strata story metadata chapter count
cancellation result           pgwf/SQLite terminal status and completion detail
target run result             normal job terminal state and app chapters
schedule/run projections      schedule rows, internal run metadata, terminal detail
```

Any in-memory cache, timer, or cursor is an optimization only. Correctness must
survive process restart because due occurrences are ordinary jobs in the runtime.
There is no background `swfd` scheduler loop that has to scan schedules and
materialize due work.

## Transaction Model

No cross-job transaction is required.

The schedule API mutates the `swf_schedules` row and, when active, submits the
first occurrence for that generation. After that, the runtime server's lease
preflight reads the schedule row and submits successors idempotently before
returning scheduled occurrence leases to app workers.

Within a single occurrence submit, the scheduler job row and its `internal.schedule`
metadata are written together in the pgwf/SQLite job metadata field. Because
`internal.schedule` is the scheduled-occurrence marker, the runtime treats a job
without that internal namespace as ordinary app work.

Cross-job side effects converge through deterministic explicit job IDs:

- submitting the first occurrence twice converges
- submitting the same successor twice converges
- stale generations cancel themselves during preflight

Crash cases:

- Crash after successor submit and before a second chapter: retry submits the
  same successor and revalidates schedule state.
- Crash before a second chapter followed by pause/update/archive/failure-pause:
  the occurrence may be cancelled on the next lease attempt.
- Crash after a second chapter: retry skips schedule validation and runs app
  code. Normal SWF retry semantics apply.

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
- `404`: schedule row not found
- `409`: expected generation mismatch, archived schedule update, invalid state
  transition, or explicit job ID conflict with a different shape
- `200`: idempotent success or synchronous projection success
- `200` with `lease: null`: targeted scheduled occurrence was not leaseable or
  was consumed by server-side preflight cancellation

## Security And Isolation

- Schedules are tenant-scoped.
- Scheduled jobs are submitted only into the schedule's tenant.
- App code cannot see or set hidden scheduler-side schedule metadata.
- Public submit APIs must treat request metadata as app metadata and must not
  allow clients to write the storage envelope's `internal` namespace.
- Schedule preflight must happen before the lease is returned to app code when
  the Strata story metadata chapter count is `1`. Once chapter count is greater
  than `1`, later lease attempts skip schedule validation.

## Open Questions

- Should `GET /schedules/{id}` return `state: FAILURE_PAUSED`, or keep
  `state: ACTIVE` and expose `effectiveState: FAILURE_PAUSED`?
- How much of internal schedule metadata should be exposed through admin-only
  schedule APIs?
- Should scheduled cancellation projection be admin-only, public on run
  summaries, or both?
- What internal attempt budget should `PollWork` use when it consumes multiple
  non-runnable scheduled occurrences before finding an app-runnable lease?

## Recommendation

Use `swf_schedules` for control-plane state and make recurring occurrences
ordinary app jobs. Store immutable hidden scheduler-side schedule metadata in
pgwf or the SQLite scheduler metadata field. During lease acquisition, scheduled
jobs may do a Strata story metadata read to check chapter count, but they do not
read chapter bodies. The server submits the next occurrence and records clear
cancellation reasons without exposing schedule internals to app code.

This removes controller-worker ownership from `swfd`, avoids tenant-managed
schedule workers, keeps REST clients generic, and advances schedules only when
normal SWF workers request ordinary app work.
