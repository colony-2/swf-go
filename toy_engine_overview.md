# ToyEngine Overview

ToyEngine is a synchronous, in-memory implementation of the `SWFEngine` interface for unit tests and demos. It skips Postgres/Strata and executes jobs immediately on the caller goroutine—no background runners, leases, or persistence.

## How It Works
- **Construction**: `NewToyEngine(worksets, opts...)` accepts `WorkSet`s plus options for logger and job ID generator.
- **Job start**: `StartJob`/`RestartJob` run inline. A job record is created in memory, marked active, and the job worker is invoked directly. The `StartJob` struct accepts an optional `JobID` field; if provided, it is used as the job identifier, otherwise a new unique ID is generated using ksuid (or the configured ID generator for ToyEngine).
- **Tasks**: `DoTask` looks up the task worker by name in the job’s `WorkSet` and runs it on the same goroutine. `AwaitDuration` sleeps locally; there is no recycle/reschedule. Missing task workers return an error (no pending-handles in v1). `SpawnAsync` is unsupported.
- **Status/results**: `CheckJobStatus` and `GetJobResult` read the in-memory record. A job that errors or panics is still marked `COMPLETED` with the error stored and returned by `GetJobResult`.
- **Cancellation**: `CancelJob` is best-effort. It flips status to `CANCELLED`, calls the job’s cancel func, and records `context.Canceled`. It only has effect while `StartJob` is still running.
- **Run**: `Run(ctx)` is a no-op surface; calling it logs an error because ToyEngine has no background loop.

## Limitations (v1)
- No persistence, leases, retries, or timeouts are honored; `RunPolicy` knobs are ignored.
- No Postgres/Strata: task/job history and artifacts are not stored.
- No rescheduling or await recycling; all waits are direct sleeps.
- No async children (`SpawnAsync`) and no pending-task/`FindTasksWaitingForCapability` flow; missing capability errors immediately.
- Single-process only; not safe for distributed or long-running workloads.

## Next Steps
- Add task pausing/external completion: create pending handles for missing capabilities and surface them via `FindTasksWaitingForCapability` with `Finish` to unblock `DoTask`.
- Honor `RunPolicy` timeouts and retries with deterministic backoff and wait handling.
- Tighten cancellation semantics and observability (timestamps, basic metrics/logging) without adding storage dependencies.
