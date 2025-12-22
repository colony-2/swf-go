# ToyEngine (In-Memory SWF Engine)

Lightweight, dependency-free `SWFEngine` implementation meant for unit tests and quick demos. Runs entirely in process memory: no Postgres, no Strata, no pgwf. Executes jobs synchronously end-to-end without rescheduling or persistence.

## Goals
- Provide a zero-dependency engine that satisfies the `SWFEngine` interface for fast local testing.
- Run a job worker and its task workers inline, in order, within the same goroutine and process.
- Keep API behavior close enough to the real engine for consumer code to compile and basic flows to run.
- Minimal surface for debugging: simple status, result, and error tracking in memory.

## Non-Goals
- No persistence or recovery: state disappears with the process.
- No rescheduling, retries, await recycling, or background task leasing.
- No Strata chapters, artifacts storage, or payload envelopes.
- No cross-process coordination or distributed task dispatch.
- No async child jobs (`SpawnAsync`). External task claiming is a future/phase 2 item.

## API Shape
- New constructor in `pkg/swf/toy`: `NewToyEngine(workers []swf.WorkSet, opts ...Option) (swf.SWFEngine, error)`.
  - Options: logger override, optional job ID generator hook (defaults to ksuid).
- Implements `SWFEngine` fully:
  - `StartJob`/`RestartJob`/`CancelJob`/`CheckJobStatus`/`GetJobResult`
  - `FindTasksWaitingForCapability`
  - `RegisterWorkers`
  - `Run(ctx)` (required by `loopWorkerApi`) but calling it returns an error (unsupported).

## Execution Model
- Jobs run in memory and complete in a single process lifetime.
- `Run(ctx)` must not be used; it returns an error immediately.
- `StartJob` executes the job synchronously on the caller goroutine; there is no background worker pool.
- `RestartJob` executes synchronously the same way; it just seeds the initial data with the provided restart payload.
- No reschedules: waits (`AwaitDuration`) and retries are honored only as direct sleeps within the running goroutine.
- Tasks are executed strictly in the order the job worker invokes `DoTask`; there is no goroutine hop. Missing capabilities currently return an error (task pausing is phase 2).

## Storage Model
- Per-job in-memory record:
  - `status`: one of `READY`, `ACTIVE`, `COMPLETED`, `CANCELLED`
  - `result`: final `TaskData` on success
  - `err`: terminal error (completed-with-error)
  - `ctxCancel`: cancel function used by `CancelJob`
  - `startedAt`/`finishedAt` timestamps (optional, for observability)
- Task inputs/outputs are not stored beyond the job lifecycle; no history is retained.

## Method Behaviors
- `RegisterWorkers`: populates the engine’s worker map; duplicate job names error.
- `StartJob`:
  - Use provided `JobID` if present in the `StartJob` struct; otherwise generate a new job ID (using the configured generator or ksuid by default). Create a job record in `READY`, then mark `ACTIVE` immediately.
  - Clone the provided `RunPolicy` but ignore retries/reschedule knobs and timeouts in v1.
  - Run the job worker synchronously on the caller goroutine; when it returns, mark `COMPLETED` and store `result` or `err`.
- `RestartJob`: treated as `StartJob` that seeds the job worker input with `restart.Data`; `LastStepToKeep` is ignored (no history to trim).
- `Run(ctx)`: returns an error indicating ToyEngine does not support background runners.
- `CancelJob`: best-effort cancel; if the job is `ACTIVE`, call its cancel func and set status to `CANCELLED` once the goroutine exits. If already terminal, no-op. Cancel has no effect if `StartJob` already returned.
- `CheckJobStatus`: reads the in-memory record; no `CRASH_CONCERN` state—panics are captured as errors and treated as completed-with-error.
- `GetJobResult`: returns stored `result`; if the job ended with an error, return that error even though status is `COMPLETED`.
- `FindTasksWaitingForCapability`: returns an empty slice in v1 (pending-task support is phase 2).

## Job/Task Execution
- Job worker is invoked with a simple `JobContext` that:
  - Implements `DoTask` by looking up the task worker by name and running it inline on the same goroutine with a `TaskContext` containing the current step, job ID, logger, and a direct `AwaitDuration` sleep.
  - If the task worker for the capability is missing, `DoTask` returns an error (phase 2 will add pending/pausing).
  - `AwaitDuration` uses `time.Sleep` (or context-aware sleep) and never recycles.
  - `SpawnAsync` returns an explicit “unsupported” error.
- `DoTask` ignores task-level retries and history; it passes through the input and returns the task worker output or error.
- Panics in job or task worker are captured and surfaced as errors; job status is still `COMPLETED` with the error stored.

## Phase 2 (Not in first cut)
- Task pausing / external completion: missing capabilities create pending handles discoverable via `FindTasksWaitingForCapability`, with `Finish` to unblock `DoTask`.
		Capability naming matches the real engine: `<jobWorkerName>:<taskWorkerName>`.
		When `DoTask` is called:
		  - If the capability exists in the registered `WorkSet`, execute the task worker inline and return its output.
		  - If the capability is missing, create a pending task handle:
		  - Store the input `TaskData`, job ID, step, and a completion channel.
		  - Add it to the pending registry for that capability (FIFO).
		  - Block the `DoTask` call waiting for `Finish` to be invoked on that handle.
		  - Multiple pending tasks for the same capability are queued FIFO.
		  `FindTasksWaitingForCapability(ctx, jobType, taskType)`:
		  - Builds the capability name and returns shallow copies of the current pending handles (non-destructive).
		  - Handles expose `Data()` to retrieve the original input and `Finish(ctx, output TaskData)` to deliver completion.
		  - `Finish` removes the handle from the pending queue and unblocks the waiting `DoTask`, supplying the provided output (or error).
		  Because `StartJob` runs synchronously, external callers can drive `FindTasksWaitingForCapability`/`Finish` from another goroutine to simulate user input while the job goroutine is blocked.
		  - Retry and timeout enforcement: honor `RunPolicy` retries and invocation/total timeouts, including deterministic sleeps for backoff.

## Testing Strategy
- Unit tests in `pkg/swf/toy` covering:
  - Happy-path job with multiple tasks executes in order and stores final result.
  - Cancellation transitions status and stops execution when `CancelJob` called mid-run.
  - Unsupported paths (missing task worker, SpawnAsync) return clear errors.
- Optional small integration test ensuring `WaitForJobToComplete` works against ToyEngine.

## Usage Notes
- Intended for hermetic unit tests and examples; not production ready.
- Because there is no persistence, consumers should not rely on `RestartJob` or replay semantics for ToyEngine.
- Logging is best-effort; defaults to `slog.Default()` unless overridden via options.
