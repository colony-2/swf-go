# Task Worker Timeout Fallback via Alternate Need

**Status**: Implemented  
**Date**: 2026-01-29  
**Author**: Codex

## Problem

When a job step uses `DoTask` but no worker advertises the task capability (e.g., `jobType:taskType`), the job is rescheduled to that capability and stalls. Run-policy invocation/total timeouts are never recorded because the job never returns to the job worker to replay the step. pgwf now supports an **alternate need after a delay**; we use it so stuck task dispatches come back to the job worker, allowing the existing timeout logic to fire and be persisted.

## Goals

- Automatically redirect stranded task-dispatch jobs back to the job worker after the existing task invocation timeout using pgwf's alternate-need support.
- Preserve current timeout semantics: once the job worker replays the task, the existing timeout code records the timeout payload exactly as if it occurred while the task was running.
- Avoid double-executing the task when a real task worker eventually appears; alternate need only unblocks the job worker path.

## Non-Goals

- Changing timeout calculation or payload shapes (scope, retryable flag, etc.).
- Adding new user-facing APIs; the change is internal to swf/pgwf integration.
- Handling child/async jobs; this applies only to task dispatches created by `DoTask`.

## Design

### 1) Dispatch timeout source

- Reuse the task invocation timeout from the effective run policy. If no invocation timeout is set (nil/zero), we do **not** set an alternate need (current behavior).

### 2) Reschedule with alternate need

- In `runner.DoTask`, when no local task worker exists and we reschedule to the task capability, populate pgwf deps with:
  - `next_need`: `<jobWorkerName>:<taskType>` (unchanged primary capability).
  - `alternate_need`: `<jobWorkerName>` (job worker capability).
  - `alternate_after`: task invocation timeout in seconds.
- Keep existing `TaskWait` payload unchanged (`in`, `out`, `next`, `input_hash`) so replay on the job worker still knows which ordinals to use.
- When the task worker successfully leases the job before `alternate_after`, pgwf leasing clears/ignores the alternate need (pgwf behavior); swf does not need extra handling.

### 3) Replay/timeout behavior (unchanged)

- When the alternate need fires, pgwf delivers the job to the job worker capability. The runner replays; when it reaches the same `DoTask`, existing deadline checks (invocation and total) execute using persisted timestamps. If the configured timeout window has already elapsed, `DoTask` emits the timeout payload (`Timeout` scope `invocation` or `total`) and persists it as usual.
- If the timeout is retryable (invocation scope), normal backoff/retry rules apply. If non-retryable (total scope), the job terminates with that timeout.

### 4) Backward compatibility

- If `TaskDispatchTimeout` is zero or missing, behavior is unchanged (no alternate need set).
- Workers that do not run task capabilities are unaffected; only the reschedule path for missing task workers is touched.

## Testing Plan

- **Unit (runner reschedule)**: stub `pgwf.Lease.Reschedule` to capture `JobDependencies`; verify `alternate_need` and `alternate_after` are set to the job worker and invocation timeout when no local task worker exists, and omitted when a worker is present or timeout unset.
- **Integration (fallback triggers timeout)**: build an engine with only the job worker registered; configure a task invocation timeout (e.g., 5s). Start a job that calls `DoTask("slow-task")`. After ~5s, pgwf should re-offer the job to the job worker; on replay, `DoTask` should persist a `Timeout` payload (scope=invocation) at the task ordinal and return that error to the caller.
- **Integration (task worker available)**: register a task worker and keep invocation timeout non-zero. Verify jobs execute via the task worker, no alternate-need lease is observed, and no timeout payload is written.
- **Integration (total timeout)**: set a short total timeout; let the job sit past `alternate_after` without a task worker. On replay, verify the total timeout is recorded (scope=total, retryable=false) and the job finishes terminally.

## Open Questions

- Whether we should persist the alternate-need deadline in the payload for easier debugging/telemetry (not required for correctness).
- Whether we should persist the alternate-need deadline in the payload for easier debugging/telemetry (not required for correctness).
