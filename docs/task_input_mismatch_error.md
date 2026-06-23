# Task Input Mismatch Error (Cached Data Return)

**Status**: Implemented  
**Date**: 2026-01-29  
**Audience**: Job/task worker authors and platform integrators

## What changed

When a task replays and hits a cached chapter whose `InputHash` does not match the hash of the current `DoTask` input, JobDB still fails with `ErrWorkflowNotDeterministic`, **but now returns the cached chapter data in a structured error** so workers can inspect or recover.

## API surface

- New error type: `workflow.TaskInputMismatchError`
  - Unwraps to `ErrWorkflowNotDeterministic` (so `errors.Is(err, ErrWorkflowNotDeterministic)` still works).
  - Fields exposed via accessors:
    - `CachedTaskData()` → `jobdb.TaskData` (cached payload + artifacts, when rehydration succeeds)
    - `CachedTaskDataErr()` → error encountered while rehydrating the cached payload
    - `CachedInputPayload()` → stored task input (if task-input storage is enabled)
    - `ChapterMeta()` → `workflow.TaskDeterminismMeta` (ordinal, task type, worker ID, attempts, hashes, timing/backoff hints, input ref, run policy, envelope version)
- Helper: `workflow.UnexpectedChapter(err) (TaskInputMismatchError, bool)` for ergonomic extraction.

## Worker usage pattern

```go
out, err := ctx.DoTask(policy, taskName, input)
if err != nil {
    if mismatch, ok := workflow.UnexpectedChapter(err); ok {
        // Determinism guard tripped; consume cached data if desired
        cached := mismatch.CachedTaskData()
        // Decide whether to fail fast, log, alert, or continue with cached output
        return cached, nil // or return err to propagate
    }
    return nil, err
}
return out, nil
```

Notes:
- If `CachedTaskDataErr()` is non-nil, the cached payload couldn't be rehydrated; handle accordingly.
- When task-input storage is disabled, `CachedInputPayload()` will be empty.

## Rationale

Previously, a hash mismatch only raised `ErrWorkflowNotDeterministic`, leaving job workers blind to the already-persisted chapter. Returning the cached data allows:
- Recovery paths (e.g., accept cached output when divergence is intentional).
- Better observability (log what was actually stored).
- Safer migrations where inputs evolve but replay is still useful for inspection.

## Behavior compatibility

- Callers in the `workflow` package that check `errors.Is(err, ErrWorkflowNotDeterministic)` keep working after this error type wraps the determinism sentinel.
- No behavior change for other determinism failures (e.g., missing input hash, async child mismatches).

## Files touched

- `pkg/workflow/determinism.go` — new error type, meta struct, helper
- `pkg/workflow/worker_runner.go` — returns `TaskInputMismatchError` on task input hash mismatch
- `pkg/workflow/determinism_test.go` — coverage for helper/accessors

## Testing

Run `go test ./pkg/workflow ./pkg/jobdb/...` to exercise the new behavior (already added to CI suite).
