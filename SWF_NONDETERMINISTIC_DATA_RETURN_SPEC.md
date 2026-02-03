# SWF Specification: Return Cached Chapter Data on Input Hash Mismatch

**Status**: Proposed  
**Date**: 2026-01-29  
**Author**: Codex

## Problem Statement

When `runner.DoTask` encounters a cached chapter whose `InputHash` does not match the hash of the current `DoTask` input, it aborts replay and returns `ErrWorkflowNotDeterministic`. The persisted chapter already contains the serialized payload (and usually the stored task input plus artifacts), but callers receive only the generic determinism error. Job workers that want to recover or reinterpret the unexpected cached output have no structured way to access it, forcing them to re-fetch chapters directly from Strata or to give up.

## Goals

- Preserve the deterministic guard: a hash mismatch must still surface as `ErrWorkflowNotDeterministic`.
- Enrich the returned error with the persisted chapter data so job workers can programmatically inspect/consume it.
- Keep existing callers that only check `errors.Is(err, ErrWorkflowNotDeterministic)` working without changes.
- Avoid extra Strata reads beyond what the replay path already performs.

## Non-Goals

- Changing how input hashes are computed or validated.
- Auto-resolving non-determinism or silently continuing execution.
- Extending this behavior to other determinism failures (e.g., missing input hash, async child mismatches) in this change; those remain unchanged.

## Proposed Design

### 1) New error type carrying cached data

Add `swf.TaskInputMismatchError` (name TBD) in `pkg/swf`:

- Fields: `TaskType`, `Ordinal`, `CachedInputHash`, `ComputedInputHash`, `CachedInput json.RawMessage` (if stored), `CachedOutput swf.TaskData`, `Meta chapterMeta` (or a minimal exported view).
- Methods:
  - `func (e TaskInputMismatchError) Error() string`
  - `func (TaskInputMismatchError) Unwrap() error` → `ErrWorkflowNotDeterministic` (preserves `errors.Is` behavior).
  - Accessors: `CachedTaskData() swf.TaskData`, `CachedInputPayload() []byte`, `ChapterMeta() TaskDeterminismMeta`.

Expose a minimal exported struct `TaskDeterminismMeta` with the deterministic fields we already persist: ordinal, attempt, maxAttempts, createdAt, workerID, taskType, inputHash, backoff/retry hints, inputRef, runPolicy pointer, and (optionally) `InputPayload` when task-input storage is enabled.

### 2) Populate the enriched error in the cached-path mismatch

In `runner.DoTask`, inside the cached chapter path:

1. Decode the chapter envelope as today.
2. If `InputHash` is empty → keep existing `ErrMissingInputHash` path (no cached data attached).
3. If `InputHash != computed`:
   - Convert Strata artifacts via the existing `convertStrataArtifacts`.
   - Rehydrate the cached payload into `swf.TaskData` using `envelopeToTaskData(env, artifacts)`. Do **not** treat payload errors as fatal here; include them in the returned error (e.g., `CachedOutputErr`) so consumers know if materialization failed.
   - Copy the deterministic metadata from `env.Meta` (including `Input` when present) into `TaskDeterminismMeta`.
   - Log the mismatch as before, but avoid double Strata reads.
   - Return `nil, TaskInputMismatchError{...}` so callers can `errors.Is(err, ErrWorkflowNotDeterministic)` or `errors.As(err, &TaskInputMismatchError{})` to get the cached data.

No cleanup should run on the cached artifacts; they remain as-is because they are Strata-backed.

### 3) Helper for ergonomic access

Add a small helper in `pkg/swf`:

```go
func UnexpectedChapter(err error) (TaskInputMismatchError, bool)
```

It wraps `errors.As` to give job workers a one-liner to extract cached data without importing the concrete type.

### 4) API and compatibility considerations

- Existing behavior for deterministic success and other error types remains unchanged.
- `errors.Is(err, ErrWorkflowNotDeterministic)` continues to work because of `Unwrap`.
- Callers that ignore the new type see no behavioral change (still a determinism error, no result).
- Task input storage toggle: if disabled, `CachedInputPayload` is empty but `CachedOutput` and metadata are still provided.
- Payload rehydration failure: the error should carry the failure reason; `CachedOutput` may be `nil` in that case to avoid panics downstream.

## Acceptance Criteria

- When a cached task chapter’s `InputHash` mismatches the computed hash, `DoTask` returns an error `errors.Is(..., ErrWorkflowNotDeterministic)` **and** `errors.As(..., TaskInputMismatchError)` succeeds.
- `TaskInputMismatchError` exposes:
  - The cached output payload as `swf.TaskData` (payload + artifacts) when it can be rehydrated.
  - The stored input payload when task-input storage is enabled.
  - Determinism metadata (ordinal, task type, worker ID, attempt, hashes).
- No extra Strata round-trips beyond the existing cached read are introduced.
- Existing determinism tests continue to pass; new tests validate the enriched error shape and contents.

## Testing Plan

- Unit test in `pkg/swf/impl/runner_test.go` (or toy suite) that:
  1. Executes a task, caches chapter 1.
  2. Replays with mutated input to force a hash mismatch.
  3. Asserts `errors.As` to `TaskInputMismatchError` succeeds and that `CachedTaskData()` returns the original payload/artifacts; `CachedInputPayload` matches the first run input when storage is enabled.
- Unit test for the helper `UnexpectedChapter`.
- Negative test: when `InputHash` is missing, behavior is unchanged (`ErrMissingInputHash`, no cached data).
- Concurrency/regression: rerun existing determinism and retry suites to ensure no behavior drift.

## Rollout Notes

- This is an additive API surface; no migration is required for existing workers.
- Document the new error type in the SWF developer docs, encouraging job workers that want recovery/inspection hooks to gate on `UnexpectedChapter(err)`.
