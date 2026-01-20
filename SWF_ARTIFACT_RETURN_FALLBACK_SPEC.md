# SWF Specification: Resilient Task Output Artifacts

**Status**: Proposed  
**Date**: 2026-01-20  
**Author**: System

## Problem Statement

`DoTask` returns `swf.TaskData` that may include artifacts created from temporary files or one-shot readers. After the task completes, SWF persists those artifacts to Strata and then calls `Cleanup()` on the original artifacts. The returned artifacts therefore become unusable if a caller tries to read them after cleanup (e.g., the file was removed or the reader was consumed).

We need `DoTask` to return artifacts that prefer the original local data when it is still available, but transparently fall back to the persisted Strata copy after the local data has been cleaned up.

## Goals

- Return task output artifacts that remain readable after SWF cleanup.
- Prefer local artifacts for immediate reads to avoid unnecessary remote fetches.
- Fall back to Strata when local artifacts are unavailable due to cleanup.
- Preserve existing `swf.Artifact` API for callers.

## Non-Goals

- Changing how artifacts are persisted to Strata.
- Introducing new public APIs for task authors to explicitly manage fallback.
- Modifying the Strata server behavior or storage model.

## Proposed Design

### 1) Add a resilient artifact wrapper

Introduce an internal decorator `fallbackArtifact` (name TBD) that implements `swf.Artifact` by composing:
- `primary` (the original task output artifact), and
- `fallback` (a Strata-backed artifact handle for the persisted copy).

Behavior:
- On read operations (`Open`, `Bytes`, `WriteTo`, `SaveToFile`, `Sha256`):
  - Try `primary` first.
  - If `primary` fails due to local cleanup/unavailability, retry using `fallback`.
- On metadata methods (`ID`, `Name`, `ContentType`, `Size`):
  - Prefer `primary` values when available; fall back to `fallback` if the primary is missing.
- `Cleanup()` only cleans up the `primary` artifact. It must not delete or mutate the Strata-backed copy.

### 2) Fallback on any read failure

Because `swf.Artifact` is an open interface and implementations are not guaranteed to return a shared sentinel error, the fallback wrapper should treat **any** error from the primary artifact as a signal to try the Strata-backed copy.

Fallback logic in the decorator should:
- Attempt the primary read first.
- If the primary returns an error, immediately retry using the fallback.
- If fallback also fails, return the fallback error (and optionally include the primary error in logs).

### 3) Define a minimal Strata remote artifact type

Add a new internal artifact implementation that represents a Strata-backed artifact by reference, without holding the full Strata object in memory:

```
type strataRemoteArtifact struct {
    anthology string
    story     string
    artifactID string

    // Metadata copied from primary at wrap time.
    name        string
    contentType string
    sizeBytes   int64
    sha256      string

    // Client used to fetch the artifact on demand.
    client *strata.Client // exact type TBD
}
```

Responsibilities:
- `Open/Bytes/WriteTo/SaveToFile/Sha256` use the stored client to fetch the artifact via standard Strata APIs.
- `Name/ContentType/Size` return the copied metadata (no network call).
- `Cleanup` is a no-op (never deletes remote artifacts).

### 4) Wrap task output artifacts after persistence

In the `DoTask` success path (and error path where artifacts are persisted), after `SaveChapter` succeeds:

1. Build `strataRemoteArtifact` values using:
   - the current story key (anthology + story),
   - the artifact IDs returned by the Strata save path,
   - metadata copied from the primary artifact (name, content type, size, sha256).
2. For each original output artifact, create a `fallbackArtifact(primary, strataRemoteArtifact)`.
3. Replace the artifacts in the `TaskData` result **immediately before cleanup** so the wrapper can copy primary metadata while it is still valid.
4. Proceed with existing cleanup of the original artifacts.

Cached/replay paths already return Strata-backed artifacts and do not need wrapping.

### 5) Artifact ordering and matching

Artifacts must be matched between `primary` and `fallback` deterministically. Proposed match rules:
- Prefer index-based matching when the chapter preserves artifact order.
- If needed, validate using name + size + sha256 to avoid mismatches.

On mismatch, fall back to returning the Strata artifact directly and log a warning.

## API and Implementation Notes

### Internal wrapper type

```go
type fallbackArtifact struct {
    primary  swf.Artifact
    fallback swf.Artifact
}
```

### Error classification

No error contract is required. The wrapper treats all primary read errors as fallback triggers.

### Client access

`strataRemoteArtifact` requires access to a Strata client to retrieve the artifact on demand. The wrapper should carry a reference to the engine's Strata client (or a minimal interface) that is safe for concurrent use.

### Thread-safety

The wrapper should be safe to call concurrently, but it may rely on the underlying artifacts' guarantees. Do not cache `Open()` results; each call should independently try primary then fallback.

## Acceptance Criteria

- After `DoTask` returns, callers can read artifacts even if the original files are deleted by cleanup.
- Cached/replay artifacts remain unchanged and continue to work.
- Errors unrelated to cleanup are surfaced as-is and do not silently fall back.

## Testing Plan

- Unit tests for `fallbackArtifact`:
  - Primary succeeds: no fallback used.
  - Primary returns `ErrArtifactUnavailable`: fallback used.
  - Primary returns non-unavailable error: error propagated.
- Integration test:
  - Task writes output artifact to a temp file.
  - `DoTask` returns output, cleanup removes the file.
  - Artifact read succeeds via Strata fallback.
