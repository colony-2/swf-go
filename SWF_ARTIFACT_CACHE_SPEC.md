# SWF Specification: Artifact Cache (Groupcache + Disk)

**Status**: Proposed  
**Date**: 2026-02-08  
**Author**: System

## Problem Statement

Job patterns often consume artifacts in rapid succession (within the same job, sibling tasks, or retries). Today each read of a Strata-backed artifact re-fetches it from remote storage. This creates avoidable latency, network overhead, and higher load on Strata when the same artifact is accessed repeatedly in a short window.

We want a built-in artifact caching capability in swf-go, with a default implementation that uses `groupcache` to coordinate downloads and a local on-disk cache to store artifact bytes between reads.

## Goals

- Provide a first-class artifact caching capability inside swf-go.
- Default to a local on-disk cache with `groupcache`-based singleflight for concurrent reads.
- Avoid repeated remote downloads for the same artifact within a process lifetime.
- Keep the existing `swf.Artifact` API intact for users.
- Maintain safety and correctness: cached artifacts must match the requested artifact.

## Non-Goals

- Distributed cache across hosts (peer-to-peer groupcache clusters are out of scope).
- Cache invalidation for mutable artifacts (Strata artifacts are treated as immutable).
- Changing Strata storage or API behavior.
- Caching artifacts that do not have stable identifiers.

## Proposed Design

### 1) Add a pluggable Artifact Cache interface

Introduce a minimal cache interface in `pkg/swf`:

```go
// ArtifactCacheKey is a stable identity for cached artifacts.
type ArtifactCacheKey struct {
    Anthology string
    Story     string
    ArtifactID string
    Sha256     string
    SizeBytes  int64
}

// ArtifactCache fetches (or returns) a local cached copy of an artifact.
type ArtifactCache interface {
    // Get ensures the artifact is present locally and returns a path + metadata.
    // The fetch function streams bytes into the provided writer on cache miss.
    Get(ctx context.Context, key ArtifactCacheKey, fetch func(ctx context.Context, w io.Writer) error) (CachedArtifact, error)

    // Delete removes a cached artifact, used for explicit invalidation or cleanup.
    Delete(ctx context.Context, key ArtifactCacheKey) error

    // Stats returns best-effort cache statistics for observability.
    Stats() ArtifactCacheStats
}

// CachedArtifact is a local, read-only view of the cached data.
type CachedArtifact struct {
    Path      string
    SizeBytes int64
    Sha256    string
}
```

Notes:
- `ArtifactCacheKey` uses `Anthology`, `Story`, and `ArtifactID` as the primary key and optionally validates `Sha256`/`SizeBytes` when available.
- If `ArtifactID` is empty, caching is skipped (fall back to direct Strata reads).

### 2) Engine configuration hooks

Extend `EngineBuilder` to accept cache configuration:

```go
func (b *EngineBuilder) WithArtifactCache(cache swf.ArtifactCache) *EngineBuilder
func (b *EngineBuilder) WithArtifactCacheDir(path string) *EngineBuilder
func (b *EngineBuilder) WithArtifactCacheLimits(maxBytes int64, maxEntries int) *EngineBuilder
```

Behavior:
- If a cache is not provided, swf-go instantiates the default groupcache-backed disk cache.
- If `WithArtifactCache(nil)` is used, caching is disabled.

### 3) Default implementation: groupcache + local disk

A new internal implementation (e.g., `groupcacheArtifactCache`) will:

- Use a `groupcache.Group` to singleflight concurrent downloads by key.
- Store artifact bytes on disk under a deterministic path derived from the cache key:
  - `cacheDir/<prefix>/<hash>.bin` (prefix shard to avoid large directories).
- Persist metadata in a small adjacent file (size, sha256, created time) or encode into the filename.
- Perform atomic writes: download to `*.tmp`, fsync, then rename.

How groupcache is used:
- The groupcache value is a small string containing the finalized on-disk path.
- On cache miss, the groupcache `Getter` performs the download and writes to disk.
- On cache hit, the caller verifies the file still exists; if missing, the entry is evicted and refetched.

### 4) Eviction policy

Because on-disk cache needs bounds, the default cache will enforce size and entry limits:

- `maxBytes` (default: 10 GiB)
- `maxEntries` (default: 50,000)

Eviction strategy:
- Best-effort LRU based on file mtime or a lightweight index file.
- Eviction runs on writes that exceed limits and on startup when limits are already exceeded.

### 5) Integration points in swf-go

Wrap Strata-backed artifacts with a cache-aware adapter:

- Introduce `cachedStrataArtifact` that implements `swf.Artifact` and uses `ArtifactCache` for reads.
- Methods that read bytes (`Open`, `Bytes`, `WriteTo`, `SaveToFile`, `Sha256`) consult the cache first.
- Metadata methods (`ID`, `Name`, `ContentType`, `Size`) delegate to the underlying Strata artifact.
- `Cleanup()` is a no-op (cache owns its lifecycle; cached files survive artifact cleanup).

Where wrapping occurs:
- `convertStrataArtifacts(...)` in `pkg/swf/impl/runner.go`.
- `buildTaskIOFromPayload(...)` in job run details paths.
- Toy engine artifact materialization paths should also use the cache to avoid redundant fetches.

### 6) Cache consistency and validation

- Strata artifacts are treated as immutable; once cached, the bytes do not change.
- On cache hit, if `Sha256` is known and provided in the key, the cached file hash is checked lazily:
  - If mismatch, delete the cache entry and refetch.
- If hash is unknown, size-only validation is performed.

### 7) Failure behavior

- Cache failures must not block workflow execution.
- If caching fails (download error, disk full, permission error), fall back to direct Strata reads.
- Errors are logged at `WARN` with key details to aid debugging.

### 8) Observability

Expose basic stats through `ArtifactCacheStats`:

```go
type ArtifactCacheStats struct {
    Hits      int64
    Misses    int64
    Evictions int64
    Bytes     int64
}
```

Log entries (debug level) for cache hit/miss and eviction events.

## Compatibility and Migration

- No public API changes to `swf.Artifact` or `TaskData`.
- Existing behavior remains when caching is disabled.
- Strata clients are unchanged.

## Testing Plan

- Unit tests for `groupcacheArtifactCache`:
  - Concurrent `Get` calls only download once.
  - Cache hit returns local path without calling fetch.
  - Eviction removes oldest entries when limits are exceeded.
- Integration tests:
  - A fake Strata artifact with a counting fetcher is wrapped; multiple reads only fetch once.
  - Disk cache survives repeated reads within a single engine run.
  - Cache failures gracefully fall back to remote reads.

## Open Questions

- Default cache directory location: `/var/tmp/swf/artifacts`, `$XDG_CACHE_HOME/swf/artifacts`, or a configurable builder default?
- Should cache statistics be exposed via engine metrics hooks?
