# JobDB Public API Surface

## Status

**Current reference** | Author: Codex | Updated: 2026-06-23

This document defines which importable packages are intentionally public after
the `jobdb` / `workflow` split. The API snapshot should track only packages
listed here as supported public API.

## Supported Public Packages

### `github.com/colony-2/jobdb/pkg/jobdb`

Runtime Go API and REST-compatible data model. This package is public, but no
longer contains the worker SDK or engine builder.

This includes task/job data APIs, artifact APIs, runtime request/response
types, job lifecycle APIs, job-run inspection APIs, list-jobs APIs, first-class
schedule APIs, error types, and the `WorkflowRuntime` interface used by runtime
adapters and advanced consumers.

The schedule surface includes `UpsertSchedule`, `GetSchedule`,
`ListSchedules`, `PauseSchedule`, `ResumeSchedule`, `ArchiveSchedule`,
`TriggerSchedule`, `ListScheduleRuns`, and their request/response structs.
`ScheduleTarget` is intentionally job-start-like: it carries target job type,
input `TaskData` including artifacts, run policy, and app metadata.

### `github.com/colony-2/jobdb/pkg/workflow`

Higher-level workflow SDK built on top of `pkg/jobdb`. This package owns the
worker-facing API and engine orchestration surface.

This includes `Engine`, `EngineBuilder`, `JobWorker`, `TaskWorker`,
`JobContext`, `TaskContext`, worker loops, replay execution, task discovery,
run-if-leaseable helpers, and worker-level determinism errors.

This package may alias stable lower-level `jobdb` data types for ergonomics,
but moved worker symbols are not re-exported from `pkg/jobdb`.

### `github.com/colony-2/jobdb/pkg/jobdb/runtime/remote`

REST-backed runtime client/server adapter. This package is public and is used
by c2j.

Remote lease operations use runtime-minted lease tokens. Poll and targeted
lease responses include a `leaseToken`; lease-mutating HTTP calls must present
that token in `X-JobDB-Lease-Token`. Keepalive returns a fresh token for the
renewed lease.

### `github.com/colony-2/jobdb/pkg/jobdb/runtime/sqlite`

SQLite-backed runtime. This package is public and is used by c2j for embedded
local execution.

### `github.com/colony-2/jobdb/pkg/jobdb/runtime/toy`

In-memory runtime for tests and local execution. This package is public and is
used by c2j tests and standalone execution paths.

### `github.com/colony-2/jobdb/pkg/jobdb/runtime/direct`

Postgres/Strata direct runtime. This package is public for compatibility with
existing users and current JobDB commands, even though c2j no longer imports it
directly.

Do not expand this package's public API during the protobuf migration unless
there is a separate design decision to keep direct/Postgres as a long-term
runtime surface.

The direct, SQLite, and toy runtime packages expose lease transport helpers such
as `KeepAliveLeaseByIDWithExpiry` because `remote.NewServer` needs a consistent
adapter surface to renew leases and mint replacement lease tokens. These helper
methods are runtime-adapter API, not application workflow API.

## Internal-Only Packages

### `github.com/colony-2/jobdb/pkg/jobdb/internal/...`

Internal implementation and test support. These packages are not part of the
public API snapshot.

### `github.com/colony-2/jobdb/pkg/internal/...`

Shared implementation and test support used by multiple top-level packages.
These packages are not part of the public API snapshot.

### `github.com/colony-2/jobdb/pkg/jobdb/runtime/*/internal/...`

Runtime implementation details. These packages are not part of the public API
snapshot.

## Removed From Public Surface

### `github.com/colony-2/jobdb/pkg/jobdb/runtime/direct/testsupport`

This package was test infrastructure for embedded Postgres/Strata setup. It is
not used by c2j and is not intended for downstream runtime construction. It has
been moved under `pkg/internal/directtestsupport`.

## API Snapshot Packages

The API snapshot should include:

```text
github.com/colony-2/jobdb/pkg/jobdb
github.com/colony-2/jobdb/pkg/workflow
github.com/colony-2/jobdb/pkg/jobdb/runtime/direct
github.com/colony-2/jobdb/pkg/jobdb/runtime/remote
github.com/colony-2/jobdb/pkg/jobdb/runtime/sqlite
github.com/colony-2/jobdb/pkg/jobdb/runtime/toy
```

No generated protobuf storage package should be added to this list unless it
is explicitly promoted to public API.
