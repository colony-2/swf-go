# SWF-Go Public API Surface

## Status

**Current reference** | Author: Codex | Updated: 2026-06-21

This document defines which importable packages are intentionally public before
the protobuf storage migration. The API snapshot should track only packages
listed here as supported public API.

## Supported Public Packages

### `github.com/colony-2/swf-go/pkg/swf`

Primary application SDK and engine API. This package is public and should stay
source-compatible through the protobuf storage migration.

This includes task/job data APIs, artifact APIs, engine construction, job
lifecycle APIs, replay/job-run inspection APIs, list-jobs APIs, first-class
schedule APIs, error types, and the `WorkflowRuntime` interface currently used
by runtime adapters and advanced consumers.

The schedule surface includes `UpsertSchedule`, `GetSchedule`,
`ListSchedules`, `PauseSchedule`, `ResumeSchedule`, `ArchiveSchedule`,
`TriggerSchedule`, `ListScheduleRuns`, and their request/response structs.
`ScheduleTarget` is intentionally job-start-like: it carries target job type,
input `TaskData` including artifacts, run policy, and app metadata.

### `github.com/colony-2/swf-go/pkg/swf/runtime/remote`

REST-backed runtime client/server adapter. This package is public and is used
by c2j.

Remote lease operations use runtime-minted lease tokens. Poll and targeted
lease responses include a `leaseToken`; lease-mutating HTTP calls must present
that token in `X-SWF-Lease-Token`. Keepalive returns a fresh token for the
renewed lease.

### `github.com/colony-2/swf-go/pkg/swf/runtime/sqlite`

SQLite-backed runtime. This package is public and is used by c2j for embedded
local execution.

### `github.com/colony-2/swf-go/pkg/swf/runtime/toy`

In-memory runtime for tests and local execution. This package is public and is
used by c2j tests and standalone execution paths.

### `github.com/colony-2/swf-go/pkg/swf/runtime/direct`

Postgres/Strata direct runtime. This package is public for compatibility with
existing users and current swf-go commands, even though c2j no longer imports
it directly.

Do not expand this package's public API during the protobuf migration unless
there is a separate design decision to keep direct/Postgres as a long-term
runtime surface.

The direct, SQLite, and toy runtime packages expose lease transport helpers such
as `KeepAliveLeaseByIDWithExpiry` because `remote.NewServer` needs a consistent
adapter surface to renew leases and mint replacement lease tokens. These helper
methods are runtime-adapter API, not application workflow API.

## Internal-Only Packages

### `github.com/colony-2/swf-go/pkg/swf/internal/...`

Internal implementation and test support. These packages are not part of the
public API snapshot.

### `github.com/colony-2/swf-go/pkg/swf/runtime/*/internal/...`

Runtime implementation details. These packages are not part of the public API
snapshot.

## Removed From Public Surface

### `github.com/colony-2/swf-go/pkg/swf/runtime/direct/testsupport`

This package was test infrastructure for embedded Postgres/Strata setup. It is
not used by c2j and is not intended for downstream runtime construction. It has
been moved under `pkg/swf/internal/directtestsupport`.

## API Snapshot Packages

The API snapshot should include:

```text
github.com/colony-2/swf-go/pkg/swf
github.com/colony-2/swf-go/pkg/swf/runtime/direct
github.com/colony-2/swf-go/pkg/swf/runtime/remote
github.com/colony-2/swf-go/pkg/swf/runtime/sqlite
github.com/colony-2/swf-go/pkg/swf/runtime/toy
```

No generated protobuf storage package should be added to this list unless it
is explicitly promoted to public API.
