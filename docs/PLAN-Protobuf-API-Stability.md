# Plan: Protobuf Storage With Stable Public API

## Status

**Proposed** | Author: Codex | Date: 2026-06-09

## Goal

Move SWF's internal durable data structures from JSON to protobuf without
changing the external `swf-go` APIs used by downstream consumers.

The protobuf work should be invisible to application code. Existing users
should still build against the same `pkg/swf` API, the same runtime
constructors, and the same task/job data interfaces. The protobuf change is an
internal storage and runtime implementation change, not a public SDK redesign.

Hold off on adding a gRPC runtime API. The remote REST API may continue to use
its current JSON wire shape while internal durable storage changes underneath.

## c2j Consumer Audit

Checked out:

```text
github.com/colony-2/c2j
commit 3374b852aa9905b0550baa3bfeb8ad24a1e08a89
```

c2j imports only these swf-go packages:

```text
github.com/colony-2/swf-go/pkg/swf
github.com/colony-2/swf-go/pkg/swf/runtime/remote
github.com/colony-2/swf-go/pkg/swf/runtime/sqlite
github.com/colony-2/swf-go/pkg/swf/runtime/toy
```

c2j does not import:

```text
github.com/colony-2/swf-go/pkg/swf/runtime/direct
github.com/colony-2/swf-go/pkg/swf/runtime/direct/testsupport
```

High-frequency `pkg/swf` symbols used by c2j include:

```text
JobKey
Artifact
ArtifactKey
TaskData
JobData
NewTaskData
RunPolicy
NewArtifactFromBytes
NewEngineBuilder
Duration
GetJobRunRequest
TaskWorker
JobStatus
WaitForJobToComplete
ListJobsRequest
SWFEngine
JobContext
WorkflowRuntime
TaskContext
SimpleTaskData
JobStore
AssignArtifactKey
SubmitJob
SubmitRestartJob
StoredChapter
PutChapterRequest
ReplayRunRequest
ReplayCacheMissError
GetJobForRun
GetJobForRunRequest
TaskIO
TaskOutcome
TaskError
JobRunOutcome
```

Runtime package symbols used by c2j include:

```text
remote.New
remote.NewServer
sqlite.Config
sqlite.NewFromConfig
toy.New
toy.WithJobIDGenerator
toy.Option
```

Implication: the protobuf storage change must preserve the source-level API of
these symbols until there is an explicit downstream migration plan.

## Current Public Package Boundary

The currently importable swf-go packages are:

```text
github.com/colony-2/swf-go/pkg/swf
github.com/colony-2/swf-go/pkg/swf/runtime/direct
github.com/colony-2/swf-go/pkg/swf/runtime/direct/testsupport
github.com/colony-2/swf-go/pkg/swf/runtime/remote
github.com/colony-2/swf-go/pkg/swf/runtime/sqlite
github.com/colony-2/swf-go/pkg/swf/runtime/toy
```

Candidate accidental public packages:

1. `runtime/direct/testsupport`
   - c2j does not use it.
   - It starts embedded Postgres and embedded Strata for tests.
   - Move to an internal test-support package before taking an API snapshot, or
     mark it explicitly unsupported if another internal package still needs it.

2. `runtime/direct`
   - c2j does not use it.
   - It exposes a Postgres/Strata direct runtime constructor and type alias.
   - Decide whether direct/Postgres remains a supported public runtime. If not,
     move it behind `internal` before the API snapshot.

Candidate accidental public symbols inside `pkg/swf`:

1. Low-level runtime mutation/read types:
   `WorkflowRuntime`, `ExecutionLease`, `PutChapterRequest`, `StoredChapter`,
   `ChapterRef`, `CompleteExecutionRequest`, `RescheduleExecutionRequest`,
   `PollWorkRequest`.
2. Runtime-run helper APIs:
   `GetJobForRun`, `GetJobForRunRequest`, `JobRunOutcome`.
3. Concrete data helpers:
   `SimpleTaskData`, `Data`.
4. Test/task helper constructors:
   `NewTaskContext`.
5. Transaction helper:
   `TxFromCtx`.
6. Replay and job-run read models:
   `ReplayCacheMissError`, `TaskIO`, `TaskOutcome`, `TaskError`,
   `JobStartEvent`, `TaskStartEvent`, `TaskEndEvent`, `JobEndEvent`.

Not all of these should be hidden. c2j actively uses many of them for
legitimate workflows, replay UI, `run-job`, `work-job --one`, and embedded
runtime wrapping. Phase 1 should classify each as either supported public API or
accidental exposure before any internal move.

## Phase 1: Hide Accidentally Leaked API

Do this before protobuf storage changes.

1. Create `docs/API-SURFACE.md` that classifies each currently importable
   package as supported, experimental, or internal-only.
2. Move `pkg/swf/runtime/direct/testsupport` under an internal path, because
   current evidence shows it is test infrastructure rather than downstream API.
3. Decide the status of `pkg/swf/runtime/direct`.
   - If supported, include it in the API snapshot.
   - If not supported, move it before the snapshot.
4. Review c2j usage of low-level `WorkflowRuntime` wrapping:
   - `cmd/c2j/internal/swfruntime/chapter_visibility_runtime.go` wraps
     `PutChapter` and `GetChapter`.
   - If this is still needed, either keep `WorkflowRuntime` public or add a
     smaller public hook that lets c2j achieve the same behavior without
     depending on chapter mutation types directly.
5. Preserve currently valid c2j uses until replacements exist. Do not move a
   symbol internal merely because it looks low-level if c2j depends on it for a
   real feature.
6. After every proposed move, run c2j against the local swf-go checkout with a
   replace directive before accepting the change.

Validation command shape:

```sh
cd /tmp/c2j-api-check
go mod edit -replace github.com/colony-2/swf-go=/src
go test ./...
```

## Phase 2: Configure API Fingerprinting

Add an API guard before protobuf implementation work.

Use two layers:

1. A checked-in API signature snapshot for stable review diffs.
2. An API compatibility checker for semantic break detection.

Recommended files:

```text
api/swf-go.public.txt
api/packages.txt
cmd/swf-api-snapshot/main.go
```

`api/packages.txt` should initially include only supported public packages,
after Phase 1 cleanup. Based on c2j, likely:

```text
github.com/colony-2/swf-go/pkg/swf
github.com/colony-2/swf-go/pkg/swf/runtime/remote
github.com/colony-2/swf-go/pkg/swf/runtime/sqlite
github.com/colony-2/swf-go/pkg/swf/runtime/toy
```

If `runtime/direct` is intentionally public, include it too.

`cmd/swf-api-snapshot` should load packages with `go/packages` and emit a
deterministic text representation of exported declarations:

1. Exported constants and variables with type and value where stable.
2. Exported functions and methods with signatures.
3. Exported types and their kind.
4. Exported struct fields.
5. Exported interface methods.
6. Exported type aliases.

Normalize output by sorting packages and declarations. Do not include comments
or source line numbers in the fingerprint.

Add commands:

```sh
go run ./cmd/swf-api-snapshot -packages api/packages.txt > api/swf-go.public.txt
go run ./cmd/swf-api-snapshot -packages api/packages.txt -check api/swf-go.public.txt
```

For semantic compatibility, also pin `golang.org/x/exp/apidiff` or
`golang.org/x/exp/cmd/gorelease` in a tools file. `apidiff` compares Go package
APIs, and `gorelease` analyzes public API changes against a base module
version. Use these as CI checks, but keep the checked-in signature file because
it gives a reviewable API contract inside this repository.

CI should run:

```sh
go test ./...
go run ./cmd/swf-api-snapshot -packages api/packages.txt -check api/swf-go.public.txt
go run golang.org/x/exp/cmd/gorelease@<pinned-version> -base=<baseline>
```

For the protobuf branch, the snapshot check is the hard gate. `gorelease` is a
secondary signal because this module currently uses pseudo-versions and may not
always have a clean release baseline.

## Phase 3: Update Internal Data Structures

Only after Phase 1 and Phase 2 are complete:

1. Add internal protobuf definitions for durable runtime state.
2. Keep generated protobuf packages internal unless a message is intentionally
   public. Because gRPC is deferred, storage protos should not become a new
   downstream API by accident.
3. Add `pkg/swf/internal/runtimecodec` to translate between public Go structs
   and internal protobuf messages.
4. Replace JSON chapter envelopes with protobuf chapter records internally.
5. Replace scheduler JSON blobs with protobuf blobs internally.
6. Keep the external `TaskData` API stable:
   - `GetData() (swf.Data, error)` still returns the same public data type.
   - `NewTaskData` and `NewTaskDataOrPanic` still accept current inputs.
   - Existing c2j code should not need protobuf awareness.
7. Preserve public metadata API shape unless a separate public migration is
   approved.
8. Preserve REST remote API shape while changing durable storage underneath.

This means protobuf is an internal representation. The public API may still
accept and return JSON-shaped `swf.Data` during this phase, even though durable
storage uses protobuf.

## Phase 4: Validate API Fingerprint

During the protobuf storage change:

1. Run the API snapshot check after every major refactor.
2. Treat any diff in `api/swf-go.public.txt` as a design review item, not as an
   incidental implementation detail.
3. Run c2j with a local replace to `/src`.
4. Run swf-go conformance and usage parity tests.
5. Run remote runtime tests to verify REST behavior remains unchanged.

Required validation commands:

```sh
go test ./...
go run ./cmd/swf-api-snapshot -packages api/packages.txt -check api/swf-go.public.txt
cd /tmp/c2j-api-check && go mod edit -replace github.com/colony-2/swf-go=/src && go test ./...
```

## Phase 5: Preserve API Behavior Tests

Add a stable API behavior test suite that should not change before and after
the protobuf storage migration.

Target tests:

1. Task data construction and round trip:
   - `NewTaskData`
   - `NewTaskDataOrPanic`
   - `TaskData.GetData`
   - `TaskData.GetArtifacts`
   - `SimpleTaskData` if it remains public
2. Artifact API:
   - `NewArtifactFromBytes`
   - `NewArtifactFromFile`
   - `AssignArtifactKey`
   - `ArtifactKey`
3. Engine API:
   - `NewEngineBuilder`
   - `SubmitJob`
   - `SubmitRestartJob`
   - `GetJob`
   - `GetJobRun`
   - `ReplayJobRun`
4. List jobs API:
   - `ListJobsRequest`
   - metadata filters
   - `JobSummary`
   - pagination token behavior
5. Error API:
   - `ErrJobFailed`
   - `ErrJobCancelled`
   - `ErrJobNotFound`
   - `ErrChapterNotFound`
   - `AppError`
   - `SystemError`
   - `TimeoutError`
   - `JobFailedError`
6. Runtime constructors:
   - `toy.New`
   - `toy.WithJobIDGenerator`
   - `sqlite.NewFromConfig`
   - `remote.New`
   - `remote.NewServer`

These tests should assert behavior through public APIs only. They should not
inspect storage bytes, JSON envelopes, protobuf messages, SQLite internal
columns, or Strata chapter bodies.

## Sequencing

1. Public surface cleanup PR.
2. API snapshot/tooling PR.
3. Public API behavior tests PR.
4. Internal protobuf codec and generated storage protos PR.
5. Runtime-by-runtime storage replacement PRs.
6. Final sweep that removes obsolete JSON envelope code and validates the API
   fingerprint against the pre-protobuf snapshot.

## Acceptance Criteria

1. `api/swf-go.public.txt` is committed before protobuf storage changes.
2. `go test ./...` passes before and after protobuf storage changes.
3. c2j passes against local swf-go with a replace directive.
4. The API snapshot check passes after protobuf storage changes.
5. No new public protobuf package is introduced unless it is explicitly listed
   in `docs/API-SURFACE.md`.
6. Storage internals can change from JSON to protobuf without requiring c2j
   source changes.
