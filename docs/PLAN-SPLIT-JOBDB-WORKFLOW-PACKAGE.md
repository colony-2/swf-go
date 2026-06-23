# Plan: Split Workflow Layer from JobDB Runtime API

## Goal

Split the current `pkg/jobdb` package into two conceptual layers as one hard
compatibility break:

- `pkg/jobdb`: the workflow runtime contract, REST API model, runtime client/server adapter support, and runtime implementations' shared Go API.
- `pkg/workflow`: the higher-level workflow SDK built on top of `pkg/jobdb`, including job workers, task workers, worker loops, replay execution, and user-facing engine orchestration.

The end state should make `pkg/jobdb` usable as the runtime API without pulling
in worker orchestration concepts, while `pkg/workflow` can import and build on
`pkg/jobdb`. This is planned as a single atomic change: code moves, import
updates, documentation updates, API snapshot updates, and test moves all land
together.

## Target Package Graph

```text
github.com/colony-2/jobdb/pkg/jobdb
  runtime value types, WorkflowRuntime, chapter/artifact APIs, schedule APIs,
  list jobs APIs, REST-compatible request/response types, runtime errors

github.com/colony-2/jobdb/pkg/jobdb/runtime/{remote,sqlite,toy,direct}
  concrete WorkflowRuntime implementations and REST adapter
  imports pkg/jobdb only, not pkg/workflow

github.com/colony-2/jobdb/pkg/workflow
  worker SDK and engine built on top of jobdb.WorkflowRuntime
  imports pkg/jobdb
```

Important dependency rule: `pkg/jobdb` must not import `pkg/workflow`. This
split is a breaking API move for worker-facing symbols currently exported from
`pkg/jobdb`; Go cannot provide aliases from `jobdb` to `workflow` without
creating an import cycle.

## Compatibility Policy

This plan intentionally does not preserve backwards compatibility.

- No `pkg/jobdb` re-exports of moved worker symbols.
- No deprecated compatibility aliases in `pkg/jobdb`.
- No transitional facade package that preserves old import paths.
- No two-release migration path.
- Downstream worker users must update imports from `jobdb` to `workflow` in the same upgrade.

## Boundary Decisions

Keep in `pkg/jobdb`:

- Runtime contract: `WorkflowRuntime`, `ExecutionLease`, `JobHandle`, submit/cancel/poll/lease/chapter/artifact request structs.
- Runtime data model: `JobKey`, `ArtifactKey`, `TaskData`, `Artifact`, `RunPolicy`, `RetryPolicy`, `Duration`, job status/info/listing types, schedule types, chapter types, runtime errors.
- REST API support: `pkg/jobdb/internal/runtimeapi`, `pkg/jobdb/runtime/remote`, `openapi/jobdb-runtime.yaml`.
- Runtime implementations: `pkg/jobdb/runtime/direct`, `pkg/jobdb/runtime/sqlite`, `pkg/jobdb/runtime/toy`.
- Read projections that are runtime-derived and worker-independent, especially `GetJobRunRequest`, `GetJobRunResponse`, and the logic that builds them from `WorkflowRuntime` chapters and `ListJobs`.

Move to `pkg/workflow`:

- Worker interfaces: `JobWorker`, `TaskWorker`, `JobContext`, `TaskContext`.
- Engine API: `Engine`, `EngineBuilder`, `WorkSet`, `WorkRegistrationOptions`, `RegisterWorkers`, `Run`.
- Worker execution internals: `workerEngine`, `workerRunner`, `runtimeEngine`, retry/timeout handling for executing workers, worker envelopes, worker runtime support, run-if-leaseable support.
- External task convenience layer: `FindTasksWaitingRequest`, `TaskHandle`, `FindTasksWaiting`, `GetWaitingTask`, and completion helpers built from `WorkflowRuntime.CompleteTaskIfWaiting`.
- Replay execution: `ReplayRunRequest`, `ReplayObserver`, replay events, replay read-only runtime wrapper, and replay worker runner.
- Worker-level artifact convenience methods that require an engine or worker runtime.

Refactor instead of simply moving:

- `ArtifactKey.ToLazyArtifact(engine Engine, tenantId string)` cannot remain in `pkg/jobdb` because `Engine` moves. Replace it with a lower-level `jobdb` helper that resolves artifacts through `WorkflowRuntime`, or move the engine-based helper to `pkg/workflow`.
- `GetJobRunResponse.GetOutput(engine Engine, tenantId string)` cannot remain in `pkg/jobdb` with an `Engine` parameter. Either change it to use a runtime/artifact resolver interface owned by `pkg/jobdb`, or move the engine-based output helper to `pkg/workflow`.
- Any worker code that currently depends on unexported `pkg/jobdb` helpers must either move with the worker package or use a small exported lower-level API. Prefer exporting only stable runtime primitives, not worker-specific helpers.

## Single-Change Implementation Sequence

All steps below land in one breaking change. The numbering is for execution
order inside that change, not for staged releases.

### Step 0: Baseline and inventory

1. Run the current baseline:

   ```bash
   go test ./...
   go run ./cmd/jobdb-api-snapshot -packages api/packages.txt -check api/jobdb.public.txt
   ```

2. Record the exported worker-facing symbols currently present in `pkg/jobdb`. The main set is:

   - `Engine`, `EngineBuilder`, `NewEngineBuilder`
   - `JobWorker`, `TaskWorker`, `JobContext`, `TaskContext`
   - `WorkSet`, `WorkRegistrationOptions`, `AsWorkSet`, `AsWorkSetWithOptions`
   - `FindTasksWaitingRequest`, `TaskHandle`
   - `GetJobForRunRequest`, `GetJobForRun`, `JobRunnable`, `JobRunOutcome`, `JobRunListener`
   - `ReplayRunRequest`, `ReplayObserver`, replay event/cache-miss types where they execute workers
   - `WaitForJobToComplete` if it remains engine-specific

3. Treat all worker-facing removals from `pkg/jobdb` as intentional API breaks. Do not add compatibility wrappers for these symbols.

### Step 1: Make `pkg/jobdb` independent of `Engine`

1. Introduce a runtime-owned artifact resolver API in `pkg/jobdb`, for example:

   ```go
   type ArtifactGetter interface {
       GetArtifact(ctx context.Context, tenantId string, key ArtifactKey) (Artifact, error)
   }
   ```

   or expose a helper backed directly by `WorkflowRuntime.OpenArtifact`.

2. Update `ArtifactKey.ToLazyArtifact` and `GetJobRunResponse.GetOutput` so `pkg/jobdb` no longer needs the `Engine` type.

3. Export a worker-independent runtime read helper if needed:

   ```go
   func GetJobRun(ctx context.Context, runtime WorkflowRuntime, req GetJobRunRequest) (GetJobRunResponse, error)
   ```

4. Keep `WorkflowRuntime`, runtime request structs, chapter types, schedules, list-jobs types, and task data/artifact types in `pkg/jobdb`.

5. After this step, `pkg/jobdb` should compile without `Engine`, `EngineBuilder`, `JobWorker`, `TaskWorker`, or `TaskContext`.

### Step 2: Create `pkg/workflow`

1. Add `pkg/workflow` with package name `workflow`.

2. Move worker-facing public types from `pkg/jobdb` into `pkg/workflow`.

3. Update moved code to qualify lower-level types through `jobdb`, for example:

   - `jobdb.WorkflowRuntime`
   - `jobdb.SubmitJob`
   - `jobdb.TaskData`
   - `jobdb.JobKey`
   - `jobdb.RunPolicy`
   - `jobdb.ExecutionLease`
   - `jobdb.Chapter`

4. Provide ergonomic aliases inside `pkg/workflow` only for lower-level
   `jobdb` value types. These aliases are not backwards-compatibility shims
   because callers must still change imports to `pkg/workflow`:

   ```go
   type JobData = jobdb.TaskData
   type TaskData = jobdb.TaskData
   type JobKey = jobdb.JobKey
   type RunPolicy = jobdb.RunPolicy
   ```

5. Move these implementation files or their contents into `pkg/workflow`:

   - `core.go`, for `Engine`
   - worker-facing portions of `jobs.go`
   - worker-facing portions of `tasks.go`
   - `loop.go`
   - `runtime_engine.go`
   - `worker_engine.go`
   - `worker_runner.go`
   - `worker_retry.go`
   - `worker_envelope.go`
   - `worker_runtime_support.go`
   - `runtime_task_handle.go`
   - `run_job_if_leaseable.go`
   - replay worker execution portions of `replay.go`

6. Leave lower-level files in `pkg/jobdb`:

   - `runtime.go`
   - `jobs.go` portions that define submit/cancel/status/runtime request types
   - `tasks.go` portions that are runtime data only, if any
   - `types.go` without `TaskWorker`
   - `artifact.go`, `chapter.go`, `schedule.go`, `list_jobs.go`, `job_run_details.go`, `job_run_runtime.go`
   - runtime error and timeout/data helpers used by runtime APIs

7. Move shared lower-level internals, such as the former runtime codec package,
   to `pkg/internal/...` when both `pkg/jobdb` and `pkg/workflow` need them.
   Prefer moving shared codec internals over exporting worker-specific chapter
   helpers from `pkg/jobdb`.

### Step 3: Update runtime packages and public helpers

1. Ensure runtime implementation packages do not depend on `pkg/workflow`.

2. Remove above-runtime methods from runtime implementations:

   - `Runtime.GetJobRun`
   - `Runtime.FindTasksWaitingForCapability`
   - `Runtime.GetWaitingTask`

   These should be provided by `pkg/workflow.Engine` or by worker-level helpers over `WorkflowRuntime`.

3. Move embedded engine helpers out of runtime packages:

   - `pkg/jobdb/runtime/direct/testing.go`
   - `pkg/jobdb/runtime/sqlite/testing.go`

   Runtime packages may keep `StartEmbeddedRuntime`. Engine helpers should move to workflow test support, for example `pkg/workflow/workflowtest` or `pkg/workflow/internal/workflowtest`.

4. Keep `cmd/jobdb` focused on serving the runtime REST API. It should not import `pkg/workflow`.

### Step 4: Update callers and documentation

1. Update example imports:

   ```go
   import (
       "github.com/colony-2/jobdb/pkg/jobdb"
       "github.com/colony-2/jobdb/pkg/workflow"
       toyruntime "github.com/colony-2/jobdb/pkg/jobdb/runtime/toy"
   )
   ```

2. Update worker code examples to use:

   - `workflow.JobContext`
   - `workflow.TaskContext`
   - `workflow.NewEngineBuilder`
   - `workflow.AsWorkSet`

3. Keep runtime-only examples using `jobdb.WorkflowRuntime` and runtime packages without importing `workflow`.

4. Update `docs/API-SURFACE.md` and `api/packages.txt`:

   - keep `github.com/colony-2/jobdb/pkg/jobdb`
   - add `github.com/colony-2/jobdb/pkg/workflow`
   - keep runtime packages
   - remove worker-facing symbols from the `pkg/jobdb` snapshot

### Step 5: Split tests by layer

Use the package boundary as the test boundary.

Keep in `pkg/jobdb` or runtime implementation test packages:

- Runtime contract tests for `WorkflowRuntime` lifecycle, leases, chapters, artifacts, schedules, list jobs, explicit IDs, and REST adapter behavior.
- Pure value/helper tests for artifacts, chapters, schedules, metadata filters, job keys, errors, timeout payloads, and runtime codecs.
- Remote API tests that exercise `pkg/jobdb/runtime/remote` through `WorkflowRuntime` only.

Move to `pkg/workflow`:

- `worker_runner_test.go`
- `worker_engine_test.go`
- `runtime_engine_test.go`
- `run_job_if_leaseable_test.go`
- `replay_readonly_test.go`
- workflow integration tests that use `EngineBuilder`, `JobWorker`, `TaskWorker`, or `JobContext`
- engine conformance and usage parity tests from the old runtime-internal test-support locations

Rewrite or split mixed tests:

- Tests like `basic_workflow_integration_test.go`, `error_workflow_integration_test.go`, `artifact_cleanup_*`, `prerequisites_*`, and `completion_status_*` are workflow tests if they execute workers. Keep only lower-level runtime assertions in `pkg/jobdb`.
- `runtime/remote/remote_integration_test.go` should keep REST runtime tests in the runtime package and move worker-sequence scenarios to workflow tests using a remote runtime.
- `list_jobs_integration_test.go` should become runtime-level if it can submit and inspect jobs through `WorkflowRuntime`; otherwise move the worker-driven scenario to `pkg/workflow`.

Recommended test support split:

```text
pkg/internal
  shared runtime-only codecs and direct runtime test support

pkg/workflow/internal/jobdbtest
  workflow engine harnesses, worker fixtures, parity helpers, wait helpers
```

After moving tests, run targeted suites:

```bash
go test ./pkg/jobdb/...
go test ./pkg/jobdb/runtime/...
go test ./pkg/workflow/...
go test ./...
```

### Step 6: Enforce the new boundary

1. Add an import-boundary test or script that fails if lower layers import `pkg/workflow`.

   Suggested checks:

   - `pkg/jobdb` must not import `github.com/colony-2/jobdb/pkg/workflow`
   - `pkg/jobdb/runtime/...` must not import `github.com/colony-2/jobdb/pkg/workflow`
   - `cmd/jobdb` must not import `github.com/colony-2/jobdb/pkg/workflow`

2. Update the API snapshot:

   ```bash
   go run ./cmd/jobdb-api-snapshot -packages api/packages.txt > api/jobdb.public.txt
   ```

3. Add breaking-change release notes or migration docs with a symbol mapping table:

   ```text
   jobdb.NewEngineBuilder      -> workflow.NewEngineBuilder
   jobdb.Engine                -> workflow.Engine
   jobdb.JobWorker             -> workflow.JobWorker
   jobdb.TaskWorker            -> workflow.TaskWorker
   jobdb.JobContext            -> workflow.JobContext
   jobdb.TaskContext           -> workflow.TaskContext
   jobdb.AsWorkSet             -> workflow.AsWorkSet
   jobdb.GetJobForRun          -> workflow.GetJobForRun
   jobdb.ReplayRunRequest      -> workflow.ReplayRunRequest
   ```

## Acceptance Criteria

- `pkg/jobdb` exposes the workflow runtime Go API and REST-compatible data model, but no worker SDK or engine builder symbols.
- `pkg/workflow` exposes the worker SDK and engine builder and depends on `pkg/jobdb`.
- Runtime implementation packages compile and test without importing `pkg/workflow`.
- `cmd/jobdb` serves the runtime API without importing `pkg/workflow`.
- API snapshot includes both `pkg/jobdb` and `pkg/workflow` as separate public packages.
- Tests are separated so runtime conformance can run without worker concepts, and workflow conformance can run against any `jobdb.WorkflowRuntime`.
- `go test ./...` passes.
- No compatibility aliases, wrappers, or re-export files remain in `pkg/jobdb`
  for moved workflow symbols.

## Main Risks

- This is a public API break for worker users currently importing everything from `pkg/jobdb`; they must update imports and type references in one upgrade.
- Worker code currently benefits from being in the same package as unexported chapter/envelope helpers. Moving it requires either relocating shared internals or adding narrow exported runtime helpers.
- Runtime packages currently expose a few above-runtime convenience methods. Removing them may break downstream callers, but that break is part of this change.
- Tests currently use shared harnesses that mix runtime and engine construction. Split the harnesses early so later file moves stay mechanical.
