x# Job Run Details API (Draft)

## Goals
- Provide a read-only API to fetch execution details for a single job by ID.
- Return job start details, task executions (with retries/attempts), and job result (if available).
- Expose task inputs/outputs and artifacts using data already stored in Strata chapters + pgwf status.
- Present structured error/timeout info without leaking internal envelope formats.

## Non-Goals
- No mutation (cancel/retry/restart) or live log streaming.
- No artifact bytes inlined; only metadata + identifiers for follow-on fetch.
- No recomputation or rehydration beyond data already stored in chapters.
- No pagination in v1 (entire job timeline returned).

## Data Sources
- Strata story chapters for the job:
  - Chapter envelope `meta` (attempts, timestamps, input hashes, input payloads).
  - Chapter artifacts (outputs).
  - Chapter payload kind + payload body (app output or error payload).
- pgwf job status for active vs archived (running vs complete).

## API Shape (Go)
```go
type GetJobRunRequest struct {
    JobKey               swf.JobKey
    IncludeInputs        bool // default true; include task/job input payloads when available
    IncludeOutputs       bool // default true
    IncludeArtifacts     bool // default true
    IncludeAttemptInputs bool // default false; if true, resolve input refs to include artifacts
}

type GetJobRunResponse struct {
    Job   JobRunSummary
    Start JobStart
    Tasks []TaskRun
    JobAttempts []JobAttempt // empty when no job attempt chapters yet
    Result *JobAttempt        // last job attempt if job is complete; nil otherwise
}

type JobRunSummary struct {
    JobKey     swf.JobKey
    JobType    string
    Status     swf.JobStatus
    CreatedAt  time.Time
    ArchivedAt *time.Time
}

type JobStart struct {
    Ordinal   int64
    WorkerID  string
    CreatedAt time.Time
    Input     *TaskIO
}

type TaskRun struct {
    TaskRunID string // stable id: "<task_type>:<first_ordinal>"
    TaskType  string
    Attempts  []TaskAttempt
}

type TaskAttempt struct {
    Ordinal       int64
    Attempt       int
    WorkerID      string
    CreatedAt     time.Time
    InputHash     string
    InputRef      *swf.InputReference
    RunPolicy     *swf.RunPolicy
    Retryable     *bool
    MaxAttempts   *int
    NextAttemptAt *time.Time
    BackoffMillis *int64
    Input         *TaskIO
    Output        *TaskIO
    State         string // "SUCCEEDED" | "FAILED" | "READY" | "LEASED" | "WAITING" | "RUNNING"
    Runtime       *TaskRuntime
    Outcome       TaskOutcome
}

type TaskRuntime struct {
    LeaseOwner     *string // worker id if known
    LeaseExpiresAt *time.Time
    NextNeed       *string   // pgwf next_need/capability
    AvailableAt    *time.Time
    WaitFor        []swf.JobId
}

type JobAttempt struct {
    Ordinal   int64
    Attempt   int
    WorkerID  string
    CreatedAt time.Time
    InputRef  *swf.InputReference
    Output    *TaskIO
    Outcome   TaskOutcome
}

type TaskIO struct {
    Data      json.RawMessage
    Artifacts []ArtifactInfo
}

type ArtifactInfo struct {
    ID          string
    Name        string
    ContentType string
    SizeBytes   int64
    Sha256      string
}

type TaskOutcome struct {
    Status      string // "SUCCEEDED" | "FAILED"
    PayloadKind string // "App" | "AppChildJob" | "AppError" | "SystemError" | "Timeout"
    Error       *TaskError
}

type TaskError struct {
    Kind      string                 // "APP" | "SYSTEM" | "TIMEOUT"
    Message   string
    Level     string                 // app errors
    Attrs     map[string]interface{} // app errors
    Component string                 // system/timeouts
    Code      string                 // system/timeouts
    Retryable *bool                  // system/timeouts
    Scope     string                 // timeout only
    After     *swf.Duration          // timeout only
    InputRef  *swf.InputReference
    Stacktrace []string
}

// New interface exposed by SWFEngine (non-exported to mirror existing patterns).
type jobRunApi interface {
    GetJobRun(ctx context.Context, req GetJobRunRequest) (GetJobRunResponse, error)
}
```

## Response Semantics
- `Start` is derived from chapter ordinal `0` (the initial job input chapter).
  - `JobRunSummary.JobType` is `Start`'s `meta.task_type` (same as job worker name).
  - `Start.Input` is the chapter payload + artifacts (job input).
- When the job is not archived/completed, expose runtime in the task model:
  - Append a synthetic `TaskRun` for the current `next_need` capability.
  - `TaskRun.TaskType` is set to `next_need` (capability) so callers can render the current task type.
  - The synthetic run has a single `TaskAttempt` with `State` and `Runtime` populated, `Output` nil.
  - `InputRef` points at the last completed ordinal; `Input` can be resolved from that chapter when available.
  - `Ordinal` is `lastOrdinal + 1` to align with the task timeline, even if no chapter exists yet.
  - State mapping from pgwf status:
    - `READY` + `available_at <= now` -> `State="READY"`.
    - Active lease present -> `State="LEASED"` with `Runtime.LeaseOwner`/`LeaseExpiresAt`.
    - `AwaitingFuture` -> `State="WAITING"` with `Runtime.AvailableAt`.
    - `PendingJobs` -> `State="WAITING"` with `Runtime.WaitFor`.
- `Tasks` are derived from chapters with `meta.task_type != JobRunSummary.JobType`.
  - Each chapter is a task attempt; retries are separate chapters with incremented `meta.attempt`.
  - `TaskRunID` is built from the first attempt’s ordinal to keep it stable and unique.
  - Attempts are grouped by scan order: a new `TaskRun` begins when `meta.attempt == 1`.
- For completed attempts, `State` mirrors `Outcome.Status` (`SUCCEEDED` or `FAILED`).
- For runtime attempts, `Outcome` is omitted and `State` is one of `READY`, `LEASED`, or `WAITING`.
- `JobAttempts` are derived from chapters with `meta.task_type == JobRunSummary.JobType` and `ordinal > 0`.
  - These represent job worker attempts (retries) and are separate from task runs.
- `Result` is the last `JobAttempt` only if pgwf reports the job archived/completed; otherwise `nil`.
- `Input` payloads for task attempts:
  - If `meta.input` is populated (task input storage enabled), use it directly.
  - Otherwise, return `nil` and rely on `InputRef` to point at the input chapter.
  - If `IncludeAttemptInputs` is true, resolve `InputRef` and return its payload/artifacts.
- `Output` payloads are always the chapter payload. Artifacts are the chapter artifacts.

## Error/Outcome Mapping
- `payload_kind` maps to `TaskOutcome`:
  - `App`, `AppChildJob` -> `Status=SUCCEEDED`, `Error=nil`.
  - `AppError` -> `Status=FAILED`, `Error.Kind="APP"` with fields from `swf.AppErrorPayload`.
  - `SystemError` -> `Status=FAILED`, `Error.Kind="SYSTEM"` with fields from `swf.SystemErrorPayload`.
  - `Timeout` -> `Status=FAILED`, `Error.Kind="TIMEOUT"` with fields from `swf.TimeoutPayload`.
- Unknown `payload_kind` values are surfaced as `Status=FAILED`, `Error.Kind="SYSTEM"`, `Code="unknown_payload_kind"`.

## Implementation Notes
- Chapters are fetched in ordinal order from Strata; no additional persistence is required.
- Artifact metadata is sourced from the chapter’s attached artifacts (ID, name, size, content type, sha256).
- When `IncludeArtifacts=false`, omit artifact arrays entirely to keep responses small.
- When a chapter’s payload is invalid JSON, return `ErrWorkflowNotDeterministic` (same as internal reads).

## Implementation Plan
- Add `GetJobRun` on `swfEngineImpl` (and interface) mirroring `ListJobs` and `GetJobResult` patterns.
- Fetch pgwf job status:
  - Determine `Status`/`ArchivedAt` (same logic as `CheckJobStatus`).
  - When not archived, populate a synthetic runtime task attempt from `pgwf.jobs_with_status` (lease, wait_for, available_at, next_need).
- Fetch Strata story chapters in ascending ordinal; decode envelopes and artifacts.
- Build response:
  - `Start` from ordinal 0.
  - `Tasks` from chapters whose `meta.task_type != JobType`, grouping by `meta.attempt==1`.
  - `JobAttempts` from chapters whose `meta.task_type == JobType` and ordinal > 0.
  - `Result` when archived: last `JobAttempt`.
- Input/Output handling:
  - Output is always the chapter payload + artifacts.
  - Input uses `meta.input` when present.
  - When `IncludeAttemptInputs=true` and `InputRef` is set, resolve input chapter to populate `Input`.
- Error mapping:
  - Convert payload kinds to `TaskOutcome` and `TaskError` as specified.
  - Unknown payload kinds return `Status=FAILED` with `Error.Kind="SYSTEM"` and `Code="unknown_payload_kind"`.

## Testing Plan
- Running job state:
  - Job not archived returns a synthetic runtime task attempt with `State` and `Runtime` populated based on pgwf state.
- Completed job:
  - Archived job returns `Result` populated with final job attempt.
- Task retries:
  - Single `TaskRun` with multiple `Attempts` and correct `Attempt` numbering.
- Job retries:
  - Multiple `JobAttempts` with error payloads and final success.
- Task input storage disabled:
  - `TaskAttempt.Input` is nil and `InputRef` is present.
- `IncludeAttemptInputs=true`:
  - Inputs are resolved from referenced chapters and include artifacts.
- Error payload mapping:
  - `AppError`, `SystemError`, `Timeout` populate `TaskError` fields correctly.
- Unknown payload kind:
  - Response marks attempt as failed with `unknown_payload_kind` code.
