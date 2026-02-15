# Proposal: StartJob/RestartJob Prerequisites (Complete vs Success)

## Summary
Introduce first-class **job prerequisites** on `StartJob` and `RestartJob` so a job can wait for other jobs to **complete** or **complete successfully** before running. Jobs waiting on **successful** prerequisites will **fail fast** if any prerequisite finishes unsuccessfully.

This proposal is designed to be **backwards compatible**, align with existing PGWF `wait_for` semantics, and use PGWF completion status/detail as the source of truth for job success/failure.

## API Changes

### New Types
```go
// JobPrereqCondition defines how a prerequisite is evaluated.
type JobPrereqCondition string

const (
    JobPrereqComplete JobPrereqCondition = "complete" // job must be archived (any outcome)
    JobPrereqSuccess  JobPrereqCondition = "success"  // job must be archived + succeeded
)

// JobPrerequisite declares a dependency on another job.
type JobPrerequisite struct {
    JobID     string             // required; same tenant as the parent job
    Condition JobPrereqCondition // required; default to JobPrereqComplete if empty
}
```

### StartJob / RestartJob
```go
type StartJob struct {
    TenantId     string
    JobType      string
    JobID        string
    SingletonKey string
    Data         JobData
    RunPolicy    RunPolicy
    Metadata     json.RawMessage

    // New: prerequisites for start
    Prerequisites []JobPrerequisite
}

type RestartJob struct {
    PriorJobKey     JobKey
    LastStepToKeep  int64
    JobID           string
    ExtraTaskInput  TaskData
    ExtraTaskOutput TaskData

    // New: prerequisites for restart
    Prerequisites []JobPrerequisite
}
```

### Optional: API Surface in Read Models
If we want visibility without inspecting payloads, add a read-only field in job summary/run APIs:
```go
// JobSummary / GetJobRunResponse
Prerequisites []JobPrerequisite
```
This is optional; we can also keep prerequisites stored in payload only.

## Behavior

### Scheduling
1. **All prerequisites** (complete + success) are added to PGWF `wait_for`.
2. The job remains `PENDING_JOBS` until all prerequisite job IDs are archived.
3. Once unblocked, the job is leased and evaluated.

### Success Prerequisites
After the job is leased (but **before** running the job worker):
1. Load prerequisite outcomes (see “Outcome Tracking”).
2. If any `Condition == success` prerequisite failed, immediately **fail the job**.
3. Record a deterministic failure outcome (non-retryable) and archive the job.

### Completion-Only Prerequisites
No additional checks are needed. If the prerequisite is archived, it satisfies `complete`.

## Outcome Tracking and Failure Management

### Source of Truth
PGWF completion status/detail in `pgwf.jobs_archive` is the source of truth for success/failure. This avoids any Strata reads at lease time.

### Implementation Strategy (Minimal Change)
Add a helper on the engine:
```go
// Returns (success, err). If job not archived, return ErrJobNotComplete.
func (s *swfEngineImpl) JobSucceeded(ctx context.Context, key swf.JobKey) (bool, error)
```
Implementation uses pgwf-go:
1. Fetch job detail via `pgwf.GetJob(..., IncludePayload=false)` (searches active + archived).
2. If `ArchivedAt` is nil, return `ErrJobNotComplete`.
3. Interpret `CompletionStatus`:
   - `success` → true
   - any other status (including `failed_*`, `cancelled`, or custom values) → false

### Failing the Dependent Job
If any required-success prerequisite fails:
- Do **not** run the job worker.
- Emit a `JobAttemptOutcome` chapter with a **non-retryable** system error, e.g.:
  - `SystemErrorPayload{Message: "prerequisite job failed", InputRef: ...}`
- Complete/archive the job normally via PGWF lease completion.

This ensures:
- The dependent job is terminal.
- `GetJobResult` returns a failure.
- Downstream jobs can reliably treat it as unsuccessful.

### Optional Optimization
Not needed. We already use PGWF completion status/detail.

## Implementation Plan

### 1) Data Model Updates
- Add `Prerequisites []JobPrerequisite` to `StartJob` and `RestartJob`.
- Extend `jobPayload` to include `Prerequisites` for runtime evaluation:
```go
type jobPayload struct {
    RunPolicy      swf.RunPolicy        `json:"run_policy,omitempty"`
    TaskWait       *taskWait            `json:"task_wait,omitempty"`
    Prerequisites  []JobPrerequisite    `json:"prereqs,omitempty"`
}
```

### 2) Start/Restart Submission
- Validate prerequisites (non-empty IDs, no duplicates, no self-reference).
- Populate `wait_for` with all prerequisite job IDs.
- Store prerequisites in job payload.

### 3) Lease-Time Gate
In the runner entry path (before executing job worker):
1. Read `Prerequisites` from the lease payload.
2. For each with `Condition == success`, call `JobSucceeded`.
3. If any fail, write a `JobAttemptOutcome` error and short-circuit job execution.

### 4) Tests
- StartJob with `complete` prereq: job runs after prereq archived.
- StartJob with `success` prereq: job fails when prereq errors.
- RestartJob parity tests.
- Ensure `ListJobs`/`GetJobRun` visibility if we add read model fields.

## Backwards Compatibility
- Existing callers unaffected (new fields are optional).
- Payload additions are additive and ignored by older readers.
- No schema change required for the minimal implementation.

## Open Questions
- Do we want cross-tenant prerequisites? (Proposal: **no**, to match current `wait_for` assumptions.)
- Should failure due to prerequisite be `SystemError` (non-retryable) or `AppError`? (Proposal: **SystemError**.)
- Do we want to surface prerequisites in `ListJobs`/`GetJobRun`? (Nice to have.)
