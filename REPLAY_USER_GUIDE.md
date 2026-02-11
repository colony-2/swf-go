# SWF Replay Run User Guide

This guide explains how to use **ReplayJobRun** to replay a workflow using cached results only. Replay runs execute your **job worker code path**, but **never mutate state** and **never wait** for timers or pending jobs. Replay is supported in both the real engine and the toy engine.

## Quick Summary
- **Purpose**: Validate workflow determinism and inspect cached outcomes without re-running tasks.
- **Execution**: The job worker runs normally; `DoTask` pulls cached task results only.
- **No mutation**: Replay never writes chapters or updates pgwf state.
- **No waiting**: Any `AwaitDuration`/`AwaitJobs` that isn’t already satisfied returns a cache-miss error.

---

## API Surface

### Go API
```go
type ReplayRunRequest struct {
    JobKey    swf.JobKey
    Observer  swf.ReplayObserver // optional
    JobWorker swf.JobWorker      // optional override
}

// Executes a job with cached results only.
ReplayJobRun(ctx context.Context, req ReplayRunRequest) (swf.JobData, error)
```

### Retry Boundary Observer
Replay can emit retry-boundary events (only information not otherwise available):
```go
type ReplayObserver interface {
    OnJobStart(event JobStartEvent)
    OnTaskStart(event TaskStartEvent)
    OnTaskEnd(event TaskEndEvent)
    OnJobEnd(event JobEndEvent)
}
```

---

## Behavior Details

### What Replay Does
- Executes your **job worker** as normal (or the override passed in `ReplayRunRequest.JobWorker`).
- When the job worker calls `DoTask`, replay **loads cached results** instead of executing task workers.
- If a cached task result is missing, replay returns `ReplayCacheMissError` immediately.

### What Replay Does NOT Do
- No chapter writes.
- No pgwf updates (no lease completion/reschedule).
- No real task execution.
- Task workers are never invoked; replay is cache-only.
- No sleeping or waiting for awaits.

### Timeouts and Errors
- If cached chapters contain timeout, app error, or system error payloads, replay returns the same error types you’d see in a real run.
- Determinism errors (e.g., input hash mismatch) are returned as usual.

### Await/Timers
- `AwaitDuration`:
  - If already satisfied (zero/negative duration), it returns nil.
  - Otherwise returns `ReplayCacheMissError` with reason `await_not_ready`.
- `AwaitJobs`:
  - If any job is not complete, returns `ReplayCacheMissError` with reason `await_jobs_pending`.

---

## Error Types

### Cache Miss
```go
type ReplayCacheMissError struct {
    JobKey   swf.JobKey
    TaskType string
    Ordinal  int64
    Attempt  int
    Reason   ReplayCacheMissReason
}
```

Reasons:
- `task_result_missing`
- `job_result_missing`
- `await_not_ready`
- `await_jobs_pending`

### Determinism Errors
Replay uses the same determinism errors as real runs:
- `swf.TaskInputMismatchError`
- `swf.ErrWorkflowNotDeterministic`

---

## Example: Real Engine
```go
ctx := context.Background()
jobKey := swf.JobKey{TenantId: "t1", JobId: "job123"}

result, err := engine.ReplayJobRun(ctx, swf.ReplayRunRequest{JobKey: jobKey})
if err != nil {
    // handle ReplayCacheMissError, determinism errors, timeouts, etc.
}
_ = result
```

With observer:
```go
obs := myObserver{}
result, err := engine.ReplayJobRun(ctx, swf.ReplayRunRequest{
    JobKey:   jobKey,
    Observer: obs,
})
```

---

## Example: Toy Engine
```go
ctx := context.Background()
jobKey := swf.JobKey{TenantId: "t1", JobId: "job123"}

result, err := toyEngine.ReplayJobRun(ctx, swf.ReplayRunRequest{JobKey: jobKey})
if err != nil {
    // handle replay errors
}
_ = result
```

---

## Practical Tips
- **Warm cache first**: run the real job once to generate chapters.
- **Replay failures** are often expected in development; missing cache is a signal that the run has not been fully materialized.
- **Avoid sleeps in job code** if you expect replay to complete without cache miss.

---

## Common Questions

**Does replay run my job worker?**
Yes. Replay always executes the job worker logic, but task execution is cache-only.

**Can replay change state?**
No. Any mutation attempt returns `ErrReplayShouldNeverMutate`.

**Is replay deterministic?**
Replay enforces determinism and surfaces the same determinism errors as real runs.

---

## Reference
Key types:
- `ReplayRunRequest`
- `ReplayCacheMissError`
- `ReplayCacheMissReason`
- `ReplayObserver`
