# Run Policy Timeout

Add deterministic wall-clock limits to `RunPolicy` with two flavors:
- **Invocation timeout**: max runtime per job/task attempt.
- **Total timeout**: max end-to-end runtime across all attempts, backoffs, awaits, and reschedules for that job/task.

## Goals
- Let callers cap both per-attempt runtime and overall runtime for a job or task.
- Make timeouts deterministic and retry-friendly: timed-out attempts persist a chapter and honor existing retry policy.
- Keep enforcement consistent whether the runner stays in-memory or is recycled during awaits/backoff.

## Out of Scope
- Global job TTLs or queue-level deadlines.
- Worker-level forced goroutine cancellation; we surface a timeout error and exit the runner, but do not kill user goroutines.
- Per-step CPU/memory limits.

## API Changes
- `RunPolicy` gains two optional duration pointers:
  - `InvocationTimeout *Duration \`yaml:"invocation_timeout,omitempty"\`` (per attempt).
  - `TotalTimeout *Duration \`yaml:"total_timeout,omitempty"\`` (overall).
  - `nil` means inherit/default; `0` explicitly disables that timeout even if the base policy set one; `>0` enables.
- `mergeRunPolicy`/`normalizeRunPolicy` extended to merge/validate both fields (override when non-nil; clamp negatives to nil).
- Chapter metadata already carries `RunPolicy`; serialized form includes both timeouts when set so replays share the same limits.
- Timeout payload & helper:
  - New payload kind `Timeout` with fields `{scope: "invocation"|"total", after: Duration, retryable: bool, input_ref, kind: string (job|task), component: "runner", code: string}`.
  - `NewTimeoutError(kind string, after time.Duration, scope string, inputRef *InputReference, retryable bool) error` returns a timeout error wrapping that payload.
  - Invocation timeouts use `retryable=true`; total timeouts use `retryable=false`. `isRetryable` special-cases timeout payloads to use this flag.

## Semantics

### Effective Policy Resolution
- Job defaults come from `StartJob.RunPolicy`.
- Task calls pass an override `RunPolicy`; merging rules remain “only override when set”.
  - A task can clear a job-level timeout by setting the corresponding field to `0`.
- `InvocationTimeout` and `TotalTimeout` are normalized once per runner/job and reused for all attempts.

### Invocation Deadline (per attempt)
- When a job attempt starts (`runner.Run`) and when each task attempt starts (`DoTask`), compute `invocationDeadline = now + invocationTimeout` if set and >0.
- Create an attempt-scoped context with that deadline; inject it into:
  - `runner.awaitUntil`/`AwaitDuration`
  - `runner.awaitChild` (async awaits)
  - Task/Job contexts via a new `Context() context.Context` accessor (or a context field) so workers can honor cancellation.
- Worker execution runs in a goroutine and is observed via `select`:
  - `result := worker.Run(...)`
  - `<-deadlineCtx.Done()` triggers timeout handling.
  - If the worker returns after the timeout fired, the timeout still wins for that attempt.

### Await/Backoff Integration
- Any awaited sleep/backoff clamps to the remaining time before the **invocation** deadline. If `remaining <= 0`, short-circuit to timeout.
- If a runner is recycled while waiting, on replay the recomputed deadline uses the original attempt start time stored in memory:
  - Store `attemptStart` alongside await state in-memory.
  - Persist the timeout outcome in the chapter; replays won’t re-enter the timed-out attempt.

### Total Deadline (across attempts)
- Compute `totalDeadline = start + totalTimeout` when total timeout is set and >0.
  - For jobs: `start` = chapter 0 `CreatedAt` (initial chapter) or earlier runner start if present; subsequent steps use the `CreatedAt` of the immediately preceding chapter as the “start” for the next step.
  - For tasks: `start` = `CreatedAt` of the prior chapter (the input chapter for this task). If no prior chapter exists (first task), use chapter 0 `CreatedAt`. This anchors total time to the last completed work, not the current wall clock.
  - If the current attempt has already persisted a chapter (e.g., a prior failed attempt), recompute remaining total budget from that chapter’s `CreatedAt`.
- Every await/backoff must also clamp to the remaining total budget. If `remainingTotal <= 0`, surface a total-timeout immediately.
- When a timeout occurs due to total limit, it ends the *current* attempt and prevents further attempts (overall budget consumed).
- Replays derive the same `totalDeadline` from persisted start time; no extra jitter is introduced.

### Timeout Outcome & Persistence
- On invocation timeout:
  - Persist a chapter for that attempt with payload kind `Timeout`, payload from `NewTimeoutError(..., scope="invocation", retryable=true)`.
  - Meta includes `Attempt`, `InputHash`, `InputRef`, and (when present) `RunPolicy` with timeouts.
  - Retry evaluation uses the timeout payload’s `retryable` flag; invocation timeouts can retry until attempts or total budget are exhausted.
- On total timeout:
  - Persist a chapter for the current attempt with payload kind `Timeout`, payload from `NewTimeoutError(..., scope="total", retryable=false)`.
  - No further attempts are allowed because the overall budget is exhausted; cached total timeouts immediately surface and are non-retryable.
- Cached timeouts behave like any cached error: invocation timeouts retry if both attempts remain and total budget remains; total timeouts never retry.

### Async Child Interactions
- `AwaitChild` honors both deadlines: whichever (invocation or total) expires first triggers a timeout for the parent attempt.
- Spawning children is allowed even when close to a deadline; timeouts only affect the parent attempt, not the child job.

### Determinism & Replay
- Timeout durations are part of serialized `RunPolicy`; replays enforce the same limits.
- Invocation timeouts use attempt start time; total timeouts use the persisted first-attempt start/initial chapter time. Crashes do not grant extra time.
- Timeout payloads are explicit (`Timeout` kind) so mixed system errors stay distinguishable.
- No new persisted timestamp fields beyond existing chapter `CreatedAt`; once a timeout chapter is written, replays skip to retry/backoff using existing metadata and total-deadline computation.

### Crash/Replay Behavior
- If a node crashes mid-attempt, on replay:
  - Invocation deadline is recomputed from the new attempt start time (it is per-attempt).
  - Total deadline is recomputed from the prior chapter’s `CreatedAt` (the end of the previous step). Crashes do not extend the total budget because the anchor time is persisted.
- If a timeout chapter was written before the crash, cached behavior applies (retryable for invocation, terminal for total).
- If no chapter was written, the next runner resumes with the same total deadline basis (prior chapter time) and a fresh invocation window for the new attempt.

## Testing Notes
- Unit tests in `impl/retry_test`/`impl/runner_test`:
  - Task attempt invocation timeout saves `Timeout` payload with `scope=invocation`, `retryable=true`.
  - Job total timeout stops further retries even with remaining attempts; payload `scope=total`, `retryable=false`.
  - Cached timeout + remaining attempts triggers backoff and rerun until total deadline is exceeded.
  - Zero/negative timeouts disable that scope; task override `invocation_timeout: 0` clears job-level invocation timeout; same for total.
  - Await/backoff clamped to remaining invocation/total budget.
- Integration test: long-running task/job gets preempted by invocation timeout, then retries succeed on a fast worker; separate scenario where total timeout expires across retries/awaits and surfaces immediately on replay.
