# Restart LastStepToKeep Boundary Validation

## Goal
Prevent `RestartJob` from trimming history at a point that bisects an in-flight retry. A restart should only occur on a stable task (or job) boundary so future retries are predictable: you can restart **before** a task starts or **after** its final recorded attempt, but never between attempts of the same task/job.

## Problem Today
- `LastStepToKeep` is only validated as `>= 0`.
- Callers can choose an ordinal that sits after attempt 1 of a task that later retried (attempt 2+). The new job replays from the middle of that retry chain, making retry math and input hashes hard to reason about.
- Applies to both real SWF engine (Strata-backed) and ToyEngine (in-memory) for API parity.

## Boundary Rule (authoritative)
`LastStepToKeep` is valid **iff** the chapter at `next = LastStepToKeep + 1` exists **and** its envelope metadata indicates `Attempt == 1` (or missing/0, treated as 1 for backward compatibility).
- If the `next` chapter is missing → **reject** (caller tried to trim to the end of history; restart must point to a real boundary with a recorded first attempt).
- If `Attempt > 1` → **reject** (restart would slice through a retry chain).

Intuition: the step immediately after the trimmed history must be the first attempt of its operation (task or job result). That guarantees the replay starts at a clean boundary.

## Durable Engine Changes (pkg/swf/impl)
- **Validation location:** inside `RestartJob` before cloning.
- **Steps:**
  1. Fetch chapter at `LastStepToKeep + 1` only (we no longer need to read `LastStepToKeep`; clone will fail later if it’s out of range).
     - If not found → return `fmt.Errorf("LastStepToKeep %d invalid: no chapter at ordinal %d", lastStep, next)`.
     - If found → decode envelope; read `Meta.Attempt` (treat 0 as 1). If `Attempt > 1`, return `fmt.Errorf("LastStepToKeep %d cuts into retry chain: next ordinal %d is attempt %d of %s", ...)`.
  2. Proceed with existing clone/extra-output logic only after validation passes.
- **Compatibility:** old chapters lacking `attempt` metadata pass (treated as attempt 1). No change to stored schema.
- **Performance:** one extra Strata read (ordinal `L+1`); acceptable because Restart already fetches chapter 0 and clones.
- **ExtraTaskOutput path:** still validated against source history; appending cached output at `L+1` is allowed only if the boundary rule passes.

## ToyEngine Changes (pkg/swf/toy)
- Extend `toyChapter` to record `Attempt` (default 1 for today’s single-attempt execution).
- On task execution, set chapter `Attempt = 1` (future retries, if added, increment accordingly).
- In `RestartJob`, perform the same boundary rule using the in-memory chapters map.
- Error message mirrors durable engine for parity.

## Tests
- **Durable engine:** `restart_job_test.go`
  - `TestRestartJobRejectsMidTaskRetry`: job with a task that fails once then succeeds (two chapters for same task). Restart with `LastStepToKeep` pointing to the first attempt → expect error; using the second attempt ordinal → succeeds.
  - `TestRestartJobRejectsMidJobRetry` (optional but recommended): job-level retry (job worker fails then succeeds). Restart after first job attempt → expect rejection.
- **ToyEngine:** `toy_test.go`
  - Craft a job, then manually mark the next chapter’s `Attempt = 2` (or simulate via helper). Restart with `LastStepToKeep` immediately before that chapter → expect boundary error. A restart at a true boundary still succeeds.

## Documentation Updates
- README “Job Restart” section: call out the boundary rule with the task3 example.
- `toy_engine_spec.md`: note that restarts honor `LastStepToKeep` and validate boundaries (even though toy tasks currently run single-attempt).

## Out of Scope
- Changing retry semantics, backoff, or chapter schema.
- New public API fields or error types; surfaced as `RestartJob` validation errors only.
