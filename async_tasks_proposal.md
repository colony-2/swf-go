**Async Tasks Proposal (Draft)**

Goal: allow workflows to launch and await asynchronous child jobs (not individual tasks), where each async child runs as its own pgwf job. Support spawning from both job workers and task workers, with deterministic IDs and restart-friendly semantics. Child jobs can run whatever tasks they need internally.

---

- **Async job identity & journal**
  - When an async task is spawned, pre-record a journal entry with the intended async job ID before submission.
  - Deterministic ID scheme: `<parent-job-id>-<await-ordinal>`, where `await-ordinal` is the chapter ordinal at which we record the spawn.
  - Chapter payload at that ordinal includes the async invocation metadata (task type, inputs, any options), so replays can reconstruct the same async job ID and avoid duplicates.

- **Launch flow (spawn)**
  - New API on `JobContext` and `TaskContext`: `SpawnAsync(jobType string, data TaskData) (*Future, error)`.
  - Steps:
    1) Build async job ID as above; write a journal chapter at the current ordinal describing the spawn (id, task type, inputs).
    2) Submit a new pgwf child job with `JobType = async:<jobType>` and its own story initialized with the input data as chapter 0.
    3) Return a `Future` owned by the runner that wraps the async job ID and encapsulates await logic.
  - Spawns can occur inside job worker logic or inside task worker logic.

- **Await flow (hybrid)**
  - New API: `AwaitAsync(f *Future) (TaskData, error)`.
  - When called:
    - Check Strata for the async job’s completion chapter (final ordinal). If present, return its output.
    - If not complete:
      - Register for notification and park: await registers interest in the engine’s notification channel and parks the goroutine, waking immediately when a notification job arrives.
      - Recycling after timeout: if the engine decides to recycle (e.g., after ~20 minutes parked), it reschedules the parent job with `wait_for` on the async completion capability, records resume state, and `runtime.Goexit()` to free the worker. On wake, `AwaitAsync` re-checks completion; if still pending, it re-parks with notification or repeats the recycle loop.
  - Completion signaling:
    - The async job, when finished, writes its final output chapter and completes its pgwf job.
    - The async job also submits a notification job with a near-future `expire_at` to the parent engine’s notification capability so a parked awaiter can wake promptly. If the awaiter was recycled, the notification will expire and the wake will happen on the next reschedule.

- **pgwf modeling**
  - Async child runs as its own pgwf job:
    - JobType: `async:<jobType>` (or similar), Capability: `async:<jobType>` for step 1, then its inner tasks as usual.
    - Completion capability for await: `async-done:<childJobID>` (or `childJobID` if we reuse job-type capabilities).
  - Parent reschedule (recycle case):
    - When recycling, reschedule the parent job with `wait_for = [async-done:<childJobID>]` and `next_need` pointing back to the parent job worker, and store the ordinal in payload to resume at the await point.
    - On wake-up, parent `AwaitAsync` re-checks completion; if done, proceeds; otherwise can park again.

- **Notification channel (engine-local)**
  - Each engine creates and monitors a special pgwf capability, e.g., `NOTIFICATION-<workerId>`.
  - When an async child completes, it submits a lightweight “notification” job with a near-future `expire_at` whose `NextNeed` is the parent engine’s notification capability and whose payload includes routing info (parent job ID, await ordinal, child job ID).
  - Engines run a notification loop (similar to task runner) that consumes these jobs and routes them back to the awaiting runner (e.g., via an in-memory map keyed by parent job ID/ordinal to a parked future).
  - This enables prompt wakeups without polling Strata and avoids holding a pgwf lease while parked.

- **Contexts & logging**
  - `JobContext` / `TaskContext` gain async APIs; logger stays injected so async operations log consistently.

- **Strata usage**
  - Parent story records:
    - Spawn chapter at ordinal N with async metadata.
    - (Optional) Await state chapter to document recycling/waiting.
  - Child story:
    - Chapter 0 = input data; final chapter = output data.
  - Await uses Strata to fetch the child’s final chapter.

- **Determinism & replay**
  - Because the async job ID is deterministic and journaled before submission, reruns of the same ordinal will reuse the same async job ID and not spawn duplicates.
  - On replay, if the async job already exists and is complete, `AwaitAsync` returns immediately; if in-progress, parent can park/recycle.

- **Error handling**
  - If async job fails:
    - Await should surface the error (e.g., encode error in final chapter or via job status), and the parent can fail or branch accordingly.
  - If spawn fails after journaling:
    - Use the deterministic ID to retry submission on next run.

- **Testing hooks**
  - Provide an in-process implementation of async spawn/await for tests (possibly with an in-memory pgwf stub), and integration tests covering:
    1) Spawn + immediate await (completes inline).
    2) Spawn + recycle (parent awaits, recycles, wakes when child completes).
    3) Retry/replay: rerunning the parent doesn’t duplicate async child.

- **API sketches**
  - `type Future struct { JobID JobId; await func(context.Context) (TaskData, error) }` (runner owns creation; `await` may be method)
  - `SpawnAsync(ctx context.Context, taskType string, data TaskData) (*Future, error)`
  - `AwaitAsync(ctx context.Context, f *Future) (TaskData, error)`; wrapper calls `f.Await(ctx)`
  - Parent’s reschedule payload includes `awaitOrdinal`, `childJobID`, and resume info.

- **Library choice for Future**
  - Use `github.com/samber/lo`’s Promise as the underlying future/promise primitive. Wrap it in our `Future` type (thin adapter) so call sites stay simple and we can swap implementations later if needed. We’ll add context-aware helpers around `lo.Promise` for cancellation/timeouts.

- **Implementation steps**
  1) Add async APIs to contexts and engine.
  2) Implement deterministic child ID + journal write + pgwf submission.
  3) Add await logic with park/recycle and `wait_for` dependency.
  4) Define capabilities and status mapping for async completion.
  5) Add integration tests for spawn/await + recycling.
