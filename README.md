# swf-go

A durable workflow library for Go that provides reliable, long-running workflow orchestration with built-in retry logic, timeout handling, and persistent state management.

## What is swf-go?

swf-go is a workflow orchestration library that helps you build reliable, distributed workflows. It handles the complexity of managing workflow state, retries, timeouts, and task coordination so you can focus on your business logic.

**Key Features:**
- **Durable Workflows**: Workflow state persists across failures and restarts
- **Task Orchestration**: Break workflows into reusable task units
- **Automatic Retries**: Configurable retry policies with exponential backoff
- **Timeout Management**: Set invocation and total timeout limits
- **Async Child Workflows**: Spawn and await child workflows
- **Artifact Support**: Handle large files and binary data efficiently
- **Multi-Tenant**: Built-in tenant isolation
- **Job Querying**: List and filter jobs with flexible criteria
- **Schedules**: First-class recurring jobs with pause/resume/archive support
- **Embedded SQLite Runtime**: Durable local execution without external services
- **Remote Runtime Protocol**: REST runtime adapter with tokenized lease operations

## Installation

```bash
go get github.com/colony-2/swf-go
```

## Local Runtime CLI

The repo includes a Cobra-based local runtime server at `cmd/swfd`.

Run the default SQLite-backed embedded runtime:

```bash
go run ./cmd/swfd --listen 127.0.0.1:9047 --db swf.db
```

The SQLite runtime stores jobs, chapters, artifacts, leases, and schedules in a
local SQLite database plus a blob directory.

Run the in-memory toy runtime explicitly:

```bash
go run ./cmd/swfd toy --listen 127.0.0.1:9047
```

For Go module migration details, including moving embedded direct-runtime users
to SQLite, see
[`docs/MIGRATION-SQLITE-EMBEDDED-RUNTIME.md`](docs/MIGRATION-SQLITE-EMBEDDED-RUNTIME.md).

## Quick Start

Here's a simple workflow that processes data through multiple tasks:

```go
package main

import (
    "context"
    "log"
    "github.com/colony-2/swf-go/pkg/swf"
    "github.com/colony-2/swf-go/pkg/swf/impl"
)

// Define a job worker (orchestrates tasks)
type DataProcessingJob struct{}

func (j DataProcessingJob) Name() string { return "data_processing" }

func (j DataProcessingJob) Run(ctx swf.JobContext, input swf.JobData) (swf.JobData, error) {
    // Execute tasks in sequence
    result, err := ctx.DoTask(swf.DefaultRunPolicy(), "validate", input)
    if err != nil {
        return nil, err
    }

    result, err = ctx.DoTask(swf.DefaultRunPolicy(), "transform", result)
    if err != nil {
        return nil, err
    }

    return result, nil
}

// Define task workers
type ValidateTask struct{}

func (t ValidateTask) Name() string { return "validate" }

func (t ValidateTask) Run(ctx swf.TaskContext, input swf.TaskData) (swf.TaskData, error) {
    // Your validation logic here
    return input, nil
}

type TransformTask struct{}

func (t TransformTask) Name() string { return "transform" }

func (t TransformTask) Run(ctx swf.TaskContext, input swf.TaskData) (swf.TaskData, error) {
    // Your transformation logic here
    return input, nil
}

func main() {
    // Build the engine
    engine, err := swf.NewEngineBuilder().
        WithPostgresDSN("postgres://user:pass@localhost/db").
        WithStrata("http://strata-server:8080").
        WithStrataAPIKey("your-api-key").
        WithWorkerTenantId("my-tenant").
        PlusWorkers(DataProcessingJob{}, ValidateTask{}, TransformTask{}).
        Build(impl.Builder)
    if err != nil {
        log.Fatal(err)
    }

    ctx := context.Background()

    // Start the engine worker loop
    go engine.Run(ctx)

    // Start a job
    input := swf.NewTaskDataOrPanic(map[string]interface{}{"value": 42})
    jobKey, err := engine.StartJob(ctx, swf.StartJob{
        TenantId: "my-tenant",
        JobType:  "data_processing",
        Data:     input,
    })
    if err != nil {
        log.Fatal(err)
    }

    log.Printf("Started job: %s", jobKey)
}
```

## Core Concepts

### Jobs vs Tasks

- **Jobs** are the top-level workflows that orchestrate tasks. A job worker defines the workflow logic.
- **Tasks** are individual units of work within a job. Task workers implement specific operations.

Jobs use `JobContext` to execute tasks, wait, and spawn child workflows. Tasks receive `TaskContext` for execution context.

### JobWorker Interface

```go
type JobWorker interface {
    Name() string
    Run(JobContext, JobData) (JobData, error)
}
```

Your job worker orchestrates the workflow:

```go
func (j MyJob) Run(ctx swf.JobContext, input swf.JobData) (swf.JobData, error) {
    // Execute tasks
    result, err := ctx.DoTask(policy, "task-name", taskInput)

    // Wait/sleep
    ctx.AwaitDuration(swf.Duration(5 * time.Minute))

    // Spawn async child workflow
    future, err := ctx.SpawnAsync("child-job-type", childInput)
    output, err := future.Await(context.Background())

    return output, nil
}
```

### TaskWorker Interface

```go
type TaskWorker interface {
    Name() string
    Run(TaskContext, TaskData) (TaskData, error)
}
```

Your task worker implements a specific operation:

```go
func (t MyTask) Run(ctx swf.TaskContext, input swf.TaskData) (swf.TaskData, error) {
    // Access job context
    ctx.Logger.Info("processing task", "job", ctx.JobKey, "step", ctx.Step)

    // Wait if needed
    ctx.AwaitDuration(swf.Duration(30 * time.Second))

    // Return result
    return swf.NewTaskData(result)
}
```

## Working with Data

### Creating TaskData

```go
// From a struct or map
data, err := swf.NewTaskData(map[string]interface{}{
    "userId": 123,
    "action": "process",
})

// Panic version for tests/simple cases
data := swf.NewTaskDataOrPanic(myStruct)

// With artifacts
data, err := swf.NewTaskData(payload, artifact1, artifact2)
```

### Reading TaskData

```go
func (t MyTask) Run(ctx swf.TaskContext, input swf.TaskData) (swf.TaskData, error) {
    // Get raw JSON data
    rawData, err := input.GetData()
    if err != nil {
        return nil, err
    }

    // Unmarshal into your struct
    var payload MyPayload
    if err := json.Unmarshal(rawData, &payload); err != nil {
        return nil, err
    }

    // Access artifacts
    artifacts, err := input.GetArtifacts()
    if err != nil {
        return nil, err
    }

    return swf.NewTaskData(result)
}
```

## Working with Artifacts

Artifacts represent file-like data that flows through workflows. They support lazy loading and automatic cleanup.

### Creating Artifacts

```go
// From bytes (in-memory)
artifact := swf.NewArtifactFromBytes("config.json", jsonBytes)

// From a reader
artifact := swf.NewArtifactFromReader("output.txt", reader, size)

// From a file (auto-cleanup enabled)
artifact, err := swf.NewArtifactFromFile("build.tar.gz", "/tmp/build.tar.gz")

// From a file (no cleanup)
artifact, err := swf.NewArtifactFromFileNoCleanup("data.csv", "/data/input.csv")

// Custom artifact with full control
artifact := swf.NewArtifact("custom.dat",
    func() (io.ReadCloser, int64, error) {
        // Your opener logic
        f, _ := os.Open(path)
        info, _ := f.Stat()
        return f, info.Size(), nil
    },
    func() error {
        // Your cleanup logic
        return os.Remove(path)
    },
)
```

### Using Artifacts

```go
// Get artifact metadata
name := artifact.Name()           // "output.tar.gz"
size := artifact.Size()            // size in bytes

// Stream artifact contents
rc, err := artifact.Open()
if err != nil {
    return err
}
defer rc.Close()
// ... read from rc

// Write to a file
err = artifact.SaveToFile(ctx, "/output/file.tar.gz")

// Get full contents (use carefully for large files)
data, err := artifact.Bytes(ctx)

// Compute SHA256 hash
hash, err := artifact.Sha256(ctx)
```

### Artifacts in Tasks

```go
func (t ProcessFileTask) Run(ctx swf.TaskContext, input swf.TaskData) (swf.TaskData, error) {
    artifacts, err := input.GetArtifacts()
    if err != nil {
        return nil, err
    }

    // Process the first artifact
    if len(artifacts) > 0 {
        inputFile := artifacts[0]

        // Save to local file for processing
        tmpPath := "/tmp/input.dat"
        if err := inputFile.SaveToFile(context.Background(), tmpPath); err != nil {
            return nil, err
        }

        // Process the file...
        processFile(tmpPath)

        // Create output artifact
        outputArtifact, err := swf.NewArtifactFromFile("output.dat", "/tmp/output.dat")
        if err != nil {
            return nil, err
        }

        return swf.NewTaskData(result, outputArtifact)
    }

    return input, nil
}
```

## Retry and Timeout Policies

### RunPolicy Configuration

```go
policy := swf.RunPolicy{
    Retry: swf.RetryPolicy{
        InitialInterval:    swf.Duration(100 * time.Millisecond),
        BackoffCoefficient: 2.0,
        MaximumInterval:    swf.Duration(30 * time.Second),
        MaximumAttempts:    5,
        NonRetryableErrorTypes: []string{"ValidationError"},
    },
    InvocationTimeout: swf.AsDuration(30 * time.Second),  // Per attempt
    TotalTimeout:      swf.AsDuration(10 * time.Minute),  // Overall
}

result, err := ctx.DoTask(policy, "my-task", input)
```

### Default Policy

```go
// Use the default policy
result, err := ctx.DoTask(swf.DefaultRunPolicy(), "my-task", input)

// Default values:
// - InvocationTimeout: 30 seconds
// - TotalTimeout: 30 minutes
// - InitialInterval: 100ms
// - BackoffCoefficient: 2.0
// - MaximumInterval: 30 seconds
// - MaximumAttempts: 3
```

## Error Handling

### Application Errors

Regular errors returned from your workers are treated as application errors and will trigger retries according to the retry policy:

```go
func (t MyTask) Run(ctx swf.TaskContext, input swf.TaskData) (swf.TaskData, error) {
    if err := validateInput(input); err != nil {
        return nil, fmt.Errorf("validation failed: %w", err)
    }
    return result, nil
}
```

### System Errors

System errors represent infrastructure failures:

```go
if err := connectToDatabase(); err != nil {
    return nil, swf.NewSystemError(swf.SystemErrorPayload{
        Message:   "database connection failed",
        Component: "database",
        Code:      "connection_error",
        Retryable: true,
    })
}
```

### Non-Retryable Errors

Mark errors as non-retryable to stop retry attempts immediately:

```go
type ValidationError struct {
    error
}

func (e ValidationError) NonRetryable() bool {
    return true
}

// Usage
if !isValid(input) {
    return nil, ValidationError{errors.New("invalid input")}
}
```

### Checking Error Types

```go
if swf.IsAppError(err) {
    // Handle application error
}

if swf.IsSystemError(err) {
    // Handle system error
}
```

## Advanced Features

### Async Child Workflows

Spawn child workflows and await their completion:

```go
func (j ParentJob) Run(ctx swf.JobContext, input swf.JobData) (swf.JobData, error) {
    // Spawn async child
    future, err := ctx.SpawnAsync("child-job-type", childInput)
    if err != nil {
        return nil, err
    }

    // Await completion
    result, err := future.Await(context.Background())
    if err != nil {
        return nil, err
    }

    return result, nil
}
```

### Job Restart

Restart a failed job from a specific step:

```go
newJobKey, err := engine.RestartJob(ctx, swf.RestartJob{
    PriorJobKey:    failedJobKey,
    LastStepToKeep: 5,  // Replay from step 6 onwards
    StartJob: swf.StartJob{
        TenantId: "my-tenant",
        JobType:  "my-job",
        Data:     newInput,
    },
})
```

### Job Cancellation

```go
err := engine.CancelJob(ctx, swf.CancelJob{
    JobKey: jobKey,
    Reason: "user requested cancellation",
})
```

### Checking Job Status

```go
status, err := engine.CheckJobStatus(ctx, jobKey)

switch status {
case swf.JobStatusCompleted:
    // Job finished successfully
case swf.JobStatusActive:
    // Job is running
case swf.JobStatusCancelled:
    // Job was cancelled
case swf.JobStatusReady:
    // Job is ready to run
}
```

### Getting Job Results

```go
result, err := engine.GetJobResult(ctx, jobKey)
if err == swf.ErrJobNotComplete {
    // Job hasn't completed yet
    return
}

// Use result
data, _ := result.GetData()
```

### Listing Jobs

```go
resp, err := engine.ListJobs(ctx, swf.ListJobsRequest{
    TenantIds:     []string{"my-tenant"},
    Statuses:      []swf.JobStatus{swf.JobStatusActive, swf.JobStatusCompleted},
    JobTypes:      []string{"data-processing"},
    PageSize:      50,
    PageToken:     "", // empty for first page
})

for _, job := range resp.Jobs {
    log.Printf("Job %s: %s", job.JobKey, job.Status)
}

// Get next page
if resp.NextPageToken != "" {
    nextResp, err := engine.ListJobs(ctx, swf.ListJobsRequest{
        PageToken: resp.NextPageToken,
        // ... other filters
    })
}
```

### Schedules

Schedules are runtime-owned recurring job definitions. A schedule target is the
same shape as a job start: job type, input `TaskData`, run policy, and app
metadata. The runtime stores the target, including an artifact snapshot, and
materializes each occurrence as a normal app job.

```go
start := time.Now().UTC()

info, err := engine.UpsertSchedule(ctx, swf.UpsertScheduleRequest{
    TenantId:   "my-tenant",
    ScheduleId: "daily-cleanup",
    Trigger: swf.ScheduleTrigger{
        Kind:     swf.ScheduleTriggerInterval,
        Interval: 24 * time.Hour,
        StartAt:  &start,
    },
    Target: swf.ScheduleTarget{
        JobType:  "data-processing",
        Data:     swf.JobData(swf.NewTaskDataOrPanic(map[string]any{"bucket": "reports"})),
        Metadata: json.RawMessage(`{"owner":"analytics"}`),
    },
    OverlapPolicy: swf.ScheduleOverlapSerial,
})
if err != nil {
    return err
}

log.Printf("next scheduled job: %s", info.NextJobKey)
```

The schedule API includes `GetSchedule`, `ListSchedules`, `PauseSchedule`,
`ResumeSchedule`, `ArchiveSchedule`, `TriggerSchedule`, and
`ListScheduleRuns`. With serial overlap policy, the runtime submits the next
occurrence before app execution starts, but makes it wait for the previous
occurrence to complete before it can be leased.

### External Task Completion

For tasks that require external input (e.g., human approval), you can complete them externally:

```go
// Find tasks waiting for capability
handles, err := engine.FindTasksWaitingForCapability(ctx,
    "approval-job",    // job type
    "human-approval",  // task type
    []string{"tenant-1"}, // tenants (nil for all)
)

for _, handle := range handles {
    // Get task input
    input, err := handle.Data()

    // ... process externally ...

    // Complete the task
    output := swf.NewTaskDataOrPanic(approvalResult)
    err = handle.Finish(ctx, output)
}
```

## Engine Configuration

### Builder Options

```go
engine, err := swf.NewEngineBuilder().
    WithPostgresDSN("postgres://user:pass@localhost/db").  // Required
    WithStrata("http://strata:8080").                      // Required
    WithStrataAPIKey("api-key").                            // Required
    WithMaxActive(10).                                      // Concurrent task limit
    WithLogger(logger).                                     // Custom logger
    WithAwaitRecycleThreshold(5 * time.Minute).            // Await recycle threshold
    PlusWorkers(job1, task1, task2).                       // Register workers
    PlusWorkers(job2, task3).                               // Add more workers
    Build(impl.Builder)
```

### Registering Workers

#### At Engine Build Time

Workers can be registered during engine construction:

```go
builder := swf.NewEngineBuilder().
    WithPostgresDSN(dsn).
    WithStrata(url).
    WithStrataAPIKey(key)

// Register a job with its tasks
builder.PlusWorkers(
    MyJobWorker{},
    Task1{},
    Task2{},
)

// Register another job
builder.PlusWorkers(
    AnotherJobWorker{},
    Task3{},
)

engine, err := builder.Build(impl.Builder)
```

#### After Engine Start (Dynamic Registration)

Workers can also be registered after the engine has started:

```go
// Engine was built with WithWorkerTenantId("my-tenant") and is already running.
go engine.Run(ctx)

// Create a workset
workset, err := swf.AsWorkSet(
    NewJobWorker{},
    NewTask1{},
    NewTask2{},
)
if err != nil {
    log.Fatal(err)
}

// Register dynamically
err = engine.RegisterWorkers(workset)
if err != nil {
    log.Fatal(err)
}

// The engine can now process jobs of type NewJobWorker.Name()
```

This is useful for:
- Plugin systems where workers are loaded dynamically
- Multi-tenant systems where different tenants have different workflows
- Hot-reloading worker implementations without restarting the engine

### Running the Engine

```go
ctx := context.Background()

// Run the engine worker loop (blocks)
engine.Run(ctx)

// Or run in background
go engine.Run(ctx)

// Cancel when done
ctx, cancel := context.WithCancel(context.Background())
defer cancel()

go engine.Run(ctx)
// ... do work ...
cancel() // Gracefully stop the engine
```

Engines with registered workers must be built with `WithWorkerTenantId`.
The worker loop polls only that tenant; run a separate engine for another
tenant.

## Best Practices

1. **Keep Tasks Idempotent**: Tasks may be retried, so ensure they can safely run multiple times
2. **Use Appropriate Timeouts**: Set realistic invocation and total timeouts based on expected task duration
3. **Handle Large Data with Artifacts**: Use artifacts for files and binary data instead of embedding in TaskData
4. **Log Generously**: Use `ctx.Logger` to log progress and debug issues
5. **Design for Failure**: Workflows should gracefully handle task failures and retries
6. **Clean Up Resources**: Implement proper cleanup in artifact handlers
7. **Use Singleton Keys**: For jobs that should only run once (e.g., daily reports)
8. **Monitor Job Status**: Use ListJobs and CheckJobStatus to monitor workflow health

## Architecture Notes

swf-go can run against several runtime backends:

- **SQLite runtime**: Stores workflow state, leases, schedules, Strata row data,
  and blobfs artifacts locally. This is the default embedded runtime and the
  default `swfd` mode.
- **Postgres/Strata direct runtime**: Stores workflow state and coordinates
  distributed execution through pgwf, with workflow data and artifacts in
  Strata.
- **Remote runtime**: Uses the same `WorkflowRuntime` API over REST. The server
  owns lease tokens and schedule preflight; clients and workers stay generic.

Multiple engine instances can run concurrently when the selected runtime backend
supports shared coordination.

## License

See LICENSE file for details.
