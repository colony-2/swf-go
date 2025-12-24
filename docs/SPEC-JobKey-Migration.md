# Specification: JobKey API Change for Per-Job Multi-Tenancy

## Status
**Proposed** | Author: System | Date: 2025-12-24

## Overview

This specification defines a fundamental architectural change to the SWF Engine API to support multi-tenancy at the job level rather than at the engine level. This is a **single breaking change** with no backwards compatibility or phased rollout.

**Key changes:**

1. **Replace `JobId` with `JobKey`** in all public APIs
2. **Define `JobKey` as a composite type** containing `TenantId` + `JobId`
3. **Remove `tenantId` from `EngineBuilder`** since tenant scope moves from engine-level to job-level

## Motivation

### Current Architecture Limitations

The current architecture has a fundamental constraint: **one engine instance = one tenant**. This creates several issues:

1. **Resource Inefficiency**: Multi-tenant applications must maintain separate engine instances per tenant, duplicating connection pools, workers, and polling loops.

2. **Scaling Complexity**: Cannot dynamically handle jobs across tenants within a single engine, limiting horizontal scaling options.

3. **API Inconsistency**: The public API uses `JobId` alone, but internally must constantly combine it with the engine's `tenantId` to form `story.Key{AnthologyID, StoryID}` (26+ locations in the codebase).

4. **Implicit Context**: Callers must ensure they use the correct engine instance for a tenant, leading to potential routing errors.

### Benefits of JobKey Approach

1. **Explicit Multi-Tenancy**: `JobKey` makes tenant identity explicit in every API call, eliminating ambiguity.

2. **Resource Consolidation**: Single engine instance can handle jobs across multiple tenants, sharing infrastructure efficiently.

3. **Simplified Architecture**: No need to maintain tenant-to-engine mappings at the application layer.

4. **Type Safety**: Composite `JobKey` type prevents passing bare job IDs that lack tenant context.

## Current Architecture

### Engine Builder with TenantId

**Location**: `/src/pkg/swf/jobs.go`

```go
type EngineBuilder struct {
    workers      map[string]WorkSet
    tenantId     string              // Single tenant for entire engine
    maxActive    int
    strataURI    string
    strataAPIKey string
    postgresDSN  string
    logger       *slog.Logger
    awaitRecycle time.Duration
}

func NewEngineBuilder(tenantId string) *EngineBuilder
```

The `tenantId` is:
- Set once at engine creation
- Stored in `EngineBuilder`
- Passed to the `Builder` function during `.Build()`
- Stored in the engine implementation as a field

### Current JobId Type

**Location**: `/src/pkg/swf/types.go:108`

```go
type JobId string
```

Simple string identifier with no tenant information.

### Public APIs Using JobId

#### jobRunApi (`/src/pkg/swf/jobs.go:67`)
```go
type jobRunApi interface {
    StartJob(ctx context.Context, start StartJob) (JobId, error)
    RestartJob(ctx context.Context, restart RestartJob) (JobId, error)
    CancelJob(ctx context.Context, cancel CancelJob) error
    CheckJobStatus(ctx context.Context, jobId JobId) (JobStatus, error)
    GetJobResult(ctx context.Context, jobId JobId) (TaskData, error)
}
```

#### taskRunApi (`/src/pkg/swf/tasks.go:10`)
```go
type taskRunApi interface {
    FindTasksWaitingForCapability(ctx context.Context, jobType string, taskType string) ([]TaskHandle, error)
    GetWaitingTask(ctx context.Context, id JobId) (TaskHandle, error)
}
```

#### Request/Response Types
- `StartJob.JobID`: Optional job ID to use
- `RestartJob.PriorJobId`: Reference to prior job
- `CancelJob.JobId`: Job to cancel
- `ListJobsRequest.JobIDs`: Filter by job IDs
- `JobSummary.JobID`: Job identifier in listings
- `JobSummary.WaitFor`: Array of job IDs for dependencies
- `Future.JobID`: Async child job reference
- `TaskHandle.JobId()`: Method returning job ID

### Internal Story Key Construction

Throughout the codebase (26+ locations), the engine manually constructs Strata story keys:

```go
key := story.Key{
    AnthologyID: s.tenantId,      // From engine field
    StoryID:     string(jobId),   // From method parameter
}
```

**Key locations**:
- `/src/pkg/swf/impl/engine.go`: 258, 299, 652
- `/src/pkg/swf/impl/runner.go`: 210, 440, 482, 526, 561
- `/src/pkg/swf/impl/task.go`: 29, 69

## Proposed Architecture

### New JobKey Type

**Location**: `/src/pkg/swf/types.go` (replace JobId definition)

```go
// JobKey uniquely identifies a job across all tenants.
// It combines tenant identity with job identity.
type JobKey struct {
    TenantId string `json:"tenantId"`
    JobId    string `json:"jobId"`
}

// String returns a string representation of the JobKey.
// Format: "tenantId/jobId"
func (jk JobKey) String() string {
    return fmt.Sprintf("%s/%s", jk.TenantId, jk.JobId)
}

// ParseJobKey parses a string representation back into a JobKey.
// Expected format: "tenantId/jobId"
func ParseJobKey(s string) (JobKey, error) {
    parts := strings.Split(s, "/")
    if len(parts) != 2 {
        return JobKey{}, fmt.Errorf("invalid JobKey format: %s", s)
    }
    return JobKey{
        TenantId: parts[0],
        JobId:    parts[1],
    }, nil
}

// ToStoryKey converts a JobKey to a Strata story.Key.
func (jk JobKey) ToStoryKey() story.Key {
    return story.Key{
        AnthologyID: jk.TenantId,
        StoryID:     jk.JobId,
    }
}

// JobKeyFromStoryKey creates a JobKey from a Strata story.Key.
func JobKeyFromStoryKey(sk story.Key) JobKey {
    return JobKey{
        TenantId: sk.AnthologyID,
        JobId:    sk.StoryID,
    }
}

// IsZero returns true if the JobKey is the zero value.
func (jk JobKey) IsZero() bool {
    return jk.TenantId == "" && jk.JobId == ""
}

// Validate checks if the JobKey is valid.
func (jk JobKey) Validate() error {
    if jk.TenantId == "" {
        return fmt.Errorf("TenantId cannot be empty")
    }
    if jk.JobId == "" {
        return fmt.Errorf("JobId cannot be empty")
    }
    return nil
}
```

### Modified EngineBuilder

**Location**: `/src/pkg/swf/jobs.go`

```go
type EngineBuilder struct {
    workers      map[string]WorkSet
    // REMOVED: tenantId field
    maxActive    int
    strataURI    string
    strataAPIKey string
    postgresDSN  string
    logger       *slog.Logger
    awaitRecycle time.Duration
}

// Constructor no longer takes tenantId
func NewEngineBuilder() *EngineBuilder {
    return &EngineBuilder{
        workers:      make(map[string]WorkSet),
        maxActive:    1,
        awaitRecycle: 10 * time.Second,
    }
}
```

### Modified Builder Function Type

**Location**: `/src/pkg/swf/jobs.go`

```go
// Builder function no longer receives tenantId parameter
type Builder func(
    db *gorm.DB,
    strataClient *strataclient.Client,
    workers []WorkSet,
    logger *slog.Logger,
) (SWFEngine, error)
```

### Modified Engine Implementation

**Location**: `/src/pkg/swf/impl/engine.go`

```go
type swfEngineImpl struct {
    // REMOVED: tenantId field
    strata          *strataclient.Client
    db              *gorm.DB
    udb             *sql.DB
    workers         map[pgwf.Capability]*swf.WorkSet
    jobWorkers      map[string]*swf.WorkSet
    maxActiveTasks  int
    awaitRecycle    time.Duration
    logger          *slog.Logger
}

var Builder swf.Builder = func(
    db *gorm.DB,
    strataClient *strataclient.Client,
    workers []swf.WorkSet,
    logger *slog.Logger,
) (swf.SWFEngine, error) {
    // Implementation no longer stores tenantId
    f := swfEngineImpl{
        strata:  strataClient,
        db:      db,
        // ...
    }
    return &f, nil
}
```

### Updated Public APIs

#### jobRunApi

```go
type jobRunApi interface {
    StartJob(ctx context.Context, start StartJob) (JobKey, error)
    RestartJob(ctx context.Context, restart RestartJob) (JobKey, error)
    CancelJob(ctx context.Context, cancel CancelJob) error
    CheckJobStatus(ctx context.Context, jobKey JobKey) (JobStatus, error)
    GetJobResult(ctx context.Context, jobKey JobKey) (TaskData, error)
}
```

#### taskRunApi

```go
type taskRunApi interface {
    // Returns tasks across all tenants for the capability
    FindTasksWaitingForCapability(ctx context.Context, jobType string, taskType string) ([]TaskHandle, error)

    // Now takes JobKey instead of JobId
    GetWaitingTask(ctx context.Context, key JobKey) (TaskHandle, error)
}
```

#### Updated Request/Response Types

**StartJob** (`/src/pkg/swf/jobs.go`)
```go
type StartJob struct {
    TenantId     string   // REQUIRED: Tenant for this job
    JobType      string
    JobID        string   // Optional - if empty, generated via ksuid
    SingletonKey string
    Data         JobData
    RunPolicy    RunPolicy
}
```

**RestartJob**
```go
type RestartJob struct {
    PriorJobKey    JobKey  // Now uses JobKey
    LastStepToKeep int64
    StartJob               // Inherits TenantId field
}
```

**CancelJob**
```go
type CancelJob struct {
    JobKey JobKey  // Now uses JobKey
    Reason string
}
```

**ListJobsRequest**
```go
type ListJobsRequest struct {
    TenantIds     []string    // NEW: Filter by tenant(s), empty = all tenants
    Statuses      []JobStatus
    Stores        []JobStore
    JobTypes      []string
    JobTasks      []JobTaskFilter
    JobKeys       []JobKey    // Changed from JobIDs
    SingletonKeys []string
    CreatedAfter  *time.Time
    CreatedBefore *time.Time
    PageSize      int
    PageToken     string
}
```

**JobSummary**
```go
type JobSummary struct {
    JobKey          JobKey      // Changed from JobID
    Status          JobStatus
    JobType         string
    SingletonKey    *string
    WaitFor         []JobKey    // Changed from []JobId
    AvailableAt     time.Time
    ExpiresAt       *time.Time
    LeaseExpiresAt  *time.Time
    CancelRequested bool
    CreatedAt       time.Time
    ArchivedAt      *time.Time
    Payload         json.RawMessage
    TaskWaitInput   *int64
    TaskWaitOutput  *int64
    TaskWaitNext    *string
}
```

**TaskHandle Interface**
```go
type TaskHandle interface {
    JobKey() JobKey              // Changed from JobId()
    Data() (TaskData, error)
    Finish(ctx context.Context, taskData TaskData) error
    TaskOrdinalToComplete() int64
}
```

**Future (Async Child Jobs)**
```go
type Future struct {
    JobKey JobKey  // Changed from JobID
    await  func(context.Context) (TaskData, error)
}
```

#### Updated Helper Function

```go
func WaitForJobToComplete(
    ctx context.Context,
    timeout time.Duration,
    jobKey JobKey,  // Changed from jobId JobId
    engine SWFEngine,
) error
```

## Changes Required

This is a **single breaking change** with no backwards compatibility. All changes will be made atomically.

### Type Changes

1. **Remove `JobId` type** (`/src/pkg/swf/types.go:108`)
   - Delete the `type JobId string` definition
   - Replace with new `JobKey` struct

2. **Add `JobKey` type** with helper methods
   - `String()` - string representation
   - `ParseJobKey()` - parse from string
   - `ToStoryKey()` - convert to Strata story.Key
   - `JobKeyFromStoryKey()` - convert from Strata story.Key
   - `IsZero()` - check for zero value
   - `Validate()` - validate tenant and job IDs are not empty

### EngineBuilder Changes

1. **Constructor signature** (`/src/pkg/swf/jobs.go`)
   - Old: `NewEngineBuilder(tenantId string) *EngineBuilder`
   - New: `NewEngineBuilder() *EngineBuilder`

2. **Remove `tenantId` field** from `EngineBuilder` struct

3. **Builder function type** (`/src/pkg/swf/jobs.go`)
   - Old: `func(tenantId string, db *gorm.DB, ...) (SWFEngine, error)`
   - New: `func(db *gorm.DB, ...) (SWFEngine, error)`

### Engine Implementation Changes

1. **Remove `tenantId` field** from `swfEngineImpl` struct (`/src/pkg/swf/impl/engine.go`)

2. **Update Builder implementation** to remove tenantId parameter

3. **Replace all story.Key construction** (26+ locations)
   - Old: `story.Key{AnthologyID: s.tenantId, StoryID: string(jobId)}`
   - New: `jobKey.ToStoryKey()`

### Public API Changes

All methods and types that use `JobId` change to use `JobKey`:

#### Interface Methods

1. `StartJob(ctx, start) (JobKey, error)` - returns JobKey
2. `RestartJob(ctx, restart) (JobKey, error)` - returns JobKey
3. `CancelJob(ctx, cancel) error` - cancel.JobKey
4. `CheckJobStatus(ctx, jobKey) (JobStatus, error)` - parameter is JobKey
5. `GetJobResult(ctx, jobKey) (TaskData, error)` - parameter is JobKey
6. `GetWaitingTask(ctx, key) (TaskHandle, error)` - parameter is JobKey
7. `TaskHandle.JobKey() JobKey` - method name and return type
8. `WaitForJobToComplete(ctx, timeout, jobKey, engine)` - parameter is JobKey

#### Request/Response Types

1. **StartJob** - add `TenantId string` field (required)
2. **RestartJob** - change `PriorJobId JobId` to `PriorJobKey JobKey`
3. **CancelJob** - change `JobId JobId` to `JobKey JobKey`
4. **ListJobsRequest**:
   - Add `TenantIds []string` field
   - Change `JobIDs []JobId` to `JobKeys []JobKey`
5. **JobSummary**:
   - Change `JobID JobId` to `JobKey JobKey`
   - Change `WaitFor []JobId` to `WaitFor []JobKey`
6. **Future** - change `JobID JobId` to `JobKey JobKey`

### Test Updates

All tests must be updated to:
- Use `NewEngineBuilder()` without tenantId parameter
- Set `TenantId` field in `StartJob` requests
- Handle `JobKey` return values and parameters
- Use `JobKey` struct for all job references

## Implementation Considerations

### Database Schema

**No database changes required**. The database already stores jobs with separate tenant and job ID concepts via the underlying `pgwf-go` framework. The change is purely at the API layer.

### Internal Story Key Construction

All 26+ locations that currently construct `story.Key` manually:

```go
// OLD
key := story.Key{
    AnthologyID: s.tenantId,
    StoryID:     string(jobId),
}
```

Will be simplified to:

```go
// NEW
key := jobKey.ToStoryKey()
```

This is a major simplification and removes the dependency on the engine's tenantId field.

### Type Conversions

The underlying `pgwf-go` framework uses `pgwf.JobID` (string). The conversion layer will need to handle:

```go
// Extract job ID portion for pgwf
pgwfJobID := pgwf.JobID(jobKey.JobId)

// Reconstruct JobKey from pgwf response
jobKey := JobKey{
    TenantId: tenantId, // From context or other source
    JobId:    string(pgwfJobID),
}
```

### Multi-Tenant Job Queries

The `FindTasksWaitingForCapability` method returns tasks across ALL tenants. This is acceptable for the worker pool model, but implementers may want to add tenant filtering in the future.

Potential enhancement (not in this spec):
```go
FindTasksWaitingForCapability(
    ctx context.Context,
    jobType string,
    taskType string,
    tenantIds []string,  // Optional filter
) ([]TaskHandle, error)
```

### Singleton Keys

Singleton keys are currently scoped per tenant implicitly (via the engine's tenantId). With the new model:
- Singleton keys remain scoped per tenant
- The `StartJob.TenantId` field determines the singleton scope
- No changes needed to singleton logic, just tenant comes from the job request instead of engine

### WaitFor Dependencies

The `JobSummary.WaitFor []JobKey` field allows cross-tenant job dependencies. While technically possible, this may have security/isolation implications that should be validated:

**Recommendation**: Add validation that jobs can only wait for jobs within the same tenant, unless explicitly configured otherwise.

### Context Propagation

Consider adding context helpers for tenant ID:

```go
// Context helpers for passing tenant scope
func WithTenantId(ctx context.Context, tenantId string) context.Context
func TenantIdFromCtx(ctx context.Context) (string, bool)
```

This would allow middleware to inject tenant ID from authentication headers.

## Security Considerations

### Tenant Isolation

With tenant ID now in every API call, implement validation:

1. **Authentication Context**: Validate that the caller has access to the specified `TenantId`
2. **Job Dependencies**: Validate that `WaitFor` job keys are within the same tenant (or explicitly allowed)
3. **List Queries**: Filter results by caller's authorized tenants

### Audit Trail

The explicit `TenantId` in every operation improves audit logging:
- Every job operation can be logged with tenant context
- No ambiguity about which tenant a job belongs to

## Alternative Approaches Considered

### Alternative 1: Keep TenantId in Engine, Add it to Responses

Keep the engine-level tenantId but add it to response types:

```go
type JobId struct {
    TenantId string
    JobId    string
}

// Engine still has tenantId field
func NewEngineBuilder(tenantId string) *EngineBuilder
```

**Rejected because**:
- Still requires separate engines per tenant
- Doesn't solve resource inefficiency
- Confusing to have tenant in two places

### Alternative 2: Make TenantId Optional in JobKey

Allow `JobKey.TenantId` to be empty and fall back to engine's tenantId:

```go
type JobKey struct {
    TenantId string  // Optional, uses engine's if empty
    JobId    string
}
```

**Rejected because**:
- Defeats the purpose of explicit multi-tenancy
- Creates ambiguity and potential bugs
- Harder to validate and audit

### Alternative 3: Use String Encoding "tenant/jobid"

Instead of a struct, use a string with a delimiter:

```go
type JobKey string  // Format: "tenantId/jobId"
```

**Rejected because**:
- Less type-safe
- Parsing overhead on every operation
- Harder to validate at compile time
- No clear API for component access

## Open Questions

1. **Should FindTasksWaitingForCapability support tenant filtering?**
   - Current spec returns tasks across all tenants
   - May want to add optional `tenantIds []string` parameter

2. **Should we enforce same-tenant WaitFor dependencies?**
   - Current spec allows cross-tenant job dependencies
   - May have security implications

3. **Should we add context helpers for TenantId?**
   - Could simplify extracting tenant from auth middleware
   - `WithTenantId(ctx, tenantId)` and `TenantIdFromCtx(ctx)`

4. **Database indexes for multi-tenant queries?**
   - Need to evaluate if existing indexes support efficient multi-tenant queries
   - May need composite indexes on (tenant_id, job_id) or (tenant_id, status)

## Success Criteria

1. Single engine instance can handle jobs across multiple tenants
2. All public APIs use `JobKey` instead of `JobId`
3. TenantId removed from EngineBuilder and engine implementation
4. No manual `story.Key` construction in implementation (use `JobKey.ToStoryKey()`)
5. All tests pass with new API
6. Multi-tenant job queries work correctly

## References

- **Current Implementation**: `/src/pkg/swf/`
- **Story Key Usage**: `/src/pkg/swf/impl/engine.go`, `/src/pkg/swf/impl/runner.go`, `/src/pkg/swf/impl/task.go`
- **pgwf-go Framework**: External dependency for workflow primitives
- **Strata Client**: `/src/pkg/swf/impl/engine.go` (story.Key structure)
