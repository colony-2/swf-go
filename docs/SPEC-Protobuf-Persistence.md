# Specification: Protobuf Durable Persistence and Runtime API

## Status

**Proposed** | Author: Codex | Date: 2026-06-09

## Decision

SWF vNext stores SWF-owned structured data as protobuf binary instead of JSON.
The protobuf schema also becomes the source of truth for a new gRPC runtime API
that parallels the existing REST runtime API.

This is a breaking storage-format cutover. There is no JSON reader, no
dual-write period, no migration path, no format marker, and no legacy-store
detection. Operators must delete old jobs and use a fresh store before running
this version.

## Scope

This spec covers:

1. Task and job input/output payloads.
2. Chapter records, chapter metadata, and error payloads.
3. Runtime scheduler payloads such as run policy, task-wait state, and
   dependency lists.
4. Job metadata used for polling and list filters.
5. Restart metadata and prerequisite metadata.
6. A fully structured gRPC API that covers the current REST behavior without
   copying REST's POST/PUT split.

This spec does not require removing JSON from:

1. Logs, CLI diagnostics, or debug dump output.
2. Tests that print protobuf messages as JSON for human readability.
3. User artifact bytes. Artifacts are opaque files. A user may attach an
   artifact named `config.json`; SWF does not inspect or type it.

Artifacts do not have content types in SWF. The protobuf model must not add
artifact content-type fields.

## Goals

1. Remove JSON as the durable encoding for SWF-owned runtime state.
2. Make durable state and remote calls use typed protobuf messages.
3. Model variant data with protobuf `oneof` instead of string discriminators.
4. Use Go protobuf generation that is idiomatic for new Go code while keeping
   the `.proto` contract usable by future non-Go implementations.
5. Preserve workflow determinism with a canonical protobuf hashing rule.
6. Keep runtime implementations behind one shared protobuf codec and service
   contract.

## Non-Goals

1. Reading, detecting, or migrating legacy JSON persisted jobs.
2. Keeping the current `json.RawMessage` task payload API source-compatible.
3. Adding artifact MIME/content-type handling.
4. Requiring the REST API to disappear immediately.
5. Optimizing arbitrary metadata queries before correctness is in place.

## No Compatibility Machinery

The protobuf runtime must not add store markers or compatibility checks whose
only purpose is recognizing old JSON data.

Operational rule:

1. Stop all SWF workers and runtime servers.
2. Delete the old SWF runtime store, including jobs, chapters, leases,
   scheduler rows, and SWF-owned artifact records.
3. Create the protobuf schema from scratch.
4. Start the protobuf build.
5. Re-submit any work that should run under the new version.

Starting this version against an old JSON-backed database or blob root is
operator error. The runtime does not need to produce a friendly diagnostic for
that case.

## Go Protobuf API Choice

Use protobuf Edition 2024 for new SWF protos.

Edition 2024 defaults generated Go code to the Opaque API. That gives generated
messages hidden fields plus generated getters, setters, and builders. The
schema should therefore be written for normal Go accessors and immediate
builder use, not for direct struct-field mutation.

Go usage rules:

1. Generate with `google.golang.org/protobuf`; do not add new `gogo/protobuf`
   usage.
2. Commit generated `.pb.go` and gRPC `.pb.go` files.
3. Prefer generated builders for nested message literals in tests and
   construction-heavy code.
4. Prefer setters in hot paths when they are simpler or measurably faster.
5. Treat protobuf messages passed across SWF runtime boundaries as immutable by
   convention after construction, even though Go protobuf messages remain
   mutable.

Generated protobuf packages should be importable by external runtime
implementations, not hidden under Go `internal`.

Recommended generated Go package:

```text
github.com/colony-2/swf-go/pkg/swf/runtimepb/v1
```

Runtime implementations may still put helper codecs under an internal package:

```text
pkg/swf/internal/runtimecodec
```

## Proto Location

Add the canonical runtime proto at:

```text
proto/swf/runtime/v1/runtime.proto
```

The same proto file defines:

1. Public runtime API messages.
2. Durable chapter and scheduler messages.
3. gRPC service methods.

Implementations may split the file later if it becomes unwieldy, but the
messages below are the v1 contract.

## Normative Proto Definition

```proto
edition = "2024";

package swf.runtime.v1;

option go_package = "github.com/colony-2/swf-go/pkg/swf/runtimepb/v1;runtimepb";

import "google/protobuf/duration.proto";
import "google/protobuf/empty.proto";
import "google/protobuf/struct.proto";
import "google/protobuf/timestamp.proto";

service WorkflowRuntime {
  rpc SubmitJob(SubmitJobRequest) returns (JobHandle);
  rpc GetJob(GetJobRequest) returns (JobInfo);
  rpc SubmitRestartJob(SubmitRestartJobRequest) returns (JobHandle);
  rpc ListJobs(ListJobsRequest) returns (ListJobsResponse);

  rpc PollWork(PollWorkRequest) returns (PollWorkResponse);
  rpc GetJobLease(GetJobLeaseRequest) returns (GetJobLeaseResponse);
  rpc KeepAliveLease(KeepAliveLeaseRequest) returns (google.protobuf.Empty);
  rpc CompleteJobWithLease(CompleteJobWithLeaseRequest) returns (google.protobuf.Empty);
  rpc RescheduleJobWithLease(RescheduleJobWithLeaseRequest) returns (google.protobuf.Empty);

  rpc AddChapterWithLease(AddChapterWithLeaseRequest) returns (google.protobuf.Empty);
  rpc CommitChapterIfWaiting(CommitChapterIfWaitingRequest) returns (google.protobuf.Empty);
  rpc ListChapters(ListChaptersRequest) returns (ListChaptersResponse);
  rpc GetChapter(GetChapterRequest) returns (ChapterRecord);
  rpc OpenArtifact(OpenArtifactRequest) returns (stream OpenArtifactResponse);

  rpc CancelJob(CancelJobRequest) returns (google.protobuf.Empty);
}

message JobKey {
  string tenant_id = 1;
  string job_id = 2;
}

message JobHandle {
  JobKey job_key = 1;
}

message Payload {
  string type_url = 1;
  bytes value = 2;
}

message ArtifactDescriptor {
  string name = 1;
  string sha256 = 2;
  int64 size_bytes = 3;
}

message ArtifactData {
  ArtifactDescriptor descriptor = 1;
  bytes data = 2;
}

message ArtifactRef {
  JobKey job_key = 1;
  int64 ordinal = 2;
  string name = 3;
  string sha256 = 4;
}

message ArtifactChunk {
  int64 offset = 1;
  bytes data = 2;
}

message OpenArtifactResponse {
  oneof item {
    ArtifactDescriptor descriptor = 1;
    ArtifactChunk chunk = 2;
  }
}

message TaskData {
  Payload payload = 1;
  repeated ArtifactData artifacts = 2;
}

message StoredTaskData {
  TaskOutcome outcome = 1;
  repeated ArtifactDescriptor artifacts = 2;
}

message InputReference {
  int64 ordinal = 1;
  string hash = 2;
}

message RetryPolicy {
  google.protobuf.Duration initial_interval = 1;
  double backoff_coefficient = 2;
  google.protobuf.Duration maximum_interval = 3;
  int32 maximum_attempts = 4;
  repeated string non_retryable_error_types = 5;
}

message RunPolicy {
  RetryPolicy retry = 1;
  google.protobuf.Duration invocation_timeout = 2;
  google.protobuf.Duration total_timeout = 3;
}

enum JobPrereqCondition {
  JOB_PREREQ_CONDITION_UNSPECIFIED = 0;
  JOB_PREREQ_CONDITION_COMPLETE = 1;
  JOB_PREREQ_CONDITION_SUCCESS = 2;
}

message JobPrerequisite {
  string job_id = 1;
  JobPrereqCondition condition = 2;
}

message AppErrorPayload {
  string message = 1;
  string level = 2;
  map<string, google.protobuf.Value> attrs = 3;
  InputReference input_ref = 4;
  repeated string stacktrace = 5;
}

message SystemErrorPayload {
  string message = 1;
  string component = 2;
  string code = 3;
  bool retryable = 4;
  InputReference input_ref = 5;
  repeated string stacktrace = 6;
}

message TimeoutPayload {
  string message = 1;
  string scope = 2;
  string component = 3;
  string code = 4;
  bool retryable = 5;
  google.protobuf.Duration after = 6;
  InputReference input_ref = 7;
}

message JobFailedPayload {
  oneof cause {
    AppErrorPayload app_error = 1;
    SystemErrorPayload system_error = 2;
    TimeoutPayload timeout = 3;
  }
}

message TaskOutcome {
  oneof result {
    Payload success = 1;
    AppErrorPayload app_error = 2;
    SystemErrorPayload system_error = 3;
    TimeoutPayload timeout = 4;
    JobFailedPayload job_failed = 5;
  }
}

enum RetryDecision {
  RETRY_DECISION_UNSPECIFIED = 0;
  RETRY_DECISION_RETRYABLE = 1;
  RETRY_DECISION_NON_RETRYABLE = 2;
}

message RetryState {
  int32 max_attempts = 1;
  google.protobuf.Timestamp next_attempt_at = 2;
  int64 backoff_millis = 3;
  RetryDecision decision = 4;
}

message AttemptInfo {
  int32 attempt = 1;
  string worker_id = 2;
  google.protobuf.Timestamp started_at = 3;
  google.protobuf.Timestamp finished_at = 4;
  InputReference input_ref = 5;
  Payload input = 6;
  RunPolicy run_policy = 7;
  RetryState retry_state = 8;
}

message JobStartChapter {
  Payload input = 1;
  google.protobuf.Struct metadata = 2;
  RunPolicy run_policy = 3;
  repeated JobPrerequisite prerequisites = 4;
}

message JobAttemptOutcomeChapter {
  AttemptInfo attempt = 1;
  TaskOutcome outcome = 2;
}

message TaskAttemptOutcomeChapter {
  AttemptInfo attempt = 1;
  TaskOutcome outcome = 2;
}

message RestartExtraChapter {
  Payload input = 1;
  TaskOutcome outcome = 2;
  InputReference input_ref = 3;
  RunPolicy run_policy = 4;
}

message ChapterRecord {
  uint32 envelope_version = 1;
  int64 ordinal = 2;
  string task_type = 3;
  string worker_id = 4;
  google.protobuf.Timestamp created_at = 5;
  string input_hash = 6;
  repeated ArtifactDescriptor artifacts = 7;

  oneof chapter {
    JobStartChapter job_start = 10;
    JobAttemptOutcomeChapter job_attempt_outcome = 11;
    TaskAttemptOutcomeChapter task_attempt_outcome = 12;
    RestartExtraChapter restart_extra = 13;
  }
}

message TaskWait {
  int64 input_ordinal = 1;
  int64 output_ordinal = 2;
  string resume_need = 3;
  string input_hash = 4;
}

message SchedulerPayload {
  RunPolicy run_policy = 1;
  TaskWait task_wait = 2;
}

message WaitForJobs {
  repeated string job_ids = 1;
}

message SubmitJob {
  string job_type = 1;
  TaskData data = 2;
  RunPolicy run_policy = 3;
  google.protobuf.Struct metadata = 4;
  repeated JobPrerequisite prerequisites = 5;
}

message SubmitJobRequest {
  string tenant_id = 1;
  // Optional. If absent or empty, the runtime generates the job ID.
  string job_id = 2;
  SubmitJob job = 3;
  string worker_id = 4;
  google.protobuf.Timestamp request_time = 5;
}

message SubmitRestartJob {
  JobKey prior_job_key = 1;
  int64 last_step_to_keep = 2;
  TaskData extra_task_input = 3;
  TaskData extra_task_output = 4;
  repeated JobPrerequisite prerequisites = 5;
}

message SubmitRestartJobRequest {
  string tenant_id = 1;
  // Optional destination job ID. If absent or empty, the runtime generates the job ID.
  string job_id = 2;
  SubmitRestartJob job = 3;
  string worker_id = 4;
  google.protobuf.Timestamp request_time = 5;
}

message CancelJobRequest {
  JobKey job_key = 1;
  string reason = 2;
  string worker_id = 3;
}

message GetJobRequest {
  JobKey job_key = 1;
}

enum JobStatus {
  JOB_STATUS_UNSPECIFIED = 0;
  JOB_STATUS_READY = 1;
  JOB_STATUS_EXPIRED = 2;
  JOB_STATUS_PENDING_JOBS = 3;
  JOB_STATUS_AWAITING_FUTURE = 4;
  JOB_STATUS_ACTIVE = 5;
  JOB_STATUS_CRASH_CONCERN = 6;
  JOB_STATUS_CANCELLED = 7;
  JOB_STATUS_COMPLETED = 8;
}

message JobInfo {
  JobStatus status = 1;
  StoredTaskData data = 2;
}

enum JobStore {
  JOB_STORE_UNSPECIFIED = 0;
  JOB_STORE_ACTIVE = 1;
  JOB_STORE_ARCHIVED = 2;
}

message JobTaskFilter {
  string job_type = 1;
  string task_type = 2;
}

message MetadataPredicate {
  repeated string path = 1;
  repeated google.protobuf.Value values = 2;
}

message ListJobsRequest {
  repeated string tenant_ids = 1;
  repeated JobStatus statuses = 2;
  repeated JobStore stores = 3;
  repeated string job_types = 4;
  repeated JobTaskFilter job_tasks = 5;
  repeated JobKey job_keys = 6;
  repeated MetadataPredicate metadata_predicates = 7;
  google.protobuf.Timestamp created_after = 8;
  google.protobuf.Timestamp created_before = 9;
  int32 page_size = 10;
  string page_token = 11;
}

message JobSummary {
  JobKey job_key = 1;
  JobStatus status = 2;
  string job_type = 3;
  string next_need = 4;
  repeated string wait_for_job_ids = 5;
  google.protobuf.Timestamp available_at = 6;
  google.protobuf.Timestamp expires_at = 7;
  google.protobuf.Timestamp lease_expires_at = 8;
  bool cancel_requested = 9;
  google.protobuf.Timestamp created_at = 10;
  google.protobuf.Timestamp archived_at = 11;
  SchedulerPayload payload = 12;
  google.protobuf.Struct metadata = 13;
  TaskWait task_wait = 14;
}

message ListJobsResponse {
  repeated JobSummary jobs = 1;
  string next_page_token = 2;
}

message PollWorkRequest {
  string tenant_id = 1;
  string worker_id = 2;
  repeated string capabilities = 3;
  int32 limit = 4;
  google.protobuf.Timestamp long_poll_until = 5;
  google.protobuf.Duration lease_duration = 6;
  repeated MetadataPredicate metadata_equals = 7;
}

message ExecutionLease {
  string lease_id = 1;
  string lease_token = 2;
  JobHandle job = 3;
  string capability = 4;
  SchedulerPayload payload = 5;
}

message PollWorkResponse {
  repeated ExecutionLease leases = 1;
}

message GetJobLeaseRequest {
  JobKey job_key = 1;
  string worker_id = 2;
  repeated string capabilities = 3;
  google.protobuf.Duration lease_duration = 4;
}

message GetJobLeaseResponse {
  ExecutionLease lease = 1;
}

message LeaseRef {
  JobKey job_key = 1;
  string lease_id = 2;
  string lease_token = 3;
  string worker_id = 4;
}

message KeepAliveLeaseRequest {
  LeaseRef lease = 1;
  google.protobuf.Duration lease_duration = 2;
}

enum CompletionStatus {
  COMPLETION_STATUS_UNSPECIFIED = 0;
  COMPLETION_STATUS_SUCCEEDED = 1;
  COMPLETION_STATUS_FAILED = 2;
  COMPLETION_STATUS_CANCELLED = 3;
}

message CompleteExecutionRequest {
  CompletionStatus status = 1;
  string detail = 2;
}

message CompleteJobWithLeaseRequest {
  LeaseRef lease = 1;
  CompleteExecutionRequest completion = 2;
}

message RescheduleExecutionRequest {
  string next_need = 1;
  google.protobuf.Timestamp wait_until = 2;
  repeated string wait_for_job_ids = 3;
  SchedulerPayload payload = 4;
  string alternate_need = 5;
  google.protobuf.Duration alternate_after = 6;
}

message RescheduleJobWithLeaseRequest {
  LeaseRef lease = 1;
  RescheduleExecutionRequest reschedule = 2;
}

message ChapterRef {
  JobKey job_key = 1;
  int64 ordinal = 2;
  int32 attempt = 3;
  string task_type = 4;
}

message AddChapterWithLeaseRequest {
  LeaseRef lease = 1;
  ChapterRef ref = 2;
  ChapterRecord chapter = 3;
  repeated ArtifactData artifact_uploads = 4;
}

message CommitChapterIfWaitingRequest {
  JobKey job_key = 1;
  string capability = 2;
  string resume_need = 3;
  int64 input_ordinal = 4;
  int64 output_ordinal = 5;
  string input_hash = 6;
  TaskData data = 7;
}

message ListChaptersRequest {
  JobKey job_key = 1;
  int64 start_ordinal = 2;
  int64 end_ordinal = 3;
}

message ListChaptersResponse {
  repeated ChapterRecord chapters = 1;
}

message GetChapterRequest {
  ChapterRef ref = 1;
}

message OpenArtifactRequest {
  ArtifactRef ref = 1;
}
```

## Proto Validation Rules

Protobuf does not express all SWF invariants. Runtime code must validate
messages before mutation.

Required validation:

1. `JobKey.tenant_id` and `JobKey.job_id` are non-empty wherever a job key is
   supplied.
2. `SubmitJobRequest.tenant_id` and `SubmitRestartJobRequest.tenant_id` are
   non-empty.
3. `SubmitJobRequest.job_id` and `SubmitRestartJobRequest.job_id` are optional.
   Absent or empty means the runtime generates the destination job ID.
4. `Payload.type_url` is non-empty for every task/job payload.
5. `Payload.value` must be deterministic protobuf bytes for `type_url`.
6. Every `ArtifactData.descriptor.name` is non-empty.
7. If `ArtifactData.descriptor.size_bytes` is non-zero, it must equal
   `len(data)`.
8. If `ArtifactData.descriptor.sha256` is non-empty, it must match `data`.
9. `ChapterRecord.ordinal` and `ChapterRef.ordinal` must match on chapter
   writes.
10. Exactly one `ChapterRecord.chapter` oneof case must be set.
11. Exactly one `TaskOutcome.result` oneof case must be set for completed
   outcomes.
12. `CommitChapterIfWaitingRequest.output_ordinal` must match the current wait
   slot's output ordinal.
13. Metadata predicates require at least one path segment and at least one
   comparison value.

Do not encode validation-only state as extra compatibility markers in storage.

## Public Go API Direction

The current public data model is JSON-first:

```go
type Data = json.RawMessage
func NewTaskData(data any, artifacts ...Artifact) (TaskData, error)
```

That should become protobuf-first:

```go
type Payload struct {
    TypeURL string
    Bytes   []byte
}

type TaskData interface {
    Payload() (Payload, error)
    GetArtifacts() ([]Artifact, error)
}

func NewProtoTaskData(msg proto.Message, artifacts ...Artifact) (TaskData, error)
func NewProtoTaskDataBytes(typeURL string, data []byte, artifacts ...Artifact) (TaskData, error)
```

Rules:

1. `NewProtoTaskData` marshals with
   `proto.MarshalOptions{Deterministic: true}`.
2. `TypeURL` should use the normal protobuf Any style:
   `type.googleapis.com/<full.proto.MessageName>`.
3. `NewProtoTaskDataBytes` is for code that already has deterministic protobuf
   bytes. It must reject an empty `typeURL`.
4. Do not add a durable JSON convenience constructor. Tests can define small
   test protobuf messages.

Metadata should stop using `json.RawMessage` in storage-facing public types.
Accept either `*structpb.Struct`, `map[string]any` converted at the boundary,
or a small SWF-owned metadata wrapper.

## Durable Storage

### Chapters

Chapter bodies are serialized `swf.runtime.v1.ChapterRecord` messages.

Current JSON envelope concepts map as:

| Current concept | Protobuf replacement |
| --- | --- |
| `chapter_type` string | `ChapterRecord.chapter` oneof case |
| `meta` object | common `ChapterRecord` fields plus type-specific chapter message |
| `payload_kind` string | `TaskOutcome.result` oneof case |
| `payload` JSON | `Payload` or typed error payload message |

There is no chapter content-type field. Storage code already knows chapter
bodies are `ChapterRecord` protobuf bytes.

### Scheduler Rows

SQLite scheduler rows should use scalar columns for indexed polling state and
protobuf blobs for structured state:

```sql
CREATE TABLE IF NOT EXISTS swf_jobs (
    tenant_id TEXT NOT NULL,
    job_id TEXT NOT NULL,
    job_type TEXT NOT NULL,
    next_need TEXT NOT NULL,
    scheduler_payload_pb BLOB NOT NULL,
    metadata_pb BLOB NOT NULL,
    wait_for_pb BLOB NOT NULL,
    available_at_ns INTEGER NOT NULL,
    created_at_ns INTEGER NOT NULL,
    updated_at_ns INTEGER NOT NULL,
    archived_at_ns INTEGER,
    cancel_requested INTEGER NOT NULL DEFAULT 0 CHECK (cancel_requested IN (0, 1)),
    completion_status TEXT,
    completion_detail TEXT,
    lease_id TEXT,
    lease_worker_id TEXT,
    lease_expires_at_ns INTEGER,
    alternate_need TEXT,
    alternate_at_ns INTEGER,
    PRIMARY KEY (tenant_id, job_id)
) STRICT;
```

Blob meanings:

1. `scheduler_payload_pb`: serialized `SchedulerPayload`.
2. `metadata_pb`: serialized `google.protobuf.Struct`.
3. `wait_for_pb`: serialized `WaitForJobs`.

If dependency checks need indexing, add a projection table:

```sql
CREATE TABLE IF NOT EXISTS swf_job_dependencies (
    tenant_id TEXT NOT NULL,
    job_id TEXT NOT NULL,
    dependency_job_id TEXT NOT NULL,
    PRIMARY KEY (tenant_id, job_id, dependency_job_id)
) STRICT;
```

The projection table is not a second canonical encoding. It is rebuilt from
`WaitForJobs` when needed.

Do not keep canonical `payload_json`, `metadata_json`, `wait_for_json`, or
chapter-envelope JSON columns.

### Filesystem Runtime

If the local filesystem runtime exists, committed files should use `.pb`
suffixes:

```text
<root>/
  tenants/
    <tenant>/
      jobs/
        <job>/
          identity/
            submit.pb
          events/
            000001.pb
          chapters/
            <chapter-id>/
              chapter.pb
              artifacts/
                <artifact>/
                  descriptor.pb
                  bytes
```

Human-readable inspection belongs in a CLI command that decodes protobuf and
prints JSON or text to stdout. That diagnostic output is not durable state.

## Deterministic Hashing

Input hashes change deliberately with this storage format.

Use a versioned hash domain:

```text
swf-input-v2-protobuf
```

Recommended hash input:

1. The hash domain string plus a NUL byte.
2. Payload `type_url` plus a NUL byte.
3. Deterministic protobuf payload bytes plus a NUL byte.
4. Artifact fingerprints sorted by artifact name:
   `name`, NUL, `sha256`, NUL, decimal `size_bytes`, NUL.

Do not hash JSON renderings of protobuf messages. Do not use non-deterministic
protobuf marshal output for user payloads or SWF-owned metadata.

Old JSON-era hashes are not relevant because old jobs are deleted before this
version is used.

## gRPC API Semantics

The gRPC API is a structured peer to the REST runtime behavior. It should
preserve the same runtime guarantees documented in
`docs/SPEC-WorkflowRuntime-Guarantees.md`.

Unlike REST, gRPC uses a single `SubmitJob` method and a single
`SubmitRestartJob` method. Each request has an optional destination `job_id`.
When `job_id` is absent or empty, the runtime generates the job ID. When
`job_id` is set, the runtime submits at that explicit ID and applies the normal
explicit-ID idempotency/conflict rules.

Error mapping:

1. Missing jobs, chapters, or artifacts: `NOT_FOUND`.
2. Lease loss and conflicting conditional writes: `ABORTED`.
3. Invalid request shape or failed validation: `INVALID_ARGUMENT`.
4. Unsupported operation in a runtime implementation: `UNIMPLEMENTED`.
5. Unexpected storage failures: `INTERNAL` or `UNAVAILABLE`, depending on
   retryability.

`GetJobLease` returns a response with `lease` unset when no targeted lease is
available. That mirrors the REST `200` with `lease: null` behavior.

`OpenArtifact` streams a descriptor first, then byte chunks. The server may
choose any chunk size. Clients must assemble chunks by offset and verify the
descriptor digest when present.

## Runtime Implementation Requirements

All runtime implementations should use one shared codec layer.

Required changes:

1. Replace `worker_envelope.go` and duplicated runtime envelope structs with
   `runtimecodec`.
2. Store `ChapterRecord` bytes as the chapter body in every durable runtime.
3. Store `SchedulerPayload`, `WaitForJobs`, and metadata `Struct` as protobuf
   blobs in scheduler state.
4. Update toy runtime to use the same protobuf messages in memory.
5. Update remote runtime to expose the new gRPC service and translate the REST
   adapter into the same protobuf-backed internal request path.
6. Update conformance tests so task/job data uses test protobuf messages.

Decoders should call `proto.Unmarshal` into the expected protobuf message. They
should not attempt JSON fallback.

## Implementation Plan

1. Add `proto/swf/runtime/v1/runtime.proto`.
2. Add `buf` or `protoc` generation for Go protobuf and gRPC output.
3. Commit generated files under `pkg/swf/runtimepb/v1`.
4. Add `pkg/swf/internal/runtimecodec` for deterministic marshal/unmarshal,
   hash construction, and conversion to existing public SWF structs during the
   transition.
5. Replace public task data constructors with protobuf-first constructors.
6. Replace storage-facing metadata fields with protobuf metadata.
7. Replace chapter encode/decode paths with `ChapterRecord`.
8. Replace scheduler JSON payload, wait-for, and metadata storage with
   protobuf blobs.
9. Add the gRPC server implementation parallel to the REST server.
10. Update examples and conformance tests to use small test protobuf messages.
11. Run runtime conformance and usage parity against toy, SQLite, direct, REST
    remote, and gRPC remote runtimes.

## Acceptance Criteria

1. Fresh protobuf storage can run the workflow runtime conformance suite after
   tests are updated to protobuf task payloads.
2. Durable chapter bodies unmarshal as `swf.runtime.v1.ChapterRecord`.
3. Scheduler blobs unmarshal as `SchedulerPayload`, `WaitForJobs`, and
   `google.protobuf.Struct`.
4. No canonical SWF-owned durable storage columns or files use JSON.
5. Artifact descriptors contain name, size, and digest only. They do not
   contain content type.
6. Input hashes are stable across repeated runs with identical protobuf
   payloads, including payloads with map fields.
7. Metadata filters work against protobuf `Struct` metadata.
8. gRPC and REST runtime paths pass the same conformance tests.
9. Durable runtime codecs do not import `encoding/json` except for diagnostic
   printing or REST adapter conversion.

## Open Questions

1. Should the REST API be regenerated from this proto through grpc-gateway, or
   should it remain an OpenAPI-owned adapter?
2. Should task payload type URLs be restricted to a configured allow-list for
   worker deployments?
3. Should metadata be limited to scalar path values for easier indexing, or
   should it keep full `Struct` expressiveness?
4. Should direct/Postgres support be cut over at the same time as SQLite, or
   should unsupported durable runtimes be disabled until their storage is
   protobuf-only?
