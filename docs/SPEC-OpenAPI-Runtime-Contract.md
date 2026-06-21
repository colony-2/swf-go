# Specification: Complete OpenAPI Runtime Contract

## Status

**Current REST contract reference** | Author: Codex | Updated: 2026-06-21

## Decision

The SWF REST API must be a complete, first-class OpenAPI contract for SWF
runtime concepts. It should keep the current REST resource shape, but replace
the loose JSON projection with fully specified JSON schemas that mirror the
structured runtime model:

1. Closed discriminated unions for chapter bodies and task outcomes.
2. Explicit JSON payload schemas instead of arbitrary undocumented payload fields.
3. Typed SWF-owned metadata, errors, retry policy, task-wait state, scheduler
   payloads, and artifact descriptors.
4. Rigid validation rules expressed in OpenAPI descriptions and conformance
   tests.

The REST contract must not require clients or alternative runtime
implementations to read the storage protobuf schema. A server may store the
same concepts as protobuf, SQL rows, or any other representation, but the REST
wire contract is defined by `openapi/swf-runtime.yaml`.

## Goals

1. Make `openapi/swf-runtime.yaml` sufficient to implement a conforming REST
   client or server without consulting Go code or protobuf definitions.
2. Preserve the current high-level REST path layout for jobs, leases, chapters,
   and artifacts.
3. Replace `interface{}`, `{}`, and undocumented open JSON fields with
   typed schemas.
4. Preserve caller-owned application payloads as JSON subtrees across REST round
   trips.
5. Make SWF-owned data queryable and validatable through explicit REST
   schemas.
6. Make generated clients useful in strongly typed languages.
7. Keep runtime behavior aligned with
   [`SPEC-WorkflowRuntime-Guarantees.md`](SPEC-WorkflowRuntime-Guarantees.md).

## Non-Goals

1. Removing the REST API or making gRPC the only complete API.
2. Exposing `pgwf`, Strata, SQLite, or storage-specific implementation details.
3. Adding artifact MIME/content-type handling.
4. Encoding JSON application payloads inside strings.
5. Defining a general schema registry for application payloads.

## Relationship To Protobuf

The REST schemas should intentionally have the same conceptual shape as the
structured runtime model, especially for one-of choices and typed SWF-owned
state. The OpenAPI document remains normative for HTTP clients.

Rules:

1. The OpenAPI descriptions may say a field contains "application JSON payload"
   or "typed metadata"; they must not say "decode this protobuf message" as the
   only way to understand the field.
2. REST schema names may match protobuf message names when helpful, but every
   field, allowed value, validation rule, and nullability rule must be present in
   OpenAPI.
3. If the protobuf schema and OpenAPI schema diverge, the REST behavior is
   governed by OpenAPI and the HTTP conformance suite.
4. No REST response should contain data that clients are expected to reinterpret
   through undocumented Go adapter behavior.

## OpenAPI Version

Use OpenAPI 3.1. OpenAPI 3.1 gives the spec normal JSON Schema semantics for
`oneOf`, recursive references, `const`, and nullable values. The target contract
should not carry OpenAPI 3.0 compatibility workarounds unless a generator issue
forces a short-lived local patch.

## Global Wire Rules

### JSON

All request and response bodies, except artifact bytes, use
`application/json`.

Every object schema must set `additionalProperties: false` unless the object is
explicitly a map or recursively arbitrary JSON, such as `Metadata.fields` or an
object inside `ApplicationPayload`.

Generated Go or TypeScript client models for the new contract should not contain
`interface{}` or `any` for SWF-owned state. `ApplicationPayload` is the
intentional open JSON value; metadata, scheduler state, chapters, outcomes, and
errors must still generate typed models.

### Names

REST JSON field names use lower camel case:

```text
tenantId
jobId
data
leasePayload
inputHash
createdAt
```

The OpenAPI document may mention Go or protobuf names only in non-normative
migration notes.

### Application Payload JSON

Application payloads in the REST API are JSON subtrees. They are not base64
strings, JSON-encoded strings, protobuf JSON, or envelopes that require a
separate type registry.

The OpenAPI schema must use a reusable `ApplicationPayload` schema for these
fields. `ApplicationPayload` accepts any valid JSON value: object, array, string,
number, boolean, or null. Servers may store the value internally as protobuf
bytes, but those bytes must be an implementation detail. REST clients work only
with the JSON value.

Servers must reject JSON objects with duplicate member names in application
payloads. Runtime comparisons and hashes use the canonical JSON encoding defined
in this document, not the original whitespace or object-member order sent by a
client.

### Byte Fields

Only actual byte blobs use base64 in JSON. Today that means inline artifact
content.

Every JSON byte field is a string containing RFC 4648 standard base64 with
padding. Required byte fields may be the empty string when the decoded byte slice
is empty.

Use this field name:

```text
contentBase64     inline artifact bytes
```

### Integer Width

Use OpenAPI `type: integer` with `format: int64` for ordinals, artifact sizes,
and other signed 64-bit values. Use `format: int32` for bounded values such as
page size, poll limit, attempt count, and maximum attempts.

### Time

Timestamps are RFC 3339 date-time strings. Servers should return UTC timestamps
with a `Z` offset. Clients may send any valid RFC 3339 offset.

Durations use an SWF duration string. The grammar is a non-empty sequence of
decimal number plus unit segments with no whitespace:

```text
Duration = Segment { Segment }
Segment  = Decimal Unit
Decimal  = Digit { Digit } [ "." Digit { Digit } ]
Unit     = "ns" | "us" | "ms" | "s" | "m" | "h"
```

Examples:

```text
100ms
1.5s
2m30s
```

Durations must be non-negative unless the field explicitly says otherwise.

### Nullability

Omit optional fields when they are absent. Use `null` only where null has
contractual meaning.

Current meaningful nulls:

1. `GetJobLeaseResponse.lease` is `null` when the job is not leaseable.
2. `JobInfo.data` is `null` when terminal task data is not materialized.

Arrays are never null. Empty arrays are represented as `[]`.

### Closed Unions

Use a required discriminator field named `kind` for every union.

Rules:

1. `kind` is a string enum.
2. Each variant has exactly the fields defined for that variant.
3. Unknown `kind` values are validation errors.
4. A variant must not carry fields from another variant.
5. The OpenAPI schema must use `oneOf` plus `discriminator`.

Do not model a union as parallel optional fields, string discriminator plus
untyped `data`, or `{}`.

## Core Schemas

The schemas below define the target OpenAPI model. They are written as concise
YAML fragments to make the intended rewrite concrete.

### Reusable Scalars

```yaml
Duration:
  type: string
  pattern: '^(0|[0-9]+(\.[0-9]+)?(ns|us|ms|s|m|h))([0-9]+(\.[0-9]+)?(ns|us|ms|s|m|h))*$'
  description: Non-negative SWF duration string.

Sha256Hex:
  type: string
  pattern: '^[a-f0-9]{64}$'

Base64Bytes:
  type: string
  format: byte

JsonValue:
  oneOf:
    - type: 'null'
    - type: boolean
    - type: number
    - type: string
    - type: array
      items:
        $ref: '#/components/schemas/JsonValue'
    - type: object
      additionalProperties:
        $ref: '#/components/schemas/JsonValue'

ApplicationPayload:
  $ref: '#/components/schemas/JsonValue'
```

### Application Payload

`ApplicationPayload` is the REST representation of caller-owned application
payload data. It is a JSON value embedded directly in the request or response.

```yaml
ApplicationPayload:
  $ref: '#/components/schemas/JsonValue'
```

The API must not ask clients to base64-encode JSON, put JSON inside strings, or
attach payload type URLs. If producers and consumers need an application-level
type discriminator, they should include it inside their own JSON payload object.

Servers that persist application payloads as bytes must use the canonical JSON
encoding of the `ApplicationPayload` value. They must not store a protobuf JSON
rendering or any representation that changes the REST-visible JSON value.

### Artifacts

Artifacts remain opaque files attached to chapter or task-data writes.

```yaml
ArtifactDescriptor:
  type: object
  additionalProperties: false
  required: [name, digest, size]
  properties:
    name:
      type: string
      minLength: 1
    digest:
      type: string
      pattern: '^[a-f0-9]{64}$'
    size:
      type: integer
      format: int64

ArtifactData:
  type: object
  additionalProperties: false
  required: [name, size, contentBase64]
  properties:
    name:
      type: string
      minLength: 1
    size:
      type: integer
      format: int64
    digest:
      type: string
      pattern: '^[a-f0-9]{64}$'
      description: Optional client-supplied checksum. If supplied, the server must validate it.
    contentBase64:
      type: string
      format: byte
```

On writes, the server must reject an artifact when:

1. `size` does not equal the decoded byte length.
2. `digest` is present and does not equal the decoded byte digest.
3. Two artifacts in the same write have the same `name`.

On reads, `ArtifactDescriptor.digest` is required and is always the digest of the
stored bytes.

### Task Data

```yaml
TaskDataWrite:
  type: object
  additionalProperties: false
  required: [data, artifacts]
  properties:
    data:
      $ref: '#/components/schemas/ApplicationPayload'
    artifacts:
      type: array
      items:
        $ref: '#/components/schemas/ArtifactData'

StoredTaskData:
  type: object
  additionalProperties: false
  required: [outcome, artifacts]
  properties:
    outcome:
      $ref: '#/components/schemas/TaskOutcome'
    artifacts:
      type: array
      items:
        $ref: '#/components/schemas/ArtifactDescriptor'
```

Successful application inputs and outputs are JSON payload values. Failed task
data is represented through `TaskOutcome`, not through a successful payload plus
a separate `payloadKind` string.

### Metadata

Metadata is a typed value tree. It is not arbitrary JSON.

```yaml
Metadata:
  type: object
  additionalProperties: false
  required: [fields]
  properties:
    fields:
      type: object
      additionalProperties:
        $ref: '#/components/schemas/MetadataValue'

MetadataValue:
  oneOf:
    - $ref: '#/components/schemas/MetadataNull'
    - $ref: '#/components/schemas/MetadataBool'
    - $ref: '#/components/schemas/MetadataInt'
    - $ref: '#/components/schemas/MetadataDouble'
    - $ref: '#/components/schemas/MetadataString'
    - $ref: '#/components/schemas/MetadataList'
    - $ref: '#/components/schemas/MetadataMap'
  discriminator:
    propertyName: kind

MetadataNull:
  type: object
  additionalProperties: false
  required: [kind]
  properties:
    kind:
      const: "null"

MetadataBool:
  type: object
  additionalProperties: false
  required: [kind, boolValue]
  properties:
    kind:
      const: "bool"
    boolValue:
      type: boolean

MetadataInt:
  type: object
  additionalProperties: false
  required: [kind, intValue]
  properties:
    kind:
      const: "int"
    intValue:
      type: integer
      format: int64

MetadataDouble:
  type: object
  additionalProperties: false
  required: [kind, doubleValue]
  properties:
    kind:
      const: "double"
    doubleValue:
      type: number

MetadataString:
  type: object
  additionalProperties: false
  required: [kind, stringValue]
  properties:
    kind:
      const: "string"
    stringValue:
      type: string

MetadataList:
  type: object
  additionalProperties: false
  required: [kind, listValue]
  properties:
    kind:
      const: "list"
    listValue:
      type: array
      items:
        $ref: '#/components/schemas/MetadataValue'

MetadataMap:
  type: object
  additionalProperties: false
  required: [kind, mapValue]
  properties:
    kind:
      const: "map"
    mapValue:
      $ref: '#/components/schemas/Metadata'
```

Typed equality matters. `{"kind":"int","intValue":"1"}` and
`{"kind":"double","doubleValue":1}` are distinct values.

### Metadata Predicates

```yaml
MetadataPredicate:
  type: object
  additionalProperties: false
  required: [path, values]
  properties:
    path:
      type: array
      minItems: 1
      items:
        type: string
        minLength: 1
    values:
      type: array
      minItems: 1
      items:
        $ref: '#/components/schemas/MetadataValue'
```

Predicate semantics:

1. `path` walks through `MetadataMap` fields from the root metadata object.
2. Values on one predicate are ORed.
3. Multiple predicates are ANDed.
4. Equality is structural over `MetadataValue`.
5. A missing field does not equal any value, including `null`.

### Run Policy

```yaml
RetryPolicy:
  type: object
  additionalProperties: false
  properties:
    initialInterval:
      $ref: '#/components/schemas/Duration'
    backoffCoefficient:
      type: number
      minimum: 0
    maximumInterval:
      $ref: '#/components/schemas/Duration'
    maximumAttempts:
      type: integer
      format: int32
      minimum: 0
    nonRetryableErrorTypes:
      type: array
      items:
        type: string

RunPolicy:
  type: object
  additionalProperties: false
  properties:
    retry:
      $ref: '#/components/schemas/RetryPolicy'
    invocationTimeout:
      $ref: '#/components/schemas/Duration'
    totalTimeout:
      $ref: '#/components/schemas/Duration'
```

Omitted policy fields mean "use the runtime default for this field". Responses
that describe persisted run state should return the effective values whenever
the runtime has materialized them.

### Error Payloads

```yaml
InputReference:
  type: object
  additionalProperties: false
  required: [ordinal]
  properties:
    ordinal:
      type: integer
      format: int64
    hash:
      type: string

AppErrorPayload:
  type: object
  additionalProperties: false
  required: [message, level, attrs, stacktrace]
  properties:
    message:
      type: string
    level:
      type: string
    attrs:
      $ref: '#/components/schemas/Metadata'
    inputRef:
      $ref: '#/components/schemas/InputReference'
    stacktrace:
      type: array
      items:
        type: string

SystemErrorPayload:
  type: object
  additionalProperties: false
  required: [message, component, code, retryable, stacktrace]
  properties:
    message:
      type: string
    component:
      type: string
    code:
      type: string
    retryable:
      type: boolean
    inputRef:
      $ref: '#/components/schemas/InputReference'
    stacktrace:
      type: array
      items:
        type: string

TimeoutPayload:
  type: object
  additionalProperties: false
  required: [kind, after, scope, retryable, component, code, message]
  properties:
    kind:
      type: string
      enum: [job, task]
    after:
      $ref: '#/components/schemas/Duration'
    scope:
      type: string
      enum: [invocation, total]
    inputRef:
      $ref: '#/components/schemas/InputReference'
    retryable:
      type: boolean
    component:
      type: string
    code:
      type: string
    message:
      type: string
```

If `JobFailedError` remains a public SWF concept, it must be represented as a
first-class REST outcome instead of a private marker inside `AppErrorPayload`
attrs.

```yaml
JobFailedPayload:
  type: object
  additionalProperties: false
  required: [cause]
  properties:
    cause:
      $ref: '#/components/schemas/TaskFailure'
```

### Task Outcomes

```yaml
TaskOutcome:
  oneOf:
    - $ref: '#/components/schemas/TaskOutcomeSuccess'
    - $ref: '#/components/schemas/TaskOutcomeAppError'
    - $ref: '#/components/schemas/TaskOutcomeSystemError'
    - $ref: '#/components/schemas/TaskOutcomeTimeout'
    - $ref: '#/components/schemas/TaskOutcomeJobFailed'
  discriminator:
    propertyName: kind

TaskOutcomeSuccess:
  type: object
  additionalProperties: false
  required: [kind, output]
  properties:
    kind:
      const: success
    output:
      $ref: '#/components/schemas/ApplicationPayload'

TaskOutcomeAppError:
  type: object
  additionalProperties: false
  required: [kind, error]
  properties:
    kind:
      const: appError
    error:
      $ref: '#/components/schemas/AppErrorPayload'

TaskOutcomeSystemError:
  type: object
  additionalProperties: false
  required: [kind, error]
  properties:
    kind:
      const: systemError
    error:
      $ref: '#/components/schemas/SystemErrorPayload'

TaskOutcomeTimeout:
  type: object
  additionalProperties: false
  required: [kind, timeout]
  properties:
    kind:
      const: timeout
    timeout:
      $ref: '#/components/schemas/TimeoutPayload'

TaskOutcomeJobFailed:
  type: object
  additionalProperties: false
  required: [kind, error]
  properties:
    kind:
      const: jobFailed
    error:
      $ref: '#/components/schemas/JobFailedPayload'

TaskFailure:
  oneOf:
    - $ref: '#/components/schemas/TaskOutcomeAppError'
    - $ref: '#/components/schemas/TaskOutcomeSystemError'
    - $ref: '#/components/schemas/TaskOutcomeTimeout'
```

If the storage/runtime model does not support `jobFailed` yet, the OpenAPI
rewrite should either add support or remove `JobFailedError` from the REST-visible
surface. The contract must not leave it as an undocumented app-error encoding.

## Chapters

Chapters are exposed as structured records. The REST contract must not expose
the current `chapterType` + `payloadKind` + untyped `data` triple.

```yaml
ChapterRecord:
  type: object
  additionalProperties: false
  required: [ordinal, createdAt, body, artifacts]
  properties:
    ordinal:
      type: integer
      format: int64
    taskType:
      type: string
    workerId:
      type: string
    createdAt:
      type: string
      format: date-time
    startedAt:
      type: string
      format: date-time
    finishedAt:
      type: string
      format: date-time
    inputHash:
      type: string
    metadata:
      $ref: '#/components/schemas/Metadata'
    input:
      $ref: '#/components/schemas/ApplicationPayload'
      description: Cached task input payload when task-input storage is enabled.
    attempt:
      type: integer
      format: int32
      minimum: 0
    maxAttempts:
      type: integer
      format: int32
      minimum: 0
    nextAttemptAt:
      type: string
      format: date-time
    backoffMillis:
      type: integer
      format: int64
    retryable:
      type: boolean
    inputRef:
      $ref: '#/components/schemas/InputReference'
    runPolicy:
      $ref: '#/components/schemas/RunPolicy'
    prerequisites:
      type: array
      items:
        $ref: '#/components/schemas/JobPrerequisite'
    body:
      $ref: '#/components/schemas/ChapterBody'
    artifacts:
      type: array
      items:
        $ref: '#/components/schemas/ArtifactDescriptor'
```

`ChapterRecord.body` is a closed union:

```yaml
ChapterBody:
  oneOf:
    - $ref: '#/components/schemas/JobStartChapter'
    - $ref: '#/components/schemas/JobAttemptOutcomeChapter'
    - $ref: '#/components/schemas/TaskAttemptOutcomeChapter'
    - $ref: '#/components/schemas/RestartExtraChapter'
  discriminator:
    propertyName: kind

JobStartChapter:
  type: object
  additionalProperties: false
  required: [kind, input]
  properties:
    kind:
      const: jobStart
    input:
      $ref: '#/components/schemas/ApplicationPayload'

JobAttemptOutcomeChapter:
  type: object
  additionalProperties: false
  required: [kind, outcome]
  properties:
    kind:
      const: jobAttemptOutcome
    outcome:
      $ref: '#/components/schemas/TaskOutcome'

TaskAttemptOutcomeChapter:
  type: object
  additionalProperties: false
  required: [kind, outcome]
  properties:
    kind:
      const: taskAttemptOutcome
    outcome:
      $ref: '#/components/schemas/TaskOutcome'

RestartExtraChapter:
  type: object
  additionalProperties: false
  required: [kind, output]
  properties:
    kind:
      const: restartExtra
    output:
      $ref: '#/components/schemas/ApplicationPayload'
```

Chapter write requests carry the structured chapter plus inline artifact bytes:

```yaml
ChapterWrite:
  type: object
  additionalProperties: false
  required: [chapter, artifactUploads]
  properties:
    chapter:
      $ref: '#/components/schemas/ChapterRecord'
    artifactUploads:
      type: array
      items:
        $ref: '#/components/schemas/ArtifactData'
```

For a write, `chapter.artifacts` must exactly describe `artifactUploads` after
the server validates or computes checksums. A simpler alternative is to omit
`chapter.artifacts` on writes and let `artifactUploads` be the source of truth,
but the OpenAPI schema must choose one rule and state it explicitly.

## Scheduler And Leases

The lease payload is typed scheduler state plus an explicitly named
caller-owned JSON payload slot.

```yaml
TaskWait:
  type: object
  additionalProperties: false
  required: [inputOrdinal, outputOrdinal, resumeNeed, inputHash]
  properties:
    inputOrdinal:
      type: integer
      format: int64
    outputOrdinal:
      type: integer
      format: int64
    resumeNeed:
      type: string
    inputHash:
      type: string

SchedulerPayload:
  type: object
  additionalProperties: false
  properties:
    runPolicy:
      $ref: '#/components/schemas/RunPolicy'
    taskWait:
      $ref: '#/components/schemas/TaskWait'
    leasePayload:
      $ref: '#/components/schemas/ApplicationPayload'
      description: Caller-owned JSON payload preserved across reschedule and lease acquisition.
```

`leasePayload` exists only for caller-owned JSON payload values. Scheduler state
such as `runPolicy` or `taskWait` must not be duplicated into `leasePayload`.

```yaml
ExecutionLease:
  type: object
  additionalProperties: false
  required: [leaseId, leaseToken, job, capability, payload]
  properties:
    leaseId:
      type: string
      minLength: 1
    leaseToken:
      type: string
      minLength: 1
    job:
      $ref: '#/components/schemas/JobHandle'
    capability:
      type: string
      minLength: 1
    payload:
      $ref: '#/components/schemas/SchedulerPayload'
```

`RescheduleExecutionRequest.payload` should become `SchedulerPayload`. If the
current field name is kept, its schema must be typed as `SchedulerPayload`. If
the field is renamed to `schedulerPayload`, keep a compatibility alias only for a
versioned transition.

### Lease Tokens

Remote lease mutation is authorized by a runtime-minted lease token rather than
by a caller-supplied worker ID. `pollWork` and `getJobLease` return
`ExecutionLease.leaseToken`. Every lease-mutating REST operation must require
that token in the `X-SWF-Lease-Token` header.

The token binds tenant ID, job ID, lease ID, worker ID, schema hash, lease
duration, and expiry. The runtime server validates those claims before calling
the local runtime lease operation. `keepAliveLease` returns a fresh
`KeepAliveLeaseResponse.leaseToken` for the renewed lease; its expiry is tied to
the renewed scheduler lease expiry, with a small skew so the transport token
does not outlive the underlying lease.

When the server handles `addChapterWithLease`, validated token claims are passed
into the local runtime so the chapter write can be authorized by the already
validated lease identity. A stale, missing, expired, or mismatched token maps to
lease-lost conflict semantics.

## Jobs

### Submit Job

`SubmitJob.data` uses `TaskDataWrite`.

```yaml
SubmitJob:
  type: object
  additionalProperties: false
  required: [jobType, data]
  properties:
    jobType:
      type: string
      minLength: 1
    data:
      $ref: '#/components/schemas/TaskDataWrite'
    runPolicy:
      $ref: '#/components/schemas/RunPolicy'
    metadata:
      $ref: '#/components/schemas/Metadata'
    prerequisites:
      type: array
      items:
        $ref: '#/components/schemas/JobPrerequisite'
```

The server-generated-ID and explicit-ID routes can remain:

```text
POST /v1/tenants/{tenantId}/jobs
PUT  /v1/tenants/{tenantId}/jobs/{jobId}
```

Explicit submit idempotency compares the structured request, including
canonical application payload JSON values and artifact descriptors. It must not
compare storage-specific bytes or adapter-specific projections.

### Restart Job

```yaml
SubmitRestartJob:
  type: object
  additionalProperties: false
  required: [priorJobKey, lastStepToKeep]
  properties:
    priorJobKey:
      $ref: '#/components/schemas/JobKey'
    lastStepToKeep:
      type: integer
      format: int64
    extraTaskInput:
      $ref: '#/components/schemas/TaskDataWrite'
    extraTaskOutput:
      $ref: '#/components/schemas/TaskDataWrite'
    prerequisites:
      type: array
      items:
        $ref: '#/components/schemas/JobPrerequisite'
```

The restart contract must state exactly how `extraTaskInput` contributes to the
input hash for `extraTaskOutput`. If it is absent, the schema description must
state the fallback input used for hashing.

### Job Info

```yaml
JobInfo:
  type: object
  additionalProperties: false
  required: [status, data]
  properties:
    status:
      $ref: '#/components/schemas/JobStatus'
    data:
      oneOf:
        - $ref: '#/components/schemas/StoredTaskData'
        - type: 'null'
```

When `status` is non-terminal, `data` is normally `null`. If an implementation
materializes terminal data for a failed or cancelled job, `data.outcome` carries
the typed failure outcome.

### List Jobs

`JobSummary.payload` must be `SchedulerPayload`, and `JobSummary.metadata` must
be `Metadata`.

Task-wait projection fields may remain for convenience only if their derivation
from `payload.taskWait` is documented. A conforming server must keep projection
fields and `payload.taskWait` consistent.

## Schedules

Schedules are first-class runtime resources. `ScheduleTarget` has the same
application start shape as `SubmitJob`: target job type, `TaskDataWrite`, run
policy, and app metadata.

```yaml
ScheduleTarget:
  type: object
  additionalProperties: false
  required: [jobType, data]
  properties:
    jobType:
      type: string
      minLength: 1
    data:
      $ref: '#/components/schemas/TaskDataWrite'
    runPolicy:
      $ref: '#/components/schemas/RunPolicy'
    metadata:
      $ref: '#/components/schemas/Metadata'
```

`upsertSchedule` stores a durable target start spec. The server snapshots target
artifact bytes/descriptors at schedule mutation time, and each occurrence is
materialized as a normal job with an ordinary start chapter. Later occurrences
must not depend on client-local files, handles, or request bodies from the
original schedule mutation.

REST schedule APIs expose app metadata and target data. Runtime-owned
`internal.schedule` metadata is stored only in the scheduler job metadata
envelope and is not accepted from clients or returned through public app-facing
job metadata fields.

## Endpoint Contract Updates

The current path layout can remain, but each operation must use the structured
schemas:

| Operation | Required schema change |
| --- | --- |
| `submitJob`, `putJob` | `SubmitJob.data` is `TaskDataWrite`; metadata is `Metadata`; run policy is `RunPolicy`. |
| `getJob` | `JobInfo.data` is nullable `StoredTaskData`; outcome is typed. |
| `submitRestartJob`, `putRestartJob` | restart payload fields are `TaskDataWrite`; `lastStepToKeep` is `integer/int64`. |
| `listJobs` | filters use typed `MetadataPredicate`; summaries expose `SchedulerPayload` and `Metadata`. |
| `pollWork`, `getJobLease` | `ExecutionLease.payload` is `SchedulerPayload`; responses include a runtime-minted `leaseToken`. |
| `keepAliveLease` | requires `X-SWF-Lease-Token`; response returns a fresh `leaseToken` for the renewed lease. |
| `rescheduleJobWithLease` | requires `X-SWF-Lease-Token`; request payload is typed `SchedulerPayload`; caller-owned lease payload is `ApplicationPayload`. |
| `completeJobWithLease` | requires `X-SWF-Lease-Token`; terminal detail remains the operation detail payload. |
| `addChapterWithLease` | requires `X-SWF-Lease-Token`; request body uses `ChapterWrite`; chapter body is a discriminated union. |
| `commitChapterIfWaiting` | completion data is `TaskDataWrite`; ordinal guards use `integer/int64`. |
| `listChapters`, `getChapter` | responses are `ChapterRecord`; no `chapterType`/`payloadKind`/`data` triple. |
| `openArtifact` | remains `application/octet-stream`; artifact metadata comes from descriptors. |
| `upsertSchedule`, `getSchedule`, `listSchedules`, `pauseSchedule`, `resumeSchedule`, `archiveSchedule`, `triggerSchedule`, `listScheduleRuns` | schedule targets use `ScheduleTarget` with `TaskDataWrite`; public metadata is app metadata; runtime-owned schedule metadata is hidden from app APIs. |

## Deterministic Input Hash

The OpenAPI contract must define input hash computation because clients may need
to supply `inputHash` guards to `commit-if-waiting`.

Use this domain-separated algorithm for the target structured REST contract:

```text
hash = SHA-256(
  "swf-input-v2-openapi" NUL
  canonical_json(task_data.data) NUL
  artifact_1.name NUL artifact_1.digest NUL artifact_1.size NUL
  artifact_2.name NUL artifact_2.digest NUL artifact_2.size NUL
  ...
)
```

Artifact descriptors are sorted by `name` ascending. If duplicate names are
present, the request is invalid before hashing.

The hash is rendered as lowercase hex.

`canonical_json(value)` is the UTF-8 JSON Canonicalization Scheme encoding of
the `ApplicationPayload` value. In practical terms, object member names are
sorted, insignificant whitespace is removed, string escapes and numbers are
normalized, and duplicate object member names are invalid before hashing.

Servers must not compute input hashes from raw request bytes, JSON object member
order, protobuf JSON rendering, or any other non-canonical representation.

## Error Responses

The OpenAPI document should replace the single-code error model with a complete
REST error envelope:

```yaml
ErrorResponse:
  type: object
  additionalProperties: false
  required: [code, message]
  properties:
    code:
      type: string
      enum:
        - invalid_request
        - unauthorized
        - not_found
        - conflict
        - execution_lease_lost
        - existing_job_mismatch
        - chapter_not_found
        - job_not_found
        - job_not_complete
        - workflow_not_deterministic
        - internal
    message:
      type: string
    details:
      type: array
      items:
        $ref: '#/components/schemas/ErrorDetail'
```

Validation failures should return `400 invalid_request` with field-level
details. Conflicting state transitions should return `409 conflict` or the more
specific conflict code.

## Compatibility

This is a breaking REST wire change. If any released clients depend on the
current JSON-shaped contract, expose the structured contract under one of:

1. `/v2/...` paths.
2. A versioned media type such as
   `application/vnd.swf.runtime.v2+json`.

Do not keep the old shape by adding optional untyped fields beside the new typed
fields. That would preserve the same ambiguity this spec removes.

## Implementation Plan

1. Add reusable scalar schemas: `Duration`, `Sha256Hex`, `Base64Bytes`,
   `JsonValue`, and `ApplicationPayload`.
2. Add artifact, metadata, policy, error, outcome, scheduler, and chapter
   schemas from this spec.
3. Replace all free-form payload fields in `openapi/swf-runtime.yaml`.
4. Replace `PayloadKind` and `ChapterType` response fields with discriminated
   unions.
5. Regenerate REST clients and servers.
6. Update remote adapters to preserve application payload JSON values through
   canonical JSON storage/rehydration without type URLs or JSON-in-string
   wrappers.
7. Add HTTP conformance tests that build requests only from OpenAPI concepts,
   not Go internal adapters.
8. Add schema validation tests that reject unknown union kinds, extra fields,
   invalid application payload JSON, duplicate application payload object keys,
   invalid base64 artifact content, invalid metadata values, invalid checksums,
   and mismatched chapter ordinals.

## Acceptance Criteria

1. A new runtime implementation can implement the REST API by reading only
   `openapi/swf-runtime.yaml` and
   [`SPEC-WorkflowRuntime-Guarantees.md`](SPEC-WorkflowRuntime-Guarantees.md).
2. Generated API models contain no untyped fields for task data, metadata,
   scheduler payloads, chapters, or outcomes.
3. REST round trips preserve `ApplicationPayload` JSON values exactly under
   canonical JSON equality.
4. Chapter reads and writes use `ChapterRecord.body.kind`, not
   `chapterType`/`payloadKind`/`data`.
5. Task outcomes use `TaskOutcome.kind`, not `payloadKind`.
6. Metadata predicates use `MetadataValue`, not arbitrary JSON values.
7. Lease payloads are `SchedulerPayload` with `leasePayload` represented as an
   `ApplicationPayload` JSON subtree.
8. Input hash behavior is testable from the OpenAPI schema and this document.
9. All validation rules have negative tests in the REST conformance suite.
