# Specification: JobDB Schema Subsystem

## Status

**Design proposal** | Author: Codex | Updated: 2026-06-23

This document describes the intended JobDB schema subsystem after the package
rename from SWF to JobDB. It also records what exists in the current tree.

## Decision

JobDB should support optional, tenant-local JSON Schemas for validating job
chapter shape. A job may omit a schema entirely; in that case JobDB behaves as
it does today and performs no schema validation.

Schemas are not part of JobDB's core execution semantics. The core runtime still
orders chapters, enforces leases, stores artifacts, schedules work, and tracks
job state. A schema is an opaque validation contract associated with a job.

## Current Implementation Status

Implemented today:

- Package layout is now `github.com/colony-2/jobdb`.
- Schema hash extraction exists in
  `pkg/jobdb/internal/jobmetadata/schema_hash.go`.
- The reserved metadata key is currently `jobdb_schema_hash`.
- Remote lease tokens carry `schema_hash` in
  `pkg/jobdb/runtime/remote/lease_token.go`.
- `AddChapterWithLease` validates the signed lease token once and passes trusted
  claims through `pkg/jobdb/internal/leaseauth`.
- Direct, SQLite, and toy runtimes can stamp a schema hash on execution leases
  and skip the extra lease-row lookup for token-authorized chapter appends.
- Keepalive replacement tokens are minted from renewed lease expiry when the
  backend exposes `KeepAliveLeaseByIDWithExpiry`.

Not implemented yet:

- No schema registry tables or persistence exist.
- No public Go schema types or schema registry API exist.
- No OpenAPI schema registration/get/list/archive endpoints exist.
- `SubmitJob` and `SubmitRestartJob` cannot formally carry `schemaHash` or an
  inline schema.
- No JSON Schema validation runs on submit, chapter append, or
  commit-if-waiting.
- No schema archive enforcement exists.
- No schema document cache exists.
- `JobInfo`, `JobSummary`, and `ExecutionLease` do not expose schema info as a
  public field.

The existing hash plumbing is therefore only preparatory. It does not provide
the complete schema subsystem.

## Goals

1. Allow different tenants to use different schemas.
2. Let jobs opt into validation by schema hash or inline schema.
3. Keep jobs without schemas fully supported.
4. Validate chapter writes without adding a pgwf/job-row read to remote
   `add_chapter`.
5. Make schema lifecycle explicit: register, get, list, archive.
6. Keep archived schemas usable by already-created mutable jobs.
7. Avoid schema defaults and other mutation behavior.

## Non-Goals

1. No extension-operation schemas in this phase.
2. No schema delete operation.
3. No schema defaults. JobDB never materializes or writes defaulted values.
4. No coupling between workflow execution logic and schema internals.
5. No generated language-specific validators in the first implementation.

## Schema Scope

A JobDB schema defines what visible `ChapterRecord` JSON may look like for a
job. It may constrain:

- chapter ordinal;
- chapter body variant;
- task type;
- metadata shape;
- input/output JSON payload shape;
- artifacts descriptors;
- attempt/retry fields.

It does not define:

- lease ownership;
- polling behavior;
- scheduling behavior;
- artifact byte storage;
- whether a job is ready, waiting, completed, or cancelled.

JSON Schema object fields remain open by default. Extra fields are allowed
unless a schema author explicitly sets `additionalProperties: false` or
`unevaluatedProperties: false`.

## Chapter Zero Versus Later Chapters

Schemas can express different shapes for chapter `0` and non-zero chapters using
ordinary JSON Schema conditionals or `oneOf`.

Example:

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "oneOf": [
    {
      "properties": {
        "ordinal": { "const": 0 },
        "body": {
          "properties": { "type": { "const": "jobStart" } },
          "required": ["type"]
        }
      },
      "required": ["ordinal", "body"]
    },
    {
      "properties": {
        "ordinal": { "minimum": 1 },
        "body": {
          "properties": {
            "type": {
              "enum": ["taskAttemptOutcome", "jobAttemptOutcome", "restartExtra"]
            }
          },
          "required": ["type"]
        }
      },
      "required": ["ordinal", "body"]
    }
  ]
}
```

JobDB does not need a special schema language for this. The schema applies to
the chapter document, and JSON Schema decides which branch matches.

## Schema Identity

Schema identity is a SHA-256 content hash over canonical JSON for the schema
document.

```text
schemaHash = "sha256:" + lowercase_hex(sha256(canonical_json(schema)))
```

The registry key is tenant-local:

```text
(tenantId, schemaHash)
```

The same schema content may produce the same hash in multiple tenants, but
active/archive state is scoped per tenant.

## Registry Lifecycle

Required operations:

- Register schema.
- Get schema by hash.
- List schemas for a tenant.
- Archive schema.

No delete operation exists.

Register is idempotent. Registering the same schema for the same tenant returns
the existing row. Registering a schema whose hash exists with different content
is a conflict, even though SHA-256 collisions are not expected.

Archive is one-way for this phase. An archived schema remains readable and
usable by existing jobs that already reference it. It is rejected for newly
inserted jobs.

## Job Association

A job may specify either:

- `schemaHash`: reference an existing active tenant schema;
- `schema`: inline JSON Schema document; or
- neither.

If `schema` is supplied, JobDB computes its hash and performs a tenant-local
put-if-absent registration. If both `schema` and `schemaHash` are supplied, the
computed hash must equal `schemaHash`.

When a job uses a schema, JobDB stores the resolved hash in immutable stored job
metadata. The canonical internal metadata field should be:

```json
{
  "internal": {
    "schemaHash": "sha256:..."
  }
}
```

The current interim helper also recognizes `jobdb_schema_hash`, `schema_hash`,
and `schemaHash` at the root, app, or internal metadata level. That compatibility
behavior should not become the public API for schema selection.

## Write Enforcement

Schema validation happens only when a job has a schema hash.

### Submit Job

On `SubmitJob`:

1. Resolve the schema reference or inline schema.
2. Reject archived or unknown schema references.
3. Store the resolved schema hash in immutable job metadata.
4. Validate chapter `0` before committing the job.

If no schema is supplied, skip all schema resolution and validation.

### Submit Restart Job

`SubmitRestartJob` should accept the same schema selector. If omitted, the new
job has no schema. If supplied, retained restart chapters and the new start
state must be valid under the new job's schema.

### Add Chapter With Lease

Remote `add_chapter` should not read the pgwf/job table solely to discover the
schema hash.

The intended path is:

1. Poll or targeted lease acquisition reads the job metadata it already needs.
2. The lease response and signed lease token include the resolved schema hash.
3. `add_chapter` validates the token and extracts `(tenantId, jobId, leaseId,
   schemaHash)`.
4. If `schemaHash` is empty, skip schema validation.
5. If present, load the schema document from a tenant/hash cache backed by the
   schema registry.
6. Validate the incoming chapter JSON before storing it.

An archived schema is accepted here because the job already chose that schema
when it was created.

### Commit If Waiting

`commit-if-waiting` is lease-less, so it cannot rely on a lease token. It already
has to inspect job wait state to enforce guards. During that read/lock, it must
also obtain the job's schema hash from metadata and validate the output chapter
before committing it.

### Complete And Reschedule

Complete and reschedule do not validate against the chapter schema unless they
write a visible chapter as part of the operation. If an implementation adds a
visible chapter for either operation, that chapter must be validated using the
job's schema.

## Cache Model

The daemon remains stateless with respect to job execution. A schema document
cache is allowed because schemas are immutable by hash.

Recommended cache key:

```text
(tenantId, schemaHash)
```

Cache values:

- canonical schema document;
- compiled JSON Schema validator;
- registry state at load time.

Archive state is only needed when selecting a schema for a new job. Existing job
writes may validate against an archived schema by hash.

## Public Go API Additions

Recommended public types in `pkg/jobdb`:

```go
type JobSchemaSelector struct {
    Hash   string
    Schema json.RawMessage
}

type JobSchemaKey struct {
    TenantId   string
    SchemaHash string
}

type JobSchemaInfo struct {
    TenantId    string
    SchemaHash  string
    Schema      json.RawMessage
    State       JobSchemaState
    CreatedAt   time.Time
    ArchivedAt  *time.Time
}

type JobSchemaState string

const (
    JobSchemaStateActive   JobSchemaState = "ACTIVE"
    JobSchemaStateArchived JobSchemaState = "ARCHIVED"
)
```

Add schema selection to job creation structs:

```go
type SubmitJob struct {
    // existing fields
    Schema *JobSchemaSelector
}

type SubmitRestartJob struct {
    // existing fields
    Schema *JobSchemaSelector
}
```

Add a registry interface implemented by concrete runtimes and the remote
runtime:

```go
type JobSchemaRegistry interface {
    RegisterJobSchema(ctx context.Context, req RegisterJobSchemaRequest) (JobSchemaInfo, error)
    GetJobSchema(ctx context.Context, key JobSchemaKey) (JobSchemaInfo, error)
    ListJobSchemas(ctx context.Context, req ListJobSchemasRequest) (ListJobSchemasResponse, error)
    ArchiveJobSchema(ctx context.Context, key JobSchemaKey) (JobSchemaInfo, error)
}
```

Whether this interface is embedded into `WorkflowRuntime` should be decided at
implementation time. Keeping it separate avoids forcing schema-listing methods
onto workflow-only fakes, while still allowing complete JobDB runtimes to expose
the registry.

## OpenAPI Additions

Add reusable schemas:

```yaml
JobSchemaHash:
  type: string
  pattern: '^sha256:[0-9a-f]{64}$'

JobSchemaDocument:
  type: object
  description: JSON Schema draft 2020-12 document.
  additionalProperties: true

JobSchemaSelector:
  type: object
  additionalProperties: false
  properties:
    schemaHash:
      $ref: '#/components/schemas/JobSchemaHash'
    schema:
      $ref: '#/components/schemas/JobSchemaDocument'

JobSchemaState:
  type: string
  enum: [ACTIVE, ARCHIVED]

JobSchemaInfo:
  type: object
  additionalProperties: false
  required: [tenantId, schemaHash, schema, state, createdAt]
  properties:
    tenantId:
      $ref: '#/components/schemas/TenantId'
    schemaHash:
      $ref: '#/components/schemas/JobSchemaHash'
    schema:
      $ref: '#/components/schemas/JobSchemaDocument'
    state:
      $ref: '#/components/schemas/JobSchemaState'
    createdAt:
      type: string
      format: date-time
    archivedAt:
      type: string
      format: date-time
```

Extend job creation schemas:

```yaml
SubmitJob:
  properties:
    schema:
      $ref: '#/components/schemas/JobSchemaSelector'

SubmitRestartJob:
  properties:
    schema:
      $ref: '#/components/schemas/JobSchemaSelector'
```

Add read fields:

```yaml
JobInfo:
  properties:
    schemaHash:
      $ref: '#/components/schemas/JobSchemaHash'

JobSummary:
  properties:
    schemaHash:
      $ref: '#/components/schemas/JobSchemaHash'

ExecutionLease:
  properties:
    schemaHash:
      $ref: '#/components/schemas/JobSchemaHash'
```

Add endpoints:

```text
POST /v1/tenants/{tenantId}/schemas
GET  /v1/tenants/{tenantId}/schemas
GET  /v1/tenants/{tenantId}/schemas/{schemaHash}
POST /v1/tenants/{tenantId}/schemas/{schemaHash}/archive
```

Register request:

```yaml
RegisterJobSchemaRequest:
  type: object
  additionalProperties: false
  required: [schema]
  properties:
    schema:
      $ref: '#/components/schemas/JobSchemaDocument'
```

List should support a state filter:

```text
state=ACTIVE | ARCHIVED | ALL
```

Default list behavior should be active-only.

## Storage Additions

SQLite and direct/Postgres runtimes need a tenant-local schema table.

Logical columns:

```text
tenant_id
schema_hash
schema_json
state
created_at
archived_at
```

Primary key:

```text
(tenant_id, schema_hash)
```

The direct runtime should store this in JobDB-owned tables, not in pgwf's core
queue schema. The SQLite runtime should add the equivalent table to
`pkg/jobdb/runtime/sqlite/schema.go`.

## Validation Errors

Schema validation failure should be a typed JobDB error that maps to HTTP `400`
for malformed new-job requests and HTTP `409` for stale/conflicting chapter
writes only when the write conflict is lease/ordinal related.

Suggested error:

```go
var ErrSchemaValidationFailed = errors.New("job schema validation failed")
```

The error payload should include:

- schema hash;
- chapter ordinal when available;
- JSON pointer to the failing location;
- concise validation message.

## Implementation Order

1. Add public schema types and error type in `pkg/jobdb`.
2. Add registry storage to SQLite, direct, and toy runtimes.
3. Add OpenAPI endpoints and regenerate `pkg/jobdb/internal/runtimeapi`.
4. Implement remote client/server registry methods.
5. Add `SubmitJob` and `SubmitRestartJob` schema selectors.
6. Store resolved schema hash in internal job metadata.
7. Add schema document cache and JSON Schema validator.
8. Enforce validation on submit, `add_chapter`, and `commit-if-waiting`.
9. Expose `schemaHash` in job/lease read models.
10. Add conformance tests covering no-schema jobs, active schema jobs, inline
    schema registration, archived schema behavior, and chapter-zero branching.
