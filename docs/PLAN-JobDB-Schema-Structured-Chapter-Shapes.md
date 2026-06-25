# Plan: Structured JobDB Chapter Schema Shapes

## Context

The current JobDB schema registry stores one JSON Schema document per schema
hash. That schema is applied to every visible chapter record. Authors can
currently express "chapter 0 differs from later chapters" using JSON Schema
conditionals over `ordinal`, but JobDB does not model that split explicitly.

Job completion has also moved to a single lease completion operation that
includes the final chapter:

```go
type CompleteExecutionRequest struct {
    Status          string
    Detail          string
    Chapter         *Chapter
    ArtifactUploads []ArtifactUpload
}
```

That gives JobDB a clean operational boundary for "last chapter" validation:
the final chapter is the chapter carried by `lease.Complete`, not something
inferred from ordinal or later job state.

## Goal

Replace the implicit "one schema over every chapter" pattern with a formal
JobDB schema document that has three chapter shape slots:

```json
{
  "chapterShape": {},
  "firstChapterShape": {},
  "lastChapterShape": {}
}
```

Semantics:

- `chapterShape` is the default shape.
- If only `chapterShape` is set, it validates every chapter.
- If `firstChapterShape` is set, it validates the first chapter instead of
  `chapterShape`.
- If `lastChapterShape` is set, it validates the final completion chapter
  instead of `chapterShape`.
- Non-first, non-last chapters always use `chapterShape`.

The registry remains tenant-local for lifecycle state. The compiled validator
cache remains keyed only by `schemaHash`.

## Proposed Schema Document

Use an envelope whose shape fields are JSON Schema draft 2020-12 schemas for a
full visible `ChapterRecord`.

```json
{
  "$schema": "https://jobdb.dev/schemas/job-schema/v1",
  "chapterShape": {
    "type": "object",
    "required": ["body"]
  },
  "firstChapterShape": {
    "type": "object",
    "required": ["body"],
    "properties": {
      "body": {
        "type": "object",
        "required": ["kind", "input"],
        "properties": {
          "kind": { "const": "jobStart" }
        }
      }
    }
  },
  "lastChapterShape": {
    "type": "object",
    "required": ["body"],
    "properties": {
      "body": {
        "type": "object",
        "required": ["kind", "outcome"],
        "properties": {
          "kind": { "const": "jobAttemptOutcome" }
        }
      }
    }
  }
}
```

`chapterShape` should be required in v1. If a tenant wants to constrain only the
first or last chapter, it can use a permissive default:

```json
{
  "chapterShape": true,
  "lastChapterShape": {
    "type": "object",
    "required": ["body"]
  }
}
```

Allowing JSON Schema boolean fragments (`true` and `false`) is useful here and
matches JSON Schema semantics.

## Compatibility

Do not support legacy raw JSON Schema documents in this change. A registered
JobDB schema document must use the formal envelope and must include
`chapterShape`.

Reject documents that do not use the envelope. This keeps the mental model
simple: every JobDB schema has the same top-level shape, and every shape slot is
explicitly named.

The envelope must not have unknown top-level fields except metadata fields that
we explicitly allow, such as `$schema` and `description`.

## Validation Roles

Introduce an internal role/position enum in `pkg/jobdb/internal/jobschema`:

```go
type ChapterRole int

const (
    ChapterRoleDefault ChapterRole = iota
    ChapterRoleFirst
    ChapterRoleLast
)
```

Compiled schema cache value changes from one validator to a small bundle:

```go
type compiledJobSchema struct {
    chapter *jsonschema.Schema
    first   *jsonschema.Schema // nil means use chapter
    last    *jsonschema.Schema // nil means use chapter
}
```

Selection rule:

```go
func (s compiledJobSchema) validator(role ChapterRole) *jsonschema.Schema {
    switch role {
    case ChapterRoleFirst:
        if s.first != nil {
            return s.first
        }
    case ChapterRoleLast:
        if s.last != nil {
            return s.last
        }
    }
    return s.chapter
}
```

`ValidateChapter` should accept the role:

```go
func ValidateChapter(
    ctx context.Context,
    registry jobdb.JobSchemaRegistry,
    key jobdb.JobSchemaKey,
    role ChapterRole,
    chapter jobdb.Chapter,
) error
```

If keeping call sites smaller is preferable, add wrappers:

```go
func ValidateFirstChapter(...)
func ValidateOrdinaryChapter(...)
func ValidateLastChapter(...)
```

## Runtime Enforcement Mapping

Apply the role based on the operation, not by guessing from ordinal alone.

| Operation | Role | Notes |
| --- | --- | --- |
| `SubmitJob` initial chapter | `first` | Validates chapter 0 before the job is committed. |
| `SubmitRestartJob` retained ordinal 0 | `first` | Restarted jobs still have a first visible chapter. |
| `SubmitRestartJob` retained non-zero chapters | `chapter` | Do not treat the retained highest ordinal as final. |
| `SubmitRestartJob` `RestartExtraChapter` | `chapter` | It is not terminal completion. |
| `PutChapter` | `chapter` | Non-terminal append path. |
| `CompleteTaskIfWaiting` | `chapter` | Task output write, not job terminal completion. |
| `ExecutionLease.Complete` / `CompleteJobWithLeaseByID` final chapter | `last` | Uses the chapter inside `CompleteExecutionRequest`. |

`lastChapterShape` is only selected for final completion chapters because the
complete operation is now the single public operation that completes the story
and carries the final chapter.

If a future API can create a one-chapter completed job, define precedence then.
For current JobDB jobs, the first chapter is created by submit and the last
chapter is written by completion, so the roles do not overlap.

## API Changes

### Public Go API

Current public request types use `json.RawMessage` for schema documents:

```go
type JobSchemaSelector struct {
    Hash   string
    Schema json.RawMessage
}

type RegisterJobSchemaRequest struct {
    TenantId string
    Schema   json.RawMessage
}
```

For the least disruptive implementation, keep these fields as raw JSON and add
a parser/canonicalizer for the new envelope. This avoids a wide public Go API
break while still making the wire/document shape formal.

Optionally add helper types for authors:

```go
type JobSchemaDocument struct {
    ChapterShape      json.RawMessage `json:"chapterShape"`
    FirstChapterShape json.RawMessage `json:"firstChapterShape,omitempty"`
    LastChapterShape  json.RawMessage `json:"lastChapterShape,omitempty"`
}
```

These helpers can be additive. The registry can continue accepting
`json.RawMessage`.

### OpenAPI

Replace the loose `JobSchemaDocument` description with a formal schema:

```yaml
JobSchemaDocument:
  type: object
  additionalProperties: false
  required: [chapterShape]
  properties:
    $schema:
      type: string
    description:
      type: string
    chapterShape:
      $ref: '#/components/schemas/JsonSchemaFragment'
    firstChapterShape:
      $ref: '#/components/schemas/JsonSchemaFragment'
    lastChapterShape:
      $ref: '#/components/schemas/JsonSchemaFragment'

JsonSchemaFragment:
  description: JSON Schema draft 2020-12 schema fragment.
  oneOf:
    - type: boolean
    - type: object
      additionalProperties: true
```

If OpenAPI code generation makes arbitrary JSON Schema fragments awkward, keep
the generated Go type as `json.RawMessage` with `x-go-type`, but still document
the formal fields in the OpenAPI contract.

## Parser And Canonicalization

Add an internal parser, for example:

```go
type ParsedJobSchema struct {
    ChapterShape      json.RawMessage
    FirstChapterShape json.RawMessage
    LastChapterShape  json.RawMessage
}

func ParseJobSchemaDocument(raw json.RawMessage) (ParsedJobSchema, error)
```

Responsibilities:

1. Decode exactly one JSON value.
2. Require the root value to be an object.
3. Require `chapterShape`.
4. Permit each shape to be a JSON object or boolean.
5. Reject unknown top-level fields other than explicitly allowed metadata
   fields.
6. Reject invalid JSON Schema fragments during registration.
7. Canonicalize the whole stored document for hashing, not each fragment
   independently.

The schema hash remains:

```text
sha256(canonical_json(original_schema_document))
```

## Compiled Cache

The compiled cache remains:

```text
schemaHash -> compiledJobSchema
```

Tenant ID is not part of the cache key. Registry reads are still tenant-local
for lifecycle and access control, but identical schema hashes compile once per
process.

Compile each shape under a distinct synthetic URI so validation errors are
traceable:

```text
jobdb-schema:///sha256:<hash>/chapter
jobdb-schema:///sha256:<hash>/first
jobdb-schema:///sha256:<hash>/last
```

## Error Behavior

Keep `jobdb.ErrJobSchemaValidation` for:

- invalid envelope documents;
- missing `chapterShape`;
- invalid JSON Schema fragments;
- chapter validation failure for any role.

Include the selected role in the error message:

```text
job schema validation failed: schema sha256:... rejected last chapter 7: ...
```

This makes last-chapter failures distinguishable without changing the public
sentinel.

## Implementation Steps

1. Add `ParsedJobSchema`, `ChapterRole`, and parser tests in
   `pkg/jobdb/internal/jobschema`.
2. Change the compiled validator cache value from `*jsonschema.Schema` to
   `compiledJobSchema`.
3. Update `ValidateSchemaDocument` to validate all configured fragments.
4. Update `ValidateChapter` and call sites to pass the role.
5. Map runtime write paths to roles:
   - submit: first;
   - restart retained first: first;
   - non-terminal writes: chapter;
   - lease completion chapter: last.
6. Update OpenAPI `JobSchemaDocument` and regenerate runtime API bindings.
7. Update `docs/SPEC-JobDB-Schema-Subsystem.md` and
   `docs/GUIDE-JobDB-Schemas.md`.
8. Add tests for:
   - `chapterShape` only applies to all roles;
   - `firstChapterShape` overrides only submit/retained first chapter;
   - `lastChapterShape` overrides only lease completion;
   - ordinary `PutChapter` does not use `lastChapterShape`;
   - raw JSON Schema documents without `chapterShape` are rejected;
   - invalid envelope shape fragments are rejected at registration.

## Rollout Notes

- New clients must send the formal envelope immediately after the parser and
  OpenAPI changes land.
- Existing raw-schema documents will not register under the new parser. If any
  are already stored in non-production data, re-register them as
  `{ "chapterShape": <raw-schema> }`.
- The archive model does not change. An archived schema remains valid for
  mutable jobs that already reference its hash.
