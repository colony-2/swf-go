# Guide: Using JobDB Schemas

JobDB schemas are optional tenant-local JSON Schemas for validating the visible
chapter records written by a job. Jobs without a schema behave exactly like
ordinary jobs: no schema is resolved, no schema hash is stored, and no schema
validation runs.

Use a schema when a tenant wants a stable contract for job inputs, outputs,
metadata, or chapter kinds. Do not use schemas for execution behavior. Schemas
do not control leasing, polling, scheduling, retries, artifact storage, or job
state transitions.

## What A Schema Validates

A schema validates each visible `ChapterRecord` document. The important fields
are:

```json
{
  "ordinal": 0,
  "createdAt": "2026-06-23T00:00:00Z",
  "taskType": "example-job",
  "body": {
    "kind": "jobStart",
    "input": {
      "kind": "example",
      "version": 1
    }
  },
  "artifacts": []
}
```

Supported `body.kind` values are:

- `jobStart`
- `taskAttemptOutcome`
- `jobAttemptOutcome`
- `restartExtra`

Successful outcomes use:

```json
{
  "body": {
    "kind": "taskAttemptOutcome",
    "outcome": {
      "kind": "success",
      "output": {
        "ok": true
      }
    }
  }
}
```

Application, system, and timeout failures use `outcome.kind` values of
`appError`, `systemError`, and `timeout`.

JSON Schema fields are open by default. Extra fields are allowed unless the
schema explicitly sets `additionalProperties: false` or
`unevaluatedProperties: false`.

## Chapter-Specific Shapes

Use normal JSON Schema conditionals or `oneOf` to give chapter zero a different
shape from later chapters.

```json
{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "required": ["ordinal", "body"],
  "allOf": [
    {
      "if": {
        "properties": {
          "ordinal": { "const": 0 }
        },
        "required": ["ordinal"]
      },
      "then": {
        "properties": {
          "body": {
            "type": "object",
            "required": ["kind", "input"],
            "properties": {
              "kind": { "const": "jobStart" },
              "input": {
                "type": "object",
                "required": ["kind", "version"],
                "properties": {
                  "kind": { "const": "example" },
                  "version": { "const": 1 }
                }
              }
            }
          }
        }
      }
    },
    {
      "if": {
        "properties": {
          "ordinal": { "minimum": 1 }
        },
        "required": ["ordinal"]
      },
      "then": {
        "properties": {
          "body": {
            "type": "object",
            "required": ["kind", "outcome"],
            "properties": {
              "kind": { "const": "taskAttemptOutcome" },
              "outcome": {
                "type": "object",
                "required": ["kind", "output"],
                "properties": {
                  "kind": { "const": "success" },
                  "output": {
                    "type": "object",
                    "required": ["ok"],
                    "properties": {
                      "ok": { "const": true }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
  ]
}
```

## Register A Schema

Concrete runtimes and the remote runtime implement `jobdb.JobSchemaRegistry`.

```go
registry, ok := rt.(jobdb.JobSchemaRegistry)
if !ok {
    return fmt.Errorf("runtime does not support job schemas")
}

schema := json.RawMessage(`{
  "$schema": "https://json-schema.org/draft/2020-12/schema",
  "type": "object",
  "required": ["ordinal", "body"],
  "properties": {
    "body": {
      "type": "object",
      "required": ["kind"],
      "properties": {
        "kind": {
          "enum": ["jobStart", "taskAttemptOutcome", "jobAttemptOutcome", "restartExtra"]
        }
      }
    }
  }
}`)

info, err := registry.RegisterJobSchema(ctx, jobdb.RegisterJobSchemaRequest{
    TenantId: "tenant-a",
    Schema:   schema,
})
if err != nil {
    return err
}
fmt.Println(info.SchemaHash)
```

Registration canonicalizes the JSON, computes a `sha256:<hex>` hash, compiles
the JSON Schema, and stores it for that tenant. Registering the same schema for
the same tenant is idempotent.

## Submit A Job With A Schema

You can attach a schema inline:

```go
handle, err := rt.SubmitJob(ctx, jobdb.SubmitJobRequest{
    Job: jobdb.SubmitJob{
        TenantId: "tenant-a",
        JobType:  "example-job",
        Data:     jobdb.NewTaskDataOrPanic(map[string]any{"kind": "example", "version": 1}),
        Schema:   &jobdb.JobSchemaSelector{Schema: schema},
    },
})
```

Or reference a registered active schema by hash:

```go
handle, err := rt.SubmitJob(ctx, jobdb.SubmitJobRequest{
    Job: jobdb.SubmitJob{
        TenantId: "tenant-a",
        JobType:  "example-job",
        Data:     jobdb.NewTaskDataOrPanic(map[string]any{"kind": "example", "version": 1}),
        Schema:   &jobdb.JobSchemaSelector{Hash: info.SchemaHash},
    },
})
```

If both `Hash` and `Schema` are supplied, JobDB computes the inline schema hash
and requires it to match `Hash`.

The resolved hash is stored in JobDB's internal job metadata and exposed on job
and lease read models as `SchemaHash`.

## Submit A Job Without A Schema

Leave `Schema` unset:

```go
handle, err := rt.SubmitJob(ctx, jobdb.SubmitJobRequest{
    Job: jobdb.SubmitJob{
        TenantId: "tenant-a",
        JobType:  "plain-job",
        Data:     jobdb.NewTaskDataOrPanic(map[string]any{"anything": true}),
    },
})
```

No schema validation runs for that job.

## Restarts

`SubmitRestartJob` accepts the same schema selector:

```go
restart, err := rt.SubmitRestartJob(ctx, jobdb.SubmitRestartJobRequest{
    Job: jobdb.SubmitRestartJob{
        PriorJobKey:    handle.JobKey,
        LastStepToKeep: 2,
        Schema:         &jobdb.JobSchemaSelector{Hash: info.SchemaHash},
    },
})
```

If a restart supplies a schema, retained chapters and any new restart chapter
must validate against the new job's schema. If the restart omits `Schema`, the
new job has no schema even if the prior job had one.

## Reads And Lifecycle

Get a schema:

```go
got, err := registry.GetJobSchema(ctx, jobdb.JobSchemaKey{
    TenantId:   "tenant-a",
    SchemaHash: info.SchemaHash,
})
```

List active schemas:

```go
active, err := registry.ListJobSchemas(ctx, jobdb.ListJobSchemasRequest{
    TenantId: "tenant-a",
})
```

List archived or all schemas:

```go
archived, err := registry.ListJobSchemas(ctx, jobdb.ListJobSchemasRequest{
    TenantId: "tenant-a",
    State:    jobdb.JobSchemaListStateArchived,
})

all, err := registry.ListJobSchemas(ctx, jobdb.ListJobSchemasRequest{
    TenantId: "tenant-a",
    State:    jobdb.JobSchemaListStateAll,
})
```

Archive a schema:

```go
archived, err := registry.ArchiveJobSchema(ctx, jobdb.JobSchemaKey{
    TenantId:   "tenant-a",
    SchemaHash: info.SchemaHash,
})
```

Archive is one-way. Archived schemas remain readable and remain valid for
already-created mutable jobs. New jobs cannot select an archived schema.

There is no delete operation.

## REST Shape

REST uses `schemaHash` instead of the Go field name `Hash`.

Register:

```http
POST /v1/tenants/tenant-a/schemas
Content-Type: application/json

{
  "schema": {
    "$schema": "https://json-schema.org/draft/2020-12/schema",
    "type": "object",
    "required": ["ordinal", "body"]
  }
}
```

Submit with an inline schema:

```json
{
  "job": {
    "jobType": "example-job",
    "data": {
      "data": {
        "kind": "example",
        "version": 1
      },
      "artifacts": []
    },
    "schema": {
      "schema": {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "type": "object",
        "required": ["ordinal", "body"]
      }
    }
  }
}
```

Submit with a registered schema:

```json
{
  "job": {
    "jobType": "example-job",
    "data": {
      "data": {
        "kind": "example",
        "version": 1
      },
      "artifacts": []
    },
    "schema": {
      "schemaHash": "sha256:0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
    }
  }
}
```

List and archive:

```http
GET  /v1/tenants/tenant-a/schemas?state=ALL
GET  /v1/tenants/tenant-a/schemas/{schemaHash}
POST /v1/tenants/tenant-a/schemas/{schemaHash}/archive
```

## Errors

Use `errors.Is` with the typed errors:

```go
switch {
case errors.Is(err, jobdb.ErrJobSchemaValidation):
    // The schema document is invalid or a chapter failed validation.
case errors.Is(err, jobdb.ErrJobSchemaArchived):
    // A new job tried to use an archived schema.
case errors.Is(err, jobdb.ErrJobSchemaNotFound):
    // The tenant/hash pair does not exist.
}
```

Schema validation failures map to HTTP `400` over the remote API. Lease and
ordinal conflicts remain conflict errors.

## Operational Notes

- Schema hashes are tenant-local for lifecycle state. The same content can have
  the same hash in multiple tenants, but each tenant registers and archives it
  independently.
- Schema documents are immutable by hash. Register a new schema to change a
  contract.
- JobDB does not apply JSON Schema defaults. Validation never mutates stored
  chapters.
- Runtime daemons keep a process-local compiled-schema cache keyed by
  `(tenantId, schemaHash)`.
- Remote chapter appends use the signed lease token's schema hash when present,
  so the append path does not need an extra job-row read just to discover the
  schema.
