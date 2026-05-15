# Migration: SQLite Embedded Runtime for Go Module Users

## Summary

`swf-go` now uses a SQLite-backed runtime for embedded/local durable execution.
If your Go module previously used any embedded runtime path other than the
in-memory `toy` runtime, move to:

```go
github.com/colony-2/swf-go/pkg/swf/runtime/sqlite
```

The `toy` runtime remains appropriate for purely in-memory tests and examples.
It is still not durable and should not be used when you expect persisted jobs,
chapters, leases, or artifacts.

## What Changed

The new SQLite runtime stores local SWF state without requiring embedded
Postgres, pgwf schema installation, a separate embedded Strata HTTP daemon, or
Pebble row storage.

It composes:

1. SWF job scheduling, leases, wait state, metadata, and archive state in a
   local SQLite database.
2. `strata-go`'s SQLite rowstore in the same SQLite database.
3. `strata-go` blobfs storage for large artifact bytes.

For `swfd`, the no-subcommand path now starts the SQLite runtime. `swfd toy`
is the explicit in-memory command.

## Recommended Runtime Construction

For durable embedded use, construct the runtime from a SQLite path:

```go
package main

import (
    "context"
    "log"

    "github.com/colony-2/swf-go/pkg/swf/runtime/sqlite"
)

func main() {
    ctx := context.Background()

    rt, err := sqlite.NewFromConfig(ctx, sqlite.Config{
        DBPath:  "swf.db",
        BlobDir: "swf.db.blobs", // optional; defaults to <db>.blobs
    })
    if err != nil {
        log.Fatal(err)
    }
    defer rt.Close(ctx)

    _ = rt
}
```

Then pass `rt` anywhere a `swf.WorkflowRuntime` is expected, including
`swf.NewEngineBuilder().WithRuntime(rt)` or `remote.NewServer(rt)`.

For tests that need a temporary durable runtime:

```go
embedded, err := sqlite.StartEmbeddedRuntime(context.Background())
if err != nil {
    t.Fatal(err)
}
defer embedded.Shutdown()

rt := embedded.Runtime
```

## Direct Runtime Migration

If you previously used the direct runtime only to get an embedded durable local
runtime, replace it with SQLite.

Before:

```go
import directruntime "github.com/colony-2/swf-go/pkg/swf/runtime/direct"

embedded, err := directruntime.StartEmbeddedRuntime(context.Background())
if err != nil {
    t.Fatal(err)
}
defer embedded.Shutdown()

rt := embedded.Runtime
```

After:

```go
import sqliteruntime "github.com/colony-2/swf-go/pkg/swf/runtime/sqlite"

embedded, err := sqliteruntime.StartEmbeddedRuntime(context.Background())
if err != nil {
    t.Fatal(err)
}
defer embedded.Shutdown()

rt := embedded.Runtime
```

If you previously constructed `direct.NewFromConfig(postgresDSN, strataURL,
strataAPIKey)` for local embedded use, switch to `sqlite.NewFromConfig` and
delete the embedded Postgres and embedded Strata daemon setup.

Before:

```go
rt, err := directruntime.NewFromConfig(postgresDSN, strataBaseURL, strataAPIKey)
```

After:

```go
rt, err := sqliteruntime.NewFromConfig(ctx, sqliteruntime.Config{
    DBPath:  "swf.db",
    BlobDir: "swf.db.blobs",
})
```

The direct runtime can remain useful as a compatibility/reference path for
existing Postgres plus Strata deployments, but it is no longer the recommended
embedded runtime.

## Toy Runtime

No migration is required for code intentionally using:

```go
github.com/colony-2/swf-go/pkg/swf/runtime/toy
```

Keep using `toy.New()` for fast in-memory tests, examples, and cases where all
state may disappear at process exit. Move to SQLite if the test or application
asserts persisted runtime state, artifact reads after restart, lease behavior,
or remote server behavior.

## CLI Changes

Before this change, `swfd` defaulted to the toy runtime. It now defaults to
SQLite:

```bash
swfd --listen 127.0.0.1:9047 --db swf.db
```

Equivalent explicit command:

```bash
swfd sqlite --listen 127.0.0.1:9047 --db swf.db
```

Useful flags and environment:

```text
--db          SQLite database path, default swf.db
--sqlite-dsn  modernc.org/sqlite DSN; overrides --db
--blob-dir    blobfs directory; defaults to <db>.blobs
SWF_SQLITE_DSN
```

Use `swfd toy` when you intentionally want the old in-memory behavior.

## Storage And Data Migration

There is no automatic data migration from existing Postgres/pgwf, Pebble
Strata, or external Strata storage into the new SQLite embedded runtime.

For new embedded deployments, start with an empty SQLite database. For existing
stateful deployments, plan an application-specific export/import if you need to
carry historical jobs, chapters, and artifacts forward.

The SQLite runtime is intended for local embedded and single-node durable use.
Do not put the SQLite database on a network filesystem for distributed worker
coordination.

## Module Notes

Go modules should pick up the newer `strata-go` dependency that includes the
SQLite rowstore and embedded builder support. The SQLite runtime imports
`modernc.org/sqlite`, so no CGO SQLite driver is required.
