package directimpl

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/jobschema"
)

const jobSchemaSQL = `
CREATE TABLE IF NOT EXISTS jobdb_schemas (
	tenant_id TEXT NOT NULL,
	schema_hash TEXT NOT NULL,
	schema_json JSONB NOT NULL,
	state TEXT NOT NULL,
	created_at TIMESTAMPTZ NOT NULL,
	archived_at TIMESTAMPTZ,
	PRIMARY KEY (tenant_id, schema_hash)
);

CREATE INDEX IF NOT EXISTS jobdb_schemas_list_idx
	ON jobdb_schemas (tenant_id, state, created_at DESC, schema_hash ASC);
`

var _ jobdb.JobSchemaRegistry = (*Runtime)(nil)

type schemaRow struct {
	tenantID   string
	schemaHash string
	schemaJSON json.RawMessage
	state      string
	createdAt  time.Time
	archivedAt sql.NullTime
}

type schemaContextDB interface {
	ExecContext(context.Context, string, ...any) (sql.Result, error)
	QueryContext(context.Context, string, ...any) (*sql.Rows, error)
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func (r *Runtime) schemaDB(ctx context.Context) schemaContextDB {
	if tx := r.sqlTxFromCtx(ctx); tx != nil {
		return tx
	}
	return r.udb
}

func migrateJobSchemas(ctx context.Context, db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("db is required")
	}
	if _, err := db.ExecContext(ctx, jobSchemaSQL); err != nil {
		return fmt.Errorf("migrate schemas: %w", err)
	}
	return nil
}

func scanSchemaRow(scanner interface{ Scan(dest ...any) error }) (schemaRow, error) {
	var row schemaRow
	var schemaJSON []byte
	if err := scanner.Scan(
		&row.tenantID,
		&row.schemaHash,
		&schemaJSON,
		&row.state,
		&row.createdAt,
		&row.archivedAt,
	); err != nil {
		return schemaRow{}, err
	}
	row.schemaJSON = append(json.RawMessage(nil), schemaJSON...)
	return row, nil
}

func (r *Runtime) RegisterJobSchema(ctx context.Context, req jobdb.RegisterJobSchemaRequest) (jobdb.JobSchemaInfo, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := r.validate(); err != nil {
		return jobdb.JobSchemaInfo{}, err
	}
	if req.TenantId == "" {
		return jobdb.JobSchemaInfo{}, fmt.Errorf("tenantId is required")
	}
	hash, canonical, err := jobdb.JobSchemaHash(req.Schema)
	if err != nil {
		return jobdb.JobSchemaInfo{}, err
	}
	if err := jobschema.ValidateSchemaDocument(hash, canonical); err != nil {
		return jobdb.JobSchemaInfo{}, err
	}
	now := time.Now().UTC()
	if _, err := r.schemaDB(ctx).ExecContext(ctx, `
INSERT INTO jobdb_schemas (
	tenant_id, schema_hash, schema_json, state, created_at
) VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (tenant_id, schema_hash) DO NOTHING`,
		req.TenantId, hash, string(canonical), string(jobdb.JobSchemaStateActive), now); err != nil {
		return jobdb.JobSchemaInfo{}, err
	}
	key := jobdb.JobSchemaKey{TenantId: req.TenantId, SchemaHash: hash}
	info, err := r.GetJobSchema(ctx, key)
	if err != nil {
		return jobdb.JobSchemaInfo{}, err
	}
	if !bytes.Equal(jobdb.NormalizeJSON(info.Schema), canonical) {
		return jobdb.JobSchemaInfo{}, jobdb.ErrConflict
	}
	return info, nil
}

func (r *Runtime) GetJobSchema(ctx context.Context, key jobdb.JobSchemaKey) (jobdb.JobSchemaInfo, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := r.validate(); err != nil {
		return jobdb.JobSchemaInfo{}, err
	}
	if err := key.Validate(); err != nil {
		return jobdb.JobSchemaInfo{}, err
	}
	row, err := scanSchemaRow(r.schemaDB(ctx).QueryRowContext(ctx, `
SELECT tenant_id, schema_hash, schema_json, state, created_at, archived_at
FROM jobdb_schemas
WHERE tenant_id = $1 AND schema_hash = $2`, key.TenantId, key.SchemaHash))
	if err == sql.ErrNoRows {
		return jobdb.JobSchemaInfo{}, jobdb.ErrJobSchemaNotFound
	}
	if err != nil {
		return jobdb.JobSchemaInfo{}, err
	}
	return schemaInfoFromRow(row), nil
}

func (r *Runtime) ListJobSchemas(ctx context.Context, req jobdb.ListJobSchemasRequest) (jobdb.ListJobSchemasResponse, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := r.validate(); err != nil {
		return jobdb.ListJobSchemasResponse{}, err
	}
	if req.TenantId == "" {
		return jobdb.ListJobSchemasResponse{}, fmt.Errorf("tenantId is required")
	}
	state := req.State
	if state == "" {
		state = jobdb.JobSchemaListStateActive
	}
	query := `
SELECT tenant_id, schema_hash, schema_json, state, created_at, archived_at
FROM jobdb_schemas
WHERE tenant_id = $1`
	args := []any{req.TenantId}
	switch state {
	case jobdb.JobSchemaListStateActive:
		query += ` AND state = $2`
		args = append(args, string(jobdb.JobSchemaStateActive))
	case jobdb.JobSchemaListStateArchived:
		query += ` AND state = $2`
		args = append(args, string(jobdb.JobSchemaStateArchived))
	case jobdb.JobSchemaListStateAll:
	default:
		return jobdb.ListJobSchemasResponse{}, fmt.Errorf("unknown schema state %q", req.State)
	}
	query += ` ORDER BY created_at DESC, schema_hash ASC`
	rows, err := r.schemaDB(ctx).QueryContext(ctx, query, args...)
	if err != nil {
		return jobdb.ListJobSchemasResponse{}, err
	}
	defer rows.Close()
	out := make([]jobdb.JobSchemaInfo, 0)
	for rows.Next() {
		row, err := scanSchemaRow(rows)
		if err != nil {
			return jobdb.ListJobSchemasResponse{}, err
		}
		out = append(out, schemaInfoFromRow(row))
	}
	if err := rows.Err(); err != nil {
		return jobdb.ListJobSchemasResponse{}, err
	}
	return jobdb.ListJobSchemasResponse{Schemas: out}, nil
}

func (r *Runtime) ArchiveJobSchema(ctx context.Context, key jobdb.JobSchemaKey) (jobdb.JobSchemaInfo, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := r.validate(); err != nil {
		return jobdb.JobSchemaInfo{}, err
	}
	if err := key.Validate(); err != nil {
		return jobdb.JobSchemaInfo{}, err
	}
	row, err := scanSchemaRow(r.schemaDB(ctx).QueryRowContext(ctx, `
UPDATE jobdb_schemas
SET state = $3, archived_at = COALESCE(archived_at, $4)
WHERE tenant_id = $1 AND schema_hash = $2
RETURNING tenant_id, schema_hash, schema_json, state, created_at, archived_at`,
		key.TenantId, key.SchemaHash, string(jobdb.JobSchemaStateArchived), time.Now().UTC()))
	if err == sql.ErrNoRows {
		return jobdb.JobSchemaInfo{}, jobdb.ErrJobSchemaNotFound
	}
	if err != nil {
		return jobdb.JobSchemaInfo{}, err
	}
	return schemaInfoFromRow(row), nil
}

func schemaInfoFromRow(row schemaRow) jobdb.JobSchemaInfo {
	info := jobdb.JobSchemaInfo{
		TenantId:   row.tenantID,
		SchemaHash: row.schemaHash,
		Schema:     append(json.RawMessage(nil), row.schemaJSON...),
		State:      jobdb.JobSchemaState(row.state),
		CreatedAt:  row.createdAt.UTC(),
	}
	if row.archivedAt.Valid {
		archivedAt := row.archivedAt.Time.UTC()
		info.ArchivedAt = &archivedAt
	}
	return info
}
