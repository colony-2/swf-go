package sqlite

import (
	"bytes"
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/jobschema"
)

var _ jobdb.JobSchemaRegistry = (*Runtime)(nil)

type schemaRow struct {
	tenantID     string
	schemaHash   string
	schemaJSON   []byte
	state        string
	createdAtNS  int64
	archivedAtNS sql.NullInt64
}

func scanSchemaRow(scanner interface{ Scan(dest ...any) error }) (schemaRow, error) {
	var row schemaRow
	var schemaJSON []byte
	if err := scanner.Scan(
		&row.tenantID,
		&row.schemaHash,
		&schemaJSON,
		&row.state,
		&row.createdAtNS,
		&row.archivedAtNS,
	); err != nil {
		return schemaRow{}, err
	}
	row.schemaJSON = cloneBytes(schemaJSON)
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
	key := jobdb.JobSchemaKey{TenantId: req.TenantId, SchemaHash: hash}
	now := timeToNS(timeNowUTC())
	err = r.withTx(ctx, func(tx *sql.Tx) error {
		_, err := tx.ExecContext(ctx, `
INSERT OR IGNORE INTO jobdb_schemas (
	tenant_id, schema_hash, schema_json, state, created_at_ns
) VALUES (?, ?, ?, ?, ?)`,
			req.TenantId, hash, canonical, string(jobdb.JobSchemaStateActive), now)
		return err
	})
	if err != nil {
		return jobdb.JobSchemaInfo{}, err
	}
	info, err := r.GetJobSchema(ctx, key)
	if err != nil {
		return jobdb.JobSchemaInfo{}, err
	}
	if !bytes.Equal(info.Schema, canonical) {
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
	row, err := scanSchemaRow(r.db.QueryRowContext(ctx, `
SELECT tenant_id, schema_hash, schema_json, state, created_at_ns, archived_at_ns
FROM jobdb_schemas
WHERE tenant_id = ? AND schema_hash = ?`, key.TenantId, key.SchemaHash))
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
SELECT tenant_id, schema_hash, schema_json, state, created_at_ns, archived_at_ns
FROM jobdb_schemas
WHERE tenant_id = ?`
	args := []any{req.TenantId}
	switch state {
	case jobdb.JobSchemaListStateActive:
		query += ` AND state = ?`
		args = append(args, string(jobdb.JobSchemaStateActive))
	case jobdb.JobSchemaListStateArchived:
		query += ` AND state = ?`
		args = append(args, string(jobdb.JobSchemaStateArchived))
	case jobdb.JobSchemaListStateAll:
	default:
		return jobdb.ListJobSchemasResponse{}, fmt.Errorf("unknown schema state %q", req.State)
	}
	query += ` ORDER BY created_at_ns DESC, schema_hash ASC`
	rows, err := r.db.QueryContext(ctx, query, args...)
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
	archivedAt := timeToNS(timeNowUTC())
	result, err := r.db.ExecContext(ctx, `
UPDATE jobdb_schemas
SET state = ?, archived_at_ns = COALESCE(archived_at_ns, ?)
WHERE tenant_id = ? AND schema_hash = ?`,
		string(jobdb.JobSchemaStateArchived), archivedAt, key.TenantId, key.SchemaHash)
	if err != nil {
		return jobdb.JobSchemaInfo{}, err
	}
	n, _ := result.RowsAffected()
	if n == 0 {
		return jobdb.JobSchemaInfo{}, jobdb.ErrJobSchemaNotFound
	}
	return r.GetJobSchema(ctx, key)
}

func schemaInfoFromRow(row schemaRow) jobdb.JobSchemaInfo {
	return jobdb.JobSchemaInfo{
		TenantId:   row.tenantID,
		SchemaHash: row.schemaHash,
		Schema:     cloneJSON(row.schemaJSON),
		State:      jobdb.JobSchemaState(row.state),
		CreatedAt:  timeFromNS(row.createdAtNS),
		ArchivedAt: nullTimeFromNS(row.archivedAtNS),
	}
}

func timeNowUTC() time.Time {
	return time.Now().UTC()
}
