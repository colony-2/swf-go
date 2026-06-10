package sqlite

import (
	"context"
	"database/sql"
	"fmt"
)

const schemaSQL = `
CREATE TABLE IF NOT EXISTS swf_jobs (
	tenant_id TEXT NOT NULL,
	job_id TEXT NOT NULL,
	job_type TEXT NOT NULL,
	next_need TEXT NOT NULL,
	payload BLOB NOT NULL DEFAULT x'',
	metadata BLOB,
	wait_for BLOB NOT NULL DEFAULT x'',
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
);

CREATE INDEX IF NOT EXISTS swf_jobs_poll_idx
	ON swf_jobs (archived_at_ns, tenant_id, next_need, available_at_ns, created_at_ns);

CREATE INDEX IF NOT EXISTS swf_jobs_list_idx
	ON swf_jobs (tenant_id, created_at_ns DESC, job_id DESC);
`

func migrate(ctx context.Context, db *sql.DB) error {
	if db == nil {
		return fmt.Errorf("sqlite runtime: db is required")
	}
	if _, err := db.ExecContext(ctx, schemaSQL); err != nil {
		return fmt.Errorf("sqlite runtime: migrate: %w", err)
	}
	return nil
}
