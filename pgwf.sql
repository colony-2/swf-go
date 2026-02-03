BEGIN;

CREATE SCHEMA IF NOT EXISTS pgwf;

SET search_path = pgwf, public;

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE SEQUENCE IF NOT EXISTS pgwf.jobs_trace_id_seq;

CREATE TABLE IF NOT EXISTS pgwf.jobs (
    tenant_id TEXT NOT NULL,
    job_id TEXT NOT NULL,
    next_need TEXT NOT NULL,
    wait_for TEXT[] NOT NULL DEFAULT '{}'::TEXT[],
    payload JSONB NOT NULL DEFAULT '{}'::JSONB,
    singleton_key TEXT,
    available_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    expires_at TIMESTAMPTZ NOT NULL DEFAULT 'infinity',
    lease_id TEXT,
    lease_expires_at TIMESTAMPTZ NOT NULL DEFAULT '-infinity',
    lease_expiration_count BIGINT NOT NULL DEFAULT 0,
    consecutive_expirations BIGINT NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    cancel_requested BOOLEAN NOT NULL DEFAULT FALSE,
    cancel_requested_by TEXT,
    cancel_requested_at TIMESTAMPTZ,
    PRIMARY KEY (tenant_id, job_id),
    CONSTRAINT jobs_payload_is_object CHECK (jsonb_typeof(payload) = 'object'),
    CONSTRAINT jobs_payload_size_limit CHECK (pg_column_size(payload) <= 512)
);

CREATE TABLE IF NOT EXISTS pgwf.jobs_archive (
    archived_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    tenant_id TEXT NOT NULL,
    job_id TEXT NOT NULL,
    next_need TEXT NOT NULL,
    wait_for TEXT[] NOT NULL DEFAULT '{}'::TEXT[],
    payload JSONB NOT NULL DEFAULT '{}'::JSONB,
    singleton_key TEXT,
    created_at TIMESTAMPTZ NOT NULL,
    expires_at TIMESTAMPTZ NOT NULL DEFAULT 'infinity',
    lease_id TEXT,
    lease_expiration_count BIGINT NOT NULL DEFAULT 0,
    consecutive_expirations BIGINT NOT NULL DEFAULT 0,
    cancel_requested BOOLEAN NOT NULL DEFAULT FALSE,
    cancel_requested_by TEXT,
    cancel_requested_at TIMESTAMPTZ,
    PRIMARY KEY (tenant_id, job_id),
    CONSTRAINT jobs_archive_payload_is_object CHECK (jsonb_typeof(payload) = 'object'),
    CONSTRAINT jobs_archive_payload_size_limit CHECK (pg_column_size(payload) <= 512)
);

CREATE TABLE IF NOT EXISTS pgwf.jobs_trace (
    trace_id BIGINT PRIMARY KEY DEFAULT nextval('pgwf.jobs_trace_id_seq'),
    tenant_id TEXT NOT NULL,
    job_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    worker_id TEXT NOT NULL,
    event_at TIMESTAMPTZ NOT NULL DEFAULT clock_timestamp(),
    input_data JSONB NOT NULL,
    output_data JSONB
);

-- Performance indexes for multi-tenant operations
CREATE INDEX IF NOT EXISTS idx_jobs_tenant_ready_work
ON pgwf.jobs(tenant_id, next_need, created_at)
WHERE NOT cancel_requested;

CREATE INDEX IF NOT EXISTS idx_jobs_tenant_active_singleton
ON pgwf.jobs(tenant_id, singleton_key)
WHERE singleton_key IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_jobs_tenant_waitfor
ON pgwf.jobs(tenant_id, job_id)
INCLUDE (wait_for);

CREATE INDEX IF NOT EXISTS idx_jobs_tenant_cancelled
ON pgwf.jobs(tenant_id, created_at)
WHERE cancel_requested = TRUE;

CREATE INDEX IF NOT EXISTS idx_trace_tenant_job_event
ON pgwf.jobs_trace(tenant_id, job_id, event_at DESC);

CREATE OR REPLACE FUNCTION pgwf.crash_concern_threshold()
RETURNS INTEGER
LANGUAGE sql
AS $$
    SELECT 5;
$$;

CREATE OR REPLACE FUNCTION pgwf.set_crash_concern_threshold(p_threshold INTEGER)
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
BEGIN
    IF p_threshold IS NULL OR p_threshold <= 0 THEN
        RAISE EXCEPTION 'threshold must be positive';
    END IF;

    EXECUTE format(
        'CREATE OR REPLACE FUNCTION pgwf.crash_concern_threshold() RETURNS INTEGER LANGUAGE plpgsql AS %L',
        format('BEGIN RETURN %s; END;', p_threshold)
    );

    RETURN p_threshold;
END;
$$;

CREATE OR REPLACE VIEW pgwf.jobs_with_status AS
SELECT
    j.*,
    CASE
        WHEN j.lease_expires_at > clock_timestamp() THEN 'ACTIVE'
        WHEN j.cancel_requested THEN 'CANCELLED'
        WHEN j.available_at > clock_timestamp() THEN 'AWAITING_FUTURE'
        WHEN EXISTS (
            SELECT 1
            FROM unnest(j.wait_for) AS dep_job_id
            WHERE NOT EXISTS (
                SELECT 1
                FROM pgwf.jobs_archive ja
                WHERE ja.tenant_id = j.tenant_id
                  AND ja.job_id = dep_job_id
            )
        ) THEN 'PENDING_JOBS'
        WHEN j.consecutive_expirations >= pgwf.crash_concern_threshold() THEN 'CRASH_CONCERN'
        WHEN j.expires_at <= clock_timestamp() THEN 'EXPIRED'
        ELSE 'READY'
    END AS status
FROM pgwf.jobs j;

CREATE OR REPLACE VIEW pgwf.jobs_friendly_status AS
SELECT
    jws.tenant_id,
    jws.job_id,
    jws.status,
    jws.created_at AS creation_dt,
    CASE WHEN jws.status = 'PENDING_JOBS' THEN jws.wait_for ELSE NULL END AS pending_jobs,
    CASE WHEN jws.status = 'AWAITING_FUTURE' THEN jws.available_at ELSE NULL END AS sleep_until,
    CASE WHEN jws.status = 'ACTIVE' THEN jws.lease_id ELSE NULL END AS worker_id,
    CASE WHEN jws.status = 'CANCELLED' THEN jws.cancel_requested_at ELSE NULL END AS cancelled_at,
    CASE WHEN jws.status = 'CANCELLED' THEN jws.cancel_requested_by ELSE NULL END AS cancelled_by,
    jws.expires_at,
    jws.payload
FROM pgwf.jobs_with_status jws;

CREATE OR REPLACE FUNCTION pgwf.is_trace_enabled()
RETURNS BOOLEAN
LANGUAGE sql
AS $$
    SELECT TRUE;
$$;

CREATE OR REPLACE FUNCTION pgwf.set_trace(enabled BOOLEAN)
RETURNS BOOLEAN
LANGUAGE plpgsql
AS $$
BEGIN
    IF enabled IS NULL THEN
        RAISE EXCEPTION 'enabled flag cannot be NULL';
    END IF;

    EXECUTE format(
        'CREATE OR REPLACE FUNCTION pgwf.is_trace_enabled() RETURNS BOOLEAN LANGUAGE plpgsql AS %L',
        CASE WHEN enabled THEN 'BEGIN RETURN TRUE; END;' ELSE 'BEGIN RETURN FALSE; END;' END
    );

    RETURN enabled;
END;
$$;

CREATE OR REPLACE FUNCTION pgwf.is_notify_enabled()
RETURNS BOOLEAN
LANGUAGE sql
AS $$
    SELECT FALSE;
$$;

CREATE OR REPLACE FUNCTION pgwf.set_notify(enabled BOOLEAN)
RETURNS BOOLEAN
LANGUAGE plpgsql
AS $$
BEGIN
    IF enabled IS NULL THEN
        RAISE EXCEPTION 'enabled flag cannot be NULL';
    END IF;

    EXECUTE format(
        'CREATE OR REPLACE FUNCTION pgwf.is_notify_enabled() RETURNS BOOLEAN LANGUAGE plpgsql AS %L',
        CASE WHEN enabled THEN 'BEGIN RETURN TRUE; END;' ELSE 'BEGIN RETURN FALSE; END;' END
    );

    RETURN enabled;
END;
$$;

CREATE OR REPLACE FUNCTION pgwf._lock_job_for_status(
    p_tenant_id TEXT,
    p_job_id TEXT,
    p_status TEXT,
    p_expected_lease_id TEXT DEFAULT NULL,
    p_missing_message TEXT DEFAULT NULL
)
RETURNS pgwf.jobs_with_status
LANGUAGE plpgsql
AS $$
DECLARE
    v_row pgwf.jobs_with_status%ROWTYPE;
BEGIN
    IF p_tenant_id IS NULL THEN
        RAISE EXCEPTION 'tenant_id cannot be NULL';
    END IF;
    IF p_job_id IS NULL THEN
        RAISE EXCEPTION 'job_id cannot be NULL';
    END IF;

    SELECT *
    INTO v_row
    FROM pgwf.jobs_with_status
    WHERE tenant_id = p_tenant_id
      AND job_id = p_job_id
      AND (
          status = p_status
          OR (p_status = 'READY' AND status IN ('CRASH_CONCERN', 'EXPIRED'))
      )
      AND (p_expected_lease_id IS NULL OR lease_id = p_expected_lease_id)
    FOR UPDATE;

    IF NOT FOUND THEN
        RAISE EXCEPTION '%', COALESCE(p_missing_message, format('job %s/%s is not in a valid status (%s requested)', p_tenant_id, p_job_id, p_status));
    END IF;

    RETURN v_row;
END;
$$;

CREATE OR REPLACE FUNCTION pgwf._notify_need(
    p_next_need TEXT,
    p_job_id TEXT
)
RETURNS VOID
LANGUAGE plpgsql
AS $$
BEGIN
    IF p_next_need IS NULL OR p_job_id IS NULL THEN
        RETURN;
    END IF;

    IF pgwf.is_notify_enabled() THEN
        PERFORM pg_notify(format('pgwf.need.%s', p_next_need), p_job_id);
    END IF;
END;
$$;

CREATE OR REPLACE FUNCTION pgwf._emit_trace_event(
    p_tenant_id TEXT,
    p_job_id TEXT,
    p_event_type TEXT,
    p_worker_id TEXT,
    p_input JSONB,
    p_output JSONB DEFAULT NULL
)
RETURNS VOID
LANGUAGE plpgsql
AS $$
BEGIN
    IF NOT pgwf.is_trace_enabled() THEN
        RETURN;
    END IF;

    INSERT INTO pgwf.jobs_trace (tenant_id, job_id, event_type, worker_id, input_data, output_data)
    VALUES (
        p_tenant_id,
        p_job_id,
        p_event_type,
        p_worker_id,
        COALESCE(p_input, '{}'::JSONB),
        p_output
    );
END;
$$;

CREATE OR REPLACE FUNCTION pgwf._archive_and_delete_job(
    p_locked_job pgwf.jobs_with_status
)
RETURNS pgwf.jobs_archive
LANGUAGE plpgsql
AS $$
DECLARE
    v_archive pgwf.jobs_archive%ROWTYPE;
BEGIN
    INSERT INTO pgwf.jobs_archive (
        tenant_id,
        job_id,
        next_need,
        wait_for,
        payload,
        singleton_key,
        created_at,
        expires_at,
        lease_id,
        lease_expiration_count,
        consecutive_expirations,
        cancel_requested,
        cancel_requested_by,
        cancel_requested_at
    )
    VALUES (
        p_locked_job.tenant_id,
        p_locked_job.job_id,
        p_locked_job.next_need,
        p_locked_job.wait_for,
        p_locked_job.payload,
        p_locked_job.singleton_key,
        p_locked_job.created_at,
        p_locked_job.expires_at,
        p_locked_job.lease_id,
        p_locked_job.lease_expiration_count,
        p_locked_job.consecutive_expirations,
        p_locked_job.cancel_requested,
        p_locked_job.cancel_requested_by,
        p_locked_job.cancel_requested_at
    )
    RETURNING * INTO v_archive;

    DELETE FROM pgwf.jobs
    WHERE tenant_id = p_locked_job.tenant_id
      AND job_id = p_locked_job.job_id;

    RETURN v_archive;
END;
$$;

CREATE OR REPLACE FUNCTION pgwf._update_waiters_for_completion_bulk(
    p_tenant_id TEXT,
    p_completed_jobs TEXT[]
)
RETURNS TABLE(job_id TEXT, next_need TEXT, became_unblocked BOOLEAN)
LANGUAGE plpgsql
AS $$
DECLARE
    v_row RECORD;
    v_now TIMESTAMPTZ := clock_timestamp();
BEGIN
    IF p_completed_jobs IS NULL OR array_length(p_completed_jobs, 1) IS NULL OR array_length(p_completed_jobs, 1) = 0 THEN
        RETURN;
    END IF;

    FOR v_row IN
        WITH targets AS (
            SELECT j.*
            FROM pgwf.jobs j
            WHERE j.tenant_id = p_tenant_id
              AND EXISTS (
                  SELECT 1
                  FROM unnest(j.wait_for) pending(job_id)
                  WHERE pending.job_id = ANY(p_completed_jobs)
              )
            FOR UPDATE
        ),
        updated AS (
            UPDATE pgwf.jobs j
            SET wait_for = (
                SELECT COALESCE(array_agg(val ORDER BY ord), ARRAY[]::TEXT[])
                FROM unnest(j.wait_for) WITH ORDINALITY AS pending(val, ord)
                WHERE pending.val IS NOT NULL
                  AND NOT (pending.val = ANY(p_completed_jobs))
            )
            FROM targets t
            WHERE j.tenant_id = t.tenant_id
              AND j.job_id = t.job_id
            RETURNING
                j.job_id,
                j.next_need,
                j.available_at,
                (COALESCE(array_length(j.wait_for, 1), 0) = 0) AS now_unblocked,
                j.cancel_requested,
                j.expires_at
        )
        SELECT *
        FROM updated
    LOOP
        IF v_row.now_unblocked
           AND v_row.available_at <= v_now
           AND NOT v_row.cancel_requested
           AND v_row.expires_at > v_now THEN
            PERFORM pgwf._notify_need(v_row.next_need, v_row.job_id);
        END IF;

        job_id := v_row.job_id;
        next_need := v_row.next_need;
        became_unblocked := v_row.now_unblocked;
        RETURN NEXT;
    END LOOP;
END;
$$;

CREATE OR REPLACE FUNCTION pgwf._update_waiters_for_completion(
    p_tenant_id TEXT,
    p_completed_job_id TEXT
)
RETURNS TABLE(job_id TEXT, next_need TEXT, became_unblocked BOOLEAN)
LANGUAGE plpgsql
AS $$
BEGIN
    IF p_completed_job_id IS NULL THEN
        RETURN;
    END IF;

    RETURN QUERY
    SELECT *
    FROM pgwf._update_waiters_for_completion_bulk(p_tenant_id, ARRAY[p_completed_job_id]);
END;
$$;

CREATE OR REPLACE FUNCTION pgwf._complete_locked_job(
    p_locked_job pgwf.jobs_with_status,
    p_worker_id TEXT,
    p_trace_context JSONB DEFAULT '{}'::JSONB
)
RETURNS BOOLEAN
LANGUAGE plpgsql
AS $$
DECLARE
    v_archive pgwf.jobs_archive%ROWTYPE;
BEGIN
    p_locked_job.consecutive_expirations := 0;
    v_archive := pgwf._archive_and_delete_job(p_locked_job);

    PERFORM pgwf._update_waiters_for_completion(p_locked_job.tenant_id, p_locked_job.job_id);

    PERFORM pgwf._emit_trace_event(
        p_locked_job.tenant_id,
        p_locked_job.job_id,
        'job_finished',
        p_worker_id,
        jsonb_build_object(
            'tenant_id', p_locked_job.tenant_id,
            'job_id', p_locked_job.job_id,
            'worker_id', p_worker_id
        ) || COALESCE(p_trace_context, '{}'::JSONB),
        jsonb_build_object('archived_row', to_jsonb(v_archive) - 'payload')
    );

    RETURN TRUE;
END;
$$;

CREATE OR REPLACE FUNCTION pgwf._reschedule_locked_job(
    p_locked_job pgwf.jobs_with_status,
    p_worker_id TEXT,
    p_next_need TEXT,
    p_wait_for TEXT[] DEFAULT '{}'::TEXT[],
    p_available_at TIMESTAMPTZ DEFAULT clock_timestamp(),
    p_payload JSONB DEFAULT NULL,
    p_trace_context JSONB DEFAULT '{}'::JSONB
)
RETURNS TABLE(job_id TEXT, next_need TEXT, wait_for TEXT[], available_at TIMESTAMPTZ)
LANGUAGE plpgsql
AS $$
DECLARE
    v_wait_for TEXT[];
    v_available_at TIMESTAMPTZ := COALESCE(p_available_at, clock_timestamp());
    v_expires_at TIMESTAMPTZ;
    v_now TIMESTAMPTZ := clock_timestamp();
    v_payload JSONB := COALESCE(p_payload, p_locked_job.payload);
BEGIN
    v_wait_for := pgwf.normalize_wait_for(p_locked_job.tenant_id, p_wait_for);

    IF p_payload IS NOT NULL THEN
        IF jsonb_typeof(p_payload) IS DISTINCT FROM 'object' THEN
            RAISE EXCEPTION 'payload must be a JSON object';
        END IF;

        IF pg_column_size(p_payload) > 512 THEN
            RAISE EXCEPTION 'payload exceeds 512 bytes';
        END IF;
    END IF;

    UPDATE pgwf.jobs j
    SET next_need = p_next_need,
        wait_for = v_wait_for,
        available_at = v_available_at,
        payload = v_payload,
        consecutive_expirations = 0,
        lease_id = NULL,
        lease_expires_at = '-infinity'
    WHERE j.tenant_id = p_locked_job.tenant_id
      AND j.job_id = p_locked_job.job_id
    RETURNING j.job_id,
              j.next_need,
              j.wait_for,
              j.available_at,
              j.expires_at
    INTO job_id, next_need, wait_for, available_at, v_expires_at;

    IF NOT p_locked_job.cancel_requested AND v_expires_at > v_now THEN
        PERFORM pgwf._notify_need(next_need, job_id);
    END IF;

    PERFORM pgwf._emit_trace_event(
        p_locked_job.tenant_id,
        job_id,
        'reschedule_job',
        p_worker_id,
        jsonb_build_object(
            'tenant_id', p_locked_job.tenant_id,
            'job_id', p_locked_job.job_id,
            'worker_id', p_worker_id,
            'previous_next_need', p_locked_job.next_need,
            'previous_wait_for', p_locked_job.wait_for,
            'previous_available_at', p_locked_job.available_at,
            'previous_expires_at', p_locked_job.expires_at,
            'next_need', p_next_need,
            'wait_for', v_wait_for,
            'available_at', v_available_at,
            'expires_at', v_expires_at
        ) || COALESCE(p_trace_context, '{}'::JSONB),
        jsonb_build_object(
            'job_id', job_id,
            'next_need', next_need,
            'wait_for', wait_for,
            'available_at', available_at,
            'expires_at', v_expires_at
        )
    );

    RETURN NEXT;
END;
$$;

CREATE OR REPLACE FUNCTION pgwf.normalize_wait_for(p_tenant_id TEXT, p_wait_for TEXT[])
RETURNS TEXT[]
LANGUAGE plpgsql
AS $$
DECLARE
    v_input TEXT[] := COALESCE(p_wait_for, ARRAY[]::TEXT[]);
    v_clean TEXT[];
    v_missing TEXT[];
BEGIN
    IF array_length(v_input, 1) IS NULL THEN
        RETURN ARRAY[]::TEXT[];
    END IF;

    WITH ordered AS (
        SELECT value AS job_id, ord
        FROM unnest(v_input) WITH ORDINALITY AS w(value, ord)
        WHERE value IS NOT NULL
    )
    SELECT
        COALESCE(array_agg(o.job_id ORDER BY ord)
                 FILTER (WHERE j.job_id IS NOT NULL), ARRAY[]::TEXT[]),
        array_agg(o.job_id ORDER BY ord)
            FILTER (WHERE j.job_id IS NULL AND ja.job_id IS NULL)
    INTO v_clean, v_missing
    FROM ordered o
    LEFT JOIN pgwf.jobs j ON j.tenant_id = p_tenant_id AND j.job_id = o.job_id
    LEFT JOIN pgwf.jobs_archive ja ON ja.tenant_id = p_tenant_id AND ja.job_id = o.job_id;

    IF v_missing IS NOT NULL THEN
        RAISE EXCEPTION 'wait_for references unknown jobs in tenant %: %', p_tenant_id, v_missing;
    END IF;

    RETURN v_clean;
END;
$$;

CREATE OR REPLACE FUNCTION pgwf.submit_job(
    p_tenant_id TEXT,
    p_job_id TEXT,
    p_worker_id TEXT,
    p_next_need TEXT,
    p_wait_for TEXT[] DEFAULT '{}'::TEXT[],
    p_payload JSONB DEFAULT '{}'::JSONB,
    p_singleton_key TEXT DEFAULT NULL,
    p_available_at TIMESTAMPTZ DEFAULT clock_timestamp(),
    p_expires_at TIMESTAMPTZ DEFAULT NULL
)
RETURNS TABLE(tenant_id TEXT, job_id TEXT, next_need TEXT, wait_for TEXT[], payload JSONB, available_at TIMESTAMPTZ)
LANGUAGE plpgsql
AS $$
DECLARE
    v_wait_for TEXT[];
    v_effective_available TIMESTAMPTZ := COALESCE(p_available_at, clock_timestamp());
    v_expires_at TIMESTAMPTZ := COALESCE(p_expires_at, 'infinity');
    v_cancel_requested BOOLEAN;
    v_now TIMESTAMPTZ := clock_timestamp();
    v_payload JSONB := COALESCE(p_payload, '{}'::JSONB);
BEGIN
    IF p_tenant_id IS NULL THEN
        RAISE EXCEPTION 'tenant_id cannot be NULL';
    END IF;

    IF EXISTS (SELECT 1 FROM pgwf.jobs_archive ja WHERE ja.tenant_id = p_tenant_id AND ja.job_id = p_job_id) THEN
        RAISE EXCEPTION 'job_id %/% has already completed and cannot be resubmitted', p_tenant_id, p_job_id;
    END IF;

    v_wait_for := pgwf.normalize_wait_for(p_tenant_id, p_wait_for);

    IF jsonb_typeof(v_payload) IS DISTINCT FROM 'object' THEN
        RAISE EXCEPTION 'payload must be a JSON object';
    END IF;

    IF pg_column_size(v_payload) > 512 THEN
        RAISE EXCEPTION 'payload exceeds 512 bytes';
    END IF;

    INSERT INTO pgwf.jobs (tenant_id, job_id, next_need, wait_for, payload, singleton_key, available_at, expires_at)
    VALUES (p_tenant_id, p_job_id, p_next_need, v_wait_for, v_payload, p_singleton_key, v_effective_available, v_expires_at)
    RETURNING pgwf.jobs.tenant_id, pgwf.jobs.job_id, pgwf.jobs.next_need, pgwf.jobs.wait_for, pgwf.jobs.payload, pgwf.jobs.available_at, pgwf.jobs.cancel_requested
    INTO tenant_id, job_id, next_need, wait_for, payload, available_at, v_cancel_requested;

    IF NOT v_cancel_requested AND v_expires_at > v_now THEN
        PERFORM pgwf._notify_need(next_need, job_id);
    END IF;

    PERFORM pgwf._emit_trace_event(
        p_tenant_id,
        p_job_id,
        'job_submitted',
        p_worker_id,
        jsonb_build_object(
            'tenant_id', p_tenant_id,
            'job_id', p_job_id,
            'worker_id', p_worker_id,
            'next_need', p_next_need,
            'wait_for', v_wait_for,
            'singleton_key', p_singleton_key,
            'available_at', v_effective_available,
            'expires_at', v_expires_at
        ),
        jsonb_build_object(
            'tenant_id', tenant_id,
            'job_id', job_id,
            'next_need', next_need,
            'wait_for', wait_for,
            'available_at', available_at,
            'expires_at', v_expires_at
        )
    );

    RETURN NEXT;
END;
$$;

CREATE OR REPLACE FUNCTION pgwf.cancel_job(
    p_tenant_id TEXT,
    p_job_id TEXT,
    p_worker_id TEXT,
    p_reason TEXT DEFAULT NULL
)
RETURNS pgwf.jobs
LANGUAGE plpgsql
AS $$
DECLARE
    v_job pgwf.jobs%ROWTYPE;
    v_now TIMESTAMPTZ := clock_timestamp();
BEGIN
    IF p_tenant_id IS NULL THEN
        RAISE EXCEPTION 'tenant_id cannot be NULL';
    END IF;
    IF p_job_id IS NULL THEN
        RAISE EXCEPTION 'job_id cannot be NULL';
    END IF;
    IF p_worker_id IS NULL THEN
        RAISE EXCEPTION 'worker_id cannot be NULL';
    END IF;

    SELECT *
    INTO v_job
    FROM pgwf.jobs
    WHERE tenant_id = p_tenant_id
      AND job_id = p_job_id
    FOR UPDATE;

    IF NOT FOUND THEN
        IF EXISTS (SELECT 1 FROM pgwf.jobs_archive WHERE tenant_id = p_tenant_id AND job_id = p_job_id) THEN
            RAISE EXCEPTION 'job_id %/% has already completed and cannot be cancelled', p_tenant_id, p_job_id;
        END IF;
        RAISE EXCEPTION 'job_id %/% does not exist', p_tenant_id, p_job_id;
    END IF;

    IF v_job.cancel_requested THEN
        PERFORM pgwf._emit_trace_event(
            p_tenant_id,
            p_job_id,
            'job_cancel_requested',
            p_worker_id,
            jsonb_build_object(
                'tenant_id', p_tenant_id,
                'job_id', p_job_id,
                'worker_id', p_worker_id,
                'reason', p_reason,
                'already_cancelled', TRUE,
                'was_active', v_job.lease_expires_at > v_now
            )
        );
        RETURN v_job;
    END IF;

    UPDATE pgwf.jobs
    SET cancel_requested = TRUE,
        cancel_requested_by = p_worker_id,
        cancel_requested_at = v_now
    WHERE tenant_id = p_tenant_id
      AND job_id = p_job_id
    RETURNING * INTO v_job;

    PERFORM pgwf._emit_trace_event(
        p_tenant_id,
        p_job_id,
        'job_cancel_requested',
        p_worker_id,
        jsonb_build_object(
            'tenant_id', p_tenant_id,
            'job_id', p_job_id,
            'worker_id', p_worker_id,
            'reason', p_reason,
            'was_active', v_job.lease_expires_at > v_now
        )
    );

    RETURN v_job;
END;
$$;

CREATE OR REPLACE FUNCTION pgwf.get_work(
    p_worker_id TEXT,
    p_worker_caps TEXT[],
    p_tenant_ids TEXT[] DEFAULT NULL,
    p_lease_seconds INTEGER DEFAULT 60,
    p_limit_jobs INTEGER DEFAULT 1
)
RETURNS TABLE(
    tenant_id TEXT,
    job_id TEXT,
    lease_id TEXT,
    next_need TEXT,
    singleton_key TEXT,
    wait_for TEXT[],
    payload JSONB,
    available_at TIMESTAMPTZ,
    lease_expires_at TIMESTAMPTZ
)
LANGUAGE plpgsql
AS $$
DECLARE
    v_caps TEXT[] := p_worker_caps;
    v_now TIMESTAMPTZ := clock_timestamp();
    v_expires TIMESTAMPTZ;
    v_count INTEGER := 0;
    v_cap TEXT;
    v_tenant_id TEXT;
    v_previous_lease_id TEXT;
    v_previous_lease_expires_at TIMESTAMPTZ;
    v_previous_lease_expired BOOLEAN;
    v_total_expirations BIGINT;
    v_consecutive_expirations BIGINT;
BEGIN
    IF v_caps IS NULL OR array_length(v_caps, 1) = 0 THEN
        RAISE EXCEPTION 'worker_caps cannot be empty';
    END IF;

    IF p_lease_seconds IS NULL OR p_lease_seconds <= 0 THEN
        RAISE EXCEPTION 'lease_seconds must be positive';
    END IF;

    IF p_limit_jobs IS NULL OR p_limit_jobs <= 0 THEN
        RAISE EXCEPTION 'limit_jobs must be positive';
    END IF;

    v_expires := v_now + make_interval(secs => p_lease_seconds);

    FOR v_tenant_id, job_id, lease_id, next_need, singleton_key, wait_for, payload, available_at, lease_expires_at,
        v_previous_lease_id, v_previous_lease_expires_at, v_previous_lease_expired,
        v_total_expirations, v_consecutive_expirations IN
        WITH candidates AS (
            SELECT jws.*,
                   (jws.lease_id IS NOT NULL AND jws.lease_expires_at <= v_now) AS lease_was_expired
            FROM pgwf.jobs_with_status jws
            WHERE jws.status = 'READY'
              AND jws.next_need = ANY(v_caps)
              AND (p_tenant_ids IS NULL OR array_length(p_tenant_ids, 1) IS NULL OR jws.tenant_id = ANY(p_tenant_ids))
              AND (
                  jws.singleton_key IS NULL OR NOT EXISTS (
                      SELECT 1
                      FROM pgwf.jobs_with_status other
                      WHERE other.tenant_id = jws.tenant_id
                        AND other.singleton_key = jws.singleton_key
                        AND other.status = 'ACTIVE'
                  )
              )
            ORDER BY jws.created_at ASC
            LIMIT p_limit_jobs
            FOR UPDATE SKIP LOCKED
        )
        UPDATE pgwf.jobs j
        SET
            lease_expiration_count = CASE
                WHEN c.lease_was_expired THEN j.lease_expiration_count + 1
                ELSE j.lease_expiration_count
            END,
            consecutive_expirations = CASE
                WHEN c.lease_was_expired THEN j.consecutive_expirations + 1
                ELSE j.consecutive_expirations
            END,
            lease_id = gen_random_uuid()::TEXT,
            lease_expires_at = v_expires
        FROM candidates c
        WHERE j.tenant_id = c.tenant_id
          AND j.job_id = c.job_id
        RETURNING j.tenant_id,
                  j.job_id,
                  j.lease_id,
                  j.next_need,
                  j.singleton_key,
                  j.wait_for,
                  j.payload,
                  j.available_at,
                  j.lease_expires_at,
                  c.lease_id AS previous_lease_id,
                  c.lease_expires_at AS previous_lease_expires_at,
                  c.lease_was_expired AS lease_previously_expired,
                  j.lease_expiration_count,
                  j.consecutive_expirations
    LOOP

        IF v_previous_lease_expired THEN
            PERFORM pgwf._emit_trace_event(
                v_tenant_id,
                job_id,
                'lease_expiration_counter_incremented',
                p_worker_id,
                jsonb_build_object(
                    'tenant_id', v_tenant_id,
                    'worker_id', p_worker_id,
                    'worker_caps', v_caps,
                    'previous_lease_id', v_previous_lease_id,
                    'previous_lease_expires_at', v_previous_lease_expires_at,
                    'lease_expiration_count', v_total_expirations,
                    'consecutive_expirations', v_consecutive_expirations
                )
            );
        END IF;

        v_count := v_count + 1;

        PERFORM pgwf._emit_trace_event(
            v_tenant_id,
            job_id,
            'job_retrieved',
            p_worker_id,
            jsonb_build_object(
                'tenant_id', v_tenant_id,
                'worker_id', p_worker_id,
                'worker_caps', v_caps,
                'tenant_ids', p_tenant_ids,
                'lease_seconds', p_lease_seconds,
                'limit_jobs', p_limit_jobs
            ),
            jsonb_build_object(
                'lease_id', lease_id,
                'lease_expires_at', lease_expires_at
            )
        );

        tenant_id := v_tenant_id;
        RETURN NEXT;
    END LOOP;

    IF v_count = 0 AND pgwf.is_notify_enabled() THEN
        FOREACH v_cap IN ARRAY v_caps LOOP
            EXIT WHEN v_cap IS NULL;
            EXECUTE 'LISTEN ' || quote_ident('pgwf.need.' || v_cap);
        END LOOP;
    END IF;
END;
$$;

CREATE OR REPLACE FUNCTION pgwf.extend_lease(
    p_tenant_id TEXT,
    p_job_id TEXT,
    p_lease_id TEXT,
    p_worker_id TEXT,
    p_additional_seconds INTEGER DEFAULT 60
)
RETURNS TIMESTAMPTZ
LANGUAGE plpgsql
AS $$
DECLARE
    v_job pgwf.jobs_with_status%ROWTYPE;
    v_new TIMESTAMPTZ;
BEGIN
    IF p_additional_seconds IS NULL OR p_additional_seconds <= 0 THEN
        RAISE EXCEPTION 'additional_seconds must be positive';
    END IF;

    SELECT *
    INTO v_job
    FROM pgwf._lock_job_for_status(
        p_tenant_id,
        p_job_id,
        'ACTIVE',
        p_lease_id,
        format('active lease not found for job %s/%s', p_tenant_id, p_job_id)
    );

    IF v_job.cancel_requested THEN
        RAISE EXCEPTION 'job %s/%s is cancelled and cannot extend the lease', p_tenant_id, p_job_id;
    END IF;

    v_new := clock_timestamp() + make_interval(secs => p_additional_seconds);

    UPDATE pgwf.jobs
    SET lease_expires_at = v_new
    WHERE tenant_id = v_job.tenant_id
      AND job_id = v_job.job_id
      AND lease_id = v_job.lease_id;

    PERFORM pgwf._emit_trace_event(
        p_tenant_id,
        p_job_id,
        'lease_extended',
        p_worker_id,
        jsonb_build_object(
            'tenant_id', p_tenant_id,
            'job_id', p_job_id,
            'lease_id', p_lease_id,
            'additional_seconds', p_additional_seconds
        ),
        jsonb_build_object(
            'previous_expires_at', v_job.lease_expires_at,
            'new_expires_at', v_new
        )
    );

    RETURN v_new;
END;
$$;

CREATE OR REPLACE FUNCTION pgwf.reschedule_job(
    p_tenant_id TEXT,
    p_job_id TEXT,
    p_lease_id TEXT,
    p_worker_id TEXT,
    p_next_need TEXT,
    p_wait_for TEXT[] DEFAULT '{}'::TEXT[],
    p_available_at TIMESTAMPTZ DEFAULT clock_timestamp(),
    p_payload JSONB DEFAULT NULL
)
RETURNS TABLE(job_id TEXT, next_need TEXT, wait_for TEXT[], available_at TIMESTAMPTZ)
LANGUAGE plpgsql
AS $$
DECLARE
    v_job pgwf.jobs_with_status%ROWTYPE;
BEGIN
    SELECT * INTO v_job
    FROM pgwf._lock_job_for_status(
        p_tenant_id,
        p_job_id,
        'ACTIVE',
        p_lease_id,
        format('job %s/%s is not currently leased with lease %s', p_tenant_id, p_job_id, p_lease_id)
    );

    IF v_job.cancel_requested THEN
        RAISE EXCEPTION 'job %s/%s is cancelled and cannot be rescheduled', p_tenant_id, p_job_id;
    END IF;

    RETURN QUERY
    SELECT *
    FROM pgwf._reschedule_locked_job(
        v_job,
        p_worker_id,
        p_next_need,
        p_wait_for,
        p_available_at,
        p_payload,
        jsonb_build_object('lease_id', p_lease_id)
    );
END;
$$;

CREATE OR REPLACE FUNCTION pgwf.complete_job(
    p_tenant_id TEXT,
    p_job_id TEXT,
    p_lease_id TEXT,
    p_worker_id TEXT
)
RETURNS BOOLEAN
LANGUAGE plpgsql
AS $$
DECLARE
    v_job pgwf.jobs_with_status%ROWTYPE;
BEGIN
    v_job := pgwf._lock_job_for_status(
        p_tenant_id,
        p_job_id,
        'ACTIVE',
        p_lease_id,
        format('job %s/%s is not actively leased by %s', p_tenant_id, p_job_id, p_lease_id)
    );

    RETURN pgwf._complete_locked_job(
        v_job,
        p_worker_id,
        jsonb_build_object('lease_id', p_lease_id)
    );
END;
$$;

CREATE OR REPLACE FUNCTION pgwf.complete_unheld_job(
    p_tenant_id TEXT,
    p_job_id TEXT,
    p_worker_id TEXT
)
RETURNS BOOLEAN
LANGUAGE plpgsql
AS $$
DECLARE
    v_job pgwf.jobs_with_status%ROWTYPE;
BEGIN
    SELECT *
    INTO v_job
    FROM pgwf._lock_job_for_status(
        p_tenant_id,
        p_job_id,
        'READY',
        NULL,
        format('job %s/%s is not available to complete', p_tenant_id, p_job_id)
    );

    RETURN pgwf._complete_locked_job(
        v_job,
        p_worker_id,
        jsonb_build_object('completed_without_lease', TRUE)
    );
END;
$$;

CREATE OR REPLACE FUNCTION pgwf.clear_crash_concern(
    p_tenant_id TEXT,
    p_job_id TEXT,
    p_worker_id TEXT,
    p_reason TEXT DEFAULT NULL
)
RETURNS BOOLEAN
LANGUAGE plpgsql
AS $$
DECLARE
    v_job pgwf.jobs%ROWTYPE;
    v_previous_consecutive BIGINT;
BEGIN
    IF p_tenant_id IS NULL THEN
        RAISE EXCEPTION 'tenant_id cannot be NULL';
    END IF;
    IF p_job_id IS NULL THEN
        RAISE EXCEPTION 'job_id cannot be NULL';
    END IF;
    IF p_worker_id IS NULL THEN
        RAISE EXCEPTION 'worker_id cannot be NULL';
    END IF;

    SELECT *
    INTO v_job
    FROM pgwf.jobs
    WHERE tenant_id = p_tenant_id
      AND job_id = p_job_id
    FOR UPDATE;

    IF NOT FOUND THEN
        IF EXISTS (SELECT 1 FROM pgwf.jobs_archive WHERE tenant_id = p_tenant_id AND job_id = p_job_id) THEN
            RAISE EXCEPTION 'job_id %/% has already been archived and cannot clear crash concern', p_tenant_id, p_job_id;
        END IF;
        RAISE EXCEPTION 'job_id %/% does not exist', p_tenant_id, p_job_id;
    END IF;

    IF v_job.cancel_requested THEN
        RAISE EXCEPTION 'job %/%  is cancelled and cannot clear crash concern', p_tenant_id, p_job_id;
    END IF;

    v_previous_consecutive := v_job.consecutive_expirations;

    UPDATE pgwf.jobs
    SET consecutive_expirations = 0
    WHERE tenant_id = v_job.tenant_id
      AND job_id = v_job.job_id;

    PERFORM pgwf._emit_trace_event(
        p_tenant_id,
        p_job_id,
        'crash_concern_cleared',
        p_worker_id,
        jsonb_build_object(
            'tenant_id', p_tenant_id,
            'job_id', p_job_id,
            'worker_id', p_worker_id,
            'previous_consecutive_expirations', v_previous_consecutive,
            'reason', p_reason
        ),
        jsonb_build_object('consecutive_expirations', 0)
    );

    RETURN TRUE;
END;
$$;

CREATE OR REPLACE FUNCTION pgwf.archive_cancelled_jobs(
    p_worker_id TEXT,
    p_tenant_ids TEXT[] DEFAULT NULL,
    p_limit INTEGER DEFAULT 100
)
RETURNS INTEGER
LANGUAGE plpgsql
AS $$
DECLARE
    v_archived_count INTEGER := 0;
    v_trace_enabled BOOLEAN := pgwf.is_trace_enabled();
    v_effective_limit INTEGER := COALESCE(p_limit, 0);
    v_tenant_id TEXT;
    v_job_ids TEXT[];
BEGIN
    IF p_worker_id IS NULL THEN
        RAISE EXCEPTION 'worker_id cannot be NULL';
    END IF;
    IF v_effective_limit <= 0 THEN
        RAISE EXCEPTION 'limit must be positive';
    END IF;

    WITH candidates AS (
        SELECT tenant_id, job_id
        FROM pgwf.jobs
        WHERE cancel_requested
          AND lease_expires_at <= clock_timestamp()
          AND (p_tenant_ids IS NULL OR array_length(p_tenant_ids, 1) IS NULL OR tenant_id = ANY(p_tenant_ids))
        ORDER BY created_at
        LIMIT v_effective_limit
        FOR UPDATE SKIP LOCKED
    ),
    archived AS (
        INSERT INTO pgwf.jobs_archive (
            tenant_id,
            job_id,
            next_need,
            wait_for,
            payload,
            singleton_key,
            created_at,
            expires_at,
            lease_id,
            lease_expiration_count,
            consecutive_expirations,
            cancel_requested,
            cancel_requested_by,
            cancel_requested_at
        )
        SELECT
            j.tenant_id,
            j.job_id,
            j.next_need,
            j.wait_for,
            j.payload,
            j.singleton_key,
            j.created_at,
            j.expires_at,
            j.lease_id,
            j.lease_expiration_count,
            j.consecutive_expirations,
            j.cancel_requested,
            j.cancel_requested_by,
            j.cancel_requested_at
        FROM pgwf.jobs j
        INNER JOIN candidates c ON j.tenant_id = c.tenant_id AND j.job_id = c.job_id
        RETURNING *
    ),
    deleted AS (
        DELETE FROM pgwf.jobs j
        USING archived a
        WHERE j.tenant_id = a.tenant_id
          AND j.job_id = a.job_id
        RETURNING j.tenant_id, j.job_id
    ),
    per_job_trace AS (
        INSERT INTO pgwf.jobs_trace (tenant_id, job_id, event_type, worker_id, input_data, output_data)
        SELECT
            a.tenant_id,
            a.job_id,
            'job_cancel_archived',
            p_worker_id,
            jsonb_build_object(
                'tenant_id', a.tenant_id,
                'job_id', a.job_id,
                'worker_id', p_worker_id,
                'cancel_requested_at', a.cancel_requested_at
            ),
            jsonb_build_object('archived_row', to_jsonb(a) - 'payload')
        FROM archived a
        WHERE v_trace_enabled
    ),
    tenant_groups AS (
        SELECT d.tenant_id, array_agg(d.job_id) AS job_ids
        FROM deleted d
        GROUP BY d.tenant_id
    )
    SELECT COUNT(*)
    INTO v_archived_count
    FROM deleted;

    IF v_archived_count = 0 THEN
        RETURN 0;
    END IF;

    -- Process each tenant's completed jobs to update waiters
    -- Use a recursive approach: iterate through recently archived cancelled jobs
    FOR v_tenant_id, v_job_ids IN
        WITH recent_archived AS (
            SELECT tenant_id, job_id
            FROM pgwf.jobs_archive
            WHERE cancel_requested
              AND NOT EXISTS (
                  SELECT 1 FROM pgwf.jobs j
                  WHERE j.tenant_id = jobs_archive.tenant_id
                    AND j.job_id = jobs_archive.job_id
              )
            ORDER BY created_at DESC
            LIMIT v_effective_limit
        )
        SELECT tenant_id, array_agg(job_id) AS job_ids
        FROM recent_archived
        GROUP BY tenant_id
    LOOP
        PERFORM pgwf._update_waiters_for_completion_bulk(v_tenant_id, v_job_ids);
    END LOOP;

    IF v_trace_enabled THEN
        PERFORM pgwf._emit_trace_event(
            'pgwf',
            'archive_cancelled_jobs',
            'job_cancel_archived_run',
            p_worker_id,
            jsonb_build_object(
                'worker_id', p_worker_id,
                'tenant_ids', p_tenant_ids,
                'limit', v_effective_limit,
                'archived_jobs', v_archived_count
            )
        );
    END IF;

    RETURN v_archived_count;
END;
$$;

CREATE OR REPLACE FUNCTION pgwf.reschedule_unheld_job(
    p_tenant_id TEXT,
    p_job_id TEXT,
    p_worker_id TEXT,
    p_next_need TEXT,
    p_wait_for TEXT[] DEFAULT '{}'::TEXT[],
    p_available_at TIMESTAMPTZ DEFAULT clock_timestamp(),
    p_payload JSONB DEFAULT NULL
)
RETURNS TABLE(job_id TEXT, next_need TEXT, wait_for TEXT[], available_at TIMESTAMPTZ)
LANGUAGE plpgsql
AS $$
DECLARE
    v_job pgwf.jobs_with_status%ROWTYPE;
BEGIN
    SELECT *
    INTO v_job
    FROM pgwf._lock_job_for_status(
        p_tenant_id,
        p_job_id,
        'READY',
        NULL,
        format('job %s/%s is not available to reschedule', p_tenant_id, p_job_id)
    );

    IF v_job.cancel_requested THEN
        RAISE EXCEPTION 'job %s/%s is cancelled and cannot be rescheduled', p_tenant_id, p_job_id;
    END IF;

    RETURN QUERY
    SELECT *
    FROM pgwf._reschedule_locked_job(
        v_job,
        p_worker_id,
        p_next_need,
        p_wait_for,
        p_available_at,
        p_payload,
        jsonb_build_object('rescheduled_without_lease', TRUE)
    );
END;
$$;

COMMIT;
