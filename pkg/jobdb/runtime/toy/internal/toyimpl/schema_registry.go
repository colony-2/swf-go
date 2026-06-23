package toyimpl

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/jobschema"
)

var _ jobdb.JobSchemaRegistry = (*Runtime)(nil)

func (r *Runtime) RegisterJobSchema(ctx context.Context, req jobdb.RegisterJobSchemaRequest) (jobdb.JobSchemaInfo, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
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
	now := time.Now().UTC()

	r.engine.mu.Lock()
	defer r.engine.mu.Unlock()
	if existing := r.engine.schemas[key]; existing != nil {
		if !bytes.Equal(existing.info.Schema, canonical) {
			return jobdb.JobSchemaInfo{}, jobdb.ErrConflict
		}
		return cloneSchemaInfo(existing.info), nil
	}
	info := jobdb.JobSchemaInfo{
		TenantId:   req.TenantId,
		SchemaHash: hash,
		Schema:     cloneJSON(canonical),
		State:      jobdb.JobSchemaStateActive,
		CreatedAt:  now,
	}
	r.engine.schemas[key] = &toySchemaRecord{info: info}
	return cloneSchemaInfo(info), nil
}

func (r *Runtime) GetJobSchema(ctx context.Context, key jobdb.JobSchemaKey) (jobdb.JobSchemaInfo, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return jobdb.JobSchemaInfo{}, err
	}
	if err := key.Validate(); err != nil {
		return jobdb.JobSchemaInfo{}, err
	}
	r.engine.mu.Lock()
	defer r.engine.mu.Unlock()
	record := r.engine.schemas[key]
	if record == nil {
		return jobdb.JobSchemaInfo{}, jobdb.ErrJobSchemaNotFound
	}
	return cloneSchemaInfo(record.info), nil
}

func (r *Runtime) ListJobSchemas(ctx context.Context, req jobdb.ListJobSchemasRequest) (jobdb.ListJobSchemasResponse, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return jobdb.ListJobSchemasResponse{}, err
	}
	if req.TenantId == "" {
		return jobdb.ListJobSchemasResponse{}, fmt.Errorf("tenantId is required")
	}
	state := req.State
	if state == "" {
		state = jobdb.JobSchemaListStateActive
	}
	switch state {
	case jobdb.JobSchemaListStateActive, jobdb.JobSchemaListStateArchived, jobdb.JobSchemaListStateAll:
	default:
		return jobdb.ListJobSchemasResponse{}, fmt.Errorf("unknown schema state %q", req.State)
	}

	r.engine.mu.Lock()
	defer r.engine.mu.Unlock()
	out := make([]jobdb.JobSchemaInfo, 0)
	for key, record := range r.engine.schemas {
		if key.TenantId != req.TenantId {
			continue
		}
		if !schemaListStateMatches(record.info.State, state) {
			continue
		}
		out = append(out, cloneSchemaInfo(record.info))
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].CreatedAt.Equal(out[j].CreatedAt) {
			return out[i].SchemaHash < out[j].SchemaHash
		}
		return out[i].CreatedAt.After(out[j].CreatedAt)
	})
	return jobdb.ListJobSchemasResponse{Schemas: out}, nil
}

func (r *Runtime) ArchiveJobSchema(ctx context.Context, key jobdb.JobSchemaKey) (jobdb.JobSchemaInfo, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return jobdb.JobSchemaInfo{}, err
	}
	if err := key.Validate(); err != nil {
		return jobdb.JobSchemaInfo{}, err
	}
	now := time.Now().UTC()
	r.engine.mu.Lock()
	defer r.engine.mu.Unlock()
	record := r.engine.schemas[key]
	if record == nil {
		return jobdb.JobSchemaInfo{}, jobdb.ErrJobSchemaNotFound
	}
	if record.info.ArchivedAt == nil {
		record.info.ArchivedAt = &now
	}
	record.info.State = jobdb.JobSchemaStateArchived
	return cloneSchemaInfo(record.info), nil
}

func schemaListStateMatches(state jobdb.JobSchemaState, filter jobdb.JobSchemaListState) bool {
	switch filter {
	case jobdb.JobSchemaListStateAll:
		return true
	case jobdb.JobSchemaListStateArchived:
		return state == jobdb.JobSchemaStateArchived
	default:
		return state == jobdb.JobSchemaStateActive
	}
}

func cloneSchemaInfo(info jobdb.JobSchemaInfo) jobdb.JobSchemaInfo {
	info.Schema = cloneJSON(info.Schema)
	if info.ArchivedAt != nil {
		archivedAt := info.ArchivedAt.UTC()
		info.ArchivedAt = &archivedAt
	}
	info.CreatedAt = info.CreatedAt.UTC()
	return info
}
