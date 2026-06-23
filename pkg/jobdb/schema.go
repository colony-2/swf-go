package jobdb

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"regexp"
	"strings"
	"time"
)

var schemaHashPattern = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)

type JobSchemaSelector struct {
	Hash   string
	Schema json.RawMessage
}

type JobSchemaKey struct {
	TenantId   string
	SchemaHash string
}

func (k JobSchemaKey) Validate() error {
	if strings.TrimSpace(k.TenantId) == "" {
		return fmt.Errorf("tenantId is required")
	}
	return ValidateJobSchemaHash(k.SchemaHash)
}

type JobSchemaState string

const (
	JobSchemaStateActive   JobSchemaState = "ACTIVE"
	JobSchemaStateArchived JobSchemaState = "ARCHIVED"
)

type JobSchemaInfo struct {
	TenantId   string
	SchemaHash string
	Schema     json.RawMessage
	State      JobSchemaState
	CreatedAt  time.Time
	ArchivedAt *time.Time
}

type RegisterJobSchemaRequest struct {
	TenantId string
	Schema   json.RawMessage
}

type ListJobSchemasRequest struct {
	TenantId string
	State    JobSchemaListState
}

type JobSchemaListState string

const (
	JobSchemaListStateActive   JobSchemaListState = "ACTIVE"
	JobSchemaListStateArchived JobSchemaListState = "ARCHIVED"
	JobSchemaListStateAll      JobSchemaListState = "ALL"
)

type ListJobSchemasResponse struct {
	Schemas []JobSchemaInfo
}

type JobSchemaRegistry interface {
	RegisterJobSchema(ctx context.Context, req RegisterJobSchemaRequest) (JobSchemaInfo, error)
	GetJobSchema(ctx context.Context, key JobSchemaKey) (JobSchemaInfo, error)
	ListJobSchemas(ctx context.Context, req ListJobSchemasRequest) (ListJobSchemasResponse, error)
	ArchiveJobSchema(ctx context.Context, key JobSchemaKey) (JobSchemaInfo, error)
}

func ValidateJobSchemaHash(hash string) error {
	if !schemaHashPattern.MatchString(hash) {
		return fmt.Errorf("schemaHash must match %s", schemaHashPattern.String())
	}
	return nil
}

func CanonicalJobSchema(schema json.RawMessage) (json.RawMessage, error) {
	if len(strings.TrimSpace(string(schema))) == 0 {
		return nil, fmt.Errorf("schema is required")
	}
	var decoded any
	dec := json.NewDecoder(strings.NewReader(string(schema)))
	dec.UseNumber()
	if err := dec.Decode(&decoded); err != nil {
		return nil, fmt.Errorf("schema must be valid JSON: %w", err)
	}
	var trailing any
	if err := dec.Decode(&trailing); err == nil {
		return nil, fmt.Errorf("schema must contain exactly one JSON value")
	} else if !errors.Is(err, io.EOF) {
		return nil, fmt.Errorf("schema must be valid JSON: %w", err)
	}
	obj, ok := decoded.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("schema must be a JSON object")
	}
	raw, err := json.Marshal(obj)
	if err != nil {
		return nil, fmt.Errorf("canonicalize schema: %w", err)
	}
	return raw, nil
}

func JobSchemaHash(schema json.RawMessage) (string, json.RawMessage, error) {
	canonical, err := CanonicalJobSchema(schema)
	if err != nil {
		return "", nil, err
	}
	sum := sha256.Sum256(canonical)
	return "sha256:" + hex.EncodeToString(sum[:]), canonical, nil
}

func ResolveJobSchemaSelector(selector *JobSchemaSelector) (hash string, schema json.RawMessage, hasInline bool, err error) {
	if selector == nil {
		return "", nil, false, nil
	}
	hash = strings.TrimSpace(selector.Hash)
	if hash != "" {
		if err := ValidateJobSchemaHash(hash); err != nil {
			return "", nil, false, err
		}
	}
	if len(strings.TrimSpace(string(selector.Schema))) == 0 {
		return hash, nil, false, nil
	}
	computed, canonical, err := JobSchemaHash(selector.Schema)
	if err != nil {
		return "", nil, false, err
	}
	if hash != "" && computed != hash {
		return "", nil, false, fmt.Errorf("schema hash %s does not match inline schema hash %s", hash, computed)
	}
	return computed, canonical, true, nil
}
