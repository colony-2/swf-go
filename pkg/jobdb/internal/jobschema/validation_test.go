package jobschema

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
)

func TestValidatorCacheIsKeyedBySchemaHash(t *testing.T) {
	oldValidator := defaultValidator
	defaultValidator = &validatorCache{schemas: make(map[string]*compiledJobSchema)}
	defer func() {
		defaultValidator = oldValidator
	}()

	rawSchema := json.RawMessage(`{"chapterShape":{"type":"object","required":["body"]}}`)
	hash, canonical, err := jobdb.JobSchemaHash(rawSchema)
	if err != nil {
		t.Fatalf("compute schema hash: %v", err)
	}
	registry := &countingRegistry{schemaHash: hash, schema: canonical}
	chapter := jobdb.Chapter{
		Ordinal:   0,
		TaskType:  "schema-cache-test",
		CreatedAt: time.Now().UTC(),
		Body: jobdb.JobStartChapter{Input: jobdb.ApplicationInputBytes{
			Data: []byte(`{"kind":"schema-cache-test"}`),
		}},
	}

	if err := ValidateChapter(context.Background(), registry, jobdb.JobSchemaKey{
		TenantId:   "tenant-a",
		SchemaHash: hash,
	}, ChapterRoleDefault, chapter); err != nil {
		t.Fatalf("validate tenant-a: %v", err)
	}
	if err := ValidateChapter(context.Background(), registry, jobdb.JobSchemaKey{
		TenantId:   "tenant-b",
		SchemaHash: hash,
	}, ChapterRoleDefault, chapter); err != nil {
		t.Fatalf("validate tenant-b: %v", err)
	}
	if registry.gets != 1 {
		t.Fatalf("schema registry gets = %d, want 1", registry.gets)
	}
}

func TestParseJobSchemaDocumentRequiresEnvelope(t *testing.T) {
	if _, err := ParseJobSchemaDocument(json.RawMessage(`{"type":"object"}`)); err == nil {
		t.Fatal("expected raw JSON Schema to be rejected")
	}
	if _, err := ParseJobSchemaDocument(json.RawMessage(`{"chapterShape":true,"extra":true}`)); err == nil {
		t.Fatal("expected unknown top-level field to be rejected")
	}
	if _, err := ParseJobSchemaDocument(json.RawMessage(`{"chapterShape":"bad"}`)); err == nil {
		t.Fatal("expected non-schema shape value to be rejected")
	}
	if _, err := ParseJobSchemaDocument(json.RawMessage(`{"description":{"text":"bad"},"chapterShape":true}`)); err == nil {
		t.Fatal("expected non-string description to be rejected")
	}
}

func TestStructuredSchemaRoles(t *testing.T) {
	schema := json.RawMessage(`{
		"chapterShape":{"type":"object","properties":{"body":{"type":"object","properties":{"kind":{"const":"taskAttemptOutcome"}}}}},
		"firstChapterShape":{"type":"object","properties":{"body":{"type":"object","properties":{"kind":{"const":"jobStart"}}}}},
		"lastChapterShape":{"type":"object","properties":{"body":{"type":"object","properties":{"kind":{"const":"jobAttemptOutcome"}}}}}
	}`)
	hash, canonical, err := jobdb.JobSchemaHash(schema)
	if err != nil {
		t.Fatalf("compute schema hash: %v", err)
	}
	registry := &countingRegistry{schemaHash: hash, schema: canonical}
	first := jobdb.Chapter{
		Ordinal:   0,
		CreatedAt: time.Now().UTC(),
		Body: jobdb.JobStartChapter{Input: jobdb.ApplicationInputBytes{
			Data: []byte(`{"ok":true}`),
		}},
	}
	ordinary := jobdb.Chapter{
		Ordinal:   1,
		CreatedAt: time.Now().UTC(),
		Body: jobdb.TaskAttemptOutcomeChapter{Outcome: jobdb.ApplicationOutputOutcome{
			Output: jobdb.ApplicationOutputBytes{Data: []byte(`{"ok":true}`)},
		}},
	}
	last := jobdb.Chapter{
		Ordinal:   2,
		CreatedAt: time.Now().UTC(),
		Body: jobdb.JobAttemptOutcomeChapter{Outcome: jobdb.ApplicationOutputOutcome{
			Output: jobdb.ApplicationOutputBytes{Data: []byte(`{"ok":true}`)},
		}},
	}
	key := jobdb.JobSchemaKey{TenantId: "tenant", SchemaHash: hash}
	if err := ValidateFirstChapter(context.Background(), registry, key, first); err != nil {
		t.Fatalf("validate first: %v", err)
	}
	if err := ValidateOrdinaryChapter(context.Background(), registry, key, ordinary); err != nil {
		t.Fatalf("validate ordinary: %v", err)
	}
	if err := ValidateLastChapter(context.Background(), registry, key, last); err != nil {
		t.Fatalf("validate last: %v", err)
	}
	if err := ValidateOrdinaryChapter(context.Background(), registry, key, last); err == nil {
		t.Fatal("expected last chapter to fail ordinary validation")
	}
}

type countingRegistry struct {
	schemaHash string
	schema     json.RawMessage
	gets       int
}

func (r *countingRegistry) RegisterJobSchema(context.Context, jobdb.RegisterJobSchemaRequest) (jobdb.JobSchemaInfo, error) {
	panic("unexpected RegisterJobSchema")
}

func (r *countingRegistry) GetJobSchema(_ context.Context, key jobdb.JobSchemaKey) (jobdb.JobSchemaInfo, error) {
	r.gets++
	return jobdb.JobSchemaInfo{
		TenantId:   key.TenantId,
		SchemaHash: r.schemaHash,
		Schema:     append(json.RawMessage(nil), r.schema...),
		State:      jobdb.JobSchemaStateActive,
		CreatedAt:  time.Now().UTC(),
	}, nil
}

func (r *countingRegistry) ListJobSchemas(context.Context, jobdb.ListJobSchemasRequest) (jobdb.ListJobSchemasResponse, error) {
	panic("unexpected ListJobSchemas")
}

func (r *countingRegistry) ArchiveJobSchema(context.Context, jobdb.JobSchemaKey) (jobdb.JobSchemaInfo, error) {
	panic("unexpected ArchiveJobSchema")
}
