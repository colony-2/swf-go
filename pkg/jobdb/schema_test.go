package jobdb

import (
	"encoding/json"
	"testing"
)

func TestResolveJobSchemaSelectorInline(t *testing.T) {
	hash, schema, hasInline, err := ResolveJobSchemaSelector(&JobSchemaSelector{
		Schema: json.RawMessage(`{"chapterShape":{"type":"object","properties":{"ordinal":{"type":"integer"}}}}`),
	})
	if err != nil {
		t.Fatalf("resolve selector: %v", err)
	}
	if hash == "" {
		t.Fatal("hash is required")
	}
	if err := ValidateJobSchemaHash(hash); err != nil {
		t.Fatalf("hash format: %v", err)
	}
	if !hasInline {
		t.Fatal("hasInline = false, want true")
	}
	if len(schema) == 0 {
		t.Fatal("canonical schema is required")
	}
}

func TestResolveJobSchemaSelectorHashMismatch(t *testing.T) {
	_, _, _, err := ResolveJobSchemaSelector(&JobSchemaSelector{
		Hash:   "sha256:0000000000000000000000000000000000000000000000000000000000000000",
		Schema: json.RawMessage(`{"chapterShape":{"type":"object"}}`),
	})
	if err == nil {
		t.Fatal("expected hash mismatch error")
	}
}

func TestBuildJobMetadataEnvelopeWithSchemaHash(t *testing.T) {
	raw, err := BuildJobMetadataEnvelope(json.RawMessage(`{"queue":"blue"}`), RuntimeJobMetadata{
		SchemaHash: "sha256:1111111111111111111111111111111111111111111111111111111111111111",
	})
	if err != nil {
		t.Fatalf("build envelope: %v", err)
	}
	var envelope JobMetadataEnvelope
	if err := json.Unmarshal(raw, &envelope); err != nil {
		t.Fatalf("unmarshal envelope: %v", err)
	}
	if envelope.Internal == nil || envelope.Internal.SchemaHash == "" {
		t.Fatalf("internal schema hash missing: %s", string(raw))
	}
	app := AppMetadataFromStoredMetadata(raw)
	if string(app) != `{"queue":"blue"}` {
		t.Fatalf("app metadata = %s, want queue metadata", string(app))
	}
}
