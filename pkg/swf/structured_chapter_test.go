package swf

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestStructuredChapterLegacyConversionRoundTrip(t *testing.T) {
	createdAt := time.Date(2026, 6, 10, 1, 2, 3, 0, time.UTC)
	inputRef := &InputReference{Ordinal: 6, Hash: "prev"}
	chapter := StructuredChapterRecord{
		Ordinal:   7,
		TaskType:  "task",
		InputHash: "input-hash",
		CreatedAt: createdAt,
		Metadata: ChapterMetadata{Fields: map[string]ChapterMetadataValue{
			"attempt": {Kind: ChapterMetadataInt, Int: 3},
			"flags": {
				Kind: ChapterMetadataList,
				List: []ChapterMetadataValue{
					{Kind: ChapterMetadataString, String: "a"},
					{Kind: ChapterMetadataNull},
				},
			},
			"nested": {
				Kind: ChapterMetadataMap,
				Map: map[string]ChapterMetadataValue{
					"ok": {Kind: ChapterMetadataBool, Bool: true},
				},
			},
		}},
		Body: TaskAttemptOutcomeChapter{Outcome: AppErrorOutcome{Error: AppErrorPayload{
			Message:    "boom",
			Level:      "error",
			Attrs:      map[string]any{"category": "user"},
			InputRef:   inputRef,
			Stacktrace: []string{"top"},
		}}},
		Artifacts: []StoredArtifact{{Name: "err.log", Digest: "sha256:abc", Size: 12}},
	}

	stored, err := StoredChapterFromStructured(chapter)
	if err != nil {
		t.Fatalf("convert to stored: %v", err)
	}
	if stored.ChapterType != chapterTypeTaskAttemptOutcome || stored.PayloadKind != payloadKindAppError {
		t.Fatalf("unexpected discriminators: %s/%s", stored.ChapterType, stored.PayloadKind)
	}
	assertRawJSONEqual(t, stored.Data, json.RawMessage(`{"message":"boom","level":"error","attrs":{"category":"user"},"input_ref":{"ordinal":6,"hash":"prev"},"stacktrace":["top"]}`))
	assertRawJSONEqual(t, stored.Metadata, json.RawMessage(`{"attempt":3,"flags":["a",null],"nested":{"ok":true}}`))

	got, err := StructuredChapterFromStored(stored)
	if err != nil {
		t.Fatalf("convert from stored: %v", err)
	}
	if got.Ordinal != chapter.Ordinal || got.TaskType != chapter.TaskType || got.InputHash != chapter.InputHash || !got.CreatedAt.Equal(chapter.CreatedAt) {
		t.Fatalf("record fields mismatch: %+v", got)
	}
	if !reflect.DeepEqual(got.Artifacts, chapter.Artifacts) {
		t.Fatalf("artifacts mismatch: %+v", got.Artifacts)
	}
	if got.Metadata.Fields["attempt"].Kind != ChapterMetadataInt || got.Metadata.Fields["attempt"].Int != 3 {
		t.Fatalf("metadata did not remain structured: %+v", got.Metadata)
	}
	body, ok := got.Body.(TaskAttemptOutcomeChapter)
	if !ok {
		t.Fatalf("unexpected body type %T", got.Body)
	}
	outcome, ok := body.Outcome.(AppErrorOutcome)
	if !ok {
		t.Fatalf("unexpected outcome type %T", body.Outcome)
	}
	if outcome.Error.Message != "boom" || outcome.Error.Level != "error" || outcome.Error.InputRef == nil || *outcome.Error.InputRef != *inputRef {
		t.Fatalf("unexpected app error payload: %+v", outcome.Error)
	}
}

func TestStructuredChapterRejectsUnsupportedLegacyShapes(t *testing.T) {
	_, err := StructuredChapterFromStored(StoredChapter{
		Ordinal:     1,
		ChapterType: "Manual",
		PayloadKind: payloadKindApp,
		Data:        json.RawMessage(`{"ok":true}`),
	})
	if err == nil || !strings.Contains(err.Error(), `unsupported chapter type "Manual"`) {
		t.Fatalf("expected unsupported chapter error, got %v", err)
	}

	_, err = StoredChapterFromStructured(StructuredChapterRecord{Ordinal: 1})
	if err == nil || !strings.Contains(err.Error(), "unsupported chapter body type") {
		t.Fatalf("expected unsupported body error, got %v", err)
	}
}

func TestStructuredWorkflowRuntimeAdapter(t *testing.T) {
	ref := ChapterRef{JobKey: JobKey{TenantId: "tenant", JobId: "job"}, Ordinal: 1}
	stored := StoredChapter{
		Ordinal:     1,
		TaskType:    "task",
		ChapterType: chapterTypeTaskAttemptOutcome,
		PayloadKind: payloadKindApp,
		InputHash:   "hash",
		CreatedAt:   time.Date(2026, 6, 10, 1, 2, 3, 0, time.UTC),
		Data:        json.RawMessage(`{"ok":true}`),
	}
	runtime := &fakeWorkflowRuntime{
		chapterResp:  stored,
		chaptersResp: []StoredChapter{stored},
	}
	structured := NewStructuredWorkflowRuntime(runtime)

	got, err := structured.GetStructuredChapter(context.Background(), ref)
	if err != nil {
		t.Fatalf("get structured chapter: %v", err)
	}
	if runtime.chapterRef != ref {
		t.Fatalf("legacy runtime ref mismatch: %+v", runtime.chapterRef)
	}
	body, ok := got.Body.(TaskAttemptOutcomeChapter)
	if !ok {
		t.Fatalf("unexpected body type %T", got.Body)
	}
	outcome, ok := body.Outcome.(ApplicationOutputOutcome)
	if !ok {
		t.Fatalf("unexpected outcome type %T", body.Outcome)
	}
	assertRawJSONEqual(t, outcome.Output.Data, json.RawMessage(`{"ok":true}`))

	listed, err := structured.ListStructuredChapters(context.Background(), ListChaptersRequest{JobKey: ref.JobKey})
	if err != nil {
		t.Fatalf("list structured chapters: %v", err)
	}
	if len(listed) != 1 {
		t.Fatalf("expected one listed chapter, got %d", len(listed))
	}

	err = structured.PutStructuredChapter(context.Background(), PutStructuredChapterRequest{
		LeaseID:    "lease",
		LeaseToken: "token",
		Ref:        ref,
		Chapter: StructuredChapterRecord{
			Ordinal:  1,
			TaskType: "task",
			Body:     RestartExtraChapter{Output: ApplicationOutputBytes{Data: []byte(`{"again":true}`)}},
		},
	})
	if err != nil {
		t.Fatalf("put structured chapter: %v", err)
	}
	if runtime.putChapterReq.LeaseID != "lease" || runtime.putChapterReq.LeaseToken != "token" {
		t.Fatalf("legacy put request lost lease fields: %+v", runtime.putChapterReq)
	}
	if runtime.putChapterReq.Chapter.ChapterType != chapterTypeRestartExtra || runtime.putChapterReq.Chapter.PayloadKind != payloadKindApp {
		t.Fatalf("unexpected legacy put chapter: %+v", runtime.putChapterReq.Chapter)
	}
}

func assertRawJSONEqual(t *testing.T, got json.RawMessage, want json.RawMessage) {
	t.Helper()
	var gotValue any
	if err := json.Unmarshal(got, &gotValue); err != nil {
		t.Fatalf("unmarshal got JSON: %v; raw=%s", err, got)
	}
	var wantValue any
	if err := json.Unmarshal(want, &wantValue); err != nil {
		t.Fatalf("unmarshal want JSON: %v; raw=%s", err, want)
	}
	if !reflect.DeepEqual(gotValue, wantValue) {
		t.Fatalf("JSON mismatch:\nwant %s\ngot  %s", want, got)
	}
}
