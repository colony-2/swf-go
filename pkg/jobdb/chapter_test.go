package jobdb

import (
	"context"
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestChapterWireRoundTrip(t *testing.T) {
	inputRef := &InputReference{Ordinal: 6, Hash: "prev"}
	body := TaskAttemptOutcomeChapter{Outcome: AppErrorOutcome{Error: AppErrorPayload{
		Message:    "boom",
		Level:      "error",
		Attrs:      map[string]any{"category": "user"},
		InputRef:   inputRef,
		Stacktrace: []string{"top"},
	}}}

	chapterType, payloadKind, data, err := chapterBodyToWire(body)
	if err != nil {
		t.Fatalf("chapter body to wire: %v", err)
	}
	if chapterType != chapterTypeTaskAttemptOutcome || payloadKind != payloadKindAppError {
		t.Fatalf("unexpected discriminators: %s/%s", chapterType, payloadKind)
	}
	assertRawJSONEqual(t, data, json.RawMessage(`{"message":"boom","level":"error","attrs":{"category":"user"},"input_ref":{"ordinal":6,"hash":"prev"},"stacktrace":["top"]}`))

	gotBody, err := chapterBodyFromWire(chapterType, payloadKind, data)
	if err != nil {
		t.Fatalf("chapter body from wire: %v", err)
	}
	got, ok := gotBody.(TaskAttemptOutcomeChapter)
	if !ok {
		t.Fatalf("unexpected body type %T", gotBody)
	}
	outcome, ok := got.Outcome.(AppErrorOutcome)
	if !ok {
		t.Fatalf("unexpected outcome type %T", got.Outcome)
	}
	if outcome.Error.Message != "boom" || outcome.Error.Level != "error" || outcome.Error.InputRef == nil || *outcome.Error.InputRef != *inputRef {
		t.Fatalf("unexpected app error payload: %+v", outcome.Error)
	}
}

func TestChapterMetadataJSONRoundTrip(t *testing.T) {
	metadata := ChapterMetadata{Fields: map[string]ChapterMetadataValue{
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
	}}

	raw, err := chapterMetadataJSON(metadata)
	if err != nil {
		t.Fatalf("metadata to JSON: %v", err)
	}
	assertRawJSONEqual(t, raw, json.RawMessage(`{"attempt":3,"flags":["a",null],"nested":{"ok":true}}`))

	got, err := chapterMetadataFromJSON(raw)
	if err != nil {
		t.Fatalf("metadata from JSON: %v", err)
	}
	if !reflect.DeepEqual(got, metadata) {
		t.Fatalf("metadata mismatch:\nwant %+v\ngot  %+v", metadata, got)
	}
}

func TestChapterWireRejectsUnsupportedShapes(t *testing.T) {
	_, err := chapterBodyFromWire("Manual", payloadKindApp, json.RawMessage(`{"ok":true}`))
	if err == nil || !strings.Contains(err.Error(), `unsupported chapter type "Manual"`) {
		t.Fatalf("expected unsupported chapter error, got %v", err)
	}

	_, _, _, err = chapterBodyToWire(Chapter{}.Body)
	if err == nil || !strings.Contains(err.Error(), "unsupported chapter body type") {
		t.Fatalf("expected unsupported body error, got %v", err)
	}
}

func TestWorkflowRuntimeUsesChapterAPI(t *testing.T) {
	ref := ChapterRef{JobKey: JobKey{TenantId: "tenant", JobId: "job"}, Ordinal: 1}
	chapter := Chapter{
		Ordinal:   1,
		TaskType:  "task",
		InputHash: "hash",
		CreatedAt: time.Date(2026, 6, 10, 1, 2, 3, 0, time.UTC),
		Body:      TaskAttemptOutcomeChapter{Outcome: ApplicationOutputOutcome{Output: ApplicationOutputBytes{Data: []byte(`{"ok":true}`)}}},
	}
	runtime := &chapterTestRuntime{
		chapterResp:  chapter,
		chaptersResp: []Chapter{chapter},
	}

	got, err := runtime.GetChapter(context.Background(), ref)
	if err != nil {
		t.Fatalf("get chapter: %v", err)
	}
	if runtime.chapterRef != ref {
		t.Fatalf("runtime ref mismatch: %+v", runtime.chapterRef)
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

	listed, err := runtime.ListChapters(context.Background(), ListChaptersRequest{JobKey: ref.JobKey})
	if err != nil {
		t.Fatalf("list chapters: %v", err)
	}
	if len(listed) != 1 {
		t.Fatalf("expected one listed chapter, got %d", len(listed))
	}

	err = runtime.PutChapter(context.Background(), PutChapterRequest{
		LeaseID:    "lease",
		LeaseToken: "token",
		Ref:        ref,
		Chapter: Chapter{
			Ordinal:  1,
			TaskType: "task",
			Body:     RestartExtraChapter{Output: ApplicationOutputBytes{Data: []byte(`{"again":true}`)}},
		},
	})
	if err != nil {
		t.Fatalf("put chapter: %v", err)
	}
	if runtime.putChapterReq.LeaseID != "lease" || runtime.putChapterReq.LeaseToken != "token" {
		t.Fatalf("put request lost lease fields: %+v", runtime.putChapterReq)
	}
	if !chapterIs(runtime.putChapterReq.Chapter, chapterTypeRestartExtra) {
		t.Fatalf("unexpected put chapter: %+v", runtime.putChapterReq.Chapter)
	}
	payloadKind, payload, err := chapterPayload(runtime.putChapterReq.Chapter)
	if err != nil {
		t.Fatalf("put chapter payload: %v", err)
	}
	if payloadKind != payloadKindApp {
		t.Fatalf("unexpected put payload kind: %s", payloadKind)
	}
	assertRawJSONEqual(t, payload, json.RawMessage(`{"again":true}`))
}

type chapterTestRuntime struct {
	chapterRef    ChapterRef
	putChapterReq PutChapterRequest
	chapterResp   Chapter
	chaptersResp  []Chapter
}

func (r *chapterTestRuntime) GetChapter(ctx context.Context, ref ChapterRef) (Chapter, error) {
	r.chapterRef = ref
	return r.chapterResp, nil
}

func (r *chapterTestRuntime) PutChapter(ctx context.Context, req PutChapterRequest) error {
	r.putChapterReq = req
	return nil
}

func (r *chapterTestRuntime) ListChapters(ctx context.Context, req ListChaptersRequest) ([]Chapter, error) {
	return append([]Chapter(nil), r.chaptersResp...), nil
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
