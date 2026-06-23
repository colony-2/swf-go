package runtimecodec

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
)

func TestChapterRoundTripPreservesEnvelope(t *testing.T) {
	now := time.Date(2026, 6, 10, 1, 2, 3, 4, time.UTC)
	started := now.Add(time.Second)
	finished := started.Add(2 * time.Second)
	nextAttempt := finished.Add(3 * time.Second)
	retryable := false
	policy := jobdb.RunPolicy{
		Retry: jobdb.RetryPolicy{
			InitialInterval:        jobdb.Duration(100 * time.Millisecond),
			BackoffCoefficient:     2,
			MaximumInterval:        jobdb.Duration(time.Second),
			MaximumAttempts:        3,
			NonRetryableErrorTypes: []string{"SystemError"},
		},
		InvocationTimeout: jobdb.AsDuration(time.Minute),
		TotalTimeout:      jobdb.AsDuration(5 * time.Minute),
	}
	meta := ChapterMeta{
		Ordinal:       7,
		TaskType:      "job:task",
		WorkerID:      "worker-1",
		CreatedAt:     now,
		StartedAt:     &started,
		FinishedAt:    &finished,
		InputHash:     "abc123",
		Metadata:      json.RawMessage(`{"queue":"blue","attempt":2,"ratio":1.25,"flags":[true,null],"nested":{"k":"v"}}`),
		Input:         json.RawMessage(`{"in":1}`),
		Attempt:       2,
		MaxAttempts:   3,
		NextAttemptAt: &nextAttempt,
		BackoffMillis: 200,
		Retryable:     &retryable,
		InputRef:      &jobdb.InputReference{Ordinal: 6, Hash: "prev"},
		RunPolicy:     &policy,
		Prerequisites: []jobdb.JobPrerequisite{{JobID: "parent", Condition: jobdb.JobPrereqSuccess}},
	}

	payload := json.RawMessage(`{"message":"boom","level":"error","attrs":{"attempt":2,"nested":{"x":true}},"input_ref":{"ordinal":6,"hash":"prev"},"stacktrace":["top"]}`)
	raw, err := EncodeChapter(meta, ChapterTypeTaskAttemptOutcome, PayloadKindAppError, payload)
	if err != nil {
		t.Fatalf("encode chapter: %v", err)
	}
	if !json.Valid(raw) {
		t.Fatalf("encoded chapter body must be valid JSON for Strata: %q", raw)
	}
	env, err := DecodeChapter(raw)
	if err != nil {
		t.Fatalf("decode chapter: %v", err)
	}

	if env.ChapterType != ChapterTypeTaskAttemptOutcome {
		t.Fatalf("chapter type mismatch: %s", env.ChapterType)
	}
	if env.PayloadKind != PayloadKindAppError {
		t.Fatalf("payload kind mismatch: %s", env.PayloadKind)
	}
	assertJSONEqual(t, env.Payload, payload)
	assertJSONEqual(t, env.Meta.Metadata, meta.Metadata)
	assertJSONEqual(t, env.Meta.Input, meta.Input)
	if !reflect.DeepEqual(env.Meta.Prerequisites, meta.Prerequisites) {
		t.Fatalf("prerequisites mismatch: %#v", env.Meta.Prerequisites)
	}
	if env.Meta.Retryable == nil || *env.Meta.Retryable {
		t.Fatalf("retryable presence/value mismatch: %#v", env.Meta.Retryable)
	}
	if env.Meta.RunPolicy == nil || !reflect.DeepEqual(*env.Meta.RunPolicy, policy) {
		t.Fatalf("run policy mismatch: %#v", env.Meta.RunPolicy)
	}
}

func TestEncodeChapterRejectsCustomChapter(t *testing.T) {
	_, err := EncodeChapter(
		ChapterMeta{Ordinal: 1, TaskType: "manual", CreatedAt: time.Date(2026, 6, 10, 1, 2, 3, 0, time.UTC)},
		"Manual",
		"ManualKind",
		json.RawMessage(`{"manual":true}`),
	)
	if err == nil || !strings.Contains(err.Error(), `unsupported chapter type "Manual"`) {
		t.Fatalf("expected unsupported chapter error, got %v", err)
	}
}

func TestEncodeChapterRejectsCustomOutcome(t *testing.T) {
	_, err := EncodeChapter(
		ChapterMeta{Ordinal: 2, TaskType: "task", CreatedAt: time.Date(2026, 6, 10, 1, 2, 3, 0, time.UTC)},
		ChapterTypeTaskAttemptOutcome,
		"Deferred",
		json.RawMessage(`{"resume":"later"}`),
	)
	if err == nil || !strings.Contains(err.Error(), `unsupported task outcome payload kind "Deferred"`) {
		t.Fatalf("expected unsupported outcome error, got %v", err)
	}
}

func TestSchedulerPayloadRoundTripAndJSONView(t *testing.T) {
	payload := SchedulerPayload{
		RunPolicy: jobdb.RunPolicy{
			Retry: jobdb.RetryPolicy{MaximumAttempts: 5},
		},
		TaskWait: &TaskWait{InputStep: 2, OutputStep: 3, Next: "resume", InputHash: "hash"},
	}

	raw, err := EncodeSchedulerPayload(payload)
	if err != nil {
		t.Fatalf("encode scheduler payload: %v", err)
	}
	got, err := DecodeSchedulerPayload(raw)
	if err != nil {
		t.Fatalf("decode scheduler payload: %v", err)
	}
	if !reflect.DeepEqual(got, payload) {
		t.Fatalf("payload mismatch:\nwant %#v\ngot  %#v", payload, got)
	}

	view, err := SchedulerPayloadJSONView(got)
	if err != nil {
		t.Fatalf("json view: %v", err)
	}
	var decoded struct {
		TaskWait struct {
			InputStep  int64  `json:"in"`
			OutputStep int64  `json:"out"`
			Next       string `json:"next"`
			InputHash  string `json:"input_hash"`
		} `json:"task_wait"`
	}
	if err := json.Unmarshal(view, &decoded); err != nil {
		t.Fatalf("unmarshal json view: %v", err)
	}
	if decoded.TaskWait.InputStep != 2 || decoded.TaskWait.OutputStep != 3 || decoded.TaskWait.Next != "resume" || decoded.TaskWait.InputHash != "hash" {
		t.Fatalf("unexpected json view: %s", view)
	}
}

func assertJSONEqual(t *testing.T, got json.RawMessage, want json.RawMessage) {
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

func TestSchedulerPayloadPreservesVisibleJSONPayload(t *testing.T) {
	payload, err := SchedulerPayloadFromJSONView(json.RawMessage(`{"kind":"rescheduled","n":2}`))
	if err != nil {
		t.Fatalf("from json view: %v", err)
	}
	raw, err := EncodeSchedulerPayload(payload)
	if err != nil {
		t.Fatalf("encode scheduler payload: %v", err)
	}
	got, err := DecodeSchedulerPayload(raw)
	if err != nil {
		t.Fatalf("decode scheduler payload: %v", err)
	}
	view, err := SchedulerPayloadJSONView(got)
	if err != nil {
		t.Fatalf("json view: %v", err)
	}
	if string(view) != `{"kind":"rescheduled","n":2}` {
		t.Fatalf("visible payload mismatch: %s", view)
	}
}

func TestSchedulerPayloadFromJSONViewDoesNotDuplicateSchedulerFields(t *testing.T) {
	payload, err := SchedulerPayloadFromJSONView(json.RawMessage(`{"run_policy":{"retry":{"maximumAttempts":2}},"task_wait":{"in":1,"out":2,"next":"job","input_hash":"abc"}}`))
	if err != nil {
		t.Fatalf("from json view: %v", err)
	}
	if len(payload.VisiblePayload) != 0 {
		t.Fatalf("scheduler-shaped payload should not be duplicated as visible JSON: %s", payload.VisiblePayload)
	}
	if payload.TaskWait == nil {
		t.Fatalf("task wait missing")
	}
	if payload.TaskWait.InputStep != 1 || payload.TaskWait.OutputStep != 2 || payload.TaskWait.Next != "job" || payload.TaskWait.InputHash != "abc" {
		t.Fatalf("unexpected task wait: %#v", payload.TaskWait)
	}
}
