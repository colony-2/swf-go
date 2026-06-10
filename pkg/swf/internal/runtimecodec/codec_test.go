package runtimecodec

import (
	"encoding/json"
	"reflect"
	"testing"
	"time"

	"github.com/colony-2/swf-go/pkg/swf"
)

func TestChapterRoundTripPreservesEnvelope(t *testing.T) {
	now := time.Date(2026, 6, 10, 1, 2, 3, 4, time.UTC)
	started := now.Add(time.Second)
	finished := started.Add(2 * time.Second)
	nextAttempt := finished.Add(3 * time.Second)
	retryable := false
	policy := swf.RunPolicy{
		Retry: swf.RetryPolicy{
			InitialInterval:        swf.Duration(100 * time.Millisecond),
			BackoffCoefficient:     2,
			MaximumInterval:        swf.Duration(time.Second),
			MaximumAttempts:        3,
			NonRetryableErrorTypes: []string{"SystemError"},
		},
		InvocationTimeout: swf.AsDuration(time.Minute),
		TotalTimeout:      swf.AsDuration(5 * time.Minute),
	}
	meta := ChapterMeta{
		Ordinal:       7,
		TaskType:      "job:task",
		WorkerID:      "worker-1",
		CreatedAt:     now,
		StartedAt:     &started,
		FinishedAt:    &finished,
		InputHash:     "abc123",
		Metadata:      json.RawMessage(`{"queue":"blue"}`),
		Input:         json.RawMessage(`{"in":1}`),
		Attempt:       2,
		MaxAttempts:   3,
		NextAttemptAt: &nextAttempt,
		BackoffMillis: 200,
		Retryable:     &retryable,
		InputRef:      &swf.InputReference{Ordinal: 6, Hash: "prev"},
		RunPolicy:     &policy,
		Prerequisites: []swf.JobPrerequisite{{JobID: "parent", Condition: swf.JobPrereqSuccess}},
	}

	raw, err := EncodeChapter(meta, ChapterTypeTaskAttemptOutcome, PayloadKindAppError, json.RawMessage(`{"message":"boom"}`))
	if err != nil {
		t.Fatalf("encode chapter: %v", err)
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
	if string(env.Payload) != `{"message":"boom"}` {
		t.Fatalf("payload mismatch: %s", env.Payload)
	}
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

func TestSchedulerPayloadRoundTripAndJSONView(t *testing.T) {
	payload := SchedulerPayload{
		RunPolicy: swf.RunPolicy{
			Retry: swf.RetryPolicy{MaximumAttempts: 5},
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
