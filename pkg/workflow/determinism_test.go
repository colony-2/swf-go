package workflow

import (
	"encoding/json"
	"errors"
	"testing"
	"time"
)

func TestTaskInputMismatchErrorUnwrapAndAccessors(t *testing.T) {
	payload := json.RawMessage(`{"hello":"world"}`)
	td := NewTaskDataOrPanic(map[string]int{"n": 1})
	meta := TaskDeterminismMeta{
		Ordinal:      1,
		TaskType:     "add",
		WorkerID:     "worker-1",
		CreatedAt:    time.Now(),
		Attempt:      1,
		InputHash:    "cached-hash",
		InputRef:     &InputReference{Ordinal: 0, Hash: "cached-hash"},
		RunPolicy:    &RunPolicy{},
		InputPayload: payload,
		Version:      1,
	}

	err := TaskInputMismatchError{
		TaskType:          "add",
		Ordinal:           1,
		CachedInputHash:   "cached-hash",
		ComputedInputHash: "new-hash",
		CachedInput:       payload,
		CachedOutput:      td,
		Meta:              meta,
	}

	if !errors.Is(err, ErrWorkflowNotDeterministic) {
		t.Fatalf("expected errors.Is to match ErrWorkflowNotDeterministic")
	}

	got, ok := UnexpectedChapter(err)
	if !ok {
		t.Fatalf("UnexpectedChapter should extract mismatch error")
	}
	if got.TaskType != "add" || got.Ordinal != 1 {
		t.Fatalf("unexpected extracted error data: %+v", got)
	}
	if string(got.CachedInputPayload()) != string(payload) {
		t.Fatalf("cached input payload mismatch: %s", string(got.CachedInputPayload()))
	}
	if got.CachedTaskData() != td {
		t.Fatalf("cached task data not preserved")
	}
	if got.ChapterMeta().InputHash != "cached-hash" {
		t.Fatalf("meta input hash mismatch: %s", got.ChapterMeta().InputHash)
	}
}

func TestUnexpectedChapterNoMatch(t *testing.T) {
	_, ok := UnexpectedChapter(errors.New("other"))
	if ok {
		t.Fatalf("UnexpectedChapter should be false for unrelated errors")
	}
}
