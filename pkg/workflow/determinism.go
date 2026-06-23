package workflow

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// TaskDeterminismMeta exposes deterministic metadata persisted with a task chapter.
// It is a public, minimal view of the internal chapter metadata used for replay.
type TaskDeterminismMeta struct {
	Ordinal       int64           `json:"ordinal"`
	TaskType      string          `json:"task_type"`
	WorkerID      string          `json:"worker_id,omitempty"`
	CreatedAt     time.Time       `json:"created_at,omitempty"`
	Attempt       int             `json:"attempt,omitempty"`
	MaxAttempts   int             `json:"max_attempts,omitempty"`
	NextAttemptAt *time.Time      `json:"next_attempt_at,omitempty"`
	BackoffMillis int64           `json:"backoff_ms,omitempty"`
	Retryable     *bool           `json:"retryable,omitempty"`
	InputHash     string          `json:"input_hash,omitempty"`
	InputRef      *InputReference `json:"input_ref,omitempty"`
	RunPolicy     *RunPolicy      `json:"run_policy,omitempty"`
	InputPayload  json.RawMessage `json:"input_payload,omitempty"`
	Version       int             `json:"version,omitempty"`
}

// TaskInputMismatchError is returned when a cached task chapter's input hash
// does not match the hash of the current DoTask input. It unwraps to
// ErrWorkflowNotDeterministic so existing callers can continue using errors.Is.
type TaskInputMismatchError struct {
	TaskType          string
	Ordinal           int64
	CachedInputHash   string
	ComputedInputHash string
	CachedInput       json.RawMessage
	CachedOutput      TaskData
	CachedOutputErr   error
	Meta              TaskDeterminismMeta
}

func (e TaskInputMismatchError) Error() string {
	return fmt.Sprintf("workflow was not deterministic: task %s ordinal %d input hash mismatch (cached=%s computed=%s)",
		e.TaskType, e.Ordinal, e.CachedInputHash, e.ComputedInputHash)
}

// Unwrap preserves compatibility with errors.Is(err, ErrWorkflowNotDeterministic).
func (e TaskInputMismatchError) Unwrap() error { return ErrWorkflowNotDeterministic }

// CachedTaskData returns the cached task output (if it could be rehydrated).
func (e TaskInputMismatchError) CachedTaskData() TaskData { return e.CachedOutput }

// CachedTaskDataErr reports any error encountered while rehydrating the cached output.
func (e TaskInputMismatchError) CachedTaskDataErr() error { return e.CachedOutputErr }

// CachedInputPayload returns the persisted task input payload, when task input storage is enabled.
func (e TaskInputMismatchError) CachedInputPayload() json.RawMessage { return e.CachedInput }

// ChapterMeta exposes the deterministic metadata captured with the cached chapter.
func (e TaskInputMismatchError) ChapterMeta() TaskDeterminismMeta { return e.Meta }

// UnexpectedChapter extracts TaskInputMismatchError from err, if present.
func UnexpectedChapter(err error) (TaskInputMismatchError, bool) {
	var mismatch TaskInputMismatchError
	if errors.As(err, &mismatch) {
		return mismatch, true
	}
	return TaskInputMismatchError{}, false
}
