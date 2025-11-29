package swf

import (
	"fmt"
	"time"
)

const (
	TimeoutScopeInvocation = "invocation"
	TimeoutScopeTotal      = "total"
)

// TimeoutPayload captures a deterministic timeout outcome.
type TimeoutPayload struct {
	Scope     string          `json:"scope"`
	After     Duration        `json:"after"`
	Retryable bool            `json:"retryable"`
	InputRef  *InputReference `json:"input_ref,omitempty"`
	Kind      string          `json:"kind,omitempty"`      // "job" or "task"
	Component string          `json:"component,omitempty"` // always "runner"
	Code      string          `json:"code,omitempty"`      // timeout_invocation/timeout_total
	Message   string          `json:"message,omitempty"`
}

// TimeoutError is returned when an invocation or total deadline elapses.
type TimeoutError struct {
	Payload TimeoutPayload
}

func (e TimeoutError) Error() string {
	if e.Payload.Message != "" {
		return e.Payload.Message
	}
	return "timeout exceeded"
}

// NewTimeoutError constructs a timeout error with the provided scope and retryability.
func NewTimeoutError(kind string, after time.Duration, scope string, inputRef *InputReference, retryable bool) error {
	code := "timeout_" + scope
	msg := fmt.Sprintf("%s %s timed out after %s", kind, scope, after.String())
	return TimeoutError{
		Payload: TimeoutPayload{
			Scope:     scope,
			After:     Duration(after),
			Retryable: retryable,
			InputRef:  inputRef,
			Kind:      kind,
			Component: "runner",
			Code:      code,
			Message:   msg,
		},
	}
}
