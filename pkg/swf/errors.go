package swf

import "errors"

var (
	ErrWorkflowNotDeterministic = errors.New("workflow was not deterministic")
	ErrMissingInputHash         = errors.New("workflow deterministic metadata missing input hash")
	ErrChapterNotFound          = errors.New("chapter not found")
	ErrJobNotComplete           = errors.New("job not complete")
	ErrJobFailed                = errors.New("job failed")
	ErrJobCancelled             = errors.New("job cancelled")
	ErrJobNotFound              = errors.New("job not found")
	ErrExecutionLeaseLost       = errors.New("execution lease lost")
)

// NonRetryableError marks an error as not eligible for retries.
// Implementors should return true when retries should stop immediately.
type NonRetryableError interface {
	NonRetryable() bool
}

// systemErrorMarker is implemented by internal system error types.
type systemErrorMarker interface {
	error
	systemErrorMarker()
}

type AppErrorPayload struct {
	Message    string                 `json:"message"`
	Level      string                 `json:"level,omitempty"`
	Attrs      map[string]interface{} `json:"attrs,omitempty"`
	InputRef   *InputReference        `json:"input_ref,omitempty"`
	Stacktrace []string               `json:"stacktrace,omitempty"`
}

type SystemErrorPayload struct {
	Message    string          `json:"message"`
	Component  string          `json:"component,omitempty"`
	Code       string          `json:"code,omitempty"`
	Retryable  bool            `json:"retryable,omitempty"`
	InputRef   *InputReference `json:"input_ref,omitempty"`
	Stacktrace []string        `json:"stacktrace,omitempty"`
}

// SystemError represents infrastructure/transport failures.
type SystemError struct {
	Payload SystemErrorPayload
}

func (e SystemError) Error() string {
	return e.Payload.Message
}

func (SystemError) systemErrorMarker() {}

// NewSystemError constructs a system error with the provided payload.
func NewSystemError(payload SystemErrorPayload) error {
	return SystemError{Payload: payload}
}

// AppError represents user-land/task errors; wraps AppErrorPayload.
type AppError struct {
	Payload AppErrorPayload
}

func (e AppError) Error() string {
	return e.Payload.Message
}

// IsAppError reports whether err is a wrapped AppError.
func IsAppError(err error) bool {
	var ae AppError
	return errors.As(err, &ae)
}

// IsSystemError reports whether err represents an internal/system failure.
func IsSystemError(err error) bool {
	var se systemErrorMarker
	return errors.As(err, &se)
}

// IsExecutionLeaseLost reports whether the current worker no longer owns the
// leased execution and should stop without treating it as a workflow failure.
func IsExecutionLeaseLost(err error) bool {
	return errors.Is(err, ErrExecutionLeaseLost)
}
