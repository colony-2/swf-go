package jobdb

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
	ErrConflict                 = errors.New("workflow state conflict")
	ErrExistingJobMismatch      = errors.New("existing job does not match request")
	ErrJobSchemaNotFound        = errors.New("job schema not found")
	ErrJobSchemaArchived        = errors.New("job schema is archived")
	ErrJobSchemaValidation      = errors.New("job schema validation failed")
)

type existingJobMismatchError struct {
	message string
}

func (e *existingJobMismatchError) Error() string {
	if e == nil || e.message == "" {
		return ErrExistingJobMismatch.Error()
	}
	return e.message
}

func (e *existingJobMismatchError) Is(target error) bool {
	return target == ErrExistingJobMismatch || target == ErrConflict
}

// NewExistingJobMismatchError reports that a custom job ID already exists but
// its durable state does not match the requested submit/restart operation.
func NewExistingJobMismatchError(message string) error {
	return &existingJobMismatchError{message: message}
}

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

func (e *SystemError) As(target any) bool {
	if e == nil {
		return false
	}
	switch t := target.(type) {
	case *SystemError:
		*t = *e
		return true
	case **SystemError:
		*t = e
		return true
	default:
		return false
	}
}

func (SystemError) systemErrorMarker() {}

// NewSystemError constructs a system error with the provided payload.
func NewSystemError(payload SystemErrorPayload) error {
	return &SystemError{Payload: payload}
}

// AppError represents user-land/task errors; wraps AppErrorPayload.
type AppError struct {
	Payload AppErrorPayload
}

func (e AppError) Error() string {
	return e.Payload.Message
}

func (e *AppError) As(target any) bool {
	if e == nil {
		return false
	}
	switch t := target.(type) {
	case *AppError:
		*t = *e
		return true
	case **AppError:
		*t = e
		return true
	default:
		return false
	}
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

// IsConflict reports whether err represents a conflicting conditional write or
// conflicting runtime state transition.
func IsConflict(err error) bool {
	return errors.Is(err, ErrConflict)
}

// IsExistingJobMismatch reports whether err represents a custom job ID that
// already exists with durable state inconsistent with the current request.
func IsExistingJobMismatch(err error) bool {
	return errors.Is(err, ErrExistingJobMismatch)
}

// IsJobSchemaNotFound reports whether err represents an unknown tenant-local
// job schema hash.
func IsJobSchemaNotFound(err error) bool {
	return errors.Is(err, ErrJobSchemaNotFound)
}

// IsJobSchemaArchived reports whether err represents an attempt to create a new
// job using an archived schema.
func IsJobSchemaArchived(err error) bool {
	return errors.Is(err, ErrJobSchemaArchived)
}

// IsJobSchemaValidation reports whether err represents a schema validation
// failure for a job or chapter document.
func IsJobSchemaValidation(err error) bool {
	return errors.Is(err, ErrJobSchemaValidation)
}
