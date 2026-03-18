package swf

import (
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"
)

const (
	jobFailedAttrKey          = "_swf_job_failed"
	jobFailedKindAttrKey      = "_swf_job_failed_kind"
	jobFailedCodeAttrKey      = "_swf_job_failed_code"
	jobFailedComponentAttrKey = "_swf_job_failed_component"
	jobFailedRetryableAttrKey = "_swf_job_failed_retryable"
	jobFailedScopeAttrKey     = "_swf_job_failed_scope"
	jobFailedAfterAttrKey     = "_swf_job_failed_after"
)

// JobFailedError preserves the semantic distinction between a failed job output
// lookup and the underlying job error payload.
type JobFailedError struct {
	Cause error
}

func (e JobFailedError) Error() string {
	if e.Cause == nil {
		return ErrJobFailed.Error()
	}
	if errors.Is(e.Cause, ErrJobFailed) {
		return e.Cause.Error()
	}
	return fmt.Sprintf("%s: %s", ErrJobFailed, e.Cause.Error())
}

func (e JobFailedError) Unwrap() error {
	return e.Cause
}

func (e JobFailedError) Is(target error) bool {
	return target == ErrJobFailed
}

func jobFailedErrorFromOutcome(outcome TaskOutcome) error {
	if outcome.Error == nil {
		return JobFailedError{}
	}
	return JobFailedError{Cause: taskErrorToError(outcome.Error, outcome.PayloadKind)}
}

func taskErrorToError(taskErr *TaskError, payloadKind string) error {
	if taskErr == nil {
		return nil
	}
	switch taskErr.Kind {
	case TaskErrorKindTimeout:
		after := Duration(0)
		if taskErr.After != nil {
			after = *taskErr.After
		}
		return TimeoutError{Payload: TimeoutPayload{
			Scope:     taskErr.Scope,
			After:     after,
			Retryable: boolPtrValue(taskErr.Retryable),
			InputRef:  taskErr.InputRef,
			Component: taskErr.Component,
			Code:      taskErr.Code,
			Message:   taskErr.Message,
		}}
	case TaskErrorKindSystem:
		return SystemError{Payload: SystemErrorPayload{
			Message:    taskErr.Message,
			Component:  taskErr.Component,
			Code:       taskErr.Code,
			Retryable:  boolPtrValue(taskErr.Retryable),
			InputRef:   taskErr.InputRef,
			Stacktrace: append([]string(nil), taskErr.Stacktrace...),
		}}
	case TaskErrorKindApp:
		fallthrough
	default:
		level := taskErr.Level
		if level == "" && payloadKind == payloadKindSystemError {
			return SystemError{Payload: SystemErrorPayload{
				Message:    taskErr.Message,
				Component:  taskErr.Component,
				Code:       taskErr.Code,
				Retryable:  boolPtrValue(taskErr.Retryable),
				InputRef:   taskErr.InputRef,
				Stacktrace: append([]string(nil), taskErr.Stacktrace...),
			}}
		}
		return AppError{Payload: AppErrorPayload{
			Message:    taskErr.Message,
			Level:      level,
			Attrs:      cloneAttrs(taskErr.Attrs),
			InputRef:   taskErr.InputRef,
			Stacktrace: append([]string(nil), taskErr.Stacktrace...),
		}}
	}
}

func encodeJobFailedError(err error, inputRef *InputReference) (json.RawMessage, string, error, bool) {
	if !errors.Is(err, ErrJobFailed) {
		return nil, "", nil, false
	}

	message := ErrJobFailed.Error()
	level := "error"
	attrs := map[string]interface{}{
		jobFailedAttrKey: true,
	}

	var jobFailed JobFailedError
	switch {
	case errors.As(err, &jobFailed) && jobFailed.Cause != nil:
		message, level = encodeJobFailedCause(jobFailed.Cause, attrs)
	default:
		message = trimJobFailedPrefix(err.Error())
	}

	payload := AppErrorPayload{
		Message:  message,
		Level:    level,
		Attrs:    attrs,
		InputRef: inputRef,
	}
	raw, marshalErr := json.Marshal(payload)
	return json.RawMessage(raw), payloadKindAppError, marshalErr, true
}

func encodeJobFailedCause(cause error, attrs map[string]interface{}) (string, string) {
	var timeoutErr TimeoutError
	if errors.As(cause, &timeoutErr) {
		attrs[jobFailedKindAttrKey] = TaskErrorKindTimeout
		attrs[jobFailedComponentAttrKey] = timeoutErr.Payload.Component
		attrs[jobFailedCodeAttrKey] = timeoutErr.Payload.Code
		attrs[jobFailedRetryableAttrKey] = timeoutErr.Payload.Retryable
		attrs[jobFailedScopeAttrKey] = timeoutErr.Payload.Scope
		if timeoutErr.Payload.After != 0 {
			attrs[jobFailedAfterAttrKey] = time.Duration(timeoutErr.Payload.After).String()
		}
		return timeoutErr.Payload.Message, "error"
	}

	var systemErr SystemError
	if errors.As(cause, &systemErr) {
		attrs[jobFailedKindAttrKey] = TaskErrorKindSystem
		attrs[jobFailedComponentAttrKey] = systemErr.Payload.Component
		attrs[jobFailedCodeAttrKey] = systemErr.Payload.Code
		attrs[jobFailedRetryableAttrKey] = systemErr.Payload.Retryable
		return systemErr.Payload.Message, "error"
	}

	var appErr AppError
	if errors.As(cause, &appErr) {
		attrs[jobFailedKindAttrKey] = TaskErrorKindApp
		for k, v := range cloneAttrs(appErr.Payload.Attrs) {
			attrs[k] = v
		}
		return appErr.Payload.Message, appErr.Payload.Level
	}

	return cause.Error(), "error"
}

func decodeJobFailedAppError(payload AppErrorPayload) (error, bool) {
	if !jobFailedMarked(payload.Attrs) {
		return nil, false
	}

	switch attrString(payload.Attrs, jobFailedKindAttrKey) {
	case TaskErrorKindTimeout:
		after := Duration(0)
		if raw := attrString(payload.Attrs, jobFailedAfterAttrKey); raw != "" {
			if parsed, err := time.ParseDuration(raw); err == nil {
				after = Duration(parsed)
			}
		}
		return JobFailedError{Cause: TimeoutError{Payload: TimeoutPayload{
			Scope:     attrString(payload.Attrs, jobFailedScopeAttrKey),
			After:     after,
			Retryable: attrBool(payload.Attrs, jobFailedRetryableAttrKey),
			InputRef:  payload.InputRef,
			Component: attrString(payload.Attrs, jobFailedComponentAttrKey),
			Code:      attrString(payload.Attrs, jobFailedCodeAttrKey),
			Message:   payload.Message,
		}}}, true
	case TaskErrorKindSystem:
		return JobFailedError{Cause: SystemError{Payload: SystemErrorPayload{
			Message:    payload.Message,
			Component:  attrString(payload.Attrs, jobFailedComponentAttrKey),
			Code:       attrString(payload.Attrs, jobFailedCodeAttrKey),
			Retryable:  attrBool(payload.Attrs, jobFailedRetryableAttrKey),
			InputRef:   payload.InputRef,
			Stacktrace: append([]string(nil), payload.Stacktrace...),
		}}}, true
	default:
		return JobFailedError{Cause: AppError{Payload: AppErrorPayload{
			Message:    payload.Message,
			Level:      payload.Level,
			Attrs:      stripJobFailedAttrs(payload.Attrs),
			InputRef:   payload.InputRef,
			Stacktrace: append([]string(nil), payload.Stacktrace...),
		}}}, true
	}
}

func jobFailedMarked(attrs map[string]interface{}) bool {
	if attrs == nil {
		return false
	}
	raw, ok := attrs[jobFailedAttrKey]
	if !ok {
		return false
	}
	value, ok := raw.(bool)
	return ok && value
}

func stripJobFailedAttrs(attrs map[string]interface{}) map[string]interface{} {
	if len(attrs) == 0 {
		return nil
	}
	out := make(map[string]interface{}, len(attrs))
	for key, value := range attrs {
		switch key {
		case jobFailedAttrKey, jobFailedKindAttrKey, jobFailedCodeAttrKey, jobFailedComponentAttrKey, jobFailedRetryableAttrKey, jobFailedScopeAttrKey, jobFailedAfterAttrKey:
			continue
		default:
			out[key] = value
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func attrString(attrs map[string]interface{}, key string) string {
	if attrs == nil {
		return ""
	}
	value, ok := attrs[key]
	if !ok {
		return ""
	}
	str, _ := value.(string)
	return str
}

func attrBool(attrs map[string]interface{}, key string) bool {
	if attrs == nil {
		return false
	}
	value, ok := attrs[key]
	if !ok {
		return false
	}
	flag, _ := value.(bool)
	return flag
}

func cloneAttrs(attrs map[string]interface{}) map[string]interface{} {
	if len(attrs) == 0 {
		return nil
	}
	out := make(map[string]interface{}, len(attrs))
	for key, value := range attrs {
		out[key] = value
	}
	return out
}

func boolPtrValue(v *bool) bool {
	return v != nil && *v
}

func trimJobFailedPrefix(message string) string {
	prefix := ErrJobFailed.Error() + ": "
	if strings.HasPrefix(message, prefix) {
		return strings.TrimPrefix(message, prefix)
	}
	return message
}
