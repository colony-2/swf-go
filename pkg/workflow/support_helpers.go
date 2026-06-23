package workflow

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

const (
	jobFailedAttrKey          = "_jobdb_job_failed"
	jobFailedKindAttrKey      = "_jobdb_job_failed_kind"
	jobFailedCodeAttrKey      = "_jobdb_job_failed_code"
	jobFailedComponentAttrKey = "_jobdb_job_failed_component"
	jobFailedRetryableAttrKey = "_jobdb_job_failed_retryable"
	jobFailedScopeAttrKey     = "_jobdb_job_failed_scope"
	jobFailedAfterAttrKey     = "_jobdb_job_failed_after"
)

func metadataPredicateSignature(predicates []MetadataPredicate) (string, error) {
	if len(predicates) == 0 {
		return "", nil
	}
	raw, err := json.Marshal(predicates)
	if err != nil {
		return "", fmt.Errorf("metadata predicates must be JSON-serializable: %w", err)
	}
	return string(raw), nil
}

func computeSha256(r io.Reader) (string, error) {
	h := sha256.New()
	if _, err := io.Copy(h, r); err != nil {
		return "", fmt.Errorf("compute sha256: %w", err)
	}
	return fmt.Sprintf("%x", h.Sum(nil)), nil
}

func loadArtifactKey(key *atomic.Pointer[ArtifactKey]) (ArtifactKey, error) {
	if key == nil {
		return ArtifactKey{}, ErrArtifactKeyUnavailable
	}
	if value := key.Load(); value != nil {
		return *value, nil
	}
	return ArtifactKey{}, ErrArtifactKeyUnavailable
}

func chapterMetadataFromJSON(raw json.RawMessage) (ChapterMetadata, error) {
	if len(raw) == 0 {
		return ChapterMetadata{}, nil
	}
	if !json.Valid(raw) {
		return ChapterMetadata{}, fmt.Errorf("metadata must be valid JSON")
	}
	value, err := decodeJSONValue(raw)
	if err != nil {
		return ChapterMetadata{}, err
	}
	if value == nil {
		return ChapterMetadata{}, nil
	}
	fields, ok := value.(map[string]any)
	if !ok {
		return ChapterMetadata{}, fmt.Errorf("metadata must be a JSON object")
	}
	out := ChapterMetadata{Fields: make(map[string]ChapterMetadataValue, len(fields))}
	for name, value := range fields {
		converted, err := chapterMetadataValueFromAny(value)
		if err != nil {
			return ChapterMetadata{}, fmt.Errorf("%s: %w", name, err)
		}
		out.Fields[name] = converted
	}
	return out, nil
}

func chapterMetadataJSON(metadata ChapterMetadata) (json.RawMessage, error) {
	if metadata.Fields == nil {
		return nil, nil
	}
	raw, err := json.Marshal(chapterMetadataFieldsToAny(metadata.Fields))
	if err != nil {
		return nil, err
	}
	return json.RawMessage(raw), nil
}

func chapterType(chapter Chapter) (string, error) {
	chapterType, _, _, err := chapterBodyToWire(chapter.Body)
	return chapterType, err
}

func chapterPayload(chapter Chapter) (string, json.RawMessage, error) {
	_, payloadKind, data, err := chapterBodyToWire(chapter.Body)
	return payloadKind, data, err
}

func chapterIs(chapter Chapter, want string) bool {
	got, err := chapterType(chapter)
	return err == nil && got == want
}

func chapterBodyFromWire(chapterType string, payloadKind string, data json.RawMessage) (ChapterBody, error) {
	switch chapterType {
	case chapterTypeJobStart:
		if payloadKind != payloadKindApp {
			return nil, fmt.Errorf("%s chapters require %s payloads", chapterType, payloadKindApp)
		}
		return JobStartChapter{Input: ApplicationInputBytes{Data: cloneRawMessage(data)}}, nil
	case chapterTypeJobAttemptOutcome:
		outcome, err := chapterOutcomeFromWire(payloadKind, data)
		if err != nil {
			return nil, err
		}
		return JobAttemptOutcomeChapter{Outcome: outcome}, nil
	case chapterTypeTaskAttemptOutcome:
		outcome, err := chapterOutcomeFromWire(payloadKind, data)
		if err != nil {
			return nil, err
		}
		return TaskAttemptOutcomeChapter{Outcome: outcome}, nil
	case chapterTypeRestartExtra:
		if payloadKind != payloadKindApp {
			return nil, fmt.Errorf("%s chapters require %s payloads", chapterType, payloadKindApp)
		}
		return RestartExtraChapter{Output: ApplicationOutputBytes{Data: cloneRawMessage(data)}}, nil
	default:
		return nil, fmt.Errorf("unsupported chapter type %q", chapterType)
	}
}

func chapterOutcomeFromWire(payloadKind string, data json.RawMessage) (ChapterOutcome, error) {
	switch payloadKind {
	case payloadKindApp:
		return ApplicationOutputOutcome{Output: ApplicationOutputBytes{Data: cloneRawMessage(data)}}, nil
	case payloadKindAppError:
		var payload AppErrorPayload
		if err := json.Unmarshal(data, &payload); err != nil {
			return nil, err
		}
		return AppErrorOutcome{Error: payload}, nil
	case payloadKindSystemError:
		var payload SystemErrorPayload
		if err := json.Unmarshal(data, &payload); err != nil {
			return nil, err
		}
		return SystemErrorOutcome{Error: payload}, nil
	case payloadKindTimeout:
		var payload TimeoutPayload
		if err := json.Unmarshal(data, &payload); err != nil {
			return nil, err
		}
		return TimeoutOutcome{Timeout: payload}, nil
	default:
		return nil, fmt.Errorf("unsupported task outcome payload kind %q", payloadKind)
	}
}

func chapterBodyToWire(body ChapterBody) (string, string, json.RawMessage, error) {
	switch body := body.(type) {
	case JobStartChapter:
		return chapterTypeJobStart, payloadKindApp, cloneRawMessage(body.Input.Data), nil
	case *JobStartChapter:
		if body == nil {
			return "", "", nil, fmt.Errorf("chapter body is required")
		}
		return chapterTypeJobStart, payloadKindApp, cloneRawMessage(body.Input.Data), nil
	case JobAttemptOutcomeChapter:
		payloadKind, data, err := chapterOutcomeToWire(body.Outcome)
		return chapterTypeJobAttemptOutcome, payloadKind, data, err
	case *JobAttemptOutcomeChapter:
		if body == nil {
			return "", "", nil, fmt.Errorf("chapter body is required")
		}
		payloadKind, data, err := chapterOutcomeToWire(body.Outcome)
		return chapterTypeJobAttemptOutcome, payloadKind, data, err
	case TaskAttemptOutcomeChapter:
		payloadKind, data, err := chapterOutcomeToWire(body.Outcome)
		return chapterTypeTaskAttemptOutcome, payloadKind, data, err
	case *TaskAttemptOutcomeChapter:
		if body == nil {
			return "", "", nil, fmt.Errorf("chapter body is required")
		}
		payloadKind, data, err := chapterOutcomeToWire(body.Outcome)
		return chapterTypeTaskAttemptOutcome, payloadKind, data, err
	case RestartExtraChapter:
		return chapterTypeRestartExtra, payloadKindApp, cloneRawMessage(body.Output.Data), nil
	case *RestartExtraChapter:
		if body == nil {
			return "", "", nil, fmt.Errorf("chapter body is required")
		}
		return chapterTypeRestartExtra, payloadKindApp, cloneRawMessage(body.Output.Data), nil
	default:
		return "", "", nil, fmt.Errorf("unsupported chapter body type %T", body)
	}
}

func chapterOutcomeToWire(outcome ChapterOutcome) (string, json.RawMessage, error) {
	switch outcome := outcome.(type) {
	case ApplicationOutputOutcome:
		return payloadKindApp, cloneRawMessage(outcome.Output.Data), nil
	case *ApplicationOutputOutcome:
		if outcome == nil {
			return "", nil, fmt.Errorf("task outcome is required")
		}
		return payloadKindApp, cloneRawMessage(outcome.Output.Data), nil
	case AppErrorOutcome:
		raw, err := json.Marshal(outcome.Error)
		return payloadKindAppError, json.RawMessage(raw), err
	case *AppErrorOutcome:
		if outcome == nil {
			return "", nil, fmt.Errorf("task outcome is required")
		}
		raw, err := json.Marshal(outcome.Error)
		return payloadKindAppError, json.RawMessage(raw), err
	case SystemErrorOutcome:
		raw, err := json.Marshal(outcome.Error)
		return payloadKindSystemError, json.RawMessage(raw), err
	case *SystemErrorOutcome:
		if outcome == nil {
			return "", nil, fmt.Errorf("task outcome is required")
		}
		raw, err := json.Marshal(outcome.Error)
		return payloadKindSystemError, json.RawMessage(raw), err
	case TimeoutOutcome:
		raw, err := json.Marshal(outcome.Timeout)
		return payloadKindTimeout, json.RawMessage(raw), err
	case *TimeoutOutcome:
		if outcome == nil {
			return "", nil, fmt.Errorf("task outcome is required")
		}
		raw, err := json.Marshal(outcome.Timeout)
		return payloadKindTimeout, json.RawMessage(raw), err
	default:
		return "", nil, fmt.Errorf("unsupported task outcome type %T", outcome)
	}
}

func chapterMetadataValueFromAny(value any) (ChapterMetadataValue, error) {
	switch v := value.(type) {
	case nil:
		return ChapterMetadataValue{Kind: ChapterMetadataNull}, nil
	case bool:
		return ChapterMetadataValue{Kind: ChapterMetadataBool, Bool: v}, nil
	case string:
		return ChapterMetadataValue{Kind: ChapterMetadataString, String: v}, nil
	case json.Number:
		return chapterMetadataNumber(v)
	case int:
		return ChapterMetadataValue{Kind: ChapterMetadataInt, Int: int64(v)}, nil
	case int8:
		return ChapterMetadataValue{Kind: ChapterMetadataInt, Int: int64(v)}, nil
	case int16:
		return ChapterMetadataValue{Kind: ChapterMetadataInt, Int: int64(v)}, nil
	case int32:
		return ChapterMetadataValue{Kind: ChapterMetadataInt, Int: int64(v)}, nil
	case int64:
		return ChapterMetadataValue{Kind: ChapterMetadataInt, Int: v}, nil
	case uint:
		return chapterMetadataUint(uint64(v))
	case uint8:
		return chapterMetadataUint(uint64(v))
	case uint16:
		return chapterMetadataUint(uint64(v))
	case uint32:
		return chapterMetadataUint(uint64(v))
	case uint64:
		return chapterMetadataUint(v)
	case float32:
		return chapterMetadataFloat(float64(v))
	case float64:
		return chapterMetadataFloat(v)
	case []any:
		out := make([]ChapterMetadataValue, 0, len(v))
		for i, item := range v {
			converted, err := chapterMetadataValueFromAny(item)
			if err != nil {
				return ChapterMetadataValue{}, fmt.Errorf("[%d]: %w", i, err)
			}
			out = append(out, converted)
		}
		return ChapterMetadataValue{Kind: ChapterMetadataList, List: out}, nil
	case map[string]any:
		out := make(map[string]ChapterMetadataValue, len(v))
		for name, item := range v {
			converted, err := chapterMetadataValueFromAny(item)
			if err != nil {
				return ChapterMetadataValue{}, fmt.Errorf("%s: %w", name, err)
			}
			out[name] = converted
		}
		return ChapterMetadataValue{Kind: ChapterMetadataMap, Map: out}, nil
	case json.RawMessage:
		if !json.Valid(v) {
			return ChapterMetadataValue{}, fmt.Errorf("raw metadata value must be valid JSON")
		}
		decoded, err := decodeJSONValue(v)
		if err != nil {
			return ChapterMetadataValue{}, err
		}
		return chapterMetadataValueFromAny(decoded)
	default:
		raw, err := json.Marshal(value)
		if err != nil {
			return ChapterMetadataValue{}, err
		}
		if !json.Valid(raw) {
			return ChapterMetadataValue{}, fmt.Errorf("metadata value cannot be represented as JSON")
		}
		decoded, err := decodeJSONValue(raw)
		if err != nil {
			return ChapterMetadataValue{}, err
		}
		return chapterMetadataValueFromAny(decoded)
	}
}

func chapterMetadataNumber(value json.Number) (ChapterMetadataValue, error) {
	text := value.String()
	if !strings.ContainsAny(text, ".eE") {
		if i, err := value.Int64(); err == nil {
			return ChapterMetadataValue{Kind: ChapterMetadataInt, Int: i}, nil
		}
	}
	f, err := strconv.ParseFloat(text, 64)
	if err != nil {
		return ChapterMetadataValue{}, err
	}
	return chapterMetadataFloat(f)
}

func chapterMetadataUint(value uint64) (ChapterMetadataValue, error) {
	const maxInt64 = uint64(1<<63 - 1)
	if value > maxInt64 {
		return ChapterMetadataValue{}, fmt.Errorf("uint64 value %d exceeds int64 metadata range", value)
	}
	return ChapterMetadataValue{Kind: ChapterMetadataInt, Int: int64(value)}, nil
}

func chapterMetadataFloat(value float64) (ChapterMetadataValue, error) {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return ChapterMetadataValue{}, fmt.Errorf("floating-point metadata values must be finite")
	}
	return ChapterMetadataValue{Kind: ChapterMetadataDouble, Double: value}, nil
}

func chapterMetadataFieldsToAny(fields map[string]ChapterMetadataValue) map[string]any {
	out := make(map[string]any, len(fields))
	for name, value := range fields {
		out[name] = chapterMetadataValueToAny(value)
	}
	return out
}

func chapterMetadataValueToAny(value ChapterMetadataValue) any {
	switch value.Kind {
	case ChapterMetadataNull:
		return nil
	case ChapterMetadataBool:
		return value.Bool
	case ChapterMetadataInt:
		return value.Int
	case ChapterMetadataDouble:
		return value.Double
	case ChapterMetadataString:
		return value.String
	case ChapterMetadataList:
		out := make([]any, 0, len(value.List))
		for _, item := range value.List {
			out = append(out, chapterMetadataValueToAny(item))
		}
		return out
	case ChapterMetadataMap:
		return chapterMetadataFieldsToAny(value.Map)
	default:
		return nil
	}
}

func decodeJSONValue(raw []byte) (any, error) {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return nil, err
	}
	return value, nil
}

func cloneRawMessage(raw []byte) json.RawMessage {
	if raw == nil {
		return nil
	}
	return append(json.RawMessage(nil), raw...)
}

func cloneStringPtr(src *string) *string {
	if src == nil {
		return nil
	}
	value := *src
	return &value
}

func strPtr(s string) *string {
	if s == "" {
		return nil
	}
	value := s
	return &value
}

func int64Ptr(v int64) *int64 {
	if v <= 0 {
		return nil
	}
	value := v
	return &value
}

func cloneChapterBody(body ChapterBody) ChapterBody {
	switch body := body.(type) {
	case JobStartChapter:
		body.Input.Data = cloneRawMessage(body.Input.Data)
		return body
	case *JobStartChapter:
		if body == nil {
			return nil
		}
		cloned := *body
		cloned.Input.Data = cloneRawMessage(body.Input.Data)
		return &cloned
	case JobAttemptOutcomeChapter:
		body.Outcome = cloneChapterOutcome(body.Outcome)
		return body
	case *JobAttemptOutcomeChapter:
		if body == nil {
			return nil
		}
		cloned := *body
		cloned.Outcome = cloneChapterOutcome(body.Outcome)
		return &cloned
	case TaskAttemptOutcomeChapter:
		body.Outcome = cloneChapterOutcome(body.Outcome)
		return body
	case *TaskAttemptOutcomeChapter:
		if body == nil {
			return nil
		}
		cloned := *body
		cloned.Outcome = cloneChapterOutcome(body.Outcome)
		return &cloned
	case RestartExtraChapter:
		body.Output.Data = cloneRawMessage(body.Output.Data)
		return body
	case *RestartExtraChapter:
		if body == nil {
			return nil
		}
		cloned := *body
		cloned.Output.Data = cloneRawMessage(body.Output.Data)
		return &cloned
	default:
		return body
	}
}

func cloneChapterOutcome(outcome ChapterOutcome) ChapterOutcome {
	switch outcome := outcome.(type) {
	case ApplicationOutputOutcome:
		outcome.Output.Data = cloneRawMessage(outcome.Output.Data)
		return outcome
	case *ApplicationOutputOutcome:
		if outcome == nil {
			return nil
		}
		cloned := *outcome
		cloned.Output.Data = cloneRawMessage(outcome.Output.Data)
		return &cloned
	case AppErrorOutcome:
		outcome.Error.Attrs = cloneAttrs(outcome.Error.Attrs)
		outcome.Error.Stacktrace = append([]string(nil), outcome.Error.Stacktrace...)
		return outcome
	case *AppErrorOutcome:
		if outcome == nil {
			return nil
		}
		cloned := *outcome
		cloned.Error.Attrs = cloneAttrs(outcome.Error.Attrs)
		cloned.Error.Stacktrace = append([]string(nil), outcome.Error.Stacktrace...)
		return &cloned
	case SystemErrorOutcome:
		outcome.Error.Stacktrace = append([]string(nil), outcome.Error.Stacktrace...)
		return outcome
	case *SystemErrorOutcome:
		if outcome == nil {
			return nil
		}
		cloned := *outcome
		cloned.Error.Stacktrace = append([]string(nil), outcome.Error.Stacktrace...)
		return &cloned
	case TimeoutOutcome:
		return outcome
	case *TimeoutOutcome:
		if outcome == nil {
			return nil
		}
		cloned := *outcome
		return &cloned
	default:
		return outcome
	}
}

func cloneChapterMetadata(metadata ChapterMetadata) ChapterMetadata {
	if metadata.Fields == nil {
		return ChapterMetadata{}
	}
	fields := make(map[string]ChapterMetadataValue, len(metadata.Fields))
	for name, value := range metadata.Fields {
		fields[name] = cloneChapterMetadataValue(value)
	}
	return ChapterMetadata{Fields: fields}
}

func cloneChapterMetadataValue(value ChapterMetadataValue) ChapterMetadataValue {
	cloned := value
	if len(value.List) > 0 {
		cloned.List = make([]ChapterMetadataValue, 0, len(value.List))
		for _, item := range value.List {
			cloned.List = append(cloned.List, cloneChapterMetadataValue(item))
		}
	}
	if len(value.Map) > 0 {
		cloned.Map = make(map[string]ChapterMetadataValue, len(value.Map))
		for name, item := range value.Map {
			cloned.Map[name] = cloneChapterMetadataValue(item)
		}
	}
	return cloned
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

	var jobFailed *JobFailedError
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
		return &JobFailedError{Cause: &TimeoutError{Payload: TimeoutPayload{
			Scope:     attrString(payload.Attrs, jobFailedScopeAttrKey),
			After:     after,
			Retryable: attrBool(payload.Attrs, jobFailedRetryableAttrKey),
			InputRef:  payload.InputRef,
			Component: attrString(payload.Attrs, jobFailedComponentAttrKey),
			Code:      attrString(payload.Attrs, jobFailedCodeAttrKey),
			Message:   payload.Message,
		}}}, true
	case TaskErrorKindSystem:
		return &JobFailedError{Cause: &SystemError{Payload: SystemErrorPayload{
			Message:    payload.Message,
			Component:  attrString(payload.Attrs, jobFailedComponentAttrKey),
			Code:       attrString(payload.Attrs, jobFailedCodeAttrKey),
			Retryable:  attrBool(payload.Attrs, jobFailedRetryableAttrKey),
			InputRef:   payload.InputRef,
			Stacktrace: append([]string(nil), payload.Stacktrace...),
		}}}, true
	default:
		return &JobFailedError{Cause: &AppError{Payload: AppErrorPayload{
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

func trimJobFailedPrefix(message string) string {
	prefix := ErrJobFailed.Error() + ": "
	if strings.HasPrefix(message, prefix) {
		return strings.TrimPrefix(message, prefix)
	}
	return message
}

func normalizeComparableError(err error) error {
	switch e := err.(type) {
	case nil:
		return nil
	case *JobFailedError:
		if e == nil {
			return nil
		}
		cause := normalizeComparableError(e.Cause)
		if cause == e.Cause {
			return e
		}
		return &JobFailedError{Cause: cause}
	case *AppError:
		return e
	case AppError:
		copy := e
		copy.Payload.Attrs = cloneAttrs(e.Payload.Attrs)
		copy.Payload.Stacktrace = append([]string(nil), e.Payload.Stacktrace...)
		return &copy
	case *SystemError:
		return e
	case SystemError:
		copy := e
		copy.Payload.Stacktrace = append([]string(nil), e.Payload.Stacktrace...)
		return &copy
	case *TimeoutError:
		return e
	case TimeoutError:
		copy := e
		return &copy
	default:
		return err
	}
}
