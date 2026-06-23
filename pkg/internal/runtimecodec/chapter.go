package runtimecodec

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/colony-2/jobdb/pkg/jobdb"
)

// ChapterType returns the storage discriminator for a typed chapter.
func ChapterType(chapter jobdb.Chapter) (string, error) {
	chapterType, _, _, err := ChapterBodyToWire(chapter.Body)
	return chapterType, err
}

// ChapterPayload returns the storage payload discriminator and payload bytes for
// a typed chapter.
func ChapterPayload(chapter jobdb.Chapter) (string, json.RawMessage, error) {
	_, payloadKind, payload, err := ChapterBodyToWire(chapter.Body)
	return payloadKind, payload, err
}

// ChapterIs reports whether a typed chapter maps to the requested storage
// chapter discriminator.
func ChapterIs(chapter jobdb.Chapter, want string) bool {
	got, err := ChapterType(chapter)
	return err == nil && got == want
}

func ChapterBodyFromWire(chapterType string, payloadKind string, data json.RawMessage) (jobdb.ChapterBody, error) {
	switch chapterType {
	case ChapterTypeJobStart:
		if payloadKind != PayloadKindApp {
			return nil, fmt.Errorf("%s chapters require %s payloads", chapterType, PayloadKindApp)
		}
		return jobdb.JobStartChapter{Input: jobdb.ApplicationInputBytes{Data: cloneJSON(data)}}, nil
	case ChapterTypeJobAttemptOutcome:
		outcome, err := ChapterOutcomeFromWire(payloadKind, data)
		if err != nil {
			return nil, err
		}
		return jobdb.JobAttemptOutcomeChapter{Outcome: outcome}, nil
	case ChapterTypeTaskAttemptOutcome:
		outcome, err := ChapterOutcomeFromWire(payloadKind, data)
		if err != nil {
			return nil, err
		}
		return jobdb.TaskAttemptOutcomeChapter{Outcome: outcome}, nil
	case ChapterTypeRestartExtra:
		if payloadKind != PayloadKindApp {
			return nil, fmt.Errorf("%s chapters require %s payloads", chapterType, PayloadKindApp)
		}
		return jobdb.RestartExtraChapter{Output: jobdb.ApplicationOutputBytes{Data: cloneJSON(data)}}, nil
	default:
		return nil, fmt.Errorf("unsupported chapter type %q", chapterType)
	}
}

func ChapterOutcomeFromWire(payloadKind string, data json.RawMessage) (jobdb.ChapterOutcome, error) {
	switch payloadKind {
	case PayloadKindApp:
		return jobdb.ApplicationOutputOutcome{Output: jobdb.ApplicationOutputBytes{Data: cloneJSON(data)}}, nil
	case PayloadKindAppError:
		var payload jobdb.AppErrorPayload
		if err := json.Unmarshal(data, &payload); err != nil {
			return nil, err
		}
		return jobdb.AppErrorOutcome{Error: payload}, nil
	case PayloadKindSystemError:
		var payload jobdb.SystemErrorPayload
		if err := json.Unmarshal(data, &payload); err != nil {
			return nil, err
		}
		return jobdb.SystemErrorOutcome{Error: payload}, nil
	case PayloadKindTimeout:
		var payload jobdb.TimeoutPayload
		if err := json.Unmarshal(data, &payload); err != nil {
			return nil, err
		}
		return jobdb.TimeoutOutcome{Timeout: payload}, nil
	default:
		return nil, fmt.Errorf("unsupported task outcome payload kind %q", payloadKind)
	}
}

func ChapterBodyToWire(body jobdb.ChapterBody) (string, string, json.RawMessage, error) {
	switch body := body.(type) {
	case jobdb.JobStartChapter:
		return ChapterTypeJobStart, PayloadKindApp, cloneJSON(body.Input.Data), nil
	case *jobdb.JobStartChapter:
		if body == nil {
			return "", "", nil, fmt.Errorf("chapter body is required")
		}
		return ChapterTypeJobStart, PayloadKindApp, cloneJSON(body.Input.Data), nil
	case jobdb.JobAttemptOutcomeChapter:
		payloadKind, data, err := ChapterOutcomeToWire(body.Outcome)
		return ChapterTypeJobAttemptOutcome, payloadKind, data, err
	case *jobdb.JobAttemptOutcomeChapter:
		if body == nil {
			return "", "", nil, fmt.Errorf("chapter body is required")
		}
		payloadKind, data, err := ChapterOutcomeToWire(body.Outcome)
		return ChapterTypeJobAttemptOutcome, payloadKind, data, err
	case jobdb.TaskAttemptOutcomeChapter:
		payloadKind, data, err := ChapterOutcomeToWire(body.Outcome)
		return ChapterTypeTaskAttemptOutcome, payloadKind, data, err
	case *jobdb.TaskAttemptOutcomeChapter:
		if body == nil {
			return "", "", nil, fmt.Errorf("chapter body is required")
		}
		payloadKind, data, err := ChapterOutcomeToWire(body.Outcome)
		return ChapterTypeTaskAttemptOutcome, payloadKind, data, err
	case jobdb.RestartExtraChapter:
		return ChapterTypeRestartExtra, PayloadKindApp, cloneJSON(body.Output.Data), nil
	case *jobdb.RestartExtraChapter:
		if body == nil {
			return "", "", nil, fmt.Errorf("chapter body is required")
		}
		return ChapterTypeRestartExtra, PayloadKindApp, cloneJSON(body.Output.Data), nil
	default:
		return "", "", nil, fmt.Errorf("unsupported chapter body type %T", body)
	}
}

func ChapterOutcomeToWire(outcome jobdb.ChapterOutcome) (string, json.RawMessage, error) {
	switch outcome := outcome.(type) {
	case jobdb.ApplicationOutputOutcome:
		return PayloadKindApp, cloneJSON(outcome.Output.Data), nil
	case *jobdb.ApplicationOutputOutcome:
		if outcome == nil {
			return "", nil, fmt.Errorf("task outcome is required")
		}
		return PayloadKindApp, cloneJSON(outcome.Output.Data), nil
	case jobdb.AppErrorOutcome:
		raw, err := json.Marshal(outcome.Error)
		return PayloadKindAppError, json.RawMessage(raw), err
	case *jobdb.AppErrorOutcome:
		if outcome == nil {
			return "", nil, fmt.Errorf("task outcome is required")
		}
		raw, err := json.Marshal(outcome.Error)
		return PayloadKindAppError, json.RawMessage(raw), err
	case jobdb.SystemErrorOutcome:
		raw, err := json.Marshal(outcome.Error)
		return PayloadKindSystemError, json.RawMessage(raw), err
	case *jobdb.SystemErrorOutcome:
		if outcome == nil {
			return "", nil, fmt.Errorf("task outcome is required")
		}
		raw, err := json.Marshal(outcome.Error)
		return PayloadKindSystemError, json.RawMessage(raw), err
	case jobdb.TimeoutOutcome:
		raw, err := json.Marshal(outcome.Timeout)
		return PayloadKindTimeout, json.RawMessage(raw), err
	case *jobdb.TimeoutOutcome:
		if outcome == nil {
			return "", nil, fmt.Errorf("task outcome is required")
		}
		raw, err := json.Marshal(outcome.Timeout)
		return PayloadKindTimeout, json.RawMessage(raw), err
	default:
		return "", nil, fmt.Errorf("unsupported task outcome type %T", outcome)
	}
}

func ChapterMetadataFromJSON(raw json.RawMessage) (jobdb.ChapterMetadata, error) {
	if len(raw) == 0 {
		return jobdb.ChapterMetadata{}, nil
	}
	if !json.Valid(raw) {
		return jobdb.ChapterMetadata{}, fmt.Errorf("metadata must be valid JSON")
	}
	value, err := decodeJSONValue(raw)
	if err != nil {
		return jobdb.ChapterMetadata{}, err
	}
	if value == nil {
		return jobdb.ChapterMetadata{}, nil
	}
	fields, ok := value.(map[string]any)
	if !ok {
		return jobdb.ChapterMetadata{}, fmt.Errorf("metadata must be a JSON object")
	}
	out := jobdb.ChapterMetadata{Fields: make(map[string]jobdb.ChapterMetadataValue, len(fields))}
	for name, value := range fields {
		converted, err := chapterMetadataValueFromAny(value)
		if err != nil {
			return jobdb.ChapterMetadata{}, fmt.Errorf("%s: %w", name, err)
		}
		out.Fields[name] = converted
	}
	return out, nil
}

func ChapterMetadataToJSON(metadata jobdb.ChapterMetadata) (json.RawMessage, error) {
	if metadata.Fields == nil {
		return nil, nil
	}
	raw, err := json.Marshal(chapterMetadataFieldsToAny(metadata.Fields))
	if err != nil {
		return nil, err
	}
	return json.RawMessage(raw), nil
}

func CloneChapter(chapter jobdb.Chapter) jobdb.Chapter {
	out := chapter
	out.Body = CloneChapterBody(chapter.Body)
	out.Metadata = CloneChapterMetadata(chapter.Metadata)
	if len(chapter.Artifacts) > 0 {
		out.Artifacts = append([]jobdb.StoredArtifact(nil), chapter.Artifacts...)
	}
	return out
}

func CloneChapterBody(body jobdb.ChapterBody) jobdb.ChapterBody {
	switch body := body.(type) {
	case jobdb.JobStartChapter:
		body.Input.Data = cloneJSON(body.Input.Data)
		return body
	case *jobdb.JobStartChapter:
		if body == nil {
			return nil
		}
		cloned := *body
		cloned.Input.Data = cloneJSON(body.Input.Data)
		return &cloned
	case jobdb.JobAttemptOutcomeChapter:
		body.Outcome = CloneChapterOutcome(body.Outcome)
		return body
	case *jobdb.JobAttemptOutcomeChapter:
		if body == nil {
			return nil
		}
		cloned := *body
		cloned.Outcome = CloneChapterOutcome(body.Outcome)
		return &cloned
	case jobdb.TaskAttemptOutcomeChapter:
		body.Outcome = CloneChapterOutcome(body.Outcome)
		return body
	case *jobdb.TaskAttemptOutcomeChapter:
		if body == nil {
			return nil
		}
		cloned := *body
		cloned.Outcome = CloneChapterOutcome(body.Outcome)
		return &cloned
	case jobdb.RestartExtraChapter:
		body.Output.Data = cloneJSON(body.Output.Data)
		return body
	case *jobdb.RestartExtraChapter:
		if body == nil {
			return nil
		}
		cloned := *body
		cloned.Output.Data = cloneJSON(body.Output.Data)
		return &cloned
	default:
		return body
	}
}

func CloneChapterOutcome(outcome jobdb.ChapterOutcome) jobdb.ChapterOutcome {
	switch outcome := outcome.(type) {
	case jobdb.ApplicationOutputOutcome:
		outcome.Output.Data = cloneJSON(outcome.Output.Data)
		return outcome
	case *jobdb.ApplicationOutputOutcome:
		if outcome == nil {
			return nil
		}
		cloned := *outcome
		cloned.Output.Data = cloneJSON(outcome.Output.Data)
		return &cloned
	case jobdb.AppErrorOutcome:
		outcome.Error = cloneAppErrorPayload(outcome.Error)
		return outcome
	case *jobdb.AppErrorOutcome:
		if outcome == nil {
			return nil
		}
		cloned := *outcome
		cloned.Error = cloneAppErrorPayload(outcome.Error)
		return &cloned
	case jobdb.SystemErrorOutcome:
		outcome.Error = cloneSystemErrorPayload(outcome.Error)
		return outcome
	case *jobdb.SystemErrorOutcome:
		if outcome == nil {
			return nil
		}
		cloned := *outcome
		cloned.Error = cloneSystemErrorPayload(outcome.Error)
		return &cloned
	case jobdb.TimeoutOutcome:
		outcome.Timeout = cloneTimeoutPayload(outcome.Timeout)
		return outcome
	case *jobdb.TimeoutOutcome:
		if outcome == nil {
			return nil
		}
		cloned := *outcome
		cloned.Timeout = cloneTimeoutPayload(outcome.Timeout)
		return &cloned
	default:
		return outcome
	}
}

func CloneChapterMetadata(metadata jobdb.ChapterMetadata) jobdb.ChapterMetadata {
	if metadata.Fields == nil {
		return jobdb.ChapterMetadata{}
	}
	return jobdb.ChapterMetadata{Fields: cloneChapterMetadataFields(metadata.Fields)}
}

func chapterMetadataValueFromAny(value any) (jobdb.ChapterMetadataValue, error) {
	switch v := value.(type) {
	case nil:
		return jobdb.ChapterMetadataValue{Kind: jobdb.ChapterMetadataNull}, nil
	case bool:
		return jobdb.ChapterMetadataValue{Kind: jobdb.ChapterMetadataBool, Bool: v}, nil
	case string:
		return jobdb.ChapterMetadataValue{Kind: jobdb.ChapterMetadataString, String: v}, nil
	case json.Number:
		return chapterMetadataNumber(v)
	case int:
		return jobdb.ChapterMetadataValue{Kind: jobdb.ChapterMetadataInt, Int: int64(v)}, nil
	case int8:
		return jobdb.ChapterMetadataValue{Kind: jobdb.ChapterMetadataInt, Int: int64(v)}, nil
	case int16:
		return jobdb.ChapterMetadataValue{Kind: jobdb.ChapterMetadataInt, Int: int64(v)}, nil
	case int32:
		return jobdb.ChapterMetadataValue{Kind: jobdb.ChapterMetadataInt, Int: int64(v)}, nil
	case int64:
		return jobdb.ChapterMetadataValue{Kind: jobdb.ChapterMetadataInt, Int: v}, nil
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
		out := make([]jobdb.ChapterMetadataValue, 0, len(v))
		for i, item := range v {
			converted, err := chapterMetadataValueFromAny(item)
			if err != nil {
				return jobdb.ChapterMetadataValue{}, fmt.Errorf("[%d]: %w", i, err)
			}
			out = append(out, converted)
		}
		return jobdb.ChapterMetadataValue{Kind: jobdb.ChapterMetadataList, List: out}, nil
	case map[string]any:
		out := make(map[string]jobdb.ChapterMetadataValue, len(v))
		for name, item := range v {
			converted, err := chapterMetadataValueFromAny(item)
			if err != nil {
				return jobdb.ChapterMetadataValue{}, fmt.Errorf("%s: %w", name, err)
			}
			out[name] = converted
		}
		return jobdb.ChapterMetadataValue{Kind: jobdb.ChapterMetadataMap, Map: out}, nil
	case json.RawMessage:
		if !json.Valid(v) {
			return jobdb.ChapterMetadataValue{}, fmt.Errorf("raw metadata value must be valid JSON")
		}
		decoded, err := decodeJSONValue(v)
		if err != nil {
			return jobdb.ChapterMetadataValue{}, err
		}
		return chapterMetadataValueFromAny(decoded)
	default:
		raw, err := json.Marshal(value)
		if err != nil {
			return jobdb.ChapterMetadataValue{}, err
		}
		if !json.Valid(raw) {
			return jobdb.ChapterMetadataValue{}, fmt.Errorf("metadata value cannot be represented as JSON")
		}
		decoded, err := decodeJSONValue(raw)
		if err != nil {
			return jobdb.ChapterMetadataValue{}, err
		}
		return chapterMetadataValueFromAny(decoded)
	}
}

func chapterMetadataNumber(value json.Number) (jobdb.ChapterMetadataValue, error) {
	text := value.String()
	if !strings.ContainsAny(text, ".eE") {
		if i, err := value.Int64(); err == nil {
			return jobdb.ChapterMetadataValue{Kind: jobdb.ChapterMetadataInt, Int: i}, nil
		}
	}
	f, err := strconv.ParseFloat(text, 64)
	if err != nil {
		return jobdb.ChapterMetadataValue{}, err
	}
	return chapterMetadataFloat(f)
}

func chapterMetadataUint(value uint64) (jobdb.ChapterMetadataValue, error) {
	const maxInt64 = uint64(1<<63 - 1)
	if value > maxInt64 {
		return jobdb.ChapterMetadataValue{}, fmt.Errorf("uint64 value %d exceeds int64 metadata range", value)
	}
	return jobdb.ChapterMetadataValue{Kind: jobdb.ChapterMetadataInt, Int: int64(value)}, nil
}

func chapterMetadataFloat(value float64) (jobdb.ChapterMetadataValue, error) {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return jobdb.ChapterMetadataValue{}, fmt.Errorf("floating-point metadata values must be finite")
	}
	return jobdb.ChapterMetadataValue{Kind: jobdb.ChapterMetadataDouble, Double: value}, nil
}

func chapterMetadataFieldsToAny(fields map[string]jobdb.ChapterMetadataValue) map[string]any {
	out := make(map[string]any, len(fields))
	for name, value := range fields {
		out[name] = chapterMetadataValueToAny(value)
	}
	return out
}

func chapterMetadataValueToAny(value jobdb.ChapterMetadataValue) any {
	switch value.Kind {
	case jobdb.ChapterMetadataNull:
		return nil
	case jobdb.ChapterMetadataBool:
		return value.Bool
	case jobdb.ChapterMetadataInt:
		return value.Int
	case jobdb.ChapterMetadataDouble:
		return value.Double
	case jobdb.ChapterMetadataString:
		return value.String
	case jobdb.ChapterMetadataList:
		out := make([]any, 0, len(value.List))
		for _, item := range value.List {
			out = append(out, chapterMetadataValueToAny(item))
		}
		return out
	case jobdb.ChapterMetadataMap:
		return chapterMetadataFieldsToAny(value.Map)
	default:
		return nil
	}
}

func cloneChapterMetadataFields(fields map[string]jobdb.ChapterMetadataValue) map[string]jobdb.ChapterMetadataValue {
	if fields == nil {
		return nil
	}
	out := make(map[string]jobdb.ChapterMetadataValue, len(fields))
	for name, value := range fields {
		out[name] = cloneChapterMetadataValue(value)
	}
	return out
}

func cloneChapterMetadataValue(value jobdb.ChapterMetadataValue) jobdb.ChapterMetadataValue {
	value.List = append([]jobdb.ChapterMetadataValue(nil), value.List...)
	for i := range value.List {
		value.List[i] = cloneChapterMetadataValue(value.List[i])
	}
	value.Map = cloneChapterMetadataFields(value.Map)
	return value
}

func cloneAppErrorPayload(payload jobdb.AppErrorPayload) jobdb.AppErrorPayload {
	payload.Attrs = cloneAttrs(payload.Attrs)
	payload.Stacktrace = append([]string(nil), payload.Stacktrace...)
	if payload.InputRef != nil {
		ref := *payload.InputRef
		payload.InputRef = &ref
	}
	return payload
}

func cloneSystemErrorPayload(payload jobdb.SystemErrorPayload) jobdb.SystemErrorPayload {
	payload.Stacktrace = append([]string(nil), payload.Stacktrace...)
	if payload.InputRef != nil {
		ref := *payload.InputRef
		payload.InputRef = &ref
	}
	return payload
}

func cloneTimeoutPayload(payload jobdb.TimeoutPayload) jobdb.TimeoutPayload {
	if payload.InputRef != nil {
		ref := *payload.InputRef
		payload.InputRef = &ref
	}
	return payload
}

func cloneAttrs(attrs map[string]any) map[string]any {
	if attrs == nil {
		return nil
	}
	out := make(map[string]any, len(attrs))
	for key, value := range attrs {
		out[key] = cloneAttrValue(value)
	}
	return out
}

func cloneAttrValue(value any) any {
	switch value := value.(type) {
	case map[string]any:
		return cloneAttrs(value)
	case []any:
		out := make([]any, len(value))
		for i, item := range value {
			out[i] = cloneAttrValue(item)
		}
		return out
	case []string:
		return append([]string(nil), value...)
	case json.RawMessage:
		return cloneJSON(value)
	case []byte:
		return append([]byte(nil), value...)
	default:
		return value
	}
}
