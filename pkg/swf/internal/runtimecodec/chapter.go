package runtimecodec

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"

	"github.com/colony-2/swf-go/pkg/swf"
)

// ChapterType returns the storage discriminator for a typed chapter.
func ChapterType(chapter swf.Chapter) (string, error) {
	chapterType, _, _, err := ChapterBodyToWire(chapter.Body)
	return chapterType, err
}

// ChapterPayload returns the storage payload discriminator and payload bytes for
// a typed chapter.
func ChapterPayload(chapter swf.Chapter) (string, json.RawMessage, error) {
	_, payloadKind, payload, err := ChapterBodyToWire(chapter.Body)
	return payloadKind, payload, err
}

// ChapterIs reports whether a typed chapter maps to the requested storage
// chapter discriminator.
func ChapterIs(chapter swf.Chapter, want string) bool {
	got, err := ChapterType(chapter)
	return err == nil && got == want
}

func ChapterBodyFromWire(chapterType string, payloadKind string, data json.RawMessage) (swf.ChapterBody, error) {
	switch chapterType {
	case ChapterTypeJobStart:
		if payloadKind != PayloadKindApp {
			return nil, fmt.Errorf("%s chapters require %s payloads", chapterType, PayloadKindApp)
		}
		return swf.JobStartChapter{Input: swf.ApplicationInputBytes{Data: cloneJSON(data)}}, nil
	case ChapterTypeJobAttemptOutcome:
		outcome, err := ChapterOutcomeFromWire(payloadKind, data)
		if err != nil {
			return nil, err
		}
		return swf.JobAttemptOutcomeChapter{Outcome: outcome}, nil
	case ChapterTypeTaskAttemptOutcome:
		outcome, err := ChapterOutcomeFromWire(payloadKind, data)
		if err != nil {
			return nil, err
		}
		return swf.TaskAttemptOutcomeChapter{Outcome: outcome}, nil
	case ChapterTypeRestartExtra:
		if payloadKind != PayloadKindApp {
			return nil, fmt.Errorf("%s chapters require %s payloads", chapterType, PayloadKindApp)
		}
		return swf.RestartExtraChapter{Output: swf.ApplicationOutputBytes{Data: cloneJSON(data)}}, nil
	default:
		return nil, fmt.Errorf("unsupported chapter type %q", chapterType)
	}
}

func ChapterOutcomeFromWire(payloadKind string, data json.RawMessage) (swf.ChapterOutcome, error) {
	switch payloadKind {
	case PayloadKindApp:
		return swf.ApplicationOutputOutcome{Output: swf.ApplicationOutputBytes{Data: cloneJSON(data)}}, nil
	case PayloadKindAppError:
		var payload swf.AppErrorPayload
		if err := json.Unmarshal(data, &payload); err != nil {
			return nil, err
		}
		return swf.AppErrorOutcome{Error: payload}, nil
	case PayloadKindSystemError:
		var payload swf.SystemErrorPayload
		if err := json.Unmarshal(data, &payload); err != nil {
			return nil, err
		}
		return swf.SystemErrorOutcome{Error: payload}, nil
	case PayloadKindTimeout:
		var payload swf.TimeoutPayload
		if err := json.Unmarshal(data, &payload); err != nil {
			return nil, err
		}
		return swf.TimeoutOutcome{Timeout: payload}, nil
	default:
		return nil, fmt.Errorf("unsupported task outcome payload kind %q", payloadKind)
	}
}

func ChapterBodyToWire(body swf.ChapterBody) (string, string, json.RawMessage, error) {
	switch body := body.(type) {
	case swf.JobStartChapter:
		return ChapterTypeJobStart, PayloadKindApp, cloneJSON(body.Input.Data), nil
	case *swf.JobStartChapter:
		if body == nil {
			return "", "", nil, fmt.Errorf("chapter body is required")
		}
		return ChapterTypeJobStart, PayloadKindApp, cloneJSON(body.Input.Data), nil
	case swf.JobAttemptOutcomeChapter:
		payloadKind, data, err := ChapterOutcomeToWire(body.Outcome)
		return ChapterTypeJobAttemptOutcome, payloadKind, data, err
	case *swf.JobAttemptOutcomeChapter:
		if body == nil {
			return "", "", nil, fmt.Errorf("chapter body is required")
		}
		payloadKind, data, err := ChapterOutcomeToWire(body.Outcome)
		return ChapterTypeJobAttemptOutcome, payloadKind, data, err
	case swf.TaskAttemptOutcomeChapter:
		payloadKind, data, err := ChapterOutcomeToWire(body.Outcome)
		return ChapterTypeTaskAttemptOutcome, payloadKind, data, err
	case *swf.TaskAttemptOutcomeChapter:
		if body == nil {
			return "", "", nil, fmt.Errorf("chapter body is required")
		}
		payloadKind, data, err := ChapterOutcomeToWire(body.Outcome)
		return ChapterTypeTaskAttemptOutcome, payloadKind, data, err
	case swf.RestartExtraChapter:
		return ChapterTypeRestartExtra, PayloadKindApp, cloneJSON(body.Output.Data), nil
	case *swf.RestartExtraChapter:
		if body == nil {
			return "", "", nil, fmt.Errorf("chapter body is required")
		}
		return ChapterTypeRestartExtra, PayloadKindApp, cloneJSON(body.Output.Data), nil
	default:
		return "", "", nil, fmt.Errorf("unsupported chapter body type %T", body)
	}
}

func ChapterOutcomeToWire(outcome swf.ChapterOutcome) (string, json.RawMessage, error) {
	switch outcome := outcome.(type) {
	case swf.ApplicationOutputOutcome:
		return PayloadKindApp, cloneJSON(outcome.Output.Data), nil
	case *swf.ApplicationOutputOutcome:
		if outcome == nil {
			return "", nil, fmt.Errorf("task outcome is required")
		}
		return PayloadKindApp, cloneJSON(outcome.Output.Data), nil
	case swf.AppErrorOutcome:
		raw, err := json.Marshal(outcome.Error)
		return PayloadKindAppError, json.RawMessage(raw), err
	case *swf.AppErrorOutcome:
		if outcome == nil {
			return "", nil, fmt.Errorf("task outcome is required")
		}
		raw, err := json.Marshal(outcome.Error)
		return PayloadKindAppError, json.RawMessage(raw), err
	case swf.SystemErrorOutcome:
		raw, err := json.Marshal(outcome.Error)
		return PayloadKindSystemError, json.RawMessage(raw), err
	case *swf.SystemErrorOutcome:
		if outcome == nil {
			return "", nil, fmt.Errorf("task outcome is required")
		}
		raw, err := json.Marshal(outcome.Error)
		return PayloadKindSystemError, json.RawMessage(raw), err
	case swf.TimeoutOutcome:
		raw, err := json.Marshal(outcome.Timeout)
		return PayloadKindTimeout, json.RawMessage(raw), err
	case *swf.TimeoutOutcome:
		if outcome == nil {
			return "", nil, fmt.Errorf("task outcome is required")
		}
		raw, err := json.Marshal(outcome.Timeout)
		return PayloadKindTimeout, json.RawMessage(raw), err
	default:
		return "", nil, fmt.Errorf("unsupported task outcome type %T", outcome)
	}
}

func ChapterMetadataFromJSON(raw json.RawMessage) (swf.ChapterMetadata, error) {
	if len(raw) == 0 {
		return swf.ChapterMetadata{}, nil
	}
	if !json.Valid(raw) {
		return swf.ChapterMetadata{}, fmt.Errorf("metadata must be valid JSON")
	}
	value, err := decodeJSONValue(raw)
	if err != nil {
		return swf.ChapterMetadata{}, err
	}
	if value == nil {
		return swf.ChapterMetadata{}, nil
	}
	fields, ok := value.(map[string]any)
	if !ok {
		return swf.ChapterMetadata{}, fmt.Errorf("metadata must be a JSON object")
	}
	out := swf.ChapterMetadata{Fields: make(map[string]swf.ChapterMetadataValue, len(fields))}
	for name, value := range fields {
		converted, err := chapterMetadataValueFromAny(value)
		if err != nil {
			return swf.ChapterMetadata{}, fmt.Errorf("%s: %w", name, err)
		}
		out.Fields[name] = converted
	}
	return out, nil
}

func ChapterMetadataToJSON(metadata swf.ChapterMetadata) (json.RawMessage, error) {
	if metadata.Fields == nil {
		return nil, nil
	}
	raw, err := json.Marshal(chapterMetadataFieldsToAny(metadata.Fields))
	if err != nil {
		return nil, err
	}
	return json.RawMessage(raw), nil
}

func CloneChapter(chapter swf.Chapter) swf.Chapter {
	out := chapter
	out.Body = CloneChapterBody(chapter.Body)
	out.Metadata = CloneChapterMetadata(chapter.Metadata)
	if len(chapter.Artifacts) > 0 {
		out.Artifacts = append([]swf.StoredArtifact(nil), chapter.Artifacts...)
	}
	return out
}

func CloneChapterBody(body swf.ChapterBody) swf.ChapterBody {
	switch body := body.(type) {
	case swf.JobStartChapter:
		body.Input.Data = cloneJSON(body.Input.Data)
		return body
	case *swf.JobStartChapter:
		if body == nil {
			return nil
		}
		cloned := *body
		cloned.Input.Data = cloneJSON(body.Input.Data)
		return &cloned
	case swf.JobAttemptOutcomeChapter:
		body.Outcome = CloneChapterOutcome(body.Outcome)
		return body
	case *swf.JobAttemptOutcomeChapter:
		if body == nil {
			return nil
		}
		cloned := *body
		cloned.Outcome = CloneChapterOutcome(body.Outcome)
		return &cloned
	case swf.TaskAttemptOutcomeChapter:
		body.Outcome = CloneChapterOutcome(body.Outcome)
		return body
	case *swf.TaskAttemptOutcomeChapter:
		if body == nil {
			return nil
		}
		cloned := *body
		cloned.Outcome = CloneChapterOutcome(body.Outcome)
		return &cloned
	case swf.RestartExtraChapter:
		body.Output.Data = cloneJSON(body.Output.Data)
		return body
	case *swf.RestartExtraChapter:
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

func CloneChapterOutcome(outcome swf.ChapterOutcome) swf.ChapterOutcome {
	switch outcome := outcome.(type) {
	case swf.ApplicationOutputOutcome:
		outcome.Output.Data = cloneJSON(outcome.Output.Data)
		return outcome
	case *swf.ApplicationOutputOutcome:
		if outcome == nil {
			return nil
		}
		cloned := *outcome
		cloned.Output.Data = cloneJSON(outcome.Output.Data)
		return &cloned
	case swf.AppErrorOutcome:
		outcome.Error = cloneAppErrorPayload(outcome.Error)
		return outcome
	case *swf.AppErrorOutcome:
		if outcome == nil {
			return nil
		}
		cloned := *outcome
		cloned.Error = cloneAppErrorPayload(outcome.Error)
		return &cloned
	case swf.SystemErrorOutcome:
		outcome.Error = cloneSystemErrorPayload(outcome.Error)
		return outcome
	case *swf.SystemErrorOutcome:
		if outcome == nil {
			return nil
		}
		cloned := *outcome
		cloned.Error = cloneSystemErrorPayload(outcome.Error)
		return &cloned
	case swf.TimeoutOutcome:
		outcome.Timeout = cloneTimeoutPayload(outcome.Timeout)
		return outcome
	case *swf.TimeoutOutcome:
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

func CloneChapterMetadata(metadata swf.ChapterMetadata) swf.ChapterMetadata {
	if metadata.Fields == nil {
		return swf.ChapterMetadata{}
	}
	return swf.ChapterMetadata{Fields: cloneChapterMetadataFields(metadata.Fields)}
}

func chapterMetadataValueFromAny(value any) (swf.ChapterMetadataValue, error) {
	switch v := value.(type) {
	case nil:
		return swf.ChapterMetadataValue{Kind: swf.ChapterMetadataNull}, nil
	case bool:
		return swf.ChapterMetadataValue{Kind: swf.ChapterMetadataBool, Bool: v}, nil
	case string:
		return swf.ChapterMetadataValue{Kind: swf.ChapterMetadataString, String: v}, nil
	case json.Number:
		return chapterMetadataNumber(v)
	case int:
		return swf.ChapterMetadataValue{Kind: swf.ChapterMetadataInt, Int: int64(v)}, nil
	case int8:
		return swf.ChapterMetadataValue{Kind: swf.ChapterMetadataInt, Int: int64(v)}, nil
	case int16:
		return swf.ChapterMetadataValue{Kind: swf.ChapterMetadataInt, Int: int64(v)}, nil
	case int32:
		return swf.ChapterMetadataValue{Kind: swf.ChapterMetadataInt, Int: int64(v)}, nil
	case int64:
		return swf.ChapterMetadataValue{Kind: swf.ChapterMetadataInt, Int: v}, nil
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
		out := make([]swf.ChapterMetadataValue, 0, len(v))
		for i, item := range v {
			converted, err := chapterMetadataValueFromAny(item)
			if err != nil {
				return swf.ChapterMetadataValue{}, fmt.Errorf("[%d]: %w", i, err)
			}
			out = append(out, converted)
		}
		return swf.ChapterMetadataValue{Kind: swf.ChapterMetadataList, List: out}, nil
	case map[string]any:
		out := make(map[string]swf.ChapterMetadataValue, len(v))
		for name, item := range v {
			converted, err := chapterMetadataValueFromAny(item)
			if err != nil {
				return swf.ChapterMetadataValue{}, fmt.Errorf("%s: %w", name, err)
			}
			out[name] = converted
		}
		return swf.ChapterMetadataValue{Kind: swf.ChapterMetadataMap, Map: out}, nil
	case json.RawMessage:
		if !json.Valid(v) {
			return swf.ChapterMetadataValue{}, fmt.Errorf("raw metadata value must be valid JSON")
		}
		decoded, err := decodeJSONValue(v)
		if err != nil {
			return swf.ChapterMetadataValue{}, err
		}
		return chapterMetadataValueFromAny(decoded)
	default:
		raw, err := json.Marshal(value)
		if err != nil {
			return swf.ChapterMetadataValue{}, err
		}
		if !json.Valid(raw) {
			return swf.ChapterMetadataValue{}, fmt.Errorf("metadata value cannot be represented as JSON")
		}
		decoded, err := decodeJSONValue(raw)
		if err != nil {
			return swf.ChapterMetadataValue{}, err
		}
		return chapterMetadataValueFromAny(decoded)
	}
}

func chapterMetadataNumber(value json.Number) (swf.ChapterMetadataValue, error) {
	text := value.String()
	if !strings.ContainsAny(text, ".eE") {
		if i, err := value.Int64(); err == nil {
			return swf.ChapterMetadataValue{Kind: swf.ChapterMetadataInt, Int: i}, nil
		}
	}
	f, err := strconv.ParseFloat(text, 64)
	if err != nil {
		return swf.ChapterMetadataValue{}, err
	}
	return chapterMetadataFloat(f)
}

func chapterMetadataUint(value uint64) (swf.ChapterMetadataValue, error) {
	const maxInt64 = uint64(1<<63 - 1)
	if value > maxInt64 {
		return swf.ChapterMetadataValue{}, fmt.Errorf("uint64 value %d exceeds int64 metadata range", value)
	}
	return swf.ChapterMetadataValue{Kind: swf.ChapterMetadataInt, Int: int64(value)}, nil
}

func chapterMetadataFloat(value float64) (swf.ChapterMetadataValue, error) {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return swf.ChapterMetadataValue{}, fmt.Errorf("floating-point metadata values must be finite")
	}
	return swf.ChapterMetadataValue{Kind: swf.ChapterMetadataDouble, Double: value}, nil
}

func chapterMetadataFieldsToAny(fields map[string]swf.ChapterMetadataValue) map[string]any {
	out := make(map[string]any, len(fields))
	for name, value := range fields {
		out[name] = chapterMetadataValueToAny(value)
	}
	return out
}

func chapterMetadataValueToAny(value swf.ChapterMetadataValue) any {
	switch value.Kind {
	case swf.ChapterMetadataNull:
		return nil
	case swf.ChapterMetadataBool:
		return value.Bool
	case swf.ChapterMetadataInt:
		return value.Int
	case swf.ChapterMetadataDouble:
		return value.Double
	case swf.ChapterMetadataString:
		return value.String
	case swf.ChapterMetadataList:
		out := make([]any, 0, len(value.List))
		for _, item := range value.List {
			out = append(out, chapterMetadataValueToAny(item))
		}
		return out
	case swf.ChapterMetadataMap:
		return chapterMetadataFieldsToAny(value.Map)
	default:
		return nil
	}
}

func cloneChapterMetadataFields(fields map[string]swf.ChapterMetadataValue) map[string]swf.ChapterMetadataValue {
	if fields == nil {
		return nil
	}
	out := make(map[string]swf.ChapterMetadataValue, len(fields))
	for name, value := range fields {
		out[name] = cloneChapterMetadataValue(value)
	}
	return out
}

func cloneChapterMetadataValue(value swf.ChapterMetadataValue) swf.ChapterMetadataValue {
	value.List = append([]swf.ChapterMetadataValue(nil), value.List...)
	for i := range value.List {
		value.List[i] = cloneChapterMetadataValue(value.List[i])
	}
	value.Map = cloneChapterMetadataFields(value.Map)
	return value
}

func cloneAppErrorPayload(payload swf.AppErrorPayload) swf.AppErrorPayload {
	payload.Attrs = cloneAttrs(payload.Attrs)
	payload.Stacktrace = append([]string(nil), payload.Stacktrace...)
	if payload.InputRef != nil {
		ref := *payload.InputRef
		payload.InputRef = &ref
	}
	return payload
}

func cloneSystemErrorPayload(payload swf.SystemErrorPayload) swf.SystemErrorPayload {
	payload.Stacktrace = append([]string(nil), payload.Stacktrace...)
	if payload.InputRef != nil {
		ref := *payload.InputRef
		payload.InputRef = &ref
	}
	return payload
}

func cloneTimeoutPayload(payload swf.TimeoutPayload) swf.TimeoutPayload {
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
