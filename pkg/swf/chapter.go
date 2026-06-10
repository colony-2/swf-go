package swf

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

// Chapter is a durable workflow chapter.
type Chapter struct {
	Ordinal   int64
	TaskType  string
	Body      ChapterBody
	InputHash string
	CreatedAt time.Time
	Metadata  ChapterMetadata
	Artifacts []StoredArtifact
}

// PutChapterRequest stores a typed chapter.
type PutChapterRequest struct {
	LeaseID         string
	LeaseToken      string
	Ref             ChapterRef
	Chapter         Chapter
	ArtifactUploads []ArtifactUpload
}

// ChapterBody is a sealed interface implemented by the supported
// chapter body variants.
type ChapterBody interface {
	chapterBody()
}

// JobStartChapter records the initial application input for a job.
type JobStartChapter struct {
	Input ApplicationInputBytes
}

func (JobStartChapter) chapterBody() {}

// JobAttemptOutcomeChapter records the final outcome for a job attempt.
type JobAttemptOutcomeChapter struct {
	Outcome ChapterOutcome
}

func (JobAttemptOutcomeChapter) chapterBody() {}

// TaskAttemptOutcomeChapter records the outcome for a task attempt.
type TaskAttemptOutcomeChapter struct {
	Outcome ChapterOutcome
}

func (TaskAttemptOutcomeChapter) chapterBody() {}

// RestartExtraChapter records output retained from a restarted job.
type RestartExtraChapter struct {
	Output ApplicationOutputBytes
}

func (RestartExtraChapter) chapterBody() {}

// ChapterOutcome is a sealed interface implemented by the supported
// task outcome variants.
type ChapterOutcome interface {
	chapterOutcome()
}

// ApplicationOutputOutcome contains successful application output bytes.
type ApplicationOutputOutcome struct {
	Output ApplicationOutputBytes
}

func (ApplicationOutputOutcome) chapterOutcome() {}

// AppErrorOutcome contains a user/application error payload.
type AppErrorOutcome struct {
	Error AppErrorPayload
}

func (AppErrorOutcome) chapterOutcome() {}

// SystemErrorOutcome contains an infrastructure/system error payload.
type SystemErrorOutcome struct {
	Error SystemErrorPayload
}

func (SystemErrorOutcome) chapterOutcome() {}

// TimeoutOutcome contains a deterministic timeout payload.
type TimeoutOutcome struct {
	Timeout TimeoutPayload
}

func (TimeoutOutcome) chapterOutcome() {}

// ApplicationInputBytes wraps application input bytes.
type ApplicationInputBytes struct {
	Data []byte
}

// ApplicationOutputBytes wraps application output bytes.
type ApplicationOutputBytes struct {
	Data []byte
}

// ChapterMetadata is chapter metadata.
type ChapterMetadata struct {
	Fields map[string]ChapterMetadataValue
}

// ChapterMetadataKind identifies which value field is populated.
type ChapterMetadataKind string

const (
	ChapterMetadataNull   ChapterMetadataKind = "null"
	ChapterMetadataBool   ChapterMetadataKind = "bool"
	ChapterMetadataInt    ChapterMetadataKind = "int"
	ChapterMetadataDouble ChapterMetadataKind = "double"
	ChapterMetadataString ChapterMetadataKind = "string"
	ChapterMetadataList   ChapterMetadataKind = "list"
	ChapterMetadataMap    ChapterMetadataKind = "map"
)

// ChapterMetadataValue is a chapter metadata value.
type ChapterMetadataValue struct {
	Kind   ChapterMetadataKind
	Bool   bool
	Int    int64
	Double float64
	String string
	List   []ChapterMetadataValue
	Map    map[string]ChapterMetadataValue
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
		outcome.Error = cloneAppErrorPayload(outcome.Error)
		return outcome
	case *AppErrorOutcome:
		if outcome == nil {
			return nil
		}
		cloned := *outcome
		cloned.Error = cloneAppErrorPayload(outcome.Error)
		return &cloned
	case SystemErrorOutcome:
		outcome.Error = cloneSystemErrorPayload(outcome.Error)
		return outcome
	case *SystemErrorOutcome:
		if outcome == nil {
			return nil
		}
		cloned := *outcome
		cloned.Error = cloneSystemErrorPayload(outcome.Error)
		return &cloned
	case TimeoutOutcome:
		outcome.Timeout = cloneTimeoutPayload(outcome.Timeout)
		return outcome
	case *TimeoutOutcome:
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

func cloneAppErrorPayload(payload AppErrorPayload) AppErrorPayload {
	payload.Attrs = cloneAttrs(payload.Attrs)
	payload.Stacktrace = append([]string(nil), payload.Stacktrace...)
	if payload.InputRef != nil {
		ref := *payload.InputRef
		payload.InputRef = &ref
	}
	return payload
}

func cloneSystemErrorPayload(payload SystemErrorPayload) SystemErrorPayload {
	payload.Stacktrace = append([]string(nil), payload.Stacktrace...)
	if payload.InputRef != nil {
		ref := *payload.InputRef
		payload.InputRef = &ref
	}
	return payload
}

func cloneTimeoutPayload(payload TimeoutPayload) TimeoutPayload {
	if payload.InputRef != nil {
		ref := *payload.InputRef
		payload.InputRef = &ref
	}
	return payload
}

func cloneChapterMetadata(metadata ChapterMetadata) ChapterMetadata {
	if metadata.Fields == nil {
		return ChapterMetadata{}
	}
	return ChapterMetadata{Fields: cloneChapterMetadataFields(metadata.Fields)}
}

func cloneChapterMetadataFields(fields map[string]ChapterMetadataValue) map[string]ChapterMetadataValue {
	out := make(map[string]ChapterMetadataValue, len(fields))
	for key, value := range fields {
		out[key] = cloneChapterMetadataValue(value)
	}
	return out
}

func cloneChapterMetadataValue(value ChapterMetadataValue) ChapterMetadataValue {
	if value.List != nil {
		value.List = append([]ChapterMetadataValue(nil), value.List...)
		for i := range value.List {
			value.List[i] = cloneChapterMetadataValue(value.List[i])
		}
	}
	if value.Map != nil {
		value.Map = cloneChapterMetadataFields(value.Map)
	}
	return value
}

func cloneStoredArtifacts(artifacts []StoredArtifact) []StoredArtifact {
	if artifacts == nil {
		return nil
	}
	return append([]StoredArtifact(nil), artifacts...)
}
