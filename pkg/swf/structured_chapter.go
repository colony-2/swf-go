package swf

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"
)

// StructuredWorkflowRuntime exposes direct chapter access without the legacy
// stringly typed chapter and payload discriminator fields.
type StructuredWorkflowRuntime interface {
	GetStructuredChapter(ctx context.Context, ref ChapterRef) (StructuredChapterRecord, error)
	ListStructuredChapters(ctx context.Context, req ListChaptersRequest) ([]StructuredChapterRecord, error)
	PutStructuredChapter(ctx context.Context, req PutStructuredChapterRequest) error
}

// NewStructuredWorkflowRuntime returns a structured chapter adapter for runtime.
func NewStructuredWorkflowRuntime(runtime WorkflowRuntime) StructuredWorkflowRuntime {
	if structured, ok := runtime.(StructuredWorkflowRuntime); ok {
		return structured
	}
	return structuredWorkflowRuntime{runtime: runtime}
}

type structuredWorkflowRuntime struct {
	runtime WorkflowRuntime
}

func (r structuredWorkflowRuntime) GetStructuredChapter(ctx context.Context, ref ChapterRef) (StructuredChapterRecord, error) {
	if r.runtime == nil {
		return StructuredChapterRecord{}, fmt.Errorf("workflow runtime is required")
	}
	chapter, err := r.runtime.GetChapter(ctx, ref)
	if err != nil {
		return StructuredChapterRecord{}, err
	}
	return StructuredChapterFromStored(chapter)
}

func (r structuredWorkflowRuntime) ListStructuredChapters(ctx context.Context, req ListChaptersRequest) ([]StructuredChapterRecord, error) {
	if r.runtime == nil {
		return nil, fmt.Errorf("workflow runtime is required")
	}
	chapters, err := r.runtime.ListChapters(ctx, req)
	if err != nil {
		return nil, err
	}
	out := make([]StructuredChapterRecord, 0, len(chapters))
	for _, chapter := range chapters {
		converted, err := StructuredChapterFromStored(chapter)
		if err != nil {
			return nil, err
		}
		out = append(out, converted)
	}
	return out, nil
}

func (r structuredWorkflowRuntime) PutStructuredChapter(ctx context.Context, req PutStructuredChapterRequest) error {
	if r.runtime == nil {
		return fmt.Errorf("workflow runtime is required")
	}
	chapter, err := StoredChapterFromStructured(req.Chapter)
	if err != nil {
		return err
	}
	return r.runtime.PutChapter(ctx, PutChapterRequest{
		LeaseID:         req.LeaseID,
		LeaseToken:      req.LeaseToken,
		Ref:             req.Ref,
		Chapter:         chapter,
		ArtifactUploads: req.ArtifactUploads,
	})
}

// StructuredChapterRecord is the structured representation of a stored chapter.
type StructuredChapterRecord struct {
	Ordinal   int64
	TaskType  string
	Body      StructuredChapterBody
	InputHash string
	CreatedAt time.Time
	Metadata  ChapterMetadata
	Artifacts []StoredArtifact
}

// PutStructuredChapterRequest stores a structured chapter.
type PutStructuredChapterRequest struct {
	LeaseID         string
	LeaseToken      string
	Ref             ChapterRef
	Chapter         StructuredChapterRecord
	ArtifactUploads []ArtifactUpload
}

// StructuredChapterBody is a sealed interface implemented by the supported
// chapter body variants.
type StructuredChapterBody interface {
	structuredChapterBody()
}

// JobStartChapter records the initial application input for a job.
type JobStartChapter struct {
	Input ApplicationInputBytes
}

func (JobStartChapter) structuredChapterBody() {}

// JobAttemptOutcomeChapter records the final outcome for a job attempt.
type JobAttemptOutcomeChapter struct {
	Outcome StructuredTaskOutcome
}

func (JobAttemptOutcomeChapter) structuredChapterBody() {}

// TaskAttemptOutcomeChapter records the outcome for a task attempt.
type TaskAttemptOutcomeChapter struct {
	Outcome StructuredTaskOutcome
}

func (TaskAttemptOutcomeChapter) structuredChapterBody() {}

// RestartExtraChapter records output retained from a restarted job.
type RestartExtraChapter struct {
	Output ApplicationOutputBytes
}

func (RestartExtraChapter) structuredChapterBody() {}

// StructuredTaskOutcome is a sealed interface implemented by the supported
// task outcome variants.
type StructuredTaskOutcome interface {
	structuredTaskOutcome()
}

// ApplicationOutputOutcome contains successful application output bytes.
type ApplicationOutputOutcome struct {
	Output ApplicationOutputBytes
}

func (ApplicationOutputOutcome) structuredTaskOutcome() {}

// AppErrorOutcome contains a user/application error payload.
type AppErrorOutcome struct {
	Error AppErrorPayload
}

func (AppErrorOutcome) structuredTaskOutcome() {}

// SystemErrorOutcome contains an infrastructure/system error payload.
type SystemErrorOutcome struct {
	Error SystemErrorPayload
}

func (SystemErrorOutcome) structuredTaskOutcome() {}

// TimeoutOutcome contains a deterministic timeout payload.
type TimeoutOutcome struct {
	Timeout TimeoutPayload
}

func (TimeoutOutcome) structuredTaskOutcome() {}

// ApplicationInputBytes wraps application input bytes.
type ApplicationInputBytes struct {
	Data []byte
}

// ApplicationOutputBytes wraps application output bytes.
type ApplicationOutputBytes struct {
	Data []byte
}

// ChapterMetadata is a structured metadata object.
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

// ChapterMetadataValue is a structured metadata value.
type ChapterMetadataValue struct {
	Kind   ChapterMetadataKind
	Bool   bool
	Int    int64
	Double float64
	String string
	List   []ChapterMetadataValue
	Map    map[string]ChapterMetadataValue
}

// ChapterMetadataFromJSON converts a legacy JSON metadata object into
// structured metadata.
func ChapterMetadataFromJSON(raw json.RawMessage) (ChapterMetadata, error) {
	if len(raw) == 0 {
		return ChapterMetadata{}, nil
	}
	if !json.Valid(raw) {
		return ChapterMetadata{}, fmt.Errorf("metadata must be valid JSON")
	}
	value, err := decodeStructuredJSONValue(raw)
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

// JSON converts structured metadata into the legacy JSON metadata form.
func (m ChapterMetadata) JSON() (json.RawMessage, error) {
	if m.Fields == nil {
		return nil, nil
	}
	raw, err := json.Marshal(chapterMetadataFieldsToAny(m.Fields))
	if err != nil {
		return nil, err
	}
	return json.RawMessage(raw), nil
}

// StructuredChapterFromStored converts a legacy stored chapter into the
// structured chapter API.
func StructuredChapterFromStored(chapter StoredChapter) (StructuredChapterRecord, error) {
	metadata, err := ChapterMetadataFromJSON(chapter.Metadata)
	if err != nil {
		return StructuredChapterRecord{}, err
	}
	body, err := structuredChapterBodyFromStored(chapter)
	if err != nil {
		return StructuredChapterRecord{}, err
	}
	return StructuredChapterRecord{
		Ordinal:   chapter.Ordinal,
		TaskType:  chapter.TaskType,
		Body:      body,
		InputHash: chapter.InputHash,
		CreatedAt: chapter.CreatedAt,
		Metadata:  metadata,
		Artifacts: cloneStoredArtifacts(chapter.Artifacts),
	}, nil
}

// StoredChapterFromStructured converts a structured chapter into the legacy
// stored chapter representation used by existing WorkflowRuntime backends.
func StoredChapterFromStructured(chapter StructuredChapterRecord) (StoredChapter, error) {
	metadata, err := chapter.Metadata.JSON()
	if err != nil {
		return StoredChapter{}, err
	}
	chapterType, payloadKind, data, err := storedChapterDiscriminatorsAndData(chapter.Body)
	if err != nil {
		return StoredChapter{}, err
	}
	return StoredChapter{
		Ordinal:     chapter.Ordinal,
		TaskType:    chapter.TaskType,
		ChapterType: chapterType,
		PayloadKind: payloadKind,
		InputHash:   chapter.InputHash,
		CreatedAt:   chapter.CreatedAt,
		Metadata:    metadata,
		Data:        data,
		Artifacts:   cloneStoredArtifacts(chapter.Artifacts),
	}, nil
}

func structuredChapterBodyFromStored(chapter StoredChapter) (StructuredChapterBody, error) {
	switch chapter.ChapterType {
	case chapterTypeJobStart:
		if chapter.PayloadKind != payloadKindApp {
			return nil, fmt.Errorf("%s chapters require %s payloads", chapter.ChapterType, payloadKindApp)
		}
		return JobStartChapter{Input: ApplicationInputBytes{Data: cloneRawMessage(chapter.Data)}}, nil
	case chapterTypeJobAttemptOutcome:
		outcome, err := structuredTaskOutcomeFromStored(chapter.PayloadKind, chapter.Data)
		if err != nil {
			return nil, err
		}
		return JobAttemptOutcomeChapter{Outcome: outcome}, nil
	case chapterTypeTaskAttemptOutcome:
		outcome, err := structuredTaskOutcomeFromStored(chapter.PayloadKind, chapter.Data)
		if err != nil {
			return nil, err
		}
		return TaskAttemptOutcomeChapter{Outcome: outcome}, nil
	case chapterTypeRestartExtra:
		if chapter.PayloadKind != payloadKindApp {
			return nil, fmt.Errorf("%s chapters require %s payloads", chapter.ChapterType, payloadKindApp)
		}
		return RestartExtraChapter{Output: ApplicationOutputBytes{Data: cloneRawMessage(chapter.Data)}}, nil
	default:
		return nil, fmt.Errorf("unsupported chapter type %q", chapter.ChapterType)
	}
}

func structuredTaskOutcomeFromStored(payloadKind string, data json.RawMessage) (StructuredTaskOutcome, error) {
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

func storedChapterDiscriminatorsAndData(body StructuredChapterBody) (string, string, json.RawMessage, error) {
	switch body := body.(type) {
	case JobStartChapter:
		return chapterTypeJobStart, payloadKindApp, cloneRawMessage(body.Input.Data), nil
	case *JobStartChapter:
		if body == nil {
			return "", "", nil, fmt.Errorf("chapter body is required")
		}
		return chapterTypeJobStart, payloadKindApp, cloneRawMessage(body.Input.Data), nil
	case JobAttemptOutcomeChapter:
		payloadKind, data, err := storedTaskOutcome(body.Outcome)
		return chapterTypeJobAttemptOutcome, payloadKind, data, err
	case *JobAttemptOutcomeChapter:
		if body == nil {
			return "", "", nil, fmt.Errorf("chapter body is required")
		}
		payloadKind, data, err := storedTaskOutcome(body.Outcome)
		return chapterTypeJobAttemptOutcome, payloadKind, data, err
	case TaskAttemptOutcomeChapter:
		payloadKind, data, err := storedTaskOutcome(body.Outcome)
		return chapterTypeTaskAttemptOutcome, payloadKind, data, err
	case *TaskAttemptOutcomeChapter:
		if body == nil {
			return "", "", nil, fmt.Errorf("chapter body is required")
		}
		payloadKind, data, err := storedTaskOutcome(body.Outcome)
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

func storedTaskOutcome(outcome StructuredTaskOutcome) (string, json.RawMessage, error) {
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
		decoded, err := decodeStructuredJSONValue(v)
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
		decoded, err := decodeStructuredJSONValue(raw)
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

func decodeStructuredJSONValue(raw []byte) (any, error) {
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

func cloneStoredArtifacts(artifacts []StoredArtifact) []StoredArtifact {
	if artifacts == nil {
		return nil
	}
	return append([]StoredArtifact(nil), artifacts...)
}
