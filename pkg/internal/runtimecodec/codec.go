package runtimecodec

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	storagepb "github.com/colony-2/jobdb/pkg/internal/storagepb/v1"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

const (
	EnvelopeVersion = 1

	PayloadKindApp         = "App"
	PayloadKindAppError    = "AppError"
	PayloadKindSystemError = "SystemError"
	PayloadKindTimeout     = "Timeout"

	ChapterTypeJobStart           = "JobStart"
	ChapterTypeJobAttemptOutcome  = "JobAttemptOutcome"
	ChapterTypeTaskAttemptOutcome = "TaskAttemptOutcome"
	ChapterTypeRestartExtra       = "RestartExtra"
)

type ChapterMeta struct {
	Version       int                     `json:"version"`
	Ordinal       int64                   `json:"ordinal"`
	TaskType      string                  `json:"task_type"`
	WorkerID      string                  `json:"worker_id"`
	CreatedAt     time.Time               `json:"created_at"`
	StartedAt     *time.Time              `json:"started_at,omitempty"`
	FinishedAt    *time.Time              `json:"finished_at,omitempty"`
	InputHash     string                  `json:"input_hash"`
	Metadata      json.RawMessage         `json:"metadata,omitempty"`
	Input         json.RawMessage         `json:"input,omitempty"`
	Attempt       int                     `json:"attempt,omitempty"`
	MaxAttempts   int                     `json:"max_attempts,omitempty"`
	NextAttemptAt *time.Time              `json:"next_attempt_at,omitempty"`
	BackoffMillis int64                   `json:"backoff_ms,omitempty"`
	Retryable     *bool                   `json:"retryable,omitempty"`
	InputRef      *jobdb.InputReference   `json:"input_ref,omitempty"`
	RunPolicy     *jobdb.RunPolicy        `json:"run_policy,omitempty"`
	Prerequisites []jobdb.JobPrerequisite `json:"prereqs,omitempty"`
}

type ChapterEnvelope struct {
	ChapterType string
	Meta        ChapterMeta
	PayloadKind string
	Payload     json.RawMessage
}

type SchedulerPayload struct {
	RunPolicy      jobdb.RunPolicy
	TaskWait       *TaskWait
	VisiblePayload json.RawMessage
}

type TaskWait struct {
	InputStep  int64
	OutputStep int64
	Next       string
	InputHash  string
}

type protobufJSONWrapper struct {
	Protobuf string `json:"protobuf"`
}

var deterministicMarshal = proto.MarshalOptions{Deterministic: true}

func EncodeChapter(meta ChapterMeta, chapterType string, payloadKind string, payload json.RawMessage) ([]byte, error) {
	if payloadKind == "" {
		return nil, fmt.Errorf("payload kind is required")
	}
	if chapterType == "" {
		return nil, fmt.Errorf("chapter type is required")
	}
	if !json.Valid(payload) {
		return nil, fmt.Errorf("payload must be valid JSON")
	}
	builder, err := chapterRecordBuilder(meta)
	if err != nil {
		return nil, err
	}
	switch chapterType {
	case ChapterTypeJobStart:
		if payloadKind != PayloadKindApp {
			return nil, fmt.Errorf("%s chapters require %s payloads", chapterType, PayloadKindApp)
		}
		builder.JobStart = storagepb.JobStartChapter_builder{Input: applicationInputBytes(payload)}.Build()
	case ChapterTypeJobAttemptOutcome:
		outcome, err := taskOutcomeToProto(payloadKind, payload)
		if err != nil {
			return nil, err
		}
		builder.JobAttemptOutcome = storagepb.JobAttemptOutcomeChapter_builder{Outcome: outcome}.Build()
	case ChapterTypeTaskAttemptOutcome:
		outcome, err := taskOutcomeToProto(payloadKind, payload)
		if err != nil {
			return nil, err
		}
		builder.TaskAttemptOutcome = storagepb.TaskAttemptOutcomeChapter_builder{Outcome: outcome}.Build()
	case ChapterTypeRestartExtra:
		if payloadKind != PayloadKindApp {
			return nil, fmt.Errorf("%s chapters require %s payloads", chapterType, PayloadKindApp)
		}
		builder.RestartExtra = storagepb.RestartExtraChapter_builder{Output: applicationOutputBytes(payload)}.Build()
	default:
		return nil, fmt.Errorf("unsupported chapter type %q", chapterType)
	}
	raw, err := deterministicMarshal.Marshal(builder.Build())
	if err != nil {
		return nil, err
	}
	return encodeProtobufJSONWrapper(raw)
}

func DecodeChapter(body []byte) (ChapterEnvelope, error) {
	body, err := decodeProtobufJSONWrapper(body)
	if err != nil {
		return ChapterEnvelope{}, err
	}
	var record storagepb.ChapterRecord
	if err := proto.Unmarshal(body, &record); err != nil {
		return ChapterEnvelope{}, err
	}
	meta, err := chapterMetaFromProto(&record)
	if err != nil {
		return ChapterEnvelope{}, err
	}
	switch record.WhichChapter() {
	case storagepb.ChapterRecord_JobStart_case:
		ch := record.GetJobStart()
		return ChapterEnvelope{
			ChapterType: ChapterTypeJobStart,
			Meta:        meta,
			PayloadKind: PayloadKindApp,
			Payload:     cloneJSON(ch.GetInput().GetData()),
		}, nil
	case storagepb.ChapterRecord_JobAttemptOutcome_case:
		kind, payload, err := taskOutcomeFromProto(record.GetJobAttemptOutcome().GetOutcome())
		if err != nil {
			return ChapterEnvelope{}, err
		}
		return ChapterEnvelope{ChapterType: ChapterTypeJobAttemptOutcome, Meta: meta, PayloadKind: kind, Payload: payload}, nil
	case storagepb.ChapterRecord_TaskAttemptOutcome_case:
		kind, payload, err := taskOutcomeFromProto(record.GetTaskAttemptOutcome().GetOutcome())
		if err != nil {
			return ChapterEnvelope{}, err
		}
		return ChapterEnvelope{ChapterType: ChapterTypeTaskAttemptOutcome, Meta: meta, PayloadKind: kind, Payload: payload}, nil
	case storagepb.ChapterRecord_RestartExtra_case:
		ch := record.GetRestartExtra()
		return ChapterEnvelope{
			ChapterType: ChapterTypeRestartExtra,
			Meta:        meta,
			PayloadKind: PayloadKindApp,
			Payload:     cloneJSON(ch.GetOutput().GetData()),
		}, nil
	default:
		return ChapterEnvelope{}, fmt.Errorf("chapter variant is required")
	}
}

func EncodeSchedulerPayload(payload SchedulerPayload) ([]byte, error) {
	builder := storagepb.SchedulerPayload_builder{
		RunPolicy: runPolicyToProto(payload.RunPolicy),
	}
	if payload.TaskWait != nil {
		builder.TaskWait = taskWaitToProto(*payload.TaskWait)
	}
	if len(payload.VisiblePayload) > 0 {
		if !json.Valid(payload.VisiblePayload) {
			return nil, fmt.Errorf("visible payload must be valid JSON")
		}
		builder.LeasePayload = leasePayloadBytes(payload.VisiblePayload)
	}
	return deterministicMarshal.Marshal(builder.Build())
}

func DecodeSchedulerPayload(raw []byte) (SchedulerPayload, error) {
	if len(raw) == 0 {
		return SchedulerPayload{}, nil
	}
	var payload storagepb.SchedulerPayload
	if err := proto.Unmarshal(raw, &payload); err != nil {
		return SchedulerPayload{}, err
	}
	out := SchedulerPayload{RunPolicy: runPolicyFromProto(payload.GetRunPolicy())}
	if payload.HasTaskWait() {
		tw := taskWaitFromProto(payload.GetTaskWait())
		out.TaskWait = &tw
	}
	if payload.HasLeasePayload() {
		out.VisiblePayload = cloneJSON(payload.GetLeasePayload().GetData())
	}
	return out, nil
}

func EncodeSchedulerPayloadJSON(payload SchedulerPayload) (json.RawMessage, error) {
	raw, err := EncodeSchedulerPayload(payload)
	if err != nil {
		return nil, err
	}
	return encodeProtobufJSONWrapper(raw)
}

func DecodeSchedulerPayloadJSON(raw json.RawMessage) (SchedulerPayload, error) {
	if len(raw) == 0 {
		return SchedulerPayload{}, nil
	}
	decoded, err := decodeProtobufJSONWrapper(raw)
	if err != nil {
		return SchedulerPayload{}, err
	}
	return DecodeSchedulerPayload(decoded)
}

func SchedulerPayloadJSONView(payload SchedulerPayload) (json.RawMessage, error) {
	if len(payload.VisiblePayload) > 0 {
		return cloneJSON(payload.VisiblePayload), nil
	}
	type taskWaitJSON struct {
		InputStep  int64  `json:"in"`
		OutputStep int64  `json:"out"`
		Next       string `json:"next"`
		InputHash  string `json:"input_hash,omitempty"`
	}
	type jobPayloadJSON struct {
		RunPolicy jobdb.RunPolicy `json:"run_policy,omitempty"`
		TaskWait  *taskWaitJSON   `json:"task_wait,omitempty"`
	}
	view := jobPayloadJSON{RunPolicy: payload.RunPolicy}
	if payload.TaskWait != nil {
		view.TaskWait = &taskWaitJSON{
			InputStep:  payload.TaskWait.InputStep,
			OutputStep: payload.TaskWait.OutputStep,
			Next:       payload.TaskWait.Next,
			InputHash:  payload.TaskWait.InputHash,
		}
	}
	raw, err := json.Marshal(view)
	if err != nil {
		return nil, err
	}
	return json.RawMessage(raw), nil
}

func SchedulerPayloadFromJSONView(raw json.RawMessage) (SchedulerPayload, error) {
	if len(raw) == 0 {
		raw = json.RawMessage(`{}`)
	}
	if !json.Valid(raw) {
		return SchedulerPayload{}, fmt.Errorf("visible payload must be valid JSON")
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return SchedulerPayload{}, err
	}
	if fields == nil {
		return SchedulerPayload{}, fmt.Errorf("visible payload must be a JSON object")
	}
	payload := SchedulerPayload{}
	parsedSchedulerField := false
	hasNonSchedulerField := false
	for name := range fields {
		if name != "run_policy" && name != "task_wait" {
			hasNonSchedulerField = true
			break
		}
	}
	if policyRaw, ok := fields["run_policy"]; ok && len(policyRaw) > 0 && string(policyRaw) != "null" {
		var policy jobdb.RunPolicy
		if err := json.Unmarshal(policyRaw, &policy); err == nil {
			payload.RunPolicy = policy
			parsedSchedulerField = true
		}
	}
	if waitRaw, ok := fields["task_wait"]; ok && len(waitRaw) > 0 && string(waitRaw) != "null" {
		type taskWaitJSON struct {
			InputStep  int64  `json:"in"`
			OutputStep int64  `json:"out"`
			Next       string `json:"next"`
			InputHash  string `json:"input_hash,omitempty"`
		}
		var wait taskWaitJSON
		if err := json.Unmarshal(waitRaw, &wait); err == nil {
			payload.TaskWait = &TaskWait{
				InputStep:  wait.InputStep,
				OutputStep: wait.OutputStep,
				Next:       wait.Next,
				InputHash:  wait.InputHash,
			}
			parsedSchedulerField = true
		}
	}
	if hasNonSchedulerField || !parsedSchedulerField {
		payload.VisiblePayload = cloneJSON(raw)
	}
	return payload, nil
}

func EncodeWaitForJobs(jobIDs []string) ([]byte, error) {
	clean := make([]string, 0, len(jobIDs))
	for _, id := range jobIDs {
		if id != "" {
			clean = append(clean, id)
		}
	}
	return deterministicMarshal.Marshal(storagepb.WaitForJobs_builder{JobIds: clean}.Build())
}

func DecodeWaitForJobs(raw []byte) ([]string, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	var msg storagepb.WaitForJobs
	if err := proto.Unmarshal(raw, &msg); err != nil {
		return nil, err
	}
	return append([]string(nil), msg.GetJobIds()...), nil
}

func encodeProtobufJSONWrapper(raw []byte) (json.RawMessage, error) {
	wrapped, err := json.Marshal(protobufJSONWrapper{Protobuf: base64.StdEncoding.EncodeToString(raw)})
	if err != nil {
		return nil, err
	}
	return json.RawMessage(wrapped), nil
}

func decodeProtobufJSONWrapper(raw []byte) ([]byte, error) {
	var wrapper protobufJSONWrapper
	if err := json.Unmarshal(raw, &wrapper); err != nil {
		return nil, err
	}
	if wrapper.Protobuf == "" {
		return nil, fmt.Errorf("protobuf payload is required")
	}
	decoded, err := base64.StdEncoding.DecodeString(wrapper.Protobuf)
	if err != nil {
		return nil, err
	}
	return decoded, nil
}

func chapterRecordBuilder(meta ChapterMeta) (storagepb.ChapterRecord_builder, error) {
	versioned := meta
	if versioned.Version == 0 {
		versioned.Version = EnvelopeVersion
	}
	metadata, err := metadataJSONToProto(versioned.Metadata)
	if err != nil {
		return storagepb.ChapterRecord_builder{}, fmt.Errorf("encode chapter metadata: %w", err)
	}
	builder := storagepb.ChapterRecord_builder{
		Ordinal:       ptr(versioned.Ordinal),
		TaskType:      ptr(versioned.TaskType),
		WorkerId:      ptr(versioned.WorkerID),
		InputHash:     ptr(versioned.InputHash),
		Attempt:       ptr(int32(versioned.Attempt)),
		MaxAttempts:   ptr(int32(versioned.MaxAttempts)),
		BackoffMillis: ptr(versioned.BackoffMillis),
		Metadata:      metadata,
	}
	if !versioned.CreatedAt.IsZero() {
		builder.CreatedAt = timestamppb.New(versioned.CreatedAt)
	}
	if versioned.StartedAt != nil {
		builder.StartedAt = timestamppb.New(*versioned.StartedAt)
	}
	if versioned.FinishedAt != nil {
		builder.FinishedAt = timestamppb.New(*versioned.FinishedAt)
	}
	if len(versioned.Input) > 0 {
		builder.Input = applicationInputBytes(versioned.Input)
	}
	if versioned.NextAttemptAt != nil {
		builder.NextAttemptAt = timestamppb.New(*versioned.NextAttemptAt)
	}
	if versioned.Retryable != nil {
		builder.Retryable = ptr(*versioned.Retryable)
	}
	if versioned.InputRef != nil {
		builder.InputRef = inputReferenceToProto(*versioned.InputRef)
	}
	if versioned.RunPolicy != nil {
		builder.RunPolicy = runPolicyToProto(*versioned.RunPolicy)
	}
	if len(versioned.Prerequisites) > 0 {
		builder.Prerequisites = prerequisitesToProto(versioned.Prerequisites)
	}
	return builder, nil
}

func chapterMetaFromProto(record *storagepb.ChapterRecord) (ChapterMeta, error) {
	metadata, err := metadataProtoToJSON(record.GetMetadata())
	if err != nil {
		return ChapterMeta{}, fmt.Errorf("decode chapter metadata: %w", err)
	}
	meta := ChapterMeta{
		Version:       EnvelopeVersion,
		Ordinal:       record.GetOrdinal(),
		TaskType:      record.GetTaskType(),
		WorkerID:      record.GetWorkerId(),
		CreatedAt:     timestampToTime(record.GetCreatedAt()),
		InputHash:     record.GetInputHash(),
		Metadata:      metadata,
		Input:         cloneJSON(record.GetInput().GetData()),
		Attempt:       int(record.GetAttempt()),
		MaxAttempts:   int(record.GetMaxAttempts()),
		BackoffMillis: record.GetBackoffMillis(),
		Prerequisites: prerequisitesFromProto(record.GetPrerequisites()),
	}
	if record.HasStartedAt() {
		t := timestampToTime(record.GetStartedAt())
		meta.StartedAt = &t
	}
	if record.HasFinishedAt() {
		t := timestampToTime(record.GetFinishedAt())
		meta.FinishedAt = &t
	}
	if record.HasNextAttemptAt() {
		t := timestampToTime(record.GetNextAttemptAt())
		meta.NextAttemptAt = &t
	}
	if record.HasRetryable() {
		v := record.GetRetryable()
		meta.Retryable = &v
	}
	if record.HasInputRef() {
		ref := inputReferenceFromProto(record.GetInputRef())
		meta.InputRef = &ref
	}
	if record.HasRunPolicy() {
		policy := runPolicyFromProto(record.GetRunPolicy())
		meta.RunPolicy = &policy
	}
	return meta, nil
}

func taskOutcomeToProto(payloadKind string, payload json.RawMessage) (*storagepb.TaskOutcome, error) {
	switch payloadKind {
	case PayloadKindApp:
		return storagepb.TaskOutcome_builder{AppOutput: applicationOutputBytes(payload)}.Build(), nil
	case PayloadKindAppError:
		var decoded jobdb.AppErrorPayload
		if err := json.Unmarshal(payload, &decoded); err != nil {
			return nil, fmt.Errorf("decode app error payload: %w", err)
		}
		converted, err := appErrorPayloadToProto(decoded)
		if err != nil {
			return nil, err
		}
		return storagepb.TaskOutcome_builder{AppError: converted}.Build(), nil
	case PayloadKindSystemError:
		var decoded jobdb.SystemErrorPayload
		if err := json.Unmarshal(payload, &decoded); err != nil {
			return nil, fmt.Errorf("decode system error payload: %w", err)
		}
		return storagepb.TaskOutcome_builder{SystemError: systemErrorPayloadToProto(decoded)}.Build(), nil
	case PayloadKindTimeout:
		var decoded jobdb.TimeoutPayload
		if err := json.Unmarshal(payload, &decoded); err != nil {
			return nil, fmt.Errorf("decode timeout payload: %w", err)
		}
		return storagepb.TaskOutcome_builder{Timeout: timeoutPayloadToProto(decoded)}.Build(), nil
	default:
		return nil, fmt.Errorf("unsupported task outcome payload kind %q", payloadKind)
	}
}

func taskOutcomeFromProto(outcome *storagepb.TaskOutcome) (string, json.RawMessage, error) {
	if outcome == nil {
		return "", nil, fmt.Errorf("task outcome result is required")
	}
	switch outcome.WhichResult() {
	case storagepb.TaskOutcome_AppOutput_case:
		return PayloadKindApp, cloneJSON(outcome.GetAppOutput().GetData()), nil
	case storagepb.TaskOutcome_AppError_case:
		payload, err := json.Marshal(appErrorPayloadFromProto(outcome.GetAppError()))
		if err != nil {
			return "", nil, err
		}
		return PayloadKindAppError, json.RawMessage(payload), nil
	case storagepb.TaskOutcome_SystemError_case:
		payload, err := json.Marshal(systemErrorPayloadFromProto(outcome.GetSystemError()))
		if err != nil {
			return "", nil, err
		}
		return PayloadKindSystemError, json.RawMessage(payload), nil
	case storagepb.TaskOutcome_Timeout_case:
		payload, err := json.Marshal(timeoutPayloadFromProto(outcome.GetTimeout()))
		if err != nil {
			return "", nil, err
		}
		return PayloadKindTimeout, json.RawMessage(payload), nil
	default:
		return "", nil, fmt.Errorf("task outcome result is required")
	}
}

func applicationInputBytes(raw []byte) *storagepb.ApplicationInputBytes {
	return storagepb.ApplicationInputBytes_builder{Data: cloneBytes(raw)}.Build()
}

func applicationOutputBytes(raw []byte) *storagepb.ApplicationOutputBytes {
	return storagepb.ApplicationOutputBytes_builder{Data: cloneBytes(raw)}.Build()
}

func leasePayloadBytes(raw []byte) *storagepb.LeasePayloadBytes {
	return storagepb.LeasePayloadBytes_builder{Data: cloneBytes(raw)}.Build()
}

func appErrorPayloadToProto(payload jobdb.AppErrorPayload) (*storagepb.AppErrorPayload, error) {
	attrs, err := metadataFieldsToProto(payload.Attrs)
	if err != nil {
		return nil, fmt.Errorf("encode app error attrs: %w", err)
	}
	builder := storagepb.AppErrorPayload_builder{
		Message:    ptr(payload.Message),
		Level:      ptr(payload.Level),
		Attrs:      attrs,
		Stacktrace: append([]string(nil), payload.Stacktrace...),
	}
	if payload.InputRef != nil {
		builder.InputRef = inputReferenceToProto(*payload.InputRef)
	}
	return builder.Build(), nil
}

func appErrorPayloadFromProto(payload *storagepb.AppErrorPayload) jobdb.AppErrorPayload {
	if payload == nil {
		return jobdb.AppErrorPayload{}
	}
	out := jobdb.AppErrorPayload{
		Message:    payload.GetMessage(),
		Level:      payload.GetLevel(),
		Attrs:      metadataFieldsFromProto(payload.GetAttrs()),
		Stacktrace: append([]string(nil), payload.GetStacktrace()...),
	}
	if payload.HasInputRef() {
		ref := inputReferenceFromProto(payload.GetInputRef())
		out.InputRef = &ref
	}
	return out
}

func systemErrorPayloadToProto(payload jobdb.SystemErrorPayload) *storagepb.SystemErrorPayload {
	builder := storagepb.SystemErrorPayload_builder{
		Message:    ptr(payload.Message),
		Component:  ptr(payload.Component),
		Code:       ptr(payload.Code),
		Retryable:  ptr(payload.Retryable),
		Stacktrace: append([]string(nil), payload.Stacktrace...),
	}
	if payload.InputRef != nil {
		builder.InputRef = inputReferenceToProto(*payload.InputRef)
	}
	return builder.Build()
}

func systemErrorPayloadFromProto(payload *storagepb.SystemErrorPayload) jobdb.SystemErrorPayload {
	if payload == nil {
		return jobdb.SystemErrorPayload{}
	}
	out := jobdb.SystemErrorPayload{
		Message:    payload.GetMessage(),
		Component:  payload.GetComponent(),
		Code:       payload.GetCode(),
		Retryable:  payload.GetRetryable(),
		Stacktrace: append([]string(nil), payload.GetStacktrace()...),
	}
	if payload.HasInputRef() {
		ref := inputReferenceFromProto(payload.GetInputRef())
		out.InputRef = &ref
	}
	return out
}

func timeoutPayloadToProto(payload jobdb.TimeoutPayload) *storagepb.TimeoutPayload {
	builder := storagepb.TimeoutPayload_builder{
		Kind:      ptr(payload.Kind),
		After:     durationToProtoValue(time.Duration(payload.After)),
		Scope:     ptr(payload.Scope),
		Retryable: ptr(payload.Retryable),
		Component: ptr(payload.Component),
		Code:      ptr(payload.Code),
		Message:   ptr(payload.Message),
	}
	if payload.InputRef != nil {
		builder.InputRef = inputReferenceToProto(*payload.InputRef)
	}
	return builder.Build()
}

func timeoutPayloadFromProto(payload *storagepb.TimeoutPayload) jobdb.TimeoutPayload {
	if payload == nil {
		return jobdb.TimeoutPayload{}
	}
	out := jobdb.TimeoutPayload{
		Kind:      payload.GetKind(),
		After:     jobdb.Duration(durationValueFromProto(payload.GetAfter())),
		Scope:     payload.GetScope(),
		Retryable: payload.GetRetryable(),
		Component: payload.GetComponent(),
		Code:      payload.GetCode(),
		Message:   payload.GetMessage(),
	}
	if payload.HasInputRef() {
		ref := inputReferenceFromProto(payload.GetInputRef())
		out.InputRef = &ref
	}
	return out
}

func metadataJSONToProto(raw json.RawMessage) (*storagepb.Metadata, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	if !json.Valid(raw) {
		return nil, fmt.Errorf("metadata must be valid JSON")
	}
	value, err := decodeJSONValue(raw)
	if err != nil {
		return nil, err
	}
	if value == nil {
		return nil, nil
	}
	fields, ok := value.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("metadata must be a JSON object")
	}
	converted, err := metadataFieldsToProto(fields)
	if err != nil {
		return nil, err
	}
	return storagepb.Metadata_builder{Fields: converted}.Build(), nil
}

func metadataProtoToJSON(metadata *storagepb.Metadata) (json.RawMessage, error) {
	if metadata == nil {
		return nil, nil
	}
	raw, err := json.Marshal(metadataFieldsFromProtoPreserveEmpty(metadata.GetFields()))
	if err != nil {
		return nil, err
	}
	return json.RawMessage(raw), nil
}

func metadataFieldsToProto(fields map[string]any) (map[string]*storagepb.MetadataValue, error) {
	if fields == nil {
		return nil, nil
	}
	out := make(map[string]*storagepb.MetadataValue, len(fields))
	for name, value := range fields {
		converted, err := metadataValueToProto(value)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", name, err)
		}
		out[name] = converted
	}
	return out, nil
}

func metadataFieldsFromProto(fields map[string]*storagepb.MetadataValue) map[string]any {
	if len(fields) == 0 {
		return nil
	}
	return metadataFieldsFromProtoPreserveEmpty(fields)
}

func metadataFieldsFromProtoPreserveEmpty(fields map[string]*storagepb.MetadataValue) map[string]any {
	out := make(map[string]any, len(fields))
	for name, value := range fields {
		out[name] = metadataValueFromProto(value)
	}
	return out
}

func metadataValueToProto(value any) (*storagepb.MetadataValue, error) {
	switch v := value.(type) {
	case nil:
		return storagepb.MetadataValue_builder{NullValue: ptr(true)}.Build(), nil
	case bool:
		return storagepb.MetadataValue_builder{BoolValue: ptr(v)}.Build(), nil
	case string:
		return storagepb.MetadataValue_builder{StringValue: ptr(v)}.Build(), nil
	case json.Number:
		return jsonNumberToMetadataValue(v)
	case int:
		return int64MetadataValue(int64(v)), nil
	case int8:
		return int64MetadataValue(int64(v)), nil
	case int16:
		return int64MetadataValue(int64(v)), nil
	case int32:
		return int64MetadataValue(int64(v)), nil
	case int64:
		return int64MetadataValue(v), nil
	case uint:
		return uint64MetadataValue(uint64(v))
	case uint8:
		return uint64MetadataValue(uint64(v))
	case uint16:
		return uint64MetadataValue(uint64(v))
	case uint32:
		return uint64MetadataValue(uint64(v))
	case uint64:
		return uint64MetadataValue(v)
	case float32:
		return float64MetadataValue(float64(v))
	case float64:
		return float64MetadataValue(v)
	case []any:
		return metadataListToProto(v)
	case map[string]any:
		return metadataMapValueToProto(v)
	case json.RawMessage:
		if !json.Valid(v) {
			return nil, fmt.Errorf("raw metadata value must be valid JSON")
		}
		decoded, err := decodeJSONValue(v)
		if err != nil {
			return nil, err
		}
		return metadataValueToProto(decoded)
	default:
		raw, err := json.Marshal(value)
		if err != nil {
			return nil, err
		}
		if !json.Valid(raw) {
			return nil, fmt.Errorf("metadata value cannot be represented as JSON")
		}
		decoded, err := decodeJSONValue(raw)
		if err != nil {
			return nil, err
		}
		return metadataValueToProto(decoded)
	}
}

func metadataListToProto(values []any) (*storagepb.MetadataValue, error) {
	out := make([]*storagepb.MetadataValue, 0, len(values))
	for i, value := range values {
		converted, err := metadataValueToProto(value)
		if err != nil {
			return nil, fmt.Errorf("[%d]: %w", i, err)
		}
		out = append(out, converted)
	}
	return storagepb.MetadataValue_builder{
		ListValue: storagepb.MetadataList_builder{Values: out}.Build(),
	}.Build(), nil
}

func metadataMapValueToProto(fields map[string]any) (*storagepb.MetadataValue, error) {
	converted, err := metadataFieldsToProto(fields)
	if err != nil {
		return nil, err
	}
	return storagepb.MetadataValue_builder{
		MapValue: storagepb.MetadataMap_builder{Fields: converted}.Build(),
	}.Build(), nil
}

func jsonNumberToMetadataValue(value json.Number) (*storagepb.MetadataValue, error) {
	text := value.String()
	if !strings.ContainsAny(text, ".eE") {
		if i, err := value.Int64(); err == nil {
			return int64MetadataValue(i), nil
		}
	}
	f, err := strconv.ParseFloat(text, 64)
	if err != nil {
		return nil, err
	}
	return float64MetadataValue(f)
}

func int64MetadataValue(value int64) *storagepb.MetadataValue {
	return storagepb.MetadataValue_builder{IntValue: ptr(value)}.Build()
}

func uint64MetadataValue(value uint64) (*storagepb.MetadataValue, error) {
	const maxInt64 = uint64(1<<63 - 1)
	if value > maxInt64 {
		return nil, fmt.Errorf("uint64 value %d exceeds int64 metadata range", value)
	}
	return int64MetadataValue(int64(value)), nil
}

func float64MetadataValue(value float64) (*storagepb.MetadataValue, error) {
	if math.IsNaN(value) || math.IsInf(value, 0) {
		return nil, fmt.Errorf("floating-point metadata values must be finite")
	}
	return storagepb.MetadataValue_builder{DoubleValue: ptr(value)}.Build(), nil
}

func metadataValueFromProto(value *storagepb.MetadataValue) any {
	if value == nil {
		return nil
	}
	switch value.WhichKind() {
	case storagepb.MetadataValue_BoolValue_case:
		return value.GetBoolValue()
	case storagepb.MetadataValue_IntValue_case:
		return value.GetIntValue()
	case storagepb.MetadataValue_DoubleValue_case:
		return value.GetDoubleValue()
	case storagepb.MetadataValue_StringValue_case:
		return value.GetStringValue()
	case storagepb.MetadataValue_ListValue_case:
		values := value.GetListValue().GetValues()
		out := make([]any, 0, len(values))
		for _, item := range values {
			out = append(out, metadataValueFromProto(item))
		}
		return out
	case storagepb.MetadataValue_MapValue_case:
		return metadataFieldsFromProtoPreserveEmpty(value.GetMapValue().GetFields())
	case storagepb.MetadataValue_NullValue_case:
		return nil
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

func runPolicyToProto(policy jobdb.RunPolicy) *storagepb.RunPolicy {
	return storagepb.RunPolicy_builder{
		Retry:             retryPolicyToProto(policy.Retry),
		InvocationTimeout: durationToProto(policy.InvocationTimeout),
		TotalTimeout:      durationToProto(policy.TotalTimeout),
	}.Build()
}

func runPolicyFromProto(policy *storagepb.RunPolicy) jobdb.RunPolicy {
	if policy == nil {
		return jobdb.RunPolicy{}
	}
	return jobdb.RunPolicy{
		Retry:             retryPolicyFromProto(policy.GetRetry()),
		InvocationTimeout: durationFromProto(policy.GetInvocationTimeout()),
		TotalTimeout:      durationFromProto(policy.GetTotalTimeout()),
	}
}

func retryPolicyToProto(policy jobdb.RetryPolicy) *storagepb.RetryPolicy {
	return storagepb.RetryPolicy_builder{
		InitialInterval:        durationToProtoValue(time.Duration(policy.InitialInterval)),
		BackoffCoefficient:     ptr(policy.BackoffCoefficient),
		MaximumInterval:        durationToProtoValue(time.Duration(policy.MaximumInterval)),
		MaximumAttempts:        ptr(policy.MaximumAttempts),
		NonRetryableErrorTypes: append([]string(nil), policy.NonRetryableErrorTypes...),
	}.Build()
}

func retryPolicyFromProto(policy *storagepb.RetryPolicy) jobdb.RetryPolicy {
	if policy == nil {
		return jobdb.RetryPolicy{}
	}
	return jobdb.RetryPolicy{
		InitialInterval:        jobdb.Duration(durationValueFromProto(policy.GetInitialInterval())),
		BackoffCoefficient:     policy.GetBackoffCoefficient(),
		MaximumInterval:        jobdb.Duration(durationValueFromProto(policy.GetMaximumInterval())),
		MaximumAttempts:        policy.GetMaximumAttempts(),
		NonRetryableErrorTypes: append([]string(nil), policy.GetNonRetryableErrorTypes()...),
	}
}

func durationToProto(value *jobdb.Duration) *durationpb.Duration {
	if value == nil {
		return nil
	}
	return durationToProtoValue(time.Duration(*value))
}

func durationToProtoValue(value time.Duration) *durationpb.Duration {
	if value == 0 {
		return nil
	}
	return durationpb.New(value)
}

func durationFromProto(value *durationpb.Duration) *jobdb.Duration {
	if value == nil {
		return nil
	}
	d := jobdb.Duration(value.AsDuration())
	return &d
}

func durationValueFromProto(value *durationpb.Duration) time.Duration {
	if value == nil {
		return 0
	}
	return value.AsDuration()
}

func inputReferenceToProto(ref jobdb.InputReference) *storagepb.InputReference {
	return storagepb.InputReference_builder{Ordinal: ptr(ref.Ordinal), Hash: ptr(ref.Hash)}.Build()
}

func inputReferenceFromProto(ref *storagepb.InputReference) jobdb.InputReference {
	if ref == nil {
		return jobdb.InputReference{}
	}
	return jobdb.InputReference{Ordinal: ref.GetOrdinal(), Hash: ref.GetHash()}
}

func prerequisitesToProto(prereqs []jobdb.JobPrerequisite) []*storagepb.JobPrerequisite {
	out := make([]*storagepb.JobPrerequisite, 0, len(prereqs))
	for _, prereq := range prereqs {
		out = append(out, storagepb.JobPrerequisite_builder{
			JobId:     ptr(prereq.JobID),
			Condition: ptr(string(prereq.Condition)),
		}.Build())
	}
	return out
}

func prerequisitesFromProto(prereqs []*storagepb.JobPrerequisite) []jobdb.JobPrerequisite {
	out := make([]jobdb.JobPrerequisite, 0, len(prereqs))
	for _, prereq := range prereqs {
		if prereq == nil {
			continue
		}
		out = append(out, jobdb.JobPrerequisite{
			JobID:     prereq.GetJobId(),
			Condition: jobdb.JobPrereqCondition(prereq.GetCondition()),
		})
	}
	return out
}

func taskWaitToProto(wait TaskWait) *storagepb.TaskWait {
	return storagepb.TaskWait_builder{
		InputStep:  ptr(wait.InputStep),
		OutputStep: ptr(wait.OutputStep),
		Next:       ptr(wait.Next),
		InputHash:  ptr(wait.InputHash),
	}.Build()
}

func taskWaitFromProto(wait *storagepb.TaskWait) TaskWait {
	if wait == nil {
		return TaskWait{}
	}
	return TaskWait{
		InputStep:  wait.GetInputStep(),
		OutputStep: wait.GetOutputStep(),
		Next:       wait.GetNext(),
		InputHash:  wait.GetInputHash(),
	}
}

func timestampToTime(value *timestamppb.Timestamp) time.Time {
	if value == nil {
		return time.Time{}
	}
	return value.AsTime()
}

func ptr[T any](value T) *T {
	return &value
}

func cloneJSON(raw []byte) json.RawMessage {
	if raw == nil {
		return nil
	}
	return append(json.RawMessage(nil), raw...)
}

func cloneBytes(raw []byte) []byte {
	if raw == nil {
		return nil
	}
	return append([]byte(nil), raw...)
}
