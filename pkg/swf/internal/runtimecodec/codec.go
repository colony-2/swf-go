package runtimecodec

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	"github.com/colony-2/swf-go/pkg/swf"
	storagepb "github.com/colony-2/swf-go/pkg/swf/internal/storagepb/v1"
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
	Version       int                   `json:"version"`
	Ordinal       int64                 `json:"ordinal"`
	TaskType      string                `json:"task_type"`
	WorkerID      string                `json:"worker_id"`
	CreatedAt     time.Time             `json:"created_at"`
	StartedAt     *time.Time            `json:"started_at,omitempty"`
	FinishedAt    *time.Time            `json:"finished_at,omitempty"`
	InputHash     string                `json:"input_hash"`
	Metadata      json.RawMessage       `json:"metadata,omitempty"`
	Input         json.RawMessage       `json:"input,omitempty"`
	Attempt       int                   `json:"attempt,omitempty"`
	MaxAttempts   int                   `json:"max_attempts,omitempty"`
	NextAttemptAt *time.Time            `json:"next_attempt_at,omitempty"`
	BackoffMillis int64                 `json:"backoff_ms,omitempty"`
	Retryable     *bool                 `json:"retryable,omitempty"`
	InputRef      *swf.InputReference   `json:"input_ref,omitempty"`
	RunPolicy     *swf.RunPolicy        `json:"run_policy,omitempty"`
	Prerequisites []swf.JobPrerequisite `json:"prereqs,omitempty"`
}

type ChapterEnvelope struct {
	ChapterType string
	Meta        ChapterMeta
	PayloadKind string
	Payload     json.RawMessage
}

type SchedulerPayload struct {
	RunPolicy swf.RunPolicy
	TaskWait  *TaskWait
}

type TaskWait struct {
	InputStep  int64
	OutputStep int64
	Next       string
	InputHash  string
}

type pgwfPayloadWrapper struct {
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
	outcome, err := taskOutcomeToProto(payloadKind, payload)
	if err != nil {
		return nil, err
	}
	builder := chapterRecordBuilder(meta)
	switch chapterType {
	case ChapterTypeJobStart:
		if payloadKind != PayloadKindApp {
			return nil, fmt.Errorf("job start chapter payload kind must be %s", PayloadKindApp)
		}
		builder.JobStart = storagepb.JobStartChapter_builder{PayloadJson: cloneBytes(payload)}.Build()
	case ChapterTypeJobAttemptOutcome:
		builder.JobAttemptOutcome = storagepb.JobAttemptOutcomeChapter_builder{Outcome: outcome}.Build()
	case ChapterTypeTaskAttemptOutcome:
		builder.TaskAttemptOutcome = storagepb.TaskAttemptOutcomeChapter_builder{Outcome: outcome}.Build()
	case ChapterTypeRestartExtra:
		if payloadKind != PayloadKindApp {
			return nil, fmt.Errorf("restart extra chapter payload kind must be %s", PayloadKindApp)
		}
		builder.RestartExtra = storagepb.RestartExtraChapter_builder{PayloadJson: cloneBytes(payload)}.Build()
	default:
		return nil, fmt.Errorf("unsupported chapter type %q", chapterType)
	}
	return deterministicMarshal.Marshal(builder.Build())
}

func DecodeChapter(body []byte) (ChapterEnvelope, error) {
	var record storagepb.ChapterRecord
	if err := proto.Unmarshal(body, &record); err != nil {
		return ChapterEnvelope{}, err
	}
	meta := chapterMetaFromProto(&record)
	switch record.WhichChapter() {
	case storagepb.ChapterRecord_JobStart_case:
		ch := record.GetJobStart()
		return ChapterEnvelope{
			ChapterType: ChapterTypeJobStart,
			Meta:        meta,
			PayloadKind: PayloadKindApp,
			Payload:     cloneJSON(ch.GetPayloadJson()),
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
			Payload:     cloneJSON(ch.GetPayloadJson()),
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
	return out, nil
}

func EncodeSchedulerPayloadJSON(payload SchedulerPayload) (json.RawMessage, error) {
	raw, err := EncodeSchedulerPayload(payload)
	if err != nil {
		return nil, err
	}
	wrapped, err := json.Marshal(pgwfPayloadWrapper{Protobuf: base64.StdEncoding.EncodeToString(raw)})
	if err != nil {
		return nil, err
	}
	return json.RawMessage(wrapped), nil
}

func DecodeSchedulerPayloadJSON(raw json.RawMessage) (SchedulerPayload, error) {
	if len(raw) == 0 {
		return SchedulerPayload{}, nil
	}
	var wrapper pgwfPayloadWrapper
	if err := json.Unmarshal(raw, &wrapper); err != nil {
		return SchedulerPayload{}, err
	}
	if wrapper.Protobuf == "" {
		return SchedulerPayload{}, fmt.Errorf("protobuf scheduler payload is required")
	}
	decoded, err := base64.StdEncoding.DecodeString(wrapper.Protobuf)
	if err != nil {
		return SchedulerPayload{}, err
	}
	return DecodeSchedulerPayload(decoded)
}

func SchedulerPayloadJSONView(payload SchedulerPayload) (json.RawMessage, error) {
	type taskWaitJSON struct {
		InputStep  int64  `json:"in"`
		OutputStep int64  `json:"out"`
		Next       string `json:"next"`
		InputHash  string `json:"input_hash,omitempty"`
	}
	type jobPayloadJSON struct {
		RunPolicy swf.RunPolicy `json:"run_policy,omitempty"`
		TaskWait  *taskWaitJSON `json:"task_wait,omitempty"`
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

func chapterRecordBuilder(meta ChapterMeta) storagepb.ChapterRecord_builder {
	versioned := meta
	if versioned.Version == 0 {
		versioned.Version = EnvelopeVersion
	}
	builder := storagepb.ChapterRecord_builder{
		Ordinal:       ptr(versioned.Ordinal),
		TaskType:      ptr(versioned.TaskType),
		WorkerId:      ptr(versioned.WorkerID),
		InputHash:     ptr(versioned.InputHash),
		Attempt:       ptr(int32(versioned.Attempt)),
		MaxAttempts:   ptr(int32(versioned.MaxAttempts)),
		BackoffMillis: ptr(versioned.BackoffMillis),
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
	if len(versioned.Metadata) > 0 {
		builder.MetadataJson = cloneBytes(versioned.Metadata)
	}
	if len(versioned.Input) > 0 {
		builder.InputJson = cloneBytes(versioned.Input)
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
	return builder
}

func chapterMetaFromProto(record *storagepb.ChapterRecord) ChapterMeta {
	meta := ChapterMeta{
		Version:       EnvelopeVersion,
		Ordinal:       record.GetOrdinal(),
		TaskType:      record.GetTaskType(),
		WorkerID:      record.GetWorkerId(),
		CreatedAt:     timestampToTime(record.GetCreatedAt()),
		InputHash:     record.GetInputHash(),
		Metadata:      cloneJSON(record.GetMetadataJson()),
		Input:         cloneJSON(record.GetInputJson()),
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
	return meta
}

func taskOutcomeToProto(payloadKind string, payload json.RawMessage) (*storagepb.TaskOutcome, error) {
	switch payloadKind {
	case PayloadKindApp:
		return storagepb.TaskOutcome_builder{AppPayloadJson: cloneBytes(payload)}.Build(), nil
	case PayloadKindAppError:
		return storagepb.TaskOutcome_builder{AppErrorPayloadJson: cloneBytes(payload)}.Build(), nil
	case PayloadKindSystemError:
		return storagepb.TaskOutcome_builder{SystemErrorPayloadJson: cloneBytes(payload)}.Build(), nil
	case PayloadKindTimeout:
		return storagepb.TaskOutcome_builder{TimeoutPayloadJson: cloneBytes(payload)}.Build(), nil
	default:
		return nil, fmt.Errorf("unsupported payload kind %q", payloadKind)
	}
}

func taskOutcomeFromProto(outcome *storagepb.TaskOutcome) (string, json.RawMessage, error) {
	switch outcome.WhichResult() {
	case storagepb.TaskOutcome_AppPayloadJson_case:
		return PayloadKindApp, cloneJSON(outcome.GetAppPayloadJson()), nil
	case storagepb.TaskOutcome_AppErrorPayloadJson_case:
		return PayloadKindAppError, cloneJSON(outcome.GetAppErrorPayloadJson()), nil
	case storagepb.TaskOutcome_SystemErrorPayloadJson_case:
		return PayloadKindSystemError, cloneJSON(outcome.GetSystemErrorPayloadJson()), nil
	case storagepb.TaskOutcome_TimeoutPayloadJson_case:
		return PayloadKindTimeout, cloneJSON(outcome.GetTimeoutPayloadJson()), nil
	default:
		return "", nil, fmt.Errorf("task outcome result is required")
	}
}

func runPolicyToProto(policy swf.RunPolicy) *storagepb.RunPolicy {
	return storagepb.RunPolicy_builder{
		Retry:             retryPolicyToProto(policy.Retry),
		InvocationTimeout: durationToProto(policy.InvocationTimeout),
		TotalTimeout:      durationToProto(policy.TotalTimeout),
	}.Build()
}

func runPolicyFromProto(policy *storagepb.RunPolicy) swf.RunPolicy {
	if policy == nil {
		return swf.RunPolicy{}
	}
	return swf.RunPolicy{
		Retry:             retryPolicyFromProto(policy.GetRetry()),
		InvocationTimeout: durationFromProto(policy.GetInvocationTimeout()),
		TotalTimeout:      durationFromProto(policy.GetTotalTimeout()),
	}
}

func retryPolicyToProto(policy swf.RetryPolicy) *storagepb.RetryPolicy {
	return storagepb.RetryPolicy_builder{
		InitialInterval:        durationToProtoValue(time.Duration(policy.InitialInterval)),
		BackoffCoefficient:     ptr(policy.BackoffCoefficient),
		MaximumInterval:        durationToProtoValue(time.Duration(policy.MaximumInterval)),
		MaximumAttempts:        ptr(policy.MaximumAttempts),
		NonRetryableErrorTypes: append([]string(nil), policy.NonRetryableErrorTypes...),
	}.Build()
}

func retryPolicyFromProto(policy *storagepb.RetryPolicy) swf.RetryPolicy {
	if policy == nil {
		return swf.RetryPolicy{}
	}
	return swf.RetryPolicy{
		InitialInterval:        swf.Duration(durationValueFromProto(policy.GetInitialInterval())),
		BackoffCoefficient:     policy.GetBackoffCoefficient(),
		MaximumInterval:        swf.Duration(durationValueFromProto(policy.GetMaximumInterval())),
		MaximumAttempts:        policy.GetMaximumAttempts(),
		NonRetryableErrorTypes: append([]string(nil), policy.GetNonRetryableErrorTypes()...),
	}
}

func durationToProto(value *swf.Duration) *durationpb.Duration {
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

func durationFromProto(value *durationpb.Duration) *swf.Duration {
	if value == nil {
		return nil
	}
	d := swf.Duration(value.AsDuration())
	return &d
}

func durationValueFromProto(value *durationpb.Duration) time.Duration {
	if value == nil {
		return 0
	}
	return value.AsDuration()
}

func inputReferenceToProto(ref swf.InputReference) *storagepb.InputReference {
	return storagepb.InputReference_builder{Ordinal: ptr(ref.Ordinal), Hash: ptr(ref.Hash)}.Build()
}

func inputReferenceFromProto(ref *storagepb.InputReference) swf.InputReference {
	if ref == nil {
		return swf.InputReference{}
	}
	return swf.InputReference{Ordinal: ref.GetOrdinal(), Hash: ref.GetHash()}
}

func prerequisitesToProto(prereqs []swf.JobPrerequisite) []*storagepb.JobPrerequisite {
	out := make([]*storagepb.JobPrerequisite, 0, len(prereqs))
	for _, prereq := range prereqs {
		out = append(out, storagepb.JobPrerequisite_builder{
			JobId:     ptr(prereq.JobID),
			Condition: ptr(string(prereq.Condition)),
		}.Build())
	}
	return out
}

func prerequisitesFromProto(prereqs []*storagepb.JobPrerequisite) []swf.JobPrerequisite {
	out := make([]swf.JobPrerequisite, 0, len(prereqs))
	for _, prereq := range prereqs {
		if prereq == nil {
			continue
		}
		out = append(out, swf.JobPrerequisite{
			JobID:     prereq.GetJobId(),
			Condition: swf.JobPrereqCondition(prereq.GetCondition()),
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
