package jobdb

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"time"
)

type ScheduleKey struct {
	TenantId   string `json:"tenantId"`
	ScheduleId string `json:"scheduleId"`
}

func (k ScheduleKey) Validate() error {
	if k.TenantId == "" {
		return fmt.Errorf("tenantId is required")
	}
	if !scheduleIDPattern.MatchString(k.ScheduleId) {
		return fmt.Errorf("scheduleId must match [A-Za-z0-9_-]+")
	}
	return nil
}

type ScheduleState string

const (
	ScheduleStateActive        ScheduleState = "ACTIVE"
	ScheduleStatePaused        ScheduleState = "PAUSED"
	ScheduleStateArchived      ScheduleState = "ARCHIVED"
	ScheduleStateFailurePaused ScheduleState = "FAILURE_PAUSED"
)

type ScheduleOverlapPolicy string

const (
	ScheduleOverlapSerial ScheduleOverlapPolicy = "serial"
)

type ScheduleTriggerKind string

const (
	ScheduleTriggerCron     ScheduleTriggerKind = "cron"
	ScheduleTriggerInterval ScheduleTriggerKind = "interval"
)

type ScheduleTrigger struct {
	Kind       ScheduleTriggerKind `json:"kind"`
	Expression string              `json:"expression,omitempty"`
	Timezone   string              `json:"timezone,omitempty"`
	StartAt    *time.Time          `json:"startAt,omitempty"`
	EndAt      *time.Time          `json:"endAt,omitempty"`
	Interval   time.Duration       `json:"interval,omitempty"`
}

type ScheduleTarget struct {
	JobType   string          `json:"jobType"`
	Data      JobData         `json:"-"`
	RunPolicy RunPolicy       `json:"runPolicy,omitempty"`
	Metadata  json.RawMessage `json:"metadata,omitempty"`
}

type ScheduleFailurePolicy struct {
	MinSuccessPercent     int `json:"minSuccessPercent,omitempty"`
	WindowSize            int `json:"windowSize,omitempty"`
	MaxSequentialFailures int `json:"maxSequentialFailures,omitempty"`
}

type ScheduleSpec struct {
	Trigger       ScheduleTrigger       `json:"trigger"`
	Target        ScheduleTarget        `json:"-"`
	OverlapPolicy ScheduleOverlapPolicy `json:"overlapPolicy,omitempty"`
	FailurePolicy ScheduleFailurePolicy `json:"failurePolicy,omitempty"`
	Paused        bool                  `json:"paused,omitempty"`
}

type UpsertScheduleRequest struct {
	TenantId           string
	ScheduleId         string
	Trigger            ScheduleTrigger
	Target             ScheduleTarget
	OverlapPolicy      ScheduleOverlapPolicy
	FailurePolicy      ScheduleFailurePolicy
	Paused             bool
	ExpectedGeneration *int64
	RequestTime        time.Time
	WorkerID           string
}

type ScheduleMutationRequest struct {
	ScheduleKey        ScheduleKey
	ExpectedGeneration *int64
	RequestTime        time.Time
	WorkerID           string
}

type TriggerScheduleRequest struct {
	ScheduleKey ScheduleKey
	RequestID   string
	RequestTime time.Time
	WorkerID    string
}

type ScheduleInfo struct {
	TenantId       string
	ScheduleId     string
	ScheduleKey    ScheduleKey
	State          ScheduleState
	EffectiveState ScheduleState
	Generation     int64
	SpecHash       string
	Trigger        ScheduleTrigger
	Target         ScheduleTarget
	OverlapPolicy  ScheduleOverlapPolicy
	FailurePolicy  ScheduleFailurePolicy
	NextFireAt     *time.Time
	NextJobKey     *JobKey
	CreatedAt      time.Time
	UpdatedAt      time.Time
}

type ListSchedulesRequest struct {
	TenantId       string
	ScheduleIds    []string
	States         []ScheduleState
	TargetJobTypes []string
	PageSize       int
	PageToken      string
}

type ListSchedulesResponse struct {
	Schedules     []ScheduleInfo
	NextPageToken string
}

type ListScheduleRunsRequest struct {
	ScheduleKey     ScheduleKey
	ScheduledAfter  *time.Time
	ScheduledBefore *time.Time
	Statuses        []JobStatus
	PageSize        int
	PageToken       string
}

type ScheduleRunSummary struct {
	JobSummary
	ScheduleId  string
	ScheduledAt time.Time
	ReasonCode  string
}

type ListScheduleRunsResponse struct {
	Runs          []ScheduleRunSummary
	NextPageToken string
}

type ScheduleOccurrenceMetadata struct {
	ScheduleId     string                 `json:"scheduleId"`
	Kind           string                 `json:"kind"`
	Generation     int64                  `json:"generation"`
	SpecHash       string                 `json:"specHash"`
	ScheduledAt    time.Time              `json:"scheduledAt"`
	RunId          string                 `json:"runId"`
	Manual         bool                   `json:"manual"`
	BackfillId     string                 `json:"backfillId,omitempty"`
	PreviousJobId  string                 `json:"previousJobId,omitempty"`
	FailureHistory ScheduleFailureHistory `json:"failureHistory,omitempty"`
}

type ScheduleFailureHistory struct {
	Bits       string `json:"bits,omitempty"`
	WindowSize int    `json:"windowSize,omitempty"`
}

type RuntimeJobMetadata struct {
	Schedule    *ScheduleOccurrenceMetadata `json:"schedule,omitempty"`
	SchemaHash  string                      `json:"schemaHash,omitempty"`
	ParentJobID string                      `json:"parentJobId,omitempty"`
}

type JobMetadataEnvelope struct {
	App      json.RawMessage     `json:"app,omitempty"`
	Internal *RuntimeJobMetadata `json:"internal,omitempty"`
}

const (
	ScheduleMetadataKind = "schedule_tick"
)

var scheduleIDPattern = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

func NormalizeScheduleOverlapPolicy(policy ScheduleOverlapPolicy) ScheduleOverlapPolicy {
	if policy == "" {
		return ScheduleOverlapSerial
	}
	return policy
}

func ValidateScheduleRequest(req UpsertScheduleRequest) error {
	if err := (ScheduleKey{TenantId: req.TenantId, ScheduleId: req.ScheduleId}).Validate(); err != nil {
		return err
	}
	if req.Target.JobType == "" {
		return fmt.Errorf("target jobType is required")
	}
	if req.Target.Data == nil {
		return fmt.Errorf("target data is required")
	}
	if err := ValidateApplicationMetadata(req.Target.Metadata); err != nil {
		return err
	}
	switch NormalizeScheduleOverlapPolicy(req.OverlapPolicy) {
	case ScheduleOverlapSerial:
	default:
		return fmt.Errorf("unsupported overlap policy %q", req.OverlapPolicy)
	}
	switch req.Trigger.Kind {
	case ScheduleTriggerCron:
		if strings.TrimSpace(req.Trigger.Expression) == "" {
			return fmt.Errorf("cron expression is required")
		}
	case ScheduleTriggerInterval:
		if req.Trigger.Interval <= 0 {
			return fmt.Errorf("interval trigger requires a positive interval")
		}
	default:
		return fmt.Errorf("unsupported trigger kind %q", req.Trigger.Kind)
	}
	if req.FailurePolicy.WindowSize < 0 || req.FailurePolicy.MinSuccessPercent < 0 || req.FailurePolicy.MinSuccessPercent > 100 || req.FailurePolicy.MaxSequentialFailures < 0 {
		return fmt.Errorf("invalid failure policy")
	}
	return nil
}

func ScheduleSpecHash(trigger ScheduleTrigger, target ScheduleTarget, overlap ScheduleOverlapPolicy, failure ScheduleFailurePolicy) (string, error) {
	data := target.Data
	var rawData json.RawMessage
	var artifacts []scheduleHashArtifact
	if data != nil {
		raw, err := data.GetData()
		if err != nil {
			return "", err
		}
		rawData = append(json.RawMessage(nil), raw...)
		artifacts, err = scheduleArtifactsForHash(context.Background(), data)
		if err != nil {
			return "", err
		}
	}
	spec := struct {
		Trigger       ScheduleTrigger       `json:"trigger"`
		Target        scheduleHashTarget    `json:"target"`
		OverlapPolicy ScheduleOverlapPolicy `json:"overlapPolicy"`
		FailurePolicy ScheduleFailurePolicy `json:"failurePolicy"`
	}{
		Trigger: trigger,
		Target: scheduleHashTarget{
			JobType:   target.JobType,
			Data:      rawData,
			Artifacts: artifacts,
			RunPolicy: target.RunPolicy,
			Metadata:  NormalizeJSON(target.Metadata),
		},
		OverlapPolicy: NormalizeScheduleOverlapPolicy(overlap),
		FailurePolicy: failure,
	}
	raw, err := json.Marshal(spec)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return "sha256:" + hex.EncodeToString(sum[:]), nil
}

type scheduleHashTarget struct {
	JobType   string                 `json:"jobType"`
	Data      json.RawMessage        `json:"data,omitempty"`
	Artifacts []scheduleHashArtifact `json:"artifacts,omitempty"`
	RunPolicy RunPolicy              `json:"runPolicy,omitempty"`
	Metadata  json.RawMessage        `json:"metadata,omitempty"`
}

type scheduleHashArtifact struct {
	Name   string `json:"name"`
	Digest string `json:"sha256,omitempty"`
	Size   int64  `json:"size,omitempty"`
}

func scheduleArtifactsForHash(ctx context.Context, data TaskData) ([]scheduleHashArtifact, error) {
	artifacts, err := data.GetArtifacts()
	if err != nil {
		return nil, err
	}
	out := make([]scheduleHashArtifact, 0, len(artifacts))
	for _, artifact := range artifacts {
		if artifact == nil {
			return nil, fmt.Errorf("target artifact is nil")
		}
		digest, err := artifact.Sha256(ctx)
		if err != nil {
			return nil, err
		}
		out = append(out, scheduleHashArtifact{
			Name:   artifact.Name(),
			Digest: digest,
			Size:   artifact.Size(),
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Name == out[j].Name {
			if out[i].Digest == out[j].Digest {
				return out[i].Size < out[j].Size
			}
			return out[i].Digest < out[j].Digest
		}
		return out[i].Name < out[j].Name
	})
	return out, nil
}

func ScheduleRunJobID(scheduleID string, generation int64, scheduledAt time.Time) string {
	return fmt.Sprintf("jobdbsched_%s_g%d_run_%s", scheduleID, generation, ScheduleRunID(scheduledAt))
}

func ScheduleManualJobID(scheduleID string, requestID string) string {
	if requestID == "" {
		requestID = ScheduleRunID(time.Now().UTC())
	}
	return fmt.Sprintf("jobdbsched_%s_manual_%s", scheduleID, requestID)
}

func ScheduleRunID(t time.Time) string {
	return t.UTC().Format("20060102T150405") + fmt.Sprintf("%09dZ", t.UTC().Nanosecond())
}

func ValidateApplicationMetadata(raw json.RawMessage) error {
	if len(strings.TrimSpace(string(raw))) == 0 {
		return nil
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err != nil {
		return fmt.Errorf("metadata must be a JSON object: %w", err)
	}
	return nil
}

func StripRuntimeMetadata(raw json.RawMessage) json.RawMessage {
	return AppMetadataFromStoredMetadata(raw)
}

func AppMetadataFromStoredMetadata(raw json.RawMessage) json.RawMessage {
	if len(strings.TrimSpace(string(raw))) == 0 {
		return nil
	}
	var obj map[string]json.RawMessage
	if err := json.Unmarshal(raw, &obj); err != nil {
		return append(json.RawMessage(nil), raw...)
	}
	if _, hasApp := obj["app"]; hasApp {
		var envelope JobMetadataEnvelope
		if err := json.Unmarshal(raw, &envelope); err != nil {
			return append(json.RawMessage(nil), raw...)
		}
		if len(strings.TrimSpace(string(envelope.App))) == 0 || strings.TrimSpace(string(envelope.App)) == "null" {
			return nil
		}
		return append(json.RawMessage(nil), envelope.App...)
	}
	if internalRaw, hasInternal := obj["internal"]; hasInternal && storedInternalMetadataKnown(internalRaw) {
		var envelope JobMetadataEnvelope
		if err := json.Unmarshal(raw, &envelope); err != nil {
			return append(json.RawMessage(nil), raw...)
		}
		if len(strings.TrimSpace(string(envelope.App))) == 0 || strings.TrimSpace(string(envelope.App)) == "null" {
			return nil
		}
		return append(json.RawMessage(nil), envelope.App...)
	}
	return append(json.RawMessage(nil), raw...)
}

func storedInternalMetadataKnown(raw json.RawMessage) bool {
	var internal map[string]json.RawMessage
	if err := json.Unmarshal(raw, &internal); err != nil {
		return false
	}
	_, hasSchedule := internal["schedule"]
	_, hasSchemaHash := internal["schemaHash"]
	_, hasParentJobID := internal["parentJobId"]
	return hasSchedule || hasSchemaHash || hasParentJobID
}

func MergeScheduleOccurrenceMetadata(appMetadata json.RawMessage, occurrence ScheduleOccurrenceMetadata, manual bool) (json.RawMessage, error) {
	occurrence.Kind = ScheduleMetadataKind
	occurrence.ScheduledAt = occurrence.ScheduledAt.UTC()
	occurrence.RunId = ScheduleRunID(occurrence.ScheduledAt)
	occurrence.Manual = manual
	return BuildJobMetadataEnvelope(appMetadata, RuntimeJobMetadata{Schedule: &occurrence})
}

func BuildJobMetadataEnvelope(appMetadata json.RawMessage, internal RuntimeJobMetadata) (json.RawMessage, error) {
	if err := ValidateApplicationMetadata(appMetadata); err != nil {
		return nil, err
	}
	app := NormalizeJSON(appMetadata)
	var internalPtr *RuntimeJobMetadata
	if !runtimeJobMetadataEmpty(internal) {
		internalCopy := internal
		internalPtr = &internalCopy
	}
	if len(app) == 0 && internalPtr == nil {
		return nil, nil
	}
	return json.Marshal(JobMetadataEnvelope{
		App:      app,
		Internal: internalPtr,
	})
}

func runtimeJobMetadataEmpty(meta RuntimeJobMetadata) bool {
	return meta.Schedule == nil && strings.TrimSpace(meta.SchemaHash) == "" && strings.TrimSpace(meta.ParentJobID) == ""
}

func ExtractParentJobID(raw json.RawMessage) (string, bool, error) {
	if len(strings.TrimSpace(string(raw))) == 0 {
		return "", false, nil
	}
	var root map[string]json.RawMessage
	if err := json.Unmarshal(raw, &root); err != nil {
		return "", false, fmt.Errorf("metadata must be a JSON object: %w", err)
	}
	internalRaw, ok := root["internal"]
	if !ok {
		return "", false, nil
	}
	var runtimeMeta RuntimeJobMetadata
	if err := json.Unmarshal(internalRaw, &runtimeMeta); err != nil {
		return "", true, fmt.Errorf("internal metadata must be a JSON object: %w", err)
	}
	parentJobID := strings.TrimSpace(runtimeMeta.ParentJobID)
	return parentJobID, parentJobID != "", nil
}

func ExtractScheduleOccurrenceMetadata(raw json.RawMessage) (ScheduleOccurrenceMetadata, bool, error) {
	if len(strings.TrimSpace(string(raw))) == 0 {
		return ScheduleOccurrenceMetadata{}, false, nil
	}
	var root map[string]json.RawMessage
	if err := json.Unmarshal(raw, &root); err != nil {
		return ScheduleOccurrenceMetadata{}, false, fmt.Errorf("metadata must be a JSON object: %w", err)
	}
	internalRaw, ok := root["internal"]
	if !ok {
		return ScheduleOccurrenceMetadata{}, false, nil
	}
	var runtimeMeta RuntimeJobMetadata
	if err := json.Unmarshal(internalRaw, &runtimeMeta); err != nil {
		return ScheduleOccurrenceMetadata{}, true, fmt.Errorf("internal metadata must be a JSON object: %w", err)
	}
	if runtimeMeta.Schedule == nil {
		return ScheduleOccurrenceMetadata{}, false, nil
	}
	meta := *runtimeMeta.Schedule
	if meta.ScheduleId == "" || meta.Generation <= 0 || meta.SpecHash == "" || meta.ScheduledAt.IsZero() {
		return ScheduleOccurrenceMetadata{}, true, fmt.Errorf("schedule metadata missing required fields")
	}
	if meta.Kind != "" && meta.Kind != ScheduleMetadataKind {
		return ScheduleOccurrenceMetadata{}, true, fmt.Errorf("schedule metadata kind %q is invalid", meta.Kind)
	}
	if meta.Kind == "" {
		meta.Kind = ScheduleMetadataKind
	}
	meta.ScheduledAt = meta.ScheduledAt.UTC()
	if meta.RunId == "" {
		meta.RunId = ScheduleRunID(meta.ScheduledAt)
	}
	return meta, true, nil
}

func NormalizeJSON(raw json.RawMessage) json.RawMessage {
	if len(strings.TrimSpace(string(raw))) == 0 {
		return nil
	}
	var v any
	if err := json.Unmarshal(raw, &v); err != nil {
		return append(json.RawMessage(nil), raw...)
	}
	out, err := json.Marshal(v)
	if err != nil {
		return append(json.RawMessage(nil), raw...)
	}
	return out
}

func AppendScheduleFailureBit(bits string, success bool, windowSize int) string {
	for _, ch := range bits {
		if ch != '0' && ch != '1' {
			bits = ""
			break
		}
	}
	if success {
		bits += "1"
	} else {
		bits += "0"
	}
	if windowSize > 0 && len(bits) > windowSize {
		bits = bits[len(bits)-windowSize:]
	}
	return bits
}

func ScheduleFailurePolicyViolated(bits string, policy ScheduleFailurePolicy) bool {
	if bits == "" {
		return false
	}
	if policy.MaxSequentialFailures > 0 {
		count := 0
		for i := len(bits) - 1; i >= 0; i-- {
			if bits[i] != '0' {
				break
			}
			count++
		}
		if count >= policy.MaxSequentialFailures {
			return true
		}
	}
	if policy.MinSuccessPercent > 0 {
		successes := 0
		total := 0
		for _, ch := range bits {
			switch ch {
			case '1':
				successes++
				total++
			case '0':
				total++
			}
		}
		if total > 0 && successes*100 < policy.MinSuccessPercent*total {
			return true
		}
	}
	return false
}
