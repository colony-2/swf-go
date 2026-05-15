package sqlite

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"reflect"
	"sort"
	"strings"
	"sync/atomic"
	"time"

	strataartifact "github.com/colony-2/strata-go/pkg/client/artifact"
	"github.com/colony-2/strata-go/pkg/client/story"
	"github.com/colony-2/swf-go/pkg/swf"
	"github.com/google/uuid"
)

const (
	envelopeVersion        = 1
	payloadKindApp         = "App"
	payloadKindAppError    = "AppError"
	payloadKindSystemError = "SystemError"
	payloadKindTimeout     = "Timeout"

	chapterTypeJobStart           = "JobStart"
	chapterTypeJobAttemptOutcome  = "JobAttemptOutcome"
	chapterTypeTaskAttemptOutcome = "TaskAttemptOutcome"
	chapterTypeRestartExtra       = "RestartExtra"

	restartExtraTaskType = "__restart_extra__"
)

type jobPayload struct {
	RunPolicy swf.RunPolicy `json:"run_policy,omitempty"`
	TaskWait  *taskWait     `json:"task_wait,omitempty"`
}

type taskWait struct {
	InputStep  int64  `json:"in"`
	OutputStep int64  `json:"out"`
	Next       string `json:"next"`
	InputHash  string `json:"input_hash,omitempty"`
}

type chapterMeta struct {
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

type chapterMetadata struct {
	Attempt       int
	MaxAttempts   int
	NextAttemptAt *time.Time
	BackoffMillis int64
	Retryable     *bool
	InputRef      *swf.InputReference
	RunPolicy     *swf.RunPolicy
	Metadata      json.RawMessage
	InputPayload  json.RawMessage
	StartedAt     *time.Time
	FinishedAt    *time.Time
	Prerequisites []swf.JobPrerequisite
}

type chapterEnvelope struct {
	ChapterType string          `json:"chapter_type"`
	Meta        chapterMeta     `json:"meta"`
	PayloadKind string          `json:"payload_kind"`
	Payload     json.RawMessage `json:"payload"`
}

func storyKeyForJob(jobKey swf.JobKey) story.Key {
	return story.Key{AnthologyID: jobKey.TenantId, StoryID: jobKey.JobId}
}

func buildChapterEnvelope(meta chapterMeta, chapterType string, payloadKind string, payload json.RawMessage) ([]byte, error) {
	if payloadKind == "" {
		return nil, fmt.Errorf("payload kind is required")
	}
	if chapterType == "" {
		return nil, fmt.Errorf("chapter type is required")
	}
	if !json.Valid(payload) {
		return nil, fmt.Errorf("payload must be valid JSON")
	}
	return json.Marshal(chapterEnvelope{
		ChapterType: chapterType,
		Meta:        meta,
		PayloadKind: payloadKind,
		Payload:     payload,
	})
}

func decodeChapterEnvelope(body []byte) (chapterEnvelope, error) {
	var env chapterEnvelope
	if err := json.Unmarshal(body, &env); err != nil {
		return chapterEnvelope{}, err
	}
	return env, nil
}

func taskDataToChapter(jobData swf.TaskData, ordinal int64, taskType string, workerID string, chapterType string, payloadKind string, inputHash string, createdAt time.Time, meta chapterMetadata) (story.Chapter, error) {
	if jobData == nil {
		return nil, fmt.Errorf("task data is required")
	}
	data, err := jobData.GetData()
	if err != nil {
		return nil, err
	}
	artifacts, err := jobData.GetArtifacts()
	if err != nil {
		return nil, err
	}
	return payloadToChapter(data, artifacts, ordinal, taskType, workerID, chapterType, payloadKind, inputHash, createdAt, meta)
}

func taskDataToCreateOptions(jobData swf.TaskData, ordinal int64, taskType string, workerID string, chapterType string, payloadKind string, inputHash string, createdAt time.Time, meta chapterMetadata) (story.CreateOptions, error) {
	chap, err := taskDataToChapter(jobData, ordinal, taskType, workerID, chapterType, payloadKind, inputHash, createdAt, meta)
	if err != nil {
		return story.CreateOptions{}, err
	}
	return story.CreateOptions{
		RequestID:      uuid.New().String(),
		InitialChapter: chap,
	}, nil
}

func payloadToChapter(payload json.RawMessage, artifacts []swf.Artifact, ordinal int64, taskType string, workerID string, chapterType string, payloadKind string, inputHash string, createdAt time.Time, metaOpts chapterMetadata) (story.Chapter, error) {
	if payload == nil {
		return nil, fmt.Errorf("payload is required")
	}
	if inputHash == "" {
		return nil, fmt.Errorf("input hash is required")
	}
	meta := chapterMeta{
		Version:   envelopeVersion,
		Ordinal:   ordinal,
		TaskType:  taskType,
		WorkerID:  workerID,
		CreatedAt: createdAt,
		InputHash: inputHash,
	}
	if metaOpts.Attempt > 0 {
		meta.Attempt = metaOpts.Attempt
	}
	if metaOpts.MaxAttempts > 0 {
		meta.MaxAttempts = metaOpts.MaxAttempts
	}
	if metaOpts.NextAttemptAt != nil {
		meta.NextAttemptAt = metaOpts.NextAttemptAt
	}
	if metaOpts.BackoffMillis > 0 {
		meta.BackoffMillis = metaOpts.BackoffMillis
	}
	if metaOpts.Retryable != nil {
		meta.Retryable = metaOpts.Retryable
	}
	if metaOpts.InputRef != nil {
		meta.InputRef = metaOpts.InputRef
	}
	if metaOpts.RunPolicy != nil {
		meta.RunPolicy = metaOpts.RunPolicy
	}
	if len(metaOpts.Metadata) > 0 {
		meta.Metadata = append(json.RawMessage(nil), metaOpts.Metadata...)
	}
	if metaOpts.InputPayload != nil {
		meta.Input = append(json.RawMessage(nil), metaOpts.InputPayload...)
	}
	if metaOpts.StartedAt != nil {
		meta.StartedAt = metaOpts.StartedAt
	}
	if metaOpts.FinishedAt != nil {
		meta.FinishedAt = metaOpts.FinishedAt
	}
	if len(metaOpts.Prerequisites) > 0 {
		meta.Prerequisites = append([]swf.JobPrerequisite(nil), metaOpts.Prerequisites...)
	}
	envBytes, err := buildChapterEnvelope(meta, chapterType, payloadKind, payload)
	if err != nil {
		return nil, err
	}
	builder := story.NewChapter().WithOrdinal(ordinal).WithBytes(envBytes)
	for _, art := range artifacts {
		builder.AddArtifact(toStrataArtifact(art))
	}
	return builder, nil
}

func encodeStoredChapter(chapter swf.StoredChapter) ([]byte, error) {
	meta := chapterMeta{
		Version:   envelopeVersion,
		Ordinal:   chapter.Ordinal,
		TaskType:  chapter.TaskType,
		CreatedAt: chapter.CreatedAt,
		InputHash: chapter.InputHash,
	}
	if len(chapter.Metadata) > 0 {
		if err := json.Unmarshal(chapter.Metadata, &meta); err != nil {
			return nil, fmt.Errorf("decode chapter metadata: %w", err)
		}
		if meta.Ordinal == 0 {
			meta.Ordinal = chapter.Ordinal
		}
		if meta.TaskType == "" {
			meta.TaskType = chapter.TaskType
		}
		if meta.CreatedAt.IsZero() {
			meta.CreatedAt = chapter.CreatedAt
		}
		if meta.InputHash == "" {
			meta.InputHash = chapter.InputHash
		}
		if meta.Version == 0 {
			meta.Version = envelopeVersion
		}
	}
	if meta.CreatedAt.IsZero() {
		meta.CreatedAt = chapter.CreatedAt
	}
	return buildChapterEnvelope(meta, chapter.ChapterType, chapter.PayloadKind, chapter.Data)
}

func storedChapterFromStoryChapter(chapter story.Chapter) (swf.StoredChapter, error) {
	env, err := decodeChapterEnvelope(chapter.Body())
	if err != nil {
		return swf.StoredChapter{}, err
	}
	metadata, err := json.Marshal(env.Meta)
	if err != nil {
		return swf.StoredChapter{}, fmt.Errorf("encode chapter metadata: %w", err)
	}
	artifacts := make([]swf.StoredArtifact, 0, len(chapter.Artifacts()))
	for _, art := range chapter.Artifacts() {
		if art == nil {
			continue
		}
		digest, _ := art.Sha256(context.Background())
		artifacts = append(artifacts, swf.StoredArtifact{
			Name:   art.Name(),
			Digest: digest,
			Size:   art.SizeBytes(),
		})
	}
	return swf.StoredChapter{
		Ordinal:     chapter.Ordinal(),
		TaskType:    env.Meta.TaskType,
		ChapterType: env.ChapterType,
		PayloadKind: env.PayloadKind,
		InputHash:   env.Meta.InputHash,
		CreatedAt:   env.Meta.CreatedAt,
		Metadata:    metadata,
		Data:        append(json.RawMessage(nil), env.Payload...),
		Artifacts:   artifacts,
	}, nil
}

func chapterToTaskData(chapter story.Chapter, jobKey swf.JobKey) (swf.TaskData, error) {
	artifacts := make([]swf.Artifact, 0, len(chapter.Artifacts()))
	for _, art := range chapter.Artifacts() {
		artifacts = append(artifacts, fromStrataArtifact(art))
	}
	assignArtifactKeys(artifacts, jobKey.JobId, chapter.Ordinal())
	env, err := decodeChapterEnvelope(chapter.Body())
	if err != nil {
		return nil, err
	}
	return envelopeToTaskData(env, artifacts)
}

func envelopeToTaskData(env chapterEnvelope, artifacts []swf.Artifact) (swf.TaskData, error) {
	copiedArtifacts := make([]swf.Artifact, 0, len(artifacts))
	for _, art := range artifacts {
		copiedArtifacts = append(copiedArtifacts, art)
	}
	payload := append([]byte(nil), env.Payload...)
	td := &swf.EnvelopedTaskData{
		SimpleTaskData: swf.SimpleTaskData{Data: swf.Data(payload), Artifacts: copiedArtifacts},
		Kind:           env.PayloadKind,
	}
	switch env.PayloadKind {
	case payloadKindApp:
		return td, nil
	case payloadKindTimeout:
		var p swf.TimeoutPayload
		if err := json.Unmarshal(env.Payload, &p); err != nil {
			return td, err
		}
		return td, &swf.TimeoutError{Payload: p}
	case payloadKindAppError:
		var p swf.AppErrorPayload
		if err := json.Unmarshal(env.Payload, &p); err != nil {
			return td, err
		}
		return td, &swf.AppError{Payload: p}
	case payloadKindSystemError:
		var p swf.SystemErrorPayload
		if err := json.Unmarshal(env.Payload, &p); err != nil {
			return td, err
		}
		return td, &swf.SystemError{Payload: p}
	default:
		return td, fmt.Errorf("unsupported payload kind %q", env.PayloadKind)
	}
}

func computeInputHash(ctx context.Context, taskData swf.TaskData) (string, error) {
	if taskData == nil {
		return "", fmt.Errorf("task data is required for hashing")
	}
	data, err := taskData.GetData()
	if err != nil {
		return "", err
	}
	artifacts, err := taskData.GetArtifacts()
	if err != nil {
		return "", err
	}
	parts := make([]string, 0, len(artifacts))
	for _, art := range artifacts {
		hash, err := art.Sha256(ctx)
		if err != nil {
			return "", err
		}
		parts = append(parts, fmt.Sprintf("%s|%s", art.Name(), hash))
	}
	sort.Strings(parts)
	h := sha256.New()
	_, _ = h.Write(data)
	for _, part := range parts {
		_, _ = h.Write([]byte(part))
	}
	computedHash := fmt.Sprintf("%x", h.Sum(nil))
	slog.Default().Debug("computeInputHash: data being hashed", "hash", computedHash, "dataLength", len(data), "artifactCount", len(artifacts))
	return computedHash, nil
}

func normalizeRetryPolicy(policy swf.RetryPolicy) swf.RetryPolicy {
	rp := policy
	if rp.MaximumAttempts <= 0 {
		rp.MaximumAttempts = 1
	}
	if rp.BackoffCoefficient == 0 {
		rp.BackoffCoefficient = 1
	}
	return rp
}

func normalizeTimeout(d *swf.Duration) *swf.Duration {
	if d == nil {
		return nil
	}
	if time.Duration(*d) < 0 {
		return nil
	}
	val := *d
	return &val
}

func normalizeRunPolicy(policy swf.RunPolicy) swf.RunPolicy {
	p := policy
	p.Retry = normalizeRetryPolicy(p.Retry)
	p.InvocationTimeout = normalizeTimeout(p.InvocationTimeout)
	p.TotalTimeout = normalizeTimeout(p.TotalTimeout)
	return p
}

func computeBackoff(rp swf.RetryPolicy, attempt int) time.Duration {
	base := time.Duration(rp.InitialInterval)
	backoff := float64(base)
	if attempt > 1 {
		backoff = float64(base) * math.Pow(rp.BackoffCoefficient, float64(attempt-1))
	}
	dur := time.Duration(backoff)
	maxInterval := time.Duration(rp.MaximumInterval)
	if maxInterval > 0 && dur > maxInterval {
		dur = maxInterval
	}
	if dur < 0 {
		dur = 0
	}
	return dur
}

func normalizePrerequisites(jobKey swf.JobKey, prereqs []swf.JobPrerequisite) ([]swf.JobPrerequisite, []string, error) {
	if len(prereqs) == 0 {
		return nil, nil, nil
	}
	seen := make(map[string]struct{}, len(prereqs))
	normalized := make([]swf.JobPrerequisite, 0, len(prereqs))
	waitFor := make([]string, 0, len(prereqs))
	for _, p := range prereqs {
		if strings.TrimSpace(p.JobID) == "" {
			return nil, nil, fmt.Errorf("prerequisite job id is required")
		}
		if p.JobID == jobKey.JobId {
			return nil, nil, fmt.Errorf("prerequisite job id cannot reference self")
		}
		if _, ok := seen[p.JobID]; ok {
			continue
		}
		seen[p.JobID] = struct{}{}
		if p.Condition == "" {
			p.Condition = swf.JobPrereqComplete
		}
		switch p.Condition {
		case swf.JobPrereqComplete, swf.JobPrereqSuccess:
		default:
			return nil, nil, fmt.Errorf("invalid prerequisite condition %q", p.Condition)
		}
		normalized = append(normalized, p)
		waitFor = append(waitFor, p.JobID)
	}
	return normalized, waitFor, nil
}

func extractTaskWaitFromRaw(payloadJSON json.RawMessage) (*taskWait, error) {
	var payload jobPayload
	if err := json.Unmarshal(payloadJSON, &payload); err == nil && payload.TaskWait != nil {
		return payload.TaskWait, nil
	}
	var legacy taskWait
	if err := json.Unmarshal(payloadJSON, &legacy); err != nil {
		return nil, err
	}
	return &legacy, nil
}

func taskTypeFromCapability(capability string) string {
	parts := strings.SplitN(capability, ":", 2)
	if len(parts) == 2 {
		return parts[1]
	}
	return capability
}

func workerCapability(jobType, taskType string) string {
	if taskType == "" {
		return jobType
	}
	return jobType + ":" + taskType
}

func metadataForStartChapter(raw json.RawMessage) json.RawMessage {
	if len(bytes.TrimSpace(raw)) == 0 {
		return json.RawMessage(`{}`)
	}
	return append(json.RawMessage(nil), raw...)
}

func jsonObjectsEqual(left json.RawMessage, right json.RawMessage) bool {
	leftNorm, leftErr := normalizeJSONObject(left)
	rightNorm, rightErr := normalizeJSONObject(right)
	if leftErr != nil || rightErr != nil {
		return bytes.Equal(bytes.TrimSpace(left), bytes.TrimSpace(right))
	}
	return bytes.Equal(leftNorm, rightNorm)
}

func normalizeJSONObject(raw json.RawMessage) (json.RawMessage, error) {
	if len(bytes.TrimSpace(raw)) == 0 {
		return json.RawMessage(`{}`), nil
	}
	var value map[string]any
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil, err
	}
	if value == nil {
		value = map[string]any{}
	}
	return json.Marshal(value)
}

func normalizePrereqSlice(prereqs []swf.JobPrerequisite) []swf.JobPrerequisite {
	if len(prereqs) == 0 {
		return nil
	}
	return append([]swf.JobPrerequisite(nil), prereqs...)
}

func runPolicyFromMetadata(raw json.RawMessage) (swf.RunPolicy, error) {
	if len(raw) == 0 {
		return swf.RunPolicy{}, nil
	}
	var payload struct {
		RunPolicy swf.RunPolicy `json:"run_policy"`
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return swf.RunPolicy{}, err
	}
	return payload.RunPolicy, nil
}

func attemptFromMetadata(raw json.RawMessage) (int, error) {
	if len(raw) == 0 {
		return 1, nil
	}
	var payload struct {
		Attempt int `json:"attempt"`
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return 0, err
	}
	if payload.Attempt == 0 {
		return 1, nil
	}
	return payload.Attempt, nil
}

func compareSubmitStartChapter(jobKey swf.JobKey, chapter story.Chapter, jobType string, inputHash string, metadata json.RawMessage, prereqs []swf.JobPrerequisite, jobPolicy swf.RunPolicy) error {
	env, err := decodeChapterEnvelope(chapter.Body())
	if err != nil {
		return swf.NewExistingJobMismatchError(fmt.Sprintf("job %s start chapter could not be decoded: %v", jobKey, err))
	}
	if env.ChapterType != chapterTypeJobStart {
		return swf.NewExistingJobMismatchError(fmt.Sprintf("job %s already exists with chapter type %q at ordinal 0", jobKey, env.ChapterType))
	}
	if env.Meta.TaskType != jobType {
		return swf.NewExistingJobMismatchError(fmt.Sprintf("job %s already exists with different job type", jobKey))
	}
	if env.Meta.InputHash != inputHash {
		return swf.NewExistingJobMismatchError(fmt.Sprintf("job %s already exists with different input", jobKey))
	}
	if len(bytes.TrimSpace(env.Meta.Metadata)) > 0 && !jsonObjectsEqual(env.Meta.Metadata, metadata) {
		return swf.NewExistingJobMismatchError(fmt.Sprintf("job %s already exists with different metadata", jobKey))
	}
	existingPolicy := swf.RunPolicy{}
	if env.Meta.RunPolicy != nil {
		existingPolicy = normalizeRunPolicy(*env.Meta.RunPolicy)
	} else {
		existingPolicy = normalizeRunPolicy(existingPolicy)
	}
	if !reflect.DeepEqual(existingPolicy, jobPolicy) {
		return swf.NewExistingJobMismatchError(fmt.Sprintf("job %s already exists with different run policy", jobKey))
	}
	if !reflect.DeepEqual(normalizePrereqSlice(env.Meta.Prerequisites), normalizePrereqSlice(prereqs)) {
		return swf.NewExistingJobMismatchError(fmt.Sprintf("job %s already exists with different prerequisites", jobKey))
	}
	return nil
}

func fromStrataArtifact(strataArt strataartifact.Artifact) swf.Artifact {
	return &strataArtifactAdapter{art: strataArt}
}

func toStrataArtifact(art swf.Artifact) strataartifact.Artifact {
	if adapter, ok := art.(*strataArtifactAdapter); ok {
		return adapter.art
	}
	return &swfToStrataAdapter{art: art}
}

type strataArtifactAdapter struct {
	art strataartifact.Artifact
	key atomic.Pointer[swf.ArtifactKey]
}

func (a *strataArtifactAdapter) Name() string { return a.art.Name() }
func (a *strataArtifactAdapter) Size() int64  { return a.art.SizeBytes() }
func (a *strataArtifactAdapter) Sha256(ctx context.Context) (string, error) {
	return a.art.Sha256(ctx)
}
func (a *strataArtifactAdapter) WriteTo(ctx context.Context, w io.Writer) error {
	return a.art.WriteTo(ctx, w)
}
func (a *strataArtifactAdapter) SaveToFile(ctx context.Context, path string) error {
	return a.art.SaveToFile(ctx, path)
}
func (a *strataArtifactAdapter) Bytes(ctx context.Context) ([]byte, error) {
	return a.art.Bytes(ctx)
}
func (a *strataArtifactAdapter) Open() (io.ReadCloser, error) {
	_, rc, err := a.art.ToInput(context.Background())
	return rc, err
}
func (a *strataArtifactAdapter) ArtifactKey() (swf.ArtifactKey, error) {
	if value := a.key.Load(); value != nil {
		return *value, nil
	}
	return swf.ArtifactKey{}, swf.ErrArtifactKeyUnavailable
}
func (a *strataArtifactAdapter) setArtifactKey(key swf.ArtifactKey) { a.key.Store(&key) }
func (a *strataArtifactAdapter) Cleanup() error {
	if cleanup, ok := a.art.(interface{ Cleanup() error }); ok {
		return cleanup.Cleanup()
	}
	return nil
}

type swfToStrataAdapter struct {
	art swf.Artifact
}

func (a *swfToStrataAdapter) ID() string          { return "" }
func (a *swfToStrataAdapter) Name() string        { return a.art.Name() }
func (a *swfToStrataAdapter) ContentType() string { return "application/octet-stream" }
func (a *swfToStrataAdapter) SizeBytes() int64    { return a.art.Size() }
func (a *swfToStrataAdapter) Sha256(ctx context.Context) (string, error) {
	return a.art.Sha256(ctx)
}
func (a *swfToStrataAdapter) WriteTo(ctx context.Context, w io.Writer) error {
	return a.art.WriteTo(ctx, w)
}
func (a *swfToStrataAdapter) SaveToFile(ctx context.Context, path string) error {
	return a.art.SaveToFile(ctx, path)
}
func (a *swfToStrataAdapter) Bytes(ctx context.Context) ([]byte, error) {
	return a.art.Bytes(ctx)
}
func (a *swfToStrataAdapter) ToInput(ctx context.Context) (strataartifact.Descriptor, io.ReadCloser, error) {
	rc, err := a.art.Open()
	if err != nil {
		return strataartifact.Descriptor{}, nil, err
	}
	return strataartifact.Descriptor{
		Name:        a.art.Name(),
		ContentType: "application/octet-stream",
		SizeBytes:   a.art.Size(),
	}, rc, nil
}

func assignArtifactKeys(artifacts []swf.Artifact, jobID string, ordinal int64) {
	if jobID == "" || ordinal < 0 {
		return
	}
	for _, art := range artifacts {
		if art == nil || art.Name() == "" {
			continue
		}
		swf.AssignArtifactKey(art, swf.ArtifactKey{
			JobId:       jobID,
			TaskOrdinal: ordinal,
			Name:        art.Name(),
			SizeBytes:   art.Size(),
		})
	}
}

func cleanupArtifacts(artifacts []swf.Artifact, logger *slog.Logger) {
	if logger == nil {
		logger = slog.Default()
	}
	for _, art := range artifacts {
		if art == nil {
			continue
		}
		if err := art.Cleanup(); err != nil {
			logger.Warn("artifact cleanup failed", "name", art.Name(), "error", err)
		}
	}
}

func validateChapterArtifactDescriptors(existing []swf.StoredArtifact, computed []swf.StoredArtifact) error {
	if len(existing) == 0 {
		return nil
	}
	if len(existing) != len(computed) {
		return fmt.Errorf("chapter artifact metadata count %d does not match uploads %d", len(existing), len(computed))
	}
	for i := range existing {
		if existing[i].Name != computed[i].Name {
			return fmt.Errorf("chapter artifact %d name %q does not match uploaded artifact %q", i, existing[i].Name, computed[i].Name)
		}
		if existing[i].Size != 0 && existing[i].Size != computed[i].Size {
			return fmt.Errorf("chapter artifact %q size %d does not match uploaded size %d", existing[i].Name, existing[i].Size, computed[i].Size)
		}
		if existing[i].Digest != "" && existing[i].Digest != computed[i].Digest {
			return fmt.Errorf("chapter artifact %q digest %q does not match uploaded digest %q", existing[i].Name, existing[i].Digest, computed[i].Digest)
		}
	}
	return nil
}

type artifactFingerprint struct {
	Name   string
	Digest string
	Size   int64
}

func storyChapterArtifacts(ctx context.Context, chapter story.Chapter) ([]artifactFingerprint, error) {
	if chapter == nil {
		return nil, nil
	}
	out := make([]artifactFingerprint, 0, len(chapter.Artifacts()))
	for _, art := range chapter.Artifacts() {
		if art == nil {
			continue
		}
		digest, err := art.Sha256(ctx)
		if err != nil {
			return nil, err
		}
		out = append(out, artifactFingerprint{Name: art.Name(), Digest: digest, Size: art.SizeBytes()})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Name < out[j].Name })
	return out, nil
}

func sameStoryChapter(ctx context.Context, left story.Chapter, right story.Chapter) (bool, error) {
	if !bytes.Equal(left.Body(), right.Body()) {
		return false, nil
	}
	leftArtifacts, err := storyChapterArtifacts(ctx, left)
	if err != nil {
		return false, err
	}
	rightArtifacts, err := storyChapterArtifacts(ctx, right)
	if err != nil {
		return false, err
	}
	return reflect.DeepEqual(leftArtifacts, rightArtifacts), nil
}

func completionStatusFromRequest(status string) string {
	switch status {
	case "", "success", "succeeded":
		return "success"
	case "failed_app", "failed_system", "failed_timeout", "cancelled":
		return status
	default:
		return status
	}
}

func isLeaseMutationLost(err error) bool {
	return errors.Is(err, swf.ErrExecutionLeaseLost)
}
