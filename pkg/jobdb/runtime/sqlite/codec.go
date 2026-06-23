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

	"github.com/colony-2/jobdb/pkg/internal/runtimecodec"
	"github.com/colony-2/jobdb/pkg/jobdb"
	strataartifact "github.com/colony-2/strata-go/pkg/client/artifact"
	"github.com/colony-2/strata-go/pkg/client/story"
	"github.com/google/uuid"
)

const (
	envelopeVersion        = runtimecodec.EnvelopeVersion
	payloadKindApp         = runtimecodec.PayloadKindApp
	payloadKindAppError    = runtimecodec.PayloadKindAppError
	payloadKindSystemError = runtimecodec.PayloadKindSystemError
	payloadKindTimeout     = runtimecodec.PayloadKindTimeout

	chapterTypeJobStart           = runtimecodec.ChapterTypeJobStart
	chapterTypeJobAttemptOutcome  = runtimecodec.ChapterTypeJobAttemptOutcome
	chapterTypeTaskAttemptOutcome = runtimecodec.ChapterTypeTaskAttemptOutcome
	chapterTypeRestartExtra       = runtimecodec.ChapterTypeRestartExtra

	restartExtraTaskType = "__restart_extra__"
)

type jobPayload = runtimecodec.SchedulerPayload
type taskWait = runtimecodec.TaskWait

type chapterMeta = runtimecodec.ChapterMeta

type chapterMetadata struct {
	Attempt       int
	MaxAttempts   int
	NextAttemptAt *time.Time
	BackoffMillis int64
	Retryable     *bool
	InputRef      *jobdb.InputReference
	RunPolicy     *jobdb.RunPolicy
	Metadata      json.RawMessage
	InputPayload  json.RawMessage
	StartedAt     *time.Time
	FinishedAt    *time.Time
	Prerequisites []jobdb.JobPrerequisite
}

type chapterEnvelope = runtimecodec.ChapterEnvelope

func storyKeyForJob(jobKey jobdb.JobKey) story.Key {
	return story.Key{AnthologyID: jobKey.TenantId, StoryID: jobKey.JobId}
}

func buildChapterEnvelope(meta chapterMeta, chapterType string, payloadKind string, payload json.RawMessage) ([]byte, error) {
	return runtimecodec.EncodeChapter(meta, chapterType, payloadKind, payload)
}

func decodeChapterEnvelope(body []byte) (chapterEnvelope, error) {
	return runtimecodec.DecodeChapter(body)
}

func taskDataToChapter(jobData jobdb.TaskData, ordinal int64, taskType string, workerID string, chapterType string, payloadKind string, inputHash string, createdAt time.Time, meta chapterMetadata) (story.Chapter, error) {
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

func taskDataToCreateOptions(jobData jobdb.TaskData, ordinal int64, taskType string, workerID string, chapterType string, payloadKind string, inputHash string, createdAt time.Time, meta chapterMetadata) (story.CreateOptions, error) {
	chap, err := taskDataToChapter(jobData, ordinal, taskType, workerID, chapterType, payloadKind, inputHash, createdAt, meta)
	if err != nil {
		return story.CreateOptions{}, err
	}
	return story.CreateOptions{
		RequestID:      uuid.New().String(),
		InitialChapter: chap,
	}, nil
}

func payloadToChapter(payload json.RawMessage, artifacts []jobdb.Artifact, ordinal int64, taskType string, workerID string, chapterType string, payloadKind string, inputHash string, createdAt time.Time, metaOpts chapterMetadata) (story.Chapter, error) {
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
		meta.Prerequisites = append([]jobdb.JobPrerequisite(nil), metaOpts.Prerequisites...)
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

func encodeChapter(chapter jobdb.Chapter) ([]byte, error) {
	meta := chapterMeta{
		Version:   envelopeVersion,
		Ordinal:   chapter.Ordinal,
		TaskType:  chapter.TaskType,
		CreatedAt: chapter.CreatedAt,
		InputHash: chapter.InputHash,
	}
	rawMetadata, err := runtimecodec.ChapterMetadataToJSON(chapter.Metadata)
	if err != nil {
		return nil, fmt.Errorf("encode chapter metadata: %w", err)
	}
	if len(rawMetadata) > 0 {
		if err := json.Unmarshal(rawMetadata, &meta); err != nil {
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
	chapterType, payloadKind, payload, err := runtimecodec.ChapterBodyToWire(chapter.Body)
	if err != nil {
		return nil, err
	}
	return buildChapterEnvelope(meta, chapterType, payloadKind, payload)
}

func chapterFromStoryChapter(chapter story.Chapter) (jobdb.Chapter, error) {
	env, err := decodeChapterEnvelope(chapter.Body())
	if err != nil {
		return jobdb.Chapter{}, err
	}
	rawMetadata, err := json.Marshal(env.Meta)
	if err != nil {
		return jobdb.Chapter{}, fmt.Errorf("encode chapter metadata: %w", err)
	}
	metadata, err := runtimecodec.ChapterMetadataFromJSON(rawMetadata)
	if err != nil {
		return jobdb.Chapter{}, fmt.Errorf("decode chapter metadata: %w", err)
	}
	body, err := runtimecodec.ChapterBodyFromWire(env.ChapterType, env.PayloadKind, env.Payload)
	if err != nil {
		return jobdb.Chapter{}, err
	}
	artifacts := make([]jobdb.StoredArtifact, 0, len(chapter.Artifacts()))
	for _, art := range chapter.Artifacts() {
		if art == nil {
			continue
		}
		digest, _ := art.Sha256(context.Background())
		artifacts = append(artifacts, jobdb.StoredArtifact{
			Name:   art.Name(),
			Digest: digest,
			Size:   art.SizeBytes(),
		})
	}
	return jobdb.Chapter{
		Ordinal:   chapter.Ordinal(),
		TaskType:  env.Meta.TaskType,
		Body:      body,
		InputHash: env.Meta.InputHash,
		CreatedAt: env.Meta.CreatedAt,
		Metadata:  metadata,
		Artifacts: artifacts,
	}, nil
}

func chapterToTaskData(chapter story.Chapter, jobKey jobdb.JobKey) (jobdb.TaskData, error) {
	artifacts := make([]jobdb.Artifact, 0, len(chapter.Artifacts()))
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

func envelopeToTaskData(env chapterEnvelope, artifacts []jobdb.Artifact) (jobdb.TaskData, error) {
	copiedArtifacts := make([]jobdb.Artifact, 0, len(artifacts))
	for _, art := range artifacts {
		copiedArtifacts = append(copiedArtifacts, art)
	}
	payload := append([]byte(nil), env.Payload...)
	td := &jobdb.EnvelopedTaskData{
		SimpleTaskData: jobdb.SimpleTaskData{Data: jobdb.Data(payload), Artifacts: copiedArtifacts},
		Kind:           env.PayloadKind,
	}
	switch env.PayloadKind {
	case payloadKindApp:
		return td, nil
	case payloadKindTimeout:
		var p jobdb.TimeoutPayload
		if err := json.Unmarshal(env.Payload, &p); err != nil {
			return td, err
		}
		return td, &jobdb.TimeoutError{Payload: p}
	case payloadKindAppError:
		var p jobdb.AppErrorPayload
		if err := json.Unmarshal(env.Payload, &p); err != nil {
			return td, err
		}
		return td, &jobdb.AppError{Payload: p}
	case payloadKindSystemError:
		var p jobdb.SystemErrorPayload
		if err := json.Unmarshal(env.Payload, &p); err != nil {
			return td, err
		}
		return td, &jobdb.SystemError{Payload: p}
	default:
		return td, fmt.Errorf("unsupported payload kind %q", env.PayloadKind)
	}
}

func computeInputHash(ctx context.Context, taskData jobdb.TaskData) (string, error) {
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

func normalizeRetryPolicy(policy jobdb.RetryPolicy) jobdb.RetryPolicy {
	rp := policy
	if rp.MaximumAttempts <= 0 {
		rp.MaximumAttempts = 1
	}
	if rp.BackoffCoefficient == 0 {
		rp.BackoffCoefficient = 1
	}
	return rp
}

func normalizeTimeout(d *jobdb.Duration) *jobdb.Duration {
	if d == nil {
		return nil
	}
	if time.Duration(*d) < 0 {
		return nil
	}
	val := *d
	return &val
}

func normalizeRunPolicy(policy jobdb.RunPolicy) jobdb.RunPolicy {
	p := policy
	p.Retry = normalizeRetryPolicy(p.Retry)
	p.InvocationTimeout = normalizeTimeout(p.InvocationTimeout)
	p.TotalTimeout = normalizeTimeout(p.TotalTimeout)
	return p
}

func computeBackoff(rp jobdb.RetryPolicy, attempt int) time.Duration {
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

func normalizePrerequisites(jobKey jobdb.JobKey, prereqs []jobdb.JobPrerequisite) ([]jobdb.JobPrerequisite, []string, error) {
	if len(prereqs) == 0 {
		return nil, nil, nil
	}
	seen := make(map[string]struct{}, len(prereqs))
	normalized := make([]jobdb.JobPrerequisite, 0, len(prereqs))
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
			p.Condition = jobdb.JobPrereqComplete
		}
		switch p.Condition {
		case jobdb.JobPrereqComplete, jobdb.JobPrereqSuccess:
		default:
			return nil, nil, fmt.Errorf("invalid prerequisite condition %q", p.Condition)
		}
		normalized = append(normalized, p)
		waitFor = append(waitFor, p.JobID)
	}
	return normalized, waitFor, nil
}

func extractTaskWaitFromRaw(payloadJSON json.RawMessage) (*taskWait, error) {
	payload, err := decodeJobPayload(payloadJSON)
	if err != nil {
		return nil, err
	}
	return payload.TaskWait, nil
}

func encodeJobPayload(payload jobPayload) ([]byte, error) {
	return runtimecodec.EncodeSchedulerPayload(payload)
}

func decodeJobPayload(raw []byte) (jobPayload, error) {
	return runtimecodec.DecodeSchedulerPayload(raw)
}

func jobPayloadFromVisibleJSON(raw json.RawMessage) (jobPayload, error) {
	return runtimecodec.SchedulerPayloadFromJSONView(raw)
}

func jobPayloadVisibleJSON(raw []byte) json.RawMessage {
	payload, err := decodeJobPayload(raw)
	if err != nil {
		return json.RawMessage(`{}`)
	}
	view, err := runtimecodec.SchedulerPayloadJSONView(payload)
	if err != nil {
		return json.RawMessage(`{}`)
	}
	return view
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

func normalizePrereqSlice(prereqs []jobdb.JobPrerequisite) []jobdb.JobPrerequisite {
	if len(prereqs) == 0 {
		return nil
	}
	return append([]jobdb.JobPrerequisite(nil), prereqs...)
}

func runPolicyFromMetadata(raw json.RawMessage) (jobdb.RunPolicy, error) {
	if len(raw) == 0 {
		return jobdb.RunPolicy{}, nil
	}
	var payload struct {
		RunPolicy jobdb.RunPolicy `json:"run_policy"`
	}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return jobdb.RunPolicy{}, err
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

func compareSubmitStartChapter(jobKey jobdb.JobKey, chapter story.Chapter, jobType string, inputHash string, metadata json.RawMessage, prereqs []jobdb.JobPrerequisite, jobPolicy jobdb.RunPolicy) error {
	env, err := decodeChapterEnvelope(chapter.Body())
	if err != nil {
		return jobdb.NewExistingJobMismatchError(fmt.Sprintf("job %s start chapter could not be decoded: %v", jobKey, err))
	}
	if env.ChapterType != chapterTypeJobStart {
		return jobdb.NewExistingJobMismatchError(fmt.Sprintf("job %s already exists with chapter type %q at ordinal 0", jobKey, env.ChapterType))
	}
	if env.Meta.TaskType != jobType {
		return jobdb.NewExistingJobMismatchError(fmt.Sprintf("job %s already exists with different job type", jobKey))
	}
	if env.Meta.InputHash != inputHash {
		return jobdb.NewExistingJobMismatchError(fmt.Sprintf("job %s already exists with different input", jobKey))
	}
	if len(bytes.TrimSpace(env.Meta.Metadata)) > 0 && !jsonObjectsEqual(env.Meta.Metadata, metadata) {
		return jobdb.NewExistingJobMismatchError(fmt.Sprintf("job %s already exists with different metadata", jobKey))
	}
	existingPolicy := jobdb.RunPolicy{}
	if env.Meta.RunPolicy != nil {
		existingPolicy = normalizeRunPolicy(*env.Meta.RunPolicy)
	} else {
		existingPolicy = normalizeRunPolicy(existingPolicy)
	}
	if !reflect.DeepEqual(existingPolicy, jobPolicy) {
		return jobdb.NewExistingJobMismatchError(fmt.Sprintf("job %s already exists with different run policy", jobKey))
	}
	if !reflect.DeepEqual(normalizePrereqSlice(env.Meta.Prerequisites), normalizePrereqSlice(prereqs)) {
		return jobdb.NewExistingJobMismatchError(fmt.Sprintf("job %s already exists with different prerequisites", jobKey))
	}
	return nil
}

func fromStrataArtifact(strataArt strataartifact.Artifact) jobdb.Artifact {
	return &strataArtifactAdapter{art: strataArt}
}

func toStrataArtifact(art jobdb.Artifact) strataartifact.Artifact {
	if adapter, ok := art.(*strataArtifactAdapter); ok {
		return adapter.art
	}
	return &jobdbToStrataAdapter{art: art}
}

type strataArtifactAdapter struct {
	art strataartifact.Artifact
	key atomic.Pointer[jobdb.ArtifactKey]
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
func (a *strataArtifactAdapter) ArtifactKey() (jobdb.ArtifactKey, error) {
	if value := a.key.Load(); value != nil {
		return *value, nil
	}
	return jobdb.ArtifactKey{}, jobdb.ErrArtifactKeyUnavailable
}
func (a *strataArtifactAdapter) setArtifactKey(key jobdb.ArtifactKey) { a.key.Store(&key) }
func (a *strataArtifactAdapter) Cleanup() error {
	if cleanup, ok := a.art.(interface{ Cleanup() error }); ok {
		return cleanup.Cleanup()
	}
	return nil
}

type jobdbToStrataAdapter struct {
	art jobdb.Artifact
}

func (a *jobdbToStrataAdapter) ID() string          { return "" }
func (a *jobdbToStrataAdapter) Name() string        { return a.art.Name() }
func (a *jobdbToStrataAdapter) ContentType() string { return "application/octet-stream" }
func (a *jobdbToStrataAdapter) SizeBytes() int64    { return a.art.Size() }
func (a *jobdbToStrataAdapter) Sha256(ctx context.Context) (string, error) {
	return a.art.Sha256(ctx)
}
func (a *jobdbToStrataAdapter) WriteTo(ctx context.Context, w io.Writer) error {
	return a.art.WriteTo(ctx, w)
}
func (a *jobdbToStrataAdapter) SaveToFile(ctx context.Context, path string) error {
	return a.art.SaveToFile(ctx, path)
}
func (a *jobdbToStrataAdapter) Bytes(ctx context.Context) ([]byte, error) {
	return a.art.Bytes(ctx)
}
func (a *jobdbToStrataAdapter) ToInput(ctx context.Context) (strataartifact.Descriptor, io.ReadCloser, error) {
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

func assignArtifactKeys(artifacts []jobdb.Artifact, jobID string, ordinal int64) {
	if jobID == "" || ordinal < 0 {
		return
	}
	for _, art := range artifacts {
		if art == nil || art.Name() == "" {
			continue
		}
		jobdb.AssignArtifactKey(art, jobdb.ArtifactKey{
			JobId:       jobID,
			TaskOrdinal: ordinal,
			Name:        art.Name(),
			SizeBytes:   art.Size(),
		})
	}
}

func cleanupArtifacts(artifacts []jobdb.Artifact, logger *slog.Logger) {
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

func validateChapterArtifactDescriptors(existing []jobdb.StoredArtifact, computed []jobdb.StoredArtifact) error {
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
	return errors.Is(err, jobdb.ErrExecutionLeaseLost)
}
