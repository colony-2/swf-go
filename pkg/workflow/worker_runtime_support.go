package workflow

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync/atomic"
	"time"
)

type workerJobPayload struct {
	RunPolicy RunPolicy       `json:"run_policy,omitempty"`
	TaskWait  *workerTaskWait `json:"task_wait,omitempty"`
}

type workerTaskWait struct {
	InputStep  int64  `json:"in"`
	OutputStep int64  `json:"out"`
	Next       string `json:"next"`
	InputHash  string `json:"input_hash,omitempty"`
}

type runtimeBackedArtifact struct {
	runtime WorkflowRuntime
	ref     ArtifactRef
	size    int64
	hash    atomic.Pointer[string]
	key     atomic.Pointer[ArtifactKey]
}

func newRuntimeBackedArtifact(runtime WorkflowRuntime, ref ArtifactRef, size int64) Artifact {
	art := &runtimeBackedArtifact{
		runtime: runtime,
		ref:     ref,
		size:    size,
	}
	if ref.Digest != "" {
		hash := ref.Digest
		art.hash.Store(&hash)
	}
	key := ArtifactKey{
		JobId:       ref.JobKey.JobId,
		TaskOrdinal: ref.Ordinal,
		Name:        ref.Name,
		SizeBytes:   size,
	}
	art.key.Store(&key)
	return art
}

func (a *runtimeBackedArtifact) Name() string { return a.ref.Name }

func (a *runtimeBackedArtifact) Size() int64 { return a.size }

func (a *runtimeBackedArtifact) ArtifactKey() (ArtifactKey, error) {
	return loadArtifactKey(&a.key)
}

func (a *runtimeBackedArtifact) Open() (io.ReadCloser, error) {
	reader, err := a.runtime.OpenArtifact(context.Background(), a.ref)
	if err != nil {
		return nil, err
	}
	return reader.Open()
}

func (a *runtimeBackedArtifact) Sha256(ctx context.Context) (string, error) {
	if h := a.hash.Load(); h != nil {
		return *h, nil
	}
	rc, err := a.Open()
	if err != nil {
		return "", err
	}
	defer rc.Close()
	hash, err := computeSha256(rc)
	if err != nil {
		return "", err
	}
	a.hash.Store(&hash)
	return hash, nil
}

func (a *runtimeBackedArtifact) WriteTo(ctx context.Context, w io.Writer) error {
	rc, err := a.Open()
	if err != nil {
		return err
	}
	defer rc.Close()
	_, err = io.Copy(w, rc)
	return err
}

func (a *runtimeBackedArtifact) SaveToFile(ctx context.Context, path string) error {
	rc, err := a.Open()
	if err != nil {
		return err
	}
	defer rc.Close()
	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = io.Copy(f, rc)
	return err
}

func (a *runtimeBackedArtifact) Bytes(ctx context.Context) ([]byte, error) {
	rc, err := a.Open()
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	return io.ReadAll(rc)
}

func (a *runtimeBackedArtifact) Cleanup() error { return nil }

func chapterMetaFromChapter(ch Chapter) (chapterMeta, error) {
	meta := chapterMeta{
		Version:   envelopeVersion,
		Ordinal:   ch.Ordinal,
		TaskType:  ch.TaskType,
		CreatedAt: ch.CreatedAt,
		InputHash: ch.InputHash,
	}
	rawMetadata, err := chapterMetadataJSON(ch.Metadata)
	if err != nil {
		return chapterMeta{}, fmt.Errorf("encode chapter metadata: %w", err)
	}
	if len(rawMetadata) > 0 {
		if err := json.Unmarshal(rawMetadata, &meta); err != nil {
			return chapterMeta{}, fmt.Errorf("decode chapter metadata: %w", err)
		}
	}
	if meta.Version == 0 {
		meta.Version = envelopeVersion
	}
	if meta.Ordinal == 0 {
		meta.Ordinal = ch.Ordinal
	}
	if meta.TaskType == "" {
		meta.TaskType = ch.TaskType
	}
	if meta.CreatedAt.IsZero() {
		meta.CreatedAt = ch.CreatedAt
	}
	if meta.InputHash == "" {
		meta.InputHash = ch.InputHash
	}
	return meta, nil
}

func chapterToTaskData(runtime WorkflowRuntime, jobKey JobKey, ch Chapter) (TaskData, error) {
	artifacts := make([]Artifact, 0, len(ch.Artifacts))
	for _, stored := range ch.Artifacts {
		artifacts = append(artifacts, newRuntimeBackedArtifact(runtime, ArtifactRef{
			JobKey:  jobKey,
			Ordinal: ch.Ordinal,
			Name:    stored.Name,
			Digest:  stored.Digest,
		}, stored.Size))
	}

	payloadKind, data, err := chapterPayload(ch)
	if err != nil {
		return nil, err
	}
	payload := append(json.RawMessage(nil), data...)
	td := &EnvelopedTaskData{
		SimpleTaskData: SimpleTaskData{
			Data:      Data(payload),
			Artifacts: artifacts,
		},
		Kind: payloadKind,
	}

	switch payloadKind {
	case payloadKindApp:
		return td, nil
	case payloadKindTimeout:
		var p TimeoutPayload
		if err := json.Unmarshal(data, &p); err != nil {
			return td, err
		}
		return td, &TimeoutError{Payload: p}
	case payloadKindAppError:
		var p AppErrorPayload
		if err := json.Unmarshal(data, &p); err != nil {
			return td, err
		}
		if jobFailedErr, ok := decodeJobFailedAppError(p); ok {
			return td, jobFailedErr
		}
		return td, &AppError{Payload: p}
	case payloadKindSystemError:
		var p SystemErrorPayload
		if err := json.Unmarshal(data, &p); err != nil {
			return td, err
		}
		return td, &SystemError{Payload: p}
	default:
		return td, fmt.Errorf("unsupported payload kind %q", payloadKind)
	}
}

func persistTaskDataChapter(ctx context.Context, runtime WorkflowRuntime, lease ExecutionLease, ref ChapterRef, taskType string, chapterType string, payloadKind string, inputHash string, createdAt time.Time, meta chapterMeta, payload json.RawMessage, artifacts []Artifact) (TaskData, error) {
	uploads, storedArtifacts, err := artifactUploadsForChapterWrite(ctx, artifacts)
	if err != nil {
		return nil, err
	}
	if lease == nil {
		return nil, fmt.Errorf("lease is required")
	}

	meta.Version = envelopeVersion
	meta.Ordinal = ref.Ordinal
	meta.TaskType = taskType
	if meta.CreatedAt.IsZero() {
		meta.CreatedAt = createdAt
	}
	if meta.InputHash == "" {
		meta.InputHash = inputHash
	}
	metaJSON, err := json.Marshal(meta)
	if err != nil {
		return nil, err
	}
	metadata, err := chapterMetadataFromJSON(metaJSON)
	if err != nil {
		return nil, err
	}
	body, err := chapterBodyFromWire(chapterType, payloadKind, payload)
	if err != nil {
		return nil, err
	}

	chapter := Chapter{
		Ordinal:   ref.Ordinal,
		TaskType:  taskType,
		Body:      body,
		InputHash: inputHash,
		CreatedAt: createdAt,
		Metadata:  metadata,
		Artifacts: storedArtifacts,
	}
	if err := runtime.PutChapter(ctx, PutChapterRequest{
		LeaseID:         lease.LeaseID(),
		LeaseToken:      executionLeaseToken(lease),
		Ref:             ref,
		Chapter:         chapter,
		ArtifactUploads: uploads,
	}); err != nil {
		return nil, err
	}

	for _, art := range artifacts {
		if art == nil || art.Name() == "" {
			continue
		}
		AssignArtifactKey(art, ArtifactKey{
			JobId:       ref.JobKey.JobId,
			TaskOrdinal: ref.Ordinal,
			Name:        art.Name(),
			SizeBytes:   art.Size(),
		})
	}

	if payloadKind != payloadKindApp {
		return nil, nil
	}
	return chapterToTaskData(runtime, ref.JobKey, chapter)
}

func executionLeaseToken(lease ExecutionLease) string {
	if tokenLease, ok := lease.(interface{ LeaseToken() string }); ok {
		return tokenLease.LeaseToken()
	}
	return ""
}

func artifactUploadsForChapterWrite(ctx context.Context, artifacts []Artifact) ([]ArtifactUpload, []StoredArtifact, error) {
	if len(artifacts) == 0 {
		return nil, nil, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}

	uploads := make([]ArtifactUpload, 0, len(artifacts))
	stored := make([]StoredArtifact, 0, len(artifacts))
	for idx, art := range artifacts {
		if art == nil {
			return nil, nil, fmt.Errorf("artifact %d is nil", idx)
		}
		if art.Name() == "" {
			return nil, nil, fmt.Errorf("artifact %d is missing name", idx)
		}
		digest, err := art.Sha256(ctx)
		if err != nil {
			return nil, nil, fmt.Errorf("artifact %d sha256: %w", idx, err)
		}
		if digest == "" {
			return nil, nil, fmt.Errorf("artifact %d sha256 is empty", idx)
		}
		uploads = append(uploads, ArtifactUpload{
			Name: art.Name(),
			Size: art.Size(),
			Open: art.Open,
		})
		stored = append(stored, StoredArtifact{
			Name:   art.Name(),
			Digest: digest,
			Size:   art.Size(),
		})
	}
	return uploads, stored, nil
}

func validateOutputArtifacts(ctx context.Context, artifacts []Artifact) ([]string, error) {
	if len(artifacts) == 0 {
		return nil, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	digests := make([]string, 0, len(artifacts))
	for idx, art := range artifacts {
		if art == nil {
			return nil, fmt.Errorf("artifact %d is nil", idx)
		}
		if art.Name() == "" {
			return nil, fmt.Errorf("artifact %d is missing name", idx)
		}
		digest, err := art.Sha256(ctx)
		if err != nil {
			return nil, fmt.Errorf("artifact %d sha256: %w", idx, err)
		}
		if digest == "" {
			return nil, fmt.Errorf("artifact %d sha256 is empty", idx)
		}
		digests = append(digests, digest)
	}
	return digests, nil
}

func replaceTaskDataArtifacts(output TaskData, dataBytes Data, artifacts []Artifact) (TaskData, error) {
	if output == nil {
		return nil, nil
	}
	if dataBytes == nil {
		raw, err := output.GetData()
		if err != nil {
			return nil, err
		}
		dataBytes = raw
	}

	switch typed := output.(type) {
	case *EnvelopedTaskData:
		return &EnvelopedTaskData{
			SimpleTaskData: SimpleTaskData{
				Data:      dataBytes,
				Artifacts: artifacts,
			},
			Kind: typed.Kind,
		}, nil
	case *SimpleTaskData:
		return &SimpleTaskData{
			Data:      dataBytes,
			Artifacts: artifacts,
		}, nil
	default:
		return &SimpleTaskData{
			Data:      dataBytes,
			Artifacts: artifacts,
		}, nil
	}
}

func cleanupArtifacts(artifacts []Artifact, logger *slog.Logger) {
	for _, art := range artifacts {
		if art == nil {
			continue
		}
		if err := art.Cleanup(); err != nil && logger != nil {
			logger.Warn("artifact cleanup failed", "name", art.Name(), "error", err)
		}
	}
}

func workerCapability(jobType string, taskType string) string {
	if taskType == "" {
		return jobType
	}
	return jobType + ":" + taskType
}

func taskTypeFromCapability(capability string) string {
	idx := strings.IndexByte(capability, ':')
	if idx < 0 {
		return capability
	}
	return capability[idx+1:]
}
