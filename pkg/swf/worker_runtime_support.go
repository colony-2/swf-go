package swf

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

type waitingTaskRuntime interface {
	FindTasksWaitingForCapability(ctx context.Context, jobType string, taskType string, tenantIds []string) ([]TaskHandle, error)
	GetWaitingTask(ctx context.Context, key JobKey) (TaskHandle, error)
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

func storedChapterMeta(ch StoredChapter) (chapterMeta, error) {
	meta := chapterMeta{
		Version:   envelopeVersion,
		Ordinal:   ch.Ordinal,
		TaskType:  ch.TaskType,
		CreatedAt: ch.CreatedAt,
		InputHash: ch.InputHash,
	}
	if len(ch.Metadata) > 0 {
		if err := json.Unmarshal(ch.Metadata, &meta); err != nil {
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

func storedChapterToTaskData(runtime WorkflowRuntime, jobKey JobKey, ch StoredChapter) (TaskData, error) {
	artifacts := make([]Artifact, 0, len(ch.Artifacts))
	for _, stored := range ch.Artifacts {
		artifacts = append(artifacts, newRuntimeBackedArtifact(runtime, ArtifactRef{
			JobKey:  jobKey,
			Ordinal: ch.Ordinal,
			Name:    stored.Name,
			Digest:  stored.Digest,
		}, stored.Size))
	}

	payload := append(json.RawMessage(nil), ch.Data...)
	td := &EnvelopedTaskData{
		SimpleTaskData: SimpleTaskData{
			Data:      Data(payload),
			Artifacts: artifacts,
		},
		Kind: ch.PayloadKind,
	}

	switch ch.PayloadKind {
	case payloadKindApp:
		return td, nil
	case payloadKindTimeout:
		var p TimeoutPayload
		if err := json.Unmarshal(ch.Data, &p); err != nil {
			return td, err
		}
		return td, TimeoutError{Payload: p}
	case payloadKindAppError:
		var p AppErrorPayload
		if err := json.Unmarshal(ch.Data, &p); err != nil {
			return td, err
		}
		if jobFailedErr, ok := decodeJobFailedAppError(p); ok {
			return td, jobFailedErr
		}
		return td, AppError{Payload: p}
	case payloadKindSystemError:
		var p SystemErrorPayload
		if err := json.Unmarshal(ch.Data, &p); err != nil {
			return td, err
		}
		return td, SystemError{Payload: p}
	default:
		return td, fmt.Errorf("unsupported payload kind %q", ch.PayloadKind)
	}
}

func persistStoredChapter(ctx context.Context, runtime WorkflowRuntime, ref ChapterRef, chapter StoredChapter) error {
	artifacts := chapter.Artifacts
	if len(artifacts) == 0 {
		return runtime.PutChapter(ctx, PutChapterRequest{Ref: ref, Chapter: chapter})
	}
	return runtime.PutChapter(ctx, PutChapterRequest{Ref: ref, Chapter: chapter})
}

func persistTaskDataChapter(ctx context.Context, runtime WorkflowRuntime, ref ChapterRef, taskType string, chapterType string, payloadKind string, inputHash string, createdAt time.Time, meta chapterMeta, payload json.RawMessage, artifacts []Artifact) (TaskData, error) {
	storedArtifacts := make([]StoredArtifact, 0, len(artifacts))
	if len(artifacts) > 0 {
		uploads := make([]ArtifactUpload, 0, len(artifacts))
		for _, art := range artifacts {
			if art == nil {
				continue
			}
			art := art
			uploads = append(uploads, ArtifactUpload{
				Name: art.Name(),
				Size: art.Size(),
				Open: art.Open,
			})
		}
		var err error
		storedArtifacts, err = runtime.PutArtifacts(ctx, PutArtifactsRequest{
			JobKey:  ref.JobKey,
			Ordinal: ref.Ordinal,
			Items:   uploads,
		})
		if err != nil {
			return nil, err
		}
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

	chapter := StoredChapter{
		Ordinal:     ref.Ordinal,
		TaskType:    taskType,
		ChapterType: chapterType,
		PayloadKind: payloadKind,
		InputHash:   inputHash,
		CreatedAt:   createdAt,
		Metadata:    metaJSON,
		Data:        append(json.RawMessage(nil), payload...),
		Artifacts:   storedArtifacts,
	}
	if err := runtime.PutChapter(ctx, PutChapterRequest{
		Ref:     ref,
		Chapter: chapter,
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
	return storedChapterToTaskData(runtime, ref.JobKey, chapter)
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
