package impl

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/colony-2/swf-go/pkg/swf"
)

const (
	envelopeVersion        = 1
	payloadKindApp         = "App"
	payloadKindAppError    = "AppError"
	payloadKindSystemError = "SystemError"
	payloadKindMissing     = "SystemError" // fallback kind for unexpected nils
)

type chapterMeta struct {
	Version   int       `json:"version"`
	Ordinal   int64     `json:"ordinal"`
	TaskType  string    `json:"task_type"`
	WorkerID  string    `json:"worker_id"`
	CreatedAt time.Time `json:"created_at"`
	InputHash string    `json:"input_hash"`
}

type chapterEnvelope struct {
	Meta        chapterMeta     `json:"meta"`
	PayloadKind string          `json:"payload_kind"`
	Payload     json.RawMessage `json:"payload"`
}

// buildChapterEnvelope wraps a raw payload (already JSON) into the envelope.
func buildChapterEnvelope(meta chapterMeta, payloadKind string, payload json.RawMessage) ([]byte, error) {
	if payloadKind == "" {
		return nil, fmt.Errorf("payload kind is required")
	}
	if !json.Valid(payload) {
		return nil, fmt.Errorf("payload must be valid JSON")
	}

	env := chapterEnvelope{
		Meta:        meta,
		PayloadKind: payloadKind,
		Payload:     payload,
	}

	return json.Marshal(env)
}

func decodeChapterEnvelope(body []byte) (chapterEnvelope, error) {
	var env chapterEnvelope
	if err := json.Unmarshal(body, &env); err != nil {
		return chapterEnvelope{}, err
	}
	return env, nil
}

func computeInputHash(ctx context.Context, taskData swf.TaskData) (string, error) {
	if taskData == nil {
		return "", fmt.Errorf("task data is required for hashing")
	}

	data, err := taskData.GetData()
	if err != nil {
		return "", err
	}
	dataBytes, err := data.ToBytes()
	if err != nil {
		return "", err
	}

	artifacts, err := taskData.GetArtifacts()
	if err != nil {
		return "", err
	}

	artifactParts := make([]string, 0, len(artifacts))
	for _, art := range artifacts {
		hash, err := artifactHash(ctx, art)
		if err != nil {
			return "", err
		}
		uri := art.ID()
		if uri == "" {
			uri = art.Name()
		}
		artifactParts = append(artifactParts, fmt.Sprintf("%s|%s|%s", uri, hash, art.Name()))
	}
	sort.Strings(artifactParts)

	h := sha256.New()
	_, _ = h.Write(dataBytes)
	for _, part := range artifactParts {
		_, _ = h.Write([]byte(part))
	}
	return fmt.Sprintf("%x", h.Sum(nil)), nil
}

func errorPayloadFromError(err error) (json.RawMessage, string, error) {
	var appErr swf.AppError
	if errors.As(err, &appErr) {
		raw, tdErr := json.Marshal(appErr.Payload)
		return json.RawMessage(raw), payloadKindAppError, tdErr
	}
	var sysErr swf.SystemError
	if errors.As(err, &sysErr) {
		raw, tdErr := json.Marshal(sysErr.Payload)
		return json.RawMessage(raw), payloadKindSystemError, tdErr
	}
	// default to system error envelope with message from err
	raw, tdErr := json.Marshal(swf.SystemErrorPayload{Message: err.Error()})
	return json.RawMessage(raw), payloadKindSystemError, tdErr
}

func artifactHash(ctx context.Context, art swf.Artifact) (string, error) {
	return art.Sha256(ctx)
}

func envelopeToTaskData(env chapterEnvelope, artifacts []swf.Artifact) (swf.TaskData, error) {
	copiedArtifacts := make([]swf.Artifact, 0, len(artifacts))
	for _, a := range artifacts {
		copiedArtifacts = append(copiedArtifacts, a)
	}

	payload := make([]byte, len(env.Payload))
	copy(payload, env.Payload)

	td := &swf.EnvelopedTaskData{
		SimpleTaskData: swf.SimpleTaskData{
			Data:      swf.NewBytesData(payload),
			Artifacts: copiedArtifacts,
		},
		Kind: env.PayloadKind,
	}

	switch env.PayloadKind {
	case payloadKindApp:
		return td, nil
	case payloadKindAppError:
		var p swf.AppErrorPayload
		if err := json.Unmarshal(env.Payload, &p); err != nil {
			return td, err
		}
		return td, swf.AppError{Payload: p}
	case payloadKindSystemError:
		var p swf.SystemErrorPayload
		if err := json.Unmarshal(env.Payload, &p); err != nil {
			return td, err
		}
		return td, swf.SystemError{Payload: p}
	default:
		return td, fmt.Errorf("unsupported payload kind %q", env.PayloadKind)
	}
}
