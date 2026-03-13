package directimpl

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/colony-2/swf-go/pkg/swf"
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

type chapterMeta struct {
	Version       int                   `json:"version"`
	Ordinal       int64                 `json:"ordinal"`
	TaskType      string                `json:"task_type"`
	WorkerID      string                `json:"worker_id"`
	CreatedAt     time.Time             `json:"created_at"`
	StartedAt     *time.Time            `json:"started_at,omitempty"`
	FinishedAt    *time.Time            `json:"finished_at,omitempty"`
	InputHash     string                `json:"input_hash"`
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

type chapterEnvelope struct {
	ChapterType string          `json:"chapter_type"`
	Meta        chapterMeta     `json:"meta"`
	PayloadKind string          `json:"payload_kind"`
	Payload     json.RawMessage `json:"payload"`
}

// buildChapterEnvelope wraps a raw payload (already JSON) into the envelope.
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

	env := chapterEnvelope{
		ChapterType: chapterType,
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

	artifacts, err := taskData.GetArtifacts()
	if err != nil {
		return "", err
	}

	artifactParts := make([]string, 0, len(artifacts))
	artifactDetails := make([]map[string]string, 0, len(artifacts))
	for _, art := range artifacts {
		hash, err := artifactHash(ctx, art)
		if err != nil {
			return "", err
		}
		artifactParts = append(artifactParts, fmt.Sprintf("%s|%s", art.Name(), hash))
		artifactDetails = append(artifactDetails, map[string]string{
			"name": art.Name(),
			"hash": hash,
		})
	}
	sort.Strings(artifactParts)

	h := sha256.New()
	_, _ = h.Write(data)
	for _, part := range artifactParts {
		_, _ = h.Write([]byte(part))
	}
	computedHash := fmt.Sprintf("%x", h.Sum(nil))

	// Debug logging: print the actual data being hashed
	slog.Default().Debug("computeInputHash: data being hashed",
		"hash", computedHash,
		"data", string(data),
		"dataLength", len(data),
		"artifacts", artifactDetails,
		"artifactCount", len(artifacts))

	return computedHash, nil
}

func errorPayloadFromError(err error, inputRef *swf.InputReference) (json.RawMessage, string, error) {
	var timeoutErr swf.TimeoutError
	if errors.As(err, &timeoutErr) {
		payload := timeoutErr.Payload
		payload.InputRef = inputRef
		raw, tdErr := json.Marshal(payload)
		return json.RawMessage(raw), payloadKindTimeout, tdErr
	}

	var sysErr swf.SystemError
	if errors.As(err, &sysErr) {
		payload := sysErr.Payload
		payload.InputRef = inputRef
		raw, tdErr := json.Marshal(payload)
		return json.RawMessage(raw), payloadKindSystemError, tdErr
	}

	var appErr swf.AppError
	if errors.As(err, &appErr) {
		payload := appErr.Payload
		payload.InputRef = inputRef
		raw, tdErr := json.Marshal(payload)
		return json.RawMessage(raw), payloadKindAppError, tdErr
	}

	// Treat every other error (including panics converted to error) as an app error.
	appErr = swf.AppError{Payload: swf.AppErrorPayload{Message: err.Error(), Level: "error", InputRef: inputRef}}
	raw, tdErr := json.Marshal(appErr.Payload)
	return json.RawMessage(raw), payloadKindAppError, tdErr
}

func artifactHash(ctx context.Context, art swf.Artifact) (string, error) {
	return art.Sha256(ctx)
}

// envelopeToTaskData returns the cached task output or the rehydrated error encoded in the envelope.
func envelopeToTaskData(env chapterEnvelope, artifacts []swf.Artifact) (swf.TaskData, error) {
	copiedArtifacts := make([]swf.Artifact, 0, len(artifacts))
	for _, a := range artifacts {
		copiedArtifacts = append(copiedArtifacts, a)
	}

	payload := make([]byte, len(env.Payload))
	copy(payload, env.Payload)

	td := &swf.EnvelopedTaskData{
		SimpleTaskData: swf.SimpleTaskData{
			Data:      swf.Data(payload),
			Artifacts: copiedArtifacts,
		},
		Kind: env.PayloadKind,
	}

	switch env.PayloadKind {
	case payloadKindApp:
		return td, nil
	case payloadKindTimeout:
		var p swf.TimeoutPayload
		if err := json.Unmarshal(env.Payload, &p); err != nil {
			return td, err
		}
		return td, swf.TimeoutError{Payload: p}
	case payloadKindAppError:
		// Rehydrate a cached application-level error.
		var p swf.AppErrorPayload
		if err := json.Unmarshal(env.Payload, &p); err != nil {
			return td, err
		}
		return td, swf.AppError{Payload: p}
	case payloadKindSystemError:
		// Rehydrate a cached system-level error.
		var p swf.SystemErrorPayload
		if err := json.Unmarshal(env.Payload, &p); err != nil {
			return td, err
		}
		return td, swf.SystemError{Payload: p}
	default:
		return td, fmt.Errorf("unsupported payload kind %q", env.PayloadKind)
	}
}
