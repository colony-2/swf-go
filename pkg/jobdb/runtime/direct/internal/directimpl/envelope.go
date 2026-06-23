package directimpl

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"

	"github.com/colony-2/jobdb/pkg/internal/runtimecodec"
	"github.com/colony-2/jobdb/pkg/jobdb"
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

type chapterMeta = runtimecodec.ChapterMeta
type chapterEnvelope = runtimecodec.ChapterEnvelope

func buildChapterEnvelope(meta chapterMeta, chapterType string, payloadKind string, payload json.RawMessage) ([]byte, error) {
	return runtimecodec.EncodeChapter(meta, chapterType, payloadKind, payload)
}

func decodeChapterEnvelope(body []byte) (chapterEnvelope, error) {
	return runtimecodec.DecodeChapter(body)
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

func errorPayloadFromError(err error, inputRef *jobdb.InputReference) (json.RawMessage, string, error) {
	var timeoutErr jobdb.TimeoutError
	if errors.As(err, &timeoutErr) {
		payload := timeoutErr.Payload
		payload.InputRef = inputRef
		raw, tdErr := json.Marshal(payload)
		return json.RawMessage(raw), payloadKindTimeout, tdErr
	}

	var sysErr jobdb.SystemError
	if errors.As(err, &sysErr) {
		payload := sysErr.Payload
		payload.InputRef = inputRef
		raw, tdErr := json.Marshal(payload)
		return json.RawMessage(raw), payloadKindSystemError, tdErr
	}

	var appErr jobdb.AppError
	if errors.As(err, &appErr) {
		payload := appErr.Payload
		payload.InputRef = inputRef
		raw, tdErr := json.Marshal(payload)
		return json.RawMessage(raw), payloadKindAppError, tdErr
	}

	// Treat every other error (including panics converted to error) as an app error.
	appErr = jobdb.AppError{Payload: jobdb.AppErrorPayload{Message: err.Error(), Level: "error", InputRef: inputRef}}
	raw, tdErr := json.Marshal(appErr.Payload)
	return json.RawMessage(raw), payloadKindAppError, tdErr
}

func artifactHash(ctx context.Context, art jobdb.Artifact) (string, error) {
	return art.Sha256(ctx)
}

// envelopeToTaskData returns the cached task output or the rehydrated error encoded in the envelope.
func envelopeToTaskData(env chapterEnvelope, artifacts []jobdb.Artifact) (jobdb.TaskData, error) {
	copiedArtifacts := make([]jobdb.Artifact, 0, len(artifacts))
	for _, a := range artifacts {
		copiedArtifacts = append(copiedArtifacts, a)
	}

	payload := make([]byte, len(env.Payload))
	copy(payload, env.Payload)

	td := &jobdb.EnvelopedTaskData{
		SimpleTaskData: jobdb.SimpleTaskData{
			Data:      jobdb.Data(payload),
			Artifacts: copiedArtifacts,
		},
		Kind: env.PayloadKind,
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
		// Rehydrate a cached application-level error.
		var p jobdb.AppErrorPayload
		if err := json.Unmarshal(env.Payload, &p); err != nil {
			return td, err
		}
		return td, &jobdb.AppError{Payload: p}
	case payloadKindSystemError:
		// Rehydrate a cached system-level error.
		var p jobdb.SystemErrorPayload
		if err := json.Unmarshal(env.Payload, &p); err != nil {
			return td, err
		}
		return td, &jobdb.SystemError{Payload: p}
	default:
		return td, fmt.Errorf("unsupported payload kind %q", env.PayloadKind)
	}
}
