package directimpl

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	strata "github.com/colony-2/strata-go/pkg/client/artifact"
	"github.com/colony-2/swf-go/pkg/swf"
)

func TestTaskAppErrorEnvelopeRoundTrip(t *testing.T) {
	ctx := context.Background()
	input := swf.NewTaskDataOrPanic(map[string]interface{}{"n": 1})
	inputHash, err := computeInputHash(ctx, input)
	if err != nil {
		t.Fatalf("hash input: %v", err)
	}

	appErr := swf.AppError{Payload: swf.AppErrorPayload{Message: "user boom", Level: "error"}}
	payload, kind, err := errorPayloadFromError(appErr, nil)
	if err != nil {
		t.Fatalf("taskDataFromError: %v", err)
	}
	if kind != payloadKindAppError {
		t.Fatalf("expected payload kind %s, got %s", payloadKindAppError, kind)
	}

	taskType := "taskErr"
	chap, err := payloadToChapter(payload, nil, 1, taskType, "worker1", chapterTypeTaskAttemptOutcome, kind, inputHash, time.Now(), chapterMetadata{})
	if err != nil {
		t.Fatalf("taskDataToChapter: %v", err)
	}
	env, err := decodeChapterEnvelope(chap.Body())
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if env.PayloadKind != payloadKindAppError {
		t.Fatalf("unexpected payload kind %s", env.PayloadKind)
	}
	if env.Meta.TaskType != taskType {
		t.Fatalf("expected task type %s, got %s", taskType, env.Meta.TaskType)
	}
	var payloadBody swf.AppErrorPayload
	if err := json.Unmarshal(env.Payload, &payloadBody); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if payloadBody.Message != appErr.Payload.Message {
		t.Fatalf("payload message mismatch: %s", payloadBody.Message)
	}

	artifacts := convertStrataArtifactsToSwf(chap.Artifacts())
	td, payloadErr := envelopeToTaskData(env, artifacts)
	if td == nil {
		t.Fatalf("expected task data envelope on error payload")
	}
	if envTD, ok := td.(*swf.EnvelopedTaskData); !ok || envTD.Kind != payloadKindAppError {
		t.Fatalf("expected enveloped task data with kind %s, got %T %+v", payloadKindAppError, td, td)
	}
	var gotAppErr swf.AppError
	if !errors.As(payloadErr, &gotAppErr) {
		t.Fatalf("expected AppError, got %v", payloadErr)
	}
	if gotAppErr.Payload.Message != appErr.Payload.Message {
		t.Fatalf("app error message mismatch: %s", gotAppErr.Payload.Message)
	}
}

func TestTaskSystemErrorEnvelopeRoundTrip(t *testing.T) {
	ctx := context.Background()
	input := swf.NewTaskDataOrPanic(map[string]interface{}{"n": 2})
	inputHash, err := computeInputHash(ctx, input)
	if err != nil {
		t.Fatalf("hash input: %v", err)
	}

	sysErr := swf.SystemError{Payload: swf.SystemErrorPayload{Message: "infra fail", Component: "strata"}}
	payload, kind, err := errorPayloadFromError(sysErr, nil)
	if err != nil {
		t.Fatalf("taskDataFromError: %v", err)
	}
	if kind != payloadKindSystemError {
		t.Fatalf("expected payload kind %s, got %s", payloadKindSystemError, kind)
	}

	taskType := "taskSysErr"
	chap, err := payloadToChapter(payload, nil, 1, taskType, "worker1", chapterTypeTaskAttemptOutcome, kind, inputHash, time.Now(), chapterMetadata{})
	if err != nil {
		t.Fatalf("taskDataToChapter: %v", err)
	}
	env, err := decodeChapterEnvelope(chap.Body())
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if env.PayloadKind != payloadKindSystemError {
		t.Fatalf("unexpected payload kind %s", env.PayloadKind)
	}
	if env.Meta.TaskType != taskType {
		t.Fatalf("expected task type %s, got %s", taskType, env.Meta.TaskType)
	}
	var payloadBody swf.SystemErrorPayload
	if err := json.Unmarshal(env.Payload, &payloadBody); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if payloadBody.Message != sysErr.Payload.Message {
		t.Fatalf("payload message mismatch: %s", payloadBody.Message)
	}

	artifacts := convertStrataArtifactsToSwf(chap.Artifacts())
	td, payloadErr := envelopeToTaskData(env, artifacts)
	if td == nil {
		t.Fatalf("expected task data envelope on system error payload")
	}
	if envTD, ok := td.(*swf.EnvelopedTaskData); !ok || envTD.Kind != payloadKindSystemError {
		t.Fatalf("expected enveloped task data with kind %s, got %T %+v", payloadKindSystemError, td, td)
	}
	var gotSysErr swf.SystemError
	if !errors.As(payloadErr, &gotSysErr) {
		t.Fatalf("expected systemError, got %v", payloadErr)
	}
	if gotSysErr.Payload.Message != sysErr.Payload.Message {
		t.Fatalf("system error message mismatch: %s", gotSysErr.Payload.Message)
	}
}

func TestJobAppErrorEnvelopeRoundTrip(t *testing.T) {
	ctx := context.Background()
	input := swf.NewTaskDataOrPanic(map[string]interface{}{"n": 3})
	inputHash, err := computeInputHash(ctx, input)
	if err != nil {
		t.Fatalf("hash input: %v", err)
	}

	appErr := swf.AppError{Payload: swf.AppErrorPayload{Message: "job failed"}}
	payload, kind, err := errorPayloadFromError(appErr, nil)
	if err != nil {
		t.Fatalf("taskDataFromError: %v", err)
	}
	if kind != payloadKindAppError {
		t.Fatalf("expected payload kind %s, got %s", payloadKindAppError, kind)
	}

	taskType := "jobWorker"
	chap, err := payloadToChapter(payload, nil, 1, taskType, "worker-job", chapterTypeJobAttemptOutcome, kind, inputHash, time.Now(), chapterMetadata{})
	if err != nil {
		t.Fatalf("taskDataToChapter: %v", err)
	}
	env, err := decodeChapterEnvelope(chap.Body())
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if env.PayloadKind != payloadKindAppError {
		t.Fatalf("unexpected payload kind %s", env.PayloadKind)
	}
	if env.Meta.TaskType != taskType {
		t.Fatalf("expected task type %s, got %s", taskType, env.Meta.TaskType)
	}

	artifacts := convertStrataArtifactsToSwf(chap.Artifacts())
	td, payloadErr := envelopeToTaskData(env, artifacts)
	if td == nil {
		t.Fatalf("expected task data envelope on job app error payload")
	}
	if envTD, ok := td.(*swf.EnvelopedTaskData); !ok || envTD.Kind != payloadKindAppError {
		t.Fatalf("expected enveloped task data with kind %s, got %T %+v", payloadKindAppError, td, td)
	}
	var gotAppErr swf.AppError
	if !errors.As(payloadErr, &gotAppErr) {
		t.Fatalf("expected AppError, got %v", payloadErr)
	}
}

func TestJobSystemErrorEnvelopeRoundTrip(t *testing.T) {
	ctx := context.Background()
	input := swf.NewTaskDataOrPanic(map[string]interface{}{"n": 4})
	inputHash, err := computeInputHash(ctx, input)
	if err != nil {
		t.Fatalf("hash input: %v", err)
	}

	sysErr := swf.SystemError{Payload: swf.SystemErrorPayload{Message: "job infra fail", Component: "pgwf"}}
	payload, kind, err := errorPayloadFromError(sysErr, nil)
	if err != nil {
		t.Fatalf("taskDataFromError: %v", err)
	}
	if kind != payloadKindSystemError {
		t.Fatalf("expected payload kind %s, got %s", payloadKindSystemError, kind)
	}

	taskType := "jobWorker"
	chap, err := payloadToChapter(payload, nil, 1, taskType, "worker-job", chapterTypeJobAttemptOutcome, kind, inputHash, time.Now(), chapterMetadata{})
	if err != nil {
		t.Fatalf("taskDataToChapter: %v", err)
	}
	env, err := decodeChapterEnvelope(chap.Body())
	if err != nil {
		t.Fatalf("decode: %v", err)
	}
	if env.PayloadKind != payloadKindSystemError {
		t.Fatalf("unexpected payload kind %s", env.PayloadKind)
	}
	if env.Meta.TaskType != taskType {
		t.Fatalf("expected task type %s, got %s", taskType, env.Meta.TaskType)
	}

	artifacts := convertStrataArtifactsToSwf(chap.Artifacts())
	td, payloadErr := envelopeToTaskData(env, artifacts)
	if td == nil {
		t.Fatalf("expected task data envelope on job system error payload")
	}
	if envTD, ok := td.(*swf.EnvelopedTaskData); !ok || envTD.Kind != payloadKindSystemError {
		t.Fatalf("expected enveloped task data with kind %s, got %T %+v", payloadKindSystemError, td, td)
	}
	var gotSysErr swf.SystemError
	if !errors.As(payloadErr, &gotSysErr) {
		t.Fatalf("expected systemError, got %v", payloadErr)
	}
}

// convertStrataArtifactsToSwf converts strata artifacts to swf artifacts
func convertStrataArtifactsToSwf(strataArts []strata.Artifact) []swf.Artifact {
	artifacts := make([]swf.Artifact, 0, len(strataArts))
	for _, a := range strataArts {
		artifacts = append(artifacts, fromStrataArtifact(a))
	}
	return artifacts
}
