package jobschema

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"sync"
	"time"

	"github.com/colony-2/jobdb/pkg/internal/runtimecodec"
	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/santhosh-tekuri/jsonschema/v6"
)

var defaultValidator = &validatorCache{
	schemas: make(map[jobdb.JobSchemaKey]*jsonschema.Schema),
}

type validatorCache struct {
	mu      sync.RWMutex
	schemas map[jobdb.JobSchemaKey]*jsonschema.Schema
}

func ValidateChapter(ctx context.Context, registry jobdb.JobSchemaRegistry, key jobdb.JobSchemaKey, chapter jobdb.Chapter) error {
	if key.SchemaHash == "" {
		return nil
	}
	if err := key.Validate(); err != nil {
		return err
	}
	schema, err := defaultValidator.schema(ctx, registry, key)
	if err != nil {
		return err
	}
	document, err := ChapterDocument(chapter)
	if err != nil {
		return err
	}
	raw, err := json.Marshal(document)
	if err != nil {
		return fmt.Errorf("marshal chapter document for schema validation: %w", err)
	}
	instance, err := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
	if err != nil {
		return fmt.Errorf("decode chapter document for schema validation: %w", err)
	}
	if err := schema.Validate(instance); err != nil {
		return fmt.Errorf("%w: schema %s rejected chapter %d: %v", jobdb.ErrJobSchemaValidation, key.SchemaHash, chapter.Ordinal, err)
	}
	return nil
}

func Prime(ctx context.Context, registry jobdb.JobSchemaRegistry, key jobdb.JobSchemaKey) error {
	if key.SchemaHash == "" {
		return nil
	}
	if err := key.Validate(); err != nil {
		return err
	}
	_, err := defaultValidator.schema(ctx, registry, key)
	return err
}

func ValidateSchemaDocument(schemaHash string, raw json.RawMessage) error {
	if _, err := compileSchema(schemaHash, raw); err != nil {
		return fmt.Errorf("%w: invalid schema %s: %v", jobdb.ErrJobSchemaValidation, schemaHash, err)
	}
	return nil
}

func (c *validatorCache) schema(ctx context.Context, registry jobdb.JobSchemaRegistry, key jobdb.JobSchemaKey) (*jsonschema.Schema, error) {
	c.mu.RLock()
	schema := c.schemas[key]
	c.mu.RUnlock()
	if schema != nil {
		return schema, nil
	}
	if registry == nil {
		return nil, fmt.Errorf("job schema registry is required")
	}
	info, err := registry.GetJobSchema(ctx, key)
	if err != nil {
		return nil, err
	}
	compiled, err := compileSchema(key.SchemaHash, info.Schema)
	if err != nil {
		return nil, err
	}
	c.mu.Lock()
	if existing := c.schemas[key]; existing != nil {
		c.mu.Unlock()
		return existing, nil
	}
	c.schemas[key] = compiled
	c.mu.Unlock()
	return compiled, nil
}

func compileSchema(schemaHash string, raw json.RawMessage) (*jsonschema.Schema, error) {
	document, err := jsonschema.UnmarshalJSON(bytes.NewReader(raw))
	if err != nil {
		return nil, fmt.Errorf("decode schema %s: %w", schemaHash, err)
	}
	location := "jobdb-schema:///" + url.PathEscape(schemaHash)
	compiler := jsonschema.NewCompiler()
	compiler.DefaultDraft(jsonschema.Draft2020)
	if err := compiler.AddResource(location, document); err != nil {
		return nil, fmt.Errorf("add schema %s: %w", schemaHash, err)
	}
	schema, err := compiler.Compile(location)
	if err != nil {
		return nil, fmt.Errorf("compile schema %s: %w", schemaHash, err)
	}
	return schema, nil
}

func ChapterDocument(chapter jobdb.Chapter) (map[string]any, error) {
	body, err := chapterBodyDocument(chapter.Body)
	if err != nil {
		return nil, err
	}
	meta, err := chapterMetaFromChapter(chapter)
	if err != nil {
		return nil, err
	}
	out := map[string]any{
		"artifacts": storedArtifactsDocument(chapter.Artifacts),
		"body":      body,
		"createdAt": meta.CreatedAt,
		"ordinal":   meta.Ordinal,
	}
	setString(out, "inputHash", meta.InputHash)
	setString(out, "taskType", meta.TaskType)
	setString(out, "workerId", meta.WorkerID)
	if meta.StartedAt != nil {
		out["startedAt"] = *meta.StartedAt
	}
	if meta.FinishedAt != nil {
		out["finishedAt"] = *meta.FinishedAt
	}
	if meta.Attempt != 0 {
		out["attempt"] = meta.Attempt
	}
	if meta.MaxAttempts != 0 {
		out["maxAttempts"] = meta.MaxAttempts
	}
	if meta.NextAttemptAt != nil {
		out["nextAttemptAt"] = *meta.NextAttemptAt
	}
	if meta.BackoffMillis != 0 {
		out["backoffMillis"] = meta.BackoffMillis
	}
	if meta.Retryable != nil {
		out["retryable"] = *meta.Retryable
	}
	if meta.InputRef != nil {
		out["inputRef"] = inputReferenceDocument(meta.InputRef)
	}
	if len(meta.Metadata) > 0 {
		value, err := jsonValue(meta.Metadata)
		if err != nil {
			return nil, fmt.Errorf("chapter metadata: %w", err)
		}
		out["metadata"] = value
	}
	if len(meta.Input) > 0 {
		value, err := jsonValue(meta.Input)
		if err != nil {
			return nil, fmt.Errorf("chapter input: %w", err)
		}
		out["input"] = value
	}
	if meta.RunPolicy != nil {
		out["runPolicy"] = runPolicyDocument(*meta.RunPolicy)
	}
	if len(meta.Prerequisites) > 0 {
		out["prerequisites"] = prerequisitesDocument(meta.Prerequisites)
	}
	return out, nil
}

func chapterMetaFromChapter(chapter jobdb.Chapter) (runtimecodec.ChapterMeta, error) {
	meta := runtimecodec.ChapterMeta{
		Version:   runtimecodec.EnvelopeVersion,
		Ordinal:   chapter.Ordinal,
		TaskType:  chapter.TaskType,
		CreatedAt: chapter.CreatedAt,
		InputHash: chapter.InputHash,
	}
	rawMetadata, err := runtimecodec.ChapterMetadataToJSON(chapter.Metadata)
	if err != nil {
		return runtimecodec.ChapterMeta{}, fmt.Errorf("encode chapter metadata: %w", err)
	}
	if len(rawMetadata) > 0 {
		if err := json.Unmarshal(rawMetadata, &meta); err != nil {
			return runtimecodec.ChapterMeta{}, fmt.Errorf("decode chapter metadata: %w", err)
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
			meta.Version = runtimecodec.EnvelopeVersion
		}
	}
	if meta.CreatedAt.IsZero() {
		meta.CreatedAt = chapter.CreatedAt
	}
	return meta, nil
}

func chapterBodyDocument(body jobdb.ChapterBody) (map[string]any, error) {
	switch body := body.(type) {
	case jobdb.JobStartChapter:
		input, err := jsonValue(body.Input.Data)
		if err != nil {
			return nil, err
		}
		return map[string]any{"kind": "jobStart", "input": input}, nil
	case *jobdb.JobStartChapter:
		if body == nil {
			return nil, fmt.Errorf("chapter body is required")
		}
		return chapterBodyDocument(*body)
	case jobdb.JobAttemptOutcomeChapter:
		outcome, err := outcomeDocument(body.Outcome)
		if err != nil {
			return nil, err
		}
		return map[string]any{"kind": "jobAttemptOutcome", "outcome": outcome}, nil
	case *jobdb.JobAttemptOutcomeChapter:
		if body == nil {
			return nil, fmt.Errorf("chapter body is required")
		}
		return chapterBodyDocument(*body)
	case jobdb.TaskAttemptOutcomeChapter:
		outcome, err := outcomeDocument(body.Outcome)
		if err != nil {
			return nil, err
		}
		return map[string]any{"kind": "taskAttemptOutcome", "outcome": outcome}, nil
	case *jobdb.TaskAttemptOutcomeChapter:
		if body == nil {
			return nil, fmt.Errorf("chapter body is required")
		}
		return chapterBodyDocument(*body)
	case jobdb.RestartExtraChapter:
		output, err := jsonValue(body.Output.Data)
		if err != nil {
			return nil, err
		}
		return map[string]any{"kind": "restartExtra", "output": output}, nil
	case *jobdb.RestartExtraChapter:
		if body == nil {
			return nil, fmt.Errorf("chapter body is required")
		}
		return chapterBodyDocument(*body)
	default:
		return nil, fmt.Errorf("unsupported chapter body type %T", body)
	}
}

func outcomeDocument(outcome jobdb.ChapterOutcome) (map[string]any, error) {
	switch outcome := outcome.(type) {
	case jobdb.ApplicationOutputOutcome:
		output, err := jsonValue(outcome.Output.Data)
		if err != nil {
			return nil, err
		}
		return map[string]any{"kind": "success", "output": output}, nil
	case *jobdb.ApplicationOutputOutcome:
		if outcome == nil {
			return nil, fmt.Errorf("task outcome is required")
		}
		return outcomeDocument(*outcome)
	case jobdb.AppErrorOutcome:
		return map[string]any{"kind": "appError", "error": appErrorDocument(outcome.Error)}, nil
	case *jobdb.AppErrorOutcome:
		if outcome == nil {
			return nil, fmt.Errorf("task outcome is required")
		}
		return outcomeDocument(*outcome)
	case jobdb.SystemErrorOutcome:
		return map[string]any{"kind": "systemError", "error": systemErrorDocument(outcome.Error)}, nil
	case *jobdb.SystemErrorOutcome:
		if outcome == nil {
			return nil, fmt.Errorf("task outcome is required")
		}
		return outcomeDocument(*outcome)
	case jobdb.TimeoutOutcome:
		return map[string]any{"kind": "timeout", "timeout": timeoutDocument(outcome.Timeout)}, nil
	case *jobdb.TimeoutOutcome:
		if outcome == nil {
			return nil, fmt.Errorf("task outcome is required")
		}
		return outcomeDocument(*outcome)
	default:
		return nil, fmt.Errorf("unsupported task outcome type %T", outcome)
	}
}

func jsonValue(raw json.RawMessage) (any, error) {
	if len(bytes.TrimSpace(raw)) == 0 {
		return nil, nil
	}
	return jsonschema.UnmarshalJSON(bytes.NewReader(raw))
}

func storedArtifactsDocument(artifacts []jobdb.StoredArtifact) []map[string]any {
	out := make([]map[string]any, 0, len(artifacts))
	for _, artifact := range artifacts {
		out = append(out, map[string]any{
			"digest": artifact.Digest,
			"name":   artifact.Name,
			"size":   artifact.Size,
		})
	}
	return out
}

func appErrorDocument(payload jobdb.AppErrorPayload) map[string]any {
	out := map[string]any{"message": payload.Message}
	setString(out, "level", payload.Level)
	if payload.Attrs != nil {
		out["attrs"] = payload.Attrs
	}
	if payload.InputRef != nil {
		out["inputRef"] = inputReferenceDocument(payload.InputRef)
	}
	if len(payload.Stacktrace) > 0 {
		out["stacktrace"] = append([]string(nil), payload.Stacktrace...)
	}
	return out
}

func systemErrorDocument(payload jobdb.SystemErrorPayload) map[string]any {
	out := map[string]any{"message": payload.Message}
	setString(out, "component", payload.Component)
	setString(out, "code", payload.Code)
	if payload.Retryable {
		out["retryable"] = payload.Retryable
	}
	if payload.InputRef != nil {
		out["inputRef"] = inputReferenceDocument(payload.InputRef)
	}
	if len(payload.Stacktrace) > 0 {
		out["stacktrace"] = append([]string(nil), payload.Stacktrace...)
	}
	return out
}

func timeoutDocument(payload jobdb.TimeoutPayload) map[string]any {
	out := map[string]any{
		"after":     time.Duration(payload.After).String(),
		"retryable": payload.Retryable,
		"scope":     payload.Scope,
	}
	setString(out, "component", payload.Component)
	setString(out, "code", payload.Code)
	setString(out, "kind", payload.Kind)
	setString(out, "message", payload.Message)
	if payload.InputRef != nil {
		out["inputRef"] = inputReferenceDocument(payload.InputRef)
	}
	return out
}

func inputReferenceDocument(ref *jobdb.InputReference) map[string]any {
	out := map[string]any{"ordinal": ref.Ordinal}
	setString(out, "hash", ref.Hash)
	return out
}

func runPolicyDocument(policy jobdb.RunPolicy) map[string]any {
	out := map[string]any{}
	if policy.InvocationTimeout != nil {
		out["invocationTimeout"] = time.Duration(*policy.InvocationTimeout).String()
	}
	if policy.TotalTimeout != nil {
		out["totalTimeout"] = time.Duration(*policy.TotalTimeout).String()
	}
	if !retryPolicyIsZero(policy.Retry) {
		retry := map[string]any{}
		if policy.Retry.InitialInterval != 0 {
			retry["initialInterval"] = time.Duration(policy.Retry.InitialInterval).String()
		}
		if policy.Retry.BackoffCoefficient != 0 {
			retry["backoffCoefficient"] = policy.Retry.BackoffCoefficient
		}
		if policy.Retry.MaximumInterval != 0 {
			retry["maximumInterval"] = time.Duration(policy.Retry.MaximumInterval).String()
		}
		if policy.Retry.MaximumAttempts != 0 {
			retry["maximumAttempts"] = policy.Retry.MaximumAttempts
		}
		if len(policy.Retry.NonRetryableErrorTypes) > 0 {
			retry["nonRetryableErrorTypes"] = append([]string(nil), policy.Retry.NonRetryableErrorTypes...)
		}
		out["retry"] = retry
	}
	return out
}

func retryPolicyIsZero(policy jobdb.RetryPolicy) bool {
	return policy.InitialInterval == 0 &&
		policy.BackoffCoefficient == 0 &&
		policy.MaximumInterval == 0 &&
		policy.MaximumAttempts == 0 &&
		len(policy.NonRetryableErrorTypes) == 0
}

func prerequisitesDocument(prereqs []jobdb.JobPrerequisite) []map[string]any {
	out := make([]map[string]any, 0, len(prereqs))
	for _, prereq := range prereqs {
		out = append(out, map[string]any{
			"condition": string(prereq.Condition),
			"jobId":     prereq.JobID,
		})
	}
	return out
}

func setString(out map[string]any, name string, value string) {
	if value != "" {
		out[name] = value
	}
}
