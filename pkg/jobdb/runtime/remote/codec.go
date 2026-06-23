package remote

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"time"

	"github.com/colony-2/jobdb/pkg/internal/runtimecodec"
	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/runtimeapi"
)

const (
	jobFailedAttrKey          = "_jobdb_job_failed"
	jobFailedKindAttrKey      = "_jobdb_job_failed_kind"
	jobFailedCodeAttrKey      = "_jobdb_job_failed_code"
	jobFailedComponentAttrKey = "_jobdb_job_failed_component"
	jobFailedRetryableAttrKey = "_jobdb_job_failed_retryable"
	jobFailedScopeAttrKey     = "_jobdb_job_failed_scope"
	jobFailedAfterAttrKey     = "_jobdb_job_failed_after"
)

type jobInfoTaskData struct {
	taskData jobdb.TaskData
	err      error
}

func (d *jobInfoTaskData) GetData() (jobdb.Data, error) {
	if d.taskData == nil {
		return nil, d.err
	}
	data, err := d.taskData.GetData()
	if err != nil {
		return data, err
	}
	return data, d.err
}

func (d *jobInfoTaskData) GetDataOrPanic() jobdb.Data {
	data, err := d.GetData()
	if err != nil {
		panic(err)
	}
	return data
}

func (d *jobInfoTaskData) GetArtifacts() ([]jobdb.Artifact, error) {
	if d.taskData == nil {
		return nil, d.err
	}
	return d.taskData.GetArtifacts()
}

func (d *jobInfoTaskData) TaskDataResult() (jobdb.TaskData, error) {
	return d.taskData, d.err
}

func toAPIJobHandle(handle jobdb.JobHandle) runtimeapi.JobHandle {
	return runtimeapi.JobHandle{
		JobKey: toAPIJobKey(handle.JobKey),
	}
}

func fromAPIJobHandle(handle runtimeapi.JobHandle) jobdb.JobHandle {
	return jobdb.JobHandle{
		JobKey: fromAPIJobKey(handle.JobKey),
	}
}

func toAPIJobKey(key jobdb.JobKey) runtimeapi.JobKey {
	return runtimeapi.JobKey{
		TenantId: key.TenantId,
		JobId:    key.JobId,
	}
}

func fromAPIJobKey(key runtimeapi.JobKey) jobdb.JobKey {
	return jobdb.JobKey{
		TenantId: key.TenantId,
		JobId:    key.JobId,
	}
}

func applicationPayloadRequired(raw json.RawMessage) (runtimeapi.ApplicationPayload, error) {
	if len(raw) == 0 {
		return runtimeapi.ApplicationPayload(nil), nil
	}
	if !json.Valid(raw) {
		return nil, fmt.Errorf("application payload must be valid JSON")
	}
	return runtimeapi.ApplicationPayload(cloneRawMessage(raw)), nil
}

func applicationPayloadOptional(raw json.RawMessage) (*runtimeapi.ApplicationPayload, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	payload, err := applicationPayloadRequired(raw)
	if err != nil {
		return nil, err
	}
	return &payload, nil
}

func applicationPayloadToRaw(value runtimeapi.ApplicationPayload) (json.RawMessage, error) {
	if len(value) == 0 {
		return nil, nil
	}
	if !json.Valid(value) {
		return nil, fmt.Errorf("application payload must be valid JSON")
	}
	return cloneRawMessage(value), nil
}

func applicationPayloadPointerToRaw(value *runtimeapi.ApplicationPayload) (json.RawMessage, error) {
	if value == nil {
		return nil, nil
	}
	return applicationPayloadToRaw(*value)
}

func toAPIDurationPointer(d time.Duration) *string {
	if d <= 0 {
		return nil
	}
	value := d.String()
	return &value
}

func fromAPIDurationPointer(value *string) (time.Duration, error) {
	if value == nil || *value == "" {
		return 0, nil
	}
	return time.ParseDuration(*value)
}

func toAPIDurationValue(value *jobdb.Duration) *string {
	if value == nil {
		return nil
	}
	v := value.String()
	return &v
}

func toAPIStdDurationValue(value *time.Duration) *string {
	if value == nil {
		return nil
	}
	v := value.String()
	return &v
}

func fromAPIDurationValue(value *string) (*jobdb.Duration, error) {
	if value == nil || *value == "" {
		return nil, nil
	}
	parsed, err := time.ParseDuration(*value)
	if err != nil {
		return nil, err
	}
	d := jobdb.Duration(parsed)
	return &d, nil
}

func fromAPIStdDurationValue(value *string) (*time.Duration, error) {
	if value == nil || *value == "" {
		return nil, nil
	}
	parsed, err := time.ParseDuration(*value)
	if err != nil {
		return nil, err
	}
	return &parsed, nil
}

func runPolicyToAPI(policy jobdb.RunPolicy) (*runtimeapi.RunPolicy, error) {
	out := runtimeapi.RunPolicy{
		InvocationTimeout: toAPIDurationValue(policy.InvocationTimeout),
		TotalTimeout:      toAPIDurationValue(policy.TotalTimeout),
	}
	if !retryPolicyIsZero(policy.Retry) {
		retry := runtimeapi.RetryPolicy{}
		if policy.Retry.InitialInterval != 0 {
			v := time.Duration(policy.Retry.InitialInterval).String()
			retry.InitialInterval = &v
		}
		if policy.Retry.BackoffCoefficient != 0 {
			v := policy.Retry.BackoffCoefficient
			retry.BackoffCoefficient = &v
		}
		if policy.Retry.MaximumInterval != 0 {
			v := time.Duration(policy.Retry.MaximumInterval).String()
			retry.MaximumInterval = &v
		}
		if policy.Retry.MaximumAttempts != 0 {
			v := policy.Retry.MaximumAttempts
			retry.MaximumAttempts = &v
		}
		if len(policy.Retry.NonRetryableErrorTypes) > 0 {
			values := append([]string(nil), policy.Retry.NonRetryableErrorTypes...)
			retry.NonRetryableErrorTypes = &values
		}
		out.Retry = &retry
	}
	if out.Retry == nil && out.InvocationTimeout == nil && out.TotalTimeout == nil {
		return nil, nil
	}
	return &out, nil
}

func runPolicyFromAPI(value *runtimeapi.RunPolicy) (jobdb.RunPolicy, error) {
	if value == nil {
		return jobdb.RunPolicy{}, nil
	}
	out := jobdb.RunPolicy{}
	if value.InvocationTimeout != nil {
		parsed, err := fromAPIDurationValue(value.InvocationTimeout)
		if err != nil {
			return jobdb.RunPolicy{}, err
		}
		out.InvocationTimeout = parsed
	}
	if value.TotalTimeout != nil {
		parsed, err := fromAPIDurationValue(value.TotalTimeout)
		if err != nil {
			return jobdb.RunPolicy{}, err
		}
		out.TotalTimeout = parsed
	}
	if value.Retry != nil {
		retry := value.Retry
		if retry.InitialInterval != nil {
			parsed, err := fromAPIDurationValue(retry.InitialInterval)
			if err != nil {
				return jobdb.RunPolicy{}, err
			}
			if parsed != nil {
				out.Retry.InitialInterval = *parsed
			}
		}
		if retry.BackoffCoefficient != nil {
			out.Retry.BackoffCoefficient = *retry.BackoffCoefficient
		}
		if retry.MaximumInterval != nil {
			parsed, err := fromAPIDurationValue(retry.MaximumInterval)
			if err != nil {
				return jobdb.RunPolicy{}, err
			}
			if parsed != nil {
				out.Retry.MaximumInterval = *parsed
			}
		}
		if retry.MaximumAttempts != nil {
			out.Retry.MaximumAttempts = *retry.MaximumAttempts
		}
		if retry.NonRetryableErrorTypes != nil {
			out.Retry.NonRetryableErrorTypes = append([]string(nil), (*retry.NonRetryableErrorTypes)...)
		}
	}
	return out, nil
}

func retryPolicyIsZero(policy jobdb.RetryPolicy) bool {
	return policy.InitialInterval == 0 &&
		policy.BackoffCoefficient == 0 &&
		policy.MaximumInterval == 0 &&
		policy.MaximumAttempts == 0 &&
		len(policy.NonRetryableErrorTypes) == 0
}

func metadataPredicatesToAPI(filter jobdb.MetadataFilter) (*[]runtimeapi.MetadataPredicate, error) {
	predicates, err := jobdb.MetadataPredicates(filter)
	if err != nil {
		return nil, err
	}
	if len(predicates) == 0 {
		return nil, nil
	}
	out := make([]runtimeapi.MetadataPredicate, 0, len(predicates))
	for _, predicate := range predicates {
		values := make([]runtimeapi.MetadataValue, 0, len(predicate.Values))
		for _, value := range predicate.Values {
			converted, err := metadataAnyValueToAPI(value)
			if err != nil {
				return nil, err
			}
			values = append(values, converted)
		}
		out = append(out, runtimeapi.MetadataPredicate{
			Path:   append([]string(nil), predicate.Path...),
			Values: values,
		})
	}
	return &out, nil
}

func metadataFilterFromAPI(predicates *[]runtimeapi.MetadataPredicate) (jobdb.MetadataFilter, error) {
	if predicates == nil || len(*predicates) == 0 {
		return nil, nil
	}
	filter := jobdb.Metadata()
	for _, predicate := range *predicates {
		if len(predicate.Path) != 1 || predicate.Path[0] == "" {
			return nil, fmt.Errorf("metadata predicate path must contain exactly one segment")
		}
		if len(predicate.Values) == 0 {
			return nil, fmt.Errorf("metadata predicate values are required")
		}
		var fieldFilter jobdb.MetadataFilter
		for idx, value := range predicate.Values {
			converted, err := metadataAPIValueToAny(value)
			if err != nil {
				return nil, err
			}
			eq, err := jobdb.Metadata().EqualFilter(jobdb.FieldName(predicate.Path[0]), converted)
			if err != nil {
				return nil, err
			}
			if idx == 0 {
				fieldFilter = eq
				continue
			}
			fieldFilter, err = fieldFilter.OrFilter(eq)
			if err != nil {
				return nil, err
			}
		}
		var err error
		filter, err = filter.AndFilter(fieldFilter)
		if err != nil {
			return nil, err
		}
	}
	return filter, nil
}

func metadataJSONToAPI(raw json.RawMessage) (*runtimeapi.Metadata, error) {
	if len(raw) == 0 || string(bytes.TrimSpace(raw)) == "null" {
		return nil, nil
	}
	metadata, err := runtimecodec.ChapterMetadataFromJSON(raw)
	if err != nil {
		return nil, err
	}
	return chapterMetadataToAPI(metadata)
}

func metadataAPIToJSON(metadata *runtimeapi.Metadata) (json.RawMessage, error) {
	if metadata == nil {
		return nil, nil
	}
	converted, err := chapterMetadataFromAPI(*metadata)
	if err != nil {
		return nil, err
	}
	return runtimecodec.ChapterMetadataToJSON(converted)
}

func metadataMapToAPI(values map[string]interface{}) (*runtimeapi.Metadata, error) {
	if len(values) == 0 {
		return nil, nil
	}
	raw, err := json.Marshal(values)
	if err != nil {
		return nil, err
	}
	return metadataJSONToAPI(raw)
}

func metadataMapFromAPI(metadata *runtimeapi.Metadata) (map[string]interface{}, error) {
	raw, err := metadataAPIToJSON(metadata)
	if err != nil {
		return nil, err
	}
	if len(raw) == 0 {
		return nil, nil
	}
	var out map[string]interface{}
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	if len(out) == 0 {
		return nil, nil
	}
	return out, nil
}

func chapterMetadataToAPI(metadata jobdb.ChapterMetadata) (*runtimeapi.Metadata, error) {
	if metadata.Fields == nil {
		return nil, nil
	}
	fields := make(map[string]runtimeapi.MetadataValue, len(metadata.Fields))
	for name, value := range metadata.Fields {
		converted, err := chapterMetadataValueToAPI(value)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", name, err)
		}
		fields[name] = converted
	}
	return &runtimeapi.Metadata{Fields: fields}, nil
}

func chapterMetadataFromAPI(metadata runtimeapi.Metadata) (jobdb.ChapterMetadata, error) {
	out := jobdb.ChapterMetadata{Fields: make(map[string]jobdb.ChapterMetadataValue, len(metadata.Fields))}
	for name, value := range metadata.Fields {
		converted, err := chapterMetadataValueFromAPI(value)
		if err != nil {
			return jobdb.ChapterMetadata{}, fmt.Errorf("%s: %w", name, err)
		}
		out.Fields[name] = converted
	}
	return out, nil
}

func chapterMetadataValueToAPI(value jobdb.ChapterMetadataValue) (runtimeapi.MetadataValue, error) {
	var out runtimeapi.MetadataValue
	switch value.Kind {
	case jobdb.ChapterMetadataNull:
		err := out.FromMetadataNull(runtimeapi.MetadataNull{})
		return out, err
	case jobdb.ChapterMetadataBool:
		err := out.FromMetadataBool(runtimeapi.MetadataBool{BoolValue: value.Bool})
		return out, err
	case jobdb.ChapterMetadataInt:
		err := out.FromMetadataInt(runtimeapi.MetadataInt{IntValue: value.Int})
		return out, err
	case jobdb.ChapterMetadataDouble:
		err := out.FromMetadataDouble(runtimeapi.MetadataDouble{DoubleValue: value.Double})
		return out, err
	case jobdb.ChapterMetadataString:
		err := out.FromMetadataString(runtimeapi.MetadataString{StringValue: value.String})
		return out, err
	case jobdb.ChapterMetadataList:
		values := make([]runtimeapi.MetadataValue, 0, len(value.List))
		for i, item := range value.List {
			converted, err := chapterMetadataValueToAPI(item)
			if err != nil {
				return out, fmt.Errorf("[%d]: %w", i, err)
			}
			values = append(values, converted)
		}
		err := out.FromMetadataList(runtimeapi.MetadataList{ListValue: values})
		return out, err
	case jobdb.ChapterMetadataMap:
		fields := make(map[string]runtimeapi.MetadataValue, len(value.Map))
		for name, item := range value.Map {
			converted, err := chapterMetadataValueToAPI(item)
			if err != nil {
				return out, fmt.Errorf("%s: %w", name, err)
			}
			fields[name] = converted
		}
		err := out.FromMetadataMap(runtimeapi.MetadataMap{MapValue: runtimeapi.Metadata{Fields: fields}})
		return out, err
	default:
		return out, fmt.Errorf("unsupported metadata kind %q", value.Kind)
	}
}

func chapterMetadataValueFromAPI(value runtimeapi.MetadataValue) (jobdb.ChapterMetadataValue, error) {
	discriminator, err := value.Discriminator()
	if err != nil {
		return jobdb.ChapterMetadataValue{}, err
	}
	switch discriminator {
	case "null":
		return jobdb.ChapterMetadataValue{Kind: jobdb.ChapterMetadataNull}, nil
	case "bool":
		converted, err := value.AsMetadataBool()
		if err != nil {
			return jobdb.ChapterMetadataValue{}, err
		}
		return jobdb.ChapterMetadataValue{Kind: jobdb.ChapterMetadataBool, Bool: converted.BoolValue}, nil
	case "int":
		converted, err := value.AsMetadataInt()
		if err != nil {
			return jobdb.ChapterMetadataValue{}, err
		}
		return jobdb.ChapterMetadataValue{Kind: jobdb.ChapterMetadataInt, Int: converted.IntValue}, nil
	case "double":
		converted, err := value.AsMetadataDouble()
		if err != nil {
			return jobdb.ChapterMetadataValue{}, err
		}
		return jobdb.ChapterMetadataValue{Kind: jobdb.ChapterMetadataDouble, Double: converted.DoubleValue}, nil
	case "string":
		converted, err := value.AsMetadataString()
		if err != nil {
			return jobdb.ChapterMetadataValue{}, err
		}
		return jobdb.ChapterMetadataValue{Kind: jobdb.ChapterMetadataString, String: converted.StringValue}, nil
	case "list":
		converted, err := value.AsMetadataList()
		if err != nil {
			return jobdb.ChapterMetadataValue{}, err
		}
		values := make([]jobdb.ChapterMetadataValue, 0, len(converted.ListValue))
		for i, item := range converted.ListValue {
			itemValue, err := chapterMetadataValueFromAPI(item)
			if err != nil {
				return jobdb.ChapterMetadataValue{}, fmt.Errorf("[%d]: %w", i, err)
			}
			values = append(values, itemValue)
		}
		return jobdb.ChapterMetadataValue{Kind: jobdb.ChapterMetadataList, List: values}, nil
	case "map":
		converted, err := value.AsMetadataMap()
		if err != nil {
			return jobdb.ChapterMetadataValue{}, err
		}
		fields := make(map[string]jobdb.ChapterMetadataValue, len(converted.MapValue.Fields))
		for name, item := range converted.MapValue.Fields {
			itemValue, err := chapterMetadataValueFromAPI(item)
			if err != nil {
				return jobdb.ChapterMetadataValue{}, fmt.Errorf("%s: %w", name, err)
			}
			fields[name] = itemValue
		}
		return jobdb.ChapterMetadataValue{Kind: jobdb.ChapterMetadataMap, Map: fields}, nil
	default:
		return jobdb.ChapterMetadataValue{}, fmt.Errorf("unsupported metadata discriminator %q", discriminator)
	}
}

func metadataAnyValueToAPI(value any) (runtimeapi.MetadataValue, error) {
	raw, err := json.Marshal(map[string]any{"value": value})
	if err != nil {
		return runtimeapi.MetadataValue{}, err
	}
	metadata, err := runtimecodec.ChapterMetadataFromJSON(raw)
	if err != nil {
		return runtimeapi.MetadataValue{}, err
	}
	converted, ok := metadata.Fields["value"]
	if !ok {
		return runtimeapi.MetadataValue{}, fmt.Errorf("metadata value is missing")
	}
	return chapterMetadataValueToAPI(converted)
}

func metadataAPIValueToAny(value runtimeapi.MetadataValue) (any, error) {
	converted, err := chapterMetadataValueFromAPI(value)
	if err != nil {
		return nil, err
	}
	return chapterMetadataValueToAny(converted), nil
}

func chapterMetadataValueToAny(value jobdb.ChapterMetadataValue) any {
	switch value.Kind {
	case jobdb.ChapterMetadataNull:
		return nil
	case jobdb.ChapterMetadataBool:
		return value.Bool
	case jobdb.ChapterMetadataInt:
		return value.Int
	case jobdb.ChapterMetadataDouble:
		return value.Double
	case jobdb.ChapterMetadataString:
		return value.String
	case jobdb.ChapterMetadataList:
		out := make([]any, 0, len(value.List))
		for _, item := range value.List {
			out = append(out, chapterMetadataValueToAny(item))
		}
		return out
	case jobdb.ChapterMetadataMap:
		out := make(map[string]any, len(value.Map))
		for name, item := range value.Map {
			out[name] = chapterMetadataValueToAny(item)
		}
		return out
	default:
		return nil
	}
}

func taskDataToAPIWrite(ctx context.Context, data jobdb.TaskData) (runtimeapi.TaskDataWrite, error) {
	if data == nil {
		return runtimeapi.TaskDataWrite{}, fmt.Errorf("task data is required")
	}
	raw, err := data.GetData()
	if err != nil {
		return runtimeapi.TaskDataWrite{}, err
	}
	value, err := applicationPayloadRequired(raw)
	if err != nil {
		return runtimeapi.TaskDataWrite{}, err
	}
	artifacts, err := artifactsToAPIWrites(ctx, data)
	if err != nil {
		return runtimeapi.TaskDataWrite{}, err
	}
	return runtimeapi.TaskDataWrite{
		Data:      value,
		Artifacts: artifacts,
	}, nil
}

func taskDataFromAPIWrite(write runtimeapi.TaskDataWrite) (jobdb.TaskData, error) {
	raw, err := applicationPayloadToRaw(write.Data)
	if err != nil {
		return nil, err
	}
	artifacts := make([]jobdb.Artifact, 0, len(write.Artifacts))
	for _, artifact := range write.Artifacts {
		artifacts = append(artifacts, jobdb.NewArtifactFromBytes(artifact.Name, append([]byte(nil), artifact.ContentBase64...)))
	}
	return &jobdb.SimpleTaskData{
		Data:      raw,
		Artifacts: artifacts,
	}, nil
}

func taskDataToAPIStored(ctx context.Context, data jobdb.TaskData, payloadErr error) (*runtimeapi.StoredTaskData, error) {
	if data == nil {
		return nil, nil
	}
	raw, err := data.GetData()
	if err != nil {
		return nil, err
	}
	outcome, err := taskOutcomeFromTaskData(data, payloadErr, raw)
	if err != nil {
		return nil, err
	}
	artifacts, err := storedArtifactsToAPI(ctx, data)
	if err != nil {
		return nil, err
	}
	return &runtimeapi.StoredTaskData{
		Artifacts: artifacts,
		Outcome:   outcome,
	}, nil
}

func taskDataFromAPIStored(runtime *Runtime, jobKey jobdb.JobKey, data runtimeapi.StoredTaskData) (jobdb.TaskData, error) {
	artifacts := make([]jobdb.Artifact, 0, len(data.Artifacts))
	for _, artifact := range data.Artifacts {
		artifacts = append(artifacts, newRemoteTaskDataArtifact(runtime, jobKey, nil, artifact))
	}
	return taskDataFromOutcome(artifacts, data.Outcome)
}

func toAPIStoredChapter(ctx context.Context, chapter jobdb.Chapter) (runtimeapi.ChapterRecord, error) {
	body, err := chapterBodyToAPI(chapter.Body)
	if err != nil {
		return runtimeapi.ChapterRecord{}, err
	}
	meta, err := chapterMetaFromRuntimeChapter(chapter)
	if err != nil {
		return runtimeapi.ChapterRecord{}, err
	}
	metadata, err := metadataJSONToAPI(meta.Metadata)
	if err != nil {
		return runtimeapi.ChapterRecord{}, err
	}
	input, err := applicationPayloadOptional(meta.Input)
	if err != nil {
		return runtimeapi.ChapterRecord{}, err
	}
	runPolicy, err := runPolicyPointerToAPI(meta.RunPolicy)
	if err != nil {
		return runtimeapi.ChapterRecord{}, err
	}
	artifacts := make([]runtimeapi.StoredArtifact, 0, len(chapter.Artifacts))
	for _, artifact := range chapter.Artifacts {
		artifacts = append(artifacts, runtimeapi.StoredArtifact{
			Name:   artifact.Name,
			Digest: artifact.Digest,
			Size:   artifact.Size,
		})
	}
	out := runtimeapi.ChapterRecord{
		Artifacts: artifacts,
		Body:      body,
		CreatedAt: meta.CreatedAt,
		Input:     input,
		InputHash: stringPtrOrNil(meta.InputHash),
		Metadata:  metadata,
		Ordinal:   meta.Ordinal,
		RunPolicy: runPolicy,
		TaskType:  stringPtrOrNil(meta.TaskType),
		WorkerId:  stringPtrOrNil(meta.WorkerID),
	}
	if meta.StartedAt != nil {
		out.StartedAt = timePtr(*meta.StartedAt)
	}
	if meta.FinishedAt != nil {
		out.FinishedAt = timePtr(*meta.FinishedAt)
	}
	if meta.Attempt != 0 {
		out.Attempt = int32Ptr(int32(meta.Attempt))
	}
	if meta.MaxAttempts != 0 {
		out.MaxAttempts = int32Ptr(int32(meta.MaxAttempts))
	}
	if meta.NextAttemptAt != nil {
		out.NextAttemptAt = timePtr(*meta.NextAttemptAt)
	}
	if meta.BackoffMillis != 0 {
		out.BackoffMillis = int64Ptr(meta.BackoffMillis)
	}
	if meta.Retryable != nil {
		out.Retryable = boolPtr(*meta.Retryable)
	}
	if meta.InputRef != nil {
		out.InputRef = inputReferenceToAPI(meta.InputRef)
	}
	if len(meta.Prerequisites) > 0 {
		out.Prerequisites = toAPIPrerequisites(meta.Prerequisites)
	}
	return out, nil
}

func fromAPIStoredChapter(chapter runtimeapi.ChapterRecord) (jobdb.Chapter, error) {
	body, err := chapterBodyFromAPI(chapter.Body)
	if err != nil {
		return jobdb.Chapter{}, err
	}
	metadata, err := chapterMetadataFromAPIRecord(chapter)
	if err != nil {
		return jobdb.Chapter{}, err
	}
	out := jobdb.Chapter{
		Ordinal:   chapter.Ordinal,
		Body:      body,
		CreatedAt: chapter.CreatedAt,
		Metadata:  metadata,
	}
	if chapter.InputHash != nil {
		out.InputHash = *chapter.InputHash
	}
	if chapter.TaskType != nil {
		out.TaskType = *chapter.TaskType
	}
	for _, artifact := range chapter.Artifacts {
		out.Artifacts = append(out.Artifacts, jobdb.StoredArtifact{
			Name:   artifact.Name,
			Digest: artifact.Digest,
			Size:   artifact.Size,
		})
	}
	return out, nil
}

func writableChapterToRuntimeChapter(chapter runtimeapi.ChapterRecord, artifactUploads []runtimeapi.ArtifactWrite) (jobdb.Chapter, []jobdb.ArtifactUpload, error) {
	out, err := fromAPIStoredChapter(chapter)
	if err != nil {
		return jobdb.Chapter{}, nil, err
	}
	uploads := make([]jobdb.ArtifactUpload, 0, len(artifactUploads))
	if len(artifactUploads) > 0 {
		out.Artifacts = out.Artifacts[:0]
	}
	for _, artifact := range artifactUploads {
		content := append([]byte(nil), artifact.ContentBase64...)
		out.Artifacts = append(out.Artifacts, jobdb.StoredArtifact{
			Name:   artifact.Name,
			Size:   artifact.Size,
			Digest: digestBytes(content),
		})
		uploads = append(uploads, jobdb.ArtifactUpload{
			Name: artifact.Name,
			Size: artifact.Size,
			Open: func() func() (io.ReadCloser, error) {
				bytesCopy := append([]byte(nil), content...)
				return func() (io.ReadCloser, error) {
					return io.NopCloser(bytes.NewReader(bytesCopy)), nil
				}
			}(),
		})
	}
	return out, uploads, nil
}

func runtimeChapterToAddRequest(ctx context.Context, chapter jobdb.Chapter, uploads []jobdb.ArtifactUpload) (runtimeapi.AddChapterRequest, error) {
	record, err := toAPIStoredChapter(ctx, chapter)
	if err != nil {
		return runtimeapi.AddChapterRequest{}, err
	}
	artifacts := make([]runtimeapi.ArtifactWrite, 0, len(uploads))
	storedArtifacts := make([]runtimeapi.StoredArtifact, 0, len(uploads))
	for _, upload := range uploads {
		rc, err := upload.Open()
		if err != nil {
			return runtimeapi.AddChapterRequest{}, err
		}
		content, readErr := io.ReadAll(rc)
		_ = rc.Close()
		if readErr != nil {
			return runtimeapi.AddChapterRequest{}, readErr
		}
		artifacts = append(artifacts, runtimeapi.ArtifactWrite{
			Name:          upload.Name,
			Size:          upload.Size,
			ContentBase64: content,
		})
		storedArtifacts = append(storedArtifacts, runtimeapi.StoredArtifact{
			Name:   upload.Name,
			Size:   upload.Size,
			Digest: digestBytes(content),
		})
	}
	if len(storedArtifacts) > 0 {
		record.Artifacts = storedArtifacts
	}
	return runtimeapi.AddChapterRequest{Chapter: record, ArtifactUploads: artifacts}, nil
}

func taskOutcomeFromTaskData(data jobdb.TaskData, payloadErr error, raw json.RawMessage) (runtimeapi.TaskOutcome, error) {
	switch payloadKindFromTaskData(data, payloadErr) {
	case runtimecodec.PayloadKindTimeout:
		var timeoutErr *jobdb.TimeoutError
		if errors.As(payloadErr, &timeoutErr) && timeoutErr != nil {
			return timeoutOutcomeToAPI(timeoutErr.Payload)
		}
		var payload jobdb.TimeoutPayload
		if err := json.Unmarshal(raw, &payload); err != nil {
			return runtimeapi.TaskOutcome{}, err
		}
		return timeoutOutcomeToAPI(payload)
	case runtimecodec.PayloadKindAppError:
		var appErr *jobdb.AppError
		if errors.As(payloadErr, &appErr) && appErr != nil {
			return appErrorOutcomeToAPI(appErr.Payload)
		}
		var payload jobdb.AppErrorPayload
		if err := json.Unmarshal(raw, &payload); err != nil {
			return runtimeapi.TaskOutcome{}, err
		}
		return appErrorOutcomeToAPI(payload)
	case runtimecodec.PayloadKindSystemError:
		var systemErr *jobdb.SystemError
		if errors.As(payloadErr, &systemErr) && systemErr != nil {
			return systemErrorOutcomeToAPI(systemErr.Payload)
		}
		var payload jobdb.SystemErrorPayload
		if err := json.Unmarshal(raw, &payload); err != nil {
			return runtimeapi.TaskOutcome{}, err
		}
		return systemErrorOutcomeToAPI(payload)
	default:
		return successOutcomeToAPI(raw)
	}
}

func taskDataFromOutcome(artifacts []jobdb.Artifact, outcome runtimeapi.TaskOutcome) (jobdb.TaskData, error) {
	discriminator, err := outcome.Discriminator()
	if err != nil {
		return nil, err
	}
	switch discriminator {
	case "success":
		converted, err := outcome.AsTaskOutcomeSuccess()
		if err != nil {
			return nil, err
		}
		raw, err := applicationPayloadToRaw(converted.Output)
		if err != nil {
			return nil, err
		}
		return envelopedTaskData(artifacts, runtimecodec.PayloadKindApp, raw), nil
	case "appError":
		converted, err := outcome.AsTaskOutcomeAppError()
		if err != nil {
			return nil, err
		}
		payload, err := appErrorFromAPI(converted.Error)
		if err != nil {
			return nil, err
		}
		raw, err := json.Marshal(payload)
		if err != nil {
			return nil, err
		}
		td := envelopedTaskData(artifacts, runtimecodec.PayloadKindAppError, raw)
		if err, ok := decodeJobFailedAppError(payload); ok {
			return td, err
		}
		return td, &jobdb.AppError{Payload: payload}
	case "systemError":
		converted, err := outcome.AsTaskOutcomeSystemError()
		if err != nil {
			return nil, err
		}
		payload := systemErrorFromAPI(converted.Error)
		raw, err := json.Marshal(payload)
		if err != nil {
			return nil, err
		}
		return envelopedTaskData(artifacts, runtimecodec.PayloadKindSystemError, raw), &jobdb.SystemError{Payload: payload}
	case "timeout":
		converted, err := outcome.AsTaskOutcomeTimeout()
		if err != nil {
			return nil, err
		}
		payload, err := timeoutFromAPI(converted.Timeout)
		if err != nil {
			return nil, err
		}
		raw, err := json.Marshal(payload)
		if err != nil {
			return nil, err
		}
		return envelopedTaskData(artifacts, runtimecodec.PayloadKindTimeout, raw), &jobdb.TimeoutError{Payload: payload}
	default:
		return nil, fmt.Errorf("unsupported task outcome discriminator %q", discriminator)
	}
}

func envelopedTaskData(artifacts []jobdb.Artifact, kind string, raw json.RawMessage) *jobdb.EnvelopedTaskData {
	return &jobdb.EnvelopedTaskData{
		SimpleTaskData: jobdb.SimpleTaskData{
			Data:      cloneRawMessage(raw),
			Artifacts: artifacts,
		},
		Kind: kind,
	}
}

func successOutcomeToAPI(raw json.RawMessage) (runtimeapi.TaskOutcome, error) {
	payload, err := applicationPayloadRequired(raw)
	if err != nil {
		return runtimeapi.TaskOutcome{}, err
	}
	var out runtimeapi.TaskOutcome
	err = out.FromTaskOutcomeSuccess(runtimeapi.TaskOutcomeSuccess{Output: payload})
	return out, err
}

func appErrorOutcomeToAPI(payload jobdb.AppErrorPayload) (runtimeapi.TaskOutcome, error) {
	converted, err := appErrorToAPI(payload)
	if err != nil {
		return runtimeapi.TaskOutcome{}, err
	}
	var out runtimeapi.TaskOutcome
	err = out.FromTaskOutcomeAppError(runtimeapi.TaskOutcomeAppError{Error: converted})
	return out, err
}

func systemErrorOutcomeToAPI(payload jobdb.SystemErrorPayload) (runtimeapi.TaskOutcome, error) {
	var out runtimeapi.TaskOutcome
	err := out.FromTaskOutcomeSystemError(runtimeapi.TaskOutcomeSystemError{Error: systemErrorToAPI(payload)})
	return out, err
}

func timeoutOutcomeToAPI(payload jobdb.TimeoutPayload) (runtimeapi.TaskOutcome, error) {
	var out runtimeapi.TaskOutcome
	err := out.FromTaskOutcomeTimeout(runtimeapi.TaskOutcomeTimeout{Timeout: timeoutToAPI(payload)})
	return out, err
}

func chapterBodyToAPI(body jobdb.ChapterBody) (runtimeapi.ChapterBody, error) {
	var out runtimeapi.ChapterBody
	switch body := body.(type) {
	case jobdb.JobStartChapter:
		input, err := applicationPayloadRequired(body.Input.Data)
		if err != nil {
			return out, err
		}
		err = out.FromJobStartChapter(runtimeapi.JobStartChapter{Input: input})
		return out, err
	case *jobdb.JobStartChapter:
		if body == nil {
			return out, fmt.Errorf("chapter body is required")
		}
		return chapterBodyToAPI(*body)
	case jobdb.JobAttemptOutcomeChapter:
		outcome, err := chapterOutcomeToAPI(body.Outcome)
		if err != nil {
			return out, err
		}
		err = out.FromJobAttemptOutcomeChapter(runtimeapi.JobAttemptOutcomeChapter{Outcome: outcome})
		return out, err
	case *jobdb.JobAttemptOutcomeChapter:
		if body == nil {
			return out, fmt.Errorf("chapter body is required")
		}
		return chapterBodyToAPI(*body)
	case jobdb.TaskAttemptOutcomeChapter:
		outcome, err := chapterOutcomeToAPI(body.Outcome)
		if err != nil {
			return out, err
		}
		err = out.FromTaskAttemptOutcomeChapter(runtimeapi.TaskAttemptOutcomeChapter{Outcome: outcome})
		return out, err
	case *jobdb.TaskAttemptOutcomeChapter:
		if body == nil {
			return out, fmt.Errorf("chapter body is required")
		}
		return chapterBodyToAPI(*body)
	case jobdb.RestartExtraChapter:
		output, err := applicationPayloadRequired(body.Output.Data)
		if err != nil {
			return out, err
		}
		err = out.FromRestartExtraChapter(runtimeapi.RestartExtraChapter{Output: output})
		return out, err
	case *jobdb.RestartExtraChapter:
		if body == nil {
			return out, fmt.Errorf("chapter body is required")
		}
		return chapterBodyToAPI(*body)
	default:
		return out, fmt.Errorf("unsupported chapter body type %T", body)
	}
}

func chapterBodyFromAPI(body runtimeapi.ChapterBody) (jobdb.ChapterBody, error) {
	discriminator, err := body.Discriminator()
	if err != nil {
		return nil, err
	}
	switch discriminator {
	case "jobStart":
		converted, err := body.AsJobStartChapter()
		if err != nil {
			return nil, err
		}
		raw, err := applicationPayloadToRaw(converted.Input)
		if err != nil {
			return nil, err
		}
		return jobdb.JobStartChapter{Input: jobdb.ApplicationInputBytes{Data: raw}}, nil
	case "jobAttemptOutcome":
		converted, err := body.AsJobAttemptOutcomeChapter()
		if err != nil {
			return nil, err
		}
		outcome, err := chapterOutcomeFromAPI(converted.Outcome)
		if err != nil {
			return nil, err
		}
		return jobdb.JobAttemptOutcomeChapter{Outcome: outcome}, nil
	case "taskAttemptOutcome":
		converted, err := body.AsTaskAttemptOutcomeChapter()
		if err != nil {
			return nil, err
		}
		outcome, err := chapterOutcomeFromAPI(converted.Outcome)
		if err != nil {
			return nil, err
		}
		return jobdb.TaskAttemptOutcomeChapter{Outcome: outcome}, nil
	case "restartExtra":
		converted, err := body.AsRestartExtraChapter()
		if err != nil {
			return nil, err
		}
		raw, err := applicationPayloadToRaw(converted.Output)
		if err != nil {
			return nil, err
		}
		return jobdb.RestartExtraChapter{Output: jobdb.ApplicationOutputBytes{Data: raw}}, nil
	default:
		return nil, fmt.Errorf("unsupported chapter discriminator %q", discriminator)
	}
}

func chapterOutcomeToAPI(outcome jobdb.ChapterOutcome) (runtimeapi.TaskOutcome, error) {
	switch outcome := outcome.(type) {
	case jobdb.ApplicationOutputOutcome:
		return successOutcomeToAPI(outcome.Output.Data)
	case *jobdb.ApplicationOutputOutcome:
		if outcome == nil {
			return runtimeapi.TaskOutcome{}, fmt.Errorf("task outcome is required")
		}
		return chapterOutcomeToAPI(*outcome)
	case jobdb.AppErrorOutcome:
		return appErrorOutcomeToAPI(outcome.Error)
	case *jobdb.AppErrorOutcome:
		if outcome == nil {
			return runtimeapi.TaskOutcome{}, fmt.Errorf("task outcome is required")
		}
		return chapterOutcomeToAPI(*outcome)
	case jobdb.SystemErrorOutcome:
		return systemErrorOutcomeToAPI(outcome.Error)
	case *jobdb.SystemErrorOutcome:
		if outcome == nil {
			return runtimeapi.TaskOutcome{}, fmt.Errorf("task outcome is required")
		}
		return chapterOutcomeToAPI(*outcome)
	case jobdb.TimeoutOutcome:
		return timeoutOutcomeToAPI(outcome.Timeout)
	case *jobdb.TimeoutOutcome:
		if outcome == nil {
			return runtimeapi.TaskOutcome{}, fmt.Errorf("task outcome is required")
		}
		return chapterOutcomeToAPI(*outcome)
	default:
		return runtimeapi.TaskOutcome{}, fmt.Errorf("unsupported task outcome type %T", outcome)
	}
}

func chapterOutcomeFromAPI(outcome runtimeapi.TaskOutcome) (jobdb.ChapterOutcome, error) {
	discriminator, err := outcome.Discriminator()
	if err != nil {
		return nil, err
	}
	switch discriminator {
	case "success":
		converted, err := outcome.AsTaskOutcomeSuccess()
		if err != nil {
			return nil, err
		}
		raw, err := applicationPayloadToRaw(converted.Output)
		if err != nil {
			return nil, err
		}
		return jobdb.ApplicationOutputOutcome{Output: jobdb.ApplicationOutputBytes{Data: raw}}, nil
	case "appError":
		converted, err := outcome.AsTaskOutcomeAppError()
		if err != nil {
			return nil, err
		}
		payload, err := appErrorFromAPI(converted.Error)
		if err != nil {
			return nil, err
		}
		return jobdb.AppErrorOutcome{Error: payload}, nil
	case "systemError":
		converted, err := outcome.AsTaskOutcomeSystemError()
		if err != nil {
			return nil, err
		}
		return jobdb.SystemErrorOutcome{Error: systemErrorFromAPI(converted.Error)}, nil
	case "timeout":
		converted, err := outcome.AsTaskOutcomeTimeout()
		if err != nil {
			return nil, err
		}
		payload, err := timeoutFromAPI(converted.Timeout)
		if err != nil {
			return nil, err
		}
		return jobdb.TimeoutOutcome{Timeout: payload}, nil
	default:
		return nil, fmt.Errorf("unsupported task outcome discriminator %q", discriminator)
	}
}

func appErrorToAPI(payload jobdb.AppErrorPayload) (runtimeapi.AppErrorPayload, error) {
	attrs, err := metadataMapToAPI(payload.Attrs)
	if err != nil {
		return runtimeapi.AppErrorPayload{}, err
	}
	out := runtimeapi.AppErrorPayload{
		Attrs:    attrs,
		InputRef: inputReferenceToAPI(payload.InputRef),
		Level:    stringPtrOrNil(payload.Level),
		Message:  payload.Message,
	}
	if len(payload.Stacktrace) > 0 {
		stack := append([]string(nil), payload.Stacktrace...)
		out.Stacktrace = &stack
	}
	return out, nil
}

func appErrorFromAPI(payload runtimeapi.AppErrorPayload) (jobdb.AppErrorPayload, error) {
	attrs, err := metadataMapFromAPI(payload.Attrs)
	if err != nil {
		return jobdb.AppErrorPayload{}, err
	}
	out := jobdb.AppErrorPayload{
		Message:  payload.Message,
		Level:    stringValue(payload.Level),
		Attrs:    attrs,
		InputRef: inputReferenceFromAPI(payload.InputRef),
	}
	if payload.Stacktrace != nil {
		out.Stacktrace = append([]string(nil), (*payload.Stacktrace)...)
	}
	return out, nil
}

func systemErrorToAPI(payload jobdb.SystemErrorPayload) runtimeapi.SystemErrorPayload {
	out := runtimeapi.SystemErrorPayload{
		Code:      stringPtrOrNil(payload.Code),
		Component: stringPtrOrNil(payload.Component),
		InputRef:  inputReferenceToAPI(payload.InputRef),
		Message:   payload.Message,
	}
	if payload.Retryable {
		out.Retryable = boolPtr(payload.Retryable)
	}
	if len(payload.Stacktrace) > 0 {
		stack := append([]string(nil), payload.Stacktrace...)
		out.Stacktrace = &stack
	}
	return out
}

func systemErrorFromAPI(payload runtimeapi.SystemErrorPayload) jobdb.SystemErrorPayload {
	out := jobdb.SystemErrorPayload{
		Message:   payload.Message,
		Component: stringValue(payload.Component),
		Code:      stringValue(payload.Code),
		Retryable: boolValue(payload.Retryable),
		InputRef:  inputReferenceFromAPI(payload.InputRef),
	}
	if payload.Stacktrace != nil {
		out.Stacktrace = append([]string(nil), (*payload.Stacktrace)...)
	}
	return out
}

func timeoutToAPI(payload jobdb.TimeoutPayload) runtimeapi.TimeoutPayload {
	out := runtimeapi.TimeoutPayload{
		After:     time.Duration(payload.After).String(),
		Code:      stringPtrOrNil(payload.Code),
		Component: stringPtrOrNil(payload.Component),
		InputRef:  inputReferenceToAPI(payload.InputRef),
		Kind:      stringPtrOrNil(payload.Kind),
		Message:   stringPtrOrNil(payload.Message),
		Retryable: payload.Retryable,
		Scope:     payload.Scope,
	}
	return out
}

func timeoutFromAPI(payload runtimeapi.TimeoutPayload) (jobdb.TimeoutPayload, error) {
	after, err := time.ParseDuration(payload.After)
	if err != nil {
		return jobdb.TimeoutPayload{}, err
	}
	return jobdb.TimeoutPayload{
		Scope:     payload.Scope,
		After:     jobdb.Duration(after),
		Retryable: payload.Retryable,
		InputRef:  inputReferenceFromAPI(payload.InputRef),
		Kind:      stringValue(payload.Kind),
		Component: stringValue(payload.Component),
		Code:      stringValue(payload.Code),
		Message:   stringValue(payload.Message),
	}, nil
}

func inputReferenceToAPI(ref *jobdb.InputReference) *runtimeapi.InputReference {
	if ref == nil {
		return nil
	}
	return &runtimeapi.InputReference{
		Hash:    stringPtrOrNil(ref.Hash),
		Ordinal: ref.Ordinal,
	}
}

func inputReferenceFromAPI(ref *runtimeapi.InputReference) *jobdb.InputReference {
	if ref == nil {
		return nil
	}
	return &jobdb.InputReference{
		Ordinal: ref.Ordinal,
		Hash:    stringValue(ref.Hash),
	}
}

func runPolicyPointerToAPI(policy *jobdb.RunPolicy) (*runtimeapi.RunPolicy, error) {
	if policy == nil {
		return nil, nil
	}
	return runPolicyToAPI(*policy)
}

func chapterMetaFromRuntimeChapter(chapter jobdb.Chapter) (runtimecodec.ChapterMeta, error) {
	meta := runtimecodec.ChapterMeta{
		Version:   runtimecodec.EnvelopeVersion,
		Ordinal:   chapter.Ordinal,
		TaskType:  chapter.TaskType,
		CreatedAt: chapter.CreatedAt,
		InputHash: chapter.InputHash,
	}
	rawMetadata, err := runtimecodec.ChapterMetadataToJSON(chapter.Metadata)
	if err != nil {
		return runtimecodec.ChapterMeta{}, err
	}
	if len(rawMetadata) > 0 {
		if chapterMetadataLooksLikeEnvelope(rawMetadata) {
			if err := json.Unmarshal(rawMetadata, &meta); err != nil {
				return runtimecodec.ChapterMeta{}, err
			}
		} else {
			meta.Metadata = cloneRawMessage(rawMetadata)
		}
	}
	if meta.Version == 0 {
		meta.Version = runtimecodec.EnvelopeVersion
	}
	if meta.Ordinal == 0 && chapter.Ordinal != 0 {
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
	return meta, nil
}

func chapterMetadataLooksLikeEnvelope(raw json.RawMessage) bool {
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		return false
	}
	for name := range fields {
		switch name {
		case "version", "ordinal", "task_type", "worker_id", "created_at", "started_at", "finished_at",
			"input_hash", "metadata", "input", "attempt", "max_attempts", "next_attempt_at",
			"backoff_ms", "retryable", "input_ref", "run_policy", "prereqs":
			return true
		}
	}
	return false
}

func chapterMetadataFromAPIRecord(chapter runtimeapi.ChapterRecord) (jobdb.ChapterMetadata, error) {
	metadataRaw, err := metadataAPIToJSON(chapter.Metadata)
	if err != nil {
		return jobdb.ChapterMetadata{}, err
	}
	inputRaw, err := applicationPayloadPointerToRaw(chapter.Input)
	if err != nil {
		return jobdb.ChapterMetadata{}, err
	}
	runPolicy, err := runPolicyFromAPI(chapter.RunPolicy)
	if err != nil {
		return jobdb.ChapterMetadata{}, err
	}
	meta := runtimecodec.ChapterMeta{
		Version:       runtimecodec.EnvelopeVersion,
		Ordinal:       chapter.Ordinal,
		TaskType:      stringValue(chapter.TaskType),
		WorkerID:      stringValue(chapter.WorkerId),
		CreatedAt:     chapter.CreatedAt,
		StartedAt:     cloneTime(chapter.StartedAt),
		FinishedAt:    cloneTime(chapter.FinishedAt),
		InputHash:     stringValue(chapter.InputHash),
		Metadata:      metadataRaw,
		Input:         inputRaw,
		Attempt:       intValue32(chapter.Attempt),
		MaxAttempts:   intValue32(chapter.MaxAttempts),
		NextAttemptAt: cloneTime(chapter.NextAttemptAt),
		BackoffMillis: int64Value(chapter.BackoffMillis),
		Retryable:     cloneBool(chapter.Retryable),
		InputRef:      inputReferenceFromAPI(chapter.InputRef),
		Prerequisites: fromAPIPrerequisites(chapter.Prerequisites),
	}
	if chapter.RunPolicy != nil {
		meta.RunPolicy = &runPolicy
	}
	raw, err := json.Marshal(meta)
	if err != nil {
		return jobdb.ChapterMetadata{}, err
	}
	return runtimecodec.ChapterMetadataFromJSON(raw)
}

func jobInfoToAPI(ctx context.Context, info jobdb.JobInfo) (runtimeapi.JobInfo, error) {
	out := runtimeapi.JobInfo{
		Status: runtimeapi.JobStatus(info.Status),
	}
	taskData, payloadErr := jobdb.ExtractTaskDataResult(info.Data)
	if errors.Is(payloadErr, jobdb.ErrJobNotComplete) && taskData == nil {
		return out, nil
	}
	stored, err := taskDataToAPIStored(ctx, taskData, payloadErr)
	if err != nil {
		return runtimeapi.JobInfo{}, err
	}
	out.Data = stored
	return out, nil
}

func jobInfoFromAPI(runtime *Runtime, jobKey jobdb.JobKey, info runtimeapi.JobInfo) (jobdb.JobInfo, error) {
	out := jobdb.JobInfo{
		Status: jobdb.JobStatus(info.Status),
		Data:   &jobInfoTaskData{err: jobdb.ErrJobNotComplete},
	}
	if info.Data == nil {
		return out, nil
	}
	taskData, payloadErr := taskDataFromAPIStored(runtime, jobKey, *info.Data)
	if payloadErr != nil && taskData == nil {
		return jobdb.JobInfo{}, payloadErr
	}
	out.Data = &jobInfoTaskData{taskData: taskData, err: payloadErr}
	return out, nil
}

func jobSummaryToAPI(summary jobdb.JobSummary) (runtimeapi.JobSummary, error) {
	payload, err := schedulerPayloadOptionalToAPI(summary.Payload)
	if err != nil {
		return runtimeapi.JobSummary{}, err
	}
	metadata, err := metadataJSONToAPI(summary.Metadata)
	if err != nil {
		return runtimeapi.JobSummary{}, err
	}
	out := runtimeapi.JobSummary{
		ArchivedAt:        summary.ArchivedAt,
		AvailableAt:       summary.AvailableAt,
		CancelRequested:   summary.CancelRequested,
		CreatedAt:         summary.CreatedAt,
		ExpiresAt:         summary.ExpiresAt,
		JobKey:            toAPIJobKey(summary.JobKey),
		JobType:           summary.JobType,
		LeaseExpiresAt:    summary.LeaseExpiresAt,
		Metadata:          metadata,
		NextNeed:          cloneString(summary.NextNeed),
		Payload:           payload,
		Status:            runtimeapi.JobStatus(summary.Status),
		TaskWaitInput:     cloneInt64(summary.TaskWaitInput),
		TaskWaitInputHash: cloneString(summary.TaskWaitInputHash),
		TaskWaitNext:      cloneString(summary.TaskWaitNext),
		TaskWaitOutput:    cloneInt64(summary.TaskWaitOutput),
		WaitFor:           append([]string(nil), summary.WaitFor...),
	}
	return out, nil
}

func jobSummaryFromAPI(summary runtimeapi.JobSummary) (jobdb.JobSummary, error) {
	payload, err := schedulerPayloadPointerFromAPI(summary.Payload)
	if err != nil {
		return jobdb.JobSummary{}, err
	}
	metadata, err := metadataAPIToJSON(summary.Metadata)
	if err != nil {
		return jobdb.JobSummary{}, err
	}
	return jobdb.JobSummary{
		JobKey:            fromAPIJobKey(summary.JobKey),
		Status:            jobdb.JobStatus(summary.Status),
		JobType:           summary.JobType,
		NextNeed:          cloneString(summary.NextNeed),
		WaitFor:           append([]string(nil), summary.WaitFor...),
		AvailableAt:       summary.AvailableAt,
		ExpiresAt:         summary.ExpiresAt,
		LeaseExpiresAt:    summary.LeaseExpiresAt,
		CancelRequested:   summary.CancelRequested,
		CreatedAt:         summary.CreatedAt,
		ArchivedAt:        summary.ArchivedAt,
		Payload:           payload,
		Metadata:          metadata,
		TaskWaitInput:     cloneInt64(summary.TaskWaitInput),
		TaskWaitOutput:    cloneInt64(summary.TaskWaitOutput),
		TaskWaitInputHash: cloneString(summary.TaskWaitInputHash),
		TaskWaitNext:      cloneString(summary.TaskWaitNext),
	}, nil
}

func scheduleTriggerToAPI(trigger jobdb.ScheduleTrigger) runtimeapi.ScheduleTrigger {
	out := runtimeapi.ScheduleTrigger{
		Kind:     runtimeapi.ScheduleTriggerKind(trigger.Kind),
		StartAt:  cloneTime(trigger.StartAt),
		EndAt:    cloneTime(trigger.EndAt),
		Timezone: stringPtrOrNil(trigger.Timezone),
	}
	if trigger.Expression != "" {
		out.Expression = stringPtr(trigger.Expression)
	}
	if trigger.Interval > 0 {
		interval := trigger.Interval.String()
		out.Interval = &interval
	}
	return out
}

func scheduleTriggerFromAPI(trigger runtimeapi.ScheduleTrigger) (jobdb.ScheduleTrigger, error) {
	interval, err := fromAPIStdDurationValue(trigger.Interval)
	if err != nil {
		return jobdb.ScheduleTrigger{}, err
	}
	out := jobdb.ScheduleTrigger{
		Kind:       jobdb.ScheduleTriggerKind(trigger.Kind),
		Expression: stringValue(trigger.Expression),
		Timezone:   stringValue(trigger.Timezone),
		StartAt:    cloneTime(trigger.StartAt),
		EndAt:      cloneTime(trigger.EndAt),
	}
	if interval != nil {
		out.Interval = *interval
	}
	return out, nil
}

func scheduleFailurePolicyToAPI(policy jobdb.ScheduleFailurePolicy) runtimeapi.ScheduleFailurePolicy {
	out := runtimeapi.ScheduleFailurePolicy{}
	if policy.MinSuccessPercent != 0 {
		out.MinSuccessPercent = int32Ptr(int32(policy.MinSuccessPercent))
	}
	if policy.WindowSize != 0 {
		out.WindowSize = int32Ptr(int32(policy.WindowSize))
	}
	if policy.MaxSequentialFailures != 0 {
		out.MaxSequentialFailures = int32Ptr(int32(policy.MaxSequentialFailures))
	}
	return out
}

func scheduleFailurePolicyFromAPI(policy *runtimeapi.ScheduleFailurePolicy) jobdb.ScheduleFailurePolicy {
	if policy == nil {
		return jobdb.ScheduleFailurePolicy{}
	}
	return jobdb.ScheduleFailurePolicy{
		MinSuccessPercent:     intValue32(policy.MinSuccessPercent),
		WindowSize:            intValue32(policy.WindowSize),
		MaxSequentialFailures: intValue32(policy.MaxSequentialFailures),
	}
}

func scheduleTargetToAPI(ctx context.Context, target jobdb.ScheduleTarget) (runtimeapi.ScheduleTarget, error) {
	data, err := taskDataToAPIWrite(ctx, jobdb.TaskData(target.Data))
	if err != nil {
		return runtimeapi.ScheduleTarget{}, err
	}
	runPolicy, err := runPolicyToAPI(target.RunPolicy)
	if err != nil {
		return runtimeapi.ScheduleTarget{}, err
	}
	metadata, err := metadataJSONToAPI(target.Metadata)
	if err != nil {
		return runtimeapi.ScheduleTarget{}, err
	}
	return runtimeapi.ScheduleTarget{
		Data:      data,
		JobType:   target.JobType,
		Metadata:  metadata,
		RunPolicy: runPolicy,
	}, nil
}

func scheduleTargetFromAPI(target runtimeapi.ScheduleTarget) (jobdb.ScheduleTarget, error) {
	data, err := taskDataFromAPIWrite(target.Data)
	if err != nil {
		return jobdb.ScheduleTarget{}, err
	}
	runPolicy, err := runPolicyFromAPI(target.RunPolicy)
	if err != nil {
		return jobdb.ScheduleTarget{}, err
	}
	metadata, err := metadataAPIToJSON(target.Metadata)
	if err != nil {
		return jobdb.ScheduleTarget{}, err
	}
	return jobdb.ScheduleTarget{
		JobType:   target.JobType,
		Data:      jobdb.JobData(data),
		RunPolicy: runPolicy,
		Metadata:  metadata,
	}, nil
}

func scheduleInfoToAPI(ctx context.Context, info jobdb.ScheduleInfo) (runtimeapi.ScheduleInfo, error) {
	target, err := scheduleTargetToAPI(ctx, info.Target)
	if err != nil {
		return runtimeapi.ScheduleInfo{}, err
	}
	out := runtimeapi.ScheduleInfo{
		CreatedAt:      info.CreatedAt,
		EffectiveState: runtimeapi.ScheduleState(info.EffectiveState),
		FailurePolicy:  scheduleFailurePolicyToAPI(info.FailurePolicy),
		Generation:     info.Generation,
		NextFireAt:     cloneTime(info.NextFireAt),
		OverlapPolicy:  runtimeapi.ScheduleOverlapPolicy(info.OverlapPolicy),
		ScheduleId:     info.ScheduleId,
		ScheduleKey:    toAPIScheduleKey(info.ScheduleKey),
		SpecHash:       info.SpecHash,
		State:          runtimeapi.ScheduleState(info.State),
		Target:         target,
		TenantId:       info.TenantId,
		Trigger:        scheduleTriggerToAPI(info.Trigger),
		UpdatedAt:      info.UpdatedAt,
	}
	if info.NextJobKey != nil {
		key := toAPIJobKey(*info.NextJobKey)
		out.NextJobKey = &key
	}
	return out, nil
}

func scheduleInfoFromAPI(info runtimeapi.ScheduleInfo) (jobdb.ScheduleInfo, error) {
	target, err := scheduleTargetFromAPI(info.Target)
	if err != nil {
		return jobdb.ScheduleInfo{}, err
	}
	trigger, err := scheduleTriggerFromAPI(info.Trigger)
	if err != nil {
		return jobdb.ScheduleInfo{}, err
	}
	out := jobdb.ScheduleInfo{
		TenantId:       info.TenantId,
		ScheduleId:     info.ScheduleId,
		ScheduleKey:    scheduleKeyFromAPI(info.ScheduleKey),
		State:          jobdb.ScheduleState(info.State),
		EffectiveState: jobdb.ScheduleState(info.EffectiveState),
		Generation:     info.Generation,
		SpecHash:       info.SpecHash,
		Trigger:        trigger,
		Target:         target,
		OverlapPolicy:  jobdb.ScheduleOverlapPolicy(info.OverlapPolicy),
		FailurePolicy:  scheduleFailurePolicyFromAPI(&info.FailurePolicy),
		NextFireAt:     cloneTime(info.NextFireAt),
		CreatedAt:      info.CreatedAt,
		UpdatedAt:      info.UpdatedAt,
	}
	if info.NextJobKey != nil {
		key := fromAPIJobKey(*info.NextJobKey)
		out.NextJobKey = &key
	}
	return out, nil
}

func toAPIScheduleKey(key jobdb.ScheduleKey) runtimeapi.ScheduleKey {
	return runtimeapi.ScheduleKey{
		TenantId:   key.TenantId,
		ScheduleId: key.ScheduleId,
	}
}

func scheduleKeyFromAPI(key runtimeapi.ScheduleKey) jobdb.ScheduleKey {
	return jobdb.ScheduleKey{
		TenantId:   key.TenantId,
		ScheduleId: key.ScheduleId,
	}
}

func scheduleRunSummaryToAPI(summary jobdb.ScheduleRunSummary) (runtimeapi.ScheduleRunSummary, error) {
	job, err := jobSummaryToAPI(summary.JobSummary)
	if err != nil {
		return runtimeapi.ScheduleRunSummary{}, err
	}
	return runtimeapi.ScheduleRunSummary{
		Job:         job,
		ReasonCode:  stringPtrOrNil(summary.ReasonCode),
		ScheduleId:  summary.ScheduleId,
		ScheduledAt: summary.ScheduledAt,
	}, nil
}

func scheduleRunSummaryFromAPI(summary runtimeapi.ScheduleRunSummary) (jobdb.ScheduleRunSummary, error) {
	job, err := jobSummaryFromAPI(summary.Job)
	if err != nil {
		return jobdb.ScheduleRunSummary{}, err
	}
	return jobdb.ScheduleRunSummary{
		JobSummary:  job,
		ScheduleId:  summary.ScheduleId,
		ScheduledAt: summary.ScheduledAt,
		ReasonCode:  stringValue(summary.ReasonCode),
	}, nil
}

func artifactsToAPIWrites(ctx context.Context, data jobdb.TaskData) ([]runtimeapi.ArtifactWrite, error) {
	artifacts, err := data.GetArtifacts()
	if err != nil {
		return nil, err
	}
	out := make([]runtimeapi.ArtifactWrite, 0, len(artifacts))
	for _, artifact := range artifacts {
		if artifact == nil {
			return nil, fmt.Errorf("artifact is nil")
		}
		content, err := artifact.Bytes(ctx)
		if err != nil {
			return nil, err
		}
		out = append(out, runtimeapi.ArtifactWrite{
			Name:          artifact.Name(),
			Size:          artifact.Size(),
			ContentBase64: content,
		})
	}
	return out, nil
}

func storedArtifactsToAPI(ctx context.Context, data jobdb.TaskData) ([]runtimeapi.StoredArtifact, error) {
	artifacts, err := data.GetArtifacts()
	if err != nil {
		return nil, err
	}
	out := make([]runtimeapi.StoredArtifact, 0, len(artifacts))
	for _, artifact := range artifacts {
		if artifact == nil {
			return nil, fmt.Errorf("artifact is nil")
		}
		digest, err := artifact.Sha256(ctx)
		if err != nil {
			return nil, err
		}
		out = append(out, runtimeapi.StoredArtifact{
			Name:   artifact.Name(),
			Digest: digest,
			Size:   artifact.Size(),
		})
	}
	return out, nil
}

func payloadKindFromTaskData(data jobdb.TaskData, payloadErr error) string {
	if enveloped, ok := data.(*jobdb.EnvelopedTaskData); ok && enveloped.Kind != "" {
		return enveloped.Kind
	}
	var timeoutErr *jobdb.TimeoutError
	if errors.As(payloadErr, &timeoutErr) {
		return runtimecodec.PayloadKindTimeout
	}
	var appErr *jobdb.AppError
	if errors.As(payloadErr, &appErr) {
		return runtimecodec.PayloadKindAppError
	}
	var systemErr *jobdb.SystemError
	if errors.As(payloadErr, &systemErr) {
		return runtimecodec.PayloadKindSystemError
	}
	return runtimecodec.PayloadKindApp
}

func schedulerPayloadOptionalToAPI(raw json.RawMessage) (*runtimeapi.SchedulerPayload, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	converted, err := schedulerPayloadToAPI(raw)
	if err != nil {
		return nil, err
	}
	return &converted, nil
}

func schedulerPayloadToAPI(raw json.RawMessage) (runtimeapi.SchedulerPayload, error) {
	parsed, err := runtimecodec.SchedulerPayloadFromJSONView(raw)
	if err != nil {
		return runtimeapi.SchedulerPayload{}, err
	}
	runPolicy, err := runPolicyToAPI(parsed.RunPolicy)
	if err != nil {
		return runtimeapi.SchedulerPayload{}, err
	}
	leasePayloadRaw, err := leasePayloadFromSchedulerJSONView(raw)
	if err != nil {
		return runtimeapi.SchedulerPayload{}, err
	}
	leasePayload, err := applicationPayloadOptional(leasePayloadRaw)
	if err != nil {
		return runtimeapi.SchedulerPayload{}, err
	}
	out := runtimeapi.SchedulerPayload{
		LeasePayload: leasePayload,
		RunPolicy:    runPolicy,
	}
	if parsed.TaskWait != nil {
		out.TaskWait = &runtimeapi.TaskWait{
			InputHash:     parsed.TaskWait.InputHash,
			InputOrdinal:  parsed.TaskWait.InputStep,
			OutputOrdinal: parsed.TaskWait.OutputStep,
			ResumeNeed:    parsed.TaskWait.Next,
		}
	}
	return out, nil
}

func schedulerPayloadPointerFromAPI(value *runtimeapi.SchedulerPayload) (json.RawMessage, error) {
	if value == nil {
		return nil, nil
	}
	return schedulerPayloadFromAPI(*value)
}

func schedulerPayloadFromAPI(value runtimeapi.SchedulerPayload) (json.RawMessage, error) {
	runPolicy, err := runPolicyFromAPI(value.RunPolicy)
	if err != nil {
		return nil, err
	}
	leasePayload, err := applicationPayloadPointerToRaw(value.LeasePayload)
	if err != nil {
		return nil, err
	}
	hasRunPolicy := !runPolicyIsZero(runPolicy)
	hasTaskWait := value.TaskWait != nil
	if !hasRunPolicy && !hasTaskWait {
		return cloneRawMessage(leasePayload), nil
	}
	fields := make(map[string]json.RawMessage)
	if len(leasePayload) > 0 {
		if err := json.Unmarshal(leasePayload, &fields); err != nil || fields == nil {
			return nil, fmt.Errorf("leasePayload must be a JSON object when runPolicy or taskWait is present")
		}
	}
	if hasRunPolicy {
		rawPolicy, err := json.Marshal(runPolicy)
		if err != nil {
			return nil, err
		}
		fields["run_policy"] = rawPolicy
	}
	if value.TaskWait != nil {
		wait := struct {
			InputStep  int64  `json:"in"`
			OutputStep int64  `json:"out"`
			Next       string `json:"next"`
			InputHash  string `json:"input_hash,omitempty"`
		}{
			InputStep:  value.TaskWait.InputOrdinal,
			OutputStep: value.TaskWait.OutputOrdinal,
			Next:       value.TaskWait.ResumeNeed,
			InputHash:  value.TaskWait.InputHash,
		}
		rawWait, err := json.Marshal(wait)
		if err != nil {
			return nil, err
		}
		fields["task_wait"] = rawWait
	}
	raw, err := json.Marshal(fields)
	if err != nil {
		return nil, err
	}
	return json.RawMessage(raw), nil
}

func leasePayloadFromSchedulerJSONView(raw json.RawMessage) (json.RawMessage, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	if !json.Valid(raw) {
		return nil, fmt.Errorf("scheduler payload must be valid JSON")
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil || fields == nil {
		return cloneRawMessage(raw), nil
	}
	hadSchedulerField := false
	if _, ok := fields["run_policy"]; ok {
		delete(fields, "run_policy")
		hadSchedulerField = true
	}
	if _, ok := fields["task_wait"]; ok {
		delete(fields, "task_wait")
		hadSchedulerField = true
	}
	if len(fields) == 0 && hadSchedulerField {
		return nil, nil
	}
	out, err := json.Marshal(fields)
	if err != nil {
		return nil, err
	}
	return json.RawMessage(out), nil
}

func runPolicyIsZero(policy jobdb.RunPolicy) bool {
	return retryPolicyIsZero(policy.Retry) && policy.InvocationTimeout == nil && policy.TotalTimeout == nil
}

func digestBytes(content []byte) string {
	sum := sha256.Sum256(content)
	return hex.EncodeToString(sum[:])
}

func cloneRawMessage(raw json.RawMessage) json.RawMessage {
	if len(raw) == 0 {
		return nil
	}
	return append(json.RawMessage(nil), raw...)
}

func boolPtr(value bool) *bool {
	return &value
}

func boolValue(value *bool) bool {
	if value == nil {
		return false
	}
	return *value
}

func int32Ptr(value int32) *int32 {
	return &value
}

func intValue32(value *int32) int {
	if value == nil {
		return 0
	}
	return int(*value)
}

func int64Value(value *int64) int64 {
	if value == nil {
		return 0
	}
	return *value
}

func cloneBool(value *bool) *bool {
	if value == nil {
		return nil
	}
	out := *value
	return &out
}

func cloneTime(value *time.Time) *time.Time {
	if value == nil {
		return nil
	}
	out := value.UTC()
	return &out
}

func stringPtr(value string) *string {
	return &value
}

func stringPtrOrNil(value string) *string {
	if value == "" {
		return nil
	}
	return &value
}

func cloneString(value *string) *string {
	if value == nil {
		return nil
	}
	out := *value
	return &out
}

func cloneInt64(value *int64) *int64 {
	if value == nil {
		return nil
	}
	out := *value
	return &out
}

type remoteTaskDataArtifact struct {
	runtime *Runtime
	jobKey  jobdb.JobKey
	stored  runtimeapi.StoredArtifact

	mu      sync.Mutex
	ordinal *int64
}

func newRemoteTaskDataArtifact(runtime *Runtime, jobKey jobdb.JobKey, ordinal *int64, stored runtimeapi.StoredArtifact) jobdb.Artifact {
	return &remoteTaskDataArtifact{
		runtime: runtime,
		jobKey:  jobKey,
		stored: runtimeapi.StoredArtifact{
			Name:   stored.Name,
			Digest: stored.Digest,
			Size:   stored.Size,
		},
		ordinal: cloneInt64(ordinal),
	}
}

func (a *remoteTaskDataArtifact) Name() string { return a.stored.Name }

func (a *remoteTaskDataArtifact) Size() int64 { return a.stored.Size }

func (a *remoteTaskDataArtifact) Sha256(context.Context) (string, error) {
	return a.stored.Digest, nil
}

func (a *remoteTaskDataArtifact) WriteTo(ctx context.Context, w io.Writer) error {
	rc, err := a.Open()
	if err != nil {
		return err
	}
	defer rc.Close()
	_, err = io.Copy(w, rc)
	return err
}

func (a *remoteTaskDataArtifact) SaveToFile(ctx context.Context, path string) error {
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

func (a *remoteTaskDataArtifact) Bytes(ctx context.Context) ([]byte, error) {
	rc, err := a.Open()
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	return io.ReadAll(rc)
}

func (a *remoteTaskDataArtifact) Open() (io.ReadCloser, error) {
	ref, err := a.ref(context.Background())
	if err != nil {
		return nil, err
	}
	reader, err := a.runtime.OpenArtifact(context.Background(), ref)
	if err != nil {
		return nil, err
	}
	return reader.Open()
}

func (a *remoteTaskDataArtifact) ArtifactKey() (jobdb.ArtifactKey, error) {
	ref, err := a.ref(context.Background())
	if err != nil {
		return jobdb.ArtifactKey{}, err
	}
	return jobdb.ArtifactKey{
		JobId:       ref.JobKey.JobId,
		TaskOrdinal: ref.Ordinal,
		Name:        ref.Name,
		SizeBytes:   a.stored.Size,
	}, nil
}

func (a *remoteTaskDataArtifact) Cleanup() error { return nil }

func (a *remoteTaskDataArtifact) ref(ctx context.Context) (jobdb.ArtifactRef, error) {
	ordinal, err := a.resolveOrdinal(ctx)
	if err != nil {
		return jobdb.ArtifactRef{}, err
	}
	return jobdb.ArtifactRef{
		JobKey:  a.jobKey,
		Ordinal: ordinal,
		Name:    a.stored.Name,
		Digest:  a.stored.Digest,
	}, nil
}

func (a *remoteTaskDataArtifact) resolveOrdinal(ctx context.Context) (int64, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.ordinal != nil {
		return *a.ordinal, nil
	}
	chapters, err := a.runtime.ListChapters(ctx, jobdb.ListChaptersRequest{
		JobKey:       a.jobKey,
		StartOrdinal: 0,
	})
	if err != nil {
		return 0, err
	}
	for i := len(chapters) - 1; i >= 0; i-- {
		for _, artifact := range chapters[i].Artifacts {
			if artifact.Name != a.stored.Name {
				continue
			}
			if a.stored.Digest != "" && artifact.Digest != a.stored.Digest {
				continue
			}
			value := chapters[i].Ordinal
			a.ordinal = &value
			return value, nil
		}
	}
	return 0, jobdb.ErrChapterNotFound
}

func decodeJobFailedAppError(payload jobdb.AppErrorPayload) (error, bool) {
	if !jobFailedMarked(payload.Attrs) {
		return nil, false
	}

	switch attrString(payload.Attrs, jobFailedKindAttrKey) {
	case jobdb.TaskErrorKindTimeout:
		after := jobdb.Duration(0)
		if raw := attrString(payload.Attrs, jobFailedAfterAttrKey); raw != "" {
			if parsed, err := time.ParseDuration(raw); err == nil {
				after = jobdb.Duration(parsed)
			}
		}
		return &jobdb.JobFailedError{Cause: &jobdb.TimeoutError{Payload: jobdb.TimeoutPayload{
			Scope:     attrString(payload.Attrs, jobFailedScopeAttrKey),
			After:     after,
			Retryable: attrBool(payload.Attrs, jobFailedRetryableAttrKey),
			InputRef:  payload.InputRef,
			Component: attrString(payload.Attrs, jobFailedComponentAttrKey),
			Code:      attrString(payload.Attrs, jobFailedCodeAttrKey),
			Message:   payload.Message,
		}}}, true
	case jobdb.TaskErrorKindSystem:
		return &jobdb.JobFailedError{Cause: &jobdb.SystemError{Payload: jobdb.SystemErrorPayload{
			Message:    payload.Message,
			Component:  attrString(payload.Attrs, jobFailedComponentAttrKey),
			Code:       attrString(payload.Attrs, jobFailedCodeAttrKey),
			Retryable:  attrBool(payload.Attrs, jobFailedRetryableAttrKey),
			InputRef:   payload.InputRef,
			Stacktrace: append([]string(nil), payload.Stacktrace...),
		}}}, true
	default:
		return &jobdb.JobFailedError{Cause: &jobdb.AppError{Payload: jobdb.AppErrorPayload{
			Message:    payload.Message,
			Level:      payload.Level,
			Attrs:      stripJobFailedAttrs(payload.Attrs),
			InputRef:   payload.InputRef,
			Stacktrace: append([]string(nil), payload.Stacktrace...),
		}}}, true
	}
}

func jobFailedMarked(attrs map[string]interface{}) bool {
	if attrs == nil {
		return false
	}
	value, ok := attrs[jobFailedAttrKey].(bool)
	return ok && value
}

func stripJobFailedAttrs(attrs map[string]interface{}) map[string]interface{} {
	if len(attrs) == 0 {
		return nil
	}
	out := make(map[string]interface{}, len(attrs))
	for key, value := range attrs {
		switch key {
		case jobFailedAttrKey, jobFailedKindAttrKey, jobFailedCodeAttrKey, jobFailedComponentAttrKey, jobFailedRetryableAttrKey, jobFailedScopeAttrKey, jobFailedAfterAttrKey:
			continue
		default:
			out[key] = value
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

func attrString(attrs map[string]interface{}, key string) string {
	if attrs == nil {
		return ""
	}
	value, _ := attrs[key].(string)
	return value
}

func attrBool(attrs map[string]interface{}, key string) bool {
	if attrs == nil {
		return false
	}
	value, _ := attrs[key].(bool)
	return value
}
