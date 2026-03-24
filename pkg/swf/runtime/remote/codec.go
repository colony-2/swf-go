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

	"github.com/colony-2/swf-go/pkg/swf"
	"github.com/colony-2/swf-go/pkg/swf/internal/runtimeapi"
)

const (
	jobFailedAttrKey          = "_swf_job_failed"
	jobFailedKindAttrKey      = "_swf_job_failed_kind"
	jobFailedCodeAttrKey      = "_swf_job_failed_code"
	jobFailedComponentAttrKey = "_swf_job_failed_component"
	jobFailedRetryableAttrKey = "_swf_job_failed_retryable"
	jobFailedScopeAttrKey     = "_swf_job_failed_scope"
	jobFailedAfterAttrKey     = "_swf_job_failed_after"
)

type jobInfoTaskData struct {
	taskData swf.TaskData
	err      error
}

func (d *jobInfoTaskData) GetData() (swf.Data, error) {
	if d.taskData == nil {
		return nil, d.err
	}
	data, err := d.taskData.GetData()
	if err != nil {
		return data, err
	}
	return data, d.err
}

func (d *jobInfoTaskData) GetDataOrPanic() swf.Data {
	data, err := d.GetData()
	if err != nil {
		panic(err)
	}
	return data
}

func (d *jobInfoTaskData) GetArtifacts() ([]swf.Artifact, error) {
	if d.taskData == nil {
		return nil, d.err
	}
	return d.taskData.GetArtifacts()
}

func (d *jobInfoTaskData) TaskDataResult() (swf.TaskData, error) {
	return d.taskData, d.err
}

func toAPIJobHandle(handle swf.JobHandle) runtimeapi.JobHandle {
	return runtimeapi.JobHandle{
		JobKey: toAPIJobKey(handle.JobKey),
	}
}

func fromAPIJobHandle(handle runtimeapi.JobHandle) swf.JobHandle {
	return swf.JobHandle{
		JobKey: fromAPIJobKey(handle.JobKey),
	}
}

func toAPIJobKey(key swf.JobKey) runtimeapi.JobKey {
	return runtimeapi.JobKey{
		TenantId: key.TenantId,
		JobId:    key.JobId,
	}
}

func fromAPIJobKey(key runtimeapi.JobKey) swf.JobKey {
	return swf.JobKey{
		TenantId: key.TenantId,
		JobId:    key.JobId,
	}
}

func marshalJSONValueOptional(raw json.RawMessage) (interface{}, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	var value interface{}
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil, err
	}
	return value, nil
}

func marshalJSONValueRequired(raw json.RawMessage) (interface{}, error) {
	if len(raw) == 0 {
		return nil, nil
	}
	var value interface{}
	if err := json.Unmarshal(raw, &value); err != nil {
		return nil, err
	}
	return value, nil
}

func unmarshalJSONValueOptional(value interface{}) (json.RawMessage, error) {
	if value == nil {
		return nil, nil
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	return json.RawMessage(raw), nil
}

func unmarshalJSONValueRequired(value interface{}) (json.RawMessage, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	return json.RawMessage(raw), nil
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

func toAPIDurationValue(value *swf.Duration) *string {
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

func fromAPIDurationValue(value *string) (*swf.Duration, error) {
	if value == nil || *value == "" {
		return nil, nil
	}
	parsed, err := time.ParseDuration(*value)
	if err != nil {
		return nil, err
	}
	d := swf.Duration(parsed)
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

func runPolicyToAPI(policy swf.RunPolicy) (*map[string]interface{}, error) {
	raw, err := json.Marshal(policy)
	if err != nil {
		return nil, err
	}
	var out map[string]interface{}
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, err
	}
	if len(out) == 0 {
		return nil, nil
	}
	return &out, nil
}

func runPolicyFromAPI(value *map[string]interface{}) (swf.RunPolicy, error) {
	if value == nil {
		return swf.RunPolicy{}, nil
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return swf.RunPolicy{}, err
	}
	var policy swf.RunPolicy
	if err := json.Unmarshal(raw, &policy); err != nil {
		return swf.RunPolicy{}, err
	}
	return policy, nil
}

func metadataPredicatesToAPI(filter swf.MetadataFilter) (*[]runtimeapi.MetadataPredicate, error) {
	predicates, err := swf.MetadataPredicates(filter)
	if err != nil {
		return nil, err
	}
	if len(predicates) == 0 {
		return nil, nil
	}
	out := make([]runtimeapi.MetadataPredicate, 0, len(predicates))
	for _, predicate := range predicates {
		values := make([]interface{}, len(predicate.Values))
		copy(values, predicate.Values)
		out = append(out, runtimeapi.MetadataPredicate{
			Path:   append([]string(nil), predicate.Path...),
			Values: values,
		})
	}
	return &out, nil
}

func metadataFilterFromAPI(predicates *[]runtimeapi.MetadataPredicate) (swf.MetadataFilter, error) {
	if predicates == nil || len(*predicates) == 0 {
		return nil, nil
	}
	filter := swf.Metadata()
	for _, predicate := range *predicates {
		if len(predicate.Path) != 1 || predicate.Path[0] == "" {
			return nil, fmt.Errorf("metadata predicate path must contain exactly one segment")
		}
		if len(predicate.Values) == 0 {
			return nil, fmt.Errorf("metadata predicate values are required")
		}
		var fieldFilter swf.MetadataFilter
		for idx, value := range predicate.Values {
			eq, err := swf.Metadata().EqualFilter(swf.FieldName(predicate.Path[0]), value)
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

func taskDataToAPIWrite(ctx context.Context, data swf.TaskData) (runtimeapi.TaskDataWrite, error) {
	if data == nil {
		return runtimeapi.TaskDataWrite{}, fmt.Errorf("task data is required")
	}
	raw, err := data.GetData()
	if err != nil {
		return runtimeapi.TaskDataWrite{}, err
	}
	value, err := marshalJSONValueRequired(raw)
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

func taskDataFromAPIWrite(write runtimeapi.TaskDataWrite) (swf.TaskData, error) {
	raw, err := unmarshalJSONValueRequired(write.Data)
	if err != nil {
		return nil, err
	}
	artifacts := make([]swf.Artifact, 0, len(write.Artifacts))
	for _, artifact := range write.Artifacts {
		artifacts = append(artifacts, swf.NewArtifactFromBytes(artifact.Name, append([]byte(nil), artifact.ContentBase64...)))
	}
	return &swf.SimpleTaskData{
		Data:      raw,
		Artifacts: artifacts,
	}, nil
}

func taskDataToAPIStored(ctx context.Context, data swf.TaskData, payloadErr error) (*runtimeapi.StoredTaskData, error) {
	if data == nil {
		return nil, nil
	}
	raw, err := data.GetData()
	if err != nil {
		return nil, err
	}
	value, err := marshalJSONValueRequired(raw)
	if err != nil {
		return nil, err
	}
	artifacts, err := storedArtifactsToAPI(ctx, data)
	if err != nil {
		return nil, err
	}
	return &runtimeapi.StoredTaskData{
		Artifacts: artifacts,
		Data:      value,
		PayloadKind: runtimeapi.PayloadKind(
			payloadKindFromTaskData(data, payloadErr),
		),
	}, nil
}

func taskDataFromAPIStored(runtime *Runtime, jobKey swf.JobKey, data runtimeapi.StoredTaskData) (swf.TaskData, error) {
	artifacts := make([]swf.Artifact, 0, len(data.Artifacts))
	for _, artifact := range data.Artifacts {
		artifacts = append(artifacts, newRemoteTaskDataArtifact(runtime, jobKey, nil, artifact))
	}
	return taskDataFromPayload(artifacts, data.PayloadKind, data.Data)
}

func toAPIStoredChapter(ctx context.Context, chapter swf.StoredChapter) (runtimeapi.StoredChapter, error) {
	dataValue, err := marshalJSONValueRequired(chapter.Data)
	if err != nil {
		return runtimeapi.StoredChapter{}, err
	}
	metadataValue, err := marshalJSONValueOptional(chapter.Metadata)
	if err != nil {
		return runtimeapi.StoredChapter{}, err
	}
	artifacts := make([]runtimeapi.StoredArtifact, 0, len(chapter.Artifacts))
	for _, artifact := range chapter.Artifacts {
		artifacts = append(artifacts, runtimeapi.StoredArtifact{
			Name:   artifact.Name,
			Digest: artifact.Digest,
			Size:   artifact.Size,
		})
	}
	out := runtimeapi.StoredChapter{
		Artifacts:   artifacts,
		ChapterType: runtimeapi.ChapterType(chapter.ChapterType),
		CreatedAt:   chapter.CreatedAt,
		Data:        dataValue,
		InputHash:   stringPtrOrNil(chapter.InputHash),
		Metadata:    metadataValue,
		Ordinal:     chapter.Ordinal,
		PayloadKind: runtimeapi.PayloadKind(chapter.PayloadKind),
	}
	if chapter.TaskType != "" {
		out.TaskType = stringPtr(chapter.TaskType)
	}
	return out, nil
}

func fromAPIStoredChapter(chapter runtimeapi.StoredChapter) (swf.StoredChapter, error) {
	data, err := unmarshalJSONValueRequired(chapter.Data)
	if err != nil {
		return swf.StoredChapter{}, err
	}
	metadata, err := unmarshalJSONValueOptional(chapter.Metadata)
	if err != nil {
		return swf.StoredChapter{}, err
	}
	out := swf.StoredChapter{
		Ordinal:     chapter.Ordinal,
		ChapterType: string(chapter.ChapterType),
		PayloadKind: string(chapter.PayloadKind),
		CreatedAt:   chapter.CreatedAt,
		Data:        data,
		Metadata:    metadata,
	}
	if chapter.InputHash != nil {
		out.InputHash = *chapter.InputHash
	}
	if chapter.TaskType != nil {
		out.TaskType = *chapter.TaskType
	}
	for _, artifact := range chapter.Artifacts {
		out.Artifacts = append(out.Artifacts, swf.StoredArtifact{
			Name:   artifact.Name,
			Digest: artifact.Digest,
			Size:   artifact.Size,
		})
	}
	return out, nil
}

func writableChapterToRuntimeChapter(chapter runtimeapi.WritableChapter) (swf.StoredChapter, []swf.ArtifactUpload, error) {
	data, err := unmarshalJSONValueRequired(chapter.Data)
	if err != nil {
		return swf.StoredChapter{}, nil, err
	}
	metadata, err := unmarshalJSONValueOptional(chapter.Metadata)
	if err != nil {
		return swf.StoredChapter{}, nil, err
	}
	out := swf.StoredChapter{
		Ordinal:     chapter.Ordinal,
		ChapterType: string(chapter.ChapterType),
		PayloadKind: string(chapter.PayloadKind),
		CreatedAt:   chapter.CreatedAt,
		Data:        data,
		Metadata:    metadata,
	}
	if chapter.InputHash != nil {
		out.InputHash = *chapter.InputHash
	}
	if chapter.TaskType != nil {
		out.TaskType = *chapter.TaskType
	}
	uploads := make([]swf.ArtifactUpload, 0, len(chapter.Artifacts))
	for _, artifact := range chapter.Artifacts {
		content := append([]byte(nil), artifact.ContentBase64...)
		out.Artifacts = append(out.Artifacts, swf.StoredArtifact{
			Name:   artifact.Name,
			Size:   artifact.Size,
			Digest: digestBytes(content),
		})
		uploads = append(uploads, swf.ArtifactUpload{
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

func runtimeChapterToWritable(ctx context.Context, chapter swf.StoredChapter, uploads []swf.ArtifactUpload) (runtimeapi.WritableChapter, error) {
	dataValue, err := marshalJSONValueRequired(chapter.Data)
	if err != nil {
		return runtimeapi.WritableChapter{}, err
	}
	metadataValue, err := marshalJSONValueOptional(chapter.Metadata)
	if err != nil {
		return runtimeapi.WritableChapter{}, err
	}
	artifacts := make([]runtimeapi.ArtifactWrite, 0, len(uploads))
	for _, upload := range uploads {
		rc, err := upload.Open()
		if err != nil {
			return runtimeapi.WritableChapter{}, err
		}
		content, readErr := io.ReadAll(rc)
		_ = rc.Close()
		if readErr != nil {
			return runtimeapi.WritableChapter{}, readErr
		}
		artifacts = append(artifacts, runtimeapi.ArtifactWrite{
			Name:          upload.Name,
			Size:          upload.Size,
			ContentBase64: content,
		})
	}
	out := runtimeapi.WritableChapter{
		Artifacts:   artifacts,
		ChapterType: runtimeapi.ChapterType(chapter.ChapterType),
		CreatedAt:   chapter.CreatedAt,
		Data:        dataValue,
		InputHash:   stringPtrOrNil(chapter.InputHash),
		Metadata:    metadataValue,
		Ordinal:     chapter.Ordinal,
		PayloadKind: runtimeapi.PayloadKind(chapter.PayloadKind),
	}
	if chapter.TaskType != "" {
		out.TaskType = stringPtr(chapter.TaskType)
	}
	return out, nil
}

func jobInfoToAPI(ctx context.Context, info swf.JobInfo) (runtimeapi.JobInfo, error) {
	out := runtimeapi.JobInfo{
		Status: runtimeapi.JobStatus(info.Status),
	}
	taskData, payloadErr := swf.ExtractTaskDataResult(info.Data)
	if errors.Is(payloadErr, swf.ErrJobNotComplete) && taskData == nil {
		return out, nil
	}
	stored, err := taskDataToAPIStored(ctx, taskData, payloadErr)
	if err != nil {
		return runtimeapi.JobInfo{}, err
	}
	out.Data = stored
	return out, nil
}

func jobInfoFromAPI(runtime *Runtime, jobKey swf.JobKey, info runtimeapi.JobInfo) (swf.JobInfo, error) {
	out := swf.JobInfo{
		Status: swf.JobStatus(info.Status),
		Data:   &jobInfoTaskData{err: swf.ErrJobNotComplete},
	}
	if info.Data == nil {
		return out, nil
	}
	taskData, payloadErr := taskDataFromAPIStored(runtime, jobKey, *info.Data)
	if payloadErr != nil && taskData == nil {
		return swf.JobInfo{}, payloadErr
	}
	out.Data = &jobInfoTaskData{taskData: taskData, err: payloadErr}
	return out, nil
}

func jobSummaryToAPI(summary swf.JobSummary) (runtimeapi.JobSummary, error) {
	payload, err := marshalJSONValueOptional(summary.Payload)
	if err != nil {
		return runtimeapi.JobSummary{}, err
	}
	metadata, err := marshalJSONValueOptional(summary.Metadata)
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
		SingletonKey:      cloneString(summary.SingletonKey),
		Status:            runtimeapi.JobStatus(summary.Status),
		TaskWaitInput:     cloneInt64(summary.TaskWaitInput),
		TaskWaitInputHash: cloneString(summary.TaskWaitInputHash),
		TaskWaitNext:      cloneString(summary.TaskWaitNext),
		TaskWaitOutput:    cloneInt64(summary.TaskWaitOutput),
		WaitFor:           append([]string(nil), summary.WaitFor...),
	}
	return out, nil
}

func jobSummaryFromAPI(summary runtimeapi.JobSummary) (swf.JobSummary, error) {
	payload, err := unmarshalJSONValueOptional(summary.Payload)
	if err != nil {
		return swf.JobSummary{}, err
	}
	metadata, err := unmarshalJSONValueOptional(summary.Metadata)
	if err != nil {
		return swf.JobSummary{}, err
	}
	return swf.JobSummary{
		JobKey:            fromAPIJobKey(summary.JobKey),
		Status:            swf.JobStatus(summary.Status),
		JobType:           summary.JobType,
		NextNeed:          cloneString(summary.NextNeed),
		SingletonKey:      cloneString(summary.SingletonKey),
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

func artifactsToAPIWrites(ctx context.Context, data swf.TaskData) ([]runtimeapi.ArtifactWrite, error) {
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

func storedArtifactsToAPI(ctx context.Context, data swf.TaskData) ([]runtimeapi.StoredArtifact, error) {
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

func taskDataFromPayload(artifacts []swf.Artifact, payloadKind runtimeapi.PayloadKind, value interface{}) (swf.TaskData, error) {
	raw, err := unmarshalJSONValueRequired(value)
	if err != nil {
		return nil, err
	}
	td := &swf.EnvelopedTaskData{
		SimpleTaskData: swf.SimpleTaskData{
			Data:      raw,
			Artifacts: artifacts,
		},
		Kind: string(payloadKind),
	}

	switch payloadKind {
	case runtimeapi.App:
		return td, nil
	case runtimeapi.Timeout:
		var payload swf.TimeoutPayload
		if err := json.Unmarshal(raw, &payload); err != nil {
			return td, err
		}
		return td, &swf.TimeoutError{Payload: payload}
	case runtimeapi.AppError:
		var payload swf.AppErrorPayload
		if err := json.Unmarshal(raw, &payload); err != nil {
			return td, err
		}
		if err, ok := decodeJobFailedAppError(payload); ok {
			return td, err
		}
		return td, &swf.AppError{Payload: payload}
	case runtimeapi.SystemError:
		var payload swf.SystemErrorPayload
		if err := json.Unmarshal(raw, &payload); err != nil {
			return td, err
		}
		return td, &swf.SystemError{Payload: payload}
	default:
		return td, fmt.Errorf("unsupported payload kind %q", payloadKind)
	}
}

func payloadKindFromTaskData(data swf.TaskData, payloadErr error) string {
	if enveloped, ok := data.(*swf.EnvelopedTaskData); ok && enveloped.Kind != "" {
		return enveloped.Kind
	}
	var timeoutErr *swf.TimeoutError
	if errors.As(payloadErr, &timeoutErr) {
		return string(runtimeapi.Timeout)
	}
	var systemErr *swf.SystemError
	if errors.As(payloadErr, &systemErr) {
		return string(runtimeapi.SystemError)
	}
	return string(runtimeapi.App)
}

func digestBytes(content []byte) string {
	sum := sha256.Sum256(content)
	return hex.EncodeToString(sum[:])
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
	jobKey  swf.JobKey
	stored  runtimeapi.StoredArtifact

	mu      sync.Mutex
	ordinal *int64
}

func newRemoteTaskDataArtifact(runtime *Runtime, jobKey swf.JobKey, ordinal *int64, stored runtimeapi.StoredArtifact) swf.Artifact {
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

func (a *remoteTaskDataArtifact) ArtifactKey() (swf.ArtifactKey, error) {
	ref, err := a.ref(context.Background())
	if err != nil {
		return swf.ArtifactKey{}, err
	}
	return swf.ArtifactKey{
		JobId:       ref.JobKey.JobId,
		TaskOrdinal: ref.Ordinal,
		Name:        ref.Name,
		SizeBytes:   a.stored.Size,
	}, nil
}

func (a *remoteTaskDataArtifact) Cleanup() error { return nil }

func (a *remoteTaskDataArtifact) ref(ctx context.Context) (swf.ArtifactRef, error) {
	ordinal, err := a.resolveOrdinal(ctx)
	if err != nil {
		return swf.ArtifactRef{}, err
	}
	return swf.ArtifactRef{
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
	chapters, err := a.runtime.ListChapters(ctx, swf.ListChaptersRequest{
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
	return 0, swf.ErrChapterNotFound
}

func decodeJobFailedAppError(payload swf.AppErrorPayload) (error, bool) {
	if !jobFailedMarked(payload.Attrs) {
		return nil, false
	}

	switch attrString(payload.Attrs, jobFailedKindAttrKey) {
	case swf.TaskErrorKindTimeout:
		after := swf.Duration(0)
		if raw := attrString(payload.Attrs, jobFailedAfterAttrKey); raw != "" {
			if parsed, err := time.ParseDuration(raw); err == nil {
				after = swf.Duration(parsed)
			}
		}
		return &swf.JobFailedError{Cause: &swf.TimeoutError{Payload: swf.TimeoutPayload{
			Scope:     attrString(payload.Attrs, jobFailedScopeAttrKey),
			After:     after,
			Retryable: attrBool(payload.Attrs, jobFailedRetryableAttrKey),
			InputRef:  payload.InputRef,
			Component: attrString(payload.Attrs, jobFailedComponentAttrKey),
			Code:      attrString(payload.Attrs, jobFailedCodeAttrKey),
			Message:   payload.Message,
		}}}, true
	case swf.TaskErrorKindSystem:
		return &swf.JobFailedError{Cause: &swf.SystemError{Payload: swf.SystemErrorPayload{
			Message:    payload.Message,
			Component:  attrString(payload.Attrs, jobFailedComponentAttrKey),
			Code:       attrString(payload.Attrs, jobFailedCodeAttrKey),
			Retryable:  attrBool(payload.Attrs, jobFailedRetryableAttrKey),
			InputRef:   payload.InputRef,
			Stacktrace: append([]string(nil), payload.Stacktrace...),
		}}}, true
	default:
		return &swf.JobFailedError{Cause: &swf.AppError{Payload: swf.AppErrorPayload{
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
