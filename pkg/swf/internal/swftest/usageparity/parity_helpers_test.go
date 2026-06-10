package usageparity_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/colony-2/swf-go/pkg/swf"
	"github.com/colony-2/swf-go/pkg/swf/internal/runtimecodec"
	swftest "github.com/colony-2/swf-go/pkg/swf/internal/swftest"
)

type parityMode string

const (
	engineMode  parityMode = "engine"
	runtimeMode parityMode = "runtime"
)

type jobSurface interface {
	SubmitJob(ctx context.Context, start swf.SubmitJob) (swf.JobKey, error)
	SubmitRestartJob(ctx context.Context, restart swf.SubmitRestartJob) (swf.JobKey, error)
	CancelJob(ctx context.Context, cancel swf.CancelJob) error
	GetJob(ctx context.Context, jobKey swf.JobKey) (swf.JobInfo, error)
	GetJobRun(ctx context.Context, req swf.GetJobRunRequest) (swf.GetJobRunResponse, error)
	ListJobs(ctx context.Context, req swf.ListJobsRequest) (swf.ListJobsResponse, error)
}

type engineSurface struct {
	engine swf.SWFEngine
}

func (s engineSurface) SubmitJob(ctx context.Context, start swf.SubmitJob) (swf.JobKey, error) {
	return s.engine.SubmitJob(ctx, start)
}

func (s engineSurface) SubmitRestartJob(ctx context.Context, restart swf.SubmitRestartJob) (swf.JobKey, error) {
	return s.engine.SubmitRestartJob(ctx, restart)
}

func (s engineSurface) CancelJob(ctx context.Context, cancel swf.CancelJob) error {
	return s.engine.CancelJob(ctx, cancel)
}

func (s engineSurface) GetJob(ctx context.Context, jobKey swf.JobKey) (swf.JobInfo, error) {
	return s.engine.GetJob(ctx, jobKey)
}

func (s engineSurface) GetJobRun(ctx context.Context, req swf.GetJobRunRequest) (swf.GetJobRunResponse, error) {
	return s.engine.GetJobRun(ctx, req)
}

func (s engineSurface) ListJobs(ctx context.Context, req swf.ListJobsRequest) (swf.ListJobsResponse, error) {
	return s.engine.ListJobs(ctx, req)
}

type runtimeSurface struct {
	runtime swf.WorkflowRuntime
	engine  swf.SWFEngine
}

func (s runtimeSurface) SubmitJob(ctx context.Context, start swf.SubmitJob) (swf.JobKey, error) {
	handle, err := s.runtime.SubmitJob(ctx, swf.SubmitJobRequest{
		Job:         start,
		RequestTime: time.Now().UTC(),
	})
	if err != nil {
		return swf.JobKey{}, err
	}
	return handle.JobKey, nil
}

func (s runtimeSurface) SubmitRestartJob(ctx context.Context, restart swf.SubmitRestartJob) (swf.JobKey, error) {
	handle, err := s.runtime.SubmitRestartJob(ctx, swf.SubmitRestartJobRequest{
		Job:         restart,
		RequestTime: time.Now().UTC(),
	})
	if err != nil {
		return swf.JobKey{}, err
	}
	return handle.JobKey, nil
}

func (s runtimeSurface) CancelJob(ctx context.Context, cancel swf.CancelJob) error {
	return s.runtime.CancelJob(ctx, swf.CancelJobRequest{
		JobKey: cancel.JobKey,
		Reason: cancel.Reason,
	})
}

func (s runtimeSurface) GetJob(ctx context.Context, jobKey swf.JobKey) (swf.JobInfo, error) {
	return s.runtime.GetJob(ctx, jobKey)
}

func (s runtimeSurface) GetJobRun(ctx context.Context, req swf.GetJobRunRequest) (swf.GetJobRunResponse, error) {
	return s.engine.GetJobRun(ctx, req)
}

func (s runtimeSurface) ListJobs(ctx context.Context, req swf.ListJobsRequest) (swf.ListJobsResponse, error) {
	return s.runtime.ListJobs(ctx, req)
}

type scenarioSubject struct {
	mode    parityMode
	built   *swftest.BuiltRuntimeHarness
	surface jobSurface
}

func (s scenarioSubject) SubmitJob(ctx context.Context, start swf.SubmitJob) (swf.JobKey, error) {
	if s.built != nil && s.built.WorkerTenantID != "" {
		start.TenantId = s.built.WorkerTenantID
	}
	return s.surface.SubmitJob(ctx, start)
}

func (s scenarioSubject) SubmitRestartJob(ctx context.Context, restart swf.SubmitRestartJob) (swf.JobKey, error) {
	return s.surface.SubmitRestartJob(ctx, restart)
}

func (s scenarioSubject) CancelJob(ctx context.Context, cancel swf.CancelJob) error {
	return s.surface.CancelJob(ctx, cancel)
}

func (s scenarioSubject) GetJob(ctx context.Context, jobKey swf.JobKey) (swf.JobInfo, error) {
	return s.surface.GetJob(ctx, jobKey)
}

func (s scenarioSubject) CheckJobStatus(ctx context.Context, jobKey swf.JobKey) (swf.JobStatus, error) {
	job, err := s.GetJob(ctx, jobKey)
	return job.Status, err
}

func (s scenarioSubject) GetJobResult(ctx context.Context, jobKey swf.JobKey) (swf.TaskData, error) {
	job, err := s.GetJob(ctx, jobKey)
	if err != nil {
		return nil, err
	}
	return swf.ExtractTaskDataResult(job.Data)
}

func (s scenarioSubject) GetJobRun(ctx context.Context, req swf.GetJobRunRequest) (swf.GetJobRunResponse, error) {
	return s.surface.GetJobRun(ctx, req)
}

func (s scenarioSubject) ListJobs(ctx context.Context, req swf.ListJobsRequest) (swf.ListJobsResponse, error) {
	return s.surface.ListJobs(ctx, req)
}

func (s scenarioSubject) WaitForStatus(t *testing.T, ctx context.Context, jobKey swf.JobKey, want swf.JobStatus) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		status, err := jobStatusForTest(s, ctx, jobKey)
		if err == nil && status == want {
			return
		}
		if err != nil && !errors.Is(err, swf.ErrJobNotFound) && !errors.Is(err, context.Canceled) {
			t.Fatalf("%s status check failed: %v", s.mode, err)
		}
		select {
		case <-ctx.Done():
			t.Fatalf("%s wait for status %s: %v", s.mode, want, ctx.Err())
		case <-time.After(20 * time.Millisecond):
		}
	}
	t.Fatalf("%s job %s did not reach status %s", s.mode, jobKey, want)
}

func (s scenarioSubject) Engine() swf.SWFEngine {
	return s.built.Engine
}

func (s scenarioSubject) Runtime() swf.WorkflowRuntime {
	return s.built.Runtime
}

func observeViaMode[T any](
	t *testing.T,
	harness swftest.RuntimeHarness,
	mode parityMode,
	workers []swf.WorkSet,
	run func(t *testing.T, ctx context.Context, subject scenarioSubject) T,
) T {
	t.Helper()
	built := harness.New(t, workers...)
	defer built.Shutdown(t)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	subject := scenarioSubject{
		mode:  mode,
		built: built,
	}
	switch mode {
	case engineMode:
		subject.surface = engineSurface{engine: built.Engine}
	case runtimeMode:
		subject.surface = runtimeSurface{runtime: built.Runtime, engine: built.Engine}
	default:
		t.Fatalf("unknown parity mode %q", mode)
	}

	return run(t, ctx, subject)
}

func compareAcrossModes[T any](
	t *testing.T,
	harness swftest.RuntimeHarness,
	workers []swf.WorkSet,
	run func(t *testing.T, ctx context.Context, subject scenarioSubject) T,
) {
	t.Helper()
	engineObs := observeViaMode(t, harness, engineMode, workers, run)
	runtimeObs := observeViaMode(t, harness, runtimeMode, workers, run)
	compareObservations(t, engineObs, runtimeObs)
}

func compareObservations[T any](t *testing.T, got, want T) {
	t.Helper()
	if reflect.DeepEqual(got, want) {
		return
	}
	gotJSON, _ := json.MarshalIndent(got, "", "  ")
	wantJSON, _ := json.MarshalIndent(want, "", "  ")
	t.Fatalf("observations differ\nengine:\n%s\nruntime:\n%s", gotJSON, wantJSON)
}

type normalizedTaskData struct {
	Data      string               `json:"data,omitempty"`
	Artifacts []normalizedArtifact `json:"artifacts,omitempty"`
}

type normalizedArtifact struct {
	Name   string `json:"name"`
	Size   int64  `json:"size"`
	Digest string `json:"digest,omitempty"`
	Bytes  string `json:"bytes,omitempty"`
}

type normalizedTaskIO struct {
	Data      string               `json:"data,omitempty"`
	Artifacts []normalizedArtifact `json:"artifacts,omitempty"`
}

type normalizedTaskOutcome struct {
	Status      string `json:"status,omitempty"`
	PayloadKind string `json:"payloadKind,omitempty"`
	ErrorKind   string `json:"errorKind,omitempty"`
	ErrorText   string `json:"errorText,omitempty"`
}

type normalizedTaskAttempt struct {
	Ordinal   int64                 `json:"ordinal"`
	Attempt   int                   `json:"attempt"`
	State     string                `json:"state,omitempty"`
	NextNeed  *string               `json:"nextNeed,omitempty"`
	WaitFor   []string              `json:"waitFor,omitempty"`
	Input     *normalizedTaskIO     `json:"input,omitempty"`
	Output    *normalizedTaskIO     `json:"output,omitempty"`
	Outcome   normalizedTaskOutcome `json:"outcome,omitempty"`
	Retryable *bool                 `json:"retryable,omitempty"`
}

type normalizedTaskRun struct {
	TaskType string                  `json:"taskType"`
	Attempts []normalizedTaskAttempt `json:"attempts"`
}

type normalizedJobAttempt struct {
	Ordinal int64                 `json:"ordinal"`
	Attempt int                   `json:"attempt"`
	Output  *normalizedTaskIO     `json:"output,omitempty"`
	Outcome normalizedTaskOutcome `json:"outcome,omitempty"`
	Tasks   []normalizedTaskRun   `json:"tasks,omitempty"`
}

type normalizedJobRun struct {
	JobKey    swf.JobKey             `json:"jobKey"`
	JobType   string                 `json:"jobType"`
	Status    swf.JobStatus          `json:"status"`
	Start     *normalizedTaskIO      `json:"start,omitempty"`
	Attempts  []normalizedJobAttempt `json:"attempts,omitempty"`
	OutputErr string                 `json:"outputErr,omitempty"`
}

type normalizedJobSummary struct {
	JobKey            swf.JobKey    `json:"jobKey"`
	Status            swf.JobStatus `json:"status"`
	JobType           string        `json:"jobType"`
	NextNeed          *string       `json:"nextNeed,omitempty"`
	WaitFor           []string      `json:"waitFor,omitempty"`
	CancelRequested   bool          `json:"cancelRequested,omitempty"`
	TaskWaitInput     *int64        `json:"taskWaitInput,omitempty"`
	TaskWaitOutput    *int64        `json:"taskWaitOutput,omitempty"`
	TaskWaitInputHash *string       `json:"taskWaitInputHash,omitempty"`
	TaskWaitNext      *string       `json:"taskWaitNext,omitempty"`
	Payload           string        `json:"payload,omitempty"`
	Metadata          string        `json:"metadata,omitempty"`
}

type normalizedStoredChapter struct {
	Ordinal     int64                `json:"ordinal"`
	TaskType    string               `json:"taskType,omitempty"`
	ChapterType string               `json:"chapterType,omitempty"`
	PayloadKind string               `json:"payloadKind,omitempty"`
	InputHash   string               `json:"inputHash,omitempty"`
	Metadata    string               `json:"metadata,omitempty"`
	Data        string               `json:"data,omitempty"`
	Artifacts   []swf.StoredArtifact `json:"artifacts,omitempty"`
}

func normalizeTaskDataResult(t *testing.T, data swf.TaskData) normalizedTaskData {
	t.Helper()
	if data == nil {
		return normalizedTaskData{}
	}
	raw, err := data.GetData()
	if err != nil {
		t.Fatalf("get task data: %v", err)
	}
	arts, err := data.GetArtifacts()
	if err != nil {
		t.Fatalf("get task artifacts: %v", err)
	}
	result := normalizedTaskData{
		Data: canonicalJSON(raw),
	}
	if len(arts) == 0 {
		return result
	}
	result.Artifacts = make([]normalizedArtifact, 0, len(arts))
	for _, art := range arts {
		result.Artifacts = append(result.Artifacts, normalizedArtifact{
			Name: art.Name(),
			Size: art.Size(),
		})
	}
	sort.Slice(result.Artifacts, func(i, j int) bool {
		if result.Artifacts[i].Name == result.Artifacts[j].Name {
			return result.Artifacts[i].Size < result.Artifacts[j].Size
		}
		return result.Artifacts[i].Name < result.Artifacts[j].Name
	})
	return result
}

func normalizeTaskIO(io *swf.TaskIO) *normalizedTaskIO {
	if io == nil {
		return nil
	}
	out := &normalizedTaskIO{
		Data: canonicalJSON(io.Data),
	}
	if len(io.Artifacts) == 0 {
		return out
	}
	out.Artifacts = make([]normalizedArtifact, 0, len(io.Artifacts))
	for _, art := range io.Artifacts {
		out.Artifacts = append(out.Artifacts, normalizedArtifact{
			Name:   art.Name,
			Size:   art.SizeBytes,
			Digest: art.Sha256,
		})
	}
	sort.Slice(out.Artifacts, func(i, j int) bool {
		if out.Artifacts[i].Name == out.Artifacts[j].Name {
			return out.Artifacts[i].Size < out.Artifacts[j].Size
		}
		return out.Artifacts[i].Name < out.Artifacts[j].Name
	})
	return out
}

func normalizeJobRun(t *testing.T, run swf.GetJobRunResponse, outputErr error) normalizedJobRun {
	t.Helper()
	out := normalizedJobRun{
		JobKey:    run.Job.JobKey,
		JobType:   run.Job.JobType,
		Status:    run.Job.Status,
		Start:     normalizeTaskIO(run.Start.Input),
		OutputErr: normalizeError(outputErr),
	}
	out.Attempts = make([]normalizedJobAttempt, 0, len(run.Attempts))
	for _, attempt := range run.Attempts {
		jobAttempt := normalizedJobAttempt{
			Ordinal: attempt.Ordinal,
			Attempt: attempt.Attempt,
			Output:  normalizeTaskIO(attempt.Output),
			Outcome: normalizeOutcome(attempt.Outcome),
			Tasks:   make([]normalizedTaskRun, 0, len(attempt.Tasks)),
		}
		for _, task := range attempt.Tasks {
			taskRun := normalizedTaskRun{
				TaskType: task.TaskType,
				Attempts: make([]normalizedTaskAttempt, 0, len(task.Attempts)),
			}
			for _, ta := range task.Attempts {
				taskAttempt := normalizedTaskAttempt{
					Ordinal:   ta.Ordinal,
					Attempt:   ta.Attempt,
					State:     ta.State,
					Input:     normalizeTaskIO(ta.Input),
					Output:    normalizeTaskIO(ta.Output),
					Outcome:   normalizeOutcome(ta.Outcome),
					Retryable: ta.Retryable,
				}
				if ta.Runtime != nil {
					taskAttempt.NextNeed = ta.Runtime.NextNeed
					taskAttempt.WaitFor = append([]string(nil), ta.Runtime.WaitFor...)
				}
				taskRun.Attempts = append(taskRun.Attempts, taskAttempt)
			}
			jobAttempt.Tasks = append(jobAttempt.Tasks, taskRun)
		}
		out.Attempts = append(out.Attempts, jobAttempt)
	}
	return out
}

func normalizeJobSummaries(jobs []swf.JobSummary) []normalizedJobSummary {
	if len(jobs) == 0 {
		return nil
	}
	out := make([]normalizedJobSummary, 0, len(jobs))
	for _, job := range jobs {
		item := normalizedJobSummary{
			JobKey:            job.JobKey,
			Status:            job.Status,
			JobType:           job.JobType,
			NextNeed:          cloneStringPtr(job.NextNeed),
			WaitFor:           append([]string(nil), job.WaitFor...),
			CancelRequested:   job.CancelRequested,
			TaskWaitInput:     cloneInt64Ptr(job.TaskWaitInput),
			TaskWaitOutput:    cloneInt64Ptr(job.TaskWaitOutput),
			TaskWaitInputHash: cloneStringPtr(job.TaskWaitInputHash),
			TaskWaitNext:      cloneStringPtr(job.TaskWaitNext),
			Payload:           canonicalJSON(job.Payload),
			Metadata:          canonicalJSON(job.Metadata),
		}
		out = append(out, item)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].JobKey.TenantId == out[j].JobKey.TenantId {
			return out[i].JobKey.JobId < out[j].JobKey.JobId
		}
		return out[i].JobKey.TenantId < out[j].JobKey.TenantId
	})
	return out
}

func appTaskAttemptChapterForParity(t testing.TB, ordinal int64, taskType string, inputHash string, data []byte, metadata json.RawMessage) swf.Chapter {
	t.Helper()
	chapterMetadata, err := runtimecodec.ChapterMetadataFromJSON(metadata)
	if err != nil {
		t.Fatalf("chapter metadata: %v", err)
	}
	return swf.Chapter{
		Ordinal:   ordinal,
		TaskType:  taskType,
		InputHash: inputHash,
		CreatedAt: time.Now().UTC(),
		Metadata:  chapterMetadata,
		Body: swf.TaskAttemptOutcomeChapter{Outcome: swf.ApplicationOutputOutcome{
			Output: swf.ApplicationOutputBytes{Data: append([]byte(nil), data...)},
		}},
	}
}

func normalizeStoredChapter(chapter swf.Chapter) normalizedStoredChapter {
	chapterType, payloadKind, data, err := runtimecodec.ChapterBodyToWire(chapter.Body)
	if err != nil {
		panic(err)
	}
	metadata, err := runtimecodec.ChapterMetadataToJSON(chapter.Metadata)
	if err != nil {
		panic(err)
	}
	out := normalizedStoredChapter{
		Ordinal:     chapter.Ordinal,
		TaskType:    chapter.TaskType,
		ChapterType: chapterType,
		PayloadKind: payloadKind,
		InputHash:   chapter.InputHash,
		Metadata:    canonicalChapterMetadata(metadata),
		Data:        canonicalJSON(data),
	}
	if len(chapter.Artifacts) == 0 {
		return out
	}
	out.Artifacts = append([]swf.StoredArtifact(nil), chapter.Artifacts...)
	sort.Slice(out.Artifacts, func(i, j int) bool {
		if out.Artifacts[i].Name == out.Artifacts[j].Name {
			return out.Artifacts[i].Digest < out.Artifacts[j].Digest
		}
		return out.Artifacts[i].Name < out.Artifacts[j].Name
	})
	return out
}

func canonicalChapterMetadata(raw json.RawMessage) string {
	if len(bytes.TrimSpace(raw)) == 0 {
		return ""
	}
	var decoded any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return canonicalJSON(raw)
	}
	stripVolatileChapterMetadata(decoded)
	normalized, err := json.Marshal(decoded)
	if err != nil {
		return canonicalJSON(raw)
	}
	return string(normalized)
}

func stripVolatileChapterMetadata(v any) {
	switch typed := v.(type) {
	case map[string]any:
		delete(typed, "created_at")
		delete(typed, "started_at")
		delete(typed, "finished_at")
		delete(typed, "worker_id")
		for _, child := range typed {
			stripVolatileChapterMetadata(child)
		}
	case []any:
		for _, child := range typed {
			stripVolatileChapterMetadata(child)
		}
	}
}

func normalizeOutcome(outcome swf.TaskOutcome) normalizedTaskOutcome {
	item := normalizedTaskOutcome{
		Status:      outcome.Status,
		PayloadKind: outcome.PayloadKind,
	}
	if outcome.Error != nil {
		item.ErrorKind = outcome.Error.Kind
		item.ErrorText = outcome.Error.Message
	}
	return item
}

func normalizeError(err error) string {
	switch {
	case err == nil:
		return ""
	case errors.Is(err, swf.ErrJobCancelled):
		return "ErrJobCancelled"
	case errors.Is(err, swf.ErrJobFailed):
		return "ErrJobFailed:" + err.Error()
	case errors.Is(err, swf.ErrJobNotComplete):
		return "ErrJobNotComplete"
	case errors.Is(err, swf.ErrJobNotFound):
		return "ErrJobNotFound"
	case errors.Is(err, swf.ErrWorkflowNotDeterministic):
		return "ErrWorkflowNotDeterministic"
	default:
		return err.Error()
	}
}

func canonicalJSON(raw json.RawMessage) string {
	if len(bytes.TrimSpace(raw)) == 0 {
		return ""
	}
	var decoded any
	if err := json.Unmarshal(raw, &decoded); err != nil {
		return strings.TrimSpace(string(raw))
	}
	normalized, err := json.Marshal(decoded)
	if err != nil {
		return strings.TrimSpace(string(raw))
	}
	return string(normalized)
}

func cloneInt64Ptr(v *int64) *int64 {
	if v == nil {
		return nil
	}
	out := *v
	return &out
}

func cloneStringPtr(v *string) *string {
	if v == nil {
		return nil
	}
	out := *v
	return &out
}

func mustReadRuntimeArtifactBytes(t *testing.T, ctx context.Context, runtime swf.WorkflowRuntime, ref swf.ArtifactRef) normalizedArtifact {
	t.Helper()
	reader, err := runtime.OpenArtifact(ctx, ref)
	if err != nil {
		t.Fatalf("open runtime artifact: %v", err)
	}
	rc, err := reader.Open()
	if err != nil {
		t.Fatalf("open runtime artifact reader: %v", err)
	}
	defer func() { _ = rc.Close() }()
	data, err := ioReadAll(rc)
	if err != nil {
		t.Fatalf("read runtime artifact: %v", err)
	}
	return normalizedArtifact{
		Name:   reader.Name(),
		Size:   reader.Size(),
		Digest: ref.Digest,
		Bytes:  string(data),
	}
}

func mustReadEngineArtifactBytes(t *testing.T, ctx context.Context, engine swf.SWFEngine, tenantID string, key swf.ArtifactKey) normalizedArtifact {
	t.Helper()
	art, err := engine.GetArtifact(tenantID, key)
	if err != nil {
		t.Fatalf("get engine artifact: %v", err)
	}
	data, err := art.Bytes(ctx)
	if err != nil {
		t.Fatalf("read engine artifact bytes: %v", err)
	}
	digest, err := art.Sha256(ctx)
	if err != nil {
		t.Fatalf("compute engine artifact digest: %v", err)
	}
	return normalizedArtifact{
		Name:   art.Name(),
		Size:   art.Size(),
		Digest: digest,
		Bytes:  string(data),
	}
}

func ioReadAll(rc io.Reader) ([]byte, error) {
	return io.ReadAll(rc)
}

func taskNumber(data swf.TaskData) (int, error) {
	if data == nil {
		return 0, errors.New("missing task data")
	}
	raw, err := data.GetData()
	if err != nil {
		return 0, err
	}
	payload := map[string]int{}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return 0, err
	}
	return payload["n"], nil
}

type passthroughJob struct {
	name string
}

func (j passthroughJob) Name() string { return j.name }
func (j passthroughJob) Run(_ swf.JobContext, data swf.JobData) (swf.JobData, error) {
	return data, nil
}

type pendingJob struct{}

func (pendingJob) Name() string { return "pending-job" }
func (pendingJob) Run(ctx swf.JobContext, data swf.JobData) (swf.JobData, error) {
	return ctx.DoTask(swf.RunPolicy{}, "pending-task", data)
}

type externalApprovalJob struct{}

func (externalApprovalJob) Name() string { return "external-approval-job" }
func (externalApprovalJob) Run(ctx swf.JobContext, data swf.JobData) (swf.JobData, error) {
	return ctx.DoTask(swf.RunPolicy{}, "approval", data)
}

type awaitChildJob struct {
	started chan<- struct{}
	release <-chan struct{}
}

func (awaitChildJob) Name() string { return "await-child" }
func (j awaitChildJob) Run(_ swf.JobContext, data swf.JobData) (swf.JobData, error) {
	if j.started != nil {
		close(j.started)
	}
	<-j.release
	return data, nil
}

type awaitParentJob struct {
	childJobID string
}

func (awaitParentJob) Name() string { return "await-parent" }
func (j awaitParentJob) Run(ctx swf.JobContext, data swf.JobData) (swf.JobData, error) {
	if err := ctx.AwaitJobs(j.childJobID); err != nil {
		return nil, err
	}
	return data, nil
}

type awaitDurationJob struct {
	name string
	wait time.Duration
}

func (j awaitDurationJob) Name() string { return j.name }
func (j awaitDurationJob) Run(ctx swf.JobContext, data swf.JobData) (swf.JobData, error) {
	if err := ctx.AwaitDuration(swf.Duration(j.wait)); err != nil {
		return nil, err
	}
	return data, nil
}

type retryJob struct {
	attempts int
}

func (retryJob) Name() string { return "retry-job" }
func (j *retryJob) Run(_ swf.JobContext, data swf.JobData) (swf.JobData, error) {
	j.attempts++
	if j.attempts == 1 {
		return nil, swf.AppError{Payload: swf.AppErrorPayload{Message: "retry me", Level: "error"}}
	}
	return data, nil
}

type retryTaskJob struct{}

func (retryTaskJob) Name() string { return "retry-task-job" }
func (retryTaskJob) Run(ctx swf.JobContext, data swf.JobData) (swf.JobData, error) {
	return ctx.DoTask(swf.RunPolicy{
		Retry: swf.RetryPolicy{
			MaximumAttempts:    3,
			BackoffCoefficient: 1,
		},
	}, "retry-task", data)
}

type retryTask struct {
	attempts int
}

func (retryTask) Name() string { return "retry-task" }
func (t *retryTask) Run(_ swf.TaskContext, data swf.TaskData) (swf.TaskData, error) {
	t.attempts++
	if t.attempts == 1 {
		return nil, swf.AppError{Payload: swf.AppErrorPayload{Message: "retry task", Level: "error"}}
	}
	return data, nil
}

type namedFailingJob struct {
	name    string
	message string
}

func (j namedFailingJob) Name() string { return j.name }
func (j namedFailingJob) Run(_ swf.JobContext, _ swf.JobData) (swf.JobData, error) {
	if j.message == "" {
		j.message = "intentional failure"
	}
	return nil, swf.AppError{Payload: swf.AppErrorPayload{Message: j.message, Level: "error"}}
}

type failingTaskJob struct {
	name string
	task string
}

func (j failingTaskJob) Name() string { return j.name }
func (j failingTaskJob) Run(ctx swf.JobContext, data swf.JobData) (swf.JobData, error) {
	return ctx.DoTask(swf.RunPolicy{}, j.task, data)
}

type namedFailingTask struct {
	name    string
	message string
}

func (t namedFailingTask) Name() string { return t.name }
func (t namedFailingTask) Run(_ swf.TaskContext, _ swf.TaskData) (swf.TaskData, error) {
	if t.message == "" {
		t.message = "intentional task failure"
	}
	return nil, swf.AppError{Payload: swf.AppErrorPayload{Message: t.message, Level: "error"}}
}

type taskErrorWithArtifactJob struct {
	name string
	task string
}

func (j taskErrorWithArtifactJob) Name() string { return j.name }
func (j taskErrorWithArtifactJob) Run(ctx swf.JobContext, data swf.JobData) (swf.JobData, error) {
	return ctx.DoTask(swf.RunPolicy{}, j.task, data)
}

type artifactFailingTask struct {
	name         string
	message      string
	artifactName string
	artifactData []byte
	output       string
}

func (t artifactFailingTask) Name() string { return t.name }
func (t artifactFailingTask) Run(_ swf.TaskContext, _ swf.TaskData) (swf.TaskData, error) {
	output := t.output
	if output == "" {
		output = `{"status":"failed"}`
	}
	return &swf.SimpleTaskData{
		Data:      []byte(output),
		Artifacts: []swf.Artifact{swf.NewArtifactFromBytes(t.artifactName, append([]byte(nil), t.artifactData...))},
	}, swf.AppError{Payload: swf.AppErrorPayload{Message: t.message, Level: "error"}}
}

type jobErrorWithArtifact struct {
	name         string
	message      string
	artifactName string
	artifactData []byte
	output       string
}

func (j jobErrorWithArtifact) Name() string { return j.name }
func (j jobErrorWithArtifact) Run(_ swf.JobContext, _ swf.JobData) (swf.JobData, error) {
	output := j.output
	if output == "" {
		output = `{"status":"job-failed"}`
	}
	return &swf.SimpleTaskData{
		Data:      []byte(output),
		Artifacts: []swf.Artifact{swf.NewArtifactFromBytes(j.artifactName, append([]byte(nil), j.artifactData...))},
	}, swf.AppError{Payload: swf.AppErrorPayload{Message: j.message, Level: "error"}}
}

type awaitTaskJob struct{}

func (awaitTaskJob) Name() string { return "await-task-parent" }
func (awaitTaskJob) Run(ctx swf.JobContext, data swf.JobData) (swf.JobData, error) {
	return ctx.DoTask(swf.RunPolicy{}, "await-task", data)
}

type awaitTaskWorker struct {
	childJobID string
}

func (awaitTaskWorker) Name() string { return "await-task" }
func (t awaitTaskWorker) Run(ctx swf.TaskContext, data swf.TaskData) (swf.TaskData, error) {
	if err := ctx.AwaitJobs(t.childJobID); err != nil {
		return nil, err
	}
	return data, nil
}

type transformPendingJob struct{}

func (transformPendingJob) Name() string { return "transform-pending-job" }
func (transformPendingJob) Run(ctx swf.JobContext, data swf.JobData) (swf.JobData, error) {
	first, err := ctx.DoTask(swf.RunPolicy{}, swftest.AddOneTaskName, data)
	if err != nil {
		return nil, err
	}
	n, err := taskNumber(first)
	if err != nil {
		return nil, err
	}
	return ctx.DoTask(swf.RunPolicy{}, "pending-task", swftest.NumberTaskData(n+100))
}

type cleanupArtifactJob struct {
	dir           string
	taskName      string
	jobFileName   string
	copyPrefix    string
	expectedNames []string
}

func (j cleanupArtifactJob) Name() string { return "cleanup-artifact-job" }
func (j cleanupArtifactJob) Run(ctx swf.JobContext, input swf.JobData) (swf.JobData, error) {
	taskOutput, err := ctx.DoTask(swf.RunPolicy{}, j.taskName, input)
	if err != nil {
		return nil, err
	}
	taskArtifacts, err := taskOutput.GetArtifacts()
	if err != nil {
		return nil, err
	}
	reuploaded := make([]swf.Artifact, 0, len(taskArtifacts))
	for _, art := range taskArtifacts {
		data, err := art.Bytes(context.Background())
		if err != nil {
			return nil, err
		}
		copyName := j.copyPrefix + art.Name()
		copyPath := filepath.Join(j.dir, copyName)
		if err := os.WriteFile(copyPath, data, 0644); err != nil {
			return nil, err
		}
		reuploaded = append(reuploaded, mustNewFileArtifact(copyPath, copyName))
	}
	jobPath := filepath.Join(j.dir, j.jobFileName)
	if err := os.WriteFile(jobPath, []byte("artifact:"+j.jobFileName), 0644); err != nil {
		return nil, err
	}
	raw, err := input.GetData()
	if err != nil {
		return nil, err
	}
	return &swf.SimpleTaskData{
		Data:      append([]byte(nil), raw...),
		Artifacts: append(reuploaded, mustNewFileArtifact(jobPath, j.jobFileName)),
	}, nil
}

type cleanupArtifactTask struct {
	dir       string
	fileNames []string
}

func (t cleanupArtifactTask) Name() string { return "cleanup-artifact-task" }
func (t cleanupArtifactTask) Run(_ swf.TaskContext, input swf.TaskData) (swf.TaskData, error) {
	artifacts := make([]swf.Artifact, 0, len(t.fileNames))
	for _, name := range t.fileNames {
		path := filepath.Join(t.dir, name)
		if err := os.WriteFile(path, []byte("artifact:"+name), 0644); err != nil {
			return nil, err
		}
		artifacts = append(artifacts, mustNewFileArtifact(path, name))
	}
	raw, err := input.GetData()
	if err != nil {
		return nil, err
	}
	return &swf.SimpleTaskData{
		Data:      append([]byte(nil), raw...),
		Artifacts: artifacts,
	}, nil
}

func mustNewFileArtifact(path, name string) swf.Artifact {
	pathCopy := path
	nameCopy := name
	return swf.NewArtifact(nameCopy, func() (io.ReadCloser, int64, error) {
		f, err := os.Open(pathCopy)
		if err != nil {
			return nil, 0, err
		}
		info, err := f.Stat()
		if err != nil {
			_ = f.Close()
			return nil, 0, err
		}
		return f, info.Size(), nil
	}, func() error {
		return os.Remove(pathCopy)
	})
}

type echoTask struct{}

func (echoTask) Name() string { return "echo" }
func (echoTask) Run(_ swf.TaskContext, data swf.TaskData) (swf.TaskData, error) {
	return data, nil
}

type singleEchoJob struct {
	runs *int
}

func (singleEchoJob) Name() string { return "single-echo" }
func (j singleEchoJob) Run(ctx swf.JobContext, data swf.JobData) (swf.JobData, error) {
	if j.runs != nil {
		*j.runs++
	}
	return ctx.DoTask(swf.RunPolicy{}, "echo", data)
}
