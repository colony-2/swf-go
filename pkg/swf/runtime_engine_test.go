package swf

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"testing"
	"time"
)

type fakeWorkflowRuntime struct {
	startReq   StartJobRequest
	restartReq RestartJobRequest
	cancelReq  CancelJobRequest
	statusReq  JobKey
	resultReq  JobKey
	jobRunReq  GetJobRunRequest
	listReq    ListJobsRequest
	findArgs   struct {
		jobType   string
		taskType  string
		tenantIDs []string
	}
	waitingKey    JobKey
	chapterRef    ChapterRef
	artifactRef   ArtifactRef
	startHandle   JobHandle
	restartHandle JobHandle
	statusResp    JobStatus
	resultResp    TaskData
	jobRunResp    GetJobRunResponse
	listResp      ListJobsResponse
	findResp      []TaskHandle
	waitingResp   TaskHandle
	chapterResp   StoredChapter
	artifactBytes []byte
}

func (r *fakeWorkflowRuntime) StartJob(ctx context.Context, req StartJobRequest) (JobHandle, error) {
	r.startReq = req
	return r.startHandle, nil
}

func (r *fakeWorkflowRuntime) RestartJob(ctx context.Context, req RestartJobRequest) (JobHandle, error) {
	r.restartReq = req
	return r.restartHandle, nil
}

func (r *fakeWorkflowRuntime) CancelJob(ctx context.Context, req CancelJobRequest) error {
	r.cancelReq = req
	return nil
}

func (r *fakeWorkflowRuntime) PollWork(ctx context.Context, req PollWorkRequest) ([]ExecutionLease, error) {
	return nil, nil
}

func (r *fakeWorkflowRuntime) CheckJobStatus(ctx context.Context, jobKey JobKey) (JobStatus, error) {
	r.statusReq = jobKey
	return r.statusResp, nil
}

func (r *fakeWorkflowRuntime) GetJobResult(ctx context.Context, jobKey JobKey) (TaskData, error) {
	r.resultReq = jobKey
	return r.resultResp, nil
}

func (r *fakeWorkflowRuntime) GetJobRun(ctx context.Context, req GetJobRunRequest) (GetJobRunResponse, error) {
	r.jobRunReq = req
	return r.jobRunResp, nil
}

func (r *fakeWorkflowRuntime) ListJobs(ctx context.Context, req ListJobsRequest) (ListJobsResponse, error) {
	r.listReq = req
	return r.listResp, nil
}

func (r *fakeWorkflowRuntime) GetChapter(ctx context.Context, ref ChapterRef) (StoredChapter, error) {
	r.chapterRef = ref
	return r.chapterResp, nil
}

func (r *fakeWorkflowRuntime) PutChapter(ctx context.Context, req PutChapterRequest) error {
	return nil
}

func (r *fakeWorkflowRuntime) OpenArtifact(ctx context.Context, ref ArtifactRef) (ArtifactReader, error) {
	r.artifactRef = ref
	return fakeArtifactReader{
		name: ref.Name,
		data: append([]byte(nil), r.artifactBytes...),
	}, nil
}

func (r *fakeWorkflowRuntime) PutArtifacts(ctx context.Context, req PutArtifactsRequest) ([]StoredArtifact, error) {
	return nil, nil
}

func (r *fakeWorkflowRuntime) FindTasksWaitingForCapability(ctx context.Context, jobType string, taskType string, tenantIds []string) ([]TaskHandle, error) {
	r.findArgs.jobType = jobType
	r.findArgs.taskType = taskType
	r.findArgs.tenantIDs = append([]string(nil), tenantIds...)
	return r.findResp, nil
}

func (r *fakeWorkflowRuntime) GetWaitingTask(ctx context.Context, key JobKey) (TaskHandle, error) {
	r.waitingKey = key
	return r.waitingResp, nil
}

type fakeArtifactReader struct {
	name string
	data []byte
}

func (r fakeArtifactReader) Open() (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(r.data)), nil
}

func (r fakeArtifactReader) Size() int64 { return int64(len(r.data)) }

func (r fakeArtifactReader) Name() string { return r.name }

type fakeTaskHandle struct {
	key JobKey
}

func (h fakeTaskHandle) JobKey() JobKey                                  { return h.key }
func (h fakeTaskHandle) Data() (TaskData, error)                         { return NewTaskData(map[string]any{"ok": true}) }
func (h fakeTaskHandle) Finish(ctx context.Context, data TaskData) error { return nil }
func (h fakeTaskHandle) TaskOrdinalToComplete() int64                    { return 1 }
func (h fakeTaskHandle) TaskType() string                                { return "task" }
func (h fakeTaskHandle) CreatedAt() time.Time                            { return time.Now().UTC() }
func (h fakeTaskHandle) Metadata() json.RawMessage                       { return json.RawMessage(`{}`) }

type fakeJobWorker struct{}

func (fakeJobWorker) Name() string { return "fake-job" }

func (fakeJobWorker) Run(JobContext, JobData) (JobData, error) {
	return NewTaskData(map[string]any{"ok": true})
}

func TestBuildEngineWithWorkflowRuntime(t *testing.T) {
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	engine, err := NewEngineBuilder().
		WithRuntime(&fakeWorkflowRuntime{}).
		WithLogger(logger).
		WithMaxActive(17).
		WithAwaitRecycleThreshold(9 * time.Second).
		PlusWorkers(fakeJobWorker{}).
		BuildEngine()
	if err != nil {
		t.Fatalf("build engine: %v", err)
	}
	if engine == nil {
		t.Fatal("expected engine")
	}
}

func TestRuntimeEngineDelegatesLifecycleMethodsToRuntime(t *testing.T) {
	runtime := &fakeWorkflowRuntime{
		startHandle:   JobHandle{JobKey: JobKey{TenantId: "tenant-a", JobId: "job-a"}},
		restartHandle: JobHandle{JobKey: JobKey{TenantId: "tenant-b", JobId: "job-b"}},
		statusResp:    JobStatusCompleted,
		resultResp:    NewTaskDataOrPanic(map[string]int{"value": 42}),
		jobRunResp:    GetJobRunResponse{Job: JobRunSummary{JobKey: JobKey{TenantId: "tenant-c", JobId: "job-c"}}},
		listResp:      ListJobsResponse{Jobs: []JobSummary{{JobKey: JobKey{TenantId: "tenant-d", JobId: "job-d"}}}},
	}
	engine, err := NewEngineBuilder().WithRuntime(runtime).PlusWorkers(fakeJobWorker{}).BuildEngine()
	if err != nil {
		t.Fatalf("build engine: %v", err)
	}

	startKey, err := engine.StartJob(context.Background(), StartJob{
		TenantId: "tenant-a",
		JobType:  "type-a",
		Data:     NewTaskDataOrPanic(map[string]int{"value": 1}),
	})
	if err != nil {
		t.Fatalf("start job: %v", err)
	}
	if startKey != runtime.startHandle.JobKey {
		t.Fatalf("unexpected start key %+v", startKey)
	}
	if runtime.startReq.Job.JobType != "type-a" || runtime.startReq.RequestTime.IsZero() {
		t.Fatalf("unexpected start request %+v", runtime.startReq)
	}

	restartKey, err := engine.RestartJob(context.Background(), RestartJob{
		PriorJobKey:    JobKey{TenantId: "tenant-z", JobId: "prior"},
		LastStepToKeep: 3,
	})
	if err != nil {
		t.Fatalf("restart job: %v", err)
	}
	if restartKey != runtime.restartHandle.JobKey {
		t.Fatalf("unexpected restart key %+v", restartKey)
	}

	jobKey := JobKey{TenantId: "tenant-status", JobId: "job-status"}
	status, err := engine.CheckJobStatus(context.Background(), jobKey)
	if err != nil {
		t.Fatalf("check status: %v", err)
	}
	if status != JobStatusCompleted || runtime.statusReq != jobKey {
		t.Fatalf("unexpected status %q req=%+v", status, runtime.statusReq)
	}

	result, err := engine.GetJobResult(context.Background(), jobKey)
	if err != nil {
		t.Fatalf("get result: %v", err)
	}
	payload := map[string]int{}
	if err := json.Unmarshal(result.GetDataOrPanic(), &payload); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if payload["value"] != 42 {
		t.Fatalf("unexpected result payload %+v", payload)
	}

	if err := engine.CancelJob(context.Background(), CancelJob{JobKey: jobKey, Reason: "reason"}); err != nil {
		t.Fatalf("cancel job: %v", err)
	}
	if runtime.cancelReq.JobKey != jobKey || runtime.cancelReq.Reason != "reason" {
		t.Fatalf("unexpected cancel request %+v", runtime.cancelReq)
	}

	jobRunReq := GetJobRunRequest{JobKey: JobKey{TenantId: "tenant-run", JobId: "job-run"}}
	if _, err := engine.GetJobRun(context.Background(), jobRunReq); err != nil {
		t.Fatalf("get job run: %v", err)
	}
	if runtime.jobRunReq.JobKey != jobRunReq.JobKey {
		t.Fatalf("unexpected job run request %+v", runtime.jobRunReq)
	}

	listReq := ListJobsRequest{TenantIds: []string{"tenant-d"}}
	if _, err := engine.ListJobs(context.Background(), listReq); err != nil {
		t.Fatalf("list jobs: %v", err)
	}
	if len(runtime.listReq.TenantIds) != 1 || runtime.listReq.TenantIds[0] != "tenant-d" {
		t.Fatalf("unexpected list request %+v", runtime.listReq)
	}
}

func TestRuntimeEngineDelegatesWaitingTaskMethodsToRuntime(t *testing.T) {
	waitKey := JobKey{TenantId: "tenant-w", JobId: "job-w"}
	expected := fakeTaskHandle{key: waitKey}
	runtime := &fakeWorkflowRuntime{
		findResp:    []TaskHandle{expected},
		waitingResp: expected,
	}
	engine, err := NewEngineBuilder().WithRuntime(runtime).PlusWorkers(fakeJobWorker{}).BuildEngine()
	if err != nil {
		t.Fatalf("build engine: %v", err)
	}

	handles, err := engine.FindTasksWaitingForCapability(context.Background(), "job", "task", []string{"tenant-x"})
	if err != nil {
		t.Fatalf("find tasks: %v", err)
	}
	if len(handles) != 1 || handles[0].JobKey() != waitKey {
		t.Fatalf("unexpected handles %+v", handles)
	}
	if runtime.findArgs.jobType != "job" || runtime.findArgs.taskType != "task" {
		t.Fatalf("unexpected find args %+v", runtime.findArgs)
	}

	handle, err := engine.GetWaitingTask(context.Background(), waitKey)
	if err != nil {
		t.Fatalf("get waiting task: %v", err)
	}
	if handle.JobKey() != waitKey || runtime.waitingKey != waitKey {
		t.Fatalf("unexpected waiting task %+v req=%+v", handle.JobKey(), runtime.waitingKey)
	}
}

func TestRuntimeEngineLoadsArtifactsThroughRuntimeStorage(t *testing.T) {
	runtime := &fakeWorkflowRuntime{
		chapterResp: StoredChapter{
			Ordinal: 7,
			Artifacts: []StoredArtifact{
				{Name: "artifact.txt", Digest: "digest-1", Size: 8},
			},
		},
		artifactBytes: []byte("artifact"),
	}
	engine, err := NewEngineBuilder().WithRuntime(runtime).PlusWorkers(fakeJobWorker{}).BuildEngine()
	if err != nil {
		t.Fatalf("build engine: %v", err)
	}

	key := ArtifactKey{JobId: "job-artifact", TaskOrdinal: 7, Name: "artifact.txt", SizeBytes: 8}
	artifact, err := engine.GetArtifact("tenant-artifact", key)
	if err != nil {
		t.Fatalf("get artifact: %v", err)
	}
	if runtime.chapterRef.JobKey.JobId != "job-artifact" || runtime.chapterRef.Ordinal != 7 {
		t.Fatalf("unexpected chapter ref %+v", runtime.chapterRef)
	}
	data, err := artifact.Bytes(context.Background())
	if err != nil {
		t.Fatalf("read artifact: %v", err)
	}
	if string(data) != "artifact" {
		t.Fatalf("unexpected artifact bytes %q", string(data))
	}
	if runtime.artifactRef.Name != "artifact.txt" || runtime.artifactRef.Ordinal != 7 {
		t.Fatalf("unexpected artifact ref %+v", runtime.artifactRef)
	}
}
