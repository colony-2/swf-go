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
	startReq      SubmitJobRequest
	restartReq    SubmitRestartJobRequest
	cancelReq     CancelJobRequest
	completeReq   CompleteTaskIfWaitingRequest
	jobReq        JobKey
	listReq       ListJobsRequest
	chapterRef    ChapterRef
	artifactRef   ArtifactRef
	startHandle   JobHandle
	restartHandle JobHandle
	jobResp       JobInfo
	jobRunResp    GetJobRunResponse
	listResp      ListJobsResponse
	chapterResp   StoredChapter
	chaptersResp  []StoredChapter
	artifactBytes []byte
}

func (r *fakeWorkflowRuntime) SubmitJob(ctx context.Context, req SubmitJobRequest) (JobHandle, error) {
	r.startReq = req
	return r.startHandle, nil
}

func (r *fakeWorkflowRuntime) SubmitRestartJob(ctx context.Context, req SubmitRestartJobRequest) (JobHandle, error) {
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

func (r *fakeWorkflowRuntime) GetJobLease(ctx context.Context, req GetJobLeaseRequest) (ExecutionLease, error) {
	return nil, nil
}

func (r *fakeWorkflowRuntime) CompleteTaskIfWaiting(ctx context.Context, req CompleteTaskIfWaitingRequest) error {
	r.completeReq = req
	return nil
}

func (r *fakeWorkflowRuntime) GetJob(ctx context.Context, jobKey JobKey) (JobInfo, error) {
	r.jobReq = jobKey
	return r.jobResp, nil
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

func (r *fakeWorkflowRuntime) ListChapters(ctx context.Context, req ListChaptersRequest) ([]StoredChapter, error) {
	return append([]StoredChapter(nil), r.chaptersResp...), nil
}

func (r *fakeWorkflowRuntime) OpenArtifact(ctx context.Context, ref ArtifactRef) (ArtifactReader, error) {
	r.artifactRef = ref
	return fakeArtifactReader{
		name: ref.Name,
		data: append([]byte(nil), r.artifactBytes...),
	}, nil
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
		jobResp:       JobInfo{Status: JobStatusCompleted, Data: NewTaskDataOrPanic(map[string]int{"value": 42})},
		listResp: ListJobsResponse{Jobs: []JobSummary{
			{JobKey: JobKey{TenantId: "tenant-d", JobId: "job-d"}},
			{JobKey: JobKey{TenantId: "tenant-run", JobId: "job-run"}, Status: JobStatusCompleted, JobType: "job-run", CreatedAt: time.Unix(100, 0).UTC()},
		}},
		chaptersResp: []StoredChapter{
			{
				Ordinal:     0,
				TaskType:    "job-run",
				ChapterType: chapterTypeJobStart,
				PayloadKind: payloadKindApp,
				InputHash:   "hash-0",
				CreatedAt:   time.Unix(100, 0).UTC(),
				Metadata:    json.RawMessage(`{"version":1,"ordinal":0,"task_type":"job-run","worker_id":"worker-1","created_at":"1970-01-01T00:01:40Z","input_hash":"hash-0","attempt":1}`),
				Data:        json.RawMessage(`{"input":true}`),
			},
			{
				Ordinal:     1,
				TaskType:    "job-run",
				ChapterType: chapterTypeJobAttemptOutcome,
				PayloadKind: payloadKindApp,
				InputHash:   "hash-0",
				CreatedAt:   time.Unix(101, 0).UTC(),
				Metadata:    json.RawMessage(`{"version":1,"ordinal":1,"task_type":"job-run","worker_id":"worker-1","created_at":"1970-01-01T00:01:41Z","input_hash":"hash-0","attempt":1,"input_ref":{"ordinal":0,"hash":"hash-0"}}`),
				Data:        json.RawMessage(`{"ok":true}`),
			},
		},
	}
	engine, err := NewEngineBuilder().WithRuntime(runtime).PlusWorkers(fakeJobWorker{}).BuildEngine()
	if err != nil {
		t.Fatalf("build engine: %v", err)
	}

	startKey, err := engine.SubmitJob(context.Background(), SubmitJob{
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

	restartKey, err := engine.SubmitRestartJob(context.Background(), SubmitRestartJob{
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
	job, err := engine.GetJob(context.Background(), jobKey)
	if err != nil {
		t.Fatalf("get job: %v", err)
	}
	if job.Status != JobStatusCompleted || runtime.jobReq != jobKey {
		t.Fatalf("unexpected job %+v req=%+v", job, runtime.jobReq)
	}

	if job.Data == nil {
		t.Fatal("expected job data")
	}
	payload := map[string]int{}
	if err := json.Unmarshal(job.Data.GetDataOrPanic(), &payload); err != nil {
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
	jobRunResp, err := engine.GetJobRun(context.Background(), jobRunReq)
	if err != nil {
		t.Fatalf("get job run: %v", err)
	}
	if len(runtime.listReq.TenantIds) == 0 || runtime.listReq.TenantIds[0] != jobRunReq.JobKey.TenantId {
		t.Fatalf("unexpected get job run list request %+v", runtime.listReq)
	}
	if jobRunResp.Job.JobKey != jobRunReq.JobKey || jobRunResp.Job.JobType != "job-run" {
		t.Fatalf("unexpected job run response %+v", jobRunResp.Job)
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
	runtime := &fakeWorkflowRuntime{
		listResp: ListJobsResponse{Jobs: []JobSummary{{
			JobKey:            waitKey,
			JobType:           "job",
			NextNeed:          strPtr("job:task"),
			Status:            JobStatusReady,
			CreatedAt:         time.Unix(100, 0).UTC(),
			Metadata:          json.RawMessage(`{"ok":true}`),
			TaskWaitInput:     int64Ptr(1),
			TaskWaitOutput:    int64Ptr(2),
			TaskWaitInputHash: strPtr("hash-1"),
			TaskWaitNext:      strPtr("job"),
		}}},
		chapterResp: StoredChapter{
			Ordinal:     1,
			TaskType:    "task",
			ChapterType: chapterTypeTaskAttemptOutcome,
			PayloadKind: payloadKindApp,
			CreatedAt:   time.Unix(99, 0).UTC(),
			Data:        json.RawMessage(`{"value":1}`),
		},
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
	if len(runtime.listReq.JobTasks) != 1 || runtime.listReq.JobTasks[0].JobType != "job" || runtime.listReq.JobTasks[0].TaskType != "task" {
		t.Fatalf("unexpected find request %+v", runtime.listReq)
	}
	if _, err := handles[0].Data(); err != nil {
		t.Fatalf("load waiting task data: %v", err)
	}
	if runtime.chapterRef.JobKey != waitKey || runtime.chapterRef.Ordinal != 1 {
		t.Fatalf("unexpected chapter ref %+v", runtime.chapterRef)
	}
	if err := handles[0].Finish(context.Background(), NewTaskDataOrPanic(map[string]int{"value": 2})); err != nil {
		t.Fatalf("finish waiting task: %v", err)
	}
	if runtime.completeReq.JobKey != waitKey || runtime.completeReq.Capability != "job:task" || runtime.completeReq.ResumeNeed != "job" {
		t.Fatalf("unexpected complete request %+v", runtime.completeReq)
	}

	handle, err := engine.GetWaitingTask(context.Background(), waitKey)
	if err != nil {
		t.Fatalf("get waiting task: %v", err)
	}
	if handle.JobKey() != waitKey || len(runtime.listReq.JobKeys) != 1 || runtime.listReq.JobKeys[0] != waitKey {
		t.Fatalf("unexpected waiting task %+v req=%+v", handle.JobKey(), runtime.listReq)
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
