package jobdbtest

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	directruntime "github.com/colony-2/jobdb/pkg/jobdb/runtime/direct"
	remoteruntime "github.com/colony-2/jobdb/pkg/jobdb/runtime/remote"
	sqliteruntime "github.com/colony-2/jobdb/pkg/jobdb/runtime/sqlite"
	toyruntime "github.com/colony-2/jobdb/pkg/jobdb/runtime/toy"
	"github.com/colony-2/jobdb/pkg/workflow"
)

type RuntimeHarness struct {
	Name                   string
	SupportsLeases         bool
	SupportsRuntimeStorage bool
	StartsWorkerLoop       bool
	New                    func(t *testing.T, workers ...workflow.WorkSet) *BuiltRuntimeHarness
}

type BuiltRuntimeHarness struct {
	Name           string
	Runtime        jobdb.WorkflowRuntime
	Engine         workflow.Engine
	WorkerTenantID string

	cancel   context.CancelFunc
	runDone  <-chan struct{}
	shutdown func()
}

var externalHarnessSeq atomic.Uint64

func (h *BuiltRuntimeHarness) Shutdown(t *testing.T) {
	t.Helper()
	if h == nil {
		return
	}
	if h.cancel != nil {
		h.cancel()
	}
	if h.runDone != nil {
		select {
		case <-h.runDone:
		case <-time.After(5 * time.Second):
			t.Fatalf("%s engine did not stop after cancellation", h.Name)
		}
	}
	if h.shutdown != nil {
		h.shutdown()
	}
}

func BuiltInRuntimeHarnesses() []RuntimeHarness {
	harnesses := []RuntimeHarness{
		{
			Name:                   "toy",
			SupportsLeases:         false,
			SupportsRuntimeStorage: true,
			StartsWorkerLoop:       true,
			New:                    newToyHarness,
		},
		{
			Name:                   "sqlite",
			SupportsLeases:         true,
			SupportsRuntimeStorage: true,
			StartsWorkerLoop:       true,
			New:                    newSQLiteHarness,
		},
		{
			Name:                   "direct",
			SupportsLeases:         true,
			SupportsRuntimeStorage: true,
			StartsWorkerLoop:       true,
			New:                    newDirectHarness,
		},
	}
	if external, ok := externalRemoteRuntimeHarness(); ok {
		if externalOnlyHarnessesEnabled() {
			return []RuntimeHarness{external}
		}
		harnesses = append(harnesses, external)
	}
	return harnesses
}

func RemoteRuntimeHarnesses() []RuntimeHarness {
	harnesses := []RuntimeHarness{
		{
			Name:                   "remote-toy",
			SupportsLeases:         false,
			SupportsRuntimeStorage: true,
			StartsWorkerLoop:       true,
			New:                    newRemoteToyHarness,
		},
		{
			Name:                   "remote-sqlite",
			SupportsLeases:         true,
			SupportsRuntimeStorage: true,
			StartsWorkerLoop:       true,
			New:                    newRemoteSQLiteHarness,
		},
		{
			Name:                   "remote-direct",
			SupportsLeases:         true,
			SupportsRuntimeStorage: true,
			StartsWorkerLoop:       true,
			New:                    newRemoteDirectHarness,
		},
	}
	if external, ok := externalRemoteRuntimeHarness(); ok {
		if externalOnlyHarnessesEnabled() {
			return []RuntimeHarness{external}
		}
		harnesses = append(harnesses, external)
	}
	return harnesses
}

func MustWorkSet(t *testing.T, job workflow.JobWorker, tasks ...workflow.TaskWorker) workflow.WorkSet {
	t.Helper()
	ws, err := workflow.AsWorkSet(job, tasks...)
	if err != nil {
		t.Fatalf("build workset: %v", err)
	}
	return *ws
}

func MustWorkSetWithOptions(t *testing.T, job workflow.JobWorker, opts workflow.WorkRegistrationOptions, tasks ...workflow.TaskWorker) workflow.WorkSet {
	t.Helper()
	ws, err := workflow.AsWorkSetWithOptions(job, opts, tasks...)
	if err != nil {
		t.Fatalf("build workset: %v", err)
	}
	return *ws
}

func WaitForEngineStatus(t *testing.T, ctx context.Context, engine workflow.Engine, jobKey jobdb.JobKey, want jobdb.JobStatus) {
	t.Helper()
	waitForStatus(t, ctx, func(ctx context.Context) (jobdb.JobStatus, error) {
		job, err := engine.GetJob(ctx, jobKey)
		return job.Status, err
	}, want)
}

func WaitForRuntimeStatus(t *testing.T, ctx context.Context, runtime jobdb.WorkflowRuntime, jobKey jobdb.JobKey, want jobdb.JobStatus) {
	t.Helper()
	waitForStatus(t, ctx, func(ctx context.Context) (jobdb.JobStatus, error) {
		job, err := runtime.GetJob(ctx, jobKey)
		return job.Status, err
	}, want)
}

func WaitForTaskHandle(t *testing.T, ctx context.Context, engine workflow.Engine, jobType string, taskType string, tenantIDs []string) workflow.TaskHandle {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		handles, err := engine.FindTasksWaitingForCapability(ctx, jobType, taskType, tenantIDs)
		if err == nil && len(handles) > 0 {
			return handles[0]
		}
		if err != nil && !errors.Is(err, context.Canceled) {
			t.Fatalf("find waiting tasks: %v", err)
		}
		select {
		case <-ctx.Done():
			t.Fatalf("wait for task handle: %v", ctx.Err())
		case <-time.After(20 * time.Millisecond):
		}
	}
	t.Fatalf("timed out waiting for %s:%s task handle", jobType, taskType)
	return nil
}

func MustDecodeNumberTaskData(t *testing.T, data jobdb.TaskData) int {
	t.Helper()
	if data == nil {
		t.Fatal("missing task data")
	}
	raw, err := data.GetData()
	if err != nil {
		t.Fatalf("get task data: %v", err)
	}
	return decodeNumber(t, raw)
}

func MustDecodeNumberTaskIO(t *testing.T, data *jobdb.TaskIO) int {
	t.Helper()
	if data == nil {
		t.Fatal("missing task io")
	}
	return decodeNumber(t, data.Data)
}

func MustStartJobAsync(t *testing.T, engine workflow.Engine, start jobdb.SubmitJob) <-chan error {
	t.Helper()
	done := make(chan error, 1)
	go func() {
		_, err := engine.SubmitJob(context.Background(), start)
		done <- err
	}()
	return done
}

func ExpectJobTypeFromNextNeed(t *testing.T, nextNeed *string, wantJobType string) {
	t.Helper()
	if nextNeed == nil {
		t.Fatal("missing runtime next_need")
	}
	if got := jobdb.JobTypeFromNextNeed(*nextNeed); got != wantJobType {
		t.Fatalf("unexpected next_need job type: got %q want %q", got, wantJobType)
	}
}

func ExpectTaskSuffix(t *testing.T, value string, wantSuffix string) {
	t.Helper()
	if !strings.HasSuffix(value, wantSuffix) {
		t.Fatalf("unexpected task value %q, want suffix %q", value, wantSuffix)
	}
}

func NumberTaskData(n int) jobdb.TaskData {
	return jobdb.NewTaskDataOrPanic(map[string]int{"n": n})
}

const (
	SequenceJobName = "seq"
	AddOneTaskName  = "add"
	DoubleTaskName  = "double"
	MissingTaskName = "missing"
	FailingJobName  = "failing"
)

type SequenceJob struct {
	Steps []string
}

func (SequenceJob) Name() string { return SequenceJobName }

func (j SequenceJob) Run(ctx workflow.JobContext, data jobdb.JobData) (jobdb.JobData, error) {
	var (
		out jobdb.TaskData = data
		err error
	)
	for _, step := range j.Steps {
		out, err = ctx.DoTask(jobdb.RunPolicy{}, step, out)
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

type AddOneTask struct{}

func (AddOneTask) Name() string { return AddOneTaskName }

func (AddOneTask) Run(_ workflow.TaskContext, input jobdb.TaskData) (jobdb.TaskData, error) {
	n, err := numberFromTaskData(input)
	if err != nil {
		return nil, err
	}
	return NumberTaskData(n + 1), nil
}

type DoubleTask struct{}

func (DoubleTask) Name() string { return DoubleTaskName }

func (DoubleTask) Run(_ workflow.TaskContext, input jobdb.TaskData) (jobdb.TaskData, error) {
	n, err := numberFromTaskData(input)
	if err != nil {
		return nil, err
	}
	return NumberTaskData(n * 2), nil
}

type FailingJob struct{}

func (FailingJob) Name() string { return FailingJobName }

func (FailingJob) Run(_ workflow.JobContext, _ jobdb.JobData) (jobdb.JobData, error) {
	return nil, errors.New("intentional failure")
}

func decodeNumber(t *testing.T, raw json.RawMessage) int {
	t.Helper()
	payload := map[string]int{}
	if err := json.Unmarshal(raw, &payload); err != nil {
		t.Fatalf("decode numeric payload: %v", err)
	}
	return payload["n"]
}

func waitForStatus(t *testing.T, ctx context.Context, check func(context.Context) (jobdb.JobStatus, error), want jobdb.JobStatus) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		status, err := check(ctx)
		if err == nil && status == want {
			return
		}
		if err != nil && !errors.Is(err, jobdb.ErrJobNotFound) && !errors.Is(err, context.Canceled) {
			t.Fatalf("check status: %v", err)
		}
		select {
		case <-ctx.Done():
			t.Fatalf("wait for status: %v", ctx.Err())
		case <-time.After(20 * time.Millisecond):
		}
	}
	t.Fatalf("job did not reach status %s", want)
}

func newToyHarness(t *testing.T, workers ...workflow.WorkSet) *BuiltRuntimeHarness {
	t.Helper()
	runtime := toyruntime.New()
	return buildHarness(t, "toy", runtime, true, func() {}, workers...)
}

func newDirectHarness(t *testing.T, workers ...workflow.WorkSet) *BuiltRuntimeHarness {
	t.Helper()
	embedded, err := directruntime.StartEmbeddedRuntime(context.Background())
	if err != nil {
		t.Fatalf("start embedded direct runtime: %v", err)
	}
	return buildHarness(t, "direct", embedded.Runtime, true, embedded.Shutdown, workers...)
}

func newSQLiteHarness(t *testing.T, workers ...workflow.WorkSet) *BuiltRuntimeHarness {
	t.Helper()
	embedded, err := sqliteruntime.StartEmbeddedRuntime(context.Background())
	if err != nil {
		t.Fatalf("start embedded sqlite runtime: %v", err)
	}
	return buildHarness(t, "sqlite", embedded.Runtime, true, embedded.Shutdown, workers...)
}

func newRemoteToyHarness(t *testing.T, workers ...workflow.WorkSet) *BuiltRuntimeHarness {
	t.Helper()
	underlying := toyruntime.New()
	server := httptest.NewServer(remoteruntime.NewServer(underlying))
	runtime, err := remoteruntime.New(server.URL, server.Client())
	if err != nil {
		server.Close()
		t.Fatalf("build remote toy runtime: %v", err)
	}
	return buildHarness(t, "remote-toy", runtime, true, server.Close, workers...)
}

func newRemoteDirectHarness(t *testing.T, workers ...workflow.WorkSet) *BuiltRuntimeHarness {
	t.Helper()
	embedded, err := directruntime.StartEmbeddedRuntime(context.Background())
	if err != nil {
		t.Fatalf("start embedded direct runtime: %v", err)
	}
	server := httptest.NewServer(remoteruntime.NewServer(embedded.Runtime))
	runtime, err := remoteruntime.New(server.URL, server.Client())
	if err != nil {
		server.Close()
		embedded.Shutdown()
		t.Fatalf("build remote direct runtime: %v", err)
	}
	shutdown := func() {
		server.Close()
		embedded.Shutdown()
	}
	return buildHarness(t, "remote-direct", runtime, true, shutdown, workers...)
}

func newRemoteSQLiteHarness(t *testing.T, workers ...workflow.WorkSet) *BuiltRuntimeHarness {
	t.Helper()
	embedded, err := sqliteruntime.StartEmbeddedRuntime(context.Background())
	if err != nil {
		t.Fatalf("start embedded sqlite runtime: %v", err)
	}
	server := httptest.NewServer(remoteruntime.NewServer(embedded.Runtime))
	runtime, err := remoteruntime.New(server.URL, server.Client())
	if err != nil {
		server.Close()
		embedded.Shutdown()
		t.Fatalf("build remote sqlite runtime: %v", err)
	}
	shutdown := func() {
		server.Close()
		embedded.Shutdown()
	}
	return buildHarness(t, "remote-sqlite", runtime, true, shutdown, workers...)
}

func newExternalRemoteHarness(t *testing.T, workers ...workflow.WorkSet) *BuiltRuntimeHarness {
	t.Helper()
	baseURL, name := externalRemoteRuntimeConfig()
	runtime, err := remoteruntime.New(baseURL, &http.Client{Timeout: 30 * time.Second})
	if err != nil {
		t.Fatalf("build %s runtime: %v", name, err)
	}
	prefix := fmt.Sprintf("__jobdbtest_external_%d__", externalHarnessSeq.Add(1))
	return buildHarness(t, name, newTenantNamespacedRuntime(runtime, prefix), true, func() {}, workers...)
}

func buildHarness(t *testing.T, name string, runtime jobdb.WorkflowRuntime, startLoop bool, shutdown func(), workers ...workflow.WorkSet) *BuiltRuntimeHarness {
	t.Helper()
	builder := workflow.NewEngineBuilder().WithRuntime(runtime)
	workerTenantID := "tenant-worker-" + name
	if len(workers) > 0 {
		builder.WithWorkerTenantId(workerTenantID)
	}
	for _, ws := range workers {
		tasks := make([]workflow.TaskWorker, 0, len(ws.TaskWorkers))
		for _, task := range ws.TaskWorkers {
			tasks = append(tasks, task)
		}
		builder.PlusWorkersWithOptions(ws.JobWorker, ws.Options, tasks...)
	}
	engine, err := builder.BuildEngine()
	if err != nil {
		shutdown()
		t.Fatalf("build %s engine: %v", name, err)
	}

	var (
		cancel  context.CancelFunc
		runDone <-chan struct{}
	)
	if startLoop && len(workers) > 0 {
		runCtx, runCancel := context.WithCancel(context.Background())
		cancel = runCancel
		done := make(chan struct{})
		runDone = done
		go func() {
			defer close(done)
			engine.Run(runCtx)
		}()
	}

	return &BuiltRuntimeHarness{
		Name:           name,
		Runtime:        runtime,
		Engine:         engine,
		WorkerTenantID: workerTenantID,
		cancel:         cancel,
		runDone:        runDone,
		shutdown:       shutdown,
	}
}

func numberFromTaskData(data jobdb.TaskData) (int, error) {
	if data == nil {
		return 0, fmt.Errorf("missing task data")
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

func externalRemoteRuntimeHarness() (RuntimeHarness, bool) {
	baseURL, name := externalRemoteRuntimeConfig()
	if baseURL == "" {
		return RuntimeHarness{}, false
	}
	return RuntimeHarness{
		Name:                   name,
		SupportsLeases:         true,
		SupportsRuntimeStorage: true,
		StartsWorkerLoop:       true,
		New:                    newExternalRemoteHarness,
	}, true
}

func externalRemoteRuntimeConfig() (baseURL string, name string) {
	baseURL = strings.TrimSpace(os.Getenv("JOBDB_EXTERNAL_REMOTE_BASE_URL"))
	name = strings.TrimSpace(os.Getenv("JOBDB_EXTERNAL_REMOTE_NAME"))
	if name == "" {
		name = "remote-worker"
	}
	return baseURL, name
}

func externalOnlyHarnessesEnabled() bool {
	switch strings.TrimSpace(strings.ToLower(os.Getenv("JOBDB_EXTERNAL_REMOTE_ONLY"))) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

type tenantNamespacedRuntime struct {
	runtime jobdb.WorkflowRuntime
	prefix  string
}

func newTenantNamespacedRuntime(runtime jobdb.WorkflowRuntime, prefix string) jobdb.WorkflowRuntime {
	return &tenantNamespacedRuntime{runtime: runtime, prefix: prefix}
}

func (r *tenantNamespacedRuntime) SubmitJob(ctx context.Context, req jobdb.SubmitJobRequest) (jobdb.JobHandle, error) {
	req.Job.TenantId = r.prefixTenant(req.Job.TenantId)
	handle, err := r.runtime.SubmitJob(ctx, req)
	if err != nil {
		return jobdb.JobHandle{}, err
	}
	handle.JobKey = r.stripJobKey(handle.JobKey)
	return handle, nil
}

func (r *tenantNamespacedRuntime) SubmitRestartJob(ctx context.Context, req jobdb.SubmitRestartJobRequest) (jobdb.JobHandle, error) {
	req.Job.PriorJobKey = r.prefixJobKey(req.Job.PriorJobKey)
	handle, err := r.runtime.SubmitRestartJob(ctx, req)
	if err != nil {
		return jobdb.JobHandle{}, err
	}
	handle.JobKey = r.stripJobKey(handle.JobKey)
	return handle, nil
}

func (r *tenantNamespacedRuntime) CancelJob(ctx context.Context, req jobdb.CancelJobRequest) error {
	req.JobKey = r.prefixJobKey(req.JobKey)
	return r.runtime.CancelJob(ctx, req)
}

func (r *tenantNamespacedRuntime) PollWork(ctx context.Context, req jobdb.PollWorkRequest) ([]jobdb.ExecutionLease, error) {
	if req.TenantId != "" {
		req.TenantId = r.prefixTenant(req.TenantId)
	}
	leases, err := r.runtime.PollWork(ctx, req)
	if err != nil {
		return nil, err
	}
	return r.wrapLeases(leases), nil
}

func (r *tenantNamespacedRuntime) GetJobLease(ctx context.Context, req jobdb.GetJobLeaseRequest) (jobdb.ExecutionLease, error) {
	req.JobKey = r.prefixJobKey(req.JobKey)
	lease, err := r.runtime.GetJobLease(ctx, req)
	if err != nil || lease == nil {
		return lease, err
	}
	return &tenantNamespacedLease{lease: lease, runtime: r}, nil
}

func (r *tenantNamespacedRuntime) CompleteTaskIfWaiting(ctx context.Context, req jobdb.CompleteTaskIfWaitingRequest) error {
	req.JobKey = r.prefixJobKey(req.JobKey)
	return r.runtime.CompleteTaskIfWaiting(ctx, req)
}

func (r *tenantNamespacedRuntime) GetJob(ctx context.Context, jobKey jobdb.JobKey) (jobdb.JobInfo, error) {
	return r.runtime.GetJob(ctx, r.prefixJobKey(jobKey))
}

func (r *tenantNamespacedRuntime) ListJobs(ctx context.Context, req jobdb.ListJobsRequest) (jobdb.ListJobsResponse, error) {
	req.TenantIds = r.prefixTenantIDs(req.TenantIds)
	if len(req.JobKeys) > 0 {
		jobKeys := make([]jobdb.JobKey, 0, len(req.JobKeys))
		for _, jobKey := range req.JobKeys {
			jobKeys = append(jobKeys, r.prefixJobKey(jobKey))
		}
		req.JobKeys = jobKeys
	}
	resp, err := r.runtime.ListJobs(ctx, req)
	if err != nil {
		return jobdb.ListJobsResponse{}, err
	}
	for i := range resp.Jobs {
		resp.Jobs[i].JobKey = r.stripJobKey(resp.Jobs[i].JobKey)
	}
	return resp, nil
}

func (r *tenantNamespacedRuntime) UpsertSchedule(ctx context.Context, req jobdb.UpsertScheduleRequest) (jobdb.ScheduleInfo, error) {
	req.TenantId = r.prefixTenant(req.TenantId)
	info, err := r.runtime.UpsertSchedule(ctx, req)
	if err != nil {
		return jobdb.ScheduleInfo{}, err
	}
	return r.stripScheduleInfo(info), nil
}

func (r *tenantNamespacedRuntime) GetSchedule(ctx context.Context, key jobdb.ScheduleKey) (jobdb.ScheduleInfo, error) {
	info, err := r.runtime.GetSchedule(ctx, r.prefixScheduleKey(key))
	if err != nil {
		return jobdb.ScheduleInfo{}, err
	}
	return r.stripScheduleInfo(info), nil
}

func (r *tenantNamespacedRuntime) ListSchedules(ctx context.Context, req jobdb.ListSchedulesRequest) (jobdb.ListSchedulesResponse, error) {
	req.TenantId = r.prefixTenant(req.TenantId)
	resp, err := r.runtime.ListSchedules(ctx, req)
	if err != nil {
		return jobdb.ListSchedulesResponse{}, err
	}
	for i := range resp.Schedules {
		resp.Schedules[i] = r.stripScheduleInfo(resp.Schedules[i])
	}
	return resp, nil
}

func (r *tenantNamespacedRuntime) PauseSchedule(ctx context.Context, req jobdb.ScheduleMutationRequest) (jobdb.ScheduleInfo, error) {
	req.ScheduleKey = r.prefixScheduleKey(req.ScheduleKey)
	info, err := r.runtime.PauseSchedule(ctx, req)
	if err != nil {
		return jobdb.ScheduleInfo{}, err
	}
	return r.stripScheduleInfo(info), nil
}

func (r *tenantNamespacedRuntime) ResumeSchedule(ctx context.Context, req jobdb.ScheduleMutationRequest) (jobdb.ScheduleInfo, error) {
	req.ScheduleKey = r.prefixScheduleKey(req.ScheduleKey)
	info, err := r.runtime.ResumeSchedule(ctx, req)
	if err != nil {
		return jobdb.ScheduleInfo{}, err
	}
	return r.stripScheduleInfo(info), nil
}

func (r *tenantNamespacedRuntime) ArchiveSchedule(ctx context.Context, req jobdb.ScheduleMutationRequest) (jobdb.ScheduleInfo, error) {
	req.ScheduleKey = r.prefixScheduleKey(req.ScheduleKey)
	info, err := r.runtime.ArchiveSchedule(ctx, req)
	if err != nil {
		return jobdb.ScheduleInfo{}, err
	}
	return r.stripScheduleInfo(info), nil
}

func (r *tenantNamespacedRuntime) TriggerSchedule(ctx context.Context, req jobdb.TriggerScheduleRequest) (jobdb.JobHandle, error) {
	req.ScheduleKey = r.prefixScheduleKey(req.ScheduleKey)
	handle, err := r.runtime.TriggerSchedule(ctx, req)
	if err != nil {
		return jobdb.JobHandle{}, err
	}
	handle.JobKey = r.stripJobKey(handle.JobKey)
	return handle, nil
}

func (r *tenantNamespacedRuntime) ListScheduleRuns(ctx context.Context, req jobdb.ListScheduleRunsRequest) (jobdb.ListScheduleRunsResponse, error) {
	req.ScheduleKey = r.prefixScheduleKey(req.ScheduleKey)
	resp, err := r.runtime.ListScheduleRuns(ctx, req)
	if err != nil {
		return jobdb.ListScheduleRunsResponse{}, err
	}
	for i := range resp.Runs {
		resp.Runs[i].JobSummary.JobKey = r.stripJobKey(resp.Runs[i].JobSummary.JobKey)
	}
	return resp, nil
}

func (r *tenantNamespacedRuntime) GetChapter(ctx context.Context, ref jobdb.ChapterRef) (jobdb.Chapter, error) {
	ref.JobKey = r.prefixJobKey(ref.JobKey)
	return r.runtime.GetChapter(ctx, ref)
}

func (r *tenantNamespacedRuntime) ListChapters(ctx context.Context, req jobdb.ListChaptersRequest) ([]jobdb.Chapter, error) {
	req.JobKey = r.prefixJobKey(req.JobKey)
	return r.runtime.ListChapters(ctx, req)
}

func (r *tenantNamespacedRuntime) PutChapter(ctx context.Context, req jobdb.PutChapterRequest) error {
	req.Ref.JobKey = r.prefixJobKey(req.Ref.JobKey)
	return r.runtime.PutChapter(ctx, req)
}

func (r *tenantNamespacedRuntime) OpenArtifact(ctx context.Context, ref jobdb.ArtifactRef) (jobdb.ArtifactReader, error) {
	ref.JobKey = r.prefixJobKey(ref.JobKey)
	return r.runtime.OpenArtifact(ctx, ref)
}

func (r *tenantNamespacedRuntime) wrapLeases(leases []jobdb.ExecutionLease) []jobdb.ExecutionLease {
	if len(leases) == 0 {
		return leases
	}
	wrapped := make([]jobdb.ExecutionLease, 0, len(leases))
	for _, lease := range leases {
		wrapped = append(wrapped, &tenantNamespacedLease{lease: lease, runtime: r})
	}
	return wrapped
}

func (r *tenantNamespacedRuntime) prefixTenantIDs(tenantIDs []string) []string {
	if len(tenantIDs) == 0 {
		return tenantIDs
	}
	out := make([]string, 0, len(tenantIDs))
	for _, tenantID := range tenantIDs {
		out = append(out, r.prefixTenant(tenantID))
	}
	return out
}

func (r *tenantNamespacedRuntime) prefixJobKey(jobKey jobdb.JobKey) jobdb.JobKey {
	jobKey.TenantId = r.prefixTenant(jobKey.TenantId)
	return jobKey
}

func (r *tenantNamespacedRuntime) prefixScheduleKey(key jobdb.ScheduleKey) jobdb.ScheduleKey {
	key.TenantId = r.prefixTenant(key.TenantId)
	return key
}

func (r *tenantNamespacedRuntime) stripJobKey(jobKey jobdb.JobKey) jobdb.JobKey {
	jobKey.TenantId = r.stripTenant(jobKey.TenantId)
	return jobKey
}

func (r *tenantNamespacedRuntime) stripScheduleInfo(info jobdb.ScheduleInfo) jobdb.ScheduleInfo {
	info.TenantId = r.stripTenant(info.TenantId)
	info.ScheduleKey.TenantId = r.stripTenant(info.ScheduleKey.TenantId)
	if info.NextJobKey != nil {
		key := r.stripJobKey(*info.NextJobKey)
		info.NextJobKey = &key
	}
	return info
}

func (r *tenantNamespacedRuntime) prefixTenant(tenantID string) string {
	if tenantID == "" {
		return ""
	}
	return r.prefix + tenantID
}

func (r *tenantNamespacedRuntime) stripTenant(tenantID string) string {
	return strings.TrimPrefix(tenantID, r.prefix)
}

type tenantNamespacedLease struct {
	lease   jobdb.ExecutionLease
	runtime *tenantNamespacedRuntime
}

func (l *tenantNamespacedLease) LeaseID() string          { return l.lease.LeaseID() }
func (l *tenantNamespacedLease) Capability() string       { return l.lease.Capability() }
func (l *tenantNamespacedLease) Payload() json.RawMessage { return l.lease.Payload() }
func (l *tenantNamespacedLease) LeaseToken() string {
	if tokenLease, ok := l.lease.(interface{ LeaseToken() string }); ok {
		return tokenLease.LeaseToken()
	}
	return ""
}
func (l *tenantNamespacedLease) KeepAlive(ctx context.Context) error { return l.lease.KeepAlive(ctx) }
func (l *tenantNamespacedLease) StopKeepAlive()                      { l.lease.StopKeepAlive() }
func (l *tenantNamespacedLease) Complete(ctx context.Context, req jobdb.CompleteExecutionRequest) error {
	return l.lease.Complete(ctx, req)
}
func (l *tenantNamespacedLease) Reschedule(ctx context.Context, req jobdb.RescheduleExecutionRequest) error {
	return l.lease.Reschedule(ctx, req)
}
func (l *tenantNamespacedLease) SubmitJob(ctx context.Context, req jobdb.SubmitJobRequest) (jobdb.JobHandle, error) {
	req.Job.TenantId = l.runtime.prefixTenant(req.Job.TenantId)
	handle, err := l.lease.SubmitJob(ctx, req)
	if err != nil {
		return jobdb.JobHandle{}, err
	}
	handle.JobKey = l.runtime.stripJobKey(handle.JobKey)
	return handle, nil
}
func (l *tenantNamespacedLease) SubmitRestartJob(ctx context.Context, req jobdb.SubmitRestartJobRequest) (jobdb.JobHandle, error) {
	req.Job.PriorJobKey = l.runtime.prefixJobKey(req.Job.PriorJobKey)
	handle, err := l.lease.SubmitRestartJob(ctx, req)
	if err != nil {
		return jobdb.JobHandle{}, err
	}
	handle.JobKey = l.runtime.stripJobKey(handle.JobKey)
	return handle, nil
}
func (l *tenantNamespacedLease) Job() jobdb.JobHandle {
	handle := l.lease.Job()
	handle.JobKey = l.runtime.stripJobKey(handle.JobKey)
	return handle
}
