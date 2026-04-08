package swftest

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

	"github.com/colony-2/swf-go/pkg/swf"
	directruntime "github.com/colony-2/swf-go/pkg/swf/runtime/direct"
	remoteruntime "github.com/colony-2/swf-go/pkg/swf/runtime/remote"
	toyruntime "github.com/colony-2/swf-go/pkg/swf/runtime/toy"
)

type RuntimeHarness struct {
	Name                   string
	SupportsLeases         bool
	SupportsRuntimeStorage bool
	StartsWorkerLoop       bool
	New                    func(t *testing.T, workers ...swf.WorkSet) *BuiltRuntimeHarness
}

type BuiltRuntimeHarness struct {
	Name    string
	Runtime swf.WorkflowRuntime
	Engine  swf.SWFEngine

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

func MustWorkSet(t *testing.T, job swf.JobWorker, tasks ...swf.TaskWorker) swf.WorkSet {
	t.Helper()
	ws, err := swf.AsWorkSet(job, tasks...)
	if err != nil {
		t.Fatalf("build workset: %v", err)
	}
	return *ws
}

func MustWorkSetWithOptions(t *testing.T, job swf.JobWorker, opts swf.WorkRegistrationOptions, tasks ...swf.TaskWorker) swf.WorkSet {
	t.Helper()
	ws, err := swf.AsWorkSetWithOptions(job, opts, tasks...)
	if err != nil {
		t.Fatalf("build workset: %v", err)
	}
	return *ws
}

func WaitForEngineStatus(t *testing.T, ctx context.Context, engine swf.SWFEngine, jobKey swf.JobKey, want swf.JobStatus) {
	t.Helper()
	waitForStatus(t, ctx, func(ctx context.Context) (swf.JobStatus, error) {
		job, err := engine.GetJob(ctx, jobKey)
		return job.Status, err
	}, want)
}

func WaitForRuntimeStatus(t *testing.T, ctx context.Context, runtime swf.WorkflowRuntime, jobKey swf.JobKey, want swf.JobStatus) {
	t.Helper()
	waitForStatus(t, ctx, func(ctx context.Context) (swf.JobStatus, error) {
		job, err := runtime.GetJob(ctx, jobKey)
		return job.Status, err
	}, want)
}

func WaitForTaskHandle(t *testing.T, ctx context.Context, engine swf.SWFEngine, jobType string, taskType string, tenantIDs []string) swf.TaskHandle {
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

func MustDecodeNumberTaskData(t *testing.T, data swf.TaskData) int {
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

func MustDecodeNumberTaskIO(t *testing.T, data *swf.TaskIO) int {
	t.Helper()
	if data == nil {
		t.Fatal("missing task io")
	}
	return decodeNumber(t, data.Data)
}

func MustStartJobAsync(t *testing.T, engine swf.SWFEngine, start swf.SubmitJob) <-chan error {
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
	if got := swf.JobTypeFromNextNeed(*nextNeed); got != wantJobType {
		t.Fatalf("unexpected next_need job type: got %q want %q", got, wantJobType)
	}
}

func ExpectTaskSuffix(t *testing.T, value string, wantSuffix string) {
	t.Helper()
	if !strings.HasSuffix(value, wantSuffix) {
		t.Fatalf("unexpected task value %q, want suffix %q", value, wantSuffix)
	}
}

func NumberTaskData(n int) swf.TaskData {
	return swf.NewTaskDataOrPanic(map[string]int{"n": n})
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

func (j SequenceJob) Run(ctx swf.JobContext, data swf.JobData) (swf.JobData, error) {
	var (
		out swf.TaskData = data
		err error
	)
	for _, step := range j.Steps {
		out, err = ctx.DoTask(swf.RunPolicy{}, step, out)
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

type AddOneTask struct{}

func (AddOneTask) Name() string { return AddOneTaskName }

func (AddOneTask) Run(_ swf.TaskContext, input swf.TaskData) (swf.TaskData, error) {
	n, err := numberFromTaskData(input)
	if err != nil {
		return nil, err
	}
	return NumberTaskData(n + 1), nil
}

type DoubleTask struct{}

func (DoubleTask) Name() string { return DoubleTaskName }

func (DoubleTask) Run(_ swf.TaskContext, input swf.TaskData) (swf.TaskData, error) {
	n, err := numberFromTaskData(input)
	if err != nil {
		return nil, err
	}
	return NumberTaskData(n * 2), nil
}

type FailingJob struct{}

func (FailingJob) Name() string { return FailingJobName }

func (FailingJob) Run(_ swf.JobContext, _ swf.JobData) (swf.JobData, error) {
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

func waitForStatus(t *testing.T, ctx context.Context, check func(context.Context) (swf.JobStatus, error), want swf.JobStatus) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		status, err := check(ctx)
		if err == nil && status == want {
			return
		}
		if err != nil && !errors.Is(err, swf.ErrJobNotFound) && !errors.Is(err, context.Canceled) {
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

func newToyHarness(t *testing.T, workers ...swf.WorkSet) *BuiltRuntimeHarness {
	t.Helper()
	runtime := toyruntime.New()
	return buildHarness(t, "toy", runtime, true, func() {}, workers...)
}

func newDirectHarness(t *testing.T, workers ...swf.WorkSet) *BuiltRuntimeHarness {
	t.Helper()
	embedded, err := directruntime.StartEmbeddedRuntime(context.Background())
	if err != nil {
		t.Fatalf("start embedded direct runtime: %v", err)
	}
	return buildHarness(t, "direct", embedded.Runtime, true, embedded.Shutdown, workers...)
}

func newRemoteToyHarness(t *testing.T, workers ...swf.WorkSet) *BuiltRuntimeHarness {
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

func newRemoteDirectHarness(t *testing.T, workers ...swf.WorkSet) *BuiltRuntimeHarness {
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

func newExternalRemoteHarness(t *testing.T, workers ...swf.WorkSet) *BuiltRuntimeHarness {
	t.Helper()
	baseURL, name := externalRemoteRuntimeConfig()
	runtime, err := remoteruntime.New(baseURL, &http.Client{Timeout: 30 * time.Second})
	if err != nil {
		t.Fatalf("build %s runtime: %v", name, err)
	}
	prefix := fmt.Sprintf("__swftest_external_%d__", externalHarnessSeq.Add(1))
	return buildHarness(t, name, newTenantNamespacedRuntime(runtime, prefix), true, func() {}, workers...)
}

func buildHarness(t *testing.T, name string, runtime swf.WorkflowRuntime, startLoop bool, shutdown func(), workers ...swf.WorkSet) *BuiltRuntimeHarness {
	t.Helper()
	builder := swf.NewEngineBuilder().WithRuntime(runtime)
	for _, ws := range workers {
		tasks := make([]swf.TaskWorker, 0, len(ws.TaskWorkers))
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
		Name:     name,
		Runtime:  runtime,
		Engine:   engine,
		cancel:   cancel,
		runDone:  runDone,
		shutdown: shutdown,
	}
}

func numberFromTaskData(data swf.TaskData) (int, error) {
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
	baseURL = strings.TrimSpace(os.Getenv("SWF_EXTERNAL_REMOTE_BASE_URL"))
	name = strings.TrimSpace(os.Getenv("SWF_EXTERNAL_REMOTE_NAME"))
	if name == "" {
		name = "remote-worker"
	}
	return baseURL, name
}

func externalOnlyHarnessesEnabled() bool {
	switch strings.TrimSpace(strings.ToLower(os.Getenv("SWF_EXTERNAL_REMOTE_ONLY"))) {
	case "1", "true", "yes", "on":
		return true
	default:
		return false
	}
}

type tenantNamespacedRuntime struct {
	runtime swf.WorkflowRuntime
	prefix  string
}

func newTenantNamespacedRuntime(runtime swf.WorkflowRuntime, prefix string) swf.WorkflowRuntime {
	return &tenantNamespacedRuntime{runtime: runtime, prefix: prefix}
}

func (r *tenantNamespacedRuntime) SubmitJob(ctx context.Context, req swf.SubmitJobRequest) (swf.JobHandle, error) {
	req.Job.TenantId = r.prefixTenant(req.Job.TenantId)
	handle, err := r.runtime.SubmitJob(ctx, req)
	if err != nil {
		return swf.JobHandle{}, err
	}
	handle.JobKey = r.stripJobKey(handle.JobKey)
	return handle, nil
}

func (r *tenantNamespacedRuntime) SubmitRestartJob(ctx context.Context, req swf.SubmitRestartJobRequest) (swf.JobHandle, error) {
	req.Job.PriorJobKey = r.prefixJobKey(req.Job.PriorJobKey)
	handle, err := r.runtime.SubmitRestartJob(ctx, req)
	if err != nil {
		return swf.JobHandle{}, err
	}
	handle.JobKey = r.stripJobKey(handle.JobKey)
	return handle, nil
}

func (r *tenantNamespacedRuntime) CancelJob(ctx context.Context, req swf.CancelJobRequest) error {
	req.JobKey = r.prefixJobKey(req.JobKey)
	return r.runtime.CancelJob(ctx, req)
}

func (r *tenantNamespacedRuntime) PollWork(ctx context.Context, req swf.PollWorkRequest) ([]swf.ExecutionLease, error) {
	if len(req.TenantIds) > 0 {
		req.TenantIds = r.prefixTenantIDs(req.TenantIds)
	}
	leases, err := r.runtime.PollWork(ctx, req)
	if err != nil {
		return nil, err
	}
	return r.wrapLeases(leases), nil
}

func (r *tenantNamespacedRuntime) GetJobLease(ctx context.Context, req swf.GetJobLeaseRequest) (swf.ExecutionLease, error) {
	req.JobKey = r.prefixJobKey(req.JobKey)
	lease, err := r.runtime.GetJobLease(ctx, req)
	if err != nil || lease == nil {
		return lease, err
	}
	return &tenantNamespacedLease{lease: lease, runtime: r}, nil
}

func (r *tenantNamespacedRuntime) CompleteTaskIfWaiting(ctx context.Context, req swf.CompleteTaskIfWaitingRequest) error {
	req.JobKey = r.prefixJobKey(req.JobKey)
	return r.runtime.CompleteTaskIfWaiting(ctx, req)
}

func (r *tenantNamespacedRuntime) GetJob(ctx context.Context, jobKey swf.JobKey) (swf.JobInfo, error) {
	return r.runtime.GetJob(ctx, r.prefixJobKey(jobKey))
}

func (r *tenantNamespacedRuntime) ListJobs(ctx context.Context, req swf.ListJobsRequest) (swf.ListJobsResponse, error) {
	req.TenantIds = r.prefixTenantIDs(req.TenantIds)
	if len(req.JobKeys) > 0 {
		jobKeys := make([]swf.JobKey, 0, len(req.JobKeys))
		for _, jobKey := range req.JobKeys {
			jobKeys = append(jobKeys, r.prefixJobKey(jobKey))
		}
		req.JobKeys = jobKeys
	}
	resp, err := r.runtime.ListJobs(ctx, req)
	if err != nil {
		return swf.ListJobsResponse{}, err
	}
	for i := range resp.Jobs {
		resp.Jobs[i].JobKey = r.stripJobKey(resp.Jobs[i].JobKey)
	}
	return resp, nil
}

func (r *tenantNamespacedRuntime) GetChapter(ctx context.Context, ref swf.ChapterRef) (swf.StoredChapter, error) {
	ref.JobKey = r.prefixJobKey(ref.JobKey)
	return r.runtime.GetChapter(ctx, ref)
}

func (r *tenantNamespacedRuntime) ListChapters(ctx context.Context, req swf.ListChaptersRequest) ([]swf.StoredChapter, error) {
	req.JobKey = r.prefixJobKey(req.JobKey)
	return r.runtime.ListChapters(ctx, req)
}

func (r *tenantNamespacedRuntime) PutChapter(ctx context.Context, req swf.PutChapterRequest) error {
	req.Ref.JobKey = r.prefixJobKey(req.Ref.JobKey)
	return r.runtime.PutChapter(ctx, req)
}

func (r *tenantNamespacedRuntime) OpenArtifact(ctx context.Context, ref swf.ArtifactRef) (swf.ArtifactReader, error) {
	ref.JobKey = r.prefixJobKey(ref.JobKey)
	return r.runtime.OpenArtifact(ctx, ref)
}

func (r *tenantNamespacedRuntime) wrapLeases(leases []swf.ExecutionLease) []swf.ExecutionLease {
	if len(leases) == 0 {
		return leases
	}
	wrapped := make([]swf.ExecutionLease, 0, len(leases))
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

func (r *tenantNamespacedRuntime) prefixJobKey(jobKey swf.JobKey) swf.JobKey {
	jobKey.TenantId = r.prefixTenant(jobKey.TenantId)
	return jobKey
}

func (r *tenantNamespacedRuntime) stripJobKey(jobKey swf.JobKey) swf.JobKey {
	jobKey.TenantId = r.stripTenant(jobKey.TenantId)
	return jobKey
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
	lease   swf.ExecutionLease
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
func (l *tenantNamespacedLease) Complete(ctx context.Context, req swf.CompleteExecutionRequest) error {
	return l.lease.Complete(ctx, req)
}
func (l *tenantNamespacedLease) Reschedule(ctx context.Context, req swf.RescheduleExecutionRequest) error {
	return l.lease.Reschedule(ctx, req)
}
func (l *tenantNamespacedLease) Job() swf.JobHandle {
	handle := l.lease.Job()
	handle.JobKey = l.runtime.stripJobKey(handle.JobKey)
	return handle
}
