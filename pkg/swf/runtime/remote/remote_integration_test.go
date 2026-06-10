package remote

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/colony-2/swf-go/pkg/swf"
	"github.com/colony-2/swf-go/pkg/swf/internal/runtimecodec"
	directruntime "github.com/colony-2/swf-go/pkg/swf/runtime/direct"
	toyruntime "github.com/colony-2/swf-go/pkg/swf/runtime/toy"
)

func leaseTokenForTest(lease swf.ExecutionLease) string {
	if leaseWithToken, ok := lease.(interface{ LeaseToken() string }); ok {
		return leaseWithToken.LeaseToken()
	}
	return ""
}

func TestRemoteRuntimeLeaseAndMetadataRoundTrip(t *testing.T) {
	for _, tc := range []struct {
		name string
		new  func(t *testing.T) (swf.WorkflowRuntime, func())
	}{
		{
			name: "toy",
			new: func(t *testing.T) (swf.WorkflowRuntime, func()) {
				return toyruntime.New(), func() {}
			},
		},
		{
			name: "direct",
			new: func(t *testing.T) (swf.WorkflowRuntime, func()) {
				t.Helper()
				embedded, err := directruntime.StartEmbeddedRuntime(context.Background())
				if err != nil {
					t.Fatalf("start embedded direct runtime: %v", err)
				}
				return embedded.Runtime, embedded.Shutdown
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			underlying, shutdown := tc.new(t)
			defer shutdown()

			server := httptest.NewServer(NewServer(underlying))
			defer server.Close()

			runtime, err := New(server.URL, server.Client())
			if err != nil {
				t.Fatalf("new remote runtime: %v", err)
			}

			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()

			tenantID := "tenant-" + tc.name
			handle, err := runtime.SubmitJob(ctx, swf.SubmitJobRequest{
				Job: swf.SubmitJob{
					TenantId: tenantID,
					JobType:  "lease-job",
					Data:     swf.NewTaskDataOrPanic(map[string]int{"n": 7}),
					Metadata: json.RawMessage(`{"queue":"blue"}`),
				},
				RequestTime: time.Now().UTC(),
			})
			if err != nil {
				t.Fatalf("submit job: %v", err)
			}

			leases, err := runtime.PollWork(ctx, swf.PollWorkRequest{
				TenantId:      tenantID,
				WorkerID:      "worker-a",
				Capabilities:  []string{"lease-job"},
				Limit:         1,
				LeaseDuration: 2 * time.Second,
				MetadataEquals: []swf.MetadataPredicate{{
					Path:   []string{"queue"},
					Values: []any{"blue"},
				}},
			})
			if err != nil {
				t.Fatalf("poll work: %v", err)
			}
			if len(leases) != 1 {
				t.Fatalf("expected 1 lease, got %d", len(leases))
			}
			if leases[0].Job().JobKey != handle.JobKey {
				t.Fatalf("unexpected lease job key %+v", leases[0].Job().JobKey)
			}

			if err := leases[0].KeepAlive(ctx); err != nil {
				t.Fatalf("keep alive: %v", err)
			}
			if err := leases[0].Reschedule(ctx, swf.RescheduleExecutionRequest{
				NextNeed: "lease-job",
				Payload:  json.RawMessage(`{"kind":"rescheduled"}`),
			}); err != nil {
				t.Fatalf("reschedule: %v", err)
			}

			leases, err = runtime.PollWork(ctx, swf.PollWorkRequest{
				TenantId:     tenantID,
				WorkerID:     "worker-b",
				Capabilities: []string{"lease-job"},
				Limit:        1,
			})
			if err != nil {
				t.Fatalf("poll work after reschedule: %v", err)
			}
			if len(leases) != 1 {
				t.Fatalf("expected 1 lease after reschedule, got %d", len(leases))
			}
			payload := map[string]string{}
			if err := json.Unmarshal(leases[0].Payload(), &payload); err != nil {
				t.Fatalf("unmarshal payload: %v", err)
			}
			if payload["kind"] != "rescheduled" {
				t.Fatalf("unexpected payload %+v", payload)
			}

			if err := leases[0].Complete(ctx, swf.CompleteExecutionRequest{Status: "succeeded"}); err != nil {
				t.Fatalf("complete lease: %v", err)
			}
			waitForRuntimeStatus(t, ctx, runtime, handle.JobKey, swf.JobStatusCompleted)
		})
	}
}

func TestRemoteRuntimePollWorkRequiresTenantId(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		http.Error(w, "unexpected request", http.StatusInternalServerError)
	}))
	defer server.Close()

	runtime, err := New(server.URL, server.Client())
	if err != nil {
		t.Fatalf("new remote runtime: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if _, err := runtime.PollWork(ctx, swf.PollWorkRequest{
		WorkerID:     "worker-startup",
		Capabilities: []string{"startup-job"},
		Limit:        1,
	}); err == nil {
		t.Fatal("expected tenant-less poll work to fail")
	}
	if got := requests.Load(); got != 0 {
		t.Fatalf("expected no server requests, got %d", got)
	}
}

func TestRemoteServerPollWorkRejectsInvalidTenantId(t *testing.T) {
	server := httptest.NewServer(NewServer(toyruntime.New()))
	defer server.Close()

	tests := []struct {
		name string
		body string
	}{
		{
			name: "missing tenantId",
			body: `{"workerId":"worker","capabilities":["job"],"limit":1}`,
		},
		{
			name: "empty tenantId",
			body: `{"tenantId":"","workerId":"worker","capabilities":["job"],"limit":1}`,
		},
		{
			name: "legacy tenantIds",
			body: `{"tenantIds":["tenant-a"],"workerId":"worker","capabilities":["job"],"limit":1}`,
		},
		{
			name: "tenantId with legacy tenantIds",
			body: `{"tenantId":"tenant-a","tenantIds":["tenant-a"],"workerId":"worker","capabilities":["job"],"limit":1}`,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			resp, err := server.Client().Post(server.URL+"/v1/jobs/poll", "application/json", strings.NewReader(tc.body))
			if err != nil {
				t.Fatalf("post poll work: %v", err)
			}
			defer resp.Body.Close()
			if resp.StatusCode != http.StatusBadRequest {
				body, _ := io.ReadAll(resp.Body)
				t.Fatalf("expected 400, got %d body=%s", resp.StatusCode, string(body))
			}
		})
	}
}

func TestRemoteRuntimeChapterAndArtifactRoundTrip(t *testing.T) {
	for _, tc := range []struct {
		name string
		new  func(t *testing.T) (swf.WorkflowRuntime, func())
	}{
		{
			name: "toy",
			new: func(t *testing.T) (swf.WorkflowRuntime, func()) {
				return toyruntime.New(), func() {}
			},
		},
		{
			name: "direct",
			new: func(t *testing.T) (swf.WorkflowRuntime, func()) {
				t.Helper()
				embedded, err := directruntime.StartEmbeddedRuntime(context.Background())
				if err != nil {
					t.Fatalf("start embedded direct runtime: %v", err)
				}
				return embedded.Runtime, embedded.Shutdown
			},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			underlying, shutdown := tc.new(t)
			defer shutdown()

			server := httptest.NewServer(NewServer(underlying))
			defer server.Close()

			runtime, err := New(server.URL, server.Client())
			if err != nil {
				t.Fatalf("new remote runtime: %v", err)
			}

			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()

			tenantID := "tenant-artifact-" + tc.name
			handle, err := runtime.SubmitJob(ctx, swf.SubmitJobRequest{
				Job: swf.SubmitJob{
					TenantId: tenantID,
					JobType:  "artifact-job",
					Data:     swf.NewTaskDataOrPanic(map[string]int{"n": 3}),
				},
				RequestTime: time.Now().UTC(),
			})
			if err != nil {
				t.Fatalf("submit job: %v", err)
			}

			lease, err := runtime.GetJobLease(ctx, swf.GetJobLeaseRequest{
				JobKey:        handle.JobKey,
				WorkerID:      "worker-artifact",
				Capabilities:  []string{"artifact-job"},
				LeaseDuration: 2 * time.Second,
			})
			if err != nil {
				t.Fatalf("get job lease: %v", err)
			}
			if lease == nil {
				t.Fatal("expected lease")
			}

			artifact := swf.NewArtifactFromBytes("hello.txt", []byte("hello remote"))
			metadata, err := runtimecodec.ChapterMetadataFromJSON(json.RawMessage(`{"version":1}`))
			if err != nil {
				t.Fatalf("metadata: %v", err)
			}
			chapter := swf.Chapter{
				Ordinal:   1,
				TaskType:  "artifact-job",
				CreatedAt: time.Now().UTC(),
				Metadata:  metadata,
				Body: swf.JobAttemptOutcomeChapter{Outcome: swf.ApplicationOutputOutcome{
					Output: swf.ApplicationOutputBytes{Data: []byte(`{"ok":true}`)},
				}},
			}
			if err := runtime.PutChapter(ctx, swf.PutChapterRequest{
				LeaseID:    lease.LeaseID(),
				LeaseToken: leaseTokenForTest(lease),
				Ref: swf.ChapterRef{
					JobKey:  handle.JobKey,
					Ordinal: 1,
				},
				Chapter: chapter,
				ArtifactUploads: []swf.ArtifactUpload{{
					Name: artifact.Name(),
					Size: artifact.Size(),
					Open: artifact.Open,
				}},
			}); err != nil {
				t.Fatalf("put chapter: %v", err)
			}

			got, err := runtime.GetChapter(ctx, swf.ChapterRef{JobKey: handle.JobKey, Ordinal: 1})
			if err != nil {
				t.Fatalf("get chapter: %v", err)
			}
			if got.Ordinal != 1 || got.TaskType != "artifact-job" || len(got.Artifacts) != 1 {
				t.Fatalf("unexpected chapter %+v", got)
			}

			chapters, err := runtime.ListChapters(ctx, swf.ListChaptersRequest{
				JobKey:       handle.JobKey,
				StartOrdinal: 0,
			})
			if err != nil {
				t.Fatalf("list chapters: %v", err)
			}
			if len(chapters) < 2 {
				t.Fatalf("expected at least 2 chapters, got %d", len(chapters))
			}

			reader, err := runtime.OpenArtifact(ctx, swf.ArtifactRef{
				JobKey:  handle.JobKey,
				Ordinal: 1,
				Name:    "hello.txt",
			})
			if err != nil {
				t.Fatalf("open artifact: %v", err)
			}
			rc, err := reader.Open()
			if err != nil {
				t.Fatalf("artifact open reader: %v", err)
			}
			defer rc.Close()
			body, err := io.ReadAll(rc)
			if err != nil {
				t.Fatalf("read artifact: %v", err)
			}
			if string(body) != "hello remote" {
				t.Fatalf("unexpected artifact body %q", string(body))
			}
			if reader.Size() != int64(len(body)) {
				t.Fatalf("unexpected artifact size %d", reader.Size())
			}

			if err := lease.Complete(ctx, swf.CompleteExecutionRequest{Status: "succeeded"}); err != nil {
				t.Fatalf("complete lease: %v", err)
			}
		})
	}
}

func TestRemoteRuntimeSupportsExplicitJobIDs(t *testing.T) {
	underlying := toyruntime.New()
	server := httptest.NewServer(NewServer(underlying))
	defer server.Close()

	runtime, err := New(server.URL, server.Client())
	if err != nil {
		t.Fatalf("new remote runtime: %v", err)
	}

	handle, err := runtime.SubmitJob(context.Background(), swf.SubmitJobRequest{
		Job: swf.SubmitJob{
			TenantId: "tenant-custom-id",
			JobID:    "custom-job-id",
			JobType:  "custom-id-job",
			Data:     swf.NewTaskDataOrPanic(map[string]int{"n": 1}),
		},
		RequestTime: time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("submit explicit job id: %v", err)
	}
	if handle.JobKey.JobId != "custom-job-id" {
		t.Fatalf("unexpected job key %+v", handle.JobKey)
	}

	matching, err := runtime.SubmitJob(context.Background(), swf.SubmitJobRequest{
		Job: swf.SubmitJob{
			TenantId: "tenant-custom-id",
			JobID:    "custom-job-id",
			JobType:  "custom-id-job",
			Data:     swf.NewTaskDataOrPanic(map[string]int{"n": 1}),
		},
		RequestTime: time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("repeat explicit job id submit: %v", err)
	}
	if matching.JobKey != handle.JobKey {
		t.Fatalf("unexpected matching handle %+v", matching.JobKey)
	}

	_, err = runtime.SubmitJob(context.Background(), swf.SubmitJobRequest{
		Job: swf.SubmitJob{
			TenantId: "tenant-custom-id",
			JobID:    "custom-job-id",
			JobType:  "custom-id-job",
			Data:     swf.NewTaskDataOrPanic(map[string]int{"n": 2}),
		},
		RequestTime: time.Now().UTC(),
	})
	if !errors.Is(err, swf.ErrExistingJobMismatch) {
		t.Fatalf("expected existing job mismatch, got %v", err)
	}
}

func TestRemoteRuntimeSupportsExplicitRestartJobIDs(t *testing.T) {
	underlying := toyruntime.New()
	tenantID := "tenant-restart-id"
	builder := swf.NewEngineBuilder().WithRuntime(underlying).WithWorkerTenantId(tenantID)
	builder.PlusWorkers(remoteSequenceJob{}, remoteAddOneTask{}, remoteDoubleTask{})
	engine, err := builder.BuildEngine()
	if err != nil {
		t.Fatalf("build engine: %v", err)
	}
	runCtx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()
	done := make(chan struct{})
	go func() {
		defer close(done)
		engine.Run(runCtx)
	}()
	defer func() {
		cancelRun()
		<-done
	}()

	server := httptest.NewServer(NewServer(underlying))
	defer server.Close()

	runtime, err := New(server.URL, server.Client())
	if err != nil {
		t.Fatalf("new remote runtime: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	originalKey, err := engine.SubmitJob(ctx, swf.SubmitJob{
		TenantId: tenantID,
		JobID:    "restart-source",
		JobType:  remoteSequenceJobName,
		Data:     swf.NewTaskDataOrPanic(map[string]int{"n": 1}),
	})
	if err != nil {
		t.Fatalf("submit source job: %v", err)
	}
	waitForEngineStatus(t, ctx, engine, originalKey, swf.JobStatusCompleted)

	handle, err := runtime.SubmitRestartJob(ctx, swf.SubmitRestartJobRequest{
		Job: swf.SubmitRestartJob{
			PriorJobKey:    originalKey,
			LastStepToKeep: 0,
			JobID:          "restart-copy",
		},
		RequestTime: time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("submit explicit restart job id: %v", err)
	}
	if handle.JobKey.JobId != "restart-copy" {
		t.Fatalf("unexpected restart job key %+v", handle.JobKey)
	}

	matching, err := runtime.SubmitRestartJob(ctx, swf.SubmitRestartJobRequest{
		Job: swf.SubmitRestartJob{
			PriorJobKey:    originalKey,
			LastStepToKeep: 0,
			JobID:          "restart-copy",
		},
		RequestTime: time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("repeat explicit restart job id submit: %v", err)
	}
	if matching.JobKey != handle.JobKey {
		t.Fatalf("unexpected matching restart handle %+v", matching.JobKey)
	}

	_, err = runtime.SubmitRestartJob(ctx, swf.SubmitRestartJobRequest{
		Job: swf.SubmitRestartJob{
			PriorJobKey:    originalKey,
			LastStepToKeep: 1,
			JobID:          "restart-copy",
		},
		RequestTime: time.Now().UTC(),
	})
	if !errors.Is(err, swf.ErrExistingJobMismatch) {
		t.Fatalf("expected existing restart job mismatch, got %v", err)
	}
}

func waitForRuntimeStatus(t *testing.T, ctx context.Context, runtime swf.WorkflowRuntime, jobKey swf.JobKey, want swf.JobStatus) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		job, err := runtime.GetJob(ctx, jobKey)
		if err == nil && job.Status == want {
			return
		}
		if err != nil && !errors.Is(err, swf.ErrJobNotFound) && !errors.Is(err, context.Canceled) {
			t.Fatalf("check runtime status: %v", err)
		}
		select {
		case <-ctx.Done():
			t.Fatalf("wait for runtime status: %v", ctx.Err())
		case <-time.After(20 * time.Millisecond):
		}
	}
	t.Fatalf("job %s did not reach status %s", jobKey, want)
}

const (
	remoteSequenceJobName = "remote-seq"
	remoteAddOneTaskName  = "remote-add"
	remoteDoubleTaskName  = "remote-double"
)

type remoteSequenceJob struct{}

func (remoteSequenceJob) Name() string { return remoteSequenceJobName }

func (remoteSequenceJob) Run(ctx swf.JobContext, data swf.JobData) (swf.JobData, error) {
	first, err := ctx.DoTask(swf.RunPolicy{}, remoteAddOneTaskName, data)
	if err != nil {
		return nil, err
	}
	second, err := ctx.DoTask(swf.RunPolicy{}, remoteDoubleTaskName, first)
	if err != nil {
		return nil, err
	}
	return second, nil
}

type remoteAddOneTask struct{}

func (remoteAddOneTask) Name() string { return remoteAddOneTaskName }

func (remoteAddOneTask) Run(_ swf.TaskContext, input swf.TaskData) (swf.TaskData, error) {
	raw, err := input.GetData()
	if err != nil {
		return nil, err
	}
	payload := map[string]int{}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return nil, err
	}
	return swf.NewTaskDataOrPanic(map[string]int{"n": payload["n"] + 1}), nil
}

type remoteDoubleTask struct{}

func (remoteDoubleTask) Name() string { return remoteDoubleTaskName }

func (remoteDoubleTask) Run(_ swf.TaskContext, input swf.TaskData) (swf.TaskData, error) {
	raw, err := input.GetData()
	if err != nil {
		return nil, err
	}
	payload := map[string]int{}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return nil, err
	}
	return swf.NewTaskDataOrPanic(map[string]int{"n": payload["n"] * 2}), nil
}

func waitForEngineStatus(t *testing.T, ctx context.Context, engine swf.SWFEngine, jobKey swf.JobKey, want swf.JobStatus) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		job, err := engine.GetJob(ctx, jobKey)
		if err == nil && job.Status == want {
			return
		}
		if err != nil && !errors.Is(err, swf.ErrJobNotFound) && !errors.Is(err, context.Canceled) {
			t.Fatalf("check engine status: %v", err)
		}
		select {
		case <-ctx.Done():
			t.Fatalf("wait for engine status: %v", ctx.Err())
		case <-time.After(20 * time.Millisecond):
		}
	}
	t.Fatalf("job %s did not reach engine status %s", jobKey, want)
}
