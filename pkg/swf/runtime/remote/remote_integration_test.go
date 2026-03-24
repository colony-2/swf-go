package remote

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/colony-2/swf-go/pkg/swf"
	directruntime "github.com/colony-2/swf-go/pkg/swf/runtime/direct"
	toyruntime "github.com/colony-2/swf-go/pkg/swf/runtime/toy"
)

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
				TenantIds:     []string{tenantID},
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
				TenantIds:    []string{tenantID},
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

func TestRemoteRuntimePollWorkWithoutTenantFilter(t *testing.T) {
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

			capability := "global-poll-" + tc.name
			handle, err := runtime.SubmitJob(ctx, swf.SubmitJobRequest{
				Job: swf.SubmitJob{
					TenantId: "tenant-global-" + tc.name,
					JobType:  capability,
					Data:     swf.NewTaskDataOrPanic(map[string]int{"n": 1}),
				},
				RequestTime: time.Now().UTC(),
			})
			if err != nil {
				t.Fatalf("submit job: %v", err)
			}

			leases, err := runtime.PollWork(ctx, swf.PollWorkRequest{
				WorkerID:     "worker-global",
				Capabilities: []string{capability},
				Limit:        1,
			})
			if err != nil {
				t.Fatalf("poll work without tenant filter: %v", err)
			}
			if len(leases) != 1 {
				t.Fatalf("expected 1 lease, got %d", len(leases))
			}
			if leases[0].Job().JobKey != handle.JobKey {
				t.Fatalf("unexpected lease job key %+v", leases[0].Job().JobKey)
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
			chapter := swf.StoredChapter{
				Ordinal:     1,
				TaskType:    "artifact-job",
				ChapterType: "JobAttemptOutcome",
				PayloadKind: "App",
				CreatedAt:   time.Now().UTC(),
				Metadata:    json.RawMessage(`{"version":1}`),
				Data:        json.RawMessage(`{"ok":true}`),
			}
			if err := runtime.PutChapter(ctx, swf.PutChapterRequest{
				LeaseID: lease.LeaseID(),
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

func TestRemoteRuntimeRejectsCustomJobIDs(t *testing.T) {
	server := httptest.NewServer(NewServer(toyruntime.New()))
	defer server.Close()

	runtime, err := New(server.URL, server.Client())
	if err != nil {
		t.Fatalf("new remote runtime: %v", err)
	}

	_, err = runtime.SubmitJob(context.Background(), swf.SubmitJobRequest{
		Job: swf.SubmitJob{
			TenantId: "tenant-custom-id",
			JobID:    "not-supported",
			JobType:  "custom-id-job",
			Data:     swf.NewTaskDataOrPanic(map[string]int{"n": 1}),
		},
		RequestTime: time.Now().UTC(),
	})
	if err == nil {
		t.Fatal("expected custom job id submit to fail")
	}
	if !strings.Contains(err.Error(), "custom job IDs are not supported") {
		t.Fatalf("unexpected error: %v", err)
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
