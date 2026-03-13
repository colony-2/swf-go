package runtimeconformance_test

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"testing"
	"time"

	"github.com/colony-2/swf-go/pkg/swf"
	swftest "github.com/colony-2/swf-go/pkg/swf/internal/swftest"
)

func TestBuiltInRuntimesConstructAndExecuteThroughBuilder(t *testing.T) {
	ws := swftest.MustWorkSet(t,
		swftest.SequenceJob{Steps: []string{swftest.AddOneTaskName, swftest.DoubleTaskName}},
		swftest.AddOneTask{},
		swftest.DoubleTask{},
	)

	for _, harness := range swftest.BuiltInRuntimeHarnesses() {
		harness := harness
		t.Run(harness.Name, func(t *testing.T) {
			built := harness.New(t, ws)
			defer built.Shutdown(t)

			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()

			jobKey, err := built.Engine.StartJob(ctx, swf.StartJob{
				TenantId: "tenant-builder-" + harness.Name,
				JobType:  swftest.SequenceJobName,
				Data:     swftest.NumberTaskData(1),
			})
			if err != nil {
				t.Fatalf("start job: %v", err)
			}

			swftest.WaitForEngineStatus(t, ctx, built.Engine, jobKey, swf.JobStatusCompleted)

			result, err := built.Engine.GetJobResult(ctx, jobKey)
			if err != nil {
				t.Fatalf("get job result: %v", err)
			}
			if got := swftest.MustDecodeNumberTaskData(t, result); got != 4 {
				t.Fatalf("unexpected result: got %d want 4", got)
			}
		})
	}
}

func TestWorkflowRuntimeLifecycleAcrossBuiltInRuntimes(t *testing.T) {
	ws := swftest.MustWorkSet(t,
		swftest.SequenceJob{Steps: []string{swftest.AddOneTaskName, swftest.DoubleTaskName}},
		swftest.AddOneTask{},
		swftest.DoubleTask{},
	)

	for _, harness := range swftest.BuiltInRuntimeHarnesses() {
		harness := harness
		t.Run(harness.Name, func(t *testing.T) {
			built := harness.New(t, ws)
			defer built.Shutdown(t)

			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()

			handle, err := built.Runtime.StartJob(ctx, swf.StartJobRequest{
				Job: swf.StartJob{
					TenantId: "tenant-runtime-" + harness.Name,
					JobType:  swftest.SequenceJobName,
					Data:     swftest.NumberTaskData(1),
				},
				RequestTime: time.Now().UTC(),
			})
			if err != nil {
				t.Fatalf("start job via runtime: %v", err)
			}

			swftest.WaitForRuntimeStatus(t, ctx, built.Runtime, handle.JobKey, swf.JobStatusCompleted)

			result, err := built.Runtime.GetJobResult(ctx, handle.JobKey)
			if err != nil {
				t.Fatalf("get job result via runtime: %v", err)
			}
			if got := swftest.MustDecodeNumberTaskData(t, result); got != 4 {
				t.Fatalf("unexpected runtime result: got %d want 4", got)
			}

			resp, err := built.Runtime.ListJobs(ctx, swf.ListJobsRequest{
				TenantIds: []string{handle.JobKey.TenantId},
				JobKeys:   []swf.JobKey{handle.JobKey},
				PageSize:  10,
			})
			if err != nil {
				t.Fatalf("list jobs via runtime: %v", err)
			}
			if len(resp.Jobs) != 1 {
				t.Fatalf("expected 1 listed job, got %d", len(resp.Jobs))
			}
			if resp.Jobs[0].JobKey != handle.JobKey {
				t.Fatalf("unexpected listed job %+v", resp.Jobs[0].JobKey)
			}
		})
	}
}

func TestWorkflowRuntimeChapterAndArtifactRoundTripAcrossBuiltInRuntimes(t *testing.T) {
	ws := swftest.MustWorkSet(t,
		swftest.SequenceJob{Steps: []string{swftest.AddOneTaskName, swftest.DoubleTaskName}},
		swftest.AddOneTask{},
		swftest.DoubleTask{},
	)

	for _, harness := range swftest.BuiltInRuntimeHarnesses() {
		harness := harness
		t.Run(harness.Name, func(t *testing.T) {
			if !harness.SupportsRuntimeStorage {
				t.Skip("runtime does not support chapter/artifact storage")
			}

			built := harness.New(t, ws)
			defer built.Shutdown(t)

			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()

			handle, err := built.Runtime.StartJob(ctx, swf.StartJobRequest{
				Job: swf.StartJob{
					TenantId: "tenant-artifacts-" + harness.Name,
					JobType:  swftest.SequenceJobName,
					Data:     swftest.NumberTaskData(1),
				},
				RequestTime: time.Now().UTC(),
			})
			if err != nil {
				t.Fatalf("start job for chapter storage: %v", err)
			}
			swftest.WaitForRuntimeStatus(t, ctx, built.Runtime, handle.JobKey, swf.JobStatusCompleted)

			artifactBytes := []byte("hello runtime")
			storedArtifacts, err := built.Runtime.PutArtifacts(ctx, swf.PutArtifactsRequest{
				JobKey:  handle.JobKey,
				Ordinal: 50,
				Items: []swf.ArtifactUpload{
					{
						Name: "hello.txt",
						Size: int64(len(artifactBytes)),
						Open: func() (io.ReadCloser, error) {
							return io.NopCloser(bytes.NewReader(artifactBytes)), nil
						},
					},
				},
			})
			if err != nil {
				t.Fatalf("put artifacts: %v", err)
			}
			if len(storedArtifacts) != 1 {
				t.Fatalf("expected 1 stored artifact, got %d", len(storedArtifacts))
			}

			req := swf.PutChapterRequest{
				Ref: swf.ChapterRef{
					JobKey:  handle.JobKey,
					Ordinal: 50,
				},
				Chapter: swf.StoredChapter{
					Ordinal:     50,
					TaskType:    "manual",
					ChapterType: "Manual",
					PayloadKind: "App",
					InputHash:   "manual-input-hash",
					CreatedAt:   time.Now().UTC(),
					Data:        json.RawMessage(`{"n":99}`),
					Artifacts:   storedArtifacts,
				},
			}
			if err := built.Runtime.PutChapter(ctx, req); err != nil {
				t.Fatalf("put chapter: %v", err)
			}

			storedChapter, err := built.Runtime.GetChapter(ctx, req.Ref)
			if err != nil {
				t.Fatalf("get chapter: %v", err)
			}
			if storedChapter.Ordinal != 50 || storedChapter.TaskType != "manual" {
				t.Fatalf("unexpected stored chapter %+v", storedChapter)
			}
			if string(storedChapter.Data) != `{"n":99}` {
				t.Fatalf("unexpected chapter payload %s", storedChapter.Data)
			}
			if len(storedChapter.Artifacts) != 1 || storedChapter.Artifacts[0].Name != "hello.txt" {
				t.Fatalf("unexpected stored artifacts %+v", storedChapter.Artifacts)
			}

			reader, err := built.Runtime.OpenArtifact(ctx, swf.ArtifactRef{
				JobKey:  handle.JobKey,
				Ordinal: 50,
				Name:    storedArtifacts[0].Name,
				Digest:  storedArtifacts[0].Digest,
			})
			if err != nil {
				t.Fatalf("open artifact: %v", err)
			}
			rc, err := reader.Open()
			if err != nil {
				t.Fatalf("artifact open: %v", err)
			}
			data, err := io.ReadAll(rc)
			_ = rc.Close()
			if err != nil {
				t.Fatalf("read artifact: %v", err)
			}
			if string(data) != string(artifactBytes) {
				t.Fatalf("unexpected artifact bytes %q", string(data))
			}
		})
	}
}

func TestWorkflowRuntimeLeaseOperationsOnSupportingRuntimes(t *testing.T) {
	for _, harness := range swftest.BuiltInRuntimeHarnesses() {
		harness := harness
		t.Run(harness.Name, func(t *testing.T) {
			if !harness.SupportsLeases {
				t.Skip("runtime does not support leases")
			}

			built := harness.New(t)
			defer built.Shutdown(t)

			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()

			handle, err := built.Runtime.StartJob(ctx, swf.StartJobRequest{
				Job: swf.StartJob{
					TenantId: "tenant-lease-" + harness.Name,
					JobType:  "lease-job",
					Data:     swftest.NumberTaskData(7),
				},
				RequestTime: time.Now().UTC(),
			})
			if err != nil {
				t.Fatalf("start job: %v", err)
			}

			leases, err := built.Runtime.PollWork(ctx, swf.PollWorkRequest{
				WorkerID:     "lease-worker",
				Capabilities: []string{"lease-job"},
				Limit:        1,
			})
			if err != nil {
				t.Fatalf("poll work: %v", err)
			}
			if len(leases) != 1 {
				t.Fatalf("expected 1 lease, got %d", len(leases))
			}
			if leases[0].Job().JobKey != handle.JobKey {
				t.Fatalf("unexpected lease job %+v", leases[0].Job().JobKey)
			}
			if leases[0].Capability() != "lease-job" {
				t.Fatalf("unexpected capability %q", leases[0].Capability())
			}
			if err := leases[0].KeepAlive(ctx); err != nil {
				t.Fatalf("keepalive: %v", err)
			}
			if err := leases[0].Reschedule(ctx, swf.RescheduleExecutionRequest{
				NextNeed: "lease-job",
				Payload:  json.RawMessage(`{"kind":"rescheduled"}`),
			}); err != nil {
				t.Fatalf("reschedule: %v", err)
			}

			leases, err = built.Runtime.PollWork(ctx, swf.PollWorkRequest{
				WorkerID:     "lease-worker",
				Capabilities: []string{"lease-job"},
				Limit:        1,
			})
			if err != nil {
				t.Fatalf("poll work second time: %v", err)
			}
			if len(leases) != 1 {
				t.Fatalf("expected 1 lease after reschedule, got %d", len(leases))
			}
			payload := map[string]string{}
			if err := json.Unmarshal(leases[0].Payload(), &payload); err != nil {
				t.Fatalf("unmarshal lease payload: %v", err)
			}
			if payload["kind"] != "rescheduled" {
				t.Fatalf("unexpected lease payload %+v", payload)
			}
			if err := leases[0].Complete(ctx, swf.CompleteExecutionRequest{Status: "succeeded"}); err != nil {
				t.Fatalf("complete lease: %v", err)
			}

			swftest.WaitForRuntimeStatus(t, ctx, built.Runtime, handle.JobKey, swf.JobStatusCompleted)
		})
	}
}
