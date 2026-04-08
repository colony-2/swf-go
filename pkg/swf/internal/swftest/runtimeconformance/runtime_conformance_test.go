package runtimeconformance_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/colony-2/swf-go/pkg/swf"
	swftest "github.com/colony-2/swf-go/pkg/swf/internal/swftest"
)

func leaseTokenForTest(lease swf.ExecutionLease) string {
	if leaseWithToken, ok := lease.(interface{ LeaseToken() string }); ok {
		return leaseWithToken.LeaseToken()
	}
	return ""
}

type explicitIDEchoJob struct {
	name string
}

func (j explicitIDEchoJob) Name() string { return j.name }

func (j explicitIDEchoJob) Run(_ swf.JobContext, data swf.JobData) (swf.JobData, error) {
	return data, nil
}

type explicitIDBlockingJob struct {
	name    string
	started chan<- struct{}
	release <-chan struct{}
}

func (j explicitIDBlockingJob) Name() string { return j.name }

func (j explicitIDBlockingJob) Run(_ swf.JobContext, data swf.JobData) (swf.JobData, error) {
	if j.started != nil {
		select {
		case j.started <- struct{}{}:
		default:
		}
	}
	<-j.release
	return data, nil
}

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

			jobKey, err := built.Engine.SubmitJob(ctx, swf.SubmitJob{
				TenantId: "tenant-builder-" + harness.Name,
				JobType:  swftest.SequenceJobName,
				Data:     swftest.NumberTaskData(1),
			})
			if err != nil {
				t.Fatalf("start job: %v", err)
			}

			swftest.WaitForEngineStatus(t, ctx, built.Engine, jobKey, swf.JobStatusCompleted)

			result, err := jobResultForTest(built.Engine, ctx, jobKey)
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

			handle, err := built.Runtime.SubmitJob(ctx, swf.SubmitJobRequest{
				Job: swf.SubmitJob{
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

			result, err := jobResultForTest(built.Runtime, ctx, handle.JobKey)
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
	for _, harness := range swftest.BuiltInRuntimeHarnesses() {
		harness := harness
		t.Run(harness.Name, func(t *testing.T) {
			if !harness.SupportsRuntimeStorage {
				t.Skip("runtime does not support chapter/artifact storage")
			}

			built := harness.New(t)
			defer built.Shutdown(t)

			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()

			handle, err := built.Runtime.SubmitJob(ctx, swf.SubmitJobRequest{
				Job: swf.SubmitJob{
					TenantId: "tenant-artifacts-" + harness.Name,
					JobType:  "manual-storage",
					Data:     swftest.NumberTaskData(1),
				},
				RequestTime: time.Now().UTC(),
			})
			if err != nil {
				t.Fatalf("start job for chapter storage: %v", err)
			}

			lease, err := built.Runtime.GetJobLease(ctx, swf.GetJobLeaseRequest{
				JobKey:        handle.JobKey,
				WorkerID:      "runtime-storage-test",
				Capabilities:  []string{"manual-storage"},
				LeaseDuration: 2 * time.Second,
			})
			if err != nil {
				t.Fatalf("get job lease: %v", err)
			}
			if lease == nil {
				t.Fatal("expected lease for chapter storage test")
			}

			artifactBytes := []byte("hello runtime")
			req := swf.PutChapterRequest{
				LeaseID:    lease.LeaseID(),
				LeaseToken: leaseTokenForTest(lease),
				Ref: swf.ChapterRef{
					JobKey:  handle.JobKey,
					Ordinal: 1,
				},
				Chapter: swf.StoredChapter{
					Ordinal:     1,
					TaskType:    "manual",
					ChapterType: "Manual",
					PayloadKind: "App",
					InputHash:   "manual-input-hash",
					CreatedAt:   time.Now().UTC(),
					Data:        json.RawMessage(`{"n":99}`),
				},
				ArtifactUploads: []swf.ArtifactUpload{
					{
						Name: "hello.txt",
						Size: int64(len(artifactBytes)),
						Open: func() (io.ReadCloser, error) {
							return io.NopCloser(bytes.NewReader(artifactBytes)), nil
						},
					},
				},
			}
			if err := built.Runtime.PutChapter(ctx, req); err != nil {
				t.Fatalf("put chapter: %v", err)
			}

			storedChapter, err := built.Runtime.GetChapter(ctx, req.Ref)
			if err != nil {
				t.Fatalf("get chapter: %v", err)
			}
			if storedChapter.Ordinal != 1 || storedChapter.TaskType != "manual" {
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
				Ordinal: 1,
				Name:    storedChapter.Artifacts[0].Name,
				Digest:  storedChapter.Artifacts[0].Digest,
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

			if err := lease.Complete(ctx, swf.CompleteExecutionRequest{Status: "succeeded"}); err != nil {
				t.Fatalf("complete lease: %v", err)
			}
		})
	}
}

func TestWorkflowRuntimeListChaptersRangeAcrossBuiltInRuntimes(t *testing.T) {
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

			handle, err := built.Runtime.SubmitJob(ctx, swf.SubmitJobRequest{
				Job: swf.SubmitJob{
					TenantId: "tenant-chapter-range-" + harness.Name,
					JobType:  swftest.SequenceJobName,
					Data:     swftest.NumberTaskData(1),
				},
				RequestTime: time.Now().UTC(),
			})
			if err != nil {
				t.Fatalf("submit job via runtime: %v", err)
			}

			swftest.WaitForRuntimeStatus(t, ctx, built.Runtime, handle.JobKey, swf.JobStatusCompleted)

			endOrdinal := int64(99)
			chapters, err := built.Runtime.ListChapters(ctx, swf.ListChaptersRequest{
				JobKey:       handle.JobKey,
				StartOrdinal: 1,
				EndOrdinal:   &endOrdinal,
			})
			if err != nil {
				t.Fatalf("list chapters range: %v", err)
			}
			if len(chapters) == 0 {
				t.Fatal("expected ranged chapter listing to return chapters")
			}
			for _, chapter := range chapters {
				if chapter.Ordinal < 1 {
					t.Fatalf("unexpected chapter ordinal %d", chapter.Ordinal)
				}
			}

			emptyStart := int64(99)
			empty, err := built.Runtime.ListChapters(ctx, swf.ListChaptersRequest{
				JobKey:       handle.JobKey,
				StartOrdinal: emptyStart,
			})
			if err != nil {
				t.Fatalf("list chapters above max ordinal: %v", err)
			}
			if len(empty) != 0 {
				t.Fatalf("expected empty chapter range above max ordinal, got %d", len(empty))
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

			handle, err := built.Runtime.SubmitJob(ctx, swf.SubmitJobRequest{
				Job: swf.SubmitJob{
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

func TestWorkflowRuntimeConflictBehaviorAcrossBuiltInRuntimes(t *testing.T) {
	for _, harness := range swftest.BuiltInRuntimeHarnesses() {
		harness := harness
		t.Run(harness.Name, func(t *testing.T) {
			t.Run("duplicate_and_non_appendable_chapters_conflict", func(t *testing.T) {
				if !harness.SupportsRuntimeStorage {
					t.Skip("runtime does not support chapter/artifact storage")
				}

				built := harness.New(t)
				defer built.Shutdown(t)

				ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
				defer cancel()

				handle, err := built.Runtime.SubmitJob(ctx, swf.SubmitJobRequest{
					Job: swf.SubmitJob{
						TenantId: "tenant-runtime-conflict-" + harness.Name,
						JobType:  "manual-storage",
						Data:     swftest.NumberTaskData(1),
					},
					RequestTime: time.Now().UTC(),
				})
				if err != nil {
					t.Fatalf("submit manual storage job: %v", err)
				}

				lease, err := built.Runtime.GetJobLease(ctx, swf.GetJobLeaseRequest{
					JobKey:        handle.JobKey,
					WorkerID:      "runtime-conflict-writer",
					Capabilities:  []string{"manual-storage"},
					LeaseDuration: 2 * time.Second,
				})
				if err != nil {
					t.Fatalf("get job lease: %v", err)
				}
				if lease == nil {
					t.Fatal("expected runtime conflict test lease")
				}

				put := func(ordinal int64) error {
					return built.Runtime.PutChapter(ctx, swf.PutChapterRequest{
						LeaseID:    lease.LeaseID(),
						LeaseToken: leaseTokenForTest(lease),
						Ref: swf.ChapterRef{
							JobKey:  handle.JobKey,
							Ordinal: ordinal,
						},
						Chapter: swf.StoredChapter{
							Ordinal:     ordinal,
							TaskType:    "manual",
							ChapterType: "Manual",
							PayloadKind: "App",
							InputHash:   "conflict-hash",
							CreatedAt:   time.Now().UTC(),
							Data:        json.RawMessage(`{"n":1}`),
						},
					})
				}

				if err := put(1); err != nil {
					t.Fatalf("put chapter 1: %v", err)
				}
				if err := put(1); !errors.Is(err, swf.ErrConflict) {
					t.Fatalf("expected duplicate chapter conflict, got %v", err)
				}
				if err := put(3); !errors.Is(err, swf.ErrConflict) {
					t.Fatalf("expected non-appendable chapter conflict, got %v", err)
				}

				if err := lease.Complete(ctx, swf.CompleteExecutionRequest{Status: "succeeded"}); err != nil {
					t.Fatalf("complete conflict test lease: %v", err)
				}
			})

			t.Run("commit_if_waiting_conflicts_after_completion", func(t *testing.T) {
				ws := swftest.MustWorkSet(t,
					swftest.SequenceJob{Steps: []string{swftest.MissingTaskName}},
				)
				built := harness.New(t, ws)
				defer built.Shutdown(t)

				ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
				defer cancel()

				jobKey, err := built.Engine.SubmitJob(ctx, swf.SubmitJob{
					TenantId: "tenant-runtime-wait-conflict-" + harness.Name,
					JobType:  swftest.SequenceJobName,
					Data:     swftest.NumberTaskData(1),
				})
				if err != nil {
					t.Fatalf("submit waiting job: %v", err)
				}

				_ = swftest.WaitForTaskHandle(t, ctx, built.Engine, swftest.SequenceJobName, swftest.MissingTaskName, []string{jobKey.TenantId})

				listed, err := built.Runtime.ListJobs(ctx, swf.ListJobsRequest{
					TenantIds: []string{jobKey.TenantId},
					JobKeys:   []swf.JobKey{jobKey},
					PageSize:  10,
				})
				if err != nil {
					t.Fatalf("list waiting job: %v", err)
				}
				if len(listed.Jobs) != 1 {
					t.Fatalf("expected 1 waiting job summary, got %d", len(listed.Jobs))
				}
				summary := listed.Jobs[0]
				if summary.NextNeed == nil || summary.TaskWaitNext == nil || summary.TaskWaitInput == nil || summary.TaskWaitOutput == nil || summary.TaskWaitInputHash == nil {
					t.Fatalf("missing waiting-task metadata in summary %+v", summary)
				}

				req := swf.CompleteTaskIfWaitingRequest{
					JobKey:        jobKey,
					Capability:    *summary.NextNeed,
					ResumeNeed:    *summary.TaskWaitNext,
					InputOrdinal:  *summary.TaskWaitInput,
					OutputOrdinal: *summary.TaskWaitOutput,
					InputHash:     *summary.TaskWaitInputHash,
					Data:          swftest.NumberTaskData(2),
				}

				if err := built.Runtime.CompleteTaskIfWaiting(ctx, req); err != nil {
					t.Fatalf("complete task if waiting: %v", err)
				}
				if err := built.Runtime.CompleteTaskIfWaiting(ctx, req); !errors.Is(err, swf.ErrConflict) {
					t.Fatalf("expected second completion to conflict, got %v", err)
				}
			})
		})
	}
}

func TestWorkflowRuntimeExplicitJobIDIdempotencyAcrossBuiltInRuntimes(t *testing.T) {
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

			submitReq := swf.SubmitJobRequest{
				Job: swf.SubmitJob{
					TenantId: "tenant-explicit-id-" + harness.Name,
					JobID:    "explicit-id-job",
					JobType:  swftest.SequenceJobName,
					Data:     swftest.NumberTaskData(1),
				},
				RequestTime: time.Now().UTC(),
			}
			handle, err := built.Runtime.SubmitJob(ctx, submitReq)
			if err != nil {
				t.Fatalf("submit explicit job id: %v", err)
			}
			matching, err := built.Runtime.SubmitJob(ctx, submitReq)
			if err != nil {
				t.Fatalf("repeat submit explicit job id: %v", err)
			}
			if matching.JobKey != handle.JobKey {
				t.Fatalf("unexpected matching handle %+v", matching.JobKey)
			}
			_, err = built.Runtime.SubmitJob(ctx, swf.SubmitJobRequest{
				Job: swf.SubmitJob{
					TenantId: submitReq.Job.TenantId,
					JobID:    submitReq.Job.JobID,
					JobType:  submitReq.Job.JobType,
					Data:     swftest.NumberTaskData(2),
				},
				RequestTime: time.Now().UTC(),
			})
			if !errors.Is(err, swf.ErrExistingJobMismatch) {
				t.Fatalf("expected explicit submit mismatch, got %v", err)
			}

			originalKey, err := built.Engine.SubmitJob(ctx, swf.SubmitJob{
				TenantId: "tenant-explicit-restart-" + harness.Name,
				JobID:    "explicit-restart-source",
				JobType:  swftest.SequenceJobName,
				Data:     swftest.NumberTaskData(1),
			})
			if err != nil {
				t.Fatalf("submit restart source: %v", err)
			}
			swftest.WaitForEngineStatus(t, ctx, built.Engine, originalKey, swf.JobStatusCompleted)

			restartReq := swf.SubmitRestartJobRequest{
				Job: swf.SubmitRestartJob{
					PriorJobKey:    originalKey,
					LastStepToKeep: 0,
					JobID:          "explicit-restart-copy",
				},
				RequestTime: time.Now().UTC(),
			}
			restartHandle, err := built.Runtime.SubmitRestartJob(ctx, restartReq)
			if err != nil {
				t.Fatalf("submit explicit restart id: %v", err)
			}
			restartMatching, err := built.Runtime.SubmitRestartJob(ctx, restartReq)
			if err != nil {
				t.Fatalf("repeat submit explicit restart id: %v", err)
			}
			if restartMatching.JobKey != restartHandle.JobKey {
				t.Fatalf("unexpected matching restart handle %+v", restartMatching.JobKey)
			}
			_, err = built.Runtime.SubmitRestartJob(ctx, swf.SubmitRestartJobRequest{
				Job: swf.SubmitRestartJob{
					PriorJobKey:    originalKey,
					LastStepToKeep: 1,
					JobID:          restartReq.Job.JobID,
				},
				RequestTime: time.Now().UTC(),
			})
			if !errors.Is(err, swf.ErrExistingJobMismatch) {
				t.Fatalf("expected explicit restart mismatch, got %v", err)
			}
		})
	}
}

func TestWorkflowRuntimeExplicitJobIDDuplicateSubmitStatePreservingAcrossBuiltInRuntimes(t *testing.T) {
	for _, harness := range swftest.BuiltInRuntimeHarnesses() {
		harness := harness
		t.Run(harness.Name, func(t *testing.T) {
			t.Run("ready", func(t *testing.T) {
				built := harness.New(t)
				defer built.Shutdown(t)

				ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
				defer cancel()

				req := swf.SubmitJobRequest{
					Job: swf.SubmitJob{
						TenantId: "tenant-explicit-ready-" + harness.Name,
						JobID:    "explicit-state-ready",
						JobType:  "ready-job",
						Data:     swftest.NumberTaskData(1),
					},
					RequestTime: time.Now().UTC(),
				}
				handle, err := built.Runtime.SubmitJob(ctx, req)
				if err != nil {
					t.Fatalf("submit ready explicit job id: %v", err)
				}
				matching, err := built.Runtime.SubmitJob(ctx, req)
				if err != nil {
					t.Fatalf("repeat ready explicit job id: %v", err)
				}
				if matching.JobKey != handle.JobKey {
					t.Fatalf("unexpected ready matching handle %+v", matching.JobKey)
				}
				status, err := jobStatusForTest(built.Runtime, ctx, handle.JobKey)
				if err != nil {
					t.Fatalf("get ready job status: %v", err)
				}
				if status != swf.JobStatusReady {
					t.Fatalf("expected ready status, got %s", status)
				}
				listed, err := built.Runtime.ListJobs(ctx, swf.ListJobsRequest{
					TenantIds: []string{handle.JobKey.TenantId},
					JobKeys:   []swf.JobKey{handle.JobKey},
					PageSize:  10,
				})
				if err != nil {
					t.Fatalf("list ready explicit job id: %v", err)
				}
				if len(listed.Jobs) != 1 {
					t.Fatalf("expected 1 ready logical job, got %d", len(listed.Jobs))
				}
			})

			t.Run("active", func(t *testing.T) {
				started := make(chan struct{}, 2)
				release := make(chan struct{})
				ws := swftest.MustWorkSet(t, explicitIDBlockingJob{
					name:    "explicit-active-job",
					started: started,
					release: release,
				})
				built := harness.New(t, ws)
				defer built.Shutdown(t)

				ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
				defer cancel()

				req := swf.SubmitJobRequest{
					Job: swf.SubmitJob{
						TenantId: "tenant-explicit-active-" + harness.Name,
						JobID:    "explicit-state-active",
						JobType:  ws.JobWorker.Name(),
						Data:     swftest.NumberTaskData(1),
					},
					RequestTime: time.Now().UTC(),
				}
				handle, err := built.Runtime.SubmitJob(ctx, req)
				if err != nil {
					t.Fatalf("submit active explicit job id: %v", err)
				}
				select {
				case <-started:
				case <-ctx.Done():
					t.Fatalf("wait for active job start: %v", ctx.Err())
				}
				swftest.WaitForRuntimeStatus(t, ctx, built.Runtime, handle.JobKey, swf.JobStatusActive)

				matching, err := built.Runtime.SubmitJob(ctx, req)
				if err != nil {
					t.Fatalf("repeat active explicit job id: %v", err)
				}
				if matching.JobKey != handle.JobKey {
					t.Fatalf("unexpected active matching handle %+v", matching.JobKey)
				}
				select {
				case <-started:
					t.Fatal("duplicate submit started a second active job")
				case <-time.After(150 * time.Millisecond):
				}
				listed, err := built.Runtime.ListJobs(ctx, swf.ListJobsRequest{
					TenantIds: []string{handle.JobKey.TenantId},
					JobKeys:   []swf.JobKey{handle.JobKey},
					PageSize:  10,
				})
				if err != nil {
					t.Fatalf("list active explicit job id: %v", err)
				}
				if len(listed.Jobs) != 1 {
					t.Fatalf("expected 1 active logical job, got %d", len(listed.Jobs))
				}

				close(release)
				swftest.WaitForRuntimeStatus(t, ctx, built.Runtime, handle.JobKey, swf.JobStatusCompleted)
			})

			t.Run("terminal", func(t *testing.T) {
				ws := swftest.MustWorkSet(t, explicitIDEchoJob{name: "explicit-terminal-job"})
				built := harness.New(t, ws)
				defer built.Shutdown(t)

				ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
				defer cancel()

				req := swf.SubmitJobRequest{
					Job: swf.SubmitJob{
						TenantId: "tenant-explicit-terminal-" + harness.Name,
						JobID:    "explicit-state-terminal",
						JobType:  ws.JobWorker.Name(),
						Data:     swftest.NumberTaskData(9),
					},
					RequestTime: time.Now().UTC(),
				}
				handle, err := built.Runtime.SubmitJob(ctx, req)
				if err != nil {
					t.Fatalf("submit terminal explicit job id: %v", err)
				}
				swftest.WaitForRuntimeStatus(t, ctx, built.Runtime, handle.JobKey, swf.JobStatusCompleted)

				matching, err := built.Runtime.SubmitJob(ctx, req)
				if err != nil {
					t.Fatalf("repeat terminal explicit job id: %v", err)
				}
				if matching.JobKey != handle.JobKey {
					t.Fatalf("unexpected terminal matching handle %+v", matching.JobKey)
				}
				result, err := jobResultForTest(built.Runtime, ctx, handle.JobKey)
				if err != nil {
					t.Fatalf("get terminal explicit job result: %v", err)
				}
				if got := swftest.MustDecodeNumberTaskData(t, result); got != 9 {
					t.Fatalf("unexpected terminal result: got %d want 9", got)
				}
				listed, err := built.Runtime.ListJobs(ctx, swf.ListJobsRequest{
					TenantIds: []string{handle.JobKey.TenantId},
					JobKeys:   []swf.JobKey{handle.JobKey},
					PageSize:  10,
				})
				if err != nil {
					t.Fatalf("list terminal explicit job id: %v", err)
				}
				if len(listed.Jobs) != 1 {
					t.Fatalf("expected 1 terminal logical job, got %d", len(listed.Jobs))
				}
			})
		})
	}
}

func TestWorkflowRuntimeExplicitJobIDConflictClassesAcrossBuiltInRuntimes(t *testing.T) {
	baseRunPolicy := swf.RunPolicy{
		Retry: swf.RetryPolicy{
			MaximumAttempts: 3,
			InitialInterval: swf.Duration(250 * time.Millisecond),
		},
	}
	basePrereqs := []swf.JobPrerequisite{{JobID: "dep-a", Condition: swf.JobPrereqSuccess}}

	testCases := []struct {
		name            string
		expectedMessage string
		mutate          func(swf.SubmitJob) swf.SubmitJob
	}{
		{
			name:            "job-type",
			expectedMessage: "different job type",
			mutate: func(job swf.SubmitJob) swf.SubmitJob {
				job.JobType = "shape-other"
				return job
			},
		},
		{
			name:            "input",
			expectedMessage: "different input",
			mutate: func(job swf.SubmitJob) swf.SubmitJob {
				job.Data = swftest.NumberTaskData(2)
				return job
			},
		},
		{
			name:            "metadata",
			expectedMessage: "different metadata",
			mutate: func(job swf.SubmitJob) swf.SubmitJob {
				job.Metadata = json.RawMessage(`{"queue":"green"}`)
				return job
			},
		},
		{
			name:            "run-policy",
			expectedMessage: "different run policy",
			mutate: func(job swf.SubmitJob) swf.SubmitJob {
				job.RunPolicy.Retry.MaximumAttempts = 5
				return job
			},
		},
		{
			name:            "prerequisites",
			expectedMessage: "different prerequisites",
			mutate: func(job swf.SubmitJob) swf.SubmitJob {
				job.Prerequisites = []swf.JobPrerequisite{{JobID: "dep-b", Condition: swf.JobPrereqComplete}}
				return job
			},
		},
	}

	for _, harness := range swftest.BuiltInRuntimeHarnesses() {
		harness := harness
		t.Run(harness.Name, func(t *testing.T) {
			for _, tc := range testCases {
				tc := tc
				t.Run(tc.name, func(t *testing.T) {
					built := harness.New(t)
					defer built.Shutdown(t)

					ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
					defer cancel()

					base := swf.SubmitJob{
						TenantId:  "tenant-explicit-shape-" + harness.Name,
						JobID:     "explicit-shape-" + tc.name,
						JobType:   "shape-base",
						Data:      swftest.NumberTaskData(1),
						Metadata:  json.RawMessage(`{"queue":"blue"}`),
						RunPolicy: baseRunPolicy,
						Prerequisites: append([]swf.JobPrerequisite(nil),
							basePrereqs...,
						),
					}
					for _, depID := range []string{"dep-a", "dep-b"} {
						if _, err := built.Runtime.SubmitJob(ctx, swf.SubmitJobRequest{
							Job: swf.SubmitJob{
								TenantId: base.TenantId,
								JobID:    depID,
								JobType:  "dep-job",
								Data:     swftest.NumberTaskData(0),
							},
							RequestTime: time.Now().UTC(),
						}); err != nil {
							t.Fatalf("seed prerequisite %s: %v", depID, err)
						}
					}
					if _, err := built.Runtime.SubmitJob(ctx, swf.SubmitJobRequest{
						Job:         base,
						RequestTime: time.Now().UTC(),
					}); err != nil {
						t.Fatalf("submit base explicit job id: %v", err)
					}

					_, err := built.Runtime.SubmitJob(ctx, swf.SubmitJobRequest{
						Job:         tc.mutate(base),
						RequestTime: time.Now().UTC(),
					})
					if !errors.Is(err, swf.ErrConflict) {
						t.Fatalf("expected %s conflict, got %v", tc.name, err)
					}
					if !strings.Contains(err.Error(), tc.expectedMessage) {
						t.Fatalf("expected %s message %q, got %v", tc.name, tc.expectedMessage, err)
					}
				})
			}
		})
	}
}

func TestWorkflowRuntimeExplicitJobIDTenantScopedAcrossBuiltInRuntimes(t *testing.T) {
	for _, harness := range swftest.BuiltInRuntimeHarnesses() {
		harness := harness
		t.Run(harness.Name, func(t *testing.T) {
			built := harness.New(t)
			defer built.Shutdown(t)

			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()

			reqA := swf.SubmitJobRequest{
				Job: swf.SubmitJob{
					TenantId: "tenant-explicit-scope-a-" + harness.Name,
					JobID:    "shared-explicit-id",
					JobType:  "tenant-scope-job",
					Data:     swftest.NumberTaskData(1),
				},
				RequestTime: time.Now().UTC(),
			}
			reqB := swf.SubmitJobRequest{
				Job: swf.SubmitJob{
					TenantId: "tenant-explicit-scope-b-" + harness.Name,
					JobID:    reqA.Job.JobID,
					JobType:  reqA.Job.JobType,
					Data:     reqA.Job.Data,
				},
				RequestTime: time.Now().UTC(),
			}

			handleA, err := built.Runtime.SubmitJob(ctx, reqA)
			if err != nil {
				t.Fatalf("submit tenant A explicit job id: %v", err)
			}
			handleB, err := built.Runtime.SubmitJob(ctx, reqB)
			if err != nil {
				t.Fatalf("submit tenant B explicit job id: %v", err)
			}
			if handleA.JobKey == handleB.JobKey {
				t.Fatalf("expected tenant-scoped job keys, got %+v and %+v", handleA.JobKey, handleB.JobKey)
			}

			repeatA, err := built.Runtime.SubmitJob(ctx, reqA)
			if err != nil {
				t.Fatalf("repeat tenant A explicit job id: %v", err)
			}
			if repeatA.JobKey != handleA.JobKey {
				t.Fatalf("unexpected tenant A matching handle %+v", repeatA.JobKey)
			}

			listedA, err := built.Runtime.ListJobs(ctx, swf.ListJobsRequest{
				TenantIds: []string{reqA.Job.TenantId},
				JobKeys:   []swf.JobKey{handleA.JobKey},
				PageSize:  10,
			})
			if err != nil {
				t.Fatalf("list tenant A explicit job id: %v", err)
			}
			if len(listedA.Jobs) != 1 {
				t.Fatalf("expected 1 tenant A logical job, got %d", len(listedA.Jobs))
			}

			listedB, err := built.Runtime.ListJobs(ctx, swf.ListJobsRequest{
				TenantIds: []string{reqB.Job.TenantId},
				JobKeys:   []swf.JobKey{handleB.JobKey},
				PageSize:  10,
			})
			if err != nil {
				t.Fatalf("list tenant B explicit job id: %v", err)
			}
			if len(listedB.Jobs) != 1 {
				t.Fatalf("expected 1 tenant B logical job, got %d", len(listedB.Jobs))
			}
		})
	}
}

func TestWorkflowRuntimePollWorkMetadataFilteringAcrossBuiltInRuntimes(t *testing.T) {
	for _, harness := range swftest.BuiltInRuntimeHarnesses() {
		harness := harness
		t.Run(harness.Name, func(t *testing.T) {
			built := harness.New(t)
			defer built.Shutdown(t)

			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()

			matching, err := built.Runtime.SubmitJob(ctx, swf.SubmitJobRequest{
				Job: swf.SubmitJob{
					TenantId: "tenant-metadata-" + harness.Name,
					JobID:    "metadata-blue-" + harness.Name,
					JobType:  "metadata-job",
					Data:     swftest.NumberTaskData(11),
					Metadata: json.RawMessage(`{"queue":"blue"}`),
				},
				RequestTime: time.Now().UTC(),
			})
			if err != nil {
				t.Fatalf("submit matching job: %v", err)
			}
			if _, err := built.Runtime.SubmitJob(ctx, swf.SubmitJobRequest{
				Job: swf.SubmitJob{
					TenantId: "tenant-metadata-" + harness.Name,
					JobID:    "metadata-green-" + harness.Name,
					JobType:  "metadata-job",
					Data:     swftest.NumberTaskData(13),
					Metadata: json.RawMessage(`{"queue":"green"}`),
				},
				RequestTime: time.Now().UTC(),
			}); err != nil {
				t.Fatalf("submit non-matching job: %v", err)
			}

			leases, err := built.Runtime.PollWork(ctx, swf.PollWorkRequest{
				WorkerID:      "metadata-worker",
				Capabilities:  []string{"metadata-job"},
				Limit:         1,
				LeaseDuration: 1500 * time.Millisecond,
				MetadataEquals: []swf.MetadataPredicate{{
					Path:   []string{"queue"},
					Values: []any{"blue"},
				}},
			})
			if err != nil {
				t.Fatalf("poll work with metadata filter: %v", err)
			}
			if len(leases) != 1 {
				t.Fatalf("expected 1 metadata-filtered lease, got %d", len(leases))
			}
			if leases[0].Job().JobKey != matching.JobKey {
				t.Fatalf("unexpected metadata-filtered lease job %+v", leases[0].Job().JobKey)
			}
			if err := leases[0].Complete(ctx, swf.CompleteExecutionRequest{Status: "succeeded"}); err != nil {
				t.Fatalf("complete metadata-filtered lease: %v", err)
			}

			misses, err := built.Runtime.PollWork(ctx, swf.PollWorkRequest{
				WorkerID:     "metadata-worker",
				Capabilities: []string{"metadata-job"},
				Limit:        1,
				MetadataEquals: []swf.MetadataPredicate{{
					Path:   []string{"queue"},
					Values: []any{"red"},
				}},
			})
			if err != nil {
				t.Fatalf("poll work with metadata miss filter: %v", err)
			}
			if len(misses) != 0 {
				t.Fatalf("expected no metadata-filtered lease miss, got %d", len(misses))
			}
		})
	}
}

func TestWorkflowRuntimeGetJobLeaseAcrossBuiltInRuntimes(t *testing.T) {
	for _, harness := range swftest.BuiltInRuntimeHarnesses() {
		harness := harness
		t.Run(harness.Name, func(t *testing.T) {
			built := harness.New(t)
			defer built.Shutdown(t)

			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()

			handle, err := built.Runtime.SubmitJob(ctx, swf.SubmitJobRequest{
				Job: swf.SubmitJob{
					TenantId: "tenant-targeted-" + harness.Name,
					JobID:    "targeted-job-" + harness.Name,
					JobType:  "targeted-job",
					Data:     swftest.NumberTaskData(5),
				},
				RequestTime: time.Now().UTC(),
			})
			if err != nil {
				t.Fatalf("submit targeted lease job: %v", err)
			}

			lease, err := built.Runtime.GetJobLease(ctx, swf.GetJobLeaseRequest{
				JobKey:        handle.JobKey,
				WorkerID:      "targeted-worker-a",
				Capabilities:  []string{"targeted-job"},
				LeaseDuration: 2 * time.Second,
			})
			if err != nil {
				t.Fatalf("get job lease: %v", err)
			}
			if lease == nil {
				t.Fatal("expected targeted job lease")
			}
			if lease.Job().JobKey != handle.JobKey {
				t.Fatalf("unexpected targeted lease job %+v", lease.Job().JobKey)
			}
			if lease.Capability() != "targeted-job" {
				t.Fatalf("unexpected targeted lease capability %q", lease.Capability())
			}

			miss, err := built.Runtime.GetJobLease(ctx, swf.GetJobLeaseRequest{
				JobKey:       handle.JobKey,
				WorkerID:     "targeted-worker-b",
				Capabilities: []string{"targeted-job"},
			})
			if err != nil {
				t.Fatalf("get job lease while leased: %v", err)
			}
			if miss != nil {
				t.Fatalf("expected nil lease while job is already leased, got %+v", miss.Job().JobKey)
			}

			if err := lease.Complete(ctx, swf.CompleteExecutionRequest{Status: "succeeded"}); err != nil {
				t.Fatalf("complete targeted lease: %v", err)
			}
			swftest.WaitForRuntimeStatus(t, ctx, built.Runtime, handle.JobKey, swf.JobStatusCompleted)

			miss, err = built.Runtime.GetJobLease(ctx, swf.GetJobLeaseRequest{
				JobKey:       handle.JobKey,
				WorkerID:     "targeted-worker-c",
				Capabilities: []string{"targeted-job"},
			})
			if err != nil {
				t.Fatalf("get job lease after completion: %v", err)
			}
			if miss != nil {
				t.Fatalf("expected nil lease after completion, got %+v", miss.Job().JobKey)
			}
		})
	}
}

func TestGetJobForRunAcrossBuiltInRuntimes(t *testing.T) {
	for _, harness := range swftest.BuiltInRuntimeHarnesses() {
		harness := harness
		t.Run(harness.Name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()

			t.Run("completed", func(t *testing.T) {
				built := harness.New(t)
				defer built.Shutdown(t)

				handle, err := built.Runtime.SubmitJob(ctx, swf.SubmitJobRequest{
					Job: swf.SubmitJob{
						TenantId: "tenant-run-job-complete-" + harness.Name,
						JobID:    "run-job-complete-" + harness.Name,
						JobType:  swftest.SequenceJobName,
						Data:     swftest.NumberTaskData(7),
					},
					RequestTime: time.Now().UTC(),
				})
				if err != nil {
					t.Fatalf("submit direct-run complete job: %v", err)
				}

				runnable, err := swf.GetJobForRun(ctx, built.Runtime, swf.GetJobForRunRequest{
					JobKey:    handle.JobKey,
					JobWorker: swftest.SequenceJob{},
					WorkerID:  "run-job-complete-worker",
				})
				if err != nil {
					t.Fatalf("get job for run: %v", err)
				}
				if !runnable.LeaseAcquired() {
					t.Fatal("expected runnable to hold a lease")
				}
				if _, ok := runnable.Outcome(); ok {
					t.Fatal("expected leased runnable to have no cached outcome before run")
				}

				outcome, err := runnable.Run(nil)
				if err != nil {
					t.Fatalf("run job runnable: %v", err)
				}
				if outcome.Status != swf.JobRunCompleted {
					t.Fatalf("unexpected outcome status %q", outcome.Status)
				}
				if !outcome.LeaseAcquired {
					t.Fatal("expected helper to acquire the lease")
				}
				if got := swftest.MustDecodeNumberTaskData(t, outcome.Output); got != 7 {
					t.Fatalf("unexpected output: got %d want 7", got)
				}

				replayed, err := swf.GetJobForRun(ctx, built.Runtime, swf.GetJobForRunRequest{
					JobKey:    handle.JobKey,
					JobWorker: swftest.SequenceJob{},
					WorkerID:  "run-job-complete-worker-2",
				})
				if err != nil {
					t.Fatalf("get completed job for run: %v", err)
				}
				if replayed.LeaseAcquired() {
					t.Fatal("expected completed job not to reacquire a lease")
				}
				cached, ok := replayed.Outcome()
				if !ok {
					t.Fatal("expected completed job runnable to expose a cached outcome")
				}
				if cached.Status != swf.JobRunCompleted {
					t.Fatalf("unexpected cached outcome status %q", cached.Status)
				}
				replayedOutcome, err := replayed.Run(nil)
				if err != nil {
					t.Fatalf("run completed job runnable: %v", err)
				}
				if got := swftest.MustDecodeNumberTaskData(t, replayedOutcome.Output); got != 7 {
					t.Fatalf("unexpected replayed output: got %d want 7", got)
				}
			})

			t.Run("failed", func(t *testing.T) {
				built := harness.New(t)
				defer built.Shutdown(t)

				handle, err := built.Runtime.SubmitJob(ctx, swf.SubmitJobRequest{
					Job: swf.SubmitJob{
						TenantId: "tenant-run-job-fail-" + harness.Name,
						JobID:    "run-job-fail-" + harness.Name,
						JobType:  swftest.FailingJobName,
						Data:     swftest.NumberTaskData(1),
					},
					RequestTime: time.Now().UTC(),
				})
				if err != nil {
					t.Fatalf("submit direct-run failing job: %v", err)
				}

				runnable, err := swf.GetJobForRun(ctx, built.Runtime, swf.GetJobForRunRequest{
					JobKey:    handle.JobKey,
					JobWorker: swftest.FailingJob{},
					WorkerID:  "run-job-fail-worker",
				})
				if err != nil {
					t.Fatalf("get failing job for run: %v", err)
				}
				if !runnable.LeaseAcquired() {
					t.Fatal("expected runnable to hold a lease")
				}

				outcome, err := runnable.Run(nil)
				if err != nil {
					t.Fatalf("run failing job runnable: %v", err)
				}
				if outcome.Status != swf.JobRunFailed {
					t.Fatalf("unexpected outcome status %q", outcome.Status)
				}
				if !outcome.LeaseAcquired {
					t.Fatal("expected helper to acquire the lease")
				}
				if outcome.JobError == nil || outcome.JobError.Error() != "intentional failure" {
					t.Fatalf("unexpected job error %v", outcome.JobError)
				}
			})

			t.Run("suspended_missing_capability", func(t *testing.T) {
				built := harness.New(t)
				defer built.Shutdown(t)

				handle, err := built.Runtime.SubmitJob(ctx, swf.SubmitJobRequest{
					Job: swf.SubmitJob{
						TenantId: "tenant-run-job-suspended-" + harness.Name,
						JobID:    "run-job-suspended-" + harness.Name,
						JobType:  swftest.SequenceJobName,
						Data:     swftest.NumberTaskData(3),
					},
					RequestTime: time.Now().UTC(),
				})
				if err != nil {
					t.Fatalf("submit direct-run suspended job: %v", err)
				}

				runnable, err := swf.GetJobForRun(ctx, built.Runtime, swf.GetJobForRunRequest{
					JobKey:    handle.JobKey,
					JobWorker: swftest.SequenceJob{Steps: []string{swftest.MissingTaskName}},
					WorkerID:  "run-job-suspended-worker",
				})
				if err != nil {
					t.Fatalf("get suspended job for run: %v", err)
				}
				if !runnable.LeaseAcquired() {
					t.Fatal("expected runnable to hold a lease")
				}

				outcome, err := runnable.Run(nil)
				if err != nil {
					t.Fatalf("run suspended job runnable: %v", err)
				}
				if outcome.Status != swf.JobRunSuspended {
					t.Fatalf("unexpected outcome status %q", outcome.Status)
				}
				if !outcome.LeaseAcquired {
					t.Fatal("expected helper to acquire the lease")
				}
				wantCapability := swftest.SequenceJobName + ":" + swftest.MissingTaskName
				if outcome.NextNeed == nil || *outcome.NextNeed != wantCapability {
					t.Fatalf("unexpected next need %+v", outcome.NextNeed)
				}
				if outcome.MissingCapability == nil || *outcome.MissingCapability != wantCapability {
					t.Fatalf("unexpected missing capability %+v", outcome.MissingCapability)
				}
			})

			t.Run("not_leaseable_while_active", func(t *testing.T) {
				built := harness.New(t)
				defer built.Shutdown(t)

				handle, err := built.Runtime.SubmitJob(ctx, swf.SubmitJobRequest{
					Job: swf.SubmitJob{
						TenantId: "tenant-run-job-active-" + harness.Name,
						JobID:    "run-job-active-" + harness.Name,
						JobType:  swftest.SequenceJobName,
						Data:     swftest.NumberTaskData(9),
					},
					RequestTime: time.Now().UTC(),
				})
				if err != nil {
					t.Fatalf("submit direct-run active job: %v", err)
				}

				lease, err := built.Runtime.GetJobLease(ctx, swf.GetJobLeaseRequest{
					JobKey:       handle.JobKey,
					WorkerID:     "held-lease-worker",
					Capabilities: []string{swftest.SequenceJobName},
				})
				if err != nil {
					t.Fatalf("hold targeted lease: %v", err)
				}
				if lease == nil {
					t.Fatal("expected targeted lease to be acquired")
				}

				runnable, err := swf.GetJobForRun(ctx, built.Runtime, swf.GetJobForRunRequest{
					JobKey:    handle.JobKey,
					JobWorker: swftest.SequenceJob{},
					WorkerID:  "run-job-active-worker",
				})
				if err != nil {
					t.Fatalf("get active job for run: %v", err)
				}
				if runnable.LeaseAcquired() {
					t.Fatal("expected held job not to reacquire a lease")
				}
				cached, ok := runnable.Outcome()
				if !ok {
					t.Fatal("expected active job runnable to expose a cached outcome")
				}
				if cached.Status != swf.JobRunNotLeaseable {
					t.Fatalf("unexpected cached outcome status %q", cached.Status)
				}
				outcome, err := runnable.Run(nil)
				if err != nil {
					t.Fatalf("run active job runnable: %v", err)
				}
				if outcome.Status != swf.JobRunNotLeaseable {
					t.Fatalf("unexpected outcome status %q", outcome.Status)
				}
				if outcome.LeaseAcquired {
					t.Fatal("expected helper not to acquire the held lease")
				}
				if outcome.JobStatus == nil || *outcome.JobStatus != swf.JobStatusActive {
					t.Fatalf("unexpected job status %+v", outcome.JobStatus)
				}

				if err := lease.Complete(ctx, swf.CompleteExecutionRequest{Status: "succeeded"}); err != nil {
					t.Fatalf("complete held lease: %v", err)
				}
			})
		})
	}
}
