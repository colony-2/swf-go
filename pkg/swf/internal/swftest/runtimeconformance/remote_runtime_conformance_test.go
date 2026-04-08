package runtimeconformance_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/colony-2/swf-go/pkg/swf"
	swftest "github.com/colony-2/swf-go/pkg/swf/internal/swftest"
)

func remoteLeaseTokenForTest(lease swf.ExecutionLease) string {
	if leaseWithToken, ok := lease.(interface{ LeaseToken() string }); ok {
		return leaseWithToken.LeaseToken()
	}
	return ""
}

func TestRemoteRuntimesConstructAndExecuteThroughBuilder(t *testing.T) {
	ws := swftest.MustWorkSet(t,
		swftest.SequenceJob{Steps: []string{swftest.AddOneTaskName, swftest.DoubleTaskName}},
		swftest.AddOneTask{},
		swftest.DoubleTask{},
	)

	for _, harness := range swftest.RemoteRuntimeHarnesses() {
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

func TestRemoteRuntimeChapterAndArtifactRoundTripAcrossExistingRuntimes(t *testing.T) {
	for _, harness := range swftest.RemoteRuntimeHarnesses() {
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
				LeaseToken: remoteLeaseTokenForTest(lease),
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

func TestRemoteRuntimeLeaseOperationsAcrossExistingRuntimes(t *testing.T) {
	for _, harness := range swftest.RemoteRuntimeHarnesses() {
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
				TenantIds:    []string{handle.JobKey.TenantId},
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
				TenantIds:    []string{handle.JobKey.TenantId},
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

func TestRemoteRuntimeConflictBehaviorAcrossExistingRuntimes(t *testing.T) {
	for _, harness := range swftest.RemoteRuntimeHarnesses() {
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
						TenantId: "tenant-remote-runtime-conflict-" + harness.Name,
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
					WorkerID:      "remote-runtime-conflict-writer",
					Capabilities:  []string{"manual-storage"},
					LeaseDuration: 2 * time.Second,
				})
				if err != nil {
					t.Fatalf("get job lease: %v", err)
				}
				if lease == nil {
					t.Fatal("expected remote runtime conflict test lease")
				}

				put := func(ordinal int64) error {
					return built.Runtime.PutChapter(ctx, swf.PutChapterRequest{
						LeaseID:    lease.LeaseID(),
						LeaseToken: remoteLeaseTokenForTest(lease),
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
					TenantId: "tenant-remote-runtime-wait-conflict-" + harness.Name,
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

func TestRemoteRuntimePollWorkMetadataFilteringAcrossExistingRuntimes(t *testing.T) {
	for _, harness := range swftest.RemoteRuntimeHarnesses() {
		harness := harness
		t.Run(harness.Name, func(t *testing.T) {
			built := harness.New(t)
			defer built.Shutdown(t)

			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()

			matching, err := built.Runtime.SubmitJob(ctx, swf.SubmitJobRequest{
				Job: swf.SubmitJob{
					TenantId: "tenant-metadata-" + harness.Name,
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
					JobType:  "metadata-job",
					Data:     swftest.NumberTaskData(13),
					Metadata: json.RawMessage(`{"queue":"green"}`),
				},
				RequestTime: time.Now().UTC(),
			}); err != nil {
				t.Fatalf("submit non-matching job: %v", err)
			}

			leases, err := built.Runtime.PollWork(ctx, swf.PollWorkRequest{
				TenantIds:     []string{matching.JobKey.TenantId},
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
				TenantIds:    []string{matching.JobKey.TenantId},
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
