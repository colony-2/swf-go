package runtimeconformance_test

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	jobdbtest "github.com/colony-2/jobdb/pkg/workflow/internal/jobdbtest"
)

func remoteLeaseTokenForTest(lease jobdb.ExecutionLease) string {
	if leaseWithToken, ok := lease.(interface{ LeaseToken() string }); ok {
		return leaseWithToken.LeaseToken()
	}
	return ""
}

func TestRemoteRuntimesConstructAndExecuteThroughBuilder(t *testing.T) {
	ws := jobdbtest.MustWorkSet(t,
		jobdbtest.SequenceJob{Steps: []string{jobdbtest.AddOneTaskName, jobdbtest.DoubleTaskName}},
		jobdbtest.AddOneTask{},
		jobdbtest.DoubleTask{},
	)

	for _, harness := range jobdbtest.RemoteRuntimeHarnesses() {
		harness := harness
		t.Run(harness.Name, func(t *testing.T) {
			built := harness.New(t, ws)
			defer built.Shutdown(t)

			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()

			jobKey, err := built.Engine.SubmitJob(ctx, jobdb.SubmitJob{
				TenantId: built.WorkerTenantID,
				JobType:  jobdbtest.SequenceJobName,
				Data:     jobdbtest.NumberTaskData(1),
			})
			if err != nil {
				t.Fatalf("start job: %v", err)
			}

			jobdbtest.WaitForEngineStatus(t, ctx, built.Engine, jobKey, jobdb.JobStatusCompleted)

			result, err := jobResultForTest(built.Engine, ctx, jobKey)
			if err != nil {
				t.Fatalf("get job result: %v", err)
			}
			if got := jobdbtest.MustDecodeNumberTaskData(t, result); got != 4 {
				t.Fatalf("unexpected result: got %d want 4", got)
			}
		})
	}
}

func TestExecutionLeaseSubmitJobTracksParentAcrossRemoteRuntimes(t *testing.T) {
	for _, harness := range jobdbtest.RemoteRuntimeHarnesses() {
		harness := harness
		t.Run(harness.Name, func(t *testing.T) {
			if !harness.SupportsLeases {
				t.Skip("runtime does not support leases")
			}
			built := harness.New(t)
			defer built.Shutdown(t)
			assertExecutionLeaseSubmitJobTracksParent(t, built)
		})
	}
}

func TestRemoteRuntimeChapterAndArtifactRoundTripAcrossExistingRuntimes(t *testing.T) {
	for _, harness := range jobdbtest.RemoteRuntimeHarnesses() {
		harness := harness
		t.Run(harness.Name, func(t *testing.T) {
			if !harness.SupportsRuntimeStorage {
				t.Skip("runtime does not support chapter/artifact storage")
			}

			built := harness.New(t)
			defer built.Shutdown(t)

			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()

			handle, err := built.Runtime.SubmitJob(ctx, jobdb.SubmitJobRequest{
				Job: jobdb.SubmitJob{
					TenantId: built.WorkerTenantID,
					JobType:  "manual-storage",
					Data:     jobdbtest.NumberTaskData(1),
				},
				RequestTime: time.Now().UTC(),
			})
			if err != nil {
				t.Fatalf("start job for chapter storage: %v", err)
			}

			lease, err := built.Runtime.GetJobLease(ctx, jobdb.GetJobLeaseRequest{
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
			req := jobdb.PutChapterRequest{
				LeaseID:    lease.LeaseID(),
				LeaseToken: remoteLeaseTokenForTest(lease),
				Ref: jobdb.ChapterRef{
					JobKey:  handle.JobKey,
					Ordinal: 1,
				},
				Chapter: appTaskAttemptChapterForTest(t, 1, "manual", "manual-input-hash", []byte(`{"n":99}`), nil),
				ArtifactUploads: []jobdb.ArtifactUpload{
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
			if payload := appChapterPayloadForTest(t, storedChapter); string(payload) != `{"n":99}` {
				t.Fatalf("unexpected chapter payload %s", payload)
			}
			if len(storedChapter.Artifacts) != 1 || storedChapter.Artifacts[0].Name != "hello.txt" {
				t.Fatalf("unexpected stored artifacts %+v", storedChapter.Artifacts)
			}
			reader, err := built.Runtime.OpenArtifact(ctx, jobdb.ArtifactRef{
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

			completeLeaseForTest(t, ctx, lease, 2)
		})
	}
}

func TestRemoteRuntimeLeaseOperationsAcrossExistingRuntimes(t *testing.T) {
	for _, harness := range jobdbtest.RemoteRuntimeHarnesses() {
		harness := harness
		t.Run(harness.Name, func(t *testing.T) {
			if !harness.SupportsLeases {
				t.Skip("runtime does not support leases")
			}

			built := harness.New(t)
			defer built.Shutdown(t)

			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()

			handle, err := built.Runtime.SubmitJob(ctx, jobdb.SubmitJobRequest{
				Job: jobdb.SubmitJob{
					TenantId: built.WorkerTenantID,
					JobType:  "lease-job",
					Data:     jobdbtest.NumberTaskData(7),
				},
				RequestTime: time.Now().UTC(),
			})
			if err != nil {
				t.Fatalf("start job: %v", err)
			}

			leases, err := built.Runtime.PollWork(ctx, jobdb.PollWorkRequest{
				TenantId:     handle.JobKey.TenantId,
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
			if err := leases[0].Reschedule(ctx, jobdb.RescheduleExecutionRequest{
				NextNeed: "lease-job",
				Payload:  json.RawMessage(`{"kind":"rescheduled"}`),
			}); err != nil {
				t.Fatalf("reschedule: %v", err)
			}

			leases, err = built.Runtime.PollWork(ctx, jobdb.PollWorkRequest{
				TenantId:     handle.JobKey.TenantId,
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
			completeLeaseForTest(t, ctx, leases[0], 1)

			jobdbtest.WaitForRuntimeStatus(t, ctx, built.Runtime, handle.JobKey, jobdb.JobStatusCompleted)
		})
	}
}

func TestRemoteRuntimeConflictBehaviorAcrossExistingRuntimes(t *testing.T) {
	for _, harness := range jobdbtest.RemoteRuntimeHarnesses() {
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

				handle, err := built.Runtime.SubmitJob(ctx, jobdb.SubmitJobRequest{
					Job: jobdb.SubmitJob{
						TenantId: built.WorkerTenantID,
						JobType:  "manual-storage",
						Data:     jobdbtest.NumberTaskData(1),
					},
					RequestTime: time.Now().UTC(),
				})
				if err != nil {
					t.Fatalf("submit manual storage job: %v", err)
				}

				lease, err := built.Runtime.GetJobLease(ctx, jobdb.GetJobLeaseRequest{
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
					return built.Runtime.PutChapter(ctx, jobdb.PutChapterRequest{
						LeaseID:    lease.LeaseID(),
						LeaseToken: remoteLeaseTokenForTest(lease),
						Ref: jobdb.ChapterRef{
							JobKey:  handle.JobKey,
							Ordinal: ordinal,
						},
						Chapter: appTaskAttemptChapterForTest(t, ordinal, "manual", "conflict-hash", []byte(`{"n":1}`), nil),
					})
				}

				if err := put(1); err != nil {
					t.Fatalf("put chapter 1: %v", err)
				}
				if err := put(1); !errors.Is(err, jobdb.ErrConflict) {
					t.Fatalf("expected duplicate chapter conflict, got %v", err)
				}
				if err := put(3); !errors.Is(err, jobdb.ErrConflict) {
					t.Fatalf("expected non-appendable chapter conflict, got %v", err)
				}

				completeLeaseForTest(t, ctx, lease, 2)
			})

			t.Run("commit_if_waiting_conflicts_after_completion", func(t *testing.T) {
				ws := jobdbtest.MustWorkSet(t,
					jobdbtest.SequenceJob{Steps: []string{jobdbtest.MissingTaskName}},
				)
				built := harness.New(t, ws)
				defer built.Shutdown(t)

				ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
				defer cancel()

				jobKey, err := built.Engine.SubmitJob(ctx, jobdb.SubmitJob{
					TenantId: built.WorkerTenantID,
					JobType:  jobdbtest.SequenceJobName,
					Data:     jobdbtest.NumberTaskData(1),
				})
				if err != nil {
					t.Fatalf("submit waiting job: %v", err)
				}

				_ = jobdbtest.WaitForTaskHandle(t, ctx, built.Engine, jobdbtest.SequenceJobName, jobdbtest.MissingTaskName, []string{jobKey.TenantId})

				listed, err := built.Runtime.ListJobs(ctx, jobdb.ListJobsRequest{
					TenantIds: []string{jobKey.TenantId},
					JobKeys:   []jobdb.JobKey{jobKey},
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

				req := jobdb.CompleteTaskIfWaitingRequest{
					JobKey:        jobKey,
					Capability:    *summary.NextNeed,
					ResumeNeed:    *summary.TaskWaitNext,
					InputOrdinal:  *summary.TaskWaitInput,
					OutputOrdinal: *summary.TaskWaitOutput,
					InputHash:     *summary.TaskWaitInputHash,
					Data:          jobdbtest.NumberTaskData(2),
				}

				if err := built.Runtime.CompleteTaskIfWaiting(ctx, req); err != nil {
					t.Fatalf("complete task if waiting: %v", err)
				}
				if err := built.Runtime.CompleteTaskIfWaiting(ctx, req); !errors.Is(err, jobdb.ErrConflict) {
					t.Fatalf("expected second completion to conflict, got %v", err)
				}
			})
		})
	}
}

func TestRemoteRuntimePollWorkMetadataFilteringAcrossExistingRuntimes(t *testing.T) {
	for _, harness := range jobdbtest.RemoteRuntimeHarnesses() {
		harness := harness
		t.Run(harness.Name, func(t *testing.T) {
			built := harness.New(t)
			defer built.Shutdown(t)

			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()

			matching, err := built.Runtime.SubmitJob(ctx, jobdb.SubmitJobRequest{
				Job: jobdb.SubmitJob{
					TenantId: built.WorkerTenantID,
					JobType:  "metadata-job",
					Data:     jobdbtest.NumberTaskData(11),
					Metadata: json.RawMessage(`{"queue":"blue"}`),
				},
				RequestTime: time.Now().UTC(),
			})
			if err != nil {
				t.Fatalf("submit matching job: %v", err)
			}
			if _, err := built.Runtime.SubmitJob(ctx, jobdb.SubmitJobRequest{
				Job: jobdb.SubmitJob{
					TenantId: built.WorkerTenantID,
					JobType:  "metadata-job",
					Data:     jobdbtest.NumberTaskData(13),
					Metadata: json.RawMessage(`{"queue":"green"}`),
				},
				RequestTime: time.Now().UTC(),
			}); err != nil {
				t.Fatalf("submit non-matching job: %v", err)
			}

			leases, err := built.Runtime.PollWork(ctx, jobdb.PollWorkRequest{
				TenantId:      matching.JobKey.TenantId,
				WorkerID:      "metadata-worker",
				Capabilities:  []string{"metadata-job"},
				Limit:         1,
				LeaseDuration: 1500 * time.Millisecond,
				MetadataEquals: []jobdb.MetadataPredicate{{
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
			completeLeaseForTest(t, ctx, leases[0], 1)

			misses, err := built.Runtime.PollWork(ctx, jobdb.PollWorkRequest{
				TenantId:     matching.JobKey.TenantId,
				WorkerID:     "metadata-worker",
				Capabilities: []string{"metadata-job"},
				Limit:        1,
				MetadataEquals: []jobdb.MetadataPredicate{{
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
