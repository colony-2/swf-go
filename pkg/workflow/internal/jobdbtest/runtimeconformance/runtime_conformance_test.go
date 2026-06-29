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

	"github.com/colony-2/jobdb/pkg/internal/runtimecodec"
	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/workflow"
	jobdbtest "github.com/colony-2/jobdb/pkg/workflow/internal/jobdbtest"
)

func appTaskAttemptChapterForTest(t *testing.T, ordinal int64, taskType string, inputHash string, data []byte, metadata json.RawMessage) jobdb.Chapter {
	t.Helper()
	chapterMetadata, err := runtimecodec.ChapterMetadataFromJSON(metadata)
	if err != nil {
		t.Fatalf("chapter metadata: %v", err)
	}
	return jobdb.Chapter{
		Ordinal:   ordinal,
		TaskType:  taskType,
		InputHash: inputHash,
		CreatedAt: time.Now().UTC(),
		Metadata:  chapterMetadata,
		Body: jobdb.TaskAttemptOutcomeChapter{Outcome: jobdb.ApplicationOutputOutcome{
			Output: jobdb.ApplicationOutputBytes{Data: append([]byte(nil), data...)},
		}},
	}
}

func appChapterPayloadForTest(t *testing.T, chapter jobdb.Chapter) []byte {
	t.Helper()
	payloadKind, payload, err := runtimecodec.ChapterPayload(chapter)
	if err != nil {
		t.Fatalf("chapter payload: %v", err)
	}
	if payloadKind != runtimecodec.PayloadKindApp {
		t.Fatalf("payload kind = %s, want %s", payloadKind, runtimecodec.PayloadKindApp)
	}
	return payload
}

func leaseTokenForTest(lease jobdb.ExecutionLease) string {
	if leaseWithToken, ok := lease.(interface{ LeaseToken() string }); ok {
		return leaseWithToken.LeaseToken()
	}
	return ""
}

type explicitIDEchoJob struct {
	name string
}

func (j explicitIDEchoJob) Name() string { return j.name }

func (j explicitIDEchoJob) Run(_ workflow.JobContext, data jobdb.JobData) (jobdb.JobData, error) {
	return data, nil
}

type explicitIDBlockingJob struct {
	name    string
	started chan<- struct{}
	release <-chan struct{}
}

func (j explicitIDBlockingJob) Name() string { return j.name }

func (j explicitIDBlockingJob) Run(_ workflow.JobContext, data jobdb.JobData) (jobdb.JobData, error) {
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
	ws := jobdbtest.MustWorkSet(t,
		jobdbtest.SequenceJob{Steps: []string{jobdbtest.AddOneTaskName, jobdbtest.DoubleTaskName}},
		jobdbtest.AddOneTask{},
		jobdbtest.DoubleTask{},
	)

	for _, harness := range jobdbtest.BuiltInRuntimeHarnesses() {
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

func TestWorkflowRuntimeLifecycleAcrossBuiltInRuntimes(t *testing.T) {
	ws := jobdbtest.MustWorkSet(t,
		jobdbtest.SequenceJob{Steps: []string{jobdbtest.AddOneTaskName, jobdbtest.DoubleTaskName}},
		jobdbtest.AddOneTask{},
		jobdbtest.DoubleTask{},
	)

	for _, harness := range jobdbtest.BuiltInRuntimeHarnesses() {
		harness := harness
		t.Run(harness.Name, func(t *testing.T) {
			built := harness.New(t, ws)
			defer built.Shutdown(t)

			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()

			handle, err := built.Runtime.SubmitJob(ctx, jobdb.SubmitJobRequest{
				Job: jobdb.SubmitJob{
					TenantId: built.WorkerTenantID,
					JobType:  jobdbtest.SequenceJobName,
					Data:     jobdbtest.NumberTaskData(1),
				},
				RequestTime: time.Now().UTC(),
			})
			if err != nil {
				t.Fatalf("start job via runtime: %v", err)
			}

			jobdbtest.WaitForRuntimeStatus(t, ctx, built.Runtime, handle.JobKey, jobdb.JobStatusCompleted)

			result, err := jobResultForTest(built.Runtime, ctx, handle.JobKey)
			if err != nil {
				t.Fatalf("get job result via runtime: %v", err)
			}
			if got := jobdbtest.MustDecodeNumberTaskData(t, result); got != 4 {
				t.Fatalf("unexpected runtime result: got %d want 4", got)
			}

			resp, err := built.Runtime.ListJobs(ctx, jobdb.ListJobsRequest{
				TenantIds: []string{handle.JobKey.TenantId},
				JobKeys:   []jobdb.JobKey{handle.JobKey},
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

func TestExecutionLeaseSubmitJobTracksParentAcrossBuiltInRuntimes(t *testing.T) {
	for _, harness := range jobdbtest.BuiltInRuntimeHarnesses() {
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

func assertExecutionLeaseSubmitJobTracksParent(t *testing.T, built *jobdbtest.BuiltRuntimeHarness) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	const parentType = "parent-tracking-parent"
	parent, err := built.Runtime.SubmitJob(ctx, jobdb.SubmitJobRequest{
		Job: jobdb.SubmitJob{
			TenantId: built.WorkerTenantID,
			JobID:    "parent-tracking-root",
			JobType:  parentType,
			Data:     jobdbtest.NumberTaskData(1),
		},
		RequestTime: time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("submit parent job: %v", err)
	}

	lease, err := built.Runtime.GetJobLease(ctx, jobdb.GetJobLeaseRequest{
		JobKey:        parent.JobKey,
		WorkerID:      "parent-tracking-worker",
		Capabilities:  []string{parentType},
		LeaseDuration: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("get parent lease: %v", err)
	}
	if lease == nil {
		t.Fatal("expected parent lease")
	}

	child, err := lease.SubmitJob(ctx, jobdb.SubmitJobRequest{
		Job: jobdb.SubmitJob{
			JobID:   "parent-tracking-child",
			JobType: "parent-tracking-child-type",
			Data:    jobdbtest.NumberTaskData(2),
		},
		RequestTime: time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("submit child job through lease: %v", err)
	}
	if child.JobKey.TenantId != parent.JobKey.TenantId {
		t.Fatalf("child tenant = %q, want %q", child.JobKey.TenantId, parent.JobKey.TenantId)
	}

	const restartSourceType = "parent-tracking-restart-source"
	restartSource, err := built.Runtime.SubmitJob(ctx, jobdb.SubmitJobRequest{
		Job: jobdb.SubmitJob{
			TenantId: built.WorkerTenantID,
			JobID:    "parent-tracking-restart-source",
			JobType:  restartSourceType,
			Data:     jobdbtest.NumberTaskData(3),
		},
		RequestTime: time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("submit restart source job: %v", err)
	}
	restartSourceLease, err := built.Runtime.GetJobLease(ctx, jobdb.GetJobLeaseRequest{
		JobKey:        restartSource.JobKey,
		WorkerID:      "parent-tracking-source-worker",
		Capabilities:  []string{restartSourceType},
		LeaseDuration: 5 * time.Second,
	})
	if err != nil {
		t.Fatalf("get restart source lease: %v", err)
	}
	if restartSourceLease == nil {
		t.Fatal("expected restart source lease")
	}
	completeLeaseForTest(t, ctx, restartSourceLease, 1)

	restartChild, err := lease.SubmitRestartJob(ctx, jobdb.SubmitRestartJobRequest{
		Job: jobdb.SubmitRestartJob{
			JobID:          "parent-tracking-restart-child",
			PriorJobKey:    restartSource.JobKey,
			LastStepToKeep: 0,
		},
		RequestTime: time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("submit child restart job through lease: %v", err)
	}
	if restartChild.JobKey.TenantId != parent.JobKey.TenantId {
		t.Fatalf("restart child tenant = %q, want %q", restartChild.JobKey.TenantId, parent.JobKey.TenantId)
	}

	children, err := built.Runtime.ListJobs(ctx, jobdb.ListJobsRequest{
		TenantIds:    []string{parent.JobKey.TenantId},
		JobKeys:      []jobdb.JobKey{parent.JobKey, child.JobKey, restartChild.JobKey},
		ParentJobIDs: []string{parent.JobKey.JobId},
		PageSize:     10,
	})
	if err != nil {
		t.Fatalf("list child jobs by parent: %v", err)
	}
	if len(children.Jobs) != 2 {
		t.Fatalf("listed children = %d, want 2", len(children.Jobs))
	}
	listedChildren := make(map[jobdb.JobKey]jobdb.JobSummary, len(children.Jobs))
	for _, childSummary := range children.Jobs {
		listedChildren[childSummary.JobKey] = childSummary
		if childSummary.ParentJobID != parent.JobKey.JobId {
			t.Fatalf("listed child parent = %q, want %q", childSummary.ParentJobID, parent.JobKey.JobId)
		}
	}
	if _, ok := listedChildren[child.JobKey]; !ok {
		t.Fatalf("submitted child %+v not listed in %+v", child.JobKey, children.Jobs)
	}
	if _, ok := listedChildren[restartChild.JobKey]; !ok {
		t.Fatalf("submitted restart child %+v not listed in %+v", restartChild.JobKey, children.Jobs)
	}

	roots, err := built.Runtime.ListJobs(ctx, jobdb.ListJobsRequest{
		TenantIds: []string{parent.JobKey.TenantId},
		JobKeys:   []jobdb.JobKey{parent.JobKey, child.JobKey, restartChild.JobKey},
		RootOnly:  true,
		PageSize:  10,
	})
	if err != nil {
		t.Fatalf("list root jobs: %v", err)
	}
	if len(roots.Jobs) != 1 {
		t.Fatalf("listed roots = %d, want 1", len(roots.Jobs))
	}
	if roots.Jobs[0].JobKey != parent.JobKey {
		t.Fatalf("listed root key = %+v, want %+v", roots.Jobs[0].JobKey, parent.JobKey)
	}
	if roots.Jobs[0].ParentJobID != "" {
		t.Fatalf("root parent = %q, want empty", roots.Jobs[0].ParentJobID)
	}

	if _, err := built.Runtime.ListJobs(ctx, jobdb.ListJobsRequest{
		TenantIds:    []string{parent.JobKey.TenantId},
		ParentJobIDs: []string{parent.JobKey.JobId},
		RootOnly:     true,
	}); err == nil {
		t.Fatal("expected list error when RootOnly and ParentJobIDs are combined")
	}
}

func TestWorkflowRuntimeChapterAndArtifactRoundTripAcrossBuiltInRuntimes(t *testing.T) {
	for _, harness := range jobdbtest.BuiltInRuntimeHarnesses() {
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
				LeaseToken: leaseTokenForTest(lease),
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

func TestWorkflowRuntimeListChaptersRangeAcrossBuiltInRuntimes(t *testing.T) {
	ws := jobdbtest.MustWorkSet(t,
		jobdbtest.SequenceJob{Steps: []string{jobdbtest.AddOneTaskName, jobdbtest.DoubleTaskName}},
		jobdbtest.AddOneTask{},
		jobdbtest.DoubleTask{},
	)

	for _, harness := range jobdbtest.BuiltInRuntimeHarnesses() {
		harness := harness
		t.Run(harness.Name, func(t *testing.T) {
			built := harness.New(t, ws)
			defer built.Shutdown(t)

			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()

			handle, err := built.Runtime.SubmitJob(ctx, jobdb.SubmitJobRequest{
				Job: jobdb.SubmitJob{
					TenantId: built.WorkerTenantID,
					JobType:  jobdbtest.SequenceJobName,
					Data:     jobdbtest.NumberTaskData(1),
				},
				RequestTime: time.Now().UTC(),
			})
			if err != nil {
				t.Fatalf("submit job via runtime: %v", err)
			}

			jobdbtest.WaitForRuntimeStatus(t, ctx, built.Runtime, handle.JobKey, jobdb.JobStatusCompleted)

			endOrdinal := int64(99)
			chapters, err := built.Runtime.ListChapters(ctx, jobdb.ListChaptersRequest{
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
			empty, err := built.Runtime.ListChapters(ctx, jobdb.ListChaptersRequest{
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
	for _, harness := range jobdbtest.BuiltInRuntimeHarnesses() {
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
			if leases[0].Job().JobKey != handle.JobKey {
				t.Fatalf("unexpected lease job %+v", leases[0].Job().JobKey)
			}
			if leases[0].Capability() != "lease-job" {
				t.Fatalf("unexpected capability %q", leases[0].Capability())
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

func TestWorkflowRuntimePollWorkRequiresTenantAcrossBuiltInRuntimes(t *testing.T) {
	for _, harness := range jobdbtest.BuiltInRuntimeHarnesses() {
		harness := harness
		t.Run(harness.Name, func(t *testing.T) {
			built := harness.New(t)
			defer built.Shutdown(t)

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			_, err := built.Runtime.PollWork(ctx, jobdb.PollWorkRequest{
				WorkerID:     "tenant-required-worker",
				Capabilities: []string{"tenant-required-job"},
				Limit:        1,
			})
			if err == nil {
				t.Fatal("expected PollWork without tenantId to fail")
			}
		})
	}
}

func TestWorkflowRuntimeConflictBehaviorAcrossBuiltInRuntimes(t *testing.T) {
	for _, harness := range jobdbtest.BuiltInRuntimeHarnesses() {
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
					return built.Runtime.PutChapter(ctx, jobdb.PutChapterRequest{
						LeaseID:    lease.LeaseID(),
						LeaseToken: leaseTokenForTest(lease),
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

func TestWorkflowRuntimeExplicitJobIDIdempotencyAcrossBuiltInRuntimes(t *testing.T) {
	ws := jobdbtest.MustWorkSet(t,
		jobdbtest.SequenceJob{Steps: []string{jobdbtest.AddOneTaskName, jobdbtest.DoubleTaskName}},
		jobdbtest.AddOneTask{},
		jobdbtest.DoubleTask{},
	)

	for _, harness := range jobdbtest.BuiltInRuntimeHarnesses() {
		harness := harness
		t.Run(harness.Name, func(t *testing.T) {
			built := harness.New(t, ws)
			defer built.Shutdown(t)

			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()

			submitReq := jobdb.SubmitJobRequest{
				Job: jobdb.SubmitJob{
					TenantId: built.WorkerTenantID,
					JobID:    "explicit-id-job",
					JobType:  jobdbtest.SequenceJobName,
					Data:     jobdbtest.NumberTaskData(1),
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
			_, err = built.Runtime.SubmitJob(ctx, jobdb.SubmitJobRequest{
				Job: jobdb.SubmitJob{
					TenantId: submitReq.Job.TenantId,
					JobID:    submitReq.Job.JobID,
					JobType:  submitReq.Job.JobType,
					Data:     jobdbtest.NumberTaskData(2),
				},
				RequestTime: time.Now().UTC(),
			})
			if !errors.Is(err, jobdb.ErrExistingJobMismatch) {
				t.Fatalf("expected explicit submit mismatch, got %v", err)
			}

			originalKey, err := built.Engine.SubmitJob(ctx, jobdb.SubmitJob{
				TenantId: built.WorkerTenantID,
				JobID:    "explicit-restart-source",
				JobType:  jobdbtest.SequenceJobName,
				Data:     jobdbtest.NumberTaskData(1),
			})
			if err != nil {
				t.Fatalf("submit restart source: %v", err)
			}
			jobdbtest.WaitForEngineStatus(t, ctx, built.Engine, originalKey, jobdb.JobStatusCompleted)

			restartReq := jobdb.SubmitRestartJobRequest{
				Job: jobdb.SubmitRestartJob{
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
			_, err = built.Runtime.SubmitRestartJob(ctx, jobdb.SubmitRestartJobRequest{
				Job: jobdb.SubmitRestartJob{
					PriorJobKey:    originalKey,
					LastStepToKeep: 1,
					JobID:          restartReq.Job.JobID,
				},
				RequestTime: time.Now().UTC(),
			})
			if !errors.Is(err, jobdb.ErrExistingJobMismatch) {
				t.Fatalf("expected explicit restart mismatch, got %v", err)
			}
		})
	}
}

func TestWorkflowRuntimeExplicitJobIDDuplicateSubmitStatePreservingAcrossBuiltInRuntimes(t *testing.T) {
	for _, harness := range jobdbtest.BuiltInRuntimeHarnesses() {
		harness := harness
		t.Run(harness.Name, func(t *testing.T) {
			t.Run("ready", func(t *testing.T) {
				built := harness.New(t)
				defer built.Shutdown(t)

				ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
				defer cancel()

				req := jobdb.SubmitJobRequest{
					Job: jobdb.SubmitJob{
						TenantId: built.WorkerTenantID,
						JobID:    "explicit-state-ready",
						JobType:  "ready-job",
						Data:     jobdbtest.NumberTaskData(1),
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
				if status != jobdb.JobStatusReady {
					t.Fatalf("expected ready status, got %s", status)
				}
				listed, err := built.Runtime.ListJobs(ctx, jobdb.ListJobsRequest{
					TenantIds: []string{handle.JobKey.TenantId},
					JobKeys:   []jobdb.JobKey{handle.JobKey},
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
				ws := jobdbtest.MustWorkSet(t, explicitIDBlockingJob{
					name:    "explicit-active-job",
					started: started,
					release: release,
				})
				built := harness.New(t, ws)
				defer built.Shutdown(t)

				ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
				defer cancel()

				req := jobdb.SubmitJobRequest{
					Job: jobdb.SubmitJob{
						TenantId: built.WorkerTenantID,
						JobID:    "explicit-state-active",
						JobType:  ws.JobWorker.Name(),
						Data:     jobdbtest.NumberTaskData(1),
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
				jobdbtest.WaitForRuntimeStatus(t, ctx, built.Runtime, handle.JobKey, jobdb.JobStatusActive)

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
				listed, err := built.Runtime.ListJobs(ctx, jobdb.ListJobsRequest{
					TenantIds: []string{handle.JobKey.TenantId},
					JobKeys:   []jobdb.JobKey{handle.JobKey},
					PageSize:  10,
				})
				if err != nil {
					t.Fatalf("list active explicit job id: %v", err)
				}
				if len(listed.Jobs) != 1 {
					t.Fatalf("expected 1 active logical job, got %d", len(listed.Jobs))
				}

				close(release)
				jobdbtest.WaitForRuntimeStatus(t, ctx, built.Runtime, handle.JobKey, jobdb.JobStatusCompleted)
			})

			t.Run("terminal", func(t *testing.T) {
				ws := jobdbtest.MustWorkSet(t, explicitIDEchoJob{name: "explicit-terminal-job"})
				built := harness.New(t, ws)
				defer built.Shutdown(t)

				ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
				defer cancel()

				req := jobdb.SubmitJobRequest{
					Job: jobdb.SubmitJob{
						TenantId: built.WorkerTenantID,
						JobID:    "explicit-state-terminal",
						JobType:  ws.JobWorker.Name(),
						Data:     jobdbtest.NumberTaskData(9),
					},
					RequestTime: time.Now().UTC(),
				}
				handle, err := built.Runtime.SubmitJob(ctx, req)
				if err != nil {
					t.Fatalf("submit terminal explicit job id: %v", err)
				}
				jobdbtest.WaitForRuntimeStatus(t, ctx, built.Runtime, handle.JobKey, jobdb.JobStatusCompleted)

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
				if got := jobdbtest.MustDecodeNumberTaskData(t, result); got != 9 {
					t.Fatalf("unexpected terminal result: got %d want 9", got)
				}
				listed, err := built.Runtime.ListJobs(ctx, jobdb.ListJobsRequest{
					TenantIds: []string{handle.JobKey.TenantId},
					JobKeys:   []jobdb.JobKey{handle.JobKey},
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
	baseRunPolicy := jobdb.RunPolicy{
		Retry: jobdb.RetryPolicy{
			MaximumAttempts: 3,
			InitialInterval: jobdb.Duration(250 * time.Millisecond),
		},
	}
	basePrereqs := []jobdb.JobPrerequisite{{JobID: "dep-a", Condition: jobdb.JobPrereqSuccess}}

	testCases := []struct {
		name            string
		expectedMessage string
		mutate          func(jobdb.SubmitJob) jobdb.SubmitJob
	}{
		{
			name:            "job-type",
			expectedMessage: "different job type",
			mutate: func(job jobdb.SubmitJob) jobdb.SubmitJob {
				job.JobType = "shape-other"
				return job
			},
		},
		{
			name:            "input",
			expectedMessage: "different input",
			mutate: func(job jobdb.SubmitJob) jobdb.SubmitJob {
				job.Data = jobdbtest.NumberTaskData(2)
				return job
			},
		},
		{
			name:            "metadata",
			expectedMessage: "different metadata",
			mutate: func(job jobdb.SubmitJob) jobdb.SubmitJob {
				job.Metadata = json.RawMessage(`{"queue":"green"}`)
				return job
			},
		},
		{
			name:            "run-policy",
			expectedMessage: "different run policy",
			mutate: func(job jobdb.SubmitJob) jobdb.SubmitJob {
				job.RunPolicy.Retry.MaximumAttempts = 5
				return job
			},
		},
		{
			name:            "prerequisites",
			expectedMessage: "different prerequisites",
			mutate: func(job jobdb.SubmitJob) jobdb.SubmitJob {
				job.Prerequisites = []jobdb.JobPrerequisite{{JobID: "dep-b", Condition: jobdb.JobPrereqComplete}}
				return job
			},
		},
	}

	for _, harness := range jobdbtest.BuiltInRuntimeHarnesses() {
		harness := harness
		t.Run(harness.Name, func(t *testing.T) {
			for _, tc := range testCases {
				tc := tc
				t.Run(tc.name, func(t *testing.T) {
					built := harness.New(t)
					defer built.Shutdown(t)

					ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
					defer cancel()

					base := jobdb.SubmitJob{
						TenantId:  built.WorkerTenantID,
						JobID:     "explicit-shape-" + tc.name,
						JobType:   "shape-base",
						Data:      jobdbtest.NumberTaskData(1),
						Metadata:  json.RawMessage(`{"queue":"blue"}`),
						RunPolicy: baseRunPolicy,
						Prerequisites: append([]jobdb.JobPrerequisite(nil),
							basePrereqs...,
						),
					}
					for _, depID := range []string{"dep-a", "dep-b"} {
						if _, err := built.Runtime.SubmitJob(ctx, jobdb.SubmitJobRequest{
							Job: jobdb.SubmitJob{
								TenantId: base.TenantId,
								JobID:    depID,
								JobType:  "dep-job",
								Data:     jobdbtest.NumberTaskData(0),
							},
							RequestTime: time.Now().UTC(),
						}); err != nil {
							t.Fatalf("seed prerequisite %s: %v", depID, err)
						}
					}
					if _, err := built.Runtime.SubmitJob(ctx, jobdb.SubmitJobRequest{
						Job:         base,
						RequestTime: time.Now().UTC(),
					}); err != nil {
						t.Fatalf("submit base explicit job id: %v", err)
					}

					_, err := built.Runtime.SubmitJob(ctx, jobdb.SubmitJobRequest{
						Job:         tc.mutate(base),
						RequestTime: time.Now().UTC(),
					})
					if !errors.Is(err, jobdb.ErrConflict) {
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
	for _, harness := range jobdbtest.BuiltInRuntimeHarnesses() {
		harness := harness
		t.Run(harness.Name, func(t *testing.T) {
			built := harness.New(t)
			defer built.Shutdown(t)

			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()

			reqA := jobdb.SubmitJobRequest{
				Job: jobdb.SubmitJob{
					TenantId: "tenant-explicit-scope-a-" + harness.Name,
					JobID:    "shared-explicit-id",
					JobType:  "tenant-scope-job",
					Data:     jobdbtest.NumberTaskData(1),
				},
				RequestTime: time.Now().UTC(),
			}
			reqB := jobdb.SubmitJobRequest{
				Job: jobdb.SubmitJob{
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

			listedA, err := built.Runtime.ListJobs(ctx, jobdb.ListJobsRequest{
				TenantIds: []string{reqA.Job.TenantId},
				JobKeys:   []jobdb.JobKey{handleA.JobKey},
				PageSize:  10,
			})
			if err != nil {
				t.Fatalf("list tenant A explicit job id: %v", err)
			}
			if len(listedA.Jobs) != 1 {
				t.Fatalf("expected 1 tenant A logical job, got %d", len(listedA.Jobs))
			}

			listedB, err := built.Runtime.ListJobs(ctx, jobdb.ListJobsRequest{
				TenantIds: []string{reqB.Job.TenantId},
				JobKeys:   []jobdb.JobKey{handleB.JobKey},
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
	for _, harness := range jobdbtest.BuiltInRuntimeHarnesses() {
		harness := harness
		t.Run(harness.Name, func(t *testing.T) {
			built := harness.New(t)
			defer built.Shutdown(t)

			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()

			matching, err := built.Runtime.SubmitJob(ctx, jobdb.SubmitJobRequest{
				Job: jobdb.SubmitJob{
					TenantId: built.WorkerTenantID,
					JobID:    "metadata-blue-" + harness.Name,
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
					JobID:    "metadata-green-" + harness.Name,
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

func TestWorkflowRuntimeGetJobLeaseAcrossBuiltInRuntimes(t *testing.T) {
	for _, harness := range jobdbtest.BuiltInRuntimeHarnesses() {
		harness := harness
		t.Run(harness.Name, func(t *testing.T) {
			built := harness.New(t)
			defer built.Shutdown(t)

			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()

			handle, err := built.Runtime.SubmitJob(ctx, jobdb.SubmitJobRequest{
				Job: jobdb.SubmitJob{
					TenantId: built.WorkerTenantID,
					JobID:    "targeted-job-" + harness.Name,
					JobType:  "targeted-job",
					Data:     jobdbtest.NumberTaskData(5),
				},
				RequestTime: time.Now().UTC(),
			})
			if err != nil {
				t.Fatalf("submit targeted lease job: %v", err)
			}

			lease, err := built.Runtime.GetJobLease(ctx, jobdb.GetJobLeaseRequest{
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

			miss, err := built.Runtime.GetJobLease(ctx, jobdb.GetJobLeaseRequest{
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

			completeLeaseForTest(t, ctx, lease, 1)
			jobdbtest.WaitForRuntimeStatus(t, ctx, built.Runtime, handle.JobKey, jobdb.JobStatusCompleted)

			miss, err = built.Runtime.GetJobLease(ctx, jobdb.GetJobLeaseRequest{
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
	for _, harness := range jobdbtest.BuiltInRuntimeHarnesses() {
		harness := harness
		t.Run(harness.Name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
			defer cancel()

			t.Run("completed", func(t *testing.T) {
				built := harness.New(t)
				defer built.Shutdown(t)

				handle, err := built.Runtime.SubmitJob(ctx, jobdb.SubmitJobRequest{
					Job: jobdb.SubmitJob{
						TenantId: built.WorkerTenantID,
						JobID:    "run-job-complete-" + harness.Name,
						JobType:  jobdbtest.SequenceJobName,
						Data:     jobdbtest.NumberTaskData(7),
					},
					RequestTime: time.Now().UTC(),
				})
				if err != nil {
					t.Fatalf("submit direct-run complete job: %v", err)
				}

				runnable, err := workflow.GetJobForRun(ctx, built.Runtime, workflow.GetJobForRunRequest{
					JobKey:    handle.JobKey,
					JobWorker: jobdbtest.SequenceJob{},
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
				if outcome.Status != workflow.JobRunCompleted {
					t.Fatalf("unexpected outcome status %q", outcome.Status)
				}
				if !outcome.LeaseAcquired {
					t.Fatal("expected helper to acquire the lease")
				}
				if got := jobdbtest.MustDecodeNumberTaskData(t, outcome.Output); got != 7 {
					t.Fatalf("unexpected output: got %d want 7", got)
				}

				replayed, err := workflow.GetJobForRun(ctx, built.Runtime, workflow.GetJobForRunRequest{
					JobKey:    handle.JobKey,
					JobWorker: jobdbtest.SequenceJob{},
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
				if cached.Status != workflow.JobRunCompleted {
					t.Fatalf("unexpected cached outcome status %q", cached.Status)
				}
				replayedOutcome, err := replayed.Run(nil)
				if err != nil {
					t.Fatalf("run completed job runnable: %v", err)
				}
				if got := jobdbtest.MustDecodeNumberTaskData(t, replayedOutcome.Output); got != 7 {
					t.Fatalf("unexpected replayed output: got %d want 7", got)
				}
			})

			t.Run("failed", func(t *testing.T) {
				built := harness.New(t)
				defer built.Shutdown(t)

				handle, err := built.Runtime.SubmitJob(ctx, jobdb.SubmitJobRequest{
					Job: jobdb.SubmitJob{
						TenantId: built.WorkerTenantID,
						JobID:    "run-job-fail-" + harness.Name,
						JobType:  jobdbtest.FailingJobName,
						Data:     jobdbtest.NumberTaskData(1),
					},
					RequestTime: time.Now().UTC(),
				})
				if err != nil {
					t.Fatalf("submit direct-run failing job: %v", err)
				}

				runnable, err := workflow.GetJobForRun(ctx, built.Runtime, workflow.GetJobForRunRequest{
					JobKey:    handle.JobKey,
					JobWorker: jobdbtest.FailingJob{},
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
				if outcome.Status != workflow.JobRunFailed {
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

				handle, err := built.Runtime.SubmitJob(ctx, jobdb.SubmitJobRequest{
					Job: jobdb.SubmitJob{
						TenantId: built.WorkerTenantID,
						JobID:    "run-job-suspended-" + harness.Name,
						JobType:  jobdbtest.SequenceJobName,
						Data:     jobdbtest.NumberTaskData(3),
					},
					RequestTime: time.Now().UTC(),
				})
				if err != nil {
					t.Fatalf("submit direct-run suspended job: %v", err)
				}

				runnable, err := workflow.GetJobForRun(ctx, built.Runtime, workflow.GetJobForRunRequest{
					JobKey:    handle.JobKey,
					JobWorker: jobdbtest.SequenceJob{Steps: []string{jobdbtest.MissingTaskName}},
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
				if outcome.Status != workflow.JobRunSuspended {
					t.Fatalf("unexpected outcome status %q", outcome.Status)
				}
				if !outcome.LeaseAcquired {
					t.Fatal("expected helper to acquire the lease")
				}
				wantCapability := jobdbtest.SequenceJobName + ":" + jobdbtest.MissingTaskName
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

				handle, err := built.Runtime.SubmitJob(ctx, jobdb.SubmitJobRequest{
					Job: jobdb.SubmitJob{
						TenantId: built.WorkerTenantID,
						JobID:    "run-job-active-" + harness.Name,
						JobType:  jobdbtest.SequenceJobName,
						Data:     jobdbtest.NumberTaskData(9),
					},
					RequestTime: time.Now().UTC(),
				})
				if err != nil {
					t.Fatalf("submit direct-run active job: %v", err)
				}

				lease, err := built.Runtime.GetJobLease(ctx, jobdb.GetJobLeaseRequest{
					JobKey:       handle.JobKey,
					WorkerID:     "held-lease-worker",
					Capabilities: []string{jobdbtest.SequenceJobName},
				})
				if err != nil {
					t.Fatalf("hold targeted lease: %v", err)
				}
				if lease == nil {
					t.Fatal("expected targeted lease to be acquired")
				}

				runnable, err := workflow.GetJobForRun(ctx, built.Runtime, workflow.GetJobForRunRequest{
					JobKey:    handle.JobKey,
					JobWorker: jobdbtest.SequenceJob{},
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
				if cached.Status != workflow.JobRunNotLeaseable {
					t.Fatalf("unexpected cached outcome status %q", cached.Status)
				}
				outcome, err := runnable.Run(nil)
				if err != nil {
					t.Fatalf("run active job runnable: %v", err)
				}
				if outcome.Status != workflow.JobRunNotLeaseable {
					t.Fatalf("unexpected outcome status %q", outcome.Status)
				}
				if outcome.LeaseAcquired {
					t.Fatal("expected helper not to acquire the held lease")
				}
				if outcome.JobStatus == nil || *outcome.JobStatus != jobdb.JobStatusActive {
					t.Fatalf("unexpected job status %+v", outcome.JobStatus)
				}

				completeLeaseForTest(t, ctx, lease, 1)
			})
		})
	}
}
