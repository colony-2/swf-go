package sqlite

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
)

func TestScheduleMetadataIsHiddenFromPublicJobViews(t *testing.T) {
	ctx := context.Background()
	embedded, err := StartEmbeddedRuntime(ctx)
	if err != nil {
		t.Fatalf("start runtime: %v", err)
	}
	defer embedded.Shutdown()

	now := time.Now().UTC()
	info := upsertScheduleForTest(t, ctx, embedded.Runtime, "schedule-hidden", now, time.Hour)
	if info.NextJobKey == nil {
		t.Fatal("expected first scheduled job")
	}
	row, err := embedded.Runtime.loadJobRow(ctx, *info.NextJobKey)
	if err != nil {
		t.Fatalf("load scheduled job row: %v", err)
	}
	assertInternalScheduleMetadata(t, row.metadata, "schedule-hidden")

	resp, err := embedded.Runtime.ListJobs(ctx, jobdb.ListJobsRequest{
		TenantIds: []string{"tenant"},
		JobKeys:   []jobdb.JobKey{*info.NextJobKey},
	})
	if err != nil {
		t.Fatalf("list jobs: %v", err)
	}
	if len(resp.Jobs) != 1 {
		t.Fatalf("listed jobs = %d, want 1", len(resp.Jobs))
	}
	assertPublicScheduleMetadata(t, resp.Jobs[0].Metadata)

	filter, err := jobdb.Metadata().EqualFilter("customer", "acme")
	if err != nil {
		t.Fatalf("metadata filter: %v", err)
	}
	resp, err = embedded.Runtime.ListJobs(ctx, jobdb.ListJobsRequest{
		TenantIds:      []string{"tenant"},
		MetadataFilter: filter,
	})
	if err != nil {
		t.Fatalf("list jobs by app metadata: %v", err)
	}
	if len(resp.Jobs) != 1 {
		t.Fatalf("app metadata jobs = %d, want 1", len(resp.Jobs))
	}
	assertPublicScheduleMetadata(t, resp.Jobs[0].Metadata)

	prefixFilter, err := jobdb.Metadata().EqualFilter("jobdb_customer", "visible")
	if err != nil {
		t.Fatalf("jobdb-prefixed app metadata filter: %v", err)
	}
	resp, err = embedded.Runtime.ListJobs(ctx, jobdb.ListJobsRequest{
		TenantIds:      []string{"tenant"},
		MetadataFilter: prefixFilter,
	})
	if err != nil {
		t.Fatalf("list jobs by jobdb-prefixed app metadata: %v", err)
	}
	if len(resp.Jobs) != 1 {
		t.Fatalf("jobdb-prefixed app metadata jobs = %d, want 1", len(resp.Jobs))
	}
	assertPublicScheduleMetadata(t, resp.Jobs[0].Metadata)

	internalNameFilter, err := jobdb.Metadata().EqualFilter("_jobdb", "app-visible")
	if err != nil {
		t.Fatalf("_jobdb app metadata filter should be allowed: %v", err)
	}
	resp, err = embedded.Runtime.ListJobs(ctx, jobdb.ListJobsRequest{
		TenantIds:      []string{"tenant"},
		MetadataFilter: internalNameFilter,
	})
	if err != nil {
		t.Fatalf("list jobs by _jobdb app metadata: %v", err)
	}
	if len(resp.Jobs) != 1 {
		t.Fatalf("_jobdb app metadata jobs = %d, want 1", len(resp.Jobs))
	}
	assertPublicScheduleMetadata(t, resp.Jobs[0].Metadata)

	scheduleFieldFilter, err := jobdb.Metadata().EqualFilter("jobdb_schedule_id", "schedule-hidden")
	if err != nil {
		t.Fatalf("jobdb-prefixed schedule-looking app metadata filter should be allowed: %v", err)
	}
	resp, err = embedded.Runtime.ListJobs(ctx, jobdb.ListJobsRequest{
		TenantIds:      []string{"tenant"},
		MetadataFilter: scheduleFieldFilter,
	})
	if err != nil {
		t.Fatalf("list jobs by schedule-looking app metadata: %v", err)
	}
	if len(resp.Jobs) != 0 {
		t.Fatalf("schedule-looking app metadata jobs = %d, want 0", len(resp.Jobs))
	}

	runs, err := embedded.Runtime.ListScheduleRuns(ctx, jobdb.ListScheduleRunsRequest{ScheduleKey: info.ScheduleKey})
	if err != nil {
		t.Fatalf("list schedule runs: %v", err)
	}
	if len(runs.Runs) != 1 {
		t.Fatalf("schedule runs = %d, want 1", len(runs.Runs))
	}
	assertPublicScheduleMetadata(t, runs.Runs[0].JobSummary.Metadata)
}

func TestScheduleLeasePreflightSubmitsSerialSuccessorBeforeReturningLease(t *testing.T) {
	ctx := context.Background()
	embedded, err := StartEmbeddedRuntime(ctx)
	if err != nil {
		t.Fatalf("start runtime: %v", err)
	}
	defer embedded.Shutdown()

	now := time.Now().UTC()
	info := upsertScheduleForTest(t, ctx, embedded.Runtime, "schedule-successor", now, time.Hour)
	if info.NextJobKey == nil {
		t.Fatal("expected first scheduled job")
	}

	leases, err := embedded.Runtime.PollWork(ctx, jobdb.PollWorkRequest{
		TenantId:     "tenant",
		WorkerID:     "worker-a",
		Capabilities: []string{"scheduled-job"},
		Limit:        1,
	})
	if err != nil {
		t.Fatalf("poll work: %v", err)
	}
	if len(leases) != 1 {
		t.Fatalf("leases = %d, want 1", len(leases))
	}
	firstJobID := leases[0].Job().JobKey.JobId
	if firstJobID != info.NextJobKey.JobId {
		t.Fatalf("leased job = %s, want %s", firstJobID, info.NextJobKey.JobId)
	}

	runs, err := embedded.Runtime.ListScheduleRuns(ctx, jobdb.ListScheduleRunsRequest{ScheduleKey: info.ScheduleKey})
	if err != nil {
		t.Fatalf("list schedule runs: %v", err)
	}
	if len(runs.Runs) != 2 {
		t.Fatalf("schedule runs = %d, want 2", len(runs.Runs))
	}
	var sawFirst, sawSuccessor bool
	for _, run := range runs.Runs {
		assertPublicScheduleMetadata(t, run.JobSummary.Metadata)
		switch run.JobSummary.JobKey.JobId {
		case firstJobID:
			sawFirst = true
			if run.JobSummary.Status != jobdb.JobStatusActive {
				t.Fatalf("first status = %s, want ACTIVE", run.JobSummary.Status)
			}
		default:
			sawSuccessor = true
			if len(run.JobSummary.WaitFor) != 1 || run.JobSummary.WaitFor[0] != firstJobID {
				t.Fatalf("successor waitFor = %+v, want [%s]", run.JobSummary.WaitFor, firstJobID)
			}
			if run.JobSummary.Status != jobdb.JobStatusPendingJobs {
				t.Fatalf("successor status = %s, want PENDING_JOBS", run.JobSummary.Status)
			}
		}
	}
	if !sawFirst || !sawSuccessor {
		t.Fatalf("saw first=%v successor=%v in runs %+v", sawFirst, sawSuccessor, runs.Runs)
	}
}

func TestScheduledTargetArtifactsAreSnapshottedAndReplayed(t *testing.T) {
	ctx := context.Background()
	embedded, err := StartEmbeddedRuntime(ctx)
	if err != nil {
		t.Fatalf("start runtime: %v", err)
	}
	defer embedded.Shutdown()

	sourcePath := filepath.Join(t.TempDir(), "input.txt")
	want := []byte("scheduled artifact input")
	if err := os.WriteFile(sourcePath, want, 0644); err != nil {
		t.Fatalf("write source artifact: %v", err)
	}
	artifact, err := jobdb.NewArtifactFromFileNoCleanup("input.txt", sourcePath)
	if err != nil {
		t.Fatalf("create source artifact: %v", err)
	}
	info, err := embedded.Runtime.UpsertSchedule(ctx, jobdb.UpsertScheduleRequest{
		TenantId:      "tenant",
		ScheduleId:    "schedule-artifact-target",
		RequestTime:   time.Now().UTC(),
		OverlapPolicy: jobdb.ScheduleOverlapSerial,
		Trigger: jobdb.ScheduleTrigger{
			Kind:     jobdb.ScheduleTriggerInterval,
			Interval: time.Hour,
		},
		Target: jobdb.ScheduleTarget{
			JobType:  "scheduled-job",
			Data:     jobdb.JobData(&jobdb.SimpleTaskData{Data: []byte(`{"n":1}`), Artifacts: []jobdb.Artifact{artifact}}),
			Metadata: json.RawMessage(`{"customer":"acme"}`),
		},
	})
	if err != nil {
		t.Fatalf("upsert schedule: %v", err)
	}
	if info.NextJobKey == nil {
		t.Fatal("expected first scheduled job")
	}
	if err := os.Remove(sourcePath); err != nil {
		t.Fatalf("remove source artifact: %v", err)
	}
	assertScheduleStartArtifact(t, ctx, embedded.Runtime, *info.NextJobKey, "input.txt", want)

	leases, err := embedded.Runtime.PollWork(ctx, jobdb.PollWorkRequest{
		TenantId:     "tenant",
		WorkerID:     "worker-a",
		Capabilities: []string{"scheduled-job"},
		Limit:        1,
	})
	if err != nil {
		t.Fatalf("poll first run: %v", err)
	}
	if len(leases) != 1 {
		t.Fatalf("leases = %d, want 1", len(leases))
	}

	runs, err := embedded.Runtime.ListScheduleRuns(ctx, jobdb.ListScheduleRunsRequest{ScheduleKey: info.ScheduleKey})
	if err != nil {
		t.Fatalf("list schedule runs: %v", err)
	}
	var successor jobdb.JobKey
	var foundSuccessor bool
	for _, run := range runs.Runs {
		if run.JobSummary.JobKey != *info.NextJobKey {
			successor = run.JobSummary.JobKey
			foundSuccessor = true
			break
		}
	}
	if !foundSuccessor {
		t.Fatalf("expected successor run in %+v", runs.Runs)
	}
	assertScheduleStartArtifact(t, ctx, embedded.Runtime, successor, "input.txt", want)
}

func TestPausedScheduleCancelsUnstartedOccurrenceWithReason(t *testing.T) {
	ctx := context.Background()
	embedded, err := StartEmbeddedRuntime(ctx)
	if err != nil {
		t.Fatalf("start runtime: %v", err)
	}
	defer embedded.Shutdown()

	info := upsertScheduleForTest(t, ctx, embedded.Runtime, "schedule-paused", time.Now().UTC(), time.Hour)
	if _, err := embedded.Runtime.PauseSchedule(ctx, jobdb.ScheduleMutationRequest{ScheduleKey: info.ScheduleKey}); err != nil {
		t.Fatalf("pause schedule: %v", err)
	}

	leases, err := embedded.Runtime.PollWork(ctx, jobdb.PollWorkRequest{
		TenantId:     "tenant",
		WorkerID:     "worker-a",
		Capabilities: []string{"scheduled-job"},
		Limit:        1,
	})
	if err != nil {
		t.Fatalf("poll work: %v", err)
	}
	if len(leases) != 0 {
		t.Fatalf("leases = %d, want 0", len(leases))
	}

	runs, err := embedded.Runtime.ListScheduleRuns(ctx, jobdb.ListScheduleRunsRequest{
		ScheduleKey: info.ScheduleKey,
		Statuses:    []jobdb.JobStatus{jobdb.JobStatusCancelled},
	})
	if err != nil {
		t.Fatalf("list schedule runs: %v", err)
	}
	if len(runs.Runs) != 1 {
		t.Fatalf("cancelled schedule runs = %d, want 1", len(runs.Runs))
	}
	if runs.Runs[0].ReasonCode != "schedule_paused" {
		t.Fatalf("reason = %q, want schedule_paused", runs.Runs[0].ReasonCode)
	}
	assertPublicScheduleMetadata(t, runs.Runs[0].JobSummary.Metadata)
}

func TestScheduleRejectsInvalidTargetMetadataAndStateConflicts(t *testing.T) {
	ctx := context.Background()
	embedded, err := StartEmbeddedRuntime(ctx)
	if err != nil {
		t.Fatalf("start runtime: %v", err)
	}
	defer embedded.Shutdown()

	_, err = embedded.Runtime.UpsertSchedule(ctx, jobdb.UpsertScheduleRequest{
		TenantId:   "tenant",
		ScheduleId: "invalid-metadata",
		Trigger: jobdb.ScheduleTrigger{
			Kind:     jobdb.ScheduleTriggerInterval,
			Interval: time.Hour,
		},
		Target: jobdb.ScheduleTarget{
			JobType:  "scheduled-job",
			Data:     jobdb.JobData(jobdb.NewTaskDataOrPanic(map[string]any{"n": 1})),
			Metadata: json.RawMessage(`["not","object"]`),
		},
	})
	if err == nil || !strings.Contains(err.Error(), "metadata must be a JSON object") {
		t.Fatalf("expected invalid metadata error, got %v", err)
	}

	info := upsertScheduleForTest(t, ctx, embedded.Runtime, "schedule-conflict", time.Now().UTC(), time.Hour)
	badGeneration := info.Generation + 1
	_, err = embedded.Runtime.PauseSchedule(ctx, jobdb.ScheduleMutationRequest{
		ScheduleKey:        info.ScheduleKey,
		ExpectedGeneration: &badGeneration,
	})
	if !errors.Is(err, jobdb.ErrConflict) {
		t.Fatalf("expected generation conflict, got %v", err)
	}

	if _, err := embedded.Runtime.ArchiveSchedule(ctx, jobdb.ScheduleMutationRequest{ScheduleKey: info.ScheduleKey}); err != nil {
		t.Fatalf("archive schedule: %v", err)
	}
	_, err = embedded.Runtime.ResumeSchedule(ctx, jobdb.ScheduleMutationRequest{ScheduleKey: info.ScheduleKey})
	if !errors.Is(err, jobdb.ErrConflict) {
		t.Fatalf("expected archived schedule conflict, got %v", err)
	}
}

func TestScheduleFailurePolicyCancelsSuccessorBeforeAppLease(t *testing.T) {
	ctx := context.Background()
	embedded, err := StartEmbeddedRuntime(ctx)
	if err != nil {
		t.Fatalf("start runtime: %v", err)
	}
	defer embedded.Shutdown()

	info, err := embedded.Runtime.UpsertSchedule(ctx, jobdb.UpsertScheduleRequest{
		TenantId:      "tenant",
		ScheduleId:    "schedule-failure-policy",
		RequestTime:   time.Now().UTC(),
		OverlapPolicy: jobdb.ScheduleOverlapSerial,
		Trigger: jobdb.ScheduleTrigger{
			Kind:     jobdb.ScheduleTriggerInterval,
			Interval: time.Millisecond,
		},
		FailurePolicy: jobdb.ScheduleFailurePolicy{
			WindowSize:            2,
			MaxSequentialFailures: 1,
		},
		Target: jobdb.ScheduleTarget{
			JobType:  "scheduled-job",
			Data:     jobdb.JobData(jobdb.NewTaskDataOrPanic(map[string]any{"n": 1})),
			Metadata: json.RawMessage(`{"customer":"acme","jobdb_customer":"visible","_jobdb":"app-visible"}`),
		},
	})
	if err != nil {
		t.Fatalf("upsert schedule: %v", err)
	}

	leases, err := embedded.Runtime.PollWork(ctx, jobdb.PollWorkRequest{
		TenantId:     "tenant",
		WorkerID:     "worker-a",
		Capabilities: []string{"scheduled-job"},
		Limit:        1,
	})
	if err != nil {
		t.Fatalf("poll first run: %v", err)
	}
	if len(leases) != 1 {
		t.Fatalf("first leases = %d, want 1", len(leases))
	}
	failedChapter := jobdb.Chapter{
		Ordinal:   1,
		TaskType:  leases[0].Capability(),
		CreatedAt: time.Now().UTC(),
		Body: jobdb.JobAttemptOutcomeChapter{Outcome: jobdb.AppErrorOutcome{
			Error: jobdb.AppErrorPayload{Message: "boom", Level: "error"},
		}},
	}
	if err := leases[0].Complete(ctx, jobdb.CompleteExecutionRequest{Status: "failed_app", Detail: "boom", Chapter: &failedChapter}); err != nil {
		t.Fatalf("complete first failed: %v", err)
	}
	time.Sleep(3 * time.Millisecond)

	leases, err = embedded.Runtime.PollWork(ctx, jobdb.PollWorkRequest{
		TenantId:     "tenant",
		WorkerID:     "worker-a",
		Capabilities: []string{"scheduled-job"},
		Limit:        1,
	})
	if err != nil {
		t.Fatalf("poll successor: %v", err)
	}
	if len(leases) != 0 {
		t.Fatalf("successor leases = %d, want 0", len(leases))
	}

	runs, err := embedded.Runtime.ListScheduleRuns(ctx, jobdb.ListScheduleRunsRequest{
		ScheduleKey: info.ScheduleKey,
		Statuses:    []jobdb.JobStatus{jobdb.JobStatusCancelled},
	})
	if err != nil {
		t.Fatalf("list cancelled runs: %v", err)
	}
	if len(runs.Runs) != 1 {
		t.Fatalf("cancelled runs = %d, want 1", len(runs.Runs))
	}
	if runs.Runs[0].ReasonCode != "failure_policy" {
		t.Fatalf("reason = %q, want failure_policy", runs.Runs[0].ReasonCode)
	}
}

func TestStartedScheduleRunIsReleasableAfterPauseAndLeaseExpiry(t *testing.T) {
	ctx := context.Background()
	embedded, err := StartEmbeddedRuntime(ctx)
	if err != nil {
		t.Fatalf("start runtime: %v", err)
	}
	defer embedded.Shutdown()

	info := upsertScheduleForTest(t, ctx, embedded.Runtime, "schedule-started-recovery", time.Now().UTC(), time.Hour)
	leases, err := embedded.Runtime.PollWork(ctx, jobdb.PollWorkRequest{
		TenantId:      "tenant",
		WorkerID:      "worker-a",
		Capabilities:  []string{"scheduled-job"},
		Limit:         1,
		LeaseDuration: 25 * time.Millisecond,
	})
	if err != nil {
		t.Fatalf("poll initial lease: %v", err)
	}
	if len(leases) != 1 {
		t.Fatalf("initial leases = %d, want 1", len(leases))
	}
	jobKey := leases[0].Job().JobKey

	if err := embedded.Runtime.PutChapter(ctx, jobdb.PutChapterRequest{
		LeaseID: leases[0].LeaseID(),
		Ref:     jobdb.ChapterRef{JobKey: jobKey, Ordinal: 1},
		Chapter: jobdb.Chapter{
			Ordinal:   1,
			TaskType:  "scheduled-job",
			CreatedAt: time.Now().UTC(),
			Body: jobdb.TaskAttemptOutcomeChapter{
				Outcome: jobdb.ApplicationOutputOutcome{Output: jobdb.ApplicationOutputBytes{Data: []byte(`{"started":true}`)}},
			},
		},
	}); err != nil {
		t.Fatalf("put started chapter: %v", err)
	}
	if _, err := embedded.Runtime.PauseSchedule(ctx, jobdb.ScheduleMutationRequest{ScheduleKey: info.ScheduleKey}); err != nil {
		t.Fatalf("pause schedule: %v", err)
	}
	time.Sleep(40 * time.Millisecond)

	leases, err = embedded.Runtime.PollWork(ctx, jobdb.PollWorkRequest{
		TenantId:     "tenant",
		WorkerID:     "worker-b",
		Capabilities: []string{"scheduled-job"},
		Limit:        1,
	})
	if err != nil {
		t.Fatalf("poll recovery lease: %v", err)
	}
	if len(leases) != 1 {
		t.Fatalf("recovery leases = %d, want 1", len(leases))
	}
	if leases[0].Job().JobKey != jobKey {
		t.Fatalf("recovery leased job = %+v, want %+v", leases[0].Job().JobKey, jobKey)
	}
}

func upsertScheduleForTest(t *testing.T, ctx context.Context, runtime *Runtime, scheduleID string, now time.Time, interval time.Duration) jobdb.ScheduleInfo {
	t.Helper()
	info, err := runtime.UpsertSchedule(ctx, jobdb.UpsertScheduleRequest{
		TenantId:      "tenant",
		ScheduleId:    scheduleID,
		RequestTime:   now,
		OverlapPolicy: jobdb.ScheduleOverlapSerial,
		Trigger: jobdb.ScheduleTrigger{
			Kind:     jobdb.ScheduleTriggerInterval,
			Interval: interval,
		},
		Target: jobdb.ScheduleTarget{
			JobType:  "scheduled-job",
			Data:     jobdb.JobData(jobdb.NewTaskDataOrPanic(map[string]any{"n": 1})),
			Metadata: json.RawMessage(`{"customer":"acme","jobdb_customer":"visible","_jobdb":"app-visible"}`),
		},
	})
	if err != nil {
		t.Fatalf("upsert schedule: %v", err)
	}
	return info
}

func assertScheduleStartArtifact(t *testing.T, ctx context.Context, runtime *Runtime, jobKey jobdb.JobKey, name string, want []byte) {
	t.Helper()
	chapter, err := runtime.GetChapter(ctx, jobdb.ChapterRef{JobKey: jobKey, Ordinal: 0})
	if err != nil {
		t.Fatalf("get start chapter for %s: %v", jobKey, err)
	}
	if len(chapter.Artifacts) != 1 {
		t.Fatalf("start artifacts for %s = %+v, want one", jobKey, chapter.Artifacts)
	}
	stored := chapter.Artifacts[0]
	if stored.Name != name {
		t.Fatalf("artifact name = %q, want %q", stored.Name, name)
	}
	if stored.Size != int64(len(want)) {
		t.Fatalf("artifact size = %d, want %d", stored.Size, len(want))
	}
	reader, err := runtime.OpenArtifact(ctx, jobdb.ArtifactRef{
		JobKey:  jobKey,
		Ordinal: 0,
		Name:    stored.Name,
		Digest:  stored.Digest,
	})
	if err != nil {
		t.Fatalf("open start artifact for %s: %v", jobKey, err)
	}
	rc, err := reader.Open()
	if err != nil {
		t.Fatalf("open artifact reader for %s: %v", jobKey, err)
	}
	defer func() { _ = rc.Close() }()
	got, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read start artifact for %s: %v", jobKey, err)
	}
	if string(got) != string(want) {
		t.Fatalf("artifact data = %q, want %q", string(got), string(want))
	}
}

func assertPublicScheduleMetadata(t *testing.T, raw json.RawMessage) {
	t.Helper()
	text := string(raw)
	if strings.Contains(text, "schedule_tick") {
		t.Fatalf("runtime schedule metadata leaked in public metadata: %s", text)
	}
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatalf("metadata JSON: %v", err)
	}
	var customer string
	if err := json.Unmarshal(fields["customer"], &customer); err != nil {
		t.Fatalf("customer metadata missing: %v in %s", err, text)
	}
	if customer != "acme" {
		t.Fatalf("customer = %q, want acme", customer)
	}
	var jobdbCustomer string
	if err := json.Unmarshal(fields["jobdb_customer"], &jobdbCustomer); err != nil {
		t.Fatalf("jobdb_customer metadata missing: %v in %s", err, text)
	}
	if jobdbCustomer != "visible" {
		t.Fatalf("jobdb_customer = %q, want visible", jobdbCustomer)
	}
	var jobdbValue string
	if err := json.Unmarshal(fields["_jobdb"], &jobdbValue); err != nil {
		t.Fatalf("_jobdb app metadata missing: %v in %s", err, text)
	}
	if jobdbValue != "app-visible" {
		t.Fatalf("_jobdb app metadata = %q, want app-visible", jobdbValue)
	}
}

func assertInternalScheduleMetadata(t *testing.T, raw json.RawMessage, scheduleID string) {
	t.Helper()
	var fields map[string]json.RawMessage
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatalf("stored metadata JSON: %v", err)
	}
	if _, ok := fields["app"]; !ok {
		t.Fatalf("stored metadata missing app namespace: %s", string(raw))
	}
	if _, ok := fields["internal"]; !ok {
		t.Fatalf("stored metadata missing internal namespace: %s", string(raw))
	}
	legacyFields := []string{
		"jobdb_kind",
		"jobdb_schedule_id",
		"jobdb_schedule_generation",
		"jobdb_scheduled_at",
		"jobdb_schedule_run_id",
		"jobdb_schedule_manual",
		"jobdb_schedule_backfill_id",
	}
	for _, key := range legacyFields {
		if _, ok := fields[key]; ok {
			t.Fatalf("stored metadata has legacy top-level schedule field %q: %s", key, string(raw))
		}
	}
	var envelope jobdb.JobMetadataEnvelope
	if err := json.Unmarshal(raw, &envelope); err != nil {
		t.Fatalf("metadata envelope JSON: %v", err)
	}
	if len(envelope.App) == 0 {
		t.Fatalf("stored metadata missing app payload: %s", string(raw))
	}
	internal := envelope.Internal
	if internal == nil {
		t.Fatalf("stored metadata missing internal payload: %s", string(raw))
	}
	if internal.Schedule == nil {
		t.Fatalf("internal metadata missing schedule: %s", string(raw))
	}
	if internal.Schedule.Kind != jobdb.ScheduleMetadataKind {
		t.Fatalf("schedule metadata kind = %q, want %q", internal.Schedule.Kind, jobdb.ScheduleMetadataKind)
	}
	if internal.Schedule.ScheduleId != scheduleID {
		t.Fatalf("scheduleId = %q, want %q", internal.Schedule.ScheduleId, scheduleID)
	}
	if internal.Schedule.RunId == "" {
		t.Fatal("schedule metadata runId is required")
	}
}
