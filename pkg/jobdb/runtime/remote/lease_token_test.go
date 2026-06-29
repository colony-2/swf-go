package remote

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/leaseauth"
	"github.com/colony-2/jobdb/pkg/jobdb/internal/runtimeapi"
)

func TestLeaseTokenMintForLeaseUsesExpiryAndSchemaHash(t *testing.T) {
	signer := &leaseTokenSigner{key: bytes.Repeat([]byte{7}, 32)}
	jobKey := jobdb.JobKey{TenantId: "tenant-token", JobId: "job-token"}
	leaseExpiresAt := time.Now().UTC().Add(5 * time.Second)

	token, err := signer.mintForLease(&testExecutionLease{
		jobKey:      jobKey,
		leaseID:     "lease-token",
		workerID:    "worker-token",
		expiresAt:   leaseExpiresAt,
		schemaHash:  "sha256:schema",
		capability:  "cap",
		payloadJSON: json.RawMessage(`{"ok":true}`),
	}, 30*time.Second)
	if err != nil {
		t.Fatalf("mint token: %v", err)
	}
	claims, err := signer.validateAndParse(token, jobKey, "lease-token", time.Now().UTC())
	if err != nil {
		t.Fatalf("parse token: %v", err)
	}
	if claims.SchemaHash != "sha256:schema" {
		t.Fatalf("schema hash = %q, want sha256:schema", claims.SchemaHash)
	}
	if !claims.expiresAt().Before(leaseExpiresAt) {
		t.Fatalf("token expiry %s should be before lease expiry %s", claims.expiresAt(), leaseExpiresAt)
	}
	if diff := leaseExpiresAt.Sub(claims.expiresAt()); diff <= 0 || diff > time.Second {
		t.Fatalf("token expiry skew = %s, want >0 and <=1s", diff)
	}
	if got := claims.leaseDuration(); got < 4*time.Second || got > 6*time.Second {
		t.Fatalf("lease duration = %s, want roughly 5s", got)
	}
}

func TestAddChapterWithLeasePassesValidatedClaimsToRuntime(t *testing.T) {
	signer := &leaseTokenSigner{key: bytes.Repeat([]byte{9}, 32)}
	jobKey := jobdb.JobKey{TenantId: "tenant-server", JobId: "job-server"}
	leaseID := "lease-server"
	schemaHash := "sha256:server-schema"
	token, err := signer.mintForLeaseExpiry(jobKey, leaseID, "worker-server", schemaHash, time.Now().UTC().Add(5*time.Second), 5*time.Second)
	if err != nil {
		t.Fatalf("mint token: %v", err)
	}
	runtime := &claimsCapturingRuntime{}
	server := &proxyServer{runtime: runtime, tokens: signer}

	chapter := jobdb.Chapter{
		Ordinal:   1,
		TaskType:  "server-task",
		CreatedAt: time.Now().UTC(),
		Body: jobdb.TaskAttemptOutcomeChapter{Outcome: jobdb.ApplicationOutputOutcome{
			Output: jobdb.ApplicationOutputBytes{Data: []byte(`{"ok":true}`)},
		}},
	}
	body, err := runtimeChapterToAddRequest(context.Background(), chapter, nil)
	if err != nil {
		t.Fatalf("build add chapter body: %v", err)
	}
	resp, err := server.AddChapterWithLease(context.Background(), runtimeapi.AddChapterWithLeaseRequestObject{
		TenantId: jobKey.TenantId,
		JobId:    jobKey.JobId,
		LeaseId:  leaseID,
		Params:   runtimeapi.AddChapterWithLeaseParams{XJobDBLeaseToken: token},
		Body:     &body,
	})
	if err != nil {
		t.Fatalf("add chapter with lease: %v", err)
	}
	if _, ok := resp.(runtimeapi.AddChapterWithLease204Response); !ok {
		t.Fatalf("response = %T, want AddChapterWithLease204Response", resp)
	}
	if runtime.req.LeaseID != leaseID || runtime.req.Ref.JobKey != jobKey {
		t.Fatalf("unexpected put chapter request %+v", runtime.req)
	}
	if !runtime.sawClaims {
		t.Fatal("runtime did not receive lease claims")
	}
	if runtime.claims.SchemaHash != schemaHash {
		t.Fatalf("schema hash = %q, want %q", runtime.claims.SchemaHash, schemaHash)
	}
	if !leaseauth.Matches(runtime.claims, jobKey, leaseID) {
		t.Fatalf("claims do not match job/lease: %+v", runtime.claims)
	}
}

type testExecutionLease struct {
	jobKey      jobdb.JobKey
	leaseID     string
	workerID    string
	expiresAt   time.Time
	schemaHash  string
	capability  string
	payloadJSON json.RawMessage
}

func (l *testExecutionLease) LeaseID() string      { return l.leaseID }
func (l *testExecutionLease) Job() jobdb.JobHandle { return jobdb.JobHandle{JobKey: l.jobKey} }
func (l *testExecutionLease) Capability() string   { return l.capability }
func (l *testExecutionLease) Payload() json.RawMessage {
	return append(json.RawMessage(nil), l.payloadJSON...)
}
func (l *testExecutionLease) KeepAlive(context.Context) error { return nil }
func (l *testExecutionLease) StopKeepAlive()                  {}
func (l *testExecutionLease) Complete(context.Context, jobdb.CompleteExecutionRequest) error {
	return nil
}
func (l *testExecutionLease) Reschedule(context.Context, jobdb.RescheduleExecutionRequest) error {
	return nil
}
func (l *testExecutionLease) SubmitJob(context.Context, jobdb.SubmitJobRequest) (jobdb.JobHandle, error) {
	return jobdb.JobHandle{}, nil
}
func (l *testExecutionLease) SubmitRestartJob(context.Context, jobdb.SubmitRestartJobRequest) (jobdb.JobHandle, error) {
	return jobdb.JobHandle{}, nil
}
func (l *testExecutionLease) LeaseWorkerID() string   { return l.workerID }
func (l *testExecutionLease) LeaseExpiry() time.Time  { return l.expiresAt }
func (l *testExecutionLease) LeaseSchemaHash() string { return l.schemaHash }

type claimsCapturingRuntime struct {
	req       jobdb.PutChapterRequest
	claims    leaseauth.Claims
	sawClaims bool
}

func (r *claimsCapturingRuntime) SubmitJob(context.Context, jobdb.SubmitJobRequest) (jobdb.JobHandle, error) {
	return jobdb.JobHandle{}, errors.New("unexpected SubmitJob")
}

func (r *claimsCapturingRuntime) SubmitRestartJob(context.Context, jobdb.SubmitRestartJobRequest) (jobdb.JobHandle, error) {
	return jobdb.JobHandle{}, errors.New("unexpected SubmitRestartJob")
}

func (r *claimsCapturingRuntime) PollWork(context.Context, jobdb.PollWorkRequest) ([]jobdb.ExecutionLease, error) {
	return nil, errors.New("unexpected PollWork")
}

func (r *claimsCapturingRuntime) GetJobLease(context.Context, jobdb.GetJobLeaseRequest) (jobdb.ExecutionLease, error) {
	return nil, errors.New("unexpected GetJobLease")
}

func (r *claimsCapturingRuntime) CancelJob(context.Context, jobdb.CancelJobRequest) error {
	return errors.New("unexpected CancelJob")
}

func (r *claimsCapturingRuntime) CompleteTaskIfWaiting(context.Context, jobdb.CompleteTaskIfWaitingRequest) error {
	return errors.New("unexpected CompleteTaskIfWaiting")
}

func (r *claimsCapturingRuntime) UpsertSchedule(context.Context, jobdb.UpsertScheduleRequest) (jobdb.ScheduleInfo, error) {
	return jobdb.ScheduleInfo{}, errors.New("unexpected UpsertSchedule")
}

func (r *claimsCapturingRuntime) GetSchedule(context.Context, jobdb.ScheduleKey) (jobdb.ScheduleInfo, error) {
	return jobdb.ScheduleInfo{}, errors.New("unexpected GetSchedule")
}

func (r *claimsCapturingRuntime) ListSchedules(context.Context, jobdb.ListSchedulesRequest) (jobdb.ListSchedulesResponse, error) {
	return jobdb.ListSchedulesResponse{}, errors.New("unexpected ListSchedules")
}

func (r *claimsCapturingRuntime) PauseSchedule(context.Context, jobdb.ScheduleMutationRequest) (jobdb.ScheduleInfo, error) {
	return jobdb.ScheduleInfo{}, errors.New("unexpected PauseSchedule")
}

func (r *claimsCapturingRuntime) ResumeSchedule(context.Context, jobdb.ScheduleMutationRequest) (jobdb.ScheduleInfo, error) {
	return jobdb.ScheduleInfo{}, errors.New("unexpected ResumeSchedule")
}

func (r *claimsCapturingRuntime) ArchiveSchedule(context.Context, jobdb.ScheduleMutationRequest) (jobdb.ScheduleInfo, error) {
	return jobdb.ScheduleInfo{}, errors.New("unexpected ArchiveSchedule")
}

func (r *claimsCapturingRuntime) TriggerSchedule(context.Context, jobdb.TriggerScheduleRequest) (jobdb.JobHandle, error) {
	return jobdb.JobHandle{}, errors.New("unexpected TriggerSchedule")
}

func (r *claimsCapturingRuntime) ListScheduleRuns(context.Context, jobdb.ListScheduleRunsRequest) (jobdb.ListScheduleRunsResponse, error) {
	return jobdb.ListScheduleRunsResponse{}, errors.New("unexpected ListScheduleRuns")
}

func (r *claimsCapturingRuntime) GetJob(context.Context, jobdb.JobKey) (jobdb.JobInfo, error) {
	return jobdb.JobInfo{}, errors.New("unexpected GetJob")
}

func (r *claimsCapturingRuntime) ListJobs(context.Context, jobdb.ListJobsRequest) (jobdb.ListJobsResponse, error) {
	return jobdb.ListJobsResponse{}, errors.New("unexpected ListJobs")
}

func (r *claimsCapturingRuntime) GetChapter(context.Context, jobdb.ChapterRef) (jobdb.Chapter, error) {
	return jobdb.Chapter{}, errors.New("unexpected GetChapter")
}

func (r *claimsCapturingRuntime) ListChapters(context.Context, jobdb.ListChaptersRequest) ([]jobdb.Chapter, error) {
	return nil, errors.New("unexpected ListChapters")
}

func (r *claimsCapturingRuntime) PutChapter(ctx context.Context, req jobdb.PutChapterRequest) error {
	r.req = req
	var ok bool
	r.claims, ok = leaseauth.ClaimsFromContext(ctx)
	r.sawClaims = ok
	if !ok {
		return errors.New("missing lease claims")
	}
	if !leaseauth.Matches(r.claims, req.Ref.JobKey, req.LeaseID) {
		return jobdb.ErrExecutionLeaseLost
	}
	return nil
}

func (r *claimsCapturingRuntime) OpenArtifact(context.Context, jobdb.ArtifactRef) (jobdb.ArtifactReader, error) {
	return nil, errors.New("unexpected OpenArtifact")
}

var _ jobdb.ExecutionLease = (*testExecutionLease)(nil)
var _ jobdb.WorkflowRuntime = (*claimsCapturingRuntime)(nil)
