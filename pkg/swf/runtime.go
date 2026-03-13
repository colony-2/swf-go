package swf

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"time"
)

// WorkflowRuntime is the backend-agnostic facade used to isolate SWF from
// concrete persistence, artifact storage, and lease-management backends.
type WorkflowRuntime interface {
	// Job lifecycle
	StartJob(ctx context.Context, req StartJobRequest) (JobHandle, error)
	RestartJob(ctx context.Context, req RestartJobRequest) (JobHandle, error)
	CancelJob(ctx context.Context, req CancelJobRequest) error

	// Worker loop
	PollWork(ctx context.Context, req PollWorkRequest) ([]ExecutionLease, error)

	// Read APIs
	CheckJobStatus(ctx context.Context, jobKey JobKey) (JobStatus, error)
	GetJobResult(ctx context.Context, jobKey JobKey) (TaskData, error)
	GetJobRun(ctx context.Context, req GetJobRunRequest) (GetJobRunResponse, error)
	ListJobs(ctx context.Context, req ListJobsRequest) (ListJobsResponse, error)

	// Chapter / replay access
	GetChapter(ctx context.Context, ref ChapterRef) (StoredChapter, error)
	PutChapter(ctx context.Context, req PutChapterRequest) error

	// Artifact access
	OpenArtifact(ctx context.Context, ref ArtifactRef) (ArtifactReader, error)
	PutArtifacts(ctx context.Context, req PutArtifactsRequest) ([]StoredArtifact, error)
}

// RuntimeBuildOptions configures the shared worker engine built on top of a
// WorkflowRuntime.
type RuntimeBuildOptions struct {
	Logger                *slog.Logger
	MaxActive             int
	AwaitRecycleThreshold time.Duration
}

// JobHandle identifies a job owned by a runtime implementation.
type JobHandle struct {
	JobKey JobKey
}

// ExecutionLease represents a leased unit of work returned by PollWork.
type ExecutionLease interface {
	Job() JobHandle
	Capability() string
	Payload() json.RawMessage
	KeepAlive(ctx context.Context) error
	Complete(ctx context.Context, req CompleteExecutionRequest) error
	Reschedule(ctx context.Context, req RescheduleExecutionRequest) error
}

type StartJobRequest struct {
	Job         StartJob
	WorkerID    string
	RequestTime time.Time
}

type RestartJobRequest struct {
	Job         RestartJob
	WorkerID    string
	RequestTime time.Time
}

type CancelJobRequest struct {
	JobKey   JobKey
	Reason   string
	WorkerID string
}

type PollWorkRequest struct {
	WorkerID      string
	Capabilities  []string
	Limit         int
	LongPollUntil *time.Time
}

type ChapterRef struct {
	JobKey   JobKey
	Ordinal  int64
	Attempt  int
	TaskType string
}

type StoredArtifact struct {
	Name   string
	Digest string
	Size   int64
}

type StoredChapter struct {
	Ordinal     int64
	TaskType    string
	ChapterType string
	PayloadKind string
	InputHash   string
	CreatedAt   time.Time
	Metadata    json.RawMessage
	Data        json.RawMessage
	Artifacts   []StoredArtifact
}

type PutChapterRequest struct {
	Ref     ChapterRef
	Chapter StoredChapter
}

type ArtifactRef struct {
	JobKey  JobKey
	Ordinal int64
	Name    string
	Digest  string
}

type ArtifactReader interface {
	Open() (io.ReadCloser, error)
	Size() int64
	Name() string
}

type ArtifactUpload struct {
	Name string
	Size int64
	Open func() (io.ReadCloser, error)
}

type PutArtifactsRequest struct {
	JobKey  JobKey
	Ordinal int64
	Items   []ArtifactUpload
}

type CompleteExecutionRequest struct {
	Status string
	Detail string
}

type RescheduleExecutionRequest struct {
	NextNeed       string
	WaitUntil      *time.Time
	WaitForJobIDs  []string
	Payload        json.RawMessage
	AlternateNeed  string
	AlternateAfter *time.Duration
}
