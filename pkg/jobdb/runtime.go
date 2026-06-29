package jobdb

import (
	"context"
	"encoding/json"
	"io"
	"time"
)

// WorkflowRuntime is the backend-agnostic facade used to isolate jobdb from
// concrete persistence, artifact storage, and lease-management backends.
type WorkflowRuntime interface {
	// Job lifecycle
	SubmitJob(ctx context.Context, req SubmitJobRequest) (JobHandle, error)
	SubmitRestartJob(ctx context.Context, req SubmitRestartJobRequest) (JobHandle, error)
	CancelJob(ctx context.Context, req CancelJobRequest) error

	// Worker loop
	PollWork(ctx context.Context, req PollWorkRequest) ([]ExecutionLease, error)
	GetJobLease(ctx context.Context, req GetJobLeaseRequest) (ExecutionLease, error)
	CompleteTaskIfWaiting(ctx context.Context, req CompleteTaskIfWaitingRequest) error

	// Schedules
	UpsertSchedule(ctx context.Context, req UpsertScheduleRequest) (ScheduleInfo, error)
	GetSchedule(ctx context.Context, key ScheduleKey) (ScheduleInfo, error)
	ListSchedules(ctx context.Context, req ListSchedulesRequest) (ListSchedulesResponse, error)
	PauseSchedule(ctx context.Context, req ScheduleMutationRequest) (ScheduleInfo, error)
	ResumeSchedule(ctx context.Context, req ScheduleMutationRequest) (ScheduleInfo, error)
	ArchiveSchedule(ctx context.Context, req ScheduleMutationRequest) (ScheduleInfo, error)
	TriggerSchedule(ctx context.Context, req TriggerScheduleRequest) (JobHandle, error)
	ListScheduleRuns(ctx context.Context, req ListScheduleRunsRequest) (ListScheduleRunsResponse, error)

	// Read APIs
	GetJob(ctx context.Context, jobKey JobKey) (JobInfo, error)
	ListJobs(ctx context.Context, req ListJobsRequest) (ListJobsResponse, error)

	// Chapter / replay access
	GetChapter(ctx context.Context, ref ChapterRef) (Chapter, error)
	ListChapters(ctx context.Context, req ListChaptersRequest) ([]Chapter, error)
	PutChapter(ctx context.Context, req PutChapterRequest) error

	// Artifact access
	OpenArtifact(ctx context.Context, ref ArtifactRef) (ArtifactReader, error)
}

// JobHandle identifies a job owned by a runtime implementation.
type JobHandle struct {
	JobKey JobKey
}

// ExecutionLease represents a leased unit of work returned by PollWork or
// GetJobLease.
type ExecutionLease interface {
	LeaseID() string
	Job() JobHandle
	Capability() string
	Payload() json.RawMessage
	KeepAlive(ctx context.Context) error
	StopKeepAlive()
	Complete(ctx context.Context, req CompleteExecutionRequest) error
	Reschedule(ctx context.Context, req RescheduleExecutionRequest) error
	SubmitJob(ctx context.Context, req SubmitJobRequest) (JobHandle, error)
	SubmitRestartJob(ctx context.Context, req SubmitRestartJobRequest) (JobHandle, error)
}

type SubmitJobRequest struct {
	Job         SubmitJob
	WorkerID    string
	RequestTime time.Time
}

type SubmitRestartJobRequest struct {
	Job         SubmitRestartJob
	WorkerID    string
	RequestTime time.Time
}

type CancelJobRequest struct {
	JobKey   JobKey
	Reason   string
	WorkerID string
}

type PollWorkRequest struct {
	TenantId       string
	WorkerID       string
	Capabilities   []string
	Limit          int
	LongPollUntil  *time.Time
	LeaseDuration  time.Duration
	MetadataEquals []MetadataPredicate
}

type GetJobLeaseRequest struct {
	JobKey        JobKey
	WorkerID      string
	Capabilities  []string
	LeaseDuration time.Duration
}

type CompleteTaskIfWaitingRequest struct {
	JobKey        JobKey
	Capability    string
	ResumeNeed    string
	InputOrdinal  int64
	OutputOrdinal int64
	InputHash     string
	Data          TaskData
}

type ChapterRef struct {
	JobKey   JobKey
	Ordinal  int64
	Attempt  int
	TaskType string
}

type ListChaptersRequest struct {
	JobKey       JobKey
	StartOrdinal int64
	EndOrdinal   *int64
}

type StoredArtifact struct {
	Name   string
	Digest string
	Size   int64
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

type CompleteExecutionRequest struct {
	Status          string
	Detail          string
	Chapter         *Chapter
	ArtifactUploads []ArtifactUpload
}

type RescheduleExecutionRequest struct {
	NextNeed       string
	WaitUntil      *time.Time
	WaitForJobIDs  []string
	Payload        json.RawMessage
	AlternateNeed  string
	AlternateAfter *time.Duration
}
