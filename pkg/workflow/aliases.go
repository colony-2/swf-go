package workflow

import (
	"context"
	"io"
	"log/slog"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
)

type Data = jobdb.Data
type Duration = jobdb.Duration
type RetryPolicy = jobdb.RetryPolicy
type RunPolicy = jobdb.RunPolicy
type InputReference = jobdb.InputReference
type JobKey = jobdb.JobKey
type ArtifactKey = jobdb.ArtifactKey
type Artifact = jobdb.Artifact
type TaskData = jobdb.TaskData
type JobData = jobdb.TaskData
type SimpleTaskData = jobdb.SimpleTaskData
type EnvelopedTaskData = jobdb.EnvelopedTaskData

type SubmitJob = jobdb.SubmitJob
type SubmitRestartJob = jobdb.SubmitRestartJob
type CancelJob = jobdb.CancelJob
type JobStatus = jobdb.JobStatus
type JobInfo = jobdb.JobInfo
type JobPrereqCondition = jobdb.JobPrereqCondition
type JobPrerequisite = jobdb.JobPrerequisite

const (
	JobPrereqComplete = jobdb.JobPrereqComplete
	JobPrereqSuccess  = jobdb.JobPrereqSuccess

	JobStatusReady          = jobdb.JobStatusReady
	JobStatusExpired        = jobdb.JobStatusExpired
	JobStatusPendingJobs    = jobdb.JobStatusPendingJobs
	JobStatusAwaitingFuture = jobdb.JobStatusAwaitingFuture
	JobStatusActive         = jobdb.JobStatusActive
	JobStatusCrashConcern   = jobdb.JobStatusCrashConcern
	JobStatusCancelled      = jobdb.JobStatusCancelled
	JobStatusCompleted      = jobdb.JobStatusCompleted
)

type WorkflowRuntime = jobdb.WorkflowRuntime
type JobHandle = jobdb.JobHandle
type ExecutionLease = jobdb.ExecutionLease
type SubmitJobRequest = jobdb.SubmitJobRequest
type SubmitRestartJobRequest = jobdb.SubmitRestartJobRequest
type CancelJobRequest = jobdb.CancelJobRequest
type PollWorkRequest = jobdb.PollWorkRequest
type GetJobLeaseRequest = jobdb.GetJobLeaseRequest
type CompleteTaskIfWaitingRequest = jobdb.CompleteTaskIfWaitingRequest
type ChapterRef = jobdb.ChapterRef
type ListChaptersRequest = jobdb.ListChaptersRequest
type StoredArtifact = jobdb.StoredArtifact
type ArtifactRef = jobdb.ArtifactRef
type ArtifactReader = jobdb.ArtifactReader
type ArtifactUpload = jobdb.ArtifactUpload
type CompleteExecutionRequest = jobdb.CompleteExecutionRequest
type RescheduleExecutionRequest = jobdb.RescheduleExecutionRequest

type RuntimeBuildOptions struct {
	Logger                *slog.Logger
	MaxActive             int
	AwaitRecycleThreshold time.Duration
	PollTenantId          string
}

type ListJobsRequest = jobdb.ListJobsRequest
type ListJobsResponse = jobdb.ListJobsResponse
type JobSummary = jobdb.JobSummary
type JobStore = jobdb.JobStore
type JobTaskFilter = jobdb.JobTaskFilter
type FieldName = jobdb.FieldName
type MetadataFilter = jobdb.MetadataFilter
type MetadataPredicate = jobdb.MetadataPredicate

const (
	DefaultListJobsPageSize = jobdb.DefaultListJobsPageSize
	MaxListJobsPageSize     = jobdb.MaxListJobsPageSize
)

type ScheduleKey = jobdb.ScheduleKey
type ScheduleState = jobdb.ScheduleState
type ScheduleOverlapPolicy = jobdb.ScheduleOverlapPolicy
type ScheduleTriggerKind = jobdb.ScheduleTriggerKind
type ScheduleTrigger = jobdb.ScheduleTrigger
type ScheduleTarget = jobdb.ScheduleTarget
type ScheduleFailurePolicy = jobdb.ScheduleFailurePolicy
type ScheduleSpec = jobdb.ScheduleSpec
type UpsertScheduleRequest = jobdb.UpsertScheduleRequest
type ScheduleMutationRequest = jobdb.ScheduleMutationRequest
type TriggerScheduleRequest = jobdb.TriggerScheduleRequest
type ScheduleInfo = jobdb.ScheduleInfo
type ListSchedulesRequest = jobdb.ListSchedulesRequest
type ListSchedulesResponse = jobdb.ListSchedulesResponse
type ListScheduleRunsRequest = jobdb.ListScheduleRunsRequest
type ScheduleRunSummary = jobdb.ScheduleRunSummary
type ListScheduleRunsResponse = jobdb.ListScheduleRunsResponse

type GetJobRunRequest = jobdb.GetJobRunRequest
type GetJobRunResponse = jobdb.GetJobRunResponse
type JobRunSummary = jobdb.JobRunSummary
type JobStart = jobdb.JobStart
type TaskRun = jobdb.TaskRun
type TaskAttempt = jobdb.TaskAttempt
type TaskRuntime = jobdb.TaskRuntime
type JobAttempt = jobdb.JobAttempt
type TaskIO = jobdb.TaskIO
type ArtifactInfo = jobdb.ArtifactInfo
type TaskOutcome = jobdb.TaskOutcome
type TaskError = jobdb.TaskError

const (
	TaskAttemptStateSucceeded = jobdb.TaskAttemptStateSucceeded
	TaskAttemptStateFailed    = jobdb.TaskAttemptStateFailed
	TaskAttemptStateReady     = jobdb.TaskAttemptStateReady
	TaskAttemptStateLeased    = jobdb.TaskAttemptStateLeased
	TaskAttemptStateWaiting   = jobdb.TaskAttemptStateWaiting
	TaskAttemptStateRunning   = jobdb.TaskAttemptStateRunning

	TaskOutcomeStatusSucceeded = jobdb.TaskOutcomeStatusSucceeded
	TaskOutcomeStatusFailed    = jobdb.TaskOutcomeStatusFailed

	TaskErrorKindApp     = jobdb.TaskErrorKindApp
	TaskErrorKindSystem  = jobdb.TaskErrorKindSystem
	TaskErrorKindTimeout = jobdb.TaskErrorKindTimeout

	TimeoutScopeInvocation = jobdb.TimeoutScopeInvocation
	TimeoutScopeTotal      = jobdb.TimeoutScopeTotal
)

type Chapter = jobdb.Chapter
type PutChapterRequest = jobdb.PutChapterRequest
type ChapterBody = jobdb.ChapterBody
type JobStartChapter = jobdb.JobStartChapter
type JobAttemptOutcomeChapter = jobdb.JobAttemptOutcomeChapter
type TaskAttemptOutcomeChapter = jobdb.TaskAttemptOutcomeChapter
type RestartExtraChapter = jobdb.RestartExtraChapter
type ChapterOutcome = jobdb.ChapterOutcome
type ApplicationOutputOutcome = jobdb.ApplicationOutputOutcome
type AppErrorOutcome = jobdb.AppErrorOutcome
type SystemErrorOutcome = jobdb.SystemErrorOutcome
type TimeoutOutcome = jobdb.TimeoutOutcome
type ApplicationInputBytes = jobdb.ApplicationInputBytes
type ApplicationOutputBytes = jobdb.ApplicationOutputBytes
type ChapterMetadata = jobdb.ChapterMetadata
type ChapterMetadataKind = jobdb.ChapterMetadataKind
type ChapterMetadataValue = jobdb.ChapterMetadataValue

const (
	ChapterMetadataNull   = jobdb.ChapterMetadataNull
	ChapterMetadataBool   = jobdb.ChapterMetadataBool
	ChapterMetadataInt    = jobdb.ChapterMetadataInt
	ChapterMetadataDouble = jobdb.ChapterMetadataDouble
	ChapterMetadataString = jobdb.ChapterMetadataString
	ChapterMetadataList   = jobdb.ChapterMetadataList
	ChapterMetadataMap    = jobdb.ChapterMetadataMap
)

type AppErrorPayload = jobdb.AppErrorPayload
type SystemErrorPayload = jobdb.SystemErrorPayload
type TimeoutPayload = jobdb.TimeoutPayload
type AppError = jobdb.AppError
type SystemError = jobdb.SystemError
type TimeoutError = jobdb.TimeoutError
type JobFailedError = jobdb.JobFailedError
type NonRetryableError = jobdb.NonRetryableError

var (
	ErrArtifactKeyUnavailable  = jobdb.ErrArtifactKeyUnavailable
	ErrChapterNotFound         = jobdb.ErrChapterNotFound
	ErrConflict                = jobdb.ErrConflict
	ErrExecutionLeaseLost      = jobdb.ErrExecutionLeaseLost
	ErrExistingJobMismatch     = jobdb.ErrExistingJobMismatch
	ErrJobCancelled            = jobdb.ErrJobCancelled
	ErrJobFailed               = jobdb.ErrJobFailed
	ErrJobNotComplete          = jobdb.ErrJobNotComplete
	ErrJobNotFound             = jobdb.ErrJobNotFound
	ErrMissingInputHash        = jobdb.ErrMissingInputHash
	ErrWorkflowNotDeterministic = jobdb.ErrWorkflowNotDeterministic
)

func AsDuration(t time.Duration) *Duration { return jobdb.AsDuration(t) }
func DefaultRunPolicy() RunPolicy          { return jobdb.DefaultRunPolicy() }

func NewTaskData(data any, artifacts ...Artifact) (TaskData, error) {
	return jobdb.NewTaskData(data, artifacts...)
}

func NewTaskDataOrPanic(data any, artifacts ...Artifact) TaskData {
	return jobdb.NewTaskDataOrPanic(data, artifacts...)
}

func NewArtifactFromBytes(name string, data []byte) Artifact {
	return jobdb.NewArtifactFromBytes(name, data)
}

func NewArtifactFromReader(name string, r io.Reader, size int64) Artifact {
	return jobdb.NewArtifactFromReader(name, r, size)
}

func NewArtifactFromFile(name string, filePath string) (Artifact, error) {
	return jobdb.NewArtifactFromFile(name, filePath)
}

func NewArtifactFromFileNoCleanup(name string, filePath string) (Artifact, error) {
	return jobdb.NewArtifactFromFileNoCleanup(name, filePath)
}

func NewArtifact(name string, opener func() (io.ReadCloser, int64, error), cleanup func() error) Artifact {
	return jobdb.NewArtifact(name, opener, cleanup)
}

func AssignArtifactKey(art Artifact, key ArtifactKey) { jobdb.AssignArtifactKey(art, key) }

func NewTimeoutError(kind string, after time.Duration, scope string, inputRef *InputReference, retryable bool) error {
	return jobdb.NewTimeoutError(kind, after, scope, inputRef, retryable)
}

func NewSystemError(payload SystemErrorPayload) error { return jobdb.NewSystemError(payload) }

func IsAppError(err error) bool             { return jobdb.IsAppError(err) }
func IsSystemError(err error) bool          { return jobdb.IsSystemError(err) }
func IsExecutionLeaseLost(err error) bool   { return jobdb.IsExecutionLeaseLost(err) }
func IsConflict(err error) bool             { return jobdb.IsConflict(err) }
func IsExistingJobMismatch(err error) bool  { return jobdb.IsExistingJobMismatch(err) }
func ExtractTaskDataResult(data TaskData) (TaskData, error) {
	return jobdb.ExtractTaskDataResult(data)
}

func Metadata() MetadataFilter { return jobdb.Metadata() }

func MetadataPredicates(filter MetadataFilter) ([]MetadataPredicate, error) {
	return jobdb.MetadataPredicates(filter)
}

func JobTypeFromNextNeed(nextNeed string) string { return jobdb.JobTypeFromNextNeed(nextNeed) }

func GetJobRun(ctx context.Context, runtime WorkflowRuntime, req GetJobRunRequest) (GetJobRunResponse, error) {
	return jobdb.GetJobRun(ctx, runtime, req)
}
