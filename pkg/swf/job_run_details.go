package swf

import (
	"encoding/json"
	"time"
)

type GetJobRunRequest struct {
	JobKey               JobKey
	IncludeInputs        bool
	IncludeOutputs       bool
	IncludeArtifacts     bool
	IncludeAttemptInputs bool
}

type GetJobRunResponse struct {
	Job         JobRunSummary
	Start       JobStart
	Tasks       []TaskRun
	JobAttempts []JobAttempt
	Result      *JobAttempt
}

type JobRunSummary struct {
	JobKey     JobKey
	JobType    string
	Status     JobStatus
	CreatedAt  time.Time
	ArchivedAt *time.Time
}

type JobStart struct {
	Ordinal   int64
	WorkerID  string
	CreatedAt time.Time
	Input     *TaskIO
}

type TaskRun struct {
	TaskRunID string // "<task_type>:<first_ordinal>"
	TaskType  string
	Attempts  []TaskAttempt
}

const (
	TaskAttemptStateSucceeded = "SUCCEEDED"
	TaskAttemptStateFailed    = "FAILED"
	TaskAttemptStateReady     = "READY"
	TaskAttemptStateLeased    = "LEASED"
	TaskAttemptStateWaiting   = "WAITING"
	TaskAttemptStateRunning   = "RUNNING"

	TaskOutcomeStatusSucceeded = "SUCCEEDED"
	TaskOutcomeStatusFailed    = "FAILED"

	TaskErrorKindApp     = "APP"
	TaskErrorKindSystem  = "SYSTEM"
	TaskErrorKindTimeout = "TIMEOUT"
)

type TaskAttempt struct {
	Ordinal       int64
	Attempt       int
	WorkerID      string
	CreatedAt     time.Time
	InputHash     string
	InputRef      *InputReference
	RunPolicy     *RunPolicy
	Retryable     *bool
	MaxAttempts   *int
	NextAttemptAt *time.Time
	BackoffMillis *int64
	Input         *TaskIO
	Output        *TaskIO
	State         string
	Runtime       *TaskRuntime
	Outcome       TaskOutcome
}

type TaskRuntime struct {
	LeaseOwner     *string
	LeaseExpiresAt *time.Time
	NextNeed       *string
	AvailableAt    *time.Time
	WaitFor        []string
}

type JobAttempt struct {
	Ordinal   int64
	Attempt   int
	WorkerID  string
	CreatedAt time.Time
	InputRef  *InputReference
	Output    *TaskIO
	Outcome   TaskOutcome
}

type TaskIO struct {
	Data      json.RawMessage
	Artifacts []ArtifactInfo
}

type ArtifactInfo struct {
	ID          string
	Name        string
	ContentType string
	SizeBytes   int64
	Sha256      string
	Key         *ArtifactKey
}

type TaskOutcome struct {
	Status      string
	PayloadKind string
	Error       *TaskError
}

type TaskError struct {
	Kind       string
	Message    string
	Level      string
	Attrs      map[string]interface{}
	Component  string
	Code       string
	Retryable  *bool
	Scope      string
	After      *Duration
	InputRef   *InputReference
	Stacktrace []string
}
