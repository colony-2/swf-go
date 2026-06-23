package jobdb

import (
	"encoding/json"
	"time"
)

// SubmitJob defines the parameters for starting a new workflow job.
// If JobID is provided, it will be used as the job identifier; otherwise, a new unique ID will be generated.
type SubmitJob struct {
	TenantId      string            // REQUIRED: Tenant for this job
	JobType       string            // The type of job to start
	JobID         string            // Optional job identifier. If empty, a new unique ID will be generated.
	Data          JobData           // Input data for the job
	RunPolicy     RunPolicy         // Runtime policy for retries, timeouts, etc.
	Metadata      json.RawMessage   // Optional metadata persisted with the job
	AvailableAt   *time.Time        // Optional time before which the job is not leaseable
	Prerequisites []JobPrerequisite // Optional prerequisites that must complete before this job starts
}

type SubmitRestartJob struct {
	PriorJobKey     JobKey
	LastStepToKeep  int64
	JobID           string            // optional override for new job id
	ExtraTaskInput  TaskData          // optional input used to compute hash for ExtraTaskOutput
	ExtraTaskOutput TaskData          // optional cached task/job output to append at LastStepToKeep+1
	Prerequisites   []JobPrerequisite // Optional prerequisites that must complete before this job starts
}

// JobPrereqCondition defines how a prerequisite is evaluated.
type JobPrereqCondition string

const (
	JobPrereqComplete JobPrereqCondition = "complete" // job must be archived (any outcome)
	JobPrereqSuccess  JobPrereqCondition = "success"  // job must be archived + succeeded
)

// JobPrerequisite declares a dependency on another job.
type JobPrerequisite struct {
	JobID     string             // required; same tenant as the parent job
	Condition JobPrereqCondition // required; default to JobPrereqComplete if empty
}

type CancelJob struct {
	JobKey JobKey
	Reason string
}

type JobStatus string

const (
	JobStatusReady          JobStatus = "READY"
	JobStatusExpired        JobStatus = "EXPIRED"
	JobStatusPendingJobs    JobStatus = "PENDING_JOBS"
	JobStatusAwaitingFuture JobStatus = "AWAITING_FUTURE"
	JobStatusActive         JobStatus = "ACTIVE"
	JobStatusCrashConcern   JobStatus = "CRASH_CONCERN"
	JobStatusCancelled      JobStatus = "CANCELLED"
	JobStatusCompleted      JobStatus = "COMPLETED"
)

type JobInfo struct {
	Status JobStatus
	Data   TaskData
}

type JobData = TaskData
