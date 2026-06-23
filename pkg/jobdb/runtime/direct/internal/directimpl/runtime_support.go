package directimpl

import (
	"database/sql"
	"encoding/json"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/colony-2/jobdb/pkg/internal/runtimecodec"
	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/pgwf-go/pkg/pgwf"
	"github.com/colony-2/strata-go/pkg/client/story"
	"github.com/google/uuid"
	"gorm.io/gorm"
)

type jobPayload = runtimecodec.SchedulerPayload
type taskWait = runtimecodec.TaskWait

type chapterMetadata struct {
	Attempt       int
	MaxAttempts   int
	NextAttemptAt *time.Time
	BackoffMillis int64
	Retryable     *bool
	InputRef      *jobdb.InputReference
	RunPolicy     *jobdb.RunPolicy
	Metadata      json.RawMessage
	InputPayload  json.RawMessage
	StartedAt     *time.Time
	FinishedAt    *time.Time
	Prerequisites []jobdb.JobPrerequisite
}

func normalizeRetryPolicy(policy jobdb.RetryPolicy) jobdb.RetryPolicy {
	rp := policy
	if rp.MaximumAttempts <= 0 {
		rp.MaximumAttempts = 1
	}
	if rp.BackoffCoefficient == 0 {
		rp.BackoffCoefficient = 1
	}
	return rp
}

func normalizeTimeout(d *jobdb.Duration) *jobdb.Duration {
	if d == nil {
		return nil
	}
	if time.Duration(*d) < 0 {
		return nil
	}
	val := *d
	return &val
}

func normalizeRunPolicy(policy jobdb.RunPolicy) jobdb.RunPolicy {
	p := policy
	p.Retry = normalizeRetryPolicy(p.Retry)
	p.InvocationTimeout = normalizeTimeout(p.InvocationTimeout)
	p.TotalTimeout = normalizeTimeout(p.TotalTimeout)
	return p
}

func computeBackoff(rp jobdb.RetryPolicy, attempt int) time.Duration {
	base := time.Duration(rp.InitialInterval)
	backoff := float64(base)
	if attempt > 1 {
		backoff = float64(base) * math.Pow(rp.BackoffCoefficient, float64(attempt-1))
	}
	dur := time.Duration(backoff)
	maxInterval := time.Duration(rp.MaximumInterval)
	if maxInterval > 0 && dur > maxInterval {
		dur = maxInterval
	}
	if dur < 0 {
		dur = 0
	}
	return dur
}

func sqlTxFromGorm(db *gorm.DB) *sql.Tx {
	if db == nil {
		return nil
	}
	if db.Statement != nil {
		if tx, ok := db.Statement.ConnPool.(*sql.Tx); ok && tx != nil {
			return tx
		}
	}
	if tx, ok := db.ConnPool.(*sql.Tx); ok && tx != nil {
		return tx
	}
	return nil
}

func taskDataToChapter(jobData jobdb.TaskData, ordinal int64, taskType string, workerID string, chapterType string, payloadKind string, inputHash string, createdAt time.Time, meta chapterMetadata) (story.Chapter, error) {
	if jobData == nil {
		return nil, fmt.Errorf("task data is required")
	}
	data, err := jobData.GetData()
	if err != nil {
		return nil, err
	}
	artifacts, err := jobData.GetArtifacts()
	if err != nil {
		return nil, err
	}
	return payloadToChapter(data, artifacts, ordinal, taskType, workerID, chapterType, payloadKind, inputHash, createdAt, meta)
}

func taskDataToCreatOptions(jobData jobdb.TaskData, ordinal int64, taskType string, workerID string, chapterType string, payloadKind string, inputHash string, createdAt time.Time, meta chapterMetadata) (story.CreateOptions, error) {
	chap, err := taskDataToChapter(jobData, ordinal, taskType, workerID, chapterType, payloadKind, inputHash, createdAt, meta)
	if err != nil {
		return story.CreateOptions{}, err
	}
	return story.CreateOptions{
		RequestID:      uuid.New().String(),
		InitialChapter: chap,
	}, nil
}

func normalizePrerequisites(jobKey jobdb.JobKey, prereqs []jobdb.JobPrerequisite) ([]jobdb.JobPrerequisite, []pgwf.JobID, error) {
	if len(prereqs) == 0 {
		return nil, nil, nil
	}
	seen := make(map[string]struct{}, len(prereqs))
	normalized := make([]jobdb.JobPrerequisite, 0, len(prereqs))
	waitFor := make([]pgwf.JobID, 0, len(prereqs))
	for _, p := range prereqs {
		if strings.TrimSpace(p.JobID) == "" {
			return nil, nil, fmt.Errorf("prerequisite job id is required")
		}
		if p.JobID == jobKey.JobId {
			return nil, nil, fmt.Errorf("prerequisite job id cannot reference self")
		}
		if _, ok := seen[p.JobID]; ok {
			continue
		}
		seen[p.JobID] = struct{}{}
		if p.Condition == "" {
			p.Condition = jobdb.JobPrereqComplete
		}
		switch p.Condition {
		case jobdb.JobPrereqComplete, jobdb.JobPrereqSuccess:
		default:
			return nil, nil, fmt.Errorf("invalid prerequisite condition %q", p.Condition)
		}
		normalized = append(normalized, p)
		waitFor = append(waitFor, pgwf.JobID(p.JobID))
	}
	return normalized, waitFor, nil
}

func payloadToChapter(payload json.RawMessage, artifacts []jobdb.Artifact, ordinal int64, taskType string, workerID string, chapterType string, payloadKind string, inputHash string, createdAt time.Time, metaOpts chapterMetadata) (story.Chapter, error) {
	if payload == nil {
		return nil, fmt.Errorf("payload is required")
	}
	if inputHash == "" {
		return nil, fmt.Errorf("input hash is required")
	}
	meta := chapterMeta{
		Version:   envelopeVersion,
		Ordinal:   ordinal,
		TaskType:  taskType,
		WorkerID:  workerID,
		CreatedAt: createdAt,
		InputHash: inputHash,
	}
	if metaOpts.Attempt > 0 {
		meta.Attempt = metaOpts.Attempt
	}
	if metaOpts.MaxAttempts > 0 {
		meta.MaxAttempts = metaOpts.MaxAttempts
	}
	if metaOpts.NextAttemptAt != nil {
		meta.NextAttemptAt = metaOpts.NextAttemptAt
	}
	if metaOpts.BackoffMillis > 0 {
		meta.BackoffMillis = metaOpts.BackoffMillis
	}
	if metaOpts.Retryable != nil {
		meta.Retryable = metaOpts.Retryable
	}
	if metaOpts.InputRef != nil {
		meta.InputRef = metaOpts.InputRef
	}
	if metaOpts.RunPolicy != nil {
		meta.RunPolicy = metaOpts.RunPolicy
	}
	if len(metaOpts.Metadata) > 0 {
		meta.Metadata = append(json.RawMessage(nil), metaOpts.Metadata...)
	}
	if metaOpts.InputPayload != nil {
		meta.Input = metaOpts.InputPayload
	}
	if metaOpts.StartedAt != nil {
		meta.StartedAt = metaOpts.StartedAt
	}
	if metaOpts.FinishedAt != nil {
		meta.FinishedAt = metaOpts.FinishedAt
	}
	if len(metaOpts.Prerequisites) > 0 {
		meta.Prerequisites = metaOpts.Prerequisites
	}

	envBytes, err := buildChapterEnvelope(meta, chapterType, payloadKind, payload)
	if err != nil {
		return nil, err
	}

	builder := story.NewChapter().WithOrdinal(ordinal).WithBytes(envBytes)
	for _, art := range artifacts {
		builder.AddArtifact(toStrataArtifact(art))
	}
	return builder, nil
}

func convertPgwfStatusToJobDB(status pgwf.JobStatus, cancelRequested bool, archivedAt *time.Time) jobdb.JobStatus {
	if archivedAt != nil {
		if cancelRequested {
			return jobdb.JobStatusCancelled
		}
		return jobdb.JobStatusCompleted
	}
	switch status {
	case pgwf.JobStatusReady:
		return jobdb.JobStatusReady
	case pgwf.JobStatusActive:
		return jobdb.JobStatusActive
	case pgwf.JobStatusCancelled:
		return jobdb.JobStatusCancelled
	case pgwf.JobStatusAwaitingFuture:
		return jobdb.JobStatusAwaitingFuture
	case pgwf.JobStatusPendingJobs:
		return jobdb.JobStatusPendingJobs
	case pgwf.JobStatusCrashConcern:
		return jobdb.JobStatusCrashConcern
	case pgwf.JobStatusExpired:
		return jobdb.JobStatusExpired
	default:
		return jobdb.JobStatusReady
	}
}

func convertJobDBStatusesToPgwf(statuses []jobdb.JobStatus) []pgwf.JobStatus {
	out := make([]pgwf.JobStatus, 0, len(statuses))
	for _, status := range statuses {
		switch status {
		case jobdb.JobStatusCompleted, jobdb.JobStatusCancelled:
			continue
		default:
			out = append(out, pgwf.JobStatus(status))
		}
	}
	return out
}

func shouldIncludeArchived(stores []jobdb.JobStore, statuses []jobdb.JobStatus) bool {
	if len(stores) > 0 {
		for _, store := range stores {
			if store == jobdb.JobStoreArchived {
				return true
			}
		}
		return false
	}
	for _, status := range statuses {
		if status == jobdb.JobStatusCompleted || status == jobdb.JobStatusCancelled {
			return true
		}
	}
	return len(statuses) == 0
}

func buildJobTypePatterns(jobTypes []string, jobTasks []jobdb.JobTaskFilter) []string {
	patterns := make([]string, 0, len(jobTypes)*2+len(jobTasks))
	for _, jobType := range jobTypes {
		patterns = append(patterns, jobType, jobType+":%")
	}
	for _, task := range jobTasks {
		if task.JobType != "" && task.TaskType != "" {
			patterns = append(patterns, task.JobType+":"+task.TaskType)
		}
	}
	return patterns
}

func normalizePageSize(pageSize int) int {
	if pageSize <= 0 {
		return jobdb.DefaultListJobsPageSize
	}
	if pageSize > jobdb.MaxListJobsPageSize {
		return jobdb.MaxListJobsPageSize
	}
	return pageSize
}

func extractTaskWaitFromRaw(payloadJSON json.RawMessage) (*taskWait, error) {
	payload, err := decodeJobPayload(payloadJSON)
	if err != nil {
		return nil, err
	}
	return payload.TaskWait, nil
}

func encodeJobPayload(payload jobPayload) (json.RawMessage, error) {
	return runtimecodec.EncodeSchedulerPayloadJSON(payload)
}

func decodeJobPayload(raw json.RawMessage) (jobPayload, error) {
	return runtimecodec.DecodeSchedulerPayloadJSON(raw)
}

func jobPayloadFromVisibleJSON(raw json.RawMessage) (jobPayload, error) {
	return runtimecodec.SchedulerPayloadFromJSONView(raw)
}

func jobPayloadVisibleJSON(raw json.RawMessage) json.RawMessage {
	payload, err := decodeJobPayload(raw)
	if err != nil {
		return json.RawMessage(`{}`)
	}
	view, err := runtimecodec.SchedulerPayloadJSONView(payload)
	if err != nil {
		return json.RawMessage(`{}`)
	}
	return view
}

func taskTypeFromCapability(capability string) string {
	parts := strings.SplitN(capability, ":", 2)
	if len(parts) == 2 {
		return parts[1]
	}
	return capability
}
