package swf

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// JobSummary is a lightweight view of a job sourced purely from pgwf tables.
type JobSummary struct {
	JobID           JobId
	Status          JobStatus
	JobType         string
	SingletonKey    *string
	WaitFor         []JobId
	AvailableAt     time.Time
	ExpiresAt       *time.Time
	LeaseExpiresAt  *time.Time
	CancelRequested bool
	CreatedAt       time.Time
	ArchivedAt      *time.Time
	Payload         json.RawMessage
	TaskWaitInput   *int64
	TaskWaitOutput  *int64
	TaskWaitNext    *string
}

type JobStore string

const (
	JobStoreActive          JobStore = "ACTIVE"
	JobStoreArchived        JobStore = "ARCHIVED"
	DefaultListJobsPageSize          = 100
	MaxListJobsPageSize              = 200
)

// JobTaskFilter narrows listings to jobs currently waiting on a specific job/task capability.
type JobTaskFilter struct {
	JobType  string
	TaskType string
}

// ListJobsRequest filters and paginates the union of active + archived jobs.
type ListJobsRequest struct {
	Statuses      []JobStatus
	Stores        []JobStore
	JobTypes      []string
	JobTasks      []JobTaskFilter
	SingletonKeys []string
	CreatedAfter  *time.Time
	CreatedBefore *time.Time
	PageSize      int
	PageToken     string
}

type ListJobsResponse struct {
	Jobs          []JobSummary
	NextPageToken string
}

// jobsListApi is embedded into SWFEngine to avoid a new exported interface.
type jobsListApi interface {
	ListJobs(ctx context.Context, req ListJobsRequest) (ListJobsResponse, error)
}

func parseNextNeedJobType(nextNeed string) string {
	if nextNeed == "" {
		return ""
	}
	if idx := strings.Index(nextNeed, ":"); idx > 0 {
		return nextNeed[:idx]
	}
	return nextNeed
}

// JobTypeFromNextNeed derives a job type from a capability/next_need string.
func JobTypeFromNextNeed(nextNeed string) string {
	return parseNextNeedJobType(nextNeed)
}

type pageCursor struct {
	CreatedAt time.Time `json:"created_at"`
	JobID     string    `json:"job_id"`
}

func encodePageToken(cur pageCursor) (string, error) {
	raw, err := json.Marshal(cur)
	if err != nil {
		return "", err
	}
	return base64.RawURLEncoding.EncodeToString(raw), nil
}

func decodePageToken(tok string) (pageCursor, error) {
	var cur pageCursor
	bytes, err := base64.RawURLEncoding.DecodeString(tok)
	if err != nil {
		return cur, fmt.Errorf("invalid page token")
	}
	if err := json.Unmarshal(bytes, &cur); err != nil {
		return cur, fmt.Errorf("invalid page token")
	}
	return cur, nil
}

// EncodeListJobsPageToken renders a cursor for clients consuming ListJobs.
func EncodeListJobsPageToken(createdAt time.Time, jobID JobId) (string, error) {
	return encodePageToken(pageCursor{CreatedAt: createdAt, JobID: string(jobID)})
}

// DecodeListJobsPageToken parses a ListJobs page token into its cursor components.
func DecodeListJobsPageToken(tok string) (time.Time, JobId, error) {
	cur, err := decodePageToken(tok)
	return cur.CreatedAt, JobId(cur.JobID), err
}
