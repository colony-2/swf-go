package jobdb

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
	JobKey            JobKey
	Status            JobStatus
	JobType           string
	NextNeed          *string
	WaitFor           []string // JobIds only - all WaitFor jobs must be in same tenant
	AvailableAt       time.Time
	ExpiresAt         *time.Time
	LeaseExpiresAt    *time.Time
	CancelRequested   bool
	CreatedAt         time.Time
	ArchivedAt        *time.Time
	Payload           json.RawMessage
	Metadata          json.RawMessage
	SchemaHash        string
	TaskWaitInput     *int64
	TaskWaitOutput    *int64
	TaskWaitInputHash *string
	TaskWaitNext      *string
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
// TenantIds must contain at least one tenant ID.
type ListJobsRequest struct {
	TenantIds      []string
	Statuses       []JobStatus
	Stores         []JobStore
	JobTypes       []string
	JobTasks       []JobTaskFilter
	JobKeys        []JobKey
	MetadataFilter MetadataFilter
	CreatedAfter   *time.Time
	CreatedBefore  *time.Time
	PageSize       int
	PageToken      string
}

type FieldName string

type MetadataFilter interface {
	AndFilter(MetadataFilter) (MetadataFilter, error)
	OrFilter(MetadataFilter) (MetadataFilter, error)
	EqualFilter(FieldName, any) (MetadataFilter, error)
	metadataFilter()
}

type metadataEmpty struct{}
type metadataEqual struct {
	field FieldName
	value any
}
type metadataOr struct {
	field  FieldName
	values []any
}
type metadataAnd struct {
	left  MetadataFilter
	right MetadataFilter
}

type MetadataPredicate struct {
	Path   []string
	Values []any
}

func Metadata() MetadataFilter {
	return metadataEmpty{}
}

func (m metadataEmpty) AndFilter(other MetadataFilter) (MetadataFilter, error) {
	if other == nil {
		return m, nil
	}
	return other, nil
}

func (m metadataEmpty) OrFilter(other MetadataFilter) (MetadataFilter, error) {
	if other == nil {
		return m, nil
	}
	return other, nil
}

func (m metadataEmpty) EqualFilter(field FieldName, value any) (MetadataFilter, error) {
	return newEqualFilter(field, value)
}

func (m metadataEqual) AndFilter(other MetadataFilter) (MetadataFilter, error) {
	if other == nil {
		return m, nil
	}
	return metadataAnd{left: m, right: other}, nil
}

func (m metadataEqual) OrFilter(other MetadataFilter) (MetadataFilter, error) {
	if other == nil {
		return m, nil
	}
	switch o := other.(type) {
	case metadataEqual:
		if m.field != o.field {
			return nil, fmt.Errorf("metadata OR requires matching fields")
		}
		return metadataOr{field: m.field, values: []any{m.value, o.value}}, nil
	case metadataOr:
		if m.field != o.field {
			return nil, fmt.Errorf("metadata OR requires matching fields")
		}
		values := append([]any{m.value}, o.values...)
		return metadataOr{field: m.field, values: values}, nil
	default:
		return nil, fmt.Errorf("metadata OR requires matching fields")
	}
}

func (m metadataEqual) EqualFilter(field FieldName, value any) (MetadataFilter, error) {
	eq, err := newEqualFilter(field, value)
	if err != nil {
		return nil, err
	}
	return metadataAnd{left: m, right: eq}, nil
}

func (m metadataOr) AndFilter(other MetadataFilter) (MetadataFilter, error) {
	if other == nil {
		return m, nil
	}
	return metadataAnd{left: m, right: other}, nil
}

func (m metadataOr) OrFilter(other MetadataFilter) (MetadataFilter, error) {
	if other == nil {
		return m, nil
	}
	switch o := other.(type) {
	case metadataEqual:
		if m.field != o.field {
			return nil, fmt.Errorf("metadata OR requires matching fields")
		}
		return metadataOr{field: m.field, values: append(m.values, o.value)}, nil
	case metadataOr:
		if m.field != o.field {
			return nil, fmt.Errorf("metadata OR requires matching fields")
		}
		return metadataOr{field: m.field, values: append(m.values, o.values...)}, nil
	default:
		return nil, fmt.Errorf("metadata OR requires matching fields")
	}
}

func (m metadataOr) EqualFilter(field FieldName, value any) (MetadataFilter, error) {
	eq, err := newEqualFilter(field, value)
	if err != nil {
		return nil, err
	}
	return metadataAnd{left: m, right: eq}, nil
}

func (m metadataAnd) AndFilter(other MetadataFilter) (MetadataFilter, error) {
	if other == nil {
		return m, nil
	}
	return metadataAnd{left: m, right: other}, nil
}

func (m metadataAnd) OrFilter(other MetadataFilter) (MetadataFilter, error) {
	if other == nil {
		return m, nil
	}
	return nil, fmt.Errorf("metadata OR requires matching fields")
}

func (m metadataAnd) EqualFilter(field FieldName, value any) (MetadataFilter, error) {
	eq, err := newEqualFilter(field, value)
	if err != nil {
		return nil, err
	}
	return metadataAnd{left: m, right: eq}, nil
}

func (metadataEmpty) metadataFilter() {}
func (metadataEqual) metadataFilter() {}
func (metadataOr) metadataFilter()    {}
func (metadataAnd) metadataFilter()   {}

func MetadataPredicates(filter MetadataFilter) ([]MetadataPredicate, error) {
	if filter == nil {
		return nil, nil
	}
	predicates := make([]MetadataPredicate, 0)
	if err := collectMetadataPredicates(filter, &predicates); err != nil {
		return nil, err
	}
	return predicates, nil
}

func collectMetadataPredicates(filter MetadataFilter, out *[]MetadataPredicate) error {
	switch v := filter.(type) {
	case metadataEmpty:
		return nil
	case metadataEqual:
		predicate, err := predicateFromEqual(v.field, []any{v.value})
		if err != nil {
			return err
		}
		*out = append(*out, predicate)
		return nil
	case metadataOr:
		predicate, err := predicateFromEqual(v.field, v.values)
		if err != nil {
			return err
		}
		*out = append(*out, predicate)
		return nil
	case metadataAnd:
		if err := collectMetadataPredicates(v.left, out); err != nil {
			return err
		}
		return collectMetadataPredicates(v.right, out)
	default:
		return fmt.Errorf("unknown metadata filter")
	}
}

func newEqualFilter(field FieldName, value any) (MetadataFilter, error) {
	if err := validateField(field, value); err != nil {
		return nil, err
	}
	return metadataEqual{field: field, value: value}, nil
}

func predicateFromEqual(field FieldName, values []any) (MetadataPredicate, error) {
	if err := validateField(field, values); err != nil {
		return MetadataPredicate{}, err
	}
	clean := make([]any, 0, len(values))
	for _, value := range values {
		if value == nil {
			return MetadataPredicate{}, fmt.Errorf("metadata field %q value is required", field)
		}
		clean = append(clean, value)
	}
	return MetadataPredicate{
		Path:   []string{string(field)},
		Values: clean,
	}, nil
}

func validateField(field FieldName, value any) error {
	name := strings.TrimSpace(string(field))
	if name == "" {
		return fmt.Errorf("metadata field name is required")
	}
	if strings.Contains(name, ".") {
		return fmt.Errorf("metadata field %q must be a top-level field", name)
	}
	if value == nil {
		return fmt.Errorf("metadata field %q value is required", name)
	}
	return nil
}

type ListJobsResponse struct {
	Jobs          []JobSummary
	NextPageToken string
}

// jobsListApi is embedded into Engine to avoid a new exported interface.
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
	TenantId  string    `json:"tenant_id"`
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
func EncodeListJobsPageToken(createdAt time.Time, jobKey JobKey) (string, error) {
	return encodePageToken(pageCursor{CreatedAt: createdAt, TenantId: jobKey.TenantId, JobID: jobKey.JobId})
}

// DecodeListJobsPageToken parses a ListJobs page token into its cursor components.
func DecodeListJobsPageToken(tok string) (time.Time, JobKey, error) {
	cur, err := decodePageToken(tok)
	return cur.CreatedAt, JobKey{TenantId: cur.TenantId, JobId: cur.JobID}, err
}

func metadataPredicateSignature(predicates []MetadataPredicate) (string, error) {
	if len(predicates) == 0 {
		return "", nil
	}
	raw, err := json.Marshal(predicates)
	if err != nil {
		return "", fmt.Errorf("metadata predicates must be JSON-serializable: %w", err)
	}
	return string(raw), nil
}
