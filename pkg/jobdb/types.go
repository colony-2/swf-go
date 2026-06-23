package jobdb

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/invopop/jsonschema"
)

type dataImpl struct {
	serialized   []byte
	deserialized map[string]interface{}
}

type Data = json.RawMessage

// Duration wraps time.Duration to provide custom YAML marshaling/unmarshaling
// It serializes to/from human-readable strings like "1s", "500ms", "2m"
type Duration time.Duration

func (d Duration) JSONSchema() *jsonschema.Schema {
	return &jsonschema.Schema{
		Type:        "string",
		Title:       "Duration",
		Description: "Human friendly duration string",
	}
}

func AsDuration(t time.Duration) *Duration {
	d := Duration(t)
	return &d
}

// MarshalYAML converts Duration to a YAML string
func (d Duration) MarshalYAML() (interface{}, error) {
	return time.Duration(d).String(), nil
}

// UnmarshalYAML parses a YAML string into a Duration
func (d *Duration) UnmarshalYAML(unmarshal func(interface{}) error) error {
	var s string
	if err := unmarshal(&s); err != nil {
		return err
	}

	duration, err := time.ParseDuration(s)
	if err != nil {
		return fmt.Errorf("invalid duration: %w", err)
	}

	*d = Duration(duration)
	return nil
}

// ToDuration converts to standard time.Duration
func (d Duration) ToDuration() time.Duration {
	return time.Duration(d)
}

// String implements the Stringer interface
func (d Duration) String() string {
	return time.Duration(d).String()
}

type RetryPolicy struct {
	InitialInterval        Duration `yaml:"initial_interval,omitempty"`
	BackoffCoefficient     float64  `yaml:"backoff_coefficient,omitempty"`
	MaximumInterval        Duration `yaml:"maximum_interval,omitempty"`
	MaximumAttempts        int32    `yaml:"maximum_attempts,omitempty"`
	NonRetryableErrorTypes []string `yaml:"non_retryable_error_types,omitempty"`
}

// RunPolicy bundles runtime directives for jobs/tasks.
// Future extensions may add fields like affinity or max duration.
type RunPolicy struct {
	Retry             RetryPolicy `yaml:"retry,omitempty"`
	InvocationTimeout *Duration   `yaml:"invocation_timeout,omitempty"`
	TotalTimeout      *Duration   `yaml:"total_timeout,omitempty"`
}

func DefaultRunPolicy() RunPolicy {
	return RunPolicy{
		InvocationTimeout: AsDuration(10 * time.Minute),
		TotalTimeout:      AsDuration(30 * time.Minute),
		Retry: RetryPolicy{
			InitialInterval:        Duration(100 * time.Millisecond),
			BackoffCoefficient:     2.0,
			MaximumInterval:        Duration(30 * time.Second),
			MaximumAttempts:        3,
			NonRetryableErrorTypes: []string{"SystemError"},
		},
	}
}

// InputReference points to an input chapter for error payloads/metadata.
type InputReference struct {
	Ordinal int64  `json:"ordinal"`
	Hash    string `json:"hash,omitempty"`
}

// JobKey uniquely identifies a job across all tenants.
// It combines tenant identity with job identity.
type JobKey struct {
	TenantId string `json:"tenantId"`
	JobId    string `json:"jobId"`
}

// ArtifactKey identifies an artifact within a persisted job run.
type ArtifactKey struct {
	JobId       string `json:"jobId"`
	TaskOrdinal int64  `json:"taskOrdinal"`
	Name        string `json:"name"`
	SizeBytes   int64  `json:"sizeBytes"`
}

// Validate checks if the ArtifactKey is valid.
func (ak ArtifactKey) Validate() error {
	if ak.JobId == "" {
		return fmt.Errorf("jobId cannot be empty")
	}
	if ak.TaskOrdinal < 0 {
		return fmt.Errorf("taskOrdinal cannot be negative")
	}
	if ak.Name == "" {
		return fmt.Errorf("artifact name cannot be empty")
	}
	if ak.SizeBytes < -1 {
		return fmt.Errorf("sizeBytes cannot be less than -1")
	}
	return nil
}

// String returns a string representation of the JobKey.
// Format: "tenantId/jobId"
func (jk JobKey) String() string {
	return fmt.Sprintf("%s/%s", jk.TenantId, jk.JobId)
}

// ParseJobKey parses a string representation back into a JobKey.
// Expected format: "tenantId/jobId"
func ParseJobKey(s string) (JobKey, error) {
	parts := strings.Split(s, "/")
	if len(parts) != 2 {
		return JobKey{}, fmt.Errorf("invalid JobKey format: %s", s)
	}
	return JobKey{
		TenantId: parts[0],
		JobId:    parts[1],
	}, nil
}

// IsZero returns true if the JobKey is the zero value.
func (jk JobKey) IsZero() bool {
	return jk.TenantId == "" && jk.JobId == ""
}

// Validate checks if the JobKey is valid.
func (jk JobKey) Validate() error {
	if jk.TenantId == "" {
		return fmt.Errorf("TenantId cannot be empty")
	}
	if jk.JobId == "" {
		return fmt.Errorf("JobId cannot be empty")
	}
	return nil
}

type SimpleTaskData struct {
	Data      Data
	Artifacts []Artifact
}

func (s *SimpleTaskData) GetDataOrPanic() Data {
	data, err := s.GetData()
	if err != nil {
		panic(err)
	}
	return data
}

func NewTaskData(data any, artifacts ...Artifact) (TaskData, error) {
	bytes, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}
	return &SimpleTaskData{Data: bytes, Artifacts: artifacts}, nil
}

func NewTaskDataOrPanic(data any, artifacts ...Artifact) TaskData {
	td, err := NewTaskData(data, artifacts...)
	if err != nil {
		panic(err)
	}
	return td
}

// EnvelopedTaskData preserves payload kind metadata for round-tripping through envelopes.
type EnvelopedTaskData struct {
	SimpleTaskData
	Kind string
}

func (s *SimpleTaskData) GetData() (Data, error) {
	return s.Data, nil
}

func (s *SimpleTaskData) GetArtifacts() ([]Artifact, error) {
	return s.Artifacts, nil
}

type TaskData interface {
	GetData() (Data, error)
	GetDataOrPanic() Data
	GetArtifacts() ([]Artifact, error)
}
