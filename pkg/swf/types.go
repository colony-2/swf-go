package swf

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/colony-2/pgwf-go/pkg/pgwf"
	strata "github.com/colony-2/strata/strata-go/pkg/client/artifact"
	"github.com/invopop/jsonschema"
)

type Data struct {
	serialized   []byte
	deserialized map[string]interface{}
}

func NewBytesData(serialized []byte) Data {
	return Data{serialized: serialized}
}

func NewMapData(deserialized map[string]interface{}) Data {
	return Data{deserialized: deserialized}
}

func (d Data) deserializeIfNeeded() error {
	if d.deserialized == nil {
		deserialized := make(map[string]interface{})
		err := json.Unmarshal(d.serialized, d.deserialized)
		if err != nil {
			return err
		}
		d.deserialized = deserialized
	}
	return nil
}

func (d Data) ToBytes() ([]byte, error) {
	if d.serialized == nil {
		serialized, err := json.Marshal(d.deserialized)
		if err != nil {
			return nil, err
		}
		d.serialized = serialized
	}
	return d.serialized, nil
}

func (d Data) Set(key string, value any) error {
	err := d.deserializeIfNeeded()
	if err != nil {
		return err
	}

	// clear serialized since it is out of sync.
	d.serialized = nil
	d.deserialized[key] = value
	return nil
}

func (d Data) Get(key string) (value any, exists bool, err error) {
	err = d.deserializeIfNeeded()
	if err != nil {
		return nil, false, err
	}
	val, ok := d.deserialized[key]
	if !ok {
		return nil, false, nil
	}
	return val, true, nil

}

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

type Lease = pgwf.Lease
type Artifact = strata.Artifact
type Dependencies = pgwf.JobDependencies

type JobId string

type SimpleTaskData struct {
	Data      Data
	Artifacts []Artifact
}

func (s *SimpleTaskData) GetData() (Data, error) {
	return s.Data, nil
}

func (s *SimpleTaskData) GetArtifacts() ([]Artifact, error) {
	return s.Artifacts, nil
}

type TaskData interface {
	GetData() (Data, error)
	GetArtifacts() ([]Artifact, error)
}


type TaskWorker interface {
	Name() string
	Run(context TaskContext, input TaskData) (TaskData, error)
}
