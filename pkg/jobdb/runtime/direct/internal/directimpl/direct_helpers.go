package directimpl

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"sync/atomic"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/pgwf-go/pkg/pgwf"
	strata "github.com/colony-2/strata-go/pkg/client/artifact"
	"github.com/colony-2/strata-go/pkg/client/story"
)

func storyKeyForJob(jobKey jobdb.JobKey) story.Key {
	return story.Key{
		AnthologyID: jobKey.TenantId,
		StoryID:     jobKey.JobId,
	}
}

func metadataPredicatesToPgwf(filter jobdb.MetadataFilter) ([]pgwf.MetadataPredicate, error) {
	preds, err := jobdb.MetadataPredicates(filter)
	if err != nil {
		return nil, err
	}
	out := make([]pgwf.MetadataPredicate, 0, len(preds))
	for _, pred := range preds {
		values, err := jsonEncodedMetadataValues(pred.Values)
		if err != nil {
			return nil, err
		}
		out = append(out, pgwf.MetadataPredicate{
			Path:   append([]string{"app"}, pred.Path...),
			Values: values,
		})
	}
	return out, nil
}

func jsonEncodedMetadataValues(values []any) ([]any, error) {
	if len(values) == 0 {
		return nil, nil
	}
	out := make([]any, 0, len(values))
	for _, value := range values {
		switch v := value.(type) {
		case json.RawMessage:
			if !json.Valid(v) {
				return nil, fmt.Errorf("metadata predicate value must be valid JSON")
			}
			out = append(out, v)
		case []byte:
			if !json.Valid(v) {
				return nil, fmt.Errorf("metadata predicate value must be valid JSON")
			}
			out = append(out, json.RawMessage(v))
		default:
			encoded, err := json.Marshal(v)
			if err != nil {
				return nil, fmt.Errorf("metadata predicate value must be JSON-serializable: %w", err)
			}
			out = append(out, json.RawMessage(encoded))
		}
	}
	return out, nil
}

func concreteMetadataPredicatesToPgwf(predicates []jobdb.MetadataPredicate) []pgwf.MetadataPredicate {
	if len(predicates) == 0 {
		return nil
	}
	out := make([]pgwf.MetadataPredicate, 0, len(predicates))
	for _, predicate := range predicates {
		values := append([]any(nil), predicate.Values...)
		out = append(out, pgwf.MetadataPredicate{
			Path:   append([]string{"app"}, predicate.Path...),
			Values: values,
		})
	}
	return out
}

func durationToLeaseSeconds(d time.Duration) int {
	if d == 0 {
		return 0
	}
	if d < 0 {
		return -1
	}
	seconds := int((d + time.Second - 1) / time.Second)
	if seconds < 1 {
		return 1
	}
	return seconds
}

func fromStrataArtifact(strataArt strata.Artifact) jobdb.Artifact {
	return &strataArtifactAdapter{art: strataArt}
}

func toStrataArtifact(art jobdb.Artifact) strata.Artifact {
	if adapter, ok := art.(*strataArtifactAdapter); ok {
		return adapter.art
	}
	return &jobdbToStrataAdapter{art: art}
}

// FromStrataArtifactForRuntime exposes the direct artifact adapter to runtime packages.
func FromStrataArtifactForRuntime(strataArt strata.Artifact) jobdb.Artifact {
	return fromStrataArtifact(strataArt)
}

// ToStrataArtifactForRuntime exposes the reverse artifact adapter to runtime packages.
func ToStrataArtifactForRuntime(art jobdb.Artifact) strata.Artifact {
	return toStrataArtifact(art)
}

type strataArtifactAdapter struct {
	art strata.Artifact
	key atomic.Pointer[jobdb.ArtifactKey]
}

func (a *strataArtifactAdapter) Name() string { return a.art.Name() }
func (a *strataArtifactAdapter) Size() int64  { return a.art.SizeBytes() }
func (a *strataArtifactAdapter) Sha256(ctx context.Context) (string, error) {
	return a.art.Sha256(ctx)
}
func (a *strataArtifactAdapter) WriteTo(ctx context.Context, w io.Writer) error {
	return a.art.WriteTo(ctx, w)
}
func (a *strataArtifactAdapter) SaveToFile(ctx context.Context, path string) error {
	return a.art.SaveToFile(ctx, path)
}
func (a *strataArtifactAdapter) Bytes(ctx context.Context) ([]byte, error) {
	return a.art.Bytes(ctx)
}
func (a *strataArtifactAdapter) Open() (io.ReadCloser, error) {
	_, rc, err := a.art.ToInput(context.Background())
	return rc, err
}
func (a *strataArtifactAdapter) ArtifactKey() (jobdb.ArtifactKey, error) {
	if value := a.key.Load(); value != nil {
		return *value, nil
	}
	return jobdb.ArtifactKey{}, jobdb.ErrArtifactKeyUnavailable
}
func (a *strataArtifactAdapter) setArtifactKey(key jobdb.ArtifactKey) { a.key.Store(&key) }
func (a *strataArtifactAdapter) Cleanup() error {
	if cleanup, ok := a.art.(interface{ Cleanup() error }); ok {
		return cleanup.Cleanup()
	}
	return nil
}

type jobdbToStrataAdapter struct {
	art jobdb.Artifact
}

func (a *jobdbToStrataAdapter) ID() string                                 { return "" }
func (a *jobdbToStrataAdapter) Name() string                               { return a.art.Name() }
func (a *jobdbToStrataAdapter) ContentType() string                        { return "application/octet-stream" }
func (a *jobdbToStrataAdapter) SizeBytes() int64                           { return a.art.Size() }
func (a *jobdbToStrataAdapter) Sha256(ctx context.Context) (string, error) { return a.art.Sha256(ctx) }
func (a *jobdbToStrataAdapter) WriteTo(ctx context.Context, w io.Writer) error {
	return a.art.WriteTo(ctx, w)
}
func (a *jobdbToStrataAdapter) SaveToFile(ctx context.Context, path string) error {
	return a.art.SaveToFile(ctx, path)
}
func (a *jobdbToStrataAdapter) Bytes(ctx context.Context) ([]byte, error) {
	return a.art.Bytes(ctx)
}
func (a *jobdbToStrataAdapter) ToInput(ctx context.Context) (strata.Descriptor, io.ReadCloser, error) {
	rc, err := a.art.Open()
	if err != nil {
		return strata.Descriptor{}, nil, err
	}
	return strata.Descriptor{
		Name:        a.art.Name(),
		ContentType: "application/octet-stream",
		SizeBytes:   a.art.Size(),
	}, rc, nil
}
