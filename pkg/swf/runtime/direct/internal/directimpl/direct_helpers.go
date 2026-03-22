package directimpl

import (
	"context"
	"io"
	"sort"
	"sync/atomic"
	"time"

	"github.com/colony-2/pgwf-go/pkg/pgwf"
	strata "github.com/colony-2/strata-go/pkg/client/artifact"
	"github.com/colony-2/strata-go/pkg/client/story"
	"github.com/colony-2/swf-go/pkg/swf"
)

func storyKeyForJob(jobKey swf.JobKey) story.Key {
	return story.Key{
		AnthologyID: jobKey.TenantId,
		StoryID:     jobKey.JobId,
	}
}

func workSetCapabilities(workset *swf.WorkSet) []pgwf.Capability {
	if workset == nil || len(workset.TaskWorkers) == 0 {
		return nil
	}
	names := make([]string, 0, len(workset.TaskWorkers))
	for name := range workset.TaskWorkers {
		names = append(names, name)
	}
	sort.Strings(names)
	out := make([]pgwf.Capability, 0, len(names))
	for _, name := range names {
		out = append(out, pgwf.Capability(workset.JobWorker.Name()+":"+name))
	}
	return out
}

func metadataPredicatesToPgwf(filter swf.MetadataFilter) ([]pgwf.MetadataPredicate, error) {
	preds, err := swf.MetadataPredicates(filter)
	if err != nil {
		return nil, err
	}
	out := make([]pgwf.MetadataPredicate, 0, len(preds))
	for _, pred := range preds {
		out = append(out, pgwf.MetadataPredicate{
			Path:   pred.Path,
			Values: pred.Values,
		})
	}
	return out, nil
}

func concreteMetadataPredicatesToPgwf(predicates []swf.MetadataPredicate) []pgwf.MetadataPredicate {
	if len(predicates) == 0 {
		return nil
	}
	out := make([]pgwf.MetadataPredicate, 0, len(predicates))
	for _, predicate := range predicates {
		values := append([]any(nil), predicate.Values...)
		out = append(out, pgwf.MetadataPredicate{
			Path:   append([]string(nil), predicate.Path...),
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

func fromStrataArtifact(strataArt strata.Artifact) swf.Artifact {
	return &strataArtifactAdapter{art: strataArt}
}

func toStrataArtifact(art swf.Artifact) strata.Artifact {
	if adapter, ok := art.(*strataArtifactAdapter); ok {
		return adapter.art
	}
	return &swfToStrataAdapter{art: art}
}

// FromStrataArtifactForRuntime exposes the direct artifact adapter to runtime packages.
func FromStrataArtifactForRuntime(strataArt strata.Artifact) swf.Artifact {
	return fromStrataArtifact(strataArt)
}

// ToStrataArtifactForRuntime exposes the reverse artifact adapter to runtime packages.
func ToStrataArtifactForRuntime(art swf.Artifact) strata.Artifact {
	return toStrataArtifact(art)
}

type strataArtifactAdapter struct {
	art strata.Artifact
	key atomic.Pointer[swf.ArtifactKey]
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
func (a *strataArtifactAdapter) ArtifactKey() (swf.ArtifactKey, error) {
	if value := a.key.Load(); value != nil {
		return *value, nil
	}
	return swf.ArtifactKey{}, swf.ErrArtifactKeyUnavailable
}
func (a *strataArtifactAdapter) setArtifactKey(key swf.ArtifactKey) { a.key.Store(&key) }
func (a *strataArtifactAdapter) Cleanup() error {
	if cleanup, ok := a.art.(interface{ Cleanup() error }); ok {
		return cleanup.Cleanup()
	}
	return nil
}

type swfToStrataAdapter struct {
	art swf.Artifact
}

func (a *swfToStrataAdapter) ID() string                                 { return "" }
func (a *swfToStrataAdapter) Name() string                               { return a.art.Name() }
func (a *swfToStrataAdapter) ContentType() string                        { return "application/octet-stream" }
func (a *swfToStrataAdapter) SizeBytes() int64                           { return a.art.Size() }
func (a *swfToStrataAdapter) Sha256(ctx context.Context) (string, error) { return a.art.Sha256(ctx) }
func (a *swfToStrataAdapter) WriteTo(ctx context.Context, w io.Writer) error {
	return a.art.WriteTo(ctx, w)
}
func (a *swfToStrataAdapter) SaveToFile(ctx context.Context, path string) error {
	return a.art.SaveToFile(ctx, path)
}
func (a *swfToStrataAdapter) Bytes(ctx context.Context) ([]byte, error) {
	return a.art.Bytes(ctx)
}
func (a *swfToStrataAdapter) ToInput(ctx context.Context) (strata.Descriptor, io.ReadCloser, error) {
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
