package jobdb

import (
	"context"
	"fmt"
	"io"
)

// ToLazyArtifact returns an artifact that defers retrieval until access.
// tenantId is required because ArtifactKey does not include tenant identity.
func (ak ArtifactKey) ToLazyArtifact(getter ArtifactGetter, tenantId string) Artifact {
	return &lazyArtifact{
		getter:   getter,
		tenantId: tenantId,
		key:      ak,
	}
}

type lazyArtifact struct {
	getter   ArtifactGetter
	tenantId string
	key      ArtifactKey
}

func (a *lazyArtifact) Name() string { return a.key.Name }
func (a *lazyArtifact) Size() int64  { return a.key.SizeBytes }

func (a *lazyArtifact) ArtifactKey() (ArtifactKey, error) {
	return a.key, nil
}

func (a *lazyArtifact) Sha256(ctx context.Context) (string, error) {
	art, err := a.materialize(ctx)
	if err != nil {
		return "", err
	}
	return art.Sha256(ctx)
}

func (a *lazyArtifact) WriteTo(ctx context.Context, w io.Writer) error {
	art, err := a.materialize(ctx)
	if err != nil {
		return err
	}
	return art.WriteTo(ctx, w)
}

func (a *lazyArtifact) SaveToFile(ctx context.Context, path string) error {
	art, err := a.materialize(ctx)
	if err != nil {
		return err
	}
	return art.SaveToFile(ctx, path)
}

func (a *lazyArtifact) Bytes(ctx context.Context) ([]byte, error) {
	art, err := a.materialize(ctx)
	if err != nil {
		return nil, err
	}
	return art.Bytes(ctx)
}

func (a *lazyArtifact) Open() (io.ReadCloser, error) {
	art, err := a.materialize(context.Background())
	if err != nil {
		return nil, err
	}
	return art.Open()
}

func (a *lazyArtifact) Cleanup() error { return nil }

func (a *lazyArtifact) materialize(ctx context.Context) (Artifact, error) {
	if a.getter == nil {
		return nil, fmt.Errorf("artifact getter is required")
	}
	if a.tenantId == "" {
		return nil, fmt.Errorf("tenantId is required")
	}
	if err := a.key.Validate(); err != nil {
		return nil, err
	}
	return a.getter.GetArtifact(a.tenantId, a.key)
}
