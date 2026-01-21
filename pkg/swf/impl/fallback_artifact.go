package impl

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"

	strataclient "github.com/colony-2/strata-go/pkg/client"
	strataartifact "github.com/colony-2/strata-go/pkg/client/artifact"
	"github.com/colony-2/strata-go/pkg/client/story"
	"github.com/colony-2/swf-go/pkg/swf"
)

type fallbackArtifact struct {
	primary  swf.Artifact
	fallback swf.Artifact
}

func (a *fallbackArtifact) ID() string {
	if a.primary != nil && a.primary.ID() != "" {
		return a.primary.ID()
	}
	if a.fallback != nil {
		return a.fallback.ID()
	}
	return ""
}

func (a *fallbackArtifact) Name() string {
	if a.primary != nil && a.primary.Name() != "" {
		return a.primary.Name()
	}
	if a.fallback != nil {
		return a.fallback.Name()
	}
	return ""
}

func (a *fallbackArtifact) ContentType() string {
	if a.primary != nil && a.primary.ContentType() != "" {
		return a.primary.ContentType()
	}
	if a.fallback != nil {
		return a.fallback.ContentType()
	}
	return ""
}

func (a *fallbackArtifact) Size() int64 {
	if a.primary != nil {
		if size := a.primary.Size(); size >= 0 {
			return size
		}
	}
	if a.fallback != nil {
		return a.fallback.Size()
	}
	return -1
}

func (a *fallbackArtifact) Sha256(ctx context.Context) (string, error) {
	if a.primary != nil {
		digest, err := a.primary.Sha256(ctx)
		if err == nil {
			return digest, nil
		}
		if a.fallback != nil {
			return a.fallback.Sha256(ctx)
		}
		return "", err
	}
	if a.fallback != nil {
		return a.fallback.Sha256(ctx)
	}
	return "", errors.New("artifact missing primary and fallback")
}

func (a *fallbackArtifact) WriteTo(ctx context.Context, w io.Writer) error {
	if a.primary != nil {
		if err := a.primary.WriteTo(ctx, w); err == nil || a.fallback == nil {
			return err
		}
	}
	if a.fallback != nil {
		return a.fallback.WriteTo(ctx, w)
	}
	return errors.New("artifact missing primary and fallback")
}

func (a *fallbackArtifact) SaveToFile(ctx context.Context, path string) error {
	if a.primary != nil {
		if err := a.primary.SaveToFile(ctx, path); err == nil || a.fallback == nil {
			return err
		}
	}
	if a.fallback != nil {
		return a.fallback.SaveToFile(ctx, path)
	}
	return errors.New("artifact missing primary and fallback")
}

func (a *fallbackArtifact) Bytes(ctx context.Context) ([]byte, error) {
	if a.primary != nil {
		if b, err := a.primary.Bytes(ctx); err == nil || a.fallback == nil {
			return b, err
		}
	}
	if a.fallback != nil {
		return a.fallback.Bytes(ctx)
	}
	return nil, errors.New("artifact missing primary and fallback")
}

func (a *fallbackArtifact) Open() (io.ReadCloser, error) {
	if a.primary != nil {
		if rc, err := a.primary.Open(); err == nil || a.fallback == nil {
			return rc, err
		}
	}
	if a.fallback != nil {
		return a.fallback.Open()
	}
	return nil, errors.New("artifact missing primary and fallback")
}

func (a *fallbackArtifact) ArtifactKey() (swf.ArtifactKey, error) {
	if a.primary != nil {
		if key, err := a.primary.ArtifactKey(); err == nil {
			return key, nil
		}
	}
	if a.fallback != nil {
		if key, err := a.fallback.ArtifactKey(); err == nil {
			return key, nil
		}
	}
	return swf.ArtifactKey{}, swf.ErrArtifactKeyUnavailable
}

func (a *fallbackArtifact) Cleanup() error {
	if a.primary != nil {
		return a.primary.Cleanup()
	}
	return nil
}

type strataRemoteArtifact struct {
	client      *strataclient.Client
	anthologyID string
	storyID     string
	ordinal     int64
	artifactID  string
	name        string
	contentType string
	sizeBytes   int64
	sha256      string
}

func (a *strataRemoteArtifact) ID() string          { return a.artifactID }
func (a *strataRemoteArtifact) Name() string        { return a.name }
func (a *strataRemoteArtifact) ContentType() string { return a.contentType }
func (a *strataRemoteArtifact) Size() int64         { return a.sizeBytes }

func (a *strataRemoteArtifact) Sha256(ctx context.Context) (string, error) {
	if a.sha256 != "" {
		return a.sha256, nil
	}
	remote, err := a.remote()
	if err != nil {
		return "", err
	}
	return remote.Sha256(ctx)
}

func (a *strataRemoteArtifact) WriteTo(ctx context.Context, w io.Writer) error {
	remote, err := a.remote()
	if err != nil {
		return err
	}
	return remote.WriteTo(ctx, w)
}

func (a *strataRemoteArtifact) SaveToFile(ctx context.Context, path string) error {
	remote, err := a.remote()
	if err != nil {
		return err
	}
	return remote.SaveToFile(ctx, path)
}

func (a *strataRemoteArtifact) Bytes(ctx context.Context) ([]byte, error) {
	remote, err := a.remote()
	if err != nil {
		return nil, err
	}
	return remote.Bytes(ctx)
}

func (a *strataRemoteArtifact) Open() (io.ReadCloser, error) {
	remote, err := a.remote()
	if err != nil {
		return nil, err
	}
	_, rc, err := remote.ToInput(context.Background())
	return rc, err
}

func (a *strataRemoteArtifact) ArtifactKey() (swf.ArtifactKey, error) {
	if a.storyID == "" || a.name == "" {
		return swf.ArtifactKey{}, swf.ErrArtifactKeyUnavailable
	}
	return swf.ArtifactKey{
		JobId:       a.storyID,
		TaskOrdinal: a.ordinal,
		Name:        a.name,
	}, nil
}

func (a *strataRemoteArtifact) Cleanup() error {
	return nil
}

func (a *strataRemoteArtifact) remote() (strataartifact.Artifact, error) {
	if a.client == nil {
		return nil, fmt.Errorf("strata client is required")
	}
	if a.name == "" {
		return nil, fmt.Errorf("artifact name is required")
	}
	loc := strataartifact.Locator{
		AnthologyID: a.anthologyID,
		StoryID:     a.storyID,
		Ordinal:     a.ordinal,
		Name:        a.name,
	}
	desc := strataartifact.Descriptor{
		Name:        a.name,
		ContentType: a.contentType,
		SizeBytes:   a.sizeBytes,
		Sha256:      a.sha256,
	}
	opts := []strataartifact.Option{}
	if a.artifactID != "" {
		opts = append(opts, strataartifact.WithID(a.artifactID))
	}
	if a.sha256 != "" {
		opts = append(opts, strataartifact.WithSha256(a.sha256))
	}
	return strataartifact.FromRemote(desc, loc, a.client.Core(), opts...), nil
}

func validateOutputArtifacts(ctx context.Context, artifacts []swf.Artifact) ([]string, error) {
	if len(artifacts) == 0 {
		return nil, nil
	}
	if ctx == nil {
		ctx = context.Background()
	}
	digests := make([]string, 0, len(artifacts))
	for idx, art := range artifacts {
		if art == nil {
			return nil, fmt.Errorf("artifact %d is nil", idx)
		}
		if art.Name() == "" {
			return nil, fmt.Errorf("artifact %d is missing name", idx)
		}
		digest, err := art.Sha256(ctx)
		if err != nil {
			return nil, fmt.Errorf("artifact %d sha256: %w", idx, err)
		}
		if digest == "" {
			return nil, fmt.Errorf("artifact %d sha256 is empty", idx)
		}
		digests = append(digests, digest)
	}
	return digests, nil
}

func wrapOutputArtifactsWithFallback(output swf.TaskData, dataBytes swf.Data, primary []swf.Artifact, digests []string, key story.Key, ordinal int64, client *strataclient.Client, logger *slog.Logger) (swf.TaskData, error) {
	if output == nil || len(primary) == 0 || client == nil {
		return output, nil
	}
	if len(digests) != len(primary) {
		return nil, fmt.Errorf("artifact digest count mismatch")
	}

	wrapped := make([]swf.Artifact, 0, len(primary))
	for idx, art := range primary {
		if art == nil {
			wrapped = append(wrapped, art)
			continue
		}
		remote, err := newStrataRemoteArtifact(art, digests[idx], key, ordinal, client, logger)
		if err != nil {
			return nil, err
		}
		wrapped = append(wrapped, &fallbackArtifact{
			primary:  art,
			fallback: remote,
		})
	}

	replaced, err := replaceTaskDataArtifacts(output, dataBytes, wrapped)
	if err != nil {
		if logger != nil {
			logger.Warn("failed to replace task output artifacts", "error", err, "ordinal", ordinal)
		}
		return nil, err
	}
	return replaced, nil
}

func newStrataRemoteArtifact(primary swf.Artifact, digest string, key story.Key, ordinal int64, client *strataclient.Client, logger *slog.Logger) (swf.Artifact, error) {
	if primary == nil {
		return nil, fmt.Errorf("primary artifact is required")
	}
	name := primary.Name()
	if name == "" {
		if logger != nil {
			logger.Warn("artifact name missing; skipping fallback wrapper", "ordinal", ordinal)
		}
		return nil, fmt.Errorf("artifact name is required")
	}
	if digest == "" {
		return nil, fmt.Errorf("artifact %s sha256 is required", name)
	}

	contentType := primary.ContentType()
	size := primary.Size()

	return &strataRemoteArtifact{
		client:      client,
		anthologyID: key.AnthologyID,
		storyID:     key.StoryID,
		ordinal:     ordinal,
		artifactID:  primary.ID(),
		name:        name,
		contentType: contentType,
		sizeBytes:   size,
		sha256:      digest,
	}, nil
}

func replaceTaskDataArtifacts(output swf.TaskData, dataBytes swf.Data, artifacts []swf.Artifact) (swf.TaskData, error) {
	if output == nil {
		return nil, nil
	}
	if dataBytes == nil {
		raw, err := output.GetData()
		if err != nil {
			return nil, err
		}
		dataBytes = raw
	}

	switch typed := output.(type) {
	case *swf.EnvelopedTaskData:
		return &swf.EnvelopedTaskData{
			SimpleTaskData: swf.SimpleTaskData{
				Data:      dataBytes,
				Artifacts: artifacts,
			},
			Kind: typed.Kind,
		}, nil
	case *swf.SimpleTaskData:
		return &swf.SimpleTaskData{
			Data:      dataBytes,
			Artifacts: artifacts,
		}, nil
	default:
		return &swf.SimpleTaskData{
			Data:      dataBytes,
			Artifacts: artifacts,
		}, nil
	}
}
