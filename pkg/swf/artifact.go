package swf

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"sync"
	"sync/atomic"

	strata "github.com/colony-2/strata-go/pkg/client/artifact"
)

// Artifact represents a file-like resource that can be consumed by tasks
// and persisted in workflow storage. Artifacts support lazy loading and
// automatic cleanup of temporary resources.
type Artifact interface {
	// Name returns the artifact name (e.g., "output.tar.gz")
	Name() string

	// Size returns the artifact size in bytes, or -1 if unknown.
	Size() int64

	// Sha256 returns the SHA256 hash of the artifact contents.
	Sha256(ctx context.Context) (string, error)

	// WriteTo writes the artifact contents to the provided writer.
	WriteTo(ctx context.Context, w io.Writer) error

	// SaveToFile saves the artifact contents to a file at the specified path.
	SaveToFile(ctx context.Context, path string) error

	// Bytes reads and returns the entire artifact contents as a byte slice.
	// For large artifacts, prefer using Open() or WriteTo() to stream the data.
	Bytes(ctx context.Context) ([]byte, error)

	// Open returns a ReadCloser to stream the artifact contents.
	// Multiple calls to Open() may return independent readers.
	// This is a convenience method not in strata.Artifact.
	Open() (io.ReadCloser, error)

	// ArtifactKey returns the key for this artifact after it has been persisted.
	// New artifacts return an error until persistence assigns a key.
	ArtifactKey() (ArtifactKey, error)

	// Cleanup is called by SWF after the artifact has been fully consumed
	// and is no longer needed. Implementations should clean up any temporary
	// resources (files, directories, connections, etc.).
	//
	// Cleanup may be called multiple times and must be idempotent.
	// Cleanup must not return an error that would halt workflow execution.
	// Errors should be logged but not propagated.
	//
	// For artifacts without cleanup needs, return nil.
	Cleanup() error
}

// ErrArtifactKeyUnavailable indicates the artifact has not been persisted yet.
var ErrArtifactKeyUnavailable = errors.New("artifact key is only available for artifacts that have been persisted")

// artifactKeySetter allows SWF to update the artifact key after persistence.
type artifactKeySetter interface {
	setArtifactKey(key ArtifactKey)
}

// AssignArtifactKey updates the key on artifacts that support it.
// This is intended for use by SWF persistence code after the artifact is saved.
func AssignArtifactKey(art Artifact, key ArtifactKey) {
	if setter, ok := art.(artifactKeySetter); ok {
		setter.setArtifactKey(key)
	}
}

func loadArtifactKey(key *atomic.Pointer[ArtifactKey]) (ArtifactKey, error) {
	if key == nil {
		return ArtifactKey{}, ErrArtifactKeyUnavailable
	}
	if value := key.Load(); value != nil {
		return *value, nil
	}
	return ArtifactKey{}, ErrArtifactKeyUnavailable
}

// NewArtifactFromBytes creates an in-memory artifact from bytes.
// No cleanup needed (no temporary resources).
//
// Example:
//
//	art := swf.NewArtifactFromBytes("config.json", jsonBytes)
func NewArtifactFromBytes(name string, data []byte) Artifact {
	return &bytesArtifact{
		name: name,
		data: data,
	}
}

// NewArtifactFromReader creates an artifact from an io.Reader.
// The reader will be consumed on first Open() call.
// If size is unknown, pass -1.
//
// Example:
//
//	art := swf.NewArtifactFromReader("output.txt", reader, 1024)
func NewArtifactFromReader(name string, r io.Reader, size int64) Artifact {
	return &readerArtifact{
		name:   name,
		reader: r,
		size:   size,
	}
}

// NewArtifactFromFile creates a lazy file-based artifact.
// The file is streamed on Open() without loading into memory.
// The file will be automatically removed when SWF is done (cleanup).
//
// Example:
//
//	art, _ := swf.NewArtifactFromFile("build.tar.gz", "/tmp/build.tar.gz")
func NewArtifactFromFile(name string, filePath string) (Artifact, error) {
	info, err := os.Stat(filePath)
	if err != nil {
		return nil, fmt.Errorf("stat file: %w", err)
	}

	return &fileArtifact{
		name:      name,
		path:      filePath,
		size:      info.Size(),
		autoClean: true,
	}, nil
}

// NewArtifactFromFileNoCleanup creates a lazy file-based artifact
// without automatic cleanup. Use this for non-temporary files.
//
// Example:
//
//	art, _ := swf.NewArtifactFromFileNoCleanup("input.txt", "/data/input.txt")
func NewArtifactFromFileNoCleanup(name string, filePath string) (Artifact, error) {
	info, err := os.Stat(filePath)
	if err != nil {
		return nil, fmt.Errorf("stat file: %w", err)
	}

	return &fileArtifact{
		name:      name,
		path:      filePath,
		size:      info.Size(),
		autoClean: false,
	}, nil
}

// NewArtifact creates a custom artifact with full control.
// Provide opener function and optional cleanup function.
// This is the low-level API for advanced use cases.
//
// Example:
//
//	art := swf.NewArtifact("custom", func() (io.ReadCloser, int64, error) {
//	    f, _ := os.Open(path)
//	    info, _ := f.Stat()
//	    return f, info.Size(), nil
//	}, func() error {
//	    return os.Remove(path)
//	})
func NewArtifact(
	name string,
	opener func() (io.ReadCloser, int64, error),
	cleanup func() error,
) Artifact {
	return &customArtifact{
		name:    name,
		opener:  opener,
		cleanup: cleanup,
	}
}

// FromStrataArtifact wraps a strata.Artifact as a swf.Artifact.
// This is used internally by SWF to bridge strata artifacts.
// Users should not need to call this directly.
func FromStrataArtifact(strataArt strata.Artifact) Artifact {
	return &strataArtifactAdapter{art: strataArt}
}

// ToStrataArtifact converts a swf.Artifact to a strata.Artifact.
// If the artifact is already a strata adapter, returns the underlying strata artifact.
// Otherwise, wraps it in a swf-to-strata adapter.
func ToStrataArtifact(art Artifact) strata.Artifact {
	// If it's already a strata adapter, return the underlying artifact
	if adapter, ok := art.(*strataArtifactAdapter); ok {
		return adapter.art
	}
	// Otherwise, wrap it
	return &swfToStrataAdapter{art: art}
}

// bytesArtifact - in-memory artifact
type bytesArtifact struct {
	name string
	data []byte
	hash atomic.Pointer[string]
	key  atomic.Pointer[ArtifactKey]
}

func (a *bytesArtifact) Name() string { return a.name }
func (a *bytesArtifact) Size() int64  { return int64(len(a.data)) }
func (a *bytesArtifact) Open() (io.ReadCloser, error) {
	return io.NopCloser(bytes.NewReader(a.data)), nil
}
func (a *bytesArtifact) ArtifactKey() (ArtifactKey, error) {
	return loadArtifactKey(&a.key)
}
func (a *bytesArtifact) Sha256(ctx context.Context) (string, error) {
	if h := a.hash.Load(); h != nil {
		return *h, nil
	}
	h, err := computeSha256(bytes.NewReader(a.data))
	if err != nil {
		return "", err
	}
	a.hash.Store(&h)
	return h, nil
}
func (a *bytesArtifact) WriteTo(ctx context.Context, w io.Writer) error {
	_, err := w.Write(a.data)
	return err
}
func (a *bytesArtifact) SaveToFile(ctx context.Context, path string) error {
	return os.WriteFile(path, a.data, 0644)
}
func (a *bytesArtifact) Bytes(ctx context.Context) ([]byte, error) {
	// Return a copy to prevent mutation
	result := make([]byte, len(a.data))
	copy(result, a.data)
	return result, nil
}
func (a *bytesArtifact) setArtifactKey(key ArtifactKey) {
	a.key.Store(&key)
}
func (a *bytesArtifact) Cleanup() error { return nil }

// readerArtifact - one-time reader artifact
type readerArtifact struct {
	name   string
	reader io.Reader
	size   int64
	once   sync.Once
	hash   atomic.Pointer[string]
	key    atomic.Pointer[ArtifactKey]
}

func (a *readerArtifact) Name() string { return a.name }
func (a *readerArtifact) Size() int64  { return a.size }
func (a *readerArtifact) Open() (io.ReadCloser, error) {
	var r io.Reader
	a.once.Do(func() { r = a.reader })
	if r == nil {
		return nil, fmt.Errorf("reader already consumed")
	}
	if rc, ok := r.(io.ReadCloser); ok {
		return rc, nil
	}
	return io.NopCloser(r), nil
}
func (a *readerArtifact) ArtifactKey() (ArtifactKey, error) {
	return loadArtifactKey(&a.key)
}
func (a *readerArtifact) Sha256(ctx context.Context) (string, error) {
	if h := a.hash.Load(); h != nil {
		return *h, nil
	}
	// For reader artifacts, we can't compute hash without consuming
	// Return empty hash
	return "", nil
}
func (a *readerArtifact) WriteTo(ctx context.Context, w io.Writer) error {
	rc, err := a.Open()
	if err != nil {
		return err
	}
	defer rc.Close()
	_, err = io.Copy(w, rc)
	return err
}
func (a *readerArtifact) SaveToFile(ctx context.Context, path string) error {
	rc, err := a.Open()
	if err != nil {
		return err
	}
	defer rc.Close()

	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = io.Copy(f, rc)
	return err
}
func (a *readerArtifact) Bytes(ctx context.Context) ([]byte, error) {
	rc, err := a.Open()
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	return io.ReadAll(rc)
}
func (a *readerArtifact) setArtifactKey(key ArtifactKey) {
	a.key.Store(&key)
}
func (a *readerArtifact) Cleanup() error { return nil }

// fileArtifact - file-based artifact with optional cleanup
type fileArtifact struct {
	name      string
	path      string
	size      int64
	autoClean bool
	cleaned   atomic.Bool
	hash      atomic.Pointer[string]
	key       atomic.Pointer[ArtifactKey]
}

func (a *fileArtifact) Name() string { return a.name }
func (a *fileArtifact) Size() int64  { return a.size }
func (a *fileArtifact) Open() (io.ReadCloser, error) {
	return os.Open(a.path)
}
func (a *fileArtifact) ArtifactKey() (ArtifactKey, error) {
	return loadArtifactKey(&a.key)
}
func (a *fileArtifact) Sha256(ctx context.Context) (string, error) {
	if h := a.hash.Load(); h != nil {
		return *h, nil
	}
	f, err := os.Open(a.path)
	if err != nil {
		return "", err
	}
	defer f.Close()
	h, err := computeSha256(f)
	if err != nil {
		return "", err
	}
	a.hash.Store(&h)
	return h, nil
}
func (a *fileArtifact) WriteTo(ctx context.Context, w io.Writer) error {
	f, err := os.Open(a.path)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = io.Copy(w, f)
	return err
}
func (a *fileArtifact) SaveToFile(ctx context.Context, path string) error {
	// If same path, no-op
	if a.path == path {
		return nil
	}
	// Copy file
	src, err := os.Open(a.path)
	if err != nil {
		return err
	}
	defer src.Close()

	dst, err := os.Create(path)
	if err != nil {
		return err
	}
	defer dst.Close()

	_, err = io.Copy(dst, src)
	return err
}
func (a *fileArtifact) Bytes(ctx context.Context) ([]byte, error) {
	return os.ReadFile(a.path)
}
func (a *fileArtifact) setArtifactKey(key ArtifactKey) {
	a.key.Store(&key)
}
func (a *fileArtifact) Cleanup() error {
	if !a.autoClean {
		return nil
	}
	if !a.cleaned.CompareAndSwap(false, true) {
		return nil // Already cleaned (idempotent)
	}
	return os.Remove(a.path)
}

// customArtifact - custom artifact with user-provided functions
type customArtifact struct {
	name    string
	opener  func() (io.ReadCloser, int64, error)
	cleanup func() error
	size    int64
	cleaned atomic.Bool
	hash    atomic.Pointer[string]
	key     atomic.Pointer[ArtifactKey]
}

func (a *customArtifact) Name() string { return a.name }
func (a *customArtifact) Size() int64  { return a.size }
func (a *customArtifact) Open() (io.ReadCloser, error) {
	rc, size, err := a.opener()
	if err != nil {
		return nil, err
	}
	a.size = size
	return rc, nil
}
func (a *customArtifact) ArtifactKey() (ArtifactKey, error) {
	return loadArtifactKey(&a.key)
}
func (a *customArtifact) Sha256(ctx context.Context) (string, error) {
	if h := a.hash.Load(); h != nil {
		return *h, nil
	}
	rc, _, err := a.opener()
	if err != nil {
		return "", err
	}
	defer rc.Close()
	h, err := computeSha256(rc)
	if err != nil {
		return "", err
	}
	a.hash.Store(&h)
	return h, nil
}
func (a *customArtifact) WriteTo(ctx context.Context, w io.Writer) error {
	rc, _, err := a.opener()
	if err != nil {
		return err
	}
	defer rc.Close()
	_, err = io.Copy(w, rc)
	return err
}
func (a *customArtifact) SaveToFile(ctx context.Context, path string) error {
	rc, _, err := a.opener()
	if err != nil {
		return err
	}
	defer rc.Close()

	f, err := os.Create(path)
	if err != nil {
		return err
	}
	defer f.Close()

	_, err = io.Copy(f, rc)
	return err
}
func (a *customArtifact) Bytes(ctx context.Context) ([]byte, error) {
	rc, _, err := a.opener()
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	return io.ReadAll(rc)
}
func (a *customArtifact) setArtifactKey(key ArtifactKey) {
	a.key.Store(&key)
}
func (a *customArtifact) Cleanup() error {
	if a.cleanup == nil {
		return nil
	}
	if !a.cleaned.CompareAndSwap(false, true) {
		return nil // Already cleaned (idempotent)
	}
	return a.cleanup()
}

// strataArtifactAdapter wraps a strata.Artifact as a swf.Artifact
type strataArtifactAdapter struct {
	art strata.Artifact
	key atomic.Pointer[ArtifactKey]
}

func (a *strataArtifactAdapter) Name() string {
	return a.art.Name()
}

func (a *strataArtifactAdapter) Size() int64 {
	return a.art.SizeBytes()
}

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

func (a *strataArtifactAdapter) ArtifactKey() (ArtifactKey, error) {
	return loadArtifactKey(&a.key)
}

func (a *strataArtifactAdapter) setArtifactKey(key ArtifactKey) {
	a.key.Store(&key)
}

func (a *strataArtifactAdapter) Cleanup() error {
	// Check if strata artifact has cleanup interface
	if cleanup, ok := a.art.(interface{ Cleanup() error }); ok {
		return cleanup.Cleanup()
	}
	return nil
}

// swfToStrataAdapter wraps a swf.Artifact as a strata.Artifact
type swfToStrataAdapter struct {
	art Artifact
}

func (a *swfToStrataAdapter) ID() string {
	return ""
}

func (a *swfToStrataAdapter) Name() string {
	return a.art.Name()
}

func (a *swfToStrataAdapter) ContentType() string {
	return "application/octet-stream"
}

func (a *swfToStrataAdapter) SizeBytes() int64 {
	return a.art.Size()
}

func (a *swfToStrataAdapter) Sha256(ctx context.Context) (string, error) {
	return a.art.Sha256(ctx)
}

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

	desc := strata.Descriptor{
		Name:        a.art.Name(),
		ContentType: "application/octet-stream",
		SizeBytes:   a.art.Size(),
	}

	return desc, rc, nil
}
