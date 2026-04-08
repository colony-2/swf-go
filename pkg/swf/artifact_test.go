package swf_test

import (
	"bytes"
	"context"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/colony-2/swf-go/pkg/swf"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewArtifactFromBytes(t *testing.T) {
	data := []byte("test data")
	art := swf.NewArtifactFromBytes("test.txt", data)

	assert.Equal(t, "test.txt", art.Name())
	assert.Equal(t, int64(len(data)), art.Size())

	// Test Open
	rc, err := art.Open()
	require.NoError(t, err)
	defer rc.Close()

	readData, err := io.ReadAll(rc)
	require.NoError(t, err)
	assert.Equal(t, data, readData)

	// Test Cleanup (should be no-op)
	err = art.Cleanup()
	assert.NoError(t, err)

	// Test SHA256
	ctx := context.Background()
	hash, err := art.Sha256(ctx)
	require.NoError(t, err)
	assert.NotEmpty(t, hash)
	assert.Equal(t, 64, len(hash)) // SHA256 is 64 hex chars

	// Second call should return cached hash
	hash2, err := art.Sha256(ctx)
	require.NoError(t, err)
	assert.Equal(t, hash, hash2)
}

func TestNewArtifactFromReader(t *testing.T) {
	data := []byte("test data from reader")
	reader := bytes.NewReader(data)
	art := swf.NewArtifactFromReader("output.txt", reader, int64(len(data)))

	assert.Equal(t, "output.txt", art.Name())
	assert.Equal(t, int64(len(data)), art.Size())

	// Test Open (first time)
	rc, err := art.Open()
	require.NoError(t, err)
	defer rc.Close()

	readData, err := io.ReadAll(rc)
	require.NoError(t, err)
	assert.Equal(t, data, readData)

	// Second Open should fail (reader already consumed)
	rc2, err := art.Open()
	assert.Error(t, err)
	assert.Nil(t, rc2)
	assert.Contains(t, err.Error(), "already consumed")

	// Test Cleanup (should be no-op)
	err = art.Cleanup()
	assert.NoError(t, err)
}

func TestNewArtifactFromFile(t *testing.T) {
	// Create temp file
	tmpFile, err := os.CreateTemp("", "test-artifact-*.txt")
	require.NoError(t, err)
	path := tmpFile.Name()

	data := []byte("test file data")
	_, err = tmpFile.Write(data)
	require.NoError(t, err)
	tmpFile.Close()

	// Create artifact
	art, err := swf.NewArtifactFromFile("test.txt", path)
	require.NoError(t, err)

	assert.Equal(t, "test.txt", art.Name())
	assert.Equal(t, int64(len(data)), art.Size())

	// Verify file exists
	_, err = os.Stat(path)
	assert.NoError(t, err)

	// Test Open
	rc, err := art.Open()
	require.NoError(t, err)
	readData, err := io.ReadAll(rc)
	require.NoError(t, err)
	rc.Close()
	assert.Equal(t, data, readData)

	// Test SHA256
	ctx := context.Background()
	hash, err := art.Sha256(ctx)
	require.NoError(t, err)
	assert.NotEmpty(t, hash)

	// Test Cleanup (should remove file)
	err = art.Cleanup()
	assert.NoError(t, err)

	// Verify file removed
	_, err = os.Stat(path)
	assert.True(t, os.IsNotExist(err))
}

func TestNewArtifactFromFileNoCleanup(t *testing.T) {
	// Create temp file
	tmpFile, err := os.CreateTemp("", "test-artifact-no-cleanup-*.txt")
	require.NoError(t, err)
	path := tmpFile.Name()
	defer os.Remove(path) // Clean up manually

	data := []byte("test file data no cleanup")
	_, err = tmpFile.Write(data)
	require.NoError(t, err)
	tmpFile.Close()

	// Create artifact without cleanup
	art, err := swf.NewArtifactFromFileNoCleanup("test.txt", path)
	require.NoError(t, err)

	assert.Equal(t, "test.txt", art.Name())
	assert.Equal(t, int64(len(data)), art.Size())

	// Test Cleanup (should be no-op)
	err = art.Cleanup()
	assert.NoError(t, err)

	// Verify file still exists
	_, err = os.Stat(path)
	assert.NoError(t, err)
}

func TestArtifactCleanup_Idempotent(t *testing.T) {
	// Create temp file
	tmpFile, err := os.CreateTemp("", "test-idempotent-*.txt")
	require.NoError(t, err)
	path := tmpFile.Name()
	tmpFile.WriteString("test data")
	tmpFile.Close()

	// Create artifact
	art, err := swf.NewArtifactFromFile("test.txt", path)
	require.NoError(t, err)

	// Call cleanup multiple times
	err1 := art.Cleanup()
	err2 := art.Cleanup()
	err3 := art.Cleanup()

	assert.NoError(t, err1)
	assert.NoError(t, err2) // Should be idempotent
	assert.NoError(t, err3) // Should be idempotent

	// File should be removed after first cleanup
	_, err = os.Stat(path)
	assert.True(t, os.IsNotExist(err))
}

func TestNewArtifact_Custom(t *testing.T) {
	// Create temp file
	tmpFile, err := os.CreateTemp("", "test-custom-*.txt")
	require.NoError(t, err)
	path := tmpFile.Name()
	data := []byte("custom artifact data")
	tmpFile.Write(data)
	tmpFile.Close()

	cleanupCalled := false
	openCount := 0

	art := swf.NewArtifact(
		"custom.txt",
		func() (io.ReadCloser, int64, error) {
			openCount++
			f, err := os.Open(path)
			if err != nil {
				return nil, 0, err
			}
			info, _ := f.Stat()
			return f, info.Size(), nil
		},
		func() error {
			cleanupCalled = true
			return os.Remove(path)
		},
	)

	assert.Equal(t, "custom.txt", art.Name())

	// Test Open
	rc, err := art.Open()
	require.NoError(t, err)
	readData, err := io.ReadAll(rc)
	require.NoError(t, err)
	rc.Close()
	assert.Equal(t, data, readData)
	assert.Equal(t, 1, openCount)

	// Test Cleanup
	assert.False(t, cleanupCalled)
	err = art.Cleanup()
	assert.NoError(t, err)
	assert.True(t, cleanupCalled)

	// Verify file removed
	_, err = os.Stat(path)
	assert.True(t, os.IsNotExist(err))

	// Test idempotent cleanup
	err = art.Cleanup()
	assert.NoError(t, err)
}

func TestNewArtifact_NilCleanup(t *testing.T) {
	data := []byte("data")
	art := swf.NewArtifact(
		"test.txt",
		func() (io.ReadCloser, int64, error) {
			return io.NopCloser(bytes.NewReader(data)), int64(len(data)), nil
		},
		nil, // No cleanup function
	)

	// Cleanup should be no-op
	err := art.Cleanup()
	assert.NoError(t, err)
}

func TestNewArtifact_Sha256PopulatesSize(t *testing.T) {
	data := []byte("size via sha")
	art := swf.NewArtifact(
		"sha-size.txt",
		func() (io.ReadCloser, int64, error) {
			return io.NopCloser(bytes.NewReader(data)), int64(len(data)), nil
		},
		nil,
	)

	assert.Equal(t, int64(0), art.Size())

	digest, err := art.Sha256(context.Background())
	require.NoError(t, err)
	assert.NotEmpty(t, digest)
	assert.Equal(t, int64(len(data)), art.Size())
}

func TestArtifactFromFile_NonExistent(t *testing.T) {
	art, err := swf.NewArtifactFromFile("test.txt", "/nonexistent/path/file.txt")
	assert.Error(t, err)
	assert.Nil(t, art)
	assert.Contains(t, err.Error(), "stat file")
}

func TestArtifactMultipleOpen(t *testing.T) {
	data := []byte("multiple open test")
	art := swf.NewArtifactFromBytes("test.txt", data)

	// Multiple opens should work for bytes artifact
	rc1, err := art.Open()
	require.NoError(t, err)
	defer rc1.Close()

	rc2, err := art.Open()
	require.NoError(t, err)
	defer rc2.Close()

	data1, _ := io.ReadAll(rc1)
	data2, _ := io.ReadAll(rc2)

	assert.Equal(t, data, data1)
	assert.Equal(t, data, data2)
}

func TestBytesArtifact_EmptyData(t *testing.T) {
	art := swf.NewArtifactFromBytes("empty.txt", []byte{})

	assert.Equal(t, "empty.txt", art.Name())
	assert.Equal(t, int64(0), art.Size())

	rc, err := art.Open()
	require.NoError(t, err)
	defer rc.Close()

	data, err := io.ReadAll(rc)
	require.NoError(t, err)
	assert.Equal(t, 0, len(data))
}

func TestFileArtifact_SHA256_Cached(t *testing.T) {
	// Create temp file
	tmpFile, err := os.CreateTemp("", "test-sha256-*.txt")
	require.NoError(t, err)
	path := tmpFile.Name()
	defer os.Remove(path)

	data := []byte("sha256 test data")
	tmpFile.Write(data)
	tmpFile.Close()

	art, err := swf.NewArtifactFromFile("test.txt", path)
	require.NoError(t, err)
	defer art.Cleanup()

	ctx := context.Background()

	// First call computes hash
	hash1, err := art.Sha256(ctx)
	require.NoError(t, err)

	// Second call should use cached hash
	hash2, err := art.Sha256(ctx)
	require.NoError(t, err)

	assert.Equal(t, hash1, hash2)
}

func TestArtifact_ConcurrentOperations(t *testing.T) {
	data := []byte("concurrent test")
	art := swf.NewArtifactFromBytes("test.txt", data)

	// Multiple goroutines opening and reading
	done := make(chan bool, 5)
	for i := 0; i < 5; i++ {
		go func() {
			rc, err := art.Open()
			if err != nil {
				t.Errorf("Open failed: %v", err)
				done <- false
				return
			}
			defer rc.Close()
			_, err = io.ReadAll(rc)
			if err != nil {
				t.Errorf("ReadAll failed: %v", err)
				done <- false
				return
			}
			done <- true
		}()
	}

	for i := 0; i < 5; i++ {
		assert.True(t, <-done)
	}
}

func TestArtifactReaderWithCloser(t *testing.T) {
	closeCalled := false
	data := []byte("test data")
	reader := &mockReadCloser{
		Reader: bytes.NewReader(data),
		onClose: func() error {
			closeCalled = true
			return nil
		},
	}

	art := swf.NewArtifactFromReader("test.txt", reader, int64(len(data)))

	rc, err := art.Open()
	require.NoError(t, err)

	// ReadCloser should be returned as-is
	assert.False(t, closeCalled)
	rc.Close()
	assert.True(t, closeCalled)
}

// mockReadCloser implements io.ReadCloser
type mockReadCloser struct {
	io.Reader
	onClose func() error
}

func (m *mockReadCloser) Close() error {
	if m.onClose != nil {
		return m.onClose()
	}
	return nil
}

func TestArtifactName_SpecialCharacters(t *testing.T) {
	testCases := []string{
		"file with spaces.txt",
		"file-with-dashes.tar.gz",
		"file_with_underscores.bin",
		"file.multiple.dots.txt",
		"файл.txt", // Unicode
	}

	for _, name := range testCases {
		t.Run(name, func(t *testing.T) {
			art := swf.NewArtifactFromBytes(name, []byte("data"))
			assert.Equal(t, name, art.Name())
		})
	}
}

func TestCustomArtifact_OpenerError(t *testing.T) {
	art := swf.NewArtifact(
		"error.txt",
		func() (io.ReadCloser, int64, error) {
			return nil, 0, os.ErrNotExist
		},
		nil,
	)

	rc, err := art.Open()
	assert.Error(t, err)
	assert.Nil(t, rc)
	assert.ErrorIs(t, err, os.ErrNotExist)
}

func TestCustomArtifact_CleanupError(t *testing.T) {
	customErr := assert.AnError

	art := swf.NewArtifact(
		"test.txt",
		func() (io.ReadCloser, int64, error) {
			return io.NopCloser(strings.NewReader("data")), 4, nil
		},
		func() error {
			return customErr
		},
	)

	err := art.Cleanup()
	assert.ErrorIs(t, err, customErr)

	// Second cleanup should not call cleanup again (idempotent)
	err = art.Cleanup()
	assert.NoError(t, err)
}

func TestFileArtifact_Directory(t *testing.T) {
	// Create temp directory
	tmpDir, err := os.MkdirTemp("", "test-dir-*")
	require.NoError(t, err)
	defer os.RemoveAll(tmpDir)

	// Create artifact from directory
	art, err := swf.NewArtifactFromFile("dir", tmpDir)
	require.NoError(t, err) // Stat succeeds for directories

	// Opening a directory succeeds on Linux but reading from it fails
	rc, err := art.Open()
	if err != nil {
		// On some systems, opening directory fails
		assert.Error(t, err)
		assert.Nil(t, rc)
	} else {
		// On Linux, opening succeeds but reading should fail or return no data
		defer rc.Close()
		buf := make([]byte, 1)
		_, readErr := rc.Read(buf)
		// Reading from directory should fail
		assert.Error(t, readErr)
	}
}

func TestArtifactSize_Unknown(t *testing.T) {
	art := swf.NewArtifactFromReader("test.txt", strings.NewReader("data"), -1)
	assert.Equal(t, int64(-1), art.Size())
}

func TestArtifact_Bytes(t *testing.T) {
	ctx := context.Background()
	testData := []byte("test artifact bytes")

	t.Run("bytes artifact", func(t *testing.T) {
		art := swf.NewArtifactFromBytes("test.txt", testData)
		data, err := art.Bytes(ctx)
		require.NoError(t, err)
		assert.Equal(t, testData, data)

		// Verify returned bytes are a copy (mutation doesn't affect original)
		data[0] = 'X'
		data2, _ := art.Bytes(ctx)
		assert.Equal(t, testData, data2)
	})

	t.Run("file artifact", func(t *testing.T) {
		tmpFile, err := os.CreateTemp("", "bytes-test-*.txt")
		require.NoError(t, err)
		path := tmpFile.Name()
		tmpFile.Write(testData)
		tmpFile.Close()
		defer os.Remove(path)

		art, err := swf.NewArtifactFromFileNoCleanup("test.txt", path)
		require.NoError(t, err)

		data, err := art.Bytes(ctx)
		require.NoError(t, err)
		assert.Equal(t, testData, data)
	})

	t.Run("reader artifact", func(t *testing.T) {
		art := swf.NewArtifactFromReader("test.txt", bytes.NewReader(testData), int64(len(testData)))
		data, err := art.Bytes(ctx)
		require.NoError(t, err)
		assert.Equal(t, testData, data)

		// Second call should fail (reader consumed)
		_, err = art.Bytes(ctx)
		assert.Error(t, err)
	})

	t.Run("custom artifact", func(t *testing.T) {
		art := swf.NewArtifact(
			"custom.txt",
			func() (io.ReadCloser, int64, error) {
				return io.NopCloser(bytes.NewReader(testData)), int64(len(testData)), nil
			},
			nil,
		)

		data, err := art.Bytes(ctx)
		require.NoError(t, err)
		assert.Equal(t, testData, data)
	})
}

func TestArtifact_BytesAndOpen(t *testing.T) {
	ctx := context.Background()
	testData := []byte("test data for both")

	art := swf.NewArtifactFromBytes("test.txt", testData)

	// Call Bytes first
	data1, err := art.Bytes(ctx)
	require.NoError(t, err)
	assert.Equal(t, testData, data1)

	// Then Open should still work
	rc, err := art.Open()
	require.NoError(t, err)
	defer rc.Close()
	data2, err := io.ReadAll(rc)
	require.NoError(t, err)
	assert.Equal(t, testData, data2)
}

func TestArtifact_WriteTo(t *testing.T) {
	ctx := context.Background()
	testData := []byte("write to test data")

	t.Run("bytes artifact", func(t *testing.T) {
		art := swf.NewArtifactFromBytes("test.txt", testData)
		var buf bytes.Buffer
		err := art.WriteTo(ctx, &buf)
		require.NoError(t, err)
		assert.Equal(t, testData, buf.Bytes())
	})

	t.Run("file artifact", func(t *testing.T) {
		tmpFile, err := os.CreateTemp("", "writeto-test-*.txt")
		require.NoError(t, err)
		path := tmpFile.Name()
		tmpFile.Write(testData)
		tmpFile.Close()
		defer os.Remove(path)

		art, err := swf.NewArtifactFromFileNoCleanup("test.txt", path)
		require.NoError(t, err)

		var buf bytes.Buffer
		err = art.WriteTo(ctx, &buf)
		require.NoError(t, err)
		assert.Equal(t, testData, buf.Bytes())
	})
}

func TestArtifact_SaveToFile(t *testing.T) {
	ctx := context.Background()
	testData := []byte("save to file test data")

	t.Run("bytes artifact", func(t *testing.T) {
		art := swf.NewArtifactFromBytes("test.txt", testData)

		tmpPath := filepath.Join(os.TempDir(), "save-test-bytes.txt")
		defer os.Remove(tmpPath)

		err := art.SaveToFile(ctx, tmpPath)
		require.NoError(t, err)

		saved, err := os.ReadFile(tmpPath)
		require.NoError(t, err)
		assert.Equal(t, testData, saved)
	})

	t.Run("file artifact", func(t *testing.T) {
		tmpFile, err := os.CreateTemp("", "saveto-src-*.txt")
		require.NoError(t, err)
		srcPath := tmpFile.Name()
		tmpFile.Write(testData)
		tmpFile.Close()
		defer os.Remove(srcPath)

		art, err := swf.NewArtifactFromFileNoCleanup("test.txt", srcPath)
		require.NoError(t, err)

		dstPath := filepath.Join(os.TempDir(), "save-test-file.txt")
		defer os.Remove(dstPath)

		err = art.SaveToFile(ctx, dstPath)
		require.NoError(t, err)

		saved, err := os.ReadFile(dstPath)
		require.NoError(t, err)
		assert.Equal(t, testData, saved)
	})

	t.Run("file artifact same path", func(t *testing.T) {
		tmpFile, err := os.CreateTemp("", "saveto-same-*.txt")
		require.NoError(t, err)
		path := tmpFile.Name()
		tmpFile.Write(testData)
		tmpFile.Close()
		defer os.Remove(path)

		art, err := swf.NewArtifactFromFileNoCleanup("test.txt", path)
		require.NoError(t, err)

		// Saving to same path should be no-op
		err = art.SaveToFile(ctx, path)
		require.NoError(t, err)

		saved, err := os.ReadFile(path)
		require.NoError(t, err)
		assert.Equal(t, testData, saved)
	})

	t.Run("reader artifact", func(t *testing.T) {
		art := swf.NewArtifactFromReader("test.txt", bytes.NewReader(testData), int64(len(testData)))

		tmpPath := filepath.Join(os.TempDir(), "save-test-reader.txt")
		defer os.Remove(tmpPath)

		err := art.SaveToFile(ctx, tmpPath)
		require.NoError(t, err)

		saved, err := os.ReadFile(tmpPath)
		require.NoError(t, err)
		assert.Equal(t, testData, saved)
	})
}
