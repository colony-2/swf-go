package swf_test

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/colony-2/swf-go/pkg/swf"
	"github.com/colony-2/swf-go/pkg/swf/impl"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestArtifactStorageOnTaskError verifies that artifacts are stored even when a task errors
func TestArtifactStorageOnTaskError(t *testing.T) {
	t.Run("task error artifacts are stored and retrievable", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Start embedded Postgres
		postgresDSN, stopPG := startEmbeddedPostgres(t)
		defer stopPG()
		if err := installPGWF(ctx, postgresDSN); err != nil {
			t.Fatalf("failed to install pgwf schema: %v", err)
		}

		// Start Strata
		baseURL, strata := startStrata(t)
		defer strata.Shutdown()
		waitForStrataReady(t, baseURL)

		// Create a temporary file artifact for error diagnostics
		tempDir := t.TempDir()
		errorLogFile := filepath.Join(tempDir, "error-diagnostics.log")
		diagnosticData := []byte("Error occurred at line 42\nStack trace: ...\nState: processing")
		err := os.WriteFile(errorLogFile, diagnosticData, 0644)
		require.NoError(t, err)

		// Track cleanup of error artifact
		var cleanupCalled atomic.Bool
		errorArtifact := swf.NewArtifact("error-diagnostics.log", func() (io.ReadCloser, int64, error) {
			f, err := os.Open(errorLogFile)
			if err != nil {
				return nil, 0, err
			}
			info, _ := f.Stat()
			return f, info.Size(), nil
		}, func() error {
			cleanupCalled.Store(true)
			return os.Remove(errorLogFile)
		})

		// Job that calls a task which errors with artifacts
		jobWorker := &taskErrorWithArtifactsJob{artifact: errorArtifact}
		taskWorker := &errorWithArtifactsTask{artifact: errorArtifact}

		// Build engine with the job and task worker
		engine, err := swf.NewEngineBuilder().
			WithPostgresDSN(postgresDSN).
			WithStrata(baseURL).
			WithStrataAPIKey(strata.APIKey).
			PlusWorkers(jobWorker, taskWorker).
			Build(impl.Builder)
		require.NoError(t, err)

		// Run engine in background
		go engine.Run(ctx)

		// Start the job
		jobKey, err := engine.StartJob(ctx, swf.StartJob{
			TenantId: "test-tenant",
			JobType:  jobWorker.Name(),
			Data:     swf.NewTaskDataOrPanic(map[string]string{"input": "data"}),
		})
		require.NoError(t, err)

		// Wait for job to complete (with error)
		require.Eventually(t, func() bool {
			status, _ := engine.CheckJobStatus(ctx, jobKey)
			return status == swf.JobStatusCompleted
		}, 30*time.Second, 200*time.Millisecond)

		// Verify cleanup was called
		assert.True(t, cleanupCalled.Load(), "error artifact cleanup should be called")

		// Verify file was cleaned up
		_, err = os.Stat(errorLogFile)
		assert.True(t, os.IsNotExist(err), "error artifact file should be cleaned up after storage")

		// Verify the error artifact was stored by reading from strata
		client := mustStrataClient(t, baseURL, strata.APIKey)
		taskChapter := waitForChapter(t, client, jobKey.ToStoryKey(), 1, 10*time.Second)

		// Verify the chapter has the error artifact
		artifacts := taskChapter.Artifacts()
		require.Len(t, artifacts, 1, "task chapter should have one error artifact")
		assert.Equal(t, "error-diagnostics.log", artifacts[0].Name(), "artifact name should match")

		// Verify we can read the artifact data from storage
		storedData, err := artifacts[0].Bytes(ctx)
		require.NoError(t, err)
		assert.Equal(t, diagnosticData, storedData, "stored artifact data should match original")
	})

	t.Run("task error artifacts are stored on retry attempts", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Start embedded Postgres
		postgresDSN, stopPG := startEmbeddedPostgres(t)
		defer stopPG()
		if err := installPGWF(ctx, postgresDSN); err != nil {
			t.Fatalf("failed to install pgwf schema: %v", err)
		}

		// Start Strata
		baseURL, strata := startStrata(t)
		defer strata.Shutdown()
		waitForStrataReady(t, baseURL)

		// Create a single artifact that will be attached on each retry
		tempDir := t.TempDir()
		logFile := filepath.Join(tempDir, "retry-error.log")
		logData := []byte("Task failed with retry")
		err := os.WriteFile(logFile, logData, 0644)
		require.NoError(t, err)

		artifact := swf.NewArtifact("retry-error.log", func() (io.ReadCloser, int64, error) {
			f, err := os.Open(logFile)
			if err != nil {
				return nil, 0, err
			}
			info, _ := f.Stat()
			return f, info.Size(), nil
		}, func() error {
			return nil // Don't delete file so it can be reused
		})

		// Job that calls a task which fails with retry
		jobWorker := &simpleRetryErrorJob{artifact: artifact}
		taskWorker := &simpleRetryErrorTask{artifact: artifact}

		// Build engine with retry policy
		engine, err := swf.NewEngineBuilder().
			WithPostgresDSN(postgresDSN).
			WithStrata(baseURL).
			WithStrataAPIKey(strata.APIKey).
			PlusWorkers(jobWorker, taskWorker).
			Build(impl.Builder)
		require.NoError(t, err)

		// Run engine in background
		go engine.Run(ctx)

		// Start the job with retry policy
		jobKey, err := engine.StartJob(ctx, swf.StartJob{
			TenantId: "test-tenant",
			JobType:  jobWorker.Name(),
			Data:     swf.NewTaskDataOrPanic(map[string]string{"input": "data"}),
			RunPolicy: swf.RunPolicy{
				Retry: swf.RetryPolicy{
					MaximumAttempts:    3,
					InitialInterval:    swf.Duration(100 * time.Millisecond),
					MaximumInterval:    swf.Duration(500 * time.Millisecond),
					BackoffCoefficient: 2.0,
				},
			},
		})
		require.NoError(t, err)

		// Wait for job to complete (after all retries fail)
		require.Eventually(t, func() bool {
			status, _ := engine.CheckJobStatus(ctx, jobKey)
			return status == swf.JobStatusCompleted
		}, 30*time.Second, 200*time.Millisecond)

		// Verify each task attempt chapter has its artifact
		client := mustStrataClient(t, baseURL, strata.APIKey)
		for i := 1; i <= 3; i++ {
			chapter := waitForChapter(t, client, jobKey.ToStoryKey(), int64(i), 10*time.Second)
			artifacts := chapter.Artifacts()
			require.Len(t, artifacts, 1, fmt.Sprintf("attempt %d should have one artifact", i))
			assert.Equal(t, "retry-error.log", artifacts[0].Name(), fmt.Sprintf("attempt %d artifact name should match", i))

			// Verify we can read the artifact data
			storedData, err := artifacts[0].Bytes(ctx)
			require.NoError(t, err)
			assert.Equal(t, logData, storedData, fmt.Sprintf("attempt %d artifact data should match", i))
		}
	})
}

// TestArtifactStorageOnJobError verifies that artifacts are stored even when a job errors
func TestArtifactStorageOnJobError(t *testing.T) {
	t.Run("job error artifacts are stored and retrievable", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		// Start embedded Postgres
		postgresDSN, stopPG := startEmbeddedPostgres(t)
		defer stopPG()
		if err := installPGWF(ctx, postgresDSN); err != nil {
			t.Fatalf("failed to install pgwf schema: %v", err)
		}

		// Start Strata
		baseURL, strata := startStrata(t)
		defer strata.Shutdown()
		waitForStrataReady(t, baseURL)

		// Create artifact for job-level error
		tempDir := t.TempDir()
		jobErrorFile := filepath.Join(tempDir, "job-error.log")
		jobErrorData := []byte("Job failed during execution\nPartial results: {...}\nError: validation failed")
		err := os.WriteFile(jobErrorFile, jobErrorData, 0644)
		require.NoError(t, err)

		var cleanupCalled atomic.Bool
		jobErrorArtifact := swf.NewArtifact("job-error.log", func() (io.ReadCloser, int64, error) {
			f, err := os.Open(jobErrorFile)
			if err != nil {
				return nil, 0, err
			}
			info, _ := f.Stat()
			return f, info.Size(), nil
		}, func() error {
			cleanupCalled.Store(true)
			return os.Remove(jobErrorFile)
		})

		// Job that errors with artifacts
		jobWorker := &errorJobWithArtifacts{artifact: jobErrorArtifact}

		// Build engine
		engine, err := swf.NewEngineBuilder().
			WithPostgresDSN(postgresDSN).
			WithStrata(baseURL).
			WithStrataAPIKey(strata.APIKey).
			PlusWorkers(jobWorker).
			Build(impl.Builder)
		require.NoError(t, err)

		// Run engine in background
		go engine.Run(ctx)

		// Start the job
		jobKey, err := engine.StartJob(ctx, swf.StartJob{
			TenantId: "test-tenant",
			JobType:  jobWorker.Name(),
			Data:     swf.NewTaskDataOrPanic(map[string]string{"input": "data"}),
		})
		require.NoError(t, err)

		// Wait for job to complete (with error)
		require.Eventually(t, func() bool {
			status, _ := engine.CheckJobStatus(ctx, jobKey)
			return status == swf.JobStatusCompleted
		}, 30*time.Second, 200*time.Millisecond)

		// Verify cleanup was called
		assert.True(t, cleanupCalled.Load(), "job error artifact cleanup should be called")

		// Verify the error artifact was stored
		// Job result should be in ordinal 1 (after job input at ordinal 0)
		client := mustStrataClient(t, baseURL, strata.APIKey)
		jobResultChapter := waitForChapter(t, client, jobKey.ToStoryKey(), 1, 10*time.Second)

		artifacts := jobResultChapter.Artifacts()
		require.Len(t, artifacts, 1, "job result should have error artifact")
		assert.Equal(t, "job-error.log", artifacts[0].Name())

		// Verify we can read the artifact data
		storedData, err := artifacts[0].Bytes(ctx)
		require.NoError(t, err)
		assert.Equal(t, jobErrorData, storedData, "stored job error artifact should match original")
	})
}

// Test workers for artifact storage on error

// taskErrorWithArtifactsJob calls a task that errors with artifacts
type taskErrorWithArtifactsJob struct {
	artifact swf.Artifact
}

func (j *taskErrorWithArtifactsJob) Name() string { return "task-error-with-artifacts-job" }

func (j *taskErrorWithArtifactsJob) Run(ctx swf.JobContext, input swf.JobData) (swf.JobData, error) {
	return ctx.DoTask(swf.RunPolicy{}, "error-with-artifacts", input)
}

// errorWithArtifactsTask returns an error along with diagnostic artifacts
type errorWithArtifactsTask struct {
	artifact swf.Artifact
}

func (t *errorWithArtifactsTask) Name() string { return "error-with-artifacts" }

func (t *errorWithArtifactsTask) Run(ctx swf.TaskContext, input swf.TaskData) (swf.TaskData, error) {
	// Return both an error and artifacts (diagnostic information)
	output := &swf.SimpleTaskData{
		Data:      []byte(`{"status":"failed","reason":"validation error"}`),
		Artifacts: []swf.Artifact{t.artifact},
	}
	return output, swf.AppError{Payload: swf.AppErrorPayload{Message: "task failed with diagnostics", Level: "error"}}
}

// simpleRetryErrorJob calls a task that fails with retry
type simpleRetryErrorJob struct {
	artifact swf.Artifact
}

func (j *simpleRetryErrorJob) Name() string { return "simple-retry-error-job" }

func (j *simpleRetryErrorJob) Run(ctx swf.JobContext, input swf.JobData) (swf.JobData, error) {
	return ctx.DoTask(swf.RunPolicy{}, "simple-retry-error", input)
}

// simpleRetryErrorTask always fails with the same artifact
type simpleRetryErrorTask struct {
	artifact swf.Artifact
}

func (t *simpleRetryErrorTask) Name() string { return "simple-retry-error" }

func (t *simpleRetryErrorTask) Run(ctx swf.TaskContext, input swf.TaskData) (swf.TaskData, error) {
	output := &swf.SimpleTaskData{
		Data:      []byte(`{"status":"failed"}`),
		Artifacts: []swf.Artifact{t.artifact},
	}
	return output, swf.AppError{Payload: swf.AppErrorPayload{Message: "task failed", Level: "error"}}
}

// errorJobWithArtifacts is a job that errors with artifacts
type errorJobWithArtifacts struct {
	artifact swf.Artifact
}

func (j *errorJobWithArtifacts) Name() string { return "error-job-with-artifacts" }

func (j *errorJobWithArtifacts) Run(ctx swf.JobContext, input swf.JobData) (swf.JobData, error) {
	// Return both error and artifacts at job level
	output := &swf.SimpleTaskData{
		Data:      []byte(`{"status":"job failed","partial_results":"..."}`),
		Artifacts: []swf.Artifact{j.artifact},
	}
	return output, swf.AppError{Payload: swf.AppErrorPayload{Message: "job failed with artifacts", Level: "error"}}
}
