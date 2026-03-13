package swf_test

import (
	"context"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	strataclient "github.com/colony-2/strata-go/pkg/client"
	"github.com/colony-2/swf-go/pkg/swf"
	"github.com/stretchr/testify/require"
)

const (
	artifactCleanupJobName  = "artifact-cleanup-job"
	artifactCleanupTaskName = "artifact-cleanup-task"
	jobArtifactName         = "job-output.txt"
	jobCopyPrefix           = "job-copy-"
)

func TestArtifactCleanupAcrossEngines(t *testing.T) {
	t.Run("toy", func(t *testing.T) {
		ctx := context.Background()
		tempDir := t.TempDir()
		fileNames := []string{"artifact-a.txt", "artifact-b.txt"}
		copyNames := prefixedNames(jobCopyPrefix, fileNames)
		filePaths := artifactPaths(tempDir, append(append(fileNames, copyNames...), jobArtifactName))

		jobWorker := &artifactCleanupJob{
			dir:         tempDir,
			jobFileName: jobArtifactName,
			taskOrdinal: 1,
		}
		taskWorker := &artifactCleanupTask{dir: tempDir, fileNames: fileNames}
		engine, cancel := buildToyEngine(t, func(b *swf.EngineBuilder) {
			b.PlusWorkers(jobWorker, taskWorker)
		})
		defer cancel()
		runArtifactCleanupScenario(t, ctx, engine, filePaths)
	})

	t.Run("real", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		postgresDSN, stopPG := startEmbeddedPostgres(t)
		defer stopPG()
		if err := installPGWF(ctx, postgresDSN); err != nil {
			t.Fatalf("failed to install pgwf schema: %v", err)
		}

		baseURL, strata := startStrata(t)
		defer strata.Shutdown()
		waitForStrataReady(t, baseURL)

		tempDir := t.TempDir()
		fileNames := []string{"artifact-a.txt", "artifact-b.txt"}
		copyNames := prefixedNames(jobCopyPrefix, fileNames)
		filePaths := artifactPaths(tempDir, append(append(fileNames, copyNames...), jobArtifactName))

		jobWorker := &artifactCleanupJob{
			dir:           tempDir,
			jobFileName:   jobArtifactName,
			taskOrdinal:   1,
			strataBaseURL: baseURL,
			strataAPIKey:  strata.APIKey,
		}
		taskWorker := &artifactCleanupTask{dir: tempDir, fileNames: fileNames}
		engine := buildDirectEngine(t, postgresDSN, baseURL, strata.APIKey, func(b *swf.EngineBuilder) {
			b.PlusWorkers(jobWorker, taskWorker)
		})

		go engine.Run(ctx)
		runArtifactCleanupScenario(t, ctx, engine, filePaths)
	})
}

type artifactCleanupJob struct {
	dir           string
	jobFileName   string
	taskOrdinal   int64
	strataBaseURL string
	strataAPIKey  string
}

func (j *artifactCleanupJob) Name() string { return artifactCleanupJobName }

func (j *artifactCleanupJob) Run(ctx swf.JobContext, input swf.JobData) (swf.JobData, error) {
	taskOutput, err := ctx.DoTask(swf.RunPolicy{}, artifactCleanupTaskName, input)
	if err != nil {
		return nil, err
	}

	jobFilePath := filepath.Join(j.dir, j.jobFileName)
	if err := os.WriteFile(jobFilePath, []byte("artifact:job-output"), 0644); err != nil {
		return nil, err
	}

	jobArtifact := newFileArtifact(jobFilePath, j.jobFileName)
	reuploadedArtifacts, err := j.downloadTaskArtifacts(ctx, taskOutput)
	if err != nil {
		return nil, err
	}

	allArtifacts := append(reuploadedArtifacts, jobArtifact)
	return &swf.SimpleTaskData{
		Data:      []byte(`{"ok":true}`),
		Artifacts: allArtifacts,
	}, nil
}

func (j *artifactCleanupJob) downloadTaskArtifacts(ctx swf.JobContext, taskOutput swf.TaskData) ([]swf.Artifact, error) {
	if j.strataBaseURL == "" {
		taskArtifacts, err := taskOutput.GetArtifacts()
		if err != nil {
			return nil, err
		}
		return j.copyArtifacts(taskArtifacts)
	}

	client, err := strataclient.New(strataclient.Config{BaseURL: j.strataBaseURL, APIKey: j.strataAPIKey})
	if err != nil {
		return nil, err
	}

	key := storyKeyForJob(ctx.GetJobKey())
	chapter, err := client.Chapter(context.Background(), key, j.taskOrdinal)
	if err != nil {
		return nil, err
	}

	strataArtifacts := chapter.Artifacts()
	taskArtifacts := make([]swf.Artifact, 0, len(strataArtifacts))
	for _, art := range strataArtifacts {
		taskArtifacts = append(taskArtifacts, swf.NewArtifact(art.Name(), func() (io.ReadCloser, int64, error) {
			_, rc, err := art.ToInput(context.Background())
			return rc, art.SizeBytes(), err
		}, nil))
	}
	return j.copyArtifacts(taskArtifacts)
}

func (j *artifactCleanupJob) copyArtifacts(artifacts []swf.Artifact) ([]swf.Artifact, error) {
	reuploaded := make([]swf.Artifact, 0, len(artifacts))
	for _, art := range artifacts {
		data, err := art.Bytes(context.Background())
		if err != nil {
			return nil, err
		}
		copyName := jobCopyPrefix + art.Name()
		copyPath := filepath.Join(j.dir, copyName)
		if err := os.WriteFile(copyPath, data, 0644); err != nil {
			return nil, err
		}
		reuploaded = append(reuploaded, newFileArtifact(copyPath, copyName))
	}
	return reuploaded, nil
}

type artifactCleanupTask struct {
	dir       string
	fileNames []string
}

func (t *artifactCleanupTask) Name() string { return artifactCleanupTaskName }

func (t *artifactCleanupTask) Run(ctx swf.TaskContext, input swf.TaskData) (swf.TaskData, error) {
	artifacts := make([]swf.Artifact, 0, len(t.fileNames))
	for _, name := range t.fileNames {
		path := filepath.Join(t.dir, name)
		if err := os.WriteFile(path, []byte("artifact:"+name), 0644); err != nil {
			return nil, err
		}
		artifacts = append(artifacts, newFileArtifact(path, name))
	}

	return &swf.SimpleTaskData{
		Data:      []byte(`{"ok":true}`),
		Artifacts: artifacts,
	}, nil
}

func artifactPaths(dir string, names []string) []string {
	paths := make([]string, 0, len(names))
	for _, name := range names {
		paths = append(paths, filepath.Join(dir, name))
	}
	return paths
}

func prefixedNames(prefix string, names []string) []string {
	out := make([]string, 0, len(names))
	for _, name := range names {
		out = append(out, prefix+name)
	}
	return out
}

func newFileArtifact(path, name string) swf.Artifact {
	pathCopy := path
	nameCopy := name
	return swf.NewArtifact(nameCopy, func() (io.ReadCloser, int64, error) {
		f, err := os.Open(pathCopy)
		if err != nil {
			return nil, 0, err
		}
		info, err := f.Stat()
		if err != nil {
			f.Close()
			return nil, 0, err
		}
		return f, info.Size(), nil
	}, func() error {
		return os.Remove(pathCopy)
	})
}

func runArtifactCleanupScenario(t *testing.T, ctx context.Context, engine swf.SWFEngine, filePaths []string) {
	t.Helper()

	jobKey, err := engine.StartJob(ctx, swf.StartJob{
		TenantId: "test-tenant",
		JobType:  artifactCleanupJobName,
		Data:     swf.NewTaskDataOrPanic(map[string]string{"hello": "world"}),
	})
	require.NoError(t, err)

	require.NoError(t, swf.WaitForJobToComplete(ctx, 30*time.Second, jobKey, engine))

	status, err := engine.CheckJobStatus(ctx, jobKey)
	require.NoError(t, err)
	require.Equal(t, swf.JobStatusCompleted, status)

	_, err = engine.GetJobResult(ctx, jobKey)
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		for _, path := range filePaths {
			if _, err := os.Stat(path); err == nil || !os.IsNotExist(err) {
				return false
			}
		}
		return true
	}, 5*time.Second, 50*time.Millisecond)
}
