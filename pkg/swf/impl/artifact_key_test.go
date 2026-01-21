package impl

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/colony-2/swf-go/pkg/swf"
)

type artifactKeyTaskWorker struct {
	name string
	data []byte
}

func (t *artifactKeyTaskWorker) Name() string { return "artifact-key-task" }

func (t *artifactKeyTaskWorker) Run(ctx swf.TaskContext, input swf.TaskData) (swf.TaskData, error) {
	return &swf.SimpleTaskData{
		Data:      []byte(`{"ok":true}`),
		Artifacts: []swf.Artifact{swf.NewArtifactFromBytes(t.name, t.data)},
	}, nil
}

type artifactKeyResult struct {
	key swf.ArtifactKey
	err error
}

type artifactKeyJobWorker struct {
	taskType string
	results  chan artifactKeyResult
}

func (j *artifactKeyJobWorker) Name() string { return "artifact-key-job" }

func (j *artifactKeyJobWorker) Run(ctx swf.JobContext, input swf.JobData) (swf.JobData, error) {
	output, err := ctx.DoTask(swf.RunPolicy{}, j.taskType, input)
	if err != nil {
		return nil, err
	}
	artifacts, err := output.GetArtifacts()
	if err != nil {
		j.results <- artifactKeyResult{err: err}
		return output, nil
	}
	if len(artifacts) == 0 {
		j.results <- artifactKeyResult{err: fmt.Errorf("no artifacts returned")}
		return output, nil
	}
	key, err := artifacts[0].ArtifactKey()
	j.results <- artifactKeyResult{key: key, err: err}
	return output, nil
}

func TestDoTaskArtifactKeyAndGetArtifact(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	task := &artifactKeyTaskWorker{
		name: "artifact.txt",
		data: []byte("payload"),
	}
	results := make(chan artifactKeyResult, 1)
	job := &artifactKeyJobWorker{
		taskType: task.Name(),
		results:  results,
	}

	ws := initWorkset(job, task)
	embedded, err := StartEmbeddedEngine(ctx, nil)
	if err != nil {
		t.Fatalf("start embedded engine: %v", err)
	}
	defer embedded.Shutdown()

	engine := embedded.SWFEngine.(*swfEngineImpl)
	if err := engine.RegisterWorkers(ws); err != nil {
		t.Fatalf("register workers: %v", err)
	}
	go engine.Run(ctx)

	jobKey, err := engine.StartJob(ctx, swf.StartJob{
		TenantId: "test-tenant",
		JobType:  job.Name(),
		Data:     swf.NewTaskDataOrPanic(map[string]string{"input": "data"}),
	})
	if err != nil {
		t.Fatalf("start job: %v", err)
	}

	deadline := time.After(20 * time.Second)
	for {
		status, _ := engine.CheckJobStatus(ctx, jobKey)
		if status == swf.JobStatusCompleted {
			break
		}
		select {
		case <-deadline:
			t.Fatal("job did not complete")
		case <-time.After(200 * time.Millisecond):
		}
	}

	var result artifactKeyResult
	select {
	case result = <-results:
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for artifact key")
	}
	if result.err != nil {
		t.Fatalf("artifact key error: %v", result.err)
	}
	if result.key.JobId != jobKey.JobId {
		t.Fatalf("expected job id %s, got %s", jobKey.JobId, result.key.JobId)
	}
	if result.key.Name != task.name {
		t.Fatalf("expected artifact name %s, got %s", task.name, result.key.Name)
	}
	if result.key.TaskOrdinal <= 0 {
		t.Fatalf("expected task ordinal > 0, got %d", result.key.TaskOrdinal)
	}
	if result.key.SizeBytes != int64(len(task.data)) {
		t.Fatalf("expected size %d, got %d", len(task.data), result.key.SizeBytes)
	}

	artifact, err := engine.GetArtifact(jobKey.TenantId, result.key)
	if err != nil {
		t.Fatalf("get artifact: %v", err)
	}
	data, err := artifact.Bytes(ctx)
	if err != nil {
		t.Fatalf("read artifact bytes: %v", err)
	}
	if string(data) != string(task.data) {
		t.Fatalf("expected artifact data %q, got %q", string(task.data), string(data))
	}

	lazy := result.key.ToLazyArtifact(engine, jobKey.TenantId)
	lazyData, err := lazy.Bytes(ctx)
	if err != nil {
		t.Fatalf("lazy bytes: %v", err)
	}
	if string(lazyData) != string(task.data) {
		t.Fatalf("expected lazy data %q, got %q", string(task.data), string(lazyData))
	}
}
