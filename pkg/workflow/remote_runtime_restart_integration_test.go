package workflow_test

import (
	"context"
	"encoding/json"
	"errors"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	remoteruntime "github.com/colony-2/jobdb/pkg/jobdb/runtime/remote"
	toyruntime "github.com/colony-2/jobdb/pkg/jobdb/runtime/toy"
	"github.com/colony-2/jobdb/pkg/workflow"
)

func TestRemoteRuntimeSupportsExplicitRestartJobIDs(t *testing.T) {
	underlying := toyruntime.New()
	tenantID := "tenant-restart-id"
	builder := workflow.NewEngineBuilder().WithRuntime(underlying).WithWorkerTenantId(tenantID)
	builder.PlusWorkers(remoteRestartSequenceJob{}, remoteRestartAddOneTask{}, remoteRestartDoubleTask{})
	engine, err := builder.BuildEngine()
	if err != nil {
		t.Fatalf("build engine: %v", err)
	}
	runCtx, cancelRun := context.WithCancel(context.Background())
	defer cancelRun()
	done := make(chan struct{})
	go func() {
		defer close(done)
		engine.Run(runCtx)
	}()
	defer func() {
		cancelRun()
		<-done
	}()

	server := httptest.NewServer(remoteruntime.NewServer(underlying))
	defer server.Close()

	runtime, err := remoteruntime.New(server.URL, server.Client())
	if err != nil {
		t.Fatalf("new remote runtime: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
	defer cancel()

	originalKey, err := engine.SubmitJob(ctx, jobdb.SubmitJob{
		TenantId: tenantID,
		JobID:    "restart-source",
		JobType:  remoteRestartSequenceJobName,
		Data:     jobdb.NewTaskDataOrPanic(map[string]int{"n": 1}),
	})
	if err != nil {
		t.Fatalf("submit source job: %v", err)
	}
	waitForRemoteRestartEngineStatus(t, ctx, engine, originalKey, jobdb.JobStatusCompleted)

	handle, err := runtime.SubmitRestartJob(ctx, jobdb.SubmitRestartJobRequest{
		Job: jobdb.SubmitRestartJob{
			PriorJobKey:    originalKey,
			LastStepToKeep: 0,
			JobID:          "restart-copy",
		},
		RequestTime: time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("submit explicit restart job id: %v", err)
	}
	if handle.JobKey.JobId != "restart-copy" {
		t.Fatalf("unexpected restart job key %+v", handle.JobKey)
	}

	matching, err := runtime.SubmitRestartJob(ctx, jobdb.SubmitRestartJobRequest{
		Job: jobdb.SubmitRestartJob{
			PriorJobKey:    originalKey,
			LastStepToKeep: 0,
			JobID:          "restart-copy",
		},
		RequestTime: time.Now().UTC(),
	})
	if err != nil {
		t.Fatalf("repeat explicit restart job id submit: %v", err)
	}
	if matching.JobKey != handle.JobKey {
		t.Fatalf("unexpected matching restart handle %+v", matching.JobKey)
	}

	_, err = runtime.SubmitRestartJob(ctx, jobdb.SubmitRestartJobRequest{
		Job: jobdb.SubmitRestartJob{
			PriorJobKey:    originalKey,
			LastStepToKeep: 1,
			JobID:          "restart-copy",
		},
		RequestTime: time.Now().UTC(),
	})
	if !errors.Is(err, jobdb.ErrExistingJobMismatch) {
		t.Fatalf("expected existing restart job mismatch, got %v", err)
	}
}

const (
	remoteRestartSequenceJobName = "remote-restart-seq"
	remoteRestartAddOneTaskName  = "remote-restart-add"
	remoteRestartDoubleTaskName  = "remote-restart-double"
)

type remoteRestartSequenceJob struct{}

func (remoteRestartSequenceJob) Name() string { return remoteRestartSequenceJobName }

func (remoteRestartSequenceJob) Run(ctx workflow.JobContext, data jobdb.JobData) (jobdb.JobData, error) {
	first, err := ctx.DoTask(jobdb.RunPolicy{}, remoteRestartAddOneTaskName, data)
	if err != nil {
		return nil, err
	}
	second, err := ctx.DoTask(jobdb.RunPolicy{}, remoteRestartDoubleTaskName, first)
	if err != nil {
		return nil, err
	}
	return second, nil
}

type remoteRestartAddOneTask struct{}

func (remoteRestartAddOneTask) Name() string { return remoteRestartAddOneTaskName }

func (remoteRestartAddOneTask) Run(_ workflow.TaskContext, input jobdb.TaskData) (jobdb.TaskData, error) {
	n, err := remoteRestartNumber(input)
	if err != nil {
		return nil, err
	}
	return jobdb.NewTaskDataOrPanic(map[string]int{"n": n + 1}), nil
}

type remoteRestartDoubleTask struct{}

func (remoteRestartDoubleTask) Name() string { return remoteRestartDoubleTaskName }

func (remoteRestartDoubleTask) Run(_ workflow.TaskContext, input jobdb.TaskData) (jobdb.TaskData, error) {
	n, err := remoteRestartNumber(input)
	if err != nil {
		return nil, err
	}
	return jobdb.NewTaskDataOrPanic(map[string]int{"n": n * 2}), nil
}

func remoteRestartNumber(input jobdb.TaskData) (int, error) {
	raw, err := input.GetData()
	if err != nil {
		return 0, err
	}
	payload := map[string]int{}
	if err := json.Unmarshal(raw, &payload); err != nil {
		return 0, err
	}
	return payload["n"], nil
}

func waitForRemoteRestartEngineStatus(t *testing.T, ctx context.Context, engine workflow.Engine, jobKey jobdb.JobKey, want jobdb.JobStatus) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		job, err := engine.GetJob(ctx, jobKey)
		if err == nil && job.Status == want {
			return
		}
		if err != nil && !errors.Is(err, jobdb.ErrJobNotFound) && !errors.Is(err, context.Canceled) {
			t.Fatalf("check engine status: %v", err)
		}
		select {
		case <-ctx.Done():
			t.Fatalf("wait for engine status: %v", ctx.Err())
		case <-time.After(20 * time.Millisecond):
		}
	}
	t.Fatalf("job %s did not reach engine status %s", jobKey, want)
}
