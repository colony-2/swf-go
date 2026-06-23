package engineconformance_test

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"strings"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	jobdbtest "github.com/colony-2/jobdb/pkg/workflow/internal/jobdbtest"
	"github.com/colony-2/jobdb/pkg/workflow"
)

type jobFailedChainAppErrorChildJob struct{}

func (jobFailedChainAppErrorChildJob) Name() string { return "job-failed-chain-app-error-child" }

func (jobFailedChainAppErrorChildJob) Run(_ workflow.JobContext, _ jobdb.JobData) (jobdb.JobData, error) {
	return nil, jobdb.AppError{Payload: jobdb.AppErrorPayload{
		Message: "child failed",
		Level:   "error",
		Attrs: map[string]interface{}{
			"nested": map[string]interface{}{"k": "v"},
		},
	}}
}

type jobFailedChainGenericChildJob struct{}

func (jobFailedChainGenericChildJob) Name() string { return "job-failed-chain-generic-child" }

func (jobFailedChainGenericChildJob) Run(_ workflow.JobContext, _ jobdb.JobData) (jobdb.JobData, error) {
	return nil, fmt.Errorf("command execution failed: exit status 1")
}

type jobFailedChainTaskChildJob struct{}

func (jobFailedChainTaskChildJob) Name() string { return "job-failed-chain-task-child" }

func (jobFailedChainTaskChildJob) Run(ctx workflow.JobContext, data jobdb.JobData) (jobdb.JobData, error) {
	return ctx.DoTask(jobdb.RunPolicy{}, "job-failed-chain-task-child-fail", data)
}

type jobFailedChainTaskChildFailTask struct{}

func (jobFailedChainTaskChildFailTask) Name() string { return "job-failed-chain-task-child-fail" }

func (jobFailedChainTaskChildFailTask) Run(_ workflow.TaskContext, _ jobdb.TaskData) (jobdb.TaskData, error) {
	return nil, fmt.Errorf("command execution failed: exit status 1")
}

type jobFailedChainParentJob struct {
	engine workflow.Engine
	child  string
}

func (jobFailedChainParentJob) Name() string { return "job-failed-chain-parent" }

func (j *jobFailedChainParentJob) Run(ctx workflow.JobContext, data jobdb.JobData) (_ jobdb.JobData, runErr error) {
	childKey, err := j.engine.SubmitJob(context.Background(), jobdb.SubmitJob{
		TenantId: ctx.GetJobKey().TenantId,
		JobType:  j.child,
		JobID:    ctx.GetJobKey().JobId + "-child",
		Data:     data,
	})
	if err != nil {
		return nil, err
	}
	if err := ctx.AwaitJobs(childKey.JobId); err != nil {
		return nil, err
	}
	run, err := j.engine.GetJobRun(context.Background(), jobdb.GetJobRunRequest{
		JobKey:           childKey,
		IncludeOutputs:   true,
		IncludeArtifacts: true,
	})
	if err != nil {
		return nil, err
	}
	defer func() {
		if rec := recover(); rec != nil {
			runErr = jobdb.AppError{Payload: jobdb.AppErrorPayload{
				Message: "panic in parent: " + rec.(error).Error() + "\n" + string(debug.Stack()),
				Level:   "error",
			}}
		}
	}()
	_, err = run.GetOutput(j.engine, childKey.TenantId)
	if err != nil {
		assertComparableErrorChain(err)
		if errors.Is(err, jobdb.ErrJobFailed) {
			return nil, err
		}
		return nil, err
	}
	return nil, nil
}

func assertComparableErrorChain(err error) {
	seen := map[error]struct{}{}
	for err != nil {
		seen[err] = struct{}{}
		switch next := err.(type) {
		case interface{ Unwrap() error }:
			err = next.Unwrap()
		default:
			err = nil
		}
	}
}

func TestGetJobRunOutputErrorChainComparableAcrossBuiltInRuntimes(t *testing.T) {
	testCases := []struct {
		name    string
		childWS workflow.WorkSet
		child   string
	}{
		{
			name:    "job-app-error",
			childWS: jobdbtest.MustWorkSet(t, jobFailedChainAppErrorChildJob{}),
			child:   "job-failed-chain-app-error-child",
		},
		{
			name:    "job-generic-error",
			childWS: jobdbtest.MustWorkSet(t, jobFailedChainGenericChildJob{}),
			child:   "job-failed-chain-generic-child",
		},
		{
			name:    "task-generic-error",
			childWS: jobdbtest.MustWorkSet(t, jobFailedChainTaskChildJob{}, jobFailedChainTaskChildFailTask{}),
			child:   "job-failed-chain-task-child",
		},
	}
	for _, harness := range jobdbtest.BuiltInRuntimeHarnesses() {
		harness := harness
		for _, tc := range testCases {
			tc := tc
			t.Run(harness.Name+"/"+tc.name, func(t *testing.T) {
				parent := &jobFailedChainParentJob{child: tc.child}
				built := harness.New(t,
					tc.childWS,
					jobdbtest.MustWorkSet(t, parent),
				)
				defer built.Shutdown(t)
				parent.engine = built.Engine

				ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
				defer cancel()

				jobKey, err := built.Engine.SubmitJob(ctx, jobdb.SubmitJob{
					TenantId: built.WorkerTenantID,
					JobType:  parent.Name(),
					JobID:    "parent",
					Data:     jobdbtest.NumberTaskData(1),
				})
				if err != nil {
					t.Fatalf("start parent: %v", err)
				}
				jobdbtest.WaitForEngineStatus(t, ctx, built.Engine, jobKey, jobdb.JobStatusCompleted)
				_, err = jobResultForTest(built.Engine, ctx, jobKey)
				if err == nil {
					t.Fatal("expected parent to fail")
				}
				if strings.Contains(err.Error(), "panic in parent:") {
					t.Fatalf("captured panic stack:\n%s", err)
				}
			})
		}
	}
}
