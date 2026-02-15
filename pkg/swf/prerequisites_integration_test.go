package swf_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/colony-2/swf-go/pkg/swf"
	"github.com/colony-2/swf-go/pkg/swf/impl"
)

const (
	prereqSuccessJobName   = "prereq_success_job"
	prereqFailJobName      = "prereq_fail_job"
	prereqDependentJobName = "prereq_dependent_job"
	prereqRestartJobName   = "prereq_restart_job"
	prereqRestartTaskName  = "prereq_restart_task"
)

type prereqSuccessWorker struct{}

func (prereqSuccessWorker) Name() string { return prereqSuccessJobName }
func (prereqSuccessWorker) Run(ctx swf.JobContext, input swf.JobData) (swf.JobData, error) {
	return input, nil
}

type prereqFailWorker struct{}

func (prereqFailWorker) Name() string { return prereqFailJobName }
func (prereqFailWorker) Run(ctx swf.JobContext, input swf.JobData) (swf.JobData, error) {
	return nil, swf.AppError{Payload: swf.AppErrorPayload{Message: "prereq failed"}}
}

type prereqDependentWorker struct{}

func (prereqDependentWorker) Name() string { return prereqDependentJobName }
func (prereqDependentWorker) Run(ctx swf.JobContext, input swf.JobData) (swf.JobData, error) {
	return input, nil
}

type prereqRestartJobWorker struct{}

func (prereqRestartJobWorker) Name() string { return prereqRestartJobName }
func (prereqRestartJobWorker) Run(ctx swf.JobContext, input swf.JobData) (swf.JobData, error) {
	return ctx.DoTask(swf.RunPolicy{}, prereqRestartTaskName, input)
}

type prereqRestartTaskWorker struct{}

func (prereqRestartTaskWorker) Name() string { return prereqRestartTaskName }
func (prereqRestartTaskWorker) Run(ctx swf.TaskContext, input swf.TaskData) (swf.TaskData, error) {
	return input, nil
}

func TestPrerequisitesSuccessAndComplete(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	postgresDSN, stopPG := startEmbeddedPostgres(t)
	defer stopPG()
	if err := installPGWF(ctx, postgresDSN); err != nil {
		t.Fatalf("failed to install pgwf: %v", err)
	}

	baseURL, strata := startStrata(t)
	defer strata.Shutdown()
	waitForStrataReady(t, baseURL)

	engine, err := swf.NewEngineBuilder().
		WithPostgresDSN(postgresDSN).
		WithStrata(baseURL).
		WithStrataAPIKey(strata.APIKey).
		PlusWorkers(prereqSuccessWorker{}).
		PlusWorkers(prereqFailWorker{}).
		PlusWorkers(prereqDependentWorker{}).
		PlusWorkers(prereqRestartJobWorker{}, prereqRestartTaskWorker{}).
		Build(impl.Builder)
	if err != nil {
		t.Fatalf("failed to build engine: %v", err)
	}

	go engine.Run(ctx)

	tenantID := "prereq-tenant"

	successJobID := "prereq-success"
	successKey, err := engine.StartJob(ctx, swf.StartJob{
		TenantId: tenantID,
		JobType:  prereqSuccessJobName,
		JobID:    successJobID,
		Data:     swf.NewTaskDataOrPanic(map[string]interface{}{"n": 1}),
	})
	if err != nil {
		t.Fatalf("start success prereq: %v", err)
	}

	failJobID := "prereq-fail"
	failKey, err := engine.StartJob(ctx, swf.StartJob{
		TenantId: tenantID,
		JobType:  prereqFailJobName,
		JobID:    failJobID,
		Data:     swf.NewTaskDataOrPanic(map[string]interface{}{"n": 2}),
	})
	if err != nil {
		t.Fatalf("start failed prereq: %v", err)
	}

	successDependent, err := engine.StartJob(ctx, swf.StartJob{
		TenantId: tenantID,
		JobType:  prereqDependentJobName,
		JobID:    "dependent-success",
		Data:     swf.NewTaskDataOrPanic(map[string]interface{}{"n": 3}),
		Prerequisites: []swf.JobPrerequisite{
			{JobID: successJobID, Condition: swf.JobPrereqSuccess},
		},
	})
	if err != nil {
		t.Fatalf("start dependent success: %v", err)
	}

	failedDependent, err := engine.StartJob(ctx, swf.StartJob{
		TenantId: tenantID,
		JobType:  prereqDependentJobName,
		JobID:    "dependent-failed",
		Data:     swf.NewTaskDataOrPanic(map[string]interface{}{"n": 4}),
		Prerequisites: []swf.JobPrerequisite{
			{JobID: failJobID, Condition: swf.JobPrereqSuccess},
		},
	})
	if err != nil {
		t.Fatalf("start dependent failed: %v", err)
	}

	completeDependent, err := engine.StartJob(ctx, swf.StartJob{
		TenantId: tenantID,
		JobType:  prereqDependentJobName,
		JobID:    "dependent-complete",
		Data:     swf.NewTaskDataOrPanic(map[string]interface{}{"n": 5}),
		Prerequisites: []swf.JobPrerequisite{
			{JobID: failJobID, Condition: swf.JobPrereqComplete},
		},
	})
	if err != nil {
		t.Fatalf("start dependent complete: %v", err)
	}

	for _, key := range []swf.JobKey{successKey, failKey, successDependent, failedDependent, completeDependent} {
		if err := swf.WaitForJobToComplete(ctx, 30*time.Second, key, engine); err != nil {
			t.Fatalf("wait for job %s: %v", key.String(), err)
		}
	}

	if _, err := engine.GetJobResult(ctx, successDependent); err != nil {
		t.Fatalf("expected success dependent to succeed, got %v", err)
	}

	if _, err := engine.GetJobResult(ctx, completeDependent); err != nil {
		t.Fatalf("expected complete dependent to succeed, got %v", err)
	}

	if _, err := engine.GetJobResult(ctx, failedDependent); err == nil {
		t.Fatalf("expected failed dependent to error")
	} else if !strings.Contains(err.Error(), "prerequisite job") {
		t.Fatalf("expected prereq error, got %v", err)
	}
}

func TestRestartPrerequisitesCheckedAtRestartExtra(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	postgresDSN, stopPG := startEmbeddedPostgres(t)
	defer stopPG()
	if err := installPGWF(ctx, postgresDSN); err != nil {
		t.Fatalf("failed to install pgwf: %v", err)
	}

	baseURL, strata := startStrata(t)
	defer strata.Shutdown()
	waitForStrataReady(t, baseURL)

	engine, err := swf.NewEngineBuilder().
		WithPostgresDSN(postgresDSN).
		WithStrata(baseURL).
		WithStrataAPIKey(strata.APIKey).
		PlusWorkers(prereqSuccessWorker{}).
		PlusWorkers(prereqFailWorker{}).
		PlusWorkers(prereqDependentWorker{}).
		PlusWorkers(prereqRestartJobWorker{}, prereqRestartTaskWorker{}).
		Build(impl.Builder)
	if err != nil {
		t.Fatalf("failed to build engine: %v", err)
	}

	go engine.Run(ctx)

	tenantID := "restart-prereq-tenant"
	baseInput := map[string]interface{}{"n": 1}
	baseKey, err := engine.StartJob(ctx, swf.StartJob{
		TenantId: tenantID,
		JobType:  prereqRestartJobName,
		JobID:    "restart-base",
		Data:     swf.NewTaskDataOrPanic(baseInput),
	})
	if err != nil {
		t.Fatalf("start base job: %v", err)
	}
	if err := swf.WaitForJobToComplete(ctx, 30*time.Second, baseKey, engine); err != nil {
		t.Fatalf("wait base job: %v", err)
	}

	failKey, err := engine.StartJob(ctx, swf.StartJob{
		TenantId: tenantID,
		JobType:  prereqFailJobName,
		JobID:    "restart-prereq-fail",
		Data:     swf.NewTaskDataOrPanic(map[string]interface{}{"n": 2}),
	})
	if err != nil {
		t.Fatalf("start fail prereq: %v", err)
	}
	if err := swf.WaitForJobToComplete(ctx, 30*time.Second, failKey, engine); err != nil {
		t.Fatalf("wait fail prereq: %v", err)
	}

	restartKey, err := engine.RestartJob(ctx, swf.RestartJob{
		PriorJobKey: baseKey,
		LastStepToKeep: 0,
		JobID: "restart-with-prereqs",
		ExtraTaskInput: swf.NewTaskDataOrPanic(baseInput),
		ExtraTaskOutput: swf.NewTaskDataOrPanic(map[string]interface{}{"n": 3}),
		Prerequisites: []swf.JobPrerequisite{
			{JobID: failKey.JobId, Condition: swf.JobPrereqSuccess},
		},
	})
	if err != nil {
		t.Fatalf("restart job: %v", err)
	}
	if err := swf.WaitForJobToComplete(ctx, 30*time.Second, restartKey, engine); err != nil {
		t.Fatalf("wait restart job: %v", err)
	}
	if _, err := engine.GetJobResult(ctx, restartKey); err == nil {
		t.Fatalf("expected restart to fail due to prereqs")
	} else if !strings.Contains(err.Error(), "prerequisite job") {
		t.Fatalf("expected prereq error, got %v", err)
	}
}
