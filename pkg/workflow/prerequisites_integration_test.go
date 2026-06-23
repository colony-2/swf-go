package workflow_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	"github.com/colony-2/jobdb/pkg/workflow"
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
func (prereqSuccessWorker) Run(ctx workflow.JobContext, input jobdb.JobData) (jobdb.JobData, error) {
	return input, nil
}

type prereqFailWorker struct{}

func (prereqFailWorker) Name() string { return prereqFailJobName }
func (prereqFailWorker) Run(ctx workflow.JobContext, input jobdb.JobData) (jobdb.JobData, error) {
	return nil, jobdb.AppError{Payload: jobdb.AppErrorPayload{Message: "prereq failed"}}
}

type prereqDependentWorker struct{}

func (prereqDependentWorker) Name() string { return prereqDependentJobName }
func (prereqDependentWorker) Run(ctx workflow.JobContext, input jobdb.JobData) (jobdb.JobData, error) {
	return input, nil
}

type prereqRestartJobWorker struct{}

func (prereqRestartJobWorker) Name() string { return prereqRestartJobName }
func (prereqRestartJobWorker) Run(ctx workflow.JobContext, input jobdb.JobData) (jobdb.JobData, error) {
	return ctx.DoTask(jobdb.RunPolicy{}, prereqRestartTaskName, input)
}

type prereqRestartTaskWorker struct{}

func (prereqRestartTaskWorker) Name() string { return prereqRestartTaskName }
func (prereqRestartTaskWorker) Run(ctx workflow.TaskContext, input jobdb.TaskData) (jobdb.TaskData, error) {
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

	tenantID := "prereq-tenant"
	engine := buildDirectEngine(t, postgresDSN, baseURL, strata.APIKey, func(b *workflow.EngineBuilder) {
		b.WithWorkerTenantId(tenantID).
			PlusWorkers(prereqSuccessWorker{}).
			PlusWorkers(prereqFailWorker{}).
			PlusWorkers(prereqDependentWorker{}).
			PlusWorkers(prereqRestartJobWorker{}, prereqRestartTaskWorker{})
	})

	go engine.Run(ctx)

	successJobID := "prereq-success"
	successKey, err := engine.SubmitJob(ctx, jobdb.SubmitJob{
		TenantId: tenantID,
		JobType:  prereqSuccessJobName,
		JobID:    successJobID,
		Data:     jobdb.NewTaskDataOrPanic(map[string]interface{}{"n": 1}),
	})
	if err != nil {
		t.Fatalf("start success prereq: %v", err)
	}

	failJobID := "prereq-fail"
	failKey, err := engine.SubmitJob(ctx, jobdb.SubmitJob{
		TenantId: tenantID,
		JobType:  prereqFailJobName,
		JobID:    failJobID,
		Data:     jobdb.NewTaskDataOrPanic(map[string]interface{}{"n": 2}),
	})
	if err != nil {
		t.Fatalf("start failed prereq: %v", err)
	}

	successDependent, err := engine.SubmitJob(ctx, jobdb.SubmitJob{
		TenantId: tenantID,
		JobType:  prereqDependentJobName,
		JobID:    "dependent-success",
		Data:     jobdb.NewTaskDataOrPanic(map[string]interface{}{"n": 3}),
		Prerequisites: []jobdb.JobPrerequisite{
			{JobID: successJobID, Condition: jobdb.JobPrereqSuccess},
		},
	})
	if err != nil {
		t.Fatalf("start dependent success: %v", err)
	}

	failedDependent, err := engine.SubmitJob(ctx, jobdb.SubmitJob{
		TenantId: tenantID,
		JobType:  prereqDependentJobName,
		JobID:    "dependent-failed",
		Data:     jobdb.NewTaskDataOrPanic(map[string]interface{}{"n": 4}),
		Prerequisites: []jobdb.JobPrerequisite{
			{JobID: failJobID, Condition: jobdb.JobPrereqSuccess},
		},
	})
	if err != nil {
		t.Fatalf("start dependent failed: %v", err)
	}

	completeDependent, err := engine.SubmitJob(ctx, jobdb.SubmitJob{
		TenantId: tenantID,
		JobType:  prereqDependentJobName,
		JobID:    "dependent-complete",
		Data:     jobdb.NewTaskDataOrPanic(map[string]interface{}{"n": 5}),
		Prerequisites: []jobdb.JobPrerequisite{
			{JobID: failJobID, Condition: jobdb.JobPrereqComplete},
		},
	})
	if err != nil {
		t.Fatalf("start dependent complete: %v", err)
	}

	for _, key := range []jobdb.JobKey{successKey, failKey, successDependent, failedDependent, completeDependent} {
		if err := workflow.WaitForJobToComplete(ctx, 30*time.Second, key, engine); err != nil {
			t.Fatalf("wait for job %s: %v", key.String(), err)
		}
	}

	if _, err := jobResultForTest(engine, ctx, successDependent); err != nil {
		t.Fatalf("expected success dependent to succeed, got %v", err)
	}

	if _, err := jobResultForTest(engine, ctx, completeDependent); err != nil {
		t.Fatalf("expected complete dependent to succeed, got %v", err)
	}

	if _, err := jobResultForTest(engine, ctx, failedDependent); err == nil {
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

	tenantID := "restart-prereq-tenant"
	engine := buildDirectEngine(t, postgresDSN, baseURL, strata.APIKey, func(b *workflow.EngineBuilder) {
		b.WithWorkerTenantId(tenantID).
			PlusWorkers(prereqSuccessWorker{}).
			PlusWorkers(prereqFailWorker{}).
			PlusWorkers(prereqDependentWorker{}).
			PlusWorkers(prereqRestartJobWorker{}, prereqRestartTaskWorker{})
	})

	go engine.Run(ctx)

	baseInput := map[string]interface{}{"n": 1}
	baseKey, err := engine.SubmitJob(ctx, jobdb.SubmitJob{
		TenantId: tenantID,
		JobType:  prereqRestartJobName,
		JobID:    "restart-base",
		Data:     jobdb.NewTaskDataOrPanic(baseInput),
	})
	if err != nil {
		t.Fatalf("start base job: %v", err)
	}
	if err := workflow.WaitForJobToComplete(ctx, 30*time.Second, baseKey, engine); err != nil {
		t.Fatalf("wait base job: %v", err)
	}

	failKey, err := engine.SubmitJob(ctx, jobdb.SubmitJob{
		TenantId: tenantID,
		JobType:  prereqFailJobName,
		JobID:    "restart-prereq-fail",
		Data:     jobdb.NewTaskDataOrPanic(map[string]interface{}{"n": 2}),
	})
	if err != nil {
		t.Fatalf("start fail prereq: %v", err)
	}
	if err := workflow.WaitForJobToComplete(ctx, 30*time.Second, failKey, engine); err != nil {
		t.Fatalf("wait fail prereq: %v", err)
	}

	restartKey, err := engine.SubmitRestartJob(ctx, jobdb.SubmitRestartJob{
		PriorJobKey:     baseKey,
		LastStepToKeep:  0,
		JobID:           "restart-with-prereqs",
		ExtraTaskInput:  jobdb.NewTaskDataOrPanic(baseInput),
		ExtraTaskOutput: jobdb.NewTaskDataOrPanic(map[string]interface{}{"n": 3}),
		Prerequisites: []jobdb.JobPrerequisite{
			{JobID: failKey.JobId, Condition: jobdb.JobPrereqSuccess},
		},
	})
	if err != nil {
		t.Fatalf("restart job: %v", err)
	}
	if err := workflow.WaitForJobToComplete(ctx, 30*time.Second, restartKey, engine); err != nil {
		t.Fatalf("wait restart job: %v", err)
	}
	if _, err := jobResultForTest(engine, ctx, restartKey); err == nil {
		t.Fatalf("expected restart to fail due to prereqs")
	} else if !strings.Contains(err.Error(), "prerequisite job") {
		t.Fatalf("expected prereq error, got %v", err)
	}
}
