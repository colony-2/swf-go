package usageparity_test

import (
	"context"
	"strings"
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb"
	jobdbtest "github.com/colony-2/jobdb/pkg/workflow/internal/jobdbtest"
	"github.com/colony-2/jobdb/pkg/workflow"
)

type cancelObservation struct {
	JobKey       jobdb.JobKey           `json:"jobKey"`
	Status       jobdb.JobStatus        `json:"status"`
	ResultErr    string                 `json:"resultErr,omitempty"`
	OutputErr    string                 `json:"outputErr,omitempty"`
	JobRun       normalizedJobRun       `json:"jobRun"`
	Listed       []normalizedJobSummary `json:"listed"`
	WaitingInput normalizedTaskData     `json:"waitingInput"`
}

type errorObservation struct {
	Error string `json:"error"`
}

func TestExplicitJobIDParityAcrossBuiltInRuntimes(t *testing.T) {
	ws := jobdbtest.MustWorkSet(t, passthroughJob{name: "custom-id-job"})

	for _, harness := range jobdbtest.BuiltInRuntimeHarnesses() {
		harness := harness
		t.Run(harness.Name, func(t *testing.T) {
			compareAcrossModes(t, harness, []workflow.WorkSet{ws}, func(t *testing.T, ctx context.Context, subject scenarioSubject) lifecycleObservation {
				jobKey, err := subject.SubmitJob(ctx, jobdb.SubmitJob{
					TenantId: subject.built.WorkerTenantID,
					JobType:  ws.JobWorker.Name(),
					JobID:    "custom-job-id",
					Data:     jobdbtest.NumberTaskData(7),
				})
				if err != nil {
					t.Fatalf("start job via %s: %v", subject.mode, err)
				}
				subject.WaitForStatus(t, ctx, jobKey, jobdb.JobStatusCompleted)

				result, err := jobResultForTest(subject, ctx, jobKey)
				if err != nil {
					t.Fatalf("get job result via %s: %v", subject.mode, err)
				}
				run, err := subject.GetJobRun(ctx, jobdb.GetJobRunRequest{
					JobKey:         jobKey,
					IncludeOutputs: true,
				})
				if err != nil {
					t.Fatalf("get job run via %s: %v", subject.mode, err)
				}
				_, outputErr := run.GetOutput(subject.Engine(), jobKey.TenantId)
				listed, err := subject.ListJobs(ctx, jobdb.ListJobsRequest{
					TenantIds: []string{jobKey.TenantId},
					JobKeys:   []jobdb.JobKey{jobKey},
					PageSize:  10,
				})
				if err != nil {
					t.Fatalf("list jobs via %s: %v", subject.mode, err)
				}
				return lifecycleObservation{
					JobKey:    jobKey,
					Status:    jobdb.JobStatusCompleted,
					Result:    normalizeTaskDataResult(t, result),
					ResultErr: "",
					JobRun:    normalizeJobRun(t, run, outputErr),
					Listed:    normalizeJobSummaries(listed.Jobs),
				}
			})
		})
	}
}

func TestCancelJobParityAcrossBuiltInRuntimes(t *testing.T) {
	ws := jobdbtest.MustWorkSet(t, pendingJob{})

	for _, harness := range jobdbtest.BuiltInRuntimeHarnesses() {
		harness := harness
		t.Run(harness.Name, func(t *testing.T) {
			compareAcrossModes(t, harness, []workflow.WorkSet{ws}, func(t *testing.T, ctx context.Context, subject scenarioSubject) cancelObservation {
				jobKey, err := subject.SubmitJob(ctx, jobdb.SubmitJob{
					TenantId: subject.built.WorkerTenantID,
					JobType:  ws.JobWorker.Name(),
					JobID:    "cancel-parity",
					Data:     jobdbtest.NumberTaskData(1),
				})
				if err != nil {
					t.Fatalf("start job via %s: %v", subject.mode, err)
				}

				handle := jobdbtest.WaitForTaskHandle(t, ctx, subject.Engine(), ws.JobWorker.Name(), "pending-task", []string{jobKey.TenantId})
				handleData, err := handle.Data()
				if err != nil {
					t.Fatalf("waiting task data via %s: %v", subject.mode, err)
				}

				if err := subject.CancelJob(ctx, jobdb.CancelJob{
					JobKey: jobKey,
					Reason: "usage parity cancel",
				}); err != nil {
					t.Fatalf("cancel via %s: %v", subject.mode, err)
				}
				subject.WaitForStatus(t, ctx, jobKey, jobdb.JobStatusCancelled)

				result, resultErr := jobResultForTest(subject, ctx, jobKey)
				_ = result
				run, err := subject.GetJobRun(ctx, jobdb.GetJobRunRequest{
					JobKey:               jobKey,
					IncludeInputs:        true,
					IncludeAttemptInputs: true,
				})
				if err != nil {
					t.Fatalf("get job run via %s: %v", subject.mode, err)
				}
				_, outputErr := run.GetOutput(subject.Engine(), jobKey.TenantId)
				listed, err := subject.ListJobs(ctx, jobdb.ListJobsRequest{
					TenantIds: []string{jobKey.TenantId},
					JobKeys:   []jobdb.JobKey{jobKey},
					PageSize:  10,
				})
				if err != nil {
					t.Fatalf("list jobs via %s: %v", subject.mode, err)
				}

				return cancelObservation{
					JobKey:       jobKey,
					Status:       jobdb.JobStatusCancelled,
					ResultErr:    normalizeError(resultErr),
					OutputErr:    normalizeError(outputErr),
					JobRun:       normalizeJobRun(t, run, outputErr),
					Listed:       normalizeJobSummaries(listed.Jobs),
					WaitingInput: normalizeTaskDataResult(t, handleData),
				}
			})
		})
	}
}

func TestRestartJobParityAcrossBuiltInRuntimes(t *testing.T) {
	ws := jobdbtest.MustWorkSet(t,
		jobdbtest.SequenceJob{Steps: []string{jobdbtest.AddOneTaskName, jobdbtest.DoubleTaskName}},
		jobdbtest.AddOneTask{},
		jobdbtest.DoubleTask{},
	)

	for _, harness := range jobdbtest.BuiltInRuntimeHarnesses() {
		harness := harness
		t.Run(harness.Name, func(t *testing.T) {
			compareAcrossModes(t, harness, []workflow.WorkSet{ws}, func(t *testing.T, ctx context.Context, subject scenarioSubject) lifecycleObservation {
				origKey, err := subject.SubmitJob(ctx, jobdb.SubmitJob{
					TenantId: subject.built.WorkerTenantID,
					JobType:  jobdbtest.SequenceJobName,
					JobID:    "restart-original",
					Data:     jobdbtest.NumberTaskData(1),
				})
				if err != nil {
					t.Fatalf("start original via %s: %v", subject.mode, err)
				}
				subject.WaitForStatus(t, ctx, origKey, jobdb.JobStatusCompleted)

				restartKey, err := subject.SubmitRestartJob(ctx, jobdb.SubmitRestartJob{
					PriorJobKey:    origKey,
					LastStepToKeep: 0,
					JobID:          "restart-copy",
				})
				if err != nil {
					t.Fatalf("restart via %s: %v", subject.mode, err)
				}
				subject.WaitForStatus(t, ctx, restartKey, jobdb.JobStatusCompleted)

				result, err := jobResultForTest(subject, ctx, restartKey)
				if err != nil {
					t.Fatalf("get restart result via %s: %v", subject.mode, err)
				}
				run, err := subject.GetJobRun(ctx, jobdb.GetJobRunRequest{
					JobKey:               restartKey,
					IncludeInputs:        true,
					IncludeOutputs:       true,
					IncludeAttemptInputs: true,
				})
				if err != nil {
					t.Fatalf("get restart run via %s: %v", subject.mode, err)
				}
				_, outputErr := run.GetOutput(subject.Engine(), restartKey.TenantId)
				listed, err := subject.ListJobs(ctx, jobdb.ListJobsRequest{
					TenantIds: []string{restartKey.TenantId},
					JobKeys:   []jobdb.JobKey{restartKey},
					PageSize:  10,
				})
				if err != nil {
					t.Fatalf("list restart job via %s: %v", subject.mode, err)
				}

				return lifecycleObservation{
					JobKey:    restartKey,
					Status:    jobdb.JobStatusCompleted,
					Result:    normalizeTaskDataResult(t, result),
					JobRun:    normalizeJobRun(t, run, outputErr),
					ResultErr: normalizeError(outputErr),
					Listed:    normalizeJobSummaries(listed.Jobs),
				}
			})
		})
	}
}

func TestRestartValidationParityAcrossBuiltInRuntimes(t *testing.T) {
	for _, harness := range jobdbtest.BuiltInRuntimeHarnesses() {
		harness := harness

		t.Run(harness.Name+"/negative-last-step", func(t *testing.T) {
			ws := jobdbtest.MustWorkSet(t, passthroughJob{name: "restart-negative"})
			compareAcrossModes(t, harness, []workflow.WorkSet{ws}, func(t *testing.T, ctx context.Context, subject scenarioSubject) errorObservation {
				_, err := subject.SubmitRestartJob(ctx, jobdb.SubmitRestartJob{
					PriorJobKey:    jobdb.JobKey{TenantId: "tenant", JobId: "missing"},
					LastStepToKeep: -1,
				})
				if err == nil {
					t.Fatalf("expected restart validation error via %s", subject.mode)
				}
				return errorObservation{Error: err.Error()}
			})
		})

		t.Run(harness.Name+"/missing-next-chapter", func(t *testing.T) {
			ws := jobdbtest.MustWorkSet(t, passthroughJob{name: "restart-missing-next"})
			compareAcrossModes(t, harness, []workflow.WorkSet{ws}, func(t *testing.T, ctx context.Context, subject scenarioSubject) errorObservation {
				jobKey, err := subject.SubmitJob(ctx, jobdb.SubmitJob{
					TenantId: subject.built.WorkerTenantID,
					JobType:  ws.JobWorker.Name(),
					JobID:    "restart-base",
					Data:     jobdbtest.NumberTaskData(1),
				})
				if err != nil {
					t.Fatalf("start base via %s: %v", subject.mode, err)
				}
				subject.WaitForStatus(t, ctx, jobKey, jobdb.JobStatusCompleted)

				_, err = subject.SubmitRestartJob(ctx, jobdb.SubmitRestartJob{
					PriorJobKey:    jobKey,
					LastStepToKeep: 1,
					JobID:          "restart-missing-next-copy",
				})
				if err == nil {
					t.Fatalf("expected restart missing-next error via %s", subject.mode)
				}
				return errorObservation{Error: err.Error()}
			})
		})

		t.Run(harness.Name+"/retry-boundary", func(t *testing.T) {
			run := func(mode parityMode) errorObservation {
				job := &retryTaskJob{}
				task := &retryTask{}
				ws := jobdbtest.MustWorkSet(t, job, task)
				return observeViaMode(t, harness, mode, []workflow.WorkSet{ws}, func(t *testing.T, ctx context.Context, subject scenarioSubject) errorObservation {
					jobKey, err := subject.SubmitJob(ctx, jobdb.SubmitJob{
						TenantId: subject.built.WorkerTenantID,
						JobType:  job.Name(),
						JobID:    "retry-boundary",
						Data:     jobdbtest.NumberTaskData(1),
					})
					if err != nil {
						t.Fatalf("start retry-boundary base via %s: %v", subject.mode, err)
					}
					subject.WaitForStatus(t, ctx, jobKey, jobdb.JobStatusCompleted)

					_, err = subject.SubmitRestartJob(ctx, jobdb.SubmitRestartJob{
						PriorJobKey:    jobKey,
						LastStepToKeep: 1,
						JobID:          "retry-boundary-copy",
					})
					if err == nil {
						t.Fatalf("expected retry-boundary validation error via %s", subject.mode)
					}
					return errorObservation{Error: err.Error()}
				})
			}

			engineObs := run(engineMode)
			runtimeObs := run(runtimeMode)
			compareObservations(t, engineObs, runtimeObs)
		})
	}
}

func TestPrerequisiteParityAcrossBuiltInRuntimes(t *testing.T) {
	successWorker := passthroughJob{name: "prereq-success-job"}
	failWorker := namedFailingJob{name: "prereq-fail-job", message: "prereq failed"}
	dependentWorker := passthroughJob{name: "prereq-dependent-job"}

	for _, harness := range jobdbtest.BuiltInRuntimeHarnesses() {
		harness := harness
		t.Run(harness.Name, func(t *testing.T) {
			compareAcrossModes(t, harness, []workflow.WorkSet{
				jobdbtest.MustWorkSet(t, successWorker),
				jobdbtest.MustWorkSet(t, failWorker),
				jobdbtest.MustWorkSet(t, dependentWorker),
			}, func(t *testing.T, ctx context.Context, subject scenarioSubject) errorObservation {
				tenantID := subject.built.WorkerTenantID

				successKey, err := subject.SubmitJob(ctx, jobdb.SubmitJob{
					TenantId: tenantID,
					JobType:  successWorker.Name(),
					JobID:    "success",
					Data:     jobdbtest.NumberTaskData(1),
				})
				if err != nil {
					t.Fatalf("start success prereq via %s: %v", subject.mode, err)
				}
				failKey, err := subject.SubmitJob(ctx, jobdb.SubmitJob{
					TenantId: tenantID,
					JobType:  failWorker.Name(),
					JobID:    "fail",
					Data:     jobdbtest.NumberTaskData(2),
				})
				if err != nil {
					t.Fatalf("start failing prereq via %s: %v", subject.mode, err)
				}
				subject.WaitForStatus(t, ctx, successKey, jobdb.JobStatusCompleted)
				subject.WaitForStatus(t, ctx, failKey, jobdb.JobStatusCompleted)

				successDependent, err := subject.SubmitJob(ctx, jobdb.SubmitJob{
					TenantId: tenantID,
					JobType:  dependentWorker.Name(),
					JobID:    "dependent-success",
					Data:     jobdbtest.NumberTaskData(3),
					Prerequisites: []jobdb.JobPrerequisite{
						{JobID: successKey.JobId, Condition: jobdb.JobPrereqSuccess},
					},
				})
				if err != nil {
					t.Fatalf("start dependent success via %s: %v", subject.mode, err)
				}
				failedDependent, err := subject.SubmitJob(ctx, jobdb.SubmitJob{
					TenantId: tenantID,
					JobType:  dependentWorker.Name(),
					JobID:    "dependent-failed",
					Data:     jobdbtest.NumberTaskData(4),
					Prerequisites: []jobdb.JobPrerequisite{
						{JobID: failKey.JobId, Condition: jobdb.JobPrereqSuccess},
					},
				})
				if err != nil {
					t.Fatalf("start dependent failed via %s: %v", subject.mode, err)
				}
				completeDependent, err := subject.SubmitJob(ctx, jobdb.SubmitJob{
					TenantId: tenantID,
					JobType:  dependentWorker.Name(),
					JobID:    "dependent-complete",
					Data:     jobdbtest.NumberTaskData(5),
					Prerequisites: []jobdb.JobPrerequisite{
						{JobID: failKey.JobId, Condition: jobdb.JobPrereqComplete},
					},
				})
				if err != nil {
					t.Fatalf("start dependent complete via %s: %v", subject.mode, err)
				}

				subject.WaitForStatus(t, ctx, successDependent, jobdb.JobStatusCompleted)
				subject.WaitForStatus(t, ctx, failedDependent, jobdb.JobStatusCompleted)
				subject.WaitForStatus(t, ctx, completeDependent, jobdb.JobStatusCompleted)

				if _, err := jobResultForTest(subject, ctx, successDependent); err != nil {
					t.Fatalf("expected success dependent to succeed via %s: %v", subject.mode, err)
				}
				if _, err := jobResultForTest(subject, ctx, completeDependent); err != nil {
					t.Fatalf("expected complete dependent to succeed via %s: %v", subject.mode, err)
				}
				_, err = jobResultForTest(subject, ctx, failedDependent)
				if err == nil {
					t.Fatalf("expected failed dependent to fail via %s", subject.mode)
				}
				if !strings.Contains(err.Error(), "prerequisite job") {
					t.Fatalf("unexpected prereq error via %s: %v", subject.mode, err)
				}
				return errorObservation{Error: err.Error()}
			})
		})
	}
}
