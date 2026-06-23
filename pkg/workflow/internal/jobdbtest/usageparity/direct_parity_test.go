package usageparity_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	jobdbtest "github.com/colony-2/jobdb/pkg/workflow/internal/jobdbtest"
	"github.com/colony-2/jobdb/pkg/workflow"
)

func TestDirectGetJobRunRetryRepresentationParity(t *testing.T) {
	for _, harness := range jobdbtest.BuiltInRuntimeHarnesses() {
		harness := harness
		if harness.Name != "direct" {
			continue
		}

		t.Run(harness.Name+"/job-retry", func(t *testing.T) {
			run := func(mode parityMode) normalizedJobRun {
				job := &retryJob{}
				ws := jobdbtest.MustWorkSet(t, job)
				return observeViaMode(t, harness, mode, []workflow.WorkSet{ws}, func(t *testing.T, ctx context.Context, subject scenarioSubject) normalizedJobRun {
					jobKey, err := subject.SubmitJob(ctx, jobdb.SubmitJob{
						TenantId: subject.built.WorkerTenantID,
						JobType:  job.Name(),
						JobID:    "job-retry-shape",
						Data:     jobdbtest.NumberTaskData(1),
						RunPolicy: jobdb.RunPolicy{
							Retry: jobdb.RetryPolicy{MaximumAttempts: 3, BackoffCoefficient: 1},
						},
					})
					if err != nil {
						t.Fatalf("start job retry via %s: %v", subject.mode, err)
					}
					subject.WaitForStatus(t, ctx, jobKey, jobdb.JobStatusCompleted)

					resp, err := subject.GetJobRun(ctx, jobdb.GetJobRunRequest{
						JobKey:         jobKey,
						IncludeOutputs: true,
					})
					if err != nil {
						t.Fatalf("get job retry run via %s: %v", subject.mode, err)
					}
					_, outputErr := resp.GetOutput(subject.Engine(), jobKey.TenantId)
					return normalizeJobRun(t, resp, outputErr)
				})
			}

			engineObs := run(engineMode)
			runtimeObs := run(runtimeMode)
			compareObservations(t, engineObs, runtimeObs)
		})

		t.Run(harness.Name+"/task-retry", func(t *testing.T) {
			run := func(mode parityMode) normalizedJobRun {
				job := &retryTaskJob{}
				task := &retryTask{}
				ws := jobdbtest.MustWorkSet(t, job, task)
				return observeViaMode(t, harness, mode, []workflow.WorkSet{ws}, func(t *testing.T, ctx context.Context, subject scenarioSubject) normalizedJobRun {
					jobKey, err := subject.SubmitJob(ctx, jobdb.SubmitJob{
						TenantId: subject.built.WorkerTenantID,
						JobType:  job.Name(),
						JobID:    "task-retry-shape",
						Data:     jobdbtest.NumberTaskData(1),
					})
					if err != nil {
						t.Fatalf("start task retry via %s: %v", subject.mode, err)
					}
					subject.WaitForStatus(t, ctx, jobKey, jobdb.JobStatusCompleted)

					resp, err := subject.GetJobRun(ctx, jobdb.GetJobRunRequest{
						JobKey:               jobKey,
						IncludeInputs:        true,
						IncludeOutputs:       true,
						IncludeAttemptInputs: true,
					})
					if err != nil {
						t.Fatalf("get task retry run via %s: %v", subject.mode, err)
					}
					_, outputErr := resp.GetOutput(subject.Engine(), jobKey.TenantId)
					return normalizeJobRun(t, resp, outputErr)
				})
			}

			engineObs := run(engineMode)
			runtimeObs := run(runtimeMode)
			compareObservations(t, engineObs, runtimeObs)
		})
	}
}

func TestDirectGetJobRunSynthesizedNextAttemptParity(t *testing.T) {
	for _, harness := range jobdbtest.BuiltInRuntimeHarnesses() {
		harness := harness
		if harness.Name != "direct" {
			continue
		}

		t.Run(harness.Name, func(t *testing.T) {
			run := func(mode parityMode) normalizedJobRun {
				job := &retryJob{}
				ws := jobdbtest.MustWorkSet(t, job)
				return observeViaMode(t, harness, mode, []workflow.WorkSet{ws}, func(t *testing.T, ctx context.Context, subject scenarioSubject) normalizedJobRun {
					jobKey, err := subject.SubmitJob(ctx, jobdb.SubmitJob{
						TenantId: subject.built.WorkerTenantID,
						JobType:  job.Name(),
						JobID:    "synth-next",
						Data:     jobdbtest.NumberTaskData(1),
						RunPolicy: jobdb.RunPolicy{
							Retry: jobdb.RetryPolicy{
								MaximumAttempts:    2,
								BackoffCoefficient: 1,
								InitialInterval:    jobdb.Duration(5 * time.Second),
							},
						},
					})
					if err != nil {
						t.Fatalf("start synthesized retry via %s: %v", subject.mode, err)
					}

					var resp jobdb.GetJobRunResponse
					deadline := time.Now().Add(2 * time.Second)
					for time.Now().Before(deadline) {
						resp, err = subject.GetJobRun(ctx, jobdb.GetJobRunRequest{JobKey: jobKey})
						if err != nil {
							t.Fatalf("get synthesized retry run via %s: %v", subject.mode, err)
						}
						if len(resp.Attempts) > 0 && resp.Attempts[0].Outcome.Status == jobdb.TaskOutcomeStatusFailed {
							break
						}
						time.Sleep(20 * time.Millisecond)
					}
					_, outputErr := resp.GetOutput(subject.Engine(), jobKey.TenantId)
					return normalizeJobRun(t, resp, outputErr)
				})
			}

			engineObs := run(engineMode)
			runtimeObs := run(runtimeMode)
			compareObservations(t, engineObs, runtimeObs)
		})
	}
}

func TestDirectRestartWithExtraOutputDeterminismErrorParity(t *testing.T) {
	for _, harness := range jobdbtest.BuiltInRuntimeHarnesses() {
		harness := harness
		if harness.Name != "direct" {
			continue
		}

		t.Run(harness.Name, func(t *testing.T) {
			run := func(mode parityMode) errorObservation {
				runs := 0
				ws := jobdbtest.MustWorkSet(t, singleEchoJob{runs: &runs}, echoTask{})
				return observeViaMode(t, harness, mode, []workflow.WorkSet{ws}, func(t *testing.T, ctx context.Context, subject scenarioSubject) errorObservation {
					origInput := jobdb.NewTaskDataOrPanic(map[string]string{"hello": "world"})
					jobKey, err := subject.SubmitJob(ctx, jobdb.SubmitJob{
						TenantId: subject.built.WorkerTenantID,
						JobType:  ws.JobWorker.Name(),
						JobID:    "restart-extra-base",
						Data:     origInput,
					})
					if err != nil {
						t.Fatalf("start restart-extra base via %s: %v", subject.mode, err)
					}
					subject.WaitForStatus(t, ctx, jobKey, jobdb.JobStatusCompleted)

					newInput := jobdb.NewTaskDataOrPanic(map[string]string{"hello": "again"})
					restartKey, err := subject.SubmitRestartJob(ctx, jobdb.SubmitRestartJob{
						PriorJobKey:     jobKey,
						LastStepToKeep:  0,
						JobID:           "restart-extra-copy",
						ExtraTaskInput:  newInput,
						ExtraTaskOutput: newInput,
					})
					if err != nil {
						t.Fatalf("restart with extra output via %s: %v", subject.mode, err)
					}
					subject.WaitForStatus(t, ctx, restartKey, jobdb.JobStatusCompleted)

					_, err = jobResultForTest(subject, ctx, restartKey)
					if err == nil || (!errors.Is(err, jobdb.ErrWorkflowNotDeterministic) && !strings.Contains(err.Error(), "workflow was not deterministic")) {
						t.Fatalf("expected determinism error via %s, got %v", subject.mode, err)
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
