package usageparity_test

import (
	"context"
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb"
	jobdbtest "github.com/colony-2/jobdb/pkg/workflow/internal/jobdbtest"
	"github.com/colony-2/jobdb/pkg/workflow"
)

type jobRunObservation struct {
	JobKey    jobdb.JobKey       `json:"jobKey"`
	Status    jobdb.JobStatus    `json:"status"`
	JobRun    normalizedJobRun   `json:"jobRun"`
	Result    normalizedTaskData `json:"result,omitempty"`
	ResultErr string             `json:"resultErr,omitempty"`
}

type jobRunOutputObservation struct {
	JobKey    jobdb.JobKey       `json:"jobKey"`
	Status    jobdb.JobStatus    `json:"status"`
	JobRun    normalizedJobRun   `json:"jobRun"`
	Output    normalizedTaskData `json:"output,omitempty"`
	OutputErr string             `json:"outputErr,omitempty"`
	Result    normalizedTaskData `json:"result,omitempty"`
	ResultErr string             `json:"resultErr,omitempty"`
}

func TestCompletedJobRunParityAcrossBuiltInRuntimes(t *testing.T) {
	ws := jobdbtest.MustWorkSet(t,
		jobdbtest.SequenceJob{Steps: []string{jobdbtest.AddOneTaskName, jobdbtest.DoubleTaskName}},
		jobdbtest.AddOneTask{},
		jobdbtest.DoubleTask{},
	)

	for _, harness := range jobdbtest.BuiltInRuntimeHarnesses() {
		harness := harness
		t.Run(harness.Name, func(t *testing.T) {
			compareAcrossModes(t, harness, []workflow.WorkSet{ws}, func(t *testing.T, ctx context.Context, subject scenarioSubject) jobRunObservation {
				jobKey, err := subject.SubmitJob(ctx, jobdb.SubmitJob{
					TenantId: subject.built.WorkerTenantID,
					JobType:  jobdbtest.SequenceJobName,
					JobID:    "completed-run",
					Data:     jobdbtest.NumberTaskData(1),
				})
				if err != nil {
					t.Fatalf("start job via %s: %v", subject.mode, err)
				}
				subject.WaitForStatus(t, ctx, jobKey, jobdb.JobStatusCompleted)

				run, err := subject.GetJobRun(ctx, jobdb.GetJobRunRequest{
					JobKey:               jobKey,
					IncludeInputs:        true,
					IncludeOutputs:       true,
					IncludeAttemptInputs: true,
				})
				if err != nil {
					t.Fatalf("get job run via %s: %v", subject.mode, err)
				}
				runOutput, outputErr := run.GetOutput(subject.Engine(), jobKey.TenantId)
				return jobRunObservation{
					JobKey: jobKey,
					Status: jobdb.JobStatusCompleted,
					JobRun: normalizeJobRun(t, run, outputErr),
					Result: normalizeTaskDataResult(t, runOutput),
				}
			})
		})
	}
}

func TestPendingRuntimeViewParityAcrossBuiltInRuntimes(t *testing.T) {
	ws := jobdbtest.MustWorkSet(t, pendingJob{})

	for _, harness := range jobdbtest.BuiltInRuntimeHarnesses() {
		harness := harness
		t.Run(harness.Name, func(t *testing.T) {
			compareAcrossModes(t, harness, []workflow.WorkSet{ws}, func(t *testing.T, ctx context.Context, subject scenarioSubject) jobRunObservation {
				jobKey, err := subject.SubmitJob(ctx, jobdb.SubmitJob{
					TenantId: subject.built.WorkerTenantID,
					JobType:  ws.JobWorker.Name(),
					JobID:    "pending-run",
					Data:     jobdbtest.NumberTaskData(1),
				})
				if err != nil {
					t.Fatalf("start job via %s: %v", subject.mode, err)
				}

				handle := jobdbtest.WaitForTaskHandle(t, ctx, subject.Engine(), ws.JobWorker.Name(), "pending-task", []string{jobKey.TenantId})
				run, err := subject.GetJobRun(ctx, jobdb.GetJobRunRequest{
					JobKey:               jobKey,
					IncludeInputs:        true,
					IncludeAttemptInputs: true,
				})
				if err != nil {
					t.Fatalf("get pending job run via %s: %v", subject.mode, err)
				}
				runOutput, outputErr := run.GetOutput(subject.Engine(), jobKey.TenantId)

				if err := handle.Finish(ctx, jobdbtest.NumberTaskData(2)); err != nil {
					t.Fatalf("finish pending task via %s: %v", subject.mode, err)
				}
				subject.WaitForStatus(t, ctx, jobKey, jobdb.JobStatusCompleted)

				return jobRunObservation{
					JobKey:    jobKey,
					Status:    run.Job.Status,
					JobRun:    normalizeJobRun(t, run, outputErr),
					Result:    normalizeTaskDataResult(t, runOutput),
					ResultErr: normalizeError(outputErr),
				}
			})
		})
	}
}

func TestLazyOutputLoadParityAcrossBuiltInRuntimes(t *testing.T) {
	ws := jobdbtest.MustWorkSet(t,
		jobdbtest.SequenceJob{Steps: []string{jobdbtest.AddOneTaskName, jobdbtest.DoubleTaskName}},
		jobdbtest.AddOneTask{},
		jobdbtest.DoubleTask{},
	)

	for _, harness := range jobdbtest.BuiltInRuntimeHarnesses() {
		harness := harness
		t.Run(harness.Name, func(t *testing.T) {
			compareAcrossModes(t, harness, []workflow.WorkSet{ws}, func(t *testing.T, ctx context.Context, subject scenarioSubject) jobRunOutputObservation {
				jobKey, err := subject.SubmitJob(ctx, jobdb.SubmitJob{
					TenantId: subject.built.WorkerTenantID,
					JobType:  jobdbtest.SequenceJobName,
					JobID:    "lazy-output",
					Data:     jobdbtest.NumberTaskData(1),
				})
				if err != nil {
					t.Fatalf("start job via %s: %v", subject.mode, err)
				}
				subject.WaitForStatus(t, ctx, jobKey, jobdb.JobStatusCompleted)

				run, err := subject.GetJobRun(ctx, jobdb.GetJobRunRequest{
					JobKey:         jobKey,
					IncludeOutputs: false,
				})
				if err != nil {
					t.Fatalf("get lazy job run via %s: %v", subject.mode, err)
				}
				output, outputErr := run.GetOutput(subject.Engine(), jobKey.TenantId)
				result, resultErr := jobResultForTest(subject, ctx, jobKey)

				return jobRunOutputObservation{
					JobKey:    jobKey,
					Status:    jobdb.JobStatusCompleted,
					JobRun:    normalizeJobRun(t, run, outputErr),
					Output:    normalizeTaskDataResult(t, output),
					OutputErr: normalizeError(outputErr),
					Result:    normalizeTaskDataResult(t, result),
					ResultErr: normalizeError(resultErr),
				}
			})
		})
	}
}

func TestFailedGetOutputParityAcrossBuiltInRuntimes(t *testing.T) {
	ws := jobdbtest.MustWorkSet(t, jobdbtest.FailingJob{})

	for _, harness := range jobdbtest.BuiltInRuntimeHarnesses() {
		harness := harness
		t.Run(harness.Name, func(t *testing.T) {
			compareAcrossModes(t, harness, []workflow.WorkSet{ws}, func(t *testing.T, ctx context.Context, subject scenarioSubject) jobRunOutputObservation {
				jobKey, err := subject.SubmitJob(ctx, jobdb.SubmitJob{
					TenantId: subject.built.WorkerTenantID,
					JobType:  jobdbtest.FailingJobName,
					JobID:    "failed-output",
					Data:     jobdbtest.NumberTaskData(1),
				})
				if err != nil {
					t.Fatalf("start failing job via %s: %v", subject.mode, err)
				}
				subject.WaitForStatus(t, ctx, jobKey, jobdb.JobStatusCompleted)

				run, err := subject.GetJobRun(ctx, jobdb.GetJobRunRequest{JobKey: jobKey})
				if err != nil {
					t.Fatalf("get failed job run via %s: %v", subject.mode, err)
				}
				output, outputErr := run.GetOutput(subject.Engine(), jobKey.TenantId)
				result, resultErr := jobResultForTest(subject, ctx, jobKey)

				return jobRunOutputObservation{
					JobKey:    jobKey,
					Status:    jobdb.JobStatusCompleted,
					JobRun:    normalizeJobRun(t, run, outputErr),
					Output:    normalizeTaskDataResult(t, output),
					OutputErr: normalizeError(outputErr),
					Result:    normalizeTaskDataResult(t, result),
					ResultErr: normalizeError(resultErr),
				}
			})
		})
	}
}

func TestCancelledGetOutputParityAcrossBuiltInRuntimes(t *testing.T) {
	ws := jobdbtest.MustWorkSet(t, jobdbtest.SequenceJob{Steps: []string{jobdbtest.MissingTaskName}})

	for _, harness := range jobdbtest.BuiltInRuntimeHarnesses() {
		harness := harness
		t.Run(harness.Name, func(t *testing.T) {
			compareAcrossModes(t, harness, []workflow.WorkSet{ws}, func(t *testing.T, ctx context.Context, subject scenarioSubject) jobRunOutputObservation {
				jobKey, err := subject.SubmitJob(ctx, jobdb.SubmitJob{
					TenantId: subject.built.WorkerTenantID,
					JobType:  jobdbtest.SequenceJobName,
					JobID:    "cancelled-output",
					Data:     jobdbtest.NumberTaskData(1),
				})
				if err != nil {
					t.Fatalf("start cancelled job via %s: %v", subject.mode, err)
				}
				_ = jobdbtest.WaitForTaskHandle(t, ctx, subject.Engine(), jobdbtest.SequenceJobName, jobdbtest.MissingTaskName, []string{jobKey.TenantId})
				if err := subject.CancelJob(ctx, jobdb.CancelJob{JobKey: jobKey}); err != nil {
					t.Fatalf("cancel job via %s: %v", subject.mode, err)
				}
				subject.WaitForStatus(t, ctx, jobKey, jobdb.JobStatusCancelled)

				run, err := subject.GetJobRun(ctx, jobdb.GetJobRunRequest{JobKey: jobKey})
				if err != nil {
					t.Fatalf("get cancelled job run via %s: %v", subject.mode, err)
				}
				output, outputErr := run.GetOutput(subject.Engine(), jobKey.TenantId)
				result, resultErr := jobResultForTest(subject, ctx, jobKey)

				return jobRunOutputObservation{
					JobKey:    jobKey,
					Status:    jobdb.JobStatusCancelled,
					JobRun:    normalizeJobRun(t, run, outputErr),
					Output:    normalizeTaskDataResult(t, output),
					OutputErr: normalizeError(outputErr),
					Result:    normalizeTaskDataResult(t, result),
					ResultErr: normalizeError(resultErr),
				}
			})
		})
	}
}
