package usageparity_test

import (
	"context"
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb"
	jobdbtest "github.com/colony-2/jobdb/pkg/workflow/internal/jobdbtest"
	"github.com/colony-2/jobdb/pkg/workflow"
)

type generatedStartObservation struct {
	TenantID     string             `json:"tenantId"`
	JobType      string             `json:"jobType"`
	JobIDPresent bool               `json:"jobIdPresent"`
	Status       jobdb.JobStatus    `json:"status"`
	Result       normalizedTaskData `json:"result"`
}

type lifecycleObservation struct {
	JobKey    jobdb.JobKey           `json:"jobKey"`
	Status    jobdb.JobStatus        `json:"status"`
	Result    normalizedTaskData     `json:"result,omitempty"`
	ResultErr string                 `json:"resultErr,omitempty"`
	JobRun    normalizedJobRun       `json:"jobRun"`
	Listed    []normalizedJobSummary `json:"listed"`
}

func TestEngineAndRuntimeConstructionParityAcrossBuiltInRuntimes(t *testing.T) {
	ws := jobdbtest.MustWorkSet(t,
		jobdbtest.SequenceJob{Steps: []string{jobdbtest.AddOneTaskName, jobdbtest.DoubleTaskName}},
		jobdbtest.AddOneTask{},
		jobdbtest.DoubleTask{},
	)

	for _, harness := range jobdbtest.BuiltInRuntimeHarnesses() {
		harness := harness
		t.Run(harness.Name, func(t *testing.T) {
			compareAcrossModes(t, harness, []workflow.WorkSet{ws}, func(t *testing.T, ctx context.Context, subject scenarioSubject) lifecycleObservation {
				jobKey, err := subject.SubmitJob(ctx, jobdb.SubmitJob{
					TenantId: subject.built.WorkerTenantID,
					JobType:  jobdbtest.SequenceJobName,
					JobID:    "construction-parity",
					Data:     jobdbtest.NumberTaskData(1),
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
					JobKey:               jobKey,
					IncludeInputs:        true,
					IncludeOutputs:       true,
					IncludeAttemptInputs: true,
				})
				if err != nil {
					t.Fatalf("get job run via %s: %v", subject.mode, err)
				}
				_, runOutputErr := run.GetOutput(subject.Engine(), jobKey.TenantId)
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
					JobRun:    normalizeJobRun(t, run, runOutputErr),
					ResultErr: normalizeError(runOutputErr),
					Listed:    normalizeJobSummaries(listed.Jobs),
				}
			})
		})
	}
}

func TestGeneratedJobIDConstructionParityAcrossBuiltInRuntimes(t *testing.T) {
	ws := jobdbtest.MustWorkSet(t,
		jobdbtest.SequenceJob{Steps: []string{jobdbtest.AddOneTaskName, jobdbtest.DoubleTaskName}},
		jobdbtest.AddOneTask{},
		jobdbtest.DoubleTask{},
	)

	for _, harness := range jobdbtest.BuiltInRuntimeHarnesses() {
		harness := harness
		t.Run(harness.Name, func(t *testing.T) {
			engineObs := observeViaMode(t, harness, engineMode, []workflow.WorkSet{ws}, func(t *testing.T, ctx context.Context, subject scenarioSubject) generatedStartObservation {
				jobKey, err := subject.SubmitJob(ctx, jobdb.SubmitJob{
					TenantId: subject.built.WorkerTenantID,
					JobType:  jobdbtest.SequenceJobName,
					Data:     jobdbtest.NumberTaskData(2),
				})
				if err != nil {
					t.Fatalf("start job via %s: %v", subject.mode, err)
				}
				subject.WaitForStatus(t, ctx, jobKey, jobdb.JobStatusCompleted)
				result, err := jobResultForTest(subject, ctx, jobKey)
				if err != nil {
					t.Fatalf("get job result via %s: %v", subject.mode, err)
				}
				status, err := jobStatusForTest(subject, ctx, jobKey)
				if err != nil {
					t.Fatalf("check status via %s: %v", subject.mode, err)
				}
				return generatedStartObservation{
					TenantID:     jobKey.TenantId,
					JobType:      jobdbtest.SequenceJobName,
					JobIDPresent: jobKey.JobId != "",
					Status:       status,
					Result:       normalizeTaskDataResult(t, result),
				}
			})
			runtimeObs := observeViaMode(t, harness, runtimeMode, []workflow.WorkSet{ws}, func(t *testing.T, ctx context.Context, subject scenarioSubject) generatedStartObservation {
				jobKey, err := subject.SubmitJob(ctx, jobdb.SubmitJob{
					TenantId: subject.built.WorkerTenantID,
					JobType:  jobdbtest.SequenceJobName,
					Data:     jobdbtest.NumberTaskData(2),
				})
				if err != nil {
					t.Fatalf("start job via %s: %v", subject.mode, err)
				}
				subject.WaitForStatus(t, ctx, jobKey, jobdb.JobStatusCompleted)
				result, err := jobResultForTest(subject, ctx, jobKey)
				if err != nil {
					t.Fatalf("get job result via %s: %v", subject.mode, err)
				}
				status, err := jobStatusForTest(subject, ctx, jobKey)
				if err != nil {
					t.Fatalf("check status via %s: %v", subject.mode, err)
				}
				return generatedStartObservation{
					TenantID:     jobKey.TenantId,
					JobType:      jobdbtest.SequenceJobName,
					JobIDPresent: jobKey.JobId != "",
					Status:       status,
					Result:       normalizeTaskDataResult(t, result),
				}
			})
			compareObservations(t, engineObs, runtimeObs)
		})
	}
}
