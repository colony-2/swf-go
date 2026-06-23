package usageparity_test

import (
	"context"
	"encoding/json"
	"testing"

	"github.com/colony-2/jobdb/pkg/jobdb"
	jobdbtest "github.com/colony-2/jobdb/pkg/workflow/internal/jobdbtest"
	"github.com/colony-2/jobdb/pkg/workflow"
)

type listJobsObservation struct {
	Completed []normalizedJobSummary `json:"completed"`
	Cancelled []normalizedJobSummary `json:"cancelled"`
}

type filteredListObservation struct {
	JobTypeFilter  []normalizedJobSummary `json:"jobTypeFilter,omitempty"`
	JobTaskFilter  []normalizedJobSummary `json:"jobTaskFilter,omitempty"`
	MetadataFilter []normalizedJobSummary `json:"metadataFilter,omitempty"`
	PageOne        []normalizedJobSummary `json:"pageOne,omitempty"`
	PageTwo        []normalizedJobSummary `json:"pageTwo,omitempty"`
	PageOneHasNext bool                   `json:"pageOneHasNext"`
	PageTwoHasNext bool                   `json:"pageTwoHasNext"`
}

func TestListJobsStatusParityAcrossBuiltInRuntimes(t *testing.T) {
	completedWS := jobdbtest.MustWorkSet(t, passthroughJob{name: "list-complete"})
	cancelWS := jobdbtest.MustWorkSet(t, pendingJob{})

	for _, harness := range jobdbtest.BuiltInRuntimeHarnesses() {
		harness := harness
		t.Run(harness.Name, func(t *testing.T) {
			compareAcrossModes(t, harness, []workflow.WorkSet{completedWS, cancelWS}, func(t *testing.T, ctx context.Context, subject scenarioSubject) listJobsObservation {
				completeKey, err := subject.SubmitJob(ctx, jobdb.SubmitJob{
					TenantId: subject.built.WorkerTenantID,
					JobType:  completedWS.JobWorker.Name(),
					JobID:    "completed",
					Data:     jobdbtest.NumberTaskData(3),
				})
				if err != nil {
					t.Fatalf("start completed job via %s: %v", subject.mode, err)
				}
				subject.WaitForStatus(t, ctx, completeKey, jobdb.JobStatusCompleted)

				cancelKey, err := subject.SubmitJob(ctx, jobdb.SubmitJob{
					TenantId: completeKey.TenantId,
					JobType:  cancelWS.JobWorker.Name(),
					JobID:    "cancelled",
					Data:     jobdbtest.NumberTaskData(4),
				})
				if err != nil {
					t.Fatalf("start cancelled job via %s: %v", subject.mode, err)
				}
				_ = jobdbtest.WaitForTaskHandle(t, ctx, subject.Engine(), cancelWS.JobWorker.Name(), "pending-task", []string{cancelKey.TenantId})
				if err := subject.CancelJob(ctx, jobdb.CancelJob{JobKey: cancelKey, Reason: "status parity"}); err != nil {
					t.Fatalf("cancel job via %s: %v", subject.mode, err)
				}
				subject.WaitForStatus(t, ctx, cancelKey, jobdb.JobStatusCancelled)

				completed, err := subject.ListJobs(ctx, jobdb.ListJobsRequest{
					TenantIds: []string{completeKey.TenantId},
					Statuses:  []jobdb.JobStatus{jobdb.JobStatusCompleted},
					PageSize:  10,
				})
				if err != nil {
					t.Fatalf("list completed via %s: %v", subject.mode, err)
				}
				cancelled, err := subject.ListJobs(ctx, jobdb.ListJobsRequest{
					TenantIds: []string{completeKey.TenantId},
					Statuses:  []jobdb.JobStatus{jobdb.JobStatusCancelled},
					PageSize:  10,
				})
				if err != nil {
					t.Fatalf("list cancelled via %s: %v", subject.mode, err)
				}

				return listJobsObservation{
					Completed: normalizeJobSummaries(completed.Jobs),
					Cancelled: normalizeJobSummaries(cancelled.Jobs),
				}
			})
		})
	}
}

func TestListJobsFilterParityAcrossBuiltInRuntimes(t *testing.T) {
	alphaWS := jobdbtest.MustWorkSet(t, passthroughJob{name: "alpha"})
	betaWS := jobdbtest.MustWorkSet(t, pendingJob{})

	for _, harness := range jobdbtest.BuiltInRuntimeHarnesses() {
		harness := harness
		t.Run(harness.Name, func(t *testing.T) {
			compareAcrossModes(t, harness, []workflow.WorkSet{alphaWS, betaWS}, func(t *testing.T, ctx context.Context, subject scenarioSubject) filteredListObservation {
				tenantID := subject.built.WorkerTenantID

				alphaMeta, err := json.Marshal(map[string]any{"rank": 1, "team": "alpha"})
				if err != nil {
					t.Fatalf("marshal alpha metadata: %v", err)
				}
				betaMeta, err := json.Marshal(map[string]any{"rank": 2, "team": "beta"})
				if err != nil {
					t.Fatalf("marshal beta metadata: %v", err)
				}

				alphaKey, err := subject.SubmitJob(ctx, jobdb.SubmitJob{
					TenantId: tenantID,
					JobType:  alphaWS.JobWorker.Name(),
					JobID:    "alpha-job",
					Metadata: alphaMeta,
					Data:     jobdbtest.NumberTaskData(1),
				})
				if err != nil {
					t.Fatalf("start alpha via %s: %v", subject.mode, err)
				}
				subject.WaitForStatus(t, ctx, alphaKey, jobdb.JobStatusCompleted)

				betaKey, err := subject.SubmitJob(ctx, jobdb.SubmitJob{
					TenantId: tenantID,
					JobType:  betaWS.JobWorker.Name(),
					JobID:    "beta-job",
					Metadata: betaMeta,
					Data:     jobdbtest.NumberTaskData(2),
				})
				if err != nil {
					t.Fatalf("start beta via %s: %v", subject.mode, err)
				}
				_ = jobdbtest.WaitForTaskHandle(t, ctx, subject.Engine(), betaWS.JobWorker.Name(), "pending-task", []string{betaKey.TenantId})

				metaFilter, err := jobdb.Metadata().EqualFilter("rank", 1)
				if err != nil {
					t.Fatalf("build metadata filter: %v", err)
				}

				jobTypeResp, err := subject.ListJobs(ctx, jobdb.ListJobsRequest{
					TenantIds: []string{tenantID},
					JobTypes:  []string{"alpha"},
					PageSize:  10,
				})
				if err != nil {
					t.Fatalf("list job type filter via %s: %v", subject.mode, err)
				}
				jobTaskResp, err := subject.ListJobs(ctx, jobdb.ListJobsRequest{
					TenantIds: []string{tenantID},
					JobTasks:  []jobdb.JobTaskFilter{{JobType: betaWS.JobWorker.Name(), TaskType: "pending-task"}},
					PageSize:  10,
				})
				if err != nil {
					t.Fatalf("list job task filter via %s: %v", subject.mode, err)
				}
				metadataResp, err := subject.ListJobs(ctx, jobdb.ListJobsRequest{
					TenantIds:      []string{tenantID},
					MetadataFilter: metaFilter,
					PageSize:       10,
				})
				if err != nil {
					t.Fatalf("list metadata filter via %s: %v", subject.mode, err)
				}

				pageOne, err := subject.ListJobs(ctx, jobdb.ListJobsRequest{
					TenantIds: []string{tenantID},
					PageSize:  1,
				})
				if err != nil {
					t.Fatalf("list page one via %s: %v", subject.mode, err)
				}
				pageTwo, err := subject.ListJobs(ctx, jobdb.ListJobsRequest{
					TenantIds: []string{tenantID},
					PageSize:  1,
					PageToken: pageOne.NextPageToken,
				})
				if err != nil {
					t.Fatalf("list page two via %s: %v", subject.mode, err)
				}

				return filteredListObservation{
					JobTypeFilter:  normalizeJobSummaries(jobTypeResp.Jobs),
					JobTaskFilter:  normalizeJobSummaries(jobTaskResp.Jobs),
					MetadataFilter: normalizeJobSummaries(metadataResp.Jobs),
					PageOne:        normalizeJobSummaries(pageOne.Jobs),
					PageTwo:        normalizeJobSummaries(pageTwo.Jobs),
					PageOneHasNext: pageOne.NextPageToken != "",
					PageTwoHasNext: pageTwo.NextPageToken != "",
				}
			})
		})
	}
}
