package usageparity_test

import (
	"context"
	"testing"
	"time"

	"github.com/colony-2/jobdb/pkg/jobdb"
	jobdbtest "github.com/colony-2/jobdb/pkg/workflow/internal/jobdbtest"
	"github.com/colony-2/jobdb/pkg/workflow"
)

type externalTaskObservation struct {
	JobKey       jobdb.JobKey           `json:"jobKey"`
	Status       jobdb.JobStatus        `json:"status"`
	WaitingInput normalizedTaskData     `json:"waitingInput"`
	Result       normalizedTaskData     `json:"result"`
	JobRun       normalizedJobRun       `json:"jobRun"`
	Listed       []normalizedJobSummary `json:"listed"`
}

type sequentialObservation struct {
	JobKey   jobdb.JobKey              `json:"jobKey"`
	Status   jobdb.JobStatus           `json:"status"`
	Result   normalizedTaskData        `json:"result"`
	JobRun   normalizedJobRun          `json:"jobRun"`
	Chapters []normalizedStoredChapter `json:"chapters"`
	Listed   []normalizedJobSummary    `json:"listed"`
}

type awaitObservation struct {
	ChildKey      jobdb.JobKey           `json:"childKey"`
	ParentKey     jobdb.JobKey           `json:"parentKey"`
	PendingStatus jobdb.JobStatus        `json:"pendingStatus"`
	PendingList   []normalizedJobSummary `json:"pendingList"`
	FinalStatus   jobdb.JobStatus        `json:"finalStatus"`
	FinalResult   normalizedTaskData     `json:"finalResult"`
	FinalRun      normalizedJobRun       `json:"finalRun"`
	FinalList     []normalizedJobSummary `json:"finalList"`
}

type pendingTaskObservation struct {
	JobKey            jobdb.JobKey           `json:"jobKey"`
	PendingStatus     jobdb.JobStatus        `json:"pendingStatus"`
	WaitingInput      normalizedTaskData     `json:"waitingInput"`
	NextNeed          *string                `json:"nextNeed,omitempty"`
	TaskWaitInput     *int64                 `json:"taskWaitInput,omitempty"`
	TaskWaitOutput    *int64                 `json:"taskWaitOutput,omitempty"`
	TaskWaitInputHash *string                `json:"taskWaitInputHash,omitempty"`
	TaskWaitNext      *string                `json:"taskWaitNext,omitempty"`
	FinalStatus       jobdb.JobStatus        `json:"finalStatus"`
	FinalResult       normalizedTaskData     `json:"finalResult"`
	FinalRun          normalizedJobRun       `json:"finalRun"`
	Listed            []normalizedJobSummary `json:"listed"`
}

type retryObservation struct {
	JobKey jobdb.JobKey       `json:"jobKey"`
	Status jobdb.JobStatus    `json:"status"`
	Result normalizedTaskData `json:"result"`
	JobRun normalizedJobRun   `json:"jobRun"`
}

type replayObservation struct {
	JobKey    jobdb.JobKey       `json:"jobKey"`
	Status    jobdb.JobStatus    `json:"status"`
	Result    normalizedTaskData `json:"result"`
	Replayed  normalizedTaskData `json:"replayed"`
	ReplayErr string             `json:"replayErr,omitempty"`
}

type failureObservation struct {
	JobKey    jobdb.JobKey           `json:"jobKey"`
	Status    jobdb.JobStatus        `json:"status"`
	Result    normalizedTaskData     `json:"result,omitempty"`
	ResultErr string                 `json:"resultErr,omitempty"`
	JobRun    normalizedJobRun       `json:"jobRun"`
	Listed    []normalizedJobSummary `json:"listed"`
}

type awaitDurationObservation struct {
	JobKey       jobdb.JobKey           `json:"jobKey"`
	WaitObserved bool                   `json:"waitObserved"`
	FinalStatus  jobdb.JobStatus        `json:"finalStatus"`
	FinalResult  normalizedTaskData     `json:"finalResult"`
	FinalRun     normalizedJobRun       `json:"finalRun"`
	FinalList    []normalizedJobSummary `json:"finalList"`
}

func TestExternalTaskCompletionParityAcrossBuiltInRuntimes(t *testing.T) {
	ws := jobdbtest.MustWorkSet(t, externalApprovalJob{})

	for _, harness := range jobdbtest.BuiltInRuntimeHarnesses() {
		harness := harness
		t.Run(harness.Name, func(t *testing.T) {
			compareAcrossModes(t, harness, []workflow.WorkSet{ws}, func(t *testing.T, ctx context.Context, subject scenarioSubject) externalTaskObservation {
				jobKey, err := subject.SubmitJob(ctx, jobdb.SubmitJob{
					TenantId: subject.built.WorkerTenantID,
					JobType:  ws.JobWorker.Name(),
					JobID:    "approval",
					Data:     jobdbtest.NumberTaskData(42),
				})
				if err != nil {
					t.Fatalf("start job via %s: %v", subject.mode, err)
				}

				handle := jobdbtest.WaitForTaskHandle(t, ctx, subject.Engine(), ws.JobWorker.Name(), "approval", []string{jobKey.TenantId})
				handleData, err := handle.Data()
				if err != nil {
					t.Fatalf("waiting task data via %s: %v", subject.mode, err)
				}
				if err := handle.Finish(ctx, jobdbtest.NumberTaskData(42)); err != nil {
					t.Fatalf("finish external task via %s: %v", subject.mode, err)
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
				_, outputErr := run.GetOutput(subject.Engine(), jobKey.TenantId)
				listed, err := subject.ListJobs(ctx, jobdb.ListJobsRequest{
					TenantIds: []string{jobKey.TenantId},
					JobKeys:   []jobdb.JobKey{jobKey},
					PageSize:  10,
				})
				if err != nil {
					t.Fatalf("list jobs via %s: %v", subject.mode, err)
				}

				return externalTaskObservation{
					JobKey:       jobKey,
					Status:       jobdb.JobStatusCompleted,
					WaitingInput: normalizeTaskDataResult(t, handleData),
					Result:       normalizeTaskDataResult(t, result),
					JobRun:       normalizeJobRun(t, run, outputErr),
					Listed:       normalizeJobSummaries(listed.Jobs),
				}
			})
		})
	}
}

func TestSequentialWorkflowParityAcrossBuiltInRuntimes(t *testing.T) {
	ws := jobdbtest.MustWorkSet(t,
		jobdbtest.SequenceJob{Steps: []string{jobdbtest.AddOneTaskName, jobdbtest.DoubleTaskName}},
		jobdbtest.AddOneTask{},
		jobdbtest.DoubleTask{},
	)

	for _, harness := range jobdbtest.BuiltInRuntimeHarnesses() {
		harness := harness
		t.Run(harness.Name, func(t *testing.T) {
			compareAcrossModes(t, harness, []workflow.WorkSet{ws}, func(t *testing.T, ctx context.Context, subject scenarioSubject) sequentialObservation {
				jobKey, err := subject.SubmitJob(ctx, jobdb.SubmitJob{
					TenantId: subject.built.WorkerTenantID,
					JobType:  jobdbtest.SequenceJobName,
					JobID:    "sequential",
					Data:     jobdbtest.NumberTaskData(1),
				})
				if err != nil {
					t.Fatalf("start sequential job via %s: %v", subject.mode, err)
				}
				subject.WaitForStatus(t, ctx, jobKey, jobdb.JobStatusCompleted)

				result, err := jobResultForTest(subject, ctx, jobKey)
				if err != nil {
					t.Fatalf("get sequential result via %s: %v", subject.mode, err)
				}
				run, err := subject.GetJobRun(ctx, jobdb.GetJobRunRequest{
					JobKey:               jobKey,
					IncludeInputs:        true,
					IncludeOutputs:       true,
					IncludeAttemptInputs: true,
				})
				if err != nil {
					t.Fatalf("get sequential run via %s: %v", subject.mode, err)
				}
				_, outputErr := run.GetOutput(subject.Engine(), jobKey.TenantId)
				listed, err := subject.ListJobs(ctx, jobdb.ListJobsRequest{
					TenantIds: []string{jobKey.TenantId},
					JobKeys:   []jobdb.JobKey{jobKey},
					PageSize:  10,
				})
				if err != nil {
					t.Fatalf("list sequential job via %s: %v", subject.mode, err)
				}

				chapters := make([]normalizedStoredChapter, 0, 3)
				for _, ordinal := range []int64{0, 1, 2} {
					chapter, err := subject.Runtime().GetChapter(ctx, jobdb.ChapterRef{JobKey: jobKey, Ordinal: ordinal})
					if err != nil {
						t.Fatalf("get sequential chapter %d via %s: %v", ordinal, subject.mode, err)
					}
					chapters = append(chapters, normalizeStoredChapter(chapter))
				}

				return sequentialObservation{
					JobKey:   jobKey,
					Status:   jobdb.JobStatusCompleted,
					Result:   normalizeTaskDataResult(t, result),
					JobRun:   normalizeJobRun(t, run, outputErr),
					Chapters: chapters,
					Listed:   normalizeJobSummaries(listed.Jobs),
				}
			})
		})
	}
}

func TestTaskFailureParityAcrossBuiltInRuntimes(t *testing.T) {
	job := failingTaskJob{name: "task-failure-job", task: "failing-task"}
	task := namedFailingTask{name: "failing-task", message: "intentional task failure"}
	ws := jobdbtest.MustWorkSet(t, job, task)

	for _, harness := range jobdbtest.BuiltInRuntimeHarnesses() {
		harness := harness
		t.Run(harness.Name, func(t *testing.T) {
			compareAcrossModes(t, harness, []workflow.WorkSet{ws}, func(t *testing.T, ctx context.Context, subject scenarioSubject) failureObservation {
				jobKey, err := subject.SubmitJob(ctx, jobdb.SubmitJob{
					TenantId: subject.built.WorkerTenantID,
					JobType:  ws.JobWorker.Name(),
					JobID:    "task-failure",
					Data:     jobdbtest.NumberTaskData(1),
				})
				if err != nil {
					t.Fatalf("start task failure via %s: %v", subject.mode, err)
				}
				subject.WaitForStatus(t, ctx, jobKey, jobdb.JobStatusCompleted)

				result, resultErr := jobResultForTest(subject, ctx, jobKey)
				run, err := subject.GetJobRun(ctx, jobdb.GetJobRunRequest{
					JobKey:               jobKey,
					IncludeInputs:        true,
					IncludeOutputs:       true,
					IncludeAttemptInputs: true,
				})
				if err != nil {
					t.Fatalf("get task failure run via %s: %v", subject.mode, err)
				}
				_, outputErr := run.GetOutput(subject.Engine(), jobKey.TenantId)
				listed, err := subject.ListJobs(ctx, jobdb.ListJobsRequest{
					TenantIds: []string{jobKey.TenantId},
					JobKeys:   []jobdb.JobKey{jobKey},
					PageSize:  10,
				})
				if err != nil {
					t.Fatalf("list task failure via %s: %v", subject.mode, err)
				}

				return failureObservation{
					JobKey:    jobKey,
					Status:    jobdb.JobStatusCompleted,
					Result:    normalizeTaskDataResult(t, result),
					ResultErr: normalizeError(resultErr),
					JobRun:    normalizeJobRun(t, run, outputErr),
					Listed:    normalizeJobSummaries(listed.Jobs),
				}
			})
		})
	}
}

func TestJobFailureParityAcrossBuiltInRuntimes(t *testing.T) {
	ws := jobdbtest.MustWorkSet(t, jobdbtest.FailingJob{})

	for _, harness := range jobdbtest.BuiltInRuntimeHarnesses() {
		harness := harness
		t.Run(harness.Name, func(t *testing.T) {
			compareAcrossModes(t, harness, []workflow.WorkSet{ws}, func(t *testing.T, ctx context.Context, subject scenarioSubject) failureObservation {
				jobKey, err := subject.SubmitJob(ctx, jobdb.SubmitJob{
					TenantId: subject.built.WorkerTenantID,
					JobType:  jobdbtest.FailingJobName,
					JobID:    "job-failure",
					Data:     jobdbtest.NumberTaskData(1),
				})
				if err != nil {
					t.Fatalf("start job failure via %s: %v", subject.mode, err)
				}
				subject.WaitForStatus(t, ctx, jobKey, jobdb.JobStatusCompleted)

				result, resultErr := jobResultForTest(subject, ctx, jobKey)
				run, err := subject.GetJobRun(ctx, jobdb.GetJobRunRequest{
					JobKey:               jobKey,
					IncludeInputs:        true,
					IncludeOutputs:       true,
					IncludeAttemptInputs: true,
				})
				if err != nil {
					t.Fatalf("get job failure run via %s: %v", subject.mode, err)
				}
				_, outputErr := run.GetOutput(subject.Engine(), jobKey.TenantId)
				listed, err := subject.ListJobs(ctx, jobdb.ListJobsRequest{
					TenantIds: []string{jobKey.TenantId},
					JobKeys:   []jobdb.JobKey{jobKey},
					PageSize:  10,
				})
				if err != nil {
					t.Fatalf("list job failure via %s: %v", subject.mode, err)
				}

				return failureObservation{
					JobKey:    jobKey,
					Status:    jobdb.JobStatusCompleted,
					Result:    normalizeTaskDataResult(t, result),
					ResultErr: normalizeError(resultErr),
					JobRun:    normalizeJobRun(t, run, outputErr),
					Listed:    normalizeJobSummaries(listed.Jobs),
				}
			})
		})
	}
}

func TestReplayAfterExternalTaskCompletionParityAcrossBuiltInRuntimes(t *testing.T) {
	ws := jobdbtest.MustWorkSet(t, externalApprovalJob{})

	for _, harness := range jobdbtest.BuiltInRuntimeHarnesses() {
		harness := harness
		t.Run(harness.Name, func(t *testing.T) {
			compareAcrossModes(t, harness, []workflow.WorkSet{ws}, func(t *testing.T, ctx context.Context, subject scenarioSubject) replayObservation {
				jobKey, err := subject.SubmitJob(ctx, jobdb.SubmitJob{
					TenantId: subject.built.WorkerTenantID,
					JobType:  ws.JobWorker.Name(),
					JobID:    "approval",
					Data:     jobdbtest.NumberTaskData(42),
					RunPolicy: jobdb.RunPolicy{
						Retry: jobdb.RetryPolicy{MaximumAttempts: 3, BackoffCoefficient: 1},
					},
				})
				if err != nil {
					t.Fatalf("start replay job via %s: %v", subject.mode, err)
				}

				handle := jobdbtest.WaitForTaskHandle(t, ctx, subject.Engine(), ws.JobWorker.Name(), "approval", []string{jobKey.TenantId})
				if err := handle.Finish(ctx, jobdbtest.NumberTaskData(42)); err != nil {
					t.Fatalf("finish approval via %s: %v", subject.mode, err)
				}
				subject.WaitForStatus(t, ctx, jobKey, jobdb.JobStatusCompleted)

				result, err := jobResultForTest(subject, ctx, jobKey)
				if err != nil {
					t.Fatalf("get replay result via %s: %v", subject.mode, err)
				}
				replayed, replayErr := subject.Engine().ReplayJobRun(ctx, workflow.ReplayRunRequest{JobKey: jobKey})

				return replayObservation{
					JobKey:    jobKey,
					Status:    jobdb.JobStatusCompleted,
					Result:    normalizeTaskDataResult(t, result),
					Replayed:  normalizeTaskDataResult(t, replayed),
					ReplayErr: normalizeError(replayErr),
				}
			})
		})
	}
}

func TestAwaitJobsParityAcrossBuiltInRuntimes(t *testing.T) {
	for _, harness := range jobdbtest.BuiltInRuntimeHarnesses() {
		harness := harness
		t.Run(harness.Name, func(t *testing.T) {
			run := func(mode parityMode) awaitObservation {
				childStarted := make(chan struct{})
				releaseChild := make(chan struct{})
				wsChild := jobdbtest.MustWorkSet(t, awaitChildJob{started: childStarted, release: releaseChild})
				wsParent := jobdbtest.MustWorkSet(t, awaitParentJob{childJobID: "child"})

				return observeViaMode(t, harness, mode, []workflow.WorkSet{wsChild, wsParent}, func(t *testing.T, ctx context.Context, subject scenarioSubject) awaitObservation {
					defer func() {
						select {
						case <-releaseChild:
						default:
							close(releaseChild)
						}
					}()

					childKey, err := subject.SubmitJob(ctx, jobdb.SubmitJob{
						TenantId: subject.built.WorkerTenantID,
						JobType:  wsChild.JobWorker.Name(),
						JobID:    "child",
						Data:     jobdbtest.NumberTaskData(1),
					})
					if err != nil {
						t.Fatalf("start child via %s: %v", subject.mode, err)
					}

					select {
					case <-childStarted:
					case <-ctx.Done():
						t.Fatalf("child start signal via %s: %v", subject.mode, ctx.Err())
					}

					parentKey, err := subject.SubmitJob(ctx, jobdb.SubmitJob{
						TenantId: childKey.TenantId,
						JobType:  wsParent.JobWorker.Name(),
						JobID:    "parent",
						Data:     jobdbtest.NumberTaskData(2),
					})
					if err != nil {
						t.Fatalf("start parent via %s: %v", subject.mode, err)
					}
					subject.WaitForStatus(t, ctx, parentKey, jobdb.JobStatusPendingJobs)

					pendingList, err := subject.ListJobs(ctx, jobdb.ListJobsRequest{
						TenantIds: []string{parentKey.TenantId},
						JobKeys:   []jobdb.JobKey{parentKey},
						PageSize:  10,
					})
					if err != nil {
						t.Fatalf("list pending jobs via %s: %v", subject.mode, err)
					}

					close(releaseChild)
					subject.WaitForStatus(t, ctx, childKey, jobdb.JobStatusCompleted)
					subject.WaitForStatus(t, ctx, parentKey, jobdb.JobStatusCompleted)

					result, err := jobResultForTest(subject, ctx, parentKey)
					if err != nil {
						t.Fatalf("get parent result via %s: %v", subject.mode, err)
					}
					finalRun, err := subject.GetJobRun(ctx, jobdb.GetJobRunRequest{
						JobKey:               parentKey,
						IncludeInputs:        true,
						IncludeOutputs:       true,
						IncludeAttemptInputs: true,
					})
					if err != nil {
						t.Fatalf("get parent run via %s: %v", subject.mode, err)
					}
					_, outputErr := finalRun.GetOutput(subject.Engine(), parentKey.TenantId)
					finalList, err := subject.ListJobs(ctx, jobdb.ListJobsRequest{
						TenantIds: []string{parentKey.TenantId},
						JobKeys:   []jobdb.JobKey{parentKey},
						PageSize:  10,
					})
					if err != nil {
						t.Fatalf("list final parent via %s: %v", subject.mode, err)
					}

					return awaitObservation{
						ChildKey:      childKey,
						ParentKey:     parentKey,
						PendingStatus: jobdb.JobStatusPendingJobs,
						PendingList:   normalizeJobSummaries(pendingList.Jobs),
						FinalStatus:   jobdb.JobStatusCompleted,
						FinalResult:   normalizeTaskDataResult(t, result),
						FinalRun:      normalizeJobRun(t, finalRun, outputErr),
						FinalList:     normalizeJobSummaries(finalList.Jobs),
					}
				})
			}

			engineObs := run(engineMode)
			runtimeObs := run(runtimeMode)
			compareObservations(t, engineObs, runtimeObs)
		})
	}
}

func TestTaskContextAwaitJobsParityAcrossBuiltInRuntimes(t *testing.T) {
	for _, harness := range jobdbtest.BuiltInRuntimeHarnesses() {
		harness := harness
		t.Run(harness.Name, func(t *testing.T) {
			run := func(mode parityMode) awaitObservation {
				childStarted := make(chan struct{})
				releaseChild := make(chan struct{})
				wsChild := jobdbtest.MustWorkSet(t, awaitChildJob{started: childStarted, release: releaseChild})
				wsParent := jobdbtest.MustWorkSet(t, awaitTaskJob{}, awaitTaskWorker{childJobID: "child"})

				return observeViaMode(t, harness, mode, []workflow.WorkSet{wsChild, wsParent}, func(t *testing.T, ctx context.Context, subject scenarioSubject) awaitObservation {
					defer func() {
						select {
						case <-releaseChild:
						default:
							close(releaseChild)
						}
					}()

					childKey, err := subject.SubmitJob(ctx, jobdb.SubmitJob{
						TenantId: subject.built.WorkerTenantID,
						JobType:  wsChild.JobWorker.Name(),
						JobID:    "child",
						Data:     jobdbtest.NumberTaskData(1),
					})
					if err != nil {
						t.Fatalf("start child via %s: %v", subject.mode, err)
					}
					select {
					case <-childStarted:
					case <-ctx.Done():
						t.Fatalf("child start signal via %s: %v", subject.mode, ctx.Err())
					}

					parentKey, err := subject.SubmitJob(ctx, jobdb.SubmitJob{
						TenantId: childKey.TenantId,
						JobType:  wsParent.JobWorker.Name(),
						JobID:    "parent",
						Data:     jobdbtest.NumberTaskData(2),
					})
					if err != nil {
						t.Fatalf("start parent via %s: %v", subject.mode, err)
					}
					subject.WaitForStatus(t, ctx, parentKey, jobdb.JobStatusPendingJobs)

					pendingList, err := subject.ListJobs(ctx, jobdb.ListJobsRequest{
						TenantIds: []string{parentKey.TenantId},
						JobKeys:   []jobdb.JobKey{parentKey},
						PageSize:  10,
					})
					if err != nil {
						t.Fatalf("list pending task-await via %s: %v", subject.mode, err)
					}

					close(releaseChild)
					subject.WaitForStatus(t, ctx, childKey, jobdb.JobStatusCompleted)
					subject.WaitForStatus(t, ctx, parentKey, jobdb.JobStatusCompleted)

					result, err := jobResultForTest(subject, ctx, parentKey)
					if err != nil {
						t.Fatalf("get parent result via %s: %v", subject.mode, err)
					}
					finalRun, err := subject.GetJobRun(ctx, jobdb.GetJobRunRequest{
						JobKey:               parentKey,
						IncludeInputs:        true,
						IncludeOutputs:       true,
						IncludeAttemptInputs: true,
					})
					if err != nil {
						t.Fatalf("get parent run via %s: %v", subject.mode, err)
					}
					_, outputErr := finalRun.GetOutput(subject.Engine(), parentKey.TenantId)
					finalList, err := subject.ListJobs(ctx, jobdb.ListJobsRequest{
						TenantIds: []string{parentKey.TenantId},
						JobKeys:   []jobdb.JobKey{parentKey},
						PageSize:  10,
					})
					if err != nil {
						t.Fatalf("list final task-await via %s: %v", subject.mode, err)
					}

					return awaitObservation{
						ChildKey:      childKey,
						ParentKey:     parentKey,
						PendingStatus: jobdb.JobStatusPendingJobs,
						PendingList:   normalizeJobSummaries(pendingList.Jobs),
						FinalStatus:   jobdb.JobStatusCompleted,
						FinalResult:   normalizeTaskDataResult(t, result),
						FinalRun:      normalizeJobRun(t, finalRun, outputErr),
						FinalList:     normalizeJobSummaries(finalList.Jobs),
					}
				})
			}

			engineObs := run(engineMode)
			runtimeObs := run(runtimeMode)
			compareObservations(t, engineObs, runtimeObs)
		})
	}
}

func TestPendingTaskHandleParityAcrossBuiltInRuntimes(t *testing.T) {
	ws := jobdbtest.MustWorkSet(t, transformPendingJob{}, jobdbtest.AddOneTask{})

	for _, harness := range jobdbtest.BuiltInRuntimeHarnesses() {
		harness := harness
		t.Run(harness.Name, func(t *testing.T) {
			compareAcrossModes(t, harness, []workflow.WorkSet{ws}, func(t *testing.T, ctx context.Context, subject scenarioSubject) pendingTaskObservation {
				jobKey, err := subject.SubmitJob(ctx, jobdb.SubmitJob{
					TenantId: subject.built.WorkerTenantID,
					JobType:  ws.JobWorker.Name(),
					JobID:    "pending",
					Data:     jobdbtest.NumberTaskData(10),
				})
				if err != nil {
					t.Fatalf("start pending via %s: %v", subject.mode, err)
				}

				handle := jobdbtest.WaitForTaskHandle(t, ctx, subject.Engine(), ws.JobWorker.Name(), "pending-task", []string{jobKey.TenantId})
				waiting, err := subject.Engine().GetWaitingTask(ctx, jobKey)
				if err != nil {
					t.Fatalf("get waiting task via %s: %v", subject.mode, err)
				}
				waitingData, err := waiting.Data()
				if err != nil {
					t.Fatalf("waiting task data via %s: %v", subject.mode, err)
				}
				pendingStatus, err := jobStatusForTest(subject, ctx, jobKey)
				if err != nil {
					t.Fatalf("check pending status via %s: %v", subject.mode, err)
				}
				pendingList, err := subject.ListJobs(ctx, jobdb.ListJobsRequest{
					TenantIds: []string{jobKey.TenantId},
					JobKeys:   []jobdb.JobKey{jobKey},
				})
				if err != nil {
					t.Fatalf("list pending job via %s: %v", subject.mode, err)
				}
				if len(pendingList.Jobs) != 1 {
					t.Fatalf("expected 1 pending summary via %s, got %d", subject.mode, len(pendingList.Jobs))
				}

				if err := handle.Finish(ctx, jobdbtest.NumberTaskData(200)); err != nil {
					t.Fatalf("finish waiting task via %s: %v", subject.mode, err)
				}
				subject.WaitForStatus(t, ctx, jobKey, jobdb.JobStatusCompleted)

				result, err := jobResultForTest(subject, ctx, jobKey)
				if err != nil {
					t.Fatalf("get pending final result via %s: %v", subject.mode, err)
				}
				finalRun, err := subject.GetJobRun(ctx, jobdb.GetJobRunRequest{
					JobKey:               jobKey,
					IncludeInputs:        true,
					IncludeOutputs:       true,
					IncludeAttemptInputs: true,
				})
				if err != nil {
					t.Fatalf("get pending final run via %s: %v", subject.mode, err)
				}
				_, outputErr := finalRun.GetOutput(subject.Engine(), jobKey.TenantId)

				return pendingTaskObservation{
					JobKey:            jobKey,
					PendingStatus:     pendingStatus,
					WaitingInput:      normalizeTaskDataResult(t, waitingData),
					NextNeed:          cloneStringPtr(pendingList.Jobs[0].NextNeed),
					TaskWaitInput:     cloneInt64Ptr(pendingList.Jobs[0].TaskWaitInput),
					TaskWaitOutput:    cloneInt64Ptr(pendingList.Jobs[0].TaskWaitOutput),
					TaskWaitInputHash: cloneStringPtr(pendingList.Jobs[0].TaskWaitInputHash),
					TaskWaitNext:      cloneStringPtr(pendingList.Jobs[0].TaskWaitNext),
					FinalStatus:       jobdb.JobStatusCompleted,
					FinalResult:       normalizeTaskDataResult(t, result),
					FinalRun:          normalizeJobRun(t, finalRun, outputErr),
					Listed:            normalizeJobSummaries(pendingList.Jobs),
				}
			})
		})
	}
}

func TestAwaitDurationParityAcrossBuiltInRuntimes(t *testing.T) {
	ws := jobdbtest.MustWorkSet(t, awaitDurationJob{name: "await-duration", wait: 300 * time.Millisecond})

	for _, harness := range jobdbtest.BuiltInRuntimeHarnesses() {
		harness := harness
		t.Run(harness.Name, func(t *testing.T) {
			compareAcrossModes(t, harness, []workflow.WorkSet{ws}, func(t *testing.T, ctx context.Context, subject scenarioSubject) awaitDurationObservation {
				started := time.Now()
				jobKey, err := subject.SubmitJob(ctx, jobdb.SubmitJob{
					TenantId: subject.built.WorkerTenantID,
					JobType:  ws.JobWorker.Name(),
					JobID:    "await-duration",
					Data:     jobdbtest.NumberTaskData(9),
				})
				if err != nil {
					t.Fatalf("start await-duration via %s: %v", subject.mode, err)
				}
				subject.WaitForStatus(t, ctx, jobKey, jobdb.JobStatusCompleted)
				result, err := jobResultForTest(subject, ctx, jobKey)
				if err != nil {
					t.Fatalf("get await-duration result via %s: %v", subject.mode, err)
				}
				finalRun, err := subject.GetJobRun(ctx, jobdb.GetJobRunRequest{
					JobKey:               jobKey,
					IncludeInputs:        true,
					IncludeOutputs:       true,
					IncludeAttemptInputs: true,
				})
				if err != nil {
					t.Fatalf("get await-duration run via %s: %v", subject.mode, err)
				}
				_, outputErr := finalRun.GetOutput(subject.Engine(), jobKey.TenantId)
				finalList, err := subject.ListJobs(ctx, jobdb.ListJobsRequest{
					TenantIds: []string{jobKey.TenantId},
					JobKeys:   []jobdb.JobKey{jobKey},
					PageSize:  10,
				})
				if err != nil {
					t.Fatalf("list completed await-duration job via %s: %v", subject.mode, err)
				}

				return awaitDurationObservation{
					JobKey:       jobKey,
					WaitObserved: time.Since(started) >= 250*time.Millisecond,
					FinalStatus:  jobdb.JobStatusCompleted,
					FinalResult:  normalizeTaskDataResult(t, result),
					FinalRun:     normalizeJobRun(t, finalRun, outputErr),
					FinalList:    normalizeJobSummaries(finalList.Jobs),
				}
			})
		})
	}
}

func TestJobRetryParityAcrossBuiltInRuntimes(t *testing.T) {
	for _, harness := range jobdbtest.BuiltInRuntimeHarnesses() {
		harness := harness
		t.Run(harness.Name, func(t *testing.T) {
			run := func(mode parityMode) retryObservation {
				job := &retryJob{}
				ws := jobdbtest.MustWorkSet(t, job)
				return observeViaMode(t, harness, mode, []workflow.WorkSet{ws}, func(t *testing.T, ctx context.Context, subject scenarioSubject) retryObservation {
					jobKey, err := subject.SubmitJob(ctx, jobdb.SubmitJob{
						TenantId: subject.built.WorkerTenantID,
						JobType:  job.Name(),
						JobID:    "retry-job",
						Data:     jobdbtest.NumberTaskData(5),
						RunPolicy: jobdb.RunPolicy{
							Retry: jobdb.RetryPolicy{MaximumAttempts: 3, BackoffCoefficient: 1},
						},
					})
					if err != nil {
						t.Fatalf("start job retry via %s: %v", subject.mode, err)
					}
					subject.WaitForStatus(t, ctx, jobKey, jobdb.JobStatusCompleted)
					result, err := jobResultForTest(subject, ctx, jobKey)
					if err != nil {
						t.Fatalf("get job retry result via %s: %v", subject.mode, err)
					}
					run, err := subject.GetJobRun(ctx, jobdb.GetJobRunRequest{
						JobKey:         jobKey,
						IncludeOutputs: true,
					})
					if err != nil {
						t.Fatalf("get job retry run via %s: %v", subject.mode, err)
					}
					_, outputErr := run.GetOutput(subject.Engine(), jobKey.TenantId)
					return retryObservation{
						JobKey: jobKey,
						Status: jobdb.JobStatusCompleted,
						Result: normalizeTaskDataResult(t, result),
						JobRun: normalizeJobRun(t, run, outputErr),
					}
				})
			}
			compareObservations(t, run(engineMode), run(runtimeMode))
		})
	}
}

func TestTaskRetryParityOnDirectRuntime(t *testing.T) {
	for _, harness := range jobdbtest.BuiltInRuntimeHarnesses() {
		harness := harness
		if harness.Name != "direct" {
			continue
		}
		t.Run(harness.Name, func(t *testing.T) {
			run := func(mode parityMode) retryObservation {
				job := &retryTaskJob{}
				task := &retryTask{}
				ws := jobdbtest.MustWorkSet(t, job, task)
				return observeViaMode(t, harness, mode, []workflow.WorkSet{ws}, func(t *testing.T, ctx context.Context, subject scenarioSubject) retryObservation {
					jobKey, err := subject.SubmitJob(ctx, jobdb.SubmitJob{
						TenantId: subject.built.WorkerTenantID,
						JobType:  job.Name(),
						JobID:    "retry-task",
						Data:     jobdbtest.NumberTaskData(5),
					})
					if err != nil {
						t.Fatalf("start task retry via %s: %v", subject.mode, err)
					}
					subject.WaitForStatus(t, ctx, jobKey, jobdb.JobStatusCompleted)
					result, err := jobResultForTest(subject, ctx, jobKey)
					if err != nil {
						t.Fatalf("get task retry result via %s: %v", subject.mode, err)
					}
					run, err := subject.GetJobRun(ctx, jobdb.GetJobRunRequest{
						JobKey:               jobKey,
						IncludeInputs:        true,
						IncludeOutputs:       true,
						IncludeAttemptInputs: true,
					})
					if err != nil {
						t.Fatalf("get task retry run via %s: %v", subject.mode, err)
					}
					_, outputErr := run.GetOutput(subject.Engine(), jobKey.TenantId)
					return retryObservation{
						JobKey: jobKey,
						Status: jobdb.JobStatusCompleted,
						Result: normalizeTaskDataResult(t, result),
						JobRun: normalizeJobRun(t, run, outputErr),
					}
				})
			}

			engineObs := run(engineMode)
			runtimeObs := run(runtimeMode)
			compareObservations(t, engineObs, runtimeObs)
		})
	}
}
