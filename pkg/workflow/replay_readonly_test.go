package workflow

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
)

func TestReplayJobRun_LocalTaskCacheMissDoesNotExecuteWorker(t *testing.T) {
	runtime := newRunnerTestRuntime()
	jobKey := JobKey{TenantId: "tenant", JobId: "replay-local-miss"}

	putCalls := &atomic.Int32{}
	runtime.putChapterHook = func(PutChapterRequest) error {
		putCalls.Add(1)
		return errors.New("unexpected replay write")
	}

	taskRuns := &atomic.Int32{}
	job := singleTaskJob{name: "replay-local-miss", taskType: "local_task"}
	task := countingTaskWorker{name: "local_task", counter: taskRuns}
	ws := mustWorkSetForRunnerTest(t, job, task)

	seedJobStartForTest(t, runtime, jobKey, job.Name(), NewTaskDataOrPanic(map[string]int{"n": 1}), RunPolicy{})

	engine, err := newWorkerEngine(runtime, []WorkSet{*ws}, RuntimeBuildOptions{PollTenantId: jobKey.TenantId})
	if err != nil {
		t.Fatalf("new worker engine: %v", err)
	}

	_, err = engine.ReplayJobRun(context.Background(), ReplayRunRequest{JobKey: jobKey})

	var miss ReplayCacheMissError
	if !errors.As(err, &miss) {
		t.Fatalf("expected ReplayCacheMissError, got %T %v", err, err)
	}
	if miss.Reason != ReplayCacheMissTaskResultMissing {
		t.Fatalf("unexpected cache miss reason: got %q want %q", miss.Reason, ReplayCacheMissTaskResultMissing)
	}
	if miss.TaskType != "local_task" || miss.Ordinal != 1 || miss.Attempt != 1 {
		t.Fatalf("unexpected cache miss details: %+v", miss)
	}
	if got := taskRuns.Load(); got != 0 {
		t.Fatalf("replay executed local task worker %d times", got)
	}
	if got := putCalls.Load(); got != 0 {
		t.Fatalf("replay attempted %d chapter writes", got)
	}
}

func TestReplayJobRun_MissingJobOutcomeReturnsCacheMissWithoutWriting(t *testing.T) {
	runtime := newRunnerTestRuntime()
	jobKey := JobKey{TenantId: "tenant", JobId: "replay-job-outcome-miss"}

	putCalls := &atomic.Int32{}
	runtime.putChapterHook = func(PutChapterRequest) error {
		putCalls.Add(1)
		return errors.New("unexpected replay write")
	}

	jobRuns := &atomic.Int32{}
	job := countingJobWorker{name: "replay-job-outcome-miss", counter: jobRuns}
	ws := mustWorkSetForRunnerTest(t, job)

	seedJobStartForTest(t, runtime, jobKey, job.Name(), NewTaskDataOrPanic(map[string]int{"n": 1}), RunPolicy{})

	engine, err := newWorkerEngine(runtime, []WorkSet{*ws}, RuntimeBuildOptions{PollTenantId: jobKey.TenantId})
	if err != nil {
		t.Fatalf("new worker engine: %v", err)
	}

	_, err = engine.ReplayJobRun(context.Background(), ReplayRunRequest{JobKey: jobKey})

	var miss ReplayCacheMissError
	if !errors.As(err, &miss) {
		t.Fatalf("expected ReplayCacheMissError, got %T %v", err, err)
	}
	if miss.Reason != ReplayCacheMissJobResultMissing {
		t.Fatalf("unexpected cache miss reason: got %q want %q", miss.Reason, ReplayCacheMissJobResultMissing)
	}
	if miss.Ordinal != 1 || miss.Attempt != 1 {
		t.Fatalf("unexpected cache miss details: %+v", miss)
	}
	if got := jobRuns.Load(); got != 1 {
		t.Fatalf("expected replay to run job orchestration once, got %d", got)
	}
	if got := putCalls.Load(); got != 0 {
		t.Fatalf("replay attempted %d chapter writes", got)
	}
}

func TestReplayReadOnlyRuntimeRejectsMutations(t *testing.T) {
	runtime := newRunnerTestRuntime()
	replayRuntime := newReplayReadOnlyRuntime(runtime)
	ctx := context.Background()

	tests := []struct {
		name string
		call func() error
	}{
		{
			name: "SubmitJob",
			call: func() error {
				_, err := replayRuntime.SubmitJob(ctx, SubmitJobRequest{})
				return err
			},
		},
		{
			name: "SubmitRestartJob",
			call: func() error {
				_, err := replayRuntime.SubmitRestartJob(ctx, SubmitRestartJobRequest{})
				return err
			},
		},
		{
			name: "CancelJob",
			call: func() error {
				return replayRuntime.CancelJob(ctx, CancelJobRequest{})
			},
		},
		{
			name: "PollWork",
			call: func() error {
				_, err := replayRuntime.PollWork(ctx, PollWorkRequest{})
				return err
			},
		},
		{
			name: "GetJobLease",
			call: func() error {
				_, err := replayRuntime.GetJobLease(ctx, GetJobLeaseRequest{})
				return err
			},
		},
		{
			name: "CompleteTaskIfWaiting",
			call: func() error {
				return replayRuntime.CompleteTaskIfWaiting(ctx, CompleteTaskIfWaitingRequest{})
			},
		},
		{
			name: "PutChapter",
			call: func() error {
				return replayRuntime.PutChapter(ctx, PutChapterRequest{})
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if err := tt.call(); !errors.Is(err, ErrReplayShouldNeverMutate) {
				t.Fatalf("expected ErrReplayShouldNeverMutate, got %v", err)
			}
		})
	}
}

func TestReplayReadOnlyRuntimeAllowsReads(t *testing.T) {
	runtime := newRunnerTestRuntime()
	jobKey := JobKey{TenantId: "tenant", JobId: "replay-read-only-runtime"}
	job := countingJobWorker{name: "replay-read-only-runtime", counter: &atomic.Int32{}}
	seedJobStartForTest(t, runtime, jobKey, job.Name(), NewTaskDataOrPanic(map[string]int{"n": 1}), RunPolicy{})

	replayRuntime := newReplayReadOnlyRuntime(runtime)
	chapter, err := replayRuntime.GetChapter(context.Background(), ChapterRef{JobKey: jobKey, Ordinal: 0})
	if err != nil {
		t.Fatalf("GetChapter through replay runtime: %v", err)
	}
	if chapter.Ordinal != 0 || chapter.TaskType != job.Name() {
		t.Fatalf("unexpected chapter: %+v", chapter)
	}
}

func TestWorkerRunnerRejectsReplayJobOutcomePersistence(t *testing.T) {
	runtime := newRunnerTestRuntime()
	jobKey := JobKey{TenantId: "tenant", JobId: "replay-persist-guard"}
	job := countingJobWorker{name: "replay-persist-guard", counter: &atomic.Int32{}}
	ws := mustWorkSetForRunnerTest(t, job)
	runner := newWorkerRunner(runtime, ws, nil, workerRunnerOptions{
		JobKey: jobKey,
		Replay: true,
	})

	_, err := runner.persistJobOutcome(context.Background(), 1, nil, nil, payloadKindApp, "hash", 1, &InputReference{}, nil, nil)
	if !errors.Is(err, ErrReplayShouldNeverMutate) {
		t.Fatalf("expected ErrReplayShouldNeverMutate, got %v", err)
	}
}
