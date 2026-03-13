package swf

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type pollTrackingRuntime struct {
	*runnerTestRuntime

	leases      []ExecutionLease
	pollDelay   time.Duration
	pollCalls   atomic.Int64
	inFlight    atomic.Int64
	maxInFlight atomic.Int64

	mu        sync.Mutex
	workerIDs []string
}

func (r *pollTrackingRuntime) PollWork(ctx context.Context, req PollWorkRequest) ([]ExecutionLease, error) {
	current := r.inFlight.Add(1)
	defer r.inFlight.Add(-1)
	for {
		maxSeen := r.maxInFlight.Load()
		if current <= maxSeen || r.maxInFlight.CompareAndSwap(maxSeen, current) {
			break
		}
	}

	r.mu.Lock()
	r.workerIDs = append(r.workerIDs, req.WorkerID)
	r.mu.Unlock()

	if r.pollDelay > 0 {
		timer := time.NewTimer(r.pollDelay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-timer.C:
		}
	}

	call := int(r.pollCalls.Add(1)) - 1
	if call >= len(r.leases) {
		return nil, nil
	}
	return []ExecutionLease{r.leases[call]}, nil
}

func (r *pollTrackingRuntime) seenWorkerIDs() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, len(r.workerIDs))
	copy(out, r.workerIDs)
	return out
}

type blockingJobWorker struct {
	name    string
	started chan<- struct{}
	release <-chan struct{}
}

func (w blockingJobWorker) Name() string { return w.name }

func (w blockingJobWorker) Run(_ JobContext, input JobData) (JobData, error) {
	select {
	case w.started <- struct{}{}:
	default:
	}
	<-w.release
	return input, nil
}

func TestWorkerEngineSerializesPollsAndUsesDistinctWorkerIDs(t *testing.T) {
	jobType := "serial-job"
	started := make(chan struct{}, 2)
	release := make(chan struct{})
	runtime := &pollTrackingRuntime{
		runnerTestRuntime: newRunnerTestRuntime(),
		pollDelay:         25 * time.Millisecond,
	}

	jobKeys := []JobKey{
		{TenantId: "tenant-a", JobId: "job-a"},
		{TenantId: "tenant-a", JobId: "job-b"},
	}
	for _, key := range jobKeys {
		seedJobStartForTest(t, runtime.runnerTestRuntime, key, jobType, NewTaskDataOrPanic(map[string]int{"value": 1}), RunPolicy{})
	}
	runtime.leases = []ExecutionLease{
		&fakeExecutionLease{job: JobHandle{JobKey: jobKeys[0]}, capability: jobType},
		&fakeExecutionLease{job: JobHandle{JobKey: jobKeys[1]}, capability: jobType},
	}

	ws := mustWorkSetForRunnerTest(t, blockingJobWorker{
		name:    jobType,
		started: started,
		release: release,
	})
	engine, err := newWorkerEngine(runtime, []WorkSet{*ws}, RuntimeBuildOptions{MaxActive: 2})
	if err != nil {
		t.Fatalf("build worker engine: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() {
		defer close(done)
		engine.Run(ctx)
	}()

	for i := 0; i < 2; i++ {
		select {
		case <-started:
		case <-time.After(5 * time.Second):
			t.Fatalf("worker %d did not start", i+1)
		}
	}

	if got := runtime.maxInFlight.Load(); got != 1 {
		t.Fatalf("expected serialized PollWork calls, max in flight=%d", got)
	}

	workerIDs := runtime.seenWorkerIDs()
	if len(workerIDs) < 2 {
		t.Fatalf("expected at least two poll calls, got %d", len(workerIDs))
	}
	if workerIDs[0] == workerIDs[1] {
		t.Fatalf("expected distinct worker IDs per lease claim, got %q", workerIDs[0])
	}

	close(release)
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("worker engine did not stop")
	}
}
