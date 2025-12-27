package impl

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/colony-2/pgwf-go/pkg/pgwf"
	"github.com/colony-2/strata-go/pkg/client/core"
	"github.com/colony-2/swf-go/pkg/swf"
)

// TestJobRestartUsesCache verifies the critical durability guarantee:
// If a job completes successfully but the lease is not completed (crash/restart),
// the job should NOT re-execute when restarted - it should use the cached result.
// This test would have FAILED before the DoJob() fix.
func TestJobRestartUsesCache(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Track execution count - should only execute once despite multiple restarts
	var executionCount atomic.Int32

	jobWorker := &countingJobWorker{
		name:    "restart-test-job",
		counter: &executionCount,
	}
	jobWorker.workset = initWorkset(jobWorker)

	embedded, err := StartEmbeddedEngine(ctx, jobWorker)
	if err != nil {
		t.Fatalf("start embedded engine: %v", err)
	}
	defer embedded.Shutdown()

	engine := embedded.SWFEngine.(*swfEngineImpl)

	// Start the job
	input := swf.NewTaskDataOrPanic(map[string]string{"test": "data"})
	jobKey, err := engine.StartJob(ctx, swf.StartJob{
		TenantId: "test-tenant",
		JobType:  jobWorker.Name(),
		Data:     input,
	})
	if err != nil {
		t.Fatalf("start job: %v", err)
	}

	// Manually execute the job ONCE without completing lease (simulating crash)
	// This represents the job executing, saving result, but crashing before lease completion
	lease := getLeaseForJob(t, ctx, engine, jobKey)
	if lease == nil {
		t.Fatalf("no lease available")
	}

	// Execute manually - this will run the job and save the result
	r := &runner{
		jobId:        lease.JobID(),
		tenantId:     jobKey.TenantId,
		worker:       jobWorker.workset,
		storyCounter: 1,
		engine:       engine,
		lease:        lease,
		logger:       engine.logger,
		jobPolicy:    normalizeRunPolicy(swf.RunPolicy{}),
		capability:   lease.NextNeed(),
		ctx:          ctx,
	}
	r.DoJob(ctx, lease)

	// Verify job executed exactly once
	if executionCount.Load() != 1 {
		t.Fatalf("expected 1 execution, got %d", executionCount.Load())
	}

	// Verify the result was saved (ordinal 1 should exist with success)
	key := jobKey.ToStoryKey()
	chap, err := engine.strata.Chapter(ctx, key, 1)
	if err != nil {
		t.Fatalf("expected chapter at ordinal 1: %v", err)
	}
	env, err := decodeChapterEnvelope(chap.Body())
	if err != nil {
		t.Fatalf("decode chapter: %v", err)
	}
	if env.PayloadKind != payloadKindApp {
		t.Fatalf("expected success payload, got %s", env.PayloadKind)
	}

	// Now simulate restart by getting work again and running DoJob again
	// The job should NOT re-execute - it should use the cached result
	lease2 := getLeaseForJob(t, ctx, engine, jobKey)
	if lease2 != nil {
		r2 := &runner{
			jobId:        lease2.JobID(),
			tenantId:     jobKey.TenantId,
			worker:       jobWorker.workset,
			storyCounter: 1,
			engine:       engine,
			lease:        lease2,
			logger:       engine.logger,
			jobPolicy:    normalizeRunPolicy(swf.RunPolicy{}),
			capability:   lease2.NextNeed(),
			ctx:          ctx,
		}
		r2.DoJob(ctx, lease2)

		// CRITICAL: Execution count should still be 1, not 2
		// The job should have used the cached result from ordinal 1
		if executionCount.Load() != 1 {
			t.Fatalf("job re-executed on restart! expected 1 execution, got %d", executionCount.Load())
		}
	}

	// Verify final result is correct
	result, err := engine.GetJobResult(ctx, jobKey)
	if err != nil {
		t.Fatalf("get job result: %v", err)
	}
	data, _ := result.GetData()
	var resultMap map[string]interface{}
	if err := json.Unmarshal(data, &resultMap); err != nil {
		t.Fatalf("unmarshal result: %v", err)
	}
	if resultMap["test"] != "data" || resultMap["executed"] != true {
		t.Fatalf("unexpected result: %v", resultMap)
	}
}

// TestJobRetryWithFailures verifies retry logic works correctly:
// - First attempt fails with retryable error
// - Second attempt succeeds
// - Both attempts are saved with correct ordinals
func TestJobRetryWithFailures(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var attemptCount atomic.Int32
	jobWorker := &failThenSucceedJobWorker{
		name:         "retry-test-job",
		failAttempts: 1,
		counter:      &attemptCount,
	}
	jobWorker.workset = initWorkset(jobWorker)

	embedded, err := StartEmbeddedEngine(ctx, nil)
	if err != nil {
		t.Fatalf("start embedded engine: %v", err)
	}
	defer embedded.Shutdown()

	engine := embedded.SWFEngine.(*swfEngineImpl)
	go engine.Run(ctx)

	// Register worker
	if err := engine.RegisterWorkers(jobWorker.workset); err != nil {
		t.Fatalf("register worker: %v", err)
	}

	// Start job with retry policy
	input := swf.NewTaskDataOrPanic(map[string]string{"test": "retry"})
	jobKey, err := engine.StartJob(ctx, swf.StartJob{
		TenantId: "test-tenant",
		JobType:  jobWorker.Name(),
		Data:     input,
		RunPolicy: swf.RunPolicy{
			Retry: swf.RetryPolicy{
				MaximumAttempts:    3,
				BackoffCoefficient: 1.0,
				InitialInterval:    swf.Duration(10 * time.Millisecond),
			},
		},
	})
	if err != nil {
		t.Fatalf("start job: %v", err)
	}

	// Wait for completion
	if err := swf.WaitForJobToComplete(ctx, 10*time.Second, jobKey, engine); err != nil {
		t.Fatalf("job did not complete: %v", err)
	}

	// Verify it executed twice (one failure, one success)
	if attemptCount.Load() != 2 {
		t.Fatalf("expected 2 attempts, got %d", attemptCount.Load())
	}

	// Verify both attempts are saved in story (each at a different ordinal)
	key := jobKey.ToStoryKey()

	// Ordinal 1 should have the first failed attempt
	chap1, err := engine.strata.Chapter(ctx, key, 1)
	if err != nil {
		t.Fatalf("expected chapter at ordinal 1: %v", err)
	}
	env1, err := decodeChapterEnvelope(chap1.Body())
	if err != nil {
		t.Fatalf("decode chapter 1: %v", err)
	}
	if env1.PayloadKind != payloadKindAppError {
		t.Fatalf("expected error at ordinal 1, got %s", env1.PayloadKind)
	}
	if env1.Meta.Attempt != 1 {
		t.Fatalf("expected attempt 1, got %d", env1.Meta.Attempt)
	}

	// Ordinal 2 should have the second successful attempt
	chap2, err := engine.strata.Chapter(ctx, key, 2)
	if err != nil {
		t.Fatalf("expected chapter at ordinal 2: %v", err)
	}
	env2, err := decodeChapterEnvelope(chap2.Body())
	if err != nil {
		t.Fatalf("decode chapter 2: %v", err)
	}
	if env2.PayloadKind != payloadKindApp {
		t.Fatalf("expected success at ordinal 2, got %s", env2.PayloadKind)
	}
	if env2.Meta.Attempt != 2 {
		t.Fatalf("expected attempt 2, got %d", env2.Meta.Attempt)
	}

	// Verify final result is success
	result, err := engine.GetJobResult(ctx, jobKey)
	if err != nil {
		t.Fatalf("get job result: %v", err)
	}
	data, _ := result.GetData()
	var resultMap map[string]interface{}
	if err := json.Unmarshal(data, &resultMap); err != nil {
		t.Fatalf("unmarshal result: %v", err)
	}
	if resultMap["test"] != "retry" || resultMap["attempt"] != float64(2) {
		t.Fatalf("unexpected result: %v", resultMap)
	}
}

// TestJobMaxRetriesExhausted verifies that jobs stop retrying after max attempts
// and don't create extra ordinals beyond the limit
func TestJobMaxRetriesExhausted(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var attemptCount atomic.Int32
	jobWorker := &alwaysFailJobWorker{
		name:    "max-retry-test-job",
		counter: &attemptCount,
	}
	jobWorker.workset = initWorkset(jobWorker)

	embedded, err := StartEmbeddedEngine(ctx, nil)
	if err != nil {
		t.Fatalf("start embedded engine: %v", err)
	}
	defer embedded.Shutdown()

	engine := embedded.SWFEngine.(*swfEngineImpl)
	go engine.Run(ctx)

	if err := engine.RegisterWorkers(jobWorker.workset); err != nil {
		t.Fatalf("register worker: %v", err)
	}

	maxAttempts := 3
	input := swf.NewTaskDataOrPanic(map[string]string{"test": "max-retry"})
	jobKey, err := engine.StartJob(ctx, swf.StartJob{
		TenantId: "test-tenant",
		JobType:  jobWorker.Name(),
		Data:     input,
		RunPolicy: swf.RunPolicy{
			Retry: swf.RetryPolicy{
				MaximumAttempts:    int32(maxAttempts),
				BackoffCoefficient: 1.0,
				InitialInterval:    swf.Duration(10 * time.Millisecond),
			},
		},
	})
	if err != nil {
		t.Fatalf("start job: %v", err)
	}

	// Wait for completion (it will complete with error)
	if err := swf.WaitForJobToComplete(ctx, 10*time.Second, jobKey, engine); err != nil {
		t.Fatalf("job did not complete: %v", err)
	}

	// Verify it executed exactly maxAttempts times, no more
	if attemptCount.Load() != int32(maxAttempts) {
		t.Fatalf("expected %d attempts, got %d", maxAttempts, attemptCount.Load())
	}

	// Verify all attempts are saved as separate chapters (write-once design)
	// Each attempt gets its own ordinal: 1, 2, 3
	key := jobKey.ToStoryKey()
	for i := 1; i <= maxAttempts; i++ {
		chap, err := engine.strata.Chapter(ctx, key, int64(i))
		if err != nil {
			t.Fatalf("expected chapter at ordinal %d: %v", i, err)
		}
		env, err := decodeChapterEnvelope(chap.Body())
		if err != nil {
			t.Fatalf("decode chapter %d: %v", i, err)
		}
		if env.Meta.Attempt != i {
			t.Fatalf("expected attempt %d at ordinal %d, got %d", i, i, env.Meta.Attempt)
		}
		if env.PayloadKind != payloadKindAppError {
			t.Fatalf("expected error at ordinal %d, got %s", i, env.PayloadKind)
		}
	}

	// Ordinal maxAttempts+1 should NOT exist (no more retries)
	_, err = engine.strata.Chapter(ctx, key, int64(maxAttempts+1))
	if !errors.Is(err, core.ErrNotFound) {
		t.Fatalf("expected no chapter at ordinal %d, got: %v", maxAttempts+1, err)
	}
}

// TestJobNonRetryableError verifies non-retryable errors complete immediately
// without retry attempts
func TestJobNonRetryableError(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var attemptCount atomic.Int32
	jobWorker := &nonRetryableErrorJobWorker{
		name:    "non-retryable-test-job",
		counter: &attemptCount,
	}
	jobWorker.workset = initWorkset(jobWorker)

	embedded, err := StartEmbeddedEngine(ctx, nil)
	if err != nil {
		t.Fatalf("start embedded engine: %v", err)
	}
	defer embedded.Shutdown()

	engine := embedded.SWFEngine.(*swfEngineImpl)
	go engine.Run(ctx)

	if err := engine.RegisterWorkers(jobWorker.workset); err != nil {
		t.Fatalf("register worker: %v", err)
	}

	input := swf.NewTaskDataOrPanic(map[string]string{"test": "non-retryable"})
	jobKey, err := engine.StartJob(ctx, swf.StartJob{
		TenantId: "test-tenant",
		JobType:  jobWorker.Name(),
		Data:     input,
		RunPolicy: swf.RunPolicy{
			Retry: swf.RetryPolicy{
				MaximumAttempts:        5,
				BackoffCoefficient:     1.0,
				InitialInterval:        swf.Duration(10 * time.Millisecond),
				NonRetryableErrorTypes: []string{"*impl.customNonRetryableError"},
			},
		},
	})
	if err != nil {
		t.Fatalf("start job: %v", err)
	}

	// Wait for completion
	if err := swf.WaitForJobToComplete(ctx, 10*time.Second, jobKey, engine); err != nil {
		t.Fatalf("job did not complete: %v", err)
	}

	// Should only execute once - non-retryable errors don't retry
	if attemptCount.Load() != 1 {
		t.Fatalf("expected 1 attempt (no retry for non-retryable), got %d", attemptCount.Load())
	}

	// Verify result is an error (non-retryable errors still save as errors)
	_, err = engine.GetJobResult(ctx, jobKey)
	if err == nil {
		t.Fatalf("expected error result, got success")
	}
	// The error message should be preserved
	if !strings.Contains(err.Error(), "non-retryable") {
		t.Fatalf("expected error message to contain 'non-retryable', got: %v", err)
	}
}

// TestJobOrdinalDeterminism verifies that job restarts use the same ordinals
// and don't create duplicate chapters
func TestJobOrdinalDeterminism(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var executionCount atomic.Int32
	jobWorker := &taskCallingJobWorker{
		name:    "ordinal-test-job",
		counter: &executionCount,
	}
	jobWorker.workset = initWorkset(jobWorker, &echoTaskWorker{})

	embedded, err := StartEmbeddedEngine(ctx, jobWorker)
	if err != nil {
		t.Fatalf("start embedded engine: %v", err)
	}
	defer embedded.Shutdown()

	engine := embedded.SWFEngine.(*swfEngineImpl)

	// Start the job
	input := swf.NewTaskDataOrPanic(map[string]string{"test": "ordinal"})
	jobKey, err := engine.StartJob(ctx, swf.StartJob{
		TenantId: "test-tenant",
		JobType:  jobWorker.Name(),
		Data:     input,
	})
	if err != nil {
		t.Fatalf("start job: %v", err)
	}

	// Execute job once (simulating crash before lease completion)
	lease := getLeaseForJob(t, ctx, engine, jobKey)
	if lease == nil {
		t.Fatalf("no lease available")
	}

	r := &runner{
		jobId:        lease.JobID(),
		tenantId:     jobKey.TenantId,
		worker:       jobWorker.workset,
		storyCounter: 1,
		engine:       engine,
		lease:        lease,
		logger:       engine.logger,
		jobPolicy:    normalizeRunPolicy(swf.RunPolicy{}),
		capability:   lease.NextNeed(),
		ctx:          ctx,
	}
	r.DoJob(ctx, lease)

	// The job calls DoTask twice, so we should have ordinals: 0 (input), 1 (task1), 2 (task2), 3 (job result)
	key := jobKey.ToStoryKey()

	// Verify all ordinals exist
	for i := int64(0); i <= 3; i++ {
		_, err := engine.strata.Chapter(ctx, key, i)
		if err != nil {
			t.Fatalf("expected chapter at ordinal %d: %v", i, err)
		}
	}

	// Now restart - should replay through cached results
	lease2 := getLeaseForJob(t, ctx, engine, jobKey)
	if lease2 != nil {
		r2 := &runner{
			jobId:        lease2.JobID(),
			tenantId:     jobKey.TenantId,
			worker:       jobWorker.workset,
			storyCounter: 1,
			engine:       engine,
			lease:        lease2,
			logger:       engine.logger,
			jobPolicy:    normalizeRunPolicy(swf.RunPolicy{}),
			capability:   lease2.NextNeed(),
			ctx:          ctx,
		}
		r2.DoJob(ctx, lease2)
	}

	// Should still have exactly the same ordinals, no duplicates
	for i := int64(0); i <= 3; i++ {
		_, err := engine.strata.Chapter(ctx, key, i)
		if err != nil {
			t.Fatalf("expected chapter at ordinal %d after restart: %v", i, err)
		}
	}

	// Ordinal 4 should NOT exist
	_, err = engine.strata.Chapter(ctx, key, 4)
	if !errors.Is(err, core.ErrNotFound) {
		t.Fatalf("expected no chapter at ordinal 4 (duplicate), got: %v", err)
	}
}

// Helper function to get a lease for a specific job
func getLeaseForJob(t *testing.T, ctx context.Context, engine *swfEngineImpl, jobKey swf.JobKey) *pgwf.Lease {
	t.Helper()

	// Query the job to find its current capability
	var capability string
	err := engine.udb.QueryRowContext(ctx,
		"SELECT next_need FROM pgwf.jobs WHERE job_id = $1",
		pgwf.JobID(jobKey.JobId)).Scan(&capability)
	if err != nil {
		return nil
	}

	lease, err := pgwf.GetWork(ctx, engine.udb, pgwf.WorkerID(engine.workerId), []pgwf.Capability{pgwf.Capability(capability)})
	if err != nil {
		t.Fatalf("get work: %v", err)
	}
	return lease
}

// Test job workers

type countingJobWorker struct {
	name     string
	counter  *atomic.Int32
	workset  *swf.WorkSet
}

func (w *countingJobWorker) Name() string { return w.name }

func (w *countingJobWorker) Run(ctx swf.JobContext, data swf.JobData) (swf.JobData, error) {
	w.counter.Add(1)

	input, err := data.GetData()
	if err != nil {
		return nil, err
	}

	// Add executed flag
	result := make(map[string]interface{})
	if err := json.Unmarshal(input, &result); err != nil {
		return nil, err
	}
	result["executed"] = true

	return swf.NewTaskDataOrPanic(result), nil
}

type failThenSucceedJobWorker struct {
	name         string
	failAttempts int
	counter      *atomic.Int32
	workset      *swf.WorkSet
}

func (w *failThenSucceedJobWorker) Name() string { return w.name }

func (w *failThenSucceedJobWorker) Run(ctx swf.JobContext, data swf.JobData) (swf.JobData, error) {
	attempt := int(w.counter.Add(1))

	if attempt <= w.failAttempts {
		return nil, swf.AppError{Payload: swf.AppErrorPayload{
			Message: fmt.Sprintf("retryable failure on attempt %d", attempt),
			Level:   "error",
		}}
	}

	input, _ := data.GetData()
	result := make(map[string]interface{})
	json.Unmarshal(input, &result)
	result["attempt"] = attempt

	return swf.NewTaskDataOrPanic(result), nil
}

type alwaysFailJobWorker struct {
	name    string
	counter *atomic.Int32
	workset *swf.WorkSet
}

func (w *alwaysFailJobWorker) Name() string { return w.name }

func (w *alwaysFailJobWorker) Run(ctx swf.JobContext, data swf.JobData) (swf.JobData, error) {
	w.counter.Add(1)
	return nil, swf.AppError{Payload: swf.AppErrorPayload{
		Message: "always fails",
		Level:   "error",
	}}
}

type nonRetryableErrorJobWorker struct {
	name    string
	counter *atomic.Int32
	workset *swf.WorkSet
}

func (w *nonRetryableErrorJobWorker) Name() string { return w.name }

func (w *nonRetryableErrorJobWorker) Run(ctx swf.JobContext, data swf.JobData) (swf.JobData, error) {
	w.counter.Add(1)
	// Return an error that implements NonRetryableError
	return nil, &customNonRetryableError{message: "this error is non-retryable"}
}

// customNonRetryableError implements swf.NonRetryableError interface
type customNonRetryableError struct {
	message string
}

func (e *customNonRetryableError) Error() string {
	return e.message
}

func (e *customNonRetryableError) NonRetryable() bool {
	return true
}

type taskCallingJobWorker struct {
	name    string
	counter *atomic.Int32
	workset *swf.WorkSet
}

func (w *taskCallingJobWorker) Name() string { return w.name }

func (w *taskCallingJobWorker) Run(ctx swf.JobContext, data swf.JobData) (swf.JobData, error) {
	w.counter.Add(1)

	// Call two tasks to create ordinals
	task1Input := swf.NewTaskDataOrPanic(map[string]string{"task": "1"})
	_, err := ctx.DoTask(swf.RunPolicy{}, "echo", task1Input)
	if err != nil {
		return nil, err
	}

	task2Input := swf.NewTaskDataOrPanic(map[string]string{"task": "2"})
	_, err = ctx.DoTask(swf.RunPolicy{}, "echo", task2Input)
	if err != nil {
		return nil, err
	}

	return data, nil
}

// Helper to initialize workset for a job worker
func initWorkset(job swf.JobWorker, tasks ...swf.TaskWorker) *swf.WorkSet {
	var ws *swf.WorkSet
	var err error
	if len(tasks) > 0 {
		ws, err = swf.AsWorkSet(job, tasks...)
	} else {
		ws, err = swf.AsWorkSet(job)
	}
	if err != nil {
		panic(err)
	}
	return ws
}

type echoTaskWorker struct{}

func (e *echoTaskWorker) Name() string { return "echo" }

func (e *echoTaskWorker) Run(ctx swf.TaskContext, data swf.TaskData) (swf.TaskData, error) {
	return data, nil
}
