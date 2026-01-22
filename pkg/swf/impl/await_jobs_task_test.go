package impl

import (
	"context"
	"testing"
	"time"

	"github.com/colony-2/swf-go/pkg/swf"
	"github.com/lib/pq"
	"github.com/segmentio/ksuid"
)

type taskAwaitJobsWorker struct {
	name     string
	childJob string
}

func (t taskAwaitJobsWorker) Name() string { return t.name }
func (t taskAwaitJobsWorker) Run(ctx swf.TaskContext, input swf.TaskData) (swf.TaskData, error) {
	if err := ctx.AwaitJobs(t.childJob); err != nil {
		return nil, err
	}
	return input, nil
}

type taskAwaitJobsParentWorker struct {
	name     string
	taskType string
}

func (t taskAwaitJobsParentWorker) Name() string { return t.name }
func (t taskAwaitJobsParentWorker) Run(ctx swf.JobContext, input swf.JobData) (swf.JobData, error) {
	return ctx.DoTask(swf.RunPolicy{}, t.taskType, input)
}

func TestTaskContextAwaitJobsReschedulesAndExits(t *testing.T) {
	ctx := context.Background()
	embedded, err := StartEmbeddedEngine(ctx, nil)
	if err != nil {
		t.Fatalf("start embedded engine: %v", err)
	}
	defer embedded.Shutdown()

	engine := embedded.SWFEngine.(*swfEngineImpl)
	tenantID := "test-tenant"
	childJobID := "child-" + ksuid.New().String()
	parentJobID := "parent-" + ksuid.New().String()

	_, err = engine.StartJob(ctx, swf.StartJob{
		TenantId: tenantID,
		JobType:  "child-worker",
		JobID:    childJobID,
		Data:     swf.NewTaskDataOrPanic(map[string]int{"n": 1}),
	})
	if err != nil {
		t.Fatalf("start child job: %v", err)
	}

	taskWorker := taskAwaitJobsWorker{name: "await-task", childJob: childJobID}
	jobWorker := taskAwaitJobsParentWorker{name: "await-parent", taskType: taskWorker.Name()}
	ws := initWorkset(jobWorker, taskWorker)

	jobKey, err := engine.StartJob(ctx, swf.StartJob{
		TenantId: tenantID,
		JobType:  jobWorker.Name(),
		JobID:    parentJobID,
		Data:     swf.NewTaskDataOrPanic(map[string]int{"n": 2}),
	})
	if err != nil {
		t.Fatalf("start parent job: %v", err)
	}

	lease := getLeaseForJob(t, ctx, engine, jobKey)
	if lease == nil {
		t.Fatalf("no lease available")
	}

	r := &runner{
		jobId:        lease.JobID(),
		tenantId:     jobKey.TenantId,
		worker:       ws,
		storyCounter: 1,
		engine:       engine,
		lease:        lease,
		logger:       engine.logger,
		jobPolicy:    normalizeRunPolicy(swf.RunPolicy{}),
		capability:   lease.NextNeed(),
		ctx:          ctx,
	}

	done := make(chan struct{})
	go func() {
		defer close(done)
		r.DoJob(ctx, lease)
	}()

	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatalf("runner did not exit")
	}

	var waitFor []string
	var nextNeed string
	if err := engine.udb.QueryRowContext(ctx, "select wait_for, next_need from pgwf.jobs where job_id = $1", parentJobID).Scan(pq.Array(&waitFor), &nextNeed); err != nil {
		t.Fatalf("query job: %v", err)
	}
	if nextNeed != jobWorker.Name() {
		t.Fatalf("expected next_need %s, got %s", jobWorker.Name(), nextNeed)
	}
	if len(waitFor) != 1 || waitFor[0] != childJobID {
		t.Fatalf("expected wait_for %s, got %v", childJobID, waitFor)
	}
}
