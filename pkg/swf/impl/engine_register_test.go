package impl

import (
	"context"
	"testing"
	"time"

	"github.com/colony-2/swf-go/pkg/swf"
)

type noopJobWorker struct{}

func (noopJobWorker) Name() string { return "noop-job" }

func (noopJobWorker) Run(ctx swf.JobContext, data swf.JobData) (swf.JobData, error) {
	return data, nil
}

// RegisterWorkers should update the capabilities monitored by the Run loop so newly added workers get leases.
func TestRegisterWorkersUpdatesCapabilities(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	embedded, err := StartEmbeddedEngine(ctx, nil)
	if err != nil {
		t.Fatalf("start embedded engine: %v", err)
	}
	defer embedded.Shutdown()

	engine := embedded.SWFEngine
	go engine.Run(ctx)

	ws, err := swf.AsWorkSet(noopJobWorker{})
	if err != nil {
		t.Fatalf("build workset: %v", err)
	}
	if err := engine.RegisterWorkers(ws); err != nil {
		t.Fatalf("register worker: %v", err)
	}

	input := swf.NewTaskDataOrPanic(map[string]string{"hello": "world"})
	jobKey, err := engine.StartJob(ctx, swf.StartJob{TenantId: "test-tenant", JobType: ws.JobWorker.Name(), Data: input})
	if err != nil {
		t.Fatalf("start job: %v", err)
	}

	if err := swf.WaitForJobToComplete(ctx, 10*time.Second, jobKey, engine); err != nil {
		t.Fatalf("job did not complete: %v", err)
	}

	result, err := engine.GetJobResult(ctx, jobKey)
	if err != nil {
		t.Fatalf("get job result: %v", err)
	}
	data, err := result.GetData()
	if err != nil {
		t.Fatalf("decode result: %v", err)
	}
	if string(data) != `{"hello":"world"}` {
		t.Fatalf("unexpected result payload: %s", string(data))
	}
}
