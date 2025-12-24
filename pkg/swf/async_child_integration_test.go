package swf_test

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	strataclient "github.com/colony-2/strata-go/pkg/client"
	"github.com/colony-2/strata-go/pkg/client/story"
	"github.com/colony-2/swf-go/pkg/swf"
	"github.com/colony-2/swf-go/pkg/swf/impl"
)

// TestAsyncChildWorkflow spawns and awaits a child job using the shared await channel
// and pgwf wait_for dependency to ensure proper completion.
func TestAsyncChildWorkflow(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	postgresDSN, stopPG := startEmbeddedPostgres(t)
	defer stopPG()
	if err := installPGWF(ctx, postgresDSN); err != nil {
		t.Fatalf("failed to install pgwf: %v", err)
	}

	baseURL, strata := startStrata(t)
	defer strata.Shutdown()
	waitForStrataReady(t, baseURL)

	parentWorker := asyncParentJob{}
	childWorker := asyncChildJob{}

	builder := swf.NewEngineBuilder().
		WithPostgresDSN(postgresDSN).
		WithStrata(baseURL).
		WithStrataAPIKey(strata.APIKey).
		PlusWorkers(parentWorker)
	builder.PlusWorkers(childWorker)
	engine, err := builder.Build(impl.Builder)
	if err != nil {
		t.Fatalf("failed to build engine: %v", err)
	}

	go engine.Run(ctx)

	tenantID := "tenant-async-child"
	inputVal := 7
	input := swf.NewTaskDataOrPanic(map[string]interface{}{"n": inputVal})
	parentJobKey, err := engine.StartJob(ctx, swf.StartJob{
		TenantId: tenantID,
		JobType:  parentWorker.Name(),
		Data:     input,
	})
	if err != nil {
		t.Fatalf("failed to start parent job: %v", err)
	}

	client, err := strataclient.New(strataclient.Config{BaseURL: baseURL, APIKey: strata.APIKey})
	if err != nil {
		t.Fatalf("failed to create strata client: %v", err)
	}

	parentKey := story.Key{AnthologyID: tenantID, StoryID: parentJobKey.JobId}
	got := waitForChapterValue(t, client, parentKey, 2, 20*time.Second)
	if got != inputVal {
		t.Fatalf("parent output mismatch: want %d, got %d", inputVal, got)
	}

	childJobID := fmt.Sprintf("%s-%d", parentJobKey.JobId, 1)
	childKey := story.Key{AnthologyID: tenantID, StoryID: childJobID}
	childVal := waitForChapterValue(t, client, childKey, 1, 20*time.Second)
	if childVal != inputVal {
		t.Fatalf("child output mismatch: want %d, got %d", inputVal, childVal)
	}

	// Ensure child job is archived.
	db, err := sql.Open("postgres", postgresDSN)
	if err != nil {
		t.Fatalf("open postgres: %v", err)
	}
	defer db.Close()

	var archived int
	if err := db.QueryRow(`SELECT COUNT(*) FROM pgwf.jobs_archive WHERE job_id = $1`, childJobID).Scan(&archived); err != nil {
		t.Fatalf("count archived child: %v", err)
	}
	if archived == 0 {
		t.Fatalf("child job not archived")
	}
}

type asyncParentJob struct{}

func (asyncParentJob) Name() string { return "async_parent_job" }
func (asyncParentJob) Run(ctx swf.JobContext, input swf.JobData) (swf.JobData, error) {
	fut, err := ctx.SpawnAsync("async_child_job", input)
	if err != nil {
		return nil, err
	}
	out, err := fut.Await(context.Background())
	if err != nil {
		return nil, err
	}
	return out, nil
}

type asyncChildJob struct{}

func (asyncChildJob) Name() string { return "async_child_job" }
func (asyncChildJob) Run(_ swf.JobContext, input swf.JobData) (swf.JobData, error) {
	data, err := input.GetData()
	if err != nil {
		return nil, err
	}
	// Return the same payload to keep the flow simple.
	return &swf.SimpleTaskData{Data: data}, nil
}
