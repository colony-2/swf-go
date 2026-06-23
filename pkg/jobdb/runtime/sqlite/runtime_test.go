package sqlite

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/colony-2/jobdb/pkg/internal/runtimecodec"
	"github.com/colony-2/jobdb/pkg/jobdb"
	remoteruntime "github.com/colony-2/jobdb/pkg/jobdb/runtime/remote"
)

func TestRuntimeSubmitJobCreatesInitialChapter(t *testing.T) {
	ctx := context.Background()
	embedded, err := StartEmbeddedRuntime(ctx)
	if err != nil {
		t.Fatalf("start runtime: %v", err)
	}
	defer embedded.Shutdown()

	handle, err := embedded.Runtime.SubmitJob(ctx, jobdb.SubmitJobRequest{
		Job: jobdb.SubmitJob{
			TenantId: "tenant",
			JobType:  "job",
			JobID:    "job-1",
			Data:     jobdb.NewTaskDataOrPanic(map[string]int{"n": 1}),
		},
	})
	if err != nil {
		t.Fatalf("submit job: %v", err)
	}
	chapter, err := embedded.Runtime.GetChapter(ctx, jobdb.ChapterRef{JobKey: handle.JobKey, Ordinal: 0})
	if err != nil {
		t.Fatalf("get chapter: %v", err)
	}
	chapterType, err := runtimecodec.ChapterType(chapter)
	if err != nil {
		t.Fatalf("chapter type: %v", err)
	}
	if chapterType != chapterTypeJobStart {
		t.Fatalf("chapter type = %s, want %s", chapterType, chapterTypeJobStart)
	}
}

func TestRuntimeThroughRemoteServerSubmitJob(t *testing.T) {
	ctx := context.Background()
	embedded, err := StartEmbeddedRuntime(ctx)
	if err != nil {
		t.Fatalf("start runtime: %v", err)
	}
	defer embedded.Shutdown()

	server := httptest.NewServer(remoteruntime.NewServer(embedded.Runtime))
	defer server.Close()
	remote, err := remoteruntime.New(server.URL, server.Client())
	if err != nil {
		t.Fatalf("remote runtime: %v", err)
	}
	_, err = remote.SubmitJob(ctx, jobdb.SubmitJobRequest{
		Job: jobdb.SubmitJob{
			TenantId: "tenant",
			JobType:  "job",
			Data:     jobdb.NewTaskDataOrPanic(map[string]int{"n": 1}),
		},
	})
	if err != nil {
		t.Fatalf("remote submit job: %v", err)
	}
}
