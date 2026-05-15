package sqlite

import (
	"context"
	"net/http/httptest"
	"testing"

	"github.com/colony-2/swf-go/pkg/swf"
	remoteruntime "github.com/colony-2/swf-go/pkg/swf/runtime/remote"
)

func TestRuntimeSubmitJobCreatesInitialChapter(t *testing.T) {
	ctx := context.Background()
	embedded, err := StartEmbeddedRuntime(ctx)
	if err != nil {
		t.Fatalf("start runtime: %v", err)
	}
	defer embedded.Shutdown()

	handle, err := embedded.Runtime.SubmitJob(ctx, swf.SubmitJobRequest{
		Job: swf.SubmitJob{
			TenantId: "tenant",
			JobType:  "job",
			JobID:    "job-1",
			Data:     swf.NewTaskDataOrPanic(map[string]int{"n": 1}),
		},
	})
	if err != nil {
		t.Fatalf("submit job: %v", err)
	}
	chapter, err := embedded.Runtime.GetChapter(ctx, swf.ChapterRef{JobKey: handle.JobKey, Ordinal: 0})
	if err != nil {
		t.Fatalf("get chapter: %v", err)
	}
	if chapter.ChapterType != chapterTypeJobStart {
		t.Fatalf("chapter type = %s, want %s", chapter.ChapterType, chapterTypeJobStart)
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
	_, err = remote.SubmitJob(ctx, swf.SubmitJobRequest{
		Job: swf.SubmitJob{
			TenantId: "tenant",
			JobType:  "job",
			Data:     swf.NewTaskDataOrPanic(map[string]int{"n": 1}),
		},
	})
	if err != nil {
		t.Fatalf("remote submit job: %v", err)
	}
}
