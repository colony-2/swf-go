package impl

import (
	"context"
	"errors"
	"log/slog"
	"time"

	"github.com/colony-2/strata-go/pkg/client/core"
	"github.com/colony-2/strata-go/pkg/client/story"
	"github.com/colony-2/swf-go/pkg/swf"
	swfinternal "github.com/colony-2/swf-go/pkg/swf/internal"
)

type replayRunnerBackend struct {
	engine *swfEngineImpl
}

func (b *replayRunnerBackend) GetChapter(ctx context.Context, key story.Key, ordinal int64) (story.Chapter, error) {
	chap, err := b.engine.strata.Chapter(ctx, key, ordinal)
	if errors.Is(err, core.ErrNotFound) {
		return nil, swf.ReplayCacheMissError{
			JobKey:   swf.JobKey{TenantId: key.AnthologyID, JobId: key.StoryID},
			Ordinal:  ordinal,
			Attempt:  1,
			Reason:   swf.ReplayCacheMissTaskResultMissing,
		}
	}
	return chap, err
}

func (b *replayRunnerBackend) SaveChapter(ctx context.Context, key story.Key, chap story.Chapter) error {
	return swf.ErrReplayShouldNeverMutate
}

func (b *replayRunnerBackend) GetJobAttemptOutcome(ctx context.Context, key story.Key, ordinal int64) (story.Chapter, error) {
	chap, err := b.engine.strata.Chapter(ctx, key, ordinal)
	if errors.Is(err, core.ErrNotFound) {
		return nil, swf.ReplayCacheMissError{
			JobKey:   swf.JobKey{TenantId: key.AnthologyID, JobId: key.StoryID},
			Ordinal:  ordinal,
			Attempt:  1,
			Reason:   swf.ReplayCacheMissJobResultMissing,
		}
	}
	return chap, err
}

func (b *replayRunnerBackend) AwaitUntil(ctx context.Context, wakeAt time.Time, info swfinternal.AwaitInfo) error {
	if wakeAt.IsZero() || time.Now().After(wakeAt) {
		return nil
	}
	return swf.ReplayCacheMissError{
		JobKey:   info.JobKey,
		TaskType: info.TaskType,
		Ordinal:  info.Ordinal,
		Attempt:  info.Attempt,
		Reason:   swf.ReplayCacheMissAwaitNotReady,
	}
}

func (b *replayRunnerBackend) AwaitJobs(ctx context.Context, jobIds []string, info swfinternal.AwaitInfo) (bool, error) {
	if len(jobIds) == 0 {
		return false, nil
	}
	// If any job is not complete, return cache miss.
	for _, id := range jobIds {
		if id == "" {
			return false, swf.ReplayCacheMissError{
				JobKey:   info.JobKey,
				TaskType: info.TaskType,
				Ordinal:  info.Ordinal,
				Attempt:  info.Attempt,
				Reason:   swf.ReplayCacheMissAwaitJobsPending,
			}
		}
		status, err := b.engine.CheckJobStatus(ctx, swf.JobKey{TenantId: info.JobKey.TenantId, JobId: id})
		if err != nil {
			return false, err
		}
		if status != swf.JobStatusCompleted && status != swf.JobStatusCancelled {
			return false, swf.ReplayCacheMissError{
				JobKey:   info.JobKey,
				TaskType: info.TaskType,
				Ordinal:  info.Ordinal,
				Attempt:  info.Attempt,
				Reason:   swf.ReplayCacheMissAwaitJobsPending,
			}
		}
	}
	return false, nil
}

func (b *replayRunnerBackend) AfterSaveTaskOutput(output swf.TaskData, dataBytes swf.Data, artifacts []swf.Artifact, digests []string, key story.Key, ordinal int64, logger *slog.Logger) (swf.TaskData, error) {
	return nil, swf.ErrReplayShouldNeverMutate
}
