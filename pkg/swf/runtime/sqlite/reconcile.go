package sqlite

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"

	"github.com/colony-2/strata-go/pkg/client/core"
	"github.com/colony-2/strata-go/pkg/client/story"
	"github.com/colony-2/swf-go/pkg/swf"
)

func (r *Runtime) reconcileExistingSubmitJob(ctx context.Context, req swf.SubmitJobRequest, jobKey swf.JobKey, inputHash string, prereqs []swf.JobPrerequisite, waitFor []string, jobPolicy swf.RunPolicy) (swf.JobHandle, bool, error) {
	start, exists, err := r.loadExistingStartChapter(ctx, jobKey)
	if err != nil {
		return swf.JobHandle{}, false, err
	}
	if !exists {
		if _, err := r.loadJobRow(ctx, jobKey); err == nil {
			return swf.JobHandle{}, true, swf.NewExistingJobMismatchError(fmt.Sprintf("job %s already exists in scheduler without a matching strata story", jobKey))
		}
		return swf.JobHandle{}, false, nil
	}
	if err := compareSubmitStartChapter(jobKey, start, req.Job.JobType, inputHash, req.Job.Metadata, prereqs, jobPolicy); err != nil {
		return swf.JobHandle{}, true, err
	}
	if err := r.ensureSubmittedJobRecord(ctx, jobKey, req.Job.JobType, req.Job.Metadata, waitFor, jobPayload{RunPolicy: jobPolicy}, req.WorkerID); err != nil {
		return swf.JobHandle{}, true, err
	}
	return swf.JobHandle{JobKey: jobKey}, true, nil
}

func (r *Runtime) reconcileExistingRestartJob(ctx context.Context, req swf.SubmitRestartJobRequest, jobKey swf.JobKey, prereqs []swf.JobPrerequisite, waitFor []string, jobType string, jobPolicy swf.RunPolicy, extra restartExtraExpectation) (swf.JobHandle, bool, error) {
	storyExists, err := r.storyExists(ctx, jobKey)
	if err != nil {
		return swf.JobHandle{}, false, err
	}
	if !storyExists {
		if _, err := r.loadJobRow(ctx, jobKey); err == nil {
			return swf.JobHandle{}, true, swf.NewExistingJobMismatchError(fmt.Sprintf("job %s already exists in scheduler without a matching strata story", jobKey))
		}
		return swf.JobHandle{}, false, nil
	}
	if err := r.compareRestartStoryPrefix(ctx, req.Job, jobKey, extra); err != nil {
		return swf.JobHandle{}, true, err
	}
	if err := r.ensureSubmittedJobRecord(ctx, jobKey, jobType, nil, waitFor, jobPayload{RunPolicy: jobPolicy}, req.WorkerID); err != nil {
		return swf.JobHandle{}, true, err
	}
	return swf.JobHandle{JobKey: jobKey}, true, nil
}

func (r *Runtime) loadExistingStartChapter(ctx context.Context, jobKey swf.JobKey) (story.Chapter, bool, error) {
	sctx := strataContext(ctx)
	chapter, err := r.strataClient.Chapter(sctx, storyKeyForJob(jobKey), 0)
	if err == nil {
		return chapter, true, nil
	}
	if !errors.Is(err, core.ErrNotFound) {
		return nil, false, err
	}
	if exists, storyErr := r.storyExists(ctx, jobKey); storyErr != nil {
		return nil, false, storyErr
	} else if exists {
		return nil, true, swf.NewExistingJobMismatchError(fmt.Sprintf("job %s already exists without a start chapter", jobKey))
	}
	return nil, false, nil
}

func (r *Runtime) storyExists(ctx context.Context, jobKey swf.JobKey) (bool, error) {
	_, err := r.strataClient.Story(strataContext(ctx), storyKeyForJob(jobKey))
	if err == nil {
		return true, nil
	}
	if errors.Is(err, core.ErrNotFound) {
		return false, nil
	}
	return false, err
}

func (r *Runtime) compareRestartStoryPrefix(ctx context.Context, job swf.SubmitRestartJob, targetJobKey swf.JobKey, extra restartExtraExpectation) error {
	sourceKey := storyKeyForJob(job.PriorJobKey)
	targetKey := storyKeyForJob(targetJobKey)
	sctx := strataContext(ctx)
	for ordinal := int64(0); ordinal <= job.LastStepToKeep; ordinal++ {
		sourceChapter, err := r.strataClient.Chapter(sctx, sourceKey, ordinal)
		if err != nil {
			if errors.Is(err, core.ErrNotFound) {
				return swf.NewExistingJobMismatchError(fmt.Sprintf("source job %s is missing chapter %d required for restart", job.PriorJobKey, ordinal))
			}
			return err
		}
		targetChapter, err := r.strataClient.Chapter(sctx, targetKey, ordinal)
		if err != nil {
			if errors.Is(err, core.ErrNotFound) {
				return swf.NewExistingJobMismatchError(fmt.Sprintf("job %s is missing copied restart chapter %d", targetJobKey, ordinal))
			}
			return err
		}
		same, compareErr := sameStoryChapter(sctx, sourceChapter, targetChapter)
		if compareErr != nil {
			return compareErr
		}
		if !same {
			return swf.NewExistingJobMismatchError(fmt.Sprintf("job %s already exists with different restart history at ordinal %d", targetJobKey, ordinal))
		}
	}
	nextOrdinal := job.LastStepToKeep + 1
	targetNext, err := r.strataClient.Chapter(sctx, targetKey, nextOrdinal)
	switch {
	case errors.Is(err, core.ErrNotFound):
		if extra.Present {
			return swf.NewExistingJobMismatchError(fmt.Sprintf("job %s is missing the restart extra chapter at ordinal %d", targetJobKey, nextOrdinal))
		}
		return nil
	case err != nil:
		return err
	}
	env, err := decodeChapterEnvelope(targetNext.Body())
	if err != nil {
		return swf.NewExistingJobMismatchError(fmt.Sprintf("job %s chapter %d could not be decoded: %v", targetJobKey, nextOrdinal, err))
	}
	if !extra.Present {
		if env.ChapterType == chapterTypeRestartExtra {
			return swf.NewExistingJobMismatchError(fmt.Sprintf("job %s already exists with restart extra output that was not requested", targetJobKey))
		}
		return nil
	}
	return compareRestartExtraChapter(sctx, targetJobKey, targetNext, extra)
}

func compareRestartExtraChapter(ctx context.Context, jobKey swf.JobKey, chapter story.Chapter, expected restartExtraExpectation) error {
	env, err := decodeChapterEnvelope(chapter.Body())
	if err != nil {
		return swf.NewExistingJobMismatchError(fmt.Sprintf("job %s restart extra chapter could not be decoded: %v", jobKey, err))
	}
	if env.ChapterType != chapterTypeRestartExtra {
		return swf.NewExistingJobMismatchError(fmt.Sprintf("job %s already exists with different restart chapter type", jobKey))
	}
	if env.PayloadKind != payloadKindApp {
		return swf.NewExistingJobMismatchError(fmt.Sprintf("job %s already exists with different restart payload kind", jobKey))
	}
	if env.Meta.TaskType != restartExtraTaskType {
		return swf.NewExistingJobMismatchError(fmt.Sprintf("job %s already exists with different restart task type", jobKey))
	}
	if env.Meta.InputHash != expected.InputHash {
		return swf.NewExistingJobMismatchError(fmt.Sprintf("job %s already exists with different restart extra input", jobKey))
	}
	if !reflect.DeepEqual(env.Meta.InputRef, expected.InputRef) {
		return swf.NewExistingJobMismatchError(fmt.Sprintf("job %s already exists with different restart extra input reference", jobKey))
	}
	if !reflect.DeepEqual(normalizePrereqSlice(env.Meta.Prerequisites), normalizePrereqSlice(expected.Prerequisites)) {
		return swf.NewExistingJobMismatchError(fmt.Sprintf("job %s already exists with different restart prerequisites", jobKey))
	}
	if !bytes.Equal(env.Payload, expected.Payload) {
		return swf.NewExistingJobMismatchError(fmt.Sprintf("job %s already exists with different restart extra output", jobKey))
	}
	gotArtifacts, err := storyChapterArtifacts(ctx, chapter)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(gotArtifacts, expected.Artifacts) {
		return swf.NewExistingJobMismatchError(fmt.Sprintf("job %s already exists with different restart extra artifacts", jobKey))
	}
	return nil
}

func storedChapterRunPolicy(chapter story.Chapter) (swf.RunPolicy, error) {
	env, err := decodeChapterEnvelope(chapter.Body())
	if err != nil {
		return swf.RunPolicy{}, err
	}
	if env.Meta.RunPolicy == nil {
		return swf.RunPolicy{}, nil
	}
	return *env.Meta.RunPolicy, nil
}
