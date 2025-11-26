package impl

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"runtime"
	"time"

	"github.com/colony-2/pgwf-go/pkg/pgwf"
	"github.com/colony-2/strata/strata-go/pkg/client/core"
	"github.com/colony-2/strata/strata-go/pkg/client/story"
	"github.com/colony-2/swf-go/pkg/swf"
)

type runner struct {
	jobId        pgwf.JobID
	worker       *swf.WorkSet
	storyCounter int64
	engine       *swfEngineImpl
	lease        *pgwf.Lease
	logger       *slog.Logger
}

func (r *runner) GetJobId() swf.JobId {
	return swf.JobId(r.jobId)
}

func (r *runner) DoTask(taskType string, data swf.TaskData) (swf.TaskData, error) {
	ordinal := r.storyCounter
	r.storyCounter++
	ctx := context.TODO()

	inputHash, err := computeInputHash(ctx, data)
	if err != nil {
		return nil, fmt.Errorf("compute input hash: %w", err)
	}

	chap, err := r.engine.strata.Chapter(ctx, story.Key{AnthologyID: r.engine.tenantId, StoryID: string(r.jobId)}, ordinal)

	if err == nil {
		env, decErr := decodeChapterEnvelope(chap.Body())
		if decErr != nil {
			return nil, fmt.Errorf("%w: decode cached chapter: %v", swf.ErrWorkflowNotDeterministic, decErr)
		}
		if env.Meta.InputHash == "" {
			return nil, fmt.Errorf("%w: ordinal %d task %s missing input hash", swf.ErrMissingInputHash, ordinal, taskType)
		}
		if env.Meta.InputHash != inputHash {
			return nil, fmt.Errorf("%w: ordinal %d task %s", swf.ErrWorkflowNotDeterministic, ordinal, taskType)
		}
		td, payloadErr := envelopeToTaskData(env, chap.Artifacts())
		return td, payloadErr
	}

	if !errors.Is(err, core.ErrNotFound) {
		return nil, fmt.Errorf("failed to get chapter %d: %w", ordinal, err)
	}

	worker, capabilityExistsLocally := r.worker.TaskWorkers[taskType]

	if !capabilityExistsLocally {
		inputOrdinal := ordinal - 1
		if inputOrdinal < 0 {
			inputOrdinal = 0
		}

		err = r.lease.Reschedule(context.TODO(), r.engine.udb, pgwf.JobDependencies{
			NextNeed: pgwf.Capability(r.worker.JobWorker.Name() + ":" + taskType),
			WaitFor:  nil,
		}, taskWait{
			InputStep:  inputOrdinal,
			OutputStep: ordinal,
			Next:       r.worker.JobWorker.Name(),
		})

		if err != nil {
			return nil, fmt.Errorf("failed to reschedule job: %w", err)
		}

		prematureCloseOut()
		return nil, nil
	}

	output, err := worker.Run(swf.TaskContext{
		JobId:  r.GetJobId(),
		Step:   r.storyCounter,
		Logger: r.logger.With("task", taskType, "step", r.storyCounter),
	}, data)

	payloadKind := payloadKindApp
	originalErr := err
	var payload json.RawMessage
	artifacts := []swf.Artifact{}
	if err != nil {
		var tdErr error
		payload, payloadKind, tdErr = errorPayloadFromError(err)
		if tdErr != nil {
			return nil, tdErr
		}
	} else {
		// success
		dataBytes, err := output.GetData()
		if err != nil {
			return nil, err
		}
		raw, err := dataBytes.ToBytes()
		if err != nil {
			return nil, err
		}
		payload = json.RawMessage(raw)
		artifacts, err = output.GetArtifacts()
		if err != nil {
			return nil, err
		}
	}

	chap, err = payloadToChapter(payload, artifacts, ordinal, taskType, r.engine.workerId, payloadKind, inputHash, time.Now().UTC())

	if err != nil {
		return nil, err
	}

	err = r.engine.strata.SaveChapter(context.TODO(), story.Key{
		AnthologyID: r.engine.tenantId,
		StoryID:     string(r.GetJobId()),
	}, chap)

	if err != nil {
		return nil, err
	}

	return output, originalErr

}

func prematureCloseOut() {
	// do any finalization
	runtime.Goexit()
}

var _ swf.JobContext = &runner{}

type RunError struct {
	Err error
}

func (r *runner) getChapter(ordinal int64) (story.Chapter, error) {
	return r.engine.strata.Chapter(context.TODO(), story.Key{AnthologyID: r.engine.tenantId, StoryID: string(r.jobId)}, ordinal)
}

func (r *runner) Logger() *slog.Logger {
	return r.logger
}

func (r *runner) Run(ctx context.Context, lease *pgwf.Lease) {
	_ = lease.WithKeepAlive(r.engine.udb)

	chap, err := r.getChapter(0)
	if err != nil {
		r.logger.Error("failed to get initial chapter", "error", err)
		return
	}
	inputData, err := chapterToTaskData(chap)
	if err != nil {
		r.logger.Error("failed to decode initial chapter", "error", err)
		return
	}
	output, jobErr := r.worker.JobWorker.Run(r, inputData)
	originalErr := jobErr
	if jobErr != nil {
		r.logger.Error("job worker run failed", "error", jobErr)
	}

	ordinal := r.storyCounter
	r.storyCounter++

	inputHash, err := computeInputHash(ctx, inputData)
	if err != nil {
		r.logger.Error("failed to hash job input", "error", err)
		return
	}

	payloadKind := payloadKindApp
	var payload json.RawMessage
	artifacts := []swf.Artifact{}
	if originalErr != nil {
		var appErr swf.AppError
		if errors.As(originalErr, &appErr) {
			raw, mErr := json.Marshal(appErr.Payload)
			if mErr != nil {
				r.logger.Error("failed to marshal app error payload", "error", mErr)
				return
			}
			payload = json.RawMessage(raw)
			payloadKind = payloadKindAppError
		} else {
			var sysErr swf.SystemError
			if errors.As(originalErr, &sysErr) {
				raw, mErr := json.Marshal(sysErr.Payload)
				if mErr != nil {
					r.logger.Error("failed to marshal system error payload", "error", mErr)
					return
				}
				payload = json.RawMessage(raw)
				payloadKind = payloadKindSystemError
			} else {
				raw, _ := json.Marshal(swf.SystemErrorPayload{Message: originalErr.Error()})
				payload = json.RawMessage(raw)
				payloadKind = payloadKindSystemError
			}
		}
	} else {
		if output == nil {
			raw, _ := json.Marshal(swf.SystemErrorPayload{Message: "missing job output"})
			payload = json.RawMessage(raw)
			payloadKind = payloadKindSystemError
		} else {
			dataBytes, err := output.GetData()
			if err != nil {
				r.logger.Error("failed to get job output data", "error", err)
				return
			}
			raw, err := dataBytes.ToBytes()
			if err != nil {
				r.logger.Error("failed to marshal job output", "error", err)
				return
			}
			payload = json.RawMessage(raw)
			artifacts, err = output.GetArtifacts()
			if err != nil {
				r.logger.Error("failed to get job output artifacts", "error", err)
				return
			}
		}
	}

	chap, err = payloadToChapter(payload, artifacts, ordinal, r.worker.JobWorker.Name(), r.engine.workerId, payloadKind, inputHash, time.Now().UTC())

	if err != nil {
		r.logger.Error("failed to build chapter", "error", err)
		return
	}

	err = r.engine.strata.SaveChapter(context.TODO(), story.Key{
		AnthologyID: r.engine.tenantId,
		StoryID:     string(r.GetJobId()),
	}, chap)

	if err != nil {
		r.logger.Error("failed to save chapter", "error", err)
	}

	err = lease.Complete(ctx, r.engine.udb)
	if err != nil {
		r.logger.Error("failed to complete lease", "error", err)
	}

	if originalErr != nil {
		return
	}
}
