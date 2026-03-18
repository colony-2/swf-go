package swf

import "fmt"

// GetOutput returns the final job output or a job-level error.
func (r GetJobRunResponse) GetOutput(engine SWFEngine, tenantId string) (JobData, error) {
	if r.Job.Status == JobStatusCancelled {
		return nil, ErrJobCancelled
	}
	if r.Job.Status != JobStatusCompleted {
		return nil, ErrJobNotComplete
	}
	if len(r.Attempts) == 0 {
		return nil, ErrJobNotComplete
	}
	latest := latestJobAttempt(r.Attempts)
	if latest.Outcome.Status == TaskOutcomeStatusFailed || latest.Outcome.Error != nil {
		return nil, jobFailedErrorFromOutcome(latest.Outcome)
	}
	if latest.Output == nil {
		return nil, JobFailedError{Cause: AppError{Payload: AppErrorPayload{
			Message: "missing output",
			Level:   "error",
		}}}
	}

	data := append([]byte(nil), latest.Output.Data...)
	artifacts, err := artifactsFromInfos(latest.Output.Artifacts, engine, tenantId, r.Job.JobKey, latest.Ordinal)
	if err != nil {
		return nil, err
	}
	return &SimpleTaskData{Data: data, Artifacts: artifacts}, nil
}

func latestJobAttempt(attempts []JobAttempt) JobAttempt {
	best := attempts[0]
	for i := 1; i < len(attempts); i++ {
		attempt := attempts[i]
		if attempt.Attempt > best.Attempt || (attempt.Attempt == best.Attempt && attempt.Ordinal > best.Ordinal) {
			best = attempt
		}
	}
	return best
}

func artifactsFromInfos(infos []ArtifactInfo, engine SWFEngine, tenantId string, jobKey JobKey, ordinal int64) ([]Artifact, error) {
	if len(infos) == 0 {
		return nil, nil
	}
	artifacts := make([]Artifact, 0, len(infos))
	for _, info := range infos {
		var key ArtifactKey
		if info.Key != nil {
			key = *info.Key
		} else {
			key = ArtifactKey{
				JobId:       jobKey.JobId,
				TaskOrdinal: ordinal,
				Name:        info.Name,
				SizeBytes:   info.SizeBytes,
			}
		}
		if err := key.Validate(); err != nil {
			return nil, fmt.Errorf("%w: invalid artifact key: %v", ErrJobFailed, err)
		}
		artifacts = append(artifacts, key.ToLazyArtifact(engine, tenantId))
	}
	return artifacts, nil
}
