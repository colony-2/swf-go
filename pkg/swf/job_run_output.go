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
	attempt := r.Result
	if attempt == nil && len(r.JobAttempts) > 0 {
		latest := latestJobAttempt(r.JobAttempts)
		attempt = &latest
	}
	if attempt == nil {
		return nil, ErrJobNotComplete
	}
	if attempt.Outcome.Status == TaskOutcomeStatusFailed || attempt.Outcome.Error != nil {
		if attempt.Outcome.Error != nil && attempt.Outcome.Error.Message != "" {
			return nil, fmt.Errorf("%w: %s", ErrJobFailed, attempt.Outcome.Error.Message)
		}
		return nil, ErrJobFailed
	}
	if attempt.Output == nil {
		return nil, fmt.Errorf("%w: missing output", ErrJobFailed)
	}

	data := append([]byte(nil), attempt.Output.Data...)
	artifacts, err := artifactsFromInfos(attempt.Output.Artifacts, engine, tenantId, r.Job.JobKey, attempt.Ordinal)
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
