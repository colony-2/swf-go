package swf

import (
	"context"
	"errors"
	"fmt"
	"time"
)

type SWFEngine interface {
	jobRunApi
	taskRunApi
	loopWorkerApi
	jobsListApi

	RegisterWorkers(workset *WorkSet) error
}

func WaitForJobToComplete(ctx context.Context, timeout time.Duration, jobKey JobKey, engine SWFEngine) error {
	pollInterval := 200 * time.Millisecond
	pollCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-pollCtx.Done():
			if errors.Is(pollCtx.Err(), context.DeadlineExceeded) {
				return fmt.Errorf("job %s did not complete within the specified timeout of %s", jobKey, timeout)
			}
			return fmt.Errorf("polling for job %s stopped unexpectedly: %v", jobKey, pollCtx.Err())

		case <-ticker.C:
			// Time to check the status
			status, err := engine.CheckJobStatus(ctx, jobKey)
			if err != nil {
				return fmt.Errorf("failed to check status for job %s: %v", jobKey, err)
			}

			if status == JobStatusCompleted {
				return nil
			}

		}
	}
}
