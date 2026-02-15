package impl

import (
	"context"
	"fmt"

	"github.com/colony-2/pgwf-go/pkg/pgwf"
	"github.com/colony-2/swf-go/pkg/swf"
)

const prereqSuccessStatus pgwf.CompletionStatus = "success"

type prereqFailedError struct {
	app swf.AppError
}

func newPrereqFailedError(message string) error {
	return &prereqFailedError{
		app: swf.AppError{Payload: swf.AppErrorPayload{Message: message, Level: "error"}},
	}
}

func (e *prereqFailedError) Error() string {
	return e.app.Error()
}

func (e *prereqFailedError) NonRetryable() bool {
	return true
}

func (e *prereqFailedError) Unwrap() error {
	return e.app
}

func (s *swfEngineImpl) prerequisitesSucceeded(ctx context.Context, tenantId string, prereqs []swf.JobPrerequisite) error {
	if s == nil || len(prereqs) == 0 {
		return nil
	}
	successIDs := make([]pgwf.JobID, 0, len(prereqs))
	for _, p := range prereqs {
		if p.Condition == swf.JobPrereqSuccess {
			successIDs = append(successIDs, pgwf.JobID(p.JobID))
		}
	}
	if len(successIDs) == 0 {
		return nil
	}
	statuses, err := pgwf.GetJobStatusBatch(ctx, s.pgwfDB(ctx), pgwf.TenantID(tenantId), successIDs)
	if err != nil {
		return fmt.Errorf("check prerequisite statuses: %w", err)
	}
	for _, id := range successIDs {
		info := statuses[string(id)]
		if info == nil || info.ArchivedAt == nil {
			return newPrereqFailedError(fmt.Sprintf("prerequisite job %s not completed", id))
		}
		if info.CompletionStatus == nil || *info.CompletionStatus != prereqSuccessStatus {
			statusVal := "unknown"
			if info.CompletionStatus != nil {
				statusVal = string(*info.CompletionStatus)
			}
			detail := ""
			if info.CompletionDetail != nil {
				detail = *info.CompletionDetail
			}
			msg := fmt.Sprintf("prerequisite job %s did not succeed (status=%s)", id, statusVal)
			if detail != "" {
				msg = msg + ": " + detail
			}
			return newPrereqFailedError(msg)
		}
	}
	return nil
}
