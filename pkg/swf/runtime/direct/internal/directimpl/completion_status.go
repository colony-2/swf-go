package directimpl

import (
	"context"
	"errors"

	"github.com/colony-2/pgwf-go/pkg/pgwf"
	"github.com/colony-2/swf-go/pkg/swf"
)

const (
	completionStatusSuccess       pgwf.CompletionStatus = "success"
	completionStatusFailedApp     pgwf.CompletionStatus = "failed_app"
	completionStatusFailedSystem  pgwf.CompletionStatus = "failed_system"
	completionStatusFailedTimeout pgwf.CompletionStatus = "failed_timeout"
	completionStatusCancelled     pgwf.CompletionStatus = "cancelled"
)

func completionStatusAndDetail(err error) (pgwf.CompletionStatus, string) {
	if err == nil {
		return completionStatusSuccess, ""
	}

	if errors.Is(err, swf.ErrJobCancelled) || errors.Is(err, context.Canceled) {
		return completionStatusCancelled, err.Error()
	}

	var te swf.TimeoutError
	if errors.As(err, &te) {
		return completionStatusFailedTimeout, messageOrFallback(te.Payload.Message, err)
	}

	var ae swf.AppError
	if errors.As(err, &ae) {
		return completionStatusFailedApp, messageOrFallback(ae.Payload.Message, err)
	}

	var se swf.SystemError
	if errors.As(err, &se) {
		return completionStatusFailedSystem, messageOrFallback(se.Payload.Message, err)
	}

	return completionStatusFailedSystem, messageOrFallback("", err)
}

func messageOrFallback(message string, err error) string {
	if message != "" {
		return message
	}
	if err != nil {
		return err.Error()
	}
	return ""
}
