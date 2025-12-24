package swf

import (
	"context"
	"fmt"
)

// Future represents an async child job handle.
// The await function is injected by the runner to integrate with engine-directed waits.
type Future struct {
	JobKey JobKey
	await  func(context.Context) (TaskData, error)
}

// Await waits for the async child job to complete and returns its output.
func (f *Future) Await(ctx context.Context) (TaskData, error) {
	if f == nil {
		return nil, fmt.Errorf("future is nil")
	}
	if f.await == nil {
		return nil, fmt.Errorf("future await not configured")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return f.await(ctx)
}

// NewFuture builds a Future with the provided await hook.
func NewFuture(jobKey JobKey, await func(context.Context) (TaskData, error)) *Future {
	return &Future{
		JobKey: jobKey,
		await:  await,
	}
}
