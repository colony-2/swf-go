package workflow

import "context"

type loopWorkerApi interface {

	// Run loop worker. Starts up a coroutine pool where up to maxConcurrentTasks tasks can run concurrently.
	Run(ctx context.Context)
}
