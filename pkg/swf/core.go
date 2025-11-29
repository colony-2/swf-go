package swf

type SWFEngine interface {
	jobRunApi
	taskRunApi
	loopWorkerApi

	RegisterWorkers(job JobWorker, tasks ...TaskWorker) error
}
