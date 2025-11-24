package swf

type SWFEngine interface {
	jobRunApi
	taskRunApi
	loopWorkerApi
}
