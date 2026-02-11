package impl

import "github.com/colony-2/swf-go/pkg/swf"

type noopReattemptObserver struct{}

func (noopReattemptObserver) OnTaskReattemptBoundary(event swf.TaskReattemptBoundary) {}
func (noopReattemptObserver) OnJobReattemptBoundary(event swf.JobReattemptBoundary)  {}
