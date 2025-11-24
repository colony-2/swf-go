package impl

import (
	"time"

	strata "github.com/colony-2/strata/strata-go/pkg/client/artifact"
	"github.com/colony-2/swf-go/pkg/swf"
)

type state struct {
	AppState   []byte
	Artifacts  []strata.Artifact
	Moment     time.Time
	RetryCount int
}

func (s state) asTaskData() swf.TaskData {
	return &swf.SimpleTaskData{
		Data:      swf.NewBytesData(s.AppState),
		Artifacts: s.Artifacts,
	}
}
