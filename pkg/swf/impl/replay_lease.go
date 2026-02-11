package impl

import (
	"context"

	"github.com/colony-2/pgwf-go/pkg/pgwf"
	"github.com/colony-2/swf-go/pkg/swf"
)

type replayLease struct{}

func (replayLease) KeepAlive(ctx context.Context) error { return nil }
func (replayLease) StopKeepAlive()                      {}
func (replayLease) Complete(ctx context.Context) error  { return nil }
func (replayLease) Reschedule(ctx context.Context, deps pgwf.JobDependencies, payload any) error {
	return swf.ErrReplayShouldNeverMutate
}
func (replayLease) NextNeed() pgwf.Capability { return "" }
func (replayLease) Payload() []byte           { return nil }
