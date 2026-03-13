package directimpl

import "github.com/colony-2/pgwf-go/pkg/pgwf"

type keepAliveStopper interface {
	StopKeepAlive()
}

func stopLeaseKeepAlive(lease *pgwf.Lease) {
	if lease == nil {
		return
	}
	if stopper, ok := any(lease).(keepAliveStopper); ok {
		stopper.StopKeepAlive()
	}
}
