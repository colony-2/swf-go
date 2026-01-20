package swf

import "sync/atomic"

var taskInputStorageEnabled atomic.Bool

func init() {
	taskInputStorageEnabled.Store(true)
}

// SetTaskInputStorageEnabled controls whether task inputs are stored in chapters.
// Enabled by default.
func SetTaskInputStorageEnabled(enabled bool) {
	taskInputStorageEnabled.Store(enabled)
}

// TaskInputStorageEnabled reports whether task input storage is enabled.
func TaskInputStorageEnabled() bool {
	return taskInputStorageEnabled.Load()
}
