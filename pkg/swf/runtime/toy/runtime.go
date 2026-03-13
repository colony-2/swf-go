package toy

import (
	"log/slog"

	toyimpl "github.com/colony-2/swf-go/pkg/swf/runtime/toy/internal/toyimpl"
)

type Runtime = toyimpl.Runtime
type JobIDGenerator = toyimpl.JobIDGenerator
type Option = toyimpl.Option

func New(opts ...Option) *Runtime {
	return toyimpl.New(opts...)
}

func WithLogger(logger *slog.Logger) Option {
	return toyimpl.WithLogger(logger)
}

func WithJobIDGenerator(gen JobIDGenerator) Option {
	return toyimpl.WithJobIDGenerator(gen)
}
