// Package sink defines the log-destination interface and a name→factory
// registry. Adding a new backend is one new file that implements Sink and
// registers a factory in its init — no changes to the core.
package sink

import (
	"context"
	"fmt"
	"log/slog"
	"sort"

	"github.com/jratienza65/intermodal/internal/config"
	"github.com/jratienza65/intermodal/internal/model"
)

// Sink is a log destination. Write must be safe to call from the pipeline's
// per-sink worker goroutine (a single goroutine per sink), and should treat the
// batch as read-only.
type Sink interface {
	Name() string
	Write(ctx context.Context, batch []model.LogRecord) error
	Close(ctx context.Context) error
}

// Options is passed to every sink factory. A factory reads whatever it needs
// from Config (typed fields), keeping construction decoupled from parsing.
type Options struct {
	Config *config.Config
	Logger *slog.Logger
}

// Factory builds a sink from Options.
type Factory func(Options) (Sink, error)

var registry = map[string]Factory{}

// Register adds a sink factory under name. Call from a package init.
func Register(name string, f Factory) {
	if _, exists := registry[name]; exists {
		panic("sink: duplicate registration for " + name)
	}
	registry[name] = f
}

// Build constructs the named sink.
func Build(name string, opts Options) (Sink, error) {
	f, ok := registry[name]
	if !ok {
		return nil, fmt.Errorf("sink: unknown sink %q (registered: %v)", name, Registered())
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	return f(opts)
}

// Registered returns the sorted list of registered sink names.
func Registered() []string {
	names := make([]string, 0, len(registry))
	for n := range registry {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}
