// Package exporter defines the push metric-exporter interface and registry.
// Pull-based Prometheus is handled directly by the metrics collector + HTTP
// handler; this abstraction is for push exporters (OTLP today, remote_write
// etc. later). Adding one is a new file that implements Exporter and registers.
package exporter

import (
	"context"
	"fmt"
	"log/slog"
	"sort"

	"github.com/jratienza65/intermodal/internal/config"
	"github.com/jratienza65/intermodal/internal/metrics"
)

// Exporter pushes a metrics snapshot to a remote system. Export is called on a
// fixed interval with the latest snapshot; it should treat the snapshot as
// read-only.
type Exporter interface {
	Name() string
	Export(ctx context.Context, snap *metrics.Snapshot) error
	Close(ctx context.Context) error
}

// Options is passed to every exporter factory.
type Options struct {
	Config *config.Config
	Logger *slog.Logger
}

// Factory builds an exporter from Options.
type Factory func(Options) (Exporter, error)

var registry = map[string]Factory{}

// Register adds an exporter factory under name. Call from a package init.
func Register(name string, f Factory) {
	if _, exists := registry[name]; exists {
		panic("exporter: duplicate registration for " + name)
	}
	registry[name] = f
}

// Build constructs the named exporter.
func Build(name string, opts Options) (Exporter, error) {
	f, ok := registry[name]
	if !ok {
		return nil, fmt.Errorf("exporter: unknown exporter %q (registered: %v)", name, Registered())
	}
	if opts.Logger == nil {
		opts.Logger = slog.Default()
	}
	return f(opts)
}

// Registered returns the sorted list of registered exporter names.
func Registered() []string {
	names := make([]string, 0, len(registry))
	for n := range registry {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}
