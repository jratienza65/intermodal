package exporter

import (
	"testing"

	"github.com/jratienza65/intermodal/internal/config"
	"go.opentelemetry.io/otel/attribute"
)

func loadCfg(t *testing.T, env map[string]string) *config.Config {
	t.Helper()
	env["RAILWAY_API_TOKEN"] = "a"
	cfg, err := config.Load(func(k string) string { return env[k] })
	if err != nil {
		t.Fatalf("config.Load: %v", err)
	}
	return cfg
}

func TestSelfResourceCarriesIdentity(t *testing.T) {
	cfg := loadCfg(t, map[string]string{
		"RAILWAY_SERVICE_ID":   "s-1",
		"RAILWAY_PROJECT_NAME": "PSMND.DEV",
	})
	set := selfResource(cfg).Set()
	for _, want := range []attribute.KeyValue{
		attribute.String("service.name", "intermodal"),
		attribute.String("service.instance.id", "s-1"),
		attribute.String("railway.project.name", "PSMND.DEV"),
	} {
		got, ok := set.Value(want.Key)
		if !ok || got != want.Value {
			t.Errorf("%s = %v (present=%v), want %v", want.Key, got.Emit(), ok, want.Value.Emit())
		}
	}
}

// The bug this guards: every intermodal exported the same bare
// service.name=intermodal resource, so all of their self-metrics landed in one
// Prometheus series. Instances on different Railway services must now produce
// different resources.
func TestSelfResourceDiffersPerInstance(t *testing.T) {
	a := selfResource(loadCfg(t, map[string]string{
		"RAILWAY_SERVICE_ID":   "s-1",
		"RAILWAY_PROJECT_NAME": "PSMND.DEV",
	}))
	b := selfResource(loadCfg(t, map[string]string{
		"RAILWAY_SERVICE_ID":   "s-2",
		"RAILWAY_PROJECT_NAME": "PROD",
	}))
	if a.Equal(b) {
		t.Fatalf("two instances share one resource: %v", a.Attributes())
	}
}
