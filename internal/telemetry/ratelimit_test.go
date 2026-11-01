package telemetry

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

func rateLimitSamples(t *testing.T) []float64 {
	t.Helper()
	r := prometheus.NewRegistry()
	r.MustRegister(RateLimitRemaining)
	mfs, err := r.Gather()
	if err != nil {
		t.Fatal(err)
	}
	var out []float64
	for _, mf := range mfs {
		for _, m := range mf.GetMetric() {
			out = append(out, m.GetGauge().GetValue())
		}
	}
	return out
}

func TestRateLimitRemainingAbsentUntilReported(t *testing.T) {
	rateLimitRemaining.Store(-1)
	rateLimitLimit.Store(-1)
	t.Cleanup(func() { rateLimitRemaining.Store(-1); rateLimitLimit.Store(-1) })

	if got := rateLimitSamples(t); len(got) != 0 {
		t.Fatalf("samples before any header: %v, want none", got)
	}
	ObserveRateLimit(4321, 10000, time.Time{})
	if got := rateLimitSamples(t); len(got) != 1 || got[0] != 4321 {
		t.Fatalf("samples after header: %v, want [4321]", got)
	}
	ObserveRateLimit(0, -1, time.Time{})
	if got := rateLimitSamples(t); len(got) != 1 || got[0] != 0 {
		t.Fatalf("samples after exhausted budget: %v, want [0]", got)
	}
}
