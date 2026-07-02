package metrics

import (
	"strings"
	"testing"

	"github.com/jratienza65/intermodal/internal/railway"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

// labelsA and labelsB are two distinct fully-populated label sets used across
// the collector tests. Keeping them here makes the expected exposition below
// easy to line up with the samples that produced it.
var (
	labelsA = Labels{
		ProjectID:            "proj-1",
		ProjectName:          "web",
		EnvironmentID:        "env-1",
		EnvironmentName:      "production",
		ServiceID:            "svc-1",
		ServiceName:          "api",
		Region:               "us-west1",
		DeploymentID:         "dep-1",
		DeploymentInstanceID: "inst-1",
	}
	labelsB = Labels{
		ProjectID:            "proj-1",
		ProjectName:          "web",
		EnvironmentID:        "env-1",
		EnvironmentName:      "production",
		ServiceID:            "svc-2",
		ServiceName:          "worker",
		Region:               "us-west1",
		DeploymentID:         "dep-2",
		DeploymentInstanceID: "inst-2",
	}
)

func sample(m railway.MetricMeasurement, l Labels, v float64) Sample {
	return Sample{Measurement: m, Labels: l, Value: v}
}

// newTestCollector returns a Collector whose store already holds snap.
func newTestCollector(snap *Snapshot) *Collector {
	store := NewStore()
	if snap != nil {
		store.Set(snap)
	}
	return NewCollector(store)
}

// TestCollectorExposition asserts the end-to-end exposition of a snapshot:
// GB->bytes conversion and the network counter's _total type. Utilization is a
// query-time concern, so no ratio metrics are exposed.
func TestCollectorExposition(t *testing.T) {
	snap := &Snapshot{Samples: []Sample{
		sample(railway.MeasurementCPUUsage, labelsA, 0.5),
		sample(railway.MeasurementCPULimit, labelsA, 2.0),
		sample(railway.MeasurementMemoryUsageGB, labelsA, 1.5), // -> 1.5e9 bytes
		sample(railway.MeasurementMemoryLimitGB, labelsA, 3.0), // -> 3.0e9 bytes
		sample(railway.MeasurementNetworkRxGB, labelsA, 2.0),   // -> 2.0e9 bytes, counter
		sample(railway.MeasurementCPUUsage, labelsB, 1.0),
		sample(railway.MeasurementCPULimit, labelsB, 4.0),
	}}
	c := newTestCollector(snap)

	// Register in a fresh registry so Gather runs the full validation path
	// (this is what catches duplicate/invalid series).
	reg := prometheus.NewRegistry()
	if err := reg.Register(c); err != nil {
		t.Fatalf("register collector: %v", err)
	}

	const lblA = `deployment_id="dep-1",deployment_instance_id="inst-1",environment_id="env-1",environment_name="production",project_id="proj-1",project_name="web",region="us-west1",service_id="svc-1",service_name="api"`
	const lblB = `deployment_id="dep-2",deployment_instance_id="inst-2",environment_id="env-1",environment_name="production",project_id="proj-1",project_name="web",region="us-west1",service_id="svc-2",service_name="worker"`

	expected := strings.Join([]string{
		`# HELP railway_service_cpu_usage_cores Current CPU usage in vCPU cores.`,
		`# TYPE railway_service_cpu_usage_cores gauge`,
		`railway_service_cpu_usage_cores{` + lblA + `} 0.5`,
		`railway_service_cpu_usage_cores{` + lblB + `} 1`,
		`# HELP railway_service_cpu_limit_cores Allocated CPU limit in vCPU cores.`,
		`# TYPE railway_service_cpu_limit_cores gauge`,
		`railway_service_cpu_limit_cores{` + lblA + `} 2`,
		`railway_service_cpu_limit_cores{` + lblB + `} 4`,
		`# HELP railway_service_memory_usage_bytes Current memory usage in bytes.`,
		`# TYPE railway_service_memory_usage_bytes gauge`,
		`railway_service_memory_usage_bytes{` + lblA + `} 1.5e+09`,
		`# HELP railway_service_memory_limit_bytes Allocated memory limit in bytes.`,
		`# TYPE railway_service_memory_limit_bytes gauge`,
		`railway_service_memory_limit_bytes{` + lblA + `} 3e+09`,
		`# HELP railway_service_network_receive_bytes_total Cumulative network bytes received (ingress).`,
		`# TYPE railway_service_network_receive_bytes_total counter`,
		`railway_service_network_receive_bytes_total{` + lblA + `} 2e+09`,
		"",
	}, "\n")

	if err := testutil.GatherAndCompare(reg, strings.NewReader(expected),
		"railway_service_cpu_usage_cores",
		"railway_service_cpu_limit_cores",
		"railway_service_memory_usage_bytes",
		"railway_service_memory_limit_bytes",
		"railway_service_network_receive_bytes_total",
	); err != nil {
		t.Fatalf("exposition mismatch: %v", err)
	}
}

// TestCollectorGBToBytes isolates the GB->bytes conversion factor (1e9) for a
// single memory-usage sample.
func TestCollectorGBToBytes(t *testing.T) {
	snap := &Snapshot{Samples: []Sample{
		sample(railway.MeasurementMemoryUsageGB, labelsA, 2.5),
		sample(railway.MeasurementNetworkRxGB, labelsA, 0.125),
	}}
	c := newTestCollector(snap)

	expected := `
# HELP railway_service_memory_usage_bytes Current memory usage in bytes.
# TYPE railway_service_memory_usage_bytes gauge
railway_service_memory_usage_bytes{deployment_id="dep-1",deployment_instance_id="inst-1",environment_id="env-1",environment_name="production",project_id="proj-1",project_name="web",region="us-west1",service_id="svc-1",service_name="api"} 2.5e+09
# HELP railway_service_network_receive_bytes_total Cumulative network bytes received (ingress).
# TYPE railway_service_network_receive_bytes_total counter
railway_service_network_receive_bytes_total{deployment_id="dep-1",deployment_instance_id="inst-1",environment_id="env-1",environment_name="production",project_id="proj-1",project_name="web",region="us-west1",service_id="svc-1",service_name="api"} 1.25e+08
`
	if err := testutil.CollectAndCompare(c, strings.NewReader(expected),
		"railway_service_memory_usage_bytes",
		"railway_service_network_receive_bytes_total",
	); err != nil {
		t.Fatalf("mismatch: %v", err)
	}
}

// TestCollectorDeduplicates verifies that duplicate series (same metric name +
// label set) are collapsed to a single value so registry Gather never fails
// with a "duplicate metrics collected" error. The first sample wins.
func TestCollectorDeduplicates(t *testing.T) {
	snap := &Snapshot{Samples: []Sample{
		sample(railway.MeasurementCPUUsage, labelsA, 0.5),
		sample(railway.MeasurementCPUUsage, labelsA, 0.9), // duplicate series
		sample(railway.MeasurementCPUUsage, labelsA, 0.7), // duplicate series
	}}
	c := newTestCollector(snap)

	reg := prometheus.NewRegistry()
	if err := reg.Register(c); err != nil {
		t.Fatalf("register: %v", err)
	}
	// Gather must not error despite the duplicate samples.
	mfs, err := reg.Gather()
	if err != nil {
		t.Fatalf("gather returned error on duplicate series: %v", err)
	}

	var count int
	for _, mf := range mfs {
		if mf.GetName() == "railway_service_cpu_usage_cores" {
			count = len(mf.GetMetric())
			for _, m := range mf.GetMetric() {
				if got := m.GetGauge().GetValue(); got != 0.5 {
					t.Errorf("dedup kept wrong value: got %v, want 0.5 (first sample)", got)
				}
			}
		}
	}
	if count != 1 {
		t.Fatalf("expected exactly 1 de-duplicated series, got %d", count)
	}

	// CollectAndCount is another way to assert the single surviving series.
	if n := testutil.CollectAndCount(c, "railway_service_cpu_usage_cores"); n != 1 {
		t.Fatalf("CollectAndCount = %d, want 1", n)
	}
}

// TestCollectorToFloat64 uses ToFloat64 on a snapshot that yields exactly one
// series (a single network counter) to spot-check the GB->bytes value.
func TestCollectorToFloat64(t *testing.T) {
	snap := &Snapshot{Samples: []Sample{
		sample(railway.MeasurementNetworkRxGB, labelsA, 3.0), // -> 3e9 bytes
	}}
	c := newTestCollector(snap)
	if got := testutil.ToFloat64(c); got != 3e9 {
		t.Fatalf("ToFloat64 = %v, want 3e9", got)
	}
}

// TestCollectorEmptyStore verifies a collector over a store with no snapshot
// emits nothing and does not panic.
func TestCollectorEmptyStore(t *testing.T) {
	c := newTestCollector(nil)
	if n := testutil.CollectAndCount(c); n != 0 {
		t.Fatalf("expected no metrics from empty store, got %d", n)
	}
}
