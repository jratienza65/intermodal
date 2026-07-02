package exporter

import (
	"testing"
	"time"

	"github.com/jratienza65/intermodal/internal/config"
	"github.com/jratienza65/intermodal/internal/metrics"
	"github.com/jratienza65/intermodal/internal/railway"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// newTestOTLP builds an otlpExporter against a dummy insecure endpoint. The
// OTLP/HTTP exporter is lazy about connections, so New succeeds without any
// live endpoint; we only ever exercise buildResourceMetrics, never Export.
func newTestOTLP(t *testing.T) *otlpExporter {
	t.Helper()
	// Mirrors INTERMODAL_OTLP_ENDPOINT + INTERMODAL_OTLP_INSECURE=true.
	cfg := &config.Config{
		OTLPEndpoint: "localhost:4318",
		OTLPInsecure: true,
		ServiceName:  "intermodal-test",
	}
	exp, err := newOTLP(Options{Config: cfg})
	if err != nil {
		t.Fatalf("newOTLP: %v", err)
	}
	oe, ok := exp.(*otlpExporter)
	if !ok {
		t.Fatalf("newOTLP returned %T, want *otlpExporter", exp)
	}
	return oe
}

func TestBuildResourceMetrics(t *testing.T) {
	e := newTestOTLP(t)

	ts := time.Date(2026, 7, 2, 12, 0, 0, 0, time.UTC)
	labels := metrics.Labels{
		ServiceID:   "svc-1",
		ServiceName: "api",
		// EnvironmentName etc. left empty on purpose to test dropping.
	}
	snap := &metrics.Snapshot{
		Time: ts,
		Samples: []metrics.Sample{
			{Measurement: railway.MeasurementCPUUsage, Labels: labels, Value: 0.5, Timestamp: ts},
			{Measurement: railway.MeasurementNetworkRxGB, Labels: labels, Value: 2, Timestamp: ts},
			// Exact duplicate of the CPU sample (same measurement + attrs): must
			// be de-duplicated.
			{Measurement: railway.MeasurementCPUUsage, Labels: labels, Value: 0.5, Timestamp: ts},
		},
	}

	rm := e.buildResourceMetrics(snap)

	if got := len(rm.ScopeMetrics); got != 1 {
		t.Fatalf("ScopeMetrics len = %d, want 1", got)
	}
	sm := rm.ScopeMetrics[0]
	if sm.Scope.Name != "intermodal" {
		t.Errorf("scope name = %q, want %q", sm.Scope.Name, "intermodal")
	}
	if got := len(sm.Metrics); got != 2 {
		t.Fatalf("Metrics len = %d, want 2", got)
	}

	byName := map[string]metricdata.Metrics{}
	for _, m := range sm.Metrics {
		byName[m.Name] = m
	}

	// --- CPU_USAGE: gauge, factor 1, de-duplicated to a single datapoint. ---
	cpuName := metrics.Measurements[railway.MeasurementCPUUsage].Name
	cpu, ok := byName[cpuName]
	if !ok {
		t.Fatalf("missing metric %q; got %v keys", cpuName, byName)
	}
	gauge, ok := cpu.Data.(metricdata.Gauge[float64])
	if !ok {
		t.Fatalf("CPU metric data type = %T, want Gauge[float64]", cpu.Data)
	}
	if got := len(gauge.DataPoints); got != 1 {
		t.Fatalf("CPU datapoints = %d, want 1 (duplicate not de-duplicated)", got)
	}
	if got := gauge.DataPoints[0].Value; got != 0.5 {
		t.Errorf("CPU value = %v, want 0.5 (factor 1)", got)
	}

	// --- NETWORK_RX_GB: monotonic cumulative sum, factor 1e9. ---
	rxName := metrics.Measurements[railway.MeasurementNetworkRxGB].Name
	rx, ok := byName[rxName]
	if !ok {
		t.Fatalf("missing metric %q; got %v keys", rxName, byName)
	}
	sum, ok := rx.Data.(metricdata.Sum[float64])
	if !ok {
		t.Fatalf("network metric data type = %T, want Sum[float64]", rx.Data)
	}
	if !sum.IsMonotonic {
		t.Error("network Sum.IsMonotonic = false, want true")
	}
	if sum.Temporality != metricdata.CumulativeTemporality {
		t.Errorf("network Sum.Temporality = %v, want Cumulative", sum.Temporality)
	}
	if got := len(sum.DataPoints); got != 1 {
		t.Fatalf("network datapoints = %d, want 1", got)
	}
	if got := sum.DataPoints[0].Value; got != 2e9 {
		t.Errorf("network value = %v, want 2e9 (factor 1e9)", got)
	}
	if !sum.DataPoints[0].StartTime.Equal(e.start) {
		t.Errorf("network StartTime = %v, want exporter start %v", sum.DataPoints[0].StartTime, e.start)
	}
	if !sum.DataPoints[0].Time.Equal(ts) {
		t.Errorf("network Time = %v, want %v", sum.DataPoints[0].Time, ts)
	}

	// --- Attribute set: only the non-empty labels survive. ---
	attrs := gauge.DataPoints[0].Attributes
	if sv, ok := attrs.Value("service_id"); !ok || sv.AsString() != "svc-1" {
		t.Errorf("service_id attr = (%v,%v), want svc-1", sv.AsString(), ok)
	}
	if sv, ok := attrs.Value("service_name"); !ok || sv.AsString() != "api" {
		t.Errorf("service_name attr = (%v,%v), want api", sv.AsString(), ok)
	}
	if _, ok := attrs.Value("environment_name"); ok {
		t.Error("empty environment_name label should be dropped from attribute set")
	}
	if got := attrs.Len(); got != 2 {
		t.Errorf("attribute set len = %d, want 2 (non-empty labels only)", got)
	}
}

func TestBuildResourceMetricsIgnoresUnknownMeasurement(t *testing.T) {
	e := newTestOTLP(t)
	snap := &metrics.Snapshot{
		Samples: []metrics.Sample{
			{Measurement: railway.MetricMeasurement("NOT_A_REAL_MEASUREMENT"), Value: 1},
		},
	}
	rm := e.buildResourceMetrics(snap)
	if len(rm.ScopeMetrics[0].Metrics) != 0 {
		t.Errorf("unknown measurement produced metrics: %+v", rm.ScopeMetrics[0].Metrics)
	}
}

func TestAttrSetDropsEmptyLabels(t *testing.T) {
	set := attrSet(metrics.Labels{ServiceID: "svc-1", Region: "us-west"})
	if got := set.Len(); got != 2 {
		t.Fatalf("attrSet len = %d, want 2", got)
	}
	for _, kv := range set.ToSlice() {
		if kv.Value.AsString() == "" {
			t.Errorf("attrSet retained empty-valued key %q", kv.Key)
		}
	}
	if _, ok := set.Value(attribute.Key("project_id")); ok {
		t.Error("empty project_id should not be present")
	}
	if sv, ok := set.Value(attribute.Key("region")); !ok || sv.AsString() != "us-west" {
		t.Errorf("region = (%v,%v), want us-west", sv.AsString(), ok)
	}

	empty := attrSet(metrics.Labels{})
	if empty.Len() != 0 {
		t.Error("all-empty labels should yield an empty attribute set")
	}
}

func TestRegisteredIncludesOTLP(t *testing.T) {
	found := false
	for _, n := range Registered() {
		if n == "otlp" {
			found = true
			break
		}
	}
	if !found {
		t.Errorf("Registered() = %v, missing %q", Registered(), "otlp")
	}
}
