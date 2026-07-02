package railway

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestLatest(t *testing.T) {
	r := MetricsResult{Values: []MetricValue{{Ts: 10, Value: 1}, {Ts: 30, Value: 3}, {Ts: 20, Value: 2}}}
	v, ok := r.Latest()
	if !ok || v.Ts != 30 || v.Value != 3 {
		t.Fatalf("Latest = %+v ok=%v, want ts=30 value=3", v, ok)
	}
	if _, ok := (MetricsResult{}).Latest(); ok {
		t.Error("empty result should have no latest")
	}
}

func TestMetricsBuildsVarsAndDecodes(t *testing.T) {
	var vars map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var req struct {
			Variables map[string]any `json:"variables"`
		}
		json.Unmarshal(body, &req)
		vars = req.Variables
		w.Write([]byte(`{"data":{"metrics":[
			{"measurement":"CPU_USAGE","tags":{"serviceId":"s1"},"values":[{"ts":100,"value":0.5}]}
		]}}`))
	}))
	defer srv.Close()

	c := fastClient(Auth{Token: "t", Type: TokenAccount}, srv.URL)
	results, err := c.Metrics(context.Background(), MetricsQuery{
		Measurements:      []MetricMeasurement{MeasurementCPUUsage, MeasurementMemoryUsageGB},
		EnvironmentID:     "env1",
		StartDate:         time.Unix(1000, 0),
		GroupBy:           []MetricTag{TagServiceID},
		SampleRateSeconds: 60,
	})
	if err != nil {
		t.Fatalf("Metrics: %v", err)
	}
	if len(results) != 1 || results[0].Measurement != MeasurementCPUUsage {
		t.Fatalf("results = %+v", results)
	}
	if results[0].Tags.ServiceID != "s1" || len(results[0].Values) != 1 {
		t.Errorf("decoded tags/values wrong: %+v", results[0])
	}

	// Variable assertions.
	meas, _ := vars["measurements"].([]any)
	if len(meas) != 2 || meas[0] != "CPU_USAGE" {
		t.Errorf("measurements var = %v", vars["measurements"])
	}
	if vars["environmentId"] != "env1" {
		t.Errorf("environmentId var = %v", vars["environmentId"])
	}
	if vars["sampleRateSeconds"] != float64(60) {
		t.Errorf("sampleRateSeconds var = %v", vars["sampleRateSeconds"])
	}
	if _, ok := vars["startDate"]; !ok {
		t.Error("startDate var missing")
	}
	gb, _ := vars["groupBy"].([]any)
	if len(gb) != 1 || gb[0] != "SERVICE_ID" {
		t.Errorf("groupBy var = %v", vars["groupBy"])
	}
}
