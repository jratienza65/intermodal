package metrics

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"slices"
	"sort"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/jratienza65/intermodal/internal/config"
	"github.com/jratienza65/intermodal/internal/railway"
	"github.com/jratienza65/intermodal/internal/target"
)

// fakeAPI is a canned metricsAPI. It returns results (or an error) keyed by the
// query's EnvironmentID, and records the queries it received. The poller calls
// Metrics concurrently (bounded fan-out over targets), so call recording is
// mutex-guarded.
type fakeAPI struct {
	byEnv  map[string][]railway.MetricsResult
	errEnv map[string]error

	mu    sync.Mutex
	calls []railway.MetricsQuery
}

func (f *fakeAPI) Metrics(_ context.Context, q railway.MetricsQuery) ([]railway.MetricsResult, error) {
	f.mu.Lock()
	f.calls = append(f.calls, q)
	f.mu.Unlock()
	if err := f.errEnv[q.EnvironmentID]; err != nil {
		return nil, err
	}
	return f.byEnv[q.EnvironmentID], nil
}

// recordedCalls returns a snapshot of the queries received.
func (f *fakeAPI) recordedCalls() []railway.MetricsQuery {
	f.mu.Lock()
	defer f.mu.Unlock()
	return slices.Clone(f.calls)
}

// fakeProvider is a canned target.Provider.
type fakeProvider struct {
	targets []target.Target
	err     error
}

func (p *fakeProvider) Targets(context.Context) ([]target.Target, error) {
	return p.targets, p.err
}

func testConfig() *config.Config {
	return &config.Config{
		PollInterval:      time.Minute,
		MetricsWindow:     10 * time.Minute,
		SampleRateSeconds: 60,
	}
}

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// result builds a MetricsResult with a single value for a service.
func result(m railway.MetricMeasurement, serviceID string, ts int64, v float64) railway.MetricsResult {
	return railway.MetricsResult{
		Measurement: m,
		Tags:        railway.MetricTags{ServiceID: serviceID},
		Values:      []railway.MetricValue{{Ts: ts, Value: v}},
	}
}

// findSample locates a sample by measurement + service ID.
func findSample(t *testing.T, samples []Sample, m railway.MetricMeasurement, serviceID string) Sample {
	t.Helper()
	for _, s := range samples {
		if s.Measurement == m && s.Labels.ServiceID == serviceID {
			return s
		}
	}
	t.Fatalf("no sample for measurement %s service %s", m, serviceID)
	return Sample{}
}

// TestPollEnrichesAndStores runs the poll path across two targets and asserts
// the resulting snapshot carries the expected samples with names enriched from
// the target (project/environment/service names).
func TestPollEnrichesAndStores(t *testing.T) {
	targets := []target.Target{
		{
			ProjectID:       "proj-1",
			ProjectName:     "web",
			EnvironmentID:   "env-1",
			EnvironmentName: "production",
			Services: []target.Service{
				{ID: "svc-1", Name: "api"},
				{ID: "svc-2", Name: "worker"},
			},
		},
	}
	api := &fakeAPI{
		byEnv: map[string][]railway.MetricsResult{
			"env-1": {
				result(railway.MeasurementCPUUsage, "svc-1", 100, 0.5),
				result(railway.MeasurementMemoryUsageGB, "svc-1", 100, 1.5),
				result(railway.MeasurementNetworkRxGB, "svc-2", 100, 2.0),
				// A result whose service ID isn't in the target's list: name
				// enrichment falls back to the raw ID.
				result(railway.MeasurementCPUUsage, "svc-unknown", 100, 0.9),
			},
		},
	}
	store := NewStore()
	p := NewPoller(api, &fakeProvider{targets: targets}, store, testConfig(), discardLogger())

	snap, ok := p.poll(context.Background())
	if !ok {
		t.Fatalf("poll ok = false, want true (all targets healthy)")
	}
	if snap == nil {
		t.Fatal("nil snapshot")
	}
	if len(snap.Samples) != 4 {
		t.Fatalf("got %d samples, want 4", len(snap.Samples))
	}

	// Enrichment: svc-1 -> "api", carried project/env names.
	s := findSample(t, snap.Samples, railway.MeasurementCPUUsage, "svc-1")
	if s.Labels.ServiceName != "api" {
		t.Errorf("service name = %q, want api", s.Labels.ServiceName)
	}
	if s.Labels.ProjectName != "web" || s.Labels.EnvironmentName != "production" {
		t.Errorf("project/env names not enriched: %+v", s.Labels)
	}
	if s.Value != 0.5 {
		t.Errorf("value = %v, want 0.5", s.Value)
	}

	// svc-2 network sample enriched to "worker".
	n := findSample(t, snap.Samples, railway.MeasurementNetworkRxGB, "svc-2")
	if n.Labels.ServiceName != "worker" {
		t.Errorf("service name = %q, want worker", n.Labels.ServiceName)
	}

	// Unknown service ID falls back to the ID itself.
	u := findSample(t, snap.Samples, railway.MeasurementCPUUsage, "svc-unknown")
	if u.Labels.ServiceName != "svc-unknown" {
		t.Errorf("unknown service name = %q, want svc-unknown", u.Labels.ServiceName)
	}

	// The API was queried with the target's project/env and configured window.
	calls := api.recordedCalls()
	if len(calls) != 1 {
		t.Fatalf("api calls = %d, want 1", len(calls))
	}
	q := calls[0]
	if q.ProjectID != "proj-1" || q.EnvironmentID != "env-1" {
		t.Errorf("query scoped wrong: %+v", q)
	}
	if q.SampleRateSeconds != 60 {
		t.Errorf("sample rate = %d, want 60", q.SampleRateSeconds)
	}
	if got := q.EndDate.Sub(q.StartDate); got != 10*time.Minute {
		t.Errorf("window = %v, want 10m", got)
	}
}

// TestPollPartialFailure asserts that when one target errors, the snapshot
// still contains the healthy target's samples and poll success is false.
func TestPollPartialFailure(t *testing.T) {
	targets := []target.Target{
		{
			ProjectID:     "proj-1",
			EnvironmentID: "env-healthy",
			Services:      []target.Service{{ID: "svc-1", Name: "api"}},
		},
		{
			ProjectID:     "proj-1",
			EnvironmentID: "env-broken",
			Services:      []target.Service{{ID: "svc-2", Name: "worker"}},
		},
	}
	api := &fakeAPI{
		byEnv: map[string][]railway.MetricsResult{
			"env-healthy": {result(railway.MeasurementCPUUsage, "svc-1", 100, 0.42)},
		},
		errEnv: map[string]error{
			"env-broken": errors.New("boom"),
		},
	}
	store := NewStore()
	p := NewPoller(api, &fakeProvider{targets: targets}, store, testConfig(), discardLogger())

	snap, ok := p.poll(context.Background())
	if ok {
		t.Errorf("poll ok = true, want false on partial failure")
	}
	if snap == nil {
		t.Fatal("snapshot nil; healthy target should still be visible")
	}
	if len(snap.Samples) != 1 {
		t.Fatalf("got %d samples, want 1 (only healthy target)", len(snap.Samples))
	}
	s := snap.Samples[0]
	if s.Labels.ServiceID != "svc-1" || s.Value != 0.42 {
		t.Errorf("healthy sample wrong: %+v", s)
	}
}

// TestPollAndStorePublishes verifies pollAndStore writes the snapshot into the
// store (the exported entry point used by Run).
func TestPollAndStorePublishes(t *testing.T) {
	targets := []target.Target{{
		ProjectID:     "proj-1",
		EnvironmentID: "env-1",
		Services:      []target.Service{{ID: "svc-1", Name: "api"}},
	}}
	api := &fakeAPI{byEnv: map[string][]railway.MetricsResult{
		"env-1": {result(railway.MeasurementCPUUsage, "svc-1", 100, 1.0)},
	}}
	store := NewStore()
	p := NewPoller(api, &fakeProvider{targets: targets}, store, testConfig(), discardLogger())

	p.pollAndStore(context.Background())

	snap := store.Snapshot()
	if snap == nil {
		t.Fatal("store empty after pollAndStore")
	}
	if len(snap.Samples) != 1 {
		t.Fatalf("got %d samples, want 1", len(snap.Samples))
	}
}

// TestPollDiscoveryFailure asserts a discovery error yields a nil snapshot and
// success=false, so the store is left untouched.
func TestPollDiscoveryFailure(t *testing.T) {
	store := NewStore()
	p := NewPoller(&fakeAPI{}, &fakeProvider{err: errors.New("discovery down")}, store, testConfig(), discardLogger())

	snap, ok := p.poll(context.Background())
	if ok {
		t.Error("poll ok = true, want false on discovery failure")
	}
	if snap != nil {
		t.Errorf("snapshot = %+v, want nil on discovery failure", snap)
	}
}

// TestPollNoTargets asserts zero targets is a success with an empty snapshot.
func TestPollNoTargets(t *testing.T) {
	p := NewPoller(&fakeAPI{}, &fakeProvider{targets: nil}, NewStore(), testConfig(), discardLogger())
	snap, ok := p.poll(context.Background())
	if !ok {
		t.Error("poll ok = false, want true for zero targets")
	}
	if snap == nil || len(snap.Samples) != 0 {
		t.Errorf("want empty non-nil snapshot, got %+v", snap)
	}
}

// TestSamplesFromResultsLatest verifies samplesFromResults picks the latest
// value by timestamp and skips series with no values, using tag IDs when
// present and falling back to the target's IDs otherwise.
func TestSamplesFromResultsLatest(t *testing.T) {
	tg := target.Target{
		ProjectID:       "tgt-proj",
		ProjectName:     "web",
		EnvironmentID:   "tgt-env",
		EnvironmentName: "production",
		Services:        []target.Service{{ID: "svc-1", Name: "api"}},
	}
	results := []railway.MetricsResult{
		{
			Measurement: railway.MeasurementCPUUsage,
			Tags:        railway.MetricTags{ServiceID: "svc-1"},
			Values: []railway.MetricValue{
				{Ts: 100, Value: 0.1},
				{Ts: 300, Value: 0.9}, // latest
				{Ts: 200, Value: 0.5},
			},
		},
		{
			Measurement: railway.MeasurementCPUUsage,
			Tags:        railway.MetricTags{ServiceID: "svc-empty"},
			Values:      nil, // skipped
		},
		{
			// Tags carry project/env IDs which should take precedence.
			Measurement: railway.MeasurementMemoryUsageGB,
			Tags:        railway.MetricTags{ServiceID: "svc-1", ProjectID: "tag-proj", EnvironmentID: "tag-env"},
			Values:      []railway.MetricValue{{Ts: 100, Value: 2.0}},
		},
	}

	got := samplesFromResults(results, tg)
	if len(got) != 2 {
		t.Fatalf("got %d samples, want 2 (empty series skipped)", len(got))
	}

	sort.Slice(got, func(i, j int) bool { return got[i].Measurement < got[j].Measurement })

	cpu := findSample(t, got, railway.MeasurementCPUUsage, "svc-1")
	if cpu.Value != 0.9 {
		t.Errorf("latest value = %v, want 0.9", cpu.Value)
	}
	if !cpu.Timestamp.Equal(time.Unix(300, 0)) {
		t.Errorf("timestamp = %v, want unix 300", cpu.Timestamp)
	}
	// Fell back to target IDs (tags empty for this result).
	if cpu.Labels.ProjectID != "tgt-proj" || cpu.Labels.EnvironmentID != "tgt-env" {
		t.Errorf("expected target IDs, got %+v", cpu.Labels)
	}

	mem := findSample(t, got, railway.MeasurementMemoryUsageGB, "svc-1")
	if mem.Labels.ProjectID != "tag-proj" || mem.Labels.EnvironmentID != "tag-env" {
		t.Errorf("expected tag IDs to win, got %+v", mem.Labels)
	}
}

// blockingAPI records the highest number of Metrics calls in flight together.
type blockingAPI struct {
	release chan struct{}

	mu       sync.Mutex
	inFlight int
	peak     int
}

func (b *blockingAPI) Metrics(context.Context, railway.MetricsQuery) ([]railway.MetricsResult, error) {
	b.mu.Lock()
	b.inFlight++
	if b.inFlight > b.peak {
		b.peak = b.inFlight
	}
	b.mu.Unlock()
	<-b.release
	b.mu.Lock()
	b.inFlight--
	b.mu.Unlock()
	return nil, nil
}

func manyTargets(n int) []target.Target {
	out := make([]target.Target, 0, n)
	for i := range n {
		id := "env-" + strconv.Itoa(i)
		out = append(out, target.Target{ProjectID: "p", EnvironmentID: id})
	}
	return out
}

// The poll fan-out must never run more targets at once than PollConcurrency:
// each one is a Railway API call, so this is the burst against the rate limit.
func TestPollConcurrencyIsBounded(t *testing.T) {
	for _, limit := range []int{1, 3, 8} {
		t.Run(strconv.Itoa(limit), func(t *testing.T) {
			api := &blockingAPI{release: make(chan struct{})}
			cfg := testConfig()
			cfg.PollConcurrency = limit
			p := NewPoller(api, &fakeProvider{targets: manyTargets(24)}, NewStore(), cfg, discardLogger())

			done := make(chan struct{})
			go func() { p.poll(context.Background()); close(done) }()

			// Let the fan-out saturate, then let every call finish.
			deadline := time.Now().Add(2 * time.Second)
			for {
				api.mu.Lock()
				got := api.inFlight
				api.mu.Unlock()
				if got >= limit || time.Now().After(deadline) {
					break
				}
				time.Sleep(time.Millisecond)
			}
			close(api.release)
			<-done

			if api.peak > limit {
				t.Errorf("peak in-flight = %d, want <= %d", api.peak, limit)
			}
			if api.peak < limit {
				t.Errorf("peak in-flight = %d, want the full %d used", api.peak, limit)
			}
		})
	}
}

// A hand-built Config (zero PollConcurrency) must still bound the fan-out. A
// zero would size the semaphore channel at 0 and deadlock the poll.
func TestPollConcurrencyZeroFallsBack(t *testing.T) {
	p := NewPoller(&fakeAPI{}, &fakeProvider{targets: manyTargets(3)}, NewStore(), testConfig(), discardLogger())
	if p.concurrency != config.DefaultConcurrency {
		t.Fatalf("concurrency = %d, want %d", p.concurrency, config.DefaultConcurrency)
	}
	done := make(chan struct{})
	go func() { p.poll(context.Background()); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("poll deadlocked with an unconfigured concurrency")
	}
}
