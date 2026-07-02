package logs

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"

	"github.com/jratienza65/intermodal/internal/config"
	"github.com/jratienza65/intermodal/internal/model"
	"github.com/jratienza65/intermodal/internal/railway"
	"github.com/jratienza65/intermodal/internal/sink"
	"github.com/jratienza65/intermodal/internal/target"
	"github.com/jratienza65/intermodal/internal/telemetry"
)

// --- fakes ---------------------------------------------------------------

// fakeSource emits a fixed set of log entries on the first (and every)
// connection, then blocks until the context is cancelled. Emitting up front and
// then blocking means runSubscription never enters its reconnect loop, keeping
// the tests deterministic.
type fakeSource struct {
	entries      []railway.LogEntry
	httpEntries  []railway.HTTPLogEntry
	buildEntries []railway.LogEntry

	mu      sync.Mutex
	started chan struct{} // closed once entries have been delivered
	once    sync.Once
}

func newFakeSource(entries ...railway.LogEntry) *fakeSource {
	return &fakeSource{entries: entries, started: make(chan struct{})}
}

func (f *fakeSource) StreamEnvironmentLogs(ctx context.Context, _ railway.LogStreamParams, fn func(railway.LogEntry) error) error {
	for _, e := range f.entries {
		if err := fn(e); err != nil {
			return err
		}
	}
	f.once.Do(func() { close(f.started) })
	<-ctx.Done()
	return ctx.Err()
}

func (f *fakeSource) StreamHTTPLogs(ctx context.Context, _ railway.HTTPLogStreamParams, fn func(railway.HTTPLogEntry) error) error {
	for _, e := range f.httpEntries {
		if err := fn(e); err != nil {
			return err
		}
	}
	<-ctx.Done()
	return ctx.Err()
}

// StreamBuildLogs delivers build entries then returns (build logs are finite).
func (f *fakeSource) StreamBuildLogs(_ context.Context, _ railway.BuildLogStreamParams, fn func(railway.LogEntry) error) error {
	for _, e := range f.buildEntries {
		if err := fn(e); err != nil {
			return err
		}
	}
	return nil
}

// fakeSink records every batch it is handed and remembers whether Close ran.
type fakeSink struct {
	name string

	mu      sync.Mutex
	records []model.LogRecord
	closed  bool
	writes  int
}

func (s *fakeSink) Name() string { return s.name }

func (s *fakeSink) Write(_ context.Context, batch []model.LogRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	// The worker reuses the batch's backing array, so copy each record.
	for _, r := range batch {
		s.records = append(s.records, r.Clone())
	}
	s.writes++
	return nil
}

func (s *fakeSink) Close(_ context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}

func (s *fakeSink) snapshot() ([]model.LogRecord, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]model.LogRecord, len(s.records))
	copy(out, s.records)
	return out, s.closed
}

func (s *fakeSink) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.records)
}

// fakeProvider returns a single target with one named service.
type fakeProvider struct {
	targets []target.Target
}

func (p *fakeProvider) Targets(_ context.Context) ([]target.Target, error) {
	return p.targets, nil
}

// --- helpers -------------------------------------------------------------

const (
	testEnvID    = "env-1"
	testProjID   = "proj-1"
	testSvcID    = "svc-1"
	testSvcName  = "api"
	testProjName = "acme"
	testEnvName  = "production"
	testOtherSvc = "svc-2"
)

func oneTarget() target.Target {
	return target.Target{
		ProjectID:       testProjID,
		ProjectName:     testProjName,
		EnvironmentID:   testEnvID,
		EnvironmentName: testEnvName,
		Services:        []target.Service{{ID: testSvcID, Name: testSvcName}},
	}
}

func baseConfig() *config.Config {
	return &config.Config{
		// BatchSize 1 flushes every record immediately, so tests never wait on
		// the batch timer.
		LogQueueSize:    1000,
		LogBatchSize:    1,
		LogBatchTimeout: 50 * time.Millisecond,
		DiscoveryTTL:    time.Hour, // avoid periodic reconcile during the test
		DeployLogs:      true,
	}
}

// waitForCount polls sink until it holds at least n records or the deadline hits.
func waitForCount(t *testing.T, s *fakeSink, n int, within time.Duration) {
	t.Helper()
	deadline := time.Now().Add(within)
	for time.Now().Before(deadline) {
		if s.count() >= n {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatalf("timed out waiting for %d records; got %d", n, s.count())
}

func entry(sev, msg, svcID string, attrs ...railway.LogAttribute) railway.LogEntry {
	return railway.LogEntry{
		Timestamp:  time.Now().UTC().Format(time.RFC3339Nano),
		Message:    msg,
		Severity:   sev,
		Tags:       railway.LogTags{ProjectID: testProjID, EnvironmentID: testEnvID, ServiceID: svcID},
		Attributes: attrs,
	}
}

// --- tests ---------------------------------------------------------------

func TestPipelineNormalizesAndDelivers(t *testing.T) {
	src := newFakeSource(
		entry("err", "boom", testSvcID,
			railway.LogAttribute{Key: "request_id", Value: "abc"},
			railway.LogAttribute{Key: "path", Value: "/v1"},
		),
	)
	sink := &fakeSink{name: "fake"}
	prov := &fakeProvider{targets: []target.Target{oneTarget()}}

	m := NewManager(src, prov, sinks(sink), baseConfig(), nil)

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() { defer close(runDone); _ = m.Run(ctx) }()

	waitForCount(t, sink, 1, 2*time.Second)
	cancel()
	<-runDone

	recs, _ := sink.snapshot()
	if len(recs) != 1 {
		t.Fatalf("want 1 record, got %d", len(recs))
	}
	r := recs[0]
	if r.Message != "boom" {
		t.Errorf("message = %q, want boom", r.Message)
	}
	if r.Severity != "err" {
		t.Errorf("severity = %q, want err (raw preserved)", r.Severity)
	}
	if r.Level != model.LevelError {
		t.Errorf("level = %q, want %q", r.Level, model.LevelError)
	}
	if r.Attributes["request_id"] != "abc" || r.Attributes["path"] != "/v1" {
		t.Errorf("attributes not mapped: %#v", r.Attributes)
	}
	// Names enriched from the target.
	if r.ProjectName != testProjName {
		t.Errorf("project name = %q, want %q", r.ProjectName, testProjName)
	}
	if r.EnvironmentName != testEnvName {
		t.Errorf("environment name = %q, want %q", r.EnvironmentName, testEnvName)
	}
	if r.ServiceName != testSvcName {
		t.Errorf("service name = %q, want %q", r.ServiceName, testSvcName)
	}
}

func TestPipelineServiceAllowlistDrops(t *testing.T) {
	src := newFakeSource(
		entry("info", "kept", testSvcID),
		entry("info", "dropped", testOtherSvc),
	)
	sink := &fakeSink{name: "fake-allow"}
	prov := &fakeProvider{targets: []target.Target{oneTarget()}}

	cfg := baseConfig()
	cfg.Services = []string{testSvcID} // only svc-1 is allowed

	m := NewManager(src, prov, sinks(sink), cfg, nil)

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() { defer close(runDone); _ = m.Run(ctx) }()

	waitForCount(t, sink, 1, 2*time.Second)
	// Give the (would-be) second record a chance to arrive so we can prove it
	// was dropped rather than merely slow.
	time.Sleep(50 * time.Millisecond)
	cancel()
	<-runDone

	recs, _ := sink.snapshot()
	if len(recs) != 1 {
		t.Fatalf("want 1 record after allowlist, got %d", len(recs))
	}
	if recs[0].Message != "kept" || recs[0].ServiceID != testSvcID {
		t.Errorf("wrong record survived allowlist: %+v", recs[0])
	}
}

func TestPipelineGracefulShutdownDrainsAndCloses(t *testing.T) {
	src := newFakeSource(
		entry("info", "a", testSvcID),
		entry("warn", "b", testSvcID),
		entry("debug", "c", testSvcID),
	)
	sink := &fakeSink{name: "fake-shutdown"}
	prov := &fakeProvider{targets: []target.Target{oneTarget()}}

	m := NewManager(src, prov, sinks(sink), baseConfig(), nil)

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() { defer close(runDone); _ = m.Run(ctx) }()

	// Wait until the source has emitted so a cancel afterward exercises the drain
	// path rather than racing delivery.
	select {
	case <-src.started:
	case <-time.After(2 * time.Second):
		t.Fatal("source never emitted")
	}
	waitForCount(t, sink, 3, 2*time.Second)

	cancel()
	select {
	case <-runDone:
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not return after cancel")
	}

	recs, closed := sink.snapshot()
	if len(recs) != 3 {
		t.Errorf("want 3 drained records, got %d", len(recs))
	}
	if !closed {
		t.Error("sink.Close was not called on shutdown")
	}
}

// TestWorkerDropsOnFullQueue drives the worker directly: with a queue depth of 1
// and no consumer running, all but the first enqueue must be dropped and counted.
func TestWorkerDropsOnFullQueue(t *testing.T) {
	const name = "drop-sink"
	w := newWorker(&fakeSink{name: name}, 1 /*queueSize*/, 10, time.Second, nil)

	before := testutil.ToFloat64(telemetry.LogsDropped.WithLabelValues(name))

	const attempts = 100
	for i := 0; i < attempts; i++ {
		w.enqueue(model.LogRecord{Message: "x"})
	}

	after := testutil.ToFloat64(telemetry.LogsDropped.WithLabelValues(name))
	dropped := after - before
	// The queue holds exactly one record; every other enqueue is dropped.
	if want := float64(attempts - 1); dropped != want {
		t.Fatalf("dropped = %v, want %v", dropped, want)
	}
}

// sinks is a tiny adapter so the []sink.Sink literal reads cleanly above.
func sinks(s ...*fakeSink) []sink.Sink {
	out := make([]sink.Sink, len(s))
	for i, x := range s {
		out[i] = x
	}
	return out
}

func TestPipelineHTTPLogs(t *testing.T) {
	src := &fakeSource{
		started: make(chan struct{}),
		httpEntries: []railway.HTTPLogEntry{{
			Timestamp:     time.Now().UTC().Format(time.RFC3339Nano),
			Method:        "POST",
			Path:          "/oauth/v2/introspect",
			Host:          "auth.example.ph",
			HTTPStatus:    200,
			TotalDuration: 25,
			RequestID:     "req-abc",
			DeploymentID:  "dep-1",
		}},
	}
	sink := &fakeSink{name: "http-sink"}
	tg := target.Target{
		ProjectID: testProjID, ProjectName: testProjName,
		EnvironmentID: testEnvID, EnvironmentName: testEnvName,
		Services: []target.Service{{ID: testSvcID, Name: testSvcName, HasDomain: true, DeploymentID: "dep-1"}},
	}
	prov := &fakeProvider{targets: []target.Target{tg}}

	cfg := baseConfig()
	cfg.DeployLogs = false
	cfg.HTTPLogs = true

	m := NewManager(src, prov, sinks(sink), cfg, nil)
	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() { defer close(runDone); _ = m.Run(ctx) }()

	waitForCount(t, sink, 1, 2*time.Second)
	cancel()
	<-runDone

	recs, _ := sink.snapshot()
	if len(recs) != 1 {
		t.Fatalf("want 1 http record, got %d", len(recs))
	}
	r := recs[0]
	if r.Kind != model.KindHTTP {
		t.Errorf("kind = %q, want http", r.Kind)
	}
	if r.ServiceName != testSvcName {
		t.Errorf("service name = %q, want %q (enriched from target)", r.ServiceName, testSvcName)
	}
	if r.Attributes["http.request.method"] != "POST" || r.Attributes["url.path"] != "/oauth/v2/introspect" {
		t.Errorf("http attributes missing/wrong: %#v", r.Attributes)
	}
	if r.Attributes["http.response.status_code"] != "200" {
		t.Errorf("status attr = %q", r.Attributes["http.response.status_code"])
	}
	if r.Attributes["http.request.id"] != "req-abc" {
		t.Errorf("request id attr = %q", r.Attributes["http.request.id"])
	}
	if r.Level != model.LevelInfo {
		t.Errorf("200 should be info level, got %q", r.Level)
	}
}

func TestPipelineBuildLogs(t *testing.T) {
	src := &fakeSource{
		started: make(chan struct{}),
		buildEntries: []railway.LogEntry{
			{Timestamp: time.Now().UTC().Format(time.RFC3339Nano), Message: "Building image...", Severity: "info"},
			{Timestamp: time.Now().UTC().Format(time.RFC3339Nano), Message: "Build complete", Severity: "info"},
		},
	}
	sink := &fakeSink{name: "build-sink"}
	tg := target.Target{
		ProjectID: testProjID, ProjectName: testProjName,
		EnvironmentID: testEnvID, EnvironmentName: testEnvName,
		Services: []target.Service{{ID: testSvcID, Name: testSvcName, DeploymentID: "dep-1"}},
	}
	prov := &fakeProvider{targets: []target.Target{tg}}

	cfg := baseConfig()
	cfg.DeployLogs = false
	cfg.BuildLogs = true

	m := NewManager(src, prov, sinks(sink), cfg, nil)
	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() { defer close(runDone); _ = m.Run(ctx) }()

	waitForCount(t, sink, 2, 2*time.Second)
	cancel()
	<-runDone

	recs, _ := sink.snapshot()
	if len(recs) != 2 {
		t.Fatalf("want 2 build records, got %d", len(recs))
	}
	for _, r := range recs {
		if r.Kind != model.KindBuild {
			t.Errorf("kind = %q, want build", r.Kind)
		}
		if r.ServiceName != testSvcName {
			t.Errorf("service name = %q, want %q (enriched)", r.ServiceName, testSvcName)
		}
	}
}

func TestInScope(t *testing.T) {
	set := toLowerSet([]string{"Zitadel", "svc-abc"})
	if !inScope(nil, "x", "y") {
		t.Error("empty set should allow everything")
	}
	if !inScope(set, "some-id", "zitadel") {
		t.Error("should match name case-insensitively")
	}
	if !inScope(set, "SVC-ABC", "other") {
		t.Error("should match id case-insensitively")
	}
	if inScope(set, "nope", "nada") {
		t.Error("should not match unknown id/name")
	}
}

// TestPipelineServiceAllowlistByName verifies the service allowlist accepts a
// service *name*, not just its ID.
func TestPipelineServiceAllowlistByName(t *testing.T) {
	src := newFakeSource(
		entry("info", "kept", testSvcID),
		entry("info", "dropped", testOtherSvc),
	)
	sink := &fakeSink{name: "by-name"}
	prov := &fakeProvider{targets: []target.Target{oneTarget()}}

	cfg := baseConfig()
	cfg.Services = []string{testSvcName} // "api" — the NAME, not the UUID

	m := NewManager(src, prov, sinks(sink), cfg, nil)
	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() { defer close(runDone); _ = m.Run(ctx) }()

	waitForCount(t, sink, 1, 2*time.Second)
	time.Sleep(50 * time.Millisecond)
	cancel()
	<-runDone

	recs, _ := sink.snapshot()
	if len(recs) != 1 || recs[0].Message != "kept" {
		t.Fatalf("name allowlist failed: %+v", recs)
	}
}

func TestHTTPStatusLevel(t *testing.T) {
	cases := map[int]model.Level{
		200: model.LevelInfo, 302: model.LevelInfo,
		404: model.LevelWarn, 429: model.LevelWarn,
		500: model.LevelError, 503: model.LevelError,
	}
	for status, want := range cases {
		if got := httpStatusLevel(status); got != want {
			t.Errorf("httpStatusLevel(%d) = %q, want %q", status, got, want)
		}
	}
}

func TestHTTPAttributes(t *testing.T) {
	a := httpAttributes(railway.HTTPLogEntry{Method: "GET", Path: "/", HTTPStatus: 200, TxBytes: 12})
	for _, k := range []string{"http.request.method", "url.path", "http.response.status_code", "http.server.duration_ms", "http.request.body.size", "http.response.body.size"} {
		if _, ok := a[k]; !ok {
			t.Errorf("numeric/core attribute %q should always be set: %#v", k, a)
		}
	}
	if _, ok := a["client.address"]; ok {
		t.Error("empty optional client.address should be dropped")
	}
}
