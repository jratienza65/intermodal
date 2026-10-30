package logs

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
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

// pagedSource answers the log history queries the way Railway does: lines at
// or after the anchor, oldest first, up to afterLimit+1 of them.
type pagedSource struct {
	fakeSource

	mu      sync.Mutex
	deploy  []railway.LogEntry
	http    []railway.HTTPLogEntry
	queries int
}

func (s *pagedSource) add(e ...railway.LogEntry) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.deploy = append(s.deploy, e...)
}

func (s *pagedSource) EnvironmentLogsPage(_ context.Context, p railway.LogPageParams) ([]railway.LogEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.queries++
	var out []railway.LogEntry
	for _, e := range s.deploy {
		ts, _ := time.Parse(time.RFC3339Nano, e.Timestamp)
		if ts.Before(p.Anchor) {
			continue
		}
		out = append(out, e)
		if len(out) > p.AfterLimit {
			break
		}
	}
	return out, nil
}

func (s *pagedSource) HTTPLogsPage(_ context.Context, p railway.LogPageParams) ([]railway.HTTPLogEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []railway.HTTPLogEntry
	for _, e := range s.http {
		ts, _ := time.Parse(time.RFC3339Nano, e.Timestamp)
		if ts.Before(p.Anchor) {
			continue
		}
		out = append(out, e)
		if len(out) > p.AfterLimit {
			break
		}
	}
	return out, nil
}

// flakySink fails every write while failing is set.
type flakySink struct {
	fakeSink
	failing atomic.Bool
}

func (s *flakySink) Write(ctx context.Context, batch []model.LogRecord) error {
	if s.failing.Load() {
		return errors.New("sink down")
	}
	return s.fakeSink.Write(ctx, batch)
}

var testNow = time.Date(2026, 1, 15, 12, 0, 0, 0, time.UTC)

func lineAt(ts time.Time, msg string) railway.LogEntry {
	return railway.LogEntry{
		Timestamp: ts.UTC().Format(time.RFC3339Nano),
		Message:   msg,
		Severity:  "info",
		Tags:      railway.LogTags{ProjectID: testProjID, EnvironmentID: testEnvID, ServiceID: testSvcID},
	}
}

func pollConfig() *config.Config {
	cfg := baseConfig()
	cfg.LogSource = config.LogSourcePoll
	cfg.LogBatchSize = 100
	cfg.LogPollInterval = 5 * time.Millisecond
	cfg.LogPollMaxInterval = 20 * time.Millisecond
	cfg.LogSettle = 20 * time.Second
	cfg.LogMaxCatchUp = 6 * time.Hour
	return cfg
}

// pollHarness is a Manager with running workers and a settable clock, for
// driving pollOnce directly.
type pollHarness struct {
	m      *Manager
	ctx    context.Context
	now    time.Time
	cancel context.CancelFunc
}

func newPollHarness(t *testing.T, src logSource, cfg *config.Config, sinks []sink.Sink) *pollHarness {
	t.Helper()
	h := &pollHarness{now: testNow}
	h.m = NewManager(src, &fakeProvider{}, sinks, cfg, nil)
	h.m.now = func() time.Time { return h.now }
	h.ctx, h.cancel = context.WithCancel(context.Background())
	for _, w := range h.m.workers {
		go w.run(h.ctx)
	}
	t.Cleanup(func() {
		h.cancel()
		for _, w := range h.m.workers {
			<-w.done
		}
	})
	return h
}

func (h *pollHarness) deploy(start time.Time) *poller {
	return h.m.deployPoller(testEnvID, oneTarget(), start)
}

// messages counts each delivered message.
func messages(s *fakeSink) map[string]int {
	recs, _ := s.snapshot()
	out := map[string]int{}
	for _, r := range recs {
		out[r.Message]++
	}
	return out
}

func TestPollPagesWithoutLossOrDuplicates(t *testing.T) {
	src := &pagedSource{}
	base := testNow.Add(-10 * time.Minute)
	var want []string
	for i := range 2600 {
		ts := base.Add(time.Duration(i) * 100 * time.Millisecond)
		// Several lines share a timestamp across the first page boundary.
		if i >= 995 && i <= 1005 {
			ts = base.Add(995 * 100 * time.Millisecond)
		}
		msg := "line-" + strconv.Itoa(i)
		src.add(lineAt(ts, msg))
		want = append(want, msg)
	}
	// Two identical lines at one instant are two lines.
	twin := base.Add(2000*100*time.Millisecond + time.Millisecond)
	src.add(lineAt(twin, "twin"), lineAt(twin, "twin"))
	sortByTimestamp(src.deploy)

	s := &fakeSink{name: "a"}
	h := newPollHarness(t, src, pollConfig(), sinks(s))
	cur := Cursor{TS: base.Add(-time.Second)}
	n, err := h.m.pollOnce(h.ctx, h.deploy(cur.TS), &cur)
	if err != nil {
		t.Fatal(err)
	}
	if n != 2602 {
		t.Fatalf("delivered %d, want 2602", n)
	}
	got := messages(s)
	for _, msg := range want {
		if got[msg] != 1 {
			t.Fatalf("%s delivered %d times", msg, got[msg])
		}
	}
	if got["twin"] != 2 {
		t.Fatalf("twin delivered %d times, want 2", got["twin"])
	}
	if want := testNow.Add(-20 * time.Second); !cur.TS.Equal(want) {
		t.Fatalf("cursor %v, want the horizon %v", cur.TS, want)
	}

	// A second round finds nothing new.
	n, err = h.m.pollOnce(h.ctx, h.deploy(cur.TS), &cur)
	if err != nil || n != 0 {
		t.Fatalf("second round delivered %d (err %v), want 0", n, err)
	}
}

// sortByTimestamp orders lines by time. Comparing the strings would not do:
// RFC3339Nano drops trailing zeros, so "…:39Z" sorts after "…:39.1Z".
func sortByTimestamp(es []railway.LogEntry) {
	at := func(e railway.LogEntry) time.Time {
		ts, _ := time.Parse(time.RFC3339Nano, e.Timestamp)
		return ts
	}
	sort.SliceStable(es, func(i, j int) bool { return at(es[i]).Before(at(es[j])) })
}

func TestPollHoldsBackUnsettledLines(t *testing.T) {
	src := &pagedSource{}
	src.add(lineAt(testNow.Add(-30*time.Second), "settled"), lineAt(testNow.Add(-5*time.Second), "fresh"))
	s := &fakeSink{name: "a"}
	h := newPollHarness(t, src, pollConfig(), sinks(s))
	cur := Cursor{TS: testNow.Add(-time.Minute)}
	p := h.deploy(cur.TS)

	if _, err := h.m.pollOnce(h.ctx, p, &cur); err != nil {
		t.Fatal(err)
	}
	if got := messages(s); got["settled"] != 1 || got["fresh"] != 0 {
		t.Fatalf("after first round: %v", got)
	}

	// A line that lands late but inside the settle window is still picked up.
	src.add(lineAt(testNow.Add(-10*time.Second), "late"))
	sortByTimestamp(src.deploy)
	h.now = testNow.Add(30 * time.Second)
	if _, err := h.m.pollOnce(h.ctx, p, &cur); err != nil {
		t.Fatal(err)
	}
	if got := messages(s); got["settled"] != 1 || got["fresh"] != 1 || got["late"] != 1 {
		t.Fatalf("after second round: %v", got)
	}
}

func TestPollCursorSurvivesRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "cursors.json")
	src := &pagedSource{}
	src.add(lineAt(testNow.Add(-5*time.Minute), "before-restart"))
	s := &fakeSink{name: "a"}

	run := func(now time.Time) {
		store, err := OpenCursorStore(path, 0)
		if err != nil {
			t.Fatal(err)
		}
		h := newPollHarness(t, src, pollConfig(), sinks(s))
		h.now = now
		h.m.SetCursorStore(store)
		p := h.deploy(testNow.Add(-10 * time.Minute))
		cur, ok := store.Load(p.key)
		if !ok {
			cur = Cursor{TS: p.start}
		}
		if _, err := h.m.pollOnce(h.ctx, p, &cur); err != nil {
			t.Fatal(err)
		}
	}

	run(testNow)
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("cursor file not written: %v", err)
	}
	// Down for an hour; Railway kept the lines.
	src.add(lineAt(testNow.Add(30*time.Minute), "during-outage"))
	run(testNow.Add(time.Hour))

	if got := messages(s); got["before-restart"] != 1 || got["during-outage"] != 1 || len(got) != 2 {
		t.Fatalf("delivered %v", got)
	}
}

func TestPollKeepsCursorWhenSinkFails(t *testing.T) {
	src := &pagedSource{}
	src.add(lineAt(testNow.Add(-time.Minute), "a"), lineAt(testNow.Add(-50*time.Second), "b"))
	s := &flakySink{fakeSink: fakeSink{name: "flaky"}}
	s.failing.Store(true)
	h := newPollHarness(t, src, pollConfig(), []sink.Sink{s})
	start := testNow.Add(-2 * time.Minute)
	cur := Cursor{TS: start}
	p := h.deploy(start)

	if _, err := h.m.pollOnce(h.ctx, p, &cur); !errors.Is(err, errDelivery) {
		t.Fatalf("err = %v, want errDelivery", err)
	}
	if !cur.TS.Equal(start) {
		t.Fatalf("cursor moved to %v while the sink was down", cur.TS)
	}

	s.failing.Store(false)
	if _, err := h.m.pollOnce(h.ctx, p, &cur); err != nil {
		t.Fatal(err)
	}
	if got := messages(&s.fakeSink); got["a"] != 1 || got["b"] != 1 {
		t.Fatalf("delivered %v", got)
	}
}

func TestPollCapsCatchUp(t *testing.T) {
	src := &pagedSource{}
	src.add(lineAt(testNow.Add(-8*time.Hour), "too-old"), lineAt(testNow.Add(-time.Hour), "recent"))
	s := &fakeSink{name: "a"}
	h := newPollHarness(t, src, pollConfig(), sinks(s))
	cur := Cursor{TS: testNow.Add(-10 * time.Hour)}
	if _, err := h.m.pollOnce(h.ctx, h.deploy(cur.TS), &cur); err != nil {
		t.Fatal(err)
	}
	if got := messages(s); got["too-old"] != 0 || got["recent"] != 1 {
		t.Fatalf("delivered %v", got)
	}
}

func TestPollBackpressureDoesNotDrop(t *testing.T) {
	src := &pagedSource{}
	for i := range 3000 {
		src.add(lineAt(testNow.Add(-time.Hour+time.Duration(i)*time.Millisecond), "l"+strconv.Itoa(i)))
	}
	cfg := pollConfig()
	cfg.LogQueueSize = 2
	cfg.LogBatchSize = 1
	s := &fakeSink{name: "tiny-queue"}
	before := testutil.ToFloat64(telemetry.LogsDropped.WithLabelValues("tiny-queue"))
	h := newPollHarness(t, src, cfg, sinks(s))
	cur := Cursor{TS: testNow.Add(-2 * time.Hour)}
	if _, err := h.m.pollOnce(h.ctx, h.deploy(cur.TS), &cur); err != nil {
		t.Fatal(err)
	}
	if s.count() != 3000 {
		t.Fatalf("delivered %d, want 3000", s.count())
	}
	if d := testutil.ToFloat64(telemetry.LogsDropped.WithLabelValues("tiny-queue")) - before; d != 0 {
		t.Fatalf("dropped %v records", d)
	}
}

func TestHTTPPollerDrainsRetiredDeployment(t *testing.T) {
	src := &pagedSource{}
	src.http = []railway.HTTPLogEntry{{
		Timestamp: time.Now().Add(-time.Second).UTC().Format(time.RFC3339Nano),
		Method:    "GET", Path: "/old", HTTPStatus: 200, DeploymentID: "dep-old",
	}}
	cfg := pollConfig()
	cfg.LogSettle = 0
	s := &fakeSink{name: "a"}
	h := newPollHarness(t, src, cfg, sinks(s))
	h.m.now = time.Now

	ht := svcContext{envID: testEnvID, envName: testEnvName, serviceID: testSvcID, serviceName: testSvcName, deploymentID: "dep-old"}
	p := h.m.httpPoller(ht, time.Now().Add(-time.Minute))
	p.retireAt = time.Now()
	close(p.retire)

	done := make(chan struct{})
	go func() {
		h.m.runPoller(h.ctx, p)
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("retired poller did not exit")
	}
	if got := messages(s); got["GET /old 200 0ms"] != 1 {
		t.Fatalf("delivered %v", got)
	}
	if _, ok := h.m.cursors.Load(p.key); ok {
		t.Fatal("retired poller kept its cursor")
	}
}

func TestPollModeRun(t *testing.T) {
	src := &pagedSource{}
	src.add(lineAt(time.Now().Add(-time.Minute), "hello"))
	cfg := pollConfig()
	cfg.LogSettle = 0
	cfg.LogInitialLookback = 5 * time.Minute
	s := &fakeSink{name: "a"}
	m := NewManager(src, &fakeProvider{targets: []target.Target{oneTarget()}}, sinks(s), cfg, nil)

	ctx, cancel := context.WithCancel(context.Background())
	errc := make(chan error, 1)
	go func() { errc <- m.Run(ctx) }()
	waitForCount(t, s, 1, 5*time.Second)
	cancel()
	<-errc

	recs, _ := s.snapshot()
	r := recs[0]
	if r.Kind != model.KindDeploy || r.ServiceName != testSvcName || r.EnvironmentName != testEnvName {
		t.Fatalf("record shape changed: %+v", r)
	}
}

func TestHTTPPollerWaitsForDeployPoller(t *testing.T) {
	src := &pagedSource{}
	src.http = []railway.HTTPLogEntry{
		{Timestamp: testNow.Add(-3 * time.Hour).Format(time.RFC3339Nano), Method: "GET", Path: "/early", HTTPStatus: 200},
		{Timestamp: testNow.Add(-time.Hour).Format(time.RFC3339Nano), Method: "GET", Path: "/late", HTTPStatus: 200},
	}
	s := &fakeSink{name: "a"}
	h := newPollHarness(t, src, pollConfig(), sinks(s))

	// The environment's deploy poller is still catching up, at now-3h.
	d := h.deploy(testNow.Add(-3 * time.Hour))
	d.setCursor(testNow.Add(-3 * time.Hour))
	h.m.deployPollers[testEnvID] = d

	ht := svcContext{envID: testEnvID, deploymentID: "dep-1", serviceID: testSvcID, serviceName: testSvcName}
	p := h.m.httpPoller(ht, testNow.Add(-4*time.Hour))
	cur := Cursor{TS: p.start}
	if _, err := h.m.pollOnce(h.ctx, p, &cur); err != nil {
		t.Fatal(err)
	}
	if got := messages(s); got["GET /early 200 0ms"] != 1 || got["GET /late 200 0ms"] != 0 {
		t.Fatalf("delivered %v", got)
	}
	if want := testNow.Add(-3*time.Hour + maxKindSkew); !cur.TS.Equal(want) {
		t.Fatalf("cursor %v, want the gate %v", cur.TS, want)
	}

	// As the deploy poller catches up, the rest follows.
	d.setCursor(testNow.Add(-30 * time.Minute))
	if _, err := h.m.pollOnce(h.ctx, p, &cur); err != nil {
		t.Fatal(err)
	}
	if got := messages(s); got["GET /late 200 0ms"] != 1 {
		t.Fatalf("delivered %v", got)
	}
}

func TestBudgetThrottle(t *testing.T) {
	defer telemetry.ObserveRateLimit(10000, 10000, time.Time{})
	cases := []struct {
		remaining int
		want      float64
	}{
		{10000, 1}, {2000, 1}, {1000, 2}, {100, maxThrottle}, {0, maxThrottle},
	}
	for _, c := range cases {
		telemetry.ObserveRateLimit(c.remaining, 10000, time.Time{})
		if got := budgetThrottle(); got != c.want {
			t.Errorf("remaining %d: throttle %v, want %v", c.remaining, got, c.want)
		}
	}
}

func TestHTTPPollerSkipsWhatLokiWouldReject(t *testing.T) {
	src := &pagedSource{}
	src.http = []railway.HTTPLogEntry{
		{Timestamp: testNow.Add(-2 * time.Hour).Format(time.RFC3339Nano), Method: "GET", Path: "/stale", HTTPStatus: 200},
		{Timestamp: testNow.Add(-30 * time.Minute).Format(time.RFC3339Nano), Method: "GET", Path: "/fresh", HTTPStatus: 200},
	}
	s := &fakeSink{name: "a"}
	h := newPollHarness(t, src, pollConfig(), sinks(s))

	// The deploy poller has already written up to now; this http poller fell
	// 3h behind.
	d := h.deploy(testNow)
	d.setCursor(testNow)
	h.m.deployPollers[testEnvID] = d

	before := testutil.ToFloat64(telemetry.LogCatchUpSkipped.WithLabelValues("http"))
	ht := svcContext{envID: testEnvID, deploymentID: "dep-1", serviceID: testSvcID, serviceName: testSvcName}
	p := h.m.httpPoller(ht, testNow.Add(-3*time.Hour))
	cur := Cursor{TS: p.start}
	if _, err := h.m.pollOnce(h.ctx, p, &cur); err != nil {
		t.Fatal(err)
	}
	if got := messages(s); got["GET /stale 200 0ms"] != 0 || got["GET /fresh 200 0ms"] != 1 {
		t.Fatalf("delivered %v", got)
	}
	skipped := testutil.ToFloat64(telemetry.LogCatchUpSkipped.WithLabelValues("http")) - before
	if want := (3*time.Hour - maxKindLag).Seconds(); skipped != want {
		t.Fatalf("counted %vs skipped, want %v", skipped, want)
	}
}
