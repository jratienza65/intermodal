package logs

import (
	"context"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jratienza65/intermodal/internal/config"
	"github.com/jratienza65/intermodal/internal/railway"
	"github.com/jratienza65/intermodal/internal/target"
)

// serviceFilterSource answers environmentLogs like Railway with a
// "@service:<id>" filter: only that service's lines. An unfiltered query is
// refused, the way Railway refuses an environment that scans too much data.
type serviceFilterSource struct {
	fakeSource

	mu      sync.Mutex
	lines   []railway.LogEntry
	filters []string
}

func (s *serviceFilterSource) EnvironmentLogsPage(_ context.Context, p railway.LogPageParams) ([]railway.LogEntry, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.filters = append(s.filters, p.Filter)
	svc, ok := strings.CutPrefix(strings.Fields(p.Filter + " x")[0], "@service:")
	if !ok {
		return nil, &railway.GraphQLError{Messages: []string{"This log query scans too much data."}}
	}
	var out []railway.LogEntry
	for _, e := range s.lines {
		ts, _ := time.Parse(time.RFC3339Nano, e.Timestamp)
		if e.Tags.ServiceID != svc || ts.Before(p.Anchor) {
			continue
		}
		out = append(out, e)
		if len(out) > p.AfterLimit {
			break
		}
	}
	return out, nil
}

func (s *serviceFilterSource) seenFilters() []string {
	s.mu.Lock()
	defer s.mu.Unlock()
	seen := map[string]bool{}
	var out []string
	for _, f := range s.filters {
		if !seen[f] {
			seen[f] = true
			out = append(out, f)
		}
	}
	sort.Strings(out)
	return out
}

func serviceLineAt(ts time.Time, svc, msg string) railway.LogEntry {
	e := lineAt(ts, msg)
	e.Tags.ServiceID = svc
	return e
}

func twoServiceTarget() target.Target {
	tg := oneTarget()
	tg.Services = []target.Service{
		{ID: testSvcID, Name: testSvcName, DeploymentID: "dep-1", HasDomain: true},
		{ID: testOtherSvc, Name: "worker", DeploymentID: "dep-2"},
		{ID: "svc-3", Name: "idle"}, // no active deployment: no poller
	}
	return tg
}

func TestPerServiceDeployScopePollsEachService(t *testing.T) {
	src := &serviceFilterSource{lines: []railway.LogEntry{
		serviceLineAt(testNow.Add(-2*time.Minute), testSvcID, "api line"),
		serviceLineAt(testNow.Add(-time.Minute), testOtherSvc, "worker line"),
	}}
	cfg := pollConfig()
	cfg.LogDeployScope = config.LogDeployScopeService
	cfg.LogFilter = "-@level:debug"
	s := &fakeSink{name: "a"}
	h := newPollHarness(t, src, cfg, sinks(s))
	h.m.provider = &fakeProvider{targets: []target.Target{twoServiceTarget()}}
	h.m.startedAt = testNow.Add(-time.Hour)

	h.m.reconcile(h.ctx)

	h.m.mu.RLock()
	var keys []string
	for k, p := range h.m.deployPollers {
		keys = append(keys, k+"="+p.key+"/"+p.service)
	}
	h.m.mu.RUnlock()
	sort.Strings(keys)
	want := []string{
		testEnvID + "|" + testOtherSvc + "=deploy|" + testEnvID + "|" + testOtherSvc + "/worker",
		testEnvID + "|" + testSvcID + "=deploy|" + testEnvID + "|" + testSvcID + "/" + testSvcName,
	}
	sort.Strings(want)
	if strings.Join(keys, ",") != strings.Join(want, ",") {
		t.Fatalf("deploy pollers %v, want %v", keys, want)
	}

	deadline := time.Now().Add(5 * time.Second)
	for {
		got := messages(s)
		if got["api line"] == 1 && got["worker line"] == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("delivered %v", got)
		}
		time.Sleep(5 * time.Millisecond)
	}
	wantFilters := []string{"@service:" + testSvcID + " -@level:debug", "@service:" + testOtherSvc + " -@level:debug"}
	sort.Strings(wantFilters)
	if got := src.seenFilters(); strings.Join(got, "|") != strings.Join(wantFilters, "|") {
		t.Fatalf("filters %q, want %q", got, wantFilters)
	}
}

func TestPerServiceDeployScopeDropsRemovedService(t *testing.T) {
	src := &serviceFilterSource{}
	cfg := pollConfig()
	cfg.LogDeployScope = config.LogDeployScopeService
	h := newPollHarness(t, src, cfg, sinks(&fakeSink{name: "a"}))
	prov := &fakeProvider{targets: []target.Target{twoServiceTarget()}}
	h.m.provider = prov
	h.m.reconcile(h.ctx)

	tg := twoServiceTarget()
	tg.Services = tg.Services[:1]
	prov.targets = []target.Target{tg}
	h.m.reconcile(h.ctx)

	h.m.mu.RLock()
	defer h.m.mu.RUnlock()
	if _, ok := h.m.deployPollers[testEnvID+"|"+testOtherSvc]; ok || len(h.m.deployPollers) != 1 || len(h.m.envSubs) != 1 {
		t.Fatalf("pollers %d subs %d after the service left", len(h.m.deployPollers), len(h.m.envSubs))
	}
}

func TestHTTPPollerGatesOnItsServiceDeployPoller(t *testing.T) {
	cfg := pollConfig()
	cfg.LogDeployScope = config.LogDeployScopeService
	h := newPollHarness(t, &pagedSource{}, cfg, sinks(&fakeSink{name: "a"}))
	tg := twoServiceTarget()

	api := h.m.deployPoller(testEnvID, tg, &tg.Services[0], testNow)
	api.setCursor(testNow.Add(-3 * time.Hour))
	worker := h.m.deployPoller(testEnvID, tg, &tg.Services[1], testNow)
	worker.setCursor(testNow.Add(-time.Minute))
	h.m.deployPollers[h.m.deploySourceKey(testEnvID, testSvcID)] = api
	h.m.deployPollers[h.m.deploySourceKey(testEnvID, testOtherSvc)] = worker

	p := h.m.httpPoller(svcContext{envID: testEnvID, serviceID: testSvcID, deploymentID: "dep-1"}, testNow)
	gate, ok := p.gate()
	if want := testNow.Add(-3*time.Hour + maxKindSkew); !ok || !gate.Equal(want) {
		t.Fatalf("gate %v %v, want %v (the api deploy poller, not the worker's)", gate, ok, want)
	}
}

func TestEnvironmentScopeKeepsOnePollerPerEnvironment(t *testing.T) {
	h := newPollHarness(t, &pagedSource{}, pollConfig(), sinks(&fakeSink{name: "a"}))
	h.m.provider = &fakeProvider{targets: []target.Target{twoServiceTarget()}}
	h.m.reconcile(h.ctx)

	h.m.mu.RLock()
	defer h.m.mu.RUnlock()
	p, ok := h.m.deployPollers[testEnvID]
	if !ok || len(h.m.deployPollers) != 1 || p.key != "deploy|"+testEnvID || p.service != "" {
		t.Fatalf("environment scope pollers: %d", len(h.m.deployPollers))
	}
}

func TestServiceFilter(t *testing.T) {
	for _, c := range []struct{ user, want string }{
		{"", "@service:s1"},
		{"  ", "@service:s1"},
		{"-@level:debug", "@service:s1 -@level:debug"},
		{"@level:error OR @level:warn", "@service:s1 (@level:error OR @level:warn)"},
		{"@level:error or @level:warn", "@service:s1 (@level:error or @level:warn)"},
		{"@msg:ORDER", "@service:s1 @msg:ORDER"},
	} {
		if got := serviceFilter("s1", c.user); got != c.want {
			t.Errorf("serviceFilter(%q) = %q, want %q", c.user, got, c.want)
		}
	}
}
