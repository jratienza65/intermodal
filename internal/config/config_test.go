package config

import (
	"strconv"
	"testing"
	"time"

	"github.com/jratienza65/intermodal/internal/railway"
)

// envMap builds a Getenv from a map.
func envMap(m map[string]string) Getenv {
	return func(k string) string { return m[k] }
}

func TestResolveTokenAccount(t *testing.T) {
	c, err := Load(envMap(map[string]string{"RAILWAY_API_TOKEN": "acc"}))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.Token != "acc" || c.TokenType != railway.TokenAccount {
		t.Fatalf("got token=%q type=%v, want acc/account", c.Token, c.TokenType)
	}
}

func TestResolveTokenProjectAliases(t *testing.T) {
	for _, key := range []string{"RAILWAY_TOKEN", "RAILWAY_PROJECT_TOKEN", "INTERMODAL_PROJECT_TOKEN"} {
		c, err := Load(envMap(map[string]string{key: "proj"}))
		if err != nil {
			t.Fatalf("Load(%s): %v", key, err)
		}
		if c.Token != "proj" || c.TokenType != railway.TokenProject {
			t.Fatalf("%s: got token=%q type=%v, want proj/project", key, c.Token, c.TokenType)
		}
	}
}

func TestResolveTokenBothIsError(t *testing.T) {
	_, err := Load(envMap(map[string]string{"RAILWAY_API_TOKEN": "a", "RAILWAY_TOKEN": "b"}))
	if err == nil {
		t.Fatal("expected error when both token types set")
	}
}

func TestResolveTokenNoneIsError(t *testing.T) {
	if _, err := Load(envMap(map[string]string{})); err == nil {
		t.Fatal("expected error when no token set")
	}
}

func TestHTTPAddrPrecedence(t *testing.T) {
	// explicit addr wins
	c, _ := Load(envMap(map[string]string{"RAILWAY_API_TOKEN": "a", "INTERMODAL_HTTP_ADDR": "127.0.0.1:9", "PORT": "1234"}))
	if c.HTTPAddr != "127.0.0.1:9" {
		t.Errorf("explicit addr: got %q", c.HTTPAddr)
	}
	// PORT second
	c, _ = Load(envMap(map[string]string{"RAILWAY_API_TOKEN": "a", "PORT": "1234"}))
	if c.HTTPAddr != "0.0.0.0:1234" {
		t.Errorf("PORT addr: got %q", c.HTTPAddr)
	}
	// default last
	c, _ = Load(envMap(map[string]string{"RAILWAY_API_TOKEN": "a"}))
	if c.HTTPAddr != "0.0.0.0:8080" {
		t.Errorf("default addr: got %q", c.HTTPAddr)
	}
}

func TestValidateLokiRequiresURL(t *testing.T) {
	_, err := Load(envMap(map[string]string{
		"RAILWAY_API_TOKEN": "a",
		"INTERMODAL_SINKS":  "loki",
	}))
	if err == nil {
		t.Fatal("expected error: loki enabled without URL")
	}
}

func TestValidateOTLPRequiresEndpoint(t *testing.T) {
	_, err := Load(envMap(map[string]string{
		"RAILWAY_API_TOKEN":            "a",
		"INTERMODAL_METRICS_EXPORTERS": "otlp",
	}))
	if err == nil {
		t.Fatal("expected error: otlp exporter without endpoint")
	}
}

func TestValidateBothDisabled(t *testing.T) {
	_, err := Load(envMap(map[string]string{
		"RAILWAY_API_TOKEN":          "a",
		"INTERMODAL_METRICS_ENABLED": "false",
		"INTERMODAL_LOGS_ENABLED":    "false",
	}))
	if err == nil {
		t.Fatal("expected error: both metrics and logs disabled")
	}
}

func TestListAndKVParsing(t *testing.T) {
	c, err := Load(envMap(map[string]string{
		"RAILWAY_API_TOKEN":      "a",
		"INTERMODAL_PROJECTS":    " p1 , p2 ,, p3 ",
		"INTERMODAL_LOKI_URL":    "http://loki:3100",
		"INTERMODAL_SINKS":       "loki",
		"INTERMODAL_LOKI_LABELS": "team=core , env=prod",
	}))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if len(c.Projects) != 3 || c.Projects[0] != "p1" || c.Projects[2] != "p3" {
		t.Errorf("Projects parse: %#v", c.Projects)
	}
	if c.LokiExtraLabels["team"] != "core" || c.LokiExtraLabels["env"] != "prod" {
		t.Errorf("kv parse: %#v", c.LokiExtraLabels)
	}
}

func TestLogBatchSizeClampAndFlags(t *testing.T) {
	c, err := Load(envMap(map[string]string{
		"RAILWAY_API_TOKEN":             "a",
		"INTERMODAL_LOG_BATCH_SIZE":     "999999",
		"INTERMODAL_METRICS_EXPORTERS":  "prometheus,otlp",
		"INTERMODAL_OTLP_ENDPOINT":      "otel:4318",
		"INTERMODAL_METRICS_AUTH_TOKEN": "sekret",
	}))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.LogBatchSize != 50000 {
		t.Errorf("LogBatchSize clamp = %d, want 50000", c.LogBatchSize)
	}
	// LogQueueSize floor is at least LogBatchSize.
	if c.LogQueueSize < c.LogBatchSize {
		t.Errorf("LogQueueSize %d < LogBatchSize %d", c.LogQueueSize, c.LogBatchSize)
	}
	if !c.MetricExportersSet {
		t.Error("MetricExportersSet should be true when the env var is set")
	}
	if c.LogSinksSet {
		t.Error("LogSinksSet should be false when INTERMODAL_SINKS is unset")
	}
	if c.MetricsAuthToken != "sekret" {
		t.Errorf("MetricsAuthToken = %q", c.MetricsAuthToken)
	}
}

func TestSelfMetricsToggles(t *testing.T) {
	// Default: both on.
	c, _ := Load(envMap(map[string]string{"RAILWAY_API_TOKEN": "a"}))
	if !c.SelfMetricsScrape || !c.SelfMetricsExport {
		t.Errorf("defaults should be on: scrape=%v export=%v", c.SelfMetricsScrape, c.SelfMetricsExport)
	}
	// Master off turns both off.
	c, _ = Load(envMap(map[string]string{"RAILWAY_API_TOKEN": "a", "INTERMODAL_SELF_METRICS": "false"}))
	if c.SelfMetricsScrape || c.SelfMetricsExport {
		t.Errorf("master off should disable both: scrape=%v export=%v", c.SelfMetricsScrape, c.SelfMetricsExport)
	}
	// Independent override: scrape off, export on.
	c, _ = Load(envMap(map[string]string{"RAILWAY_API_TOKEN": "a", "INTERMODAL_SELF_METRICS_SCRAPE": "false"}))
	if c.SelfMetricsScrape || !c.SelfMetricsExport {
		t.Errorf("scrape override: scrape=%v export=%v (want false/true)", c.SelfMetricsScrape, c.SelfMetricsExport)
	}
	// Master off, but export explicitly on.
	c, _ = Load(envMap(map[string]string{"RAILWAY_API_TOKEN": "a", "INTERMODAL_SELF_METRICS": "false", "INTERMODAL_SELF_METRICS_EXPORT": "true"}))
	if c.SelfMetricsScrape || !c.SelfMetricsExport {
		t.Errorf("export re-enable over master: scrape=%v export=%v (want false/true)", c.SelfMetricsScrape, c.SelfMetricsExport)
	}
}

func TestDefaults(t *testing.T) {
	c, err := Load(envMap(map[string]string{"RAILWAY_API_TOKEN": "a"}))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.PollInterval != 60*time.Second {
		t.Errorf("PollInterval default: %v", c.PollInterval)
	}
	if !c.MetricsEnabled || !c.LogsEnabled {
		t.Error("metrics/logs should default enabled")
	}
	if !c.MetricsExporterEnabled("prometheus") {
		t.Error("prometheus exporter should be default-enabled")
	}
	if !c.LogSinkEnabled("stdout") {
		t.Error("stdout sink should be default")
	}
}

// Railway injects the identity variables into every service, so an intermodal
// on Railway identifies itself with no configuration at all.
func TestIdentityFromRailwayEnv(t *testing.T) {
	c, err := Load(envMap(map[string]string{
		"RAILWAY_API_TOKEN":        "a",
		"RAILWAY_PROJECT_ID":       "p-1",
		"RAILWAY_PROJECT_NAME":     "PSMND.DEV",
		"RAILWAY_ENVIRONMENT_ID":   "e-1",
		"RAILWAY_ENVIRONMENT_NAME": "prod",
		"RAILWAY_SERVICE_ID":       "s-1",
		"RAILWAY_SERVICE_NAME":     "intermodal",
	}))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	want := Identity{
		InstanceID:      "s-1",
		ProjectID:       "p-1",
		ProjectName:     "PSMND.DEV",
		EnvironmentID:   "e-1",
		EnvironmentName: "prod",
		ServiceID:       "s-1",
		ServiceName:     "intermodal",
	}
	if c.Identity != want {
		t.Fatalf("identity = %+v, want %+v", c.Identity, want)
	}
}

func TestInstanceIDPrecedence(t *testing.T) {
	cases := []struct {
		name string
		env  map[string]string
		want string
	}{
		{"explicit wins", map[string]string{
			"INTERMODAL_INSTANCE_ID": "replica-7",
			"RAILWAY_SERVICE_ID":     "s-1",
			"HOSTNAME":               "box",
		}, "replica-7"},
		{"railway service id second", map[string]string{
			"RAILWAY_SERVICE_ID": "s-1",
			"HOSTNAME":           "box",
		}, "s-1"},
		{"hostname last", map[string]string{"HOSTNAME": "box"}, "box"},
		{"empty off railway", map[string]string{}, ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			env := map[string]string{"RAILWAY_API_TOKEN": "a"}
			for k, v := range tc.env {
				env[k] = v
			}
			c, err := Load(envMap(env))
			if err != nil {
				t.Fatalf("Load: %v", err)
			}
			if c.Identity.InstanceID != tc.want {
				t.Errorf("instance id = %q, want %q", c.Identity.InstanceID, tc.want)
			}
		})
	}
}

// The self-metric resource must carry service.instance.id, because that is
// what the OTLP-to-Prometheus translation turns into the `instance` label. Two
// intermodal instances without it write one shared series.
func TestSelfResourceAttributes(t *testing.T) {
	c, err := Load(envMap(map[string]string{
		"RAILWAY_API_TOKEN":    "a",
		"RAILWAY_SERVICE_ID":   "s-1",
		"RAILWAY_PROJECT_NAME": "PSMND.DEV",
	}))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	got := attrMap(c.SelfResourceAttributes())
	for k, want := range map[string]string{
		"service.name":         "intermodal",
		"service.instance.id":  "s-1",
		"railway.service.id":   "s-1",
		"railway.project.name": "PSMND.DEV",
	} {
		if got[k] != want {
			t.Errorf("%s = %q, want %q", k, got[k], want)
		}
	}
	// Unset Railway variables must not appear as empty attributes.
	if _, ok := got["railway.environment.id"]; ok {
		t.Errorf("empty identity field exported: %#v", got)
	}
}

func TestResourceAttrsOverride(t *testing.T) {
	for _, key := range []string{"INTERMODAL_RESOURCE_ATTRIBUTES", "OTEL_RESOURCE_ATTRIBUTES"} {
		c, err := Load(envMap(map[string]string{
			"RAILWAY_API_TOKEN":  "a",
			"RAILWAY_SERVICE_ID": "s-1",
			key:                  "service.instance.id=custom,deployment.tier=edge",
		}))
		if err != nil {
			t.Fatalf("Load(%s): %v", key, err)
		}
		attrs := c.SelfResourceAttributes()
		got := attrMap(attrs)
		if got["service.instance.id"] != "custom" {
			t.Errorf("%s: instance id = %q, want custom", key, got["service.instance.id"])
		}
		if got["deployment.tier"] != "edge" {
			t.Errorf("%s: extra attribute = %q, want edge", key, got["deployment.tier"])
		}
		if len(got) != len(attrs) {
			t.Errorf("%s: duplicate keys in %+v", key, attrs)
		}
	}
}

func attrMap(attrs []Attr) map[string]string {
	m := make(map[string]string, len(attrs))
	for _, a := range attrs {
		m[a.Key] = a.Value
	}
	return m
}

func TestConcurrencyDefaults(t *testing.T) {
	c, err := Load(envMap(map[string]string{"RAILWAY_API_TOKEN": "a"}))
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if c.PollConcurrency != DefaultConcurrency || c.BuildLogConcurrency != DefaultConcurrency {
		t.Fatalf("defaults = poll %d / build %d, want %d for both",
			c.PollConcurrency, c.BuildLogConcurrency, DefaultConcurrency)
	}
}

// One master knob moves every fan-out; a per-subsystem knob overrides it.
func TestConcurrencyMasterAndOverrides(t *testing.T) {
	c, _ := Load(envMap(map[string]string{
		"RAILWAY_API_TOKEN":      "a",
		"INTERMODAL_CONCURRENCY": "12",
	}))
	if c.PollConcurrency != 12 || c.BuildLogConcurrency != 12 {
		t.Errorf("master: poll %d / build %d, want 12 for both", c.PollConcurrency, c.BuildLogConcurrency)
	}

	c, _ = Load(envMap(map[string]string{
		"RAILWAY_API_TOKEN":                "a",
		"INTERMODAL_CONCURRENCY":           "12",
		"INTERMODAL_POLL_CONCURRENCY":      "2",
		"INTERMODAL_BUILD_LOG_CONCURRENCY": "30",
	}))
	if c.PollConcurrency != 2 {
		t.Errorf("poll override = %d, want 2", c.PollConcurrency)
	}
	if c.BuildLogConcurrency != 30 {
		t.Errorf("build override = %d, want 30", c.BuildLogConcurrency)
	}
}

// A zero or negative value must never reach a semaphore: it would size the
// channel at 0 and block the fan-out forever.
func TestConcurrencyClamp(t *testing.T) {
	for _, tc := range []struct{ set, wantPoll string }{
		{"0", "1"}, {"-5", "1"}, {"9999", "256"}, {"not-a-number", "4"},
	} {
		c, err := Load(envMap(map[string]string{
			"RAILWAY_API_TOKEN":      "a",
			"INTERMODAL_CONCURRENCY": tc.set,
		}))
		if err != nil {
			t.Fatalf("Load(%s): %v", tc.set, err)
		}
		want, _ := strconv.Atoi(tc.wantPoll)
		if c.PollConcurrency != want || c.BuildLogConcurrency != want {
			t.Errorf("%q: poll %d / build %d, want %d", tc.set, c.PollConcurrency, c.BuildLogConcurrency, want)
		}
		if c.PollConcurrency < 1 {
			t.Errorf("%q: concurrency below 1 would deadlock the fan-out", tc.set)
		}
	}
}
