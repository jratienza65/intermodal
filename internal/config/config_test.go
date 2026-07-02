package config

import (
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
