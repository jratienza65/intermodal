// Package config loads intermodal's configuration from the environment.
//
// It is env-first by design (Railway is configured via service variables), with
// an INTERMODAL_ prefix for our own knobs and support for Railway's own token
// variable names so it drops in with zero renaming.
package config

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jratienza65/intermodal/internal/railway"
)

// Config is the fully-resolved runtime configuration.
type Config struct {
	// --- Railway auth ---
	Token       string
	TokenType   railway.TokenType
	WorkspaceID string

	// --- Railway client tuning ---
	RPS           float64
	Burst         int
	MaxRetries    int
	HTTPEndpoints []string
	WSEndpoints   []string

	// --- Discovery / target selection ---
	DiscoveryTTL time.Duration
	Projects     []string // allowlist of project IDs (account/workspace token)
	Environments []string // allowlist of environment IDs
	Services     []string // allowlist of service IDs

	// --- HTTP server (Prometheus /metrics + health) ---
	HTTPAddr string
	// MetricsAuthToken, when set, requires a matching `Authorization: Bearer`
	// on /metrics (health probes stay open).
	MetricsAuthToken string

	// --- Metrics ---
	MetricsEnabled    bool
	MetricExporters   []string // "prometheus", "otlp"
	PollInterval      time.Duration
	SampleRateSeconds int
	MetricsWindow     time.Duration // startDate = now - MetricsWindow
	// Self-metrics (intermodal_*, railway_api_*) can be scraped and/or exported
	// independently. SelfMetricsScrape exposes them on /metrics; SelfMetricsExport
	// pushes them via OTLP (when the otlp metric exporter is enabled). Both
	// default to INTERMODAL_SELF_METRICS (default true) and can be overridden.
	SelfMetricsScrape bool
	SelfMetricsExport bool

	// --- Logs ---
	LogsEnabled     bool
	LogSinks        []string // "stdout", "loki", "otlp"
	LogFilter       string   // Railway filter DSL applied to deploy-log subscriptions
	LogBackfill     int      // beforeLimit on connect
	LogQueueSize    int      // per-sink buffered queue depth
	LogBatchSize    int      // max records per sink flush
	LogBatchTimeout time.Duration
	// DeployLogs streams stdout/stderr (environmentLogs); HTTPLogs streams
	// edge/router request logs (httpLogs, only for services with a domain).
	// Both sit under the LogsEnabled master.
	DeployLogs    bool
	HTTPLogs      bool
	HTTPLogFilter string // Railway HTTP-log filter DSL, e.g. "@httpStatus:>=500"
	// HTTPLogServices scopes HTTP-log subscriptions to specific service IDs
	// (independent of INTERMODAL_SERVICES, so it doesn't affect metrics or deploy
	// logs). Empty = every service that has a domain.
	HTTPLogServices []string
	// BuildLogs streams each deployment's build output (finite, fetched once per
	// deployment). BuildLogServices scopes it independently. Empty = all services.
	BuildLogs        bool
	BuildLogServices []string

	// --- Loki sink ---
	LokiURL         string
	LokiTenantID    string
	LokiExtraLabels map[string]string

	// --- OTLP (shared by otlp log sink and otlp metric exporter) ---
	OTLPEndpoint string // e.g. otel-collector.railway.internal:4318 or full URL
	OTLPInsecure bool
	OTLPHeaders  map[string]string

	// --- General ---
	LogLevel    string
	ServiceName string // resource attribute + self-identification

	// Explicit-presence flags (whether the env var was set), used to warn on
	// contradictory config such as "metrics disabled but exporters configured".
	MetricExportersSet bool
	LogSinksSet        bool
}

// MetricsExporterEnabled reports whether the named metric exporter is on.
func (c *Config) MetricsExporterEnabled(name string) bool { return contains(c.MetricExporters, name) }

// LogSinkEnabled reports whether the named log sink is on.
func (c *Config) LogSinkEnabled(name string) bool { return contains(c.LogSinks, name) }

// Getenv is the lookup function shape used by Load, injectable for tests.
type Getenv func(string) string

// Load resolves configuration using the provided environment lookup.
func Load(getenv Getenv) (*Config, error) {
	e := env{getenv}
	c := &Config{
		RPS:               e.float("INTERMODAL_RAILWAY_RPS", 5),
		Burst:             e.int("INTERMODAL_RAILWAY_BURST", 5),
		MaxRetries:        e.int("INTERMODAL_RAILWAY_MAX_RETRIES", 4),
		WorkspaceID:       e.str("INTERMODAL_WORKSPACE_ID", ""),
		DiscoveryTTL:      e.dur("INTERMODAL_DISCOVERY_TTL", 5*time.Minute),
		Projects:          e.list("INTERMODAL_PROJECTS"),
		Environments:      e.list("INTERMODAL_ENVIRONMENTS"),
		Services:          e.list("INTERMODAL_SERVICES"),
		MetricsEnabled:    e.bool("INTERMODAL_METRICS_ENABLED", true),
		MetricExporters:   e.listDefault("INTERMODAL_METRICS_EXPORTERS", []string{"prometheus"}),
		PollInterval:      e.dur("INTERMODAL_POLL_INTERVAL", 60*time.Second),
		SampleRateSeconds: e.int("INTERMODAL_SAMPLE_RATE_SECONDS", 60),
		MetricsWindow:     e.dur("INTERMODAL_METRICS_WINDOW", 10*time.Minute),
		LogsEnabled:       e.bool("INTERMODAL_LOGS_ENABLED", true),
		LogSinks:          e.listDefault("INTERMODAL_SINKS", []string{"stdout"}),
		LogFilter:         e.str("INTERMODAL_LOG_FILTER", ""),
		LogBackfill:       e.int("INTERMODAL_LOG_BACKFILL", 0),
		LogQueueSize:      e.int("INTERMODAL_LOG_QUEUE_SIZE", 10000),
		LogBatchSize:      e.int("INTERMODAL_LOG_BATCH_SIZE", 500),
		LogBatchTimeout:   e.dur("INTERMODAL_LOG_BATCH_TIMEOUT", 2*time.Second),
		DeployLogs:        e.bool("INTERMODAL_DEPLOY_LOGS", true),
		HTTPLogs:          e.bool("INTERMODAL_HTTP_LOGS", false),
		HTTPLogFilter:     e.str("INTERMODAL_HTTP_LOG_FILTER", ""),
		HTTPLogServices:   e.list("INTERMODAL_HTTP_LOG_SERVICES"),
		BuildLogs:         e.bool("INTERMODAL_BUILD_LOGS", false),
		BuildLogServices:  e.list("INTERMODAL_BUILD_LOG_SERVICES"),
		LokiURL:           e.str("INTERMODAL_LOKI_URL", ""),
		LokiTenantID:      e.str("INTERMODAL_LOKI_TENANT_ID", ""),
		LokiExtraLabels:   e.kvMap("INTERMODAL_LOKI_LABELS"),
		OTLPEndpoint:      e.strAny([]string{"INTERMODAL_OTLP_ENDPOINT", "OTEL_EXPORTER_OTLP_ENDPOINT"}, ""),
		OTLPInsecure:      e.bool("INTERMODAL_OTLP_INSECURE", false),
		OTLPHeaders:       e.kvMap("INTERMODAL_OTLP_HEADERS"),
		LogLevel:          e.str("INTERMODAL_LOG_LEVEL", "info"),
		ServiceName:       e.str("INTERMODAL_SERVICE_NAME", "intermodal"),
		MetricsAuthToken:  e.str("INTERMODAL_METRICS_AUTH_TOKEN", ""),

		MetricExportersSet: strings.TrimSpace(getenv("INTERMODAL_METRICS_EXPORTERS")) != "",
		LogSinksSet:        strings.TrimSpace(getenv("INTERMODAL_SINKS")) != "",
	}

	// Clamp batch/queue sizes to sane bounds. LogBatchSize in particular sizes
	// the OTLP sink's queue, so an unbounded value would blow memory.
	c.LogBatchSize = clamp(c.LogBatchSize, 1, 50000)
	c.LogQueueSize = clamp(c.LogQueueSize, c.LogBatchSize, 1_000_000)

	// Self-metrics: a master toggle plus independent scrape/export overrides.
	selfMaster := e.bool("INTERMODAL_SELF_METRICS", true)
	c.SelfMetricsScrape = e.bool("INTERMODAL_SELF_METRICS_SCRAPE", selfMaster)
	c.SelfMetricsExport = e.bool("INTERMODAL_SELF_METRICS_EXPORT", selfMaster)

	// HTTP address: prefer explicit, else Railway's injected PORT, else 8080.
	if addr := e.str("INTERMODAL_HTTP_ADDR", ""); addr != "" {
		c.HTTPAddr = addr
	} else if port := e.str("PORT", ""); port != "" {
		c.HTTPAddr = "0.0.0.0:" + port
	} else {
		c.HTTPAddr = "0.0.0.0:8080"
	}

	if eps := e.list("INTERMODAL_HTTP_ENDPOINTS"); len(eps) > 0 {
		c.HTTPEndpoints = eps
	}
	if eps := e.list("INTERMODAL_WS_ENDPOINTS"); len(eps) > 0 {
		c.WSEndpoints = eps
	}

	if err := c.resolveToken(e); err != nil {
		return nil, err
	}
	if err := c.validate(); err != nil {
		return nil, err
	}
	return c, nil
}

// resolveToken determines the token and its type from the environment.
//
//   - Account/workspace token: RAILWAY_API_TOKEN or INTERMODAL_ACCOUNT_TOKEN (Bearer).
//   - Project token: RAILWAY_PROJECT_TOKEN, RAILWAY_TOKEN, or INTERMODAL_PROJECT_TOKEN.
//
// Setting both an account and a project token is a hard error, since the header
// scheme is mutually exclusive.
func (c *Config) resolveToken(e env) error {
	account := firstNonEmpty(e.str("INTERMODAL_ACCOUNT_TOKEN", ""), e.str("RAILWAY_API_TOKEN", ""))
	project := firstNonEmpty(
		e.str("INTERMODAL_PROJECT_TOKEN", ""),
		e.str("RAILWAY_PROJECT_TOKEN", ""),
		e.str("RAILWAY_TOKEN", ""),
	)

	switch {
	case account != "" && project != "":
		return fmt.Errorf("config: both an account token and a project token are set; provide exactly one (account tokens use Authorization: Bearer, project tokens use Project-Access-Token)")
	case account != "":
		c.Token = account
		c.TokenType = railway.TokenAccount
	case project != "":
		c.Token = project
		c.TokenType = railway.TokenProject
	default:
		return fmt.Errorf("config: no Railway token found; set RAILWAY_API_TOKEN (account/workspace) or RAILWAY_TOKEN/RAILWAY_PROJECT_TOKEN (project)")
	}
	return nil
}

func (c *Config) validate() error {
	if !c.MetricsEnabled && !c.LogsEnabled {
		return fmt.Errorf("config: both metrics and logs are disabled; nothing to do")
	}
	if c.LogsEnabled && c.LogSinkEnabled("loki") && c.LokiURL == "" {
		return fmt.Errorf("config: loki sink enabled but INTERMODAL_LOKI_URL is empty")
	}
	otlpNeeded := (c.LogsEnabled && c.LogSinkEnabled("otlp")) || (c.MetricsEnabled && c.MetricsExporterEnabled("otlp"))
	if otlpNeeded && c.OTLPEndpoint == "" {
		return fmt.Errorf("config: otlp enabled but no OTLP endpoint set (INTERMODAL_OTLP_ENDPOINT or OTEL_EXPORTER_OTLP_ENDPOINT)")
	}
	if c.PollInterval <= 0 {
		return fmt.Errorf("config: INTERMODAL_POLL_INTERVAL must be positive")
	}
	return nil
}

// --- env parsing helpers ---

type env struct{ get Getenv }

func (e env) str(key, def string) string {
	if v := strings.TrimSpace(e.get(key)); v != "" {
		return v
	}
	return def
}

func (e env) strAny(keys []string, def string) string {
	for _, k := range keys {
		if v := strings.TrimSpace(e.get(k)); v != "" {
			return v
		}
	}
	return def
}

func (e env) bool(key string, def bool) bool {
	v := strings.TrimSpace(e.get(key))
	if v == "" {
		return def
	}
	b, err := strconv.ParseBool(v)
	if err != nil {
		return def
	}
	return b
}

func (e env) int(key string, def int) int {
	v := strings.TrimSpace(e.get(key))
	if v == "" {
		return def
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return def
	}
	return n
}

func (e env) float(key string, def float64) float64 {
	v := strings.TrimSpace(e.get(key))
	if v == "" {
		return def
	}
	f, err := strconv.ParseFloat(v, 64)
	if err != nil {
		return def
	}
	return f
}

func (e env) dur(key string, def time.Duration) time.Duration {
	v := strings.TrimSpace(e.get(key))
	if v == "" {
		return def
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return def
	}
	return d
}

// list splits a comma-separated value into trimmed, non-empty items.
func (e env) list(key string) []string {
	return splitList(e.get(key))
}

func (e env) listDefault(key string, def []string) []string {
	if l := e.list(key); len(l) > 0 {
		return l
	}
	return def
}

// kvMap parses "k1=v1,k2=v2" into a map.
func (e env) kvMap(key string) map[string]string {
	raw := splitList(e.get(key))
	if len(raw) == 0 {
		return nil
	}
	m := make(map[string]string, len(raw))
	for _, pair := range raw {
		k, v, ok := strings.Cut(pair, "=")
		if !ok {
			continue
		}
		m[strings.TrimSpace(k)] = strings.TrimSpace(v)
	}
	return m
}

func splitList(s string) []string {
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	return out
}

func clamp(v, lo, hi int) int {
	if v < lo {
		return lo
	}
	if v > hi {
		return hi
	}
	return v
}

func firstNonEmpty(vals ...string) string {
	for _, v := range vals {
		if v != "" {
			return v
		}
	}
	return ""
}

func contains(list []string, s string) bool {
	for _, v := range list {
		if strings.EqualFold(v, s) {
			return true
		}
	}
	return false
}
