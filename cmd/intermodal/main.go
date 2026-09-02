// Command intermodal is a one-stop Railway observability agent: it exports
// Railway platform metrics in Prometheus format and streams Railway logs to
// pluggable sinks (Loki, OTLP, stdout). It works with either an account/
// workspace token or a project token.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strings"
	"sync"
	"syscall"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"

	"github.com/jratienza65/intermodal/internal/config"
	"github.com/jratienza65/intermodal/internal/exporter"
	"github.com/jratienza65/intermodal/internal/logs"
	"github.com/jratienza65/intermodal/internal/metrics"
	"github.com/jratienza65/intermodal/internal/railway"
	"github.com/jratienza65/intermodal/internal/server"
	"github.com/jratienza65/intermodal/internal/sink"
	"github.com/jratienza65/intermodal/internal/target"
	"github.com/jratienza65/intermodal/internal/telemetry"
)

// version is overridable at build time via -ldflags "-X main.version=...".
var version = "dev"

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "intermodal: "+err.Error())
		os.Exit(1)
	}
}

func run(args []string) error {
	cmd := "serve"
	if len(args) > 0 {
		cmd = args[0]
	}
	if cmd == "version" || cmd == "--version" || cmd == "-v" {
		fmt.Println("intermodal", version)
		return nil
	}

	cfg, err := config.Load(os.Getenv)
	if err != nil {
		return err
	}
	logger := newLogger(cfg)
	slog.SetDefault(logger)

	client := buildClient(cfg, logger)

	switch cmd {
	case "doctor":
		return doctor(context.Background(), cfg, client, logger)
	case "serve":
		return serve(cfg, client, logger)
	default:
		return fmt.Errorf("unknown command %q (want: serve, doctor, version)", cmd)
	}
}

func serve(cfg *config.Config, client *railway.Client, logger *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	warnContradictoryConfig(cfg, logger)

	provider := target.New(client, cfg, logger)

	// /metrics registry: Go/process runtime + business (railway_service_*) metrics.
	reg := prometheus.NewRegistry()
	reg.MustRegister(collectors.NewGoCollector())
	reg.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))

	// Self-metrics live in their own registry so scrape (/metrics) and export
	// (OTLP) can be toggled independently of each other and of business metrics.
	var selfReg *prometheus.Registry
	if cfg.SelfMetricsScrape || cfg.SelfMetricsExport {
		telemetry.SetBuildInfo(version)
		selfReg = prometheus.NewRegistry()
		if err := telemetry.Register(selfReg); err != nil {
			return fmt.Errorf("register self-metrics: %w", err)
		}
	}
	gatherer := prometheus.Gatherers{reg}
	if cfg.SelfMetricsScrape && selfReg != nil {
		gatherer = append(gatherer, selfReg)
	}

	store := metrics.NewStore()
	var wg sync.WaitGroup

	if cfg.MetricsEnabled {
		if cfg.MetricsExporterEnabled("prometheus") {
			reg.MustRegister(metrics.NewCollector(store))
		}
		poller := metrics.NewPoller(client, provider, store, cfg, logger)
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = poller.Run(ctx)
		}()

		exps, err := buildExporters(cfg, logger)
		if err != nil {
			return err
		}
		if len(exps) > 0 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				exporter.Run(ctx, store, exps, cfg.PollInterval, logger)
			}()
		}

		// Push intermodal's own metrics via OTLP too, when enabled.
		if cfg.SelfMetricsExport && selfReg != nil && cfg.MetricsExporterEnabled("otlp") {
			wg.Add(1)
			go func() {
				defer wg.Done()
				if err := exporter.RunSelfMetrics(ctx, selfReg, cfg, cfg.PollInterval, logger); err != nil {
					logger.Error("self-metrics push stopped", "err", err)
				}
			}()
		}
	}

	if cfg.LogsEnabled {
		sinks, err := buildSinks(cfg, logger)
		if err != nil {
			return err
		}
		mgr := logs.NewManager(client, provider, sinks, cfg, logger)
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = mgr.Run(ctx)
		}()
	}

	srv := server.New(cfg.HTTPAddr, cfg.MetricsAuthToken, gatherer, logger)
	srv.SetReady(true)
	wg.Add(1)
	go func() {
		defer wg.Done()
		if err := srv.Run(ctx); err != nil {
			logger.Error("http server error", "err", err)
			stop() // trigger shutdown of everything else
		}
	}()

	logger.Info("intermodal started",
		"version", version,
		"token_type", cfg.TokenType.String(),
		"metrics_enabled", cfg.MetricsEnabled,
		"metric_exporters", cfg.MetricExporters,
		"poll_concurrency", cfg.PollConcurrency,
		"logs_enabled", cfg.LogsEnabled,
		"log_sinks", cfg.LogSinks,
		"addr", cfg.HTTPAddr,
	)

	<-ctx.Done()
	logger.Info("shutdown signal received; draining")
	wg.Wait()
	logger.Info("stopped")
	return nil
}

// warnContradictoryConfig surfaces configs that are internally inconsistent —
// e.g. a subsystem disabled while its downstream is explicitly configured — so
// they don't silently do nothing.
func warnContradictoryConfig(cfg *config.Config, logger *slog.Logger) {
	if !cfg.MetricsEnabled && cfg.MetricExportersSet {
		logger.Warn("metrics disabled but INTERMODAL_METRICS_EXPORTERS is set; exporters will not run",
			"metric_exporters", cfg.MetricExporters)
	}
	if !cfg.LogsEnabled && cfg.LogSinksSet {
		logger.Warn("logs disabled but INTERMODAL_SINKS is set; sinks will not run",
			"log_sinks", cfg.LogSinks)
	}
}

func buildClient(cfg *config.Config, logger *slog.Logger) *railway.Client {
	return railway.New(railway.Options{
		Auth:          railway.Auth{Token: cfg.Token, Type: cfg.TokenType},
		HTTPEndpoints: cfg.HTTPEndpoints,
		WSEndpoints:   cfg.WSEndpoints,
		RPS:           cfg.RPS,
		Burst:         cfg.Burst,
		MaxRetries:    cfg.MaxRetries,
		UserAgent:     "intermodal/" + version,
		Hooks: railway.Hooks{
			OnRequest:   telemetry.ObserveAPIRequest,
			OnRateLimit: telemetry.ObserveRateLimit,
		},
		Logger: logger,
	})
}

func buildSinks(cfg *config.Config, logger *slog.Logger) ([]sink.Sink, error) {
	var out []sink.Sink
	for _, name := range cfg.LogSinks {
		s, err := sink.Build(name, sink.Options{Config: cfg, Logger: logger})
		if err != nil {
			return nil, err
		}
		out = append(out, s)
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("logs enabled but no sinks configured (INTERMODAL_SINKS)")
	}
	return out, nil
}

func buildExporters(cfg *config.Config, logger *slog.Logger) ([]exporter.Exporter, error) {
	var out []exporter.Exporter
	for _, name := range cfg.MetricExporters {
		if name == "prometheus" {
			continue // pull path, handled by the collector + /metrics
		}
		e, err := exporter.Build(name, exporter.Options{Config: cfg, Logger: logger})
		if err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, nil
}

func newLogger(cfg *config.Config) *slog.Logger {
	level := slog.LevelInfo
	switch strings.ToLower(cfg.LogLevel) {
	case "debug":
		level = slog.LevelDebug
	case "warn", "warning":
		level = slog.LevelWarn
	case "error":
		level = slog.LevelError
	}
	// Log to stderr so the stdout log sink's JSONL stays clean.
	return slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: level}))
}
