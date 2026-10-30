// Package telemetry defines intermodal's own operational metrics and the
// observer hooks the Railway client and log pipeline report into. Metrics are
// defined as package-level collectors (not auto-registered) and exposed via
// Collectors()/Register so a caller-owned registry can be used in tests.
package telemetry

import (
	"strconv"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var (
	BuildInfo = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Name: "intermodal_build_info",
		Help: "Build information; constant 1, labeled by version.",
	}, []string{"version"})

	APIRequests = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "railway_api_requests_total",
		Help: "Total Railway GraphQL requests, labeled by operation and outcome (HTTP status or \"error\").",
	}, []string{"op", "code"})

	RateLimitRemaining = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "railway_api_rate_limit_remaining",
		Help: "Most recent X-RateLimit-Remaining value reported by Railway.",
	})

	PollSuccess = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "intermodal_metrics_poll_success",
		Help: "1 if the last metrics poll cycle fully succeeded, 0 otherwise.",
	})

	PollDuration = prometheus.NewHistogram(prometheus.HistogramOpts{
		Name:    "intermodal_metrics_poll_duration_seconds",
		Help:    "Duration of a full metrics poll cycle.",
		Buckets: prometheus.DefBuckets,
	})

	PollErrors = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "intermodal_metrics_poll_errors_total",
		Help: "Total per-target errors encountered while polling metrics.",
	})

	TargetsDiscovered = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "intermodal_targets_discovered",
		Help: "Number of Railway environments currently discovered as targets.",
	})

	LogsReceived = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "intermodal_logs_received_total",
		Help: "Total log records received from Railway.",
	})

	LogsForwarded = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "intermodal_logs_forwarded_total",
		Help: "Total log records successfully forwarded, labeled by sink.",
	}, []string{"sink"})

	LogSinkErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "intermodal_log_sink_errors_total",
		Help: "Total sink write errors, labeled by sink.",
	}, []string{"sink"})

	LogsDropped = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "intermodal_logs_dropped_total",
		Help: "Total log records dropped due to a full sink queue, labeled by sink.",
	}, []string{"sink"})

	SubscriptionsActive = prometheus.NewGauge(prometheus.GaugeOpts{
		Name: "intermodal_log_subscriptions_active",
		Help: "Number of currently-open Railway log subscriptions.",
	})

	SubscriptionReconnects = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "intermodal_log_subscription_reconnects_total",
		Help: "Total Railway log subscription reconnect attempts.",
	})

	// LogCursorLag reports intermodal_log_cursor_lag_seconds, computed at
	// collection time from each poller's live cursor.
	LogCursorLag = newLagCollector()

	LogCatchUpSkipped = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "intermodal_log_catchup_skipped_seconds_total",
		Help: "Seconds of log history skipped because a cursor was older than INTERMODAL_LOG_MAX_CATCHUP, labeled by kind. The gap needs a backfill.",
	}, []string{"kind"})

	LogTimestampSkips = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "intermodal_log_timestamp_skips_total",
		Help: "Times a poller stepped past a timestamp holding more than a page of lines (lines beyond the page at that instant are lost), labeled by kind.",
	}, []string{"kind"})

	LogPollErrors = prometheus.NewCounterVec(prometheus.CounterOpts{
		Name: "intermodal_log_poll_errors_total",
		Help: "Total failed log poll rounds (Railway query or sink delivery), labeled by kind.",
	}, []string{"kind"})
)

// rateLimitRemaining and rateLimitLimit mirror Railway's rate-limit headers
// for readers inside the process; -1 until Railway has reported a value.
var rateLimitRemaining, rateLimitLimit atomic.Int64

func init() {
	rateLimitRemaining.Store(-1)
	rateLimitLimit.Store(-1)
}

// RateLimitBudget returns the most recent X-RateLimit-Remaining and
// X-RateLimit-Limit values; either is -1 if Railway has not reported it.
func RateLimitBudget() (remaining, limit int64) {
	return rateLimitRemaining.Load(), rateLimitLimit.Load()
}

// Collectors returns every intermodal self-metric collector.
func Collectors() []prometheus.Collector {
	return []prometheus.Collector{
		BuildInfo, APIRequests, RateLimitRemaining,
		PollSuccess, PollDuration, PollErrors, TargetsDiscovered,
		LogsReceived, LogsForwarded, LogSinkErrors, LogsDropped,
		SubscriptionsActive, SubscriptionReconnects,
		LogCursorLag, LogPollErrors, LogCatchUpSkipped, LogTimestampSkips,
	}
}

// Register registers all self-metrics with r.
func Register(r prometheus.Registerer) error {
	for _, c := range Collectors() {
		if err := r.Register(c); err != nil {
			return err
		}
	}
	return nil
}

// SetBuildInfo records the running version.
func SetBuildInfo(version string) {
	BuildInfo.WithLabelValues(version).Set(1)
}

// ObserveAPIRequest matches railway.Hooks.OnRequest. On a transport error the
// code label is "error"; otherwise it's the HTTP status.
func ObserveAPIRequest(op string, status int, err error) {
	code := "error"
	if err == nil && status > 0 {
		code = strconv.Itoa(status)
	}
	APIRequests.WithLabelValues(op, code).Inc()
}

// ObserveRateLimit matches railway.Hooks.OnRateLimit. limit is -1 when
// Railway sent no X-RateLimit-Limit.
func ObserveRateLimit(remaining, limit int, _ time.Time) {
	RateLimitRemaining.Set(float64(remaining))
	rateLimitRemaining.Store(int64(remaining))
	if limit > 0 {
		rateLimitLimit.Store(int64(limit))
	}
}
