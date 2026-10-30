package telemetry

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

var lagDesc = prometheus.NewDesc(
	"intermodal_log_cursor_lag_seconds",
	"Seconds between now and the point up to which a log poller has delivered every line. service is empty for deploy logs, which are polled per environment.",
	[]string{"kind", "project", "environment", "service"}, nil,
)

// lagCollector computes cursor lag when it is collected, so the value keeps
// growing while a poller is stuck in a long catch-up or blocked on a sink.
type lagCollector struct {
	mu       sync.Mutex
	trackers map[*CursorTracker]struct{}
	now      func() time.Time
}

func newLagCollector() *lagCollector {
	return &lagCollector{trackers: map[*CursorTracker]struct{}{}, now: time.Now}
}

// CursorTracker is one poller's live cursor.
type CursorTracker struct {
	c      *lagCollector
	labels [4]string
	ts     atomic.Int64 // unix nanoseconds
}

// Track registers a poller's cursor, starting at ts. The series exists from
// this call on, so a poller that never succeeds is still visible.
func (c *lagCollector) Track(kind, project, environment, service string, ts time.Time) *CursorTracker {
	t := &CursorTracker{c: c, labels: [4]string{kind, project, environment, service}}
	t.Set(ts)
	c.mu.Lock()
	c.trackers[t] = struct{}{}
	c.mu.Unlock()
	return t
}

// Set moves the cursor.
func (t *CursorTracker) Set(ts time.Time) { t.ts.Store(ts.UnixNano()) }

// Stop removes the cursor from the metric.
func (t *CursorTracker) Stop() {
	t.c.mu.Lock()
	delete(t.c.trackers, t)
	t.c.mu.Unlock()
}

func (c *lagCollector) Describe(ch chan<- *prometheus.Desc) { ch <- lagDesc }

// Collect reports the largest lag per label set: during a deployment
// handover the old and new HTTP pollers of a service share labels.
func (c *lagCollector) Collect(ch chan<- prometheus.Metric) {
	now := c.now()
	c.mu.Lock()
	worst := map[[4]string]float64{}
	for t := range c.trackers {
		lag := now.Sub(time.Unix(0, t.ts.Load())).Seconds()
		if cur, ok := worst[t.labels]; !ok || lag > cur {
			worst[t.labels] = lag
		}
	}
	c.mu.Unlock()
	for l, lag := range worst {
		ch <- prometheus.MustNewConstMetric(lagDesc, prometheus.GaugeValue, lag, l[0], l[1], l[2], l[3])
	}
}
