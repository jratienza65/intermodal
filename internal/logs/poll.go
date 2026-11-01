package logs

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"maps"
	"sort"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/jratienza65/intermodal/internal/model"
	"github.com/jratienza65/intermodal/internal/railway"
	"github.com/jratienza65/intermodal/internal/target"
	"github.com/jratienza65/intermodal/internal/telemetry"
)

// Railway's log subscriptions deliver only a fraction of the lines it stores,
// so by default intermodal polls the log history queries instead. Each poller
// owns one source (an environment's deploy logs, or a deployment's HTTP logs)
// and a Cursor. A poll round pages forward from the cursor up to now-settle,
// waits until every sink has accepted the lines, and only then moves and saves
// the cursor. A restart resumes from the saved cursor, so an outage is caught
// up rather than lost. "Accepted" is the sink's own reply: for OTLP that is the
// first collector, and a later hop can still drop the lines.

const (
	// pollPageSize is the afterLimit of each history query.
	pollPageSize = 1000
	// maxKindSkew is how far an environment's HTTP pollers may run ahead of
	// its deploy poller. Behind an OTLP collector a service's deploy and HTTP
	// lines often share one Loki stream, and Loki rejects lines more than 1h
	// behind a stream's newest entry. Without the gate, fast HTTP pollers
	// finish a catch-up first and the deploy lines behind them are rejected.
	maxKindSkew = 45 * time.Minute
	// maxKindLag is how far an HTTP poller may fall behind its environment's
	// deploy poller, the same Loki window seen from the other side. A poller
	// further behind (say, after httpLogs kept failing) would only send lines
	// Loki rejects, silently, so it skips ahead and counts the gap instead.
	// Deploy never waits on HTTP: one broken HTTP poller must not stall the
	// environment's deploy logs.
	maxKindLag = 50 * time.Minute
	// budgetLowWater is the fraction of Railway's hourly request budget under
	// which pollers slow down, in proportion to how little is left.
	budgetLowWater = 0.2
	// maxThrottle caps that slowdown, as a multiple of the normal wait.
	maxThrottle = 20
	// catchUpPageWait is the wait between pages of one round while throttled.
	catchUpPageWait = time.Second
)

var errDelivery = errors.New("a sink dropped part of the page")

// pollEntry is one fetched line, converted and hashed.
type pollEntry struct {
	ts   time.Time
	hash string
	rec  model.LogRecord
}

// poller is one polled log source.
type poller struct {
	key   string // cursor key
	kind  model.LogKind
	start time.Time // where to begin when no cursor is stored
	// Lag gauge labels.
	project, environment, service string
	fetch                         func(ctx context.Context, anchor time.Time) ([]pollEntry, error)
	// gate, when set, caps the horizon: lines past it wait for a later round.
	gate func() (time.Time, bool)
	// floor, when set, is the oldest position worth reading; a cursor behind
	// it skips ahead.
	floor func() (time.Time, bool)

	// pos is the live cursor timestamp (unix nanoseconds), read by gates and
	// by the lag metric.
	pos atomic.Int64
	lag *telemetry.CursorTracker

	// retire is closed when the source has left discovery (a replaced
	// deployment). The poller then drains up to retireAt + settle and exits.
	retire   chan struct{}
	retireAt time.Time
}

// setCursor publishes the poller's position.
func (p *poller) setCursor(ts time.Time) {
	p.pos.Store(ts.UnixNano())
	if p.lag != nil {
		p.lag.Set(ts)
	}
}

func (p *poller) position() time.Time { return time.Unix(0, p.pos.Load()) }

// deployPoller polls deploy logs: one environment's (svc nil, one query
// covers every service), or one service's, through a @service:<id> filter
// on the same environment query.
func (m *Manager) deployPoller(env string, tg target.Target, svc *target.Service, start time.Time) *poller {
	key, filter, service := "deploy|"+env, m.deployFilter, ""
	if svc != nil {
		key += "|" + svc.ID
		filter = serviceFilter(svc.ID, m.deployFilter)
		service = svc.Name
	}
	return &poller{
		key: key, kind: model.KindDeploy, start: start,
		project: tg.ProjectName, environment: tg.EnvironmentName, service: service,
		fetch: func(ctx context.Context, anchor time.Time) ([]pollEntry, error) {
			page, err := m.src.EnvironmentLogsPage(ctx, railway.LogPageParams{
				ID: env, Filter: filter, Anchor: anchor, AfterLimit: pollPageSize,
			})
			if err != nil {
				return nil, err
			}
			out := make([]pollEntry, 0, len(page))
			for _, e := range page {
				rec, ok := m.convert(e)
				if !ok {
					m.log.Debug("logs: skipping deploy line without a timestamp", "environment_id", env)
					continue
				}
				out = append(out, pollEntry{ts: rec.Timestamp, hash: entryHash(e), rec: rec})
			}
			return out, nil
		},
		retire: make(chan struct{}),
	}
}

// httpPoller polls one deployment's HTTP logs.
func (m *Manager) httpPoller(ht svcContext, start time.Time) *poller {
	return &poller{
		key: "http|" + ht.deploymentID, kind: model.KindHTTP, start: start,
		project: ht.projectName, environment: ht.envName, service: ht.serviceName,
		gate: func() (time.Time, bool) {
			d, ok := m.deployPollerOf(m.deploySourceKey(ht.envID, ht.serviceID))
			if !ok {
				return time.Time{}, false
			}
			return d.position().Add(maxKindSkew), true
		},
		floor: func() (time.Time, bool) {
			d, ok := m.deployPollerOf(m.deploySourceKey(ht.envID, ht.serviceID))
			if !ok {
				return time.Time{}, false
			}
			return d.position().Add(-maxKindLag), true
		},
		fetch: func(ctx context.Context, anchor time.Time) ([]pollEntry, error) {
			page, err := m.src.HTTPLogsPage(ctx, railway.LogPageParams{
				ID: ht.deploymentID, Filter: m.httpFilter, Anchor: anchor, AfterLimit: pollPageSize,
			})
			if err != nil {
				return nil, err
			}
			out := make([]pollEntry, 0, len(page))
			for _, e := range page {
				rec, ok := m.convertHTTP(e, ht)
				if !ok {
					m.log.Debug("logs: skipping http line without a timestamp", "deployment_id", ht.deploymentID)
					continue
				}
				out = append(out, pollEntry{ts: rec.Timestamp, hash: entryHash(e), rec: rec})
			}
			return out, nil
		},
		retire: make(chan struct{}),
	}
}

// serviceFilter narrows a deploy-log query to one service, keeping the user's
// INTERMODAL_LOG_FILTER. A filter with OR is grouped so the service term
// applies to every alternative, not only the first.
func serviceFilter(serviceID, userFilter string) string {
	f := "@service:" + serviceID
	userFilter = strings.TrimSpace(userFilter)
	switch {
	case userFilter == "":
		return f
	case strings.Contains(" "+strings.ToUpper(userFilter)+" ", " OR "):
		return f + " (" + userFilter + ")"
	default:
		return f + " " + userFilter
	}
}

func (m *Manager) deployPollerOf(key string) (*poller, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	d, ok := m.deployPollers[key]
	return d, ok
}

// entryHash identifies a raw Railway line, to recognise it when a page
// boundary returns it again.
func entryHash(e any) string {
	b, _ := json.Marshal(e)
	h := fnv.New64a()
	h.Write(b)
	return strconv.FormatUint(h.Sum64(), 16)
}

// runPoller polls one source until ctx ends, or until a retired source has
// been drained.
func (m *Manager) runPoller(ctx context.Context, p *poller) {
	cur, ok := m.cursors.Load(p.key)
	if ok {
		m.log.Info("logs: resuming from stored cursor", "source", p.key, "cursor", cur.TS)
	} else {
		cur = Cursor{TS: p.start}
	}
	p.lag = telemetry.LogCursorLag.Track(string(p.kind), p.project, p.environment, p.service, cur.TS)
	defer p.lag.Stop()
	p.setCursor(cur.TS)

	interval := m.pollMin
	for {
		n, err := m.pollOnce(ctx, p, &cur)
		if ctx.Err() != nil {
			return
		}
		switch {
		case err != nil:
			telemetry.LogPollErrors.WithLabelValues(string(p.kind)).Inc()
			m.log.Warn("logs: poll failed; retrying from cursor", "source", p.key, "cursor", cur.TS, "err", err)
			interval = min(interval*2, m.pollMax)
		case n > 0:
			interval = m.pollMin
		default:
			interval = min(interval*2, m.pollMax)
		}
		if f := budgetThrottle(); f > 1 {
			interval = time.Duration(float64(max(interval, m.pollMax)) * f)
		}

		select {
		case <-p.retire:
			if err == nil && !cur.TS.Before(p.retireAt) {
				m.log.Info("logs: drained retired source", "source", p.key)
				if err := m.cursors.Delete(p.key); err != nil {
					m.log.Warn("logs: delete cursor failed", "source", p.key, "err", err)
				}
				return
			}
			// Lines up to retireAt reach Railway within the settle window, so
			// drain at the fast interval.
			interval = m.pollMin
		default:
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(interval):
		}
	}
}

// pollOnce delivers everything from the cursor up to now-settle and returns
// how many lines it delivered. On error the cursor stays at the last page the
// sinks fully wrote.
func (m *Manager) pollOnce(ctx context.Context, p *poller, cur *Cursor) (int, error) {
	now := m.now()
	horizon := now.Add(-m.settle)
	if floor := now.Add(-m.maxCatchUp); cur.TS.Before(floor) {
		m.log.Warn("logs: cursor is older than the catch-up limit; skipping ahead (use the backfill tool for the gap)",
			"source", p.key, "cursor", cur.TS, "skip_to", floor)
		telemetry.LogCatchUpSkipped.WithLabelValues(string(p.kind)).Add(floor.Sub(cur.TS).Seconds())
		*cur = Cursor{TS: floor}
		p.setCursor(cur.TS)
	}
	if p.floor != nil {
		if floor, ok := p.floor(); ok && cur.TS.Before(floor) {
			m.log.Warn("logs: too far behind the deploy logs sharing its Loki stream; skipping ahead (use the backfill tool for the gap)",
				"source", p.key, "gap_start", cur.TS, "gap_end", floor)
			telemetry.LogCatchUpSkipped.WithLabelValues(string(p.kind)).Add(floor.Sub(cur.TS).Seconds())
			*cur = Cursor{TS: floor}
			p.setCursor(cur.TS)
		}
	}
	if p.gate != nil {
		if g, ok := p.gate(); ok && g.Before(horizon) {
			horizon = g
		}
	}
	if !horizon.After(cur.TS) {
		return 0, nil
	}

	delivered := 0
	for first := true; ; first = false {
		if f := budgetThrottle(); f > 1 && !first {
			select {
			case <-ctx.Done():
				return delivered, ctx.Err()
			case <-time.After(time.Duration(float64(catchUpPageWait) * f)):
			}
		}
		page, err := p.fetch(ctx, cur.TS)
		if err != nil {
			return delivered, err
		}
		sort.SliceStable(page, func(i, j int) bool { return page[i].ts.Before(page[j].ts) })

		next := cur.clone()
		seen := maps.Clone(cur.Seen)
		marks := m.marks()
		beyond := false
		for _, e := range page {
			if e.ts.Before(cur.TS) {
				continue
			}
			if e.ts.After(horizon) {
				beyond = true
				break
			}
			if e.ts.Equal(cur.TS) && seen[e.hash] > 0 {
				seen[e.hash]--
				continue
			}
			if err := m.dispatchWait(ctx, e.rec); err != nil {
				return delivered, err
			}
			delivered++
			next.advance(e.ts, e.hash)
		}
		if err := m.waitWritten(ctx, marks); err != nil {
			return delivered, err
		}

		full := len(page) >= pollPageSize
		stuck := full && !beyond && !next.TS.After(cur.TS)
		*cur = next
		p.setCursor(cur.TS)
		if beyond || !full {
			break
		}
		if stuck {
			// A full page of lines sharing one timestamp: anchorDate cannot
			// page past it. Step over the timestamp; lines beyond this page
			// at that exact instant are lost.
			m.log.Warn("logs: more than a page of lines share one timestamp; skipping past it", "source", p.key, "ts", cur.TS)
			telemetry.LogTimestampSkips.WithLabelValues(string(p.kind)).Inc()
			*cur = Cursor{TS: cur.TS.Add(time.Nanosecond)}
			p.setCursor(cur.TS)
		}
		if err := m.cursors.Save(p.key, *cur); err != nil {
			m.log.Warn("logs: save cursor failed", "source", p.key, "err", err)
		}
	}

	// Everything up to the horizon is delivered, so the cursor can move there
	// even if the source was quiet. That keeps lag honest for idle services.
	if cur.TS.Before(horizon) {
		*cur = Cursor{TS: horizon}
		p.setCursor(cur.TS)
	}
	if err := m.cursors.Save(p.key, *cur); err != nil {
		m.log.Warn("logs: save cursor failed", "source", p.key, "err", err)
	}
	return delivered, nil
}

// budgetThrottle returns how much to stretch waits: 1 while more than
// budgetLowWater of Railway's request budget is left, rising in proportion
// as it runs out, up to maxThrottle. The budget is per token, so other
// intermodal instances and tools on the same token draw from it too.
func budgetThrottle() float64 {
	remaining, limit := telemetry.RateLimitBudget()
	if remaining < 0 || limit <= 0 {
		return 1
	}
	frac := float64(remaining) / float64(limit)
	if frac >= budgetLowWater {
		return 1
	}
	if remaining == 0 {
		return maxThrottle
	}
	return min(budgetLowWater/frac, maxThrottle)
}

// dispatchWait is dispatch with backpressure: it blocks while a sink queue is
// full instead of dropping.
func (m *Manager) dispatchWait(ctx context.Context, rec model.LogRecord) error {
	telemetry.LogsReceived.Inc()
	if !inScope(m.services, rec.ServiceID, rec.ServiceName) {
		return nil
	}
	for _, w := range m.workers {
		if err := w.enqueueWait(ctx, rec); err != nil {
			return err
		}
	}
	return nil
}

// marks snapshots every worker's queue position.
func (m *Manager) marks() []uint64 {
	out := make([]uint64, len(m.workers))
	for i, w := range m.workers {
		out[i] = w.mark()
	}
	return out
}

// waitWritten waits until every worker has written what was enqueued since
// marks, and fails if any of it was dropped.
func (m *Manager) waitWritten(ctx context.Context, marks []uint64) error {
	for i, w := range m.workers {
		ok, err := w.waitWritten(ctx, marks[i])
		if err != nil {
			return err
		}
		if !ok {
			return fmt.Errorf("%w (sink %s)", errDelivery, w.sink.Name())
		}
	}
	return nil
}
