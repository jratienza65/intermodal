// Package logs reads logs from Railway, normalizes them, and fans them out to
// sinks. It reads one deploy-log source per target environment and one
// HTTP-log source per (service, active deployment). By default each source is
// a cursor-based poller of Railway's log history (see poll.go); the older
// subscription mode is kept behind INTERMODAL_LOG_SOURCE=subscribe. One worker
// goroutine runs per sink.
package logs

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/jratienza65/intermodal/internal/config"
	"github.com/jratienza65/intermodal/internal/model"
	"github.com/jratienza65/intermodal/internal/railway"
	"github.com/jratienza65/intermodal/internal/sink"
	"github.com/jratienza65/intermodal/internal/target"
	"github.com/jratienza65/intermodal/internal/telemetry"
)

// logSource is the subset of *railway.Client the pipeline needs.
type logSource interface {
	StreamEnvironmentLogs(ctx context.Context, p railway.LogStreamParams, fn func(railway.LogEntry) error) error
	StreamHTTPLogs(ctx context.Context, p railway.HTTPLogStreamParams, fn func(railway.HTTPLogEntry) error) error
	StreamBuildLogs(ctx context.Context, p railway.BuildLogStreamParams, fn func(railway.LogEntry) error) error
	EnvironmentLogsPage(ctx context.Context, p railway.LogPageParams) ([]railway.LogEntry, error)
	HTTPLogsPage(ctx context.Context, p railway.LogPageParams) ([]railway.HTTPLogEntry, error)
}

// svcContext is the per-service enrichment context for an HTTP- or build-log
// subscription (which carry deployment info but no service/project identity).
type svcContext struct {
	projectID, projectName string
	envID, envName         string
	serviceID, serviceName string
	deploymentID           string
}

// Manager coordinates subscriptions and sink workers.
type Manager struct {
	src      logSource
	provider target.Provider
	workers  []*worker
	log      *slog.Logger

	deployEnabled bool
	httpEnabled   bool
	buildEnabled  bool
	deployFilter  string
	httpFilter    string
	backfill      int
	services      map[string]struct{} // service allowlist; empty = all
	httpServices  map[string]struct{} // http-log-only service allowlist; empty = all domained
	buildServices map[string]struct{} // build-log-only service allowlist; empty = all
	reconcileTTL  time.Duration
	// buildSem bounds concurrent build-log fetches. Deploy and HTTP logs are
	// long-lived streams — one goroutine each is the design, and a semaphore
	// there would starve the targets that never got a slot. Build-log fetches
	// are finite, so they queue behind this instead.
	buildSem chan struct{}

	// Poll mode (the default). See poll.go.
	poll            bool
	cursors         CursorStore
	pollMin         time.Duration
	pollMax         time.Duration
	settle          time.Duration
	maxCatchUp      time.Duration
	initialLookback time.Duration
	startedAt       time.Time
	now             func() time.Time

	mu        sync.RWMutex
	envSubs   map[string]context.CancelFunc // deploy logs, by environment id
	httpSubs  map[string]context.CancelFunc // http logs, by "serviceID|deploymentID"
	httpPolls map[string]*poller            // poll mode http logs, same key
	// deployPollers are the poll-mode deploy pollers by environment id; the
	// environment's HTTP pollers gate on them.
	deployPollers map[string]*poller
	// lastReconcile is when discovery last succeeded. A source that appears
	// later was not active before then, so a poller with no cursor starts
	// there.
	lastReconcile time.Time
	buildSeen     map[string]struct{}      // deployment ids whose build logs were fetched
	targets       map[string]target.Target // by environment id, for enrichment
	subWG         sync.WaitGroup
}

// NewManager wraps each sink in a worker and returns a ready manager.
func NewManager(src logSource, provider target.Provider, sinks []sink.Sink, cfg *config.Config, log *slog.Logger) *Manager {
	if log == nil {
		log = slog.Default()
	}
	workers := make([]*worker, 0, len(sinks))
	for _, s := range sinks {
		workers = append(workers, newWorker(s, cfg.LogQueueSize, cfg.LogBatchSize, cfg.LogBatchTimeout, log))
	}
	services := toLowerSet(cfg.Services)
	httpServices := toLowerSet(cfg.HTTPLogServices)
	buildServices := toLowerSet(cfg.BuildLogServices)
	ttl := cfg.DiscoveryTTL
	if ttl <= 0 {
		ttl = 5 * time.Minute
	}
	buildConcurrency := cfg.BuildLogConcurrency
	if buildConcurrency < 1 {
		buildConcurrency = config.DefaultConcurrency
	}
	pollMin := cfg.LogPollInterval
	if pollMin <= 0 {
		pollMin = 10 * time.Second
	}
	pollMax := max(cfg.LogPollMaxInterval, pollMin)
	maxCatchUp := cfg.LogMaxCatchUp
	if maxCatchUp <= 0 {
		maxCatchUp = 6 * time.Hour
	}
	cursors, _ := newCursorStore("", 0) // in memory; never fails
	return &Manager{
		src:             src,
		provider:        provider,
		workers:         workers,
		log:             log,
		deployEnabled:   cfg.DeployLogs,
		httpEnabled:     cfg.HTTPLogs,
		buildEnabled:    cfg.BuildLogs,
		deployFilter:    cfg.LogFilter,
		httpFilter:      cfg.HTTPLogFilter,
		backfill:        cfg.LogBackfill,
		services:        services,
		httpServices:    httpServices,
		buildServices:   buildServices,
		reconcileTTL:    ttl,
		buildSem:        make(chan struct{}, buildConcurrency),
		poll:            cfg.LogSource != config.LogSourceSubscribe,
		cursors:         cursors,
		pollMin:         pollMin,
		pollMax:         pollMax,
		settle:          max(cfg.LogSettle, 0),
		maxCatchUp:      maxCatchUp,
		initialLookback: max(cfg.LogInitialLookback, 0),
		startedAt:       time.Now(),
		now:             time.Now,
		envSubs:         map[string]context.CancelFunc{},
		httpSubs:        map[string]context.CancelFunc{},
		httpPolls:       map[string]*poller{},
		deployPollers:   map[string]*poller{},
		buildSeen:       map[string]struct{}{},
		targets:         map[string]target.Target{},
	}
}

// SetCursorStore makes pollers keep their cursors in s. Call it before Run;
// without it cursors live in memory and a restart resumes from
// INTERMODAL_LOG_INITIAL_LOOKBACK.
func (m *Manager) SetCursorStore(s CursorStore) { m.cursors = s }

// OpenCursorStore opens the cursor file at path, or returns an in-memory store
// when path is empty. Cursors older than pruneAfter are dropped on save.
func OpenCursorStore(path string, pruneAfter time.Duration) (CursorStore, error) {
	return newCursorStore(path, pruneAfter)
}

// Run starts sink workers and subscription management, blocking until ctx is
// cancelled. On shutdown it stops subscriptions, then waits for workers to drain
// and close their sinks.
func (m *Manager) Run(ctx context.Context) error {
	for _, w := range m.workers {
		go w.run(ctx)
	}

	m.reconcile(ctx)
	ticker := time.NewTicker(m.reconcileTTL)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			m.shutdown()
			return ctx.Err()
		case <-ticker.C:
			m.reconcile(ctx)
		}
	}
}

// shutdown cancels all subscriptions and waits for workers to finish draining.
func (m *Manager) shutdown() {
	m.mu.Lock()
	for k, cancel := range m.envSubs {
		cancel()
		delete(m.envSubs, k)
	}
	for k, cancel := range m.httpSubs {
		cancel()
		delete(m.httpSubs, k)
	}
	clear(m.httpPolls) // their contexts are in httpSubs
	clear(m.deployPollers)
	m.mu.Unlock()
	m.subWG.Wait()
	for _, w := range m.workers {
		<-w.done
	}
}

// reconcile aligns running subscriptions with the current target set.
func (m *Manager) reconcile(ctx context.Context) {
	targets, err := m.provider.Targets(ctx)
	if err != nil {
		m.log.Warn("logs: discovery failed; keeping existing subscriptions", "err", err)
		return
	}

	next := make(map[string]target.Target, len(targets))
	for _, t := range targets {
		next[t.EnvironmentID] = t
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	m.targets = next

	// Where a new poller with no cursor starts. On the first reconcile that
	// is shortly before startup, to cover the restart gap; later, a new
	// source was not there at the previous reconcile.
	start := m.startedAt.Add(-m.initialLookback)
	if !m.lastReconcile.IsZero() {
		start = m.lastReconcile
	}
	m.lastReconcile = m.now()

	if m.deployEnabled {
		m.reconcileDeploy(ctx, next, start)
	}
	if m.httpEnabled {
		m.reconcileHTTP(ctx, targets, start)
	}
	if m.buildEnabled {
		m.reconcileBuild(ctx, targets)
	}
}

// reconcileDeploy ensures one deploy-log source per target environment.
// Removed environments stop at once: a redeploy keeps the environment, so
// its poller already covers both the old and the new deployment.
func (m *Manager) reconcileDeploy(ctx context.Context, next map[string]target.Target, start time.Time) {
	for env := range next {
		if _, ok := m.envSubs[env]; ok {
			continue
		}
		sctx, cancel := context.WithCancel(ctx)
		m.envSubs[env] = cancel
		var p *poller
		if m.poll {
			p = m.deployPoller(env, next[env], start)
			// HTTP pollers gate on this from the moment they start, which can
			// be before this poller loads its cursor, so seed it now.
			if c, ok := m.cursors.Load(p.key); ok {
				p.setCursor(c.TS)
			} else {
				p.setCursor(start)
			}
			m.deployPollers[env] = p
		}
		m.subWG.Add(1)
		go func(env string) {
			defer m.subWG.Done()
			if p != nil {
				m.runPoller(sctx, p)
				return
			}
			m.runEnvSubscription(sctx, env)
		}(env)
	}
	for env, cancel := range m.envSubs {
		if _, ok := next[env]; !ok {
			cancel()
			delete(m.envSubs, env)
			delete(m.deployPollers, env)
		}
	}
}

// reconcileHTTP ensures one HTTP-log source per (service, active deployment)
// with a domain. Keying by serviceID|deploymentID means a new deployment
// automatically supersedes the previous source. In poll mode the previous
// deployment's poller first drains the lines it served before the handover.
func (m *Manager) reconcileHTTP(ctx context.Context, targets []target.Target, start time.Time) {
	wanted := map[string]svcContext{}
	for _, t := range targets {
		for _, s := range t.Services {
			if !s.HasDomain || s.DeploymentID == "" {
				continue
			}
			if !inScope(m.httpServices, s.ID, s.Name) {
				continue
			}
			key := s.ID + "|" + s.DeploymentID
			wanted[key] = svcContext{
				projectID: t.ProjectID, projectName: t.ProjectName,
				envID: t.EnvironmentID, envName: t.EnvironmentName,
				serviceID: s.ID, serviceName: s.Name, deploymentID: s.DeploymentID,
			}
		}
	}
	for key, ht := range wanted {
		if _, ok := m.httpSubs[key]; ok {
			continue
		}
		sctx, cancel := context.WithCancel(ctx)
		m.httpSubs[key] = cancel
		if m.poll {
			p := m.httpPoller(ht, start)
			m.httpPolls[key] = p
			m.subWG.Add(1)
			go func(key string) {
				defer m.subWG.Done()
				m.runPoller(sctx, p)
				m.mu.Lock()
				defer m.mu.Unlock()
				if m.httpPolls[key] == p {
					delete(m.httpPolls, key)
					delete(m.httpSubs, key)
				}
				cancel()
			}(key)
			continue
		}
		m.subWG.Add(1)
		go func(ht svcContext) {
			defer m.subWG.Done()
			m.runHTTPSubscription(sctx, ht)
		}(ht)
	}
	for key, cancel := range m.httpSubs {
		if _, ok := wanted[key]; ok {
			continue
		}
		if p, ok := m.httpPolls[key]; ok {
			// The poller removes itself once drained.
			if p.retireAt.IsZero() {
				p.retireAt = m.now()
				close(p.retire)
			}
			continue
		}
		cancel()
		delete(m.httpSubs, key)
	}
}

// runEnvSubscription keeps one environment's deploy-log subscription alive,
// reconnecting with backoff and resuming from the last-seen timestamp.
func (m *Manager) runEnvSubscription(ctx context.Context, envID string) {
	telemetry.SubscriptionsActive.Inc()
	defer telemetry.SubscriptionsActive.Dec()

	var after time.Time
	attempt := 0
	for {
		if ctx.Err() != nil {
			return
		}
		connectedAt := time.Now()
		err := m.src.StreamEnvironmentLogs(ctx, railway.LogStreamParams{
			EnvironmentID: envID,
			Filter:        m.deployFilter,
			AfterDate:     after,
			BeforeLimit:   m.backfill,
		}, func(e railway.LogEntry) error {
			rec, tsOK := m.convert(e)
			if tsOK && rec.Timestamp.After(after) {
				after = rec.Timestamp
			}
			m.dispatch(rec)
			return nil
		})
		if ctx.Err() != nil {
			return
		}
		if time.Since(connectedAt) > time.Minute {
			attempt = 0
		}
		attempt++
		telemetry.SubscriptionReconnects.Inc()
		wait := retryBackoff(attempt)
		m.log.Warn("deploy log subscription ended; reconnecting", "environment_id", envID, "wait", wait, "err", err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
	}
}

// runHTTPSubscription keeps one deployment's HTTP-log subscription alive.
func (m *Manager) runHTTPSubscription(ctx context.Context, ht svcContext) {
	telemetry.SubscriptionsActive.Inc()
	defer telemetry.SubscriptionsActive.Dec()

	var after time.Time
	attempt := 0
	for {
		if ctx.Err() != nil {
			return
		}
		connectedAt := time.Now()
		err := m.src.StreamHTTPLogs(ctx, railway.HTTPLogStreamParams{
			DeploymentID: ht.deploymentID,
			Filter:       m.httpFilter,
			AfterDate:    after,
			BeforeLimit:  m.backfill,
		}, func(e railway.HTTPLogEntry) error {
			rec, tsOK := m.convertHTTP(e, ht)
			if tsOK && rec.Timestamp.After(after) {
				after = rec.Timestamp
			}
			m.dispatch(rec)
			return nil
		})
		if ctx.Err() != nil {
			return
		}
		if time.Since(connectedAt) > time.Minute {
			attempt = 0
		}
		attempt++
		telemetry.SubscriptionReconnects.Inc()
		wait := retryBackoff(attempt)
		m.log.Warn("http log subscription ended; reconnecting", "service_id", ht.serviceID, "wait", wait, "err", err)
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
	}
}

// dispatch applies the service allowlist and fans a record out to every worker.
func (m *Manager) dispatch(rec model.LogRecord) {
	telemetry.LogsReceived.Inc()
	if !inScope(m.services, rec.ServiceID, rec.ServiceName) {
		return
	}
	for _, w := range m.workers {
		w.enqueue(rec)
	}
}

// convert normalizes a Railway deploy-log entry, enriching IDs with names from
// the current target set. The bool reports whether the timestamp was parsed.
func (m *Manager) convert(e railway.LogEntry) (model.LogRecord, bool) {
	ts, tsOK := parseTimestamp(e.Timestamp)
	rec := model.LogRecord{
		Timestamp:            ts,
		Message:              e.Message,
		Severity:             e.Severity,
		Level:                model.NormalizeLevel(e.Severity),
		Kind:                 model.KindDeploy,
		ProjectID:            e.Tags.ProjectID,
		EnvironmentID:        e.Tags.EnvironmentID,
		ServiceID:            e.Tags.ServiceID,
		DeploymentID:         e.Tags.DeploymentID,
		DeploymentInstanceID: e.Tags.DeploymentInstanceID,
		PluginID:             e.Tags.PluginID,
		SnapshotID:           e.Tags.SnapshotID,
		Attributes:           attributesMap(e.Attributes),
	}

	m.mu.RLock()
	tg, ok := m.targets[rec.EnvironmentID]
	m.mu.RUnlock()
	if ok {
		rec.ProjectName = tg.ProjectName
		rec.EnvironmentName = tg.EnvironmentName
		rec.ServiceName = tg.ServiceName(rec.ServiceID)
	}
	return rec, tsOK
}

// convertHTTP normalizes a Railway HTTP-log entry into a structured record with
// correlation-friendly attributes. Identity comes from the subscription's
// target context.
func (m *Manager) convertHTTP(e railway.HTTPLogEntry, ht svcContext) (model.LogRecord, bool) {
	ts, tsOK := parseTimestamp(e.Timestamp)
	level := httpStatusLevel(e.HTTPStatus)
	rec := model.LogRecord{
		Timestamp:            ts,
		Message:              fmt.Sprintf("%s %s %d %dms", e.Method, e.Path, e.HTTPStatus, e.TotalDuration),
		Level:                level,
		Severity:             string(level),
		Kind:                 model.KindHTTP,
		ProjectID:            ht.projectID,
		ProjectName:          ht.projectName,
		EnvironmentID:        ht.envID,
		EnvironmentName:      ht.envName,
		ServiceID:            ht.serviceID,
		ServiceName:          ht.serviceName,
		DeploymentID:         e.DeploymentID,
		DeploymentInstanceID: e.DeploymentInstanceID,
		Region:               e.EdgeRegion,
		Attributes:           httpAttributes(e),
	}
	return rec, tsOK
}

// httpAttributes maps an HTTP log entry to correlation-friendly attributes
// (OpenTelemetry HTTP semantic conventions where they apply).
func httpAttributes(e railway.HTTPLogEntry) map[string]string {
	attrs := map[string]string{
		"http.request.method":       e.Method,
		"url.path":                  e.Path,
		"http.response.status_code": strconv.Itoa(e.HTTPStatus),
		"http.server.duration_ms":   strconv.Itoa(e.TotalDuration),
		"http.request.body.size":    strconv.Itoa(e.RxBytes),
		"http.response.body.size":   strconv.Itoa(e.TxBytes),
	}
	set := func(k, v string) {
		if v != "" {
			attrs[k] = v
		}
	}
	set("server.address", e.Host)
	set("http.request.id", e.RequestID)
	set("client.address", e.SrcIP)
	set("user_agent.original", e.ClientUA)
	set("railway.upstream_proto", e.UpstreamProto)
	set("railway.downstream_proto", e.DownstreamProto)
	set("railway.upstream_address", e.UpstreamAddress)
	set("railway.upstream_errors", e.UpstreamErrors)
	set("railway.response_details", e.ResponseDetails)
	set("railway.edge_region", e.EdgeRegion)
	if e.UpstreamRqDuration > 0 {
		attrs["railway.upstream_rq_duration_ms"] = strconv.Itoa(e.UpstreamRqDuration)
	}
	return attrs
}

func httpStatusLevel(status int) model.Level {
	switch {
	case status >= 500:
		return model.LevelError
	case status >= 400:
		return model.LevelWarn
	default:
		return model.LevelInfo
	}
}

const buildLogLimit = 1000

// reconcileBuild fetches build logs once per newly-seen deployment. Build logs
// are finite, so each deployment is fetched exactly once (deduped by ID). The
// first reconcile finds one deployment per service, so the fetches run at most
// BuildLogConcurrency at a time.
func (m *Manager) reconcileBuild(ctx context.Context, targets []target.Target) {
	for _, t := range targets {
		for _, s := range t.Services {
			if s.DeploymentID == "" {
				continue
			}
			if !inScope(m.buildServices, s.ID, s.Name) {
				continue
			}
			if _, ok := m.buildSeen[s.DeploymentID]; ok {
				continue
			}
			m.buildSeen[s.DeploymentID] = struct{}{}
			sc := svcContext{
				projectID: t.ProjectID, projectName: t.ProjectName,
				envID: t.EnvironmentID, envName: t.EnvironmentName,
				serviceID: s.ID, serviceName: s.Name, deploymentID: s.DeploymentID,
			}
			m.subWG.Add(1)
			go func(sc svcContext) {
				defer m.subWG.Done()
				// Acquire here, not in the loop above: reconcile also drives
				// the deploy and HTTP subscription lifecycle, so it must not
				// block waiting for a build-log slot.
				select {
				case m.buildSem <- struct{}{}:
				case <-ctx.Done():
					return
				}
				defer func() { <-m.buildSem }()
				m.fetchBuildLogs(ctx, sc)
			}(sc)
		}
	}
}

// fetchBuildLogs streams one deployment's build logs to completion (no reconnect
// — build logs are a finite, one-shot stream).
func (m *Manager) fetchBuildLogs(ctx context.Context, sc svcContext) {
	err := m.src.StreamBuildLogs(ctx, railway.BuildLogStreamParams{
		DeploymentID: sc.deploymentID,
		Limit:        buildLogLimit,
	}, func(e railway.LogEntry) error {
		m.dispatch(m.convertBuild(e, sc))
		return nil
	})
	if err != nil && ctx.Err() == nil {
		m.log.Warn("build log fetch failed", "service_id", sc.serviceID, "deployment_id", sc.deploymentID, "err", err)
	}
}

// convertBuild normalizes a build-log line into a record.
func (m *Manager) convertBuild(e railway.LogEntry, sc svcContext) model.LogRecord {
	ts, _ := parseTimestamp(e.Timestamp)
	return model.LogRecord{
		Timestamp:       ts,
		Message:         e.Message,
		Severity:        e.Severity,
		Level:           model.NormalizeLevel(e.Severity),
		Kind:            model.KindBuild,
		ProjectID:       sc.projectID,
		ProjectName:     sc.projectName,
		EnvironmentID:   sc.envID,
		EnvironmentName: sc.envName,
		ServiceID:       sc.serviceID,
		ServiceName:     sc.serviceName,
		DeploymentID:    sc.deploymentID,
		Attributes:      attributesMap(e.Attributes),
	}
}

// attributesMap converts Railway log attributes to a plain map (nil if empty).
// Railway hands back each attribute value as its raw JSON token, so string
// values arrive quoted (e.g. `"warn"`); unquoteJSON decodes them to the bare
// value.
func attributesMap(attrs []railway.LogAttribute) map[string]string {
	if len(attrs) == 0 {
		return nil
	}
	m := make(map[string]string, len(attrs))
	for _, a := range attrs {
		m[a.Key] = unquoteJSON(a.Value)
	}
	return m
}

// unquoteJSON decodes a Railway structured-log attribute value. Railway returns
// each value as its raw JSON token, so a JSON string arrives with its quotes
// (e.g. `"warn"`, `"boom: it broke"`). Decode those to the bare string so
// downstream labels and Loki's level detection see `warn`, not `"warn"`.
// Non-string tokens (numbers, objects, arrays) and already-bare values start
// with something other than `"` and are returned unchanged; a quoted-looking
// value that fails to decode is also left as-is.
func unquoteJSON(v string) string {
	if len(v) < 2 || v[0] != '"' {
		return v
	}
	var s string
	if json.Unmarshal([]byte(v), &s) == nil {
		return s
	}
	return v
}

// toLowerSet builds a case-insensitive membership set from allowlist entries
// (which may be service IDs or names).
func toLowerSet(items []string) map[string]struct{} {
	m := make(map[string]struct{}, len(items))
	for _, it := range items {
		m[strings.ToLower(it)] = struct{}{}
	}
	return m
}

// inScope reports whether id or name is in the set (case-insensitive). An empty
// set means "all".
func inScope(set map[string]struct{}, id, name string) bool {
	if len(set) == 0 {
		return true
	}
	if id != "" {
		if _, ok := set[strings.ToLower(id)]; ok {
			return true
		}
	}
	if name != "" {
		if _, ok := set[strings.ToLower(name)]; ok {
			return true
		}
	}
	return false
}

func parseTimestamp(s string) (time.Time, bool) {
	if s != "" {
		for _, layout := range []string{time.RFC3339Nano, time.RFC3339} {
			if t, err := time.Parse(layout, s); err == nil {
				return t, true
			}
		}
	}
	return time.Now().UTC(), false
}
