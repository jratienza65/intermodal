package metrics

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/jratienza65/intermodal/internal/config"
	"github.com/jratienza65/intermodal/internal/railway"
	"github.com/jratienza65/intermodal/internal/target"
	"github.com/jratienza65/intermodal/internal/telemetry"
)

// metricsAPI is the subset of *railway.Client the poller needs.
type metricsAPI interface {
	Metrics(ctx context.Context, q railway.MetricsQuery) ([]railway.MetricsResult, error)
}

const maxPollConcurrency = 4

// Poller periodically queries Railway metrics for every target and publishes
// the result to the Store. It decouples Railway polling from Prometheus scrapes
// so scrape frequency can't blow the API rate limit.
type Poller struct {
	api          metricsAPI
	provider     target.Provider
	store        *Store
	log          *slog.Logger
	interval     time.Duration
	window       time.Duration
	sampleRate   int
	measurements []railway.MetricMeasurement
	groupBy      []railway.MetricTag
}

// NewPoller builds a Poller from config.
func NewPoller(api metricsAPI, provider target.Provider, store *Store, cfg *config.Config, log *slog.Logger) *Poller {
	if log == nil {
		log = slog.Default()
	}
	return &Poller{
		api:          api,
		provider:     provider,
		store:        store,
		log:          log,
		interval:     cfg.PollInterval,
		window:       cfg.MetricsWindow,
		sampleRate:   cfg.SampleRateSeconds,
		measurements: railway.DefaultMeasurements,
		groupBy:      []railway.MetricTag{railway.TagServiceID, railway.TagDeploymentInstanceID},
	}
}

// Run polls immediately, then on each interval tick, until ctx is cancelled.
func (p *Poller) Run(ctx context.Context) error {
	t := time.NewTicker(p.interval)
	defer t.Stop()
	p.pollAndStore(ctx)
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-t.C:
			p.pollAndStore(ctx)
		}
	}
}

func (p *Poller) pollAndStore(ctx context.Context) {
	start := time.Now()
	snap, ok := p.poll(ctx)
	telemetry.PollDuration.Observe(time.Since(start).Seconds())
	if snap != nil {
		p.store.Set(snap)
	}
	if ok {
		telemetry.PollSuccess.Set(1)
	} else {
		telemetry.PollSuccess.Set(0)
	}
}

// poll queries all targets and returns a snapshot plus whether every target
// succeeded. A snapshot is returned even on partial failure so healthy targets
// stay visible.
func (p *Poller) poll(ctx context.Context) (*Snapshot, bool) {
	targets, err := p.provider.Targets(ctx)
	if err != nil {
		p.log.Error("metrics: discovery failed", "err", err)
		telemetry.PollErrors.Inc()
		return nil, false
	}
	telemetry.TargetsDiscovered.Set(float64(len(targets)))
	if len(targets) == 0 {
		return &Snapshot{Time: time.Now()}, true
	}

	now := time.Now()
	start := now.Add(-p.window)

	var (
		mu      sync.Mutex
		samples []Sample
		allOK   = true
		wg      sync.WaitGroup
		sem     = make(chan struct{}, maxPollConcurrency)
	)

	for _, tg := range targets {
		wg.Add(1)
		sem <- struct{}{}
		go func(tg target.Target) {
			defer wg.Done()
			defer func() { <-sem }()

			results, err := p.api.Metrics(ctx, railway.MetricsQuery{
				Measurements:      p.measurements,
				ProjectID:         tg.ProjectID,
				EnvironmentID:     tg.EnvironmentID,
				StartDate:         start,
				EndDate:           now,
				GroupBy:           p.groupBy,
				SampleRateSeconds: p.sampleRate,
			})
			if err != nil {
				p.log.Warn("metrics: poll failed for target", "environment_id", tg.EnvironmentID, "err", err)
				telemetry.PollErrors.Inc()
				mu.Lock()
				allOK = false
				mu.Unlock()
				return
			}
			s := samplesFromResults(results, tg)
			mu.Lock()
			samples = append(samples, s...)
			mu.Unlock()
		}(tg)
	}
	wg.Wait()

	return &Snapshot{Samples: samples, Time: now}, allOK
}

// samplesFromResults converts Railway metric series into Samples, enriching IDs
// with human names from the target.
func samplesFromResults(results []railway.MetricsResult, tg target.Target) []Sample {
	out := make([]Sample, 0, len(results))
	for _, r := range results {
		v, ok := r.Latest()
		if !ok {
			continue
		}
		labels := Labels{
			ProjectID:            firstNonEmpty(r.Tags.ProjectID, tg.ProjectID),
			ProjectName:          tg.ProjectName,
			EnvironmentID:        firstNonEmpty(r.Tags.EnvironmentID, tg.EnvironmentID),
			EnvironmentName:      tg.EnvironmentName,
			ServiceID:            r.Tags.ServiceID,
			ServiceName:          tg.ServiceName(r.Tags.ServiceID),
			Region:               r.Tags.Region,
			DeploymentID:         r.Tags.DeploymentID,
			DeploymentInstanceID: r.Tags.DeploymentInstanceID,
		}
		out = append(out, Sample{
			Measurement: r.Measurement,
			Labels:      labels,
			Value:       v.Value,
			Timestamp:   time.Unix(v.Ts, 0),
		})
	}
	return out
}

func firstNonEmpty(a, b string) string {
	if a != "" {
		return a
	}
	return b
}
