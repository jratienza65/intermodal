package metrics

import (
	"strings"

	"github.com/jratienza65/intermodal/internal/railway"
	"github.com/prometheus/client_golang/prometheus"
)

// Collector is a prometheus.Collector that exposes the latest polled snapshot.
// It is registered once; each scrape reads the current snapshot, so scrapes
// never touch the Railway API. It exposes only the raw measurements — ratios
// like utilization are left to query-time (usage / limit), which keeps the pull
// and OTLP-push metric sets identical.
type Collector struct {
	store *Store
	descs map[railway.MetricMeasurement]*prometheus.Desc
}

// NewCollector builds a Collector over the given store.
func NewCollector(store *Store) *Collector {
	descs := make(map[railway.MetricMeasurement]*prometheus.Desc, len(Measurements))
	for m, info := range Measurements {
		descs[m] = prometheus.NewDesc(info.Name, info.Help, LabelNames, nil)
	}
	return &Collector{store: store, descs: descs}
}

// Describe implements prometheus.Collector.
func (c *Collector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range c.descs {
		ch <- d
	}
}

// Collect implements prometheus.Collector.
func (c *Collector) Collect(ch chan<- prometheus.Metric) {
	snap := c.store.Snapshot()
	if snap == nil {
		return
	}

	seen := make(map[string]struct{}, len(snap.Samples))
	for _, s := range snap.Samples {
		info, ok := Measurements[s.Measurement]
		if !ok {
			continue
		}
		labelValues := s.Labels.Values()
		key := info.Name + "\x00" + strings.Join(labelValues, "\x00")
		if _, dup := seen[key]; dup {
			continue // defend against duplicate series so Gather never fails
		}
		seen[key] = struct{}{}

		vt := prometheus.GaugeValue
		if info.Type == Counter {
			vt = prometheus.CounterValue
		}
		m, err := prometheus.NewConstMetric(c.descs[s.Measurement], vt, s.Value*info.Factor, labelValues...)
		if err != nil {
			continue
		}
		ch <- m
	}
}
