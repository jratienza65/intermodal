package exporter

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/jratienza65/intermodal/internal/config"
	"github.com/jratienza65/intermodal/internal/metrics"
	"github.com/jratienza65/intermodal/internal/railway"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/sdk/instrumentation"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/resource"
)

func init() { Register("otlp", newOTLP) }

// otlpExporter pushes the metrics snapshot over OTLP/HTTP. It translates our
// snapshot into OTel metricdata directly (rather than via SDK instruments) so
// the exported values match the Prometheus path exactly, using the same
// measurement mapping.
type otlpExporter struct {
	exp   sdkmetric.Exporter
	res   *resource.Resource
	start time.Time
}

func newOTLP(opts Options) (Exporter, error) {
	cfg := opts.Config
	if cfg.OTLPEndpoint == "" {
		return nil, fmt.Errorf("otlp: OTLP endpoint required (INTERMODAL_OTLP_ENDPOINT)")
	}
	exp, err := otlpmetrichttp.New(context.Background(), otlpMetricOptions(cfg)...)
	if err != nil {
		return nil, fmt.Errorf("otlp: create metric exporter: %w", err)
	}
	return &otlpExporter{
		exp:   exp,
		res:   resource.NewSchemaless(attribute.String("service.name", cfg.ServiceName)),
		start: time.Now(),
	}, nil
}

func (e *otlpExporter) Name() string { return "otlp" }

func (e *otlpExporter) Export(ctx context.Context, snap *metrics.Snapshot) error {
	rm := e.buildResourceMetrics(snap)
	if len(rm.ScopeMetrics) == 0 || len(rm.ScopeMetrics[0].Metrics) == 0 {
		return nil
	}
	return e.exp.Export(ctx, rm)
}

func (e *otlpExporter) Close(ctx context.Context) error { return e.exp.Shutdown(ctx) }

func (e *otlpExporter) buildResourceMetrics(snap *metrics.Snapshot) *metricdata.ResourceMetrics {
	byMeasure := map[railway.MetricMeasurement][]metricdata.DataPoint[float64]{}
	seen := map[string]struct{}{}
	enc := attribute.DefaultEncoder()

	for _, s := range snap.Samples {
		info, ok := metrics.Measurements[s.Measurement]
		if !ok {
			continue
		}
		set := attrSet(s.Labels)
		key := info.Name + "|" + set.Encoded(enc)
		if _, dup := seen[key]; dup {
			continue
		}
		seen[key] = struct{}{}
		byMeasure[s.Measurement] = append(byMeasure[s.Measurement], metricdata.DataPoint[float64]{
			Attributes: set,
			StartTime:  e.start,
			Time:       s.Timestamp,
			Value:      s.Value * info.Factor,
		})
	}

	var ms []metricdata.Metrics
	for meas, dps := range byMeasure {
		info := metrics.Measurements[meas]
		m := metricdata.Metrics{Name: info.Name, Description: info.Help, Unit: info.Unit}
		if info.Type == metrics.Counter {
			m.Data = metricdata.Sum[float64]{
				Temporality: metricdata.CumulativeTemporality,
				IsMonotonic: true,
				DataPoints:  dps,
			}
		} else {
			m.Data = metricdata.Gauge[float64]{DataPoints: dps}
		}
		ms = append(ms, m)
	}

	return &metricdata.ResourceMetrics{
		Resource: e.res,
		ScopeMetrics: []metricdata.ScopeMetrics{{
			Scope:   instrumentation.Scope{Name: "intermodal"},
			Metrics: ms,
		}},
	}
}

func attrSet(l metrics.Labels) attribute.Set {
	vals := l.Values()
	kvs := make([]attribute.KeyValue, 0, len(metrics.LabelNames))
	for i, name := range metrics.LabelNames {
		if vals[i] != "" {
			kvs = append(kvs, attribute.String(name, vals[i]))
		}
	}
	return attribute.NewSet(kvs...)
}

func otlpMetricOptions(cfg *config.Config) []otlpmetrichttp.Option {
	var opts []otlpmetrichttp.Option
	ep := cfg.OTLPEndpoint
	if strings.HasPrefix(ep, "http://") || strings.HasPrefix(ep, "https://") {
		opts = append(opts, otlpmetrichttp.WithEndpointURL(ep))
	} else {
		opts = append(opts, otlpmetrichttp.WithEndpoint(ep))
		if cfg.OTLPInsecure {
			opts = append(opts, otlpmetrichttp.WithInsecure())
		}
	}
	if len(cfg.OTLPHeaders) > 0 {
		opts = append(opts, otlpmetrichttp.WithHeaders(cfg.OTLPHeaders))
	}
	return opts
}
