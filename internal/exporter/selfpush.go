package exporter

import (
	"context"
	"log/slog"
	"time"

	"github.com/jratienza65/intermodal/internal/config"
	"github.com/prometheus/client_golang/prometheus"
	otelprom "go.opentelemetry.io/contrib/bridges/prometheus"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
)

// RunSelfMetrics pushes intermodal's own Prometheus-registered metrics (from
// gatherer) over OTLP on the given interval, using the Prometheus->OTLP bridge.
// intermodal is the resource (service.name), since these are its own metrics,
// identified per instance by service.instance.id (see selfResource).
// It blocks until ctx is cancelled, then shuts the pipeline down.
func RunSelfMetrics(ctx context.Context, gatherer prometheus.Gatherer, cfg *config.Config, interval time.Duration, log *slog.Logger) error {
	if log == nil {
		log = slog.Default()
	}
	if interval <= 0 {
		interval = time.Minute
	}
	if cfg.Identity.InstanceID == "" {
		log.Warn("self-metrics have no service.instance.id; if more than one intermodal pushes to this backend, they all write the same series and overwrite each other — set INTERMODAL_INSTANCE_ID")
	}
	exp, err := otlpmetrichttp.New(ctx, otlpMetricOptions(cfg)...)
	if err != nil {
		return err
	}
	producer := otelprom.NewMetricProducer(otelprom.WithGatherer(gatherer))
	reader := sdkmetric.NewPeriodicReader(exp,
		sdkmetric.WithInterval(interval),
		sdkmetric.WithProducer(producer),
	)
	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithReader(reader),
		sdkmetric.WithResource(selfResource(cfg)),
	)

	<-ctx.Done()
	sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	return mp.Shutdown(sctx)
}

// selfResource is the OTLP resource for intermodal's own metrics.
//
// These metrics describe the instance, not a Railway service, so their labels
// hold nothing that separates one instance from another. The resource carries
// that identity instead: service.instance.id becomes the Prometheus `instance`
// label — the same label a scrape of /metrics adds — and the railway.* keys
// give a readable name to group by.
func selfResource(cfg *config.Config) *resource.Resource {
	attrs := cfg.SelfResourceAttributes()
	kvs := make([]attribute.KeyValue, 0, len(attrs))
	for _, a := range attrs {
		kvs = append(kvs, attribute.String(a.Key, a.Value))
	}
	return resource.NewSchemaless(kvs...)
}
