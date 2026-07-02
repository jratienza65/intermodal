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
// intermodal is the resource (service.name), since these are its own metrics.
// It blocks until ctx is cancelled, then shuts the pipeline down.
func RunSelfMetrics(ctx context.Context, gatherer prometheus.Gatherer, cfg *config.Config, interval time.Duration, log *slog.Logger) error {
	if log == nil {
		log = slog.Default()
	}
	if interval <= 0 {
		interval = time.Minute
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
		sdkmetric.WithResource(resource.NewSchemaless(attribute.String("service.name", cfg.ServiceName))),
	)

	<-ctx.Done()
	sctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()
	return mp.Shutdown(sctx)
}
