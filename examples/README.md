# intermodal — Grafana dashboards

Two Grafana dashboards for [`intermodal`](../README.md), the Railway metrics
exporter + log drain, plus a Prometheus scrape config to feed them. The
dashboards read the metrics intermodal exports — whether you scrape its
`/metrics` endpoint (`prometheus-scrape.yml`, below) or push them via OTLP into
your metrics backend.

## Dashboards

- **`grafana-dashboard.json` — Railway services.** Per-service CPU (cores),
  memory (bytes), network throughput, and utilization (computed in-query as
  usage ÷ limit), with **Project → Service** template variables. Reads the
  `railway_service_*` series.
- **`grafana-dashboard-intermodal.json` — intermodal self / headroom.**
  intermodal's own health: Railway API request rate vs plan budget, metrics
  poll duration, log throughput, drops, sink errors, and subscription
  reconnects. Reads the `intermodal_*` / `railway_api_*` series.

## Requirements

- A Prometheus-compatible datasource (Prometheus, Mimir, or Grafana Cloud)
  holding intermodal's metrics. Both dashboards expose a `datasource` variable,
  so you select it at import time.
- Grafana 10+ (schemaVersion 39).

## Collecting the metrics (pull path)

If you scrape rather than push, `prometheus-scrape.yml` is a ready scrape config
targeting intermodal's `/metrics` over Railway's private network
(`intermodal.railway.internal:8080`). Drop it into your `prometheus.yml`
`scrape_configs`, running Prometheus (or Grafana Agent / Alloy) as a sibling
service in the same Railway project + environment so the internal DNS resolves.
It also includes a Grafana Alloy `prometheus.scrape → prometheus.remote_write`
snippet for shipping to Grafana Cloud. (Alternatively, push via OTLP with
`INTERMODAL_METRICS_EXPORTERS=otlp` — no scraper needed.)

## Import

Grafana → **Dashboards → New → Import** → upload the JSON (or paste it), then
pick your Prometheus datasource. On the **Railway services** board the
**Project** and **Service** dropdowns populate automatically from the metric
labels.

## Metrics the dashboards expect

intermodal exports these identically over the Prometheus `/metrics` pull
endpoint and OTLP push:

**Railway service metrics** — `railway_service_cpu_usage_cores`,
`railway_service_cpu_limit_cores`, `railway_service_memory_usage_bytes`,
`railway_service_memory_limit_bytes`, `railway_service_disk_usage_bytes`,
`railway_service_network_receive_bytes_total`,
`railway_service_network_transmit_bytes_total` — labeled with `project_id`,
`project_name`, `environment_id`, `environment_name`, `service_id`,
`service_name`, `region`, `deployment_id`, `deployment_instance_id`. Only raw
measurements are exported; ratios like utilization are derived in-query
(e.g. `railway_service_cpu_usage_cores / railway_service_cpu_limit_cores`).

**intermodal self-metrics** — `intermodal_metrics_poll_success`,
`intermodal_metrics_poll_duration_seconds`, `intermodal_targets_discovered`,
`intermodal_logs_received_total`, `intermodal_logs_forwarded_total{sink}`,
`intermodal_logs_dropped_total{sink}`, `intermodal_log_sink_errors_total{sink}`,
`intermodal_log_subscriptions_active`,
`intermodal_log_subscription_reconnects_total`, and
`railway_api_requests_total{op,code}`.

> Metric names assume the standard OTLP→Prometheus translation. If your
> collector appends unit suffixes, adjust the dashboard queries to match.

See the [project README](../README.md) for how to deploy intermodal and produce
these metrics.
