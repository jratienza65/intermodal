# intermodal

**intermodal** is a single Go binary that turns any [Railway](https://railway.app) account or project into a first-class observability source. It runs as a Railway service and does two jobs at once: it **exports Railway platform metrics** in Prometheus format (pull) with optional OTLP push, and it acts as a **log drain**, streaming your services' logs off Railway and fanning them out to pluggable sinks (stdout, Loki, OTLP). It works unchanged with either an account/workspace token (all projects) or a single project token.

## Why

Railway ships a nice metrics UI and a live log viewer, but no Prometheus endpoint and no log drain you can point at your own stack. If you run Grafana/Loki/Mimir or an OpenTelemetry collector, there's no supported way to get Railway's CPU/memory/disk/network numbers or your application logs into it.

intermodal is that missing piece, as **one binary**:

- Scrape-ready `/metrics` for Prometheus, plus optional OTLP metric push.
- A log drain that reconnects, batches, and forwards to Loki, an OTLP collector, or stdout.
- Works with **both** Railway token types — an account/workspace token that enumerates every project you can see, or a project token scoped to one environment.
- Zero-config-friendly: it reads Railway's own `PORT` and token variables, so it drops into a Railway service with almost nothing to set.

## Quick start (deploy on Railway)

1. Create a new service in your Railway project from this repo (or a prebuilt image).
2. Set service variables:

   ```bash
   # Auth — an account/workspace token sees every project you can access.
   RAILWAY_API_TOKEN=<your-account-or-workspace-token>

   # Turn on the log drain sinks you want (metrics + stdout are on by default).
   INTERMODAL_SINKS=loki,otlp

   # Loki push target (base URL; /loki/api/v1/push is appended automatically).
   INTERMODAL_LOKI_URL=https://loki.example.com

   # OTLP collector (shared by the otlp log sink and the otlp metric exporter).
   INTERMODAL_OTLP_ENDPOINT=otel-collector.railway.internal:4318
   INTERMODAL_OTLP_INSECURE=true   # plain-http internal collector

   # Also push metrics via OTLP (Prometheus pull stays on unless removed).
   INTERMODAL_METRICS_EXPORTERS=prometheus,otlp
   ```

3. Railway injects `PORT`; intermodal binds `0.0.0.0:$PORT`. Point Prometheus at the service's `/metrics`.
4. Before (or after) deploying, run `intermodal doctor` to confirm the token can see targets and pull metrics/logs. See [`intermodal doctor`](#intermodal-doctor).

An example Prometheus scrape config lives at [`examples/prometheus-scrape.yml`](examples/prometheus-scrape.yml).

## The two token types

Railway has two kinds of API tokens, and they use different HTTP auth schemes. intermodal supports both and picks the scheme automatically from which variable you set. **Setting both an account token and a project token at once is a hard error.**

| | Account / workspace token | Project token |
|---|---|---|
| Header | `Authorization: Bearer <token>` | `Project-Access-Token: <token>` |
| Scope | Every project/environment/service the token can see | One project + one environment |
| Discovery | Enumerates projects via GraphQL (cached, TTL `INTERMODAL_DISCOVERY_TTL`) | The single environment the token is bound to |
| Set via | `RAILWAY_API_TOKEN` or `INTERMODAL_ACCOUNT_TOKEN` | `RAILWAY_PROJECT_TOKEN`, `RAILWAY_TOKEN`, or `INTERMODAL_PROJECT_TOKEN` |
| Optional scoping | `INTERMODAL_WORKSPACE_ID` limits enumeration to one workspace | — |

**Creating an account or workspace token:** Railway dashboard → **Account Settings → Tokens**. A token created with no team selected is a personal (account) token; a token created for a specific team is a workspace token. Both are used as `RAILWAY_API_TOKEN`.

**Creating a project token:** open the project → **Settings → Tokens → Create Token**, choosing the environment it should be scoped to. Use it as `RAILWAY_PROJECT_TOKEN`.

You can further narrow an account/workspace token to specific projects, environments, or services with the `INTERMODAL_PROJECTS` / `INTERMODAL_ENVIRONMENTS` / `INTERMODAL_SERVICES` allowlists (comma-separated Railway IDs).

## Configuration reference

All configuration is environment variables. intermodal-specific knobs use the `INTERMODAL_` prefix; Railway's own token variable names are accepted so it drops in with no renaming. Defaults are shown in parentheses.

### Auth

| Variable | Description |
|---|---|
| `RAILWAY_API_TOKEN` | Account/workspace token (`Authorization: Bearer`). Enumerates all visible projects. |
| `INTERMODAL_ACCOUNT_TOKEN` | Alias for the account/workspace token. |
| `RAILWAY_PROJECT_TOKEN` / `RAILWAY_TOKEN` / `INTERMODAL_PROJECT_TOKEN` | Project token (`Project-Access-Token`), single environment. |
| `INTERMODAL_WORKSPACE_ID` | Optional. Scope account-token enumeration to one workspace. |

> Provide exactly one of {account, project} token. Setting both fails at startup.

### Railway client tuning

| Variable | Default | Description |
|---|---|---|
| `INTERMODAL_RAILWAY_RPS` | `5` | Client request rate limit (requests/sec). |
| `INTERMODAL_RAILWAY_BURST` | `5` | Rate-limiter burst. |
| `INTERMODAL_RAILWAY_MAX_RETRIES` | `4` | Max retries on transient/API errors. |
| `INTERMODAL_HTTP_ENDPOINTS` | Railway default | Comma-separated override for GraphQL HTTP endpoint(s). |
| `INTERMODAL_WS_ENDPOINTS` | Railway default | Comma-separated override for GraphQL-WS endpoint(s). |

### Discovery / target selection

| Variable | Default | Description |
|---|---|---|
| `INTERMODAL_DISCOVERY_TTL` | `5m` | How long discovered targets are cached (account token). |
| `INTERMODAL_PROJECTS` | — | Allowlist of project IDs. |
| `INTERMODAL_ENVIRONMENTS` | — | Allowlist of environment IDs. |
| `INTERMODAL_SERVICES` | — | Allowlist of service IDs. |

### HTTP server

| Variable | Default | Description |
|---|---|---|
| `INTERMODAL_HTTP_ADDR` | — | Explicit listen address. If unset, falls back to `PORT`, then `0.0.0.0:8080`. |
| `PORT` | — | Injected by Railway; used as `0.0.0.0:$PORT` when `INTERMODAL_HTTP_ADDR` is unset. |

### Metrics

| Variable | Default | Description |
|---|---|---|
| `INTERMODAL_METRICS_ENABLED` | `true` | Enable the metrics exporter. |
| `INTERMODAL_METRICS_EXPORTERS` | `prometheus` | Comma-separated: `prometheus` (pull) and/or `otlp` (push). |
| `INTERMODAL_POLL_INTERVAL` | `60s` | How often Railway metrics are polled (also the OTLP push interval). |
| `INTERMODAL_SAMPLE_RATE_SECONDS` | `60` | Railway metrics sample resolution. |
| `INTERMODAL_METRICS_WINDOW` | `10m` | Look-back window per poll (`startDate = now − window`). |

### Logs

| Variable | Default | Description |
|---|---|---|
| `INTERMODAL_LOGS_ENABLED` | `true` | Enable the log drain. |
| `INTERMODAL_SINKS` | `stdout` | Comma-separated sinks: `stdout`, `loki`, `otlp` (e.g. `loki,otlp`). |
| `INTERMODAL_LOG_FILTER` | — | Railway filter DSL applied to every subscription (e.g. `@level:error`). |
| `INTERMODAL_LOG_BACKFILL` | `0` | Number of prior lines to fetch on connect (`beforeLimit`). |
| `INTERMODAL_LOG_QUEUE_SIZE` | `10000` | Per-sink buffered queue depth (overflow → dropped, counted). |
| `INTERMODAL_LOG_BATCH_SIZE` | `500` | Max records per sink flush. |
| `INTERMODAL_LOG_BATCH_TIMEOUT` | `2s` | Max time to wait before flushing a partial batch. |

### Loki sink

| Variable | Default | Description |
|---|---|---|
| `INTERMODAL_LOKI_URL` | — | **Required if `loki` enabled.** Base URL; `/loki/api/v1/push` is appended. |
| `INTERMODAL_LOKI_TENANT_ID` | — | Sent as `X-Scope-OrgID` for multi-tenant Loki. |
| `INTERMODAL_LOKI_LABELS` | — | Extra static stream labels, `k=v,k=v`. |

### OTLP (shared by the otlp log sink and otlp metric exporter)

| Variable | Default | Description |
|---|---|---|
| `INTERMODAL_OTLP_ENDPOINT` / `OTEL_EXPORTER_OTLP_ENDPOINT` | — | **Required if any otlp path enabled.** Host:port or full URL (e.g. `otel-collector.railway.internal:4318`). |
| `INTERMODAL_OTLP_INSECURE` | `false` | Set `true` for plain-http internal collectors. |
| `INTERMODAL_OTLP_HEADERS` | — | Extra headers, `k=v,k=v`. |

### General

| Variable | Default | Description |
|---|---|---|
| `INTERMODAL_LOG_LEVEL` | `info` | intermodal's own log level (`debug`, `info`, `warn`, `error`). |
| `INTERMODAL_SERVICE_NAME` | `intermodal` | Resource/service-name attribute and self-identification. |

> Validation: if both metrics and logs are disabled, startup fails. If `loki` is enabled without `INTERMODAL_LOKI_URL`, or any otlp path is enabled without an OTLP endpoint, startup fails.

## Exported metrics

### Railway service metrics

Polled from Railway and exposed on `/metrics` (and optionally pushed via OTLP). Byte-valued metrics are converted from Railway's decimal GB (1 GB = 1e9 bytes).

| Metric | Type | Description |
|---|---|---|
| `railway_service_cpu_usage_cores` | gauge | Current CPU usage in vCPU cores. |
| `railway_service_cpu_limit_cores` | gauge | Allocated CPU limit in vCPU cores. |
| `railway_service_memory_usage_bytes` | gauge | Current memory usage in bytes. |
| `railway_service_memory_limit_bytes` | gauge | Allocated memory limit in bytes. |
| `railway_service_disk_usage_bytes` | gauge | Current disk usage in bytes. |
| `railway_service_ephemeral_disk_usage_bytes` | gauge | Current ephemeral disk usage in bytes. |
| `railway_service_backup_usage_bytes` | gauge | Current backup storage usage in bytes. |
| `railway_service_network_receive_bytes_total` | counter | Cumulative network bytes received (ingress). |
| `railway_service_network_transmit_bytes_total` | counter | Cumulative network bytes transmitted (egress). |
| `railway_service_cpu_utilization_ratio` | gauge | Derived: CPU usage ÷ CPU limit (0–1). |
| `railway_service_memory_utilization_ratio` | gauge | Derived: memory usage ÷ memory limit (0–1). |

All of the above carry the labels: `project_id`, `project_name`, `environment_id`, `environment_name`, `service_id`, `service_name`, `region`, `deployment_id`, `deployment_instance_id`.

### Self / operational metrics

| Metric | Type | Description |
|---|---|---|
| `intermodal_build_info{version}` | gauge | Constant `1`, labeled by build version. |
| `railway_api_requests_total{op,code}` | counter | Railway GraphQL requests by operation and outcome (HTTP status or `error`). |
| `railway_api_rate_limit_remaining` | gauge | Most recent `X-RateLimit-Remaining` from Railway. |
| `intermodal_metrics_poll_success` | gauge | `1` if the last poll cycle fully succeeded, else `0`. |
| `intermodal_metrics_poll_duration_seconds` | histogram | Duration of a full metrics poll cycle. |
| `intermodal_metrics_poll_errors_total` | counter | Per-target errors encountered while polling. |
| `intermodal_targets_discovered` | gauge | Number of environments currently discovered as targets. |
| `intermodal_logs_received_total` | counter | Log records received from Railway subscriptions. |
| `intermodal_logs_forwarded_total{sink}` | counter | Records successfully forwarded, by sink. |
| `intermodal_log_sink_errors_total{sink}` | counter | Sink write errors, by sink. |
| `intermodal_logs_dropped_total{sink}` | counter | Records dropped due to a full sink queue, by sink. |
| `intermodal_log_subscriptions_active` | gauge | Currently-open Railway log subscriptions. |
| `intermodal_log_subscription_reconnects_total` | counter | Log subscription reconnect attempts. |

Standard Go runtime and process collectors are also registered.

## Logs: record shape and sinks

Railway log lines are streamed from the GraphQL-WS `environmentLogs` subscription, normalized into a common record, and fanned out to every configured sink through per-sink batched queues. Railway's free-form `severity` is folded into a canonical `level` (`debug`/`info`/`warn`/`error`), with the raw value preserved as `severity`.

The `stdout` sink writes each record as one JSON object per line (JSONL). intermodal's own diagnostics go to **stderr** (slog JSON), so stdout JSONL stays clean for a tailing collector.

```json
{
  "timestamp": "2026-07-02T12:34:56.789Z",
  "message": "handled request",
  "level": "info",
  "severity": "info",
  "kind": "deploy",
  "project_id": "...",
  "project_name": "...",
  "environment_id": "...",
  "environment_name": "...",
  "service_id": "...",
  "service_name": "...",
  "deployment_id": "...",
  "deployment_instance_id": "...",
  "region": "us-west1",
  "attributes": { "path": "/health", "status": "200" }
}
```

Empty fields are omitted. `kind` is one of `deploy`, `http`, or `build`. Structured key/values from the log line are nested under `attributes`.

**Available sinks:**

- `stdout` — JSONL to stdout. Great for local debugging or hand-off to a stdout-tailing collector (e.g. Vector).
- `loki` — pushes to a Grafana Loki `/loki/api/v1/push` endpoint. Low-cardinality stream labels; identity and attributes ride along as Loki structured metadata. Honors `INTERMODAL_LOKI_TENANT_ID` and `INTERMODAL_LOKI_LABELS`.
- `otlp` — exports logs to an OpenTelemetry collector using the shared OTLP settings.

## `intermodal doctor`

`doctor` probes the configured token and prints a plain-text capability report, so the account-vs-project question and any scope/auth issue is answered before you run for real. It reports:

- **token type** and the resolved HTTP/WS endpoints,
- **identity** (account token: `Me` name/email) or **project scope** (project token: bound project + environment),
- **discovery**: the target environments found (with service counts),
- a **trial metrics call** against the first target,
- a **trial log subscription** that connects briefly and reports how many lines it saw.

```bash
intermodal doctor
```

```text
intermodal doctor
=================
token type:      account
http endpoint:   https://backboard.railway.com/graphql/v2
ws endpoint:     wss://backboard.railway.com/graphql/v2

identity:        Ada Lovelace <ada@example.com>
discovery:       2 target environment(s)
  - web-api / production (env=..., 3 service(s))
  - web-api / staging (env=..., 2 service(s))
metrics probe:   OK (5 series for env ...)
log probe:       OK (received 1 log line(s))

Done.
```

## HTTP endpoints

| Path | Description |
|---|---|
| `/metrics` | Prometheus exposition (scrape target). |
| `/healthz` | Liveness probe. |
| `/readyz` | Readiness probe. |
| `/` | Plain-text index describing the endpoints. |

## Commands

| Command | Description |
|---|---|
| `intermodal serve` | Run the exporter + log drain (default when no subcommand is given). |
| `intermodal doctor` | Print the token capability report described above and exit. |
| `intermodal version` | Print the version and exit (`--version` / `-v` also work). |

## Extending: add a log sink

A log sink is one file. Implement `internal/sink.Sink` and register a factory from an `init()` — no core changes, no wiring. The name you register under is what users put in `INTERMODAL_SINKS`. (Push metric exporters follow the identical pattern with `internal/exporter.Exporter` and `INTERMODAL_METRICS_EXPORTERS`.)

```go
// Sink is a log destination. Write is called from a single per-sink worker
// goroutine and should treat the batch as read-only.
type Sink interface {
    Name() string
    Write(ctx context.Context, batch []model.LogRecord) error
    Close(ctx context.Context) error
}
```

```go
package sink

import (
    "context"

    "github.com/jratienza65/intermodal/internal/model"
)

// Self-register so the sink is available by name in INTERMODAL_SINKS.
func init() { Register("mysink", newMySink) }

type mySink struct{ /* client, config, logger... */ }

func newMySink(opts Options) (Sink, error) {
    // Read typed fields off opts.Config; opts.Logger is always non-nil.
    return &mySink{}, nil
}

func (s *mySink) Name() string { return "mysink" }

func (s *mySink) Write(ctx context.Context, batch []model.LogRecord) error {
    for _, r := range batch {
        _ = r // ship it
    }
    return nil
}

func (s *mySink) Close(ctx context.Context) error { return nil }
```

Sink factories receive `Options{ Config *config.Config; Logger *slog.Logger }`. Use `sink.EncodeRecord` if you want the same flat JSON shape the stdout sink emits.

## Local development

Requires **Go 1.25**.

```bash
# Build
CGO_ENABLED=0 go build -o intermodal ./cmd/intermodal

# Build with an explicit version stamp
CGO_ENABLED=0 go build -ldflags "-X main.version=$(git describe --tags --always)" \
  -o intermodal ./cmd/intermodal

# Run locally against a project token, stdout sink only
export RAILWAY_PROJECT_TOKEN=... 
export INTERMODAL_SINKS=stdout
go run ./cmd/intermodal serve
# metrics at http://localhost:8080/metrics

# Probe your token first
go run ./cmd/intermodal doctor

# Tests
go test ./...
```
