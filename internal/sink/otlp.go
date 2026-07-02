package sink

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/jratienza65/intermodal/internal/model"
)

func init() { Register("otlp", newOTLPLogSink) }

// otlpLogSink exports logs over OTLP/HTTP (JSON). Records are grouped by their
// source Railway service and each group is emitted under its own OTLP
// **resource** (service.name = the source service), so downstream (Loki via the
// collector) attributes each line to the service that produced it — not to
// intermodal. intermodal identifies itself as the instrumentation scope.
type otlpLogSink struct {
	logsURL string
	headers map[string]string
	client  *http.Client
}

func newOTLPLogSink(opts Options) (Sink, error) {
	cfg := opts.Config
	if cfg.OTLPEndpoint == "" {
		return nil, fmt.Errorf("otlp: OTLP endpoint required (INTERMODAL_OTLP_ENDPOINT)")
	}
	base := normalizeOTLPBase(cfg.OTLPEndpoint, cfg.OTLPInsecure)
	return &otlpLogSink{
		logsURL: base + "/v1/logs",
		headers: cfg.OTLPHeaders,
		client:  &http.Client{Timeout: 30 * time.Second},
	}, nil
}

func (s *otlpLogSink) Name() string { return "otlp" }

func (s *otlpLogSink) Write(ctx context.Context, batch []model.LogRecord) error {
	if len(batch) == 0 {
		return nil
	}
	payload := buildOTLPLogs(batch)
	body, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.logsURL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	for k, v := range s.headers {
		req.Header.Set(k, v)
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		_, _ = io.Copy(io.Discard, resp.Body)
		return fmt.Errorf("otlp: logs export failed: status %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	return nil
}

func (s *otlpLogSink) Close(context.Context) error { return nil }

// normalizeOTLPBase returns a scheme://host[:port] base URL with no trailing
// slash. A bare host:port becomes http:// when insecure, else https://.
func normalizeOTLPBase(endpoint string, insecure bool) string {
	if strings.HasPrefix(endpoint, "http://") || strings.HasPrefix(endpoint, "https://") {
		return strings.TrimRight(endpoint, "/")
	}
	scheme := "https"
	if insecure {
		scheme = "http"
	}
	return scheme + "://" + strings.TrimRight(endpoint, "/")
}

// --- OTLP/HTTP JSON log encoding ---

type otlpLogsPayload struct {
	ResourceLogs []otlpResourceLogs `json:"resourceLogs"`
}

type otlpResourceLogs struct {
	Resource  otlpResource    `json:"resource"`
	ScopeLogs []otlpScopeLogs `json:"scopeLogs"`
}

type otlpResource struct {
	Attributes []otlpKV `json:"attributes"`
}

type otlpScopeLogs struct {
	Scope      otlpScope    `json:"scope"`
	LogRecords []otlpLogRec `json:"logRecords"`
}

type otlpScope struct {
	Name string `json:"name"`
}

type otlpLogRec struct {
	TimeUnixNano         string   `json:"timeUnixNano"`
	ObservedTimeUnixNano string   `json:"observedTimeUnixNano"`
	SeverityNumber       int      `json:"severityNumber,omitempty"`
	SeverityText         string   `json:"severityText,omitempty"`
	Body                 otlpAny  `json:"body"`
	Attributes           []otlpKV `json:"attributes,omitempty"`
}

type otlpKV struct {
	Key   string  `json:"key"`
	Value otlpAny `json:"value"`
}

type otlpAny struct {
	StringValue string `json:"stringValue"`
}

// buildOTLPLogs groups records by source service into per-resource ResourceLogs.
func buildOTLPLogs(batch []model.LogRecord) otlpLogsPayload {
	order := []string{}
	groups := map[string][]model.LogRecord{}
	for _, r := range batch {
		k := resourceKey(r)
		if _, ok := groups[k]; !ok {
			order = append(order, k)
		}
		groups[k] = append(groups[k], r)
	}

	payload := otlpLogsPayload{ResourceLogs: make([]otlpResourceLogs, 0, len(order))}
	for _, k := range order {
		recs := groups[k]
		logRecords := make([]otlpLogRec, 0, len(recs))
		for _, r := range recs {
			logRecords = append(logRecords, otlpLogRecord(r))
		}
		payload.ResourceLogs = append(payload.ResourceLogs, otlpResourceLogs{
			Resource:  otlpResource{Attributes: resourceAttrs(recs[0])},
			ScopeLogs: []otlpScopeLogs{{Scope: otlpScope{Name: "intermodal"}, LogRecords: logRecords}},
		})
	}
	return payload
}

// resourceKey identifies a distinct OTLP resource (one per source service).
func resourceKey(r model.LogRecord) string {
	return r.ProjectID + "|" + r.EnvironmentID + "|" + r.ServiceID
}

// resourceAttrs builds the OTLP resource attributes for a source service. The
// source service is the resource's service.name so it drives the downstream
// Loki/label attribution.
func resourceAttrs(r model.LogRecord) []otlpKV {
	name := r.ServiceName
	if name == "" {
		name = r.ServiceID
	}
	if name == "" {
		name = "railway"
	}
	kv := []otlpKV{{Key: "service.name", Value: otlpAny{StringValue: name}}}
	add := func(k, v string) {
		if v != "" {
			kv = append(kv, otlpKV{Key: k, Value: otlpAny{StringValue: v}})
		}
	}
	add("railway.project.id", r.ProjectID)
	add("railway.project.name", r.ProjectName)
	add("railway.environment.id", r.EnvironmentID)
	add("railway.environment.name", r.EnvironmentName)
	add("railway.service.id", r.ServiceID)
	return kv
}

func otlpLogRecord(r model.LogRecord) otlpLogRec {
	ts := strconv.FormatInt(r.Timestamp.UnixNano(), 10)
	rec := otlpLogRec{
		TimeUnixNano:         ts,
		ObservedTimeUnixNano: ts,
		SeverityNumber:       otlpSeverityNumber(r.Level),
		SeverityText:         r.Severity,
		Body:                 otlpAny{StringValue: r.Message},
	}
	add := func(k, v string) {
		if v != "" {
			rec.Attributes = append(rec.Attributes, otlpKV{Key: k, Value: otlpAny{StringValue: v}})
		}
	}
	add("railway.log_kind", string(r.Kind))
	add("railway.deployment.id", r.DeploymentID)
	add("railway.deployment.instance.id", r.DeploymentInstanceID)
	add("railway.region", r.Region)
	add("railway.plugin.id", r.PluginID)
	add("railway.snapshot.id", r.SnapshotID)
	for k, v := range r.Attributes {
		rec.Attributes = append(rec.Attributes, otlpKV{Key: k, Value: otlpAny{StringValue: v}})
	}
	return rec
}

// otlpSeverityNumber maps a normalized level to the OTLP SeverityNumber enum.
func otlpSeverityNumber(l model.Level) int {
	switch l {
	case model.LevelDebug:
		return 5
	case model.LevelInfo:
		return 9
	case model.LevelWarn:
		return 13
	case model.LevelError:
		return 17
	default:
		return 0
	}
}
