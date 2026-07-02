package sink

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jratienza65/intermodal/internal/model"
)

func init() { Register("loki", newLoki) }

// lokiSink pushes logs to a Grafana Loki (or Grafana Cloud) push endpoint.
// Records are grouped into streams by a small set of low-cardinality labels;
// high-cardinality identity and custom attributes ride along as Loki structured
// metadata (the modern, cardinality-safe way to attach per-line context).
type lokiSink struct {
	pushURL     string
	tenantID    string
	extraLabels map[string]string
	client      *http.Client
	log         *slog.Logger
}

func newLoki(opts Options) (Sink, error) {
	cfg := opts.Config
	if cfg.LokiURL == "" {
		return nil, fmt.Errorf("loki: INTERMODAL_LOKI_URL is required")
	}
	return &lokiSink{
		pushURL:     strings.TrimRight(cfg.LokiURL, "/") + "/loki/api/v1/push",
		tenantID:    cfg.LokiTenantID,
		extraLabels: cfg.LokiExtraLabels,
		client:      &http.Client{Timeout: 30 * time.Second},
		log:         opts.Logger,
	}, nil
}

func (s *lokiSink) Name() string { return "loki" }

// lokiPush is the Loki push payload. A value is [ts_ns, line, structuredMeta].
type lokiPush struct {
	Streams []lokiStream `json:"streams"`
}

type lokiStream struct {
	Stream map[string]string `json:"stream"`
	Values [][]any           `json:"values"`
}

func (s *lokiSink) Write(ctx context.Context, batch []model.LogRecord) error {
	if len(batch) == 0 {
		return nil
	}

	// Group records by stream label set.
	streams := map[string]*lokiStream{}
	for _, r := range batch {
		labels := s.labelsFor(r)
		key := streamKey(labels)
		st := streams[key]
		if st == nil {
			st = &lokiStream{Stream: labels}
			streams[key] = st
		}
		value := []any{
			strconv.FormatInt(r.Timestamp.UnixNano(), 10),
			r.Message,
		}
		if meta := structuredMetadata(r); len(meta) > 0 {
			value = append(value, meta)
		}
		st.Values = append(st.Values, value)
	}

	push := lokiPush{Streams: make([]lokiStream, 0, len(streams))}
	for _, st := range streams {
		push.Streams = append(push.Streams, *st)
	}

	body, err := json.Marshal(push)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, s.pushURL, bytes.NewReader(body))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	if s.tenantID != "" {
		req.Header.Set("X-Scope-OrgID", s.tenantID)
	}
	resp, err := s.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		b, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		_, _ = io.Copy(io.Discard, resp.Body) // drain remainder so the connection can be pooled
		return fmt.Errorf("loki: push failed: status %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	return nil
}

func (s *lokiSink) Close(context.Context) error { return nil }

// labelsFor builds the low-cardinality stream labels for a record.
func (s *lokiSink) labelsFor(r model.LogRecord) map[string]string {
	labels := map[string]string{
		"job":    "intermodal",
		"source": "railway",
	}
	putLabel(labels, "project", firstNonEmpty(r.ProjectName, r.ProjectID))
	putLabel(labels, "environment", firstNonEmpty(r.EnvironmentName, r.EnvironmentID))
	putLabel(labels, "service", firstNonEmpty(r.ServiceName, r.ServiceID))
	putLabel(labels, "level", string(r.Level))
	putLabel(labels, "kind", string(r.Kind))
	for k, v := range s.extraLabels {
		labels[sanitizeLabel(k)] = v
	}
	return labels
}

// structuredMetadata carries high-cardinality identity + custom attributes.
func structuredMetadata(r model.LogRecord) map[string]string {
	meta := map[string]string{}
	putLabel(meta, "severity", r.Severity)
	putLabel(meta, "deployment_id", r.DeploymentID)
	putLabel(meta, "deployment_instance_id", r.DeploymentInstanceID)
	putLabel(meta, "region", r.Region)
	putLabel(meta, "plugin_id", r.PluginID)
	putLabel(meta, "snapshot_id", r.SnapshotID)
	// User-supplied attribute keys are sanitized, which can collide with a
	// reserved key (e.g. an app field named "region") or with another attribute
	// that differs only in punctuation. uniqueKey disambiguates rather than
	// letting a last-writer-wins overwrite silently corrupt a value.
	for k, v := range r.Attributes {
		meta[uniqueKey(meta, sanitizeLabel(k))] = v
	}
	if len(meta) == 0 {
		return nil
	}
	return meta
}

// uniqueKey returns key if unused, else key_2, key_3, … so no existing entry
// (reserved field or earlier attribute) is overwritten.
func uniqueKey(m map[string]string, key string) string {
	if _, exists := m[key]; !exists {
		return key
	}
	for i := 2; ; i++ {
		alt := key + "_" + strconv.Itoa(i)
		if _, ok := m[alt]; !ok {
			return alt
		}
	}
}

func putLabel(m map[string]string, key, val string) {
	if val != "" {
		m[key] = val
	}
}

func streamKey(labels map[string]string) string {
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, k := range keys {
		b.WriteString(k)
		b.WriteByte('=')
		b.WriteString(labels[k])
		b.WriteByte('\x00')
	}
	return b.String()
}

// sanitizeLabel maps arbitrary attribute keys to valid Loki label names.
func sanitizeLabel(k string) string {
	var b strings.Builder
	for i, r := range k {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r == '_':
			b.WriteRune(r)
		case r >= '0' && r <= '9' && i > 0:
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	if b.Len() == 0 {
		return "_"
	}
	return b.String()
}

func firstNonEmpty(a, b string) string {
	if a != "" {
		return a
	}
	return b
}
