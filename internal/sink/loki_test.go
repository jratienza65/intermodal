package sink

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"testing"
	"time"

	"github.com/jratienza65/intermodal/internal/config"
	"github.com/jratienza65/intermodal/internal/model"
)

type capturedRequest struct {
	path        string
	contentType string
	orgID       string
	body        []byte
}

// lokiTestServer stands up an httptest.Server that records the last request and
// replies with the given status.
func lokiTestServer(t *testing.T, status int, capture *capturedRequest) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b, _ := io.ReadAll(r.Body)
		if capture != nil {
			capture.path = r.URL.Path
			capture.contentType = r.Header.Get("Content-Type")
			capture.orgID = r.Header.Get("X-Scope-OrgID")
			capture.body = b
		}
		w.WriteHeader(status)
	}))
	t.Cleanup(srv.Close)
	return srv
}

func buildLoki(t *testing.T, cfg *config.Config) Sink {
	t.Helper()
	s, err := Build("loki", Options{Config: cfg})
	if err != nil {
		t.Fatalf("Build(loki) error = %v", err)
	}
	return s
}

func TestLokiWritePushesBatch(t *testing.T) {
	var cap capturedRequest
	srv := lokiTestServer(t, http.StatusNoContent, &cap)

	cfg := &config.Config{
		LokiURL:      srv.URL,
		LokiTenantID: "tenant-42",
	}
	s := buildLoki(t, cfg)

	ts := time.Date(2026, 7, 2, 10, 0, 0, 7, time.UTC)
	batch := []model.LogRecord{
		// Two records with identical stream labels -> should share a stream.
		{Timestamp: ts, Message: "a", Level: model.LevelInfo, ServiceName: "api"},
		{Timestamp: ts.Add(time.Second), Message: "b", Level: model.LevelInfo, ServiceName: "api"},
		// Different level -> different stream.
		{Timestamp: ts, Message: "c", Level: model.LevelError, ServiceName: "api"},
		// Record with attributes that need label-name sanitization.
		{
			Timestamp:   ts,
			Message:     "d",
			Level:       model.LevelWarn,
			ServiceName: "worker",
			Severity:    "warning",
			Attributes:  map[string]string{"http.status": "500", "1bad": "x"},
		},
	}

	if err := s.Write(context.Background(), batch); err != nil {
		t.Fatalf("Write error = %v", err)
	}

	if cap.path != "/loki/api/v1/push" {
		t.Errorf("path = %q, want /loki/api/v1/push", cap.path)
	}
	if cap.contentType != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", cap.contentType)
	}
	if cap.orgID != "tenant-42" {
		t.Errorf("X-Scope-OrgID = %q, want tenant-42", cap.orgID)
	}

	var push lokiPush
	if err := json.Unmarshal(cap.body, &push); err != nil {
		t.Fatalf("unmarshal body: %v\nbody=%s", err, cap.body)
	}

	// 3 distinct label sets: info/api, error/api, warn/worker.
	if len(push.Streams) != 3 {
		t.Fatalf("got %d streams, want 3: %+v", len(push.Streams), push.Streams)
	}

	var infoAPI *lokiStream
	var warnWorker *lokiStream
	for i := range push.Streams {
		st := &push.Streams[i]
		if st.Stream["job"] != "intermodal" {
			t.Errorf("stream missing job=intermodal: %v", st.Stream)
		}
		if st.Stream["source"] != "railway" {
			t.Errorf("stream missing source=railway: %v", st.Stream)
		}
		switch {
		case st.Stream["level"] == "info" && st.Stream["service"] == "api":
			infoAPI = st
		case st.Stream["level"] == "warn" && st.Stream["service"] == "worker":
			warnWorker = st
		}
	}

	if infoAPI == nil {
		t.Fatal("no info/api stream found")
	}
	// Two records shared this stream.
	if len(infoAPI.Values) != 2 {
		t.Errorf("info/api stream has %d values, want 2", len(infoAPI.Values))
	}

	// Value shape: [ts_ns, message, {metadata?}].
	v := infoAPI.Values[0]
	if len(v) < 2 {
		t.Fatalf("value too short: %v", v)
	}
	tsStr, ok := v[0].(string)
	if !ok {
		t.Fatalf("ts not a string: %T", v[0])
	}
	if _, err := strconv.ParseInt(tsStr, 10, 64); err != nil {
		t.Errorf("ts_ns not parseable int: %q", tsStr)
	}
	if v[1].(string) != "a" {
		t.Errorf("message = %v, want a", v[1])
	}

	if warnWorker == nil {
		t.Fatal("no warn/worker stream found")
	}
	if len(warnWorker.Values) != 1 {
		t.Fatalf("warn/worker has %d values, want 1", len(warnWorker.Values))
	}
	wv := warnWorker.Values[0]
	if len(wv) != 3 {
		t.Fatalf("expected structured metadata element, got %v", wv)
	}
	meta, ok := wv[2].(map[string]any)
	if !ok {
		t.Fatalf("metadata not an object: %T", wv[2])
	}
	// Attribute keys sanitized: "http.status" -> "http_status", "1bad" -> "_bad".
	if meta["http_status"] != "500" {
		t.Errorf("metadata http_status = %v, want 500 (sanitized key)", meta["http_status"])
	}
	if meta["_bad"] != "x" {
		t.Errorf("metadata _bad = %v, want x (leading-digit sanitized)", meta["_bad"])
	}
	if meta["severity"] != "warning" {
		t.Errorf("metadata severity = %v, want warning", meta["severity"])
	}
}

func TestLokiNoTenantOmitsHeader(t *testing.T) {
	var cap capturedRequest
	srv := lokiTestServer(t, http.StatusNoContent, &cap)

	s := buildLoki(t, &config.Config{LokiURL: srv.URL})
	err := s.Write(context.Background(), []model.LogRecord{
		{Timestamp: time.Now(), Message: "m", Level: model.LevelInfo},
	})
	if err != nil {
		t.Fatalf("Write error = %v", err)
	}
	if cap.orgID != "" {
		t.Errorf("X-Scope-OrgID = %q, want empty when no tenant configured", cap.orgID)
	}
}

func TestLokiNon2xxReturnsError(t *testing.T) {
	srv := lokiTestServer(t, http.StatusInternalServerError, nil)
	s := buildLoki(t, &config.Config{LokiURL: srv.URL})
	err := s.Write(context.Background(), []model.LogRecord{
		{Timestamp: time.Now(), Message: "m", Level: model.LevelInfo},
	})
	if err == nil {
		t.Fatal("Write returned nil error on 500 response, want error")
	}
}

func TestLokiEmptyBatchNoop(t *testing.T) {
	s := buildLoki(t, &config.Config{LokiURL: "http://127.0.0.1:1"})
	if err := s.Write(context.Background(), nil); err != nil {
		t.Errorf("Write(nil) error = %v, want nil (no request)", err)
	}
}

func TestLokiMissingURLErrors(t *testing.T) {
	if _, err := Build("loki", Options{Config: &config.Config{}}); err == nil {
		t.Fatal("Build(loki) with empty LokiURL returned nil error, want error")
	}
}
