package sink

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/jratienza65/intermodal/internal/config"
	"github.com/jratienza65/intermodal/internal/model"
)

func TestNormalizeOTLPBase(t *testing.T) {
	cases := []struct {
		ep       string
		insecure bool
		want     string
	}{
		{"otel:4318", true, "http://otel:4318"},
		{"otel:4318", false, "https://otel:4318"},
		{"https://otel.example.com", false, "https://otel.example.com"},
		{"http://otel:4318/", true, "http://otel:4318"},
	}
	for _, c := range cases {
		if got := normalizeOTLPBase(c.ep, c.insecure); got != c.want {
			t.Errorf("normalizeOTLPBase(%q,%v)=%q want %q", c.ep, c.insecure, got, c.want)
		}
	}
}

func resourceByServiceName(p otlpLogsPayload) map[string]otlpResourceLogs {
	out := map[string]otlpResourceLogs{}
	for _, rl := range p.ResourceLogs {
		for _, kv := range rl.Resource.Attributes {
			if kv.Key == "service.name" {
				out[kv.Value.StringValue] = rl
			}
		}
	}
	return out
}

func TestBuildOTLPLogsAttributesToSourceService(t *testing.T) {
	batch := []model.LogRecord{
		{Message: "a", Level: model.LevelInfo, Severity: "info", ServiceID: "s1", ServiceName: "Zitadel", ProjectID: "p", EnvironmentID: "e", Attributes: map[string]string{"tenant": "acme"}},
		{Message: "b", Level: model.LevelError, Severity: "err", ServiceID: "s1", ServiceName: "Zitadel", ProjectID: "p", EnvironmentID: "e"},
		{Message: "c", Level: model.LevelWarn, Severity: "warn", ServiceID: "s2", ServiceName: "kong", ProjectID: "p", EnvironmentID: "e"},
	}
	p := buildOTLPLogs(batch)
	if len(p.ResourceLogs) != 2 {
		t.Fatalf("want 2 resourceLogs (one per source service), got %d", len(p.ResourceLogs))
	}

	byName := resourceByServiceName(p)
	z, ok := byName["Zitadel"]
	if !ok {
		t.Fatalf("no resource with service.name=Zitadel; got %v", byName)
	}
	if len(z.ScopeLogs) != 1 || z.ScopeLogs[0].Scope.Name != "intermodal" {
		t.Errorf("scope should be intermodal: %+v", z.ScopeLogs)
	}
	if n := len(z.ScopeLogs[0].LogRecords); n != 2 {
		t.Fatalf("want 2 Zitadel records, got %d", n)
	}
	rec0 := z.ScopeLogs[0].LogRecords[0]
	if rec0.SeverityNumber != 9 {
		t.Errorf("info severityNumber=%d want 9", rec0.SeverityNumber)
	}
	if rec0.Body.StringValue != "a" {
		t.Errorf("body=%q", rec0.Body.StringValue)
	}
	found := false
	for _, kv := range rec0.Attributes {
		if kv.Key == "tenant" && kv.Value.StringValue == "acme" {
			found = true
		}
	}
	if !found {
		t.Errorf("custom attribute not carried: %+v", rec0.Attributes)
	}
	// The error record maps to severity 17.
	if z.ScopeLogs[0].LogRecords[1].SeverityNumber != 17 {
		t.Errorf("error severityNumber=%d want 17", z.ScopeLogs[0].LogRecords[1].SeverityNumber)
	}
	if _, ok := byName["kong"]; !ok {
		t.Errorf("kong resource missing")
	}
}

func TestOTLPLogSinkWrite(t *testing.T) {
	var gotPath, gotCT string
	var payload otlpLogsPayload
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotCT = r.Header.Get("Content-Type")
		b, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(b, &payload)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	s, err := Build("otlp", Options{Config: &config.Config{OTLPEndpoint: srv.URL, ServiceName: "intermodal"}})
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	err = s.Write(context.Background(), []model.LogRecord{
		{Message: "hi", Level: model.LevelInfo, Severity: "info", ServiceID: "s1", ServiceName: "Zitadel", Timestamp: time.Unix(1, 0)},
	})
	if err != nil {
		t.Fatalf("write: %v", err)
	}
	if gotPath != "/v1/logs" {
		t.Errorf("path=%q want /v1/logs", gotPath)
	}
	if gotCT != "application/json" {
		t.Errorf("content-type=%q", gotCT)
	}
	if len(payload.ResourceLogs) != 1 {
		t.Fatalf("resourceLogs=%d want 1", len(payload.ResourceLogs))
	}
}

func TestOTLPLogSinkWriteError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()
	s, _ := Build("otlp", Options{Config: &config.Config{OTLPEndpoint: srv.URL}})
	if err := s.Write(context.Background(), []model.LogRecord{{Message: "x", ServiceID: "s"}}); err == nil {
		t.Fatal("expected error on 500 response")
	}
}
