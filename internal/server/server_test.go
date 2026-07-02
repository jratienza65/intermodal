package server

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/prometheus/client_golang/prometheus"
)

func TestMetricsAuth(t *testing.T) {
	s := New("127.0.0.1:0", "secret", prometheus.NewRegistry(), nil)
	h := s.http.Handler

	// No credentials -> 401.
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("unauthenticated /metrics = %d, want 401", rec.Code)
	}

	// Wrong token -> 401.
	rec = httptest.NewRecorder()
	req := httptest.NewRequest(http.MethodGet, "/metrics", nil)
	req.Header.Set("Authorization", "Bearer nope")
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusUnauthorized {
		t.Errorf("wrong-token /metrics = %d, want 401", rec.Code)
	}

	// Correct token -> 200.
	rec = httptest.NewRecorder()
	req = httptest.NewRequest(http.MethodGet, "/metrics", nil)
	req.Header.Set("Authorization", "Bearer secret")
	h.ServeHTTP(rec, req)
	if rec.Code != http.StatusOK {
		t.Errorf("authenticated /metrics = %d, want 200", rec.Code)
	}

	// Health probe stays open regardless of auth.
	rec = httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if rec.Code != http.StatusOK {
		t.Errorf("/healthz = %d, want 200", rec.Code)
	}
}

func TestMetricsOpenWhenNoToken(t *testing.T) {
	s := New("127.0.0.1:0", "", prometheus.NewRegistry(), nil)
	rec := httptest.NewRecorder()
	s.http.Handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if rec.Code != http.StatusOK {
		t.Errorf("open /metrics = %d, want 200", rec.Code)
	}
}
