// Package server exposes the Prometheus /metrics endpoint and health probes.
package server

import (
	"context"
	"crypto/subtle"
	"errors"
	"log/slog"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Server serves /metrics, /healthz, /readyz and a root index.
type Server struct {
	http  *http.Server
	log   *slog.Logger
	ready atomic.Bool
}

// New builds a Server. The gatherer feeds /metrics. If authToken is non-empty,
// /metrics requires an "Authorization: Bearer <authToken>" header; the health
// probes stay open.
func New(addr, authToken string, gatherer prometheus.Gatherer, log *slog.Logger) *Server {
	if log == nil {
		log = slog.Default()
	}
	s := &Server{log: log}

	mux := http.NewServeMux()
	var metricsHandler http.Handler = promhttp.HandlerFor(gatherer, promhttp.HandlerOpts{
		ErrorHandling: promhttp.ContinueOnError,
	})
	if authToken != "" {
		metricsHandler = bearerAuth(authToken, metricsHandler)
	}
	mux.Handle("/metrics", metricsHandler)
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) {
		writeText(w, http.StatusOK, "ok")
	})
	mux.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) {
		if s.ready.Load() {
			writeText(w, http.StatusOK, "ready")
			return
		}
		writeText(w, http.StatusServiceUnavailable, "not ready")
	})
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/" {
			http.NotFound(w, r)
			return
		}
		writeText(w, http.StatusOK, "intermodal — Railway metrics exporter + log drain\n\n/metrics  Prometheus metrics\n/healthz  liveness\n/readyz   readiness\n")
	})

	s.http = &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}
	return s
}

// SetReady marks the readiness probe.
func (s *Server) SetReady(ready bool) { s.ready.Store(ready) }

// Run serves until ctx is cancelled, then gracefully shuts down.
func (s *Server) Run(ctx context.Context) error {
	errc := make(chan error, 1)
	go func() {
		s.log.Info("http server listening", "addr", s.http.Addr)
		errc <- s.http.ListenAndServe()
	}()

	select {
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
		defer cancel()
		return s.http.Shutdown(shutdownCtx)
	case err := <-errc:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	}
}

// bearerAuth guards next with a constant-time bearer-token check.
func bearerAuth(token string, next http.Handler) http.Handler {
	want := []byte("Bearer " + token)
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		got := []byte(r.Header.Get("Authorization"))
		if subtle.ConstantTimeCompare(got, want) != 1 {
			w.Header().Set("WWW-Authenticate", "Bearer")
			writeText(w, http.StatusUnauthorized, "unauthorized")
			return
		}
		next.ServeHTTP(w, r)
	})
}

func writeText(w http.ResponseWriter, status int, body string) {
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.WriteHeader(status)
	_, _ = w.Write([]byte(body))
}
