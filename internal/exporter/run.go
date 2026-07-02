package exporter

import (
	"context"
	"log/slog"
	"time"

	"github.com/jratienza65/intermodal/internal/metrics"
)

// Run pushes the latest metrics snapshot to every exporter on each interval
// tick until ctx is cancelled, then closes them. It is a no-op when there are
// no push exporters (the Prometheus pull path is handled elsewhere).
func Run(ctx context.Context, store *metrics.Store, exporters []Exporter, interval time.Duration, log *slog.Logger) {
	if len(exporters) == 0 {
		return
	}
	if interval <= 0 {
		interval = time.Minute
	}
	if log == nil {
		log = slog.Default()
	}

	push := func() {
		snap := store.Snapshot()
		if snap == nil {
			return
		}
		for _, e := range exporters {
			ectx, cancel := context.WithTimeout(ctx, 30*time.Second)
			if err := e.Export(ectx, snap); err != nil {
				log.Warn("metric export failed", "exporter", e.Name(), "err", err)
			}
			cancel()
		}
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	push()
	for {
		select {
		case <-ctx.Done():
			cctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
			for _, e := range exporters {
				if err := e.Close(cctx); err != nil {
					log.Warn("exporter close failed", "exporter", e.Name(), "err", err)
				}
			}
			cancel()
			return
		case <-ticker.C:
			push()
		}
	}
}
