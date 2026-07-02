package logs

import (
	"context"
	"log/slog"
	"time"

	"github.com/jratienza65/intermodal/internal/model"
	"github.com/jratienza65/intermodal/internal/sink"
	"github.com/jratienza65/intermodal/internal/telemetry"
)

// worker owns one sink, buffering records in a bounded queue and flushing them
// in batches. Enqueue never blocks: if the queue is full the record is dropped
// and counted, so a slow sink can never stall the Railway subscription.
type worker struct {
	sink         sink.Sink
	ch           chan model.LogRecord
	batchSize    int
	batchTimeout time.Duration
	log          *slog.Logger
	done         chan struct{}
}

func newWorker(s sink.Sink, queueSize, batchSize int, batchTimeout time.Duration, log *slog.Logger) *worker {
	if queueSize <= 0 {
		queueSize = 10000
	}
	if batchSize <= 0 {
		batchSize = 500
	}
	if batchTimeout <= 0 {
		batchTimeout = 2 * time.Second
	}
	return &worker{
		sink:         s,
		ch:           make(chan model.LogRecord, queueSize),
		batchSize:    batchSize,
		batchTimeout: batchTimeout,
		log:          log,
		done:         make(chan struct{}),
	}
}

// enqueue offers a record to the queue without blocking.
func (w *worker) enqueue(r model.LogRecord) {
	select {
	case w.ch <- r:
	default:
		telemetry.LogsDropped.WithLabelValues(w.sink.Name()).Inc()
	}
}

// run consumes the queue until ctx is cancelled, then drains and closes.
func (w *worker) run(ctx context.Context) {
	defer close(w.done)
	ticker := time.NewTicker(w.batchTimeout)
	defer ticker.Stop()

	batch := make([]model.LogRecord, 0, w.batchSize)
	flush := func(fctx context.Context) {
		if len(batch) == 0 {
			return
		}
		w.write(fctx, batch)
		batch = batch[:0]
	}

	for {
		select {
		case <-ctx.Done():
			w.drainAndClose(batch)
			return
		case r := <-w.ch:
			batch = append(batch, r)
			if len(batch) >= w.batchSize {
				// If shutdown raced in, hand off to drainAndClose so the flush
				// uses its fresh context, not the already-cancelled ctx.
				if ctx.Err() != nil {
					w.drainAndClose(batch)
					return
				}
				flush(ctx)
			}
		case <-ticker.C:
			if ctx.Err() != nil {
				w.drainAndClose(batch)
				return
			}
			flush(ctx)
		}
	}
}

// drainAndClose flushes everything still queued (using a fresh, bounded context
// since ctx is already cancelled) and closes the sink.
func (w *worker) drainAndClose(batch []model.LogRecord) {
	fctx, cancel := context.WithTimeout(context.WithoutCancel(context.Background()), 10*time.Second)
	defer cancel()

	for {
		select {
		case r := <-w.ch:
			batch = append(batch, r)
			if len(batch) >= w.batchSize {
				w.write(fctx, batch)
				batch = batch[:0]
			}
		default:
			if len(batch) > 0 {
				w.write(fctx, batch)
			}
			if err := w.sink.Close(fctx); err != nil {
				w.log.Warn("sink close failed", "sink", w.sink.Name(), "err", err)
			}
			return
		}
	}
}

// write sends a batch to the sink with bounded retries; a batch that never
// succeeds is dropped (and counted) rather than blocking the pipeline forever.
func (w *worker) write(ctx context.Context, batch []model.LogRecord) {
	const maxAttempts = 3
	name := w.sink.Name()
	for attempt := 1; ; attempt++ {
		err := w.sink.Write(ctx, batch)
		if err == nil {
			telemetry.LogsForwarded.WithLabelValues(name).Add(float64(len(batch)))
			return
		}
		telemetry.LogSinkErrors.WithLabelValues(name).Inc()
		if attempt >= maxAttempts || ctx.Err() != nil {
			w.log.Warn("sink write failed; dropping batch", "sink", name, "records", len(batch), "err", err)
			telemetry.LogsDropped.WithLabelValues(name).Add(float64(len(batch)))
			return
		}
		select {
		case <-ctx.Done():
			telemetry.LogsDropped.WithLabelValues(name).Add(float64(len(batch)))
			return
		case <-time.After(retryBackoff(attempt)):
		}
	}
}

func retryBackoff(attempt int) time.Duration {
	d := 250 * time.Millisecond << (attempt - 1)
	if max := 5 * time.Second; d > max || d <= 0 {
		d = max
	}
	return d
}
