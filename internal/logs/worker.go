package logs

import (
	"context"
	"log/slog"
	"sync"
	"time"

	"github.com/jratienza65/intermodal/internal/model"
	"github.com/jratienza65/intermodal/internal/sink"
	"github.com/jratienza65/intermodal/internal/telemetry"
)

// worker owns one sink, buffering records in a bounded queue and flushing them
// in batches. enqueue never blocks: if the queue is full the record is dropped
// and counted, so a slow sink can never stall a Railway subscription. Pollers
// use enqueueWait instead, which blocks for room, and then waitWritten, so a
// cursor only moves past lines the sink has accepted.
type worker struct {
	sink         sink.Sink
	ch           chan item
	batchSize    int
	batchTimeout time.Duration
	log          *slog.Logger
	done         chan struct{}

	// sendMu orders sequence numbers with channel sends, so the queue is in
	// seq order and written can be a single watermark. It is held across a
	// blocking send, so the worker itself never takes it.
	sendMu sync.Mutex
	// mu guards the counters below.
	mu       sync.Mutex
	enqueued uint64        // seq of the last record put on the queue
	written  uint64        // seq of the last record the sink write finished for
	failed   uint64        // seq of the last record in a batch that was dropped
	progress chan struct{} // closed and replaced whenever written moves
}

// item is a queued record with its sequence number.
type item struct {
	rec model.LogRecord
	seq uint64
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
		ch:           make(chan item, queueSize),
		batchSize:    batchSize,
		batchTimeout: batchTimeout,
		log:          log,
		done:         make(chan struct{}),
		progress:     make(chan struct{}),
	}
}

// enqueue offers a record to the queue without blocking.
func (w *worker) enqueue(r model.LogRecord) {
	w.sendMu.Lock()
	defer w.sendMu.Unlock()
	seq := w.mark() + 1
	select {
	case w.ch <- item{rec: r, seq: seq}:
		w.setEnqueued(seq)
	default:
		telemetry.LogsDropped.WithLabelValues(w.sink.Name()).Inc()
	}
}

// enqueueWait puts a record on the queue, blocking while it is full. It fails
// only if ctx ends first.
func (w *worker) enqueueWait(ctx context.Context, r model.LogRecord) error {
	w.sendMu.Lock()
	defer w.sendMu.Unlock()
	seq := w.mark() + 1
	select {
	case w.ch <- item{rec: r, seq: seq}:
		w.setEnqueued(seq)
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (w *worker) setEnqueued(seq uint64) {
	w.mu.Lock()
	w.enqueued = seq
	w.mu.Unlock()
}

// mark returns the seq of the last record enqueued so far. Pass it to
// waitWritten after enqueueing more records.
func (w *worker) mark() uint64 {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.enqueued
}

// waitWritten blocks until every record enqueued so far has been through a
// sink write. It reports false if any batch holding a record enqueued after
// since was dropped. Another caller's records can share those batches, so a
// failure may be reported for records that were written; callers re-send in
// that case, which only duplicates.
func (w *worker) waitWritten(ctx context.Context, since uint64) (bool, error) {
	w.mu.Lock()
	target := w.enqueued
	w.mu.Unlock()
	for {
		w.mu.Lock()
		written, failed, progress := w.written, w.failed, w.progress
		w.mu.Unlock()
		if written >= target {
			return failed <= since, nil
		}
		select {
		case <-progress:
		case <-ctx.Done():
			return false, ctx.Err()
		}
	}
}

// settle records that the sink write for a batch ending at seq has finished.
func (w *worker) settle(seq uint64, ok bool) {
	w.mu.Lock()
	defer w.mu.Unlock()
	if seq > w.written {
		w.written = seq
	}
	if !ok && seq > w.failed {
		w.failed = seq
	}
	close(w.progress)
	w.progress = make(chan struct{})
}

// run consumes the queue until ctx is cancelled, then drains and closes.
func (w *worker) run(ctx context.Context) {
	defer close(w.done)
	ticker := time.NewTicker(w.batchTimeout)
	defer ticker.Stop()

	batch := make([]item, 0, w.batchSize)
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
		case it := <-w.ch:
			batch = append(batch, it)
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
func (w *worker) drainAndClose(batch []item) {
	fctx, cancel := context.WithTimeout(context.WithoutCancel(context.Background()), 10*time.Second)
	defer cancel()

	for {
		select {
		case it := <-w.ch:
			batch = append(batch, it)
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

// write sends a batch to the sink and records the outcome for waitWritten.
func (w *worker) write(ctx context.Context, batch []item) {
	recs := make([]model.LogRecord, len(batch))
	for i, it := range batch {
		recs[i] = it.rec
	}
	w.settle(batch[len(batch)-1].seq, w.writeRecords(ctx, recs))
}

// writeRecords sends records to the sink with bounded retries; a batch that
// never succeeds is dropped (and counted) rather than blocking the pipeline
// forever. It reports whether the write succeeded.
func (w *worker) writeRecords(ctx context.Context, batch []model.LogRecord) bool {
	const maxAttempts = 3
	name := w.sink.Name()
	for attempt := 1; ; attempt++ {
		err := w.sink.Write(ctx, batch)
		if err == nil {
			telemetry.LogsForwarded.WithLabelValues(name).Add(float64(len(batch)))
			return true
		}
		telemetry.LogSinkErrors.WithLabelValues(name).Inc()
		if attempt >= maxAttempts || ctx.Err() != nil {
			w.log.Warn("sink write failed; dropping batch", "sink", name, "records", len(batch), "err", err)
			telemetry.LogsDropped.WithLabelValues(name).Add(float64(len(batch)))
			return false
		}
		select {
		case <-ctx.Done():
			telemetry.LogsDropped.WithLabelValues(name).Add(float64(len(batch)))
			return false
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
