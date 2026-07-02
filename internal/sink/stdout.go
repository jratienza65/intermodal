package sink

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"sync"

	"github.com/jratienza65/intermodal/internal/model"
)

func init() { Register("stdout", newStdout) }

// stdoutSink writes each record as a JSON line to stdout. Useful for local
// debugging and for hand-off to a stdout-tailing collector (e.g. Vector).
type stdoutSink struct {
	mu  sync.Mutex
	enc *json.Encoder
	log *slog.Logger
}

func newStdout(opts Options) (Sink, error) {
	return &stdoutSink{enc: json.NewEncoder(os.Stdout), log: opts.Logger}, nil
}

func (s *stdoutSink) Name() string { return "stdout" }

func (s *stdoutSink) Write(_ context.Context, batch []model.LogRecord) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, r := range batch {
		if err := s.enc.Encode(EncodeRecord(r)); err != nil {
			return err
		}
	}
	return nil
}

func (s *stdoutSink) Close(context.Context) error { return nil }
