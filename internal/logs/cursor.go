package logs

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"maps"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Cursor is how far a log poller has delivered. Every line up to and including
// TS has been written to every sink. Seen counts the lines at exactly TS, by
// content hash, so the next page (which starts at TS again) can skip them
// while still delivering any new line that shares the timestamp.
type Cursor struct {
	TS   time.Time      `json:"ts"`
	Seen map[string]int `json:"seen,omitempty"`
}

// advance records a delivered line.
func (c *Cursor) advance(ts time.Time, hash string) {
	if ts.After(c.TS) {
		c.TS = ts
		c.Seen = map[string]int{hash: 1}
		return
	}
	if c.Seen == nil {
		c.Seen = map[string]int{}
	}
	c.Seen[hash]++
}

func (c Cursor) clone() Cursor {
	c.Seen = maps.Clone(c.Seen)
	return c
}

// CursorStore keeps poll cursors, keyed by log source.
type CursorStore interface {
	Load(key string) (Cursor, bool)
	Save(key string, c Cursor) error
	Delete(key string) error
}

// fileCursorStore keeps every cursor in memory and, when path is set, rewrites
// them to one JSON file on each change. The file is replaced atomically, so a
// crash mid-write leaves the previous version.
type fileCursorStore struct {
	path string
	// pruneAfter drops cursors older than this on save: they belong to
	// sources that are gone, and a poller would not resume from that far back.
	pruneAfter time.Duration
	now        func() time.Time

	mu      sync.Mutex
	cursors map[string]Cursor
}

// newCursorStore opens the cursor file at path, or returns an in-memory store
// when path is empty. A missing file is an empty store.
func newCursorStore(path string, pruneAfter time.Duration) (*fileCursorStore, error) {
	s := &fileCursorStore{path: path, pruneAfter: pruneAfter, now: time.Now, cursors: map[string]Cursor{}}
	if path == "" {
		return s, nil
	}
	b, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return s, nil
	}
	if err != nil {
		return nil, fmt.Errorf("logs: read cursor file: %w", err)
	}
	if err := json.Unmarshal(b, &s.cursors); err != nil {
		return nil, fmt.Errorf("logs: parse cursor file %s: %w", path, err)
	}
	return s, nil
}

func (s *fileCursorStore) Load(key string) (Cursor, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	c, ok := s.cursors[key]
	return c.clone(), ok
}

func (s *fileCursorStore) Save(key string, c Cursor) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.cursors[key] = c.clone()
	if s.pruneAfter > 0 {
		cutoff := s.now().Add(-s.pruneAfter)
		for k, v := range s.cursors {
			if v.TS.Before(cutoff) {
				delete(s.cursors, k)
			}
		}
	}
	return s.flush()
}

func (s *fileCursorStore) Delete(key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.cursors[key]; !ok {
		return nil
	}
	delete(s.cursors, key)
	return s.flush()
}

// flush writes the cursors to disk. The caller holds mu.
func (s *fileCursorStore) flush() error {
	if s.path == "" {
		return nil
	}
	b, err := json.Marshal(s.cursors)
	if err != nil {
		return err
	}
	tmp, err := os.CreateTemp(filepath.Dir(s.path), filepath.Base(s.path)+".tmp*")
	if err != nil {
		return fmt.Errorf("logs: write cursor file: %w", err)
	}
	defer os.Remove(tmp.Name()) // no-op after a successful rename
	if _, err := tmp.Write(b); err != nil {
		tmp.Close()
		return fmt.Errorf("logs: write cursor file: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return fmt.Errorf("logs: write cursor file: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("logs: write cursor file: %w", err)
	}
	if err := os.Rename(tmp.Name(), s.path); err != nil {
		return fmt.Errorf("logs: write cursor file: %w", err)
	}
	return nil
}
