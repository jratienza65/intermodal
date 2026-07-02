package logs

import (
	"testing"
	"time"
)

func TestParseTimestamp(t *testing.T) {
	ts, ok := parseTimestamp("2026-07-02T12:34:56.789Z")
	if !ok {
		t.Fatal("expected valid RFC3339Nano to parse")
	}
	if ts.UTC().Format(time.RFC3339) != "2026-07-02T12:34:56Z" {
		t.Errorf("parsed time = %v", ts)
	}

	if _, ok := parseTimestamp(""); ok {
		t.Error("empty timestamp should report not-ok")
	}
	if _, ok := parseTimestamp("not-a-time"); ok {
		t.Error("garbage timestamp should report not-ok")
	}
	// The fallback still yields a usable (current) time so the record delivers.
	if fb, _ := parseTimestamp(""); fb.IsZero() {
		t.Error("fallback timestamp should be now(), not zero")
	}
}
