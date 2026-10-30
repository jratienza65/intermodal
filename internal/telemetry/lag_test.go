package telemetry

import (
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/testutil"
)

func TestCursorLagIsComputedAtCollection(t *testing.T) {
	c := newLagCollector()
	now := time.Date(2026, 1, 15, 12, 0, 0, 0, time.UTC)
	c.now = func() time.Time { return now }

	tr := c.Track("deploy", "acme", "prod", "", now.Add(-10*time.Second))
	// Old and new HTTP pollers of one service during a handover share labels;
	// the worse lag wins.
	c.Track("http", "acme", "prod", "api", now.Add(-time.Minute))
	c.Track("http", "acme", "prod", "api", now.Add(-5*time.Second))

	// No poll finished, but time passed: the lag must grow by itself.
	now = now.Add(10 * time.Minute)
	want := `
# HELP intermodal_log_cursor_lag_seconds Seconds between now and the point up to which a log poller has delivered every line. service is empty for deploy logs, which are polled per environment.
# TYPE intermodal_log_cursor_lag_seconds gauge
intermodal_log_cursor_lag_seconds{environment="prod",kind="deploy",project="acme",service=""} 610
intermodal_log_cursor_lag_seconds{environment="prod",kind="http",project="acme",service="api"} 660
`
	if err := testutil.CollectAndCompare(c, strings.NewReader(want)); err != nil {
		t.Fatal(err)
	}

	tr.Stop()
	if n := testutil.CollectAndCount(c); n != 1 {
		t.Fatalf("%d series after Stop, want 1", n)
	}
}
