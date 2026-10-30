package railway

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// TestLogPagesUseAnchorPaging pins the variables the history queries send.
// Railway pages reliably only with anchorDate + afterLimit; afterDate without
// an anchor returns the newest lines instead.
func TestLogPagesUseAnchorPaging(t *testing.T) {
	var got []map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			Variables map[string]any `json:"variables"`
		}
		_ = json.NewDecoder(r.Body).Decode(&body)
		got = append(got, body.Variables)
		_, _ = w.Write([]byte(`{"data":{"environmentLogs":[{"timestamp":"2026-01-15T12:00:00Z","message":"hi"}],"httpLogs":[{"timestamp":"2026-01-15T12:00:00Z","path":"/"}]}}`))
	}))
	defer srv.Close()
	c := fastClient(Auth{Token: "t", Type: TokenAccount}, srv.URL)

	anchor := time.Date(2026, 1, 15, 12, 0, 0, 5, time.UTC)
	deploy, err := c.EnvironmentLogsPage(context.Background(), LogPageParams{ID: "env", Filter: "@service:s", Anchor: anchor, AfterLimit: 1000})
	if err != nil || len(deploy) != 1 || deploy[0].Message != "hi" {
		t.Fatalf("EnvironmentLogsPage = %v, %v", deploy, err)
	}
	httpLines, err := c.HTTPLogsPage(context.Background(), LogPageParams{ID: "dep", Anchor: anchor, AfterLimit: 1000})
	if err != nil || len(httpLines) != 1 || httpLines[0].Path != "/" {
		t.Fatalf("HTTPLogsPage = %v, %v", httpLines, err)
	}

	for i, idKey := range []string{"environmentId", "deploymentId"} {
		v := got[i]
		if v[idKey] == nil || v["anchorDate"] != "2026-01-15T12:00:00.000000005Z" ||
			v["beforeLimit"] != float64(0) || v["afterLimit"] != float64(1000) {
			t.Errorf("query %d variables = %v", i, v)
		}
		if _, ok := v["afterDate"]; ok {
			t.Errorf("query %d sent afterDate", i)
		}
	}
	if got[0]["filter"] != "@service:s" {
		t.Errorf("filter = %v", got[0]["filter"])
	}
	if _, ok := got[1]["filter"]; ok {
		t.Errorf("empty filter was sent")
	}
}

func TestRateLimitHookReportsLimit(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("X-RateLimit-Remaining", "9324")
		w.Header().Set("X-RateLimit-Limit", "10000")
		_, _ = w.Write([]byte(`{"data":{}}`))
	}))
	defer srv.Close()
	var remaining, limit int
	c := New(Options{
		Auth:          Auth{Token: "t", Type: TokenAccount},
		HTTPEndpoints: []string{srv.URL},
		Hooks: Hooks{OnRateLimit: func(r, l int, _ time.Time) {
			remaining, limit = r, l
		}},
	})
	if err := c.Execute(context.Background(), Request{OpName: "x", Query: "{x}"}, nil); err != nil {
		t.Fatal(err)
	}
	if remaining != 9324 || limit != 10000 {
		t.Fatalf("hook got remaining=%d limit=%d", remaining, limit)
	}
}
