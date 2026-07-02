package railway

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func fastClient(auth Auth, endpoints ...string) *Client {
	return New(Options{
		Auth:           auth,
		HTTPEndpoints:  endpoints,
		RPS:            10000,
		Burst:          10000,
		MaxRetries:     4,
		RateLimitFloor: time.Millisecond,
	})
}

func TestLooksLikeCloudflareRateLimit(t *testing.T) {
	cases := []struct {
		body string
		want bool
	}{
		{`{"data":{}}`, false},
		{`<html>error code: 1015</html>`, true},
		{`<html>Error 1015 you are being rate limited</html>`, true},
		{`<html>cloudflare: rate limited</html>`, true},
		{`<html>some other error</html>`, false},
		{``, false},
		{`   {"errors":[]}`, false},
	}
	for _, c := range cases {
		if got := looksLikeCloudflareRateLimit([]byte(c.body)); got != c.want {
			t.Errorf("looksLikeCloudflareRateLimit(%q) = %v, want %v", c.body, got, c.want)
		}
	}
}

func TestParseRetryAfter(t *testing.T) {
	h := http.Header{}
	h.Set("Retry-After", "5")
	if got := parseRetryAfter(h); got != 5*time.Second {
		t.Errorf("seconds: got %v", got)
	}
	h.Set("Retry-After", "")
	if got := parseRetryAfter(h); got != 0 {
		t.Errorf("empty: got %v", got)
	}
}

func TestBackoffGrowsAndCaps(t *testing.T) {
	if backoff(1) != 500*time.Millisecond {
		t.Errorf("backoff(1) = %v", backoff(1))
	}
	if backoff(2) != time.Second {
		t.Errorf("backoff(2) = %v", backoff(2))
	}
	if backoff(50) != 30*time.Second {
		t.Errorf("backoff(50) should cap at 30s, got %v", backoff(50))
	}
}

func TestExecuteSendsQueryParamAndBearer(t *testing.T) {
	var gotQuery, gotAuth, gotProjHdr string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotQuery = r.URL.Query().Get("query")
		gotAuth = r.Header.Get("Authorization")
		gotProjHdr = r.Header.Get("Project-Access-Token")
		w.Write([]byte(`{"data":{"value":42}}`))
	}))
	defer srv.Close()

	c := fastClient(Auth{Token: "tok", Type: TokenAccount}, srv.URL)
	var out struct {
		Value int `json:"value"`
	}
	if err := c.Execute(context.Background(), Request{OpName: "myOp", Query: "query myOp {value}"}, &out); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if gotQuery != "myOp" {
		t.Errorf("?query= = %q, want myOp (avoids Cloudflare strict bucket)", gotQuery)
	}
	if gotAuth != "Bearer tok" {
		t.Errorf("Authorization = %q, want 'Bearer tok'", gotAuth)
	}
	if gotProjHdr != "" {
		t.Errorf("account token must not send Project-Access-Token, got %q", gotProjHdr)
	}
	if out.Value != 42 {
		t.Errorf("decoded value = %d", out.Value)
	}
}

func TestExecuteProjectTokenHeader(t *testing.T) {
	var gotAuth, gotProjHdr string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		gotProjHdr = r.Header.Get("Project-Access-Token")
		w.Write([]byte(`{"data":{}}`))
	}))
	defer srv.Close()

	c := fastClient(Auth{Token: "ptok", Type: TokenProject}, srv.URL)
	if err := c.Execute(context.Background(), Request{OpName: "op", Query: "q"}, nil); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if gotProjHdr != "ptok" {
		t.Errorf("Project-Access-Token = %q, want ptok", gotProjHdr)
	}
	if gotAuth != "" {
		t.Errorf("project token must not send Authorization, got %q", gotAuth)
	}
}

func TestExecuteRetriesOn429ThenSucceeds(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.AddInt32(&calls, 1) == 1 {
			w.WriteHeader(http.StatusTooManyRequests)
			w.Write([]byte(`{"errors":[{"message":"rate"}]}`))
			return
		}
		w.Write([]byte(`{"data":{"ok":true}}`))
	}))
	defer srv.Close()

	c := fastClient(Auth{Token: "t", Type: TokenAccount}, srv.URL)
	var out struct {
		OK bool `json:"ok"`
	}
	if err := c.Execute(context.Background(), Request{OpName: "op", Query: "q"}, &out); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if !out.OK {
		t.Error("expected success after retry")
	}
	if calls < 2 {
		t.Errorf("expected >=2 calls, got %d", calls)
	}
}

func TestExecuteHandlesCloudflare1015(t *testing.T) {
	var calls int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.AddInt32(&calls, 1) == 1 {
			// Cloudflare returns 200/429 with HTML, not GraphQL JSON.
			w.Write([]byte(`<!DOCTYPE html><html>error code: 1015</html>`))
			return
		}
		w.Write([]byte(`{"data":{"ok":true}}`))
	}))
	defer srv.Close()

	c := fastClient(Auth{Token: "t", Type: TokenAccount}, srv.URL)
	var out struct {
		OK bool `json:"ok"`
	}
	if err := c.Execute(context.Background(), Request{OpName: "op", Query: "q"}, &out); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if !out.OK {
		t.Error("expected success after 1015 retry")
	}
}

func TestExecuteReturnsGraphQLError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"errors":[{"message":"Not Authorized"}]}`))
	}))
	defer srv.Close()

	c := fastClient(Auth{Token: "t", Type: TokenAccount}, srv.URL)
	err := c.Execute(context.Background(), Request{OpName: "op", Query: "q"}, nil)
	var gqlErr *GraphQLError
	if !errors.As(err, &gqlErr) {
		t.Fatalf("expected *GraphQLError, got %v", err)
	}
	if len(gqlErr.Messages) != 1 || gqlErr.Messages[0] != "Not Authorized" {
		t.Errorf("messages = %v", gqlErr.Messages)
	}
}

func TestExecuteHostFallback(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"data":{"ok":true}}`))
	}))
	defer srv.Close()

	// First endpoint refuses connections; client must fall back to the second.
	c := fastClient(Auth{Token: "t", Type: TokenAccount}, "http://127.0.0.1:1", srv.URL)
	var out struct {
		OK bool `json:"ok"`
	}
	if err := c.Execute(context.Background(), Request{OpName: "op", Query: "q"}, &out); err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if !out.OK {
		t.Error("expected success via fallback host")
	}
}

func TestExecuteContextCancel(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusTooManyRequests) // always rate-limited => keeps retrying
	}))
	defer srv.Close()

	c := fastClient(Auth{Token: "t", Type: TokenAccount}, srv.URL)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	err := c.Execute(ctx, Request{OpName: "op", Query: "q"}, nil)
	if err == nil {
		t.Fatal("expected error on context cancel")
	}
}
