package railway

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"golang.org/x/time/rate"
)

// Default Railway API endpoints. `.com` is the canonical post-rebrand host;
// `.app` is kept as a still-live fallback for transport-level failures.
var (
	DefaultHTTPEndpoints = []string{
		"https://backboard.railway.com/graphql/v2",
		"https://backboard.railway.app/graphql/v2",
	}
	DefaultWSEndpoints = []string{
		"wss://backboard.railway.com/graphql/v2",
		"wss://backboard.railway.app/graphql/v2",
	}
)

const maxRespBytes = 8 << 20 // 8 MiB cap on a single GraphQL response body

// Hooks let the caller observe API traffic (wired to telemetry in main) without
// coupling this package to Prometheus.
type Hooks struct {
	OnRequest   func(op string, status int, err error)
	OnRateLimit func(remaining int, reset time.Time)
}

func (h Hooks) onRequest(op string, status int, err error) {
	if h.OnRequest != nil {
		h.OnRequest(op, status, err)
	}
}

func (h Hooks) onRateLimit(remaining int, reset time.Time) {
	if h.OnRateLimit != nil {
		h.OnRateLimit(remaining, reset)
	}
}

// Options configures a Client. Only Auth is required; everything else defaults.
type Options struct {
	Auth          Auth
	HTTPEndpoints []string
	WSEndpoints   []string
	HTTPClient    *http.Client
	RPS           float64
	Burst         int
	MaxRetries    int
	UserAgent     string
	Hooks         Hooks
	Logger        *slog.Logger
	// RateLimitFloor is the minimum wait applied when Cloudflare rate-limits
	// (1015) without a Retry-After header. Defaults to 15s.
	RateLimitFloor time.Duration
}

// Client is a Railway GraphQL client. It is safe for concurrent use.
type Client struct {
	auth           Auth
	endpoints      []string
	wsEndpoints    []string
	http           *http.Client
	limiter        *rate.Limiter
	maxRetries     int
	userAgent      string
	hooks          Hooks
	log            *slog.Logger
	rateLimitFloor time.Duration
}

// New builds a Client, applying defaults for any unset option.
func New(opts Options) *Client {
	c := &Client{
		auth:           opts.Auth,
		endpoints:      opts.HTTPEndpoints,
		wsEndpoints:    opts.WSEndpoints,
		http:           opts.HTTPClient,
		maxRetries:     opts.MaxRetries,
		userAgent:      opts.UserAgent,
		hooks:          opts.Hooks,
		log:            opts.Logger,
		rateLimitFloor: opts.RateLimitFloor,
	}
	if len(c.endpoints) == 0 {
		c.endpoints = DefaultHTTPEndpoints
	}
	if len(c.wsEndpoints) == 0 {
		c.wsEndpoints = DefaultWSEndpoints
	}
	if c.http == nil {
		c.http = &http.Client{Timeout: 30 * time.Second}
	}
	if c.maxRetries <= 0 {
		c.maxRetries = 4
	}
	if c.userAgent == "" {
		c.userAgent = "intermodal"
	}
	if c.log == nil {
		c.log = slog.Default()
	}
	if c.rateLimitFloor <= 0 {
		c.rateLimitFloor = 15 * time.Second
	}
	rps := opts.RPS
	if rps <= 0 {
		rps = 5 // conservative default; well under the Hobby 10 RPS cap
	}
	burst := opts.Burst
	if burst <= 0 {
		burst = 5
	}
	c.limiter = rate.NewLimiter(rate.Limit(rps), burst)
	return c
}

// Auth returns the client's auth (used by the WebSocket subscription code).
func (c *Client) Auth() Auth { return c.auth }

// WSEndpoints returns the WebSocket endpoints in priority order.
func (c *Client) WSEndpoints() []string { return c.wsEndpoints }

// Logger returns the client's logger.
func (c *Client) Logger() *slog.Logger { return c.log }

// Request is a GraphQL operation. OpName is used both as the `operationName`
// and, critically, as the `?query=` URL parameter — Railway applies a much
// stricter Cloudflare rate-limit bucket to POSTs that carry no query param.
type Request struct {
	OpName    string
	Query     string
	Variables map[string]any
}

type graphqlBody struct {
	Query         string         `json:"query"`
	Variables     map[string]any `json:"variables,omitempty"`
	OperationName string         `json:"operationName,omitempty"`
}

type graphqlResponse struct {
	Data   json.RawMessage `json:"data"`
	Errors []graphqlError  `json:"errors"`
}

type graphqlError struct {
	Message string `json:"message"`
}

// RateLimitError signals a Railway/Cloudflare rate-limit rejection.
type RateLimitError struct {
	RetryAfter time.Duration
	Status     int
}

func (e *RateLimitError) Error() string {
	return fmt.Sprintf("railway: rate limited (status %d, retry after %s)", e.Status, e.RetryAfter)
}

// GraphQLError is returned when Railway responds 200 with an `errors` array.
type GraphQLError struct {
	Messages []string
}

func (e *GraphQLError) Error() string {
	return "railway: graphql error: " + strings.Join(e.Messages, "; ")
}

// Execute runs a GraphQL operation and unmarshals the `data` field into out.
// It retries transient failures (rate limits, 5xx, transport errors) with
// exponential backoff, honoring Retry-After, and rotates to the fallback host
// on transport errors.
func (c *Client) Execute(ctx context.Context, req Request, out any) error {
	body, err := json.Marshal(graphqlBody{
		Query:         req.Query,
		Variables:     req.Variables,
		OperationName: req.OpName,
	})
	if err != nil {
		return fmt.Errorf("railway: marshal request: %w", err)
	}

	var lastErr error
	for attempt := 0; attempt <= c.maxRetries; attempt++ {
		if attempt > 0 {
			wait := backoff(attempt)
			if rle := (&RateLimitError{}); errors.As(lastErr, &rle) && rle.RetryAfter > wait {
				wait = rle.RetryAfter
			}
			c.log.Debug("railway: retrying", "op", req.OpName, "attempt", attempt, "wait", wait, "err", lastErr)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(wait):
			}
		}

		if err := c.limiter.Wait(ctx); err != nil {
			return err
		}

		// Rotate the starting host each attempt so retries alternate .com/.app
		// rather than always re-hitting a degraded primary.
		respBody, status, retryAfter, doErr := c.do(ctx, req.OpName, body, attempt)
		c.hooks.onRequest(req.OpName, status, doErr)
		if doErr != nil {
			// Transport-level error (all hosts failed): retryable.
			lastErr = doErr
			continue
		}

		switch {
		case status == http.StatusOK && !looksLikeCloudflareRateLimit(respBody):
			return decodeGraphQL(respBody, out)
		case status == http.StatusTooManyRequests || looksLikeCloudflareRateLimit(respBody):
			ra := retryAfter
			// Cloudflare's 1015 lock carries no Retry-After but can last minutes;
			// apply a floor so we don't spin through the short default backoff.
			if ra == 0 && looksLikeCloudflareRateLimit(respBody) {
				ra = c.rateLimitFloor
			}
			lastErr = &RateLimitError{RetryAfter: ra, Status: status}
			continue
		case status >= 500:
			lastErr = fmt.Errorf("railway: server error %d: %s", status, snippet(respBody))
			continue
		default:
			// 4xx that isn't a rate limit (e.g. 401/403) is not retryable.
			return fmt.Errorf("railway: unexpected status %d: %s", status, snippet(respBody))
		}
	}
	return fmt.Errorf("railway: %q exhausted retries: %w", req.OpName, lastErr)
}

// do performs one HTTP round trip, rotating hosts on transport failure. It
// returns the (possibly nil) body, HTTP status, parsed Retry-After, and a
// non-nil error only when every endpoint failed at the transport layer.
func (c *Client) do(ctx context.Context, opName string, body []byte, start int) ([]byte, int, time.Duration, error) {
	var lastErr error
	n := len(c.endpoints)
	for i := range n {
		base := c.endpoints[(start+i)%n]
		u := base + "?query=" + url.QueryEscape(opName)
		httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost, u, bytes.NewReader(body))
		if err != nil {
			return nil, 0, 0, err
		}
		httpReq.Header.Set("Content-Type", "application/json")
		httpReq.Header.Set("Accept", "application/json")
		httpReq.Header.Set("User-Agent", c.userAgent)
		c.auth.apply(httpReq.Header)

		resp, err := c.http.Do(httpReq)
		if err != nil {
			lastErr = err
			continue // rotate to fallback host
		}
		b, _ := io.ReadAll(io.LimitReader(resp.Body, maxRespBytes))
		resp.Body.Close()

		c.observeRateLimit(resp.Header)
		return b, resp.StatusCode, parseRetryAfter(resp.Header), nil
	}
	return nil, 0, 0, fmt.Errorf("railway: all endpoints failed: %w", lastErr)
}

func (c *Client) observeRateLimit(h http.Header) {
	remaining := -1
	if v := h.Get("X-RateLimit-Remaining"); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			remaining = n
		}
	}
	var reset time.Time
	if v := h.Get("X-RateLimit-Reset"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil {
			// Reset may be epoch seconds or seconds-from-now; treat large values
			// as epoch, small as a delta.
			if n > 1_000_000_000 {
				reset = time.Unix(n, 0)
			} else {
				reset = time.Now().Add(time.Duration(n) * time.Second)
			}
		}
	}
	if remaining >= 0 {
		c.hooks.onRateLimit(remaining, reset)
	}
}

func decodeGraphQL(body []byte, out any) error {
	var gql graphqlResponse
	if err := json.Unmarshal(body, &gql); err != nil {
		return fmt.Errorf("railway: decode response: %w (body: %s)", err, snippet(body))
	}
	if len(gql.Errors) > 0 {
		msgs := make([]string, len(gql.Errors))
		for i, e := range gql.Errors {
			msgs[i] = e.Message
		}
		return &GraphQLError{Messages: msgs}
	}
	if out == nil {
		return nil
	}
	if err := json.Unmarshal(gql.Data, out); err != nil {
		return fmt.Errorf("railway: decode data: %w", err)
	}
	return nil
}

// looksLikeCloudflareRateLimit detects the HTML "error code: 1015" page that
// Cloudflare returns (instead of a GraphQL JSON error) when the strict bucket
// is hit. Bodies are small HTML, so a substring scan is sufficient.
func looksLikeCloudflareRateLimit(body []byte) bool {
	if len(body) == 0 || bytes.HasPrefix(bytes.TrimSpace(body), []byte("{")) {
		return false
	}
	lower := bytes.ToLower(body)
	return bytes.Contains(lower, []byte("error code: 1015")) ||
		bytes.Contains(lower, []byte("error 1015")) ||
		(bytes.Contains(lower, []byte("cloudflare")) && bytes.Contains(lower, []byte("rate limited")))
}

func parseRetryAfter(h http.Header) time.Duration {
	v := h.Get("Retry-After")
	if v == "" {
		return 0
	}
	if secs, err := strconv.Atoi(v); err == nil {
		return time.Duration(secs) * time.Second
	}
	if t, err := http.ParseTime(v); err == nil {
		if d := time.Until(t); d > 0 {
			return d
		}
	}
	return 0
}

func backoff(attempt int) time.Duration {
	const base = 500 * time.Millisecond
	const max = 30 * time.Second
	d := base << (attempt - 1)
	if d > max || d <= 0 {
		d = max
	}
	return d
}

func snippet(b []byte) string {
	const n = 256
	s := strings.TrimSpace(string(b))
	if len(s) > n {
		return s[:n] + "…"
	}
	return s
}
