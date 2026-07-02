package railway

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/coder/websocket"
)

func mustJSON(v any) []byte {
	b, _ := json.Marshal(v)
	return b
}

func wsURL(srv *httptest.Server) string {
	return strings.Replace(srv.URL, "http", "ws", 1)
}

// graphqlWSServer plays the graphql-transport-ws server role: ack the init,
// then on subscribe run onSubscribe with a writer that sends `next`/`error`.
func graphqlWSServer(t *testing.T, onSubscribe func(id string, send func(msg wsMessage))) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := websocket.Accept(w, r, &websocket.AcceptOptions{Subprotocols: []string{wsSubprotocol}})
		if err != nil {
			return
		}
		defer c.Close(websocket.StatusNormalClosure, "")
		ctx := r.Context()
		var mu sync.Mutex
		send := func(m wsMessage) {
			mu.Lock()
			defer mu.Unlock()
			_ = c.Write(ctx, websocket.MessageText, mustJSON(m))
		}
		for {
			_, data, err := c.Read(ctx)
			if err != nil {
				return
			}
			var m wsMessage
			if json.Unmarshal(data, &m) != nil {
				continue
			}
			switch m.Type {
			case "connection_init":
				send(wsMessage{Type: "connection_ack"})
			case "ping":
				send(wsMessage{Type: "pong"})
			case "subscribe":
				go onSubscribe(m.ID, send)
			}
		}
	}))
}

func newWSClient(srv *httptest.Server) *Client {
	return New(Options{
		Auth:        Auth{Token: "t", Type: TokenAccount},
		WSEndpoints: []string{wsURL(srv)},
	})
}

func TestStreamEnvironmentLogsReceivesEntries(t *testing.T) {
	entries := []LogEntry{
		{Timestamp: "2026-07-02T00:00:00Z", Message: "hello", Severity: "info", Tags: LogTags{ServiceID: "s1"}},
		{Timestamp: "2026-07-02T00:00:01Z", Message: "boom", Severity: "err", Tags: LogTags{ServiceID: "s1"}},
	}
	srv := graphqlWSServer(t, func(id string, send func(wsMessage)) {
		send(wsMessage{ID: id, Type: "next", Payload: mustJSON(map[string]any{
			"data": map[string]any{"environmentLogs": entries},
		})})
	})
	defer srv.Close()

	c := newWSClient(srv)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var mu sync.Mutex
	var got []LogEntry
	done := make(chan error, 1)
	go func() {
		done <- c.StreamEnvironmentLogs(ctx, LogStreamParams{EnvironmentID: "e1"}, func(e LogEntry) error {
			mu.Lock()
			got = append(got, e)
			n := len(got)
			mu.Unlock()
			if n >= len(entries) {
				cancel()
			}
			return nil
		})
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("StreamEnvironmentLogs returned error on clean cancel: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for log entries")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(got) != len(entries) {
		t.Fatalf("got %d entries, want %d", len(got), len(entries))
	}
	if got[0].Message != "hello" || got[1].Message != "boom" {
		t.Errorf("entries = %+v", got)
	}
}

func TestStreamEnvironmentLogsSurfacesSubscriptionError(t *testing.T) {
	srv := graphqlWSServer(t, func(id string, send func(wsMessage)) {
		send(wsMessage{ID: id, Type: "error", Payload: mustJSON([]map[string]string{{"message": "bad filter"}})})
	})
	defer srv.Close()

	c := newWSClient(srv)
	err := c.StreamEnvironmentLogs(context.Background(), LogStreamParams{EnvironmentID: "e1"}, func(LogEntry) error {
		return nil
	})
	if err == nil {
		t.Fatal("expected error from subscription 'error' message")
	}
}
