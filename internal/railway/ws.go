package railway

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"sync"
	"time"

	"github.com/coder/websocket"
)

// wsSubprotocol is the modern graphql-ws ("graphql-transport-ws") protocol that
// Railway's backboard speaks (message types: connection_init/ack, subscribe,
// next, complete, error, ping/pong).
const wsSubprotocol = "graphql-transport-ws"

type wsMessage struct {
	ID      string          `json:"id,omitempty"`
	Type    string          `json:"type"`
	Payload json.RawMessage `json:"payload,omitempty"`
}

// subscribe runs a single graphql-ws subscription over one WebSocket connection
// and invokes handle for each `next` payload's data. It returns nil when the
// context is cancelled (clean shutdown) or the server sends `complete`, and a
// non-nil error on any protocol/transport failure so the caller can reconnect.
func (c *Client) subscribe(ctx context.Context, query string, vars map[string]any, handle func(data json.RawMessage) error) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	conn, err := c.dialWS(ctx)
	if err != nil {
		return err
	}
	defer conn.Close(websocket.StatusNormalClosure, "")
	conn.SetReadLimit(maxRespBytes)

	var wmu sync.Mutex
	writeJSON := func(m wsMessage) error {
		b, err := json.Marshal(m)
		if err != nil {
			return err
		}
		wmu.Lock()
		defer wmu.Unlock()
		wctx, wcancel := context.WithTimeout(ctx, 10*time.Second)
		defer wcancel()
		return conn.Write(wctx, websocket.MessageText, b)
	}

	initPayload, _ := json.Marshal(c.auth.connectionInitPayload())
	if err := writeJSON(wsMessage{Type: "connection_init", Payload: initPayload}); err != nil {
		return fmt.Errorf("railway: ws connection_init: %w", err)
	}

	subPayload, _ := json.Marshal(map[string]any{"query": query, "variables": vars})
	const subID = "1"

	// Keepalive: proactively ping so idle connections aren't reaped and dead
	// connections surface as a write error. Writes are serialized via wmu.
	go func() {
		t := time.NewTicker(25 * time.Second)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				if err := writeJSON(wsMessage{Type: "ping"}); err != nil {
					return
				}
			}
		}
	}()

	for {
		typ, data, err := conn.Read(ctx)
		if err != nil {
			if ctx.Err() != nil {
				return nil // clean shutdown
			}
			return fmt.Errorf("railway: ws read: %w", err)
		}
		if typ != websocket.MessageText {
			continue
		}
		var m wsMessage
		if err := json.Unmarshal(data, &m); err != nil {
			continue
		}
		switch m.Type {
		case "connection_ack":
			if err := writeJSON(wsMessage{ID: subID, Type: "subscribe", Payload: subPayload}); err != nil {
				return fmt.Errorf("railway: ws subscribe: %w", err)
			}
		case "ping":
			if err := writeJSON(wsMessage{Type: "pong"}); err != nil {
				return fmt.Errorf("railway: ws pong: %w", err)
			}
		case "pong":
			// keepalive reply; ignore
		case "next":
			if m.ID != subID {
				continue
			}
			var res struct {
				Data   json.RawMessage `json:"data"`
				Errors []graphqlError  `json:"errors"`
			}
			if err := json.Unmarshal(m.Payload, &res); err != nil {
				return fmt.Errorf("railway: ws next decode: %w", err)
			}
			if len(res.Errors) > 0 {
				msgs := make([]string, len(res.Errors))
				for i, e := range res.Errors {
					msgs[i] = e.Message
				}
				return &GraphQLError{Messages: msgs}
			}
			if err := handle(res.Data); err != nil {
				return err
			}
		case "error":
			return fmt.Errorf("railway: ws subscription error: %s", snippet(m.Payload))
		case "complete":
			if m.ID == subID {
				return nil
			}
		}
	}
}

func (c *Client) dialWS(ctx context.Context) (*websocket.Conn, error) {
	header := http.Header{}
	header.Set("User-Agent", c.userAgent)
	c.auth.apply(header)

	var lastErr error
	for _, ep := range c.wsEndpoints {
		conn, resp, err := websocket.Dial(ctx, ep, &websocket.DialOptions{
			Subprotocols: []string{wsSubprotocol},
			HTTPHeader:   header,
		})
		if resp != nil && resp.Body != nil {
			resp.Body.Close()
		}
		if err != nil {
			lastErr = err
			continue
		}
		return conn, nil
	}
	return nil, fmt.Errorf("railway: ws dial failed: %w", lastErr)
}
