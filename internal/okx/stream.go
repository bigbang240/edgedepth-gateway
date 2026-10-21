package okx

import (
	"context"
	"encoding/json"
	"log/slog"
	"math/rand"
	"net/http"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// WSBase is the OKX v5 WebSocket root host. All market channels used here —
// including liquidation-orders, verified live — live on /ws/v5/public.
var WSBase = "wss://ws.okx.com:8443"

func publicURL() string { return WSBase + "/ws/v5/public" }

// arg is one OKX subscription argument. Most channels filter by instId; the
// liquidation-orders channel filters by instType/instFamily.
type arg struct {
	Channel    string `json:"channel"`
	InstID     string `json:"instId,omitempty"`
	InstFamily string `json:"instFamily,omitempty"`
	InstType   string `json:"instType,omitempty"`
}

// event is the OKX WS envelope for both market data and control frames:
//
//	{"arg":{"channel":"books","instId":"BTC-USDT-SWAP"},"action":"snapshot","data":[...]}
//	{"event":"subscribe","arg":{...}}
//	{"event":"error","code":"60013","msg":"..."}
type event struct {
	Arg    arg               `json:"arg"`
	Action string            `json:"action"`
	Data   []json.RawMessage `json:"data"`
	Event  string            `json:"event"`
	Code   string            `json:"code"`
	Msg    string            `json:"msg"`
}

// conn is one self-healing OKX WebSocket connection. Unlike Binance, OKX
// pushes no data until the client subscribes, so every (re)connect re-sends
// the op:subscribe frame and the venue answers with fresh snapshots. That
// makes the reconnect path the normal resync path: no REST snapshot race.
type conn struct {
	url     string
	args    []arg
	onEvent func(arg string, action string, data json.RawMessage)
	onReset func() // after every successful (re)connect, before frames flow
	log     *slog.Logger

	mu   sync.Mutex
	ws   *websocket.Conn
	drop bool // Drop() requested: close on next ping or immediately
}

// newConn builds one connection. url selects the OKX root (public/business);
// args is the subscribe list sent on every connect.
func newConn(url string, args []arg, log *slog.Logger,
	onEvent func(arg string, action string, data json.RawMessage), onReset func()) *conn {
	return &conn{url: url, args: args, onEvent: onEvent, onReset: onReset, log: log}
}

// Drop closes the current socket so the reconnect loop subscribes afresh.
// Used on orderbook desync: OKX has no re-anchor but a fresh subscribe, and a
// fresh subscribe IS a new connection on this venue.
func (c *conn) Drop() {
	c.mu.Lock()
	c.drop = true
	ws := c.ws
	c.mu.Unlock()
	if ws != nil {
		_ = ws.Close()
	}
}

// Run blocks until ctx is cancelled, reconnecting with jittered exponential
// backoff. OKX closes sockets routinely; a clean close is normal life, fast
// repeated failures are the escalation case.
func (c *conn) Run(ctx context.Context) {
	backoff := time.Second
	for ctx.Err() == nil {
		start := time.Now()
		err := c.dial(ctx)
		if ctx.Err() != nil {
			return
		}
		if time.Since(start) > time.Minute {
			backoff = time.Second
		}
		if err != nil {
			c.log.Warn("okx stream dropped, reconnecting", "url", c.url, "err", err, "backoff", backoff.String())
		}
		jitter := time.Duration(rand.Int63n(int64(backoff/2 + 1)))
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff + jitter):
		}
		if backoff < 30*time.Second {
			backoff *= 2
		}
	}
}

func (c *conn) dial(ctx context.Context) error {
	dialCtx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()

	c.mu.Lock()
	c.drop = false
	c.mu.Unlock()

	ws, _, err := websocket.DefaultDialer.DialContext(dialCtx, c.url, http.Header{})
	if err != nil {
		return err
	}
	c.mu.Lock()
	c.ws = ws
	c.mu.Unlock()
	defer func() {
		ws.Close()
		c.mu.Lock()
		if c.ws == ws {
			c.ws = nil
		}
		c.mu.Unlock()
	}()

	done := make(chan struct{})
	defer close(done)

	// Subscribe immediately: OKX delivers nothing until told what it wants.
	sub, _ := json.Marshal(map[string]any{"op": "subscribe", "args": c.args})
	if err := ws.WriteMessage(websocket.TextMessage, sub); err != nil {
		return err
	}

	c.log.Info("okx stream connected", "url", c.url, "channels", len(c.args))
	if c.onReset != nil {
		c.onReset()
	}

	// OKX requires a client "ping" text frame inside every 30s idle window;
	// the server answers "pong". Server-sent "ping" frames expect a "pong"
	// reply. Either side going silent for a minute means a dead socket even
	// if TCP has not noticed yet.
	_ = ws.SetReadDeadline(time.Now().Add(60 * time.Second))
	ping := time.NewTicker(20 * time.Second)
	defer ping.Stop()
	go func() {
		for {
			select {
			case <-ctx.Done():
				_ = ws.Close()
				return
			case <-done:
				return
			case <-ping.C:
				c.mu.Lock()
				dropped := c.drop
				c.mu.Unlock()
				if dropped {
					_ = ws.Close()
					return
				}
				if err := ws.WriteMessage(websocket.TextMessage, []byte("ping")); err != nil {
					_ = ws.Close()
					return
				}
			}
		}
	}()

	for {
		kind, raw, err := ws.ReadMessage()
		if err != nil {
			return err
		}
		_ = ws.SetReadDeadline(time.Now().Add(60 * time.Second))
		if kind != websocket.TextMessage {
			continue
		}
		// Control plane: bare "pong" frames keep the deadline honest and
		// carry no payload; server "ping" expects our "pong".
		if len(raw) == 4 && string(raw) == "pong" {
			continue
		}
		if len(raw) == 3 && string(raw) == "ping" {
			_ = ws.WriteMessage(websocket.TextMessage, []byte("pong"))
			continue
		}
		var ev event
		if err := json.Unmarshal(raw, &ev); err != nil {
			c.log.Warn("okx stream: unparsable frame", "err", err)
			continue
		}
		switch ev.Event {
		case "subscribe", "channel-conn-count":
			continue
		case "error":
			// A rejected subscribe (bad channel name, wrong argument shape)
			// would otherwise sit here forever with a live socket and no data.
			c.log.Warn("okx stream rejected a subscription", "code", ev.Code, "msg", ev.Msg)
			continue
		}
		if len(ev.Data) == 0 {
			continue
		}
		for _, d := range ev.Data {
			c.onEvent(ev.Arg.Channel, ev.Action, d)
		}
	}
}
