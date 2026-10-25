package okx

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// RESTBase is the OKX v5 REST host. Override for demo trading hosts.
var RESTBase = "https://www.okx.com"

var httpClient = &http.Client{Timeout: 15 * time.Second}

// getOKX fetches one OKX REST endpoint into out. OKX wraps every response in
// {code, msg, data}; a non-zero code is an application error even over HTTP
// 200, so it is checked before the body is trusted.
func getOKX(ctx context.Context, path string, q url.Values, out any) error {
	u := RESTBase + path
	if len(q) > 0 {
		u += "?" + q.Encode()
	}
	var lastErr error
	for attempt := 0; attempt < 2; attempt++ {
		if attempt > 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(500 * time.Millisecond):
			}
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
		if err != nil {
			return err
		}
		resp, err := httpClient.Do(req)
		if err != nil {
			lastErr = err
			continue
		}
		body, err := io.ReadAll(io.LimitReader(resp.Body, 8<<20))
		resp.Body.Close()
		if err != nil {
			lastErr = err
			continue
		}
		if resp.StatusCode != http.StatusOK {
			lastErr = fmt.Errorf("okx %s: http %d", path, resp.StatusCode)
			continue
		}
		var env struct {
			Code string          `json:"code"`
			Msg  string          `json:"msg"`
			Data json.RawMessage `json:"data"`
		}
		if err := json.Unmarshal(body, &env); err != nil {
			lastErr = fmt.Errorf("okx %s: %w", path, err)
			continue
		}
		if env.Code != "" && env.Code != "0" {
			return fmt.Errorf("okx %s: code %s: %s", path, env.Code, env.Msg)
		}
		if out == nil {
			return nil
		}
		return json.Unmarshal(env.Data, out)
	}
	return lastErr
}

// ─── instruments ─────────────────────────────────────────────────────────────

// instrument carries what translating OKX contract units into coin units
// needs. Every OKX SWAP size — book, trade, liquidation — is in CONTRACTS;
// the EdgeDepth wire (and the whole terminal) speaks BASE COIN, so qty =
// sz × ctVal × ctMult is applied at every ingest point in feed.go.
type instrument struct {
	InstID     string  `json:"instId"`
	InstFamily string  `json:"instFamily"`
	CtVal      float64 `json:"ctVal"`
	CtMult     float64 `json:"ctMult"`
	TickSz     float64 `json:"tickSz"`
	LotSz      float64 `json:"lotSz"`
	State      string  `json:"state"`
}

type instrumentJSON struct {
	InstID     string      `json:"instId"`
	InstFamily string      `json:"instFamily"`
	CtVal      json.Number `json:"ctVal"`
	CtMult     json.Number `json:"ctMult"`
	TickSz     json.Number `json:"tickSz"`
	LotSz      json.Number `json:"lotSz"`
	State      string      `json:"state"`
}

func (j instrumentJSON) toInstrument() instrument {
	return instrument{
		InstID:     j.InstID,
		InstFamily: j.InstFamily,
		CtVal:      numF(j.CtVal),
		CtMult:     numF(j.CtMult),
		TickSz:     numF(j.TickSz),
		LotSz:      numF(j.LotSz),
		State:      j.State,
	}
}

// contractSize returns coin units for n contracts, defaulting to 1×1 when the
// venue did not answer (a feed that scales nothing is better than a feed that
// scales by zero and shows an empty book).
func (i instrument) contractSize(contracts float64) float64 {
	ctVal, ctMult := i.CtVal, i.CtMult
	if ctVal <= 0 {
		ctVal = 1
	}
	if ctMult <= 0 {
		ctMult = 1
	}
	return contracts * ctVal * ctMult
}

// Instruments lists USDT/USD-margined perpetual swaps. instID narrows to one
// instrument when non-empty.
func Instruments(ctx context.Context, instID string) ([]instrument, error) {
	q := url.Values{"instType": {"SWAP"}}
	if instID != "" {
		q.Set("instId", instID)
	}
	var raw []instrumentJSON
	if err := getOKX(ctx, "/api/v5/public/instruments", q, &raw); err != nil {
		return nil, err
	}
	out := make([]instrument, 0, len(raw))
	for _, j := range raw {
		out = append(out, j.toInstrument())
	}
	return out, nil
}

// ─── 24h tickers ─────────────────────────────────────────────────────────────

type tickerJSON struct {
	InstID    string  `json:"instId"`
	Last      flexNum `json:"last"`
	Open24h   flexNum `json:"open24h"`
	VolCcy24h flexNum `json:"volCcy24h"`
	Vol24h    flexNum `json:"vol24h"`
	Ts        flexNum `json:"ts"`
}

// Tickers24h polls the all-SWAP 24h ticker snapshot. OKX has no all-market
// ticker push (one tickers channel per instrument would be 200+ subscriptions
// for data that moves once a second at best), so the global feed is poll-only.
// volCcy24h is base-coin volume on SWAP, so quote volume ≈ volCcy24h × last.
func Tickers24h(ctx context.Context) ([]tickerJSON, error) {
	var out []tickerJSON
	err := getOKX(ctx, "/api/v5/market/tickers", url.Values{"instType": {"SWAP"}}, &out)
	return out, err
}

// ─── candles ─────────────────────────────────────────────────────────────────

// barFor maps a timeframe in seconds to an OKX bar string. OKX uniquely
// supports a 1s bar over REST; the other sub-minute buckets (5s/15s/30s the
// terminal's RT grid uses) exist only as live trade aggregation, same as
// Binance.
func barFor(tfSec int64) (string, bool) {
	switch tfSec {
	case 1:
		return "1s", true
	case 60:
		return "1m", true
	case 180:
		return "3m", true
	case 300:
		return "5m", true
	case 900:
		return "15m", true
	case 1800:
		return "30m", true
	case 3600:
		return "1H", true
	case 7200:
		return "2H", true
	case 14400:
		return "4H", true
	case 21600:
		return "6H", true
	case 43200:
		return "12H", true
	case 86400:
		return "1D", true
	case 604800:
		return "1W", true
	default:
		return "", false
	}
}

// candleRow is one OKX candle array entry:
// [ts, o, h, l, c, vol(contracts), volCcy(base coin), volCcyQuote, confirm].
// Newest first on the wire; Candles reverses to oldest-first before returning.
type candleRow struct {
	Ts         int64
	O, H, L, C float64
	Vol        float64 // contracts
	VolCcy     float64 // base coin
	Confirm    bool
}

func parseCandleRow(raw []string) (candleRow, bool) {
	if len(raw) < 9 {
		return candleRow{}, false
	}
	f := func(i int) float64 { v, _ := strconv.ParseFloat(strings.TrimSpace(raw[i]), 64); return v }
	i64 := func(i int) int64 { v, _ := strconv.ParseInt(strings.TrimSpace(raw[i]), 10, 64); return v }
	return candleRow{
		Ts: i64(0),
		O:  f(1), H: f(2), L: f(3), C: f(4),
		Vol: f(5), VolCcy: f(6),
		Confirm: strings.TrimSpace(raw[8]) == "1",
	}, true
}

// Candles fetches up to count candles of the given bar ending at endTimeMs
// (0 = now), OLDEST FIRST. /market/candles serves only recent history, so
// pages past its retention edge continue from /market/history-candles —
// the terminal seeds charts with 500-1500 bars and a silent truncation to a
// few hundred would read as a broken chart.
func Candles(ctx context.Context, instID, bar string, count int, endTimeMs int64) ([]candleRow, error) {
	if count > 1500 {
		count = 1500
	}
	if count <= 0 {
		count = 500
	}

	seen := make(map[int64]struct{}, count)
	var out []candleRow
	path := "/api/v5/market/candles"
	limit := 300
	after := endTimeMs
	for len(out) < count {
		q := url.Values{
			"instId": {instID},
			"bar":    {bar},
			"limit":  {strconv.Itoa(limit)},
		}
		if after > 0 {
			// OKX "after" returns records earlier than the ts. +1 keeps a bar
			// whose open equals the boundary (the forming bar case).
			q.Set("after", strconv.FormatInt(after+1, 10))
		}
		var raw [][]string
		if err := getOKX(ctx, path, q, &raw); err != nil {
			return nil, err
		}
		if len(raw) == 0 {
			if path == "/api/v5/market/candles" {
				// Retention edge reached; the tail lives in history-candles
				// (limit 100 there, and it needs the same after cursor).
				path = "/api/v5/market/history-candles"
				limit = 100
				continue
			}
			break
		}
		oldest := int64(0)
		for _, r := range raw {
			row, ok := parseCandleRow(r)
			if !ok {
				continue
			}
			if _, dup := seen[row.Ts]; dup {
				continue
			}
			seen[row.Ts] = struct{}{}
			out = append(out, row)
			if oldest == 0 || row.Ts < oldest {
				oldest = row.Ts
			}
		}
		if oldest == 0 {
			break
		}
		after = oldest
		if len(raw) < limit/2 && path == "/api/v5/market/candles" {
			// A short page means the recent window is nearly exhausted; stop
			// paging a well that is running dry and switch to the deep well.
			path = "/api/v5/market/history-candles"
			limit = 100
		}
	}
	if len(out) > count {
		out = out[:count]
	}
	// Wire order is newest-first; the hub wants oldest-first.
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return out, nil
}

// ─── shared helpers ──────────────────────────────────────────────────────────

// flexNum is a json.Number that tolerates OKX's empty-string / null numerics.
// The all-market tickers frame spans every SWAP instrument, including freshly
// listed and pre-open contracts whose last/open24h/vol fields arrive as ""
// (or null). A strict json.Number unmarshal rejected the ENTIRE frame for one
// such row, so the OKX watchlist universe collapsed to zero symbols (phase 6
// close). The tolerant type keeps the json.Number surface numF/numI consume:
// an unparseable value decodes as empty and numF/numI map that to 0.
type flexNum json.Number

func (f *flexNum) UnmarshalJSON(b []byte) error {
	s := string(b)
	if s == `""` || s == "null" || s == "" {
		*f = ""
		return nil
	}
	n := json.Number(s)
	if _, err := n.Float64(); err != nil {
		// Not a parseable number despite the wire encoding; keep the frame.
		*f = ""
		return nil
	}
	*f = flexNum(n)
	return nil
}

// numF parses a json.Number tolerantly: OKX mixes string and number scalars
// across endpoints, and a zero from a failed parse would silently multiply
// every contract size into nothing. Generic over ~string so the tolerant
// flexNum fields (tickerJSON) decode through the same path.
func numF[T ~string](n T) float64 {
	v := json.Number(n)
	if v == "" {
		return 0
	}
	f, err := v.Float64()
	if err != nil {
		return 0
	}
	return f
}

func numI[T ~string](n T) int64 {
	v := json.Number(n)
	if v == "" {
		return 0
	}
	i, err := v.Int64()
	if err != nil {
		return 0
	}
	return i
}

// strF parses an optional string scalar ("12.5"); empty/absent → 0.
func strF(s string) float64 {
	if s == "" {
		return 0
	}
	v, _ := strconv.ParseFloat(s, 64)
	return v
}

// log2 helpers reused by feed.go
func warnUnparsable(log *slog.Logger, what string, err error) {
	log.Warn("unparsable "+what+" payload", "err", err)
}
