// Package okx adapts OKX v5 public market data (perpetual swaps) to the
// EdgeDepth terminal wire. Transport and reconnect in stream.go, JSON shapes
// and contract→coin unit conversion in feed.go, REST in rest.go, the all-SWAP
// 24h ticker poll in ticker.go.
//
// Unit contract: every OKX SWAP size (book, trade, liquidation) is in
// CONTRACTS. Everything on the EdgeDepth wire is BASE COIN. instrument.
// contractSize is the single conversion point, applied at every ingest.
package okx

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/edgedepthhq/edgedepth-gateway/internal/candle"
	"github.com/edgedepthhq/edgedepth-gateway/internal/exchange"
	"github.com/edgedepthhq/edgedepth-gateway/pkg/pb"
)

// OKX is the exchange.Exchange adapter for OKX perpetual swaps.
type OKX struct {
	log *slog.Logger

	mu      sync.Mutex
	byCanon map[string]instrument // canonical instId → instrument
	byLower map[string]instrument // lowercase instId → instrument (URL typos)
}

// New creates the adapter.
func New(log *slog.Logger) *OKX {
	return &OKX{log: log, byCanon: map[string]instrument{}, byLower: map[string]instrument{}}
}

// ID is "okx" — the venue string the terminal's URL router knows after
// feat(phase6). It must match the client's venue id exactly.
func (o *OKX) ID() string { return "okx" }

// Symbols returns the live-SWAP whitelist. Both the canonical instId and its
// lowercase form are admitted: the terminal's URL router keeps venue case for
// OKX symbols, but a hand-typed lowercase deep link
// (/terminal/okx/btc-usdt-swap) should still route; NewFeed canonicalizes.
func (o *OKX) Symbols(ctx context.Context) (map[string]bool, error) {
	insts, err := Instruments(ctx, "")
	if err != nil {
		return nil, err
	}
	set := make(map[string]bool, len(insts)*2)
	o.mu.Lock()
	o.byCanon = make(map[string]instrument, len(insts))
	o.byLower = make(map[string]instrument, len(insts))
	for _, in := range insts {
		if in.State != "" && in.State != "live" {
			continue // pre-open/settled instruments must not route
		}
		o.byCanon[in.InstID] = in
		o.byLower[strings.ToLower(in.InstID)] = in
		set[in.InstID] = true
		set[strings.ToLower(in.InstID)] = true
	}
	o.mu.Unlock()
	return set, nil
}

// resolve canonicalizes any user-supplied symbol to its instrument, falling
// back to a direct REST lookup when the whitelist never loaded (validation is
// a nicety; a feed that guesses ctVal=1 is worse than one that asked).
func (o *OKX) resolve(symbol string) instrument {
	o.mu.Lock()
	if in, ok := o.byCanon[symbol]; ok {
		o.mu.Unlock()
		return in
	}
	if in, ok := o.byLower[strings.ToLower(symbol)]; ok {
		o.mu.Unlock()
		return in
	}
	o.mu.Unlock()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	insts, err := Instruments(ctx, symbol)
	if err != nil || len(insts) == 0 {
		o.log.Warn("okx: unknown instrument, feeding with unit contracts",
			"symbol", symbol, "err", err)
		return instrument{InstID: symbol, InstFamily: symbol}
	}
	in := insts[0]
	o.mu.Lock()
	o.byCanon[in.InstID] = in
	o.byLower[strings.ToLower(in.InstID)] = in
	o.mu.Unlock()
	return in
}

// NewFeed creates the per-instrument feed.
func (o *OKX) NewFeed(symbol string, emit exchange.Emit) exchange.Feed {
	return NewFeed(o.resolve(symbol), o.log, emit)
}

// GlobalTicker polls the all-SWAP 24h ticker over REST (no venue push).
func (o *OKX) GlobalTicker(emit exchange.Emit) exchange.Runner {
	return NewTickerFeed(o.log, func(u *pb.Ticker24HUpdate) {
		emit(pb.Stream_STREAM_TICKER24H, 0, u.TimestampMs, u)
	})
}

// HistoricalCandles serves REST candles for 1s and every bar from 1m up. The
// 5s/15s/30s buckets the terminal's RT grid uses have no REST source and
// legitimately start empty, filling from live trades.
func (o *OKX) HistoricalCandles(ctx context.Context, symbol string, tfSec int64, count int, endTimeMs int64) ([]*pb.Candle, error) {
	if tfSec != 1 {
		if candle.SubMinute(tfSec) {
			return nil, exchange.ErrUnsupported
		}
	}
	bar, ok := barFor(tfSec)
	if !ok {
		return nil, exchange.ErrUnsupported
	}
	inst := o.resolve(symbol)
	rows, err := Candles(ctx, inst.InstID, bar, count, endTimeMs)
	if err != nil {
		return nil, err
	}
	out := make([]*pb.Candle, 0, len(rows))
	for _, r := range rows {
		// OKX publishes no taker split on public candles; a neutral 50/50
		// keeps volume-profile renders sane (all-neutral) instead of pinning
		// the whole bar to one side. Live bars from the trade stream carry
		// the real split.
		half := r.VolCcy / 2
		out = append(out, &pb.Candle{
			Open:        r.O,
			High:        r.H,
			Low:         r.L,
			Close:       r.C,
			Volume:      r.VolCcy,
			Vbuy:        half,
			Vsell:       r.VolCcy - half,
			TimestampMs: r.Ts,
			Timeframe:   tfSec,
			Final:       r.Confirm,
		})
	}
	return out, nil
}
