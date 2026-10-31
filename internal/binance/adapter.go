package binance

import (
	"context"
	"log/slog"
	"time"

	"github.com/edgedepthhq/edgedepth-gateway/internal/candle"
	"github.com/edgedepthhq/edgedepth-gateway/internal/exchange"
	"github.com/edgedepthhq/edgedepth-gateway/pkg/pb"
)

// Binance is the exchange.Exchange adapter for Binance USD-M futures. It is
// the reference venue implementation: transport in stream.go, JSON shapes and
// the orderbook sync procedure in feed.go, REST in rest.go.
type Binance struct {
	log *slog.Logger
}

// New creates the adapter.
func New(log *slog.Logger) *Binance {
	return &Binance{log: log}
}

// ID is "binancef" because that is the venue string the terminal uses for
// Binance futures. It predates this gateway and cannot change here.
func (b *Binance) ID() string { return "binancef" }

// Symbols returns the exchangeInfo whitelist.
func (b *Binance) Symbols(ctx context.Context) (map[string]bool, error) {
	return ExchangeSymbols(ctx)
}

// NewFeed creates the per-symbol feed.
func (b *Binance) NewFeed(symbol string, emit exchange.Emit) exchange.Feed {
	return NewFeed(symbol, b.log, emit)
}

// GlobalTicker streams !ticker@arr, the all-market 24h ticker.
func (b *Binance) GlobalTicker(emit exchange.Emit) exchange.Runner {
	return NewTickerFeed(b.log, func(u *pb.Ticker24HUpdate) {
		emit(pb.Stream_STREAM_TICKER24H, 0, u.TimestampMs, u)
	})
}

// HistoricalCandles serves REST klines for 1m and up. Sub-minute timeframes
// have no REST source on Binance and legitimately start empty, filling from
// live trades.
func (b *Binance) HistoricalCandles(ctx context.Context, symbol string, tfSec int64, count int, endTimeMs int64) ([]*pb.Candle, error) {
	if candle.SubMinute(tfSec) {
		return nil, exchange.ErrUnsupported
	}
	kl, err := Klines(ctx, symbol, tfSec, count, endTimeMs)
	if err != nil {
		return nil, err
	}
	out := make([]*pb.Candle, 0, len(kl))
	for i := range kl {
		k := &kl[i]
		// Binance gives taker BUY volume; the sell side is the remainder.
		vsell := k.Volume - k.TakerBuyVolume
		if vsell < 0 {
			vsell = 0
		}
		out = append(out, &pb.Candle{
			Open:        k.Open,
			High:        k.High,
			Low:         k.Low,
			Close:       k.Close,
			Volume:      k.Volume,
			Vbuy:        k.TakerBuyVolume,
			Vsell:       vsell,
			TimestampMs: k.OpenTime,
			Timeframe:   tfSec,
			// The final kline is still forming unless its close time has passed.
			Final: k.CloseTime < time.Now().UnixMilli(),
		})
	}
	return out, nil
}

// SpotRESTBase and SpotWSBase are the Binance spot hosts. Overridable like
// the futures ones (see cmd/edgedepth-gateway) for region mirrors or tests.
var (
	SpotRESTBase = "https://api.binance.com"
	SpotWSBase   = "wss://stream.binance.com:9443"
)

// Spot is the ticker-only exchange.Exchange adapter for Binance spot,
// registered as "binancespot". It exists to seed the terminal's Spot-Perp
// basis layer: the global 24h ticker (same !ticker@arr schema as futures,
// one push per second) is enough for that, so there is deliberately no
// depth/trade feed behind it - the terminal never chart-subscribes this
// venue, and a stray subscription just gets an idle feed that never syncs.
type Spot struct {
	log *slog.Logger
}

// NewSpot creates the adapter.
func NewSpot(log *slog.Logger) *Spot {
	return &Spot{log: log}
}

// ID is "binancespot": the venue string the terminal keys the spot ticker
// subscription with. Distinct from "binancef" so TickerManager can hold both.
func (s *Spot) ID() string { return "binancespot" }

// Symbols returns the spot exchangeInfo whitelist. The spot response shares
// the futures parser shape ({symbol,status}), so one helper serves both.
func (s *Spot) Symbols(ctx context.Context) (map[string]bool, error) {
	return exchangeSymbolsAt(ctx, SpotRESTBase, "/api/v3/exchangeInfo")
}

// NewFeed returns an idle placeholder: the terminal never subscribes spot
// depth, and returning a feed that never syncs beats a nil that would panic
// the hub if a stray subscription ever arrived.
func (s *Spot) NewFeed(symbol string, emit exchange.Emit) exchange.Feed {
	return idleFeed{}
}

// GlobalTicker streams the spot !ticker@arr feed, one push per second.
func (s *Spot) GlobalTicker(emit exchange.Emit) exchange.Runner {
	return NewSpotTickerFeed(s.log, func(u *pb.Ticker24HUpdate) {
		emit(pb.Stream_STREAM_TICKER24H, 0, u.TimestampMs, u)
	})
}

// HistoricalCandles: no klines on this adapter - the venue exists for the
// basis layer, not for charts. The hub answers with an empty batch.
func (s *Spot) HistoricalCandles(context.Context, string, int64, int, int64) ([]*pb.Candle, error) {
	return nil, exchange.ErrUnsupported
}

// idleFeed is the placeholder Feed behind the ticker-only venue.
type idleFeed struct{}

func (idleFeed) Run(ctx context.Context)                       { <-ctx.Done() }
func (idleFeed) Snapshot() *pb.BookUpdate                      { return nil }
func (idleFeed) MarkState() (float64, float64, float64, int64) { return 0, 0, 0, 0 }
