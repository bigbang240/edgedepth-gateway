package okx

import (
	"context"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/edgedepthhq/edgedepth-gateway/pkg/pb"
)

// TickerFeed serves the all-SWAP 24h ticker by polling REST.
//
// OKX has no all-market ticker push: the per-instrument tickers channel would
// need 200+ subscriptions to replicate what Binance ships in one frame. The
// terminal's watchlist wants last price, 24h change and quote volume — data
// that is complete on a 30s poll and matches how the Binance feed already
// degrades when !ticker@arr is unreachable.
type TickerFeed struct {
	log  *slog.Logger
	emit func(*pb.Ticker24HUpdate)
	logs atomic.Bool // warn once, not every poll
}

// tickerPollInterval is the REST refresh cadence. The watchlist shows last
// price and 24h change, neither of which needs sub-minute freshness. A var so
// a test does not have to wait 30 seconds.
var tickerPollInterval = 30 * time.Second

// NewTickerFeed creates the global ticker feed.
func NewTickerFeed(log *slog.Logger, emit func(*pb.Ticker24HUpdate)) *TickerFeed {
	return &TickerFeed{log: log.With("feed", "ticker24h"), emit: emit}
}

// Run blocks until ctx is cancelled. The first poll fires immediately so the
// watchlist fills during boot instead of one interval in.
func (t *TickerFeed) Run(ctx context.Context) {
	tick := time.NewTicker(tickerPollInterval)
	defer tick.Stop()
	for n := 0; ; n++ {
		if arr, err := Tickers24h(ctx); err != nil {
			if ctx.Err() == nil && n == 0 {
				t.log.Warn("24h ticker poll failed", "err", err)
			}
		} else if len(arr) > 0 {
			if n == 0 {
				t.log.Info("24h ticker serving from REST poll", "symbols", len(arr), "interval", tickerPollInterval.String())
			}
			t.emit(t.build(arr))
		}
		select {
		case <-ctx.Done():
			return
		case <-tick.C:
		}
	}
}

func (t *TickerFeed) build(arr []tickerJSON) *pb.Ticker24HUpdate {
	out := &pb.Ticker24HUpdate{
		Entries:     make([]*pb.Ticker24HEntry, 0, len(arr)),
		TimestampMs: nowMs(),
	}
	warned := t.logs.Load()
	for _, e := range arr {
		last := numF(e.Last)
		open := numF(e.Open24h)
		change := 0.0
		if open > 0 && last > 0 {
			change = (last - open) / open * 100
		}
		if !warned && open == 0 {
			// A zero open24h (freshly listed instrument) collapses the change
			// to 0 rather than a divide-by-zero inf; worth one line, once.
			t.log.Warn("ticker with zero open24h; change reported as 0", "instId", e.InstID)
			t.logs.Store(true)
			warned = true
		}
		out.Entries = append(out.Entries, &pb.Ticker24HEntry{
			// The instId is already the terminal's venue symbol ("BTC-USDT-SWAP");
			// TickerManager lowercases map keys, so case never mismatches.
			Symbol:      e.InstID,
			LastPrice:   last,
			ChangePct:   change,
			VolumeQuote: numF(e.VolCcy24h) * last,
			EventTimeMs: numI(e.Ts),
		})
	}
	return out
}
