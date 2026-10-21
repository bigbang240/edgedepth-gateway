package okx

import (
	"context"
	"encoding/json"
	"log/slog"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/edgedepthhq/edgedepth-gateway/internal/exchange"
	"github.com/edgedepthhq/edgedepth-gateway/pkg/pb"
)

// timeNow is a var so tests can pin the clock.
var timeNow = time.Now

// Emit hands a decoded market message to the hub. timeframe is 0 for
// non-timeframed streams.
type Emit = exchange.Emit

// Feed owns every upstream OKX stream for one instrument and turns them into
// EdgeDepth protobuf messages, translating contract units into base coin on
// the way in (every OKX SWAP size is contracts; the wire speaks coin).
//
// The orderbook is maintained locally for the same reason as the Binance
// feed: a client that connects ten minutes in needs a base snapshot before
// diffs mean anything. Unlike Binance there is no REST re-anchor dance —
// OKX's books channel opens with a full snapshot on every subscribe, so the
// reconnect IS the resync.
type Feed struct {
	symbol     string // canonical instId, e.g. "BTC-USDT-SWAP"
	instFamily string // e.g. "BTC-USDT" (liquidation-orders subscribe key)
	inst       instrument

	emit Emit
	log  *slog.Logger

	// dropConn closes the public socket; wired by Run. orderbook desync uses
	// it to force a fresh subscribe (OKX re-opens with a full snapshot).
	dropConn func()

	mu            sync.RWMutex
	bids          map[float64]float64 // price → size in coin
	asks          map[float64]float64 // price → size in coin
	lastSeq       int64
	bookSynced    bool
	lastTrade     float64
	tradeCount    int64
	lastTradeID   int64
	lastTradeTime int64
	tradeReset    func(int64)

	markPrice   float64
	funding     float64
	nextFunding int64
	openInt     float64
}

// NewFeed creates a feed. Call Run to start it.
func NewFeed(inst instrument, log *slog.Logger, emit Emit) *Feed {
	return &Feed{
		symbol:     inst.InstID,
		instFamily: inst.InstFamily,
		inst:       inst,
		emit:       emit,
		log:        log.With("symbol", inst.InstID),
		bids:       make(map[float64]float64),
		asks:       make(map[float64]float64),
	}
}

// SetTradeReset installs the hub's tick-volume minute reset callback
// (exchange.TradeContinuity). OKX tradeId is a monotonic per-instrument
// sequence, so gaps and regressions are detectable the same way as Binance.
func (f *Feed) SetTradeReset(reset func(int64)) { f.tradeReset = reset }

// Run connects the upstream streams and blocks until ctx is cancelled.
//
// One public connection carries everything with instId-scoped channels
// (books, trades, mark-price, funding-rate, open-interest). The
// liquidation-orders channel is family-scoped and REQUIRES instType (60018
// without it), verified live on the public root — it is not on /business.
func (f *Feed) Run(ctx context.Context) {
	resetBook := func() {
		f.mu.Lock()
		f.bids = make(map[float64]float64)
		f.asks = make(map[float64]float64)
		f.lastSeq = 0
		f.bookSynced = false
		f.mu.Unlock()
	}
	resetTrades := func() {
		f.mu.Lock()
		f.lastTradeID = 0
		f.lastTradeTime = 0
		f.mu.Unlock()
		if f.tradeReset != nil {
			f.tradeReset(resetNow())
		}
	}

	public := newConn(publicURL(), []arg{
		{Channel: "books", InstID: f.symbol},
		{Channel: "trades", InstID: f.symbol},
		{Channel: "mark-price", InstID: f.symbol},
		{Channel: "funding-rate", InstID: f.symbol},
		{Channel: "open-interest", InstID: f.symbol},
	}, f.log, f.onMessage, func() {
		// Any reconnect invalidates both the book (a stale seq space) and the
		// trade identity (tradeId continues across sockets, but a socket loss
		// may have swallowed prints — the minute reset is the honest answer).
		resetBook()
		resetTrades()
	})

	liquidations := newConn(publicURL(), []arg{
		{Channel: "liquidation-orders", InstType: "SWAP", InstFamily: f.instFamily},
	}, f.log, f.onMessage, nil)

	f.dropConn = public.Drop

	var streams sync.WaitGroup
	streams.Add(1)
	go func() { defer streams.Done(); liquidations.Run(ctx) }()
	public.Run(ctx)
	streams.Wait()
}

func resetNow() int64 { return nowMs() }

func (f *Feed) onMessage(channel string, action string, raw json.RawMessage) {
	switch channel {
	case "books":
		f.onBooks(action, raw)
	case "trades":
		f.onTrades(raw)
	case "mark-price":
		f.onMarkPrice(raw)
	case "funding-rate":
		f.onFundingRate(raw)
	case "open-interest":
		f.onOpenInterest(raw)
	case "liquidation-orders":
		f.onLiquidationOrders(raw)
	}
}

// ─── orderbook ───────────────────────────────────────────────────────────────

// bookFrame is one books-channel data entry:
//
//	{"asks":[["px","sz","liqOrders","numOrders"],...],"bids":[...],
//	 "ts":..,"seqId":..,"prevSeqId":..,"checksum":..}
//
// action is "snapshot" on the first frame of every subscribe and "update"
// after. Sizes are contracts; sz[1] × ctVal lands on the wire in coin.
// ts/seqId/prevSeqID are json.Number because OKX mixes string and number
// scalars across (and inside) channels — a typed int64 here fails exactly
// the way the Binance "e"/"E" note in CONTRIBUTING.md describes.
type bookFrame struct {
	Asks      [][]string  `json:"asks"`
	Bids      [][]string  `json:"bids"`
	Ts        json.Number `json:"ts"`
	SeqID     json.Number `json:"seqId"`
	PrevSeqID json.Number `json:"prevSeqId"`
}

func (f *Feed) onBooks(action string, raw json.RawMessage) {
	var d bookFrame
	if err := json.Unmarshal(raw, &d); err != nil {
		warnUnparsable(f.log, "books", err)
		return
	}
	apply := func(side map[float64]float64, rows [][]string) {
		for _, r := range rows {
			if len(r) < 2 {
				continue
			}
			p := strF(r[0])
			s := f.inst.contractSize(strF(r[1]))
			if p <= 0 {
				continue
			}
			if s == 0 {
				delete(side, p)
			} else {
				side[p] = s
			}
		}
	}

	f.mu.Lock()
	seq := numI(d.SeqID)
	prev := numI(d.PrevSeqID)
	ts := numI(d.Ts)
	if seq <= 0 {
		f.mu.Unlock()
		return
	}
	if f.bookSynced && action != "snapshot" && f.lastSeq > 0 && prev != f.lastSeq {
		// Sequence gap: the book cannot be trusted and OKX offers no
		// re-anchor frame — dropping the socket makes the reconnect loop
		// subscribe afresh, which re-opens with a full snapshot.
		f.mu.Unlock()
		f.log.Warn("orderbook sequence gap; resubscribing", "expected", f.lastSeq, "got", prev)
		f.requestResubscribe()
		return
	}
	apply(f.bids, d.Bids)
	apply(f.asks, d.Asks)
	f.lastSeq = seq
	snapshot := action == "snapshot" || !f.bookSynced
	f.bookSynced = true
	wasNew := snapshot
	bids, asks := f.sortedLocked()
	last := f.lastTrade
	f.mu.Unlock()

	// TEMP breadcrumb (phase6 bring-up): the maintained map must never cross.
	if len(bids) > 0 && len(asks) > 0 && asks[0].Price <= bids[0].Price {
		f.log.Warn("gateway book crossed",
			"bid", bids[0].Price, "ask", asks[0].Price,
			"action", action, "seq", seq, "prev", prev,
			"frameBids", len(d.Bids), "frameAsks", len(d.Asks),
			"mapBids", len(bids), "mapAsks", len(asks),
			"topBidLevel", d.Bids[0], "topAskLevel", d.Asks[0])
	}

	if wasNew && action == "snapshot" {
		f.log.Info("orderbook synced", "levels", len(bids)+len(asks))
	}

	f.emit(pb.Stream_STREAM_ORDERBOOK, 0, ts, &pb.BookUpdate{
		TimestampMs: ts,
		Asks:        asks,
		Bids:        bids,
		Snapshot:    snapshot,
		LastPrice:   last,
		// FirstUpdateId carries seq (the diff's own id), mirroring Binance's
		// U-range start. The client's bridge rule then admits ONLY the exact
		// next event after a re-primed snapshot (seq == snapshotSeq+1) and
		// honestly rejects anything older: admitting stale in-flight diffs
		// (FirstUpdateId=prevSeqId did) lets a diff predating the snapshot
		// cross the freshly re-anchored book, which re-triggered the very
		// refresh that caused the re-prime — a self-sustaining recovering loop.
		LastUpdateId:     seq,
		FirstUpdateId:    seq,
		PreviousUpdateId: prev,
	})
}

// requestResubscribe finds the public conn and drops it. The conn pointer is
// wired at Run time; before that (no socket yet) a Drop is harmless.
func (f *Feed) requestResubscribe() {
	if f.dropConn != nil {
		f.dropConn()
	}
}

// ─── trades ──────────────────────────────────────────────────────────────────

type tradeMsg struct {
	InstID  string      `json:"instId"`
	TradeID json.Number `json:"tradeId"`
	Px      json.Number `json:"px"`
	Sz      json.Number `json:"sz"`
	Side    string      `json:"side"`
	Ts      json.Number `json:"ts"`
}

func (f *Feed) onTrades(raw json.RawMessage) {
	var t tradeMsg
	if err := json.Unmarshal(raw, &t); err != nil {
		warnUnparsable(f.log, "trades", err)
		return
	}
	price := numF(t.Px)
	qty := numF(t.Sz) // contracts → coin
	id := numI(t.TradeID)
	ts := numI(t.Ts)
	if price <= 0 || qty <= 0 || math.IsNaN(price) || math.IsInf(price, 0) || ts <= 0 {
		return
	}

	f.mu.Lock()
	if id > 0 && f.lastTradeID > 0 && id <= f.lastTradeID {
		f.mu.Unlock()
		return // duplicate print
	}
	gap := (id > 0 && f.lastTradeID > 0 && id != f.lastTradeID+1) || (f.lastTradeTime > 0 && ts < f.lastTradeTime)
	f.lastTradeID = id
	f.lastTradeTime = ts
	f.lastTrade = price
	f.tradeCount++
	f.mu.Unlock()
	if gap && f.tradeReset != nil {
		f.tradeReset(nowMs())
		f.log.Warn("trade sequence gap; discarding partial volume minute")
	}

	// OKX side is the TAKER side verbatim ("buy" = aggressive buy), which is
	// exactly the IsBuy contract — no Binance-style maker inversion here.
	f.emit(pb.Stream_STREAM_TRADES, 0, ts, &pb.Trade{
		Price:       price,
		Qty:         f.inst.contractSize(qty),
		IsBuy:       t.Side == "buy",
		TimestampMs: ts,
	})
}

// ─── stats: mark / funding / open interest ───────────────────────────────────

type markMsg struct {
	MarkPx json.Number `json:"markPx"`
	Ts     json.Number `json:"ts"`
}

func (f *Feed) onMarkPrice(raw json.RawMessage) {
	var m markMsg
	if err := json.Unmarshal(raw, &m); err != nil {
		warnUnparsable(f.log, "mark-price", err)
		return
	}
	f.mu.Lock()
	f.markPrice = numF(m.MarkPx)
	f.mu.Unlock()
}

type fundingMsg struct {
	FundingRate json.Number `json:"fundingRate"`
	FundingTime json.Number `json:"fundingTime"`
}

func (f *Feed) onFundingRate(raw json.RawMessage) {
	var m fundingMsg
	if err := json.Unmarshal(raw, &m); err != nil {
		warnUnparsable(f.log, "funding-rate", err)
		return
	}
	f.mu.Lock()
	f.funding = numF(m.FundingRate)
	f.nextFunding = numI(m.FundingTime)
	f.mu.Unlock()
}

type oiMsg struct {
	Oi    json.Number `json:"oi"`    // contracts
	OiCcy json.Number `json:"oiCcy"` // base coin
	Ts    json.Number `json:"ts"`
}

func (f *Feed) onOpenInterest(raw json.RawMessage) {
	var m oiMsg
	if err := json.Unmarshal(raw, &m); err != nil {
		warnUnparsable(f.log, "open-interest", err)
		return
	}
	oi := numF(m.OiCcy)
	if oi == 0 {
		oi = f.inst.contractSize(numF(m.Oi))
	}
	f.mu.Lock()
	f.openInt = oi
	f.mu.Unlock()
}

// MarkState is the latest funding/mark/OI snapshot, for the stats aggregator.
func (f *Feed) MarkState() (mark, funding, oi float64, nextFunding int64) {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.markPrice, f.funding, f.openInt, f.nextFunding
}

// ─── liquidations ────────────────────────────────────────────────────────────

// liqMsg is one liquidation-orders data entry:
//
//	{"details":[{"instId":"BTC-USDT-SWAP","side":"sell","sz":"12",
//	             "bkPx":"64123.4","ts":"1731..."}],"instFamily":"BTC-USDT"}
//
// side is the bankruptcy ORDER side: a liquidated LONG force-SELLS, so
// IsBuy = (side == "buy") mirrors the Binance force-order mapping exactly
// (a BUY force order closes a short). Inverting this flips the heatmap.
type liqMsg struct {
	Details []struct {
		InstID string      `json:"instId"`
		Side   string      `json:"side"`
		Sz     json.Number `json:"sz"`
		BkPx   json.Number `json:"bkPx"`
		Ts     json.Number `json:"ts"`
	} `json:"details"`
}

func (f *Feed) onLiquidationOrders(raw json.RawMessage) {
	var m liqMsg
	if err := json.Unmarshal(raw, &m); err != nil {
		warnUnparsable(f.log, "liquidation-orders", err)
		return
	}
	for _, d := range m.Details {
		if d.InstID != f.symbol {
			continue // the subscribe is family-scoped; keep only our instrument
		}
		price := numF(d.BkPx)
		qty := f.inst.contractSize(numF(d.Sz))
		ts := numI(d.Ts)
		if price <= 0 || qty <= 0 || ts <= 0 {
			continue
		}
		// OKX exposes the bankruptcy price only; there is no realized avg
		// fill on the public channel, so AvgPrice carries the same value and
		// the terminal renders one marker, not two.
		f.emit(pb.Stream_STREAM_LIQUIDATIONS, 0, ts, &pb.Liquidation{
			TimestampMs: ts,
			Price:       price,
			AvgPrice:    price,
			Qty:         qty,
			IsBuy:       d.Side == "buy",
		})
	}
}

// ─── snapshot for late joiners ───────────────────────────────────────────────

// LastTrade returns the most recent traded price, used to stamp BookUpdate.
func (f *Feed) LastTrade() float64 {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.lastTrade
}

// Snapshot returns the current book as a snapshot BookUpdate, or nil if the
// book has not synced yet. Used to prime a newly subscribed client.
func (f *Feed) Snapshot() *pb.BookUpdate {
	f.mu.RLock()
	defer f.mu.RUnlock()
	if !f.bookSynced {
		return nil
	}
	bids, asks := f.sortedLocked()
	return &pb.BookUpdate{
		TimestampMs:  nowMs(),
		Asks:         asks,
		Bids:         bids,
		Snapshot:     true,
		LastPrice:    f.lastTrade,
		LastUpdateId: f.lastSeq,
	}
}

// sortedLocked returns bids (price-desc) and asks (price-asc). Caller holds mu.
func (f *Feed) sortedLocked() (bids, asks []*pb.BookLevel) {
	bids = make([]*pb.BookLevel, 0, len(f.bids))
	for p, s := range f.bids {
		bids = append(bids, &pb.BookLevel{Price: p, Size: s})
	}
	asks = make([]*pb.BookLevel, 0, len(f.asks))
	for p, s := range f.asks {
		asks = append(asks, &pb.BookLevel{Price: p, Size: s})
	}
	sort.Slice(bids, func(i, j int) bool { return bids[i].Price > bids[j].Price })
	sort.Slice(asks, func(i, j int) bool { return asks[i].Price < asks[j].Price })
	return bids, asks
}

func nowMs() int64 { return timeNow().UnixMilli() }
