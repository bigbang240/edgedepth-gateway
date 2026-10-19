package binance

import (
        "context"
        "encoding/json"
        "log/slog"
        "math"
        "sort"
        "strconv"
        "sync"
        "time"

        "github.com/edgedepthhq/edgedepth-gateway/internal/exchange"
        "github.com/edgedepthhq/edgedepth-gateway/pkg/pb"
)

// Emit hands a decoded market message to the hub. timeframe is 0 for
// non-timeframed streams.
type Emit = exchange.Emit

// Feed owns every upstream Binance stream for one symbol and turns them into
// EdgeDepth protobuf messages.
//
// It maintains the orderbook rather than blindly forwarding diffs, for one
// reason: a client that connects ten minutes in needs a base snapshot before
// diffs mean anything. Forwarding only diffs leaves a late joiner with an
// orderbook that never fills.
type Feed struct {
        bookEmitMu sync.Mutex // serialize book state changes with snapshot/diff emission
        runCtx     context.Context
        depthEpoch uint64
        nextResync time.Time
        Symbol     string // lowercase, e.g. "btcusdt"

        emit Emit
        log  *slog.Logger

        mu            sync.RWMutex
        bids          map[float64]float64
        asks          map[float64]float64
        lastU         int64 // last applied update id
        synced        bool
        lastTrade     float64
        tradeCount    int64
        lastTradeID   int64
        lastTradeTime int64
        tradeReset    func(int64)

        // awaitFirst is true between a REST snapshot and the first stream diff
        // applied on top of it. The snapshot's lastUpdateId is not a stream "u"
        // value, so the pu == lastU continuity rule cannot hold across that
        // boundary; the first diff is matched with the straddle rule instead.
        awaitFirst bool

        // partial is true while the book is anchored by @depth20@100ms
        // top-of-book frames instead of a REST snapshot. Binance bans shared
        // egress IPs (HTTP 418) for hours, which makes the REST /depth snapshot
        // unreachable exactly when a fresh boot needs it; the partial frames
        // arrive on the SAME websocket as the diffs and keep the heatmap live
        // with the top 20 levels on each side. pollREST nudges an upgrade to
        // the full 1000-level REST book once the ban lifts.
        partial bool

        // resyncing collapses concurrent resync attempts. Without it a burst of
        // out-of-sequence diffs fires a REST snapshot each, which is both wasted
        // work and a fast route to a Binance rate limit.
        resyncing bool

        // pending buffers depth diffs that arrive before the REST snapshot lands.
        pending []depthDiff

        markPrice   float64
        funding     float64
        nextFunding int64
        openInt     float64
}

// Every Binance payload carries BOTH "e" (event type, a string) and "E"
// (event time, a number). Go's JSON decoder prefers an exact tag match but
// falls back to a CASE-INSENSITIVE one, so a struct that declares only "E"
// has "e" assigned to it and the whole message fails to unmarshal with
// "cannot unmarshal string into Go struct field ... of type int64".
//
// Declaring EventType with the exact lowercase tag gives "e" somewhere
// correct to land, and the exact match then wins for both keys. Do not
// delete these seemingly unused fields: without them nothing parses.

type depthDiff struct {
        EventType string     `json:"e"`
        EventTime int64      `json:"E"`
        FirstID   int64      `json:"U"`
        FinalID   int64      `json:"u"`
        PrevID    int64      `json:"pu"`
        Bids      [][]string `json:"b"`
        Asks      [][]string `json:"a"`
}

// aggTrade parses BOTH @aggTrade and @trade, which share p/q/T/m.
//
// TradeID absorbs "t" for the same reason EventType absorbs "e": the @trade
// payload carries both "T" (trade time, ms) and "t" (trade id), and without
// an exact home for "t" the case-insensitive fallback puts the trade ID into
// the timestamp. That produces a 10-digit value that is still positive and
// still plausible-looking, so it survives naive validation and lands every
// trade at the wrong point in time.
type aggTrade struct {
        SymbolType int    `json:"st"`
        EventType  string `json:"e"`
        EventTime  int64  `json:"E"`
        Price      string `json:"p"`
        Qty        string `json:"q"`
        TradeTime  int64  `json:"T"`
        TradeID    int64  `json:"t"`
        AggID      int64  `json:"a"`
        Maker      *bool  `json:"m"`
}

type markPrice struct {
        EventType   string `json:"e"`
        EventTime   int64  `json:"E"`
        Mark        string `json:"p"`
        FundingRate string `json:"r"`
        NextFunding int64  `json:"T"`
}

type forceOrder struct {
        EventType string `json:"e"`
        EventTime int64  `json:"E"`
        Order     struct {
                Side     string `json:"S"`
                Price    string `json:"p"`
                AvgPrice string `json:"ap"`
                Qty      string `json:"q"`
        } `json:"o"`
}

// NewFeed creates a feed. Call Run to start it.
func NewFeed(symbol string, log *slog.Logger, emit Emit) *Feed {
        return &Feed{
                Symbol: lower(symbol),
                emit:   emit,
                log:    log.With("symbol", lower(symbol)),
                bids:   make(map[float64]float64),
                asks:   make(map[float64]float64),
        }
}

// TradeStream selects which Binance trade stream to consume.
//
// "aggTrade" is the default and mirrors what the hosted backend ingests:
// trades aggregated per taker order, so fewer messages for the same volume.
// "trade" is the raw per-fill stream. Both carry the price, quantity, taker
// side and timestamp this gateway needs, so either works.
//
// The switch exists because @aggTrade is not reachable from every network,
// and a tape that silently never ticks is a miserable thing to debug.
var TradeStream = "aggTrade"

func (f *Feed) SetTradeReset(reset func(int64)) { f.tradeReset = reset }

// Run connects the upstream streams and blocks until ctx is cancelled.
func (f *Feed) Run(ctx context.Context) {
        f.runCtx = ctx
        s := f.Symbol
        market := NewStream([]string{s + "@" + TradeStream, s + "@markPrice@1s", s + "@forceOrder"}, f.log, f.onMessage, func() {
                f.mu.Lock()
                f.lastTradeID = 0
                f.lastTradeTime = 0
                f.mu.Unlock()
                if f.tradeReset != nil {
                        f.tradeReset(time.Now().UnixMilli())
                }
        })
        depth := NewStream([]string{s + "@depth@100ms"}, f.log, f.onMessage, func() {
                f.bookEmitMu.Lock()
                f.mu.Lock()
                f.depthEpoch++
                f.synced = false
                f.pending = nil
                f.resyncing = false
                f.nextResync = time.Time{}
                f.mu.Unlock()
                f.bookEmitMu.Unlock()
                f.triggerResync("depth connection reset", 0, 0)
        })
        partial := NewStream([]string{s + "@depth20@100ms"}, f.log, f.onMessage, func() {
                // The frame stream dropping means our anchor is gone. Fall back
                // to the @depth buffering path until either the frames resume
                // or a REST snapshot lands.
                f.bookEmitMu.Lock()
                f.mu.Lock()
                if f.partial {
                        f.partial = false
                        f.synced = false
                        f.pending = nil
                        f.resyncing = false
                        f.nextResync = time.Time{}
                }
                f.mu.Unlock()
                f.bookEmitMu.Unlock()
                f.triggerResync("partial depth connection reset", 0, 0)
        })
        var streams sync.WaitGroup
        streams.Add(2)
        go func() { defer streams.Done(); depth.Run(ctx) }()
        go func() { defer streams.Done(); partial.Run(ctx) }()
        go f.pollREST(ctx)
        go f.warnIfNoTrades(ctx)
        market.Run(ctx)
        streams.Wait()
}

func (f *Feed) onMessage(env Envelope) {
        // The stream name is "<symbol>@<type>" or "<symbol>@depth@100ms".
        // The partial-book streams (@depth5/10/20@100ms) MUST match before the
        // generic @depth case: their frames are complete top-of-book anchors,
        // not sequence diffs, and onDepth would buffer or drop them forever -
        // which is exactly what silently disabled the ban-window anchor.
        switch {
        case hasSuffix(env.Stream, "@aggTrade"), hasSuffix(env.Stream, "@trade"):
                f.onAggTrade(env.Data)
        case contains(env.Stream, "@depth20@100ms"), contains(env.Stream, "@depth10@100ms"),
                contains(env.Stream, "@depth5@100ms"):
                f.onPartialDepth(env.Data)
        case contains(env.Stream, "@depth"):
                f.onDepth(env.Data)
        case contains(env.Stream, "@markPrice"):
                f.onMarkPrice(env.Data)
        case hasSuffix(env.Stream, "@forceOrder"):
                f.onForceOrder(env.Data)
        }
}

func (f *Feed) onAggTrade(raw json.RawMessage) {
        var t aggTrade
        if err := json.Unmarshal(raw, &t); err != nil {
                // Never swallow this. A silent return here is how a wire-shape
                // change turns into an empty panel with no explanation.
                f.log.Warn("unparsable onAggTrade payload", "err", err)
                return
        }
        price := parseF(t.Price)
        qty := parseF(t.Qty)

        // The raw @trade stream carries occasional non-market events ("X":"NA")
        // with price and qty both "0". Folding one into a candle zeroes its
        // low/close until the next real trade repairs them, and the flush loop
        // ships that window to every chart, where autoscale stretches the y-axis
        // to zero. @aggTrade never delivers these, so the hosted backend has
        // never needed this guard.
        if price <= 0 || qty <= 0 || math.IsNaN(price) || math.IsNaN(qty) ||
                math.IsInf(price, 0) || math.IsInf(qty, 0) || t.TradeTime <= 0 || t.Maker == nil || t.SymbolType == 2 {
                return
        }

        id := t.AggID
        if t.EventType == "trade" {
                id = t.TradeID
        }
        f.mu.Lock()
        if id > 0 && f.lastTradeID > 0 && id <= f.lastTradeID {
                f.mu.Unlock()
                return
        }
        gap := (id > 0 && f.lastTradeID > 0 && id != f.lastTradeID+1) || t.TradeTime < f.lastTradeTime
        f.lastTradeID = id
        f.lastTradeTime = t.TradeTime
        f.mu.Unlock()
        if gap && f.tradeReset != nil {
                f.tradeReset(time.Now().UnixMilli())
                f.log.Warn("trade sequence gap; discarding partial volume minute")
        }
        f.mu.Lock()
        f.lastTrade = price
        f.tradeCount++
        f.mu.Unlock()

        // Binance "m" is "was the BUYER the maker". If the buyer was the maker,
        // the aggressor was the seller, so this is a sell. is_buy is the
        // aggressor side, hence the negation.
        f.emit(pb.Stream_STREAM_TRADES, 0, t.TradeTime, &pb.Trade{
                Price:       price,
                Qty:         qty,
                IsBuy:       !*t.Maker,
                TimestampMs: t.TradeTime,
        })
}

func (f *Feed) onForceOrder(raw json.RawMessage) {
        var o forceOrder
        if err := json.Unmarshal(raw, &o); err != nil {
                // Never swallow this. A silent return here is how a wire-shape
                // change turns into an empty panel with no explanation.
                f.log.Warn("unparsable onForceOrder payload", "err", err)
                return
        }
        // IsBuy mirrors the backend exactly: side == "BUY". A BUY force order
        // closes a SHORT. Inverting this flips the liquidation heatmap, so it is
        // a straight copy of the exchange field, not an interpretation.
        f.emit(pb.Stream_STREAM_LIQUIDATIONS, 0, o.EventTime, &pb.Liquidation{
                TimestampMs: o.EventTime,
                Price:       parseF(o.Order.Price),
                AvgPrice:    parseF(o.Order.AvgPrice),
                Qty:         parseF(o.Order.Qty),
                IsBuy:       o.Order.Side == "BUY",
        })
}

func (f *Feed) onMarkPrice(raw json.RawMessage) {
        var m markPrice
        if err := json.Unmarshal(raw, &m); err != nil {
                // Never swallow this. A silent return here is how a wire-shape
                // change turns into an empty panel with no explanation.
                f.log.Warn("unparsable onMarkPrice payload", "err", err)
                return
        }
        f.mu.Lock()
        f.markPrice = parseF(m.Mark)
        f.funding = parseF(m.FundingRate)
        f.nextFunding = m.NextFunding
        f.mu.Unlock()
}

// MarkState is the latest funding/mark/OI snapshot, for the stats aggregator.
func (f *Feed) MarkState() (mark, funding, oi float64, nextFunding int64) {
        f.mu.RLock()
        defer f.mu.RUnlock()
        return f.markPrice, f.funding, f.openInt, f.nextFunding
}

// LastTrade returns the most recent traded price, used to stamp BookUpdate.
func (f *Feed) LastTrade() float64 {
        f.mu.RLock()
        defer f.mu.RUnlock()
        return f.lastTrade
}

// pollREST refreshes the values that either have no WebSocket stream (open
// interest) or whose stream may be unreachable (mark price and funding, via
// premiumIndex). Both endpoints are weight-1 and both values move slowly, so
// 30s costs nothing and keeps the stats panel populated even when the
// markPrice stream delivers nothing.
func (f *Feed) pollREST(ctx context.Context) {
        tick := time.NewTicker(30 * time.Second)
        defer tick.Stop()
        for {
                // Fetch both before publishing either. Applying them as they arrived
                // let a stats flush land in the gap and ship a frame carrying open
                // interest with a zero mark price, which the panel renders as a
                // confident "0.00" rather than leaving the field blank.
                oi, oiErr := OpenInterest(ctx, f.Symbol)
                prem, premErr := Premium(ctx, f.Symbol)

                f.mu.Lock()
                if oiErr == nil {
                        f.openInt = oi
                }
                if premErr == nil {
                        // The WebSocket stream, when it works, is fresher than a 30s
                        // poll, so only fill in what is still missing.
                        if f.markPrice == 0 {
                                f.markPrice = prem.MarkPrice
                        }
                        if f.funding == 0 {
                                f.funding = prem.LastFundingRate
                        }
                        if f.nextFunding == 0 {
                                f.nextFunding = prem.NextFundingTime
                        }
                }
                f.mu.Unlock()

                if ctx.Err() == nil {
                        if oiErr != nil {
                                f.log.Debug("open interest poll failed", "err", oiErr)
                        }
                        if premErr != nil {
                                f.log.Debug("premium index poll failed", "err", premErr)
                        }
                }

                // Nudge the REST upgrade while the partial frames are holding the
                // book. triggerResync self-collapses, and nextResync throttling
                // keeps a standing ban from turning this into a request storm.
                f.mu.RLock()
                partial := f.partial
                f.mu.RUnlock()
                if partial {
                        f.triggerResync("upgrade partial book to full snapshot", 0, 0)
                }

                select {
                case <-ctx.Done():
                        return
                case <-tick.C:
                }
        }
}

// warnIfNoTrades flags the case where the book is updating but the tape is
// not. That combination means the trade stream specifically is unreachable,
// which is otherwise invisible: the chart just never ticks.
func (f *Feed) warnIfNoTrades(ctx context.Context) {
        select {
        case <-ctx.Done():
                return
        case <-time.After(30 * time.Second):
        }
        f.mu.RLock()
        trades, book := f.tradeCount, f.synced
        f.mu.RUnlock()
        if trades == 0 && book {
                f.log.Warn("orderbook is live but no trades received in 30s: "+
                        "the trade stream may be unreachable from this network, "+
                        "try starting with -trade-stream=trade",
                        "current", TradeStream)
        }
}

// ── orderbook ───────────────────────────────────────────────────────────────

// onPartialDepth consumes <symbol>@depth20@100ms frames: complete top-of-book
// snapshots delivered on the websocket, immune to REST-side IP bans. Each
// frame REPLACES the book — no diff application, no sequence continuity to
// maintain — which is why this anchor cannot be corrupted by a missed frame.
//
// While a full REST-anchored book is live (synced && !partial) the frames are
// ignored: the @depth20 map only holds 20 levels per side and applying it
// would shrink a full book. The moment the full book is lost (resync loop,
// connection reset, or boot with a banned IP) the frames take over within
// 100ms, so the heatmap never sits empty waiting for a REST window.
func (f *Feed) onPartialDepth(raw json.RawMessage) {
        f.bookEmitMu.Lock()
        defer f.bookEmitMu.Unlock()
        var d partialDepth
        if err := json.Unmarshal(raw, &d); err != nil {
                f.log.Warn("unparsable partial depth payload", "err", err)
                return
        }
        f.mu.Lock()
        if f.synced && !f.partial {
                // Full REST book is live; frames add nothing.
                f.mu.Unlock()
                return
        }
        anchored := f.synced && f.partial
        f.bids = make(map[float64]float64, len(d.Bids))
        f.asks = make(map[float64]float64, len(d.Asks))
        for _, b := range d.Bids {
                f.bids[parseF(b[0])] = parseF(b[1])
        }
        for _, a := range d.Asks {
                f.asks[parseF(a[0])] = parseF(a[1])
        }
        f.lastU = d.FinalID
        f.pending = nil
        f.awaitFirst = false
        f.synced = true
        f.partial = true
        f.resyncing = false
        f.nextResync = time.Time{}
        bids, asks := f.sortedLocked()
        last := f.lastTrade
        f.mu.Unlock()
        if !anchored {
                f.log.Info("partial depth anchor (REST snapshot unavailable): serving top-of-book",
                        "levels", len(bids)+len(asks))
        }
        f.emit(pb.Stream_STREAM_ORDERBOOK, 0, d.EventTime, &pb.BookUpdate{
                TimestampMs:  d.EventTime,
                Asks:         asks,
                Bids:         bids,
                Snapshot:     true,
                LastPrice:    last,
                LastUpdateId: d.FinalID,
        })
}

// partialDepth parses <symbol>@depth20@100ms payloads. Fields mirror depthDiff
// for the same tag reasons documented there ("e" must land somewhere exact).
type partialDepth struct {
        EventType string     `json:"e"`
        EventTime int64      `json:"E"`
        Symbol    string     `json:"s"`
        FirstID   int64      `json:"U"`
        FinalID   int64      `json:"u"`
        PrevID    int64      `json:"pu"`
        Bids      [][]string `json:"b"`
        Asks      [][]string `json:"a"`
}

func (f *Feed) onDepth(raw json.RawMessage) {
        f.bookEmitMu.Lock()
        defer f.bookEmitMu.Unlock()
        var d depthDiff
        if err := json.Unmarshal(raw, &d); err != nil {
                // Never swallow this. A silent return here is how a wire-shape
                // change turns into an empty panel with no explanation.
                f.log.Warn("unparsable depth payload", "err", err)
                return
        }
        f.mu.Lock()
        if f.partial {
                // The book is anchored by @depth20 frames. @depth diffs carry
                // sequence ids from the same space, but applying them would
                // grow levels the frames never publish and re-delete levels a
                // newer frame already replaced. Ignore them while anchored;
                // resync() upgrades to the full book when REST allows.
                f.mu.Unlock()
                return
        }
        if !f.synced {
                // Buffer until the REST snapshot arrives. Cap it so a snapshot that
                // never lands cannot grow this without bound.
                if len(f.pending) < 5000 {
                        f.pending = append(f.pending, d)
                }
                retry := !f.resyncing && !time.Now().Before(f.nextResync)
                f.mu.Unlock()
                if retry {
                        f.triggerResync("unsynced depth", 0, 0)
                }
                return
        }

        if !f.awaitFirst && d.FinalID <= f.lastU {
                f.mu.Unlock()
                return
        }
        // Binance's documented futures procedure, in order:
        //   - drop any event whose u is below the snapshot id (already included)
        //   - the FIRST event applied must satisfy U <= lastUpdateId <= u
        //   - from then on each event's pu must equal the previous event's u
        // Applying the pu rule to the first event compares a stream id against a
        // REST snapshot id, which almost never matches, and the book resyncs in a
        // loop forever.
        if f.awaitFirst {
                if d.FinalID < f.lastU {
                        f.mu.Unlock() // entirely before the snapshot
                        return
                }
                if d.FirstID > f.lastU {
                        f.mu.Unlock()
                        f.triggerResync("snapshot older than first diff", f.lastU, d.FirstID)
                        return
                }
                f.awaitFirst = false
        } else if d.PrevID != f.lastU {
                f.mu.Unlock()
                f.triggerResync("sequence gap", f.lastU, d.PrevID)
                return
        }

        f.applyLocked(d)
        f.lastU = d.FinalID
        last := f.lastTrade
        f.mu.Unlock()

        f.emit(pb.Stream_STREAM_ORDERBOOK, 0, d.EventTime, &pb.BookUpdate{
                TimestampMs:      d.EventTime,
                Asks:             levels(d.Asks),
                Bids:             levels(d.Bids),
                Snapshot:         false,
                LastPrice:        last,
                FirstUpdateId:    d.FirstID,
                LastUpdateId:     d.FinalID,
                PreviousUpdateId: d.PrevID,
        })
}

// triggerResync marks the book unusable and schedules one resync, collapsing
// concurrent attempts so a burst of bad sequence numbers cannot storm the
// REST endpoint.
func (f *Feed) triggerResync(reason string, want, got int64) {
        f.mu.Lock()
        if f.resyncing || time.Now().Before(f.nextResync) {
                f.mu.Unlock()
                return
        }
        ctx := f.runCtx
        if ctx == nil || ctx.Err() != nil {
                f.mu.Unlock()
                return
        }
        f.resyncing = true
        f.synced = false
        f.nextResync = time.Now().Add(time.Second)
        epoch := f.depthEpoch
        f.mu.Unlock()
        f.log.Warn("orderbook resync", "reason", reason, "expected", want, "got", got)
        go f.resync(ctx, epoch)
}

// resync pulls a REST snapshot and replays buffered diffs on top, following
// Binance's documented futures procedure.
func (f *Feed) resync(ctx context.Context, epoch uint64) {
        defer func() {
                f.mu.Lock()
                if f.depthEpoch == epoch {
                        f.resyncing = false
                }
                f.mu.Unlock()
        }()
        if ctx.Err() != nil {
                return
        }
        ctx, cancel := context.WithTimeout(ctx, 30*time.Second)
        defer cancel()

        snap, err := Depth(ctx, f.Symbol, 1000)
        if err != nil {
                f.log.Warn("depth snapshot failed, will retry on next diff", "err", err)
                return
        }

        f.bookEmitMu.Lock()
        defer f.bookEmitMu.Unlock()
        f.mu.Lock()
        if ctx.Err() != nil || f.depthEpoch != epoch {
                f.mu.Unlock()
                return
        }
        f.bids = make(map[float64]float64, len(snap.Bids))
        f.asks = make(map[float64]float64, len(snap.Asks))
        for _, b := range snap.Bids {
                f.bids[parseF(b[0])] = parseF(b[1])
        }
        for _, a := range snap.Asks {
                f.asks[parseF(a[0])] = parseF(a[1])
        }
        f.lastU = snap.LastUpdateID

        // Drop stale buffered diffs, then apply the rest. The first diff we keep
        // must straddle the snapshot id (U <= lastUpdateId <= u).
        applied := 0
        for _, d := range f.pending {
                if d.FinalID < snap.LastUpdateID {
                        continue // entirely before the snapshot
                }
                if applied == 0 && d.FirstID > snap.LastUpdateID {
                        // A gap between the snapshot and our earliest buffered diff. The
                        // snapshot is already too old; give up and let the next diff
                        // trigger another resync.
                        f.log.Warn("snapshot too old for buffered diffs, retrying",
                                "snapshot_id", snap.LastUpdateID, "first_buffered_U", d.FirstID)
                        f.pending = nil
                        f.mu.Unlock()
                        return
                }
                if applied > 0 && d.FinalID <= f.lastU {
                        continue
                }
                if applied > 0 && d.PrevID != f.lastU {
                        f.log.Warn("buffered depth sequence gap; retrying", "expected", f.lastU, "got", d.PrevID)
                        f.pending = nil
                        f.mu.Unlock()
                        return
                }
                f.applyLocked(d)
                f.lastU = d.FinalID
                applied++
        }
        f.pending = nil
        f.synced = true
        f.partial = false
        // If no buffered diff was applied on top of the snapshot, the next live
        // diff is the first one and must be matched with the straddle rule rather
        // than the pu continuity rule.
        f.awaitFirst = applied == 0
        bids, asks := f.sortedLocked()
        last := f.lastTrade
        lastU := f.lastU
        f.mu.Unlock()

        f.log.Info("orderbook synced", "levels", len(bids)+len(asks), "replayed_diffs", applied)

        f.emit(pb.Stream_STREAM_ORDERBOOK, 0, time.Now().UnixMilli(), &pb.BookUpdate{
                TimestampMs:  time.Now().UnixMilli(),
                Asks:         asks,
                Bids:         bids,
                Snapshot:     true,
                LastPrice:    last,
                LastUpdateId: lastU,
        })
}

func (f *Feed) applyLocked(d depthDiff) {
        for _, b := range d.Bids {
                p, s := parseF(b[0]), parseF(b[1])
                if s == 0 {
                        delete(f.bids, p)
                } else {
                        f.bids[p] = s
                }
        }
        for _, a := range d.Asks {
                p, s := parseF(a[0]), parseF(a[1])
                if s == 0 {
                        delete(f.asks, p)
                } else {
                        f.asks[p] = s
                }
        }
}

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

// Snapshot returns the current book as a snapshot BookUpdate, or nil if the
// book has not synced yet. Used to prime a newly subscribed client.
func (f *Feed) Snapshot() *pb.BookUpdate {
        f.mu.RLock()
        defer f.mu.RUnlock()
        if !f.synced {
                return nil
        }
        bids, asks := f.sortedLocked()
        return &pb.BookUpdate{
                TimestampMs:  time.Now().UnixMilli(),
                Asks:         asks,
                Bids:         bids,
                Snapshot:     true,
                LastPrice:    f.lastTrade,
                LastUpdateId: f.lastU,
        }
}

// ── helpers ─────────────────────────────────────────────────────────────────

func levels(src [][]string) []*pb.BookLevel {
        out := make([]*pb.BookLevel, 0, len(src))
        for _, l := range src {
                if len(l) < 2 {
                        continue
                }
                out = append(out, &pb.BookLevel{Price: parseF(l[0]), Size: parseF(l[1])})
        }
        return out
}

func parseF(s string) float64 {
        f, _ := strconv.ParseFloat(s, 64)
        return f
}

func hasSuffix(s, suf string) bool {
        return len(s) >= len(suf) && s[len(s)-len(suf):] == suf
}

func contains(s, sub string) bool {
        for i := 0; i+len(sub) <= len(s); i++ {
                if s[i:i+len(sub)] == sub {
                        return true
                }
        }
        return false
}
