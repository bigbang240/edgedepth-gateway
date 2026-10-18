// ─────────────────────────────────────────────────────────────────────────────
// EdgeDepth RT egress — a JSON bridge in front of the gateway's protobuf wire.
//
// The gateway (:9001) speaks the EdgeDepth terminal protocol: one protobuf
// WSPayload per binary WebSocket frame, JSON text frames for the control
// plane. That is compact but awkward for a second consumer (your own
// terminal, a bot, a dashboards process). This service:
//
//   - connects to the gateway as a client,
//   - ref-counts subscriptions and re-subscribes across reconnects,
//   - decodes WSPayload frames via protobufjs (plain protobuf, no zstd on
//     this gateway's outbound),
//   - re-publishes each frame as one JSON text message on its own WebSocket,
//   - exposes GET / for health + the active subscription table.
//
// Downstream protocol (text frames):
//   → {"action":"sub",    "exchange":"binancef","symbol":"BTCUSDT","stream":1,"tf":0}
//   → {"action":"unsub",  ...same fields...}
//   ← {"stream":3,"exchange":"binancef","symbol":"BTCUSDT","tf":0,
//      "t":1690000000000,"data":{ ...decoded inner message, camelCase... }}
//
// Streams served by this gateway (proto/edgedepth.proto):
//   1 TRADES · 2 CANDLES(tf) · 3 ORDERBOOK · 5 LIQUIDATIONS · 29 TICKER24H
//
// Run: bun run index.ts   (env: EGRESS_PORT, EGRESS_GATEWAY_WS)
// ─────────────────────────────────────────────────────────────────────────────
import { load, type Type, type Root } from "protobufjs";

const GATEWAY_WS = process.env.EGRESS_GATEWAY_WS ?? "ws://127.0.0.1:9001/ws";
const PORT = Number(process.env.EGRESS_PORT ?? 9010);

const DECODERS: Record<number, { type: string; label: string }> = {
    1: { type: "Trade", label: "trades" },
    2: { type: "Candles", label: "candles" },
    3: { type: "BookUpdate", label: "orderbook" },
    5: { type: "Liquidation", label: "liquidations" },
    29: { type: "Ticker24hUpdate", label: "ticker24h" },
};
const STREAM_LABEL: Record<number, string> = Object.fromEntries(
    Object.entries(DECODERS).map(([k, v]) => [Number(k), v.label]),
);

type SubKey = string; // "exchange|symbol|stream|tf"
const key = (exchange: string, symbol: string, stream: number, tf: number): SubKey =>
    `${exchange}|${symbol}|${stream}|${tf}`;

// subscription key → set of downstream socket ids; id → live socket
const subs = new Map<SubKey, Set<string>>();
const sockets = new Map<string, ServerWebSocket<{ id: string }>>();
let upstream: WebSocket | null = null;
let upstreamReady = false;
let reconnectDelay = 500;

// ── protobufjs setup ─────────────────────────────────────────────────────────
let root: Root | null = null;
const types = new Map<string, Type>();
async function initProto(): Promise<void> {
    root = await load(new URL("../proto/edgedepth.proto", import.meta.url).pathname);
    for (const spec of Object.values(DECODERS)) {
        const t = root.lookupType(spec.type);
        types.set(spec.type, t);
    }
    console.log(`[egress] proto loaded: ${Object.keys(DECODERS).length} decoders`);
}

function decodeInner(stream: number, data: Uint8Array): unknown {
    const spec = DECODERS[stream];
    if (!spec) return { base64: Buffer.from(data).toString("base64") };
    const t = types.get(spec.type);
    if (!t) return null;
    try {
        const msg = t.decode(data);
        return t.toObject(msg, { longs: Number, enums: Number, defaults: false });
    } catch (err) {
        console.warn("[egress] decode failed", stream, err);
        return null;
    }
}

// ── upstream (gateway) connection ────────────────────────────────────────────
function upstreamSend(payload: object): boolean {
    if (!upstream || upstream.readyState !== WebSocket.OPEN) return false;
    upstream.send(JSON.stringify(payload));
    return true;
}

function subscribeUpstream(k: SubKey): void {
    const [exchange, symbol, stream, tf] = k.split("|");
    upstreamSend({
        method: "subscribe",
        data: { pair: { exchange, symbol }, stream: Number(stream), timeframe: Number(tf) },
    });
}

function unsubscribeUpstream(k: SubKey): void {
    const [exchange, symbol, stream, tf] = k.split("|");
    upstreamSend({
        method: "unsubscribe",
        data: { pair: { exchange, symbol }, stream: Number(stream), timeframe: Number(tf) },
    });
}

function connectUpstream(): void {
    upstream = new WebSocket(GATEWAY_WS);
    upstream.binaryType = "arraybuffer";
    upstream.onopen = () => {
        console.log(`[egress] upstream connected: ${GATEWAY_WS}`);
        upstreamReady = true;
        reconnectDelay = 500;
        for (const k of subs.keys()) subscribeUpstream(k);
    };
    upstream.onmessage = (ev: MessageEvent) => {
        if (typeof ev.data === "string") return; // control frames: ignore
        const buf = new Uint8Array(ev.data as ArrayBuffer);
        try {
            const WSPayload = root!.lookupType("WSPayload");
            const payload = WSPayload.toObject(WSPayload.decode(buf), {
                longs: Number, enums: Number, defaults: false,
            }) as {
                pair?: { exchange?: string; symbol?: string };
                stream?: number;
                timeframe?: number;
                data?: Uint8Array;
                eventTimeMs?: number;
            };
            if (!payload.pair || payload.stream == null) return;
            const k = key(payload.pair.exchange ?? "", payload.pair.symbol ?? "",
                payload.stream, payload.timeframe ?? 0);
            const targets = subs.get(k);
            if (!targets || targets.size === 0) return;
            const message = {
                stream: payload.stream,
                label: STREAM_LABEL[payload.stream] ?? "unknown",
                exchange: payload.pair.exchange,
                symbol: payload.pair.symbol,
                tf: payload.timeframe ?? 0,
                t: payload.eventTimeMs ?? 0,
                data: payload.data ? decodeInner(payload.stream, payload.data) : null,
            };
            const text = JSON.stringify(message);
            for (const id of targets) {
                try { sockets.get(id)?.send(text); } catch { /* dropped */ }
            }
        } catch (err) {
            console.warn("[egress] frame handling failed", err);
        }
    };
    upstream.onclose = () => {
        upstreamReady = false;
        console.warn(`[egress] upstream closed; retry in ${reconnectDelay}ms`);
        setTimeout(connectUpstream, reconnectDelay);
        reconnectDelay = Math.min(reconnectDelay * 2, 15000);
    };
    upstream.onerror = () => { try { upstream?.close(); } catch { /* noop */ } };
}

// ── downstream (consumers) server ────────────────────────────────────────────
const wss = Bun.serve({
    port: PORT,
    async fetch(req, server) {
        const url = new URL(req.url);
        if (req.method === "GET" && url.pathname === "/") {
            return Response.json({
                ok: true,
                service: "edgedepth-rt-egress",
                upstream: upstreamReady ? "connected" : "down",
                gateway: GATEWAY_WS,
                clients: server.subscriberCount ? undefined : undefined,
                subs: [...subs.keys()],
            });
        }
        if (url.pathname === "/ws" || url.pathname === "/") {
            if (server.upgrade(req, { data: { id: crypto.randomUUID() } })) return;
            return new Response("upgrade failed", { status: 400 });
        }
        return new Response("not found", { status: 404 });
    },
    websocket: {
        open(ws) {
            const id = (ws.data as { id: string }).id;
            sockets.set(id, ws);
            console.log("[egress] client connected");
            ws.send(JSON.stringify({ hello: "edgedepth-rt-egress", streams: STREAM_LABEL }));
        },
        message(ws, raw) {
            let req: { action?: string; exchange?: string; symbol?: string; stream?: number; tf?: number };
            try { req = JSON.parse(String(raw)); } catch { return; }
            if (!req.exchange || !req.symbol || typeof req.stream !== "number") return;
            const k = key(req.exchange, req.symbol, req.stream, req.tf ?? 0);
            const id = (ws.data as { id: string }).id;
            if (req.action === "sub") {
                const isNew = !subs.has(k) || subs.get(k)!.size === 0;
                if (!subs.has(k)) subs.set(k, new Set());
                subs.get(k)!.add(id);
                if (isNew && upstreamReady) subscribeUpstream(k);
                ws.send(JSON.stringify({ ack: "sub", key: k }));
            } else if (req.action === "unsub") {
                const set = subs.get(k);
                if (set) {
                    set.delete(id);
                    if (set.size === 0) {
                        subs.delete(k);
                        if (upstreamReady) unsubscribeUpstream(k);
                    }
                }
                ws.send(JSON.stringify({ ack: "unsub", key: k }));
            }
        },
        close(ws) {
            const id = (ws.data as { id: string }).id;
            sockets.delete(id);
            for (const [k, set] of subs) {
                if (!set.delete(id)) continue;
                if (set.size === 0) {
                    subs.delete(k);
                    if (upstreamReady) unsubscribeUpstream(k);
                }
            }
            console.log("[egress] client disconnected");
        },
    },
});

// ── main ─────────────────────────────────────────────────────────────────────
await initProto();
connectUpstream();
console.log(`[egress] listening on ws://0.0.0.0:${PORT}/ws  (upstream ${GATEWAY_WS})`);
