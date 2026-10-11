# Gateway data-workflow changes (historical)

The 8 September 2026 candidate is no longer pending. It shipped in commit
`b822284` and is in the GHCR images from `sha-b822284` onward, including
`latest`. The older `0.1.0` image predates it.

- Route Binance market and depth streams through their current separate endpoints.
- Retain bounded closed-minute per-price observations and answer existing footprint/profile requests.
- Reset partial trade history across detected gaps; repair depth resync retry and stale-snapshot handling.
- Bound queued bytes and active symbols, and require continuity reporting for adapter history.

Current behaviour and limits are in the [README](../README.md#observed-volume-history).
