# 📔 CHANGELOG — EdgeDepth Gateway (fork شخصی bigbang240)

قالب: [Keep a Changelog](https://keepachangelog.com) · نسخه‌بندی: [SemVer](https://semver.org)
گیت‌وی Go چند-صرافی (بایننس USDⓈ-M + OKX SWAP) پشت ترمینال WASM.

## [Unreleased] — بستن باگ تیکر OKX (تکمیل فاز ۶)
### Fixed
- تیکر سراسری OKX روی یک فریم کامل می‌شکست: قراردادهای تازه‌لیست‌شده/pre-market (مثل MP-USDT-SWAP) در `last`/`open24h`/`vol` رشتهٔ خالی `""` می‌فرستند و unmarshal سخت‌گیرِ `json.Number` کل فریم tickers را رد می‌کرد → یونیورس watchlist OKX صفر می‌شد؛ نوع tolerant `flexNum` (پذیرش `""`/`null`/عدد نامعتبر به‌صورت صفر) + عمومی‌سازی `numF`/`numI` روی `~string` در `internal/okx/rest.go` — پول ۳۰s حالا ۵۲۴ نماد را پایدار سرو می‌کند
### Added
- فعال‌سازی GitHub Actions روی fork + `workflow_dispatch` برای اجرای دستی
- این CHANGELOG

## [0.6.0] — 2026-10-21 — فاز ۶ «داده و یکپارچه‌سازی» (مرحلهٔ ۱: OKX) — PR #3
### Added
- آداپتور OKX v5 (`internal/okx`): books/trades/mark-price/funding-rate/open-interest روی `/ws/v5/public` + liquidation-orders؛ تبدیل قرارداد→کوین با ctVal؛ تیکر سراسری REST-poll هر ۳۰s (OKX پوش همه‌بازار ندارد)؛ کندل تاریخی REST با 1s و صفحه‌بندی history-candles؛ ثبت با فلگ `-okx-base` (~۱۰۴۴ نماد SWAP)
### Fixed
- نظم prime اشتراک orderbook نسبت به broadcast (نوشتن زیر قفل hub)؛ diff های OKX با FirstUpdateId=seq (معادل U-range بایننس)

## [0.5.0] — 2026-10-21 — مقاوم‌سازی REST (PR #2)
### Added
- Circuit breaker برای IP مشترک egress + دنبالهٔ retry برای history
### Fixed
- مسیریابی فریم‌های @depth20 به onPartialDepth

## [0.4.0] — 2026-10-18 — پل خروجی داده (فاز ۱)
### Added
- پل JSON-WS روی سیم protobuf گیت‌وی (`feat(egress)`) — مصرف‌کنندگان بیرونی بدون protobuf

## [0.3.0] — 2026-10-18 — تاب‌آوری عمق (PR #1)
### Added
- Fallback عمق partial-book (@depth20) در برابر 418/محدودیت نرخ بایننس

## [مبنا] — 2026-08-09 / 2026-10-11 — انتشار upstream
- گیت‌وی Go بایننس با اتصال عمق/trade/candle و تصویر GHCR
