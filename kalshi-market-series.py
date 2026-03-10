"""
kalshi-market-series.py — Fetch and display raw market data for a Kalshi series or specific ticker.

Modes (mutually exclusive — pick one):
  (default)    Series list:   GET /markets?series_ticker=X&limit=N
  --ticker     Single market: GET /markets/{ticker}
  --candles    Candlesticks:  GET /series/{series}/markets/{ticker}/candlesticks
                              Automatically uses the market's own open_time/close_time
                              as the candle window. Override with --lookback if needed.

Usage:
  python kalshi-market-series.py
  python kalshi-market-series.py --series KXBTC15M --limit 10

  python kalshi-market-series.py --ticker KXBTC15M-26MAR080430-30
  python kalshi-market-series.py --ticker KXBTC15M-26MAR080430-30 --raw

  python kalshi-market-series.py --ticker KXBTC15M-26MAR080430-30 --candles
  python kalshi-market-series.py --ticker KXBTC15M-26MAR080430-30 --candles --interval 1
  python kalshi-market-series.py --ticker KXBTC15M-26MAR080430-30 --candles --lookback 3600

Options:
  --series      Series ticker (default: KXBTC15M)
  --ticker      Target a specific market ticker (required for --candles)
  --limit       Max records for series list queries (default: 5)
  --candles     Fetch candlestick OHLC data for --ticker
  --interval    Candle period in minutes: 1, 5, 15, 60, 1440 (default: 1)
  --lookback    Override: seconds back from now instead of market open/close window
  --raw         Dump compact JSON instead of pretty-printed
"""

import argparse
import json
import sys
import time
from datetime import datetime, timezone

sys.path.insert(0, ".")
import connectors.kalshi as kalshi


DEFAULT_SERIES   = "KXBTC15M"
DEFAULT_LIMIT    = 2
DEFAULT_INTERVAL = 1       # minutes


def fetch_series(series_ticker: str, limit: int) -> dict:
    """GET /markets?series_ticker=X&limit=N"""
    return kalshi._get("/markets", params={"series_ticker": series_ticker, "limit": limit})


def fetch_single(ticker: str) -> dict:
    """GET /markets/{ticker}"""
    return kalshi._get(f"/markets/{ticker}")


def fetch_candles(series: str, ticker: str, interval: int, lookback: int = None) -> dict:
    """
    GET /series/{series}/markets/{ticker}/candlesticks

    Window priority:
      1. If lookback is given: now-lookback → now
      2. Otherwise: fetch the market's open_time/close_time and use that exact window
    """
    if lookback is not None:
        end_ts   = int(time.time())
        start_ts = end_ts - lookback
        print(f"  Window: last {lookback}s (override)", file=sys.stderr)
    else:
        market = kalshi._get(f"/markets/{ticker}").get("market", {})
        open_str  = market.get("open_time")
        close_str = market.get("close_time")
        if not open_str or not close_str:
            raise ValueError(f"Market {ticker} missing open_time/close_time — use --lookback instead")
        def iso_to_ts(s):
            return int(datetime.fromisoformat(s.replace("Z", "+00:00")).timestamp())
        start_ts = iso_to_ts(open_str)
        end_ts   = iso_to_ts(close_str)
        print(f"  Window: {open_str} → {close_str}  ({end_ts - start_ts}s)", file=sys.stderr)

    return kalshi._get(
        f"/series/{series}/markets/{ticker}/candlesticks",
        params={"start_ts": start_ts, "end_ts": end_ts, "period_interval": interval},
    )


def main():
    parser = argparse.ArgumentParser(description="Kalshi market series inspector")
    parser.add_argument("--series",   default=DEFAULT_SERIES,        help=f"Series ticker (default: {DEFAULT_SERIES})")
    parser.add_argument("--ticker",   default=None,                   help="Specific market ticker")
    parser.add_argument("--limit",    type=int, default=DEFAULT_LIMIT, help=f"Max records for series list (default: {DEFAULT_LIMIT})")
    parser.add_argument("--candles",  action="store_true",            help="Fetch candlestick OHLC for --ticker")
    parser.add_argument("--interval", type=int, default=DEFAULT_INTERVAL, help=f"Candle period in minutes (default: {DEFAULT_INTERVAL})")
    parser.add_argument("--lookback", type=int, default=None,         help="Override candle window: seconds back from now")
    parser.add_argument("--raw",      action="store_true",            help="Compact JSON output")
    args = parser.parse_args()

    if args.candles:
        if not args.ticker:
            print("ERROR: --candles requires --ticker", file=sys.stderr)
            sys.exit(1)
        print(f"\nFetching candles: {args.ticker}  interval={args.interval}m", file=sys.stderr)
        data = fetch_candles(args.series, args.ticker, args.interval, args.lookback)

    elif args.ticker:
        print(f"\nFetching single market: {args.ticker}\n", file=sys.stderr)
        data = fetch_single(args.ticker)

    else:
        print(f"\nFetching series: {args.series}  limit={args.limit}\n", file=sys.stderr)
        data = fetch_series(args.series, args.limit)

    print(json.dumps(data) if args.raw else json.dumps(data, indent=2))


if __name__ == "__main__":
    main()
