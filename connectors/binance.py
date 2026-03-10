"""
connectors/binance.py — Binance.us public REST API connector.

No API key required for public OHLCV data.
Supports pagination via startTime/endTime for deep historical data.
"""

import logging
import time
from typing import Any, Dict, List, Optional

import requests

logger = logging.getLogger(__name__)

BASE_URL = "https://api.binance.us"


def _get(path: str, params: Optional[Dict] = None) -> Any:
    resp = requests.get(BASE_URL + path, params=params or {}, timeout=10)
    resp.raise_for_status()
    return resp.json()


def get_ohlcv(
    symbol: str,
    interval: str = "15m",
    limit: int = 1000,
    start_time_ms: Optional[int] = None,
    end_time_ms: Optional[int] = None,
) -> List[Dict[str, Any]]:
    """
    GET /api/v3/klines
    Returns list of {ts, open, high, low, close, volume} oldest-first.

    symbol: e.g. 'BTCUSDT'
    interval: '1m', '5m', '15m', '30m', '1h', '4h', '1d'
    limit: max 1000 per call
    start_time_ms / end_time_ms: optional unix ms timestamps
    """
    params: Dict[str, Any] = {"symbol": symbol, "interval": interval, "limit": limit}
    if start_time_ms is not None:
        params["startTime"] = start_time_ms
    if end_time_ms is not None:
        params["endTime"] = end_time_ms

    data = _get("/api/v3/klines", params=params)
    return [
        {
            "ts":     int(c[0]) // 1000,   # convert ms → seconds
            "open":   float(c[1]),
            "high":   float(c[2]),
            "low":    float(c[3]),
            "close":  float(c[4]),
            "volume": float(c[5]),
        }
        for c in data
    ]


def get_ohlcv_range(
    symbol: str,
    interval: str = "15m",
    start_time_ms: Optional[int] = None,
    end_time_ms: Optional[int] = None,
    max_candles: int = 10000,
) -> List[Dict[str, Any]]:
    """
    Paginated fetch — returns candles from start_time_ms to end_time_ms.
    Walks forward in time using startTime pagination.
    Capped at max_candles to prevent runaway calls.
    """
    all_candles: Dict[int, Any] = {}
    current_start = start_time_ms

    while len(all_candles) < max_candles:
        params: Dict[str, Any] = {"limit": 1000}
        if current_start is not None:
            params["start_time_ms"] = current_start
        if end_time_ms is not None:
            params["end_time_ms"] = end_time_ms

        batch = get_ohlcv(symbol, interval, limit=1000,
                          start_time_ms=current_start,
                          end_time_ms=end_time_ms)
        if not batch:
            break

        added = 0
        for c in batch:
            if c["ts"] not in all_candles:
                all_candles[c["ts"]] = c
                added += 1

        if added == 0:
            break

        # Advance start to just after last candle
        last_ts_ms = batch[-1]["ts"] * 1000
        if end_time_ms and last_ts_ms >= end_time_ms:
            break
        current_start = last_ts_ms + 1
        time.sleep(0.1)  # be polite to the API

    candles_sorted = sorted(all_candles.values(), key=lambda c: c["ts"])
    return candles_sorted


def get_ticker(symbol: str) -> Dict[str, Any]:
    """
    GET /api/v3/ticker/price
    Returns current price for symbol.
    """
    data = _get("/api/v3/ticker/price", params={"symbol": symbol})
    return {"last_price": float(data["price"])}
