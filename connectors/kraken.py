"""
connectors/kraken.py — Kraken REST API connector.

Public endpoints used for ticker/orderbook.
Authenticated endpoints used for OHLCV (may provide more history).
Credentials loaded from .env via config.py.
"""

import base64
import hashlib
import hmac
import logging
import os
import time
import urllib.parse
from typing import Any, Dict, List, Optional

import requests
from dotenv import load_dotenv

load_dotenv()

logger = logging.getLogger(__name__)

BASE_URL   = "https://api.kraken.com"
API_KEY    = os.getenv("KRAKEN_API_KEY", "")
API_SECRET = os.getenv("KRAKEN_API_SECRET", "")


def _get(path: str, params: Optional[Dict] = None) -> Any:
    resp = requests.get(BASE_URL + path, params=params or {}, timeout=10)
    resp.raise_for_status()
    data = resp.json()
    if data.get("error"):
        raise ValueError(f"Kraken API error: {data['error']}")
    return data.get("result", {})


def _sign(path: str, data: dict, secret: str) -> str:
    """Generate Kraken API-Sign header value."""
    post_data = urllib.parse.urlencode(data)
    encoded = (str(data["nonce"]) + post_data).encode()
    message = path.encode() + hashlib.sha256(encoded).digest()
    mac = hmac.new(base64.b64decode(secret), message, hashlib.sha512)
    return base64.b64encode(mac.digest()).decode()


def _post_private(path: str, data: Optional[Dict] = None) -> Any:
    """POST to a private Kraken endpoint with authentication."""
    if not API_KEY or not API_SECRET:
        raise ValueError("KRAKEN_API_KEY / KRAKEN_API_SECRET not set in .env")
    payload = data or {}
    payload["nonce"] = str(int(time.time() * 1000))
    headers = {
        "API-Key":  API_KEY,
        "API-Sign": _sign(path, payload, API_SECRET),
    }
    resp = requests.post(BASE_URL + path, data=payload, headers=headers, timeout=10)
    resp.raise_for_status()
    result = resp.json()
    if result.get("error"):
        raise ValueError(f"Kraken API error: {result['error']}")
    return result.get("result", {})


def get_ohlcv(pair: str, interval_minutes: int, since: Optional[int] = None) -> List[Dict[str, Any]]:
    """
    GET /0/public/OHLC
    Returns list of {ts, open, high, low, close, volume} oldest-first.

    pair: e.g. 'XBTUSD'
    interval_minutes: 1, 5, 15, 30, 60, 240, 1440, 10080, 21600
    since: optional unix timestamp to fetch from
    """
    params: Dict[str, Any] = {"pair": pair, "interval": interval_minutes}
    if since is not None:
        params["since"] = since

    result = _get("/0/public/OHLC", params=params)
    pair_key = [k for k in result.keys() if k != "last"][0]
    candles = result[pair_key]

    return [
        {
            "ts": int(c[0]),
            "open": float(c[1]),
            "high": float(c[2]),
            "low": float(c[3]),
            "close": float(c[4]),
            "volume": float(c[6]),
        }
        for c in candles
    ]


def get_ohlcv_authenticated(pair: str, interval_minutes: int, since: Optional[int] = None) -> List[Dict[str, Any]]:
    """
    POST /0/private/OHLC (authenticated) — may provide more historical data than public endpoint.
    Falls back to public endpoint if credentials not available.
    """
    if not API_KEY or not API_SECRET:
        logger.warning("No Kraken credentials — falling back to public OHLCV")
        return get_ohlcv(pair, interval_minutes, since)

    payload: Dict[str, Any] = {"pair": pair, "interval": interval_minutes}
    if since is not None:
        payload["since"] = since

    try:
        result = _post_private("/0/private/OHLC", payload)
        pair_key = [k for k in result.keys() if k != "last"][0]
        candles = result[pair_key]
        return [
            {
                "ts": int(c[0]),
                "open": float(c[1]),
                "high": float(c[2]),
                "low": float(c[3]),
                "close": float(c[4]),
                "volume": float(c[6]),
            }
            for c in candles
        ]
    except Exception as e:
        logger.warning(f"Authenticated OHLCV failed ({e}) — falling back to public")
        return get_ohlcv(pair, interval_minutes, since)


def get_orderbook(pair: str, depth: int = 25) -> Dict[str, Any]:
    """
    GET /0/public/Depth
    Returns dict with 'bids' and 'asks', each a list of [price, volume] strings.

    pair: e.g. 'XBTUSD'
    depth: number of levels (max 500)
    """
    result = _get("/0/public/Depth", params={"pair": pair, "count": depth})
    pair_key = list(result.keys())[0]
    ob = result[pair_key]
    return {
        "bids": [[float(b[0]), float(b[1])] for b in ob["bids"]],
        "asks": [[float(a[0]), float(a[1])] for a in ob["asks"]],
    }


def get_ticker(pair: str) -> Dict[str, Any]:
    """
    GET /0/public/Ticker
    Returns dict with last_price (float), volume_24h (float), spread (float).

    pair: e.g. 'XBTUSD'
    """
    result = _get("/0/public/Ticker", params={"pair": pair})
    pair_key = list(result.keys())[0]
    t = result[pair_key]
    last = float(t["c"][0])
    vol_24h = float(t["v"][1])
    best_ask = float(t["a"][0])
    best_bid = float(t["b"][0])
    spread = best_ask - best_bid
    return {
        "last_price": last,
        "volume_24h": vol_24h,
        "spread": spread,
    }
