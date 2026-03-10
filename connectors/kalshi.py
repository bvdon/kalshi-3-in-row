"""
connectors/kalshi.py — Kalshi REST API connector.

Auth: RSA-PSS signing of (timestamp_ms + METHOD + /trade-api/v2/path)
Credentials loaded from config.py (never hardcoded here).
"""

import base64
import logging
import time
import uuid
from typing import Any, Dict, List, Optional

import requests
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import padding

import config

logger = logging.getLogger(__name__)

BASE_URL = "https://api.elections.kalshi.com"
API_PREFIX = "/trade-api/v2"


def _load_private_key():
    """Load RSA private key from file path in config."""
    with open(config.KALSHI_KEY_FILE, "rb") as f:
        return serialization.load_pem_private_key(f.read(), password=None)


def _sign(private_key, text: str) -> str:
    """Sign text with RSA-PSS and return base64-encoded signature."""
    message = text.encode("utf-8")
    signature = private_key.sign(
        message,
        padding.PSS(
            mgf=padding.MGF1(hashes.SHA256()),
            salt_length=padding.PSS.DIGEST_LENGTH,
        ),
        hashes.SHA256(),
    )
    return base64.b64encode(signature).decode("utf-8")


def _headers(method: str, path: str) -> Dict[str, str]:
    """Build signed auth headers for a Kalshi API request."""
    private_key = _load_private_key()
    ts = str(int(time.time() * 1000))
    path_no_query = path.split("?")[0]
    signature = _sign(private_key, ts + method.upper() + path_no_query)
    return {
        "Content-Type": "application/json",
        "KALSHI-ACCESS-KEY": config.KALSHI_KEY_ID,
        "KALSHI-ACCESS-SIGNATURE": signature,
        "KALSHI-ACCESS-TIMESTAMP": ts,
    }


def _get(path: str, params: Optional[Dict] = None) -> Any:
    full_path = API_PREFIX + path
    resp = requests.get(BASE_URL + full_path, headers=_headers("GET", full_path), params=params or {}, timeout=10)
    resp.raise_for_status()
    return resp.json()


def _post(path: str, body: Optional[Dict] = None) -> Any:
    full_path = API_PREFIX + path
    resp = requests.post(BASE_URL + full_path, headers=_headers("POST", full_path), json=body or {}, timeout=10)
    resp.raise_for_status()
    return resp.json()


def _delete(path: str) -> Any:
    full_path = API_PREFIX + path
    resp = requests.delete(BASE_URL + full_path, headers=_headers("DELETE", full_path), timeout=10)
    resp.raise_for_status()
    return resp.json()


# ── Public API ───────────────────────────────────────────────────────────────

def get_balance() -> int:
    """
    GET /portfolio/balance
    Returns balance in cents.
    """
    data = _get("/portfolio/balance")
    return data.get("balance", 0)


def get_position(ticker: str) -> Optional[Dict[str, Any]]:
    """
    GET /portfolio/positions
    Returns position dict if net_contracts != 0, else None.
    Checks exact ticker first, then falls back to any open position
    in the same series (handles market window rollovers).
    """
    # First try exact ticker match
    data = _get("/portfolio/positions", params={"ticker": ticker, "count_filter": "position"})
    positions = data.get("market_positions", [])
    for pos in positions:
        if pos.get("ticker") == ticker and pos.get("position", 0) != 0:
            pos["side"] = "yes" if pos["position"] > 0 else "no"
            pos["contracts"] = abs(pos["position"])
            return pos

    # Fallback: check all open positions for any ticker in the same series
    # Only return if the market is still open (not expired)
    series = ticker.rsplit("-", 2)[0]  # e.g. KXBTC15M from KXBTC15M-26FEB261500-00
    data2 = _get("/portfolio/positions", params={"count_filter": "position", "limit": 50})
    for pos in data2.get("market_positions", []):
        pos_ticker = pos.get("ticker", "")
        if pos_ticker.startswith(series) and pos.get("position", 0) != 0:
            # Verify the market is still open before treating as active position
            try:
                market_data = _get(f"/markets/{pos_ticker}")
                status = market_data.get("market", {}).get("status", "")
                if status != "open":
                    logger.info(f"Skipping settled position on {pos_ticker} (status={status})")
                    continue
            except Exception:
                continue
            pos["side"] = "yes" if pos["position"] > 0 else "no"
            pos["contracts"] = abs(pos["position"])
            logger.info(f"Found position on rolled-over ticker: {pos_ticker}")
            return pos
    return None


def get_market(ticker: str) -> Dict[str, Any]:
    """
    GET /markets/{ticker}
    Returns dict with yes_bid, yes_ask, no_bid, no_ask (all in cents).
    """
    data = _get(f"/markets/{ticker}")
    market = data.get("market", {})
    return {
        "yes_bid": market.get("yes_bid", 0),
        "yes_ask": market.get("yes_ask", 0),
        "no_bid": market.get("no_bid", 0),
        "no_ask": market.get("no_ask", 0),
        "close_time": market.get("close_time"),
        "yes_sub_title": market.get("yes_sub_title", ""),
    }


def get_market_history(ticker: str, limit: int = 100) -> List[Dict[str, Any]]:
    """
    GET /markets/{ticker}/history
    Returns list of {ts, yes_price} oldest-first.
    """
    data = _get(f"/markets/{ticker}/history", params={"limit": limit})
    history = data.get("history", [])
    result = [{"ts": h.get("ts"), "yes_price": h.get("yes_bid", 0)} for h in history]
    return list(reversed(result))  # oldest-first


def get_orderbook(ticker: str) -> Dict[str, Any]:
    """
    GET /markets/{ticker}/orderbook
    Returns dict with yes/no depth lists.
    """
    data = _get(f"/markets/{ticker}/orderbook")
    return data.get("orderbook", {})


def place_order(
    ticker: str,
    side: str,
    contracts: int,
    price_cents: int,
    order_type: str = "limit",
    action: str = "buy",
) -> str:
    """
    POST /portfolio/orders
    Returns order_id string.

    side: 'yes' or 'no'
    price_cents: 1-99
    order_type: 'limit' or 'market'
    action: 'buy' or 'sell'
    """
    body = {
        "ticker": ticker,
        "client_order_id": str(uuid.uuid4()),
        "action": action,
        "side": side,
        "count": contracts,
        "type": order_type,
        "yes_price": price_cents if side == "yes" else (100 - price_cents),
    }
    data = _post("/portfolio/orders", body=body)
    return data.get("order", {}).get("order_id", "")


def get_order(order_id: str) -> Dict[str, Any]:
    """
    GET /portfolio/orders/{order_id}
    Returns order dict with fill details.
    """
    data = _get(f"/portfolio/orders/{order_id}")
    return data.get("order", {})


def cancel_all_resting_orders(ticker: str) -> int:
    """
    Cancel all resting (unfilled) orders for a given market ticker.
    Returns the number of orders cancelled.
    """
    data = _get("/portfolio/orders", params={"status": "resting", "ticker": ticker, "limit": 50})
    orders = data.get("orders", [])
    cancelled = 0
    for order in orders:
        order_id = order.get("order_id")
        if order_id:
            if cancel_order(order_id):
                logger.info(f"Cancelled resting order {order_id} for {ticker}")
                cancelled += 1
    return cancelled


def get_active_ticker(series_ticker: str) -> Optional[str]:
    """
    GET /markets?series_ticker=X&status=open
    Returns the ticker of the soonest-closing open market in the series,
    or None if no open markets are found.

    Each Kalshi series (e.g. KXBTCD) has multiple rolling windows open at
    any given time. This picks the one expiring soonest — i.e. the current
    active window you'd want to trade.
    """
    data = _get("/markets", params={"series_ticker": series_ticker, "status": "open", "limit": 100})
    markets = data.get("markets", [])
    if not markets:
        logger.warning(f"No open markets found for series {series_ticker}")
        return None
    # Sort by close_time ascending, pick the nearest expiry
    markets_with_close = [m for m in markets if m.get("close_time")]
    if not markets_with_close:
        return markets[0].get("ticker")
    markets_with_close.sort(key=lambda m: m["close_time"])
    ticker = markets_with_close[0]["ticker"]
    logger.info(f"Active ticker for {series_ticker}: {ticker} (closes {markets_with_close[0]['close_time']})")
    return ticker


def get_cycle_floor(ticker: str, side: str) -> Optional[float]:
    """
    Fetch the lowest available price for the given side during the first minute
    of the market cycle, using the candlestick API on a finalized market.

    side: 'yes' or 'no'
    Returns price in dollars (e.g. 0.35), or None if unavailable.

    YES floor  = candle low  (direct)
    NO  floor  = 1.00 - candle high  (inverse: NO price = 100 - YES price)
    """
    try:
        # Derive series from ticker: KXBTC15M-26MAR090200-00 → KXBTC15M
        series = ticker.rsplit("-", 2)[0]

        # Fetch market to get open_time
        market = _get(f"/markets/{ticker}").get("market", {})
        open_time_str = market.get("open_time")
        if not open_time_str:
            return None

        from datetime import datetime, timezone
        open_dt  = datetime.fromisoformat(open_time_str.replace("Z", "+00:00"))
        start_ts = int(open_dt.timestamp())
        end_ts   = start_ts + 60  # first 60 seconds of cycle

        data = _get(
            f"/series/{series}/markets/{ticker}/candlesticks",
            params={"start_ts": start_ts, "end_ts": end_ts, "period_interval": 1},
        )
        candles = data.get("candlesticks", [])
        if not candles:
            return None

        # Use traded price (price.low / price.high) — reflects actual fills,
        # works correctly for both maker and taker entries.
        # YES floor = lowest YES price that traded
        # NO  floor = 100 - highest YES price that traded (inverse)
        if side == "yes":
            low_cents = min(
                c.get("price", {}).get("low", 100) for c in candles
            )
            return round(low_cents / 100, 2)
        else:
            high_cents = max(
                c.get("price", {}).get("high", 0) for c in candles
            )
            return round((100 - high_cents) / 100, 2)

    except Exception as e:
        logger.warning(f"get_cycle_floor failed for {ticker}: {e}")
        return None


def cancel_order(order_id: str) -> bool:
    """
    DELETE /portfolio/orders/{order_id}
    Returns True on success.
    """
    try:
        _delete(f"/portfolio/orders/{order_id}")
        return True
    except Exception as e:
        logger.error(f"cancel_order failed for {order_id}: {e}")
        return False
