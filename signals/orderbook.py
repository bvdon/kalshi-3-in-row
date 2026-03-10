"""
signals/orderbook.py — Order book depth signals.

All functions are pure: no API calls, no side effects.
Input: order book depth data (lists of [price, volume]).
Output: float in [-1.0, 1.0]
"""

from typing import List, Tuple


def imbalance_signal(bids: List[List[float]], asks: List[List[float]], depth: int = 10) -> float:
    """
    Order book imbalance signal.

    Measures volume imbalance between top `depth` bid and ask levels.
    Positive = more bid volume (bullish pressure).
    Negative = more ask volume (bearish pressure).

    Args:
        bids: List of [price, volume] pairs, best bid first.
        asks: List of [price, volume] pairs, best ask first.
        depth: Number of levels to consider (default 10).

    Returns:
        Float in [-1.0, 1.0].
    """
    bid_vol = sum(b[1] for b in bids[:depth])
    ask_vol = sum(a[1] for a in asks[:depth])
    total = bid_vol + ask_vol

    if total == 0:
        return 0.0

    imbalance = (bid_vol - ask_vol) / total
    return max(-1.0, min(1.0, imbalance))


def spread_signal(bids: List[List[float]], asks: List[List[float]]) -> float:
    """
    Spread-based signal.

    Tight spread → neutral/slightly positive (liquid market).
    Wide spread relative to mid price → negative (illiquid, avoid).

    Normalizes spread as % of mid price; wide = >0.5%, maps to -1.0.

    Args:
        bids: List of [price, volume] pairs, best bid first.
        asks: List of [price, volume] pairs, best ask first.

    Returns:
        Float in [-1.0, 1.0]. Negative = wide spread (bad conditions).
    """
    if not bids or not asks:
        return 0.0

    best_bid = bids[0][0]
    best_ask = asks[0][0]
    mid = (best_bid + best_ask) / 2

    if mid == 0:
        return 0.0

    spread_pct = (best_ask - best_bid) / mid
    # 0% spread → +0.5 (perfect liquidity bonus), 0.5%+ spread → -1.0
    max_spread = 0.005  # 0.5%
    normalized = 1.0 - (spread_pct / max_spread) * 2
    return max(-1.0, min(1.0, normalized))


def trade_delta_signal(trades: list, window_seconds: int = 180) -> float:
    """
    Compute a rolling buy/sell volume delta from a trade feed.

    Measures buy vs sell pressure over the given time window:
      delta = (buy_volume - sell_volume) / (buy_volume + sell_volume)

    Args:
        trades: List of dicts with keys:
                  price  (float)  — trade price
                  volume (float)  — trade size
                  side   (str)    — 'buy' or 'sell'
                  time   (float)  — unix timestamp
        window_seconds: How far back to look (default 180 = 3 minutes).

    Returns:
        Float in [-1.0, 1.0]. Positive = buy pressure dominates.
        Returns 0.0 if no trades in window.
    """
    import time as _time

    if not trades:
        return 0.0

    cutoff = _time.time() - window_seconds
    recent = [t for t in trades if t.get("time", 0) >= cutoff]

    if not recent:
        return 0.0

    buy_vol = sum(t["volume"] for t in recent if t.get("side") == "buy")
    sell_vol = sum(t["volume"] for t in recent if t.get("side") == "sell")
    total = buy_vol + sell_vol

    if total == 0:
        return 0.0

    delta = (buy_vol - sell_vol) / total
    return max(-1.0, min(1.0, delta))
