"""
signals/kalshi_momentum.py — Kalshi contract price momentum signals.

All functions are pure: no API calls, no side effects.
Input: list of dicts {ts, yes_price} (cents, 0-100), oldest-first.
Output: float in [-1.0, 1.0]
"""

from typing import Any, Dict, List

from signals.momentum import ema, rsi_signal


def _extract_prices(price_history: List[Dict[str, Any]]) -> List[float]:
    """Extract yes_price floats from history list."""
    return [float(h["yes_price"]) for h in price_history]


def contract_momentum(price_history: List[Dict[str, Any]], window: int = 5) -> float:
    """
    Short-term Kalshi contract price momentum.

    Measures rate of change over `window` bars.
    Positive = contract price rising (market becoming more bullish).

    Args:
        price_history: List of {ts, yes_price} dicts, oldest-first.
        window: Lookback bars (default 5).

    Returns:
        Float in [-1.0, 1.0].
    """
    prices = _extract_prices(price_history)
    if len(prices) < window + 1:
        return 0.0

    old_price = prices[-(window + 1)]
    new_price = prices[-1]

    if old_price == 0:
        return 0.0

    # Contract prices are 1-99 cents; 20-cent swing = full signal
    change = new_price - old_price
    signal = change / 20.0
    return max(-1.0, min(1.0, signal))


def contract_rsi(price_history: List[Dict[str, Any]], period: int = 10) -> float:
    """
    RSI applied to Kalshi contract yes_price history.

    Args:
        price_history: List of {ts, yes_price} dicts, oldest-first.
        period: RSI period (default 10).

    Returns:
        Float in [-1.0, 1.0]. Positive = bullish (oversold contract).
    """
    prices = _extract_prices(price_history)
    return rsi_signal(prices, period=period)


def contract_ma_signal(price_history: List[Dict[str, Any]], fast: int = 5, slow: int = 15) -> float:
    """
    EMA crossover on Kalshi contract yes_price history.

    Args:
        price_history: List of {ts, yes_price} dicts, oldest-first.
        fast: Fast EMA period (default 5).
        slow: Slow EMA period (default 15).

    Returns:
        Float in [-1.0, 1.0].
    """
    prices = _extract_prices(price_history)
    if len(prices) < slow:
        return 0.0

    fast_vals = ema(prices, fast)
    slow_vals = ema(prices, slow)

    f = fast_vals[-1]
    s = slow_vals[-1]

    if s == 0:
        return 0.0

    # Contract prices 1-99; 10-cent difference = full signal
    diff = f - s
    signal = diff / 10.0
    return max(-1.0, min(1.0, signal))
