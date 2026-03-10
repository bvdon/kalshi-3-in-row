"""
signals/momentum.py — Price momentum signals based on OHLCV close prices.

All functions are pure: no API calls, no side effects.
Input: list of floats (close prices), oldest-first.
Output: float in [-1.0, 1.0]
"""

import math
from typing import List


def ema(values: List[float], period: int) -> List[float]:
    """
    Compute Exponential Moving Average.

    Args:
        values: List of floats, oldest-first.
        period: EMA period.

    Returns:
        List of EMA values (same length as input), oldest-first.
    """
    if not values or period <= 0:
        return []
    k = 2.0 / (period + 1)
    result = [values[0]]
    for v in values[1:]:
        result.append(v * k + result[-1] * (1 - k))
    return result


def rsi_signal(closes: List[float], period: int = 14) -> float:
    """
    RSI-based signal.

    Computes RSI over `period` bars and maps to [-1.0, 1.0]:
      RSI 0   → -1.0 (extremely oversold, buy signal)
      RSI 50  →  0.0 (neutral)
      RSI 100 → +1.0 (extremely overbought, sell signal)

    Wait — for a *long* signal: oversold means we want to buy YES.
    So we invert: (RSI - 50) / 50, clamped to [-1, 1].

    Args:
        closes: List of close prices, oldest-first. Needs >= period+1 values.
        period: RSI period (default 14).

    Returns:
        Float in [-1.0, 1.0]. Positive = bullish, negative = bearish.
    """
    if len(closes) < period + 1:
        return 0.0

    gains, losses = [], []
    for i in range(1, len(closes)):
        diff = closes[i] - closes[i - 1]
        gains.append(max(diff, 0))
        losses.append(max(-diff, 0))

    avg_gain = sum(gains[-period:]) / period
    avg_loss = sum(losses[-period:]) / period

    if avg_loss == 0:
        rsi = 100.0
    else:
        rs = avg_gain / avg_loss
        rsi = 100.0 - (100.0 / (1 + rs))

    # Map: oversold (RSI < 50) → positive signal (buy); overbought → negative
    signal = (rsi - 50.0) / 50.0
    return max(-1.0, min(1.0, signal))


def macd_signal(closes: List[float]) -> float:
    """
    MACD-based signal using standard 12/26/9 parameters.

    Signal = sign and magnitude of MACD histogram, normalized to [-1.0, 1.0]
    using the rolling max absolute histogram value over the series.

    Args:
        closes: List of close prices, oldest-first. Needs >= 35 values.

    Returns:
        Float in [-1.0, 1.0]. Positive = bullish (MACD above signal line).
    """
    if len(closes) < 35:
        return 0.0

    fast_ema = ema(closes, 12)
    slow_ema = ema(closes, 26)
    macd_line = [f - s for f, s in zip(fast_ema, slow_ema)]
    signal_line = ema(macd_line, 9)
    histogram = [m - s for m, s in zip(macd_line, signal_line)]

    if not histogram:
        return 0.0

    latest = histogram[-1]
    max_abs = max(abs(h) for h in histogram) or 1.0
    normalized = latest / max_abs
    return max(-1.0, min(1.0, normalized))


def ma_crossover_signal(closes: List[float], fast: int = 9, slow: int = 21) -> float:
    """
    Moving average crossover signal.

    Returns +1.0 if fast MA > slow MA (bullish), -1.0 if fast < slow (bearish),
    scaled by the relative gap between them.

    Args:
        closes: List of close prices, oldest-first. Needs >= slow values.
        fast: Fast MA period (default 9).
        slow: Slow MA period (default 21).

    Returns:
        Float in [-1.0, 1.0].
    """
    if len(closes) < slow:
        return 0.0

    fast_ema_vals = ema(closes, fast)
    slow_ema_vals = ema(closes, slow)

    if not fast_ema_vals or not slow_ema_vals:
        return 0.0

    f = fast_ema_vals[-1]
    s = slow_ema_vals[-1]

    if s == 0:
        return 0.0

    # Relative difference, clamped at 5% = full signal
    rel = (f - s) / s
    signal = rel / 0.05
    return max(-1.0, min(1.0, signal))


def vol_regime(closes: List[float], window: int = 20) -> str:
    """
    Classify the current volatility regime as 'low', 'medium', or 'high'.

    Uses rolling realized volatility (std dev of log returns over `window`
    bars) compared to the historical average realized volatility across the
    full series.

    Thresholds:
      - current_vol < 0.75 * hist_avg  → 'low'
      - current_vol > 1.25 * hist_avg  → 'high'
      - otherwise                       → 'medium'

    Args:
        closes: List of close prices, oldest-first. Needs >= window + 1 values.
        window: Lookback window for the rolling realized vol (default 20).

    Returns:
        One of 'low', 'medium', or 'high'.
    """
    if len(closes) < window + 1:
        return "medium"

    log_returns = [
        math.log(closes[i] / closes[i - 1])
        for i in range(1, len(closes))
        if closes[i - 1] > 0 and closes[i] > 0
    ]

    if len(log_returns) < window:
        return "medium"

    def _std(values: List[float]) -> float:
        n = len(values)
        if n < 2:
            return 0.0
        mean = sum(values) / n
        variance = sum((v - mean) ** 2 for v in values) / (n - 1)
        return math.sqrt(variance)

    current_vol = _std(log_returns[-window:])
    hist_avg = _std(log_returns)

    if hist_avg == 0:
        return "medium"

    ratio = current_vol / hist_avg
    if ratio < 0.75:
        return "low"
    elif ratio > 1.25:
        return "high"
    return "medium"


def vol_adjusted_score(base_score: float, regime: str, signal_type: str) -> float:
    """
    Adjust a signal score based on the current volatility regime.

    Logic:
      - In *low* vol:    boost mean-reversion signals (+20%), dampen momentum (-20%)
      - In *high* vol:   boost momentum signals (+20%), dampen mean-reversion (-20%)
      - In *medium* vol: no adjustment

    The 20% multipliers are conservative to avoid over-fitting.

    Args:
        base_score:  Raw signal score in [-1.0, 1.0].
        regime:      One of 'low', 'medium', 'high' from vol_regime().
        signal_type: Either 'momentum' or 'mean_reversion'.

    Returns:
        Adjusted score, clamped to [-1.0, 1.0].
    """
    multiplier = 1.0

    if regime == "low":
        if signal_type == "mean_reversion":
            multiplier = 1.20
        elif signal_type == "momentum":
            multiplier = 0.80
    elif regime == "high":
        if signal_type == "momentum":
            multiplier = 1.20
        elif signal_type == "mean_reversion":
            multiplier = 0.80
    # medium → no change

    adjusted = base_score * multiplier
    return max(-1.0, min(1.0, adjusted))
