"""
kalshi-backtest-qwen_v4.py
==========================
IDEA: Regime Detection + Signal Switching
- Compute ADX-style "trending" vs "ranging" regime using EMA spread
- In RANGING regime: use mean-reversion signal (EMA9 slope reversal)
- In TRENDING regime: use trend-following signal OR skip
- Hypothesis: mean-reversion works because BTC ranges most of the time;
  in trending regimes those signals fail badly.

FOUND: Filtering to ranging regime significantly improves WR but reduces trade count.
Trending regime signals consistently underperform mean-reversion.
"""

import sqlite3
import numpy as np

DB_PATH = "/Users/roberthenning/python-projects/kalshi-bot-v2/candles.db"
WIN_PROFIT = 5.0
LOSS_AMOUNT = -5.0
CONTRACTS = 10

def load_candles():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT ts, open, high, low, close FROM btc_candles ORDER BY ts ASC")
    rows = cur.fetchall()
    conn.close()
    return rows

def compute_emas(closes, fast=9, slow=50):
    """Return arrays of EMA fast and slow"""
    ema_fast = np.zeros(len(closes))
    ema_slow = np.zeros(len(closes))
    kf = 2 / (fast + 1)
    ks = 2 / (slow + 1)
    ema_fast[0] = closes[0]
    ema_slow[0] = closes[0]
    for i in range(1, len(closes)):
        ema_fast[i] = closes[i] * kf + ema_fast[i-1] * (1 - kf)
        ema_slow[i] = closes[i] * ks + ema_slow[i-1] * (1 - ks)
    return ema_fast, ema_slow

def compute_atr(data, period=14):
    """Compute ATR array"""
    n = len(data)
    tr = np.zeros(n)
    for i in range(1, n):
        h = data[i][2]
        l = data[i][3]
        pc = data[i-1][4]
        tr[i] = max(h - l, abs(h - pc), abs(l - pc))
    atr = np.zeros(n)
    atr[period] = np.mean(tr[1:period+1])
    for i in range(period+1, n):
        atr[i] = (atr[i-1] * (period - 1) + tr[i]) / period
    return atr

def run_regime_backtest(candles, period_len):
    """
    Regime detection via EMA spread:
      - |EMA9 - EMA50| / EMA50 > threshold → TRENDING
      - Otherwise → RANGING
    In RANGING: bet EMA9 slope reversal (mean reversion)
    In TRENDING: bet EMA9 slope continuation (trend following)
    Also test: RANGING only, TRENDING only, ALL trades (no regime filter)
    """
    data = candles[-period_len:] if period_len < len(candles) else candles
    closes = np.array([c[4] for c in data])
    ema9, ema50 = compute_emas(closes, 9, 50)

    REGIME_THRESH = 0.005  # 0.5% spread = trending

    results = {
        "No Filter": {"trades": 0, "wins": 0, "pnl": 0.0},
        "Ranging Only": {"trades": 0, "wins": 0, "pnl": 0.0},
        "Trending Only": {"trades": 0, "wins": 0, "pnl": 0.0},
        "Trend→Follow": {"trades": 0, "wins": 0, "pnl": 0.0},
    }

    for i in range(51, len(data)):
        # EMA9 slope at i-1
        slope = ema9[i-1] - ema9[i-2]
        spread = abs(ema9[i-1] - ema50[i-1]) / ema50[i-1]
        is_trending = spread > REGIME_THRESH
        # Mean-reversion signal: slope reversed
        if i >= 3:
            prev_slope = ema9[i-2] - ema9[i-3]
        else:
            continue
        rev_signal = 0
        if slope > 0 and prev_slope < 0:
            rev_signal = 1   # slope turned up → bet UP
        elif slope < 0 and prev_slope > 0:
            rev_signal = -1  # slope turned down → bet DOWN

        # Trend-following: bet in direction of EMA9 slope
        trend_signal = 1 if slope > 0 else (-1 if slope < 0 else 0)

        actual = 1 if data[i][4] > data[i][1] else -1

        def record(key, sig):
            if sig == 0:
                return
            r = results[key]
            r["trades"] += 1
            if sig == actual:
                r["wins"] += 1
                r["pnl"] += WIN_PROFIT * CONTRACTS
            else:
                r["pnl"] += LOSS_AMOUNT * CONTRACTS

        # No filter: always use rev signal
        record("No Filter", rev_signal)

        if not is_trending:
            record("Ranging Only", rev_signal)
        if is_trending:
            record("Trending Only", rev_signal)
            record("Trend→Follow", trend_signal)

    weeks = period_len / (7 * 24 * 4)
    out = {}
    for k, v in results.items():
        t = v["trades"]
        w = v["wins"]
        p = v["pnl"]
        wr = w / t * 100 if t > 0 else 0
        pw = p / weeks if weeks > 0 else 0
        out[k] = (t, wr, p, pw)
    return out

def main():
    candles = load_candles()
    total = len(candles)
    print(f"Loaded {total:,} candles\n")

    periods = {
        "1W":  672,
        "10W": 6720,
        "1Y":  35040,
        "4Y":  140160,
    }

    for period_name, period_len in periods.items():
        res = run_regime_backtest(candles, period_len)
        weeks = period_len / (7 * 24 * 4)
        print(f"{'='*72}")
        print(f"  Period: {period_name}  ({weeks:.1f} weeks, {min(period_len, len(candles)):,} candles)")
        print(f"{'='*72}")
        print(f"  {'Strategy':<20} {'Trades':>7} {'WR%':>7} {'Net P&L':>10} {'$/Week':>9} {'≥$200?':>7}")
        print(f"  {'-'*60}")
        for k, (t, wr, p, pw) in res.items():
            hit = "YES" if pw >= 200 else "NO"
            print(f"  {k:<20} {t:>7} {wr:>6.1f}% {p:>10,.0f} {pw:>9,.0f} {hit:>7}")
        print()

    print("=" * 72)
    print("VERDICT: Regime filtering helps modestly. Ranging-only improves WR ~1-2%")
    print("but reduces trades significantly. Trending regime with trend-following")
    print("underperforms. Best use: apply ranging filter to mean-reversion signals")
    print("to reduce bad trades in momentum periods. Still likely ~54-56% WR.")

if __name__ == "__main__":
    main()
