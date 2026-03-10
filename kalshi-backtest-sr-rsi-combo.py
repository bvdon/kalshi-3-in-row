"""
kalshi-backtest-sr-rsi-combo.py
================================
IDEA: Combined S/R + RSI filter
- Only trade when BOTH signals agree:
    * RSI(14) < 40 (oversold)  AND price near rolling low  → bet YES
    * RSI(14) > 60 (overbought) AND price near rolling high → bet NO
- Hypothesis: requiring both conditions reduces noise and raises win rate
  vs either strategy alone, at the cost of fewer trades.

Also shows standalone RSI-only and S/R-only for direct comparison.
"""

import sqlite3
import numpy as np

DB_PATH   = "/Users/roberthenning/python-projects/kalshi-bot-v2/candles.db"
WIN_PROFIT  = 5.0
LOSS_AMOUNT = 5.0
CONTRACTS   = 10

# Parameters (best from individual backtests)
RSI_PERIOD = 14
RSI_OB     = 60
RSI_OS     = 40
SR_LOOKBACK = 40     # ~10 hours
SR_NEAR_PCT = 0.003  # 0.3%


def load_candles():
    conn = sqlite3.connect(DB_PATH)
    cur  = conn.cursor()
    cur.execute("SELECT ts, open, high, low, close FROM btc_candles ORDER BY ts ASC")
    rows = cur.fetchall()
    conn.close()
    return rows


def compute_rsi(closes, period=RSI_PERIOD):
    n      = len(closes)
    rsi    = np.full(n, 50.0)
    gains  = np.zeros(n)
    losses = np.zeros(n)
    for i in range(1, n):
        diff = closes[i] - closes[i - 1]
        if diff > 0:
            gains[i] = diff
        else:
            losses[i] = -diff
    avg_g = np.mean(gains[1:period + 1])
    avg_l = np.mean(losses[1:period + 1])
    for i in range(period, n):
        avg_g = (avg_g * (period - 1) + gains[i]) / period
        avg_l = (avg_l * (period - 1) + losses[i]) / period
        if avg_l == 0:
            rsi[i] = 100.0
        else:
            rs     = avg_g / avg_l
            rsi[i] = 100.0 - 100.0 / (1.0 + rs)
    return rsi


def run_backtest(candles, period_len):
    data   = candles[-period_len:] if period_len < len(candles) else candles
    closes = np.array([c[4] for c in data])
    n      = len(data)

    rsi_vals = compute_rsi(closes)

    results = {
        "RSI Only":    {"trades": 0, "wins": 0, "pnl": 0.0},
        "S/R Only":    {"trades": 0, "wins": 0, "pnl": 0.0},
        "SR+RSI Combo":{"trades": 0, "wins": 0, "pnl": 0.0},
    }

    # Re-fire guard per strategy (tracks last trigger index)
    last_trigger = {"RSI Only": -1, "S/R Only": -1, "SR+RSI Combo": -1}

    warmup = max(RSI_PERIOD, SR_LOOKBACK) + 1

    for i in range(warmup, n - 1):
        actual   = 1 if data[i + 1][4] > data[i + 1][1] else -1
        rsi_now  = rsi_vals[i]
        close_i  = closes[i]

        # ── Rolling S/R window (excludes current candle) ──────────────────
        window    = closes[i - SR_LOOKBACK: i]
        roll_high = float(np.max(window))
        roll_low  = float(np.min(window))

        near_high = close_i >= roll_high * (1 - SR_NEAR_PCT) and close_i <= roll_high
        near_low  = close_i <= roll_low  * (1 + SR_NEAR_PCT) and close_i >= roll_low

        # ── Individual signals ─────────────────────────────────────────────
        rsi_signal = 0
        if rsi_now > RSI_OB:
            rsi_signal = -1
        elif rsi_now < RSI_OS:
            rsi_signal = 1

        sr_signal = 0
        if near_high:
            sr_signal = -1
        elif near_low:
            sr_signal = 1

        # ── Combo: both must agree ─────────────────────────────────────────
        combo_signal = rsi_signal if (rsi_signal != 0 and rsi_signal == sr_signal) else 0

        def record(key, sig):
            if sig == 0:
                return
            # Re-fire guard: skip if same candle as last trigger
            if i <= last_trigger[key]:
                return
            last_trigger[key] = i
            r = results[key]
            r["trades"] += 1
            if sig == actual:
                r["wins"] += 1
                r["pnl"] += WIN_PROFIT * CONTRACTS
            else:
                r["pnl"] -= LOSS_AMOUNT * CONTRACTS

        record("RSI Only",     rsi_signal)
        record("S/R Only",     sr_signal)
        record("SR+RSI Combo", combo_signal)

    weeks = period_len / (7 * 24 * 4)
    out   = {}
    for k, v in results.items():
        t  = v["trades"]
        w  = v["wins"]
        p  = v["pnl"]
        wr = w / t * 100 if t > 0 else 0
        pw = p / weeks if weeks > 0 else 0
        out[k] = (t, wr, p, pw)
    return out


def main():
    candles = load_candles()
    print(f"Loaded {len(candles):,} candles\n")
    print(f"RSI({RSI_PERIOD}) OB={RSI_OB} OS={RSI_OS} | S/R lookback={SR_LOOKBACK} near={SR_NEAR_PCT*100:.1f}%\n")

    periods = {
        "1W":  672,
        "10W": 6720,
        "1Y":  35040,
        "4Y":  140160,
    }

    for period_name, period_len in periods.items():
        res   = run_backtest(candles, period_len)
        weeks = period_len / (7 * 24 * 4)
        actual_candles = min(period_len, len(candles))

        print(f"{'='*72}")
        print(f"  Period: {period_name}  ({weeks:.1f} weeks, {actual_candles:,} candles)")
        print(f"{'='*72}")
        print(f"  {'Strategy':<18} {'Trades':>7} {'WR%':>7} {'Net P&L':>11} {'$/Week':>9} {'≥$200?':>7}")
        print(f"  {'-'*62}")
        for k, (t, wr, p, pw) in res.items():
            hit = "YES" if pw >= 200 else "NO"
            print(f"  {k:<18} {t:>7} {wr:>6.1f}% {p:>11,.0f} {pw:>9,.0f} {hit:>7}")
        print()

    print("=" * 72)
    print("VERDICT:")
    print("  SR+RSI Combo trades far less than either alone — only fires when")
    print("  RSI is at an extreme AND price is near a rolling S/R level.")
    print("  Watch for: higher WR% vs standalone, lower trade count.")
    print("  If WR >= 54-55% the combo may outperform per-trade even if")
    print("  total P&L is lower due to fewer entries.")


if __name__ == "__main__":
    main()
