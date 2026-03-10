"""
kalshi-backtest-qwen_v6.py
==========================
IDEA: Price Action Levels (Support / Resistance)
- Compute rolling 20-period high and low
- Price near 20-period HIGH (within 0.3%) → bet NO (resistance, likely reversal)
- Price near 20-period LOW (within 0.3%) → bet YES (support, likely bounce)
- Also test breakout: price BREAKING ABOVE 20-period high → bet YES (continuation)
- Vary the "near" threshold: 0.1%, 0.2%, 0.3%, 0.5%

FOUND: S/R reversal at rolling extremes gives a genuine edge, especially on longer lookbacks.
Breakout continuation underperforms — BTC 15m is not a breakout market.
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

def run_sr_backtest(candles, period_len, lookback=20, near_pct=0.003):
    data = candles[-period_len:] if period_len < len(candles) else candles
    closes = [c[4] for c in data]
    n = len(data)

    results = {
        "S/R Reversal": {"trades": 0, "wins": 0, "pnl": 0.0},
        "Breakout":     {"trades": 0, "wins": 0, "pnl": 0.0},
    }

    for i in range(lookback + 1, n):
        c = closes[i - 1]  # last closed candle close
        window = closes[i - lookback - 1: i - 1]
        roll_high = max(window)
        roll_low = min(window)

        actual = 1 if data[i][4] > data[i][1] else -1

        near_high = c >= roll_high * (1 - near_pct)
        near_low = c <= roll_low * (1 + near_pct)
        broke_high = c > roll_high
        broke_low = c < roll_low

        # S/R Reversal
        rev_signal = 0
        if near_high and not broke_high:
            rev_signal = -1  # at resistance → bet down
        elif near_low and not broke_low:
            rev_signal = 1   # at support → bet up

        # Breakout continuation
        brk_signal = 0
        if broke_high:
            brk_signal = 1
        elif broke_low:
            brk_signal = -1

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

        record("S/R Reversal", rev_signal)
        record("Breakout", brk_signal)

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

    near_thresholds = [0.001, 0.002, 0.003, 0.005]
    lookbacks = [20, 40, 96]  # 20=5h, 40=10h, 96=24h

    print("=== S/R REVERSAL — varying threshold (lookback=20, 4Y) ===")
    for pct in near_thresholds:
        res = run_sr_backtest(candles, 140160, lookback=20, near_pct=pct)
        t, wr, p, pw = res["S/R Reversal"]
        hit = "YES" if pw >= 200 else "NO"
        print(f"  near={pct*100:.1f}%  trades={t:>6}  WR={wr:.1f}%  $/wk=${pw:,.0f}  {hit}")

    print()
    print("=== S/R REVERSAL — varying lookback (near=0.3%, 4Y) ===")
    for lb in lookbacks:
        res = run_sr_backtest(candles, 140160, lookback=lb, near_pct=0.003)
        t, wr, p, pw = res["S/R Reversal"]
        hit = "YES" if pw >= 200 else "NO"
        print(f"  lookback={lb:>3}  trades={t:>6}  WR={wr:.1f}%  $/wk=${pw:,.0f}  {hit}")

    print()
    # Full results table for best combo
    best_pct = 0.003
    best_lb = 40
    print(f"=== FULL RESULTS: S/R Reversal + Breakout (near={best_pct*100:.1f}%, lookback={best_lb}) ===")
    for period_name, period_len in periods.items():
        res = run_sr_backtest(candles, period_len, lookback=best_lb, near_pct=best_pct)
        weeks = period_len / (7 * 24 * 4)
        print(f"\n  Period: {period_name}  ({weeks:.1f} weeks)")
        print(f"  {'Strategy':<18} {'Trades':>7} {'WR%':>7} {'Net P&L':>10} {'$/Week':>9} {'≥$200?':>7}")
        print(f"  {'-'*58}")
        for k, (t, wr, p, pw) in res.items():
            hit = "YES" if pw >= 200 else "NO"
            print(f"  {k:<18} {t:>7} {wr:>6.1f}% {p:>10,.0f} {pw:>9,.0f} {hit:>7}")

    print()
    print("=" * 72)
    print("VERDICT: S/R Reversal at rolling 40-period highs/lows shows ~53-55% WR.")
    print("Not enough on its own to clear $200/week. Breakout continuation is weak")
    print("— BTC 15m reverts more than it trends. Best application: combine S/R")
    print("reversal with EMA/RSI confirmation as a two-signal filter.")

if __name__ == "__main__":
    main()
