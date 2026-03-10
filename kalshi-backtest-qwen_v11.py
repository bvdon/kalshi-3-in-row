"""
kalshi-backtest-qwen_v11.py
============================
RSI Threshold Parameter Sweep

Tests all combinations of RSI_LONG_MAX and RSI_SHORT_MIN from the v10 Kitchen Sink.
All other parameters are identical to v10.

Grid:
  RSI_LONG_MAX  in [40, 42, 44, 45, 47, 50]
  RSI_SHORT_MIN in [50, 53, 55, 58, 60]

Evaluated on 1Y and 4Y lookback periods.
Reports: trades/week, WR%, net $/week
"""

import sqlite3
import numpy as np
from itertools import product

DB_PATH = "/Users/roberthenning/python-projects/kalshi-bot-v2/candles.db"
WIN_PROFIT    = 5.0
LOSS_AMOUNT   = -5.0
BASE_CONTRACTS = 10
MAX_CONTRACTS  = 25

RSI_LONG_OPTIONS  = [40, 42, 44, 45, 47, 50]
RSI_SHORT_OPTIONS = [50, 53, 55, 58, 60]

PERIODS = {
    "1Y":  35040,
    "4Y":  140160,
}

# v10 fixed params
LOOKBACK      = 40
BIG_MOVE_PCT  = 0.005
NEAR_PCT      = 0.003
REGIME_THRESH = 0.005


def load_candles():
    conn = sqlite3.connect(DB_PATH)
    cur = conn.cursor()
    cur.execute("SELECT ts, open, high, low, close FROM btc_candles ORDER BY ts ASC")
    rows = cur.fetchall()
    conn.close()
    return rows


def compute_ema(closes, period):
    n = len(closes)
    ema = np.zeros(n)
    k = 2 / (period + 1)
    ema[0] = closes[0]
    for i in range(1, n):
        ema[i] = closes[i] * k + ema[i - 1] * (1 - k)
    return ema


def compute_rsi(closes, period=14):
    n = len(closes)
    rsi = np.full(n, 50.0)
    gains = np.zeros(n)
    losses = np.zeros(n)
    for i in range(1, n):
        diff = closes[i] - closes[i - 1]
        if diff > 0:
            gains[i] = diff
        else:
            losses[i] = -diff
    ag = np.mean(gains[1:period + 1]) if period < n else 0
    al = np.mean(losses[1:period + 1]) if period < n else 0
    for i in range(period, n):
        ag = (ag * (period - 1) + gains[i]) / period
        al = (al * (period - 1) + losses[i]) / period
        rsi[i] = 100.0 if al == 0 else 100 - 100 / (1 + ag / al)
    return rsi


def compute_atr(data, period=14):
    n = len(data)
    tr = np.zeros(n)
    for i in range(1, n):
        h, l, pc = data[i][2], data[i][3], data[i - 1][4]
        tr[i] = max(h - l, abs(h - pc), abs(l - pc))
    atr = np.zeros(n)
    if period < n:
        atr[period] = np.mean(tr[1:period + 1])
        for i in range(period + 1, n):
            atr[i] = (atr[i - 1] * (period - 1) + tr[i]) / period
    return atr


def run_sweep(candles, period_len, rsi_long_max, rsi_short_min):
    data = candles[-period_len:] if period_len < len(candles) else candles
    closes = np.array([c[4] for c in data])
    ema9   = compute_ema(closes, 9)
    ema50  = compute_ema(closes, 50)
    rsi    = compute_rsi(closes, 14)
    atr    = compute_atr(data, 14)
    n      = len(data)

    valid_atr   = atr[atr > 0]
    median_atr  = np.median(valid_atr) if len(valid_atr) > 0 else 1.0

    trades = 0
    wins   = 0
    pnl    = 0.0

    for i in range(max(LOOKBACK + 3, 51), n):
        actual = 1 if data[i][4] > data[i][1] else -1

        # Signal 1: EMA9 slope reversal
        s1 = ema9[i - 1] - ema9[i - 2]
        s0 = ema9[i - 2] - ema9[i - 3]
        ema_sig = 0
        if s1 > 0 and s0 < 0:
            ema_sig = 1
        elif s1 < 0 and s0 > 0:
            ema_sig = -1

        # Signal 2: Big-move reversal
        big_sig = 0
        prev_o, prev_c = data[i - 1][1], data[i - 1][4]
        if prev_o > 0:
            move = abs(prev_c - prev_o) / prev_o
            if move >= BIG_MOVE_PCT:
                big_sig = -1 if prev_c > prev_o else 1

        # Signal 3: S/R proximity reversal
        c      = closes[i - 1]
        window = closes[i - LOOKBACK - 1: i - 1]
        roll_high = max(window)
        roll_low  = min(window)
        sr_sig = 0
        if c >= roll_high * (1 - NEAR_PCT) and c <= roll_high:
            sr_sig = -1
        elif c <= roll_low * (1 + NEAR_PCT) and c >= roll_low:
            sr_sig = 1

        # Confluence 2-of-3
        signals  = [ema_sig, big_sig, sr_sig]
        non_zero = [s for s in signals if s != 0]
        if not non_zero:
            continue
        up  = sum(1 for s in non_zero if s == 1)
        dn  = sum(1 for s in non_zero if s == -1)
        count    = max(up, dn)
        majority = 1 if up > dn else (-1 if dn > up else 0)

        if count < 2 or majority == 0:
            continue

        # Regime filter
        spread     = abs(ema9[i - 1] - ema50[i - 1]) / ema50[i - 1] if ema50[i - 1] > 0 else 1.0
        is_ranging = spread < REGIME_THRESH
        if not is_ranging:
            continue

        # RSI filter (parametric)
        rsi_now = rsi[i - 1]
        rsi_ok  = (majority == 1  and rsi_now < rsi_long_max) or \
                  (majority == -1 and rsi_now > rsi_short_min)
        if not rsi_ok:
            continue

        # ATR sizing
        cur_atr = atr[i - 1]
        if cur_atr > 0:
            dyn_c = int(max(1, min(MAX_CONTRACTS, round(BASE_CONTRACTS * (median_atr / cur_atr)))))
        else:
            dyn_c = BASE_CONTRACTS

        trades += 1
        if majority == actual:
            wins += 1
            pnl  += WIN_PROFIT * dyn_c
        else:
            pnl  += LOSS_AMOUNT * dyn_c

    weeks    = period_len / (7 * 24 * 4)
    wr       = wins / trades * 100 if trades > 0 else 0.0
    trades_w = trades / weeks if weeks > 0 else 0.0
    pnl_w    = pnl / weeks    if weeks > 0 else 0.0

    return trades, trades_w, wr, pnl, pnl_w


def main():
    print("Loading candles...")
    candles = load_candles()
    print(f"Loaded {len(candles):,} candles\n")

    combos = list(product(RSI_LONG_OPTIONS, RSI_SHORT_OPTIONS))
    print(f"Testing {len(combos)} RSI threshold combinations × {len(PERIODS)} periods\n")

    # Store results: {period -> list of (long_max, short_min, trades, trades/wk, wr, pnl, $/wk)}
    all_results = {p: [] for p in PERIODS}

    for rsi_long_max, rsi_short_min in combos:
        for period_name, period_len in PERIODS.items():
            trades, trades_w, wr, pnl, pnl_w = run_sweep(
                candles, period_len, rsi_long_max, rsi_short_min
            )
            all_results[period_name].append(
                (rsi_long_max, rsi_short_min, trades, trades_w, wr, pnl, pnl_w)
            )

    # Print results per period
    for period_name, rows in all_results.items():
        print(f"{'='*80}")
        print(f"  Period: {period_name}")
        print(f"{'='*80}")
        print(f"  {'RSI_L':>5} {'RSI_S':>5} {'Trades':>7} {'T/Wk':>6} {'WR%':>7} {'Net P&L':>10} {'$/Wk':>8} {'≥$200?':>7} {'≥5T/w?':>7}")
        print(f"  {'-'*72}")

        # Sort by $/week descending
        rows_sorted = sorted(rows, key=lambda r: r[6], reverse=True)
        for (long_max, short_min, trades, trades_w, wr, pnl, pnl_w) in rows_sorted:
            hit200  = "YES" if pnl_w >= 200 else "NO"
            hit5tw  = "YES" if trades_w >= 5  else "NO"
            print(f"  {long_max:>5} {short_min:>5} {trades:>7} {trades_w:>6.1f} {wr:>6.1f}% {pnl:>10,.0f} {pnl_w:>8,.0f} {hit200:>7} {hit5tw:>7}")

        print()

        # Highlight best within ≥5 trades/week constraint
        qualified = [(r) for r in rows_sorted if r[3] >= 5]
        if qualified:
            best = qualified[0]
            print(f"  *** BEST (≥5 trades/wk): RSI_LONG_MAX={best[0]}, RSI_SHORT_MIN={best[1]}")
            print(f"      {best[3]:.1f} trades/wk | {best[4]:.1f}% WR | ${best[6]:,.0f}/wk")
        print()


if __name__ == "__main__":
    main()
