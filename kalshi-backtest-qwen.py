"""
kalshi-backtest-qwen.py — Simple indicator backtester for BTC 15m Kalshi markets.

Rule: signal is based on indicators computed through candle (i-1) to predict
      direction of candle i. No lookahead bias.

Strategies tested:
  A: RSI(14) mean reversion  — <30=YES, >70=NO
  B: RSI(14) trend-following — >60=YES, <40=NO
  C: RSI(14) trend reversed  — >60=NO,  <40=YES
  D: EMA9/21 crossover       — golden=YES, death=NO
  E: EMA9/21 crossover rev   — contrarian
  F: Price vs EMA20          — above=YES, below=NO
  G: Price vs EMA20 rev      — above=NO,  below=YES
  H: EMA9 slope              — rising=YES, falling=NO
  I: EMA9 slope rev          — rising=NO,  falling=YES

Periods: 1W, 10W, 1Y, 4Y
Sizing:  10 contracts @ $0.50 entry | $1.00 payout
Goal:    $200+/week net P&L

"Last" = close of last fully closed candle before current cycle.
Candle source: local candles.db
"""

import sqlite3
from pathlib import Path
from datetime import datetime, timezone, timedelta

CANDLES_DB  = Path(__file__).parent / "candles.db"
EST         = timezone(timedelta(hours=-5))
CONTRACTS   = 10
ENTRY_PRICE = 0.50   # cost per contract
PAYOUT      = 1.00   # payout per contract on win
WIN_PER_TRADE  = CONTRACTS * (PAYOUT - ENTRY_PRICE)   # +$5
LOSS_PER_TRADE = CONTRACTS * ENTRY_PRICE               # -$5

PERIODS = [
    ("1W",   7 * 24 * 4),        #    672 candles
    ("10W",  70 * 24 * 4),       #  6,720 candles
    ("1Y",   365 * 24 * 4),      # 35,040 candles
    ("4Y",   4 * 365 * 24 * 4),  # 140,160 candles
]


# ── Indicators ────────────────────────────────────────────────────────────────

def compute_ema(values: list, period: int) -> list:
    """EMA array, same length as values. None during warm-up."""
    out = [None] * (period - 1)
    if len(values) < period:
        return out + [None] * (len(values) - (period - 1))
    seed = sum(values[:period]) / period
    out.append(seed)
    k = 2.0 / (period + 1)
    prev = seed
    for v in values[period:]:
        prev = prev * (1 - k) + v * k
        out.append(prev)
    return out


def compute_rsi(values: list, period: int = 14) -> list:
    """RSI array, same length as values. None during warm-up."""
    out = [None] * period
    if len(values) <= period:
        return out + [None] * (len(values) - period)
    # seed with SMA of first `period` moves
    diffs = [values[i] - values[i - 1] for i in range(1, period + 1)]
    avg_g = sum(max(d, 0) for d in diffs) / period
    avg_l = sum(max(-d, 0) for d in diffs) / period

    def _rsi(ag, al):
        return 100.0 if al == 0 else 100.0 - 100.0 / (1.0 + ag / al)

    out.append(_rsi(avg_g, avg_l))
    for i in range(period + 1, len(values)):
        d = values[i] - values[i - 1]
        g = max(d, 0)
        l = max(-d, 0)
        avg_g = (avg_g * (period - 1) + g) / period
        avg_l = (avg_l * (period - 1) + l) / period
        out.append(_rsi(avg_g, avg_l))
    return out


# ── Load candles ──────────────────────────────────────────────────────────────

def load_candles() -> list:
    conn = sqlite3.connect(CANDLES_DB)
    rows = conn.execute(
        "SELECT ts, open, close FROM btc_candles ORDER BY ts ASC"
    ).fetchall()
    conn.close()
    return [{"ts": r[0], "open": r[1], "close": r[2]} for r in rows]


# ── Strategy signal functions ─────────────────────────────────────────────────
# Each takes (indicators dict, index i) → "YES", "NO", or None

def sig_rsi_mean(ind, i):
    v = ind["rsi14"][i]
    if v is None: return None
    if v < 30:   return "YES"
    if v > 70:   return "NO"
    return None

def sig_rsi_trend(ind, i):
    v = ind["rsi14"][i]
    if v is None: return None
    if v > 60:   return "YES"
    if v < 40:   return "NO"
    return None

def sig_rsi_trend_rev(ind, i):
    v = ind["rsi14"][i]
    if v is None: return None
    if v > 60:   return "NO"
    if v < 40:   return "YES"
    return None

def sig_ema_cross(ind, i):
    if i < 1: return None
    e9n, e9p   = ind["ema9"][i],  ind["ema9"][i - 1]
    e21n, e21p = ind["ema21"][i], ind["ema21"][i - 1]
    if None in (e9n, e9p, e21n, e21p): return None
    if e9p <= e21p and e9n > e21n:  return "YES"
    if e9p >= e21p and e9n < e21n:  return "NO"
    return None

def sig_ema_cross_rev(ind, i):
    s = sig_ema_cross(ind, i)
    return None if s is None else ("NO" if s == "YES" else "YES")

def sig_price_ema20(ind, i):
    e = ind["ema20"][i]
    p = ind["close"][i]
    if e is None: return None
    if p > e: return "YES"
    if p < e: return "NO"
    return None

def sig_price_ema20_rev(ind, i):
    s = sig_price_ema20(ind, i)
    return None if s is None else ("NO" if s == "YES" else "YES")

def sig_ema9_slope(ind, i):
    if i < 1: return None
    n, p = ind["ema9"][i], ind["ema9"][i - 1]
    if n is None or p is None: return None
    if n > p: return "YES"
    if n < p: return "NO"
    return None

def sig_ema9_slope_rev(ind, i):
    s = sig_ema9_slope(ind, i)
    return None if s is None else ("NO" if s == "YES" else "YES")

STRATEGIES = [
    ("A: RSI<30=YES >70=NO  (mean rev)",  sig_rsi_mean),
    ("B: RSI>60=YES <40=NO  (trend)",     sig_rsi_trend),
    ("C: RSI>60=NO  <40=YES (rev trend)", sig_rsi_trend_rev),
    ("D: EMA9x21 cross",                  sig_ema_cross),
    ("E: EMA9x21 cross (reversed)",       sig_ema_cross_rev),
    ("F: Price > EMA20 = YES",            sig_price_ema20),
    ("G: Price > EMA20 = NO  (rev)",      sig_price_ema20_rev),
    ("H: EMA9 slope up = YES",            sig_ema9_slope),
    ("I: EMA9 slope up = NO  (rev)",      sig_ema9_slope_rev),
]


# ── Backtest engine ───────────────────────────────────────────────────────────

def backtest(candles, indicators, n_candles, strat_fn):
    total_c = len(candles)
    start   = max(0, total_c - n_candles)
    subset  = candles[start:]

    wins = losses = skipped = 0
    net = 0.0

    for j, c in enumerate(subset):
        gi = start + j          # global index of current candle (to predict)
        si = gi - 1             # signal index = last closed candle

        if si < 0:
            skipped += 1
            continue

        side = strat_fn(indicators, si)
        if side is None:
            skipped += 1
            continue

        if c["close"] == c["open"]:   # doji — skip
            skipped += 1
            continue

        actual = "YES" if c["close"] > c["open"] else "NO"

        if side == actual:
            net += WIN_PER_TRADE
            wins += 1
        else:
            net -= LOSS_PER_TRADE
            losses += 1

    total = wins + losses
    wr    = wins / total * 100 if total else 0.0

    # Weeks spanned by subset
    if len(subset) > 1:
        weeks = (subset[-1]["ts"] - subset[0]["ts"]) / (7 * 24 * 3600)
    else:
        weeks = 1.0

    weekly = net / weeks if weeks > 0 else 0.0
    return dict(wins=wins, losses=losses, skipped=skipped,
                total=total, wr=wr, net=net, weeks=weeks, weekly=weekly)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("Loading candles...", flush=True)
    candles = load_candles()
    candles = candles[:-1]   # drop in-progress candle

    closes = [c["close"] for c in candles]

    print("Precomputing indicators...", flush=True)
    ind = {
        "close": closes,
        "rsi14": compute_rsi(closes, 14),
        "ema9":  compute_ema(closes, 9),
        "ema20": compute_ema(closes, 20),
        "ema21": compute_ema(closes, 21),
    }

    first_dt = datetime.fromtimestamp(candles[0]["ts"],  tz=EST).strftime("%Y-%m-%d")
    last_dt  = datetime.fromtimestamp(candles[-1]["ts"], tz=EST).strftime("%Y-%m-%d")

    print(f"\n{'='*108}")
    print(f"  kalshi-backtest-qwen.py — BTC 15m Kalshi Backtest")
    print(f"  DB: {len(candles):,} candles  ({first_dt} → {last_dt})")
    print(f"  Sizing: {CONTRACTS} contracts @ ${ENTRY_PRICE:.2f}  |  win=+${WIN_PER_TRADE:.2f}  loss=-${LOSS_PER_TRADE:.2f}  per trade")
    print(f"  Goal: $200+/week net P&L")
    print(f"{'='*108}\n")

    all_results = {}

    W = 33   # strategy column width

    for period_label, n_candles in PERIODS:
        n_actual = min(n_candles, len(candles))
        span_dt  = datetime.fromtimestamp(candles[-(n_actual)]["ts"], tz=EST).strftime("%Y-%m-%d")

        print(f"┌── {period_label}  ({n_actual:,} candles | {span_dt} → {last_dt})")
        print(f"│  {'Strategy':<{W}} {'Trades':>8} {'Win%':>7}  {'Net P&L':>11}  {'$/Week':>9}  {'$200?':>6}")
        print(f"│  {'-'*(W+52)}")

        for name, fn in STRATEGIES:
            r = backtest(candles, ind, n_candles, fn)
            sign  = "+" if r["net"]    >= 0 else ""
            wsign = "+" if r["weekly"] >= 0 else ""
            hit   = "✓" if r["weekly"] >= 200 else "✗"
            print(f"│  {name:<{W}} {r['total']:>8,} {r['wr']:>6.1f}%  "
                  f"{sign}${r['net']:>9,.2f}  {wsign}${r['weekly']:>7,.2f}  {hit:>6}")
            all_results[(name, period_label)] = r

        print()

    # ── Top performers ────────────────────────────────────────────────────────
    print(f"{'='*108}")
    print(f"  TOP PERFORMERS  (sorted by $/week, all periods)")
    print(f"  {'Strategy':<{W}} {'Period':<5} {'Trades':>8} {'Win%':>7}  {'Net P&L':>11}  {'$/Week':>9}  {'$200?':>6}")
    print(f"  {'-'*(W+52)}")
    top = sorted(all_results.items(), key=lambda x: x[1]["weekly"], reverse=True)
    for (name, period), r in top[:12]:
        sign  = "+" if r["net"]    >= 0 else ""
        wsign = "+" if r["weekly"] >= 0 else ""
        hit   = "✓" if r["weekly"] >= 200 else "✗"
        print(f"  {name:<{W}} {period:<5} {r['total']:>8,} {r['wr']:>6.1f}%  "
              f"{sign}${r['net']:>9,.2f}  {wsign}${r['weekly']:>7,.2f}  {hit:>6}")

    print(f"\n{'='*108}")
    # Winners
    winners = [(k, v) for k, v in all_results.items() if v["weekly"] >= 200]
    if winners:
        print(f"\n  🎯 Strategies hitting $200+/week: {len(winners)}")
        for (name, period), r in sorted(winners, key=lambda x: x[1]["weekly"], reverse=True):
            print(f"     {name}  [{period}]  $/week: +${r['weekly']:,.2f}  win rate: {r['wr']:.1f}%")
    else:
        print(f"\n  ✗ No strategy hit $200/week with flat {CONTRACTS} contracts.")
        print(f"    Best performer:")
        best = top[0]
        print(f"    → {best[0][0]}  [{best[0][1]}]  $/week: ${best[1]['weekly']:,.2f}  win rate: {best[1]['wr']:.1f}%")

    print(f"\n{'='*108}\n")


if __name__ == "__main__":
    main()
