"""
kalshi-backtest-qwen2.py — Combined signal + scaling study.

Findings from v1:
  - Contrarian beats trend at 15m timeframe (52-54% win rate)
  - Best single signals: EMA9 slope reversed, Price>EMA20 reversed, RSI rev-trend
  - Need ~57%+ win rate OR more contracts to hit $200/week with flat sizing

This version:
  1. Combines top signals as filters (both must agree)
  2. Tests 15 / 20 / 30 contracts (instead of 10)
  3. Tries RSI extreme zones (e.g. <20 / >80 for higher conviction)
  4. Adds anti-martingale: win streak doubles contracts (up to 4x)

Candle source: local candles.db
"""

import sqlite3
from pathlib import Path
from datetime import datetime, timezone, timedelta

CANDLES_DB  = Path(__file__).parent / "candles.db"
EST         = timezone(timedelta(hours=-5))
ENTRY_PRICE = 0.50
PAYOUT      = 1.00

PERIODS = [
    ("1W",   7 * 24 * 4),
    ("10W",  70 * 24 * 4),
    ("1Y",   365 * 24 * 4),
    ("4Y",   4 * 365 * 24 * 4),
]


# ── Indicators ────────────────────────────────────────────────────────────────

def compute_ema(values, period):
    out = [None] * (period - 1)
    if len(values) < period:
        return out + [None] * (len(values) - len(out))
    seed = sum(values[:period]) / period
    out.append(seed)
    k = 2.0 / (period + 1)
    prev = seed
    for v in values[period:]:
        prev = prev * (1 - k) + v * k
        out.append(prev)
    return out


def compute_rsi(values, period=14):
    out = [None] * period
    if len(values) <= period:
        return out + [None] * (len(values) - period)
    diffs = [values[i] - values[i - 1] for i in range(1, period + 1)]
    avg_g = sum(max(d, 0) for d in diffs) / period
    avg_l = sum(max(-d, 0) for d in diffs) / period

    def _r(ag, al):
        return 100.0 if al == 0 else 100.0 - 100.0 / (1.0 + ag / al)

    out.append(_r(avg_g, avg_l))
    for i in range(period + 1, len(values)):
        d = values[i] - values[i - 1]
        avg_g = (avg_g * (period - 1) + max(d, 0))  / period
        avg_l = (avg_l * (period - 1) + max(-d, 0)) / period
        out.append(_r(avg_g, avg_l))
    return out


def load_candles():
    conn = sqlite3.connect(CANDLES_DB)
    rows = conn.execute(
        "SELECT ts, open, close FROM btc_candles ORDER BY ts ASC"
    ).fetchall()
    conn.close()
    return [{"ts": r[0], "open": r[1], "close": r[2]} for r in rows]


# ── Signal library ────────────────────────────────────────────────────────────

def ema9_slope_rev(ind, i):
    """EMA9 slope reversed — bet against current momentum"""
    if i < 1: return None
    n, p = ind["ema9"][i], ind["ema9"][i - 1]
    if n is None or p is None: return None
    if n < p: return "YES"   # slope down → bet YES (reversal)
    if n > p: return "NO"    # slope up   → bet NO  (reversal)
    return None

def price_ema20_rev(ind, i):
    """Price vs EMA20 reversed"""
    e = ind["ema20"][i]
    p = ind["close"][i]
    if e is None: return None
    if p < e: return "YES"   # below EMA → bet YES
    if p > e: return "NO"    # above EMA → bet NO
    return None

def rsi_rev_trend(ind, i):
    """RSI rev trend — overbought→NO, oversold→YES"""
    v = ind["rsi14"][i]
    if v is None: return None
    if v < 40: return "YES"
    if v > 60: return "NO"
    return None

def rsi_extreme_rev(ind, i, low=25, high=75):
    """RSI extreme zones — higher conviction"""
    v = ind["rsi14"][i]
    if v is None: return None
    if v < low:  return "YES"
    if v > high: return "NO"
    return None

def rsi_20_80(ind, i):
    return rsi_extreme_rev(ind, i, 20, 80)

def rsi_25_75(ind, i):
    return rsi_extreme_rev(ind, i, 25, 75)

def rsi_30_70(ind, i):
    return rsi_extreme_rev(ind, i, 30, 70)  # same as v1 strategy A

# ── Combined signal filters ────────────────────────────────────────────────────

def combined_ema9_rsi(ind, i):
    """Both EMA9-slope-rev AND RSI-rev-trend must agree"""
    a = ema9_slope_rev(ind, i)
    b = rsi_rev_trend(ind, i)
    if a is None or b is None or a != b: return None
    return a

def combined_ema20_rsi(ind, i):
    """Price-EMA20-rev AND RSI-rev-trend must agree"""
    a = price_ema20_rev(ind, i)
    b = rsi_rev_trend(ind, i)
    if a is None or b is None or a != b: return None
    return a

def combined_ema9_ema20(ind, i):
    """EMA9-slope-rev AND price-EMA20-rev must agree"""
    a = ema9_slope_rev(ind, i)
    b = price_ema20_rev(ind, i)
    if a is None or b is None or a != b: return None
    return a

def combined_all_three(ind, i):
    """All three must agree"""
    a = ema9_slope_rev(ind, i)
    b = price_ema20_rev(ind, i)
    c = rsi_rev_trend(ind, i)
    if a is None or b is None or c is None: return None
    if a == b == c: return a
    return None


# ── Backtest engine ───────────────────────────────────────────────────────────

def backtest_flat(candles, indicators, n_candles, strat_fn, contracts):
    """Flat sizing backtest."""
    start  = max(0, len(candles) - n_candles)
    subset = candles[start:]
    wins = losses = skipped = 0
    net = 0.0
    for j, c in enumerate(subset):
        gi = start + j
        si = gi - 1
        if si < 0: skipped += 1; continue
        side = strat_fn(indicators, si)
        if side is None: skipped += 1; continue
        if c["close"] == c["open"]: skipped += 1; continue
        actual = "YES" if c["close"] > c["open"] else "NO"
        if side == actual:
            net += contracts * (PAYOUT - ENTRY_PRICE)
            wins += 1
        else:
            net -= contracts * ENTRY_PRICE
            losses += 1
    total = wins + losses
    wr    = wins / total * 100 if total else 0.0
    weeks = (subset[-1]["ts"] - subset[0]["ts"]) / (7*24*3600) if len(subset) > 1 else 1.0
    return dict(wins=wins, losses=losses, skipped=skipped, total=total,
                wr=wr, net=net, weekly=net/weeks if weeks > 0 else 0)


def backtest_anti_mg(candles, indicators, n_candles, strat_fn, base=10, max_rounds=4):
    """Anti-martingale: win→double (up to max_rounds), loss→reset to base."""
    start  = max(0, len(candles) - n_candles)
    subset = candles[start:]
    wins = losses = skipped = 0
    net = 0.0
    ladder = [base * (2 ** r) for r in range(max_rounds + 1)]
    rnd = 0
    for j, c in enumerate(subset):
        gi = start + j
        si = gi - 1
        if si < 0: skipped += 1; continue
        side = strat_fn(indicators, si)
        if side is None: skipped += 1; continue
        if c["close"] == c["open"]: skipped += 1; continue
        actual = "YES" if c["close"] > c["open"] else "NO"
        k = ladder[rnd]
        if side == actual:
            net += k * (PAYOUT - ENTRY_PRICE)
            wins += 1
            rnd = min(rnd + 1, max_rounds)
        else:
            net -= k * ENTRY_PRICE
            losses += 1
            rnd = 0
    total = wins + losses
    wr    = wins / total * 100 if total else 0.0
    weeks = (subset[-1]["ts"] - subset[0]["ts"]) / (7*24*3600) if len(subset) > 1 else 1.0
    return dict(wins=wins, losses=losses, skipped=skipped, total=total,
                wr=wr, net=net, weekly=net/weeks if weeks > 0 else 0)


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("Loading & computing...", flush=True)
    candles = load_candles()[:-1]
    closes  = [c["close"] for c in candles]
    ind = {
        "close": closes,
        "rsi14": compute_rsi(closes, 14),
        "ema9":  compute_ema(closes, 9),
        "ema20": compute_ema(closes, 20),
    }

    first_dt = datetime.fromtimestamp(candles[0]["ts"],  tz=EST).strftime("%Y-%m-%d")
    last_dt  = datetime.fromtimestamp(candles[-1]["ts"], tz=EST).strftime("%Y-%m-%d")

    print(f"\n{'='*115}")
    print(f"  kalshi-backtest-qwen2.py — Combined Signals + Scaling")
    print(f"  DB: {len(candles):,} candles ({first_dt} → {last_dt})")
    print(f"  Entry: ${ENTRY_PRICE:.2f} | Payout: ${PAYOUT:.2f} | Goal: $200+/week")
    print(f"{'='*115}\n")

    # ── Section 1: Flat sizing, vary contracts ────────────────────────────────
    print("━━━ SECTION 1: FLAT SIZING — vary contracts (single signals) ━━━\n")

    single_signals = [
        ("EMA9-slope-rev",   ema9_slope_rev),
        ("Price<EMA20=YES",  price_ema20_rev),
        ("RSI-rev-trend",    rsi_rev_trend),
        ("RSI<30>70 rev",    rsi_30_70),
        ("RSI<25>75 rev",    rsi_25_75),
        ("RSI<20>80 rev",    rsi_20_80),
    ]

    for period_label, n_candles in PERIODS:
        n_actual = min(n_candles, len(candles))
        span = datetime.fromtimestamp(candles[-n_actual]["ts"], tz=EST).strftime("%Y-%m-%d")
        print(f"  ── {period_label} ({span} → {last_dt})")
        print(f"  {'Signal':<22} {'K':>4}  {'Trades':>8}  {'Win%':>6}  {'$/Week':>10}  {'$200?':>6}")
        print(f"  {'-'*65}")
        for sig_name, sig_fn in single_signals:
            for k in [10, 15, 20, 30]:
                r = backtest_flat(candles, ind, n_candles, sig_fn, k)
                hit = "✓" if r["weekly"] >= 200 else ""
                wsign = "+" if r["weekly"] >= 0 else ""
                print(f"  {sig_name:<22} {k:>4}  {r['total']:>8,}  {r['wr']:>5.1f}%  "
                      f"{wsign}${r['weekly']:>8,.2f}  {hit:>6}")
        print()

    # ── Section 2: Combined signal filters, flat 10 contracts ─────────────────
    print("━━━ SECTION 2: COMBINED SIGNAL FILTERS (10 contracts flat) ━━━\n")

    combined_signals = [
        ("EMA9 + RSI agree",          combined_ema9_rsi),
        ("EMA20 + RSI agree",         combined_ema20_rsi),
        ("EMA9 + EMA20 agree",        combined_ema9_ema20),
        ("All 3 agree",               combined_all_three),
    ]

    for period_label, n_candles in PERIODS:
        n_actual = min(n_candles, len(candles))
        span = datetime.fromtimestamp(candles[-n_actual]["ts"], tz=EST).strftime("%Y-%m-%d")
        print(f"  ── {period_label} ({span} → {last_dt})")
        print(f"  {'Signal combo':<28}  {'Trades':>8}  {'Win%':>6}  {'Net P&L':>11}  {'$/Week':>10}  {'$200?':>6}")
        print(f"  {'-'*80}")
        for sig_name, sig_fn in combined_signals:
            r = backtest_flat(candles, ind, n_candles, sig_fn, 10)
            hit = "✓" if r["weekly"] >= 200 else ""
            sign  = "+" if r["net"]    >= 0 else ""
            wsign = "+" if r["weekly"] >= 0 else ""
            print(f"  {sig_name:<28}  {r['total']:>8,}  {r['wr']:>5.1f}%  "
                  f"{sign}${r['net']:>9,.2f}  {wsign}${r['weekly']:>8,.2f}  {hit:>6}")
        print()

    # ── Section 3: Anti-Martingale on best signals ────────────────────────────
    print("━━━ SECTION 3: ANTI-MARTINGALE (base=10, up to 4 doublings → max 160 contracts) ━━━\n")
    print("  Win streak ladder: 10→20→40→80→160 contracts, reset on any loss\n")

    amg_signals = [
        ("EMA9-slope-rev",    ema9_slope_rev),
        ("RSI-rev-trend",     rsi_rev_trend),
        ("EMA9 + RSI agree",  combined_ema9_rsi),
        ("All 3 agree",       combined_all_three),
    ]

    for period_label, n_candles in PERIODS:
        n_actual = min(n_candles, len(candles))
        span = datetime.fromtimestamp(candles[-n_actual]["ts"], tz=EST).strftime("%Y-%m-%d")
        print(f"  ── {period_label} ({span} → {last_dt})")
        print(f"  {'Signal':<24}  {'Trades':>8}  {'Win%':>6}  {'Net P&L':>12}  {'$/Week':>10}  {'$200?':>6}")
        print(f"  {'-'*75}")
        for sig_name, sig_fn in amg_signals:
            r = backtest_anti_mg(candles, ind, n_candles, sig_fn, base=10, max_rounds=4)
            hit = "✓" if r["weekly"] >= 200 else ""
            sign  = "+" if r["net"]    >= 0 else ""
            wsign = "+" if r["weekly"] >= 0 else ""
            print(f"  {sig_name:<24}  {r['total']:>8,}  {r['wr']:>5.1f}%  "
                  f"{sign}${r['net']:>11,.2f}  {wsign}${r['weekly']:>8,.2f}  {hit:>6}")
        print()

    print(f"{'='*115}\n")


if __name__ == "__main__":
    main()
