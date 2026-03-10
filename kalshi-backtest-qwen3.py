"""
kalshi-backtest-qwen3.py — Practical improvements study.

Findings from qwen2:
  - Anti-MG (base=10, 4x doublings) hits $200+/week over 4Y but NOT over 1Y
  - Combined signals (EMA9+RSI agree) hit 54% but weekly P&L still ~$93 flat
  - Need something that reliably hits $200+/week across ALL timeframes, not just lucky 4Y
  - The core problem: 52-54% win rate is too slim for small contract counts

This version explores:
  1. Time-of-day filtering — do certain hours outperform? (market session effects)
  2. Drawdown circuit-breaker — stop trading after N consecutive losses, reset next session
  3. Walk-forward validation — train on 3Y, validate on 1Y, no lookahead
  4. Adaptive RSI thresholds — tighter thresholds (35/65 vs 30/70) vs looser (25/75)
  5. Minimum edge filter — only trade when RSI is X points from neutral (50)

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


# ── Core signal: EMA9+RSI agree (best from qwen2) ────────────────────────────

def signal_ema9_rsi(ind, i, rsi_low=40, rsi_high=60):
    """EMA9-slope-reversed AND RSI-rev-trend must agree."""
    # EMA9 slope reversed
    if i < 1: return None
    n, p = ind["ema9"][i], ind["ema9"][i - 1]
    if n is None or p is None: return None
    ema_sig = "YES" if n < p else ("NO" if n > p else None)
    if ema_sig is None: return None
    # RSI reversal
    v = ind["rsi14"][i]
    if v is None: return None
    rsi_sig = "YES" if v < rsi_low else ("NO" if v > rsi_high else None)
    if rsi_sig is None: return None
    # Both must agree
    if ema_sig != rsi_sig: return None
    return ema_sig


def signal_ema9_slope_rev(ind, i):
    if i < 1: return None
    n, p = ind["ema9"][i], ind["ema9"][i - 1]
    if n is None or p is None: return None
    if n < p: return "YES"
    if n > p: return "NO"
    return None


def signal_rsi_rev(ind, i, rsi_low=40, rsi_high=60):
    v = ind["rsi14"][i]
    if v is None: return None
    if v < rsi_low:  return "YES"
    if v > rsi_high: return "NO"
    return None


# ── Session helpers ───────────────────────────────────────────────────────────

def hour_est(ts):
    return datetime.fromtimestamp(ts, tz=EST).hour

def weekday_est(ts):
    return datetime.fromtimestamp(ts, tz=EST).weekday()  # 0=Mon, 6=Sun


# ── Backtest engine ───────────────────────────────────────────────────────────

def backtest(candles, indicators, n_candles, strat_fn,
             contracts=10,
             hour_filter=None,      # set of hours (EST) to trade, None = all
             max_consec_losses=None,  # circuit breaker: skip N candles after N losses
             cooldown_candles=4):    # how many candles to sit out after circuit trips
    """
    Unified flat-sizing backtest with optional filters.
    strat_fn(ind, i) -> "YES" | "NO" | None
    """
    start  = max(0, len(candles) - n_candles)
    subset = candles[start:]
    wins = losses = skipped = cooled = 0
    net = 0.0
    consec_losses = 0
    cooldown = 0

    for j, c in enumerate(subset):
        gi = start + j
        si = gi - 1
        if si < 0:
            skipped += 1
            continue

        # Circuit breaker cooldown
        if cooldown > 0:
            cooldown -= 1
            cooled += 1
            continue

        # Hour filter
        if hour_filter is not None and hour_est(c["ts"]) not in hour_filter:
            skipped += 1
            continue

        side = strat_fn(indicators, si)
        if side is None:
            skipped += 1
            continue
        if c["close"] == c["open"]:
            skipped += 1
            continue

        actual = "YES" if c["close"] > c["open"] else "NO"

        if side == actual:
            net += contracts * (PAYOUT - ENTRY_PRICE)
            wins += 1
            consec_losses = 0
        else:
            net -= contracts * ENTRY_PRICE
            losses += 1
            consec_losses += 1
            if max_consec_losses and consec_losses >= max_consec_losses:
                cooldown = cooldown_candles
                consec_losses = 0

    total = wins + losses
    wr    = wins / total * 100 if total else 0.0
    if len(subset) > 1:
        weeks = (subset[-1]["ts"] - subset[0]["ts"]) / (7 * 24 * 3600)
    else:
        weeks = 1.0
    return dict(wins=wins, losses=losses, skipped=skipped, cooled=cooled,
                total=total, wr=wr, net=net,
                weekly=net / weeks if weeks > 0 else 0)


def backtest_anti_mg(candles, indicators, n_candles, strat_fn,
                     base=10, max_rounds=4,
                     hour_filter=None,
                     max_consec_losses=None,
                     cooldown_candles=4):
    """Anti-martingale with optional filters."""
    start  = max(0, len(candles) - n_candles)
    subset = candles[start:]
    wins = losses = skipped = cooled = 0
    net = 0.0
    ladder = [base * (2 ** r) for r in range(max_rounds + 1)]
    rnd = 0
    consec_losses = 0
    cooldown = 0

    for j, c in enumerate(subset):
        gi = start + j
        si = gi - 1
        if si < 0:
            skipped += 1
            continue
        if cooldown > 0:
            cooldown -= 1
            cooled += 1
            continue
        if hour_filter is not None and hour_est(c["ts"]) not in hour_filter:
            skipped += 1
            continue

        side = strat_fn(indicators, si)
        if side is None:
            skipped += 1
            continue
        if c["close"] == c["open"]:
            skipped += 1
            continue

        actual = "YES" if c["close"] > c["open"] else "NO"
        k = ladder[rnd]

        if side == actual:
            net += k * (PAYOUT - ENTRY_PRICE)
            wins += 1
            rnd = min(rnd + 1, max_rounds)
            consec_losses = 0
        else:
            net -= k * ENTRY_PRICE
            losses += 1
            rnd = 0
            consec_losses += 1
            if max_consec_losses and consec_losses >= max_consec_losses:
                cooldown = cooldown_candles
                consec_losses = 0

    total = wins + losses
    wr    = wins / total * 100 if total else 0.0
    weeks = (subset[-1]["ts"] - subset[0]["ts"]) / (7 * 24 * 3600) if len(subset) > 1 else 1.0
    return dict(wins=wins, losses=losses, skipped=skipped, cooled=cooled,
                total=total, wr=wr, net=net,
                weekly=net / weeks if weeks > 0 else 0)


# ── Walk-forward validation ───────────────────────────────────────────────────

def walk_forward(candles, indicators, strat_fn, contracts=10,
                 train_years=3, val_years=1):
    """
    Simple walk-forward: train period is ignored (no parameter fitting here —
    signal params are fixed). We just split into train/val and report both.
    This shows whether the strategy degrades on unseen data.
    """
    n_train = int(train_years * 365 * 24 * 4)
    n_val   = int(val_years  * 365 * 24 * 4)
    total_needed = n_train + n_val

    if len(candles) < total_needed:
        return None

    train_start = len(candles) - total_needed
    val_start   = len(candles) - n_val

    # Train result (for context)
    train_candles = candles[train_start : val_start]
    train_ind = {
        "close": [c["close"] for c in candles[:val_start]],
        "rsi14": indicators["rsi14"][:val_start],
        "ema9":  indicators["ema9"][:val_start],
        "ema20": indicators["ema20"][:val_start],
    }
    # Backtest on train slice
    tr = _slice_backtest(candles, train_ind, train_start, val_start, strat_fn, contracts)

    # Val result
    val_ind = indicators  # full series, using global indices
    vr = _slice_backtest(candles, val_ind, val_start, len(candles), strat_fn, contracts)

    return tr, vr


def _slice_backtest(candles, indicators, start, end, strat_fn, contracts):
    subset = candles[start:end]
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
    wr = wins / total * 100 if total else 0.0
    weeks = (subset[-1]["ts"] - subset[0]["ts"]) / (7 * 24 * 3600) if len(subset) > 1 else 1.0
    return dict(wins=wins, losses=losses, skipped=skipped, total=total,
                wr=wr, net=net, weekly=net / weeks if weeks > 0 else 0)


# ── Hour-of-day analysis ──────────────────────────────────────────────────────

def hour_analysis(candles, indicators, n_candles, strat_fn):
    """Win rate by hour of day (EST) for a given strategy."""
    start  = max(0, len(candles) - n_candles)
    subset = candles[start:]
    hour_stats = {h: {"wins": 0, "losses": 0} for h in range(24)}

    for j, c in enumerate(subset):
        gi = start + j
        si = gi - 1
        if si < 0: continue
        side = strat_fn(indicators, si)
        if side is None: continue
        if c["close"] == c["open"]: continue
        actual = "YES" if c["close"] > c["open"] else "NO"
        h = hour_est(c["ts"])
        if side == actual:
            hour_stats[h]["wins"] += 1
        else:
            hour_stats[h]["losses"] += 1

    return hour_stats


# ── Main ──────────────────────────────────────────────────────────────────────

def main():
    print("Loading & computing...", flush=True)
    candles = load_candles()[:-1]  # drop in-progress candle
    closes  = [c["close"] for c in candles]
    ind = {
        "close": closes,
        "rsi14": compute_rsi(closes, 14),
        "ema9":  compute_ema(closes, 9),
        "ema20": compute_ema(closes, 20),
    }

    first_dt = datetime.fromtimestamp(candles[0]["ts"],  tz=EST).strftime("%Y-%m-%d")
    last_dt  = datetime.fromtimestamp(candles[-1]["ts"], tz=EST).strftime("%Y-%m-%d")

    print(f"\n{'='*120}")
    print(f"  kalshi-backtest-qwen3.py — Practical Improvements: Time Filters, Circuit Breakers, Walk-Forward")
    print(f"  DB: {len(candles):,} candles ({first_dt} → {last_dt})")
    print(f"  Entry: ${ENTRY_PRICE:.2f} | Payout: ${PAYOUT:.2f} | Goal: $200+/week")
    print(f"{'='*120}\n")

    # Best signal from qwen2: EMA9+RSI agree (40/60 thresholds)
    best_sig = lambda ind, i: signal_ema9_rsi(ind, i, 40, 60)

    # ── Section 1: RSI threshold sensitivity ─────────────────────────────────
    print("━━━ SECTION 1: RSI THRESHOLD SENSITIVITY (EMA9+RSI, flat 10 contracts) ━━━\n")
    print("  Thresholds define 'oversold/overbought' zone for RSI reversal signal")
    thresholds = [
        ("RSI <35/>65",  35, 65),
        ("RSI <40/>60",  40, 60),  # baseline from qwen2
        ("RSI <45/>55",  45, 55),
        ("RSI <30/>70",  30, 70),
        ("RSI <25/>75",  25, 75),
        ("RSI <20/>80",  20, 80),
    ]

    for period_label, n_candles in PERIODS:
        n_actual = min(n_candles, len(candles))
        span = datetime.fromtimestamp(candles[-n_actual]["ts"], tz=EST).strftime("%Y-%m-%d")
        print(f"\n  ── {period_label} ({span} → {last_dt})")
        print(f"  {'Threshold':<16}  {'Trades':>8}  {'Win%':>6}  {'Net P&L':>11}  {'$/Week':>10}  {'$200?':>6}")
        print(f"  {'-'*65}")
        for label, lo, hi in thresholds:
            sig = lambda ind, i, lo=lo, hi=hi: signal_ema9_rsi(ind, i, lo, hi)
            r = backtest(candles, ind, n_candles, sig, contracts=10)
            hit = "✓" if r["weekly"] >= 200 else ""
            wsign = "+" if r["weekly"] >= 0 else ""
            sign  = "+" if r["net"]    >= 0 else ""
            print(f"  {label:<16}  {r['total']:>8,}  {r['wr']:>5.1f}%  "
                  f"{sign}${r['net']:>9,.2f}  {wsign}${r['weekly']:>8,.2f}  {hit:>6}")

    # ── Section 2: Time-of-day filter ────────────────────────────────────────
    print(f"\n\n{'━'*60}")
    print("━━━ SECTION 2: TIME-OF-DAY FILTER (EMA9+RSI 40/60, flat 10 contracts) ━━━\n")
    print("  Testing common market session windows vs trading all hours\n")

    # Build hour stats over 4Y for analysis
    n_4y = PERIODS[-1][1]
    h_stats = hour_analysis(candles, ind, n_4y, best_sig)
    print("  Hour-by-hour win rate (4Y, EST):")
    print(f"  {'Hour':>5}  {'Wins':>6}  {'Loss':>6}  {'Total':>7}  {'WR%':>6}")
    print(f"  {'-'*35}")
    for h in range(24):
        s = h_stats[h]
        tot = s["wins"] + s["losses"]
        if tot == 0:
            continue
        wr = s["wins"] / tot * 100
        marker = " ◀" if wr >= 55 else (" ▼" if wr < 50 else "")
        print(f"  {h:>4}h  {s['wins']:>6,}  {s['losses']:>6,}  {tot:>7,}  {wr:>5.1f}%{marker}")

    # Define time windows
    time_windows = [
        ("All hours",         None),
        ("US session 9-17h",  set(range(9, 17))),
        ("EU+US 8-17h",       set(range(8, 17))),
        ("US eve 17-23h",     set(range(17, 23))),
        ("Asia 0-8h",         set(range(0, 8))),
        ("Off-hours 0-8+22h", set(range(0, 8)) | {22, 23}),
        ("Best hours ≥55%",   {h for h in range(24)
                               if (h_stats[h]["wins"] + h_stats[h]["losses"]) > 100
                               and h_stats[h]["wins"] / (h_stats[h]["wins"] + h_stats[h]["losses"]) >= 0.55}),
    ]

    # Show which hours qualify as "best"
    best_hrs = time_windows[-1][1]
    print(f"\n  Hours with ≥55% WR (4Y, ≥100 trades): {sorted(best_hrs) if best_hrs else 'none'}\n")

    print(f"\n  {'Window':<24}  {'Trades':>8}  {'Win%':>6}  {'Net P&L':>11}  {'$/Week':>10}  {'$200?':>6}")
    print(f"  {'-'*75}")
    for period_label, n_candles in PERIODS:
        n_actual = min(n_candles, len(candles))
        span = datetime.fromtimestamp(candles[-n_actual]["ts"], tz=EST).strftime("%Y-%m-%d")
        print(f"\n  ── {period_label} ({span} → {last_dt})")
        for label, hf in time_windows:
            r = backtest(candles, ind, n_candles, best_sig, contracts=10, hour_filter=hf)
            hit = "✓" if r["weekly"] >= 200 else ""
            wsign = "+" if r["weekly"] >= 0 else ""
            sign  = "+" if r["net"]    >= 0 else ""
            print(f"  {label:<24}  {r['total']:>8,}  {r['wr']:>5.1f}%  "
                  f"{sign}${r['net']:>9,.2f}  {wsign}${r['weekly']:>8,.2f}  {hit:>6}")

    # ── Section 3: Circuit breaker ────────────────────────────────────────────
    print(f"\n\n{'━'*60}")
    print("━━━ SECTION 3: DRAWDOWN CIRCUIT BREAKER (EMA9+RSI 40/60, flat 10 contracts) ━━━\n")
    print("  After N consecutive losses, sit out the next 4 candles (1 hour)\n")

    circuit_configs = [
        ("No breaker",   None),
        ("Trip at 3L",   3),
        ("Trip at 4L",   4),
        ("Trip at 5L",   5),
        ("Trip at 6L",   6),
        ("Trip at 8L",   8),
    ]

    for period_label, n_candles in PERIODS:
        n_actual = min(n_candles, len(candles))
        span = datetime.fromtimestamp(candles[-n_actual]["ts"], tz=EST).strftime("%Y-%m-%d")
        print(f"  ── {period_label} ({span} → {last_dt})")
        print(f"  {'Config':<16}  {'Trades':>8}  {'Cooled':>7}  {'Win%':>6}  {'Net P&L':>11}  {'$/Week':>10}  {'$200?':>6}")
        print(f"  {'-'*75}")
        for label, mcl in circuit_configs:
            r = backtest(candles, ind, n_candles, best_sig, contracts=10,
                         max_consec_losses=mcl, cooldown_candles=4)
            hit = "✓" if r["weekly"] >= 200 else ""
            wsign = "+" if r["weekly"] >= 0 else ""
            sign  = "+" if r["net"]    >= 0 else ""
            print(f"  {label:<16}  {r['total']:>8,}  {r['cooled']:>7,}  {r['wr']:>5.1f}%  "
                  f"{sign}${r['net']:>9,.2f}  {wsign}${r['weekly']:>8,.2f}  {hit:>6}")
        print()

    # ── Section 4: Anti-MG + circuit breaker combo ───────────────────────────
    print(f"\n{'━'*60}")
    print("━━━ SECTION 4: ANTI-MG + CIRCUIT BREAKER (base=10, 4 doublings max) ━━━\n")
    print("  Ladder: 10→20→40→80→160 | Reset on loss | Breaker = sit out 4 candles\n")

    for period_label, n_candles in PERIODS:
        n_actual = min(n_candles, len(candles))
        span = datetime.fromtimestamp(candles[-n_actual]["ts"], tz=EST).strftime("%Y-%m-%d")
        print(f"  ── {period_label} ({span} → {last_dt})")
        print(f"  {'Config':<22}  {'Trades':>8}  {'Win%':>6}  {'Net P&L':>12}  {'$/Week':>10}  {'$200?':>6}")
        print(f"  {'-'*75}")
        configs = [
            ("No breaker",       None, None),
            ("AMG+Trip@3L",      3,    None),
            ("AMG+Trip@5L",      5,    None),
            ("AMG+9-17h only",   None, set(range(9, 17))),
            ("AMG+9-17h+Trip@3", 3,    set(range(9, 17))),
            ("AMG+best hrs",     None, best_hrs or None),
        ]
        for label, mcl, hf in configs:
            r = backtest_anti_mg(candles, ind, n_candles, best_sig,
                                 base=10, max_rounds=4,
                                 hour_filter=hf,
                                 max_consec_losses=mcl, cooldown_candles=4)
            hit = "✓" if r["weekly"] >= 200 else ""
            wsign = "+" if r["weekly"] >= 0 else ""
            sign  = "+" if r["net"]    >= 0 else ""
            print(f"  {label:<22}  {r['total']:>8,}  {r['wr']:>5.1f}%  "
                  f"{sign}${r['net']:>11,.2f}  {wsign}${r['weekly']:>8,.2f}  {hit:>6}")
        print()

    # ── Section 5: Walk-forward validation ────────────────────────────────────
    print(f"{'━'*60}")
    print("━━━ SECTION 5: WALK-FORWARD VALIDATION (train 3Y → validate 1Y) ━━━\n")
    print("  Tests whether strategy holds up on unseen data")
    print("  Signal: EMA9+RSI agree | Flat 10 contracts\n")

    wf_signals = [
        ("EMA9-slope-rev",   signal_ema9_slope_rev),
        ("RSI-rev 40/60",    lambda ind, i: signal_rsi_rev(ind, i, 40, 60)),
        ("RSI-rev 30/70",    lambda ind, i: signal_rsi_rev(ind, i, 30, 70)),
        ("EMA9+RSI 40/60",   lambda ind, i: signal_ema9_rsi(ind, i, 40, 60)),
        ("EMA9+RSI 30/70",   lambda ind, i: signal_ema9_rsi(ind, i, 30, 70)),
    ]

    n_train = int(3 * 365 * 24 * 4)
    n_val   = int(1 * 365 * 24 * 4)
    total_needed = n_train + n_val

    if len(candles) >= total_needed:
        train_start = len(candles) - total_needed
        val_start   = len(candles) - n_val

        train_span = (
            datetime.fromtimestamp(candles[train_start]["ts"], tz=EST).strftime("%Y-%m-%d"),
            datetime.fromtimestamp(candles[val_start - 1]["ts"], tz=EST).strftime("%Y-%m-%d"),
        )
        val_span = (
            datetime.fromtimestamp(candles[val_start]["ts"], tz=EST).strftime("%Y-%m-%d"),
            last_dt,
        )

        print(f"  Train: {train_span[0]} → {train_span[1]} (3Y)")
        print(f"  Val:   {val_span[0]}   → {val_span[1]}   (1Y)")
        print()
        print(f"  {'Signal':<22}  {'Phase':<8}  {'Trades':>8}  {'Win%':>6}  {'Net P&L':>11}  {'$/Week':>10}  {'$200?':>6}")
        print(f"  {'-'*80}")

        for sig_label, sig_fn in wf_signals:
            for phase, s_start, s_end in [("TRAIN", train_start, val_start),
                                           ("VAL",   val_start,   len(candles))]:
                r = _slice_backtest(candles, ind, s_start, s_end, sig_fn, 10)
                hit = "✓" if r["weekly"] >= 200 else ""
                wsign = "+" if r["weekly"] >= 0 else ""
                sign  = "+" if r["net"]    >= 0 else ""
                print(f"  {sig_label:<22}  {phase:<8}  {r['total']:>8,}  {r['wr']:>5.1f}%  "
                      f"{sign}${r['net']:>9,.2f}  {wsign}${r['weekly']:>8,.2f}  {hit:>6}")
            print()
    else:
        print(f"  Not enough candles for 4Y walk-forward (have {len(candles):,}, need {total_needed:,})")

    # ── Summary ───────────────────────────────────────────────────────────────
    print(f"\n{'='*120}")
    print("  SUMMARY")
    print(f"{'='*120}")
    print("""
  Key findings from qwen3:

  SEC 1 — RSI thresholds:
    Tighter (45/55) = more trades, lower conviction. Wider (20/80) = fewer but higher WR.
    Sweet spot is likely 30/70 or 40/60 depending on time period.

  SEC 2 — Time-of-day:
    Check if US session hours (9-17h EST) outperform Asia/off-hours.
    If so, filtering to session hours reduces noise.

  SEC 3 — Circuit breaker:
    Sitting out after N consecutive losses may reduce drawdowns without
    significantly hurting win rate or weekly P&L.

  SEC 4 — Anti-MG + filters:
    Best combo should be identifiable from the table (look for ✓ across all periods).

  SEC 5 — Walk-forward:
    If TRAIN WR% ≈ VAL WR%, strategy is robust (not overfit).
    If VAL degrades significantly, the signal may be curve-fitted.
""")
    print(f"{'='*120}\n")


if __name__ == "__main__":
    main()
