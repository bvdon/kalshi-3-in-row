"""
kalshi_backtest_mg.py — Martingale backtest: always YES, skip cycles on bad signals.

Strategy:
  - Always bet YES
  - Use 15m EMA filters to skip cycles when momentum is bearish
  - MG round stays frozen on skip — only resets on win or max round hit

Modes:
  SWEEP_MODE = False  → single run with configured filters, prints full trade log
  SWEEP_MODE = True   → tests all filter combinations, prints comparison table only

Usage:
  python kalshi_backtest_mg.py
"""

import sys
import sqlite3
sys.path.insert(0, ".")

from pathlib import Path
from datetime import datetime, timezone, timedelta
import connectors.kraken as kraken

# ── Config ────────────────────────────────────────────────────────────────────
SIDE             = "YES"
BACKTEST_RANGE   = 672        # number of 15m candles (0 = ALL) 139776 is 4 years, 34944 is 1 year
ENTRY_PRICE      = 0.50
MG_MAX_ROUND     = 9
EST              = timezone(timedelta(hours=-5))
CANDLES_DB       = Path(__file__).parent / "candles.db"

# -1 = use only fully closed candles (EMA excludes current candle) — matches live bot behavior
#  0 = include current in-progress candle close in EMA — simulates mid-candle live check
CANDLE_OFFSET    = -1

# ── Mode ──────────────────────────────────────────────────────────────────────
SWEEP_MODE       = False          # True = comparison table | False = single run + trade log

# ── Single-run filter config (used when SWEEP_MODE = False) ──────────────────
# 🏆 BEST: EMA2 Slope>0.5 — Score #1 | Win 88.7% | MAX_L 3 | 0 round hits | Net +$61,276
EMA_FAST         = 9
EMA_SLOW         = 21
EMA_LONG         = None           # None to disable
USE_EMA_CROSS    = True           # skip if EMA_FAST < EMA_SLOW
EMA_SPREAD_MIN   = None           # skip if spread < this (e.g. 50.0) | None = off
EMA_SLOPE_MIN    = None           # skip if slope < this change from 0.5 to None
USE_EMA_LONG     = False          # skip if EMA_FAST < EMA_LONG


# USE_EMA_CROSS    = False
# EMA_SPREAD_MIN   = None
# EMA_SLOPE_MIN    = 0.5   # ← this is the key one, change from 0.5 to None
# USE_EMA_LONG     = False


# ── Sweep variations (used when SWEEP_MODE = True) ───────────────────────────
SWEEP_VARIATIONS = [
    # label                              fast  slow  long   cross   spread  slope    long_filter
    # ── Baseline ──────────────────────────────────────────────────────────────────────────────
    ("No filters",                        9,   21,   None,  False,  None,   None,    False),

    # ── EMA2 slope sweep ──────────────────────────────────────────────────────────────────────
    ("EMA2 Slope>-5",                     2,   21,   None,  False,  None,   -5.0,    False),
    ("EMA2 Slope>-2",                     2,   21,   None,  False,  None,   -2.0,    False),
    ("EMA2 Slope>-1",                     2,   21,   None,  False,  None,   -1.0,    False),
    ("EMA2 Slope>0",                      2,   21,   None,  False,  None,   0.0,     False),
    ("EMA2 Slope>1",                      2,   21,   None,  False,  None,   1.0,     False),
    ("EMA2 Slope>2",                      2,   21,   None,  False,  None,   2.0,     False),
    ("EMA2 Slope>5",                      2,   21,   None,  False,  None,   5.0,     False),
    ("EMA2 Slope>10",                     2,   21,   None,  False,  None,   10.0,    False),
    ("EMA2 Slope>20",                     2,   21,   None,  False,  None,   20.0,    False),

    # ── EMA3 slope sweep ──────────────────────────────────────────────────────────────────────
    ("EMA3 Slope>-5",                     3,   21,   None,  False,  None,   -5.0,    False),
    ("EMA3 Slope>-2",                     3,   21,   None,  False,  None,   -2.0,    False),
    ("EMA3 Slope>-1",                     3,   21,   None,  False,  None,   -1.0,    False),
    ("EMA3 Slope>0",                      3,   21,   None,  False,  None,   0.0,     False),
    ("EMA3 Slope>1",                      3,   21,   None,  False,  None,   1.0,     False),
    ("EMA3 Slope>2",                      3,   21,   None,  False,  None,   2.0,     False),
    ("EMA3 Slope>5",                      3,   21,   None,  False,  None,   5.0,     False),
    ("EMA3 Slope>10",                     3,   21,   None,  False,  None,   10.0,    False),
    ("EMA3 Slope>20",                     3,   21,   None,  False,  None,   20.0,    False),

    # ── EMA4 slope sweep ──────────────────────────────────────────────────────────────────────
    ("EMA4 Slope>-2",                     4,   21,   None,  False,  None,   -2.0,    False),
    ("EMA4 Slope>-1",                     4,   21,   None,  False,  None,   -1.0,    False),
    ("EMA4 Slope>0",                      4,   21,   None,  False,  None,   0.0,     False),
    ("EMA4 Slope>1",                      4,   21,   None,  False,  None,   1.0,     False),
    ("EMA4 Slope>2",                      4,   21,   None,  False,  None,   2.0,     False),
    ("EMA4 Slope>5",                      4,   21,   None,  False,  None,   5.0,     False),

    # ── EMA5 slope sweep ──────────────────────────────────────────────────────────────────────
    ("EMA5 Slope>-2",                     5,   21,   None,  False,  None,   -2.0,    False),
    ("EMA5 Slope>-1",                     5,   21,   None,  False,  None,   -1.0,    False),
    ("EMA5 Slope>0",                      5,   21,   None,  False,  None,   0.0,     False),
    ("EMA5 Slope>1",                      5,   21,   None,  False,  None,   1.0,     False),
    ("EMA5 Slope>2",                      5,   21,   None,  False,  None,   2.0,     False),
    ("EMA5 Slope>5",                      5,   21,   None,  False,  None,   5.0,     False),

    # ── EMA3 fine slope (0.1 increments near 0) ───────────────────────────────────────────────
    ("EMA3 Slope>-0.5",                   3,   21,   None,  False,  None,   -0.5,    False),
    ("EMA3 Slope>0.5",                    3,   21,   None,  False,  None,   0.5,     False),
    ("EMA3 Slope>1.5",                    3,   21,   None,  False,  None,   1.5,     False),
    ("EMA3 Slope>2.5",                    3,   21,   None,  False,  None,   2.5,     False),
    ("EMA3 Slope>3",                      3,   21,   None,  False,  None,   3.0,     False),
    ("EMA3 Slope>4",                      3,   21,   None,  False,  None,   4.0,     False),

    # ── EMA2 fine slope ───────────────────────────────────────────────────────────────────────
    ("EMA2 Slope>0.5",                    2,   21,   None,  False,  None,   0.5,     False),
    ("EMA2 Slope>1.5",                    2,   21,   None,  False,  None,   1.5,     False),
    ("EMA2 Slope>3",                      2,   21,   None,  False,  None,   3.0,     False),

    # ── Best candidates + Cross ───────────────────────────────────────────────────────────────
    ("EMA2 Slope>0 + Cross",              2,   21,   None,  True,   None,   0.0,     False),
    ("EMA3 Slope>0 + Cross",              3,   21,   None,  True,   None,   0.0,     False),
    ("EMA3 Slope>1 + Cross",              3,   21,   None,  True,   None,   1.0,     False),
    ("EMA4 Slope>0 + Cross",              4,   21,   None,  True,   None,   0.0,     False),
    ("EMA5 Slope>0 + Cross",              5,   21,   None,  True,   None,   0.0,     False),
]

# Contract sizes
MG_CONTRACTS = {r: 2 ** r for r in range(1, MG_MAX_ROUND + 1)}


# ── EMA ───────────────────────────────────────────────────────────────────────
def _ema(values: list, period: int) -> list:
    if len(values) < period:
        return []
    k = 2 / (period + 1)
    result = [sum(values[:period]) / period]
    for v in values[period:]:
        result.append(v * k + result[-1] * (1 - k))
    return result


# ── Load candles ──────────────────────────────────────────────────────────────
def load_candles() -> list:
    if CANDLES_DB.exists():
        conn = sqlite3.connect(CANDLES_DB)
        rows = conn.execute(
            "SELECT ts, open, high, low, close FROM btc_candles ORDER BY ts ASC"
        ).fetchall()
        conn.close()
        if rows:
            candles = [{"ts": r[0], "open": r[1], "high": r[2], "low": r[3], "close": r[4]} for r in rows]
            first_dt = datetime.fromtimestamp(rows[0][0],  tz=timezone.utc).strftime("%Y-%m-%d")
            last_dt  = datetime.fromtimestamp(rows[-1][0], tz=timezone.utc).strftime("%Y-%m-%d")
            print(f"  Loaded {len(candles):,} candles from DB ({first_dt} → {last_dt})")
            return candles

    print(f"  No local DB — fetching from Kraken API...", end="", flush=True)
    candles = kraken.get_ohlcv("XBTUSD", interval_minutes=15)
    candles = candles[:-1]
    print(f" {len(candles)} candles (API)")
    return candles


# ── Core backtest engine ──────────────────────────────────────────────────────
def run_backtest(candles, fast, slow, long, use_cross, spread_min, slope_min, use_long, verbose=False):
    all_closes   = [c["close"] for c in candles]
    ema_fast_all = _ema(all_closes, fast)
    ema_slow_all = _ema(all_closes, slow)
    ema_long_all = _ema(all_closes, long) if long else []

    fast_offset = fast - 1
    slow_offset = slow - 1
    long_offset = (long - 1) if long else 0

    mg_round               = 1
    total_profit           = 0.0
    total_loss             = 0.0
    wins = losses = skipped = 0
    running_net            = 0.0
    max_round_hit          = 0
    consecutive_losses     = 0
    max_consecutive_losses = 0
    consecutive_wins       = 0
    max_consecutive_wins   = 0

    HEADER = f"  {'DATE/TIME':<22} {'OPEN':>12} {'CLOSE':>12} {'COLOR':<6} {'E_FAST':>9} {'E_SLOW':>9} {'SPREAD':>8} {'SLOPE':>7} {'SKIP?':<22} {'WIN?':<5} {'MG':>4} {'CONTR':>6} {'PROFIT':>9} {'LOSS':>9} {'NET':>12}"
    REPEAT_HEADER_EVERY = 100  # reprint header every N rows

    if verbose:
        print(HEADER)
        print(f"  {'-'*160}")

    rows_printed = 0
    for idx, c in enumerate(candles):
        candle_open  = c["open"]
        candle_close = c["close"]
        candle_color = "GREEN" if candle_close >= candle_open else "RED"

        fi = idx - fast_offset + CANDLE_OFFSET
        si = idx - slow_offset + CANDLE_OFFSET
        li = idx - long_offset + CANDLE_OFFSET if long else -1

        if fi < 1 or si < 0 or (long and li < 0):
            skipped += 1
            continue

        ef        = ema_fast_all[fi]
        ef_prev   = ema_fast_all[fi - 1]
        es        = ema_slow_all[si]
        el        = ema_long_all[li] if long and li >= 0 else None
        slope     = ef - ef_prev
        spread    = abs(ef - es)

        skip_reason = None
        if use_cross and ef < es:
            skip_reason = "bearish_cross"
        elif spread_min is not None and spread < spread_min:
            skip_reason = "spread_low"
        elif slope_min is not None and slope < slope_min:
            skip_reason = "slope_low"
        elif use_long and el is not None and ef < el:
            skip_reason = "below_long"

        if skip_reason:
            skipped += 1
            if verbose:
                ts_str = datetime.fromtimestamp(c["ts"], tz=EST).strftime("%Y-%m-%d %I:%M %p")
                rows_printed += 1
                if rows_printed % REPEAT_HEADER_EVERY == 0:
                    print(f"\n{HEADER}\n")
                print(f"  {ts_str:<22} ${candle_open:>11,.2f} ${candle_close:>11,.2f} {candle_color:<6} ${ef:>8,.2f} ${es:>8,.2f} ${spread:>7,.2f} {slope:>7.2f} {'SKIP:'+skip_reason:<22} {'—':<5} {mg_round:>4} {'—':>6} {'—':>9} {'—':>9} {'—':>12}")
            continue

        if candle_close > candle_open:
            result = "yes"
        elif candle_close < candle_open:
            result = "no"
        else:
            continue  # doji

        current_round = mg_round
        contracts     = MG_CONTRACTS[min(mg_round, MG_MAX_ROUND)]
        cost          = contracts * ENTRY_PRICE
        payout        = contracts * 1.00
        won           = True  # always YES

        if result == "yes":
            profit = payout - cost
            total_profit       += profit
            running_net        += profit
            wins               += 1
            mg_round            = 1
            consecutive_losses  = 0
            consecutive_wins   += 1
            max_consecutive_wins = max(max_consecutive_wins, consecutive_wins)
            profit_str, loss_str, win_str = f"+${profit:,.2f}", "—", "WIN"
        else:
            loss = cost
            total_loss         += loss
            running_net        -= loss
            losses             += 1
            consecutive_wins    = 0
            consecutive_losses += 1
            max_consecutive_losses = max(max_consecutive_losses, consecutive_losses)
            if mg_round >= MG_MAX_ROUND:
                max_round_hit += 1
                mg_round = 1
            else:
                mg_round += 1
            profit_str, loss_str, win_str = "—", f"-${loss:,.2f}", "LOSS"

        if verbose:
            ts_str  = datetime.fromtimestamp(c["ts"], tz=EST).strftime("%Y-%m-%d %I:%M %p")
            net_str = f"{'+'if running_net>=0 else ''}${running_net:,.2f}"
            rows_printed += 1
            if rows_printed % REPEAT_HEADER_EVERY == 0:
                print(f"\n{HEADER}\n")
            print(f"  {ts_str:<22} ${candle_open:>11,.2f} ${candle_close:>11,.2f} {candle_color:<6} ${ef:>8,.2f} ${es:>8,.2f} ${spread:>7,.2f} {slope:>7.2f} {'—':<22} {win_str:<5} {current_round:>4} {contracts:>6} {profit_str:>9} {loss_str:>9} {net_str:>12}")

    total_trades = wins + losses
    win_rate     = (wins / total_trades * 100) if total_trades else 0
    net          = total_profit - total_loss

    return {
        "trades": total_trades, "wins": wins, "losses": losses,
        "skipped": skipped, "win_rate": win_rate,
        "max_wins": max_consecutive_wins, "max_losses": max_consecutive_losses,
        "round_hits": max_round_hit, "net": net,
        "profit": total_profit, "loss": total_loss,
    }


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    now_est = datetime.now(EST).strftime("%Y-%m-%d %I:%M %p EST")

    all_candles = load_candles()
    all_candles = all_candles[:-1]

    if BACKTEST_RANGE and BACKTEST_RANGE < len(all_candles):
        candles = all_candles[-BACKTEST_RANGE:]
    else:
        candles = all_candles

    first_dt = datetime.fromtimestamp(candles[0]["ts"], tz=timezone.utc).strftime("%Y-%m-%d")
    last_dt  = datetime.fromtimestamp(candles[-1]["ts"], tz=timezone.utc).strftime("%Y-%m-%d")

    if not SWEEP_MODE:
        # ── Single run with trade log ─────────────────────────────────────────
        filters = []
        if USE_EMA_CROSS:  filters.append(f"Cross(EMA{EMA_FAST}<EMA{EMA_SLOW})")
        if EMA_SPREAD_MIN: filters.append(f"Spread<{EMA_SPREAD_MIN}")
        if EMA_SLOPE_MIN is not None: filters.append(f"Slope<{EMA_SLOPE_MIN}")
        if USE_EMA_LONG:   filters.append(f"EMA{EMA_FAST}<EMA{EMA_LONG}")
        filter_str = " | ".join(filters) if filters else "NONE"

        candle_mode = "fully closed candles" if CANDLE_OFFSET == -1 else "includes current candle"
        print(f"\n{'='*160}")
        print(f"  MG Backtest — SIDE=YES | Filters: {filter_str} | MG cap={MG_MAX_ROUND} | CANDLE_OFFSET={CANDLE_OFFSET} ({candle_mode}) | {now_est}")
        print(f"  {len(candles):,} candles | {first_dt} → {last_dt}")
        print(f"{'='*160}\n")

        r = run_backtest(candles, EMA_FAST, EMA_SLOW, EMA_LONG, USE_EMA_CROSS,
                         EMA_SPREAD_MIN, EMA_SLOPE_MIN, USE_EMA_LONG, verbose=True)

        print(f"\n  {'DATE/TIME':<22} {'OPEN':>12} {'CLOSE':>12} {'COLOR':<6} {'E_FAST':>9} {'E_SLOW':>9} {'SPREAD':>8} {'SLOPE':>7} {'SKIP?':<22} {'WIN?':<5} {'MG':>4} {'CONTR':>6} {'PROFIT':>9} {'LOSS':>9} {'NET':>12}")
        print(f"\n  {'─'*160}")
        print(f"  Trades: {r['trades']:,}  |  Wins: {r['wins']:,}  |  Losses: {r['losses']:,}  |  Skipped: {r['skipped']:,}  |  Win Rate: {r['win_rate']:.1f}%")
        print(f"  Max Consec Wins: {r['max_wins']}  |  Max Consec Losses: {r['max_losses']}  |  MG Round Hits: {r['round_hits']}")
        print(f"  Total Profit: +${r['profit']:,.2f}  |  Total Loss: -${r['loss']:,.2f}  |  Net P&L: {'+'if r['net']>=0 else ''}${r['net']:,.2f}")
        print(f"{'='*160}\n")

    else:
        # ── Sweep mode — comparison table ─────────────────────────────────────
        print(f"\n{'='*150}")
        print(f"  MG Backtest SWEEP — SIDE=YES | MG cap={MG_MAX_ROUND} | {len(candles):,} candles | {first_dt} → {last_dt}")
        print(f"  {now_est}")
        print(f"{'='*150}\n")
        print(f"  {'VARIATION':<35} {'TRADES':>8} {'WIN%':>6} {'SKIPPED':>8} {'MAX_W':>6} {'MAX_L':>6} {'HITS':>5} {'NET_PNL':>12}")
        print(f"  {'-'*93}")

        results = []
        for var in SWEEP_VARIATIONS:
            label, fast, slow, long, cross, spread, slope, use_long = var
            print(f"  {label:<35}", end="", flush=True)
            r = run_backtest(candles, fast, slow, long, cross, spread, slope, use_long, verbose=False)
            results.append((label, r))
            net_str = f"{'+'if r['net']>=0 else ''}${r['net']:,.2f}"
            print(f" {r['trades']:>8,} {r['win_rate']:>5.1f}% {r['skipped']:>8,} {r['max_wins']:>6} {r['max_losses']:>6} {r['round_hits']:>5} {net_str:>12}")

        # Score = net P&L * win_rate_factor, penalized by max_loss streak and round hits
        # Higher score = better balance of profit + safety
        def score(r):
            if r['trades'] < 1000:
                return -999999  # too few trades to be meaningful
            streak_penalty = r['max_losses'] * 2000
            hit_penalty    = r['round_hits'] * 5000
            return r['net'] - streak_penalty - hit_penalty

        # Sort by net P&L
        by_pnl = sorted(results, key=lambda x: x[1]['net'], reverse=True)
        # Sort by score
        by_score = sorted(results, key=lambda x: score(x[1]), reverse=True)

        def print_table(title, rows):
            print(f"\n  {'─'*105}")
            print(f"  {title}:")
            print(f"  {'─'*105}")
            print(f"  {'#':<4} {'VARIATION':<35} {'TRADES':>8} {'WIN%':>6} {'SKIPPED':>8} {'MAX_W':>6} {'MAX_L':>6} {'HITS':>5} {'NET_PNL':>12} {'SCORE':>12}")
            print(f"  {'-'*105}")
            for i, (label, r) in enumerate(rows, 1):
                net_str   = f"{'+'if r['net']>=0 else ''}${r['net']:,.2f}"
                score_val = score(r)
                score_str = f"{'+'if score_val>=0 else ''}${score_val:,.0f}"
                print(f"  {i:<4} {label:<35} {r['trades']:>8,} {r['win_rate']:>5.1f}% {r['skipped']:>8,} {r['max_wins']:>6} {r['max_losses']:>6} {r['round_hits']:>5} {net_str:>12} {score_str:>12}")

        print_table("RANKED BY NET P&L", by_pnl)
        print_table("RANKED BY SCORE (P&L - streak/hit penalties)", by_score)
        print(f"{'='*150}\n")


if __name__ == "__main__":
    main()
