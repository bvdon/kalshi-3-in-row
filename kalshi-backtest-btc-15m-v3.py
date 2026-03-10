"""
kalshi-backtest-btc-15m-v3.py — BTC 15m backtest using Kraken candles only.

- Strike        : Kraken candle open price
- Win/Loss      : close > open → YES wins | close < open → NO wins
- EMA signals   : calculated from prior closed candles (CANDLE_OFFSET=-1, no lookahead)
- MG tracking   : simulated locally
- Candle source : local candles.db (362k candles, 2013–2026)

Usage:
  python kalshi-backtest-btc-15m-v3.py
"""

import sys
import sqlite3
sys.path.insert(0, ".")

from pathlib import Path
from datetime import datetime, timezone, timedelta

CANDLES_DB = Path(__file__).parent / "candles.db"

# ── Config ────────────────────────────────────────────────────────────────────
BACKTEST_RANGE       = 672     # number of 15m candles (84 days × 96 = 8064) | 0 = ALL 6720 34944

EMA_FAST             = 9
EMA_SLOW             = 21
EMA_LONG             = 50
EMA_SPREAD_MIN       = 10.0
SLOPE_CONFLICT_MIN   = 55.0
USE_EMA_CROSS        = True     # skip if EMA_FAST < EMA_SLOW (bearish cross)

MG_CONTRACTS         = {1: 2, 2: 4, 3: 8, 4: 16, 5: 32, 6: 64, 7: 128, 8: 256, 9: 512}
MG_MAX_ROUND         = 9
MG_RESET_ON_MAX_LOSS = True    # True = reset to round 1 after max round hit
ENTRY_PRICE          = 0.50
EST                  = timezone(timedelta(hours=-5))


# ── EMA ───────────────────────────────────────────────────────────────────────
def _ema(values: list, period: int) -> list:
    if len(values) < period:
        return []
    k = 2 / (period + 1)
    ema_vals = [sum(values[:period]) / period]
    for v in values[period:]:
        ema_vals.append(v * k + ema_vals[-1] * (1 - k))
    return ema_vals


# ── Load candles from local DB ────────────────────────────────────────────────
def load_candles() -> list:
    print(f"  Loading candles from DB...", end="", flush=True)
    conn = sqlite3.connect(CANDLES_DB)
    rows = conn.execute(
        "SELECT ts, open, high, low, close FROM btc_candles ORDER BY ts ASC"
    ).fetchall()
    conn.close()
    candles = [{"ts": r[0], "open": r[1], "high": r[2], "low": r[3], "close": r[4]} for r in rows]
    first_dt = datetime.fromtimestamp(rows[0][0],  tz=timezone.utc).strftime("%Y-%m-%d")
    last_dt  = datetime.fromtimestamp(rows[-1][0], tz=timezone.utc).strftime("%Y-%m-%d")
    print(f" {len(candles):,} candles ({first_dt} → {last_dt})")
    return candles


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    now_est   = datetime.now(EST).strftime("%Y-%m-%d %I:%M %p EST")
    reset_str = "CutLoss@MaxRound" if MG_RESET_ON_MAX_LOSS else "StayAtMaxRound"

    all_candles = load_candles()
    all_candles = all_candles[:-1]  # drop in-progress candle

    if BACKTEST_RANGE and BACKTEST_RANGE < len(all_candles):
        candles = all_candles[-BACKTEST_RANGE:]
    else:
        candles = all_candles

    first_dt = datetime.fromtimestamp(candles[0]["ts"], tz=timezone.utc).strftime("%Y-%m-%d")
    last_dt  = datetime.fromtimestamp(candles[-1]["ts"], tz=timezone.utc).strftime("%Y-%m-%d")

    print(f"\n{'='*175}")
    print(f"  BTC 15m Backtest v3 (Kraken candles only) — {now_est}")
    print(f"  Range: {len(candles):,} candles | {first_dt} → {last_dt}")
    cross_str = f"EMA{EMA_FAST}>EMA{EMA_SLOW}" if USE_EMA_CROSS else "no cross filter"
    print(f"  EMA{EMA_FAST}/{EMA_SLOW}/{EMA_LONG} | Spread≥${EMA_SPREAD_MIN} | SlopeConflict>{SLOPE_CONFLICT_MIN} | {cross_str} | MG cap={MG_MAX_ROUND} | {reset_str}")
    print(f"{'='*175}\n")

    # Pre-compute EMAs over full candle set
    all_closes   = [c["close"] for c in all_candles]
    # Index offset: for candle at position idx in all_candles,
    # ema index = idx - (period-1) - 1  (CANDLE_OFFSET=-1, no lookahead)
    ema_fast_all = _ema(all_closes, EMA_FAST)
    ema_slow_all = _ema(all_closes, EMA_SLOW)
    ema_long_all = _ema(all_closes, EMA_LONG)

    fast_offset = EMA_FAST - 1
    slow_offset = EMA_SLOW - 1
    long_offset = EMA_LONG - 1

    # Starting index in all_candles for our test window
    start_idx = len(all_candles) - len(candles)

    # ── Header ────────────────────────────────────────────────────────────────
    HEADER = (f"  {'DATE/TIME':<22} {'STRIKE/OPEN':>13} {'CLOSE':>13} {'COLOR':<7} "
              f"{'EMA_FAST':>12} {'EMA_SLOW':>12} {'EMA_LONG':>12} "
              f"{'SPREAD':>9} {'SLOPE':>8} {'SIDE':<5} "
              f"{'SKIP?':<18} {'WIN?':<5} {'MG':>3} {'K':>5} {'PROFIT':>9} {'LOSS':>9} {'NET':>12}")
    print(HEADER)
    print(f"  {'-'*175}")

    # ── State ─────────────────────────────────────────────────────────────────
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
    rows_printed           = 0
    REPEAT_HEADER_EVERY    = 100

    for i, c in enumerate(candles):
        idx = start_idx + i  # position in all_candles

        fi = idx - fast_offset - 1   # CANDLE_OFFSET = -1
        si = idx - slow_offset - 1
        li = idx - long_offset - 1

        if fi < 1 or si < 0 or li < 0:
            skipped += 1
            continue

        ef      = ema_fast_all[fi]
        ef_prev = ema_fast_all[fi - 1]
        es      = ema_slow_all[si]
        el      = ema_long_all[li]
        slope   = ef - ef_prev
        spread  = abs(ef - es)

        candle_open  = c["open"]
        candle_close = c["close"]
        candle_color = "GREEN" if candle_close >= candle_open else "RED"
        ts_str       = datetime.fromtimestamp(c["ts"], tz=EST).strftime("%Y-%m-%d %I:%M %p")

        # ── Filters / Signal ─────────────────────────────────────────────────
        skip_reason = None
        side        = None

        if spread < EMA_SPREAD_MIN:
            skip_reason = "spread_low"
        elif slope > SLOPE_CONFLICT_MIN and ef < es:
            skip_reason = "slope_conflict"
        elif slope < -SLOPE_CONFLICT_MIN and ef > es:
            skip_reason = "slope_conflict"
        elif el > es:
            skip_reason = "long>slow"
        elif USE_EMA_CROSS and ef < es:
            skip_reason = "bearish_cross"
        elif ef > es:
            side = "YES"
        elif ef < es:
            side = "NO"
        else:
            skip_reason = "ema_equal"

        if skip_reason:
            skipped += 1
            rows_printed += 1
            if rows_printed % REPEAT_HEADER_EVERY == 0:
                print(f"\n{HEADER}\n")
            print(f"  {ts_str:<22} ${candle_open:>12,.2f} ${candle_close:>12,.2f} {candle_color:<7} "
                  f"${ef:>11,.2f} ${es:>11,.2f} ${el:>11,.2f} "
                  f"${spread:>8,.2f} {slope:>8.2f} {'—':<5} "
                  f"{skip_reason:<18} {'—':<5} {mg_round:>3} {'—':>5} {'—':>9} {'—':>9} {'—':>12}")
            continue

        # ── Determine result ──────────────────────────────────────────────────
        if candle_close > candle_open:
            actual = "yes"
        elif candle_close < candle_open:
            actual = "no"
        else:
            skipped += 1
            continue  # doji — skip

        current_round = mg_round
        contracts     = MG_CONTRACTS.get(mg_round, MG_CONTRACTS[MG_MAX_ROUND])
        cost          = contracts * ENTRY_PRICE
        payout        = contracts * 1.00

        if side.lower() == actual:
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
                if MG_RESET_ON_MAX_LOSS:
                    mg_round = 1
            else:
                mg_round += 1
            profit_str, loss_str, win_str = "—", f"-${loss:,.2f}", "LOSS"

        net_str = f"{'+'if running_net>=0 else ''}${running_net:,.2f}"
        rows_printed += 1
        if rows_printed % REPEAT_HEADER_EVERY == 0:
            print(f"\n{HEADER}\n")
        print(f"  {ts_str:<22} ${candle_open:>12,.2f} ${candle_close:>12,.2f} {candle_color:<7} "
              f"${ef:>11,.2f} ${es:>11,.2f} ${el:>11,.2f} "
              f"${spread:>8,.2f} {slope:>8.2f} {side:<5} "
              f"{'—':<18} {win_str:<5} {current_round:>3} {contracts:>5} {profit_str:>9} {loss_str:>9} {net_str:>12}")

    # ── Summary ───────────────────────────────────────────────────────────────
    total_trades = wins + losses
    win_rate     = (wins / total_trades * 100) if total_trades else 0
    net          = total_profit - total_loss

    print(f"\n  {'─'*175}")
    print(f"  Trades: {total_trades:,}  |  Wins: {wins:,}  |  Losses: {losses:,}  |  Skipped: {skipped:,}  |  Win Rate: {win_rate:.1f}%")
    print(f"  Max Consec Wins: {max_consecutive_wins}  |  Max Consec Losses: {max_consecutive_losses}  |  MG Round Hits: {max_round_hit}")
    print(f"  Total Profit: +${total_profit:,.2f}  |  Total Loss: -${total_loss:,.2f}  |  Net P&L: {'+'if net>=0 else ''}${net:,.2f}")
    print(f"{'='*175}\n")


if __name__ == "__main__":
    main()
