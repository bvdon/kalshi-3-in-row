"""
kalshi-backtest-v4.py — BTC 15m backtest with multi-timeframe EMA conditions.

YES only when ALL true:
  - 15m EMA9 slope > 0
  - Last close > 15m EMA9
  - Last close > 15m EMA50
  - Last close > 1H EMA50

NO only when ALL true:
  - 15m EMA9 slope < 0
  - Last close < 15m EMA9
  - Last close < 15m EMA50
  - Last close < 1H EMA50

Otherwise: SKIP

"Last" = close price of the last fully closed candle.

Candle source : local candles.db (362k 15m candles, 2013–2026)
1H candles    : aggregated from 15m candles (4 × 15m per hour)

Usage:
  python kalshi-backtest-v4.py
"""

import sys
import sqlite3
sys.path.insert(0, ".")

from pathlib import Path
from datetime import datetime, timezone, timedelta

CANDLES_DB = Path(__file__).parent / "candles.db"

# ── Config ────────────────────────────────────────────────────────────────────
BACKTEST_RANGE  = 35040    # number of 15m candles (0 = ALL) | 8064 = ~84 days
EMA_FAST        = 21       # 15m EMA (slope + price filter)
EMA_SLOW        = 50      # 15m EMA (price filter)
EMA_1H          = 50      # 1H EMA (price filter)
CONTRACTS       = 20       # contracts per trade (flat, no MG)
REVERSE_SIGNAL  = True   # True = bet opposite of signal (NO when conditions bullish, YES when bearish)
ENTRY_PRICE     = 0.50
EST             = timezone(timedelta(hours=-5))


# ── EMA ───────────────────────────────────────────────────────────────────────
def _ema(values: list, period: int) -> list:
    if len(values) < period:
        return []
    k = 2 / (period + 1)
    result = [sum(values[:period]) / period]
    for v in values[period:]:
        result.append(v * k + result[-1] * (1 - k))
    return result


# ── Load 15m candles ──────────────────────────────────────────────────────────
def load_candles() -> list:
    print(f"  Loading 15m candles from DB...", end="", flush=True)
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


# ── Aggregate 15m → 1H candles ────────────────────────────────────────────────
def aggregate_1h(candles_15m: list) -> list:
    hourly = {}
    for c in candles_15m:
        h_ts = (c["ts"] // 3600) * 3600
        if h_ts not in hourly:
            hourly[h_ts] = {"ts": h_ts, "open": c["open"], "close": c["close"], "count": 1}
        else:
            hourly[h_ts]["close"] = c["close"]
            hourly[h_ts]["count"] += 1
    complete = [v for v in hourly.values() if v["count"] == 4]
    complete.sort(key=lambda x: x["ts"])
    print(f"  Aggregated {len(complete):,} complete 1H candles")
    return complete


# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    now_est = datetime.now(EST).strftime("%Y-%m-%d %I:%M %p EST")

    all_candles = load_candles()
    all_candles = all_candles[:-1]  # drop in-progress candle

    candles_1h = aggregate_1h(all_candles)

    if BACKTEST_RANGE and BACKTEST_RANGE < len(all_candles):
        candles = all_candles[-BACKTEST_RANGE:]
    else:
        candles = all_candles

    first_dt  = datetime.fromtimestamp(candles[0]["ts"],  tz=timezone.utc).strftime("%Y-%m-%d")
    last_dt   = datetime.fromtimestamp(candles[-1]["ts"], tz=timezone.utc).strftime("%Y-%m-%d")
    start_idx = len(all_candles) - len(candles)

    # ── Pre-compute EMAs ──────────────────────────────────────────────────────
    all_closes_15m = [c["close"] for c in all_candles]
    ema_fast_all   = _ema(all_closes_15m, EMA_FAST)
    ema_slow_all   = _ema(all_closes_15m, EMA_SLOW)

    closes_1h     = [c["close"] for c in candles_1h]
    ema_1h_all    = _ema(closes_1h, EMA_1H)
    ema_1h_lookup = {}
    for i, c in enumerate(candles_1h):
        ei = i - (EMA_1H - 1)
        if ei >= 0:
            ema_1h_lookup[c["ts"]] = ema_1h_all[ei]

    fast_offset = EMA_FAST - 1
    slow_offset = EMA_SLOW - 1

    # ── Header ────────────────────────────────────────────────────────────────
    print(f"\n{'='*170}")
    print(f"  BTC 15m Backtest v4 — Multi-TF EMA | {now_est}")
    print(f"  {len(candles):,} candles | {first_dt} → {last_dt}")
    print(f"  YES: slope>0 + last>EMA{EMA_FAST}_15m + last>EMA{EMA_SLOW}_15m + last>EMA{EMA_1H}_1H")
    print(f"  NO:  slope<0 + last<EMA{EMA_FAST}_15m + last<EMA{EMA_SLOW}_15m + last<EMA{EMA_1H}_1H")
    print(f"  Flat sizing: {CONTRACTS} contracts per trade | REVERSE_SIGNAL={REVERSE_SIGNAL}")
    print(f"{'='*170}\n")

    HEADER = (f"  {'DATE/TIME':<22} {'LAST':>12} {'OPEN':>12} {'CLOSE':>12} {'COLOR':<6} "
              f"{'EMA9_15M':>11} {'EMA50_15M':>11} {'EMA50_1H':>11} "
              f"{'SLOPE':>8} {'SIDE':<5} {'SKIP?':<18} "
              f"{'WIN?':<5} {'PROFIT':>9} {'LOSS':>9} {'NET':>12}")
    print(HEADER)
    print(f"  {'-'*170}")
    REPEAT_HEADER_EVERY = 100

    # ── State ─────────────────────────────────────────────────────────────────
    total_profit           = 0.0
    total_loss             = 0.0
    wins = losses = skipped = 0
    running_net            = 0.0
    consecutive_losses     = 0
    max_consecutive_losses = 0
    consecutive_wins       = 0
    max_consecutive_wins   = 0
    rows_printed           = 0

    for i, c in enumerate(candles):
        idx          = start_idx + i
        ts           = c["ts"]
        candle_open  = c["open"]
        candle_close = c["close"]
        candle_color = "GREEN" if candle_close >= candle_open else "RED"
        ts_str       = datetime.fromtimestamp(ts, tz=EST).strftime("%Y-%m-%d %I:%M %p")

        # ── 15m EMA (CANDLE_OFFSET = -1, no lookahead) ───────────────────────
        fi = idx - fast_offset - 1
        si = idx - slow_offset - 1

        if fi < 1 or si < 0:
            skipped += 1
            continue

        ef      = ema_fast_all[fi]
        ef_prev = ema_fast_all[fi - 1]
        es      = ema_slow_all[si]
        slope   = ef - ef_prev

        # ── 1H EMA50 (last completed 1H before this candle) ──────────────────
        last_1h_ts = (ts // 3600) * 3600 - 3600
        ema_1h_val = ema_1h_lookup.get(last_1h_ts)

        # Last close = close of the previous fully closed candle
        last_close = all_candles[idx - 1]["close"]

        def _row(side, skip_reason, win_str, profit_str, loss_str, net_str):
            nonlocal rows_printed
            rows_printed += 1
            if rows_printed % REPEAT_HEADER_EVERY == 0:
                print(f"\n{HEADER}\n")
            e1h = f"${ema_1h_val:>10,.2f}" if ema_1h_val is not None else f"{'—':>11}"
            print(f"  {ts_str:<22} ${last_close:>11,.2f} ${candle_open:>11,.2f} ${candle_close:>11,.2f} {candle_color:<6} "
                  f"${ef:>10,.2f} ${es:>10,.2f} {e1h} "
                  f"{slope:>8.2f} {side:<5} {skip_reason:<18} "
                  f"{win_str:<5} {profit_str:>9} {loss_str:>9} {net_str:>12}")

        if ema_1h_val is None:
            skipped += 1
            _row("—", "no_1h_ema", "—", "—", "—", "—")
            continue

        # ── Signal logic ──────────────────────────────────────────────────────
        yes_cond = slope > 0 and last_close > ef and last_close > es and last_close > ema_1h_val
        no_cond  = slope < 0 and last_close < ef and last_close < es and last_close < ema_1h_val

        if yes_cond:
            side = "NO" if REVERSE_SIGNAL else "YES"
        elif no_cond:
            side = "YES" if REVERSE_SIGNAL else "NO"
        else:
            skipped += 1
            _row("—", "no_signal", "—", "—", "—", "—")
            continue

        # ── Doji ─────────────────────────────────────────────────────────────
        if candle_close == candle_open:
            skipped += 1
            _row(side, "doji", "—", "—", "—", "—")
            continue

        # ── Result ───────────────────────────────────────────────────────────
        actual = "yes" if candle_close > candle_open else "no"
        cost   = CONTRACTS * ENTRY_PRICE
        payout = CONTRACTS * 1.00

        if side.lower() == actual:
            profit = payout - cost
            total_profit       += profit
            running_net        += profit
            wins               += 1
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
            profit_str, loss_str, win_str = "—", f"-${loss:,.2f}", "LOSS"

        net_str = f"{'+'if running_net>=0 else ''}${running_net:,.2f}"
        _row(side, "—", win_str, profit_str, loss_str, net_str)

    # ── Summary ───────────────────────────────────────────────────────────────
    total_trades = wins + losses
    win_rate     = (wins / total_trades * 100) if total_trades else 0
    net          = total_profit - total_loss

    print(f"\n  {'─'*170}")
    print(f"  Trades: {total_trades:,}  |  Wins: {wins:,}  |  Losses: {losses:,}  |  Skipped: {skipped:,}  |  Win Rate: {win_rate:.1f}%")
    print(f"  Max Consec Wins: {max_consecutive_wins}  |  Max Consec Losses: {max_consecutive_losses}")
    print(f"  Total Profit: +${total_profit:,.2f}  |  Total Loss: -${total_loss:,.2f}  |  Net P&L: {'+'if net>=0 else ''}${net:,.2f}")
    print(f"{'='*170}\n")


if __name__ == "__main__":
    main()
