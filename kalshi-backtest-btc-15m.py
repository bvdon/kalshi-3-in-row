"""
kalshi-backtest-btc-15m.py — BTC 15-minute backtest using Kraken OHLCV data.

Strategy mirrors btc_15m_updown_mg.py:
  - Strike  = candle OPEN price
  - Signal  = EMA9 vs EMA21 direction (YES = bullish, NO = bearish)
  - Win/Loss = CLOSE > OPEN → YES wins | CLOSE < OPEN → NO wins
  - Filters = EMA spread min, slope conflict
  - MG      = Martingale rounds track across candles

Usage:
  python kalshi-backtest-btc-15m.py
"""

import sys
sys.path.insert(0, ".")

from datetime import datetime, timezone, timedelta
import connectors.kraken as kraken

# ── Config ────────────────────────────────────────────────────────────────────
KRAKEN_PAIR            = "XBTUSD"
INTERVAL_MINUTES       = 15
BACKTEST_RANGE         = 672       # number of 15-minute candles to backtest. 96 is one day. 672 is 7 days. 2688 is 4 weeks

EMA_FAST               = 9
EMA_SLOW               = 21
EMA_SPREAD_MIN         = 15.0      # min $ spread between EMA9 and EMA21
SLOPE_CONFLICT_MIN     = 50.0      # skip if slope > this AND EMA9 < EMA21

MG_CONTRACTS           = {1: 4, 2: 8, 3: 16, 4: 32, 5: 64, 6: 128, 7: 256}
ENTRY_PRICE            = 0.50      # assume 50¢ entry for P&L calc
EST                    = timezone(timedelta(hours=-5))

# ── EMA ───────────────────────────────────────────────────────────────────────
def _ema(values: list, period: int) -> list:
    if len(values) < period:
        return []
    k = 2 / (period + 1)
    ema_vals = [sum(values[:period]) / period]
    for v in values[period:]:
        ema_vals.append(v * k + ema_vals[-1] * (1 - k))
    return ema_vals

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    # Fetch enough candles: BACKTEST_RANGE + EMA_SLOW warmup + 1 buffer
    total_needed = BACKTEST_RANGE + EMA_SLOW + 2
    candles = kraken.get_ohlcv(KRAKEN_PAIR, interval_minutes=INTERVAL_MINUTES)

    # Drop the last (in-progress) candle
    candles = candles[:-1]

    if len(candles) < total_needed:
        print(f"Not enough candles: got {len(candles)}, need {total_needed}")
        return

    # Use the last `total_needed` candles
    candles = candles[-total_needed:]

    # Backtest state
    mg_round           = 1
    consecutive_losses = 0
    total_profit       = 0.0
    total_loss         = 0.0
    wins               = 0
    losses             = 0
    skipped            = 0

    print(f"\n{'='*150}")
    print(f"  BTC 15m Backtest — {BACKTEST_RANGE} candles | EMA{EMA_FAST}/{EMA_SLOW} | Spread≥${EMA_SPREAD_MIN} | SlopeConflict>{SLOPE_CONFLICT_MIN}")
    print(f"{'='*150}")
    print(f"  {'DATE/TIME':<22} {'STRIKE':>12} {'EMA_FAST':>12} {'EMA_SLOW':>12} {'SPREAD':>10} {'SLOPE':>10} {'SIDE':<5} {'OPEN':>12} {'CLOSE':>12} {'PROFIT':>8} {'LOSS':>8} {'MG':>4} {'K':>4}")
    print(f"  {'-'*140}")

    for i in range(EMA_SLOW + 1, len(candles)):
        candle = candles[i]
        closes = [c["close"] for c in candles[:i]]

        ema_fast_vals = _ema(closes, EMA_FAST)
        ema_slow_vals = _ema(closes, EMA_SLOW)
        if not ema_fast_vals or not ema_slow_vals:
            skipped += 1
            continue

        ema9      = ema_fast_vals[-1]
        ema21     = ema_slow_vals[-1]
        ema9_prev = ema_fast_vals[-2] if len(ema_fast_vals) >= 2 else ema9
        slope     = ema9 - ema9_prev
        spread    = abs(ema9 - ema21)

        strike    = candle["open"]
        close     = candle["close"]
        ts        = datetime.fromtimestamp(candle["ts"], tz=EST)
        ts_str    = ts.strftime("%Y-%m-%d %I:%M %p")
        side      = None

        # ── Filters / Signal ──────────────────────────────────────────────────
        skip_reason = None
        if spread < EMA_SPREAD_MIN:
            skip_reason = f"spread<{EMA_SPREAD_MIN}"
        elif slope > SLOPE_CONFLICT_MIN and ema9 < ema21:
            skip_reason = f"slope_conflict"
        elif ema9 > ema21:
            side = "YES"
        elif ema9 < ema21:
            side = "NO"
        else:
            skip_reason = "ema_equal"

        if skip_reason:
            skipped += 1
            print(f"  {ts_str:<22} ${strike:>11,.2f} ${ema9:>11,.2f} ${ema21:>11,.2f} ${spread:>9,.2f} {slope:>10.2f} {'NULL':<5} ${strike:>11,.2f} ${close:>11,.2f} {'—':>8} {'—':>8} {mg_round:>4} {'—':>4}")
            continue

        # ── Result ────────────────────────────────────────────────────────────
        current_round = mg_round
        contracts = MG_CONTRACTS.get(mg_round, 256)
        cost      = contracts * ENTRY_PRICE
        payout    = contracts * 1.00   # $1 per contract if win

        if (side == "YES" and close > strike) or (side == "NO" and close < strike):
            profit = payout - cost
            loss   = 0.0
            total_profit += profit
            wins += 1
            mg_round = 1
            consecutive_losses = 0
            profit_str = f"+${profit:.2f}"
            loss_str   = "—"
        else:
            profit = 0.0
            loss   = cost
            total_loss += loss
            losses += 1
            consecutive_losses += 1
            mg_round = min(mg_round + 1, 7)
            profit_str = "—"
            loss_str   = f"-${loss:.2f}"

        print(f"  {ts_str:<22} ${strike:>11,.2f} ${ema9:>11,.2f} ${ema21:>11,.2f} ${spread:>9,.2f} {slope:>10.2f} {side:<5} ${strike:>11,.2f} ${close:>11,.2f} {profit_str:>8} {loss_str:>8} {current_round:>4} {contracts:>4}")

    # ── Summary ───────────────────────────────────────────────────────────────
    net = total_profit - total_loss
    total_trades = wins + losses
    win_rate = (wins / total_trades * 100) if total_trades else 0

    print(f"\n  {'─'*140}")
    print(f"  Trades: {total_trades}  |  Wins: {wins}  |  Losses: {losses}  |  Skipped: {skipped}  |  Win Rate: {win_rate:.1f}%")
    print(f"  Total Profit: +${total_profit:.2f}  |  Total Loss: -${total_loss:.2f}  |  Net P&L: {'+'if net>=0 else ''}${net:.2f}")
    print(f"{'='*150}\n")


if __name__ == "__main__":
    main()
