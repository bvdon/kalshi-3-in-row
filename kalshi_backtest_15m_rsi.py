"""
kalshi_backtest_15m_rsi.py — 15m RSI Only strategy backtest.

Strategy (mirrors markets/kalshi_15m_rsi.py exactly):
  - Compute 14-period Wilder RSI on fully closed 15m candles
  - RSI > 60 (overbought) → bet NO (expect reversal down)
  - RSI < 40 (oversold)   → bet YES (expect reversal up)
  - Otherwise             → no trade

Re-fire guard: once a signal fires on candle[i], no new signal until the
RSI leaves the extreme zone (i.e. next trigger must be on a fresh candle
where the prior candle was neutral). Mirrors last_trigger_ts logic in live bot.

Data source flag:
  USE_DB = True   → load from candles.db (local SQLite, 12+ years of history)
  USE_DB = False  → fetch live from Kraken API (recent candles only, ~720 max)
"""

import sqlite3
import numpy as np
from datetime import datetime, timezone, timedelta
from connectors.kraken import get_ohlcv

# ── Data source ───────────────────────────────────────────────────────────────
USE_DB  = True
DB_PATH = "/Users/roberthenning/python-projects/kalshi-bot-v2/candles.db"
# ─────────────────────────────────────────────────────────────────────────────

CANDLESTICK_RANGE = 35040   # number of candles to evaluate; 0 = ALL
                           # 672 = 1 week, 2880 = 1 month, 0 = full history

# RSI parameters (must match live bot)
RSI_PERIOD = 14
RSI_OB     = 60    # overbought → bet NO
RSI_OS     = 40    # oversold   → bet YES

# P&L per trade (paper sizing: 10 contracts @ $0.50 entry, $1.00 payout)
CONTRACTS    = 80 # 10 contracts for 4 markets (BTC, ETH, SOL, XRP)
ENTRY_PRICE  = 0.50
PAYOUT_EACH  = 1.00
WIN_PROFIT   = CONTRACTS * (PAYOUT_EACH - ENTRY_PRICE)   # $5.00
LOSS_AMOUNT  = CONTRACTS * ENTRY_PRICE                   # $5.00


def _load_candles() -> list:
    if USE_DB:
        conn = sqlite3.connect(DB_PATH)
        cur  = conn.cursor()
        cur.execute("SELECT ts, open, high, low, close FROM btc_candles ORDER BY ts ASC")
        rows = cur.fetchall()
        conn.close()
        return [{"ts": r[0], "open": r[1], "high": r[2], "low": r[3], "close": r[4]} for r in rows]
    else:
        return get_ohlcv("XBTUSD", interval_minutes=15)


def _compute_rsi(closes: list, period: int = RSI_PERIOD) -> list:
    """Standard Wilder RSI. Values before index `period` are 50.0 (neutral)."""
    n   = len(closes)
    arr = np.array(closes, dtype=float)
    rsi = np.full(n, 50.0)

    gains  = np.zeros(n)
    losses = np.zeros(n)
    for i in range(1, n):
        diff = arr[i] - arr[i - 1]
        if diff > 0:
            gains[i] = diff
        else:
            losses[i] = -diff

    avg_g = float(np.mean(gains[1:period + 1]))
    avg_l = float(np.mean(losses[1:period + 1]))

    for i in range(period, n):
        avg_g = (avg_g * (period - 1) + gains[i]) / period
        avg_l = (avg_l * (period - 1) + losses[i]) / period
        if avg_l == 0:
            rsi[i] = 100.0
        else:
            rs    = avg_g / avg_l
            rsi[i] = 100.0 - 100.0 / (1.0 + rs)

    return rsi.tolist()


def fmt(price: float) -> str:
    return "${:,.2f}".format(price)


def main():
    all_candles  = _load_candles()
    source_label = (f"candles.db ({len(all_candles):,} candles)"
                    if USE_DB else f"Kraken API ({len(all_candles):,} candles)")
    print(f"\n[Source: {source_label}]")

    candles = all_candles[-CANDLESTICK_RANGE:] if CANDLESTICK_RANGE > 0 else all_candles

    # Pre-compute RSI across the full slice (we need at least RSI_PERIOD prior candles)
    closes     = [c["close"] for c in candles]
    rsi_values = _compute_rsi(closes)

    wins           = 0
    losses         = 0
    no_trades      = 0
    net_pnl        = 0.0
    last_trigger_i = -1   # index of the candle that fired the last signal

    # Pending bet: set when signal fires, resolved by the *next* candle
    pending_signal = None   # "yes" | "no"
    pending_i      = None   # candle index the bet was placed on

    print(f"\n{'─'*96}")
    print(f"  BTC/USD 15m Candles — {len(candles):,} candles | RSI({RSI_PERIOD}) OB={RSI_OB} OS={RSI_OS}")
    print(f"{'─'*96}")
    print(f"  {'#':<5}  {'TIME':<18}  {'OPEN':>12}  {'CLOSE':>12}  {'RSI':>6}  {'COLOR':<6}  {'SIGNAL':<7}  {'RESULT':<6}  {'P&L':>7}")
    print(f"{'─'*96}")

    for i, c in enumerate(candles):
        ts    = datetime.fromtimestamp(c["ts"], tz=timezone(timedelta(hours=-5))).strftime("%Y-%m-%d %H:%M")
        color = "GREEN" if c["close"] >= c["open"] else "RED"
        rsi   = rsi_values[i]

        result_str = ""
        pnl_str    = ""
        signal_str = ""

        # ── Resolve pending bet from previous candle ──────────────────────────
        if pending_signal is not None:
            actual = "yes" if c["close"] > c["open"] else "no"
            if actual == pending_signal:
                wins    += 1
                net_pnl += WIN_PROFIT
                result_str = "WIN"
                pnl_str    = f"+${WIN_PROFIT:.2f}"
            else:
                losses  += 1
                net_pnl -= LOSS_AMOUNT
                result_str = "LOSS"
                pnl_str    = f"-${LOSS_AMOUNT:.2f}"
            pending_signal = None
            pending_i      = None

        # ── Evaluate RSI signal on this candle (acts on NEXT candle) ─────────
        # Re-fire guard: only allow a new signal if this is a fresh candle
        # (index > last_trigger_i, meaning the prior trigger has been resolved)
        if i >= RSI_PERIOD:
            if rsi > RSI_OB:
                raw_signal = "no"
            elif rsi < RSI_OS:
                raw_signal = "yes"
            else:
                raw_signal = None

            if raw_signal is not None and i > last_trigger_i:
                signal_str     = raw_signal.upper()
                pending_signal = raw_signal
                pending_i      = i
                last_trigger_i = i
            elif raw_signal is None:
                no_trades += 1

        rsi_display = f"{rsi:6.2f}"

        print(f"  {i+1:<5}  {ts:<18}  {fmt(c['open']):>12}  {fmt(c['close']):>12}  "
              f"{rsi_display}  {color:<6}  {signal_str:<7}  {result_str:<6}  {pnl_str:>7}")

    print(f"{'─'*96}\n")

    total_trades = wins + losses
    win_rate     = (wins / total_trades * 100) if total_trades > 0 else 0.0

    print(f"  Candles evaluated : {len(candles):,}")
    print(f"  Trades placed     : {total_trades}  (wins={wins}, losses={losses}, no-trade={no_trades})")
    print(f"  Win rate          : {win_rate:.1f}%")
    print(f"  Net P&L           : ${net_pnl:,.2f}")
    print()


if __name__ == "__main__":
    main()
