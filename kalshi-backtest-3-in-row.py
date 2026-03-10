"""
kalshi-backtest-3-in-row.py — 3-in-a-row exhaustion backtest.

Data source flag:
  USE_DB = True   → load from candles.db (local SQLite, 12+ years of history)
  USE_DB = False  → fetch live from Kraken API (recent candles only, ~720 max)
"""

import sqlite3
from connectors.kraken import get_ohlcv

# ── Data source ───────────────────────────────────────────────────────────────
USE_DB  = True   # True = candles.db | False = Kraken live API
DB_PATH = "/Users/roberthenning/python-projects/kalshi-bot-v2/candles.db"
# ─────────────────────────────────────────────────────────────────────────────

CANDLESTICK_RANGE = 35040  # number of candles to evaluate (672 = 1 week); set to 0 for ALL


def _load_candles() -> list:
    if USE_DB:
        conn = sqlite3.connect(DB_PATH)
        cur  = conn.cursor()
        cur.execute("SELECT ts, open, high, low, close FROM btc_candles ORDER BY ts ASC")
        rows = cur.fetchall()
        conn.close()
        # Normalise to same dict format as Kraken connector
        return [{"ts": r[0], "open": r[1], "high": r[2], "low": r[3], "close": r[4]} for r in rows]
    else:
        return get_ohlcv("XBTUSD", interval_minutes=15)


def fmt(price: float) -> str:
    return "${:,.2f}".format(price)


def main():
    all_candles = _load_candles()
    source_label = f"candles.db ({len(all_candles):,} candles)" if USE_DB else f"Kraken API ({len(all_candles):,} candles)"
    print(f"\n[Source: {source_label}]")

    candles = all_candles[-CANDLESTICK_RANGE:] if CANDLESTICK_RANGE > 0 else all_candles


    recent = candles

    red_count = 0
    green_count = 0

    wins = 0
    losses = 0

    streak_color = None
    streak_len = 0
    awaiting_result = False

    print(f"\n{'─'*70}")
    print(f"  BTC/USD 15m Candles — {len(recent):,} candles")
    print(f"{'─'*70}")
    print(f"  {'#':<4}  {'TIME':<20}  {'OPEN':>12}  {'HIGH':>12}  {'LOW':>12}  {'CLOSE':>12}  {'COLOR':<6}  {'RESULT':<6}")
    print(f"{'─'*78}")

    for i, c in enumerate(recent, 1):
        from datetime import datetime, timezone

        ts = datetime.fromtimestamp(c["ts"], tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
        color = "GREEN" if c["close"] >= c["open"] else "RED"

        result = ""

        # Count colors
        if color == "GREEN":
            green_count += 1
        else:
            red_count += 1

        # If waiting for the 4th candle
        if awaiting_result:

            if streak_color == "RED":
                if color == "GREEN":
                    result = "WIN"
                    wins += 1
                else:
                    result = "LOSS"
                    losses += 1

            elif streak_color == "GREEN":
                if color == "RED":
                    result = "WIN"
                    wins += 1
                else:
                    result = "LOSS"
                    losses += 1

            # Reset after resolving bet
            awaiting_result = False
            streak_len = 1
            streak_color = color

        else:
            # Continue streak
            if color == streak_color:
                streak_len += 1
            else:
                streak_color = color
                streak_len = 1

            # If we hit 3 in a row, next candle decides outcome
            if streak_len == 3:
                awaiting_result = True

        print(f"  {i:<4}  {ts:<20}  {fmt(c['open']):>12}  {fmt(c['high']):>12}  {fmt(c['low']):>12}  {fmt(c['close']):>12}  {color:<6}  {result:<6}")

    print(f"{'─'*70}\n")
    print(f"   CANDLES: {len(recent):,}")
    print("       RED:", red_count)
    print("     GREEN:", green_count)
    print("      WINS:", wins)
    print("    LOSSES:", losses)
    net_wins = wins - losses
    win_rate = wins / (wins + losses) * 100
    print(f"  WIN RATE: {win_rate:.1f}%")
    net_profit = (net_wins * 5) * .50
    print("  NET WINS:", net_wins)
    print("NET Profit:", net_profit)


if __name__ == "__main__":
    main()