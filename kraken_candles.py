"""
kraken_candles.py — Pull recent BTC/USD 15m candles from Kraken and display OHLC prices.
"""

from connectors.kraken import get_ohlcv

CANDLESTICK_RANGE = 672  # number of candles to display 672 is a week


def fmt(price: float) -> str:
    return "${:,.2f}".format(price)


def main():
    candles = get_ohlcv("XBTUSD", interval_minutes=15)
    recent = candles[-CANDLESTICK_RANGE:]
    red_count = 0
    green_count = 0


    print(f"\n{'─'*70}")
    print(f"  BTC/USD 15m Candles — Last {CANDLESTICK_RANGE}")
    print(f"{'─'*70}")
    print(f"  {'#':<4}  {'TIME':<20}  {'OPEN':>12}  {'HIGH':>12}  {'LOW':>12}  {'CLOSE':>12}  {'COLOR':<6}")
    print(f"{'─'*78}")

    for i, c in enumerate(recent, 1):
        from datetime import datetime, timezone
        ts = datetime.fromtimestamp(c["ts"], tz=timezone.utc).strftime("%Y-%m-%d %H:%M")
        color = "GREEN" if c["close"] >= c["open"] else "RED"

        if color == "GREEN":
            green_count += 1
        else:
            red_count += 1

        print(f"  {i:<4}  {ts:<20}  {fmt(c['open']):>12}  {fmt(c['high']):>12}  {fmt(c['low']):>12}  {fmt(c['close']):>12}  {color:<6}")

    print(f"{'─'*70}\n")
    print("RED", red_count)
    print("GREEN", green_count)

if __name__ == "__main__":
    main()
