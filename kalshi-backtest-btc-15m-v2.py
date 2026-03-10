"""
kalshi-backtest-btc-15m-v2.py — BTC 15m backtest using Kalshi strike prices + Kraken close prices.

- Strike        : Kalshi oracle strike price (from yes_sub_title)
- Close price   : Kraken candle close at Kalshi market close_time
- Win/Loss      : close > strike → YES wins | close < strike → NO wins
- EMA signals   : calculated from Kraken candles up to Kalshi open_time
- MG tracking   : simulated locally as a variable
- Candle source : local candles.db (362k candles, 2013–2026)

Usage:
  python kalshi-backtest-btc-15m-v2.py
"""

import sys
import sqlite3
sys.path.insert(0, ".")

from pathlib import Path
from datetime import datetime, timezone, timedelta
import connectors.kalshi as kalshi

CANDLES_DB = Path(__file__).parent / "candles.db"

# ── Config ────────────────────────────────────────────────────────────────────
BACKTEST_RANGE         = 7518      # number of finalized Kalshi markets to backtest (max ~84 days)

EMA_FAST               = 9
EMA_SLOW               = 21
EMA_LONG               = 50
EMA_SPREAD_MIN         = 10.0
SLOPE_CONFLICT_MIN     = 55.0

MG_CONTRACTS           = {1: 2, 2: 4, 3: 8, 4: 16, 5: 32, 6: 64, 7: 128}
MG_MAX_ROUND           = 7
MG_RESET_ON_MAX_LOSS   = False   # True = cut losses at max round and reset to round 1
ENTRY_PRICE            = 0.50
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


# ── Parse strike from yes_sub_title ──────────────────────────────────────────
def _parse_strike(sub_title: str):
    try:
        raw = sub_title.replace("Price to beat:", "").replace("$", "").replace(",", "").strip()
        return float(raw)
    except Exception:
        return None

# ── Fetch Kalshi finalized markets ───────────────────────────────────────────
def fetch_kalshi_markets(n: int) -> list:
    """Fetch last N finalized KXBTC15M markets with valid strike + result, oldest first."""
    markets = []
    cursor = None
    print(f"  Fetching Kalshi markets...", end="", flush=True)
    while True:
        params = {"series_ticker": "KXBTC15M", "limit": 100}
        if cursor:
            params["cursor"] = cursor
        data = kalshi._get("/markets", params=params)
        batch = data.get("markets", [])
        if not batch:
            break
        for m in batch:
            if m.get("status") != "finalized":
                continue
            if not m.get("result") in ("yes", "no"):
                continue
            strike = _parse_strike(m.get("yes_sub_title", ""))
            if strike is None:
                continue
            if not m.get("open_time") or not m.get("close_time"):
                continue
            markets.append({
                "ticker":     m["ticker"],
                "open_time":  m["open_time"],
                "close_time": m["close_time"],
                "strike":     strike,
                "result":     m["result"],
            })
            if len(markets) >= n + EMA_LONG + 5:
                break
        cursor = data.get("cursor")
        print(".", end="", flush=True)
        if len(markets) >= n + EMA_LONG + 5 or not cursor:
            break
    print(f" {len(markets)} fetched")
    # Sort oldest first
    markets.sort(key=lambda m: m["open_time"])
    return markets

# ── Load Kraken candles from local DB ────────────────────────────────────────
def fetch_kraken_candles() -> tuple:
    """
    Loads all 15m candles from local candles.db (362k candles, 2013–2026).
    Returns (lookup dict ts->candle, sorted candle list).
    """
    print(f"  Loading candles from DB...", end="", flush=True)
    conn = sqlite3.connect(CANDLES_DB)
    rows = conn.execute(
        "SELECT ts, open, high, low, close FROM btc_candles ORDER BY ts ASC"
    ).fetchall()
    conn.close()
    candles = [{"ts": r[0], "open": r[1], "high": r[2], "low": r[3], "close": r[4]} for r in rows]
    lookup = {c["ts"]: c for c in candles}
    first_dt = datetime.fromtimestamp(rows[0][0],  tz=timezone.utc).strftime("%Y-%m-%d")
    last_dt  = datetime.fromtimestamp(rows[-1][0], tz=timezone.utc).strftime("%Y-%m-%d")
    print(f" {len(candles):,} candles ({first_dt} → {last_dt})")
    return lookup, candles

# ── Main ──────────────────────────────────────────────────────────────────────
def main():
    now_est = datetime.now(EST).strftime("%Y-%m-%d %I:%M %p EST")
    print(f"\n{'='*160}")
    print(f"  BTC 15m Backtest v2 (Kalshi Strike + Kraken Close) — {now_est}")
    reset_str = "CutLoss@MaxRound" if MG_RESET_ON_MAX_LOSS else "StayAtMaxRound"
    print(f"  Range: {BACKTEST_RANGE} markets | EMA{EMA_FAST}/{EMA_SLOW} | Spread≥${EMA_SPREAD_MIN} | SlopeConflict>{SLOPE_CONFLICT_MIN} | MG: {reset_str}")
    print(f"{'='*160}\n")

    kalshi_markets = fetch_kalshi_markets(BACKTEST_RANGE + EMA_LONG + 5)

    lookup, candles = fetch_kraken_candles()

    # Build sorted list of candle timestamps for EMA lookback
    kraken_ts_sorted = sorted(lookup.keys())

    # Use last BACKTEST_RANGE Kalshi markets
    test_markets = kalshi_markets[-(BACKTEST_RANGE):]

    print(f"\n  Using {len(test_markets)} Kalshi markets | {len(lookup)} Kraken candles")
    print(f"  Range: {test_markets[0]['open_time']} → {test_markets[-1]['close_time']}\n")

    # ── Header ────────────────────────────────────────────────────────────────
    print(f"  {'DATE/TIME':<22} {'STRIKE':>12} {'OPEN':>12} {'CLOSE':>12} {'COLOR':<7} {'EMA_FAST':>12} {'EMA_SLOW':>12} {'EMA_LONG':>12} {'SPREAD':>10} {'SLOPE':>10} {'SIDE':<5} {'KR_CLOSE':>12} {'RESULT':<7} {'PROFIT':>8} {'LOSS':>8} {'MG':>4} {'K':>4}")
    print(f"  {'-'*188}")

    # ── Backtest state ────────────────────────────────────────────────────────
    mg_round           = 1
    total_profit       = 0.0
    total_loss         = 0.0
    wins               = 0
    losses             = 0
    skipped            = 0
    no_data            = 0

    for mkt in test_markets:
        open_dt  = datetime.fromisoformat(mkt["open_time"].replace("Z", "+00:00"))
        close_dt = datetime.fromisoformat(mkt["close_time"].replace("Z", "+00:00"))
        ts_str   = open_dt.astimezone(EST).strftime("%Y-%m-%d %I:%M %p")
        strike   = mkt["strike"]
        result   = mkt["result"]   # actual Kalshi result

        # Kraken: candle that closes at Kalshi close_time has ts = close_time - 15min
        close_candle_ts = int((close_dt - timedelta(minutes=15)).timestamp())
        close_candle    = lookup.get(close_candle_ts)

        # Kraken: candles available up to (not including) Kalshi open_time for EMA
        open_ts = int(open_dt.timestamp())
        ema_candles = [lookup[ts] for ts in kraken_ts_sorted if ts < open_ts and ts in lookup]

        if not close_candle or len(ema_candles) < EMA_LONG + 1:
            no_data += 1
            print(f"  {ts_str:<22} ${strike:>11,.2f} {'—':>12} {'—':>12} {'—':<7} {'—':>12} {'—':>12} {'—':>12} {'—':>10} {'—':>10} {'N/A':<5} {'—':>12} {result.upper():<7} {'—':>8} {'—':>8} {mg_round:>4} {'—':>4}")
            continue

        btc_close    = close_candle["close"]
        candle_open  = close_candle["open"]
        candle_close = close_candle["close"]
        candle_color = "GREEN" if candle_close > candle_open else "RED"
        closes    = [c["close"] for c in ema_candles]

        ema_fast_vals = _ema(closes, EMA_FAST)
        ema_slow_vals = _ema(closes, EMA_SLOW)
        ema_long_vals = _ema(closes, EMA_LONG)
        if not ema_fast_vals or not ema_slow_vals or not ema_long_vals:
            no_data += 1
            continue

        ema9      = ema_fast_vals[-1]
        ema21     = ema_slow_vals[-1]
        ema50     = ema_long_vals[-1]
        ema9_prev = ema_fast_vals[-2] if len(ema_fast_vals) >= 2 else ema9
        slope     = ema9 - ema9_prev
        spread    = abs(ema9 - ema21)
        side      = None

        # ── Filters / Signal ─────────────────────────────────────────────────
        btc_current = closes[-1]  # most recent price before cycle open
        skip_reason = None
        if spread < EMA_SPREAD_MIN:
            skip_reason = "spread_filter"
        elif slope > SLOPE_CONFLICT_MIN and ema9 < ema21:
            skip_reason = "slope_conflict"
        elif slope < -SLOPE_CONFLICT_MIN and ema9 > ema21:
            skip_reason = "slope_conflict"
        elif ema9 > ema21:
            side = "YES"
        elif ema9 < ema21:
            side = "NO"
        else:
            skip_reason = "ema_equal"



        if skip_reason:
            skipped += 1
            print(f"  {ts_str:<22} ${strike:>11,.2f} ${candle_open:>11,.2f} ${candle_close:>11,.2f} {candle_color:<7} ${ema9:>11,.2f} ${ema21:>11,.2f} ${ema50:>11,.2f} ${spread:>9,.2f} {slope:>10.2f} {'NULL':<5} ${btc_close:>11,.2f} {result.upper():<7} {'—':>8} {'—':>8} {mg_round:>4} {'—':>4}")
            continue

        # ── Result ───────────────────────────────────────────────────────────
        current_round = mg_round
        contracts     = MG_CONTRACTS.get(mg_round, 128)
        cost          = contracts * ENTRY_PRICE
        payout        = contracts * 1.00

        # Win: signal matches Kalshi result
        if side.lower() == result:
            profit = payout - cost
            loss   = 0.0
            total_profit += profit
            wins += 1
            mg_round = 1
            profit_str = f"+${profit:.2f}"
            loss_str   = "—"
        else:
            profit = 0.0
            loss   = cost
            total_loss += loss
            losses += 1
            if MG_RESET_ON_MAX_LOSS and mg_round >= MG_MAX_ROUND:
                mg_round = 1  # cut losses at max round, reset
            else:
                mg_round = min(mg_round + 1, MG_MAX_ROUND)
            profit_str = "—"
            loss_str   = f"-${loss:.2f}"

        print(f"  {ts_str:<22} ${strike:>11,.2f} ${candle_open:>11,.2f} ${candle_close:>11,.2f} {candle_color:<7} ${ema9:>11,.2f} ${ema21:>11,.2f} ${ema50:>11,.2f} ${spread:>9,.2f} {slope:>10.2f} {side:<5} ${btc_close:>11,.2f} {result.upper():<7} {profit_str:>8} {loss_str:>8} {current_round:>4} {contracts:>4}")

    # ── Summary ───────────────────────────────────────────────────────────────
    net = total_profit - total_loss
    total_trades = wins + losses
    win_rate = (wins / total_trades * 100) if total_trades else 0

    print(f"\n  {'─'*188}")
    print(f"  Trades: {total_trades}  |  Wins: {wins}  |  Losses: {losses}  |  Skipped: {skipped}  |  No Data: {no_data}  |  Win Rate: {win_rate:.1f}%")
    print(f"  Total Profit: +${total_profit:.2f}  |  Total Loss: -${total_loss:.2f}  |  Net P&L: {'+'if net>=0 else ''}${net:.2f}")
    print(f"{'='*160}\n")


if __name__ == "__main__":
    main()
