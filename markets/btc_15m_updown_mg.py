"""
markets/btc_15m_updown_mg.py — BTC 15-minute Up/Down Martingale strategy (PAPER TRADE only).

Strategy:
  - One position per 15-minute Kalshi cycle
  - Direction: EMA 9 > EMA 21 → YES (UP) | EMA 9 < EMA 21 → NO (DOWN)
  - Hold till expiry — Kalshi settles automatically

Single cron job fires at :00 :15 :30 :45 and does everything in sequence:
  1. Resolve previous cycle result (poll every RESULT_POLL_DELAY seconds)
  2. Update mg_state
  3. If resolved within RESULT_TIMEOUT_SECONDS → enter new cycle immediately
  4. If not resolved in time → skip entry this cycle

PAPER_TRADE is read from config (PAPER_TRADE env var). Set to false in .env to go live.
"""

import json
import sqlite3
import threading
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

import config
import connectors.kalshi as kalshi
import connectors.kraken as kraken

EST = timezone(timedelta(hours=-5))

# ── Config ────────────────────────────────────────────────────────────────────
MARKET_SERIES          = "KXBTC15M"
PAPER_TRADE            = config.PAPER_TRADE
PAPER_COST             = 0.50
PAPER_PAYOUT           = 1.00
STRATEGY_NAME          = "MG_PAPER"
MG_CONTRACTS           = {1: 2, 2: 4, 3: 8, 4: 16, 5: 32, 6: 64, 7: 128}
MG_MAX_ROUND           = 7


EMA_FAST               = 9
EMA_SLOW               = 21
EMA_SPREAD_MIN         = 20.0   # min $ spread between EMA9 and EMA21

KRAKEN_PAIR            = "XBTUSD"

RESULT_POLL_DELAY      = 1      # seconds between finalization polls
RESULT_TIMEOUT_SECONDS = 120    # max seconds to wait for result before skipping entry

ENTRY_CUTOFF_SECONDS   = 120    # skip entry if >= this many seconds into the cycle
ENTRY_LIMIT_CENTS      = 60     # max limit price for live orders (fills at best available up to this)
TICKER_POLL_DELAY      = 2      # seconds between ticker polls

CANDLE_CONFIDENCE_THRESHOLD = 30.0  # min $ distance from strike to infer result from Kraken candle

MG_STATE_PATH   = Path(__file__).parent.parent / "mg_state.json"
STOP_FLAG_PATH  = Path(__file__).parent.parent / "bot_stopped.flag"
# ─────────────────────────────────────────────────────────────────────────────


# ── MG State ──────────────────────────────────────────────────────────────────

def _load_mg_state() -> dict:
    if MG_STATE_PATH.exists():
        try:
            with open(MG_STATE_PATH) as f:
                state = json.load(f)
            if "round" in state and "consecutive_losses" in state:
                return state
        except Exception:
            pass
    default = {"round": 1, "consecutive_losses": 0}
    _save_mg_state(default)
    return default


def _save_mg_state(state: dict) -> None:
    with open(MG_STATE_PATH, "w") as f:
        json.dump(state, f)


# ── EMA ───────────────────────────────────────────────────────────────────────

def _ema(values: list, period: int) -> list:
    if len(values) < period:
        return []
    k = 2 / (period + 1)
    ema_vals = [sum(values[:period]) / period]
    for v in values[period:]:
        ema_vals.append(v * k + ema_vals[-1] * (1 - k))
    return ema_vals


# ── DB ────────────────────────────────────────────────────────────────────────

def _db_insert_trade(
    start_time, market_ticker, side, mg_round, contracts,
    entry_price, total_cost, payout, strike_price, btc_price
) -> Optional[int]:
    try:
        conn = sqlite3.connect(config.DB_PATH)
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO kalshi_trades
                (start_time, market_ticker, side, martingale_round, contracts,
                 entry_price, total_cost, strategy, strike_price, btc_price, payout)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (start_time, market_ticker, side, mg_round, contracts,
             entry_price, total_cost, STRATEGY_NAME, strike_price, btc_price, payout),
        )
        conn.commit()
        row_id = cur.lastrowid
        conn.close()
        return row_id
    except Exception as e:
        print(f"[DB] Insert failed: {e}")
        return None


def _db_insert_no_trade(market_ticker: str, start_time: str, reason: str) -> None:
    try:
        conn = sqlite3.connect(config.DB_PATH)
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO kalshi_trades (start_time, market_ticker, strategy, no_trade)
            VALUES (?, ?, ?, ?)
            """,
            (start_time, market_ticker, STRATEGY_NAME, reason),
        )
        conn.commit()
        conn.close()
        print(f"[DB] No-trade recorded: {reason}")
    except Exception as e:
        print(f"[DB] Insert no-trade failed: {e}")


def _db_insert_signals(
    trade_id: int,
    start_time: str,
    ticker_data: str,
    ema9: Optional[float],
    ema21: Optional[float],
    spread: Optional[float],
    slope: Optional[float],
) -> None:
    try:
        conn = sqlite3.connect(config.DB_PATH)
        cur = conn.cursor()
        cur.execute(
            """
            INSERT INTO kalshi_trade_signals
                (trade_id, start_time, ticker_data, signal_1, signal_2, signal_3, signal_4)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                trade_id,
                start_time,
                ticker_data,
                f"EMA 9, ${ema9:,.2f}"   if ema9   is not None else None,
                f"EMA 21, ${ema21:,.2f}" if ema21  is not None else None,
                f"EMA Spread, ${spread:,.2f}" if spread is not None else None,
                f"EMA 9 Slope, {slope:,.2f}" if slope is not None else None,
            ),
        )
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"[DB] Insert signals failed: {e}")


def _db_update_entry_price(market_ticker: str, entry_price: float, total_cost: float) -> None:
    try:
        conn = sqlite3.connect(config.DB_PATH)
        cur = conn.cursor()
        cur.execute(
            "UPDATE kalshi_trades SET entry_price=?, total_cost=? WHERE market_ticker=? AND strategy=?",
            (entry_price, total_cost, market_ticker, STRATEGY_NAME),
        )
        conn.commit()
        conn.close()
        print(f"[DB]    Entry price updated: {market_ticker} @ {entry_price:.2f} total=${total_cost:.2f}")
    except Exception as e:
        print(f"[DB] Update entry price failed: {e}")


def _db_update_result(market_ticker: str, profit: float, loss: float) -> None:
    try:
        conn = sqlite3.connect(config.DB_PATH)
        cur = conn.cursor()
        cur.execute(
            "UPDATE kalshi_trades SET profit=?, loss=? WHERE market_ticker=? AND strategy=?",
            (profit, loss, market_ticker, STRATEGY_NAME),
        )
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"[DB] Update result failed: {e}")


def _db_get_latest_unresolved() -> Optional[dict]:
    """Get most recent trade with no profit/loss recorded."""
    try:
        conn = sqlite3.connect(config.DB_PATH)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute(
            """
            SELECT * FROM kalshi_trades
            WHERE strategy=? AND profit IS NULL AND loss IS NULL
            ORDER BY id DESC LIMIT 1
            """,
            (STRATEGY_NAME,),
        )
        row = cur.fetchone()
        conn.close()
        return dict(row) if row else None
    except Exception as e:
        print(f"[DB] Get unresolved failed: {e}")
        return None


def _db_get_open_trade(market_ticker: str) -> Optional[dict]:
    try:
        conn = sqlite3.connect(config.DB_PATH)
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        cur.execute(
            """
            SELECT * FROM kalshi_trades
            WHERE market_ticker=? AND strategy=? AND profit IS NULL AND loss IS NULL
            ORDER BY id DESC LIMIT 1
            """,
            (market_ticker, STRATEGY_NAME),
        )
        row = cur.fetchone()
        conn.close()
        return dict(row) if row else None
    except Exception as e:
        print(f"[DB] Get open trade failed: {e}")
        return None


# ── Signal ────────────────────────────────────────────────────────────────────

def _get_signal():
    try:
        candles = kraken.get_ohlcv(KRAKEN_PAIR, interval_minutes=15)
    except Exception as e:
        print(f"[{STRATEGY_NAME}] Kraken OHLCV fetch failed: {e}")
        return None, None, None

    closes = [c["close"] for c in candles]
    if len(closes) < EMA_SLOW + 1:
        print(f"[{STRATEGY_NAME}] Not enough candles for EMA{EMA_SLOW}")
        return None, None, None

    ema_fast_vals = _ema(closes, EMA_FAST)
    ema_slow_vals = _ema(closes, EMA_SLOW)
    if not ema_fast_vals or not ema_slow_vals:
        return None, None, None

    ema9  = ema_fast_vals[-1]
    ema21 = ema_slow_vals[-1]
    ema9_prev = ema_fast_vals[-2] if len(ema_fast_vals) >= 2 else ema9
    slope = ema9 - ema9_prev

    # ── EMA slope filter (stub) ───────────────────────────────────────────────
    # EMA_SLOPE_MIN = 0.0
    # if abs(slope) < EMA_SLOPE_MIN:
    #     return None, ema9, ema21, spread, slope
    # ─────────────────────────────────────────────────────────────────────────

    spread = abs(ema9 - ema21)
    if spread < EMA_SPREAD_MIN:
        print(f"[{STRATEGY_NAME}] EMA spread ${spread:.2f} < min ${EMA_SPREAD_MIN:.2f} — skipping entry")
        return None, ema9, ema21, spread, slope

    # ── Slope conflict filter ─────────────────────────────────────────────────
    # Strong upward slope but bearish EMA alignment → conflicting signal, skip
    if slope > 55.0 and ema9 < ema21:
        print(f"[{STRATEGY_NAME}] Slope conflict: slope={slope:.2f} > 55 but EMA9 < EMA21 — skipping entry")
        return None, ema9, ema21, spread, slope
    # Strong downward slope but bullish EMA alignment → conflicting signal, skip
    if slope < -55.0 and ema9 > ema21:
        print(f"[{STRATEGY_NAME}] Slope conflict: slope={slope:.2f} < -55 but EMA9 > EMA21 — skipping entry")
        return None, ema9, ema21, spread, slope

    if ema9 > ema21:
        return "yes", ema9, ema21, spread, slope
    elif ema9 < ema21:
        return "no", ema9, ema21, spread, slope
    return None, ema9, ema21, spread, slope


# ── Helpers ───────────────────────────────────────────────────────────────────

def _parse_strike(yes_sub_title: str) -> Optional[float]:
    raw = yes_sub_title.replace("Price to beat:", "").replace("$", "").replace(",", "").strip()
    try:
        return float(raw)
    except (ValueError, TypeError):
        return None


def _fetch_market(ticker: str) -> Optional[dict]:
    try:
        raw = kalshi._get(f"/markets/{ticker}")
        info = raw.get("market", {})
        return {
            "yes_bid":       info.get("yes_bid", 0),
            "yes_ask":       info.get("yes_ask", 0),
            "no_bid":        info.get("no_bid", 0),
            "no_ask":        info.get("no_ask", 0),
            "close_time":    info.get("close_time"),
            "yes_sub_title": info.get("yes_sub_title", ""),
            "status":        info.get("status", ""),
            "result":        info.get("result", ""),
        }
    except Exception as e:
        print(f"[MG] Failed to fetch market {ticker}: {e}")
        return None


def _seconds_elapsed(close_time_str: str) -> Optional[int]:
    """How many seconds into the current 15-min cycle are we?"""
    try:
        close_time = datetime.fromisoformat(close_time_str.replace("Z", "+00:00"))
        seconds_left = int((close_time - datetime.now(timezone.utc)).total_seconds())
        return (15 * 60) - seconds_left
    except Exception:
        return None


# ── Fast resolve via Kraken candle ───────────────────────────────────────────

def _fast_resolve(trade: dict, mg_state: dict) -> Optional[tuple[dict, bool]]:
    """
    Try to infer the previous trade result from Kraken candle data.
    Uses candle close vs strike price — if distance >= CANDLE_CONFIDENCE_THRESHOLD,
    result is considered known without waiting for Kalshi.

    Returns (updated_mg_state, True) if confident, or None if inconclusive.
    """
    strike_price = trade.get("strike_price")
    side_bet     = trade.get("side", "")
    payout       = trade.get("payout", 0) or 0
    total_cost   = trade.get("total_cost", 0) or 0
    mg_round     = trade.get("martingale_round", 1) or 1
    start_time   = trade.get("start_time", "")

    if not strike_price or not side_bet or not start_time:
        return None

    try:
        # Parse cycle open time from start_time string → Unix timestamp
        cycle_open_dt = datetime.strptime(start_time, "%Y-%m-%d %I:%M:%S %p EST").replace(tzinfo=EST)
        cycle_close_ts = int((cycle_open_dt + timedelta(minutes=15)).timestamp())
        # Kraken candle ts = open time of candle; candle covering previous cycle has ts = cycle_open
        cycle_open_ts = int(cycle_open_dt.timestamp())

        candles = kraken.get_ohlcv(KRAKEN_PAIR, interval_minutes=15)
        # Find candle whose open time matches the previous cycle
        match = next((c for c in candles if c["ts"] == cycle_open_ts), None)
        if not match:
            print(f"[MG] Fast resolve: no Kraken candle found for ts={cycle_open_ts}")
            return None

        candle_close = match["close"]
        distance     = abs(candle_close - strike_price)
        direction    = "yes" if candle_close > strike_price else "no"

        print(f"[MG] Fast resolve: candle close=${candle_close:,.2f}  strike=${strike_price:,.2f}  distance=${distance:.2f}  inferred={direction}")

        if distance < CANDLE_CONFIDENCE_THRESHOLD:
            print(f"[MG] Fast resolve: distance ${distance:.2f} < threshold ${CANDLE_CONFIDENCE_THRESHOLD:.2f} — inconclusive, falling back to Kalshi poll")
            return None

        # Confident result
        result = direction
        prev_ticker = trade.get("market_ticker", "")
        if result == side_bet:
            profit = payout - total_cost
            _db_update_result(prev_ticker, profit=profit, loss=0.0)
            mg_state["round"] = 1
            mg_state["consecutive_losses"] = 0
            print(f"[MG] Fast resolve: WIN  profit=${profit:.2f} | reset to round 1")
        else:
            _db_update_result(prev_ticker, profit=0.0, loss=total_cost)
            mg_state["consecutive_losses"] = mg_state.get("consecutive_losses", 0) + 1
            mg_state["round"] = min(mg_round + 1, MG_MAX_ROUND)
            print(f"[MG] Fast resolve: LOSS cost=${total_cost:.2f} | next round={mg_state['round']}")

        _save_mg_state(mg_state)
        return mg_state, True

    except Exception as e:
        print(f"[MG] Fast resolve error: {e}")
        return None


# ── Step 1: Resolve previous cycle ───────────────────────────────────────────

def _resolve_previous(mg_state: dict) -> tuple[dict, bool]:
    """
    Poll Kalshi for the result of the latest unresolved trade.
    Returns (updated_mg_state, resolved).
    """
    trade = _db_get_latest_unresolved()
    if not trade:
        print("[MG] No unresolved trade — nothing to resolve.")
        return mg_state, True

    prev_ticker = trade.get("market_ticker", "")
    side_bet    = trade.get("side", "")
    payout      = trade.get("payout", 0) or 0
    total_cost  = trade.get("total_cost", 0) or 0
    mg_round    = trade.get("martingale_round", 1) or 1

    print(f"[MG] Resolving {prev_ticker} (side={side_bet})...")

    # Try fast resolution via Kraken candle first
    fast = _fast_resolve(trade, mg_state)
    if fast is not None:
        return fast

    deadline = time.time() + RESULT_TIMEOUT_SECONDS
    attempt  = 0

    while time.time() < deadline:
        attempt += 1
        market = _fetch_market(prev_ticker)
        if not market:
            time.sleep(RESULT_POLL_DELAY)
            continue

        status = market.get("status", "")
        result = market.get("result", "")

        if status == "finalized" and result in ("yes", "no"):
            print(f"[MG] {prev_ticker} finalized on attempt {attempt} — result={result}")
            if result == side_bet:
                profit = payout - total_cost
                _db_update_result(prev_ticker, profit=profit, loss=0.0)
                mg_state["round"] = 1
                mg_state["consecutive_losses"] = 0
                print(f"[MG] WIN  profit=${profit:.2f} | reset to round 1")
            else:
                _db_update_result(prev_ticker, profit=0.0, loss=total_cost)
                mg_state["consecutive_losses"] = mg_state.get("consecutive_losses", 0) + 1
                mg_state["round"] = min(mg_round + 1, MG_MAX_ROUND)
                print(f"[MG] LOSS cost=${total_cost:.2f} | next round={mg_state['round']}")
            _save_mg_state(mg_state)
            return mg_state, True

        print(f"[MG] attempt {attempt} — status={status} result='{result}', retry in {RESULT_POLL_DELAY}s...")
        time.sleep(RESULT_POLL_DELAY)

    print(f"[MG] {prev_ticker} not finalized within {RESULT_TIMEOUT_SECONDS}s — skipping entry this cycle.")
    _db_insert_no_trade(prev_ticker, datetime.now(EST).strftime("%Y-%m-%d %I:%M:%S %p EST"), f"Result timeout after {RESULT_TIMEOUT_SECONDS}s")
    return mg_state, False


# ── Step 2: Enter new cycle ───────────────────────────────────────────────────

def _enter_new_cycle(mg_state: dict, market_ticker: str, market: dict) -> None:
    mg_round  = max(1, min(mg_state.get("round", 1), MG_MAX_ROUND))
    contracts = MG_CONTRACTS[mg_round]

    # Sanity check — don't double-enter
    if _db_get_open_trade(market_ticker):
        print(f"[MG] Already have open trade for {market_ticker} — skipping.")
        return

    # TBD / oracle check — re-fetch if stale TBD (strike may have populated during resolution)
    sub_title = market.get("yes_sub_title", "")
    if not sub_title or "TBD" in sub_title:
        print(f"[MG] Strike TBD in cached data — re-fetching market...")
        fresh = _fetch_market(market_ticker)
        if fresh:
            market = fresh
        sub_title = market.get("yes_sub_title", "")
    if not sub_title or "TBD" in sub_title:
        print(f"[MG] Strike TBD — Kalshi oracle down, skipping entry.")
        _db_insert_no_trade(market_ticker, datetime.now(EST).strftime("%Y-%m-%d %I:%M:%S %p EST"), "Strike TBD")
        return
    strike_price = _parse_strike(sub_title)

    # BTC price
    try:
        btc_price = kraken.get_ticker(KRAKEN_PAIR).get("last_price")
    except Exception:
        btc_price = None

    # EMA signal
    signal, ema9, ema21, spread, slope = _get_signal()

    yes_ask = market.get("yes_ask", 0)
    yes_bid = market.get("yes_bid", 0)
    no_ask  = market.get("no_ask", 0)
    no_bid  = market.get("no_bid", 0)

    print(f"\n{'='*60}")
    print(f"[{STRATEGY_NAME}] NEW CYCLE: {market_ticker}")
    print(f"[{STRATEGY_NAME}] Strike: ${strike_price}  |  BTC: ${btc_price}")
    print(f"[{STRATEGY_NAME}] YES ask={yes_ask}¢ bid={yes_bid}¢  |  NO ask={no_ask}¢ bid={no_bid}¢")
    if ema9 and ema21:
        print(f"[{STRATEGY_NAME}] EMA{EMA_FAST}={ema9:.2f}  EMA{EMA_SLOW}={ema21:.2f}  spread=${spread:.2f}  slope=${slope:.2f}  → signal={signal or 'NEUTRAL'}")
    print(f"[{STRATEGY_NAME}] MG round={mg_round}  contracts={contracts}  PAPER_TRADE={PAPER_TRADE}")
    print(f"{'='*60}")

    if signal is None:
        print(f"[{STRATEGY_NAME}] No signal — skipping entry.")
        _db_insert_no_trade(market_ticker, datetime.now(EST).strftime("%Y-%m-%d %I:%M:%S %p EST"), f"EMA spread ${spread:.2f} < min ${EMA_SPREAD_MIN:.2f}" if spread is not None else "No EMA signal")
        return

    start_time = datetime.now(EST).strftime("%Y-%m-%d %I:%M:%S %p EST")

    payout = contracts * 1.00  # $1 per contract at settlement

    # ── Place order (live) or simulate (paper) ────────────────────────────────
    if not PAPER_TRADE:
        try:
            order_id = kalshi.place_order(
                market_ticker, signal, contracts,
                price_cents=ENTRY_LIMIT_CENTS, order_type="market"
            )
            print(f"[LIVE]  BUY {contracts}x {signal.upper()} on {market_ticker} limit={ENTRY_LIMIT_CENTS}¢ | order={order_id}")
        except Exception as e:
            print(f"[LIVE]  Order FAILED: {e}")
            return
        # Insert with null entry_price — will be updated after fill confirmed
        entry_price = None
        total_cost  = None
    else:
        entry_price_cents = int(PAPER_COST * 100)
        entry_price = PAPER_COST
        total_cost  = contracts * entry_price
        print(f"[PAPER] BUY {contracts}x {signal.upper()} on {market_ticker} @ {entry_price_cents}¢")

    print(f"[{'LIVE' if not PAPER_TRADE else 'PAPER'}] Payout: ${payout:.2f}")

    # ── DB insert ─────────────────────────────────────────────────────────────
    row_id = _db_insert_trade(
        start_time=start_time,
        market_ticker=market_ticker,
        side=signal,
        mg_round=mg_round,
        contracts=contracts,
        entry_price=entry_price,
        total_cost=total_cost,
        payout=payout,
        strike_price=strike_price,
        btc_price=btc_price,
    )

    # ── Fetch actual fill price in background (live only) ─────────────────────
    if not PAPER_TRADE and order_id:
        def _update_fill_price():
            try:
                time.sleep(5)  # give Kalshi a moment to process
                resp = kalshi._get(f"/portfolio/orders/{order_id}")
                order = resp.get("order", {})
                filled = order.get("fill_count", 0) or 0
                taker_cost = order.get("taker_fill_cost", 0) or 0  # in cents
                maker_cost = order.get("maker_fill_cost", 0) or 0
                fill_cost_cents = taker_cost + maker_cost
                if filled > 0 and fill_cost_cents > 0:
                    actual_price = (fill_cost_cents / filled) / 100.0
                    actual_total = fill_cost_cents / 100.0
                    _db_update_entry_price(market_ticker, actual_price, actual_total)
                    print(f"[LIVE]  Fill confirmed: {filled} contracts @ avg {actual_price:.2f} total=${actual_total:.2f}")
                else:
                    print(f"[LIVE]  Order {order_id} not yet filled (fill_count={filled}) — entry_price stays null")
            except Exception as e:
                print(f"[LIVE]  Fill price fetch failed: {e}")
        threading.Thread(target=_update_fill_price).start()

    if row_id:
        print(f"[DB]    Trade inserted id={row_id}")
        _db_insert_signals(
            trade_id=row_id,
            start_time=start_time,
            ticker_data="BTC",
            ema9=ema9,
            ema21=ema21,
            spread=spread,
            slope=slope,
        )


# ── Background Kalshi confirm (DB update only, mg_state already set) ─────────

def _kalshi_confirm_db(trade: dict) -> None:
    """
    Poll Kalshi until finalized and update the DB result.
    Does NOT touch mg_state — that was already set by fast resolve.
    Runs in a background thread.
    """
    prev_ticker = trade.get("market_ticker", "")
    side_bet    = trade.get("side", "")
    payout      = trade.get("payout", 0) or 0
    total_cost  = trade.get("total_cost", 0) or 0

    print(f"[MG] Background: confirming {prev_ticker} with Kalshi...")
    deadline = time.time() + RESULT_TIMEOUT_SECONDS * 2
    attempt  = 0

    while time.time() < deadline:
        attempt += 1
        market = _fetch_market(prev_ticker)
        if not market:
            time.sleep(RESULT_POLL_DELAY)
            continue
        status = market.get("status", "")
        result = market.get("result", "")
        if status == "finalized" and result in ("yes", "no"):
            if result == side_bet:
                profit = payout - total_cost
                _db_update_result(prev_ticker, profit=profit, loss=0.0)
                print(f"[MG] Background: {prev_ticker} confirmed WIN profit=${profit:.2f}")
            else:
                _db_update_result(prev_ticker, profit=0.0, loss=total_cost)
                print(f"[MG] Background: {prev_ticker} confirmed LOSS cost=${total_cost:.2f}")
            return
        time.sleep(RESULT_POLL_DELAY)

    print(f"[MG] Background: {prev_ticker} did not finalize within timeout — DB not updated.")


# ── Main ──────────────────────────────────────────────────────────────────────

def run(mode: str = "entry") -> None:
    print(f"\n{'='*60}")
    print(f"[{STRATEGY_NAME}] CYCLE START — {datetime.now(EST).strftime('%Y-%m-%d %I:%M %p EST')}")
    print(f"{'='*60}")

    # ── Stop flag check ───────────────────────────────────────────────────────
    if STOP_FLAG_PATH.exists():
        print(f"[MG] Bot stopped — exiting immediately.")
        return

    mg_state = _load_mg_state()

    # ── Step 1: Fast resolve from Kraken candle ───────────────────────────────
    trade = _db_get_latest_unresolved()
    bg_thread = None

    if trade:
        fast = _fast_resolve(trade, mg_state)
        if fast:
            mg_state = fast[0]
            print(f"[MG] Fast resolve succeeded — MG round={mg_state['round']}")
        else:
            # Inconclusive candle — default to round 1 (conservative)
            mg_state["round"] = 1
            mg_state["consecutive_losses"] = 0
            _save_mg_state(mg_state)
            print(f"[MG] Fast resolve inconclusive — defaulting to round 1")

        # Always confirm with Kalshi in background for DB accuracy
        bg_thread = threading.Thread(target=_kalshi_confirm_db, args=(trade,))
        bg_thread.start()

    # ── Step 2: Find fresh ticker ─────────────────────────────────────────────
    ticker = None
    market = None
    deadline = time.time() + ENTRY_CUTOFF_SECONDS
    while time.time() < deadline:
        t = kalshi.get_active_ticker(MARKET_SERIES)
        if t:
            m = _fetch_market(t)
            if m:
                elapsed = _seconds_elapsed(m.get("close_time", ""))
                if elapsed is not None and elapsed < ENTRY_CUTOFF_SECONDS:
                    ticker = t
                    market = m
                    print(f"[MG] New ticker ready: {ticker} ({elapsed}s into cycle)")
                    break
        time.sleep(TICKER_POLL_DELAY)

    if not ticker:
        print(f"[MG] No fresh ticker found in time — skipping entry.")
        _db_insert_no_trade("unknown", datetime.now(EST).strftime("%Y-%m-%d %I:%M:%S %p EST"), "No fresh ticker found")
        if bg_thread:
            bg_thread.join()
        return

    # Re-check cutoff
    elapsed = _seconds_elapsed(market.get("close_time", ""))
    if elapsed is None or elapsed >= ENTRY_CUTOFF_SECONDS:
        print(f"[MG] Ticker now {elapsed}s into cycle — past cutoff, skipping entry.")
        _db_insert_no_trade(ticker, datetime.now(EST).strftime("%Y-%m-%d %I:%M:%S %p EST"), f"Past entry cutoff ({elapsed}s >= {ENTRY_CUTOFF_SECONDS}s)")
        if bg_thread:
            bg_thread.join()
        return


    # ── Step 3: Enter immediately ─────────────────────────────────────────────
    _enter_new_cycle(mg_state, ticker, market)

    # ── Wait for background DB confirm to complete ────────────────────────────
    if bg_thread:
        bg_thread.join()
