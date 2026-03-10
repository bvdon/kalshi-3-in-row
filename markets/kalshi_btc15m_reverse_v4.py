"""
markets/kalshi_btc15m_reverse.py — BTC 15-minute Up/Down Reverse EMA Strategy

Strategy (George Costanza):
  - When all conditions look bullish → bet NO (price likely to pull back)
  - When all conditions look bearish → bet YES (price likely to bounce)

YES when ALL true (reversed bearish signal):
  - 15m EMA21 slope < 0
  - Last close < 15m EMA21
  - Last close < 15m EMA50
  - Last close < 1H EMA50

NO when ALL true (reversed bullish signal):
  - 15m EMA21 slope > 0
  - Last close > 15m EMA21
  - Last close > 15m EMA50
  - Last close > 1H EMA50

Otherwise: SKIP

Backtest results (2 years, 44k trades):
  Win rate: 51.9% | Max consec losses: 14 | Net P&L: +$1,719 @ 2 contracts flat

"Last" = close of the last fully closed candle.
Candle source: local candles.db (15m Kraken data)
1H candles: aggregated from 15m (4 × 15m per hour)
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
MARKET_SERIES     = "KXBTC15M"
PAPER_TRADE       = config.PAPER_TRADE
STRATEGY_NAME     = "REVERSE_EMA"

EMA_FAST          = 21       # 15m EMA (slope + price filter)
EMA_SLOW          = 50       # 15m EMA (price filter)
EMA_1H            = 50       # 1H EMA (price filter)

CONTRACTS         = 20       # flat contracts per trade (10x backtest base of 2)
ENTRY_LIMIT_CENTS = 60       # max limit price for live orders
PAPER_COST        = 0.50

RESULT_POLL_DELAY        = 1
RESULT_TIMEOUT_SECONDS   = 120
ENTRY_CUTOFF_SECONDS     = 180
TICKER_POLL_DELAY        = 2
CANDLE_CONFIDENCE_THRESHOLD = 30.0

CANDLES_DB     = Path(__file__).parent.parent / "candles.db"
STATE_PATH     = Path(__file__).parent.parent / "reverse_state.json"
STOP_FLAG_PATH = Path(__file__).parent.parent / "bot_stopped.flag"
KRAKEN_PAIR    = "XBTUSD"


# ── State ─────────────────────────────────────────────────────────────────────

def _load_state() -> dict:
    if STATE_PATH.exists():
        try:
            with open(STATE_PATH) as f:
                return json.load(f)
        except Exception:
            pass
    default = {"consecutive_wins": 0, "consecutive_losses": 0, "total_trades": 0}
    _save_state(default)
    return default


def _save_state(state: dict) -> None:
    with open(STATE_PATH, "w") as f:
        json.dump(state, f)


# ── EMA ───────────────────────────────────────────────────────────────────────

def _ema(values: list, period: int) -> list:
    if len(values) < period:
        return []
    k = 2 / (period + 1)
    result = [sum(values[:period]) / period]
    for v in values[period:]:
        result.append(v * k + result[-1] * (1 - k))
    return result


# ── Load candles from DB ──────────────────────────────────────────────────────

def _load_candles() -> list:
    conn = sqlite3.connect(CANDLES_DB)
    rows = conn.execute(
        "SELECT ts, open, high, low, close FROM btc_candles ORDER BY ts ASC"
    ).fetchall()
    conn.close()
    return [{"ts": r[0], "open": r[1], "high": r[2], "low": r[3], "close": r[4]} for r in rows]


# ── Aggregate 15m → 1H ────────────────────────────────────────────────────────

def _aggregate_1h(candles_15m: list) -> list:
    hourly = {}
    for c in candles_15m:
        h_ts = (c["ts"] // 3600) * 3600
        if h_ts not in hourly:
            hourly[h_ts] = {"ts": h_ts, "close": c["close"], "count": 1}
        else:
            hourly[h_ts]["close"] = c["close"]
            hourly[h_ts]["count"] += 1
    complete = [v for v in hourly.values() if v["count"] == 4]
    complete.sort(key=lambda x: x["ts"])
    return complete


# ── Signal ────────────────────────────────────────────────────────────────────

def _get_signal() -> tuple:
    """
    Returns (side, ef, es, ema_1h_val, slope, last_close) or (None, ...) if skip.
    side = "yes" or "no" based on reversed EMA conditions.
    """
    try:
        all_candles = _load_candles()
        all_candles = all_candles[:-1]  # drop in-progress candle
    except Exception as e:
        print(f"[{STRATEGY_NAME}] DB load failed: {e}")
        return None, None, None, None, None, None

    if len(all_candles) < max(EMA_FAST, EMA_SLOW) + 2:
        print(f"[{STRATEGY_NAME}] Not enough candles in DB")
        return None, None, None, None, None, None

    # 1H EMA50
    candles_1h    = _aggregate_1h(all_candles)
    closes_1h     = [c["close"] for c in candles_1h]
    ema_1h_all    = _ema(closes_1h, EMA_1H)
    ema_1h_lookup = {}
    for i, c in enumerate(candles_1h):
        ei = i - (EMA_1H - 1)
        if ei >= 0:
            ema_1h_lookup[c["ts"]] = ema_1h_all[ei]

    # 15m EMAs (CANDLE_OFFSET = -1, use last 2 values)
    all_closes = [c["close"] for c in all_candles]
    ema_fast_all = _ema(all_closes, EMA_FAST)
    ema_slow_all = _ema(all_closes, EMA_SLOW)

    if len(ema_fast_all) < 2 or len(ema_slow_all) < 1:
        print(f"[{STRATEGY_NAME}] Not enough EMA values")
        return None, None, None, None, None, None

    ef      = ema_fast_all[-1]   # EMA21 at last closed candle
    ef_prev = ema_fast_all[-2]
    es      = ema_slow_all[-1]   # EMA50 at last closed candle
    slope   = ef - ef_prev

    # Last close = close of last fully closed candle
    last_close = all_candles[-1]["close"]

    # 1H EMA50: last completed 1H before current candle
    current_ts = all_candles[-1]["ts"]
    last_1h_ts = (current_ts // 3600) * 3600 - 3600
    ema_1h_val = ema_1h_lookup.get(last_1h_ts)

    if ema_1h_val is None:
        print(f"[{STRATEGY_NAME}] No 1H EMA available — skip")
        return None, ef, es, None, slope, last_close

    print(f"[{STRATEGY_NAME}] last={last_close:,.2f}  EMA{EMA_FAST}={ef:,.2f}  EMA{EMA_SLOW}={es:,.2f}  EMA{EMA_1H}_1H={ema_1h_val:,.2f}  slope={slope:.2f}")

    # Reversed signal logic
    # YES when bearish conditions (last < all EMAs, slope < 0)
    yes_cond = slope < 0 and last_close < ef and last_close < es and last_close < ema_1h_val
    # NO when bullish conditions (last > all EMAs, slope > 0)
    no_cond  = slope > 0 and last_close > ef and last_close > es and last_close > ema_1h_val

    if yes_cond:
        print(f"[{STRATEGY_NAME}] Signal: YES (bearish reversal)")
        return "yes", ef, es, ema_1h_val, slope, last_close
    elif no_cond:
        print(f"[{STRATEGY_NAME}] Signal: NO (bullish reversal)")
        return "no", ef, es, ema_1h_val, slope, last_close
    else:
        print(f"[{STRATEGY_NAME}] Signal: SKIP (mixed conditions)")
        return None, ef, es, ema_1h_val, slope, last_close


# ── DB helpers ────────────────────────────────────────────────────────────────

def _db_insert_trade(start_time, market_ticker, side, contracts,
                     entry_price, total_cost, payout, strike_price, btc_price) -> Optional[int]:
    try:
        conn = sqlite3.connect(config.DB_PATH)
        cur  = conn.cursor()
        cur.execute(
            """INSERT INTO kalshi_trades
               (start_time, market_ticker, side, martingale_round, contracts,
                entry_price, total_cost, strategy, strike_price, btc_price, payout)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (start_time, market_ticker, side, 1, contracts,
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
        cur  = conn.cursor()
        cur.execute(
            "INSERT INTO kalshi_trades (start_time, market_ticker, strategy, no_trade) VALUES (?, ?, ?, ?)",
            (start_time, market_ticker, STRATEGY_NAME, reason),
        )
        conn.commit()
        conn.close()
        print(f"[DB] No-trade: {reason}")
    except Exception as e:
        print(f"[DB] Insert no-trade failed: {e}")


def _db_update_result(market_ticker: str, profit: float, loss: float) -> None:
    try:
        conn = sqlite3.connect(config.DB_PATH)
        cur  = conn.cursor()
        cur.execute(
            "UPDATE kalshi_trades SET profit=?, loss=? WHERE market_ticker=? AND strategy=?",
            (profit, loss, market_ticker, STRATEGY_NAME),
        )
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"[DB] Update result failed: {e}")


def _db_update_entry_price(market_ticker: str, entry_price: float, total_cost: float) -> None:
    try:
        conn = sqlite3.connect(config.DB_PATH)
        cur  = conn.cursor()
        cur.execute(
            "UPDATE kalshi_trades SET entry_price=?, total_cost=? WHERE market_ticker=? AND strategy=?",
            (entry_price, total_cost, market_ticker, STRATEGY_NAME),
        )
        conn.commit()
        conn.close()
        print(f"[DB] Entry price updated: {market_ticker} @ {entry_price:.2f}")
    except Exception as e:
        print(f"[DB] Update entry price failed: {e}")


def _db_get_latest_unresolved() -> Optional[dict]:
    try:
        conn = sqlite3.connect(config.DB_PATH)
        conn.row_factory = sqlite3.Row
        cur  = conn.cursor()
        cur.execute(
            "SELECT * FROM kalshi_trades WHERE strategy=? AND profit IS NULL AND loss IS NULL AND no_trade IS NULL ORDER BY id DESC LIMIT 1",
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
        cur  = conn.cursor()
        cur.execute(
            "SELECT * FROM kalshi_trades WHERE market_ticker=? AND strategy=? AND profit IS NULL AND loss IS NULL ORDER BY id DESC LIMIT 1",
            (market_ticker, STRATEGY_NAME),
        )
        row = cur.fetchone()
        conn.close()
        return dict(row) if row else None
    except Exception as e:
        print(f"[DB] Get open trade failed: {e}")
        return None


# ── Market helpers ────────────────────────────────────────────────────────────

def _parse_strike(yes_sub_title: str) -> Optional[float]:
    raw = yes_sub_title.replace("Price to beat:", "").replace("$", "").replace(",", "").strip()
    try:
        return float(raw)
    except (ValueError, TypeError):
        return None


def _fetch_market(ticker: str) -> Optional[dict]:
    try:
        raw  = kalshi._get(f"/markets/{ticker}")
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
        print(f"[{STRATEGY_NAME}] Fetch market {ticker} failed: {e}")
        return None


def _seconds_elapsed(close_time_str: str) -> Optional[int]:
    try:
        close_time   = datetime.fromisoformat(close_time_str.replace("Z", "+00:00"))
        seconds_left = int((close_time - datetime.now(timezone.utc)).total_seconds())
        return (15 * 60) - seconds_left
    except Exception:
        return None


# ── Fast resolve via Kraken candle ────────────────────────────────────────────

def _fast_resolve(trade: dict, state: dict) -> Optional[tuple]:
    strike_price = trade.get("strike_price")
    side_bet     = trade.get("side", "")
    payout       = trade.get("payout", 0) or 0
    total_cost   = trade.get("total_cost", 0) or 0
    start_time   = trade.get("start_time", "")

    if not strike_price or not side_bet or not start_time:
        return None

    try:
        cycle_open_dt = datetime.strptime(start_time, "%Y-%m-%d %I:%M %p EST").replace(tzinfo=EST)
        cycle_open_ts = int(cycle_open_dt.timestamp())

        candles = kraken.get_ohlcv(KRAKEN_PAIR, interval_minutes=15)
        match   = next((c for c in candles if c["ts"] == cycle_open_ts), None)
        if not match:
            return None

        candle_close = match["close"]
        distance     = abs(candle_close - strike_price)
        direction    = "yes" if candle_close > strike_price else "no"

        print(f"[{STRATEGY_NAME}] Fast resolve: close=${candle_close:,.2f} strike=${strike_price:,.2f} dist=${distance:.2f} inferred={direction}")

        if distance < CANDLE_CONFIDENCE_THRESHOLD:
            print(f"[{STRATEGY_NAME}] Fast resolve: dist ${distance:.2f} < threshold — inconclusive")
            return None

        prev_ticker = trade.get("market_ticker", "")
        if direction == side_bet:
            profit = payout - total_cost
            _db_update_result(prev_ticker, profit=profit, loss=0.0)
            state["consecutive_losses"] = 0
            state["consecutive_wins"]   = state.get("consecutive_wins", 0) + 1
            print(f"[{STRATEGY_NAME}] Fast resolve: WIN profit=${profit:.2f}")
        else:
            _db_update_result(prev_ticker, profit=0.0, loss=total_cost)
            state["consecutive_wins"]   = 0
            state["consecutive_losses"] = state.get("consecutive_losses", 0) + 1
            print(f"[{STRATEGY_NAME}] Fast resolve: LOSS cost=${total_cost:.2f}")

        _save_state(state)
        return state, True

    except Exception as e:
        print(f"[{STRATEGY_NAME}] Fast resolve error: {e}")
        return None


# ── Background Kalshi confirm ─────────────────────────────────────────────────

def _kalshi_confirm_db(trade: dict) -> None:
    prev_ticker = trade.get("market_ticker", "")
    side_bet    = trade.get("side", "")
    payout      = trade.get("payout", 0) or 0
    total_cost  = trade.get("total_cost", 0) or 0

    print(f"[{STRATEGY_NAME}] Background: confirming {prev_ticker}...")
    deadline = time.time() + RESULT_TIMEOUT_SECONDS * 2
    while time.time() < deadline:
        market = _fetch_market(prev_ticker)
        if not market:
            time.sleep(RESULT_POLL_DELAY)
            continue
        status = market.get("status", "")
        result = market.get("result", "")
        if status == "finalized" and result in ("yes", "no"):
            if result == side_bet:
                _db_update_result(prev_ticker, profit=payout - total_cost, loss=0.0)
                print(f"[{STRATEGY_NAME}] Background: WIN")
            else:
                _db_update_result(prev_ticker, profit=0.0, loss=total_cost)
                print(f"[{STRATEGY_NAME}] Background: LOSS")
            return
        time.sleep(RESULT_POLL_DELAY)
    print(f"[{STRATEGY_NAME}] Background: {prev_ticker} did not finalize in time.")


# ── Enter new cycle ───────────────────────────────────────────────────────────

def _enter_new_cycle(state: dict, market_ticker: str, market: dict) -> None:
    if _db_get_open_trade(market_ticker):
        print(f"[{STRATEGY_NAME}] Already have open trade for {market_ticker} — skipping.")
        return

    # Strike price
    sub_title = market.get("yes_sub_title", "")
    if not sub_title or "TBD" in sub_title:
        fresh = _fetch_market(market_ticker)
        if fresh:
            market = fresh
        sub_title = market.get("yes_sub_title", "")
    if not sub_title or "TBD" in sub_title:
        print(f"[{STRATEGY_NAME}] Strike TBD — skipping.")
        _db_insert_no_trade(market_ticker, datetime.now(EST).strftime("%Y-%m-%d %I:%M %p EST"), "Strike TBD")
        return

    strike_price = _parse_strike(sub_title)

    try:
        btc_price = kraken.get_ticker(KRAKEN_PAIR).get("last_price")
    except Exception:
        btc_price = None

    # Sync candles DB before signal
    try:
        import sys
        sys.path.insert(0, str(Path(__file__).parent.parent))
        import importlib
        sync = importlib.import_module("kraken_sync_candles")
        sync.sync()
        print(f"[{STRATEGY_NAME}] Candles synced.")
    except Exception as e:
        print(f"[{STRATEGY_NAME}] Candle sync warning: {e}")

    signal, ef, es, ema_1h_val, slope, last_close = _get_signal()

    print(f"\n{'='*60}")
    print(f"[{STRATEGY_NAME}] NEW CYCLE: {market_ticker}")
    print(f"[{STRATEGY_NAME}] Strike: ${strike_price}  BTC last: ${last_close:,.2f}" if last_close else f"[{STRATEGY_NAME}] Strike: ${strike_price}")
    print(f"[{STRATEGY_NAME}] Signal: {signal or 'SKIP'}  contracts={CONTRACTS}")
    print(f"{'='*60}")

    start_time = datetime.now(EST).strftime("%Y-%m-%d %I:%M %p EST")

    if signal is None:
        _db_insert_no_trade(market_ticker, start_time, "no_signal")
        return

    payout = CONTRACTS * 1.00

    if not PAPER_TRADE:
        try:
            order_id = kalshi.place_order(
                market_ticker, signal, CONTRACTS,
                price_cents=ENTRY_LIMIT_CENTS, order_type="market"
            )
            print(f"[LIVE]  BUY {CONTRACTS}x {signal.upper()} on {market_ticker} limit={ENTRY_LIMIT_CENTS}¢ order={order_id}")
        except Exception as e:
            print(f"[LIVE]  Order FAILED: {e}")
            return
        entry_price = None
        total_cost  = None
    else:
        entry_price = PAPER_COST
        total_cost  = CONTRACTS * PAPER_COST
        order_id    = None
        print(f"[PAPER] BUY {CONTRACTS}x {signal.upper()} on {market_ticker} @ {int(PAPER_COST*100)}¢")

    row_id = _db_insert_trade(
        start_time=start_time,
        market_ticker=market_ticker,
        side=signal,
        contracts=CONTRACTS,
        entry_price=entry_price,
        total_cost=total_cost,
        payout=payout,
        strike_price=strike_price,
        btc_price=btc_price,
    )

    if not PAPER_TRADE and order_id:
        def _update_fill():
            try:
                time.sleep(5)
                resp       = kalshi._get(f"/portfolio/orders/{order_id}")
                order      = resp.get("order", {})
                filled     = order.get("fill_count", 0) or 0
                fill_cents = (order.get("taker_fill_cost", 0) or 0) + (order.get("maker_fill_cost", 0) or 0)
                if filled > 0 and fill_cents > 0:
                    avg_price = (fill_cents / filled) / 100.0
                    total     = fill_cents / 100.0
                    _db_update_entry_price(market_ticker, avg_price, total)
                    print(f"[LIVE]  Fill: {filled}x @ {avg_price:.2f} total=${total:.2f}")
            except Exception as e:
                print(f"[LIVE]  Fill fetch failed: {e}")
        threading.Thread(target=_update_fill).start()

    if row_id:
        print(f"[DB]    Trade inserted id={row_id}")

    state["total_trades"] = state.get("total_trades", 0) + 1
    _save_state(state)


# ── Main ──────────────────────────────────────────────────────────────────────

def run(mode: str = "entry") -> None:
    print(f"\n{'='*60}")
    print(f"[{STRATEGY_NAME}] CYCLE START — {datetime.now(EST).strftime('%Y-%m-%d %I:%M %p EST')}")
    print(f"{'='*60}")

    if STOP_FLAG_PATH.exists():
        print(f"[{STRATEGY_NAME}] Bot stopped — exiting.")
        return

    state = _load_state()

    # ── Resolve previous ──────────────────────────────────────────────────────
    trade     = _db_get_latest_unresolved()
    bg_thread = None

    if trade:
        fast = _fast_resolve(trade, state)
        if fast:
            state = fast[0]
        bg_thread = threading.Thread(target=_kalshi_confirm_db, args=(trade,))
        bg_thread.start()

    # ── Find fresh ticker ─────────────────────────────────────────────────────
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
                    print(f"[{STRATEGY_NAME}] Ticker: {ticker} ({elapsed}s into cycle)")
                    break
        time.sleep(TICKER_POLL_DELAY)

    if not ticker:
        print(f"[{STRATEGY_NAME}] No fresh ticker — skipping.")
        _db_insert_no_trade("unknown", datetime.now(EST).strftime("%Y-%m-%d %I:%M %p EST"), "No fresh ticker")
        if bg_thread:
            bg_thread.join()
        return

    elapsed = _seconds_elapsed(market.get("close_time", ""))
    if elapsed is None or elapsed >= ENTRY_CUTOFF_SECONDS:
        print(f"[{STRATEGY_NAME}] Past cutoff — skipping.")
        _db_insert_no_trade(ticker, datetime.now(EST).strftime("%Y-%m-%d %I:%M %p EST"), f"Past cutoff ({elapsed}s)")
        if bg_thread:
            bg_thread.join()
        return

    _enter_new_cycle(state, ticker, market)

    if bg_thread:
        bg_thread.join()
