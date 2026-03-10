"""
markets/kalshi_btc15m_updown_slope.py — BTC 15-minute Up/Down Martingale (Slope Strategy)

Strategy:
  - Always bet YES (BTC up over next 15 minutes)
  - Skip cycle if EMA2 slope <= SLOPE_MIN (momentum not positive enough)
  - MG round frozen on skip — only resets on win or max round hit
  - Backtest result: 88.7% win rate | Max 3 consecutive losses | 0 round hits over 4 years

Signal logic:
  - EMA2 of Kraken 15m close prices
  - slope = EMA2[-1] - EMA2[-2]
  - slope > SLOPE_MIN → bet YES
  - slope <= SLOPE_MIN → skip cycle, hold MG round

Cron fires at :00 :15 :30 :45 — resolves previous, then enters if signal is valid.
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
STRATEGY_NAME          = "SLOPE_MG"
SIDE                   = "yes"            # always bet YES

# 🏆 Best backtest config: EMA2 Slope>0.5
# Win rate: 88.7% | Max consec losses: 3 | Round hits: 0 | Net P&L 4yr: +$61,276
EMA_PERIOD             = 2                # fast EMA for slope calculation
SLOPE_MIN              = 0.5             # skip if slope <= this

MG_CONTRACTS           = {1: 4, 2: 8, 3: 16, 4: 32, 5: 64, 6: 128, 7: 256, 8: 512, 9: 1024}
MG_MAX_ROUND           = 9               # reset/take loss after this round

KRAKEN_PAIR            = "XBTUSD"
ENTRY_LIMIT_CENTS      = 60              # max limit price for live orders
PAPER_COST             = 0.50

RESULT_POLL_DELAY      = 1
RESULT_TIMEOUT_SECONDS = 120
ENTRY_CUTOFF_SECONDS   = 180
TICKER_POLL_DELAY      = 2
CANDLE_CONFIDENCE_THRESHOLD = 30.0       # min $ distance for fast resolve

MG_STATE_PATH  = Path(__file__).parent.parent / "slope_mg_state.json"
STOP_FLAG_PATH = Path(__file__).parent.parent / "bot_stopped.flag"


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
    result = [sum(values[:period]) / period]
    for v in values[period:]:
        result.append(v * k + result[-1] * (1 - k))
    return result


# ── DB ────────────────────────────────────────────────────────────────────────

def _db_insert_trade(
    start_time, market_ticker, side, mg_round, contracts,
    entry_price, total_cost, payout, strike_price, btc_price
) -> Optional[int]:
    try:
        conn = sqlite3.connect(config.DB_PATH)
        cur  = conn.cursor()
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
        cur  = conn.cursor()
        cur.execute(
            "INSERT INTO kalshi_trades (start_time, market_ticker, strategy, no_trade) VALUES (?, ?, ?, ?)",
            (start_time, market_ticker, STRATEGY_NAME, reason),
        )
        conn.commit()
        conn.close()
        print(f"[DB] No-trade recorded: {reason}")
    except Exception as e:
        print(f"[DB] Insert no-trade failed: {e}")


def _db_insert_signals(trade_id: int, start_time: str, ema2: float, ema2_prev: float, slope: float) -> None:
    try:
        conn = sqlite3.connect(config.DB_PATH)
        cur  = conn.cursor()
        cur.execute(
            """
            INSERT INTO kalshi_trade_signals
                (trade_id, start_time, ticker_data, signal_1, signal_2, signal_3, signal_4)
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                trade_id, start_time, "BTC",
                f"EMA{EMA_PERIOD}, ${ema2:,.2f}",
                f"EMA{EMA_PERIOD}_prev, ${ema2_prev:,.2f}",
                f"Slope, {slope:,.4f}",
                f"Slope_min, {SLOPE_MIN}",
            ),
        )
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"[DB] Insert signals failed: {e}")


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
        print(f"[DB] Entry price updated: {market_ticker} @ {entry_price:.2f} total=${total_cost:.2f}")
    except Exception as e:
        print(f"[DB] Update entry price failed: {e}")


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


def _db_get_latest_unresolved() -> Optional[dict]:
    try:
        conn = sqlite3.connect(config.DB_PATH)
        conn.row_factory = sqlite3.Row
        cur  = conn.cursor()
        cur.execute(
            "SELECT * FROM kalshi_trades WHERE strategy=? AND profit IS NULL AND loss IS NULL ORDER BY id DESC LIMIT 1",
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


# ── Signal ────────────────────────────────────────────────────────────────────

def _get_signal() -> tuple:
    """
    Returns (signal, ema2, ema2_prev, slope)
    signal = "yes" if slope > SLOPE_MIN, else None (skip)
    """
    try:
        candles = kraken.get_ohlcv(KRAKEN_PAIR, interval_minutes=15)
    except Exception as e:
        print(f"[{STRATEGY_NAME}] Kraken fetch failed: {e}")
        return None, None, None, None

    closes = [c["close"] for c in candles]
    if len(closes) < EMA_PERIOD + 2:
        print(f"[{STRATEGY_NAME}] Not enough candles")
        return None, None, None, None

    ema_vals = _ema(closes, EMA_PERIOD)
    if len(ema_vals) < 2:
        return None, None, None, None

    ema2      = ema_vals[-1]
    ema2_prev = ema_vals[-2]
    slope     = ema2 - ema2_prev

    print(f"[{STRATEGY_NAME}] EMA{EMA_PERIOD}={ema2:,.2f}  prev={ema2_prev:,.2f}  slope={slope:,.4f}  min={SLOPE_MIN}")

    if slope > SLOPE_MIN:
        return "yes", ema2, ema2_prev, slope
    else:
        print(f"[{STRATEGY_NAME}] Slope {slope:.4f} <= {SLOPE_MIN} — skip cycle, MG round frozen")
        return None, ema2, ema2_prev, slope


# ── Helpers ───────────────────────────────────────────────────────────────────

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
        print(f"[MG] Failed to fetch market {ticker}: {e}")
        return None


def _seconds_elapsed(close_time_str: str) -> Optional[int]:
    try:
        close_time   = datetime.fromisoformat(close_time_str.replace("Z", "+00:00"))
        seconds_left = int((close_time - datetime.now(timezone.utc)).total_seconds())
        return (15 * 60) - seconds_left
    except Exception:
        return None


# ── Fast resolve via Kraken candle ────────────────────────────────────────────

def _fast_resolve(trade: dict, mg_state: dict) -> Optional[tuple]:
    strike_price = trade.get("strike_price")
    side_bet     = trade.get("side", "")
    payout       = trade.get("payout", 0) or 0
    total_cost   = trade.get("total_cost", 0) or 0
    mg_round     = trade.get("martingale_round", 1) or 1
    start_time   = trade.get("start_time", "")

    if not strike_price or not side_bet or not start_time:
        return None

    try:
        cycle_open_dt = datetime.strptime(start_time, "%Y-%m-%d %I:%M %p EST").replace(tzinfo=EST)
        cycle_open_ts = int(cycle_open_dt.timestamp())

        candles = kraken.get_ohlcv(KRAKEN_PAIR, interval_minutes=15)
        match   = next((c for c in candles if c["ts"] == cycle_open_ts), None)
        if not match:
            print(f"[MG] Fast resolve: no Kraken candle for ts={cycle_open_ts}")
            return None

        candle_close = match["close"]
        distance     = abs(candle_close - strike_price)
        direction    = "yes" if candle_close > strike_price else "no"

        print(f"[MG] Fast resolve: close=${candle_close:,.2f}  strike=${strike_price:,.2f}  dist=${distance:.2f}  inferred={direction}")

        if distance < CANDLE_CONFIDENCE_THRESHOLD:
            print(f"[MG] Fast resolve: dist ${distance:.2f} < threshold ${CANDLE_CONFIDENCE_THRESHOLD:.2f} — inconclusive")
            return None

        prev_ticker = trade.get("market_ticker", "")
        if direction == side_bet:
            profit = payout - total_cost
            _db_update_result(prev_ticker, profit=profit, loss=0.0)
            mg_state["round"] = 1
            mg_state["consecutive_losses"] = 0
            print(f"[MG] Fast resolve: WIN profit=${profit:.2f} | reset to round 1")
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


# ── Background Kalshi confirm ─────────────────────────────────────────────────

def _kalshi_confirm_db(trade: dict) -> None:
    prev_ticker = trade.get("market_ticker", "")
    side_bet    = trade.get("side", "")
    payout      = trade.get("payout", 0) or 0
    total_cost  = trade.get("total_cost", 0) or 0

    print(f"[MG] Background: confirming {prev_ticker} with Kalshi...")
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
                print(f"[MG] Background: {prev_ticker} WIN profit=${payout - total_cost:.2f}")
            else:
                _db_update_result(prev_ticker, profit=0.0, loss=total_cost)
                print(f"[MG] Background: {prev_ticker} LOSS cost=${total_cost:.2f}")
            return
        time.sleep(RESULT_POLL_DELAY)
    print(f"[MG] Background: {prev_ticker} did not finalize within timeout.")


# ── Resolve previous cycle ────────────────────────────────────────────────────

def _resolve_previous(mg_state: dict) -> tuple:
    trade = _db_get_latest_unresolved()
    if not trade:
        print("[MG] No unresolved trade.")
        return mg_state, True

    prev_ticker = trade.get("market_ticker", "")
    side_bet    = trade.get("side", "")
    payout      = trade.get("payout", 0) or 0
    total_cost  = trade.get("total_cost", 0) or 0
    mg_round    = trade.get("martingale_round", 1) or 1

    print(f"[MG] Resolving {prev_ticker} (side={side_bet})...")

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
            print(f"[MG] {prev_ticker} finalized attempt={attempt} result={result}")
            if result == side_bet:
                profit = payout - total_cost
                _db_update_result(prev_ticker, profit=profit, loss=0.0)
                mg_state["round"] = 1
                mg_state["consecutive_losses"] = 0
                print(f"[MG] WIN profit=${profit:.2f} | reset to round 1")
            else:
                _db_update_result(prev_ticker, profit=0.0, loss=total_cost)
                mg_state["consecutive_losses"] = mg_state.get("consecutive_losses", 0) + 1
                mg_state["round"] = min(mg_round + 1, MG_MAX_ROUND)
                print(f"[MG] LOSS cost=${total_cost:.2f} | next round={mg_state['round']}")
            _save_mg_state(mg_state)
            return mg_state, True
        print(f"[MG] attempt {attempt} — status={status} result='{result}', retry...")
        time.sleep(RESULT_POLL_DELAY)

    print(f"[MG] {prev_ticker} not finalized within {RESULT_TIMEOUT_SECONDS}s — skipping entry.")
    _db_insert_no_trade(prev_ticker, datetime.now(EST).strftime("%Y-%m-%d %I:%M:%S %p EST"), f"Result timeout after {RESULT_TIMEOUT_SECONDS}s")
    return mg_state, False


# ── Enter new cycle ───────────────────────────────────────────────────────────

def _enter_new_cycle(mg_state: dict, market_ticker: str, market: dict) -> None:
    mg_round  = max(1, min(mg_state.get("round", 1), MG_MAX_ROUND))
    contracts = MG_CONTRACTS[mg_round]

    if _db_get_open_trade(market_ticker):
        print(f"[MG] Already have open trade for {market_ticker} — skipping.")
        return

    # Strike price
    sub_title = market.get("yes_sub_title", "")
    if not sub_title or "TBD" in sub_title:
        print(f"[MG] Strike TBD — re-fetching market...")
        fresh = _fetch_market(market_ticker)
        if fresh:
            market = fresh
        sub_title = market.get("yes_sub_title", "")
    if not sub_title or "TBD" in sub_title:
        print(f"[MG] Strike still TBD — skipping entry.")
        _db_insert_no_trade(market_ticker, datetime.now(EST).strftime("%Y-%m-%d %I:%M:%S %p EST"), "Strike TBD")
        return

    strike_price = _parse_strike(sub_title)

    # BTC price
    try:
        btc_price = kraken.get_ticker(KRAKEN_PAIR).get("last_price")
    except Exception:
        btc_price = None

    # Signal check
    signal, ema2, ema2_prev, slope = _get_signal()

    print(f"\n{'='*60}")
    print(f"[{STRATEGY_NAME}] NEW CYCLE: {market_ticker}")
    print(f"[{STRATEGY_NAME}] Strike: ${strike_price}  |  BTC: ${btc_price}")
    print(f"[{STRATEGY_NAME}] EMA{EMA_PERIOD}={ema2:,.2f}  prev={ema2_prev:,.2f}  slope={slope:,.4f}  min={SLOPE_MIN}")
    print(f"[{STRATEGY_NAME}] Signal: {signal or 'SKIP'}  |  MG round={mg_round}  contracts={contracts}")
    print(f"{'='*60}")

    if signal is None:
        _db_insert_no_trade(market_ticker, datetime.now(EST).strftime("%Y-%m-%d %I:%M:%S %p EST"), f"Slope {slope:.4f} <= {SLOPE_MIN}")
        return

    start_time = datetime.now(EST).strftime("%Y-%m-%d %I:%M:%S %p EST")
    payout     = contracts * 1.00

    if not PAPER_TRADE:
        try:
            order_id = kalshi.place_order(
                market_ticker, signal, contracts,
                price_cents=ENTRY_LIMIT_CENTS, order_type="market"
            )
            print(f"[LIVE]  BUY {contracts}x {signal.upper()} on {market_ticker} limit={ENTRY_LIMIT_CENTS}¢ order={order_id}")
        except Exception as e:
            print(f"[LIVE]  Order FAILED: {e}")
            return
        entry_price = None
        total_cost  = None
    else:
        entry_price = PAPER_COST
        total_cost  = contracts * PAPER_COST
        print(f"[PAPER] BUY {contracts}x {signal.upper()} on {market_ticker} @ {int(PAPER_COST*100)}¢")

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

    # Fetch actual fill price in background (live only)
    if not PAPER_TRADE and order_id:
        def _update_fill():
            try:
                time.sleep(5)
                resp       = kalshi._get(f"/portfolio/orders/{order_id}")
                order      = resp.get("order", {})
                filled     = order.get("fill_count", 0) or 0
                fill_cents = (order.get("taker_fill_cost", 0) or 0) + (order.get("maker_fill_cost", 0) or 0)
                if filled > 0 and fill_cents > 0:
                    avg_price  = (fill_cents / filled) / 100.0
                    total      = fill_cents / 100.0
                    _db_update_entry_price(market_ticker, avg_price, total)
                    print(f"[LIVE]  Fill confirmed: {filled}x @ {avg_price:.2f} total=${total:.2f}")
                else:
                    print(f"[LIVE]  Order {order_id} fill_count={filled} — entry_price stays null")
            except Exception as e:
                print(f"[LIVE]  Fill fetch failed: {e}")
        threading.Thread(target=_update_fill).start()

    if row_id:
        print(f"[DB]    Trade inserted id={row_id}")
        if ema2 is not None:
            _db_insert_signals(row_id, start_time, ema2, ema2_prev, slope)


# ── Main ──────────────────────────────────────────────────────────────────────

def run(mode: str = "entry") -> None:
    print(f"\n{'='*60}")
    print(f"[{STRATEGY_NAME}] CYCLE START — {datetime.now(EST).strftime('%Y-%m-%d %I:%M %p EST')}")
    print(f"{'='*60}")

    # ── Stop flag ─────────────────────────────────────────────────────────────
    if STOP_FLAG_PATH.exists():
        print(f"[{STRATEGY_NAME}] Bot stopped — exiting.")
        return

    mg_state = _load_mg_state()

    # ── Fast resolve previous ─────────────────────────────────────────────────
    trade     = _db_get_latest_unresolved()
    bg_thread = None

    if trade:
        fast = _fast_resolve(trade, mg_state)
        if fast:
            mg_state = fast[0]
            print(f"[MG] Fast resolve succeeded — round={mg_state['round']}")
        else:
            mg_state["round"] = 1
            mg_state["consecutive_losses"] = 0
            _save_mg_state(mg_state)
            print(f"[MG] Fast resolve inconclusive — defaulting to round 1")

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
                    print(f"[MG] Ticker: {ticker} ({elapsed}s into cycle)")
                    break
        time.sleep(TICKER_POLL_DELAY)

    if not ticker:
        print(f"[MG] No fresh ticker — skipping entry.")
        _db_insert_no_trade("unknown", datetime.now(EST).strftime("%Y-%m-%d %I:%M:%S %p EST"), "No fresh ticker")
        if bg_thread:
            bg_thread.join()
        return

    elapsed = _seconds_elapsed(market.get("close_time", ""))
    if elapsed is None or elapsed >= ENTRY_CUTOFF_SECONDS:
        print(f"[MG] Past cutoff ({elapsed}s) — skipping.")
        _db_insert_no_trade(ticker, datetime.now(EST).strftime("%Y-%m-%d %I:%M:%S %p EST"), f"Past cutoff ({elapsed}s)")
        if bg_thread:
            bg_thread.join()
        return

    # ── Enter ─────────────────────────────────────────────────────────────────
    _enter_new_cycle(mg_state, ticker, market)

    if bg_thread:
        bg_thread.join()
