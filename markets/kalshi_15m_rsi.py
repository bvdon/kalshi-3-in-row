"""
markets/kalshi_15m_rsi.py — BTC 15m RSI Only Strategy

Signal: 14-period RSI on closed 15m candles.
  - RSI > 60 (overbought) → bet NO (expect reversal down)
  - RSI < 40 (oversold)   → bet YES (expect reversal up)
  - Otherwise             → no trade

Signal source: Kraken live REST API. NOT candles.db.
Sizing: flat 10 contracts always.
Paper trade unless PAPER_TRADE=false in .env.
"""

import json
import sqlite3
import threading
import time
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Optional

import numpy as np

import config
import connectors.kalshi as kalshi
import connectors.kraken as kraken

EST = timezone(timedelta(hours=-5))

# ── Config ────────────────────────────────────────────────────────────────────
MARKET_SERIES               = "KXBTC15M"
KRAKEN_PAIR                 = "XBTUSD"
PAPER_TRADE                 = config.PAPER_TRADE
STRATEGY_NAME               = "15m RSI Only"
PAPER_COST                  = 0.50
BASE_CONTRACTS              = 10

RSI_PERIOD                  = 14
RSI_OB                      = 60    # overbought threshold → bet NO
RSI_OS                      = 40    # oversold threshold  → bet YES

# Timing
ENTRY_CUTOFF_SECONDS        = 180
TICKER_POLL_DELAY           = 2
RESULT_POLL_DELAY           = 1
RESULT_TIMEOUT_SECONDS      = 240
CANDLE_CONFIDENCE_THRESHOLD = 30.0  # min $ distance from strike for fast resolve

ENTRY_LIMIT_CENTS           = 60

# State / flag paths
STATE_PATH      = Path(__file__).parent.parent / "rsi_15m_state.json"
STOP_FLAG_PATH  = Path(__file__).parent.parent / "bot_stopped.flag"
PAUSE_FLAG_PATH = Path(__file__).parent.parent / "bot_paused.flag"


# ── State ─────────────────────────────────────────────────────────────────────

def _load_state() -> dict:
    if STATE_PATH.exists():
        try:
            with open(STATE_PATH) as f:
                return json.load(f)
        except Exception:
            pass
    default = {"consecutive_losses": 0, "last_trigger_ts": 0}
    _save_state(default)
    return default


def _save_state(state: dict) -> None:
    with open(STATE_PATH, "w") as f:
        json.dump(state, f)


# ── RSI computation ───────────────────────────────────────────────────────────

def _compute_rsi(closes: list, period: int = RSI_PERIOD) -> list:
    """
    Standard Wilder RSI. Returns a list of RSI values (same length as closes).
    Values before index `period` are set to 50.0 (neutral).
    """
    n = len(closes)
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
            rs = avg_g / avg_l
            rsi[i] = 100.0 - 100.0 / (1.0 + rs)

    return rsi.tolist()


# ── Signal computation ────────────────────────────────────────────────────────

def _closed_candles(candles: list) -> list:
    """Filter out the in-progress candle (ts >= current 15m cycle boundary)."""
    now_ts      = int(datetime.now(timezone.utc).timestamp())
    cycle_start = (now_ts // 900) * 900
    return [c for c in candles if c["ts"] < cycle_start]


def _compute_signals(state: dict) -> dict:
    """
    Fetch live Kraken OHLCV, compute RSI on closed candles, return signal.

    RSI is evaluated on candles[-2] (the last fully closed candle before the
    current cycle), so we use the close price that is fully settled.

    Re-fire guard: same pattern as THREE_IN_ROW — track last_trigger_ts (ts of
    the candle that triggered the last signal). Only fire if c_last.ts > last_trigger_ts.

    Returns dict: signal ("yes"|"no"|None), contracts, details, reason, trigger_ts.
    """
    try:
        candles = kraken.get_ohlcv(KRAKEN_PAIR, interval_minutes=15)
    except Exception as e:
        msg = f"Kraken OHLCV fetch failed: {e}"
        print(f"[{STRATEGY_NAME}] {msg}")
        return {"signal": None, "contracts": BASE_CONTRACTS, "details": {}, "reason": msg, "trigger_ts": None}

    closed      = _closed_candles(candles)
    now_ts      = int(datetime.now(timezone.utc).timestamp())
    cycle_start = (now_ts // 900) * 900
    print(f"[{STRATEGY_NAME}] Kraken returned {len(candles)} candles, {len(closed)} fully closed (cycle boundary: {cycle_start})")

    # Need at least RSI_PERIOD + 1 candles to get a meaningful RSI
    min_candles = RSI_PERIOD + 1
    if len(closed) < min_candles:
        msg = f"Not enough closed candles ({len(closed)} < {min_candles})"
        print(f"[{STRATEGY_NAME}] {msg}")
        return {"signal": None, "contracts": BASE_CONTRACTS, "details": {}, "reason": msg, "trigger_ts": None}

    closes      = [c["close"] for c in closed]
    rsi_values  = _compute_rsi(closes)
    rsi_now     = rsi_values[-1]          # RSI of the last fully closed candle
    signal_candle = closed[-1]            # the candle whose RSI we're acting on

    last_trigger_ts = state.get("last_trigger_ts", 0)

    print(f"[{STRATEGY_NAME}] --- Signal Summary ---")
    print(f"[{STRATEGY_NAME}] Last closed candle ts={signal_candle['ts']}  close={signal_candle['close']:,.2f}")
    print(f"[{STRATEGY_NAME}] RSI({RSI_PERIOD})={rsi_now:.2f}  OB={RSI_OB}  OS={RSI_OS}")
    print(f"[{STRATEGY_NAME}] Last trigger ts={last_trigger_ts}")

    details = {
        "rsi":        rsi_now,
        "close":      signal_candle["close"],
        "candle_ts":  signal_candle["ts"],
        "contracts":  BASE_CONTRACTS,
    }

    # Determine raw signal
    if rsi_now > RSI_OB:
        raw_signal = -1   # overbought → bet NO
    elif rsi_now < RSI_OS:
        raw_signal = 1    # oversold → bet YES
    else:
        reason = f"RSI({rsi_now:.2f}) in neutral zone [{RSI_OS}, {RSI_OB}] — no trade"
        print(f"[{STRATEGY_NAME}] SKIP — {reason}")
        return {"signal": None, "contracts": BASE_CONTRACTS, "details": details, "reason": reason, "trigger_ts": None}

    # Re-fire guard: don't re-signal on the same candle as the previous trigger
    if signal_candle["ts"] <= last_trigger_ts:
        reason = (f"Re-fire blocked: candle ts={signal_candle['ts']} <= last_trigger_ts={last_trigger_ts}")
        print(f"[{STRATEGY_NAME}] SKIP — {reason}")
        return {"signal": None, "contracts": BASE_CONTRACTS, "details": details, "reason": reason, "trigger_ts": None}

    direction = "yes" if raw_signal == 1 else "no"
    label     = "OVERSOLD" if raw_signal == 1 else "OVERBOUGHT"
    print(f"[{STRATEGY_NAME}] ✓ SIGNAL: {direction.upper()}  contracts={BASE_CONTRACTS}  "
          f"(RSI={rsi_now:.2f} {label})")
    return {"signal": direction, "contracts": BASE_CONTRACTS, "details": details, "reason": None, "trigger_ts": signal_candle["ts"]}


# ── DB helpers ────────────────────────────────────────────────────────────────

def _db_insert_trade(start_time, market_ticker, side, contracts,
                     entry_price, total_cost, payout, strike_price, btc_price) -> Optional[int]:
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
        print(f"[DB] No-trade recorded: {reason}")
    except Exception as e:
        print(f"[DB] Insert no-trade failed: {e}")


def _db_insert_signals(trade_id: int, start_time: str, details: dict) -> None:
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
                trade_id,
                start_time,
                "BTC",
                f"RSI({RSI_PERIOD})={details.get('rsi', 0):.2f}",
                f"close={details.get('close', 0):,.2f}",
                f"candle_ts={details.get('candle_ts', 0)}",
                f"contracts={details.get('contracts', 0)}",
            ),
        )
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"[DB] Insert signals failed: {e}")


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
            "SELECT * FROM kalshi_trades WHERE strategy=? AND profit IS NULL AND loss IS NULL AND no_trade IS NULL ORDER BY id DESC LIMIT 1",
            (STRATEGY_NAME,),
        )
        row = cur.fetchone()
        conn.close()
        return dict(row) if row else None
    except Exception as e:
        print(f"[DB] Get unresolved failed: {e}")
        return None


# ── Market helpers ────────────────────────────────────────────────────────────

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
        print(f"[{STRATEGY_NAME}] Failed to fetch market {ticker}: {e}")
        return None


def _parse_strike(yes_sub_title: str) -> Optional[float]:
    raw = yes_sub_title.replace("Price to beat:", "").replace("$", "").replace(",", "").strip()
    try:
        return float(raw)
    except (ValueError, TypeError):
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
        cycle_open_dt = datetime.strptime(start_time, "%Y-%m-%d %I:%M:%S %p EST").replace(tzinfo=EST)
        cycle_open_ts = (int(cycle_open_dt.timestamp()) // 900) * 900  # floor to 15m boundary

        candles = kraken.get_ohlcv(KRAKEN_PAIR, interval_minutes=15)
        match   = next((c for c in candles if c["ts"] == cycle_open_ts), None)
        if not match:
            print(f"[{STRATEGY_NAME}] Fast resolve: no Kraken candle for ts={cycle_open_ts}")
            return None

        candle_close = match["close"]
        distance     = abs(candle_close - strike_price)
        direction    = "yes" if candle_close > strike_price else "no"

        print(f"[{STRATEGY_NAME}] Fast resolve: close=${candle_close:,.2f}  strike=${strike_price:,.2f}  dist=${distance:.2f}  inferred={direction}")

        if distance < CANDLE_CONFIDENCE_THRESHOLD:
            print(f"[{STRATEGY_NAME}] Fast resolve: dist ${distance:.2f} < threshold — inconclusive")
            return None

        prev_ticker = trade.get("market_ticker", "")
        if direction == side_bet:
            profit = payout - total_cost
            _db_update_result(prev_ticker, profit=profit, loss=0.0)
            state["consecutive_losses"] = 0
            print(f"[{STRATEGY_NAME}] Fast resolve: WIN profit=${profit:.2f}")
        else:
            _db_update_result(prev_ticker, profit=0.0, loss=total_cost)
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

    print(f"[{STRATEGY_NAME}] Background: confirming {prev_ticker} with Kalshi...")
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
                print(f"[{STRATEGY_NAME}] Background: {prev_ticker} WIN profit=${payout - total_cost:.2f}")
            else:
                _db_update_result(prev_ticker, profit=0.0, loss=total_cost)
                print(f"[{STRATEGY_NAME}] Background: {prev_ticker} LOSS cost=${total_cost:.2f}")
            return
        time.sleep(RESULT_POLL_DELAY)
    print(f"[{STRATEGY_NAME}] Background: {prev_ticker} did not finalize within timeout.")


# ── Main run ──────────────────────────────────────────────────────────────────

def run() -> None:
    now_str = datetime.now(EST).strftime("%Y-%m-%d %I:%M:%S %p EST")
    print(f"\n{'='*60}")
    print(f"[{STRATEGY_NAME}] CYCLE START — {now_str}")
    print(f"{'='*60}")

    # ── Stop / Pause flags ────────────────────────────────────────────────────
    if STOP_FLAG_PATH.exists():
        print(f"[{STRATEGY_NAME}] bot_stopped.flag present — exiting.")
        return
    if PAUSE_FLAG_PATH.exists():
        print(f"[{STRATEGY_NAME}] bot_paused.flag present — skipping this cycle.")
        return

    state = _load_state()

    # ── Step 1: Resolve previous trade ────────────────────────────────────────
    trade     = _db_get_latest_unresolved()
    bg_thread = None

    if trade:
        fast = _fast_resolve(trade, state)
        if fast:
            state = fast[0]
            print(f"[{STRATEGY_NAME}] Fast resolve succeeded.")
        else:
            print(f"[{STRATEGY_NAME}] Fast resolve inconclusive — Kalshi will confirm in background.")
        bg_thread = threading.Thread(target=_kalshi_confirm_db, args=(trade,), daemon=True)
        bg_thread.start()
    else:
        print(f"[{STRATEGY_NAME}] No unresolved trade from previous cycle.")

    # ── Step 2: Find fresh ticker ─────────────────────────────────────────────
    ticker   = None
    market   = None
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
        print(f"[{STRATEGY_NAME}] No fresh ticker found in time — skipping entry.")
        _db_insert_no_trade("unknown", now_str, "No fresh ticker found")
        if bg_thread:
            bg_thread.join(timeout=2)
        return

    elapsed = _seconds_elapsed(market.get("close_time", ""))
    if elapsed is None or elapsed > ENTRY_CUTOFF_SECONDS:
        print(f"[{STRATEGY_NAME}] Past entry cutoff ({elapsed}s) — skipping.")
        _db_insert_no_trade(ticker, now_str, f"Past cutoff ({elapsed}s >= {ENTRY_CUTOFF_SECONDS}s)")
        if bg_thread:
            bg_thread.join(timeout=2)
        return

    # ── Step 3: Compute signal ────────────────────────────────────────────────
    result     = _compute_signals(state)
    signal     = result["signal"]
    contracts  = result["contracts"]
    details    = result["details"]
    reason     = result["reason"]
    trigger_ts = result["trigger_ts"]

    if signal is None:
        _db_insert_no_trade(ticker, now_str, reason or "No signal")
        if bg_thread:
            bg_thread.join(timeout=2)
        return

    # Save trigger_ts so next cycle can apply re-fire guard
    state["last_trigger_ts"] = trigger_ts
    _save_state(state)

    # ── Step 4: Resolve strike price ──────────────────────────────────────────
    sub_title = market.get("yes_sub_title", "")
    if not sub_title or "TBD" in sub_title:
        print(f"[{STRATEGY_NAME}] Strike TBD — re-fetching market...")
        fresh = _fetch_market(ticker)
        if fresh:
            market = fresh
        sub_title = market.get("yes_sub_title", "")
    if not sub_title or "TBD" in sub_title:
        print(f"[{STRATEGY_NAME}] Strike still TBD — skipping entry.")
        _db_insert_no_trade(ticker, now_str, "Strike TBD")
        if bg_thread:
            bg_thread.join(timeout=2)
        return

    strike_price = _parse_strike(sub_title)

    try:
        btc_price = kraken.get_ticker(KRAKEN_PAIR).get("last_price")
    except Exception:
        btc_price = None

    payout     = contracts * 1.00
    start_time = datetime.now(EST).strftime("%Y-%m-%d %I:%M:%S %p EST")

    # ── Step 5: Place order (paper or live) ───────────────────────────────────
    if not PAPER_TRADE:
        try:
            order_id = kalshi.place_order(
                ticker, signal, contracts,
                price_cents=ENTRY_LIMIT_CENTS, order_type="market"
            )
            print(f"[LIVE]  BUY {contracts}x {signal.upper()} on {ticker} limit={ENTRY_LIMIT_CENTS}¢  order={order_id}")
        except Exception as e:
            print(f"[LIVE]  Order FAILED: {e}")
            if bg_thread:
                bg_thread.join(timeout=2)
            return
        entry_price = None
        total_cost  = None
    else:
        entry_price = PAPER_COST
        total_cost  = contracts * PAPER_COST
        print(f"[PAPER] BUY {contracts}x {signal.upper()} on {ticker} @ {int(PAPER_COST*100)}¢  "
              f"total=${total_cost:.2f}  payout=${payout:.2f}")

    # ── Step 6: DB insert ─────────────────────────────────────────────────────
    row_id = _db_insert_trade(
        start_time=start_time,
        market_ticker=ticker,
        side=signal,
        contracts=contracts,
        entry_price=entry_price,
        total_cost=total_cost,
        payout=payout,
        strike_price=strike_price,
        btc_price=btc_price,
    )

    if row_id:
        print(f"[DB]    Trade inserted id={row_id}")
        _db_insert_signals(row_id, start_time, details)

    if bg_thread:
        bg_thread.join(timeout=2)
