"""
markets/kalshi_3_in_row.py — BTC 15m Three-In-Row Exhaustion Strategy

Signal: Three consecutive same-color 15m candles → bet reversal on the 4th.
  - c0=candle[-4], c1=candle[-3], c2=candle[-2] (all fully closed)
  - All same color → signal = reversal of c2
  - c2 green (1) → bet NO (down); c2 red (-1) → bet YES (up)

Signal source: Kraken live REST API (get_ohlcv). NOT candles.db.
Sizing: flat 10 contracts always (no Martingale, no ATR).
Paper trade unless PAPER_TRADE=false in .env.
"""

import json
import sqlite3
import time
from datetime import datetime, timezone, timedelta
import zoneinfo
from pathlib import Path
from typing import Optional

import config
import connectors.kalshi as kalshi
import connectors.kraken as kraken

EST = zoneinfo.ZoneInfo("America/New_York")  # handles EST/EDT automatically

# ── Config ────────────────────────────────────────────────────────────────────
MARKET_SERIES       = "KXBTC15M"
KRAKEN_PAIR         = "XBTUSD"
PAPER_TRADE         = config.PAPER_TRADE
STRATEGY_NAME       = "THREE_IN_ROW"
PAPER_COST          = 0.50
BASE_CONTRACTS      = 20

# Timing
ENTRY_CUTOFF_SECONDS        = 120   # max seconds into cycle we'll accept an entry (14 min)
TICKER_POLL_TIMEOUT         = 120    # max seconds to wait for a fresh ticker before giving up
TICKER_POLL_DELAY           = 2
RESULT_POLL_DELAY           = 1
RESULT_TIMEOUT_SECONDS      = 180
CANDLE_CONFIDENCE_THRESHOLD = 30.0   # min $ distance from strike for fast resolve

ENTRY_LIMIT_CENTS_BTC  = 42
ENTRY_LIMIT_CENTS_ETH  = 42
ENTRY_LIMIT_CENTS_SOL  = 42
ENTRY_LIMIT_CENTS_XRP  = 42

# State / flag paths
STATE_PATH      = Path(__file__).parent.parent / "three_in_row_state.json"
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


# ── Signal computation ────────────────────────────────────────────────────────

def _candle_color(o: float, c: float) -> int:
    """1 = green/up, -1 = red/down."""
    return 1 if c >= o else -1


def _three_in_row_signal(closed: list) -> int:
    """
    Evaluate three-in-row exhaustion on the last three fully closed candles.

    closed = list of candles with ts < current cycle boundary (no in-progress candle).
    Uses closed[-3], closed[-2], closed[-1] — the three most recent completed candles.

    Returns: 1 (bet YES/up), -1 (bet NO/down), 0 (no signal)
    """
    if len(closed) < 3:
        return 0
    c0 = _candle_color(closed[-3]["open"], closed[-3]["close"])
    c1 = _candle_color(closed[-2]["open"], closed[-2]["close"])
    c2 = _candle_color(closed[-1]["open"], closed[-1]["close"])
    if c0 == c1 == c2:
        return -c2   # bet reversal
    return 0


def _closed_candles(candles: list) -> list:
    """
    Return only fully-closed 15m candles by filtering out any candle whose
    open timestamp falls within the current 15-minute cycle boundary.

    Kraken candle ts = open time of that candle (Unix seconds).
    A candle is closed once the next 15m boundary has passed.
    We compute the current cycle start (floor of now to 15m) and exclude
    any candle with ts >= that boundary.
    """
    now_ts = int(datetime.now(timezone.utc).timestamp())
    cycle_start = (now_ts // 900) * 900   # floor to current 15m boundary (900s = 15m)
    closed = [c for c in candles if c["ts"] < cycle_start]
    return closed


def _compute_signals(state: dict) -> dict:
    """
    Fetch live Kraken OHLCV and evaluate the three-in-row signal.

    Uses only fully-closed candles (filtered by timestamp vs current cycle boundary).
    Never touches the in-progress candle regardless of cron timing.

    Streak reset rule (matches backtest logic exactly):
      After a signal fires on a streak ending at c2_ts, we must see a color break
      AFTER c2_ts before the next signal can fire. This prevents double-firing on
      the same streak (e.g. RED/RED/RED → LOSS (RED) → RED/RED/RED → fires again).
      State tracks 'last_trigger_ts' = ts of c2 when the last signal fired.
      A new signal is only allowed if c2.ts > last_trigger_ts (i.e. c2 is a fresh candle).
      But we also need to confirm a streak break occurred — we do this by checking
      that c0 is NOT part of the previous triggering streak, i.e. c0.ts > last_trigger_ts.

    Returns dict: signal ("yes"|"no"|None), contracts (int), details (dict),
                  reason (str|None), trigger_ts (int|None)
    """
    try:
        candles = kraken.get_ohlcv(KRAKEN_PAIR, interval_minutes=15)
    except Exception as e:
        msg = f"Kraken OHLCV fetch failed: {e}"
        print(f"[{STRATEGY_NAME}] {msg}")
        return {"signal": None, "contracts": BASE_CONTRACTS, "details": {}, "reason": msg, "trigger_ts": None}

    # Filter to only closed candles — drop anything from the current 15m cycle
    closed = _closed_candles(candles)
    now_ts = int(datetime.now(timezone.utc).timestamp())
    cycle_start = (now_ts // 900) * 900
    print(f"[{STRATEGY_NAME}] Kraken returned {len(candles)} candles, {len(closed)} fully closed (cycle boundary: {cycle_start})")

    if len(closed) < 3:
        msg = f"Not enough closed candles ({len(closed)} < 3)"
        print(f"[{STRATEGY_NAME}] {msg}")
        return {"signal": None, "contracts": BASE_CONTRACTS, "details": {}, "reason": msg, "trigger_ts": None}

    # Use the last 3 fully-closed candles
    c0 = closed[-3]
    c1 = closed[-2]
    c2 = closed[-1]

    col0 = _candle_color(c0["open"], c0["close"])
    col1 = _candle_color(c1["open"], c1["close"])
    col2 = _candle_color(c2["open"], c2["close"])
    color_label = {1: "GREEN", -1: "RED"}

    last_trigger_ts = state.get("last_trigger_ts", 0)

    print(f"[{STRATEGY_NAME}] --- Signal Summary (closed candles only) ---")
    print(f"[{STRATEGY_NAME}] c0 ts={c0['ts']}: open={c0['open']:,.2f}  close={c0['close']:,.2f}  color={color_label[col0]}")
    print(f"[{STRATEGY_NAME}] c1 ts={c1['ts']}: open={c1['open']:,.2f}  close={c1['close']:,.2f}  color={color_label[col1]}")
    print(f"[{STRATEGY_NAME}] c2 ts={c2['ts']}: open={c2['open']:,.2f}  close={c2['close']:,.2f}  color={color_label[col2]}")
    print(f"[{STRATEGY_NAME}] Last trigger ts={last_trigger_ts}")

    raw_signal = _three_in_row_signal(closed)

    details = {
        "c0_open": c0["open"], "c0_close": c0["close"], "c0_color": col0,
        "c1_open": c1["open"], "c1_close": c1["close"], "c1_color": col1,
        "c2_open": c2["open"], "c2_close": c2["close"], "c2_color": col2,
        "raw_signal": raw_signal,
        "contracts": BASE_CONTRACTS,
    }

    if raw_signal == 0:
        reason = f"No three-in-row: colors={color_label[col0]},{color_label[col1]},{color_label[col2]}"
        print(f"[{STRATEGY_NAME}] SKIP — {reason}")
        return {"signal": None, "contracts": BASE_CONTRACTS, "details": details, "reason": reason, "trigger_ts": None}

    # ── Streak re-fire guard ──────────────────────────────────────────────────
    # The backtest resets streak_len=1 on the result candle, meaning c0 must be
    # a candle that came AFTER the previous trigger's result candle.
    # In timestamp terms: c0.ts must be > last_trigger_ts (streak was broken first).
    if c0["ts"] <= last_trigger_ts:
        reason = (f"Streak re-fire blocked: c0 ts={c0['ts']} <= last_trigger_ts={last_trigger_ts} "
                  f"(streak not reset since last signal)")
        print(f"[{STRATEGY_NAME}] SKIP — {reason}")
        return {"signal": None, "contracts": BASE_CONTRACTS, "details": details, "reason": reason, "trigger_ts": None}

    direction = "yes" if raw_signal == 1 else "no"
    print(f"[{STRATEGY_NAME}] ✓ SIGNAL: {direction.upper()}  contracts={BASE_CONTRACTS}  "
          f"(3x {color_label[col2]} → bet reversal)")
    return {"signal": direction, "contracts": BASE_CONTRACTS, "details": details, "reason": None, "trigger_ts": c2["ts"]}


# ── DB helpers ────────────────────────────────────────────────────────────────

def _db_insert_trade(start_time, market_ticker, side, contracts,
                     entry_price, total_cost, fees, payout, strike_price, btc_price) -> Optional[int]:
    try:
        conn = sqlite3.connect(config.DB_PATH)
        cur  = conn.cursor()
        cur.execute(
            """
            INSERT INTO kalshi_trades
                (start_time, market_ticker, side, martingale_round, contracts,
                 entry_price, total_cost, fees, strategy, strike_price, btc_price, payout)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (start_time, market_ticker, side, 1, contracts,
             entry_price, total_cost, fees, STRATEGY_NAME, strike_price, btc_price, payout),
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
    color_label = {1: "GREEN", -1: "RED"}
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
                f"c0={color_label.get(details.get('c0_color',0),'?')}  open={details.get('c0_open',0):,.2f}  close={details.get('c0_close',0):,.2f}",
                f"c1={color_label.get(details.get('c1_color',0),'?')}  open={details.get('c1_open',0):,.2f}  close={details.get('c1_close',0):,.2f}",
                f"c2={color_label.get(details.get('c2_color',0),'?')}  open={details.get('c2_open',0):,.2f}  close={details.get('c2_close',0):,.2f}",
                f"signal={details.get('raw_signal',0):+d}  contracts={details.get('contracts',0)}",
            ),
        )
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"[DB] Insert signals failed: {e}")


def _db_update_result(market_ticker: str, profit: float, loss: float) -> None:
    try:
        conn = sqlite3.connect(config.DB_PATH)
        conn.row_factory = sqlite3.Row
        cur  = conn.cursor()
        # Read fees already stored at insert time
        row = cur.execute(
            "SELECT fees FROM kalshi_trades WHERE market_ticker=? AND strategy=?",
            (market_ticker, STRATEGY_NAME)
        ).fetchone()
        fees = (row["fees"] or 0.0) if row else 0.0
        # Factor fees into profit/loss
        if profit > 0:
            profit = round(profit - fees, 4)
        else:
            loss = round(loss + fees, 4)
        cur.execute(
            "UPDATE kalshi_trades SET profit=?, loss=? WHERE market_ticker=? AND strategy=?",
            (profit, loss, market_ticker, STRATEGY_NAME),
        )
        conn.commit()
        conn.close()
    except Exception as e:
        print(f"[DB] Update result failed: {e}")


def _db_update_floor(market_ticker: str, side: str) -> None:
    """Fetch and store the cycle floor price for the given trade."""
    try:
        floor = kalshi.get_cycle_floor(market_ticker, side)
        if floor is None:
            return
        conn = sqlite3.connect(config.DB_PATH)
        cur  = conn.cursor()
        cur.execute(
            "UPDATE kalshi_trades SET floor=? WHERE market_ticker=? AND strategy=?",
            (floor, market_ticker, STRATEGY_NAME),
        )
        conn.commit()
        conn.close()
        print(f"[DB]    Floor price stored: ${floor:.2f}")
    except Exception as e:
        print(f"[DB] Update floor failed: {e}")


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
            "open_time":     info.get("open_time"),
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


# ── Fast resolve via Kraken live candle ──────────────────────────────────────

def _fast_resolve(trade: dict, state: dict) -> Optional[tuple]:
    strike_price = trade.get("strike_price")
    side_bet     = trade.get("side", "")
    payout       = trade.get("payout", 0) or 0
    total_cost   = trade.get("total_cost", 0) or 0
    start_time   = trade.get("start_time", "")
    prev_ticker  = trade.get("market_ticker", "")

    # If fill data is missing, try to fetch it before resolving
    if not total_cost and prev_ticker:
        try:
            orders = kalshi._get("/portfolio/orders", params={"ticker": prev_ticker, "limit": 10}).get("orders", [])
            for o in orders:
                if o.get("status") == "executed" and o.get("fill_count", 0) > 0:
                    fill_cost = (o.get("taker_fill_cost") or 0) + (o.get("maker_fill_cost") or 0)
                    fill_fees = (o.get("taker_fees") or 0) + (o.get("maker_fees") or 0)
                    if fill_cost:
                        total_cost  = round(fill_cost / 100, 4)
                        fees_val    = round(fill_fees / 100, 4)
                        entry_price = round(total_cost / (trade.get("contracts") or 10), 4)
                        conn = sqlite3.connect(config.DB_PATH)
                        cur  = conn.cursor()
                        cur.execute(
                            "UPDATE kalshi_trades SET entry_price=?, total_cost=?, fees=? WHERE market_ticker=? AND strategy=?",
                            (entry_price, total_cost, fees_val, prev_ticker, STRATEGY_NAME),
                        )
                        conn.commit()
                        conn.close()
                        print(f"[{STRATEGY_NAME}] Fast resolve: fill fetched — entry=${entry_price:.2f}  cost=${total_cost:.2f}")
                    break
                elif o.get("status") == "canceled" and o.get("fill_count", 0) == 0:
                    conn = sqlite3.connect(config.DB_PATH)
                    cur  = conn.cursor()
                    cur.execute(
                        "UPDATE kalshi_trades SET no_trade=?, entry_price=NULL, total_cost=NULL, fees=NULL, payout=NULL, profit=NULL, loss=NULL WHERE market_ticker=? AND strategy=?",
                        ("Order not filled (canceled)", prev_ticker, STRATEGY_NAME),
                    )
                    conn.commit()
                    conn.close()
                    print(f"[{STRATEGY_NAME}] Fast resolve: order canceled with no fill — marked as no_trade")
                    return None  # skip further resolution
        except Exception as e:
            print(f"[{STRATEGY_NAME}] Fast resolve: fill fetch failed: {e}")

    if not strike_price or not side_bet or not start_time:
        return None

    try:
        # Handle both "EST" and "EDT" in stored timestamps
        start_time_clean = start_time.replace(" EDT", "").replace(" EST", "")
        cycle_open_dt = datetime.strptime(start_time_clean, "%Y-%m-%d %I:%M:%S %p").replace(tzinfo=EST)
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

        _db_update_floor(prev_ticker, side_bet)
        _save_state(state)
        return state, True

    except Exception as e:
        print(f"[{STRATEGY_NAME}] Fast resolve error: {e}")
        return None


# ── Synchronous Kalshi confirm ────────────────────────────────────────────────

def _kalshi_confirm_db(trade: dict) -> None:
    """
    Synchronously confirm a trade result via Kalshi API.
    Called at the start of the next cycle — market should already be finalized.
    Fast: single API call, no polling loop.
    """
    prev_ticker = trade.get("market_ticker", "")
    side_bet    = trade.get("side", "")
    payout      = trade.get("payout", 0) or 0
    total_cost  = trade.get("total_cost", 0) or 0
    fees        = trade.get("fees", 0) or 0

    print(f"[{STRATEGY_NAME}] Confirming result for {prev_ticker} via Kalshi...")
    try:
        # If fill data is missing, fetch it from orders API now
        if not total_cost:
            orders = kalshi._get("/portfolio/orders", params={"ticker": prev_ticker, "limit": 10}).get("orders", [])
            filled = False
            for o in orders:
                if o.get("status") == "executed" and o.get("fill_count", 0) > 0:
                    fill_cost = (o.get("taker_fill_cost") or 0) + (o.get("maker_fill_cost") or 0)
                    fill_fees = (o.get("taker_fees") or 0) + (o.get("maker_fees") or 0)
                    if fill_cost:
                        total_cost  = round(fill_cost / 100, 4)
                        fees        = round(fill_fees / 100, 4)
                        entry_price = round(total_cost / (trade.get("contracts") or 10), 4)
                        conn = sqlite3.connect(config.DB_PATH)
                        cur  = conn.cursor()
                        cur.execute(
                            "UPDATE kalshi_trades SET entry_price=?, total_cost=?, fees=? WHERE market_ticker=? AND strategy=?",
                            (entry_price, total_cost, fees, prev_ticker, STRATEGY_NAME),
                        )
                        conn.commit()
                        conn.close()
                        print(f"[{STRATEGY_NAME}] Confirm: fill fetched — entry=${entry_price:.2f}  cost=${total_cost:.2f}  fees=${fees:.2f}")
                        filled = True
                    break
                elif o.get("status") == "canceled" and o.get("fill_count", 0) == 0:
                    # Order expired/canceled without any fill — mark as no_trade
                    conn = sqlite3.connect(config.DB_PATH)
                    cur  = conn.cursor()
                    cur.execute(
                        "UPDATE kalshi_trades SET no_trade=?, entry_price=NULL, total_cost=NULL, fees=NULL, payout=NULL, profit=NULL, loss=NULL WHERE market_ticker=? AND strategy=?",
                        ("Order not filled (canceled)", prev_ticker, STRATEGY_NAME),
                    )
                    conn.commit()
                    conn.close()
                    print(f"[{STRATEGY_NAME}] Confirm: order canceled with no fill — marked as no_trade")
                    return  # nothing more to do
            if not filled and not orders:
                print(f"[{STRATEGY_NAME}] Confirm: no orders found for {prev_ticker} — skipping P&L")

        market = _fetch_market(prev_ticker)
        if not market:
            print(f"[{STRATEGY_NAME}] Confirm: could not fetch market {prev_ticker}")
            return
        status = market.get("status", "")
        result = market.get("result", "")
        if status == "finalized" and result in ("yes", "no"):
            if result == side_bet:
                _db_update_result(prev_ticker, profit=payout - total_cost, loss=0.0)
                print(f"[{STRATEGY_NAME}] Confirm: {prev_ticker} WIN profit=${payout - total_cost:.2f}")
            else:
                _db_update_result(prev_ticker, profit=0.0, loss=total_cost)
                print(f"[{STRATEGY_NAME}] Confirm: {prev_ticker} LOSS cost=${total_cost:.2f}")
            _db_update_floor(prev_ticker, side_bet)
        else:
            print(f"[{STRATEGY_NAME}] Confirm: {prev_ticker} not yet finalized (status={status} result={result}) — will retry next cycle")
    except Exception as e:
        print(f"[{STRATEGY_NAME}] Confirm error: {e}")


# ── Main run ──────────────────────────────────────────────────────────────────

def run() -> None:
    now_str = datetime.now(EST).strftime("%Y-%m-%d %I:%M:%S %p %Z")
    print(f"\n{'='*60}")
    print(f"[{STRATEGY_NAME}] CYCLE START — {now_str}")
    print(f"{'='*60}")

    # ── Stop flag ─────────────────────────────────────────────────────────────
    if STOP_FLAG_PATH.exists():
        print(f"[{STRATEGY_NAME}] bot_stopped.flag present — exiting.")
        return

    # ── Pause flag ────────────────────────────────────────────────────────────
    if PAUSE_FLAG_PATH.exists():
        print(f"[{STRATEGY_NAME}] bot_paused.flag present — skipping this cycle.")
        return

    state = _load_state()

    # ── Step 1: Resolve previous trade ────────────────────────────────────────
    trade = _db_get_latest_unresolved()

    if trade:
        fast = _fast_resolve(trade, state)
        if fast:
            state = fast[0]
            print(f"[{STRATEGY_NAME}] Fast resolve succeeded.")
        else:
            print(f"[{STRATEGY_NAME}] Fast resolve inconclusive — confirming via Kalshi...")
            _kalshi_confirm_db(trade)
    else:
        print(f"[{STRATEGY_NAME}] No unresolved trade from previous cycle.")

    # ── Step 2: Find fresh ticker ─────────────────────────────────────────────
    ticker   = None
    market   = None
    deadline = time.time() + TICKER_POLL_TIMEOUT   # short window just to find a fresh ticker

    while time.time() < deadline:
        t = kalshi.get_active_ticker(MARKET_SERIES)
        if t:
            m = _fetch_market(t)
            if m:
                # Use open_time to determine how old the market is
                open_time_str = m.get("open_time", "")
                try:
                    open_time = datetime.fromisoformat(open_time_str.replace("Z", "+00:00"))
                    secs_since_open = int((datetime.now(timezone.utc) - open_time).total_seconds())
                except Exception:
                    secs_since_open = 9999
                if secs_since_open < ENTRY_CUTOFF_SECONDS:
                    ticker = t
                    market = m
                    print(f"[{STRATEGY_NAME}] Ticker: {ticker} ({secs_since_open}s into cycle)")
                    break
                else:
                    print(f"[{STRATEGY_NAME}] Waiting for fresh market (current is {secs_since_open}s old)...")
        time.sleep(TICKER_POLL_DELAY)

    if not ticker:
        print(f"[{STRATEGY_NAME}] No fresh ticker found in time — skipping entry.")
        _db_insert_no_trade("unknown", now_str, "No fresh ticker found")
        return

    try:
        open_time    = datetime.fromisoformat(market.get("open_time","").replace("Z","+00:00"))
        elapsed      = int((datetime.now(timezone.utc) - open_time).total_seconds())
    except Exception:
        elapsed      = 9999
    if elapsed is None or elapsed > ENTRY_CUTOFF_SECONDS:
        print(f"[{STRATEGY_NAME}] Past entry cutoff ({elapsed}s) — skipping.")
        _db_insert_no_trade(ticker, now_str, f"Past cutoff ({elapsed}s >= {ENTRY_CUTOFF_SECONDS}s)")
        return

    # ── Step 3: Compute signals (live Kraken candles) ─────────────────────────
    result      = _compute_signals(state)
    signal      = result["signal"]
    contracts   = result["contracts"]
    details     = result["details"]
    reason      = result["reason"]
    trigger_ts  = result["trigger_ts"]

    if signal is None:
        _db_insert_no_trade(ticker, now_str, reason or "No signal")
        return

    # Save trigger_ts so next cycle knows not to re-fire on the same streak
    state["last_trigger_ts"] = trigger_ts
    _save_state(state)

    # ── Step 4: Resolve strike price (retry up to 5x, 3s apart) ─────────────
    sub_title = market.get("yes_sub_title", "")
    if not sub_title or "TBD" in sub_title:
        for attempt in range(1, 6):
            print(f"[{STRATEGY_NAME}] Strike TBD — re-fetching market (attempt {attempt}/5)...")
            time.sleep(3)
            fresh = _fetch_market(ticker)
            if fresh:
                market = fresh
            sub_title = market.get("yes_sub_title", "")
            if sub_title and "TBD" not in sub_title:
                break
    if not sub_title or "TBD" in sub_title:
        print(f"[{STRATEGY_NAME}] Strike still TBD after retries — skipping entry.")
        _db_insert_no_trade(ticker, now_str, "Strike TBD")
        return

    strike_price = _parse_strike(sub_title)

    try:
        btc_price = kraken.get_ticker(KRAKEN_PAIR).get("last_price")
    except Exception:
        btc_price = None

    payout     = contracts * 1.00
    start_time = datetime.now(EST).strftime("%Y-%m-%d %I:%M:%S %p %Z")

    # ── Step 5: Place order (paper or live) ───────────────────────────────────
    if not PAPER_TRADE:
        try:
            order_id = kalshi.place_order(
                ticker, signal, contracts,
                price_cents=ENTRY_LIMIT_CENTS_BTC, order_type="market"
            )
            print(f"[LIVE]  BUY {contracts}x {signal.upper()} on {ticker} limit={ENTRY_LIMIT_CENTS_BTC}¢  order={order_id}")
        except Exception as e:
            print(f"[LIVE]  Order FAILED: {e}")
            return
        # Fetch fill price and fees from order details
        fees = None
        try:
            import time as _time
            _time.sleep(1)  # brief pause for fill to register
            order_detail = kalshi.get_order(order_id)
            taker_fill_cost = order_detail.get("taker_fill_cost") or 0   # cents (market order fills)
            maker_fill_cost = order_detail.get("maker_fill_cost") or 0   # cents (limit order fills)
            taker_fees      = order_detail.get("taker_fees", 0) or 0     # cents
            maker_fees      = order_detail.get("maker_fees", 0) or 0     # cents (usually 0)
            fill_count      = order_detail.get("fill_count") or contracts

            fill_cost_cents = taker_fill_cost + maker_fill_cost          # one will be 0
            fill_fees_cents = taker_fees + maker_fees

            if fill_cost_cents and fill_count:
                total_cost  = fill_cost_cents / 100.0
                entry_price = total_cost / fill_count
                fees        = fill_fees_cents / 100.0
                print(f"[LIVE]  Fill: {fill_count}x @ {entry_price*100:.0f}¢  total=${total_cost:.2f}  fees=${fees:.2f}  (taker_cost={taker_fill_cost}¢ maker_cost={maker_fill_cost}¢)")
            else:
                entry_price = None
                total_cost  = None
                fees        = None
                print(f"[LIVE]  Fill price unavailable (order not yet filled?) detail={order_detail}")
        except Exception as e:
            entry_price = None
            total_cost  = None
            fees        = None
            print(f"[LIVE]  Could not fetch order details: {e}")
    else:
        entry_price = PAPER_COST
        total_cost  = contracts * PAPER_COST
        fees        = None
        print(f"[PAPER] BUY {contracts}x {signal.upper()} on {ticker} @ {int(PAPER_COST*100)}¢  total=${total_cost:.2f}  payout=${payout:.2f}")

    # ── Step 5b: Place companion orders (ETH, SOL, XRP) — fire and forget ─────
    if not PAPER_TRADE:
        companion_assets = [
            ("KXETH15M", "ETH", ENTRY_LIMIT_CENTS_ETH),
            ("KXSOL15M", "SOL", ENTRY_LIMIT_CENTS_SOL),
            ("KXXRP15M", "XRP", ENTRY_LIMIT_CENTS_XRP),
        ]
        for series, name, limit_cents in companion_assets:
            try:
                # Get active ticker for the series (same cycle as BTC)
                comp_ticker = kalshi.get_active_ticker(series)
                if not comp_ticker:
                    print(f"[LIVE]  {name}: no active ticker found, skipping")
                    continue

                # Place order at same side, same contracts, specified limit
                comp_order_id = kalshi.place_order(
                    comp_ticker, signal, contracts,
                    price_cents=limit_cents, order_type="market"
                )
                print(f"[LIVE]  {name}: BUY {contracts}x {signal.upper()} on {comp_ticker} @ {limit_cents}¢  order={comp_order_id}")
            except Exception as e:
                print(f"[LIVE]  {name}: order FAILED: {e}")

    # ── Step 6: DB insert ─────────────────────────────────────────────────────
    row_id = _db_insert_trade(
        start_time=start_time,
        market_ticker=ticker,
        side=signal,
        contracts=contracts,
        entry_price=entry_price,
        total_cost=total_cost,
        fees=fees,
        payout=payout,
        strike_price=strike_price,
        btc_price=btc_price,
    )

    if row_id:
        print(f"[DB]    Trade inserted id={row_id}")
        _db_insert_signals(row_id, start_time, details)

