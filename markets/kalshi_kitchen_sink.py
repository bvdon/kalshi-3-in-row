"""
markets/kalshi-kitchen-sink.py — BTC 15m Kitchen Sink Strategy (v10)

v10 Kitchen Sink: EMA9-slope-rev + Big-move-rev + S/R-proximity-rev
Filters: ranging regime (EMA9/EMA50 spread) + RSI exhaustion
Sizing: ATR-adaptive (base 10, max 25 contracts)
Entry: 2-of-3 signals + both filters pass

Signal data source: Kraken public OHLCV API (live candles, NOT candles.db).
All signal computation uses the last closed candle (index -2), never the
in-progress candle (index -1).

Paper trade only unless PAPER_TRADE=false in .env.
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
MARKET_SERIES           = "KXBTC15M"
KRAKEN_PAIR             = "XBTUSD"
PAPER_TRADE             = config.PAPER_TRADE
STRATEGY_NAME           = "KITCHEN_SINK"
PAPER_COST              = 0.50

# Signal params
LOOKBACK                = 40      # bars for rolling S/R high/low
BIG_MOVE_PCT            = 0.005   # 0.5% threshold for big-move signal
SR_NEAR_PCT             = 0.003   # 0.3% band around S/R level
REGIME_THRESH           = 0.005   # EMA9/EMA50 spread < 0.5% = ranging
RSI_LONG_MAX            = 45      # RSI must be below this for long entries
RSI_SHORT_MIN           = 55      # RSI must be above this for short entries

# Sizing
BASE_CONTRACTS          = 10
MAX_CONTRACTS           = 25
ENTRY_LIMIT_CENTS       = 60

# Timing
RESULT_POLL_DELAY       = 1
RESULT_TIMEOUT_SECONDS  = 120
ENTRY_CUTOFF_SECONDS    = 180
TICKER_POLL_DELAY       = 2
CANDLE_CONFIDENCE_THRESHOLD = 30.0   # min $ distance from strike for fast resolve

# Candles to fetch from Kraken live API
# Need enough for EMA50 (50), RSI14 (14+warmup), LOOKBACK (40), ATR14 (14)
# 100 is comfortable; Kraken returns up to 720 15m candles by default
CANDLES_NEEDED          = 100

# State / flag paths
STATE_PATH     = Path(__file__).parent.parent / "kitchen_sink_state.json"
STOP_FLAG_PATH = Path(__file__).parent.parent / "bot_stopped.flag"
PAUSE_FLAG_PATH = Path(__file__).parent.parent / "bot_paused.flag"


# ── State ─────────────────────────────────────────────────────────────────────

def _load_state() -> dict:
    if STATE_PATH.exists():
        try:
            with open(STATE_PATH) as f:
                return json.load(f)
        except Exception:
            pass
    default = {"round": 1, "consecutive_losses": 0}
    _save_state(default)
    return default


def _save_state(state: dict) -> None:
    with open(STATE_PATH, "w") as f:
        json.dump(state, f)


# ── Indicators (computed on live Kraken candles) ──────────────────────────────

def _ema(closes: list, period: int) -> list:
    """Compute EMA. Returns list same length as closes (first period-1 values are seeded)."""
    n = len(closes)
    if n < period:
        return []
    k = 2 / (period + 1)
    result = [sum(closes[:period]) / period]
    for v in closes[period:]:
        result.append(v * k + result[-1] * (1 - k))
    # Pad front so indices align with closes
    return [None] * (period - 1) + result


def _rsi(closes: list, period: int = 14) -> list:
    """Compute RSI. Returns list aligned with closes (first period values are None)."""
    n = len(closes)
    if n < period + 1:
        return [None] * n
    gains = []
    losses = []
    for i in range(1, n):
        diff = closes[i] - closes[i - 1]
        gains.append(max(diff, 0))
        losses.append(max(-diff, 0))
    avg_g = sum(gains[:period]) / period
    avg_l = sum(losses[:period]) / period
    rsi_vals = [None] * (period + 1)
    for i in range(period, len(gains)):
        avg_g = (avg_g * (period - 1) + gains[i]) / period
        avg_l = (avg_l * (period - 1) + losses[i]) / period
        rs = avg_g / avg_l if avg_l != 0 else float("inf")
        rsi_vals.append(100 - 100 / (1 + rs))
    return rsi_vals


def _atr(candles: list, period: int = 14) -> list:
    """Compute ATR aligned with candles list."""
    n = len(candles)
    tr = [None]
    for i in range(1, n):
        h = candles[i]["high"]
        l = candles[i]["low"]
        pc = candles[i - 1]["close"]
        tr.append(max(h - l, abs(h - pc), abs(l - pc)))
    if n < period + 1:
        return [None] * n
    seed = sum(tr[1:period + 1]) / period
    atr_vals = [None] * period + [seed]
    for i in range(period + 1, n):
        atr_vals.append((atr_vals[-1] * (period - 1) + tr[i]) / period)
    return atr_vals


# ── Signal computation ─────────────────────────────────────────────────────────

def _closed_candles(candles: list) -> list:
    """
    Return only fully-closed 15m candles by filtering out any candle whose
    open timestamp falls within the current 15-minute cycle boundary.

    Kraken candle ts = open time of that candle (Unix seconds).
    A candle is only closed once the next 15m boundary has passed.
    We compute the current cycle start and exclude any candle with ts >= that boundary.
    """
    now_ts = int(datetime.now(timezone.utc).timestamp())
    cycle_start = (now_ts // 900) * 900  # floor to current 15m boundary (900s = 15m)
    return [c for c in candles if c["ts"] < cycle_start]


def _compute_signals() -> dict:
    """
    Fetches live candles from Kraken REST API and computes all signals.

    Uses timestamp-based filtering to guarantee only fully-closed candles
    are evaluated — never the in-progress candle, regardless of cron timing.

    Returns dict with:
        signal: "yes" | "no" | None
        contracts: int (ATR-sized)
        details: dict of raw indicator values for logging/DB
    """
    try:
        candles = kraken.get_ohlcv(KRAKEN_PAIR, interval_minutes=15)
    except Exception as e:
        print(f"[{STRATEGY_NAME}] Kraken OHLCV fetch failed: {e}")
        return {"signal": None, "contracts": BASE_CONTRACTS, "details": {}, "reason": f"Kraken error: {e}"}

    # Filter to only closed candles — drop anything from the current 15m cycle
    now_ts = int(datetime.now(timezone.utc).timestamp())
    cycle_start = (now_ts // 900) * 900
    all_closed = _closed_candles(candles)
    print(f"[{STRATEGY_NAME}] Kraken returned {len(candles)} candles, {len(all_closed)} fully closed (cycle boundary: {cycle_start})")

    if len(all_closed) < CANDLES_NEEDED:
        reason = f"Not enough closed candles ({len(all_closed)} < {CANDLES_NEEDED})"
        print(f"[{STRATEGY_NAME}] {reason}")
        return {"signal": None, "contracts": BASE_CONTRACTS, "details": {}, "reason": reason}

    # Use the last CANDLES_NEEDED fully-closed candles
    window = all_closed[-CANDLES_NEEDED:]
    closes = [c["close"] for c in window]
    n = len(window)
    last_idx = n - 1  # last element IS the last closed candle (in-progress already filtered out)

    # ── Indicators ────────────────────────────────────────────────────────────
    ema9_series  = _ema(closes, 9)
    ema50_series = _ema(closes, 50)
    rsi_series   = _rsi(closes, 14)
    atr_series   = _atr(window, 14)

    ema9       = ema9_series[last_idx]
    ema9_prev  = ema9_series[last_idx - 1]
    ema9_prev2 = ema9_series[last_idx - 2]
    ema50      = ema50_series[last_idx]
    rsi_now    = rsi_series[last_idx]
    atr_now    = atr_series[last_idx]

    if any(v is None for v in [ema9, ema9_prev, ema9_prev2, ema50, rsi_now, atr_now]):
        reason = "Indicator warmup incomplete (None values)"
        print(f"[{STRATEGY_NAME}] {reason}")
        return {"signal": None, "contracts": BASE_CONTRACTS, "details": {}, "reason": reason}

    # ── ATR sizing ────────────────────────────────────────────────────────────
    valid_atrs = [v for v in atr_series if v is not None and v > 0]
    median_atr = sorted(valid_atrs)[len(valid_atrs) // 2] if valid_atrs else atr_now
    if atr_now > 0:
        raw_contracts = BASE_CONTRACTS * (median_atr / atr_now)
        contracts = int(max(1, min(MAX_CONTRACTS, round(raw_contracts))))
    else:
        contracts = BASE_CONTRACTS

    # ── Signal 1: EMA9 slope reversal ─────────────────────────────────────────
    slope_now  = ema9 - ema9_prev       # current slope
    slope_prev = ema9_prev - ema9_prev2  # previous slope
    ema_sig = 0
    if slope_now > 0 and slope_prev < 0:
        ema_sig = 1   # slope was falling, now rising → long
    elif slope_now < 0 and slope_prev > 0:
        ema_sig = -1  # slope was rising, now falling → short

    # ── Signal 2: Big-move reversal ───────────────────────────────────────────
    prev_open  = window[last_idx]["open"]
    prev_close = window[last_idx]["close"]
    big_sig = 0
    if prev_open > 0:
        move_pct = abs(prev_close - prev_open) / prev_open
        if move_pct >= BIG_MOVE_PCT:
            big_sig = -1 if prev_close > prev_open else 1  # fade the move

    # ── Signal 3: S/R proximity reversal ─────────────────────────────────────
    # Include last_idx in the S/R window (last_idx + 1 = exclusive upper bound)
    sr_window_closes = closes[last_idx - LOOKBACK: last_idx + 1]
    roll_high = max(sr_window_closes)
    roll_low  = min(sr_window_closes)
    price = prev_close
    sr_sig = 0
    if price >= roll_high * (1 - SR_NEAR_PCT):
        sr_sig = -1  # near rolling high → short
    elif price <= roll_low * (1 + SR_NEAR_PCT):
        sr_sig = 1   # near rolling low → long

    # ── Filter 1: Ranging regime ──────────────────────────────────────────────
    regime_spread = abs(ema9 - ema50) / ema50 if ema50 > 0 else 1.0
    is_ranging = regime_spread < REGIME_THRESH

    # ── Filter 2: RSI exhaustion ──────────────────────────────────────────────
    # Applied per-direction after majority is determined (below)

    # ── Confluence: 2-of-3 signals ────────────────────────────────────────────
    signals = [ema_sig, big_sig, sr_sig]
    up_count = sum(1 for s in signals if s == 1)
    dn_count = sum(1 for s in signals if s == -1)
    fired = max(up_count, dn_count)
    majority = 1 if up_count > dn_count else (-1 if dn_count > up_count else 0)

    details = {
        "ema9": ema9,
        "ema9_prev": ema9_prev,
        "slope_now": slope_now,
        "slope_prev": slope_prev,
        "ema50": ema50,
        "regime_spread_pct": regime_spread * 100,
        "is_ranging": is_ranging,
        "rsi": rsi_now,
        "atr_now": atr_now,
        "median_atr": median_atr,
        "move_pct": abs(prev_close - prev_open) / prev_open * 100 if prev_open > 0 else 0,
        "roll_high": roll_high,
        "roll_low": roll_low,
        "price": price,
        "sig_ema": ema_sig,
        "sig_big": big_sig,
        "sig_sr": sr_sig,
        "up_count": up_count,
        "dn_count": dn_count,
        "contracts": contracts,
    }

    # ── Log signal summary ────────────────────────────────────────────────────
    last_candle_ts = window[last_idx]["ts"]
    print(f"[{STRATEGY_NAME}] --- Signal Summary ---")
    print(f"[{STRATEGY_NAME}] Last closed candle ts={last_candle_ts}  price=${price:,.2f}")
    print(f"[{STRATEGY_NAME}] EMA9={ema9:,.2f}  EMA50={ema50:,.2f}  spread={regime_spread*100:.3f}%  ranging={is_ranging}")
    print(f"[{STRATEGY_NAME}] RSI14={rsi_now:.1f}  ATR={atr_now:.1f}  median_atr={median_atr:.1f}  contracts={contracts}")
    print(f"[{STRATEGY_NAME}] Sig1(EMA slope)={ema_sig:+d}  slope_now={slope_now:,.2f}  slope_prev={slope_prev:,.2f}")
    print(f"[{STRATEGY_NAME}] Sig2(Big-move)={big_sig:+d}  move={abs(prev_close-prev_open)/prev_open*100:.3f}%  thresh={BIG_MOVE_PCT*100:.1f}%")
    print(f"[{STRATEGY_NAME}] Sig3(S/R rev)={sr_sig:+d}  price=${price:,.2f}  hi=${roll_high:,.2f}  lo=${roll_low:,.2f}")
    print(f"[{STRATEGY_NAME}] Confluence: UP={up_count} DN={dn_count} fired={fired} majority={'YES' if majority==1 else 'NO' if majority==-1 else 'NONE'}")

    # ── Apply filters ─────────────────────────────────────────────────────────
    if fired < 2 or majority == 0:
        reason = f"Confluence insufficient ({fired}/3 signals, need 2)"
        print(f"[{STRATEGY_NAME}] SKIP — {reason}")
        return {"signal": None, "contracts": contracts, "details": details, "reason": reason}

    if not is_ranging:
        reason = f"Regime filter: trending (spread={regime_spread*100:.3f}% >= {REGIME_THRESH*100:.1f}%)"
        print(f"[{STRATEGY_NAME}] SKIP — {reason}")
        return {"signal": None, "contracts": contracts, "details": details, "reason": reason}

    rsi_ok = (majority == 1 and rsi_now < RSI_LONG_MAX) or \
             (majority == -1 and rsi_now > RSI_SHORT_MIN)
    if not rsi_ok:
        reason = f"RSI filter: RSI={rsi_now:.1f} not exhausted for {'long' if majority==1 else 'short'} (need {'<'+str(RSI_LONG_MAX) if majority==1 else '>'+str(RSI_SHORT_MIN)})"
        print(f"[{STRATEGY_NAME}] SKIP — {reason}")
        return {"signal": None, "contracts": contracts, "details": details, "reason": reason}

    direction = "yes" if majority == 1 else "no"
    print(f"[{STRATEGY_NAME}] ✓ SIGNAL: {direction.upper()}  contracts={contracts}  (all filters passed)")
    return {"signal": direction, "contracts": contracts, "details": details, "reason": None}


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
                f"EMA9_slope={details.get('sig_ema',0):+d}  slope={details.get('slope_now',0):,.2f}",
                f"BigMove={details.get('sig_big',0):+d}  move={details.get('move_pct',0):.3f}%",
                f"SR_rev={details.get('sig_sr',0):+d}  hi={details.get('roll_high',0):,.2f}  lo={details.get('roll_low',0):,.2f}",
                f"RSI={details.get('rsi',0):.1f}  ATR={details.get('atr_now',0):.1f}  spread={details.get('regime_spread_pct',0):.3f}%",
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


# ── Fast resolve via Kraken live candle ──────────────────────────────────────
# NOTE: Uses kraken.get_ohlcv() (live API) to infer result from candle close vs strike.

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

        # Live fetch from Kraken API
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

    # ── Stop flag ─────────────────────────────────────────────────────────────
    if STOP_FLAG_PATH.exists():
        print(f"[{STRATEGY_NAME}] bot_stopped.flag present — exiting.")
        return

    # ── Pause flag ────────────────────────────────────────────────────────────
    if PAUSE_FLAG_PATH.exists():
        print(f"[{STRATEGY_NAME}] bot_paused.flag present — skipping this cycle.")
        return

    state = _load_state()

    # ── Step 1: Resolve previous trade ───────────────────────────────────────
    trade     = _db_get_latest_unresolved()
    bg_thread = None

    if trade:
        fast = _fast_resolve(trade, state)
        if fast:
            state = fast[0]
            print(f"[{STRATEGY_NAME}] Fast resolve succeeded.")
        else:
            print(f"[{STRATEGY_NAME}] Fast resolve inconclusive — Kalshi will confirm in background.")

        # Always confirm with Kalshi in background for DB accuracy
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

    # ── Step 3: Compute signals (live Kraken candles) ─────────────────────────
    result = _compute_signals()
    signal    = result["signal"]
    contracts = result["contracts"]
    details   = result["details"]
    reason    = result["reason"]

    if signal is None:
        _db_insert_no_trade(ticker, now_str, reason or "No signal")
        if bg_thread:
            bg_thread.join(timeout=2)
        return

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
        print(f"[PAPER] BUY {contracts}x {signal.upper()} on {ticker} @ {int(PAPER_COST*100)}¢  total=${total_cost:.2f}  payout=${payout:.2f}")

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
