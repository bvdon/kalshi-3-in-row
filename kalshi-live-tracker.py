"""
kalshi-live-tracker.py
======================
Live win-rate and P&L tracker for Kalshi trades.

Connects to trades.db at /Users/roberthenning/python-projects/kalshi-dashboard/trades.db
and reports rolling stats for the last N trades.

Usage:
  python3 kalshi-live-tracker.py           # last 100 trades
  python3 kalshi-live-tracker.py --n 50    # last 50 trades
  python3 kalshi-live-tracker.py --n 200   # last 200 trades
"""

import sqlite3
import argparse
from datetime import datetime, timezone

TRADES_DB = "/Users/roberthenning/python-projects/kalshi-dashboard/trades.db"
WR_WARNING_THRESHOLD = 0.52  # Warn if WR < 52%


def get_db_schema(conn):
    """Discover table and column names."""
    cur = conn.cursor()
    cur.execute("SELECT name FROM sqlite_master WHERE type='table'")
    tables = [r[0] for r in cur.fetchall()]
    schema = {}
    for t in tables:
        cur.execute(f"PRAGMA table_info({t})")
        cols = [row[1] for row in cur.fetchall()]
        schema[t] = cols
    return schema


def find_trades_table(schema):
    """Best-guess the trades table."""
    for name in schema:
        if "trade" in name.lower():
            return name
    # fallback: first table
    return list(schema.keys())[0] if schema else None


def load_trades(n):
    conn = sqlite3.connect(TRADES_DB)
    schema = get_db_schema(conn)

    if not schema:
        print("ERROR: No tables found in trades.db")
        conn.close()
        return []

    print(f"Available tables: {list(schema.keys())}")
    table = find_trades_table(schema)
    cols  = schema[table]
    print(f"Using table: '{table}' | columns: {cols}\n")

    cur = conn.cursor()

    # Try to find sensible column names
    # Common patterns: result/outcome/pnl, created_at/timestamp/date, amount/profit
    result_col = next((c for c in cols if c.lower() in ("result", "outcome", "win", "pnl", "profit", "net")), None)
    date_col   = next((c for c in cols if c.lower() in ("created_at", "timestamp", "date", "ts", "time", "settled_at")), None)
    amount_col = next((c for c in cols if c.lower() in ("pnl", "profit", "net", "amount", "net_profit", "gain")), None)

    # If no result col, try to derive from pnl sign
    if result_col is None and amount_col is not None:
        result_col = None  # will derive below

    if date_col:
        cur.execute(f"SELECT * FROM {table} ORDER BY {date_col} DESC LIMIT {n}")
    else:
        cur.execute(f"SELECT * FROM {table} LIMIT {n}")

    rows = cur.fetchall()
    conn.close()

    return rows, cols, table, result_col, date_col, amount_col


def parse_result(val):
    """Interpret a result value as win (1), loss (-1), or unknown (0)."""
    if val is None:
        return 0
    if isinstance(val, (int, float)):
        if val > 0:
            return 1
        elif val < 0:
            return -1
        return 0
    s = str(val).lower().strip()
    if s in ("win", "won", "yes", "1", "true", "profit", "w"):
        return 1
    if s in ("loss", "lose", "lost", "no", "-1", "false", "l"):
        return -1
    return 0


def main():
    parser = argparse.ArgumentParser(description="Kalshi live trade tracker")
    parser.add_argument("--n", type=int, default=100, help="Number of recent trades to analyze")
    args = parser.parse_args()

    print(f"{'='*60}")
    print(f"  KALSHI LIVE TRADE TRACKER  |  Last {args.n} trades")
    print(f"{'='*60}\n")

    try:
        result = load_trades(args.n)
    except FileNotFoundError:
        print(f"ERROR: trades.db not found at:\n  {TRADES_DB}")
        print("\nHint: Make sure the kalshi-dashboard project has trades recorded.")
        return
    except Exception as e:
        print(f"ERROR loading trades: {e}")
        return

    rows, cols, table, result_col, date_col, amount_col = result

    if not rows:
        print(f"No trades found in table '{table}'.")
        return

    col_idx = {c: i for i, c in enumerate(cols)}

    wins      = 0
    losses    = 0
    total_pnl = 0.0
    dates     = []

    for row in rows:
        # Try result column
        if result_col and result_col in col_idx:
            rv = row[col_idx[result_col]]
            outcome = parse_result(rv)
        elif amount_col and amount_col in col_idx:
            rv = row[col_idx[amount_col]]
            outcome = parse_result(rv)
        else:
            outcome = 0

        if outcome == 1:
            wins += 1
        elif outcome == -1:
            losses += 1

        # P&L
        if amount_col and amount_col in col_idx:
            v = row[col_idx[amount_col]]
            try:
                total_pnl += float(v)
            except (TypeError, ValueError):
                pass

        # Dates
        if date_col and date_col in col_idx:
            dv = row[col_idx[date_col]]
            if dv is not None:
                dates.append(dv)

    total_with_outcome = wins + losses
    wr        = wins / total_with_outcome if total_with_outcome > 0 else 0.0
    avg_pnl   = total_pnl / len(rows) if rows else 0.0

    # Trades per week
    trades_per_week = None
    if len(dates) >= 2:
        try:
            # Try to parse as unix timestamp or ISO string
            def parse_date(d):
                if isinstance(d, (int, float)):
                    return datetime.fromtimestamp(d, tz=timezone.utc)
                return datetime.fromisoformat(str(d).replace("Z", "+00:00"))

            parsed = sorted([parse_date(d) for d in dates])
            span_days = (parsed[-1] - parsed[0]).total_seconds() / 86400
            if span_days > 0:
                trades_per_week = len(rows) / (span_days / 7)
        except Exception:
            pass

    # Print summary
    print(f"  Trades analyzed : {len(rows)}")
    print(f"  With outcome    : {total_with_outcome}")
    print(f"  Wins            : {wins}")
    print(f"  Losses          : {losses}")
    print(f"  Win Rate        : {wr*100:.1f}%")
    print(f"  Total P&L       : ${total_pnl:,.2f}")
    print(f"  Avg P&L/trade   : ${avg_pnl:,.2f}")
    if trades_per_week is not None:
        print(f"  Trades/week     : {trades_per_week:.1f}")
    print()

    # Warnings
    if total_with_outcome > 0 and wr < WR_WARNING_THRESHOLD:
        print(f"  ⚠️  WARNING: Live WR {wr*100:.1f}% is BELOW {WR_WARNING_THRESHOLD*100:.0f}% threshold!")
        print(f"  ⚠️  Review strategy — edge may be degrading.")
    elif total_with_outcome > 0:
        print(f"  ✅  WR {wr*100:.1f}% is above {WR_WARNING_THRESHOLD*100:.0f}% warning threshold.")

    print(f"\n{'='*60}")


if __name__ == "__main__":
    main()
