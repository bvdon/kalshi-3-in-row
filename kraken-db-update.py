"""
kraken-db-update.py — Sync candles.db with the latest Kraken 15m OHLCV data.

Steps:
  1. Find the most recent ts already in candles.db
  2. Fetch from Kraken starting at that ts (returns up to 720 candles)
  3. Insert only candles with ts > existing max (no duplicates)
  4. Display the last 800 rows inserted (oldest-first)
  5. Print a summary: rows inserted, date range of new candles
"""

import sqlite3
from datetime import datetime, timezone, timedelta

from connectors.kraken import get_ohlcv

DB_PATH      = "./candles.db"
KRAKEN_PAIR  = "XBTUSD"
INTERVAL_MIN = 15
DISPLAY_ROWS = 800

EST = timezone(timedelta(hours=-5))


def ts_to_str(ts: int) -> str:
    return datetime.fromtimestamp(ts, tz=EST).strftime("%Y-%m-%d %I:%M:%S %p EST")


def main():
    conn = sqlite3.connect(DB_PATH)
    cur  = conn.cursor()

    # ── Step 1: Find current max ts ───────────────────────────────────────────
    cur.execute("SELECT MAX(ts), COUNT(*) FROM btc_candles")
    max_ts, total_before = cur.fetchone()
    print(f"candles.db before update: {total_before:,} rows")
    print(f"Most recent candle:       {ts_to_str(max_ts)}  (ts={max_ts})")
    print()

    # ── Step 2: Fetch from Kraken since max_ts ────────────────────────────────
    print(f"Fetching from Kraken since ts={max_ts} ...")
    candles = get_ohlcv(KRAKEN_PAIR, interval_minutes=INTERVAL_MIN, since=max_ts)
    print(f"Kraken returned {len(candles)} candles")

    # ── Step 3: Insert only new candles (ts > max_ts, no duplicates) ──────────
    new_candles = [c for c in candles if c["ts"] > max_ts]
    inserted = 0
    for c in new_candles:
        try:
            cur.execute(
                "INSERT OR IGNORE INTO btc_candles (ts, open, high, low, close) VALUES (?, ?, ?, ?, ?)",
                (c["ts"], c["open"], c["high"], c["low"], c["close"]),
            )
            if cur.rowcount:
                inserted += 1
        except Exception as e:
            print(f"  Insert error ts={c['ts']}: {e}")

    conn.commit()

    cur.execute("SELECT COUNT(*) FROM btc_candles")
    total_after = cur.fetchone()[0]

    # ── Step 4: Display last DISPLAY_ROWS inserted rows ───────────────────────
    cur.execute(
        f"SELECT ts, open, high, low, close FROM btc_candles ORDER BY ts DESC LIMIT {DISPLAY_ROWS}"
    )
    rows = cur.fetchall()
    rows_asc = list(reversed(rows))  # show oldest-first

    print()
    print(f"{'─'*90}")
    print(f"  {'#':<5}  {'TIMESTAMP (EST)':<28}  {'OPEN':>12}  {'HIGH':>12}  {'LOW':>12}  {'CLOSE':>12}  {'COLOR':>12}")
    print(f"{'─'*90}")
    for i, (ts, o, h, l, c) in enumerate(rows_asc, 1):
        color = "RED"
        if o < c:
            color = "GREEN"



        marker = " ◀ NEW" if ts > max_ts else ""
        print(f"  {i:<5}  {ts_to_str(ts):<28}  ${o:>11,.2f}  ${h:>11,.2f}  ${l:>11,.2f}  ${c:>11,.2f} {color}{marker}")
    print(f"{'─'*90}")

    # ── Step 5: Summary ───────────────────────────────────────────────────────
    print()
    print("=" * 60)
    print("  UPDATE SUMMARY")
    print("=" * 60)
    print(f"  Rows before:   {total_before:,}")
    print(f"  Rows after:    {total_after:,}")
    print(f"  New inserted:  {inserted}")

    if inserted > 0:
        new_ts_list = [c["ts"] for c in new_candles if c["ts"] > max_ts]
        print(f"  Oldest new:    {ts_to_str(min(new_ts_list))}")
        print(f"  Newest new:    {ts_to_str(max(new_ts_list))}")
    else:
        print("  No new candles — database is already up to date.")

    print("=" * 60)
    conn.close()


if __name__ == "__main__":
    main()
