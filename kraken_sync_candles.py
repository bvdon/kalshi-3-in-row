"""
kraken_sync_candles.py — Sync recent BTC/USD 15m candles from Kraken API into local SQLite.

Run this once after importing the CSV to fill the gap to today.
Then run it daily (or add to cron) to keep the DB current.

Usage:
  python kraken_sync_candles.py
"""

import sqlite3
from pathlib import Path
from datetime import datetime, timezone

import connectors.kraken as kraken

DB_PATH  = Path(__file__).parent / "candles.db"
PAIR     = "XBTUSD"
INTERVAL = 15


def init_db(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS btc_candles (
            ts       INTEGER PRIMARY KEY,
            open     REAL NOT NULL,
            high     REAL NOT NULL,
            low      REAL NOT NULL,
            close    REAL NOT NULL,
            volume   REAL,
            trades   INTEGER
        )
    """)
    conn.execute("CREATE INDEX IF NOT EXISTS idx_btc_candles_ts ON btc_candles(ts)")
    conn.commit()


def main():
    conn = sqlite3.connect(DB_PATH)
    init_db(conn)

    existing = conn.execute("SELECT COUNT(*) FROM btc_candles").fetchone()[0]
    last_ts  = conn.execute("SELECT MAX(ts) FROM btc_candles").fetchone()[0]

    last_dt = datetime.fromtimestamp(last_ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC") if last_ts else "none"
    print(f"  DB: {DB_PATH}")
    print(f"  Existing rows: {existing:,}  |  Last candle: {last_dt}")
    print(f"  Fetching latest candles from Kraken API...")

    candles = kraken.get_ohlcv(PAIR, interval_minutes=INTERVAL)
    candles = candles[:-1]  # drop in-progress candle

    rows = [
        (c["ts"], c["open"], c["high"], c["low"], c["close"], c.get("volume"), None)
        for c in candles
    ]

    conn.executemany(
        "INSERT OR IGNORE INTO btc_candles (ts, open, high, low, close, volume, trades) VALUES (?,?,?,?,?,?,?)",
        rows
    )
    conn.commit()

    new_total = conn.execute("SELECT COUNT(*) FROM btc_candles").fetchone()[0]
    added     = new_total - existing
    new_last  = conn.execute("SELECT MAX(ts) FROM btc_candles").fetchone()[0]
    new_last_dt = datetime.fromtimestamp(new_last, tz=timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    print(f"  Candles from API: {len(candles)}")
    print(f"  New rows added:   {added}")
    print(f"  Total in DB:      {new_total:,}")
    print(f"  Last candle now:  {new_last_dt}")
    print(f"\n  ✅ Sync complete.")


if __name__ == "__main__":
    main()
