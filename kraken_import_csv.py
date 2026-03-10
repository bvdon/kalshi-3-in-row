"""
kraken_import_csv.py — One-time import of Kraken historical OHLCVT CSV into local SQLite.

Download the CSV from Kraken's Google Drive:
  https://support.kraken.com/hc/en-us/articles/360047124832

Look for: XBTUSD_15.csv  (15-minute BTC/USD candles)

Then run:
  python kraken_import_csv.py /path/to/XBTUSD_15.csv

Kraken CSV format (no header):
  timestamp, open, high, low, close, volume, trades
"""

import sys
import sqlite3
import csv
from pathlib import Path
from datetime import datetime, timezone

DB_PATH    = Path(__file__).parent / "candles.db"
PAIR       = "XBTUSD"
INTERVAL   = 15


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
    if len(sys.argv) < 2:
        print("Usage: python kraken_import_csv.py /path/to/XBTUSD_15.csv")
        sys.exit(1)

    csv_path = Path(sys.argv[1])
    if not csv_path.exists():
        print(f"Error: file not found: {csv_path}")
        sys.exit(1)

    conn = sqlite3.connect(DB_PATH)
    init_db(conn)

    # Count existing rows
    existing = conn.execute("SELECT COUNT(*) FROM btc_candles").fetchone()[0]
    print(f"  DB: {DB_PATH}")
    print(f"  Existing rows: {existing:,}")
    print(f"  Importing: {csv_path} ...")

    inserted = 0
    skipped  = 0
    rows     = []

    with open(csv_path, "r") as f:
        reader = csv.reader(f)
        for line in reader:
            if not line or line[0].startswith("#"):
                continue
            try:
                ts     = int(line[0])
                open_  = float(line[1])
                high   = float(line[2])
                low    = float(line[3])
                close  = float(line[4])
                volume = float(line[5]) if len(line) > 5 else None
                trades = int(line[6])   if len(line) > 6 else None
                rows.append((ts, open_, high, low, close, volume, trades))
            except Exception as e:
                skipped += 1

    # Bulk insert, ignore duplicates
    conn.executemany(
        "INSERT OR IGNORE INTO btc_candles (ts, open, high, low, close, volume, trades) VALUES (?,?,?,?,?,?,?)",
        rows
    )
    conn.commit()
    inserted = len(rows) - skipped

    total = conn.execute("SELECT COUNT(*) FROM btc_candles").fetchone()[0]
    first = conn.execute("SELECT ts FROM btc_candles ORDER BY ts ASC  LIMIT 1").fetchone()[0]
    last  = conn.execute("SELECT ts FROM btc_candles ORDER BY ts DESC LIMIT 1").fetchone()[0]

    first_dt = datetime.fromtimestamp(first, tz=timezone.utc).strftime("%Y-%m-%d")
    last_dt  = datetime.fromtimestamp(last,  tz=timezone.utc).strftime("%Y-%m-%d")

    print(f"  Rows parsed:  {len(rows):,}")
    print(f"  Rows skipped: {skipped:,}")
    print(f"  Total in DB:  {total:,} candles")
    print(f"  Range:        {first_dt} → {last_dt}")
    print(f"\n  ✅ Done. Run kraken_sync_candles.py to pull recent data from the API.")


if __name__ == "__main__":
    main()
