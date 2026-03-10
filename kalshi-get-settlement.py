"""
kalshi-get-settlement.py — Standalone diagnostic: show most recent 2 settlements.
Read only. No side effects.

Usage:
  python kalshi-get-settlement.py
"""

import sys
import zoneinfo
from datetime import datetime, timezone

sys.path.insert(0, ".")
import connectors.kalshi as kalshi

EST = zoneinfo.ZoneInfo("America/New_York")  # handles EST/EDT automatically


def fmt_time(t_str):
    if not t_str:
        return "—"
    try:
        dt = datetime.fromisoformat(t_str.replace("Z", "+00:00"))
        return dt.astimezone(EST).strftime("%Y-%m-%d %I:%M:%S %p %Z")
    except Exception:
        return t_str


def print_settlement(s: dict, label: str = ""):
    print(f"\n  {'─'*55}")
    if label:
        print(f"  {label}")
    print(f"  Ticker          : {s.get('ticker', '—')}")
    print(f"  Event Ticker    : {s.get('event_ticker', '—')}")
    print(f"  Market Result   : {s.get('market_result', '—')}")
    print(f"  Settled Time    : {fmt_time(s.get('settled_time'))}")
    print(f"  YES Count       : {s.get('yes_count', '—')}  ({s.get('yes_count_fp', '—')} fp)")
    print(f"  YES Total Cost  : {s.get('yes_total_cost', '—')}¢")
    print(f"  NO Count        : {s.get('no_count', '—')}  ({s.get('no_count_fp', '—')} fp)")
    print(f"  NO Total Cost   : {s.get('no_total_cost', '—')}¢")
    print(f"  Revenue         : {s.get('revenue', '—')}¢")
    print(f"  Fee Cost        : ${s.get('fee_cost', '—')}")
    print(f"  Value (yes/¢)   : {s.get('value', '—')}")
    print(f"  {'─'*55}")


def main():
    now_est = datetime.now(EST).strftime("%Y-%m-%d %I:%M:%S %p %Z")
    print(f"\n{'='*58}")
    print(f"  Kalshi Settlements — {now_est}")
    print(f"{'='*58}")

    resp = kalshi._get("/portfolio/settlements", params={"limit": 3})
    settlements = resp.get("settlements", [])

    if not settlements:
        print("\n  No settlements found.")
    else:
        for i, s in enumerate(settlements):
            print_settlement(s, label=f"[ Settlement {i+1} ]")

    print(f"\n{'='*58}\n")


if __name__ == "__main__":
    main()
