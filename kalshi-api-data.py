"""
kalshi-api-data.py — Standalone Kalshi API diagnostic tool.
Run anytime to inspect current and recent BTC 15m markets.
No side effects — read only, nothing written to DB or mg_state.

Usage:
  python kalshi-api-data.py
"""

import sys
from datetime import datetime, timezone, timedelta

sys.path.insert(0, ".")
import connectors.kalshi as kalshi

EST = timezone(timedelta(hours=-5))
MARKET_SERIES = "KXBTC15M"


def fmt_time(t_str):
    if not t_str:
        return "—"
    try:
        dt = datetime.fromisoformat(t_str.replace("Z", "+00:00"))
        return dt.astimezone(EST).strftime("%Y-%m-%d %I:%M:%S %p EST")
    except Exception:
        return t_str


def print_market(m: dict, label: str = ""):
    ticker      = m.get("ticker", "—")
    status      = m.get("status", "—")
    result      = m.get("result", "—") or "pending"
    yes_ask     = m.get("yes_ask", "—")
    yes_bid     = m.get("yes_bid", "—")
    no_ask      = m.get("no_ask", "—")
    no_bid      = m.get("no_bid", "—")
    close_time  = fmt_time(m.get("close_time"))
    open_time   = fmt_time(m.get("open_time"))
    sub_title   = m.get("yes_sub_title", "—")

    # Time remaining
    close_raw = m.get("close_time")
    if close_raw:
        try:
            ct = datetime.fromisoformat(close_raw.replace("Z", "+00:00"))
            secs = int((ct - datetime.now(timezone.utc)).total_seconds())
            if secs > 0:
                time_left = f"{secs // 60}m {secs % 60}s remaining"
            else:
                time_left = f"closed {abs(secs)}s ago"
        except Exception:
            time_left = "—"
    else:
        time_left = "—"

    print(f"\n  {'─'*50}")
    if label:
        print(f"  {label}")
    print(f"  Ticker    : {ticker}")
    print(f"  Status    : {status}  |  Result: {result}")
    print(f"  Strike    : {sub_title}")
    print(f"  Open      : {open_time}")
    print(f"  Close     : {close_time}  ({time_left})")
    print(f"  YES       : ask={yes_ask}¢  bid={yes_bid}¢")
    print(f"  NO        : ask={no_ask}¢  bid={no_bid}¢")
    print(f"  {'─'*50}")


def main():
    now_est = datetime.now(EST).strftime("%Y-%m-%d %I:%M:%S %p EST")
    #print(f"\n{'='*55}")
    #print(f"  Kalshi API Diagnostic — {now_est}")
    #print(f"  Series: {MARKET_SERIES}")
    #print(f"{'='*55}")

    # 1. Active ticker
    print("\n📍 ACTIVE MARKET")
    active_ticker = kalshi.get_active_ticker(MARKET_SERIES)
    if active_ticker:
        data = kalshi._get(f"/markets/{active_ticker}")
        print_market(data.get("market", {}), label="[ CURRENT CYCLE ]")
    else:
        print("  No active market found.")

    '''
    # 2. Recent markets — derive last 4 tickers from active ticker's close time
    print("\n📋 RECENT MARKETS (last 4 closed)")
    if active_ticker:
        try:
            active_data = kalshi._get(f"/markets/{active_ticker}")
            close_str = active_data.get("market", {}).get("open_time", "")
            if close_str:
                open_dt = datetime.fromisoformat(close_str.replace("Z", "+00:00"))
                found = 0
                for i in range(0, 7):
                    past_dt = (open_dt - timedelta(minutes=15 * i)).astimezone(EST)
                    yr    = past_dt.strftime("%y")
                    month = past_dt.strftime("%b").upper()
                    day   = past_dt.strftime("%d")
                    hhmm  = past_dt.strftime("%H%M")
                    mins  = past_dt.strftime("%M")
                    past_ticker = f"{MARKET_SERIES}-{yr}{month}{day}{hhmm}-{mins}"
                    try:
                        pdata = kalshi._get(f"/markets/{past_ticker}")
                        pm = pdata.get("market", {})
                        if pm:
                            print_market(pm)
                            found += 1
                            if found >= 4:
                                break
                    except Exception:
                        continue
        except Exception as e:
            print(f"  Error fetching recent markets: {e}")
    else:
        print("  No active ticker — cannot derive recent markets.")

    print(f"\n{'='*55}\n")
'''

if __name__ == "__main__":
    main()
