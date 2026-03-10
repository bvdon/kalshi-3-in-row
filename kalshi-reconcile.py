"""
kalshi-reconcile.py — Reconcile unresolved trade records in the DB.

Finds any trade rows where:
  - no_trade IS NULL (it was a real trade attempt)
  - entry_price OR total_cost IS NULL  → fetch fill from Kalshi orders API
  - profit IS NULL AND loss IS NULL    → resolve outcome from market result

Safe to run anytime — read-heavy, only writes to rows that are incomplete.
Skips rows that are already fully resolved.

Usage:
  .venv/bin/python3 kalshi-reconcile.py
  .venv/bin/python3 kalshi-reconcile.py --dry-run
  .venv/bin/python3 kalshi-reconcile.py --strategy THREE_IN_ROW
"""

import argparse
import sqlite3
import sys
import zoneinfo
from datetime import datetime, timezone

sys.path.insert(0, ".")
import config
import connectors.kalshi as kalshi

EST = zoneinfo.ZoneInfo("America/New_York")


def get_unresolved(strategy: str = None) -> list[dict]:
    conn = sqlite3.connect(config.DB_PATH)
    conn.row_factory = sqlite3.Row
    cur  = conn.cursor()
    query = """
        SELECT * FROM kalshi_trades
        WHERE no_trade IS NULL
          AND (
            entry_price IS NULL
            OR total_cost IS NULL
            OR profit IS NULL
            OR loss IS NULL
            OR floor IS NULL
          )
        ORDER BY id
    """
    params = []
    if strategy:
        query = query.replace("WHERE no_trade IS NULL", "WHERE no_trade IS NULL AND strategy=?")
        params.append(strategy)
    rows = cur.execute(query, params).fetchall()
    conn.close()
    return [dict(r) for r in rows]


def fetch_order_fill(ticker: str, side: str) -> dict:
    """
    Fetch fill details from Kalshi orders API for a given ticker+side.
    Returns dict with: fill_count, total_cost, entry_price, fees, status
    or None if no order found.
    """
    try:
        data   = kalshi._get("/portfolio/orders", params={"ticker": ticker, "limit": 10})
        orders = data.get("orders", [])
        # Find the matching executed order (prefer executed, fall back to canceled)
        executed = [o for o in orders if o.get("side") == side and o.get("status") == "executed"]
        canceled = [o for o in orders if o.get("side") == side and o.get("status") == "canceled"]
        order = executed[0] if executed else (canceled[0] if canceled else None)
        if not order:
            return None

        fill_count = order.get("fill_count") or 0
        tfc = order.get("taker_fill_cost") or 0
        mfc = order.get("maker_fill_cost") or 0
        tf  = order.get("taker_fees") or 0
        mf  = order.get("maker_fees") or 0

        fill_cost_cents = tfc + mfc
        fill_fees_cents = tf + mf

        return {
            "status":      order.get("status"),
            "fill_count":  fill_count,
            "total_cost":  fill_cost_cents / 100.0 if fill_cost_cents else None,
            "entry_price": (fill_cost_cents / 100.0 / fill_count) if fill_cost_cents and fill_count else None,
            "fees":        fill_fees_cents / 100.0,
        }
    except Exception as e:
        print(f"  [WARN] fetch_order_fill failed for {ticker}: {e}")
        return None


def fetch_market_result(ticker: str) -> dict:
    """Returns status and result for a market ticker."""
    try:
        m = kalshi._get(f"/markets/{ticker}").get("market", {})
        return {"status": m.get("status"), "result": m.get("result")}
    except Exception as e:
        print(f"  [WARN] fetch_market_result failed for {ticker}: {e}")
        return {}


def reconcile_row(row: dict, dry_run: bool) -> str:
    """
    Reconcile a single trade row. Returns a human-readable summary line.
    """
    rid     = row["id"]
    ticker  = row["market_ticker"]
    side    = row["side"]
    payout  = row.get("payout") or 10.0   # default: 10 contracts × $1

    updates  = {}
    notes    = []

    # ── Step 1: Fill data ─────────────────────────────────────────────────────
    needs_fill = row.get("entry_price") is None or row.get("total_cost") is None
    fill       = fetch_order_fill(ticker, side) if needs_fill else None

    if fill:
        if fill["status"] == "canceled" and (fill["fill_count"] or 0) == 0:
            # Order never filled — mark as no_trade
            updates["no_trade"]    = "Order not filled (canceled)"
            updates["entry_price"] = None
            updates["total_cost"]  = None
            updates["fees"]        = None
            updates["payout"]      = None
            updates["profit"]      = None
            updates["loss"]        = None
            notes.append("canceled → no_trade")
        elif fill["total_cost"] is not None:
            updates["entry_price"] = fill["entry_price"]
            updates["total_cost"]  = fill["total_cost"]
            updates["fees"]        = fill["fees"]
            notes.append(f"fill={fill['fill_count']}x @ {fill['entry_price']*100:.0f}¢  cost=${fill['total_cost']:.2f}  fees=${fill['fees']:.2f}")
        else:
            notes.append("fill found but cost=0 (unfilled?)")
    elif needs_fill:
        notes.append("no order found on Kalshi")

    # ── Step 2: P&L resolution ────────────────────────────────────────────────
    # Skip P&L if we just marked it no_trade
    if "no_trade" not in updates:
        total_cost = updates.get("total_cost") or row.get("total_cost")
        fees_val   = updates.get("fees") if "fees" in updates else row.get("fees") or 0.0
        payout_val = row.get("payout") or 0.0

        # Recalculate if P&L is missing OR if profit looks wrong (e.g. profit == payout, meaning total_cost was 0 at resolve time)
        profit_val = row.get("profit")
        pnl_looks_wrong = (
            profit_val is not None
            and total_cost is not None
            and total_cost > 0
            and abs(profit_val - payout_val) < 0.01  # profit == payout → cost wasn't subtracted
        )
        needs_pnl  = row.get("profit") is None or row.get("loss") is None or pnl_looks_wrong

        if needs_pnl and total_cost is not None:
            market = fetch_market_result(ticker)
            result = market.get("result")
            status = market.get("status")

            if status == "finalized" and result in ("yes", "no"):
                fees = fees_val
                if result == side:
                    profit = round(payout - total_cost - fees, 4)
                    updates["profit"] = profit
                    updates["loss"]   = 0.0
                    notes.append(f"WIN profit=${profit:.2f}")
                else:
                    loss = round(total_cost + fees, 4)
                    updates["profit"] = 0.0
                    updates["loss"]   = loss
                    notes.append(f"LOSS loss=${loss:.2f}")
            elif status != "finalized":
                notes.append(f"market not finalized yet (status={status})")
            else:
                notes.append(f"unexpected result={result}")
        elif needs_pnl:
            notes.append("skipping P&L — total_cost still unknown")

    # ── Step 3: Floor price ───────────────────────────────────────────────────
    if "no_trade" not in updates and side and row.get("floor") is None:
        try:
            market_status = fetch_market_result(ticker).get("status")
            if market_status == "finalized":
                floor = kalshi.get_cycle_floor(ticker, side)
                if floor is not None:
                    updates["floor"] = floor
                    notes.append(f"floor=${floor:.2f}")
        except Exception as e:
            notes.append(f"floor fetch failed: {e}")

    summary = f"id={rid} {ticker} side={side} | {' | '.join(notes) if notes else 'nothing to do'}"

    if updates and not dry_run:
        conn = sqlite3.connect(config.DB_PATH)
        cur  = conn.cursor()
        set_clause = ", ".join(f"{k}=?" for k in updates)
        values     = list(updates.values()) + [rid]
        cur.execute(f"UPDATE kalshi_trades SET {set_clause} WHERE id=?", values)
        conn.commit()
        conn.close()
        summary += "  ✓ updated"
    elif updates and dry_run:
        summary += f"  [DRY RUN] would set: {updates}"

    return summary


def main():
    parser = argparse.ArgumentParser(description="Reconcile unresolved Kalshi trade records")
    parser.add_argument("--dry-run",   action="store_true", help="Show what would change without writing")
    parser.add_argument("--strategy",  default=None,        help="Filter to one strategy (e.g. THREE_IN_ROW)")
    args = parser.parse_args()

    rows = get_unresolved(args.strategy)
    now  = datetime.now(EST).strftime("%Y-%m-%d %I:%M:%S %p %Z")

    print(f"\nKalshi Reconcile — {now}")
    print(f"Strategy filter: {args.strategy or 'all'}")
    print(f"Unresolved rows: {len(rows)}")
    if args.dry_run:
        print("Mode: DRY RUN\n")
    else:
        print()

    if not rows:
        print("Nothing to reconcile.")
        return

    for row in rows:
        result = reconcile_row(row, dry_run=args.dry_run)
        print(f"  {result}")

    print()


if __name__ == "__main__":
    main()
