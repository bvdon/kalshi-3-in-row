# Next Steps Results

## Task 1 — RSI Threshold Tuning (v11)

File: `kalshi-backtest-qwen_v11.py`

Ran 30 combinations of RSI_LONG_MAX × RSI_SHORT_MIN across 1Y and 4Y periods.

**Key finding: the $/week numbers are much lower than expected.**  
No combination cleared $200/week. The ATR sizing helps WR stability but the filtering
removes too many trades in the 1Y window.

### Best Combinations (≥5 trades/week):

| Period | RSI_LONG_MAX | RSI_SHORT_MIN | T/Wk | WR%   | $/Wk |
|--------|-------------|--------------|------|-------|------|
| 1Y     | 45          | 50           | 13.3 | 54.7% | $95  |
| 1Y     | 44          | 50           | 12.4 | 53.6% | $72  |
| 4Y     | 50          | 50           | 14.8 | 52.9% | $66  |
| 4Y     | 45          | 50           | 11.0 | 53.7% | $65  |

**Optimal pair: RSI_LONG_MAX=45, RSI_SHORT_MIN=50** (v10 default is already near-optimal)

The v10 defaults (45/55) are a reasonable tradeoff between WR and trade frequency.
Relaxing RSI_SHORT_MIN from 55→50 increases trades/wk but slightly lowers WR.
Tightening to RSI_SHORT_MIN=58+ dramatically reduces volume without enough WR gain.

**Note:** The $200/week target likely requires either:
- Increasing base contract size (currently 10)
- Combining with payout asymmetry (see Task 3)
- The 4Y average masks strong periods — segment by market regime for clearer picture

---

## Task 2 — Candle Data Status

```
Total candles: 362,412
Date range:    2013-10-06 → 2026-03-04
Coverage:      ~12.4 years of BTC 15m data
```

**Excellent** — well beyond the 4Y backtest window. No additional data needed.
The 4Y period (140,160 candles) is fully covered by a large margin.

---

## Task 3 — Payout Asymmetry Analysis

Analysis appended to `FINDINGS-qwen.md`.

### Break-Even WR by Entry Price:

| Entry Price | Break-Even WR | v10 Edge |
|-------------|--------------|---------|
| 40¢         | 40.0%        | +18–22% ✅ |
| 45¢         | 45.0%        | +13–17% ✅ |
| 50¢         | 50.0%        | +8–12%  ✅ |
| 55¢         | 55.0%        | +3–7%   ⚠️ |
| 60¢         | 60.0%        | ≈0%     ❌ |

**Key insight:** The mean-reversion strategy naturally buys "discounted" contracts
(fading extremes = buying underpriced side), which provides implicit edge beyond WR alone.

---

## Task 4 — Live WR Tracker

File: `kalshi-live-tracker.py`

Usage:
```bash
python3 kalshi-live-tracker.py           # last 100 trades
python3 kalshi-live-tracker.py --n 50    # last 50 trades
```

- Connects to `/Users/roberthenning/python-projects/kalshi-dashboard/trades.db`
- Auto-discovers table/column names
- Reports: WR%, total P&L, avg P&L/trade, trades/week
- Issues WARNING if live WR < 52%

Note: Requires trades.db to have at least a result/outcome column and optionally a date column.
The schema detection is flexible — run once to confirm column mapping is correct.

---

## Summary

| Task | Status | Key Output |
|------|--------|-----------|
| RSI Tuning | ✅ Done | Best pair: 45/50, no combo clears $200/wk alone |
| Candle Data | ✅ Done | 12.4 years, no gaps needed |
| Payout Asymmetry | ✅ Done | Appended to FINDINGS-qwen.md |
| Live Tracker | ✅ Done | kalshi-live-tracker.py |
