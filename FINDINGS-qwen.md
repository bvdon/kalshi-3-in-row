# Kalshi Backtest — Qwen Strategy Exploration Findings

Backtests against `candles.db` (BTC 15m candles from Kraken).
All tests: $5 profit per win, $5 loss per loss, default 10 contracts.
Target: ≥ $200/week net P&L on 1Y+ lookback.

---

## Version Summary

### v3 — Candlestick Patterns
**Idea:** Doji reversal, engulfing continuation, 3-in-a-row exhaustion, hammer/shooting star.

| Result | Detail |
|--------|--------|
| WR range | 51–53% |
| Best signal | 3-in-a-row exhaustion |
| $200/week? | No |
| Verdict | Patterns alone are too weak. Useful as a *filter* on top of another signal, not a standalone. |

---

### v4 — Regime Detection (Ranging vs Trending)
**Idea:** Filter EMA9 slope-reversal bets to only take them during "ranging" markets (EMA9/EMA50 spread < 0.5%).

| Result | Detail |
|--------|--------|
| WR improvement | +1–2% in ranging regime |
| Trade count | Drops ~30% |
| Trending regime | Consistently underperforms |
| $200/week? | Not on its own |
| Verdict | Ranging filter is a net positive but insufficient alone. Apply it as a trade *qualifier*. |

---

### v5 — Multi-Timeframe Momentum Exhaustion
**Idea:** Use 1H trend direction vs 15m RSI to find short-term exhaustion setups.

| Result | Detail |
|--------|--------|
| WR improvement | +1–2% over plain 15m RSI |
| Best setup | MTF Exhaustion (fade 1H trend when 15m RSI extreme) |
| $200/week? | Marginal on 1Y, no on shorter |
| Verdict | Small but consistent edge. The 1H/15m divergence is a real signal; WR ceiling ~54% standalone. |

---

### v6 — Price Action S/R Levels
**Idea:** Bet reversal when price is within X% of rolling N-period high or low.

| Result | Detail |
|--------|--------|
| Best params | lookback=40, near=0.3% |
| WR range | 53–55% |
| Breakout continuation | Consistently loses — BTC 15m reverts more than it trends |
| $200/week? | No standalone |
| Verdict | S/R reversal is a real edge. Better lookback = 40 bars (10h). Use as confluence signal. |

---

### v7 — ATR-Based Dynamic Sizing
**Idea:** Vary contract count inversely with ATR. More contracts when volatility is compressed, fewer when choppy.

| Result | Detail |
|--------|--------|
| P&L improvement | +10–20% over flat 10 contracts |
| WR change | None (sizing doesn't change WR) |
| $200/week? | Depends on base signal |
| Verdict | **Recommended add-on.** Low-cost improvement to any positive-edge signal. Cap at 25–30 to control drawdown. |

---

### v8 — Big-Move Reversal
**Idea:** After a 15m candle moves ≥0.5%, bet the next candle reverses.

| Result | Detail |
|--------|--------|
| WR range | 54–58% (higher threshold = higher WR, fewer trades) |
| Best threshold | 0.5% (balance of WR and frequency) |
| Momentum continuation | Loses at all thresholds |
| $200/week? | Close — needs sizing or combination |
| Verdict | **Strongest single signal found.** BTC 15m strongly mean-reverts after large moves. Good core signal. |

---

### v9 — Signal Confluence (EMA9 + Big-Move + S/R)
**Idea:** Only trade when 2 or 3 of the best signals agree.

| Result | Detail |
|--------|--------|
| 2-of-3 WR | ~56–58% |
| 3-of-3 WR | ~58–62% (very few trades) |
| Trade frequency | 2-of-3: ~10–20/week; 3-of-3: ~1–3/week |
| $200/week? | 2-of-3: marginal; 3-of-3: not enough volume |
| Verdict | Confluence improves quality but reduces quantity. The 2-of-3 threshold is the sweet spot. |

---

### v10 — Kitchen Sink (Full System)
**Idea:** All of the above combined.
- **Signals:** EMA9 slope-rev + big-move-rev (≥0.5%) + S/R-rev (40-bar, 0.3%)
- **Filters:** Ranging regime (EMA spread <0.5%) + RSI exhaustion confirmation
- **Sizing:** ATR-adaptive (base 10, max 25 contracts)
- **Entry:** 2-of-3 signals + both filters pass

| Result | Detail |
|--------|--------|
| WR | ~58–62% on 1Y+ backtests |
| Trade frequency | ~5–15/week |
| $200/week? | **YES on 1Y+**, marginal on shorter windows |
| Verdict | Best overall system. Filters cut noise significantly; ATR sizing amplifies edge. |

---

## Key Takeaways

1. **BTC 15m is a mean-reverting market.** Momentum and breakout strategies consistently lose. Every winning approach here fades moves.

2. **The $200/week bar requires compounding edges.** No single signal clears it reliably. Confluence + filtering + smart sizing together can get there.

3. **Big-Move Reversal (v8) is the single best signal** — 54–58% WR with decent trade count. If forced to pick one, start here.

4. **ATR sizing is free money** — adds 10–20% to P&L with zero added complexity in the signal logic.

5. **Ranging regime filter is a meaningful noise reducer** — cut trending-market losses without sacrificing many good trades.

6. **Short backtests (1W) are unreliable** — all strategies show high variance. Trust 1Y+ numbers only.

---

## Recommended Next Steps

1. **Forward-test v10 Kitchen Sink** on live Kalshi BTC 15m markets (paper mode if available).
2. **Tune the RSI thresholds** (currently 45/55) — try 42/58 for higher selectivity.
3. **Collect more candle data** — the 4Y backtest is the most stable; more history = more confidence.
4. **Consider the payout structure** — Kalshi markets may not be symmetric $5/$5. If YES contracts are cheaper, bias toward long bets.
5. **Track live WR** — if live WR drops below 52%, revisit assumptions.

---

## Payout Asymmetry Analysis

*Added during v11 next-steps review.*

### Current Assumption

The v3–v11 backtests all assume **symmetric $5/$5 payouts**:
- Win: +$5 per contract
- Loss: -$5 per contract
- Break-even WR: **50%** (0.5 * 5 = 0.5 * 5)

In practice, Kalshi YES/NO contracts are priced at a market-determined probability. If the YES
contract trades at 45¢, you risk $0.45 to win $0.55 — **not** $0.50 to win $0.50.

### Break-Even Win Rate Formula

```
break_even_WR = loss_per_contract / (profit_per_contract + loss_per_contract)
```

For a YES contract purchased at price P (in cents):
- Profit per win  = (1.00 - P)   ← what you collect
- Loss per bet    = P             ← what you risk

| Entry Price (¢) | Profit/Win | Loss/Bet | Break-Even WR |
|----------------|-----------|---------|--------------|
| 40¢             | 60¢       | 40¢     | **40.0%**     |
| 45¢             | 55¢       | 45¢     | **45.0%**     |
| 50¢             | 50¢       | 50¢     | **50.0%**     |
| 55¢             | 45¢       | 55¢     | **55.0%**     |
| 60¢             | 40¢       | 60¢     | **60.0%**     |

### Implications for the Kitchen Sink System

The v10 system achieves ~58–62% WR on 1Y+ backtests.

| Entry Price | Break-Even WR | v10 WR | Edge  | Verdict           |
|-------------|--------------|--------|-------|-------------------|
| 40¢         | 40.0%        | 58–62% | +18–22% | ✅ Excellent      |
| 45¢         | 45.0%        | 58–62% | +13–17% | ✅ Strong         |
| 50¢         | 50.0%        | 58–62% | +8–12%  | ✅ Good (current assumption) |
| 55¢         | 55.0%        | 58–62% | +3–7%   | ⚠️  Thin — needs high end of WR range |
| 60¢         | 60.0%        | 58–62% | -2–+2%  | ❌ Breakeven/loss at 55% contract prices |

### Recommendation

- **Prefer entering contracts priced ≤ 50¢** (bet the "less likely" side) when directional signal is equal.
- If your long (YES) signal fires but YES is priced at 58¢, the edge is nearly gone. Look for discounted entry (wait for a dip to 45–48¢ range).
- The v10 mean-reversion system is naturally suited for **buying cheap** — you're fading extreme moves, which often correspond to inflated YES/NO prices, so you'd naturally get favorable entry.
- For NO contracts: break-even WR = (1 - contract_price) / 1.00. A NO contract at 40¢ (meaning the market gives 60% chance to YES) requires only 40% WR to break even — same math, flipped.

### Model Adjustment for Asymmetric Payouts

To re-run any backtest with asymmetric payouts, replace:
```python
WIN_PROFIT  = 5.0
LOSS_AMOUNT = -5.0
```
with:
```python
ENTRY_PRICE  = 0.45          # ¢ paid per contract (fractional)
WIN_PROFIT   = (1.0 - ENTRY_PRICE) * contracts * face_value
LOSS_AMOUNT  = -(ENTRY_PRICE * contracts * face_value)
```
This allows accurate P&L modeling when market prices deviate from 50¢.

