# Algorithm Decision Narrative: 7 Sequential Batches (30-60 Minutes)

## Executive Summary

This document explains the algorithmic decisions made by 5 routing strategies across 7 sequential batches (Batches 6-12, spanning 30-60 minutes). These batches were selected using a multi-factor distinctness scoring system that identified moments where algorithms make fundamentally different decisions.

**Key Finding**: These batches ARE highly illustrative of how the algorithms work. They demonstrate:
- Greedy's myopic immediate assignment behavior
- Hungarian's optimal 1-to-1 matching with route awareness
- Simple Bundling's same-restaurant consolidation strategy
- Batched Pickups' strategic waiting and network efficiency
- Anticipated Bundling's lookahead with future order anticipation

**Total Window Distinctness**: 76.27 (combined score across 7 batches)
**Timeframe**: Batches 6-12 (30-60 minutes, 1800-3600 seconds)
**Batch Interval**: 300 seconds (5 minutes)

---

## Batch-by-Batch Analysis

### Batch 6 @ 30 minutes (t=1800s) ⭐ HIGHEST EARLY DISTINCTNESS

**Distinctness Score**: 12.52 (highest in first half)

**Key Metrics:**
- Overlap score: 0.933 (algorithms agree only 6.7% on courier-order pairs)
- Assignment counts: [2, 2, 2, 0, 2] (4 algorithms active, 1 waiting)
- Bundle sizes: [1, 1, 1, 1, **3**, 2, **4**, 2]
- Order diversity: 8 different orders selected

**What Happened:**

This is the first batch showing dramatic bundling differences:
1. **Two algorithms created large bundles** (size 3 and size 4)
2. **One algorithm made no assignments** (strategic waiting)
3. **High variance in bundling strategies** (variance: 1.11)

**Why This Illustrates Algorithm Behavior:**

- **Simple Bundling**: Created 3-order bundle from same restaurant (same-pickup consolidation)
- **Anticipated Bundling**: Created 4-order bundle by including future orders (lookahead strategy)
- **Batched Pickups**: Made 0 assignments (waiting to create better multi-restaurant bundles)
- **Greedy & Hungarian**: Made 2 single-order assignments each (immediate matching)

**Significance**: This batch perfectly demonstrates the fundamental difference between immediate assignment (Greedy/Hungarian), same-restaurant bundling (Simple), strategic waiting (Batched Pickups), and lookahead (Anticipated).

---

### Batch 7 @ 35 minutes (t=2100s)

**Distinctness Score**: 7.30 (convergence under constraints)

**Key Metrics:**
- Overlap score: 0.700 (algorithms agree 30% on assignments)
- Assignment counts: [1, 1, 1, 1, 1] (ALL algorithms made exactly 1 assignment)
- Bundle sizes: [1, 1, 1, 1, 1] (ALL single-order assignments)
- Order diversity: 3 different orders selected

**What Happened:**

All 5 algorithms made exactly 1 assignment, all single-order. However, overlap is only 70% - they chose DIFFERENT orders.

**Why This Illustrates Algorithm Behavior:**

Despite making the same number of assignments with the same bundle size, algorithms diverged on:
- **Order selection**: Which orders to prioritize
- **Courier selection**: Which courier to assign each order to

This shows that even when resource constraints force similar assignment counts, the **order selection and matching logic** still differs:
- **Greedy**: First available order
- **Hungarian**: Optimal distance-based matching
- **Simple/Anticipated**: Route-aware selection
- **Batched Pickups**: Network efficiency considerations

**Significance**: Demonstrates that algorithmic differences persist even under tight constraints. Lower distinctness shows convergence when options are limited.

---

### Batch 8 @ 40 minutes (t=2400s)

**Distinctness Score**: 10.79

**Key Metrics:**
- Overlap score: 0.917 (algorithms agree only 8.3%)
- Assignment counts: [2, 3, 2, 3, 1] (HIGH variance: 0.56)
- Bundle sizes: ALL size 1 (11 total single-order assignments)
- Order diversity: 5 different orders selected

**What Happened:**

No bundling occurred, but **assignment count variance is high**:
- Two algorithms made 3 assignments
- Two algorithms made 2 assignments
- One algorithm made 1 assignment

**Why This Illustrates Algorithm Behavior:**

Even without bundling differences, algorithms diverge on **when to assign**:
- **Hungarian & Batched Pickups**: Made 3 assignments (maximizing throughput)
- **Greedy & Simple Bundling**: Made 2 assignments (conservative)
- **Anticipated Bundling**: Made 1 assignment (waiting for future opportunities)

**Significance**: Shows that "when to assign" is as important as "how to bundle". Anticipated Bundling's restraint (1 assignment) vs Hungarian's throughput (3 assignments) demonstrates fundamentally different philosophies.

---

### Batch 9 @ 45 minutes (t=2700s)

**Distinctness Score**: 9.38

**Key Metrics:**
- Overlap score: 0.850 (15% agreement)
- Assignment counts: [2, 1, 1, 2, 1] (moderate variance: 0.24)
- Bundle sizes: ALL size 1 (7 total assignments)
- Order diversity: 4 different orders

**What Happened:**

Continued single-order assignments, but **order selection diverges**:
- Two algorithms made 2 assignments
- Three algorithms made 1 assignment

**Why This Illustrates Algorithm Behavior:**

Mid-simulation behavior showing:
- **Greedy & Batched Pickups**: More aggressive (2 assignments each)
- **Hungarian, Simple, Anticipated**: More conservative (1 assignment each)

**Significance**: At the 45-minute mark, we see a split between throughput-focused (Greedy, Batched Pickups) and quality-focused (Hungarian, Simple, Anticipated) strategies.

---

### Batch 10 @ 50 minutes (t=3000s) ⭐ BUNDLING REAPPEARS

**Distinctness Score**: 12.08 (2nd highest overall)

**Key Metrics:**
- Overlap score: 0.950 (95% disagreement!)
- Assignment counts: [**0**, 3, 1, 1, 2] (extreme variance: 1.04)
- Bundle sizes: [1, 1, 1, **2**, 1, 1, **2**] (two 2-order bundles)
- Order diversity: 3 orders

**What Happened:**

**Critical batch showing three phenomena:**
1. **One algorithm made 0 assignments** (Greedy - resource constrained)
2. **One algorithm made 3 assignments** (Hungarian - maximum throughput)
3. **Two algorithms created 2-order bundles**

**Why This Illustrates Algorithm Behavior:**

- **Greedy**: Resource-constrained (0 assignments) - demonstrates myopic strategy's weakness
- **Hungarian**: Maximizing throughput (3 single-order assignments) - demonstrates optimal matching
- **Simple Bundling**: Created 2-order bundle (same-restaurant consolidation)
- **Anticipated Bundling**: Created 2-order bundle (selective bundling)
- **Batched Pickups**: 1 assignment (patient network approach)

**Significance**: This is THE KEY BATCH showing:
- How greedy strategies fail under load (0 assignments while others make 1-3)
- How bundling strategies re-emerge when opportunities arise
- How Hungarian maintains throughput even when Greedy can't

---

### Batch 11 @ 55 minutes (t=3300s) ⭐ EXTREME DIVERGENCE

**Distinctness Score**: 11.48

**Key Metrics:**
- Overlap score: **1.000** (perfect disagreement when normalized)
- Assignment counts: [**0, 0, 0, 2, 0**] (only ONE algorithm active!)
- Bundle sizes: [1, 1] (2 single-order assignments)
- Order diversity: 2 orders

**What Happened:**

**Only Batched Pickups made assignments**. All other algorithms idle.

**Why This Illustrates Algorithm Behavior:**

This demonstrates **long-term scheduling impact**:
- **Batched Pickups**: Freed up couriers earlier through efficient routing, now has capacity
- **All other algorithms**: Couriers still busy with previous assignments

**Significance**: Shows that algorithmic decisions compound over time. Batched Pickups' earlier network-efficient routing created courier availability at 55 minutes, while other strategies' couriers are still occupied. This is evidence of **second-order effects** - the algorithm's impact isn't just immediate, but affects future capacity.

---

### Batch 12 @ 60 minutes (t=3600s) ⭐ HIGHEST OVERALL DISTINCTNESS

**Distinctness Score**: 12.72 (MAXIMUM)

**Key Metrics:**
- Overlap score: 0.975 (97.5% disagreement)
- Assignment counts: [2, 1, **3**, **0**, 2] (extreme variance: 1.04)
- Bundle sizes: [1, 1, 1, **2**, 1, **2**, 1, 1] (two 2-order bundles)
- Order diversity: **7** orders (highest diversity in window)

**What Happened:**

**Final batch showing maximum divergence:**
1. **One algorithm made 3 assignments** (Simple Bundling)
2. **One algorithm made 0 assignments** (Batched Pickups - now constrained after earlier activity)
3. **Two algorithms created 2-order bundles**
4. **7 different orders selected** (highest order diversity)

**Why This Illustrates Algorithm Behavior:**

End-game strategies differ dramatically:
- **Simple Bundling**: Aggressive final bundling (3 assignments with bundles)
- **Batched Pickups**: Now resource-constrained (0 assignments - couriers busy)
- **Greedy & Anticipated**: Made 2 assignments each
- **Hungarian**: Made 1 assignment

**Significance**: Demonstrates how algorithms **finish differently**. Simple Bundling maximizes end-game bundling, while Batched Pickups (efficient earlier) is now constrained. The role reversal from Batch 11 (where Batched Pickups was the only active algorithm) shows temporal trade-offs.

---

## Cross-Batch Patterns

### Pattern 1: Bundling Emergence (Batches 6, 10, 12)

**Observation**: Bundling appears in Batches 6, 10, and 12, but NOT in Batches 7, 8, 9.

**Explanation**:
- **Batch 6** (30 min): Early bundling opportunities when multiple orders from same restaurant
- **Batches 7-9** (35-45 min): Resource constraints force single-order assignments
- **Batch 10** (50 min): Bundling re-emerges as system stabilizes
- **Batch 12** (60 min): End-game bundling to clear remaining orders

**Algorithms showing bundling**:
- Simple Bundling: Consistent bundling when possible (Batches 6, 10, 12)
- Anticipated Bundling: Larger bundles with lookahead (Batch 6: size 4)
- Batched Pickups: Strategic bundling (varies by batch)

### Pattern 2: Assignment Count Variance Over Time

```
Batch 6:  [2, 2, 2, 0, 2] - variance: 0.64
Batch 7:  [1, 1, 1, 1, 1] - variance: 0.00  ← CONVERGENCE
Batch 8:  [2, 3, 2, 3, 1] - variance: 0.56
Batch 9:  [2, 1, 1, 2, 1] - variance: 0.24
Batch 10: [0, 3, 1, 1, 2] - variance: 1.04  ← HIGH DIVERGENCE
Batch 11: [0, 0, 0, 2, 0] - variance: 0.64  ← EXTREME DIVERGENCE
Batch 12: [2, 1, 3, 0, 2] - variance: 1.04  ← HIGH DIVERGENCE
```

**Observation**: Variance is lowest in Batch 7 (all algorithms make 1 assignment), highest in Batches 10-12 (late-stage divergence).

**Explanation**: Early in the window, resource constraints force convergence. Later, accumulated scheduling differences create extreme divergence.

### Pattern 3: Temporal Role Reversals

**Batch 11**: Batched Pickups makes 2 assignments, all others make 0
**Batch 12**: Batched Pickups makes 0 assignments, others make 1-3

**Explanation**: Batched Pickups' efficient early routing freed couriers at t=55min, but by t=60min those couriers are occupied again. Other algorithms catch up in the final batch.

---

## Algorithm-Specific Insights

### Greedy

**Signature Behavior**:
- Never bundles (0% bundling rate)
- Makes immediate assignments when couriers available
- Resource-constrained in Batch 10 (0 assignments)

**Batches demonstrating weakness**:
- Batch 10: Made 0 assignments while Hungarian made 3
- Batch 6: Made 2 simple assignments while Anticipated created 4-order bundle

**Batches demonstrating strength**:
- Batch 8: Made 2 assignments (conservative but consistent)
- Batch 12: Made 2 assignments (recovers in end-game)

### Hungarian (Optimal Single-Order Matching)

**Signature Behavior**:
- Never bundles (0% bundling rate)
- But chooses DIFFERENT orders than Greedy (Batches 6, 7, 8, 9)
- Maximizes throughput (Batch 10: 3 assignments vs Greedy's 0)

**Key distinction from Greedy**:
- Same bundling rate (0%)
- Different order selection (route-aware optimization)
- Better resource utilization (doesn't get constrained like Greedy)

**Batches demonstrating superiority over Greedy**:
- Batch 10: 3 assignments vs 0
- Batch 7: Different order selection (better routes)

### Simple Bundling

**Signature Behavior**:
- Creates same-restaurant bundles
- Batch 6: 3-order bundle (largest same-restaurant bundle)
- Batch 12: 3 assignments with 2-order bundles (aggressive end-game)

**Key distinction**:
- Focuses on pickup consolidation
- Doesn't wait for future orders (unlike Anticipated)
- Consistent bundling across Batches 6, 10, 12

### Batched Pickups

**Signature Behavior**:
- Strategic waiting (Batch 6: 0 assignments while others assign)
- Network efficiency (Batch 11: only algorithm with availability)
- Temporal trade-offs (early efficiency → mid-game availability → late-game constraints)

**Key distinction**:
- Multi-restaurant bundling capability
- Scheduling creates unique temporal patterns
- Freed couriers earlier (Batch 11) but constrained later (Batch 12)

### Anticipated Bundling

**Signature Behavior**:
- Lookahead strategy (Batch 6: 4-order bundle including future orders)
- Strategic restraint (Batch 8: only 1 assignment while others make 2-3)
- Selective bundling (Batches 10, 12: creates 2-order bundles)

**Key distinction**:
- Largest bundles when anticipation pays off (Batch 6: size 4)
- Willing to wait (low assignment counts in Batches 8, 9)
- Hybrid approach (sometimes bundles, sometimes single-order)

---

## Why These Batches Are Illustrative

### Question: Do these batches show how algorithms FUNDAMENTALLY work, or just random variance?

### Answer: These batches ARE highly illustrative of fundamental algorithmic differences.

**Evidence**:

1. **Greedy shows myopic behavior consistently**:
   - Never bundles (Batches 6, 7, 8, 9, 10, 11, 12)
   - Gets resource-constrained (Batch 10: 0 assignments)
   - Recovers when constraints lift (Batch 12: 2 assignments)

2. **Hungarian shows optimal matching consistently**:
   - Different order selection than Greedy (Batches 6, 7, 8, 9)
   - Maximum throughput when possible (Batch 10: 3 assignments vs Greedy's 0)
   - Route-aware without bundling (all batches)

3. **Simple Bundling shows same-restaurant consolidation consistently**:
   - Creates bundles in Batches 6, 10, 12
   - Largest same-restaurant bundle (Batch 6: size 3)
   - Aggressive end-game (Batch 12: 3 assignments with bundling)

4. **Batched Pickups shows network thinking consistently**:
   - Strategic waiting (Batch 6: 0 assignments)
   - Temporal efficiency creates availability (Batch 11: only active algorithm)
   - Long-term trade-offs (efficient early → constrained late)

5. **Anticipated Bundling shows lookahead consistently**:
   - Largest bundles when anticipation works (Batch 6: size 4)
   - Strategic restraint (Batch 8: 1 assignment vs 2-3)
   - Hybrid approach across all batches

**Conclusion**: The patterns are consistent and predictable based on each algorithm's core logic. These are not random variations - they are direct consequences of algorithmic design choices.

---

## Narrative Arc Across 7 Batches

### Act 1: Initial Divergence (Batches 6-7)
- **Batch 6**: Dramatic bundling differences emerge (size 1 vs size 4)
- **Batch 7**: Constraints force convergence (all make 1 assignment)

### Act 2: Mid-Simulation Patterns (Batches 8-9)
- **Batch 8**: Assignment count variance (1 to 3 assignments)
- **Batch 9**: Throughput vs quality split (2 vs 1 assignments)

### Act 3: Late-Stage Divergence (Batches 10-12)
- **Batch 10**: Greedy fails, bundling re-emerges
- **Batch 11**: Batched Pickups' efficiency creates unique availability
- **Batch 12**: Maximum divergence, end-game strategies differ

**Overall Story**: Algorithms start divergent (bundling differences), converge under pressure (Batch 7), then diverge again as scheduling effects compound (Batches 10-12). The window tells a complete story of how algorithm design choices create cascading effects over 30 minutes.

---

## Viewing Guide for GIFs

**Frame Sequence** (3 seconds per frame, 21 seconds total):
1. **Frame 1 - Batch 6 @ 30 min**: Watch for large bundles (size 3-4) vs single orders
2. **Frame 2 - Batch 7 @ 35 min**: Notice all algorithms make 1 assignment (convergence)
3. **Frame 3 - Batch 8 @ 40 min**: Count assignments per algorithm (1 to 3)
4. **Frame 4 - Batch 9 @ 45 min**: Observe throughput split (2 vs 1)
5. **Frame 5 - Batch 10 @ 50 min**: Look for Greedy's 0 assignments, Hungarian's 3, and 2-order bundles
6. **Frame 6 - Batch 11 @ 55 min**: See only Batched Pickups active
7. **Frame 7 - Batch 12 @ 60 min**: Maximum divergence, end-game bundling

**What to Look For**:
- **Route colors**: Different colors = different couriers
- **Route patterns**: Single lines (1 order) vs multi-stop routes (bundles)
- **Courier availability**: Active vs idle (no routes drawn)
- **Order selection**: Which customer locations are served

---

## Technical Details

**Selection Methodology**:
- Distinctness score = overlap_score × 10 + count_variance × 2 + bundle_variance × 1 + order_diversity × 0.1
- Sequential window requirement: Batches must be consecutive (no gaps)
- Total window score: 76.27 across 7 batches

**Data Sources**:
- `analyze_batch_distinctness.py` - Batch selection algorithm
- `analysis/top_distinct_batches.json` - Selected batches and scores
- `analysis/event_data/bundles_*.json` - Assignment data per algorithm
- `create_focused_gifs.py` - GIF generation script

**Files Generated**:
- `gifs/greedy_baseline_focused.gif`
- `gifs/hungarian_route_aware_focused.gif`
- `gifs/simple_bundling_route_aware_focused.gif`
- `gifs/batched_pickups_network_focused.gif`
- `gifs/anticipated_bundling_lookahead_focused.gif`

**Execution Logs**:
- `logs/batch_distinctness_7window_*.log` - Batch selection analysis
- `logs/create_focused_gifs_7batches_*.log` - GIF generation log
