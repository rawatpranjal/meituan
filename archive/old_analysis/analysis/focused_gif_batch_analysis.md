# Focused GIF Batch Analysis

## Batch Selection Methodology

Sequential batches 8-12 (40-60 minutes) were selected using a multi-factor distinctness scoring system to identify where the 5 routing algorithms make the most divergent assignment decisions.

**Distinctness Score Components:**
- Assignment overlap (weight: 10.0) - Primary signal measuring courier-order pair differences
- Assignment count variance (weight: 2.0) - Secondary signal measuring number of assignments per algorithm
- Bundle size variance (weight: 1.0) - Tertiary signal measuring differences in bundling strategies
- Order diversity (weight: 0.1) - Context signal measuring total unique orders selected

**Sequential Window Selection:**
- Total distinctness: 56.45 (combined score across 5 batches)
- Window: Batches 8-12 (consecutive, no gaps)
- Timeframe: 40-60 minutes (2400-3600 seconds)

## Batch-by-Batch Breakdown

### Batch 8 @ 40 minutes (t=2400s)
**Distinctness Score: 10.79**

**Characteristics:**
- Assignment overlap: 0.917 (algorithms agree 91.7% on courier-order pairs)
- Assignment counts: [2, 3, 2, 3, 1] (variance: 0.56)
- Bundle sizes: 11 bundles, all single-order (size 1)
- Order diversity: 5 unique orders selected

**What to observe:**
- High assignment count variance (1 to 3 assignments per algorithm)
- No bundling differences (all algorithms use single-order assignments)
- Differences driven by which orders are selected, not how they're bundled

---

### Batch 9 @ 45 minutes (t=2700s)
**Distinctness Score: 9.38**

**Characteristics:**
- Assignment overlap: 0.850 (algorithms agree 85% on courier-order pairs)
- Assignment counts: [2, 1, 1, 2, 1] (variance: 0.24)
- Bundle sizes: 7 bundles, all single-order (size 1)
- Order diversity: 4 unique orders selected

**What to observe:**
- Lower overlap score indicates more divergent assignments
- Low count variance (most algorithms make 1-2 assignments)
- Still no bundling; differences in order selection only

---

### Batch 10 @ 50 minutes (t=3000s)
**Distinctness Score: 12.08** (2nd highest)

**Characteristics:**
- Assignment overlap: 0.950 (algorithms agree 95% on courier-order pairs)
- Assignment counts: [0, 3, 1, 1, 2] (variance: 1.04)
- Bundle sizes: 8 bundles, includes 2 bundles of size 2 (multi-order)
- Order diversity: 3 unique orders selected

**What to observe:**
- **First batch with bundling differences** (bundle_variance: 0.204)
- One algorithm makes 0 assignments (possibly waiting)
- High count variance (0 to 3 assignments per algorithm)
- Bundle sizes: [1, 1, 1, 2, 1, 1, 2] - two algorithms create 2-order bundles

---

### Batch 11 @ 55 minutes (t=3300s)
**Distinctness Score: 11.48**

**Characteristics:**
- Assignment overlap: 1.000 (perfect agreement on which courier-order pairs)
- Assignment counts: [0, 0, 0, 2, 0] (variance: 0.64)
- Bundle sizes: 2 bundles, both single-order (size 1)
- Order diversity: 2 unique orders selected

**What to observe:**
- **Only 1 algorithm makes assignments** (4 algorithms idle)
- Perfect overlap when normalized (100% agreement)
- Late-stage batch where most algorithms have completed deliveries
- Demonstrates end-of-simulation behavior differences

---

### Batch 12 @ 60 minutes (t=3600s)
**Distinctness Score: 12.72** (HIGHEST)

**Characteristics:**
- Assignment overlap: 0.975 (algorithms agree 97.5% on courier-order pairs)
- Assignment counts: [2, 1, 3, 0, 2] (variance: 1.04)
- Bundle sizes: 8 bundles, includes 2 bundles of size 2 (multi-order)
- Order diversity: 7 unique orders selected

**What to observe:**
- **Highest overall distinctness** (most divergent batch)
- Bundling differences: bundle sizes [1, 1, 1, 2, 1, 2, 1, 1]
- High count variance (0 to 3 assignments)
- High order diversity (7 different orders selected)
- End-of-simulation: algorithms finalize remaining deliveries differently

## Key Insights

**Bundling Behavior:**
- Batches 8-9: No bundling observed (all single-order assignments)
- Batches 10, 12: Bundling strategies diverge (some algorithms create 2-order bundles)
- Batch 11: Minimal activity (most algorithms idle)

**Assignment Strategy:**
- Early batches (8-9): Differences in order selection timing
- Mid batches (10): Introduction of bundling creates divergence
- Late batches (11-12): End-game strategies differ significantly

**Distinctness Drivers:**
- Primary: Assignment overlap (which courier-order pairs are created)
- Secondary: Assignment count variance (how many assignments per batch)
- Tertiary: Bundle size variance (single vs multi-order bundles)

## GIF Viewing Guide

**Frame Sequence:**
1. Frame 1: Batch 8 @ 40 min
2. Frame 2: Batch 9 @ 45 min
3. Frame 3: Batch 10 @ 50 min
4. Frame 4: Batch 11 @ 55 min
5. Frame 5: Batch 12 @ 60 min

**Display Settings:**
- Frame rate: 0.33 fps (3 seconds per frame)
- Total duration: 15 seconds
- Labels show: "[Batch X @ Y min]"

**Files Generated:**
- `greedy_baseline_focused.gif`
- `hungarian_route_aware_focused.gif`
- `simple_bundling_route_aware_focused.gif`
- `batched_pickups_network_focused.gif`
- `anticipated_bundling_lookahead_focused.gif`

## Data Sources

**Analysis Script:** `analyze_batch_distinctness.py`
**Batch Data:** `analysis/top_distinct_batches.json`
**GIF Generator:** `create_focused_gifs.py`
**Execution Logs:** `logs/create_focused_gifs_*.log`
