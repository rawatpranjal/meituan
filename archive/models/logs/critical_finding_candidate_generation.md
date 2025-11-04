# Critical Finding: Candidate Generation Bottleneck

**Date**: 2025-11-01
**Discovered During**: Hungarian strategy testing

## Problem Statement
The Hungarian algorithm achieves only 2.7% assignment rate when candidate generation is enabled, compared to 80.77% without it.

## Root Cause Analysis

### Test Results Comparison

| Strategy | Candidate Generation | Assignment Rate | Orders Assigned |
|----------|---------------------|-----------------|-----------------|
| Hungarian | Disabled | **80.77%** | 15,831/19,601 |
| Hungarian | Radius=100 | 2.0% | 3,286/165,514 |
| Hungarian | Radius=10 (default) | 2.7% | 4,470/165,514 |
| Greedy | Not used | 80.8% | 133,562/165,284 |

### Evidence of the Problem

**Batch 1 Example** (658 orders, 802 couriers):
- With candidates (radius=100): Only 68 candidate pairs → 34 assignments
- Without candidates: Full matrix → 658 assignments (568 accepted)

**Batch 2 Example** (1255 orders, 855 couriers):
- With candidates (radius=100): Only 125 candidate pairs → 47 assignments
- Without candidates: Full matrix → 314 assignments (285 accepted)

## Impact
The candidate generation logic is severely limiting the Hungarian algorithm's ability to find optimal assignments. With so few candidate pairs, the algorithm cannot explore enough of the solution space.

## Temporary Solution Implemented

Added CLI parameters to control candidate generation (`models/run.py`):

```python
--candidate-radius        # Max radius for candidate generation (default: 50.0)
--disable-candidates      # Disable candidate generation entirely
--max-candidates-per-order    # Max candidates per order (default: 20)
--max-candidates-per-courier  # Max candidates per courier (default: 50)
```

## Recommendations

### For Fair Strategy Comparison
1. **Option A**: Disable candidate generation for all strategies
   ```bash
   python3 -m models.run --strategy hungarian --disable-candidates
   ```

2. **Option B**: Fix the candidate generation logic to be less restrictive
   - Investigate why so few pairs are generated even with large radius
   - Consider other constraints besides distance

3. **Option C**: Ensure all strategies use identical candidate generation
   - Currently Greedy doesn't use candidates at all
   - This creates unfair comparison

### Next Steps
1. Investigate the exact logic in `CandidateGenerator.generate()` that's filtering out so many pairs
2. Check if there are additional constraints beyond radius (time windows, etc.)
3. Consider whether the per-order and per-courier limits are too restrictive

## Test Commands for Reproduction

```bash
# Hungarian with candidates (poor performance)
python3 -m models.run --mode batch --strategy hungarian --candidate-radius 100

# Hungarian without candidates (good performance)
python3 -m models.run --mode batch --strategy hungarian --disable-candidates

# Greedy baseline (no candidates by default)
python3 -m models.run --mode batch --strategy greedy
```

## File Locations
- Test logs:
  - `/tmp/test_hungarian_no_candidates.log`
  - `/tmp/test_hungarian_radius100.log`
- Code changes:
  - `models/run.py:90-116` (CLI arguments)
  - `models/run.py:210-219` (candidate generator creation)
- Candidate generator: `models/simulator/services/candidate_generation.py`