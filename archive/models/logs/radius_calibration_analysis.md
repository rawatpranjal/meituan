# Candidate Generation Radius Calibration Analysis

**Date**: 2025-11-01
**Objective**: Determine appropriate radius values for the coordinate system used in the dataset

## Problem Statement

The default radius values (10-100) were producing extremely poor performance (2-3% assignment rate) because they were calibrated for a different coordinate system.

## Coordinate System Analysis

### Scale Discovery

The dataset uses **large-scale coordinates**:
- Typical latitude: ~45,862,054
- Typical longitude: ~174,581,041
- Distances measured in tens of thousands

### Distance Distribution (Batch 1, 10 sample orders vs 2,503 couriers)

| Metric | Distance |
|--------|----------|
| Minimum | 88.09 |
| 25th percentile | 27,133 |
| Median | 43,526 |
| 75th percentile | 63,077 |
| 90th percentile | 101,839 |

## Radius Calibration Results

### Candidate Coverage by Radius

| Radius | Avg Candidates/Order | Coverage | Expected Assignment Rate |
|--------|---------------------|----------|--------------------------|
| 100 | 0 | 0.0% | **2.0%** ❌ |
| 1,000 | 9 | 0.4% | ~2.7% ❌ |
| 5,000 | 55 | 2.2% | ~10-15% ❌ |
| 10,000 | 163 | 6.5% | ~25-30% ⚠️ |
| 20,000 | 442 | 17.7% | ~40-45% ⚠️ |
| 30,000 | 798 | 31.9% | ~50-55% ⚠️ |
| 40,000 | 1,217 | 48.6% | ~60-65% ⚠️ |
| 50,000 | 1,544 | 61.7% | ~70-75% ⚠️ |
| **75,000** | **2,016** | **80.5%** | **79.6%** ✅ |
| 100,000 | 2,221 | 88.7% | **79.6%** ✅ |

**Note**: With max-candidates-per-order=500 and max-candidates-per-courier=500

## Performance Validation Tests

### Hungarian Algorithm Performance by Configuration

| Configuration | Assignment Rate | Orders Assigned | Notes |
|--------------|-----------------|-----------------|-------|
| No candidates (baseline) | **80.77%** | 15,831/19,601 | Optimal performance |
| radius=10, default limits | 2.7% | 4,470/165,514 | Original bug |
| radius=100, default limits | 2.0% | 3,286/165,514 | Still broken |
| radius=75k, default limits | 64.4% | 15,536/24,110 | Better but limited |
| **radius=75k, limits=500** | **79.6%** | **15,831/19,876** | ✅ Near-optimal |
| radius=100k, limits=500 | **79.6%** | 15,831/19,876 | ✅ Plateau reached |

## Root Cause Explanation

### Why Original Radius Values Failed

The original implementation assumed a normalized coordinate system (e.g., lat/lng in degrees or meters). The actual dataset uses **raw shifted coordinates** with:
- Coordinate magnitudes: ~10^7 to 10^8
- Distances: 4-5 orders of magnitude larger than assumed

**Example**:
- Intended radius: 100 meters
- Actual dataset requires: 75,000 units
- **Scale mismatch: 750x**

### Why Per-Order/Per-Courier Limits Matter

Even with correct radius, the default limits (20/50) were too restrictive:
- At radius=75k: ~2,016 candidates available
- Default limit cuts to 20 → 99% reduction
- Result: 64.4% assignment (vs 79.6% with limits=500)

## Recommendations

### For Production Use

**Option A: Disable Candidate Generation** (Simplest)
```bash
python3 -m models.run --strategy hungarian --disable-candidates
```
- Assignment rate: 80.77%
- Pros: Maximum performance, no tuning needed
- Cons: O(n²) space complexity for large batches

**Option B: Calibrated Candidate Generation** (Recommended)
```bash
python3 -m models.run --strategy hungarian \
  --candidate-radius 75000 \
  --max-candidates-per-order 500 \
  --max-candidates-per-courier 500
```
- Assignment rate: 79.6% (99% of optimal)
- Pros: Reduces memory/compute, nearly optimal
- Cons: Requires parameter tuning per dataset

### Default Value Updates

Recommended changes to `models/run.py`:

```python
# OLD (broken for this dataset)
--candidate-radius 50.0
--max-candidates-per-order 20
--max-candidates-per-courier 50

# NEW (calibrated for Meituan dataset)
--candidate-radius 75000.0
--max-candidates-per-order 500
--max-candidates-per-courier 500
```

### For Other Datasets

To calibrate radius for a new dataset:

1. **Measure typical distances**:
```python
# Compute distance percentiles
distances = compute_all_distances(orders, couriers)
print(f"Median: {np.percentile(distances, 50)}")
print(f"75th percentile: {np.percentile(distances, 75)}")
```

2. **Set radius to 75th-90th percentile**:
```python
radius = np.percentile(distances, 80)  # 80% coverage target
```

3. **Set high limits** (500+ for dense urban scenarios):
```python
max_candidates_per_order = 500
max_candidates_per_courier = 500
```

4. **Validate assignment rate** matches no-candidate baseline.

## Coordinate System Investigation

### Possible Explanations

The large coordinate values suggest one of:
1. **GCJ-02 (China GPS offset)**: Coordinates in microdegrees (10^-6 degrees)
2. **Custom projection**: UTM or local coordinate system
3. **Privacy obfuscation**: Uniform shift applied to all coordinates

### Evidence for Microdegrees

```
lat = 45,862,054 → 45.862054° (reasonable latitude)
lng = 174,581,041 → 174.581041° (reasonable longitude for Pacific region)
```

**If this is true**: Distances are in microdegrees, and 75,000 units ≈ 0.075° ≈ 8.3 km at 45° latitude.

This is a **reasonable delivery radius** for urban food delivery!

## Conclusion

**Root Cause**: Coordinate system mismatch - code assumed small-scale coordinates, data uses microdegree-scale.

**Solution**: Use radius=75,000 with high candidate limits, or disable candidates entirely.

**Performance Recovery**: From 2.0% → 79.6% (40x improvement, 99% of theoretical maximum)

## Test Commands for Reproduction

```bash
# Broken (original)
python3 -m models.run --mode batch --strategy hungarian --candidate-radius 100

# Fixed (calibrated)
python3 -m models.run --mode batch --strategy hungarian \
  --candidate-radius 75000 \
  --max-candidates-per-order 500 \
  --max-candidates-per-courier 500

# Optimal (no candidates)
python3 -m models.run --mode batch --strategy hungarian --disable-candidates
```

## Files Generated
- `/tmp/test_hungarian_radius75k.log` (64.4% with default limits)
- `/tmp/test_hungarian_radius75k_highcap.log` (79.6% with limits=500)
- `/tmp/test_hungarian_radius100k_highcap.log` (79.6% validation)