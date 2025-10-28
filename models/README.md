# Models Directory Documentation

## Overview

This directory contains courier-order assignment models tested against Meituan historical data.

## File Naming Convention

Format: `{model_id}_{tier}_{algorithm}_{objective}.py`

- `model_id`: Sequential number (01, 02, 03...)
- `tier`: Model complexity level (tier1, tier2, tier3...)
- `algorithm`: Assignment algorithm (bipartite, greedy, auction, rl...)
- `objective`: Cost function (distance_to_pickup, total_delivery_time, detour_cost...)

Output logs follow same naming pattern with additional file type and timestamp:
`{model_id}_{tier}_{algorithm}_{objective}_{file_type}_{timestamp}.{ext}`

## Implemented Models

### 01_tier1_bipartite_distance_to_pickup.py

**Model ID:** 01

**Tier:** 1 (Static batch-wise assignment)

**Algorithm:** Bipartite matching (Hungarian algorithm via scipy.optimize.linear_sum_assignment)

**Cost Function:** DistanceToPickup - Euclidean distance from courier to restaurant

**Description:**
- Processes orders in discrete batches at each dispatch moment
- One-to-one optimal assignment minimizing total distance to pickup
- No order batching (one courier serves one order at a time)
- No dynamic reassignment

**Simulation Parameters:**
- Task duration: 1900 seconds
- Courier rejection probability: 0.1311
- Data source: dispatch_waybill_meituan.csv, dispatch_rider_meituan.csv

**Outputs:**
- Assignment log CSV (~16K rows)
- Cycle summary CSV (~24 rows)
- Courier timeline CSV (~28K rows)
- Execution log TXT
- Playbook visualizations PNG (first 3 dispatch cycles)
- Journey detail visualizations PNG (first 3 dispatch cycles)

**Run Location:**
```bash
cd /Users/pranjal/Code/meituan/models
python3 01_tier1_bipartite_distance_to_pickup.py
```

**Analysis Scripts:**
- `evaluation/analyze_01_tier1_bipartite_distance_to_pickup.py`
- `evaluation/stakeholder_scorecard.py`

### 02_tier2_batch_vrp_distance_to_pickup.py

**Model ID:** 02

**Tier:** 2 (Clustering-based bundled assignment)

**Algorithm:** Batch VRP (K-Means clustering + Hungarian assignment + bundling)

**Cost Function:** DistanceToPickup - Euclidean distance from courier to cluster centroid

**Description:**
- Groups orders geographically using K-Means clustering
- Assigns couriers to cluster centroids using Hungarian algorithm
- Creates bundled assignments (multiple orders per courier)
- Task duration scales with bundle size (AVERAGE_TASK_DURATION * num_orders)
- Bundle-level rejection (courier accepts/rejects entire bundle)

**Simulation Parameters:**
- Task duration: 1451 seconds per order in bundle
- Courier rejection probability: 0.1311 (applied to entire bundle)
- Clustering: K = min(num_couriers, num_orders)

**Outputs:**
- Assignment log CSV (~16K rows, same courier may appear multiple times)
- Cycle summary CSV (~24 rows)
- Courier timeline CSV (~28K rows)
- Execution log TXT
- Visualizations PNG (first 3 dispatch cycles)

**Run Location:**
```bash
cd /Users/pranjal/Code/meituan/models
python3 02_tier2_batch_vrp_distance_to_pickup.py
```

### 03_tier3_online_greedy_distance_to_pickup.py

**Model ID:** 03

**Tier:** 3 (Greedy first-come first-served)

**Algorithm:** Online Greedy (nearest neighbor within batch)

**Cost Function:** DistanceToPickup - Euclidean distance from courier to restaurant

**Description:**
- Processes orders greedily within each batch
- Assigns each order immediately to closest available courier
- No optimization, no batching across orders
- One-to-one assignment (no bundling)
- Greedy local decisions

**Simulation Parameters:**
- Task duration: 1451 seconds
- Courier rejection probability: 0.1311
- Assignment: Greedy nearest neighbor

**Outputs:**
- Assignment log CSV (~16K rows)
- Cycle summary CSV (~24 rows)
- Courier timeline CSV (~28K rows)
- Execution log TXT
- Visualizations PNG (first 3 dispatch cycles)

**Run Location:**
```bash
cd /Users/pranjal/Code/meituan/models
python3 03_tier3_online_greedy_distance_to_pickup.py
```

## Directory Structure

```
models/
├── README.md                                      # This file
├── 01_tier1_bipartite_distance_to_pickup.py      # Model script
├── cost/                                          # Cost function module
│   ├── __init__.py
│   ├── base.py
│   ├── distance_to_pickup.py
│   └── README.md
├── simulator/                                     # Simulation framework
│   ├── __init__.py
│   ├── physics.py
│   ├── state.py
│   ├── assignment_strategy.py
│   ├── logger.py
│   ├── courier_timeline_logger.py
│   └── README.md
├── evaluation/                                    # Analysis scripts
│   ├── analyze_01_tier1_bipartite_distance_to_pickup.py
│   ├── stakeholder_scorecard.py
│   └── README.md
└── logs/                                          # Output directory
    ├── {model}_{file_type}_{timestamp}.csv
    ├── {model}_{file_type}_{timestamp}.log
    └── {model}_{file_type}_{timestamp}.png
```

## Model Registry

| ID | Tier | Algorithm | Objective | File | Status |
|----|------|-----------|-----------|------|--------|
| 01 | tier1 | bipartite | distance_to_pickup | 01_tier1_bipartite_distance_to_pickup.py | ✓ Baseline |
| 02 | tier2 | batch_vrp | distance_to_pickup | 02_tier2_batch_vrp_distance_to_pickup.py | ✓ Bundling |
| 03 | tier3 | online_greedy | distance_to_pickup | 03_tier3_online_greedy_distance_to_pickup.py | ✓ FCFS |
