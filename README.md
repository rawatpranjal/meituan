# Meituan Courier-Order Assignment Optimization

Research project comparing assignment algorithms for batch-based food delivery dispatch using real-world operational data.

**Dataset**: Meituan/INFORMS TSL Data-Driven Research Challenge
**License**: CC BY-NC 4.0 (Academic use only)

---

## Repository Structure

```
meituan/
├── data/                           # Raw Meituan dataset (INFORMS challenge)
├── eda/                            # Exploratory data analysis scripts
├── context/                        # Research papers and background docs
├── models/                         # Assignment algorithms and simulation
│   ├── cost/                       # Cost function modules (pluggable)
│   ├── simulator/                  # Stateful batch-dispatch simulation framework
│   ├── evaluation/                 # Analysis and scorecard scripts
│   ├── visualization/              # GIF animation generation tools
│   ├── logs/                       # Execution logs, CSVs, and GIF outputs
│   ├── 01_tier1_bipartite_distance_to_pickup.py
│   ├── 02_tier2_batch_vrp_distance_to_pickup.py
│   ├── 03_tier3_online_greedy_distance_to_pickup.py
│   └── README.md                   # Model documentation
└── README.md                       # This file
```

---

## The Problem

**Real-Time Batch Dispatch for Food Delivery**

Our system simulates a real-world courier-order assignment problem:

- **Orders arrive continuously** from restaurants across a delivery area
- **The platform dispatches in discrete batches** at regular intervals (every 1-3 minutes)
- **At each dispatch moment**, the system must decide:
  - Which waiting orders to assign to which available couriers
  - Whether to bundle multiple orders to a single courier
  - How to balance competing objectives

**Objectives**:
- **Platform**: Minimize total system cost (distance, time)
- **Customer**: Minimize wait time for assignment and delivery
- **Courier**: Maximize earnings potential (more orders, less idle time)

**Constraints**:
- Courier availability (stateful: busy couriers cannot accept new tasks)
- Geographic distribution (pickup and delivery locations)
- Temporal dynamics (orders have deadlines, couriers have task durations)
- Rejection probability (couriers may reject assignments)

---

## Our Three Solutions

We built a stateful, batch-based dispatch simulator to compare three distinct assignment strategies.

### Model 01: Greedy Bipartite (One-to-One)

**Algorithm**: Hungarian algorithm for optimal bipartite matching
**Assignment**: One courier → One order
**Cost Function**: Euclidean distance from courier to restaurant (pickup location)

**Strategy**: At each dispatch cycle, find the optimal one-to-one pairing between waiting orders and available idle couriers that minimizes total distance to pickup.

**Hypothesis**: Simple and fast, but myopic. Fails to exploit bundling opportunities, leading to lower system throughput and courier utilization.

**File**: `models/01_tier1_bipartite_distance_to_pickup.py`

---

### Model 02: Batch VRP (Many-to-One Bundling)

**Algorithm**: K-Means clustering + Hungarian assignment + bundling
**Assignment**: One courier → Multiple orders (bundled route)
**Cost Function**: Distance from courier to cluster centroid

**Strategy**:
1. **Cluster orders** geographically using K-Means (K = number of available couriers)
2. **Assign couriers to clusters** using Hungarian algorithm
3. **Bundle all orders in cluster** to assigned courier

**Hypothesis**: More efficient for platform (lower total distance) and couriers (more orders per trip). Trade-off: potentially longer wait for customers last in bundled route.

**File**: `models/02_tier2_batch_vrp_distance_to_pickup.py`

---

### Model 03: Online Greedy (FCFS Baseline)

**Algorithm**: Greedy nearest-neighbor within batch
**Assignment**: One courier → One order (first-come, first-served)
**Cost Function**: Distance from courier to restaurant

**Strategy**: Process orders sequentially within each batch. Assign each order immediately to the closest available courier.

**Hypothesis**: Simplest possible strategy. Minimizes individual wait time but sacrifices global optimality. Useful baseline for comparison.

**File**: `models/03_tier3_online_greedy_distance_to_pickup.py`

---

### "Reality": Meituan's Historical Assignments

**Source**: Actual assignments from Meituan's production system (in dataset)
**Strategy**: Unknown (proprietary production algorithm)

**Purpose**: Our benchmark. This represents the "professional" solution from a sophisticated, production-grade system. We compare our models against this reality to evaluate performance.

---

## Visual Comparison

We generate animated GIFs showing side-by-side comparisons of each model's assignments vs. Meituan's actual assignments across all 24 dispatch cycles.

### Model 01: Greedy Bipartite (One-to-One)
![Model 01 Visualization](models/logs/01_tier1_bipartite_distance_to_pickup_20251027_203531_comparison.gif)

**Left**: Our bipartite matching algorithm
**Right**: Meituan's actual assignments

---

### Model 02: Batch VRP (Many-to-One Bundling)
![Model 02 Visualization](models/logs/02_tier2_batch_vrp_distance_to_pickup_20251027_205445_comparison.gif)

**Left**: Our bundling algorithm
**Right**: Meituan's actual assignments

---

### Model 03: Online Greedy (FCFS)
![Model 03 Visualization](models/logs/03_tier3_online_greedy_distance_to_pickup_20251027_205649_comparison.gif)

**Left**: Our greedy FCFS algorithm
**Right**: Meituan's actual assignments

---

### Visualization Legend

- **Blue circles**: Waiting orders (pickup locations at restaurants)
- **Red/gray triangles**: Available couriers
- **Lines**: Assignment connections (courier → order)
- **Visual hierarchy**:
  - Bright, large markers = Unmatched (still waiting)
  - Faded, small markers = Matched (assignment made)
- **Animation**: 24 dispatch cycles at 2.5s per frame

The "pulse" effect shows orders arriving, getting matched, then fading away as new orders arrive.

---

## Logging & Outputs

Each model execution generates comprehensive logs:

### Execution Logs (`.log`)
- Timestamped run metadata
- Section-by-section progress
- Final statistics summary

### Assignment Logs (CSV)
- Every proposed assignment
- Acceptance/rejection outcomes
- Cost per assignment
- Courier and order locations

### Cycle Summaries (CSV)
- Per-dispatch-cycle metrics
- Supply/demand ratio
- Assignment rate, acceptance rate
- Total cost, average cost per assignment
- Agreement rate with Meituan's actual assignments

### Courier Timelines (CSV)
- Second-by-second courier state tracking
- State transitions (idle → busy → idle)
- Task start/end times

### Animated Visualizations (GIF)
- Side-by-side comparison frames
- 24 dispatch cycles
- Zoomed to dense central region

**Location**: All outputs saved to `models/logs/`

---

## Results Comparison

### Three-Way Performance Scorecard

| Stakeholder | Metric | Model 01<br>Bipartite | Model 02<br>Batch VRP | Model 03<br>Greedy | Reality<br>(Meituan) |
|-------------|--------|------------|-----------|---------|----------|
| **Platform** | Total System Cost | TBD | TBD | TBD | TBD |
| **Platform** | Assignment Rate (%) | TBD | TBD | TBD | TBD |
| **Platform** | Orders Assigned | TBD | TBD | TBD | TBD |
| **Customer** | Avg Wait for Assignment (s) | TBD | TBD | TBD | TBD |
| **Customer** | Assignment Coverage (%) | TBD | TBD | TBD | TBD |
| **Courier** | Assignments per Courier | TBD | TBD | TBD | TBD |
| **Courier** | Courier Utilization (%) | TBD | TBD | TBD | TBD |

### Key Research Questions

1. **Does bundling improve system efficiency?**
   Compare Model 01 (one-to-one) vs Model 02 (many-to-one)

2. **What is the customer wait time trade-off?**
   Bundling may increase wait for orders last in route

3. **How close can we get to production performance?**
   Compare our models against Meituan's actual assignments

4. **Which strategy best balances stakeholder needs?**
   Platform efficiency vs Customer QoS vs Courier earnings

---

## Getting Started

### Run Models

```bash
cd models/

# Model 01: Bipartite matching
python3 01_tier1_bipartite_distance_to_pickup.py

# Model 02: Batch VRP bundling
python3 02_tier2_batch_vrp_distance_to_pickup.py

# Model 03: Online greedy
python3 03_tier3_online_greedy_distance_to_pickup.py
```

Logs and CSVs saved to `models/logs/`

### Generate Visualizations

```bash
cd models/visualization/

# Generate GIF for a specific model (example for Model 01)
python3 create_gifs.py \
  --assignment-log ../logs/01_tier1_bipartite_distance_to_pickup_assignment_log_TIMESTAMP.csv \
  --mode comparison \
  --zoom

# Options:
#   --mode: 'baseline', 'actual', 'comparison', or 'all'
#   --duration: Frame duration in seconds (default: 2.5)
#   --show-mode: 'active' (fade matched) or 'all' (show everything)
#   --zoom: Focus on dense central region
```

### Run Analysis

```bash
cd models/evaluation/

# Model 01 analysis
python3 analyze_01_tier1_bipartite_distance_to_pickup.py

# Cross-model comparison (TBD)
python3 stakeholder_scorecard.py
```

---

## Technical Details

### Simulation Framework

- **Stateful**: Tracks courier availability over time
- **Batch-based**: Discrete dispatch cycles every 120-180 seconds
- **Realistic**: Models courier task durations, rejection probability
- **Pluggable**: Modular cost functions and assignment strategies

### Cost Function Architecture

All models use pluggable cost functions from `models/cost/`:
- `BaseCostFunction`: Abstract interface
- `DistanceToPickup`: Euclidean distance (current implementation)
- Extensible for future cost functions (time, detour, hybrid)

### Data Pipeline

1. Load dispatch events and courier availability from CSVs
2. For each dispatch cycle:
   - Identify waiting orders and available couriers
   - Run assignment algorithm
   - Simulate acceptance/rejection
   - Update courier state (busy with task duration)
3. Log assignments, cycle summaries, courier timelines
4. Generate visualizations

---

## Dataset Attribution

This research uses data provided by Meituan as part of the INFORMS Transportation Science and Logistics (TSL) Data-Driven Research Challenge.

**Citation**: "This research was supported by data provided by Meituan."

**License**: Creative Commons Attribution-NonCommercial 4.0 International (CC BY-NC 4.0)

**Source**: https://github.com/meituan/meituan_informs_data

---

## Future Work

- [ ] Implement additional cost functions (time-based, detour cost, hybrid)
- [ ] Test alternative clustering algorithms (DBSCAN, hierarchical)
- [ ] Implement dynamic reassignment strategies
- [ ] Add real routing (TSP solver for bundled routes)
- [ ] Incorporate order priority and deadlines
- [ ] Model customer cancellations
- [ ] Reinforcement learning baseline

---

## References

See `context/` directory for relevant research papers on:
- Vehicle routing problems (VRP)
- Dynamic fleet management
- Online matching algorithms
- Food delivery optimization