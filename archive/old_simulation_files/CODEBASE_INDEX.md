# Simulation Test Codebase - Complete Index

## Quick Navigation

- **[Complete Technical Report](./EXPLORATION_REPORT.md)** - 509-line detailed analysis
- **[README.md](./README.md)** - User-facing documentation with usage examples
- **[Main Execution](./main.py)** - Entry point for running simulations

## File Organization

### Core Simulation (2 files)

#### 1. `simulator_core.py` (633 lines)
**Purpose:** Discrete-event simulation engine
**Key Classes:**
- `Restaurant`: Pickup location (id, lat/lon)
- `Order`: Full order lifecycle with state machine
- `Courier`: Courier state tracking and metrics
- `SimulationState`: Central state container

**Key Functions:**
- `generate_scenario()`: Creates restaurant/courier/order distribution
- `run_simulation()`: Main 1-second granularity event loop
- `euclidean_distance()`, `get_travel_time()`: Logistics calculations

**State Machines:**
- Orders: PENDING → READY → ASSIGNED → PICKED_UP → DELIVERED
- Couriers: IDLE → DRIVING_TO_PICKUP → AT_PICKUP → DRIVING_TO_DROPOFF

**Notable Features:**
- Batch assignment every 30 seconds
- Timeline snapshots every 10 seconds
- Comprehensive event logging
- 20+ performance metrics

#### 2. `assignment_algorithms.py` (682 lines)
**Purpose:** Implementation of all 5 assignment algorithms
**Algorithms:**
1. `assign_greedy()` (lines 25-57): O(nm) baseline
2. `assign_hungarian()` (lines 64-117): O(n³) optimal 1-to-1
3. `assign_simple_bundling()` (lines 124-205): Same-restaurant grouping + Hungarian
4. `assign_route_cost_bundling()` (lines 499-565): Full route evaluation + Hungarian
5. `assign_batched_pickups()` (lines 572-662): Geographic bundling + TSP + Hungarian

**Helper Functions:**
- `calculate_route_duration()` (lines 212-300): Complete route cost with TSP
- `optimize_delivery_sequence()` (lines 303-366): TSP solver (exact ≤8, heuristic >8)
- `optimize_pickup_sequence()` (lines 369-383): TSP for multi-restaurant pickup
- `generate_geographic_bundles()` (lines 386-459): Proximity-based bundling
- `generate_bundle_candidates()` (lines 462-496): All possible bundles

**Algorithm Registry:**
- `ALGORITHMS` dict (lines 669-675): Maps names to functions
- `get_algorithm()` (lines 678-682): Factory function

### Execution & Analysis (4 files)

#### 3. `main.py` (133 lines)
**Purpose:** Main entry point orchestrating entire simulation
**Flow:**
1. Parse command-line arguments
2. Generate scenario
3. Run each algorithm sequentially
4. Save results to timestamped log file
5. Print comparison report
6. Launch dashboard (unless --no-dashboard)
7. Optional GIF export

**Arguments:**
- `--export-gif`: Save animation to GIF
- `--no-dashboard`: Skip visualization
- `--algorithms`: Select subset of algorithms
- `--duration`: Override simulation duration

**Output:**
- Log file: `logs/06_batch_simulator_YYYYMMDD_HHMMSS.log`
- GIF file (optional): `gifs/simulation_YYYYMMDD_HHMMSS.gif`

#### 4. `holistic_analysis.py` (227 lines)
**Purpose:** Comprehensive multi-perspective performance analysis
**Perspectives:**
- **Customer:** fulfillment, delivery time, freshness, wait
- **Courier:** utilization, productivity, distance
- **Platform:** throughput, bundle efficiency

**Output:**
- Prints formatted tables for each perspective
- Identifies best performers by metric
- Analyzes trade-offs between algorithms
- Saves log file with timestamp

**Key Metrics Shown:**
- Fulfillment rate (%)
- Click-to-door time (human-readable)
- Ready-to-door time (freshness indicator)
- Courier utilization (%)
- Orders per courier per hour
- Distance traveled (km)
- System throughput (orders/hr)
- Bundle size statistics

#### 5. `decision_snapshots.py` (655 lines)
**Purpose:** Create forensic comparison images of algorithm decisions
**Scenarios:**
- `create_scenario_t60()`: 3 orders, 2 couriers (early batch)
- `create_scenario_t180()`: 6 orders, 3 couriers (complex choices)

**SnapshotVisualizer Class:**
- `draw_snapshot()`: Single algorithm visualization
- Color-coded courier assignments
- Route visualization (courier → restaurant → customer)
- Metrics box with assignment summary

**Output:**
- PNG images: `snapshots/{algorithm}_{time}.png`
- Comparison grid: `snapshots/algorithm_comparison_differentiation.png`
- High resolution: 1440×1200 @ 100 DPI

### Visualization (2 files)

#### 6. `dashboard.py` (362 lines)
**Purpose:** Interactive matplotlib-based visualization
**SimulationDashboard Class:**
- `setup_figure()`: Creates 2×3 grid for 5 algorithms
- `render_frame()`: Draws single time step
- `export_gif()`: Saves animation with Pillow

**Interactive Controls:**
- Spacebar: Play/pause
- Left/Right: Step through frames
- +/-: Speed adjustment (0.5x to 10x)
- Timeline slider: Jump to specific time

**Visual Elements:**
- Green stars: Restaurants
- Colored triangles: Couriers (per-courier colors)
- Colored circles: Orders (state-dependent colors)
- Dashed lines: Active routes
- Metrics overlay: Time, delivered count, bundles, distance

**GIF Export:**
- Default: 5 fps at 80 DPI (18"×12")
- Full simulation: ~35 MB
- Short duration: ~3 MB
- Optimized for file size

#### 7. `generate_individual_gifs.py` (222 lines)
**Purpose:** Per-algorithm GIF generation for detailed comparison
**IndividualDashboard Class:**
- Single-panel layout (instead of 2×3 grid)
- Customizable FPS and duration
- Dedicated algorithm visualization

**Functions:**
- `generate_individual_gif()`: Run one algorithm and export
- `generate_short_scenario()`: Create test scenario with custom parameters

**Command-line Options:**
- `--duration`: Simulation seconds (default 600)
- `--fps`: GIF frame rate (default 5)
- `--orders`: Number of orders
- `--couriers`: Number of couriers
- `--algorithms`: Select subset

## Data Flow Diagram

```
Command Line
    ↓
main.py
    ├─→ simulator_core.generate_scenario()
    │       ├─→ Restaurant objects
    │       ├─→ Courier objects
    │       └─→ Order schedule (Poisson)
    │
    ├─→ simulator_core.run_simulation() [×5 algorithms]
    │       ├─→ assignment_algorithms.assign_*()
    │       │       ├─→ Greedy
    │       │       ├─→ Hungarian (scipy.linear_sum_assignment)
    │       │       ├─→ Simple Bundling
    │       │       ├─→ Route Cost (with TSP)
    │       │       └─→ Batched Pickups (with TSP)
    │       ├─→ Update courier states
    │       ├─→ Update order states
    │       ├─→ Snapshot every 10 seconds
    │       └─→ Compute metrics
    │
    ├─→ Save to logs/06_batch_simulator_*.log
    │
    ├─→ dashboard.SimulationDashboard()
    │       ├─→ Create 2×3 grid visualization
    │       └─→ Optional GIF export
    │
    └─→ Print comparison report
```

## Output Artifact Locations

### Logs
```
logs/06_batch_simulator_YYYYMMDD_HHMMSS.log (14-21 KB each)
├─ ALGORITHM: GREEDY
│  ├─ FINAL METRICS (all 20+ metrics)
│  └─ EVENT LOG (first 50 events with timestamps)
├─ ALGORITHM: HUNGARIAN
├─ ALGORITHM: SIMPLE_BUNDLING
├─ ALGORITHM: ROUTE_COST_BUNDLING
└─ ALGORITHM: BATCHED_PICKUPS
```

### GIFs
```
gifs/simulation_YYYYMMDD_HHMMSS.gif
└─ 2×3 grid animation of all 5 algorithms
   - Default: 5 fps, 80 DPI, 18"×12"
   - Full 3-hour: ~35 MB (1080 frames)
   - 25 minutes: ~3 MB (50 frames)
```

### Decision Snapshots
```
snapshots/
├─ greedy_t60.png, greedy_t180.png
├─ hungarian_t60.png, hungarian_t180.png
├─ simple_bundling_t60.png, simple_bundling_t180.png
├─ route_cost_bundling_t60.png, route_cost_bundling_t180.png
├─ batched_pickups_t60.png, batched_pickups_t180.png
└─ algorithm_comparison_differentiation.png (35"×28" @ 300 DPI)
```

## Simulation Parameters Reference

### Scenario Configuration (simulator_core.py)
```python
GRID_SIZE = 5.0                      # km × km
NUM_RESTAURANTS = 5
NUM_COURIERS = 10
NUM_ORDERS = 250                     # Target for 3-hour sim
SIMULATION_DURATION = 10800          # 3 hours in seconds
BATCH_INTERVAL = 30                  # seconds

COURIER_SPEED_KMH = 30               # km/h → 8.33 m/s
MEAL_PREP_TIME = 300                 # 5 minutes
PICKUP_SERVICE_TIME = 90             # seconds
DROPOFF_SERVICE_TIME = 45            # seconds

BASE_LAMBDA = 0.67                   # orders/min off-peak (~40/hr)
PEAK_LAMBDA = 2.67                   # orders/min peak (~160/hr)
PEAK_START = 3600                    # 1 hour into sim
PEAK_END = 7200                      # 2 hours into sim

RANDOM_SEED = 42                     # For reproducibility
```

### Bundle Configuration (assignment_algorithms.py)
```python
MAX_BUNDLE_SIZE = 3                  # Simple Bundling
max_pickup_radius = 500.0            # meters (Batched Pickups)
max_dropoff_radius = 1000.0          # meters (Batched Pickups)
```

## Algorithm Complexity Analysis

| Algorithm | Time Complexity | Space | Bundling | Notes |
|-----------|-----------------|-------|----------|-------|
| Greedy | O(n·m) | O(1) | None | n=orders, m=couriers |
| Hungarian | O(n³) | O(n²) | None | Optimal 1-to-1 |
| Simple Bundling | O(n) + O(n³) | O(n) | Same-rest | Grouping O(n), Hungarian O(n³) |
| Route Cost | O(n·m·k) + O(n³) | O(n·m) | Selective | Route eval O(n·m·k), k=orders/bundle |
| Batched Pickups | O(n²) + O(n·m·k) | O(n²) | Multi-geo | Clustering O(n²), route eval, Hungarian |

## Performance Results Summary

### 3-Hour Simulation (233 orders, 10 couriers, 5 restaurants)

**Fulfillment Rate (% delivered):**
- Greedy: 52.8%
- Hungarian: 63.1%
- Simple Bundling: **83.3%** ← WINNER
- Route Cost: 69.5%
- Batched Pickups: 69.5%

**Average Delivery Time (minutes):**
- Greedy: 39.6
- Hungarian: 32.5
- Simple Bundling: 30.6
- Route Cost: 23.9
- Batched Pickups: **23.1** ← WINNER

**P90 Delivery Time (minutes):**
- Greedy: 72.0
- Hungarian: 60.0
- Simple Bundling: 58.0
- Route Cost: 50.1
- Batched Pickups: **43.1** ← WINNER (Most Consistent)

**Total Distance (km):**
- Greedy: 636.7
- Hungarian: 614.3
- Simple Bundling: 613.1
- Route Cost: **596.5** ← WINNER
- Batched Pickups: 598.8

**Orders per Hour:**
- Greedy: 41.0
- Hungarian: 49.0
- Simple Bundling: **64.7** ← WINNER
- Route Cost: 54.0
- Batched Pickups: 54.0

## Key Metrics Definitions

### Customer Perspective
- **Fulfillment Rate:** % of valid orders delivered (excludes out-of-scope)
- **Click-to-Door:** Time from order placement to delivery
- **P90 Click-to-Door:** 90th percentile delivery time (consistency indicator)
- **Ready-to-Door:** Time from meal ready to delivery (food freshness)
- **Pickup Wait:** Time from ready until courier arrives at restaurant

### Courier Perspective
- **Utilization:** Active time / total shift time (%)
- **Productivity:** Average orders delivered per courier per hour
- **Distance:** Total kilometers traveled by all couriers

### Platform Perspective
- **Throughput:** Orders delivered per hour of simulation
- **Bundles Created:** Total number of multi-order assignments
- **Average Bundle Size:** Orders per bundle (1.0 = no bundling)

## Testing & Development

### Quick Test Runs
```bash
# Fast 10-minute test
python main.py --duration 600 --no-dashboard

# Test specific algorithm
python main.py --algorithms simple_bundling --no-dashboard

# See decision-making at scale
python decision_snapshots.py
```

### Full Validation
```bash
# Standard 3-hour run with all output
python main.py --export-gif

# Comprehensive analysis
python holistic_analysis.py

# All analysis outputs
python decision_snapshots.py
python generate_individual_gifs.py
```

## Implementation Notes

### Design Principles
1. **Modularity:** Each algorithm is self-contained and pluggable
2. **Reproducibility:** Fixed random seed (RANDOM_SEED = 42)
3. **Extensibility:** Add new algorithms by implementing interface
4. **Observability:** Event logging throughout simulation
5. **Metrics-Driven:** 20+ tracked metrics for analysis

### Critical Sections
- **TSP Optimization:** Lines 303-383 in assignment_algorithms.py
- **Route Evaluation:** Lines 212-300 in assignment_algorithms.py
- **Simulation Loop:** Lines 429-633 in simulator_core.py
- **State Transitions:** Lines 438-568 in simulator_core.py
- **Metrics Computation:** Lines 219-301 in simulator_core.py

### Performance Considerations
- Snapshot storage: ~10-20 MB for 3-hour sim (1080 frames)
- GIF generation: ~1 minute for full simulation export
- Dashboard rendering: Smooth at 2x speed or higher
- Memory usage: ~100-200 MB per concurrent simulation

## References

- TSL-Meituan Data-Driven Research Challenge
- License: Non-commercial, academic use only
- Attribution: "This research was supported by data provided by Meituan."

---

**Last Updated:** November 2, 2025
**Explored:** Complete directory with 2,914 lines of code across 7 modules
**Test Coverage:** 205+ log files with timestamped execution results
