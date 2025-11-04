# Comprehensive Exploration of /Users/pranjal/Code/meituan/simulation_test

## 1. DIRECTORY STRUCTURE

```
simulation_test/
├── README.md                    # Comprehensive documentation
├── main.py                      # Main entry point (133 lines)
├── simulator_core.py            # Core simulation engine (633 lines)
├── assignment_algorithms.py     # All 5 algorithms (682 lines)
├── dashboard.py                 # Interactive visualization (362 lines)
├── holistic_analysis.py         # Performance analysis script (227 lines)
├── decision_snapshots.py        # Forensic snapshot generator (655 lines)
├── generate_individual_gifs.py  # Individual algorithm GIF generator (222 lines)
├── __pycache__/                 # Python cache
├── logs/                        # Simulation execution logs (200+ files)
│   └── *.log                   # Timestamped logs (14-21KB each)
├── gifs/                        # Animated simulation exports
│   └── simulation_*.gif        # GIF animations (1.6-35MB)
└── snapshots/                   # Decision snapshot images
    ├── *_t60.png               # Snapshots at 60 seconds
    ├── *_t180.png              # Snapshots at 180 seconds
    └── algorithm_comparison_differentiation.png
```

Total: 2,914 lines of Python code

## 2. MAIN ENTRY POINTS

### Primary Entry Point: main.py
```bash
python main.py [--export-gif] [--no-dashboard] [--algorithms] [--duration]
```

**Parameters:**
- `--export-gif`: Export animation to GIF file (default: no export)
- `--no-dashboard`: Skip interactive dashboard, report only
- `--algorithms greedy hungarian simple_bundling route_cost_bundling batched_pickups`: Select subset (default: all 5)
- `--duration SECONDS`: Simulation duration in seconds (default: 10800 for 3 hours)

**Execution Flow:**
1. Generate scenario with restaurants, couriers, and orders
2. Run each algorithm on the same scenario
3. Save detailed results to log file with timestamp
4. Print comparison report
5. Launch interactive dashboard (unless --no-dashboard)
6. Optionally export animation to GIF

### Secondary Entry Points:
- `holistic_analysis.py`: Runs comprehensive analysis from Customer/Courier/Platform perspectives
- `decision_snapshots.py`: Creates forensic comparison images of algorithm decisions
- `generate_individual_gifs.py`: Generates per-algorithm GIF animations

## 3. ALGORITHMS IMPLEMENTED

All algorithms implement a common interface:
```python
def assign_algorithm(state, idle_couriers, ready_orders) -> List[Tuple[courier_id, [order_ids]]]
```

### Algorithm 1: GREEDY (Baseline - Order-First, Nearest Courier)
**Location:** `assignment_algorithms.py` lines 25-57
- **Strategy:** For each ready order, find nearest available courier
- **Bundling:** None (1-to-1 assignments)
- **Cost Function:** Travel time from courier to restaurant
- **Performance:** 52.8% fulfillment, 39.6 min avg delivery
- **Complexity:** O(n*m) where n=orders, m=couriers

### Algorithm 2: HUNGARIAN (Optimal 1-to-1 Bipartite Matching)
**Location:** `assignment_algorithms.py` lines 64-117
- **Strategy:** Linear sum assignment for globally optimal 1-to-1 matching
- **Library:** scipy.optimize.linear_sum_assignment
- **Bundling:** None (1-to-1 assignments)
- **Cost Function:** Travel time from courier to restaurant
- **Performance:** 63.1% fulfillment, 32.5 min avg delivery
- **Complexity:** O(n^3) for Hungarian algorithm
- **Handling:** Pads cost matrix with dummy couriers/orders for unbalanced cases

### Algorithm 3: SIMPLE BUNDLING (Same-Restaurant Grouping + Hungarian)
**Location:** `assignment_algorithms.py` lines 124-205
- **Strategy:** Group orders by restaurant (max 3 per bundle), then Hungarian assignment
- **Steps:**
  1. Group all ready orders by restaurant_id
  2. Split large groups into bundles of MAX_BUNDLE_SIZE=3
  3. Sort bundles by earliest ready time
  4. Build cost matrix (couriers × bundles)
  5. Apply Hungarian algorithm for optimal assignment
- **Cost Function:** Travel time from courier current location to restaurant
- **Performance:** **83.3% fulfillment (BEST)**, 30.6 min avg delivery, 2.02 avg bundle size
- **Key Insight:** Maximizes throughput despite slower delivery times

### Algorithm 4: ROUTE COST BUNDLING (Full Route Optimization)
**Location:** `assignment_algorithms.py` lines 499-565
- **Strategy:** Generate all possible bundles (single + multi-restaurant), evaluate full route costs
- **Steps:**
  1. Generate bundle candidates (single-order + multi-restaurant bundles)
  2. For each courier-bundle pair, calculate full route duration:
     - Travel to all pickup locations (TSP-optimized)
     - Service time at each pickup (90s)
     - Travel to all dropoff locations (TSP-optimized)
     - Service time at each dropoff (45s)
  3. Apply Hungarian algorithm on full route cost matrix
- **TSP Optimization:**
  - **n ≤ 8:** Exact solution via permutations
  - **n > 8:** Nearest neighbor heuristic
- **Cost Function:** Total time = travel + service times
- **Performance:** 69.5% fulfillment, **23.9 min avg delivery (fastest)**, 596.5 km distance
- **Key Insight:** Optimizes for speed and distance efficiency

### Algorithm 5: BATCHED PICKUPS (Multi-Restaurant + Full TSP)
**Location:** `assignment_algorithms.py` lines 572-662
- **Strategy:** Geographic bundling allowing multi-restaurant orders with complete TSP optimization
- **Steps:**
  1. Generate geographic bundles:
     - Restaurants within 500m of each other
     - Customers within 1km of each other
  2. Also include all single-order bundles for flexibility
  3. For each courier-bundle pair, calculate full route duration with TSP
  4. Apply Hungarian algorithm
- **Geographic Constraints:**
  - `max_pickup_radius`: 500 meters (restaurants)
  - `max_dropoff_radius`: 1000 meters (customers)
- **Cost Function:** Total time with TSP optimization and service times
- **Performance:** 69.5% fulfillment, **23.1 min avg delivery (FASTEST)**, 43.1 min P90 (MOST CONSISTENT)
- **Key Insight:** Enables multi-restaurant bundling for better speed/consistency trade-off

## 4. CORE SIMULATION COMPONENTS

### simulator_core.py Structure

**Classes:**
- `Restaurant`: Pickup locations (id, location)
- `Order`: Delivery orders with full lifecycle tracking
- `Courier`: Delivery drivers with state tracking
- `SimulationState`: Main simulation state container with metrics and timeline

**Key Functions:**
- `euclidean_distance(loc1, loc2)`: Distance calculation in km
- `get_travel_time(loc1, loc2)`: Travel time in seconds at 30 km/h
- `generate_scenario(seed, duration)`: Creates restaurants, couriers, orders
- `run_simulation(scenario, assignment_algorithm, name)`: Main simulation loop

**Simulation Loop (run_simulation, 1-second granularity):**
1. Process order placements from queue
2. Update order states (PENDING → READY when ready_time reached)
3. Update courier states and handle arrivals
4. Run batch assignments every 30 seconds (BATCH_INTERVAL)
5. Create snapshots every 10 seconds for visualization
6. Compute final metrics at end

### Order States
```
PENDING → READY → ASSIGNED → PICKED_UP → DELIVERED
```

### Courier States
```
IDLE → DRIVING_TO_PICKUP → AT_PICKUP → DRIVING_TO_DROPOFF → IDLE
```

### Key Parameters
- **GRID_SIZE**: 5 km × 5 km
- **NUM_RESTAURANTS**: 5
- **NUM_COURIERS**: 10
- **SIMULATION_DURATION**: 10,800 seconds (3 hours)
- **BATCH_INTERVAL**: 30 seconds
- **COURIER_SPEED_KMH**: 30 km/h (8.33 m/s)
- **MEAL_PREP_TIME**: 300 seconds (5 minutes)
- **PICKUP_SERVICE_TIME**: 90 seconds
- **DROPOFF_SERVICE_TIME**: 45 seconds
- **POISSON_BASE_LAMBDA**: 0.67/min (~40 orders/hr off-peak)
- **POISSON_PEAK_LAMBDA**: 2.67/min (~160 orders/hr peak)
- **PEAK_HOURS**: 1-2 hours into simulation

### Metrics Tracked

**Customer Perspective:**
- `fulfillment_rate_pct`: % of valid orders delivered
- `avg_click_to_door_time`: From order placement to delivery
- `p90_click_to_door_time`: 90th percentile delivery time
- `avg_ready_to_door_time`: From ready to delivery (freshness)
- `avg_pickup_wait_time`: Time from ready to pickup

**Courier Perspective:**
- `courier_utilization_pct`: Active time / total time
- `avg_orders_per_courier_hour`: Productivity metric
- `total_distance_traveled_km`: Total km driven

**Platform Perspective:**
- `system_throughput_orders_per_hour`: Orders delivered per simulation hour
- `orders_delivered`: Total delivered
- `bundles_created`: Number of bundles
- `avg_bundle_size`: Orders per bundle

**Intermediate Metrics:**
- `orders_in_transit`: PICKED_UP + ASSIGNED orders
- `orders_unassigned`: READY + PENDING orders
- `orders_out_of_scope`: Orders that couldn't be ready in time

## 5. OUTPUT FORMAT AND METRICS TRACKING

### Log Files
**Location:** `/Users/pranjal/Code/meituan/simulation_test/logs/`
**Naming:** `06_batch_simulator_YYYYMMDD_HHMMSS.log`
**Format:** Structured text with sections per algorithm
**Content per Algorithm:**
- Final metrics (all 20+ metrics)
- Event log (first 50 events, with timestamps and descriptions)

**Example Events:**
- ORDER_PLACED: Order entered system
- ORDER_READY: Order ready for pickup
- ASSIGNMENT_MADE: Courier assigned (with order_ids and bundle_size)
- PICKUP_SERVICE_START: Courier at restaurant
- ORDER_PICKED_UP: Order picked up
- ROUTE_OPTIMIZED: TSP optimization applied
- COURIER_DELIVERING: Starting delivery phase
- ORDER_DELIVERED: Order delivered
- MULTI_RESTAURANT_BUNDLE: Multi-restaurant bundle created

### Dashboard Visualization
**Type:** Interactive matplotlib-based
**Layout:** 2×3 grid (1 empty spot) showing all 5 algorithms
**Controls:**
- Spacebar: Play/pause
- Left/Right arrows: Step through frames
- +/-: Speed control (0.5x to 10x)
- Timeline slider: Jump to time
- Resolution: 18" × 12" at 80 DPI
- Frame rate: 5 fps default, 10 fps for GIF export

**Visual Elements:**
- Green stars: Restaurants
- Colored triangles: Couriers (color per courier)
- Colored circles: Orders (state-dependent colors)
- Dashed lines: Active courier routes
- Metrics overlay: Time, delivered count, bundles, avg bundle size, distance

### GIF Exports
**Format:** PNG frames stitched together with Pillow
**Default Duration:** 10,800 seconds (180 frames at 5 fps)
**Size:** 35 MB for full 3-hour simulation (1080 frames)
**Short Duration:** 25 minutes ~3 MB (50 frames)
**Location:** `/Users/pranjal/Code/meituan/simulation_test/gifs/simulation_YYYYMMDD_HHMMSS.gif`

### Decision Snapshots
**Location:** `/Users/pranjal/Code/meituan/simulation_test/snapshots/`
**Format:** High-resolution PNG images (1440 × 1200 pixels at 100 DPI)
**Content:**
- Algorithm name and decision point (t=60s or t=180s)
- Restaurants: Green stars with labels
- Couriers: Colored triangles with IDs
- Orders: Circles at restaurant (pickup) with diner destination
- Routes: Lines from courier to restaurants and to diners
- Assignment colors: Each courier gets unique color for assigned orders
- Unassigned orders: Yellow circles

## 6. TEST RESULTS AND LOGS PRESENT

### Available Test Runs (205 log files)
- `06_batch_simulator_*.log`: 14+ simulation runs (14-21 KB each)
- `holistic_analysis_*.log`: 190+ analysis runs (mostly 0 bytes - writes to stdout)

### Latest Successful Run (Nov 2, 04:08:55)
File: `/Users/pranjal/Code/meituan/simulation_test/logs/06_batch_simulator_20251102_040855.log`

**Results for 900-second simulation (15 minutes):**
- All 5 algorithms ran on same 6-order scenario
- 10 couriers available
- Greedy: 0 delivered (orders in transit at end)
- Hungarian: 0 delivered
- Simple Bundling: 0 delivered
- Route Cost: 0 delivered
- Batched Pickups: 0 delivered
- Total distance: 3.56 km per algorithm
- Utilization: 60% per algorithm
- Courier idle time: 3600s (1 hour)

Note: Short duration (900s) means orders placed near end weren't completed.

### GIF Archives
- Largest: 35 MB (full 3-hour simulation)
- Medium: 2.9 MB (shorter duration)
- Smallest: 1.6 MB

## 7. DEPENDENCIES AND REQUIREMENTS

### External Libraries
```python
import numpy as np              # Numerical operations
from scipy.optimize import linear_sum_assignment  # Hungarian algorithm
import matplotlib.pyplot as plt # Plotting
import matplotlib.patches as patches  # Shape drawing
from matplotlib.widgets import Slider, Button  # Interactive controls
from matplotlib.animation import FuncAnimation, PillowWriter  # Animation
from datetime import datetime   # Timestamps
from copy import deepcopy       # Deep copying
from typing import List, Tuple, Dict, Optional  # Type hints
from dataclasses import dataclass  # Data classes
from itertools import permutations  # TSP optimization
import json                     # Logging/serialization
```

### Python Version
- Python 3.13+ (uses f-strings, type hints)

### Installation
```bash
pip install numpy scipy matplotlib pillow
```

## 8. COMPLETE PERFORMANCE COMPARISON

### 3-Hour Simulation Results (233 orders, 10 couriers, 5 restaurants)

| Metric | Greedy | Hungarian | Simple Bundle | Route Cost | Batched Pickups |
|--------|--------|-----------|--------------|------------|-----------------|
| **Delivered Orders** | 123 | 147 | **194** | 162 | 162 |
| **Fulfillment Rate** | 52.8% | 63.1% | **83.3%** | 69.5% | 69.5% |
| **Avg Delivery Time** | 39.6 min | 32.5 min | 30.6 min | **23.9 min** | **23.1 min** |
| **P90 Delivery Time** | 1.2 h | 1.0 h | 58 min | 50.1 min | **43.1 min** |
| **Total Distance** | 636.7 km | 614.3 km | 613.1 km | **596.5 km** | 598.8 km |
| **Orders/Hour** | 41.0 | 49.0 | **64.7** | 54.0 | 54.0 |
| **Avg Bundle Size** | 1.0 | 1.0 | 2.02 | 1.3 | 1.4 |
| **Utilization** | High | High | High | High | High |

### Recommendations by Use Case

**High Volume Platform:**
- Use **Simple Bundling** during peak hours
- Delivers 194 orders (83.3% fulfillment)
- Best throughput (64.7 orders/hour)
- Accept 28% slower delivery for 20% more volume

**Premium Service Platform:**
- Use **Batched Pickups** for best experience
- Fastest delivery (23.1 min average)
- Most consistent (43.1 min P90)
- TSP optimization ensures efficient routes

**Hybrid Approach:**
- Off-peak: Batched Pickups (speed)
- Peak hours: Simple Bundling (volume)
- Premium tier: Always Batched Pickups

## 9. HOW COMPONENTS WORK TOGETHER

### Simulation Loop Architecture
```
main.py
  ├─ generate_scenario()          [simulator_core.py]
  │  └─ Creates restaurants, couriers, orders
  ├─ run_simulation(algorithm)    [simulator_core.py]
  │  ├─ Loop through each second (0 to duration)
  │  │  ├─ Process order placements
  │  │  ├─ Update order states
  │  │  ├─ Update courier states
  │  │  ├─ Every 30s: Call algorithm()  [assignment_algorithms.py]
  │  │  │  └─ Returns (courier_id, [order_ids]) assignments
  │  │  └─ Every 10s: Take snapshot (for visualization)
  │  └─ Compute final metrics
  ├─ dashboard.py (if not --no-dashboard)
  │  ├─ Load results from all algorithms
  │  ├─ Create 2×3 grid visualization
  │  ├─ Render each frame
  │  └─ Handle playback controls
  └─ Write logs
```

### TSP Optimization Pipeline
```
When courier picks up orders:
  1. Get all dropoff locations
  2. If n ≤ 8: Try all permutations, find min distance
  3. If n > 8: Use nearest neighbor heuristic
  4. Reorder assigned_order_ids based on optimal sequence
  5. Continue simulation with optimized route
```

### Algorithm Selection Flow
```
assignment_algorithm(state, idle_couriers, ready_orders)
  ├─ Greedy: O(n*m) - nearest courier per order
  ├─ Hungarian: O(n³) - optimal 1-to-1 matching
  ├─ Simple Bundling: O(n) grouping + Hungarian
  ├─ Route Cost: O(n) bundling + route eval + Hungarian
  └─ Batched Pickups: O(n) geographic bundling + route eval + Hungarian
```

## 10. CONFIGURATION AND CUSTOMIZATION

### Key Tunable Parameters (simulator_core.py)
```python
# Simulation time
SIMULATION_DURATION = 10800  # seconds

# Entity counts
NUM_RESTAURANTS = 5
NUM_COURIERS = 10
NUM_ORDERS = 250  # target for 3-hour sim

# Logistics
BATCH_INTERVAL = 30  # seconds between assignment batches
COURIER_SPEED_KMH = 30
MEAL_PREP_TIME = 300  # 5 minutes
PICKUP_SERVICE_TIME = 90  # seconds
DROPOFF_SERVICE_TIME = 45  # seconds

# Demand patterns
BASE_LAMBDA = 0.67  # orders/min off-peak (~40/hr)
PEAK_LAMBDA = 2.67  # orders/min peak (~160/hr)
PEAK_START = 3600   # start of peak (1 hour)
PEAK_END = 7200     # end of peak (2 hours)
```

### Bundle Configuration (assignment_algorithms.py)
```python
# Simple Bundling
MAX_BUNDLE_SIZE = 3  # max orders per bundle

# Batched Pickups (geographic constraints)
max_pickup_radius = 500.0    # meters between restaurants
max_dropoff_radius = 1000.0  # meters between customers
```

### Visualization Configuration (dashboard.py)
```python
# GIF export
fps = 5  # frames per second
dpi = 80  # dots per inch
figsize = (18, 12)  # inches
```

## 11. RUNNING THE SIMULATOR

### Standard 3-Hour Simulation
```bash
cd /Users/pranjal/Code/meituan/simulation_test
python main.py
```

### Without Dashboard (Report Only)
```bash
python main.py --no-dashboard
```

### Specific Algorithms
```bash
python main.py --algorithms greedy simple_bundling batched_pickups
```

### Export Full GIF (35MB)
```bash
python main.py --export-gif
```

### Export Short GIF (3MB, 25 minutes)
```bash
python main.py --export-gif --duration 1500
```

### Custom Duration (10 minutes)
```bash
python main.py --duration 600
```

### Comprehensive Analysis
```bash
python holistic_analysis.py
```

### Decision Snapshots
```bash
python decision_snapshots.py
```

### Individual Algorithm GIFs
```bash
python generate_individual_gifs.py --duration 600 --fps 5 --orders 30 --couriers 5
```

## 12. KEY INSIGHTS FROM IMPLEMENTATION

### Design Patterns
1. **Factory Pattern**: `get_algorithm(name)` returns function
2. **Strategy Pattern**: Algorithms implement common interface
3. **Observer Pattern**: Event logging throughout simulation
4. **State Pattern**: Order/Courier states with lifecycle management

### Technical Highlights
- **Discrete-event simulation** with 1-second granularity
- **TSP optimization** using exact (n≤8) or heuristic (n>8) methods
- **Hungarian algorithm** for optimal assignment
- **Realistic service times** included in route calculations
- **Geographic clustering** for multi-restaurant bundling
- **Comprehensive metrics** tracking 20+ KPIs

### Trade-offs Observed
1. **Bundling vs Speed**: More bundling = higher volume but slower delivery
2. **Greedy vs Optimal**: Greedy is O(nm) but loses 30-50% vs Hungarian
3. **TSP vs Heuristic**: Exact TSP only feasible for ≤8 items
4. **Multi-restaurant vs Same-restaurant**: Broader bundles need geographic constraints

### Real-World Applications
- Food delivery platforms need to switch strategies by time of day
- Peak hours: Simple Bundling (capacity)
- Off-peak: Batched Pickups (quality)
- Premium tier: Always optimal (Hungarian + TSP)

