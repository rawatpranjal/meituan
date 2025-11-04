# Performance Analysis: Why Anticipated Bundling Fails

## Executive Summary

**Critical Bug Identified**: Anticipated Network Bundling is underperforming by 47-50% due to an architectural mismatch between the algorithm design and the simulator's order filtering.

## Performance Results

### Downtown Crush (382 orders, 3 hours)

| Algorithm | Delivered | Fulfillment | Distance | Utilization | Throughput |
|-----------|-----------|-------------|----------|-------------|------------|
| **Hungarian (Best)** | 209 | 55.4% | 460.9 km | 91.1% | 69.7 ord/hr |
| Simple Bundling | 207 | 54.2% | 457.5 km | - | - |
| Network Bundling | 208 | 54.5% | 458.5 km | - | - |
| Greedy | 175 | 45.8% | 485.3 km | - | - |
| **Anticipated (Worst)** | 110 | **28.8%** | 223.9 km | **53.7%** | 36.7 ord/hr |

**Key Finding**: Anticipated delivers 47% FEWER orders than Hungarian despite being designed as the most sophisticated algorithm.

### Popup Problem (240 orders, 4 hours)

| Algorithm | Delivered | Fulfillment | Distance |
|-----------|-----------|-------------|----------|
| Hungarian | 109 | 45.4% | 348.8 km |
| Simple Bundling | 108 | 45.0% | 351.3 km |
| Network Bundling | 105 | 43.8% | 336.0 km |
| Greedy | 109 | 45.4% | 372.6 km |
| **Anticipated** | 73 | **30.4%** | 228.4 km |

**Key Finding**: Anticipated delivers 33% FEWER orders across the board.

## Root Cause Analysis

### The Architectural Mismatch

**Location 1: Simulator (simulator_core.py:274-277)**
```python
def get_ready_orders(self) -> List[Order]:
    """Get all orders in READY state (excluding EXPIRED)."""
    return [o for o in self.orders.values()
            if o.state == "READY" and self.current_time >= o.ready_time]
```

**Problem**: Only returns orders in "READY" state. Completely ignores "PENDING" orders.

**Location 2: Anticipated Algorithm (assignment_algorithms.py:701-704)**
```python
assignable_orders = [
    o for o in state.orders.values()
    if o.state in ["PENDING", "READY"] and o.ready_time <= current_time + LOOKAHEAD_WINDOW
]
```

**Design Intent**: Looks for BOTH "PENDING" and "READY" orders within 15-minute lookahead window.

### The Execution Flow

1. **Simulator calls** `get_ready_orders()` → Only "READY" orders returned
2. **Passes to algorithm** `assignment_algorithm(state, idle_couriers, ready_orders)`
3. **Anticipated filters again** for orders within lookahead window
4. **Result**: Very few orders meet both criteria → "READY ORDERS (0)" in logs
5. **Couriers sit idle** → 53.7% utilization vs 91.1% for Hungarian
6. **Massive underperformance** → 47% fewer deliveries

## Why This Happens

### Order Lifecycle
- **PENDING**: Order placed, meal being prepared (5 minutes prep time)
- **READY**: Meal ready for pickup
- **ASSIGNED**: Courier assigned
- **PICKED_UP**: Courier has the meal
- **DELIVERED**: Complete

### The Problem
- Most orders spend significant time in "PENDING" state (meal prep = 300s)
- Hungarian gets "READY" orders and dispatches immediately
- Anticipated tries to look ahead at "PENDING" orders for anticipatory dispatch
- **BUT**: Simulator only gives it "READY" orders!
- Anticipated re-filters these "READY" orders (pointlessly) and ends up with same or fewer orders
- No anticipatory benefit, just extra filtering overhead

## Why Other Algorithms Work Fine

| Algorithm | Design | Works With READY Orders? |
|-----------|--------|--------------------------|
| Greedy | Nearest courier per order | ✓ Yes |
| Hungarian | Optimal 1-to-1 matching | ✓ Yes |
| Simple Bundling | Same-restaurant grouping | ✓ Yes |
| Network Bundling | Multi-restaurant bundles | ✓ Yes |
| **Anticipated** | **Lookahead + bundles** | ✗ **NO - NEEDS PENDING** |

## Evidence from Logs

### Downtown Crush - Anticipated Bundling Log

```
BATCH 3 @ t=600s (00:10:00)
AVAILABLE COURIERS (12):
  - Courier 0 @ (1.94, 1.63) - IDLE
  - Courier 1 @ (2.07, 1.35) - IDLE
  [... 10 more idle couriers ...]

READY ORDERS (0):

ASSIGNMENTS MADE (1):
✓ Courier 10 ← Orders [0] (Bundle size: 1)
```

**Problem**: 12 idle couriers, but 0 ready orders → Only 1 assignment made

### Comparison: Hungarian Log (same timestamp)

```
BATCH 3 @ t=600s
AVAILABLE COURIERS (12):
READY ORDERS (8):

ASSIGNMENTS MADE (8):
[... 8 assignments made ...]
```

**Hungarian**: Same 12 couriers, but has 8 ready orders → Makes 8 assignments

## The Irony

The anticipated bundling algorithm was designed to be **THE MOST SOPHISTICATED**:
- Lookahead window (15 minutes)
- Anticipatory dispatch (assign before food is ready)
- Holistic cost function (route + wait penalties + delay penalties)

But it performs **THE WORST** because:
- It's being starved of the data it needs (PENDING orders)
- The anticipatory logic becomes useless
- Couriers sit idle waiting for orders that never appear

## Solution

**Option 1: Modify simulator to pass PENDING orders to anticipated bundling**
- Create `get_assignable_orders()` method that returns both PENDING and READY
- Only call this for anticipated bundling algorithm
- Keep `get_ready_orders()` for other algorithms

**Option 2: Modify anticipated algorithm to work with READY orders only**
- Remove lookahead logic
- Make it work like network bundling but with smarter cost function
- Loses anticipatory capability but at least works

**Option 3: Give ALL algorithms access to PENDING orders**
- Modify simulator to always pass both PENDING and READY
- Let each algorithm decide what to do with them
- Most algorithms will ignore PENDING (by design)
- Anticipated can finally use them

## Recommendations

**Immediate Fix**: Option 1
- Minimal code change
- Preserves anticipated bundling's design intent
- Doesn't affect other algorithms

**Long-term**: Option 3
- More flexible architecture
- Enables future algorithms that might want lookahead
- Clean separation of concerns

## Manhattan Distance Impact

Note: The switch to Manhattan distance (vs Euclidean) is working correctly across all algorithms. The performance issues with anticipated bundling are unrelated to the distance metric change.

Manhattan distance results in longer routes (~22% increase) but this is realistic for city-block navigation and affects all algorithms equally.
