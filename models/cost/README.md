# Cost Functions Module Documentation

## Overview

This module defines cost functions for courier-order assignment optimization. Cost functions calculate the cost of assigning a specific courier to a specific order.

## Architecture

Assignment strategies use cost functions to build cost matrices. The optimization algorithm and cost calculation are decoupled via the BaseCostFunction interface.

## Base Interface

### BaseCostFunction (Abstract Base Class)

Location: `base.py`

**Required Methods:**

`compute_cost(self, courier, order, order_location) -> float`
- Parameters:
  - courier: Dict containing courier_id, rider_lat, rider_lng
  - order: Dict containing order_id
  - order_location: Dict containing sender_lat, sender_lng, recipient_lat, recipient_lng
- Returns: float (cost value, lower = better match)

`get_name(self) -> str`
- Returns: String identifier for cost function (used in logs)

`get_description(self) -> str`
- Returns: Human-readable description of cost function

## Implemented Cost Functions

### DistanceToPickup

Location: `distance_to_pickup.py`

**Name:** `distance_to_pickup`

**Description:** Euclidean distance from courier current location to restaurant pickup location

**Formula:**
```
cost = sqrt((courier_lat - restaurant_lat)² + (courier_lng - restaurant_lng)²)
```

**Implementation:**
```python
def compute_cost(self, courier, order, order_location):
    courier_lat = courier['rider_lat']
    courier_lng = courier['rider_lng']
    restaurant_lat = order_location['sender_lat']
    restaurant_lng = order_location['sender_lng']

    distance = np.sqrt(
        (courier_lat - restaurant_lat)**2 +
        (courier_lng - restaurant_lng)**2
    )
    return distance
```

**Usage:**
- Tier 1 baseline model
- Minimizes courier travel to pickup location
- Does not consider customer delivery location
- Does not consider time constraints

## Module Exports

Location: `__init__.py`

Exported classes:
- `BaseCostFunction`
- `DistanceToPickup`

## Integration with Simulator

Cost function instances are passed to assignment strategy constructors:

```python
from cost import DistanceToPickup
from simulator.assignment_strategy import Tier1Baseline

cost_function = DistanceToPickup()
strategy = Tier1Baseline(cost_function)
```

Assignment strategy calls `cost_function.compute_cost()` for each order-courier pair to build cost matrix.

Cost function name logged to assignment and cycle summary CSVs via `cost_function.get_name()`.
