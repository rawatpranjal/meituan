# Simulator Module Documentation

## Overview

This module provides a discrete-event simulation framework for testing courier-order assignment strategies using historical Meituan food delivery data.

## Module Components

### physics.py
Defines simulation constants calibrated from historical data.

**Constants:**
- `AVERAGE_TASK_DURATION`: 1451 seconds (24.2 minutes)
- `GLOBAL_REJECTION_PROBABILITY`: 0.1311 (13.11%)

**Source:** Calibrated from historical waybill data analysis.

### state.py
Manages courier state throughout simulation.

**Functions:**

`initialize_courier_states(first_dispatch_snapshot, waybill_lookup, timeline_logger=None)`
- Initializes courier state dictionary from first dispatch moment
- Returns: Dict mapping courier_id to state dict
- State dict contains: status, becomes_available_at, lat, lng

`get_available_couriers(current_time, courier_states, timeline_logger=None)`
- Returns list of couriers with status AVAILABLE at current_time
- Updates courier states from BUSY to AVAILABLE when becomes_available_at <= current_time
- Returns: List of dicts with courier_id, rider_lat, rider_lng

`update_courier_after_assignment(courier_id, courier_states, dispatch_time, delivery_location, task_duration, timeline_logger=None)`
- Sets courier status to BUSY
- Sets becomes_available_at to dispatch_time + task_duration
- Updates courier location to delivery_location

`get_courier_state_summary(courier_states, current_time)`
- Returns: Dict with total_couriers, available, busy, utilization

**Courier States:**
- `AVAILABLE`: Courier is idle and can accept assignments
- `BUSY`: Courier is on active delivery

### assignment_strategy.py
Defines abstract base class and assignment algorithm implementations.

**Classes:**

`AssignmentStrategy` (Abstract Base Class)
- Method: `make_assignments(waiting_orders, available_couriers, waybill_lookup)`
- Returns: List of (order_dict, courier_dict, cost) tuples

`Tier1Baseline`
- Implementation: Hungarian algorithm (scipy.optimize.linear_sum_assignment)
- Constructor parameter: cost_function instance
- Builds n_orders × n_couriers cost matrix using cost_function.compute_cost()
- Returns optimal one-to-one assignment minimizing total cost

### logger.py
CSV logging for assignment and cycle-level metrics.

**Class: SimulationLogger**

Constructor parameters:
- log_dir: Directory path for CSV files
- model_name: Model identifier for filenames
- cost_function_name: Cost function identifier

**Methods:**

`log_assignment(dispatch_time, order, courier, cost, rank, is_assigned, was_accepted, actual_courier_id, is_match, n_orders, n_couriers, pickup_lat, pickup_lng, platform_order_time=None)`

Logs one row to assignment CSV with columns:
- dispatch_time, order_id, baseline_assigned_courier_id, baseline_cost
- baseline_courier_rank_by_cost, is_assigned_by_baseline, was_accepted
- actual_assigned_courier_id, is_match_with_actual
- num_orders_in_batch, num_couriers_in_pool
- order_pickup_lat, order_pickup_lng
- platform_order_time, wait_for_assignment_seconds, cost_function

`log_cycle_summary(dispatch_time, n_orders, n_couriers, n_proposed, n_accepted, n_rejections, total_cost, agreement_rate)`

Logs one row to cycle summary CSV with columns:
- dispatch_time, num_orders_in_batch, num_available_couriers
- supply_demand_ratio, num_proposed_assignments, num_accepted_assignments
- num_rejections, assignment_rate, acceptance_rate
- total_cost_of_cycle, avg_cost_per_assignment
- agreement_rate_with_actual, cost_function

**Calculated fields:**
- supply_demand_ratio = n_couriers / n_orders
- assignment_rate = n_accepted / n_orders
- acceptance_rate = n_accepted / n_proposed
- avg_cost = total_cost / n_accepted

### courier_timeline_logger.py
CSV logging for courier state transitions.

**Class: CourierTimelineLogger**

Constructor parameters:
- log_dir: Directory path for CSV file
- model_name: Model identifier for filename

**Methods:**

`log_state_transition(timestamp, courier_id, new_state, reason)`

Logs one row to timeline CSV with columns:
- timestamp, courier_id, event_type, new_state, reason

**State transition reasons:**
- `initialized`: Initial state assignment
- `assigned_order`: Courier assigned to order
- `completed_delivery`: Courier finished delivery

## Integration

State management functions accept optional `timeline_logger` parameter. When provided, state transitions are automatically logged to courier_timeline CSV.

Assignment strategy accepts cost_function instance via constructor, enabling separation of optimization algorithm from cost calculation.

## Output Files

Simulation run produces 3 CSV files:
1. `{model_name}_assignment_log_{timestamp}.csv`
2. `{model_name}_cycle_summary_{timestamp}.csv`
3. `{model_name}_courier_timeline_{timestamp}.csv`

File paths returned by logger.get_log_paths() and timeline_logger.get_log_path().
