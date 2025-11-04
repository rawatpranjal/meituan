"""
Metrics Logger - The "Instrumentation"

Handles structured logging of simulation metrics to CSV files.
"""

import csv
from datetime import datetime


class SimulationLogger:
    """
    Manages CSV logging for granular and summary metrics
    """

    def __init__(self, model_name, log_dir, cost_function_name="unknown"):
        """
        Initialize logger with output directory, model name, and cost function

        Args:
            model_name: Name of the model being tested (for filename)
            log_dir: Directory to save CSV files
            cost_function_name: Name of the cost function being used
        """
        self.log_dir = log_dir
        self.model_name = model_name
        self.cost_function_name = cost_function_name
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

        # Create assignment log
        self.assignment_log_path = f"{log_dir}/{model_name}_assignment_log_{timestamp}.csv"
        self.assignment_csv = open(self.assignment_log_path, 'w', newline='')
        self.assignment_writer = csv.writer(self.assignment_csv)
        self.assignment_writer.writerow([
            'dispatch_time', 'order_id',
            'baseline_assigned_courier_id', 'baseline_cost', 'baseline_courier_rank_by_cost',
            'is_assigned_by_baseline', 'was_accepted',
            'actual_assigned_courier_id', 'is_match_with_actual',
            'num_orders_in_batch', 'num_couriers_in_pool',
            'order_pickup_lat', 'order_pickup_lng',
            'baseline_courier_lat', 'baseline_courier_lng',
            'actual_courier_lat', 'actual_courier_lng',
            'platform_order_time', 'wait_for_assignment_seconds',
            'cost_function',
            # New fields for dual-mode system
            'mode', 'strategy_key', 'bundling_on', 'micro_batch_sec',
            'unit_type', 'bundle_size'
        ])

        # Create cycle summary log
        self.cycle_summary_path = f"{log_dir}/{model_name}_cycle_summary_{timestamp}.csv"
        self.cycle_csv = open(self.cycle_summary_path, 'w', newline='')
        self.cycle_writer = csv.writer(self.cycle_csv)
        self.cycle_writer.writerow([
            'dispatch_time',
            'num_orders_in_batch', 'num_available_couriers', 'supply_demand_ratio',
            'num_proposed_assignments', 'num_accepted_assignments', 'num_rejections',
            'assignment_rate', 'acceptance_rate',
            'total_cost_of_cycle', 'avg_cost_per_assignment',
            'agreement_rate_with_actual',
            'cost_function',
            # New fields for dual-mode system
            'mode', 'strategy_key',
            'num_units', 'num_bundles', 'avg_bundle_size',
            'micro_batch_sec', 'optimizer_status',
            'num_deferred_in', 'num_deferred_out', 'num_deferred_carry',
            # Latency instrumentation fields
            'decision_ms', 'optimizer_ms'
        ])

    def log_assignment(self, dispatch_time=None, order_id=None,
                      baseline_assigned_courier_id=None, baseline_cost=None,
                      baseline_courier_rank_by_cost=1, is_assigned_by_baseline=False,
                      was_accepted=False, actual_assigned_courier_id=None,
                      is_match_with_actual=False, num_orders_in_batch=0,
                      num_couriers_in_pool=0, order_pickup_lat=None, order_pickup_lng=None,
                      platform_order_time=None, wait_for_assignment_seconds=None,
                      cost_function=None, baseline_courier_lat=None, baseline_courier_lng=None,
                      actual_courier_lat=None, actual_courier_lng=None,
                      # New fields
                      mode='BATCH', strategy_key='unknown', bundling_on=0,
                      micro_batch_sec=0, unit_type='order', bundle_size=1,
                      # Legacy support
                      order=None, courier=None, cost=None, rank=None, is_assigned=None,
                      n_orders=None, n_couriers=None, pickup_lat=None, pickup_lng=None):
        """
        Log a single order assignment with support for both new and legacy interfaces.

        New interface uses named parameters matching CSV columns.
        Legacy interface supported for backward compatibility.
        """
        # Handle legacy interface
        if order is not None:
            order_id = order.get('order_id') if isinstance(order, dict) else order_id
        if courier is not None:
            baseline_assigned_courier_id = courier.get('courier_id') if isinstance(courier, dict) else baseline_assigned_courier_id
            if baseline_courier_lat is None and isinstance(courier, dict):
                baseline_courier_lat = courier.get('lat') or courier.get('rider_lat')
                baseline_courier_lng = courier.get('lng') or courier.get('rider_lng')
        if cost is not None:
            baseline_cost = cost
        if rank is not None:
            baseline_courier_rank_by_cost = rank
        if is_assigned is not None:
            is_assigned_by_baseline = is_assigned
        if n_orders is not None:
            num_orders_in_batch = n_orders
        if n_couriers is not None:
            num_couriers_in_pool = n_couriers
        if pickup_lat is not None:
            order_pickup_lat = pickup_lat
        if pickup_lng is not None:
            order_pickup_lng = pickup_lng

        # Calculate wait time if not provided
        if wait_for_assignment_seconds is None and platform_order_time is not None and dispatch_time is not None:
            wait_for_assignment_seconds = dispatch_time - platform_order_time

        # Use instance cost function name if not provided
        if cost_function is None:
            cost_function = self.cost_function_name

        self.assignment_writer.writerow([
            dispatch_time, order_id,
            baseline_assigned_courier_id, baseline_cost, baseline_courier_rank_by_cost,
            is_assigned_by_baseline, was_accepted,
            actual_assigned_courier_id, is_match_with_actual,
            num_orders_in_batch, num_couriers_in_pool,
            order_pickup_lat, order_pickup_lng,
            baseline_courier_lat, baseline_courier_lng,
            actual_courier_lat, actual_courier_lng,
            platform_order_time, wait_for_assignment_seconds,
            cost_function,
            mode, strategy_key, bundling_on, micro_batch_sec,
            unit_type, bundle_size
        ])

    def log_cycle_summary(self, dispatch_time=None, num_orders_in_batch=None,
                         num_available_couriers=None, supply_demand_ratio=None,
                         num_proposed_assignments=None, num_accepted_assignments=None,
                         num_rejections=None, assignment_rate=None, acceptance_rate=None,
                         total_cost_of_cycle=None, avg_cost_per_assignment=None,
                         agreement_rate_with_actual=None, cost_function=None,
                         # New fields
                         mode='BATCH', strategy_key='unknown',
                         num_units=None, num_bundles=0, avg_bundle_size=1.0,
                         micro_batch_sec=0, optimizer_status='ok',
                         num_deferred_in=0, num_deferred_out=0, num_deferred_carry=0,
                         # Latency instrumentation
                         decision_ms=0.0, optimizer_ms=0.0,
                         # Legacy support
                         n_orders=None, n_couriers=None, n_proposed=None,
                         n_accepted=None, total_cost=None, agreement_rate=None):
        """
        Log summary metrics for a dispatch cycle with support for both new and legacy interfaces.

        New interface uses named parameters matching CSV columns.
        Legacy interface supported for backward compatibility.
        """
        # Handle legacy interface
        if n_orders is not None:
            num_orders_in_batch = n_orders
        if n_couriers is not None:
            num_available_couriers = n_couriers
        if n_proposed is not None:
            num_proposed_assignments = n_proposed
        if n_accepted is not None:
            num_accepted_assignments = n_accepted
        if num_rejections is None and num_proposed_assignments is not None and num_accepted_assignments is not None:
            num_rejections = num_proposed_assignments - num_accepted_assignments
        if total_cost is not None:
            total_cost_of_cycle = total_cost
        if agreement_rate is not None:
            agreement_rate_with_actual = agreement_rate

        # Calculate derived metrics if not provided
        if supply_demand_ratio is None and num_available_couriers is not None and num_orders_in_batch:
            supply_demand_ratio = num_available_couriers / num_orders_in_batch if num_orders_in_batch > 0 else 0
        if assignment_rate is None and num_accepted_assignments is not None and num_orders_in_batch:
            assignment_rate = num_accepted_assignments / num_orders_in_batch if num_orders_in_batch > 0 else 0
        if acceptance_rate is None and num_accepted_assignments is not None and num_proposed_assignments:
            acceptance_rate = num_accepted_assignments / num_proposed_assignments if num_proposed_assignments > 0 else 0
        if avg_cost_per_assignment is None and total_cost_of_cycle is not None and num_accepted_assignments:
            avg_cost_per_assignment = total_cost_of_cycle / num_accepted_assignments if num_accepted_assignments > 0 else 0

        # Use instance cost function name if not provided
        if cost_function is None:
            cost_function = self.cost_function_name

        # Default num_units to num_orders if not provided
        if num_units is None:
            num_units = num_orders_in_batch

        self.cycle_writer.writerow([
            dispatch_time,
            num_orders_in_batch, num_available_couriers, supply_demand_ratio,
            num_proposed_assignments, num_accepted_assignments, num_rejections,
            assignment_rate, acceptance_rate,
            total_cost_of_cycle, avg_cost_per_assignment,
            agreement_rate_with_actual,
            cost_function,
            mode, strategy_key,
            num_units, num_bundles, avg_bundle_size,
            micro_batch_sec, optimizer_status,
            num_deferred_in, num_deferred_out, num_deferred_carry,
            decision_ms, optimizer_ms
        ])

    def flush(self):
        """Flush CSV buffers to disk"""
        self.assignment_csv.flush()
        self.cycle_csv.flush()

    def close(self):
        """Close CSV files"""
        self.assignment_csv.close()
        self.cycle_csv.close()

    def get_log_paths(self):
        """Get paths to log files"""
        return {
            'assignment_log': self.assignment_log_path,
            'cycle_summary': self.cycle_summary_path
        }
