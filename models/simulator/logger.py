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

    def __init__(self, log_dir, model_name="tier1_baseline", cost_function_name="unknown"):
        """
        Initialize logger with output directory, model name, and cost function

        Args:
            log_dir: Directory to save CSV files
            model_name: Name of the model being tested (for filename)
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
            'cost_function'
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
            'cost_function'
        ])

    def log_assignment(self, dispatch_time, order, courier, cost, rank, is_assigned,
                      was_accepted, actual_courier_id, is_match, n_orders, n_couriers,
                      pickup_lat, pickup_lng, platform_order_time=None,
                      baseline_courier_lat=None, baseline_courier_lng=None,
                      actual_courier_lat=None, actual_courier_lng=None):
        """
        Log a single order assignment (or non-assignment)

        Args:
            dispatch_time: When this decision was made
            order: Order dict
            courier: Courier dict (None if unassigned)
            cost: Assignment cost (None if unassigned)
            rank: Courier rank by cost (None if unassigned)
            is_assigned: Whether this order was assigned by our model
            was_accepted: Whether courier accepted (False if rejected)
            actual_courier_id: Historical courier assignment
            is_match: Whether our assignment matches actual
            n_orders: Total orders in batch
            n_couriers: Total available couriers
            pickup_lat, pickup_lng: Order pickup location
            platform_order_time: When customer placed order (for wait time calculation)
            baseline_courier_lat, baseline_courier_lng: Baseline courier location (optional)
            actual_courier_lat, actual_courier_lng: Actual courier location (optional)
        """
        courier_id = courier['courier_id'] if courier else None

        # Calculate wait time if platform_order_time is available
        wait_seconds = None
        if platform_order_time is not None and is_assigned:
            wait_seconds = dispatch_time - platform_order_time

        self.assignment_writer.writerow([
            dispatch_time, order['order_id'],
            courier_id, cost, rank,
            is_assigned, was_accepted,
            actual_courier_id, is_match,
            n_orders, n_couriers,
            pickup_lat, pickup_lng,
            baseline_courier_lat, baseline_courier_lng,
            actual_courier_lat, actual_courier_lng,
            platform_order_time, wait_seconds,
            self.cost_function_name
        ])

    def log_cycle_summary(self, dispatch_time, n_orders, n_couriers,
                         n_proposed, n_accepted, n_rejections,
                         total_cost, agreement_rate):
        """
        Log summary metrics for a dispatch cycle

        Args:
            dispatch_time: The dispatch moment
            n_orders: Number of orders in batch
            n_couriers: Number of available couriers
            n_proposed: Number of assignments proposed by algorithm
            n_accepted: Number of assignments accepted by couriers
            n_rejections: Number of assignments rejected
            total_cost: Sum of assignment costs
            agreement_rate: Fraction matching actual system
        """
        supply_demand_ratio = n_couriers / n_orders if n_orders > 0 else 0
        assignment_rate = n_accepted / n_orders if n_orders > 0 else 0
        acceptance_rate = n_accepted / n_proposed if n_proposed > 0 else 0
        avg_cost = total_cost / n_accepted if n_accepted > 0 else 0

        self.cycle_writer.writerow([
            dispatch_time,
            n_orders, n_couriers, supply_demand_ratio,
            n_proposed, n_accepted, n_rejections,
            assignment_rate, acceptance_rate,
            total_cost, avg_cost,
            agreement_rate,
            self.cost_function_name
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
