"""
Batch mode orchestrator that processes orders at dispatch_time checkpoints.
Mimics the original batch dispatch behavior using dataset dispatch_time values.
"""
import polars as pl
import logging
from typing import Dict, List, Any
from datetime import datetime

from .base_orchestrator import BaseOrchestrator
from ..state import (
    initialize_courier_states,
    get_available_couriers,
    get_courier_state_summary
)

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class BatchOrchestrator(BaseOrchestrator):
    """
    Batch mode orchestrator that processes orders in waves at dispatch_time checkpoints.

    This orchestrator:
    - Uses dispatch_time from the dataset as batch boundaries
    - Processes all orders waiting at each dispatch_time
    - Maintains backlog across batches
    - Logs cycle-level metrics
    """

    def get_mode_name(self) -> str:
        """Return mode name for logging."""
        return "BATCH"

    def run_simulation(self, data_paths: Dict[str, str], model_name: str) -> Dict[str, Any]:
        """
        Run batch mode simulation processing orders at dispatch_time checkpoints.

        Args:
            data_paths: Dictionary with paths to:
                - waybill_path: all_waybill_info_meituan_0322.csv
                - dispatch_waybill_path: dispatch_waybill_meituan.csv
                - dispatch_rider_path: dispatch_rider_meituan.csv
            model_name: Name for logging/output files

        Returns:
            Dictionary with simulation metrics and results
        """
        logger.info(f"Starting BATCH mode simulation for model: {model_name}")

        # Load data
        logger.info("Loading data files...")
        waybill = pl.read_csv(data_paths['waybill_path'])
        dispatch_waybill = pl.read_csv(data_paths['dispatch_waybill_path'])
        dispatch_rider = pl.read_csv(data_paths['dispatch_rider_path'])

        # Build lookup for order details
        logger.info("Building waybill lookup...")
        waybill_lookup = {}
        for row in waybill.iter_rows(named=True):
            waybill_lookup[row['order_id']] = row

        # Get unique dispatch times (batch checkpoints)
        dispatch_times = sorted(dispatch_waybill['dispatch_time'].unique().to_list())
        logger.info(f"Found {len(dispatch_times)} dispatch time checkpoints")

        # Initialize courier states from first dispatch
        first_dispatch_time = dispatch_times[0]
        first_dispatch_couriers = dispatch_rider.filter(
            pl.col('dispatch_time') == first_dispatch_time
        ).to_dicts()

        courier_states = initialize_courier_states(
            first_dispatch_couriers, waybill_lookup, self.timeline_logger
        )
        logger.info(f"Initialized {len(courier_states)} couriers at time {first_dispatch_time}")

        # Build actual assignments lookup for comparison
        actual_assignments = self._build_actual_assignments(waybill)

        # Simulation metrics
        total_orders = 0
        total_assigned = 0
        total_rejected = 0
        total_deferred = 0

        # Process each batch/wave
        for batch_idx, dispatch_time in enumerate(dispatch_times):
            logger.info(f"\n{'='*60}")
            logger.info(f"Processing batch {batch_idx + 1}/{len(dispatch_times)} at time {dispatch_time}")

            # Get orders waiting at this dispatch time
            waiting_orders = dispatch_waybill.filter(
                pl.col('dispatch_time') == dispatch_time
            ).to_dicts()

            # Get available couriers
            dispatch_couriers = dispatch_rider.filter(
                pl.col('dispatch_time') == dispatch_time
            ).to_dicts()

            # Update courier states with new couriers
            self._update_courier_pool(courier_states, dispatch_couriers, dispatch_time)

            available_couriers = get_available_couriers(
                dispatch_time, courier_states, self.timeline_logger
            )

            logger.info(f"Batch has {len(waiting_orders)} orders, "
                       f"{len(available_couriers)} available couriers")

            # Clear candidate cache for new wave (if shared candidates enabled)
            self.clear_candidate_cache()

            # Run dispatch pipeline
            metrics = self.dispatch_pipeline(
                waiting_orders,
                available_couriers,
                dispatch_time,
                waybill_lookup,
                actual_assignments,
                courier_states
            )

            # Log batch metrics
            self._log_batch_metrics(
                dispatch_time,
                batch_idx,
                metrics,
                courier_states,
                waybill_lookup,
                actual_assignments
            )

            # Update totals
            total_orders += metrics['num_waiting_orders']
            total_assigned += len(metrics['accepted_assignments'])
            total_rejected += len(metrics['rejected_assignments'])
            total_deferred = metrics['num_backlog']

            # Flush logs after each batch
            self.simulation_logger.flush()

        # Final summary
        logger.info(f"\n{'='*60}")
        logger.info("BATCH SIMULATION COMPLETE")
        logger.info(f"Total orders: {total_orders}")
        logger.info(f"Total assigned: {total_assigned} ({total_assigned/total_orders*100:.1f}%)")
        logger.info(f"Total rejected: {total_rejected} ({total_rejected/total_orders*100:.1f}%)")
        logger.info(f"Final backlog: {total_deferred}")

        # Close loggers
        self.simulation_logger.close()
        self.timeline_logger.close()

        return {
            'mode': 'BATCH',
            'total_orders': total_orders,
            'total_assigned': total_assigned,
            'total_rejected': total_rejected,
            'final_backlog': total_deferred,
            'assignment_rate': total_assigned / total_orders if total_orders > 0 else 0,
            'num_batches': len(dispatch_times)
        }

    def _update_courier_pool(
        self,
        courier_states: Dict,
        dispatch_couriers: List[Dict],
        dispatch_time: int
    ):
        """
        Update courier pool with new couriers appearing at this dispatch time.

        Args:
            courier_states: Current courier states dictionary
            dispatch_couriers: Couriers at this dispatch time
            dispatch_time: Current time
        """
        for courier in dispatch_couriers:
            courier_id = courier['courier_id']
            if courier_id not in courier_states:
                # New courier entering the system
                courier_states[courier_id] = {
                    'status': 'AVAILABLE',
                    'becomes_available_at': dispatch_time,
                    'lat': courier['rider_lat'],
                    'lng': courier['rider_lng']
                }
                self.timeline_logger.log_state_transition(
                    dispatch_time, courier_id, 'AVAILABLE', 'new_courier'
                )
            else:
                # Update existing courier location if available
                if courier_states[courier_id]['status'] == 'AVAILABLE':
                    courier_states[courier_id]['lat'] = courier['rider_lat']
                    courier_states[courier_id]['lng'] = courier['rider_lng']

    def _build_actual_assignments(self, waybill: pl.DataFrame) -> Dict[str, str]:
        """
        Build lookup of actual historical assignments.

        Args:
            waybill: Waybill dataframe

        Returns:
            Dictionary mapping order_id -> actual_courier_id
        """
        actual_assignments = {}

        grabbed_orders = waybill.filter(pl.col('is_courier_grabbed') == 1)
        for row in grabbed_orders.iter_rows(named=True):
            actual_assignments[row['order_id']] = row['courier_id']

        logger.info(f"Built actual assignments for {len(actual_assignments)} orders")
        return actual_assignments

    def _log_batch_metrics(
        self,
        dispatch_time: int,
        batch_idx: int,
        metrics: Dict,
        courier_states: Dict,
        waybill_lookup: Dict,
        actual_assignments: Dict
    ):
        """
        Log metrics for this batch/cycle.

        Args:
            dispatch_time: Current dispatch time
            batch_idx: Batch index
            metrics: Metrics from dispatch pipeline
            courier_states: Current courier states
            waybill_lookup: Order details lookup
            actual_assignments: Historical assignments
        """
        # Log individual assignments
        for order, courier, cost in metrics['accepted_assignments']:
            order_details = waybill_lookup[order['order_id']]

            # Check if this matches actual assignment
            actual_courier = actual_assignments.get(order['order_id'])
            is_match = (actual_courier == courier['courier_id'])

            # Calculate wait time
            wait_time = dispatch_time - order_details['platform_order_time']

            self.simulation_logger.log_assignment(
                dispatch_time=dispatch_time,
                order_id=order['order_id'],
                baseline_assigned_courier_id=courier['courier_id'],
                baseline_cost=cost,
                baseline_courier_rank_by_cost=1,  # TODO: Calculate rank
                is_assigned_by_baseline=True,
                was_accepted=True,
                actual_assigned_courier_id=actual_courier,
                is_match_with_actual=is_match,
                num_orders_in_batch=metrics['num_waiting_orders'],
                num_couriers_in_pool=metrics['num_available_couriers'],
                order_pickup_lat=order_details['sender_lat'],
                order_pickup_lng=order_details['sender_lng'],
                platform_order_time=order_details['platform_order_time'],
                wait_for_assignment_seconds=wait_time,
                cost_function=self.cost_function.get_name(),
                baseline_courier_lat=courier['lat'],
                baseline_courier_lng=courier['lng'],
                actual_courier_lat=0,  # TODO: Get actual courier location
                actual_courier_lng=0,
                mode=self.get_mode_name(),
                strategy_key=self.assignment_strategy.get_name(),
                bundling_on=1 if self.bundling_service else 0,
                micro_batch_sec=0,  # Not applicable for batch mode
                unit_type='order',  # TODO: Handle bundles
                bundle_size=1  # TODO: Handle bundles
            )

        # Log rejected assignments
        for order, courier, cost in metrics['rejected_assignments']:
            order_details = waybill_lookup[order['order_id']]
            actual_courier = actual_assignments.get(order['order_id'])
            wait_time = dispatch_time - order_details['platform_order_time']

            self.simulation_logger.log_assignment(
                dispatch_time=dispatch_time,
                order_id=order['order_id'],
                baseline_assigned_courier_id=courier['courier_id'],
                baseline_cost=cost,
                baseline_courier_rank_by_cost=1,
                is_assigned_by_baseline=True,
                was_accepted=False,  # Rejected
                actual_assigned_courier_id=actual_courier,
                is_match_with_actual=False,
                num_orders_in_batch=metrics['num_waiting_orders'],
                num_couriers_in_pool=metrics['num_available_couriers'],
                order_pickup_lat=order_details['sender_lat'],
                order_pickup_lng=order_details['sender_lng'],
                platform_order_time=order_details['platform_order_time'],
                wait_for_assignment_seconds=wait_time,
                cost_function=self.cost_function.get_name(),
                baseline_courier_lat=courier['lat'],
                baseline_courier_lng=courier['lng'],
                actual_courier_lat=0,
                actual_courier_lng=0,
                mode=self.get_mode_name(),
                strategy_key=self.assignment_strategy.get_name(),
                bundling_on=1 if self.bundling_service else 0,
                micro_batch_sec=0,
                unit_type='order',
                bundle_size=1
            )

        # Calculate cycle-level metrics
        num_orders = metrics['num_waiting_orders']
        num_couriers = metrics['num_available_couriers']
        supply_demand_ratio = num_couriers / num_orders if num_orders > 0 else float('inf')

        assignment_rate = len(metrics['accepted_assignments']) / num_orders if num_orders > 0 else 0
        acceptance_rate = (len(metrics['accepted_assignments']) /
                          metrics['num_proposed_assignments']
                          if metrics['num_proposed_assignments'] > 0 else 0)

        # Calculate total cost
        total_cost = sum(cost for _, _, cost in metrics['accepted_assignments'])
        avg_cost = total_cost / len(metrics['accepted_assignments']) if metrics['accepted_assignments'] else 0

        # Calculate agreement with actual
        agreement_rate = self.calculate_agreement_with_actual(
            metrics['accepted_assignments'],
            actual_assignments
        )

        # Log cycle summary
        self.simulation_logger.log_cycle_summary(
            dispatch_time=dispatch_time,
            num_orders_in_batch=num_orders,
            num_available_couriers=num_couriers,
            supply_demand_ratio=supply_demand_ratio,
            num_proposed_assignments=metrics['num_proposed_assignments'],
            num_accepted_assignments=len(metrics['accepted_assignments']),
            num_rejections=len(metrics['rejected_assignments']),
            assignment_rate=assignment_rate,
            acceptance_rate=acceptance_rate,
            total_cost_of_cycle=total_cost,
            avg_cost_per_assignment=avg_cost,
            agreement_rate_with_actual=agreement_rate,
            cost_function=self.cost_function.get_name(),
            mode=self.get_mode_name(),
            strategy_key=self.assignment_strategy.get_name(),
            num_units=len(metrics.get('assignable_units', [])),
            num_bundles=len(metrics.get('bundle_mapping', {})),
            avg_bundle_size=0,  # TODO: Calculate
            micro_batch_sec=0,
            optimizer_status='ok',
            num_deferred_in=0,  # TODO: Track properly
            num_deferred_out=metrics['num_backlog'],
            num_deferred_carry=metrics['num_backlog'],
            decision_ms=metrics.get('decision_ms', 0.0),
            optimizer_ms=metrics.get('optimizer_ms', 0.0)
        )

        # Log courier utilization
        state_summary = get_courier_state_summary(courier_states, dispatch_time)
        logger.info(f"Courier utilization: {state_summary}")

        logger.info(f"Batch {batch_idx + 1} complete: "
                   f"{len(metrics['accepted_assignments'])} assigned, "
                   f"{len(metrics['rejected_assignments'])} rejected, "
                   f"{metrics['num_backlog']} in backlog")