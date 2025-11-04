"""
Real-time mode orchestrator that processes orders at platform_order_time with micro-batches.
Bootstraps from first dispatch snapshot then simulates forward.
"""
import polars as pl
import logging
from typing import Dict, List, Any, Tuple
from datetime import datetime
import heapq

from .base_orchestrator import BaseOrchestrator
from ..state import (
    initialize_courier_states,
    get_available_couriers,
    get_courier_state_summary,
    update_courier_after_assignment
)
from ..physics import euclidean_distance

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class RealtimeOrchestrator(BaseOrchestrator):
    """
    Real-time mode orchestrator that processes orders as they arrive at platform_order_time.

    This orchestrator:
    - Bootstraps from the first dispatch_time snapshot
    - Processes orders in micro-batches (default 10 seconds)
    - Simulates courier movement forward based on our assignments
    - Spawns new couriers when they appear in dispatch snapshots
    """

    def __init__(self, *args, micro_batch_sec: int = 10, **kwargs):
        """
        Initialize real-time orchestrator.

        Args:
            micro_batch_sec: Size of micro-batch window in seconds (default 10)
            *args, **kwargs: Passed to BaseOrchestrator
        """
        super().__init__(*args, **kwargs)
        self.micro_batch_sec = micro_batch_sec
        self.courier_routes = {}  # Track planned routes for each courier

    def get_mode_name(self) -> str:
        """Return mode name for logging."""
        return "REALTIME"

    def run_simulation(self, data_paths: Dict[str, str], model_name: str) -> Dict[str, Any]:
        """
        Run real-time mode simulation processing orders at platform_order_time.

        Args:
            data_paths: Dictionary with paths to data files
            model_name: Name for logging/output files

        Returns:
            Dictionary with simulation metrics and results
        """
        logger.info(f"Starting REALTIME mode simulation for model: {model_name}")
        logger.info(f"Micro-batch window: {self.micro_batch_sec} seconds")

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

        # Get dispatch times for courier spawning
        dispatch_times = sorted(dispatch_rider['dispatch_time'].unique().to_list())

        # Bootstrap: Initialize from first dispatch snapshot
        first_dispatch_time = dispatch_times[0]
        first_dispatch_couriers = dispatch_rider.filter(
            pl.col('dispatch_time') == first_dispatch_time
        ).to_dicts()

        courier_states = initialize_courier_states(
            first_dispatch_couriers, first_dispatch_time, self.timeline_logger
        )
        logger.info(f"Bootstrapped {len(courier_states)} couriers at time {first_dispatch_time}")

        # Build actual assignments for comparison
        actual_assignments = self._build_actual_assignments(waybill)

        # Create order arrival queue (priority queue by platform_order_time)
        order_queue = self._build_order_queue(dispatch_waybill, waybill_lookup)
        logger.info(f"Built order queue with {len(order_queue)} orders")

        # Determine simulation time range
        sim_start_time = first_dispatch_time
        sim_end_time = dispatch_times[-1] + 3600  # Run 1 hour past last dispatch

        # Simulation metrics
        total_orders = 0
        total_assigned = 0
        total_rejected = 0
        total_deferred = 0

        # Run micro-batch event loop
        current_time = sim_start_time
        next_dispatch_idx = 1  # Index for courier spawning

        while current_time <= sim_end_time and order_queue:
            micro_batch_end = current_time + self.micro_batch_sec

            # Collect orders arriving in this micro-batch window
            arriving_orders = []
            while order_queue and order_queue[0][0] <= micro_batch_end:
                _, _, order = heapq.heappop(order_queue)
                arriving_orders.append(order)

            if not arriving_orders and not self.backlog:
                # No orders to process, advance time
                current_time = micro_batch_end
                continue

            logger.info(f"\n{'='*60}")
            logger.info(f"Micro-batch at time {current_time} - {micro_batch_end}")
            logger.info(f"Processing {len(arriving_orders)} new orders + {len(self.backlog)} backlog")

            # Spawn new couriers if we've reached a dispatch checkpoint
            while (next_dispatch_idx < len(dispatch_times) and
                   dispatch_times[next_dispatch_idx] <= current_time):
                spawn_time = dispatch_times[next_dispatch_idx]
                new_couriers = dispatch_rider.filter(
                    pl.col('dispatch_time') == spawn_time
                ).to_dicts()
                self._spawn_couriers(courier_states, new_couriers, spawn_time)
                logger.info(f"Spawned couriers from dispatch time {spawn_time}")
                next_dispatch_idx += 1

            # Evolve courier positions based on their routes
            self._evolve_courier_positions(courier_states, current_time)

            # Get available couriers
            available_couriers = get_available_couriers(
                current_time, courier_states, self.timeline_logger
            )

            # Run dispatch pipeline
            metrics = self.dispatch_pipeline(
                arriving_orders,
                available_couriers,
                current_time,
                waybill_lookup,
                actual_assignments,
                courier_states
            )

            # Update courier routes for accepted assignments
            self._update_courier_routes(metrics['accepted_assignments'], current_time, waybill_lookup, courier_states)

            # Log micro-batch metrics
            self._log_microbatch_metrics(
                current_time,
                micro_batch_end,
                metrics,
                courier_states,
                waybill_lookup,
                actual_assignments
            )

            # Update totals
            total_orders += len(arriving_orders)
            total_assigned += len(metrics['accepted_assignments'])
            total_rejected += len(metrics['rejected_assignments'])
            total_deferred = metrics['num_backlog']

            # Flush logs
            self.simulation_logger.flush()

            # Advance time
            current_time = micro_batch_end

        # Process remaining backlog
        if self.backlog:
            logger.warning(f"Simulation ended with {len(self.backlog)} orders in backlog")

        # Final summary
        logger.info(f"\n{'='*60}")
        logger.info("REALTIME SIMULATION COMPLETE")
        logger.info(f"Total orders: {total_orders}")
        logger.info(f"Total assigned: {total_assigned} ({total_assigned/total_orders*100:.1f}%)")
        logger.info(f"Total rejected: {total_rejected} ({total_rejected/total_orders*100:.1f}%)")
        logger.info(f"Final backlog: {total_deferred}")

        # Close loggers
        self.simulation_logger.close()
        self.timeline_logger.close()

        return {
            'mode': 'REALTIME',
            'total_orders': total_orders,
            'total_assigned': total_assigned,
            'total_rejected': total_rejected,
            'final_backlog': total_deferred,
            'assignment_rate': total_assigned / total_orders if total_orders > 0 else 0,
            'micro_batch_sec': self.micro_batch_sec
        }

    def _build_order_queue(
        self,
        dispatch_waybill: pl.DataFrame,
        waybill_lookup: Dict
    ) -> List[Tuple[int, Dict]]:
        """
        Build priority queue of orders sorted by platform_order_time.

        Args:
            dispatch_waybill: Dispatch waybill dataframe
            waybill_lookup: Order details lookup

        Returns:
            Priority queue (min-heap) of (platform_order_time, order) tuples
        """
        order_queue = []
        order_idx = 0  # Tie-breaker for equal timestamps

        for order in dispatch_waybill.to_dicts():
            order_id = order['order_id']
            if order_id in waybill_lookup:
                platform_time = waybill_lookup[order_id]['platform_order_time']
                heapq.heappush(order_queue, (platform_time, order_idx, order))
                order_idx += 1

        return order_queue

    def _spawn_couriers(
        self,
        courier_states: Dict,
        new_couriers: List[Dict],
        spawn_time: int
    ):
        """
        Add new couriers to the simulation when they appear in dispatch snapshots.

        Args:
            courier_states: Current courier states
            new_couriers: New couriers from dispatch snapshot
            spawn_time: Time when couriers appear
        """
        for courier in new_couriers:
            courier_id = courier['courier_id']
            if courier_id not in courier_states:
                # New courier entering system
                courier_states[courier_id] = {
                    'status': 'AVAILABLE',
                    'becomes_available_at': spawn_time,
                    'lat': courier['rider_lat'],
                    'lng': courier['rider_lng']
                }
                self.courier_routes[courier_id] = []  # No planned route yet
                self.timeline_logger.log_state_transition(
                    spawn_time, courier_id, 'AVAILABLE', 'spawned'
                )

    def _evolve_courier_positions(self, courier_states: Dict, current_time: int):
        """
        Update courier positions based on their planned routes.

        For couriers with routes, interpolate position along current leg.
        For idle couriers, position remains fixed.

        Args:
            courier_states: Courier states to update
            current_time: Current simulation time
        """
        for courier_id, state in courier_states.items():
            if courier_id not in self.courier_routes:
                continue

            route = self.courier_routes[courier_id]
            if not route:
                # No route planned, courier stays at current position
                continue

            # Find current leg in route
            for i, (leg_type, lat, lng, start_time, end_time) in enumerate(route):
                if start_time <= current_time <= end_time:
                    # Courier is on this leg, interpolate position
                    if start_time == end_time:
                        # Instant arrival (shouldn't happen)
                        state['lat'] = lat
                        state['lng'] = lng
                    else:
                        # Linear interpolation
                        progress = (current_time - start_time) / (end_time - start_time)

                        if i == 0:
                            # First leg, interpolate from current position
                            start_lat = state['lat']
                            start_lng = state['lng']
                        else:
                            # Interpolate from previous leg endpoint
                            _, start_lat, start_lng, _, _ = route[i-1]

                        state['lat'] = start_lat + progress * (lat - start_lat)
                        state['lng'] = start_lng + progress * (lng - start_lng)
                    break
                elif current_time > end_time and i == len(route) - 1:
                    # Past last leg, courier is at final destination
                    state['lat'] = lat
                    state['lng'] = lng

            # Clean up completed route segments
            self.courier_routes[courier_id] = [
                leg for leg in route if leg[4] > current_time
            ]

    def _update_courier_routes(
        self,
        accepted_assignments: List[Tuple],
        current_time: int,
        waybill_lookup: Dict,
        courier_states: Dict
    ):
        """
        Update planned routes for couriers with new assignments.

        Args:
            accepted_assignments: List of (order, courier, cost) tuples
            current_time: Current simulation time
            waybill_lookup: Order details lookup
        """
        # Group assignments by courier
        courier_assignments = {}
        for order, courier, cost in accepted_assignments:
            courier_id = courier['courier_id']
            if courier_id not in courier_assignments:
                courier_assignments[courier_id] = []
            courier_assignments[courier_id].append(order)

        # Plan routes for each courier
        for courier_id, orders in courier_assignments.items():
            state = next((s for cid, s in courier_states.items() if cid == courier_id), None)
            if not state:
                continue

            # Start from courier's current position
            current_lat = state['lat']
            current_lng = state['lng']
            route_time = current_time

            route = []

            # Plan pickup and delivery for each order
            for order in orders:
                order_details = waybill_lookup[order['order_id']]

                # Leg 1: Current position to pickup
                pickup_lat = order_details['sender_lat']
                pickup_lng = order_details['sender_lng']
                pickup_distance = euclidean_distance(
                    current_lat, current_lng, pickup_lat, pickup_lng
                )
                pickup_duration = int(pickup_distance * 100)  # Simple time model
                pickup_arrival = route_time + pickup_duration

                route.append(('PICKUP', pickup_lat, pickup_lng, route_time, pickup_arrival))

                # Leg 2: Pickup to delivery
                delivery_lat = order_details['recipient_lat']
                delivery_lng = order_details['recipient_lng']
                delivery_distance = euclidean_distance(
                    pickup_lat, pickup_lng, delivery_lat, delivery_lng
                )
                delivery_duration = int(delivery_distance * 100)
                delivery_arrival = pickup_arrival + delivery_duration

                route.append(('DELIVERY', delivery_lat, delivery_lng, pickup_arrival, delivery_arrival))

                # Update position for next order
                current_lat = delivery_lat
                current_lng = delivery_lng
                route_time = delivery_arrival

            self.courier_routes[courier_id] = route

    def _build_actual_assignments(self, waybill: pl.DataFrame) -> Dict[str, str]:
        """Build lookup of actual historical assignments."""
        actual_assignments = {}
        grabbed_orders = waybill.filter(pl.col('is_courier_grabbed') == 1)
        for row in grabbed_orders.iter_rows(named=True):
            actual_assignments[row['order_id']] = row['courier_id']
        return actual_assignments

    def _log_microbatch_metrics(
        self,
        micro_batch_start: int,
        micro_batch_end: int,
        metrics: Dict,
        courier_states: Dict,
        waybill_lookup: Dict,
        actual_assignments: Dict
    ):
        """Log metrics for this micro-batch."""
        # Log individual assignments (similar to batch, but with micro_batch_sec)
        for order, courier, cost in metrics['accepted_assignments']:
            order_details = waybill_lookup[order['order_id']]
            actual_courier = actual_assignments.get(order['order_id'])
            is_match = (actual_courier == courier['courier_id'])
            wait_time = micro_batch_start - order_details['platform_order_time']

            self.simulation_logger.log_assignment(
                dispatch_time=micro_batch_start,
                order_id=order['order_id'],
                baseline_assigned_courier_id=courier['courier_id'],
                baseline_cost=cost,
                baseline_courier_rank_by_cost=1,
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
                actual_courier_lat=0,
                actual_courier_lng=0,
                mode=self.get_mode_name(),
                strategy_key=self.assignment_strategy.get_name(),
                bundling_on=1 if self.bundling_service else 0,
                micro_batch_sec=self.micro_batch_sec,
                unit_type='order',
                bundle_size=1
            )

        # Log cycle summary with micro-batch info
        num_orders = metrics['num_waiting_orders']
        num_couriers = metrics['num_available_couriers']
        supply_demand_ratio = num_couriers / num_orders if num_orders > 0 else float('inf')
        assignment_rate = len(metrics['accepted_assignments']) / num_orders if num_orders > 0 else 0
        acceptance_rate = (len(metrics['accepted_assignments']) /
                          metrics['num_proposed_assignments']
                          if metrics['num_proposed_assignments'] > 0 else 0)
        total_cost = sum(cost for _, _, cost in metrics['accepted_assignments'])
        avg_cost = total_cost / len(metrics['accepted_assignments']) if metrics['accepted_assignments'] else 0
        agreement_rate = self.calculate_agreement_with_actual(
            metrics['accepted_assignments'],
            actual_assignments
        )

        self.simulation_logger.log_cycle_summary(
            dispatch_time=micro_batch_start,
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
            num_bundles=0,
            avg_bundle_size=1,
            micro_batch_sec=self.micro_batch_sec,
            optimizer_status='ok',
            num_deferred_in=0,
            num_deferred_out=metrics['num_backlog'],
            num_deferred_carry=metrics['num_backlog'],
            decision_ms=metrics.get('decision_ms', 0.0),
            optimizer_ms=metrics.get('optimizer_ms', 0.0)
        )

        logger.info(f"Micro-batch complete: "
                   f"{len(metrics['accepted_assignments'])} assigned, "
                   f"{len(metrics['rejected_assignments'])} rejected, "
                   f"{metrics['num_backlog']} in backlog")