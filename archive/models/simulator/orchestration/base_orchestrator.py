"""
Base orchestrator defining the shared dispatch pipeline for both batch and real-time modes.
"""
from abc import ABC, abstractmethod
from typing import List, Dict, Tuple, Optional, Any
import random
import logging
import time

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class BaseOrchestrator(ABC):
    """
    Abstract base class for dispatch orchestrators.
    Defines the shared pipeline that both batch and real-time modes use.
    """

    def __init__(
        self,
        assignment_strategy,
        cost_function,
        simulation_logger,
        timeline_logger,
        physics,
        bundling_service=None,
        candidate_generator=None,
        shared_candidates=False
    ):
        """
        Initialize the orchestrator with required services.

        Args:
            assignment_strategy: Strategy for making assignments (Greedy, Hungarian, etc.)
            cost_function: Cost function for scoring assignments
            simulation_logger: Logger for assignment and cycle metrics
            timeline_logger: Logger for courier state transitions
            physics: Physics constants (task duration, rejection probability)
            bundling_service: Optional service for creating order bundles
            candidate_generator: Optional service for sparse pairing
            shared_candidates: If True, generate candidates once per wave and reuse
        """
        self.assignment_strategy = assignment_strategy
        self.cost_function = cost_function
        self.simulation_logger = simulation_logger
        self.timeline_logger = timeline_logger
        self.physics = physics
        self.bundling_service = bundling_service
        self.candidate_generator = candidate_generator
        self.shared_candidates = shared_candidates

        # Backlog for deferred/unassigned orders
        self.backlog = []

        # Cache for shared candidates (when shared_candidates=True)
        self.cached_candidates = None
        self.cached_candidates_hash = None

    @abstractmethod
    def run_simulation(self, data_paths: Dict[str, str], model_name: str) -> Dict[str, Any]:
        """
        Run the complete simulation for this orchestrator mode.

        Args:
            data_paths: Dictionary with paths to data files
            model_name: Name for logging/output files

        Returns:
            Dictionary with simulation metrics and results
        """
        pass

    @abstractmethod
    def get_mode_name(self) -> str:
        """Return the mode name (BATCH or REALTIME) for logging."""
        pass

    def dispatch_pipeline(
        self,
        waiting_orders: List[Dict],
        available_couriers: List[Dict],
        current_time: int,
        waybill_lookup: Dict,
        actual_assignments: Dict,
        courier_states: Dict
    ) -> Dict[str, Any]:
        """
        Shared dispatch pipeline used by both batch and real-time modes.

        This is the core logic that:
        1. Adds backlog orders to waiting orders
        2. Optionally generates candidates (sparse pairing)
        3. Optionally creates bundles
        4. Makes assignments via the strategy
        5. Processes rejections
        6. Updates backlog
        7. Logs metrics

        Args:
            waiting_orders: Orders to assign in this cycle
            available_couriers: Couriers available for assignment
            current_time: Current simulation time
            waybill_lookup: Full order details lookup
            actual_assignments: Historical assignments for comparison

        Returns:
            Dictionary with assignment results and metrics
        """
        # Start timing the entire dispatch pipeline
        pipeline_start = time.perf_counter()

        # 1. Prepend backlog orders (oldest first)
        if self.backlog:
            logger.info(f"Adding {len(self.backlog)} backlog orders to {len(waiting_orders)} new orders")
            # Sort backlog by platform_order_time (oldest first)
            self.backlog.sort(key=lambda o: waybill_lookup[o['order_id']]['platform_order_time'])
            waiting_orders = self.backlog + waiting_orders
            self.backlog = []  # Clear backlog (will re-add unassigned)

        # 2. Generate candidates (sparse pairing) if service available
        candidates = None
        if self.candidate_generator:
            if self.shared_candidates and self.cached_candidates is not None:
                # Use cached candidates if available
                candidates = self.cached_candidates
                logger.debug(f"Using cached {len(candidates)} candidate pairs (hash: {self.cached_candidates_hash})")
            else:
                # Generate new candidates
                candidates = self.candidate_generator.generate(
                    waiting_orders, available_couriers, waybill_lookup
                )
                logger.debug(f"Generated {len(candidates)} candidate pairs")

                # Cache if shared_candidates is enabled
                if self.shared_candidates:
                    self.cached_candidates = candidates
                    # Compute a simple hash of the candidates for verification
                    if candidates:
                        self.cached_candidates_hash = hash((len(candidates),
                                                           len(waiting_orders),
                                                           len(available_couriers)))
                    logger.info(f"Cached candidate graph with {len(candidates)} pairs (hash: {self.cached_candidates_hash})")

        # 3. Create bundles if bundling service available
        assignable_units = waiting_orders  # Default: orders are units
        bundle_mapping = {}  # Maps unit_id -> list of order_ids

        if self.bundling_service:
            assignable_units, bundle_mapping = self.bundling_service.create_bundles(
                waiting_orders, waybill_lookup
            )
            logger.info(f"Created {len(assignable_units)} units from {len(waiting_orders)} orders")

        # 4. Make assignments via strategy
        optimizer_start = time.perf_counter()
        proposed_assignments = self.assignment_strategy.make_assignments(
            assignable_units,
            available_couriers,
            waybill_lookup,
            candidates=candidates,
            bundle_mapping=bundle_mapping
        )
        optimizer_ms = (time.perf_counter() - optimizer_start) * 1000

        logger.info(f"Strategy proposed {len(proposed_assignments)} assignments in {optimizer_ms:.2f}ms")

        # 5. Process rejections
        accepted_assignments = []
        rejected_assignments = []
        assigned_order_ids = set()

        for unit, courier, cost in proposed_assignments:
            # Get actual orders (unit might be a bundle)
            if self.bundling_service and unit['order_id'] in bundle_mapping:
                orders_in_unit = bundle_mapping[unit['order_id']]
            else:
                orders_in_unit = [unit]

            # Process rejection for each order
            unit_accepted = []
            unit_rejected = []

            for order in orders_in_unit:
                if random.random() < self.physics.GLOBAL_REJECTION_PROBABILITY:
                    unit_rejected.append(order)
                    rejected_assignments.append((order, courier, cost))
                else:
                    unit_accepted.append(order)
                    accepted_assignments.append((order, courier, cost))
                    assigned_order_ids.add(order['order_id'])

            # Update courier state if at least one order accepted
            if unit_accepted:
                # Calculate task duration based on accepted orders
                num_accepted = len(unit_accepted)
                # For bundles, scale task duration (capped at 5x)
                task_duration = self.physics.AVERAGE_TASK_DURATION * min(num_accepted, 5)

                # Get delivery location (last order's delivery for bundles)
                last_order = unit_accepted[-1]
                order_details = waybill_lookup[last_order['order_id']]
                delivery_lat = order_details['recipient_lat']
                delivery_lng = order_details['recipient_lng']

                # Update courier state
                from ..state import update_courier_after_assignment
                update_courier_after_assignment(
                    courier['courier_id'],
                    courier_states,
                    current_time,
                    (delivery_lat, delivery_lng),
                    task_duration,
                    self.timeline_logger
                )

        # 6. Update backlog with unassigned orders
        for order in waiting_orders:
            if order['order_id'] not in assigned_order_ids:
                self.backlog.append(order)

        logger.info(f"Accepted: {len(accepted_assignments)}, "
                   f"Rejected: {len(rejected_assignments)}, "
                   f"Backlog: {len(self.backlog)}")

        # Calculate total decision time (entire pipeline)
        decision_ms = (time.perf_counter() - pipeline_start) * 1000

        # 7. Log metrics (delegated to concrete orchestrator)
        metrics = {
            'num_waiting_orders': len(waiting_orders),
            'num_available_couriers': len(available_couriers),
            'num_proposed_assignments': len(proposed_assignments),
            'num_accepted_assignments': len(accepted_assignments),
            'num_rejected_assignments': len(rejected_assignments),
            'num_backlog': len(self.backlog),
            'accepted_assignments': accepted_assignments,
            'rejected_assignments': rejected_assignments,
            'assignable_units': assignable_units,
            'bundle_mapping': bundle_mapping,
            'decision_ms': decision_ms,
            'optimizer_ms': optimizer_ms
        }

        return metrics

    def clear_candidate_cache(self):
        """Clear the cached candidates (used between dispatch waves when shared_candidates=True)"""
        self.cached_candidates = None
        self.cached_candidates_hash = None
        if self.shared_candidates:
            logger.debug("Cleared candidate cache for next wave")

    def calculate_agreement_with_actual(
        self,
        accepted_assignments: List[Tuple],
        actual_assignments: Dict[str, str]
    ) -> float:
        """
        Calculate agreement rate between our assignments and historical data.

        Args:
            accepted_assignments: List of (order, courier, cost) tuples
            actual_assignments: Dict mapping order_id -> actual_courier_id

        Returns:
            Agreement rate (0.0 to 1.0)
        """
        if not accepted_assignments:
            return 0.0

        matches = 0
        for order, courier, _ in accepted_assignments:
            order_id = order['order_id']
            if order_id in actual_assignments:
                if actual_assignments[order_id] == courier['courier_id']:
                    matches += 1

        return matches / len(accepted_assignments)