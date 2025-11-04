"""
Mixed Integer Linear Programming (MILP) strategy for order-courier assignment.
Uses OR-Tools CP-SAT solver to find optimal assignments with capacity constraints.
"""
from typing import List, Dict, Tuple, Optional, Any
import logging

# OR-Tools import (user must install: pip install ortools)
try:
    from ortools.sat.python import cp_model
except ImportError:
    raise ImportError(
        "OR-Tools is required for MILP strategy. "
        "Install with: pip3 install --break-system-packages ortools"
    )

from . import BaseStrategy

logger = logging.getLogger(__name__)


class MILPStrategy(BaseStrategy):
    """
    MILP strategy using OR-Tools CP-SAT solver.

    Formulation:
    - Minimize total distance to pickup
    - Each order assigned to at most one courier
    - Each courier can handle multiple orders (capacity constraint)
    - Only use candidate pairs from sparse graph (if provided)

    This strategy internally manages its own size limits since MILP
    cannot handle arbitrarily large instances.
    """

    def __init__(self, cost_function,
                 time_limit_sec: int = 5,
                 max_orders_cap: int = 150,
                 max_candidates_per_order: int = 50,
                 max_orders_per_courier: int = 5):
        """
        Initialize MILP strategy.

        Args:
            cost_function: Cost function for scoring assignments
            time_limit_sec: Time limit for solver (default 5 seconds)
            max_orders_cap: Max orders to process in one MILP instance (default 150)
            max_candidates_per_order: Max candidate couriers per order (default 50)
            max_orders_per_courier: Max orders per courier (default 5)
        """
        super().__init__(cost_function)
        self.time_limit_sec = time_limit_sec
        self.max_orders_cap = max_orders_cap
        self.max_candidates_per_order = max_candidates_per_order
        self.max_orders_per_courier = max_orders_per_courier

    def get_name(self) -> str:
        """Return strategy name for logging."""
        return "milp_vrp"

    def make_assignments(
        self,
        waiting_orders: List[Dict],
        available_couriers: List[Dict],
        waybill_lookup: Dict,
        candidates: Optional[List[Tuple]] = None,
        bundle_mapping: Optional[Dict] = None
    ) -> List[Tuple[Dict, Dict, float]]:
        """
        Make assignments using MILP optimization.

        This method internally caps the problem size to ensure tractability.

        Args:
            waiting_orders: Orders/bundles to assign
            available_couriers: Available couriers
            waybill_lookup: Full order details
            candidates: Optional pre-computed candidates
            bundle_mapping: Optional bundle mapping

        Returns:
            List of (order, courier, cost) tuples
        """
        if not waiting_orders or not available_couriers:
            return []

        # MILP can't handle huge instances - cap orders internally
        original_order_count = len(waiting_orders)
        if len(waiting_orders) > self.max_orders_cap:
            # Sort by platform_order_time to prioritize oldest orders (fairness)
            waiting_orders_sorted = sorted(
                waiting_orders,
                key=lambda o: waybill_lookup.get(o['order_id'], {}).get('platform_order_time', 0)
            )
            working_orders = waiting_orders_sorted[:self.max_orders_cap]
            logger.info(f"MILP internal cap: {original_order_count} → {self.max_orders_cap} orders (oldest first)")
        else:
            working_orders = waiting_orders

        # Build set of working order IDs for filtering
        working_order_ids = {o['order_id'] for o in working_orders}

        logger.info(f"MILP solving for {len(working_orders)} orders, "
                   f"{len(available_couriers)} couriers")

        # Create model
        model = cp_model.CpModel()

        # Build cost matrix and valid pairs
        valid_pairs = []
        costs = {}

        if candidates:
            # Filter candidates to only include working orders
            # AND limit to max_candidates_per_order per order
            order_candidate_count = {}

            for order_id, courier_id, distance in candidates:
                # Skip if order not in working set
                if order_id not in working_order_ids:
                    continue

                # Count candidates per order
                if order_id not in order_candidate_count:
                    order_candidate_count[order_id] = 0

                # Skip if we already have enough candidates for this order
                if order_candidate_count[order_id] >= self.max_candidates_per_order:
                    continue

                order_candidate_count[order_id] += 1
                valid_pairs.append((order_id, courier_id))
                # Scale down microdegrees to avoid overflow
                costs[(order_id, courier_id)] = int(distance / 1000)

            logger.info(f"MILP using {len(valid_pairs)} candidate pairs "
                       f"(filtered from {len(candidates)})")
        else:
            # Compute all pairs (but still respect order cap)
            for order in working_orders:
                order_id = order['order_id']
                order_details = waybill_lookup.get(order_id, {})

                # Limit candidates per order even in full matrix mode
                courier_count = 0
                for courier in available_couriers:
                    if courier_count >= self.max_candidates_per_order:
                        break

                    courier_id = courier['courier_id']

                    # Compute cost
                    cost = self.cost_function.compute_cost(
                        courier, order, order_details
                    )

                    valid_pairs.append((order_id, courier_id))
                    # Scale down microdegrees to avoid overflow
                    costs[(order_id, courier_id)] = int(cost / 1000)
                    courier_count += 1

        if not valid_pairs:
            logger.warning("No valid order-courier pairs found")
            return []

        # Create decision variables: x[order_id, courier_id] = 1 if assigned
        x = {}
        for order_id, courier_id in valid_pairs:
            x[(order_id, courier_id)] = model.NewBoolVar(f'x_{order_id}_{courier_id}')

        # Constraint 2: Each courier handles at most max_orders_per_courier orders
        for courier in available_couriers:
            courier_id = courier['courier_id']
            assignments_for_courier = [
                x[(order_id, courier_id)]
                for order_id, cid in valid_pairs
                if cid == courier_id
            ]
            if assignments_for_courier:
                model.Add(sum(assignments_for_courier) <= self.max_orders_per_courier)

        # Objective: Maximize assignments while minimizing cost
        # Use a large penalty for unassigned orders
        BIG_PENALTY = 1000000  # Large penalty for unassigned orders

        # Add slack variables for unassigned orders
        unassigned = {}
        for order in working_orders:
            order_id = order['order_id']
            unassigned[order_id] = model.NewBoolVar(f'unassigned_{order_id}')

            # Order is either assigned or unassigned
            assignments_for_order = [
                x[(order_id, courier_id)]
                for oid, courier_id in valid_pairs
                if oid == order_id
            ]
            if assignments_for_order:
                model.Add(sum(assignments_for_order) + unassigned[order_id] == 1)

        # Objective: Minimize assignment costs + penalties for unassigned
        objective_terms = []

        # Cost of assignments
        for (order_id, courier_id) in valid_pairs:
            objective_terms.append(x[(order_id, courier_id)] * costs[(order_id, courier_id)])

        # Penalty for unassigned orders
        for order_id in unassigned:
            objective_terms.append(unassigned[order_id] * BIG_PENALTY)

        model.Minimize(sum(objective_terms))

        # Solve
        solver = cp_model.CpSolver()
        solver.parameters.max_time_in_seconds = self.time_limit_sec
        solver.parameters.relative_gap_limit = 0.05  # 5% optimality gap
        solver.parameters.log_search_progress = False  # Quiet mode

        status = solver.Solve(model)

        # Extract solution
        assignments = []

        if status == cp_model.OPTIMAL or status == cp_model.FEASIBLE:
            logger.info(f"MILP solution found: status={solver.StatusName(status)}, "
                       f"objective={solver.ObjectiveValue()/1000.0:.2f}")

            # Map IDs back to objects
            order_map = {o['order_id']: o for o in working_orders}
            courier_map = {c['courier_id']: c for c in available_couriers}

            for (order_id, courier_id) in valid_pairs:
                if solver.Value(x[(order_id, courier_id)]) == 1:
                    order = order_map[order_id]
                    courier = courier_map[courier_id]
                    cost = costs[(order_id, courier_id)] * 1000.0  # Convert back from scaled integer
                    assignments.append((order, courier, cost))

            logger.info(f"MILP assigned {len(assignments)} orders out of {len(working_orders)}")
        elif status == cp_model.UNKNOWN:
            logger.warning(f"MILP solver timeout with {len(working_orders)} orders")
            # Could implement retry logic here with fewer orders
            # For now, just return empty and let orders go to backlog
        else:
            logger.warning(f"MILP solver failed: {solver.StatusName(status)}")

        return assignments

    def assign_single_order(
        self,
        order: Dict,
        available_couriers: List[Dict],
        waybill_lookup: Dict
    ) -> Optional[Tuple[Dict, float]]:
        """
        Assign a single order (for real-time mode).

        For single orders, MILP reduces to finding the minimum cost courier.
        This is equivalent to greedy for single assignments.

        Args:
            order: Single order to assign
            available_couriers: Available couriers
            waybill_lookup: Full order details

        Returns:
            (courier, cost) tuple if successful, None otherwise
        """
        if not available_couriers:
            return None

        # For single order, just find minimum cost courier (same as greedy)
        order_details = waybill_lookup.get(order['order_id'], {})

        best_courier = None
        best_cost = float('inf')

        for courier in available_couriers:
            cost = self.cost_function.compute_cost(courier, order, order_details)
            if cost < best_cost:
                best_cost = cost
                best_courier = courier

        if best_courier:
            return (best_courier, best_cost)

        return None