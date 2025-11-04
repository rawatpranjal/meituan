"""
Greedy nearest-neighbor strategy for fast local assignment.
Extracted from original Tier3OnlineGreedy implementation.
"""
from typing import List, Dict, Tuple, Optional
import logging

from . import BaseStrategy

logger = logging.getLogger(__name__)


class GreedyStrategy(BaseStrategy):
    """
    Greedy nearest-neighbor assignment (First-Come, First-Served).

    This strategy:
    - Processes orders sequentially
    - Assigns each order to its nearest available courier
    - No global optimization
    - Fast and simple
    - Good for real-time mode
    """

    def get_name(self) -> str:
        """Get strategy name for logging."""
        return "greedy"

    def make_assignments(
        self,
        waiting_orders: List[Dict],
        available_couriers: List[Dict],
        waybill_lookup: Dict,
        candidates: Optional[List[Tuple]] = None,
        bundle_mapping: Optional[Dict] = None
    ) -> List[Tuple[Dict, Dict, float]]:
        """
        Make greedy assignments (nearest neighbor for each order).

        Args:
            waiting_orders: Orders to assign
            available_couriers: Available couriers
            waybill_lookup: Full order details
            candidates: Optional sparse candidates (can speed up search)
            bundle_mapping: Optional bundle mapping

        Returns:
            List of (order, courier, cost) tuples
        """
        assignments = []
        used_couriers = set()

        # Build candidate lookup if provided
        candidate_lookup = self._build_candidate_lookup(candidates) if candidates else None

        for order in waiting_orders:
            order_id = order['order_id']

            # Skip if order not in lookup
            if order_id not in waybill_lookup:
                logger.debug(f"Order {order_id} not found in waybill lookup")
                continue

            # Find available couriers (not yet used in this batch)
            available = [
                c for c in available_couriers
                if c['courier_id'] not in used_couriers
            ]

            if not available:
                logger.debug(f"No available couriers for order {order_id}")
                break

            # Find best courier for this order
            best_courier, min_cost = self._find_best_courier(
                order,
                available,
                waybill_lookup,
                candidate_lookup
            )

            if best_courier is not None and min_cost < 1e9:
                assignments.append((order, best_courier, min_cost))
                used_couriers.add(best_courier['courier_id'])
                logger.debug(f"Assigned order {order_id} to courier {best_courier['courier_id']} "
                           f"with cost {min_cost:.2f}")

        logger.info(f"Greedy: Made {len(assignments)} assignments from {len(waiting_orders)} orders")
        return assignments

    def assign_single_order(
        self,
        order: Dict,
        available_couriers: List[Dict],
        waybill_lookup: Dict
    ) -> Optional[Tuple[Dict, float]]:
        """
        Assign a single order to nearest courier (for real-time mode).

        Args:
            order: Single order to assign
            available_couriers: Available couriers
            waybill_lookup: Full order details

        Returns:
            (courier, cost) tuple if successful, None otherwise
        """
        if not available_couriers:
            return None

        best_courier, min_cost = self._find_best_courier(
            order,
            available_couriers,
            waybill_lookup,
            candidate_lookup=None
        )

        if best_courier is None or min_cost >= 1e9:
            return None

        return (best_courier, min_cost)

    def _find_best_courier(
        self,
        order: Dict,
        available_couriers: List[Dict],
        waybill_lookup: Dict,
        candidate_lookup: Optional[Dict] = None
    ) -> Tuple[Optional[Dict], float]:
        """
        Find the best (nearest) courier for an order.

        Args:
            order: Order to assign
            available_couriers: List of available couriers
            waybill_lookup: Full order details
            candidate_lookup: Optional pre-computed candidates

        Returns:
            (best_courier, cost) tuple, or (None, inf) if no match
        """
        order_id = order['order_id']
        if order_id not in waybill_lookup:
            return None, float('inf')

        order_location = waybill_lookup[order_id]
        min_cost = float('inf')
        best_courier = None

        # If we have candidates, use them to limit search
        if candidate_lookup and order_id in candidate_lookup:
            candidate_courier_ids = set(candidate_lookup[order_id].keys())
            search_couriers = [
                c for c in available_couriers
                if c['courier_id'] in candidate_courier_ids
            ]
            if not search_couriers:
                # No candidates available, fall back to all couriers
                search_couriers = available_couriers
        else:
            search_couriers = available_couriers

        # Find minimum cost courier
        for courier in search_couriers:
            # Use pre-computed cost if available
            if candidate_lookup and order_id in candidate_lookup:
                courier_id = courier['courier_id']
                if courier_id in candidate_lookup[order_id]:
                    cost = candidate_lookup[order_id][courier_id]
                else:
                    # Compute cost if not in candidates
                    cost = self.cost_function.compute_cost(
                        courier=courier,
                        order=order,
                        order_location=order_location
                    )
            else:
                # Compute cost
                cost = self.cost_function.compute_cost(
                    courier=courier,
                    order=order,
                    order_location=order_location
                )

            if cost < min_cost:
                min_cost = cost
                best_courier = courier

        return best_courier, min_cost

    def _build_candidate_lookup(
        self,
        candidates: List[Tuple]
    ) -> Dict[str, Dict[str, float]]:
        """
        Build lookup structure from candidate list.

        Args:
            candidates: List of (order_id, courier_id, cost) tuples

        Returns:
            Nested dict: lookup[order_id][courier_id] = cost
        """
        lookup = {}

        for order_id, courier_id, cost in candidates:
            if order_id not in lookup:
                lookup[order_id] = {}
            lookup[order_id][courier_id] = cost

        return lookup