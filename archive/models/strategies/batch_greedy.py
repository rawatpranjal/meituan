"""
Batch-aware greedy strategy that processes the entire batch before committing.
A smarter version of greedy that considers all orders in the batch.
"""
from typing import List, Dict, Tuple, Optional
import logging
import heapq

from . import BaseStrategy

logger = logging.getLogger(__name__)


class BatchGreedyStrategy(BaseStrategy):
    """
    Batch-aware greedy assignment.

    This strategy:
    - Considers all orders in the batch simultaneously
    - Prioritizes order-courier pairs by cost globally
    - Assigns greedily but with batch-wide visibility
    - Better than pure greedy but not globally optimal like Hungarian
    """

    def get_name(self) -> str:
        """Get strategy name for logging."""
        return "batch_greedy"

    def make_assignments(
        self,
        waiting_orders: List[Dict],
        available_couriers: List[Dict],
        waybill_lookup: Dict,
        candidates: Optional[List[Tuple]] = None,
        bundle_mapping: Optional[Dict] = None
    ) -> List[Tuple[Dict, Dict, float]]:
        """
        Make batch-aware greedy assignments.

        Instead of processing orders sequentially, this:
        1. Computes all possible order-courier pairs
        2. Sorts them by cost
        3. Greedily selects pairs ensuring no conflicts

        Args:
            waiting_orders: Orders to assign
            available_couriers: Available couriers
            waybill_lookup: Full order details
            candidates: Optional sparse candidates
            bundle_mapping: Optional bundle mapping

        Returns:
            List of (order, courier, cost) tuples
        """
        if not waiting_orders or not available_couriers:
            return []

        logger.debug(f"Batch-Greedy: Processing {len(waiting_orders)} orders, "
                    f"{len(available_couriers)} couriers")

        # Build all possible pairs with costs
        all_pairs = self._build_all_pairs(
            waiting_orders,
            available_couriers,
            waybill_lookup,
            candidates
        )

        # Sort pairs by cost (greedy selection order)
        # Using heap for efficient extraction
        heapq.heapify(all_pairs)

        # Greedy selection ensuring no conflicts
        assignments = []
        assigned_orders = set()
        assigned_couriers = set()

        while all_pairs:
            cost, idx, order, courier = heapq.heappop(all_pairs)

            # Skip if already assigned
            order_id = order['order_id']
            courier_id = courier['courier_id']

            if order_id in assigned_orders or courier_id in assigned_couriers:
                continue

            # Valid assignment
            assignments.append((order, courier, cost))
            assigned_orders.add(order_id)
            assigned_couriers.add(courier_id)

            logger.debug(f"Assigned order {order_id} to courier {courier_id} with cost {cost:.2f}")

            # Stop if we've assigned all possible
            if len(assignments) >= min(len(waiting_orders), len(available_couriers)):
                break

        logger.info(f"Batch-Greedy: Made {len(assignments)} assignments")
        return assignments

    def _build_all_pairs(
        self,
        orders: List[Dict],
        couriers: List[Dict],
        waybill_lookup: Dict,
        candidates: Optional[List[Tuple]] = None
    ) -> List[Tuple[float, int, Dict, Dict]]:
        """
        Build all possible order-courier pairs with costs.

        Args:
            orders: Orders to assign
            couriers: Available couriers
            waybill_lookup: Full order details
            candidates: Optional sparse candidates to limit pairs

        Returns:
            List of (cost, index, order, courier) tuples for heap
        """
        pairs = []
        pair_idx = 0  # Tie-breaker index for heap

        if candidates:
            # Use sparse candidates
            candidate_dict = {}
            for order_id, courier_id, cost in candidates:
                if order_id not in candidate_dict:
                    candidate_dict[order_id] = {}
                candidate_dict[order_id][courier_id] = cost

            # Build pairs from candidates
            order_dict = {o['order_id']: o for o in orders}
            courier_dict = {c['courier_id']: c for c in couriers}

            for order_id, courier_costs in candidate_dict.items():
                if order_id not in order_dict:
                    continue

                order = order_dict[order_id]

                for courier_id, cost in courier_costs.items():
                    if courier_id not in courier_dict:
                        continue

                    courier = courier_dict[courier_id]
                    pairs.append((cost, pair_idx, order, courier))
                    pair_idx += 1

        else:
            # Compute all pairs
            for order in orders:
                order_id = order['order_id']

                if order_id not in waybill_lookup:
                    continue

                order_location = waybill_lookup[order_id]

                for courier in couriers:
                    cost = self.cost_function.compute_cost(
                        courier=courier,
                        order=order,
                        order_location=order_location
                    )

                    # Skip invalid pairs
                    if cost >= 1e9:
                        continue

                    pairs.append((cost, pair_idx, order, courier))
                    pair_idx += 1

        logger.debug(f"Built {len(pairs)} valid order-courier pairs")
        return pairs

    def assign_single_order(
        self,
        order: Dict,
        available_couriers: List[Dict],
        waybill_lookup: Dict
    ) -> Optional[Tuple[Dict, float]]:
        """
        Assign a single order (falls back to greedy for single order).

        Args:
            order: Single order to assign
            available_couriers: Available couriers
            waybill_lookup: Full order details

        Returns:
            (courier, cost) tuple if successful, None otherwise
        """
        if not available_couriers:
            return None

        order_id = order['order_id']
        if order_id not in waybill_lookup:
            return None

        order_location = waybill_lookup[order_id]

        # Find minimum cost courier (same as regular greedy for single order)
        min_cost = float('inf')
        best_courier = None

        for courier in available_couriers:
            cost = self.cost_function.compute_cost(
                courier=courier,
                order=order,
                order_location=order_location
            )

            if cost < min_cost:
                min_cost = cost
                best_courier = courier

        if best_courier is None or min_cost >= 1e9:
            return None

        return (best_courier, min_cost)