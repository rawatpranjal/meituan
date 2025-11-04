"""
Hungarian algorithm strategy for optimal bipartite matching.
Extracted from original Tier1Baseline implementation.
"""
import numpy as np
from scipy.optimize import linear_sum_assignment
from typing import List, Dict, Tuple, Optional
import logging

from . import BaseStrategy

logger = logging.getLogger(__name__)


class HungarianStrategy(BaseStrategy):
    """
    Optimal one-to-one assignment using Hungarian algorithm.

    This strategy:
    - Builds a cost matrix between all orders and couriers
    - Solves the bipartite matching problem optimally
    - Guarantees minimum total cost (globally optimal)
    - Each courier gets at most one order
    """

    def get_name(self) -> str:
        """Get strategy name for logging."""
        return "hungarian"

    def make_assignments(
        self,
        waiting_orders: List[Dict],
        available_couriers: List[Dict],
        waybill_lookup: Dict,
        candidates: Optional[List[Tuple]] = None,
        bundle_mapping: Optional[Dict] = None
    ) -> List[Tuple[Dict, Dict, float]]:
        """
        Make optimal assignments using Hungarian algorithm.

        Args:
            waiting_orders: Orders (or bundles) to assign
            available_couriers: Available couriers
            waybill_lookup: Full order details
            candidates: Optional sparse candidate pairs (not used yet)
            bundle_mapping: Optional bundle mapping (for logging)

        Returns:
            List of (order, courier, cost) tuples
        """
        if not waiting_orders or not available_couriers:
            return []

        n_orders = len(waiting_orders)
        n_couriers = len(available_couriers)

        logger.debug(f"Hungarian: Building {n_orders}x{n_couriers} cost matrix")

        # Build cost matrix
        cost_matrix = self._build_cost_matrix(
            waiting_orders,
            available_couriers,
            waybill_lookup,
            candidates
        )

        # Solve assignment problem
        row_ind, col_ind = linear_sum_assignment(cost_matrix)
        logger.debug(f"Hungarian: Found {len(row_ind)} assignments")

        # Build assignment list
        assignments = []
        for i in range(len(row_ind)):
            order_idx = row_ind[i]
            courier_idx = col_ind[i]
            cost = cost_matrix[order_idx, courier_idx]

            # Skip if it was a "no match" (high cost sentinel)
            if cost >= 1e9:
                continue

            assignments.append((
                waiting_orders[order_idx],
                available_couriers[courier_idx],
                cost
            ))

        logger.info(f"Hungarian: Made {len(assignments)} assignments")
        return assignments

    def _build_cost_matrix(
        self,
        orders: List[Dict],
        couriers: List[Dict],
        waybill_lookup: Dict,
        candidates: Optional[List[Tuple]] = None
    ) -> np.ndarray:
        """
        Build cost matrix for Hungarian algorithm.

        Args:
            orders: Orders to assign
            couriers: Available couriers
            waybill_lookup: Full order details
            candidates: Optional sparse candidates (for future optimization)

        Returns:
            Cost matrix of shape (n_orders, n_couriers)
        """
        n_orders = len(orders)
        n_couriers = len(couriers)

        # Initialize with high cost (no match)
        cost_matrix = np.full((n_orders, n_couriers), 1e9)

        # If candidates provided, use sparse matrix
        if candidates:
            # Build lookup for fast access
            candidate_dict = {}
            for order_id, courier_id, distance in candidates:
                if order_id not in candidate_dict:
                    candidate_dict[order_id] = {}
                candidate_dict[order_id][courier_id] = distance

            # Fill matrix using candidates
            for i, order in enumerate(orders):
                order_id = order['order_id']
                if order_id in candidate_dict:
                    for j, courier in enumerate(couriers):
                        courier_id = courier['courier_id']
                        if courier_id in candidate_dict[order_id]:
                            # Use pre-computed distance as cost
                            cost_matrix[i, j] = candidate_dict[order_id][courier_id]
        else:
            # Compute full matrix
            for i, order in enumerate(orders):
                order_id = order['order_id']

                # Skip if order not in lookup
                if order_id not in waybill_lookup:
                    continue

                order_location = waybill_lookup[order_id]

                for j, courier in enumerate(couriers):
                    # Calculate cost using cost function
                    cost = self.cost_function.compute_cost(
                        courier=courier,
                        order=order,
                        order_location=order_location
                    )
                    cost_matrix[i, j] = cost

        return cost_matrix

    def assign_single_order(
        self,
        order: Dict,
        available_couriers: List[Dict],
        waybill_lookup: Dict
    ) -> Optional[Tuple[Dict, float]]:
        """
        Assign a single order optimally (for real-time mode).

        For a single order, this reduces to finding the minimum cost courier.

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

        # Find minimum cost courier
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