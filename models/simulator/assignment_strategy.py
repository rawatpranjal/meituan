"""
Assignment Strategy - The "Brain"

Pluggable algorithms for order-to-courier assignment.
This module allows easy swapping of different assignment strategies.

Note: Assignment strategies now accept cost functions as parameters,
allowing separation of the optimization algorithm from the cost function.
"""

from abc import ABC, abstractmethod
import numpy as np
from scipy.optimize import linear_sum_assignment


class AssignmentStrategy(ABC):
    """
    Abstract base class for assignment algorithms

    Any assignment strategy must implement the make_assignments method.
    """

    @abstractmethod
    def make_assignments(self, waiting_orders, available_couriers, waybill_lookup):
        """
        Make order-to-courier assignments

        Args:
            waiting_orders: List of order dicts with order_id
            available_couriers: List of courier dicts with courier_id, rider_lat, rider_lng
            waybill_lookup: Dictionary mapping order_id to order details (pickup/delivery locations)

        Returns:
            List of tuples: (order_dict, courier_dict, cost)
            where cost is the assignment cost for this pair
        """
        pass


class Tier1Baseline(AssignmentStrategy):
    """
    Tier 1 Baseline: Static Bipartite Matching

    Algorithm: Hungarian algorithm (optimal bipartite matching)
    Cost function: Pluggable (passed as parameter)
    """

    def __init__(self, cost_function):
        """
        Initialize assignment strategy with a cost function

        Args:
            cost_function: Instance of a cost function from models.cost module
        """
        self.cost_function = cost_function

    def make_assignments(self, waiting_orders, available_couriers, waybill_lookup):
        """
        Assign orders to couriers using the configured cost function

        Returns list of (order, courier, cost) tuples
        """
        if not waiting_orders or not available_couriers:
            return []

        n_orders = len(waiting_orders)
        n_couriers = len(available_couriers)

        # Build cost matrix: |orders| x |couriers|
        cost_matrix = np.zeros((n_orders, n_couriers))

        for i, order in enumerate(waiting_orders):
            order_id = order['order_id']

            # Skip if order not in lookup
            if order_id not in waybill_lookup:
                # Set to high cost so it won't be assigned
                cost_matrix[i, :] = 1e9
                continue

            # Get order location details
            order_location = waybill_lookup[order_id]

            for j, courier in enumerate(available_couriers):
                # Calculate cost using the configured cost function
                cost = self.cost_function.compute_cost(
                    courier=courier,
                    order=order,
                    order_location=order_location
                )

                cost_matrix[i, j] = cost

        # Solve assignment problem using Hungarian algorithm
        row_ind, col_ind = linear_sum_assignment(cost_matrix)

        # Build list of assignments with costs
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

        return assignments
