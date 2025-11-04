"""
Capacity token manager for tracking courier order assignments.
Prevents over-allocation when processing multiple clusters.
"""
from typing import Dict, Set, Optional, List
import logging
from threading import Lock

logger = logging.getLogger(__name__)


class CapacityTokenManager:
    """
    Manages courier capacity across multiple cluster assignments.

    Ensures that couriers don't exceed their maximum order capacity
    when being assigned orders from different spatial clusters.
    """

    def __init__(self, max_orders_per_courier: int = 5):
        """
        Initialize capacity token manager.

        Args:
            max_orders_per_courier: Maximum orders a courier can handle (default 5)
        """
        self.max_orders_per_courier = max_orders_per_courier
        self.tokens = {}  # {courier_id: remaining_capacity}
        self.assignments = {}  # {courier_id: Set[order_id]}
        self.lock = Lock()  # Thread safety if needed

        logger.info(f"CapacityTokenManager initialized: max_orders={max_orders_per_courier}")

    def initialize(self, couriers: List[Dict]):
        """
        Set all couriers to full capacity.

        Args:
            couriers: List of courier dictionaries
        """
        with self.lock:
            self.tokens.clear()
            self.assignments.clear()

            for courier in couriers:
                courier_id = courier['courier_id']
                self.tokens[courier_id] = self.max_orders_per_courier
                self.assignments[courier_id] = set()

            logger.info(f"Initialized capacity for {len(couriers)} couriers")

    def reset(self):
        """Reset all capacity tokens to initial state."""
        with self.lock:
            for courier_id in self.tokens:
                self.tokens[courier_id] = self.max_orders_per_courier
                self.assignments[courier_id] = set()

            logger.info("Reset all courier capacities")

    def get_available_capacity(self, courier_id: str) -> int:
        """
        Get remaining capacity for a courier.

        Args:
            courier_id: Courier identifier

        Returns:
            Remaining capacity (0 if courier not registered)
        """
        return self.tokens.get(courier_id, 0)

    def has_capacity(self, courier_id: str, required: int = 1) -> bool:
        """
        Check if courier has required capacity.

        Args:
            courier_id: Courier identifier
            required: Number of orders to assign (default 1)

        Returns:
            True if courier can take the orders
        """
        return self.get_available_capacity(courier_id) >= required

    def reserve(self, courier_id: str, order_ids: List[str]) -> bool:
        """
        Reserve capacity for orders.

        Args:
            courier_id: Courier identifier
            order_ids: List of order IDs to assign

        Returns:
            True if reservation successful, False if insufficient capacity
        """
        with self.lock:
            current_capacity = self.tokens.get(courier_id, 0)
            required = len(order_ids)

            if current_capacity < required:
                logger.warning(f"Courier {courier_id} has capacity {current_capacity}, "
                             f"needs {required} - reservation failed")
                return False

            # Update capacity and assignments
            self.tokens[courier_id] = current_capacity - required
            self.assignments[courier_id].update(order_ids)

            logger.debug(f"Reserved {required} slots for courier {courier_id}, "
                        f"remaining capacity: {self.tokens[courier_id]}")
            return True

    def release(self, courier_id: str, order_ids: List[str]):
        """
        Release capacity (e.g., if assignment fails later).

        Args:
            courier_id: Courier identifier
            order_ids: Order IDs to release
        """
        with self.lock:
            if courier_id not in self.tokens:
                return

            # Return capacity
            released = 0
            for order_id in order_ids:
                if order_id in self.assignments[courier_id]:
                    self.assignments[courier_id].remove(order_id)
                    released += 1

            self.tokens[courier_id] = min(
                self.tokens[courier_id] + released,
                self.max_orders_per_courier
            )

            if released > 0:
                logger.debug(f"Released {released} slots for courier {courier_id}, "
                           f"capacity now: {self.tokens[courier_id]}")

    def filter_couriers_with_capacity(self,
                                     couriers: List[Dict],
                                     min_capacity: int = 1) -> List[Dict]:
        """
        Filter couriers to only those with minimum capacity.

        Args:
            couriers: List of courier dictionaries
            min_capacity: Minimum required capacity (default 1)

        Returns:
            Filtered list of couriers with sufficient capacity
        """
        available_couriers = []

        for courier in couriers:
            courier_id = courier['courier_id']
            if self.get_available_capacity(courier_id) >= min_capacity:
                available_couriers.append(courier)

        logger.debug(f"Filtered {len(couriers)} couriers to {len(available_couriers)} "
                    f"with capacity >= {min_capacity}")
        return available_couriers

    def get_assignments(self, courier_id: str) -> Set[str]:
        """
        Get current order assignments for a courier.

        Args:
            courier_id: Courier identifier

        Returns:
            Set of assigned order IDs
        """
        return self.assignments.get(courier_id, set()).copy()

    def get_utilization_stats(self) -> Dict:
        """
        Get capacity utilization statistics.

        Returns:
            Dictionary with utilization metrics
        """
        total_couriers = len(self.tokens)
        if total_couriers == 0:
            return {
                'total_couriers': 0,
                'fully_utilized': 0,
                'partially_utilized': 0,
                'unutilized': 0,
                'avg_utilization': 0.0,
                'total_capacity': 0,
                'used_capacity': 0
            }

        fully_utilized = sum(1 for cap in self.tokens.values() if cap == 0)
        partially_utilized = sum(1 for cap in self.tokens.values()
                               if 0 < cap < self.max_orders_per_courier)
        unutilized = sum(1 for cap in self.tokens.values()
                        if cap == self.max_orders_per_courier)

        total_capacity = total_couriers * self.max_orders_per_courier
        used_capacity = sum(self.max_orders_per_courier - cap
                          for cap in self.tokens.values())
        avg_utilization = (used_capacity / total_capacity * 100) if total_capacity > 0 else 0

        return {
            'total_couriers': total_couriers,
            'fully_utilized': fully_utilized,
            'partially_utilized': partially_utilized,
            'unutilized': unutilized,
            'avg_utilization': avg_utilization,
            'total_capacity': total_capacity,
            'used_capacity': used_capacity
        }

    def log_utilization(self):
        """Log current utilization statistics."""
        stats = self.get_utilization_stats()
        logger.info(f"Capacity utilization: {stats['used_capacity']}/{stats['total_capacity']} "
                   f"({stats['avg_utilization']:.1f}%), "
                   f"Fully utilized: {stats['fully_utilized']}, "
                   f"Partial: {stats['partially_utilized']}, "
                   f"Unutilized: {stats['unutilized']}")

    def create_capacity_aware_courier_list(self,
                                          couriers: List[Dict]) -> List[Dict]:
        """
        Create courier list with capacity annotations.

        Adds 'available_capacity' field to each courier dict.

        Args:
            couriers: Original courier list

        Returns:
            Courier list with capacity information
        """
        annotated_couriers = []

        for courier in couriers:
            courier_copy = courier.copy()
            courier_copy['available_capacity'] = self.get_available_capacity(
                courier['courier_id']
            )
            annotated_couriers.append(courier_copy)

        return annotated_couriers