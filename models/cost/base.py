"""
Base Cost Function Interface

All cost functions must inherit from BaseCostFunction and implement the compute_cost method.
Cost functions evaluate the "cost" of assigning a courier to an order.
Lower cost = better match.
"""

from abc import ABC, abstractmethod
from typing import Dict, Any


class BaseCostFunction(ABC):
    """
    Abstract base class for cost functions used in courier-order assignment
    """

    def __init__(self):
        """Initialize cost function"""
        pass

    @abstractmethod
    def compute_cost(self, courier: Dict[str, Any], order: Dict[str, Any],
                     order_location: Dict[str, Any]) -> float:
        """
        Compute the cost of assigning a courier to an order

        Args:
            courier: Courier dict with keys:
                - courier_id: int
                - rider_lat: float (current location)
                - rider_lng: float (current location)
                - current_time: int (unix timestamp, when courier becomes available)
                - ... (other courier state)

            order: Order dict with keys:
                - order_id: int
                - ... (other order properties)

            order_location: Order location dict with keys:
                - sender_lat: float (restaurant/pickup location)
                - sender_lng: float
                - recipient_lat: float (customer/delivery location)
                - recipient_lng: float

        Returns:
            float: Cost score (lower is better)
        """
        pass

    @abstractmethod
    def get_name(self) -> str:
        """
        Return the name of this cost function for logging purposes

        Returns:
            str: Cost function name (e.g., "distance_to_pickup", "total_delivery_time")
        """
        pass

    @abstractmethod
    def get_description(self) -> str:
        """
        Return a human-readable description of what this cost function optimizes

        Returns:
            str: Description of the cost function
        """
        pass

    def __repr__(self) -> str:
        """String representation"""
        return f"{self.__class__.__name__}()"
