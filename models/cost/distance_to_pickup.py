"""
Distance to Pickup Cost Function

Cost = Euclidean distance from courier's current location to the restaurant/pickup location

This is the simplest baseline cost function. It optimizes for:
- Minimizing courier travel distance to reach the restaurant
- Fast assignment decisions (no complex calculations)

This does NOT consider:
- Delivery distance
- Total journey time
- Detour cost
- Time windows
"""

import math
from typing import Dict, Any
from .base import BaseCostFunction


def euclidean_distance(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    """
    Calculate Euclidean distance between two points in shifted planar coordinates

    Args:
        lat1, lng1: First point coordinates
        lat2, lng2: Second point coordinates

    Returns:
        float: Euclidean distance in grid units
    """
    return math.sqrt((lat1 - lat2) ** 2 + (lng1 - lng2) ** 2)


class DistanceToPickup(BaseCostFunction):
    """
    Cost function that minimizes courier distance to pickup location
    """

    def __init__(self):
        """Initialize distance to pickup cost function"""
        super().__init__()

    def compute_cost(self, courier: Dict[str, Any], order: Dict[str, Any],
                     order_location: Dict[str, Any]) -> float:
        """
        Compute Euclidean distance from courier to pickup location

        Args:
            courier: Courier dict with rider_lat, rider_lng
            order: Order dict (not used in this cost function)
            order_location: Order location dict with sender_lat, sender_lng (pickup location)

        Returns:
            float: Distance in grid units (shifted planar coordinates)
        """
        courier_lat = courier['rider_lat']
        courier_lng = courier['rider_lng']
        pickup_lat = order_location['sender_lat']
        pickup_lng = order_location['sender_lng']

        distance = euclidean_distance(courier_lat, courier_lng, pickup_lat, pickup_lng)
        return distance

    def get_name(self) -> str:
        """Return cost function name"""
        return "distance_to_pickup"

    def get_description(self) -> str:
        """Return description"""
        return "Minimizes Euclidean distance from courier to restaurant pickup location"
