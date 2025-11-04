"""
Cost Functions Module

This module contains different cost functions for courier-order assignment optimization.
Each cost function implements the BaseCostFunction interface.

Available cost functions:
- DistanceToPickup: Minimizes distance from courier to pickup location (Tier 1 baseline)

Future cost functions to implement:
- TotalDeliveryTime: Minimizes total time from pickup to delivery
- DetourCost: Minimizes additional detour distance if courier has existing orders
- TimeWindowPenalty: Penalizes assignments that violate time windows
- HybridCost: Combines multiple cost factors with weights
"""

from .base import BaseCostFunction
from .distance_to_pickup import DistanceToPickup

__all__ = [
    'BaseCostFunction',
    'DistanceToPickup',
    'get_cost_function',
]


def get_cost_function(name: str) -> BaseCostFunction:
    """
    Get a cost function instance by name.

    Args:
        name: Name of the cost function

    Returns:
        Instance of the requested cost function

    Raises:
        ValueError: If cost function not found
    """
    cost_functions = {
        'distance_to_pickup': DistanceToPickup,
        # Add more cost functions here as they're implemented
        # 'total_delivery_time': TotalDeliveryTime,
        # 'detour_cost': DetourCost,
    }

    if name not in cost_functions:
        available = list(cost_functions.keys())
        raise ValueError(f"Cost function '{name}' not found. Available: {available}")

    return cost_functions[name]()
