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
]
