"""
Distance metric calculations for the food delivery simulator.

This module provides Manhattan distance (L1 norm) calculations,
which is the only metric currently used by the simulation.
"""

from typing import Tuple

def manhattan_distance(loc1: Tuple[float, float], loc2: Tuple[float, float]) -> float:
    """Calculate Manhattan (L1) distance between two points in km."""
    return abs(loc1[0] - loc2[0]) + abs(loc1[1] - loc2[1])
