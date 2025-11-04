"""
Distance Metrics for Food Delivery Simulation

Provides different distance calculation methods for the delivery routing simulator.
Supports both Euclidean and Manhattan distance metrics.
"""

import numpy as np
from typing import Tuple, Callable


def euclidean_distance(loc1: Tuple[float, float], loc2: Tuple[float, float]) -> float:
    """
    Calculate Euclidean (straight-line) distance between two points in km.

    Formula: sqrt((x1 - x2)^2 + (y1 - y2)^2)

    Args:
        loc1: First location as (x, y) tuple in km
        loc2: Second location as (x, y) tuple in km

    Returns:
        Distance in km
    """
    return np.sqrt((loc1[0] - loc2[0])**2 + (loc1[1] - loc2[1])**2)


def manhattan_distance(loc1: Tuple[float, float], loc2: Tuple[float, float]) -> float:
    """
    Calculate Manhattan (city-block) distance between two points in km.

    Formula: |x1 - x2| + |y1 - y2|

    This metric models urban grid navigation where couriers can only travel
    along streets (horizontal and vertical movement), not diagonally.

    Args:
        loc1: First location as (x, y) tuple in km
        loc2: Second location as (x, y) tuple in km

    Returns:
        Distance in km
    """
    return abs(loc1[0] - loc2[0]) + abs(loc1[1] - loc2[1])


# Distance metric registry
DISTANCE_METRICS = {
    'euclidean': euclidean_distance,
    'manhattan': manhattan_distance
}


def get_distance_metric(metric_name: str) -> Callable[[Tuple[float, float], Tuple[float, float]], float]:
    """
    Factory function to get distance calculation function by name.

    Args:
        metric_name: Name of distance metric ('euclidean' or 'manhattan')

    Returns:
        Distance function that takes two (x, y) tuples and returns distance in km

    Raises:
        ValueError: If metric_name is not recognized
    """
    if metric_name not in DISTANCE_METRICS:
        raise ValueError(
            f"Unknown distance metric: '{metric_name}'. "
            f"Available metrics: {list(DISTANCE_METRICS.keys())}"
        )
    return DISTANCE_METRICS[metric_name]


def compare_metrics(loc1: Tuple[float, float], loc2: Tuple[float, float]) -> dict:
    """
    Compare all available distance metrics for two locations.

    Useful for understanding the impact of metric choice.

    Args:
        loc1: First location as (x, y) tuple in km
        loc2: Second location as (x, y) tuple in km

    Returns:
        Dictionary mapping metric names to distances
    """
    return {
        name: func(loc1, loc2)
        for name, func in DISTANCE_METRICS.items()
    }
