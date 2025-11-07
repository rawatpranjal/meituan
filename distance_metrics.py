
import numpy as np
from typing import Tuple, Callable

def euclidean_distance(loc1: Tuple[float, float], loc2: Tuple[float, float]) -> float:

    return np.sqrt((loc1[0] - loc2[0])**2 + (loc1[1] - loc2[1])**2)

def manhattan_distance(loc1: Tuple[float, float], loc2: Tuple[float, float]) -> float:

    return abs(loc1[0] - loc2[0]) + abs(loc1[1] - loc2[1])

# Distance metric registry
DISTANCE_METRICS = {
    'euclidean': euclidean_distance,
    'manhattan': manhattan_distance
}

def get_distance_metric(metric_name: str) -> Callable[[Tuple[float, float], Tuple[float, float]], float]:

    if metric_name not in DISTANCE_METRICS:
        raise ValueError(
            f"Unknown distance metric: '{metric_name}'. "
            f"Available metrics: {list(DISTANCE_METRICS.keys())}"
        )
    return DISTANCE_METRICS[metric_name]

def compare_metrics(loc1: Tuple[float, float], loc2: Tuple[float, float]) -> dict:

    return {
        name: func(loc1, loc2)
        for name, func in DISTANCE_METRICS.items()
    }
