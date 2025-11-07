from typing import Tuple

def manhattan_distance(loc1: Tuple[float, float], loc2: Tuple[float, float]) -> float:
    return abs(loc1[0] - loc2[0]) + abs(loc1[1] - loc2[1])
