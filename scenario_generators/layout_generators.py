"""
Layout Generators for Food Delivery Simulation

Generates restaurant and courier placement patterns for different scenario types.
Supports clustered, divided, scattered, random, and explicit placement strategies.
"""

import numpy as np
from typing import List, Tuple, Dict, Any
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from simulator_core import Restaurant, Courier


def generate_restaurant_layout(config: Dict[str, Any]) -> List[Restaurant]:
    """
    Generate restaurant locations based on config specification.

    Args:
        config: Full configuration dictionary

    Returns:
        List of Restaurant objects with assigned locations
    """
    restaurant_config = config['restaurants']
    count = restaurant_config['count']
    layout_type = restaurant_config.get('layout', 'random')
    map_size_km = config['physics']['map_size_m'] / 1000.0
    random_seed = config['scenario'].get('random_seed', 42)

    np.random.seed(random_seed)

    if layout_type == 'explicit':
        return _generate_explicit_restaurants(restaurant_config, count)
    elif layout_type == 'clustered':
        return _generate_clustered_restaurants(restaurant_config, count, map_size_km)
    elif layout_type == 'divided':
        return _generate_divided_restaurants(restaurant_config, count, map_size_km)
    elif layout_type == 'scattered':
        return _generate_scattered_restaurants(restaurant_config, count, map_size_km)
    elif layout_type == 'random':
        return _generate_random_restaurants(count, map_size_km)
    else:
        raise ValueError(f"Unknown restaurant layout type: {layout_type}")


def _generate_explicit_restaurants(config: Dict[str, Any], count: int) -> List[Restaurant]:
    """Generate restaurants at explicitly specified locations."""
    locations = config.get('locations', [])
    if len(locations) != count:
        raise ValueError(
            f"Explicit layout requires {count} locations, got {len(locations)}"
        )

    return [Restaurant(i, tuple(loc)) for i, loc in enumerate(locations)]


def _generate_clustered_restaurants(config: Dict[str, Any], count: int, map_size: float) -> List[Restaurant]:
    """
    Generate restaurants with most clustered in 'downtown' area.

    Pattern for Downtown Crush scenario:
    - 75% of restaurants in tight downtown cluster (bottom-left quadrant)
    - 25% scattered elsewhere on map
    """
    downtown_fraction = config.get('clustered', {}).get('downtown_fraction', 0.75)
    downtown_center = config.get('clustered', {}).get('downtown_center', [1.25, 1.25])
    downtown_radius = config.get('clustered', {}).get('downtown_radius', 0.5)

    num_downtown = int(count * downtown_fraction)
    num_scattered = count - num_downtown

    restaurants = []

    # Generate downtown cluster
    for i in range(num_downtown):
        # Random position within circular cluster
        angle = np.random.uniform(0, 2 * np.pi)
        radius = np.random.uniform(0, downtown_radius)
        x = downtown_center[0] + radius * np.cos(angle)
        y = downtown_center[1] + radius * np.sin(angle)

        # Clamp to map boundaries
        x = np.clip(x, 0.1, map_size - 0.1)
        y = np.clip(y, 0.1, map_size - 0.1)

        restaurants.append(Restaurant(i, (x, y)))

    # Generate scattered restaurants
    for i in range(num_downtown, count):
        x = np.random.uniform(0.5, map_size - 0.5)
        y = np.random.uniform(0.5, map_size - 0.5)
        restaurants.append(Restaurant(i, (x, y)))

    return restaurants


def _generate_divided_restaurants(config: Dict[str, Any], count: int, map_size: float) -> List[Restaurant]:
    """
    Generate restaurants all on one side of a dividing line (e.g., river).

    Pattern for River Divide scenario:
    - ALL restaurants south of center line
    - Uniform distribution within allowed region
    """
    # FIX: Convert river_position from meters to km (config stores meters, map_size is in km)
    river_position_m = config.get('divided', {}).get('river_position', map_size * 1000 / 2)
    river_position = river_position_m / 1000.0  # Convert to km
    side = config.get('divided', {}).get('side', 'south')

    restaurants = []

    for i in range(count):
        x = np.random.uniform(0.5, map_size - 0.5)

        if side == 'south':
            # Restaurants south of river (y < river_position)
            y = np.random.uniform(0.5, river_position - 0.2)
        else:  # north
            # Restaurants north of river (y > river_position)
            y = np.random.uniform(river_position + 0.2, map_size - 0.5)

        restaurants.append(Restaurant(i, (x, y)))

    return restaurants


def _generate_scattered_restaurants(config: Dict[str, Any], count: int, map_size: float) -> List[Restaurant]:
    """
    Generate restaurants in distinct geographic clusters (corner clusters).

    Pattern for Pop-Up Problem scenario:
    - 4 geographic zones (corners of map)
    - Restaurants evenly distributed across zones
    - Each zone has tight cluster
    """
    num_clusters = config.get('scattered', {}).get('num_clusters', 4)
    cluster_radius = config.get('scattered', {}).get('cluster_radius', 0.3)

    # Define cluster centers (corners of map with some padding)
    padding = 1.0
    cluster_centers = [
        (padding, padding),  # Bottom-left
        (map_size - padding, padding),  # Bottom-right
        (padding, map_size - padding),  # Top-left
        (map_size - padding, map_size - padding)  # Top-right
    ]

    if num_clusters != len(cluster_centers):
        # Adjust if different number of clusters requested
        cluster_centers = cluster_centers[:num_clusters]

    restaurants = []
    restaurants_per_cluster = count // num_clusters
    remainder = count % num_clusters

    restaurant_id = 0
    for cluster_idx, center in enumerate(cluster_centers):
        # Distribute remainder evenly
        cluster_count = restaurants_per_cluster + (1 if cluster_idx < remainder else 0)

        for _ in range(cluster_count):
            # Random position within cluster
            angle = np.random.uniform(0, 2 * np.pi)
            radius = np.random.uniform(0, cluster_radius)
            x = center[0] + radius * np.cos(angle)
            y = center[1] + radius * np.sin(angle)

            # Clamp to map boundaries
            x = np.clip(x, 0.1, map_size - 0.1)
            y = np.clip(y, 0.1, map_size - 0.1)

            restaurants.append(Restaurant(restaurant_id, (x, y)))
            restaurant_id += 1

    return restaurants


def _generate_random_restaurants(count: int, map_size: float) -> List[Restaurant]:
    """Generate restaurants at completely random locations."""
    restaurants = []
    for i in range(count):
        x = np.random.uniform(0.5, map_size - 0.5)
        y = np.random.uniform(0.5, map_size - 0.5)
        restaurants.append(Restaurant(i, (x, y)))
    return restaurants


# =============================================================================
# COURIER LAYOUT GENERATORS
# =============================================================================

def generate_courier_layout(config: Dict[str, Any]) -> List[Courier]:
    """
    Generate courier starting locations based on config specification.

    Args:
        config: Full configuration dictionary

    Returns:
        List of Courier objects with assigned starting locations
    """
    courier_config = config['couriers']
    count = courier_config['count']
    layout_type = courier_config.get('layout', 'random')
    map_size_km = config['physics']['map_size_m'] / 1000.0
    simulation_duration = config['scenario']['duration_hours'] * 3600
    random_seed = config['scenario'].get('random_seed', 42)

    np.random.seed(random_seed + 1000)  # Different seed from restaurants

    if layout_type == 'explicit':
        return _generate_explicit_couriers(courier_config, count, simulation_duration)
    elif layout_type == 'central':
        return _generate_central_couriers(courier_config, count, map_size_km, simulation_duration)
    elif layout_type == 'zonal':
        return _generate_zonal_couriers(courier_config, count, map_size_km, simulation_duration)
    elif layout_type == 'random':
        return _generate_random_couriers(count, map_size_km, simulation_duration)
    else:
        raise ValueError(f"Unknown courier layout type: {layout_type}")


def _generate_explicit_couriers(config: Dict[str, Any], count: int, duration: int) -> List[Courier]:
    """Generate couriers at explicitly specified locations."""
    locations = config.get('locations', [])
    if len(locations) != count:
        raise ValueError(
            f"Explicit layout requires {count} locations, got {len(locations)}"
        )

    return [Courier(i, tuple(loc), 0, duration) for i, loc in enumerate(locations)]


def _generate_central_couriers(config: Dict[str, Any], count: int, map_size: float, duration: int) -> List[Courier]:
    """
    Generate couriers clustered around a central point.

    Pattern for Downtown Crush and Pop-Up Problem scenarios:
    - All couriers start near map center
    - Small random scatter for realism
    """
    center = config.get('central', {}).get('center', [map_size / 2, map_size / 2])
    scatter_radius = config.get('central', {}).get('scatter_radius', 0.5)

    couriers = []

    for i in range(count):
        # Random position within scatter radius
        angle = np.random.uniform(0, 2 * np.pi)
        radius = np.random.uniform(0, scatter_radius)
        x = center[0] + radius * np.cos(angle)
        y = center[1] + radius * np.sin(angle)

        # Clamp to map boundaries
        x = np.clip(x, 0.1, map_size - 0.1)
        y = np.clip(y, 0.1, map_size - 0.1)

        couriers.append(Courier(i, (x, y), 0, duration))

    return couriers


def _generate_zonal_couriers(config: Dict[str, Any], count: int, map_size: float, duration: int) -> List[Courier]:
    """
    Generate couriers split between geographic zones.

    Pattern for River Divide scenario:
    - 67% of couriers south of boundary
    - 33% of couriers north of boundary
    """
    zones_config = config.get('zonal', {}).get('zones', {'south': 10, 'north': 5})
    # FIX: Convert boundary_y from meters to km (config stores meters, map_size is in km)
    boundary_y_m = config.get('zonal', {}).get('boundary_y', map_size * 1000 / 2)
    boundary_y = boundary_y_m / 1000.0  # Convert to km

    # Calculate distribution
    total_specified = sum(zones_config.values())
    if total_specified != count:
        # Scale proportionally if mismatch
        scale = count / total_specified
        zones_config = {k: int(v * scale) for k, v in zones_config.items()}
        # Handle rounding remainder
        remainder = count - sum(zones_config.values())
        if remainder > 0:
            zones_config['south'] += remainder

    couriers = []
    courier_id = 0

    # Generate south zone couriers
    if 'south' in zones_config:
        for _ in range(zones_config['south']):
            x = np.random.uniform(0.5, map_size - 0.5)
            y = np.random.uniform(0.5, boundary_y - 0.2)
            couriers.append(Courier(courier_id, (x, y), 0, duration))
            courier_id += 1

    # Generate north zone couriers
    if 'north' in zones_config:
        for _ in range(zones_config['north']):
            x = np.random.uniform(0.5, map_size - 0.5)
            y = np.random.uniform(boundary_y + 0.2, map_size - 0.5)
            couriers.append(Courier(courier_id, (x, y), 0, duration))
            courier_id += 1

    return couriers


def _generate_random_couriers(count: int, map_size: float, duration: int) -> List[Courier]:
    """Generate couriers at completely random locations."""
    couriers = []
    for i in range(count):
        x = np.random.uniform(0.5, map_size - 0.5)
        y = np.random.uniform(0.5, map_size - 0.5)
        couriers.append(Courier(i, (x, y), 0, duration))
    return couriers
