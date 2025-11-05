"""
Demand Generators for Food Delivery Simulation

Generates order schedules with different temporal and spatial patterns.
Supports sustained peaks, steady demand, unpredictable bursts, Poisson processes,
and scripted order sequences.
"""

import numpy as np
from typing import List, Dict, Any, Tuple
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from simulator_core import Order


def generate_demand(config: Dict[str, Any], restaurants: List) -> List[Order]:
    """
    Generate order schedule based on config specification.

    Args:
        config: Full configuration dictionary
        restaurants: List of Restaurant objects (for location reference)

    Returns:
        List of Order objects with placement times and locations
    """
    demand_config = config['demand']
    profile = demand_config['profile']

    if profile == 'sustained_peak':
        return _generate_sustained_peak(config, restaurants)
    elif profile == 'steady_high':
        return _generate_steady_high(config, restaurants)
    elif profile == 'unpredictable_bursts':
        return _generate_unpredictable_bursts(config, restaurants)
    elif profile == 'poisson':
        return _generate_poisson(config, restaurants)
    elif profile == 'scripted':
        return _generate_scripted(config, restaurants)
    elif profile == 'manual':
        return _generate_manual(config, restaurants)
    else:
        raise ValueError(f"Unknown demand profile: {profile}")


def _generate_sustained_peak(config: Dict[str, Any], restaurants: List) -> List[Order]:
    """
    Generate sustained peak demand pattern (Downtown Crush scenario).

    Pattern:
    - 2-hour dinner rush centered on downtown restaurants
    - 90% of orders go to clustered downtown restaurants during peak
    - High order rate during peak, low otherwise
    - Total: 400 orders over 3 hours
    """
    demand_config = config['demand']
    total_orders = demand_config['total_orders']
    duration_hours = config['scenario']['duration_hours']
    duration_seconds = duration_hours * 3600

    profile_config = demand_config.get('sustained_peak', {})
    peak_duration_hours = profile_config.get('peak_duration_hours', 2.0)
    peak_start_hour = profile_config.get('peak_start_hour', 0.5)
    peak_restaurant_weight = profile_config.get('peak_restaurant_weight', 0.9)
    off_peak_rate = profile_config.get('off_peak_rate', 0.33)  # orders/min
    peak_rate = profile_config.get('peak_rate', 2.5)  # orders/min

    random_seed = config['scenario'].get('random_seed', 42)
    np.random.seed(random_seed + 2000)

    # Calculate time windows
    peak_start_s = peak_start_hour * 3600
    peak_end_s = peak_start_s + (peak_duration_hours * 3600)

    # Identify downtown restaurants (assume first 75% are downtown for clustered layout)
    restaurant_config = config.get('restaurants', {})
    if restaurant_config.get('layout') == 'clustered':
        downtown_fraction = restaurant_config.get('clustered', {}).get('downtown_fraction', 0.75)
        num_downtown = int(len(restaurants) * downtown_fraction)
        downtown_restaurant_ids = list(range(num_downtown))
        other_restaurant_ids = list(range(num_downtown, len(restaurants)))
    else:
        # Fallback: all restaurants equally weighted
        downtown_restaurant_ids = list(range(len(restaurants)))
        other_restaurant_ids = []

    # Generate orders using Poisson process
    orders = []
    order_id = 0
    current_time = 0.0

    while current_time < duration_seconds and order_id < total_orders:
        # Determine rate based on time
        if peak_start_s <= current_time < peak_end_s:
            rate = peak_rate / 60.0  # Convert to orders/second
            is_peak = True
        else:
            rate = off_peak_rate / 60.0
            is_peak = False

        # Sample next order arrival (exponential distribution)
        inter_arrival = np.random.exponential(1.0 / rate)
        current_time += inter_arrival

        if current_time >= duration_seconds:
            break

        # Select restaurant
        if is_peak and downtown_restaurant_ids:
            # During peak: 90% downtown, 10% others
            if np.random.random() < peak_restaurant_weight:
                restaurant_id = np.random.choice(downtown_restaurant_ids)
            else:
                if other_restaurant_ids:
                    restaurant_id = np.random.choice(other_restaurant_ids)
                else:
                    restaurant_id = np.random.choice(downtown_restaurant_ids)
        else:
            # Off-peak: uniform
            restaurant_id = np.random.randint(0, len(restaurants))

        # Generate customer location (ring around restaurant)
        customer_location = _generate_customer_near_restaurant(
            restaurants[restaurant_id].location,
            config
        )

        # Create order
        order = Order(
            order_id=order_id,
            restaurant_id=restaurant_id,
            restaurant_location=restaurants[restaurant_id].location,
            diner_location=customer_location,
            placement_time=current_time
        )
        orders.append(order)
        order_id += 1

    return orders[:total_orders]  # Ensure exact count


def _generate_steady_high(config: Dict[str, Any], restaurants: List) -> List[Order]:
    """
    Generate steady high demand pattern (River Divide scenario).

    Pattern:
    - Constant pressure throughout simulation
    - Uniform rate across all restaurants
    - All customers on north side of river (if divided layout)
    - Total: 300 orders over 3 hours
    """
    demand_config = config['demand']
    total_orders = demand_config['total_orders']
    duration_hours = config['scenario']['duration_hours']
    duration_seconds = duration_hours * 3600

    profile_config = demand_config.get('steady_high', {})
    rate = profile_config.get('rate', 1.67)  # orders/min
    customer_constraint = profile_config.get('customer_constraint', None)

    random_seed = config['scenario'].get('random_seed', 42)
    np.random.seed(random_seed + 2000)

    # Generate orders with uniform Poisson rate
    orders = []
    order_id = 0
    current_time = 0.0
    rate_per_second = rate / 60.0

    while current_time < duration_seconds and order_id < total_orders:
        # Sample next order arrival
        inter_arrival = np.random.exponential(1.0 / rate_per_second)
        current_time += inter_arrival

        if current_time >= duration_seconds:
            break

        # Select restaurant uniformly
        restaurant_id = np.random.randint(0, len(restaurants))

        # Generate customer location with constraint
        if customer_constraint == 'north_only':
            customer_location = _generate_customer_north_side(config)
        elif customer_constraint == 'south_only':
            customer_location = _generate_customer_south_side(config)
        else:
            customer_location = _generate_customer_near_restaurant(
                restaurants[restaurant_id].location,
                config
            )

        # Create order
        order = Order(
            order_id=order_id,
            restaurant_id=restaurant_id,
            restaurant_location=restaurants[restaurant_id].location,
            diner_location=customer_location,
            placement_time=current_time
        )
        orders.append(order)
        order_id += 1

    return orders[:total_orders]


def _generate_unpredictable_bursts(config: Dict[str, Any], restaurants: List) -> List[Order]:
    """
    Generate unpredictable burst demand pattern (Pop-Up Problem scenario).

    Pattern:
    - Long calm periods (0.2 orders/min)
    - Sudden 20-minute bursts (4.0 orders/min) at ONE cluster
    - 3-4 bursts randomly timed
    - Bursts rotate between different restaurant clusters
    - Total: 350 orders over 4 hours
    """
    demand_config = config['demand']
    total_orders = demand_config['total_orders']
    duration_hours = config['scenario']['duration_hours']
    duration_seconds = duration_hours * 3600

    profile_config = demand_config.get('unpredictable_bursts', {})
    calm_rate = profile_config.get('calm_rate', 0.2)  # orders/min
    burst_rate = profile_config.get('burst_rate', 4.0)  # orders/min
    burst_duration_minutes = profile_config.get('burst_duration_minutes', 20)
    num_bursts = profile_config.get('num_bursts', 4)
    burst_pattern = profile_config.get('burst_pattern', 'rotating_zones')

    random_seed = config['scenario'].get('random_seed', 42)
    np.random.seed(random_seed + 2000)

    # Identify restaurant clusters (for scattered layout)
    restaurant_clusters = _identify_restaurant_clusters(restaurants, config)

    # Generate random burst times
    burst_duration_s = burst_duration_minutes * 60
    min_gap = 15 * 60  # Minimum 15 min between bursts

    burst_times = []
    for _ in range(num_bursts):
        # Find valid start time
        attempts = 0
        while attempts < 100:
            start_time = np.random.uniform(0, duration_seconds - burst_duration_s)
            # Check no overlap with existing bursts
            valid = all(
                start_time > (b + burst_duration_s + min_gap) or
                start_time + burst_duration_s + min_gap < b
                for b in burst_times
            )
            if valid:
                burst_times.append(start_time)
                break
            attempts += 1

    burst_times.sort()

    # Assign clusters to bursts (rotating or random)
    if burst_pattern == 'rotating_zones':
        # Store cluster INDEX, not the cluster itself
        burst_clusters = [i % len(restaurant_clusters)
                          for i in range(num_bursts)]
    else:  # random
        burst_clusters = [np.random.choice(range(len(restaurant_clusters)))
                          for _ in range(num_bursts)]

    # Generate orders
    orders = []
    order_id = 0
    current_time = 0.0

    while current_time < duration_seconds and order_id < total_orders:
        # Determine if we're in a burst
        in_burst = False
        active_cluster = None

        for burst_idx, burst_start in enumerate(burst_times):
            if burst_start <= current_time < burst_start + burst_duration_s:
                in_burst = True
                active_cluster = burst_clusters[burst_idx]
                break

        # Determine rate
        if in_burst:
            rate = burst_rate / 60.0
        else:
            rate = calm_rate / 60.0

        # Sample next order arrival
        inter_arrival = np.random.exponential(1.0 / rate)
        current_time += inter_arrival

        if current_time >= duration_seconds:
            break

        # Select restaurant
        if in_burst and active_cluster is not None:
            # During burst: only restaurants in active cluster
            cluster_restaurants = restaurant_clusters[active_cluster]
            restaurant_id = np.random.choice(cluster_restaurants)
        else:
            # Calm period: any restaurant
            restaurant_id = np.random.randint(0, len(restaurants))

        # Generate customer location
        customer_location = _generate_customer_near_restaurant(
            restaurants[restaurant_id].location,
            config
        )

        # Create order
        order = Order(
            order_id=order_id,
            restaurant_id=restaurant_id,
            restaurant_location=restaurants[restaurant_id].location,
            diner_location=customer_location,
            placement_time=current_time
        )
        orders.append(order)
        order_id += 1

    return orders[:total_orders]


def _generate_poisson(config: Dict[str, Any], restaurants: List) -> List[Order]:
    """Generate generic Poisson process with configurable peak period."""
    demand_config = config['demand']
    total_orders = demand_config['total_orders']
    duration_hours = config['scenario']['duration_hours']
    duration_seconds = duration_hours * 3600

    profile_config = demand_config.get('poisson', {})
    base_rate = profile_config.get('base_rate', 1.0)  # orders/min
    peak_multiplier = profile_config.get('peak_multiplier', 2.5)
    peak_start_hour = profile_config.get('peak_start_hour', 0.25)
    peak_end_hour = profile_config.get('peak_end_hour', 0.75)

    random_seed = config['scenario'].get('random_seed', 42)
    np.random.seed(random_seed + 2000)

    peak_start_s = peak_start_hour * 3600
    peak_end_s = peak_end_hour * 3600

    orders = []
    order_id = 0
    current_time = 0.0

    while current_time < duration_seconds and order_id < total_orders:
        # Determine rate
        if peak_start_s <= current_time < peak_end_s:
            rate = base_rate * peak_multiplier / 60.0
        else:
            rate = base_rate / 60.0

        inter_arrival = np.random.exponential(1.0 / rate)
        current_time += inter_arrival

        if current_time >= duration_seconds:
            break

        restaurant_id = np.random.randint(0, len(restaurants))
        customer_location = _generate_customer_near_restaurant(
            restaurants[restaurant_id].location,
            config
        )

        order = Order(
            order_id=order_id,
            restaurant_id=restaurant_id,
            restaurant_location=restaurants[restaurant_id].location,
            diner_location=customer_location,
            placement_time=current_time
        )
        orders.append(order)
        order_id += 1

    return orders[:total_orders]


def _generate_scripted(config: Dict[str, Any], restaurants: List) -> List[Order]:
    """Generate explicitly scripted order sequence."""
    demand_config = config['demand']
    scripted_orders = demand_config.get('scripted', {}).get('orders', [])

    orders = []
    for i, order_spec in enumerate(scripted_orders):
        restaurant_id = order_spec['restaurant_id']
        customer_location = tuple(order_spec['customer_location'])
        placement_time = order_spec['time']

        order = Order(
            order_id=i,
            restaurant_id=restaurant_id,
            restaurant_location=restaurants[restaurant_id].location,
            diner_location=customer_location,
            placement_time=placement_time
        )
        orders.append(order)

    return orders


def _generate_manual(config: Dict[str, Any], restaurants: List) -> List[Order]:
    """
    Generate manually specified orders with full control over all parameters.

    Supports custom meal prep times and expiration times per order.
    """
    demand_config = config['demand']
    manual_orders = demand_config.get('manual', {}).get('orders', [])
    physics_config = config['physics']
    default_meal_prep = physics_config.get('meal_prep_time_s', 300)

    orders = []
    for i, order_spec in enumerate(manual_orders):
        restaurant_index = order_spec['restaurant_index']
        customer_location = tuple(order_spec['customer_location'])
        placement_time = order_spec['placement_time']
        meal_prep_time = order_spec.get('meal_prep_time', default_meal_prep)

        order = Order(
            order_id=i,
            restaurant_id=restaurant_index,
            restaurant_location=restaurants[restaurant_index].location,
            diner_location=customer_location,
            placement_time=placement_time,
            meal_prep_time=meal_prep_time
        )
        orders.append(order)

    return orders


# =============================================================================
# HELPER FUNCTIONS
# =============================================================================

def _generate_customer_near_restaurant(restaurant_loc: Tuple[float, float],
                                       config: Dict[str, Any]) -> Tuple[float, float]:
    """
    Generate customer location in ring around restaurant.

    Args:
        restaurant_loc: Restaurant (x, y) in km
        config: Configuration dictionary

    Returns:
        Customer (x, y) location in km
    """
    map_size_km = config['physics']['map_size_m'] / 1000.0

    # Ring parameters: 1-2 km from restaurant
    min_radius = 1.0
    max_radius = 2.0

    angle = np.random.uniform(0, 2 * np.pi)
    radius = np.random.uniform(min_radius, max_radius)

    x = restaurant_loc[0] + radius * np.cos(angle)
    y = restaurant_loc[1] + radius * np.sin(angle)

    # Clamp to map boundaries
    x = np.clip(x, 0.1, map_size_km - 0.1)
    y = np.clip(y, 0.1, map_size_km - 0.1)

    return (x, y)


def _generate_customer_north_side(config: Dict[str, Any]) -> Tuple[float, float]:
    """Generate customer location on north side of river."""
    map_size_km = config['physics']['map_size_m'] / 1000.0
    boundary_y = config.get('geography', {}).get('river_y_position', map_size_km / 2) / 1000.0

    x = np.random.uniform(0.5, map_size_km - 0.5)
    y = np.random.uniform(boundary_y + 0.2, map_size_km - 0.5)

    return (x, y)


def _generate_customer_south_side(config: Dict[str, Any]) -> Tuple[float, float]:
    """Generate customer location on south side of river."""
    map_size_km = config['physics']['map_size_m'] / 1000.0
    boundary_y = config.get('geography', {}).get('river_y_position', map_size_km / 2) / 1000.0

    x = np.random.uniform(0.5, map_size_km - 0.5)
    y = np.random.uniform(0.5, boundary_y - 0.2)

    return (x, y)


def _identify_restaurant_clusters(restaurants: List, config: Dict[str, Any]) -> List[List[int]]:
    """
    Identify which restaurants belong to which geographic cluster.

    For scattered layout with 4 corner clusters.

    Returns:
        List of lists, where each inner list is restaurant IDs in that cluster
    """
    restaurant_config = config.get('restaurants', {})

    if restaurant_config.get('layout') == 'scattered':
        num_clusters = restaurant_config.get('scattered', {}).get('num_clusters', 4)
        restaurants_per_cluster = len(restaurants) // num_clusters

        clusters = []
        for i in range(num_clusters):
            start_idx = i * restaurants_per_cluster
            end_idx = start_idx + restaurants_per_cluster if i < num_clusters - 1 else len(restaurants)
            clusters.append(list(range(start_idx, end_idx)))

        return clusters
    else:
        # Fallback: all restaurants in one cluster
        return [list(range(len(restaurants)))]
