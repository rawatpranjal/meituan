"""
Assignment Algorithms for Food Delivery Routing

This module implements four batched assignment strategies:
1. Greedy: Iterative order-first, nearest courier (1-to-1)
2. Hungarian: Optimal bipartite matching (1-to-1)
3. Simple Bundling: Group by restaurant + Hungarian matching
4. Route Cost Bundling: Generate bundles + full route cost + Hungarian matching
"""

import numpy as np
from scipy.optimize import linear_sum_assignment
from typing import List, Tuple, Dict, Optional
from simulator_core import (
    SimulationState, Courier, Order,
    euclidean_distance, get_distance, get_travel_time,
    PICKUP_SERVICE_TIME, DROPOFF_SERVICE_TIME
)


# ============================================================================
# ALGORITHM 1: BATCHED GREEDY (ORDER-FIRST, NEAREST COURIER)
# ============================================================================

def assign_greedy(state: SimulationState, idle_couriers: List[Courier],
                  ready_orders: List[Order]) -> List[Tuple[int, List[int]]]:
    """
    Greedy assignment: For each order, find the nearest available courier.

    Orders are processed sequentially in order of ready_time (oldest first).
    This makes the algorithm predictable and scientifically consistent.

    Returns:
        List of (courier_id, [order_id]) assignments
    """
    assignments = []
    available_couriers = list(idle_couriers)

    # Sort orders by ready_time to ensure consistent, deterministic behavior
    pending_orders = sorted(ready_orders, key=lambda o: o.ready_time)

    for order in pending_orders:
        if not available_couriers:
            break

        # Find nearest courier (by travel time)
        best_courier = None
        min_time = float('inf')

        for courier in available_couriers:
            travel_time = get_travel_time(courier.current_location, order.restaurant_location)
            if travel_time < min_time:
                min_time = travel_time
                best_courier = courier

        if best_courier:
            assignments.append((best_courier.id, [order.id]))
            available_couriers.remove(best_courier)

    return assignments


# ============================================================================
# ALGORITHM 2: BATCHED HUNGARIAN (OPTIMAL 1-TO-1 MATCHING)
# ============================================================================

def assign_hungarian(state: SimulationState, idle_couriers: List[Courier],
                     ready_orders: List[Order]) -> List[Tuple[int, List[int]]]:
    """
    Hungarian Route-Aware algorithm for optimal 1-to-1 matching.

    Key Innovation: Unlike the traditional Hungarian which only considers
    time-to-pickup, this version considers the FULL delivery commitment:
    - Travel to restaurant
    - Pickup service time (90s)
    - Travel to customer
    - Dropoff service time (45s)

    This makes globally optimal single-delivery assignments by choosing
    assignments that minimize total completion time, not just pickup time.

    Example: Will choose a far pickup with nearby delivery over a near pickup
    with far delivery if the total completion time is shorter.

    Returns:
        List of (courier_id, [order_id]) assignments
    """
    if not idle_couriers or not ready_orders:
        return []

    assignments = []

    # Build cost matrix: rows = couriers, cols = orders
    num_couriers = len(idle_couriers)
    num_orders = len(ready_orders)

    # Create cost matrix
    cost_matrix = np.zeros((num_couriers, num_orders))

    for i, courier in enumerate(idle_couriers):
        for j, order in enumerate(ready_orders):
            # Cost = FULL route duration including pickup and delivery
            cost_matrix[i, j] = calculate_route_duration(
                courier.current_location,
                [order.id],
                state,
                use_tsp_optimization=False,  # Single order, no TSP needed
                include_service_times=True   # Include 90s pickup + 45s dropoff
            )

    # Handle unbalanced assignment (more orders than couriers or vice versa)
    if num_couriers > num_orders:
        # More couriers than orders: add dummy orders with zero cost
        dummy_cols = num_couriers - num_orders
        dummy_cost = np.zeros((num_couriers, dummy_cols))
        cost_matrix = np.hstack([cost_matrix, dummy_cost])
    elif num_orders > num_couriers:
        # More orders than couriers: add dummy couriers with high cost
        dummy_rows = num_orders - num_couriers
        BIG_M = 1e9
        dummy_cost = np.full((dummy_rows, num_orders), BIG_M)
        cost_matrix = np.vstack([cost_matrix, dummy_cost])

    # Solve assignment problem
    row_ind, col_ind = linear_sum_assignment(cost_matrix)

    # Extract real assignments (filter out dummy assignments)
    for courier_idx, order_idx in zip(row_ind, col_ind):
        if courier_idx < num_couriers and order_idx < num_orders:
            # Check cost is not dummy penalty
            if cost_matrix[courier_idx, order_idx] < 1e8:
                courier = idle_couriers[courier_idx]
                order = ready_orders[order_idx]
                assignments.append((courier.id, [order.id]))

    return assignments


# ============================================================================
# ALGORITHM 3: SIMPLE BUNDLING (GROUP BY RESTAURANT + HUNGARIAN)
# ============================================================================

def _generate_partitions(items: List[int], max_size: int = 3) -> List[List[List[int]]]:
    """
    Generate all partitions of items where each part has ≤ max_size elements.

    Args:
        items: List of items to partition
        max_size: Maximum size of each partition part

    Returns:
        List of partitions, where each partition is a list of non-overlapping bundles
    """
    if not items:
        return [[]]

    if len(items) == 1:
        return [[[items[0]]]]

    result = []
    first = items[0]
    rest = items[1:]

    # For each partition of the rest
    for partition in _generate_partitions(rest, max_size):
        # Option 1: Put first element in its own bundle
        result.append([[first]] + partition)

        # Option 2: Add first element to an existing bundle (if it doesn't exceed max_size)
        for i, bundle in enumerate(partition):
            if len(bundle) < max_size:
                new_partition = [bundle + [first] if j == i else list(b) for j, b in enumerate(partition)]
                result.append(new_partition)

    return result


def _find_best_partition(candidates: List[List[int]], all_order_ids: List[int],
                        couriers: List[Courier], state: SimulationState) -> List[List[int]]:
    """
    Find best non-overlapping partition of orders into bundles.

    For small order counts (≤7), enumerates ALL valid partitions and picks the one
    with lowest cost (as estimated by Hungarian assignment). For larger counts,
    falls back to heuristic strategies.

    Args:
        candidates: All possible bundles (not used, kept for compatibility)
        all_order_ids: All order IDs that must be covered
        couriers: Available couriers (used for cost estimation)
        state: Simulation state

    Returns:
        Best partition (list of non-overlapping bundles)
    """
    MAX_ORDERS_FOR_FULL_ENUMERATION = 7  # 7 orders → ~877 partitions (manageable)

    def estimate_partition_cost(partition):
        """
        Estimate REALISTIC total cost of partition using Hungarian assignment.

        This accounts for courier competition - bundles don't get their "best"
        courier, but rather the optimal global assignment across all bundles.
        """
        num_couriers_local = len(couriers)
        num_bundles = len(partition)

        # Build cost matrix: rows=couriers, cols=bundles
        cost_matrix = np.zeros((num_couriers_local, num_bundles))

        for i, courier in enumerate(couriers):
            for j, bundle in enumerate(partition):
                cost_matrix[i, j] = calculate_route_duration(
                    courier.current_location,
                    bundle,
                    state,
                    use_tsp_optimization=(len(bundle) > 1),
                    include_service_times=True
                )

        # Handle unbalanced case
        if num_couriers_local > num_bundles:
            # More couriers than bundles - pad with zeros
            dummy_cols = num_couriers_local - num_bundles
            cost_matrix = np.hstack([cost_matrix, np.zeros((num_couriers_local, dummy_cols))])
        elif num_bundles > num_couriers_local:
            # More bundles than couriers - pad with high cost
            dummy_rows = num_bundles - num_couriers_local
            cost_matrix = np.vstack([cost_matrix, np.full((dummy_rows, num_bundles), 1e9)])

        # Solve Hungarian to get REALISTIC cost
        row_ind, col_ind = linear_sum_assignment(cost_matrix)

        # Sum only real assignments (not dummy)
        total_cost = 0
        for r, c in zip(row_ind, col_ind):
            if r < num_couriers_local and c < num_bundles:
                if cost_matrix[r, c] < 1e8:
                    total_cost += cost_matrix[r, c]

        return total_cost

    # For small order counts, enumerate all valid partitions
    if len(all_order_ids) <= MAX_ORDERS_FOR_FULL_ENUMERATION:
        all_partitions = _generate_partitions(all_order_ids, max_size=3)

        best_partition = None
        best_cost = float('inf')

        for partition in all_partitions:
            cost = estimate_partition_cost(partition)
            if cost < best_cost:
                best_cost = cost
                best_partition = partition

        return best_partition

    # For large order counts, use heuristic strategies
    strategies = []

    # Strategy 1: All singles (equivalent to Hungarian)
    strategies.append([[oid] for oid in all_order_ids])

    # Strategy 2: Greedy max bundles
    max_bundles = []
    for i in range(0, len(all_order_ids), 3):
        max_bundles.append(all_order_ids[i:i+3])
    strategies.append(max_bundles)

    # Strategy 3: Balanced (if > 3 orders)
    if len(all_order_ids) >= 4:
        n = len(all_order_ids)
        # Try to balance bundle sizes
        if n % 2 == 0:
            # Split into pairs
            balanced = []
            for i in range(0, n, 2):
                balanced.append(all_order_ids[i:i+2])
            strategies.append(balanced)

    # Evaluate all strategies
    best_partition = strategies[0]
    best_cost = estimate_partition_cost(best_partition)

    for partition in strategies[1:]:
        cost = estimate_partition_cost(partition)
        if cost < best_cost:
            best_cost = cost
            best_partition = partition

    return best_partition


def assign_simple_bundling(state: SimulationState, idle_couriers: List[Courier],
                          ready_orders: List[Order]) -> List[Tuple[int, List[int]]]:
    """
    Simple Bundling Route-Aware: Smart same-restaurant consolidation with optimal bundling.

    Key Innovation: Generates ALL possible bundle candidates (sizes 1-3) from same restaurant,
    then intelligently selects non-overlapping bundles that minimize total route cost.
    This ensures Simple Bundling is ALWAYS >= Hungarian (since singles are an option).

    Algorithm:
    1. Group orders by restaurant_id
    2. For each restaurant, generate all bundle candidates (combinations of 1, 2, 3 orders)
    3. Select best non-overlapping bundle set per restaurant
    4. Use Hungarian algorithm to assign bundles to couriers optimally

    This properly solves the set packing problem rather than greedily forcing bundles.

    Returns:
        List of (courier_id, [order_id1, order_id2, ...]) assignments
    """
    MAX_BUNDLE_SIZE = 3  # Realistic courier capacity limit

    if not idle_couriers or not ready_orders:
        return []

    # Step 1: Group orders by restaurant
    orders_by_restaurant = {}
    for order in ready_orders:
        if order.restaurant_id not in orders_by_restaurant:
            orders_by_restaurant[order.restaurant_id] = []
        orders_by_restaurant[order.restaurant_id].append(order)

    # Step 2: For each restaurant, find best bundling strategy
    bundles = []

    for restaurant_id, restaurant_orders in orders_by_restaurant.items():
        # Sort by ready time
        restaurant_orders.sort(key=lambda o: o.ready_time)
        order_ids = [o.id for o in restaurant_orders]

        # Find best partition (tries singles, max bundles, balanced)
        best_partition = _find_best_partition([], order_ids, idle_couriers, state)

        # Add bundles from best partition
        for bundle_order_ids in best_partition:
            bundle = {
                'restaurant_id': restaurant_id,
                'restaurant_location': restaurant_orders[0].restaurant_location,
                'order_ids': bundle_order_ids,
                'bundle_ready_time': max(state.orders[oid].ready_time for oid in bundle_order_ids)
            }
            bundles.append(bundle)

    if not bundles:
        return []

    # Step 3: Build cost matrix for courier-to-bundle assignment
    num_couriers = len(idle_couriers)
    num_bundles = len(bundles)

    cost_matrix = np.zeros((num_couriers, num_bundles))

    for i, courier in enumerate(idle_couriers):
        for j, bundle in enumerate(bundles):
            # Cost = FULL route duration for the bundle with TSP optimization
            bundle_order_ids = bundle['order_ids']
            base_cost = calculate_route_duration(
                courier.current_location,
                bundle_order_ids,
                state,
                use_tsp_optimization=(len(bundle_order_ids) > 1),   # Only optimize multi-order bundles
                include_service_times=True   # Include realistic service times
            )

            # Use pure route duration cost (no artificial bundling incentive)
            cost_matrix[i, j] = base_cost

    # Handle unbalanced assignment
    if num_couriers > num_bundles:
        dummy_cols = num_couriers - num_bundles
        dummy_cost = np.zeros((num_couriers, dummy_cols))
        cost_matrix = np.hstack([cost_matrix, dummy_cost])
    elif num_bundles > num_couriers:
        dummy_rows = num_bundles - num_couriers
        BIG_M = 1e9
        dummy_cost = np.full((dummy_rows, num_bundles), BIG_M)
        cost_matrix = np.vstack([cost_matrix, dummy_cost])

    # Solve assignment problem
    row_ind, col_ind = linear_sum_assignment(cost_matrix)

    # Extract real assignments
    assignments = []
    for courier_idx, bundle_idx in zip(row_ind, col_ind):
        if courier_idx < num_couriers and bundle_idx < num_bundles:
            if cost_matrix[courier_idx, bundle_idx] < 1e8:
                courier = idle_couriers[courier_idx]
                bundle = bundles[bundle_idx]
                assignments.append((courier.id, bundle['order_ids']))

    return assignments


# ============================================================================
# ALGORITHM 4: FLEXIBLE BUNDLING (SINGLE + MULTI BUNDLE OPTIMIZATION)
# ============================================================================

def calculate_route_duration(courier_location: Tuple[float, float],
                            order_ids: List[int],
                            state: SimulationState,
                            use_tsp_optimization: bool = True,
                            include_service_times: bool = True) -> float:
    """
    Calculate total duration for a courier to complete a bundle of orders.

    Route calculation:
    1. Travel to restaurant(s) - optimized sequence if multiple
    2. Service time at each pickup
    3. Travel to all dropoff locations - optimized using TSP
    4. Service time at each dropoff

    Args:
        courier_location: Courier's current location
        order_ids: List of order IDs to deliver
        state: Current simulation state
        use_tsp_optimization: Whether to optimize delivery sequence (default True)
        include_service_times: Whether to include service times (default True)

    Returns:
        Total time in seconds
    """
    if not order_ids:
        return 0.0

    orders = [state.orders[oid] for oid in order_ids]

    # Group orders by restaurant
    orders_by_restaurant = {}
    for order in orders:
        if order.restaurant_id not in orders_by_restaurant:
            orders_by_restaurant[order.restaurant_id] = []
        orders_by_restaurant[order.restaurant_id].append(order)

    total_time = 0.0
    current_location = courier_location

    # Handle multiple restaurants (optimize pickup sequence)
    if len(orders_by_restaurant) > 1:
        # Get unique restaurant locations
        restaurant_locations = [orders[0].restaurant_location
                               for orders in orders_by_restaurant.values()]

        # Optimize pickup sequence
        if use_tsp_optimization:
            pickup_sequence = optimize_pickup_sequence(current_location, restaurant_locations)
        else:
            pickup_sequence = list(range(len(restaurant_locations)))

        # Calculate time for all pickups
        for idx in pickup_sequence:
            restaurant_loc = restaurant_locations[idx]
            total_time += get_travel_time(current_location, restaurant_loc)
            if include_service_times:
                total_time += PICKUP_SERVICE_TIME  # Add service time at each restaurant
            current_location = restaurant_loc

    else:
        # Single restaurant case
        restaurant_location = orders[0].restaurant_location
        total_time += get_travel_time(current_location, restaurant_location)
        if include_service_times:
            total_time += PICKUP_SERVICE_TIME  # Add service time at restaurant
        current_location = restaurant_location

    # Calculate optimized delivery time
    dropoff_locations = [order.diner_location for order in orders]

    if use_tsp_optimization and len(dropoff_locations) > 1:
        # Get optimal delivery sequence
        delivery_sequence = optimize_delivery_sequence(current_location, dropoff_locations)

        # Calculate delivery time using optimized sequence
        for idx in delivery_sequence:
            total_time += get_travel_time(current_location, dropoff_locations[idx])
            if include_service_times:
                total_time += DROPOFF_SERVICE_TIME  # Add service time at each dropoff
            current_location = dropoff_locations[idx]
    else:
        # No optimization or single delivery
        for location in dropoff_locations:
            total_time += get_travel_time(current_location, location)
            if include_service_times:
                total_time += DROPOFF_SERVICE_TIME  # Add service time at each dropoff
            current_location = location

    return total_time


def optimize_delivery_sequence(start_location: Tuple[float, float],
                              dropoff_locations: List[Tuple[float, float]]) -> List[int]:
    """
    Find optimal sequence to visit all dropoff locations using TSP solver.

    For small instances (n <= 8), uses exact solution via permutations.
    For larger instances, uses nearest neighbor heuristic.

    Args:
        start_location: Starting (x, y) coordinates
        dropoff_locations: List of (x, y) coordinates to visit

    Returns:
        List of indices representing optimal visit order
    """
    n = len(dropoff_locations)

    if n <= 1:
        return list(range(n))

    # For small instances, find exact solution
    if n <= 8:
        from itertools import permutations

        best_sequence = None
        min_distance = float('inf')

        # Try all possible permutations
        for perm in permutations(range(n)):
            total_distance = 0
            current = start_location

            for idx in perm:
                total_distance += get_distance(current, dropoff_locations[idx])
                current = dropoff_locations[idx]

            if total_distance < min_distance:
                min_distance = total_distance
                best_sequence = list(perm)

        return best_sequence

    # For larger instances, use nearest neighbor heuristic
    else:
        unvisited = set(range(n))
        sequence = []
        current = start_location

        while unvisited:
            # Find nearest unvisited location
            nearest_idx = None
            min_dist = float('inf')

            for idx in unvisited:
                dist = get_distance(current, dropoff_locations[idx])
                if dist < min_dist:
                    min_dist = dist
                    nearest_idx = idx

            sequence.append(nearest_idx)
            unvisited.remove(nearest_idx)
            current = dropoff_locations[nearest_idx]

        return sequence


def optimize_pickup_sequence(start_location: Tuple[float, float],
                            pickup_locations: List[Tuple[float, float]]) -> List[int]:
    """
    Find optimal sequence to visit all pickup locations (restaurants).
    Uses same TSP logic as optimize_delivery_sequence.

    Args:
        start_location: Starting (x, y) coordinates
        pickup_locations: List of restaurant (x, y) coordinates

    Returns:
        List of indices representing optimal pickup order
    """
    # Reuse the same TSP logic for pickups
    return optimize_delivery_sequence(start_location, pickup_locations)


def generate_bundle_candidates(ready_orders: List[Order], max_bundle_size: int = 3) -> List[List[int]]:
    """
    Generate ALL possible bundle combinations from ready orders.

    No geographic filtering - pure combinatorial approach:
    - All single-order bundles
    - All 2-order combinations (any restaurants)
    - All 3-order combinations (any restaurants)

    The Hungarian solver will pick the best combinations based on pure route cost.

    Returns:
        List of bundles, where each bundle is a list of order_ids
    """
    from itertools import combinations

    bundles = []
    order_ids = [o.id for o in ready_orders]

    # Generate all combinations from size 1 to max_bundle_size
    for size in range(1, min(max_bundle_size + 1, len(order_ids) + 1)):
        for combo in combinations(order_ids, size):
            bundles.append(list(combo))

    return bundles


def generate_geographic_bundles(ready_orders: List[Order],
                                max_bundle_size: int = 3,
                                max_pickup_radius: float = 1000,
                                max_dropoff_radius: float = 2000) -> List[List[int]]:
    """
    Generate geographically coherent bundles using radius-based clustering.

    This is the "Smart Clustering" approach that creates bundles where:
    - Restaurants are geographically clustered (within max_pickup_radius)
    - Customers are geographically clustered (within max_dropoff_radius)
    - Bundle sizes respect max_bundle_size constraint

    Unlike combinatorial generation which creates ALL possible combinations,
    this function only creates geographically sensible bundles, dramatically
    reducing the search space while maintaining solution quality.

    Strategy:
    1. Cluster orders by restaurant proximity (max_pickup_radius meters)
    2. Within each restaurant cluster, sub-cluster by customer proximity (max_dropoff_radius)
    3. Create multi-order bundles (size 2+) from geographically coherent groups
    4. Add all single-order bundles to give solver flexibility

    Args:
        ready_orders: List of orders available for bundling
        max_bundle_size: Maximum orders per bundle (default: 3)
        max_pickup_radius: Maximum distance between restaurants in a bundle (meters, default: 1000)
        max_dropoff_radius: Maximum distance between customers in a bundle (meters, default: 2000)

    Returns:
        List of bundles, where each bundle is a list of order_ids
    """
    from collections import defaultdict

    if not ready_orders:
        return []

    bundles = []

    # Step 1: Cluster orders by restaurant proximity using greedy clustering
    restaurant_clusters = []
    processed = set()

    for i, order in enumerate(ready_orders):
        if i in processed:
            continue

        # Start new restaurant cluster with this order
        cluster = [order]
        processed.add(i)

        # Find all orders with restaurants within pickup radius
        for j, other_order in enumerate(ready_orders):
            if j in processed:
                continue

            # Check if this order's restaurant is close to ANY restaurant in the cluster
            for cluster_order in cluster:
                dist = euclidean_distance(
                    cluster_order.restaurant_location,
                    other_order.restaurant_location
                )
                # FIX: Convert meters to km for comparison (euclidean_distance returns km)
                if dist <= max_pickup_radius / 1000:
                    cluster.append(other_order)
                    processed.add(j)
                    break

        restaurant_clusters.append(cluster)

    # Step 2: Within each restaurant cluster, sub-cluster by customer proximity
    for restaurant_cluster in restaurant_clusters:
        # Sub-cluster customers within this restaurant cluster
        customer_subclusters = []
        cluster_processed = set()

        for i, order in enumerate(restaurant_cluster):
            if i in cluster_processed:
                continue

            # Start new customer subcluster
            subcluster = [order]
            cluster_processed.add(i)

            # Find nearby customers (up to max_bundle_size)
            for j, other_order in enumerate(restaurant_cluster):
                if j in cluster_processed or len(subcluster) >= max_bundle_size:
                    continue

                # Check if this customer is close to ANY customer in the subcluster
                for subcluster_order in subcluster:
                    dist = euclidean_distance(
                        subcluster_order.diner_location,
                        other_order.diner_location
                    )
                    # FIX: Convert meters to km for comparison (euclidean_distance returns km)
                    if dist <= max_dropoff_radius / 1000:
                        subcluster.append(other_order)
                        cluster_processed.add(j)
                        break

            customer_subclusters.append(subcluster)

        # Convert subclusters to bundles (only include multi-order bundles, size 2+)
        for subcluster in customer_subclusters:
            if len(subcluster) >= 2:
                bundles.append([o.id for o in subcluster])

    # Step 3: Add single-order bundles for solver flexibility
    # FIX: Only add single-order bundles for orders NOT already in multi-order bundles
    # This prevents the solver from cherry-picking single orders and leaving others unassigned
    bundled_orders = set()
    for bundle in bundles:
        bundled_orders.update(bundle)

    for order in ready_orders:
        if order.id not in bundled_orders:
            bundles.append([order.id])

    return bundles


def assign_network_bundling(state: SimulationState, idle_couriers: List[Courier],
                            ready_orders: List[Order]) -> List[Tuple[int, List[int]]]:
    """
    Algorithm 4: Network Bundling (Multi-Restaurant Intelligence)

    Key Innovation: Uses GEOGRAPHIC CLUSTERING to create intelligent
    multi-restaurant bundles, enabling network-level route optimization
    while avoiding combinatorial explosion.

    Strategy:
    1. Generate geographically coherent bundles (restaurants within 1000m, customers within 2000m)
    2. Automatically includes single-order bundles for flexibility
    3. Hungarian algorithm selects optimal non-overlapping assignment
    4. Uses pure route cost (no artificial bundling incentive)

    This differs from Simple Bundling which only bundles same-restaurant orders.
    Network Bundling intelligently finds multi-restaurant opportunities based
    on geographic proximity, not blind combinatorial search.

    Returns:
        List of (courier_id, [order_id1, ...]) assignments
    """
    if not idle_couriers or not ready_orders:
        return []

    # Step 1: Generate geographically coherent bundle candidates
    bundle_candidates = generate_geographic_bundles(ready_orders, max_bundle_size=3)

    if not bundle_candidates:
        return []

    # Step 2: Build cost matrix with route costs
    num_couriers = len(idle_couriers)
    num_bundles = len(bundle_candidates)

    cost_matrix = np.zeros((num_couriers, num_bundles))

    for i, courier in enumerate(idle_couriers):
        for j, bundle_order_ids in enumerate(bundle_candidates):
            # Cost = full route duration for this courier to complete this bundle
            base_cost = calculate_route_duration(
                courier.current_location,
                bundle_order_ids,
                state
            )

            # Use pure route duration cost (no artificial bundling incentive)
            cost_matrix[i, j] = base_cost

    # Handle unbalanced assignment
    if num_couriers > num_bundles:
        dummy_cols = num_couriers - num_bundles
        dummy_cost = np.zeros((num_couriers, dummy_cols))
        cost_matrix = np.hstack([cost_matrix, dummy_cost])
    elif num_bundles > num_couriers:
        dummy_rows = num_bundles - num_couriers
        BIG_M = 1e9
        dummy_cost = np.full((dummy_rows, num_bundles), BIG_M)
        cost_matrix = np.vstack([cost_matrix, dummy_cost])

    # Step 3: Solve assignment problem
    row_ind, col_ind = linear_sum_assignment(cost_matrix)

    # Extract real assignments
    assignments = []
    assigned_orders = set()  # Track which orders are already assigned

    for courier_idx, bundle_idx in zip(row_ind, col_ind):
        if courier_idx < num_couriers and bundle_idx < num_bundles:
            if cost_matrix[courier_idx, bundle_idx] < 1e8:
                courier = idle_couriers[courier_idx]
                bundle_order_ids = bundle_candidates[bundle_idx]

                # Check for conflicts (order already assigned in a different bundle)
                if not any(oid in assigned_orders for oid in bundle_order_ids):
                    assignments.append((courier.id, bundle_order_ids))
                    assigned_orders.update(bundle_order_ids)

    return assignments


# ============================================================================
# ALGORITHM 5: ANTICIPATED NETWORK BUNDLING (THE WORKHORSE)
# ============================================================================


def assign_anticipated_bundling(state: SimulationState, idle_couriers: List[Courier],
                                ready_orders: List[Order]) -> List[Tuple[int, List[int]]]:
    """
    Algorithm 5: Anticipated Network Bundling (The Workhorse)

    This algorithm combines ALL layers of intelligence:
    1. Anticipatory Horizon: Looks ahead 15 minutes for assignable orders
    2. Smart Network Intelligence: Generates geographically coherent multi-restaurant bundles
    3. Holistic Cost Function: Minimizes route duration + wait penalties + delay penalties

    Key Innovation: The holistic cost function accounts for:
    - Base route duration (travel + service times + TSP optimization)
    - Courier wait time (T_wait): time courier arrives before food is ready
    - Food freshness loss (T_delay): time food waits after being ready

    Cost = Route Duration + α·T_wait + β·T_delay

    This enables proactive dispatch: couriers can be assigned to orders before
    they're ready, if the wait time cost is justified by avoiding later delays.

    Uses geographic clustering (not combinatorial explosion) to create intelligent
    bundles where restaurants and customers are in close proximity.

    Returns:
        List of (courier_id, [order_id1, ...]) assignments
    """
    LOOKAHEAD_WINDOW = 300  # 5 minutes (focused on immediate demand, not distant future)
    MAX_BUNDLE_SIZE = 3
    ALPHA_PENALTY = 0.5  # Penalty multiplier for courier wait time (reduced to make early dispatch attractive)
    BETA_PENALTY = 0.3   # Penalty multiplier for food freshness loss (reduced to prevent stale order rejection)
    URGENCY_BONUS = 300  # Bonus (negative cost) for READY orders vs PENDING (5-minute equivalent)

    if not idle_couriers:
        return []

    # ========================================================================
    # STEP 1: ANTICIPATORY HORIZON
    # ========================================================================
    current_time = state.current_time
    assignable_orders = [
        o for o in state.orders.values()
        if o.state in ["PENDING", "READY"] and o.ready_time <= current_time + LOOKAHEAD_WINDOW
    ]

    if not assignable_orders:
        return []

    # ========================================================================
    # STEP 2: GEOGRAPHIC BUNDLE GENERATION (Smart Network Intelligence)
    # ========================================================================
    candidate_bundles_ids = generate_geographic_bundles(assignable_orders, max_bundle_size=MAX_BUNDLE_SIZE)

    candidate_bundles = []
    for bundle_ids in candidate_bundles_ids:
        bundle_orders = [state.orders[oid] for oid in bundle_ids]
        candidate_bundles.append({
            'order_ids': bundle_ids,
            'bundle_ready_time': max(o.ready_time for o in bundle_orders)
        })

    if not candidate_bundles:
        return []

    # ========================================================================
    # STEP 3: HOLISTIC COST MATRIX (Anticipatory Intelligence)
    # ========================================================================
    num_couriers = len(idle_couriers)
    num_bundles = len(candidate_bundles)
    cost_matrix = np.zeros((num_couriers, num_bundles))

    for i, courier in enumerate(idle_couriers):
        for j, bundle in enumerate(candidate_bundles):
            bundle_order_ids = bundle['order_ids']
            bundle_ready_time = bundle['bundle_ready_time']

            # Calculate courier arrival time at first pickup
            first_pickup_loc = state.orders[bundle_order_ids[0]].restaurant_location
            travel_to_first_pickup = get_travel_time(courier.current_location, first_pickup_loc)
            courier_arrival_time = current_time + travel_to_first_pickup

            # --- Three components of holistic cost ---

            # 1. Full Route Duration (TSP-optimized)
            route_duration = calculate_route_duration(
                courier.current_location,
                bundle_order_ids,
                state,
                use_tsp_optimization=True,
                include_service_times=True
            )

            # 2. Courier Wait Time (T_wait)
            T_wait = max(0, bundle_ready_time - courier_arrival_time)

            # 3. Food Freshness Loss (T_delay) - summed across all orders in bundle
            pickup_start_time = max(courier_arrival_time, bundle_ready_time)
            T_delay_total = 0
            for oid in bundle_order_ids:
                order = state.orders[oid]
                T_delay_total += max(0, pickup_start_time - order.ready_time)

            # 4. Urgency Bonus - prioritize READY orders over PENDING orders
            urgency_penalty = 0
            for oid in bundle_order_ids:
                if state.orders[oid].state == "READY":
                    urgency_penalty -= URGENCY_BONUS  # Negative cost = priority boost

            # --- Final Holistic Cost ---
            holistic_cost = route_duration + (ALPHA_PENALTY * T_wait) + (BETA_PENALTY * T_delay_total) + urgency_penalty
            cost_matrix[i, j] = holistic_cost

    # ========================================================================
    # STEP 4: SOLVE ASSIGNMENT PROBLEM
    # ========================================================================

    # Handle unbalanced matrix
    if num_couriers > num_bundles:
        dummy_cols = num_couriers - num_bundles
        cost_matrix = np.hstack([cost_matrix, np.zeros((num_couriers, dummy_cols))])
    elif num_bundles > num_couriers:
        dummy_rows = num_bundles - num_couriers
        cost_matrix = np.vstack([cost_matrix, np.full((dummy_rows, num_bundles), 1e9)])

    # Solve for minimum cost assignment
    row_ind, col_ind = linear_sum_assignment(cost_matrix)

    # Extract real assignments
    assignments = []
    assigned_orders = set()
    for courier_idx, bundle_idx in zip(row_ind, col_ind):
        if courier_idx < num_couriers and bundle_idx < num_bundles:
            if cost_matrix[courier_idx, bundle_idx] < 1e8:
                bundle_order_ids = candidate_bundles[bundle_idx]['order_ids']
                if not any(oid in assigned_orders for oid in bundle_order_ids):
                    assignments.append((idle_couriers[courier_idx].id, bundle_order_ids))
                    assigned_orders.update(bundle_order_ids)

    return assignments


# ============================================================================
# ALGORITHM REGISTRY
# ============================================================================

ALGORITHMS = {
    # Clean 5-Algorithm Hierarchy (ascending intelligence)
    'greedy': assign_greedy,                               # Level 1: No intelligence (baseline)
    'hungarian': assign_hungarian,                         # Level 2: Route intelligence
    'simple_bundling': assign_simple_bundling,             # Level 3: Same-restaurant bundling
    'network_bundling': assign_network_bundling,           # Level 4: Multi-restaurant bundling
    'anticipated_bundling': assign_anticipated_bundling    # Level 5: Anticipatory + network intelligence
}


def get_algorithm(name: str):
    """Get assignment algorithm by name."""
    if name not in ALGORITHMS:
        raise ValueError(f"Unknown algorithm: {name}. Available: {list(ALGORITHMS.keys())}")
    return ALGORITHMS[name]
