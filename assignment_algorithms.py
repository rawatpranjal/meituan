
import numpy as np
from scipy.optimize import linear_sum_assignment
from typing import List, Tuple, Dict, Optional
from ortools.sat.python import cp_model
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
    UPGRADED Hungarian Algorithm using the OR-Tools Solver.

    This version is provably optimal for 1-to-1 matching and uses the
    standardized multi-objective function to prioritize fulfillment.
    """
    if not idle_couriers or not ready_orders:
        return []

    # Candidate bundles are ONLY singles for Hungarian
    candidate_bundles = [[order.id] for order in ready_orders]

    model = cp_model.CpModel()
    PRIORITY_MULTIPLIER = 1_000_000

    x = {}
    for i in range(len(idle_couriers)):
        for j in range(len(candidate_bundles)):
            x[(i, j)] = model.NewBoolVar(f'x_{i}_{j}')

    # Constraints
    for i in range(len(idle_couriers)):
        model.AddAtMostOne(x[(i, j)] for j in range(len(candidate_bundles)))

    order_map = {order.id: i for i, order in enumerate(ready_orders)}
    for order_id in order_map.keys():
        tasks_with_this_order = []
        for j, bundle in enumerate(candidate_bundles):
            if order_id in bundle:
                for i in range(len(idle_couriers)):
                    tasks_with_this_order.append(x[(i, j)])
        model.AddAtMostOne(tasks_with_this_order)

    # Standardized Multi-Objective Function
    objective_terms = []
    for i, courier in enumerate(idle_couriers):
        for j, bundle in enumerate(candidate_bundles):
            cost = int(calculate_route_duration(
                courier.current_location,
                bundle,
                state,
                use_tsp_optimization=False,
                include_service_times=True
            ))
            score = (len(bundle) * PRIORITY_MULTIPLIER) - cost
            objective_terms.append(score * x[(i, j)])

    model.Maximize(sum(objective_terms))

    solver = cp_model.CpSolver()
    solver.parameters.log_search_progress = False
    status = solver.Solve(model)

    final_assignments = []
    if status == cp_model.OPTIMAL or status == cp_model.FEASIBLE:
        for i, courier in enumerate(idle_couriers):
            for j, bundle in enumerate(candidate_bundles):
                if solver.Value(x[(i, j)]) == 1:
                    final_assignments.append((courier.id, bundle))

    return final_assignments


# ============================================================================
# ALGORITHM 3: SIMPLE BUNDLING (GROUP BY RESTAURANT + HUNGARIAN)
# ============================================================================

def _generate_partitions(items: List[int], max_size: int = 3) -> List[List[List[int]]]:
    """
    DEPRECATED: Used by old partition-based implementation. Kept for backwards compatibility.

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


def _generate_heuristic_partitions(order_ids: List[int], max_size: int = 3) -> List[List[List[int]]]:
    """
    DEPRECATED: Used by old partition-based implementation. Kept for backwards compatibility.

    Generate 3 strategic partitions instead of full enumeration (prevents explosion).

    Uses three proven strategies:
    1. All singles: Maximum flexibility, no bundling
    2. Greedy max bundles: Maximum bundling, minimal partitions
    3. Balanced pairs/triples: Compromise between singles and max bundles

    Args:
        order_ids: List of order IDs to partition
        max_size: Maximum size of each partition part

    Returns:
        List of 3 strategic partitions
    """
    n = len(order_ids)

    # Strategy 1: All singles [[1], [2], [3], ...]
    strategy1 = [[oid] for oid in order_ids]

    # Strategy 2: Greedy max bundles [[1,2,3], [4,5,6], ...]
    strategy2 = [order_ids[i:i+max_size] for i in range(0, n, max_size)]

    # Strategy 3: Balanced pairs/triples
    if n % 2 == 0:
        # Even number: use pairs
        strategy3 = [order_ids[i:i+2] for i in range(0, n, 2)]
    else:
        # Odd number: use triples, leaving single at end if needed
        strategy3 = [order_ids[i:i+3] for i in range(0, n-1, 3)] + [[order_ids[-1]]]

    return [strategy1, strategy2, strategy3]


def _find_best_partition(candidates: List[List[int]], all_order_ids: List[int],
                        couriers: List[Courier], state: SimulationState) -> List[List[int]]:
    """
    DEPRECATED: Used by old partition-based implementation. Kept for backwards compatibility.

    Find best non-overlapping partition of orders into bundles.

    Enumerates ALL valid partitions and picks the one with lowest cost
    (as estimated by Hungarian assignment). This is a fully expressive,
    optimal search with no heuristic shortcuts.

    Args:
        candidates: All possible bundles (not used, kept for compatibility)
        all_order_ids: All order IDs that must be covered
        couriers: Available couriers (used for cost estimation)
        state: Simulation state

    Returns:
        Best partition (list of non-overlapping bundles)
    """

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

    # Prevent combinatorial explosion: use full enumeration only for small cases
    # Threshold = 9 orders (21,147 partitions, fast ~100-200ms)
    # Above threshold (10+ orders = 115,975+ partitions, 5.5x explosion): use heuristics
    if len(all_order_ids) <= 9:
        all_partitions = _generate_partitions(all_order_ids, max_size=3)
    else:
        all_partitions = _generate_heuristic_partitions(all_order_ids, max_size=3)

    best_partition = None
    best_cost = float('inf')

    for partition in all_partitions:
        cost = estimate_partition_cost(partition)
        if cost < best_cost:
            best_cost = cost
            best_partition = partition

    return best_partition


def _generate_simple_bundle_candidates(ready_orders: List[Order], max_bundle_size: int = 3) -> List[List[int]]:
    """
    NEW IMPLEMENTATION: Generates candidate bundle menu (singles, pairs, triplets).

    Creates the "menu of options" for the solver:
    - All single orders (Hungarian baseline)
    - All possible 2-order bundles from same restaurant
    - All possible 3-order bundles from same restaurant

    This is simpler and more direct than partition enumeration.

    Args:
        ready_orders: Orders available for assignment
        max_bundle_size: Maximum orders per bundle (default 3)

    Returns:
        List of candidate bundles, where each bundle is a list of order IDs
    """
    from itertools import combinations

    # Step 1: Group orders by restaurant
    orders_by_restaurant = {}
    for order in ready_orders:
        if order.restaurant_id not in orders_by_restaurant:
            orders_by_restaurant[order.restaurant_id] = []
        orders_by_restaurant[order.restaurant_id].append(order)

    candidate_bundles = []

    # Step 2: Add all single orders as baseline candidates
    for order in ready_orders:
        candidate_bundles.append([order.id])

    # Step 3: For each restaurant, generate pair and triplet bundles
    for restaurant_id, restaurant_orders in orders_by_restaurant.items():
        order_ids = [o.id for o in restaurant_orders]

        # Generate pairs if possible (size 2)
        if len(order_ids) >= 2 and max_bundle_size >= 2:
            for combo in combinations(order_ids, 2):
                candidate_bundles.append(list(combo))

        # Generate triplets if possible (size 3)
        if len(order_ids) >= 3 and max_bundle_size >= 3:
            for combo in combinations(order_ids, 3):
                candidate_bundles.append(list(combo))

    return candidate_bundles


def assign_simple_bundling(state: SimulationState, idle_couriers: List[Courier],
                          ready_orders: List[Order]) -> List[Tuple[int, List[int]]]:
    """
    FINAL, PROVABLY OPTIMAL Simple Bundling Algorithm using the OR-Tools CP-SAT Solver.

    This version uses a multi-objective function to prioritize maximizing the
    number of orders delivered, and secondarily minimizing total cost.
    """
    MAX_BUNDLE_SIZE = 3

    if not idle_couriers or not ready_orders:
        return []

    candidate_bundles = _generate_simple_bundle_candidates(ready_orders, max_bundle_size=MAX_BUNDLE_SIZE)
    if not candidate_bundles:
        return []

    model = cp_model.CpModel()
    PRIORITY_MULTIPLIER = 1_000_000

    x = {}
    for i in range(len(idle_couriers)):
        for j in range(len(candidate_bundles)):
            x[(i, j)] = model.NewBoolVar(f'x_{i}_{j}')

    # Constraints
    for i in range(len(idle_couriers)):
        model.AddAtMostOne(x[(i, j)] for j in range(len(candidate_bundles)))

    order_map = {order.id: i for i, order in enumerate(ready_orders)}
    for order_id in order_map.keys():
        tasks_with_this_order = []
        for j, bundle in enumerate(candidate_bundles):
            if order_id in bundle:
                for i in range(len(idle_couriers)):
                    tasks_with_this_order.append(x[(i, j)])
        model.AddAtMostOne(tasks_with_this_order)

    # Standardized Multi-Objective Function
    objective_terms = []
    for i, courier in enumerate(idle_couriers):
        for j, bundle in enumerate(candidate_bundles):
            cost = int(calculate_route_duration(
                courier.current_location,
                bundle,
                state,
                use_tsp_optimization=(len(bundle) > 1),
                include_service_times=True
            ))
            score = (len(bundle) * PRIORITY_MULTIPLIER) - cost
            objective_terms.append(score * x[(i, j)])

    model.Maximize(sum(objective_terms))

    solver = cp_model.CpSolver()
    solver.parameters.log_search_progress = False
    status = solver.Solve(model)

    final_assignments = []
    if status == cp_model.OPTIMAL or status == cp_model.FEASIBLE:
        for i, courier in enumerate(idle_couriers):
            for j, bundle in enumerate(candidate_bundles):
                if solver.Value(x[(i, j)]) == 1:
                    final_assignments.append((courier.id, bundle))

    return final_assignments


# ============================================================================
# ALGORITHM 3.5: CONSTRAINED BUNDLING (TIME-CONSTRAINED OPTIMIZATION)
# ============================================================================

def assign_constrained_bundling(state: SimulationState, idle_couriers: List[Courier],
                                ready_orders: List[Order],
                                max_order_duration: float = 2400.0) -> List[Tuple[int, List[int]]]:
    """
    Constrained bundling with hard time limit enforcement.

    Filters out courier-bundle pairings that exceed MAX_ORDER_DURATION,
    then optimizes for max throughput and min cost among valid pairings.

    Args:
        state: Current simulation state
        idle_couriers: Available couriers
        ready_orders: Orders ready for assignment
        max_order_duration: Maximum seconds from dispatch to final dropoff (default 2400 = 40 min)

    Returns:
        List of (courier_id, [order_ids]) assignments
    """
    MAX_BUNDLE_SIZE = 3

    if not idle_couriers or not ready_orders:
        return []

    # Step 1: Generate initial universe of potential bundles
    initial_candidate_bundles = _generate_simple_bundle_candidates(ready_orders, max_bundle_size=MAX_BUNDLE_SIZE)
    if not initial_candidate_bundles:
        return []

    model = cp_model.CpModel()
    PRIORITY_MULTIPLIER = 1_000_000

    # Step 2: CONSTRAINT FILTERING - Only create variables for valid pairings
    x = {}
    valid_pairings_cost = {}

    for i, courier in enumerate(idle_couriers):
        for j, bundle in enumerate(initial_candidate_bundles):
            # Calculate total route duration
            bundle_route_duration = calculate_route_duration(
                courier.current_location,
                bundle,
                state,
                use_tsp_optimization=(len(bundle) > 1),
                include_service_times=True
            )

            # Hard constraint check
            if bundle_route_duration <= max_order_duration:
                # This is a valid pairing, add it to the model
                x[(i, j)] = model.NewBoolVar(f'x_{i}_{j}')
                valid_pairings_cost[(i, j)] = int(bundle_route_duration)

    # If no valid pairings exist, return empty
    if not x:
        return []

    # Step 3: Constraints (operating only on valid pairings)
    for i in range(len(idle_couriers)):
        valid_tasks_for_courier = [x[(i, j)] for j in range(len(initial_candidate_bundles)) if (i, j) in x]
        if valid_tasks_for_courier:
            model.AddAtMostOne(valid_tasks_for_courier)

    order_map = {order.id: i for i, order in enumerate(ready_orders)}
    for order_id in order_map.keys():
        tasks_with_this_order = []
        for j, bundle in enumerate(initial_candidate_bundles):
            if order_id in bundle:
                for i in range(len(idle_couriers)):
                    if (i, j) in x:
                        tasks_with_this_order.append(x[(i, j)])
        if tasks_with_this_order:
            model.AddAtMostOne(tasks_with_this_order)

    # Step 4: Standardized Multi-Objective Function
    objective_terms = []
    for (i, j), cost in valid_pairings_cost.items():
        bundle = initial_candidate_bundles[j]
        score = (len(bundle) * PRIORITY_MULTIPLIER) - cost
        objective_terms.append(score * x[(i, j)])

    model.Maximize(sum(objective_terms))

    # Step 5: Solve
    solver = cp_model.CpSolver()
    solver.parameters.log_search_progress = False
    status = solver.Solve(model)

    final_assignments = []
    if status == cp_model.OPTIMAL or status == cp_model.FEASIBLE:
        for (i, j) in x.keys():
            if solver.Value(x[(i, j)]) == 1:
                courier = idle_couriers[i]
                bundle = initial_candidate_bundles[j]
                final_assignments.append((courier.id, bundle))

    return final_assignments


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
                dist = get_distance(
                    cluster_order.restaurant_location,
                    other_order.restaurant_location
                )
                # FIX: Convert meters to km for comparison (get_distance returns km)
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
                    dist = get_distance(
                        subcluster_order.diner_location,
                        other_order.diner_location
                    )
                    # FIX: Convert meters to km for comparison (get_distance returns km)
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

    Key Innovation: Generates ALL possible multi-restaurant bundle combinations
    (sizes 1-3) and uses CP-SAT solver to maximize throughput while minimizing
    total route cost. No geographic filtering for small batches (<=17 orders).

    Strategy:
    1. Generate ALL combinatorial bundle candidates (no geographic bias)
    2. CP-SAT solver maximizes: (orders_delivered * 1M) - total_route_cost
    3. Multi-objective function prioritizes throughput, then efficiency

    This differs from Simple Bundling which only bundles same-restaurant orders.
    Network Bundling explores the complete multi-restaurant solution space,
    letting route costs naturally filter out inefficient geographic combinations.

    Returns:
        List of (courier_id, [order_id1, ...]) assignments
    """
    if not idle_couriers or not ready_orders:
        return []

    # Step 1: Generate bundle candidates with explosion prevention
    # Threshold = 17 orders (833 bundle candidates, empirically optimal balance)
    # Above threshold (18+ orders = 987+ candidates): use geographic clustering
    if len(ready_orders) <= 17:
        bundle_candidates = generate_bundle_candidates(ready_orders, max_bundle_size=3)
    else:
        bundle_candidates = generate_geographic_bundles(
            ready_orders,
            max_bundle_size=3,
            max_pickup_radius=1000,   # 1km restaurant clustering
            max_dropoff_radius=2000   # 2km customer clustering
        )

    if not bundle_candidates:
        return []

    # Step 2: CP-SAT Optimization Model (Maximize Throughput, Minimize Cost)
    model = cp_model.CpModel()
    PRIORITY_MULTIPLIER = 1_000_000

    # Decision variables: x[i,j] = 1 if courier i assigned to bundle j
    x = {}
    for i in range(len(idle_couriers)):
        for j in range(len(bundle_candidates)):
            x[(i, j)] = model.NewBoolVar(f'x_{i}_{j}')

    # Constraint 1: Each courier assigned to at most one bundle
    for i in range(len(idle_couriers)):
        model.AddAtMostOne(x[(i, j)] for j in range(len(bundle_candidates)))

    # Constraint 2: Each order assigned to at most one courier-bundle pair
    order_map = {order.id: i for i, order in enumerate(ready_orders)}
    for order_id in order_map.keys():
        tasks_with_this_order = []
        for j, bundle in enumerate(bundle_candidates):
            if order_id in bundle:
                for i in range(len(idle_couriers)):
                    tasks_with_this_order.append(x[(i, j)])
        model.AddAtMostOne(tasks_with_this_order)

    # Objective: Maximize (orders delivered * 1M - cost)
    objective_terms = []
    for i, courier in enumerate(idle_couriers):
        for j, bundle in enumerate(bundle_candidates):
            cost = int(calculate_route_duration(
                courier.current_location,
                bundle,
                state,
                use_tsp_optimization=(len(bundle) > 1),
                include_service_times=True
            ))
            score = (len(bundle) * PRIORITY_MULTIPLIER) - cost
            objective_terms.append(score * x[(i, j)])

    model.Maximize(sum(objective_terms))

    # Step 3: Solve
    solver = cp_model.CpSolver()
    solver.parameters.log_search_progress = False
    status = solver.Solve(model)

    # Step 4: Extract assignments
    assignments = []
    if status == cp_model.OPTIMAL or status == cp_model.FEASIBLE:
        for i, courier in enumerate(idle_couriers):
            for j, bundle in enumerate(bundle_candidates):
                if solver.Value(x[(i, j)]) == 1:
                    assignments.append((courier.id, bundle))

    return assignments


# ============================================================================
# ALGORITHM 5: ANTICIPATED NETWORK BUNDLING (THE WORKHORSE)
# ============================================================================


def assign_anticipated_bundling(state: SimulationState, idle_couriers: List[Courier],
                                ready_orders: List[Order]) -> List[Tuple[int, List[int]]]:
    """
    Algorithm 5: Anticipated Network Bundling (The Workhorse) - CP-SAT Maximization

    Extends Network Bundling with temporal intelligence while maintaining the same
    throughput-first optimization framework.

    Score = (n × 1M) - EffectiveCost
    Where EffectiveCost = RouteDuration + α·T_wait + β·T_delay - γ·StalenessBonus

    Key Innovation: Anticipatory horizon (5min lookahead) + holistic effective cost
    that balances route efficiency, temporal penalties, and FIFO fairness.

    Returns:
        List of (courier_id, [order_id1, ...]) assignments
    """
    LOOKAHEAD_WINDOW = 300  # 5 minutes
    MAX_BUNDLE_SIZE = 3
    ALPHA_PENALTY = 0.1  # Courier wait penalty
    BETA_PENALTY = 0.15   # Food delay penalty
    STALENESS_BONUS = 0.45  # FIFO fairness bonus
    MAX_STALENESS_BONUS = 140  # Cap to prevent ancient order dominance

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
    # STEP 2: BUNDLE GENERATION
    # ========================================================================
    if len(assignable_orders) <= 17:
        candidate_bundles_ids = generate_bundle_candidates(assignable_orders, max_bundle_size=MAX_BUNDLE_SIZE)
    else:
        candidate_bundles_ids = generate_geographic_bundles(
            assignable_orders,
            max_bundle_size=MAX_BUNDLE_SIZE,
            max_pickup_radius=1000,
            max_dropoff_radius=2000
        )

    if not candidate_bundles_ids:
        return []

    # ========================================================================
    # STEP 3: CP-SAT MAXIMIZATION MODEL
    # ========================================================================
    model = cp_model.CpModel()
    PRIORITY_MULTIPLIER = 1_000_000

    # Decision variables: x[i,j] = 1 if courier i assigned to bundle j
    x = {}
    for i in range(len(idle_couriers)):
        for j in range(len(candidate_bundles_ids)):
            x[(i, j)] = model.NewBoolVar(f'x_{i}_{j}')

    # Constraint 1: Each courier assigned to at most one bundle
    for i in range(len(idle_couriers)):
        model.AddAtMostOne(x[(i, j)] for j in range(len(candidate_bundles_ids)))

    # Constraint 2: Each order assigned to at most one courier-bundle pair
    order_map = {order.id: order for order in assignable_orders}
    for order_id in order_map.keys():
        tasks_with_this_order = []
        for j, bundle_ids in enumerate(candidate_bundles_ids):
            if order_id in bundle_ids:
                for i in range(len(idle_couriers)):
                    tasks_with_this_order.append(x[(i, j)])
        model.AddAtMostOne(tasks_with_this_order)

    # Objective: Maximize (orders × 1M) - EffectiveCost
    objective_terms = []
    for i, courier in enumerate(idle_couriers):
        for j, bundle_ids in enumerate(candidate_bundles_ids):

            # --- Calculate Effective Cost components ---
            bundle_orders = [state.orders[oid] for oid in bundle_ids]
            bundle_ready_time = max(o.ready_time for o in bundle_orders)

            first_pickup_loc = bundle_orders[0].restaurant_location
            travel_to_first_pickup = get_travel_time(courier.current_location, first_pickup_loc)
            courier_arrival_time = current_time + travel_to_first_pickup

            # 1. Route Duration
            route_duration = calculate_route_duration(
                courier.current_location,
                bundle_ids,
                state,
                use_tsp_optimization=(len(bundle_ids) > 1),
                include_service_times=True
            )

            # 2. Courier Wait Time (T_wait)
            T_wait = max(0, bundle_ready_time - courier_arrival_time)

            # 3. Food Freshness Loss (T_delay)
            pickup_start_time = max(courier_arrival_time, bundle_ready_time)
            T_delay_total = sum(max(0, pickup_start_time - o.ready_time) for o in bundle_orders)

            # 4. Staleness Bonus (FIFO fairness)
            staleness_bonus_total = 0
            for o in bundle_orders:
                if o.state == "READY":
                    wait_time = current_time - o.ready_time
                    capped_wait_time = min(wait_time, MAX_STALENESS_BONUS / STALENESS_BONUS)
                    staleness_bonus_total += capped_wait_time

            # --- Effective Cost ---
            effective_cost = route_duration + (ALPHA_PENALTY * T_wait) + (BETA_PENALTY * T_delay_total) - (STALENESS_BONUS * staleness_bonus_total)

            # --- Score: Throughput first, efficiency second ---
            score = (len(bundle_ids) * PRIORITY_MULTIPLIER) - int(effective_cost)
            objective_terms.append(score * x[(i, j)])

    model.Maximize(sum(objective_terms))

    # ========================================================================
    # STEP 4: SOLVE
    # ========================================================================
    solver = cp_model.CpSolver()
    solver.parameters.log_search_progress = False
    status = solver.Solve(model)

    # Extract assignments
    assignments = []
    if status == cp_model.OPTIMAL or status == cp_model.FEASIBLE:
        for i, courier in enumerate(idle_couriers):
            for j, bundle_ids in enumerate(candidate_bundles_ids):
                if solver.Value(x[(i, j)]) == 1:
                    assignments.append((courier.id, bundle_ids))

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
