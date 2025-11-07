
import numpy as np
from scipy.optimize import linear_sum_assignment
from typing import List, Tuple, Dict, Optional
from ortools.sat.python import cp_model
from itertools import permutations
from simulator_core import (
    SimulationState, Courier, Order,
    manhattan_distance, get_distance, get_travel_time
)

# ============================================================================
# ALGORITHM 1: BATCHED GREEDY (ORDER-FIRST, NEAREST COURIER)
# ============================================================================

def _speed_mps(state: SimulationState) -> float:

    if hasattr(state, 'config') and state.config is not None and 'physics' in state.config:
        speed_kmh = state.config['physics']['courier_speed_kmh']
    else:
        # Fallback for tests that don't have config
        speed_kmh = 30.0
    return (speed_kmh * 1000.0) / 3600.0

def _get_service_times(state: SimulationState) -> Tuple[float, float]:

    if hasattr(state, 'config') and state.config is not None and 'physics' in state.config:
        pickup = state.config['physics']['pickup_service_time_s']
        dropoff = state.config['physics']['dropoff_service_time_s']
    else:
        # Fallback for tests that don't have config
        pickup = 90.0
        dropoff = 45.0
    return pickup, dropoff

def _manhattan_travel_time(loc1: Tuple[float, float], loc2: Tuple[float, float], state: SimulationState) -> float:

    dx_km = abs(loc1[0] - loc2[0])
    dy_km = abs(loc1[1] - loc2[1])
    manhattan_distance_km = dx_km + dy_km
    manhattan_distance_m = manhattan_distance_km * 1000.0
    speed_mps = _speed_mps(state)
    return manhattan_distance_m / speed_mps

def _single_edge_manhattan_finish_and_cost(
    state: SimulationState,
    courier: Courier,
    order: Order
) -> Optional[int]:

    now = state.current_time

    # STRICT READY-ONLY + STRICT PERISHABILITY GUARDS
    if order.ready_time > now:
        return None
    if order.expiration_time is None or order.expiration_time <= 0:
        return None
    if now > order.ready_time + order.expiration_time:
        return None

    # Manhattan travel to pickup (no waiting; item is READY by construction)
    t_to_pickup = _manhattan_travel_time(courier.current_location, order.restaurant_location, state)
    pickup_start = now + t_to_pickup

    pickup_service_time, dropoff_service_time = _get_service_times(state)

    t_pickup_to_drop = _manhattan_travel_time(order.restaurant_location, order.diner_location, state)
    finish_time = pickup_start + pickup_service_time + t_pickup_to_drop + dropoff_service_time

    deadline = order.ready_time + order.expiration_time
    if finish_time > deadline:
        return None

    total_duration = (pickup_start - now) + pickup_service_time + t_pickup_to_drop + dropoff_service_time
    return int(round(total_duration))

def _bundle_lex_code(bundle: List[int]) -> int:

    code = 0
    base = 1_000_000
    for oid in sorted(bundle):
        code = code * base + int(oid)
    return code

def _bundle_cost_if_feasible(
    state: SimulationState,
    courier: Courier,
    bundle_order_ids: List[int]
) -> Optional[int]:

    now = state.current_time
    if not bundle_order_ids:
        return 0

    orders = [state.orders[oid] for oid in bundle_order_ids]
    # Enforce single-restaurant bundles
    if len({o.restaurant_id for o in orders}) != 1:
        return None

    # STRICT READY-ONLY + STRICT PERISHABILITY PER ORDER
    for o in orders:
        if o.ready_time > now:
            return None
        if o.expiration_time is None or o.expiration_time <= 0:
            return None
        if now > o.ready_time + o.expiration_time:  # already expired
            return None

    # Travel to the restaurant
    rest_loc = orders[0].restaurant_location
    t_to_rest = _manhattan_travel_time(courier.current_location, rest_loc, state)

    # No waiting for future-ready items; all items must already be READY at 'now'
    arrival_time = now + t_to_rest
    pickup_start = arrival_time

    pickup_service_time, dropoff_service_time = _get_service_times(state)

    # One pickup service only for same-restaurant bundle
    time_cursor = pickup_start + pickup_service_time
    cur_loc = rest_loc

    # Optimal drop sequence among ≤3 orders by brute-force permutations
    best_total = None
    feasible = False

    for seq in permutations(orders):
        t = time_cursor
        loc = cur_loc
        ok = True

        for o in seq:
            t += _manhattan_travel_time(loc, o.diner_location, state)
            t += dropoff_service_time

            # Strict finite deadline
            deadline = o.ready_time + o.expiration_time
            if t > deadline:
                ok = False
                break

            loc = o.diner_location

        if ok:
            feasible = True
            if best_total is None or t - now < best_total:
                best_total = t - now

    if not feasible:
        return None

    return int(round(best_total))

def _simulate_bundle_route(state: SimulationState, courier_loc: Tuple[float, float],
                          order_ids: List[int], allow_wait: bool) -> Optional[Tuple[int, Dict[int, float]]]:

    if not order_ids:
        return 0, {}

    # Get config parameters
    pickup_service, dropoff_service = _get_service_times(state)
    now = state.current_time
    orders = [state.orders[oid] for oid in order_ids]

    # Group orders by restaurant
    restaurants = {}
    for order in orders:
        rest_id = order.restaurant_id
        if rest_id not in restaurants:
            restaurants[rest_id] = []
        restaurants[rest_id].append(order)

    # Build pickup sequence (TSP over restaurants)
    rest_locations = []
    rest_ids = []
    for rest_id, rest_orders in restaurants.items():
        # Use first order's restaurant location as representative
        rest_locations.append(rest_orders[0].restaurant_location)
        rest_ids.append(rest_id)

    # TSP for pickup sequence (exact for ≤8, nearest-neighbor for >8)
    if len(rest_locations) <= 8 and len(rest_locations) > 1:
        # Exact TSP via permutations
        from itertools import permutations
        best_seq = None
        best_time = float('inf')

        for perm in permutations(range(len(rest_locations))):
            time = 0
            loc = courier_loc
            for idx in perm:
                time += _manhattan_travel_time(loc, rest_locations[idx], state)
                loc = rest_locations[idx]
            if time < best_time:
                best_time = time
                best_seq = list(perm)
        pickup_sequence = best_seq
    elif len(rest_locations) > 1:
        # Nearest-neighbor for >8 restaurants
        remaining = set(range(len(rest_locations)))
        sequence = []
        current = courier_loc

        while remaining:
            nearest_idx = min(remaining,
                            key=lambda i: _manhattan_travel_time(current, rest_locations[i], state))
            sequence.append(nearest_idx)
            current = rest_locations[nearest_idx]
            remaining.remove(nearest_idx)
        pickup_sequence = sequence
    else:
        # Single restaurant
        pickup_sequence = [0]

    # Simulate pickup phase
    t = now
    current_loc = courier_loc
    picked_orders = []
    total_wait_time = 0  # Track cumulative wait time across all restaurants

    for idx in pickup_sequence:
        rest_id = rest_ids[idx]
        rest_loc = rest_locations[idx]
        rest_orders = restaurants[rest_id]

        # Travel to restaurant
        t += _manhattan_travel_time(current_loc, rest_loc, state)

        # Check if we need to wait for orders to be ready
        max_ready = max(o.ready_time for o in rest_orders)

        if allow_wait:
            # Anticipated bundling: wait if arrived early
            if t < max_ready:
                wait_time = max_ready - t
                total_wait_time += wait_time
                # Cap TOTAL waiting at 5 minutes across all restaurants
                if total_wait_time > 300:
                    return None  # Infeasible - too much cumulative waiting
                t = max_ready
        else:
            # Network bundling: reject if any order not ready
            if t < max_ready:
                return None  # Infeasible - can't pick up yet

        # Pickup service time (once per restaurant)
        t += pickup_service
        picked_orders.extend(rest_orders)
        current_loc = rest_loc

    # Delivery phase - TSP over all customer locations
    if len(orders) <= 8 and len(orders) > 1:
        # Exact TSP for deliveries
        from itertools import permutations
        best_seq = None
        best_time = float('inf')

        for perm in permutations(range(len(orders))):
            test_t = t
            test_loc = current_loc
            for idx in perm:
                test_t += _manhattan_travel_time(test_loc, orders[idx].diner_location, state)
                test_t += dropoff_service
                test_loc = orders[idx].diner_location
            if test_t < best_time:
                best_time = test_t
                best_seq = list(perm)
        delivery_sequence = best_seq
    elif len(orders) > 1:
        # Nearest-neighbor for >8 deliveries
        remaining = set(range(len(orders)))
        sequence = []
        current = current_loc

        while remaining:
            nearest_idx = min(remaining,
                            key=lambda i: _manhattan_travel_time(current, orders[i].diner_location, state))
            sequence.append(nearest_idx)
            current = orders[nearest_idx].diner_location
            remaining.remove(nearest_idx)
        delivery_sequence = sequence
    else:
        # Single delivery
        delivery_sequence = [0]

    # Simulate deliveries and check per-order deadlines
    drop_times = {}
    for idx in delivery_sequence:
        order = orders[idx]

        # Travel to customer
        t += _manhattan_travel_time(current_loc, order.diner_location, state)

        # Dropoff service
        t += dropoff_service

        # Check deadline
        deadline = order.ready_time + order.expiration_time
        if t > deadline:
            return None  # Infeasible - order would expire

        drop_times[order.id] = t
        current_loc = order.diner_location

    total_time = int(round(t - now))
    return total_time, drop_times

def _generate_2r_candidates(ready_orders: List[Order], max_bundle_size: int) -> List[List[int]]:

    from itertools import combinations

    # Start with all singles
    order_ids = [o.id for o in ready_orders]
    candidates = [[oid] for oid in order_ids]

    # Add bundles of size 2-3 with at most 2 unique restaurants
    for size in range(2, min(max_bundle_size + 1, len(order_ids) + 1)):
        for combo in combinations(range(len(order_ids)), size):
            bundle_orders = [ready_orders[i] for i in combo]
            unique_restaurants = set(o.restaurant_id for o in bundle_orders)

            # Only keep bundles with ≤2 unique restaurants
            if len(unique_restaurants) <= 2:
                candidates.append([order_ids[i] for i in combo])

    return candidates

def assign_greedy(state: SimulationState, idle_couriers: List[Courier],
                  ready_orders: List[Order]) -> List[Tuple[int, List[int]]]:

    assignments = []
    available_couriers = list(idle_couriers)
    now = state.current_time

    # Sort orders by ready_time to ensure consistent, deterministic behavior
    pending_orders = sorted(ready_orders, key=lambda o: o.ready_time)

    for order in pending_orders:
        if not available_couriers:
            break

        # Find feasible couriers who can meet the deadline
        feasible_couriers = []

        pickup_service_time, dropoff_service_time = _get_service_times(state)

        for courier in available_couriers:
            # Compute finish_time
            t_to_pickup = get_travel_time(courier.current_location, order.restaurant_location)
            t_pickup_to_dropoff = get_travel_time(order.restaurant_location, order.diner_location)
            finish_time = now + t_to_pickup + pickup_service_time + t_pickup_to_dropoff + dropoff_service_time

            # Check feasibility: finish_time <= order.ready_time + order.expiration_time
            if finish_time <= order.ready_time + order.expiration_time:
                feasible_couriers.append(courier)

        if not feasible_couriers:
            # No feasible courier for this order, skip it
            continue

        # Tie-break by Manhattan travel time to pickup (lowest wins)
        best_courier = None
        min_manhattan_time = float('inf')

        for courier in feasible_couriers:
            manhattan_time = _manhattan_travel_time(courier.current_location, order.restaurant_location, state)
            if manhattan_time < min_manhattan_time or (manhattan_time == min_manhattan_time and (best_courier is None or courier.id < best_courier.id)):
                min_manhattan_time = manhattan_time
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

    if not idle_couriers or not ready_orders:
        return []

    model = cp_model.CpModel()
    x: Dict[Tuple[int, int], cp_model.IntVar] = {}

    # Costs for objectives
    pickup_cost: Dict[Tuple[int, int], int] = {}          # Manhattan time-to-pickup (seconds, int)
    tie_weight: Dict[Tuple[int, int], int] = {}           # Deterministic small weight

    I = range(len(idle_couriers))
    J = range(len(ready_orders))

    # Build only feasible edges; compute Manhattan pickup time
    for i in I:
        ci = idle_couriers[i]
        for j in J:
            oj = ready_orders[j]
            # Use your existing feasibility function
            finish_cost = _single_edge_manhattan_finish_and_cost(state, ci, oj)
            if finish_cost is None:
                continue  # infeasible edge, skip

            var = model.NewBoolVar(f'x_{i}_{j}')
            x[(i, j)] = var

            # Manhattan time-to-pickup in seconds (int)
            t_pick = _manhattan_travel_time(ci.current_location, oj.restaurant_location, state)
            pickup_cost[(i, j)] = int(round(t_pick))

            # Deterministic tiebreak weight: prefer lower courier id, then order id
            # Scale order-id so courier precedence dominates
            tie_weight[(i, j)] = idle_couriers[i].id * 1_000_000 + ready_orders[j].id

    if not x:
        return []

    # At-most-one constraints
    for i in I:
        vars_i = [x[(i, j)] for j in J if (i, j) in x]
        if vars_i:
            model.AddAtMostOne(vars_i)

    for j in J:
        vars_j = [x[(i, j)] for i in I if (i, j) in x]
        if vars_j:
            model.AddAtMostOne(vars_j)

    solver = cp_model.CpSolver()
    solver.parameters.log_search_progress = False

    # ----- Pass 1: maximize cardinality -----
    total_assigned = sum(x[e] for e in x)
    model.Maximize(total_assigned)
    status = solver.Solve(model)
    if status not in (cp_model.OPTIMAL, cp_model.FEASIBLE):
        return []
    best_card = int(round(solver.Value(total_assigned)))

    # ----- Pass 2: fix cardinality, minimize total pickup time -----
    model.Add(total_assigned == best_card)
    total_pickup = sum(pickup_cost[e] * x[e] for e in x)
    model.Minimize(total_pickup)
    status = solver.Solve(model)
    if status not in (cp_model.OPTIMAL, cp_model.FEASIBLE):
        return []
    best_pickup = int(round(solver.Value(total_pickup)))

    # ----- Pass 3: fix both, minimize deterministic tie-weight -----
    model.Add(total_pickup == best_pickup)
    total_tie = sum(tie_weight[e] * x[e] for e in x)
    model.Minimize(total_tie)
    status = solver.Solve(model)
    if status not in (cp_model.OPTIMAL, cp_model.FEASIBLE):
        return []

    # Extract assignments in (courier_id, [order_id]) form
    final_assignments: List[Tuple[int, List[int]]] = []
    for (i, j), var in x.items():
        if solver.Value(var) == 1:
            final_assignments.append((idle_couriers[i].id, [ready_orders[j].id]))

    return final_assignments

# ============================================================================
# ALGORITHM 3: SIMPLE BUNDLING (GROUP BY RESTAURANT + HUNGARIAN)
# ============================================================================

def _generate_partitions(items: List[int], max_size: int = 3) -> List[List[List[int]]]:

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

    def estimate_partition_cost(partition):

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

def assign_simple_bundling(state: SimulationState,
                           idle_couriers: List[Courier],
                           ready_orders: List[Order]) -> List[Tuple[int, List[int]]]:

    # Get max bundle size from config if available, otherwise use hardcoded default
    if state.config and 'algorithms' in state.config and 'bundling' in state.config['algorithms']:
        MAX_BUNDLE_SIZE = state.config['algorithms']['bundling']['max_bundle_size']
    else:
        MAX_BUNDLE_SIZE = 3

    if not idle_couriers or not ready_orders:
        return []

    # STRICT READY-ONLY + STRICT PERISHABILITY FILTER
    now = state.current_time
    ready_orders = [
        o for o in ready_orders
        if o.state == "READY"
        and o.ready_time <= now
        and o.expiration_time is not None
        and o.expiration_time > 0
        and now <= o.ready_time + o.expiration_time  # not already expired
    ]
    if not ready_orders:
        return []

    # Candidate menu: singles + same-restaurant pairs/triples
    candidate_bundles = _generate_simple_bundle_candidates(ready_orders, max_bundle_size=MAX_BUNDLE_SIZE)
    if not candidate_bundles:
        return []

    model = cp_model.CpModel()

    I = range(len(idle_couriers))
    J = range(len(candidate_bundles))

    # Decision vars only for FEASIBLE edges; costs are Manhattan durations from now.
    x: Dict[Tuple[int, int], cp_model.IntVar] = {}
    edge_cost_sec: Dict[Tuple[int, int], int] = {}

    # Bundle metadata
    bundle_size = [len(candidate_bundles[j]) for j in J]

    # Build only feasible edges under Manhattan timing and expiries
    for i in I:
        courier = idle_couriers[i]
        for j in J:
            bundle = candidate_bundles[j]
            c = _bundle_cost_if_feasible(state, courier, bundle)
            if c is None:
                continue  # infeasible under deadlines or restaurant-mix
            var = model.NewBoolVar(f'x_{i}_{j}')
            x[(i, j)] = var
            edge_cost_sec[(i, j)] = int(c)

    if not x:
        return []

    # Each courier at most one bundle
    for i in I:
        vars_i = [x[(i, j)] for j in J if (i, j) in x]
        if vars_i:
            model.AddAtMostOne(vars_i)

    # Each order used at most once across all couriers and bundles
    order_ids = [o.id for o in ready_orders]
    for oid in order_ids:
        hits = []
        for j in J:
            if oid in candidate_bundles[j]:
                for i in I:
                    if (i, j) in x:
                        hits.append(x[(i, j)])
        if hits:
            model.AddAtMostOne(hits)

    # y[i] indicates whether courier i is used; for Objective 3
    y: Dict[int, cp_model.IntVar] = {}
    for i in I:
        yi = model.NewBoolVar(f'y_{i}')
        y[i] = yi
        # Link y[i] >= any x[i,j]
        vars_i = [x[(i, j)] for j in J if (i, j) in x]
        if vars_i:
            model.AddMaxEquality(yi, vars_i)
        else:
            # No edges for this courier → force to 0
            model.Add(yi == 0)

    solver = cp_model.CpSolver()
    solver.parameters.log_search_progress = False
    solver.parameters.num_search_workers = 1  # determinism

    # ----- Pass 1: maximize total orders -----
    total_orders = sum(bundle_size[j] * x[(i, j)] for (i, j) in x.keys())
    model.Maximize(total_orders)
    status = solver.Solve(model)
    if status not in (cp_model.OPTIMAL, cp_model.FEASIBLE):
        return []
    best_orders = int(solver.Value(total_orders))

    # ----- Pass 2: fix orders, minimize total Manhattan time -----
    model.Add(total_orders == best_orders)
    total_time = sum(edge_cost_sec[(i, j)] * x[(i, j)] for (i, j) in x.keys())
    model.Minimize(total_time)
    status = solver.Solve(model)
    if status not in (cp_model.OPTIMAL, cp_model.FEASIBLE):
        return []
    best_time = int(solver.Value(total_time))

    # ----- Pass 3: fix both, minimize number of couriers used -----
    model.Add(total_time == best_time)
    total_couriers_used = sum(y[i] for i in I)
    model.Minimize(total_couriers_used)
    status = solver.Solve(model)
    if status not in (cp_model.OPTIMAL, cp_model.FEASIBLE):
        return []

    # ----- Pass 4: deterministic tie-break (stable, but tiny weight) -----
    # Courier precedence dominates, then a lexicographic code of bundle ids.
    # Only matters when orders/time/couriers are identical.
    tie_weight = {}
    BASE = 1_000_000  # assumes order ids < 1e6 like _bundle_lex_code
    for (i, j) in x.keys():
        code = idle_couriers[i].id * BASE * BASE + _bundle_lex_code(candidate_bundles[j])
        tie_weight[(i, j)] = code

    model.Add(total_couriers_used == solver.Value(total_couriers_used))
    total_tie = sum(tie_weight[(i, j)] * x[(i, j)] for (i, j) in x.keys())
    model.Minimize(total_tie)
    status = solver.Solve(model)
    if status not in (cp_model.OPTIMAL, cp_model.FEASIBLE):
        return []

    # Extract chosen assignments
    final_assignments: List[Tuple[int, List[int]]] = []
    for (i, j), var in x.items():
        if solver.Value(var) == 1:
            final_assignments.append((idle_couriers[i].id, candidate_bundles[j]))

    return final_assignments

# ============================================================================
# ALGORITHM 3.5: CONSTRAINED BUNDLING (TIME-CONSTRAINED OPTIMIZATION)
# ============================================================================

def assign_constrained_bundling(state: SimulationState, idle_couriers: List[Courier],
                                ready_orders: List[Order],
                                max_order_duration: float = 2400.0) -> List[Tuple[int, List[int]]]:

    # Get max bundle size from config if available, otherwise use hardcoded default
    if state.config and 'algorithms' in state.config and 'bundling' in state.config['algorithms']:
        MAX_BUNDLE_SIZE = state.config['algorithms']['bundling']['max_bundle_size']
    else:
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

    if not order_ids:
        return 0.0

    # Get service times from config
    pickup_service_time, dropoff_service_time = _get_service_times(state)

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
                total_time += pickup_service_time  # Add service time at each restaurant
            current_location = restaurant_loc

    else:
        # Single restaurant case
        restaurant_location = orders[0].restaurant_location
        total_time += get_travel_time(current_location, restaurant_location)
        if include_service_times:
            total_time += pickup_service_time  # Add service time at restaurant
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
                total_time += dropoff_service_time  # Add service time at each dropoff
            current_location = dropoff_locations[idx]
    else:
        # No optimization or single delivery
        for location in dropoff_locations:
            total_time += get_travel_time(current_location, location)
            if include_service_times:
                total_time += dropoff_service_time  # Add service time at each dropoff
            current_location = location

    return total_time

def optimize_delivery_sequence(start_location: Tuple[float, float],
                              dropoff_locations: List[Tuple[float, float]]) -> List[int]:

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

    # Reuse the same TSP logic for pickups
    return optimize_delivery_sequence(start_location, pickup_locations)

def generate_bundle_candidates(ready_orders: List[Order], max_bundle_size: int = 3) -> List[List[int]]:

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

    if not idle_couriers or not ready_orders:
        return []

    # Get config parameters
    MAX_BUNDLE_SIZE = state.config['algorithms']['bundling']['max_bundle_size']

    # READY-only filter (same as simple_bundling)
    now = state.current_time
    ready_orders = [o for o in ready_orders
                    if o.state == "READY"
                    and o.ready_time <= now
                    and o.expiration_time and o.expiration_time > 0
                    and now <= o.ready_time + o.expiration_time]

    if not ready_orders:
        return []

    # Generate candidates with ≤2 restaurants
    if len(ready_orders) <= 25:
        # Small batch: enumerate all ≤2 restaurant bundles
        bundle_candidates = _generate_2r_candidates(ready_orders, MAX_BUNDLE_SIZE)
    else:
        # Large batch: use geographic clustering, then filter to ≤2 restaurants
        geo_bundles = generate_geographic_bundles(
            ready_orders,
            max_bundle_size=MAX_BUNDLE_SIZE,
            max_pickup_radius=1000,
            max_dropoff_radius=2000
        )
        # Filter to keep only ≤2 restaurant bundles
        bundle_candidates = []
        for bundle in geo_bundles:
            bundle_orders = [o for o in ready_orders if o.id in bundle]
            unique_restaurants = set(o.restaurant_id for o in bundle_orders)
            if len(unique_restaurants) <= 2:
                bundle_candidates.append(bundle)

    if not bundle_candidates:
        return []

    # CP-SAT model with feasibility checking
    model = cp_model.CpModel()
    x = {}
    edge_cost = {}
    size = {}

    I = range(len(idle_couriers))
    J = range(len(bundle_candidates))

    # Only create variables for feasible assignments
    for j, bundle in enumerate(bundle_candidates):
        size[j] = len(bundle)
        for i, courier in enumerate(idle_couriers):
            # Check feasibility with route simulator
            result = _simulate_bundle_route(state, courier.current_location, bundle, allow_wait=False)
            if result is not None:
                total_time, _ = result
                x[(i, j)] = model.NewBoolVar(f'x_{i}_{j}')
                edge_cost[(i, j)] = total_time

    if not x:
        return []

    # Constraints
    # At most one bundle per courier
    for i in I:
        vars_i = [x[(i, j)] for j in J if (i, j) in x]
        if vars_i:
            model.AddAtMostOne(vars_i)

    # At most one assignment per order
    order_ids = [o.id for o in ready_orders]
    for oid in order_ids:
        hits = []
        for j in J:
            if oid in bundle_candidates[j]:
                for i in I:
                    if (i, j) in x:
                        hits.append(x[(i, j)])
        if hits:
            model.AddAtMostOne(hits)

    # Lexicographic objectives
    # 1. Maximize orders assigned
    total_orders = sum(size[j] * x[(i, j)] for (i, j) in x)
    model.Maximize(total_orders)

    solver = cp_model.CpSolver()
    solver.parameters.num_search_workers = 1
    solver.parameters.log_search_progress = False
    solver.Solve(model)
    best_orders = int(solver.Value(total_orders))

    # 2. Minimize total time (fixing orders)
    model.Add(total_orders == best_orders)
    total_time = sum(edge_cost[(i, j)] * x[(i, j)] for (i, j) in x)
    model.Minimize(total_time)
    solver.Solve(model)
    best_time = int(solver.Value(total_time))

    # 3. Minimize couriers used (fixing time)
    model.Add(total_time == best_time)
    y = {}
    for i in I:
        y[i] = model.NewBoolVar(f'y_{i}')
        vars_i = [x[(i, j)] for j in J if (i, j) in x]
        if vars_i:
            model.AddMaxEquality(y[i], vars_i)
        else:
            model.Add(y[i] == 0)

    total_couriers = sum(y[i] for i in I)
    model.Minimize(total_couriers)
    solver.Solve(model)

    # 4. Deterministic tie-breaking
    BASE = 1_000_000
    tie = sum((idle_couriers[i].id * BASE * BASE + _bundle_lex_code(bundle_candidates[j])) * x[(i, j)]
              for (i, j) in x)
    model.Minimize(tie)
    solver.Solve(model)

    # Extract solution
    assignments = []
    for (i, j), var in x.items():
        if solver.Value(var) == 1:
            assignments.append((idle_couriers[i].id, bundle_candidates[j]))

    return assignments

# ============================================================================
# ALGORITHM 5: ANTICIPATED NETWORK BUNDLING (THE WORKHORSE)
# ============================================================================

def assign_anticipated_bundling(state: SimulationState, idle_couriers: List[Courier],
                                _ignored_ready_orders: List[Order]) -> List[Tuple[int, List[int]]]:

    if not idle_couriers:
        return []

    # Get config parameters
    LOOKAHEAD_WINDOW = state.config['algorithms']['anticipated']['lookahead_window_s']
    MAX_BUNDLE_SIZE = state.config['algorithms']['bundling']['max_bundle_size']

    now = state.current_time

    # Include PENDING and READY orders within lookahead window
    pool = [o for o in state.orders.values()
            if o.state in ("PENDING", "READY")
            and o.ready_time <= now + LOOKAHEAD_WINDOW
            and o.expiration_time and o.expiration_time > 0]

    if not pool:
        return []

    # Same-restaurant candidates only (reuse simple's generator)
    candidate_bundles = _generate_simple_bundle_candidates(pool, max_bundle_size=MAX_BUNDLE_SIZE)

    if not candidate_bundles:
        return []

    # CP-SAT model with feasibility checking
    model = cp_model.CpModel()
    x = {}
    edge_cost = {}
    size = {}

    I = range(len(idle_couriers))
    J = range(len(candidate_bundles))

    # Only create variables for feasible assignments
    for j, bundle in enumerate(candidate_bundles):
        size[j] = len(bundle)
        # Verify same-restaurant constraint (should be guaranteed by generator)
        bundle_orders = [o for o in pool if o.id in bundle]
        unique_restaurants = set(o.restaurant_id for o in bundle_orders)
        if len(unique_restaurants) != 1:
            continue  # Skip if not same-restaurant

        for i, courier in enumerate(idle_couriers):
            # Check feasibility with route simulator (with waiting allowed)
            result = _simulate_bundle_route(state, courier.current_location, bundle, allow_wait=True)
            if result is not None:
                total_time, _ = result
                x[(i, j)] = model.NewBoolVar(f'x_{i}_{j}')
                edge_cost[(i, j)] = total_time

    if not x:
        return []

    # Constraints
    # At most one bundle per courier
    for i in I:
        vars_i = [x[(i, j)] for j in J if (i, j) in x]
        if vars_i:
            model.AddAtMostOne(vars_i)

    # At most one assignment per order
    pool_ids = [o.id for o in pool]
    for oid in pool_ids:
        hits = []
        for j in J:
            if oid in candidate_bundles[j]:
                for i in I:
                    if (i, j) in x:
                        hits.append(x[(i, j)])
        if hits:
            model.AddAtMostOne(hits)

    # Lexicographic objectives (no penalty terms)
    # 1. Maximize orders assigned
    total_orders = sum(size[j] * x[(i, j)] for (i, j) in x)
    model.Maximize(total_orders)

    solver = cp_model.CpSolver()
    solver.parameters.num_search_workers = 1
    solver.parameters.log_search_progress = False
    solver.Solve(model)
    best_orders = int(solver.Value(total_orders))

    # 2. Minimize total time (fixing orders)
    model.Add(total_orders == best_orders)
    total_time = sum(edge_cost[(i, j)] * x[(i, j)] for (i, j) in x)
    model.Minimize(total_time)
    solver.Solve(model)
    best_time = int(solver.Value(total_time))

    # 3. Minimize couriers used (fixing time)
    model.Add(total_time == best_time)
    y = {}
    for i in I:
        y[i] = model.NewBoolVar(f'y_{i}')
        vars_i = [x[(i, j)] for j in J if (i, j) in x]
        if vars_i:
            model.AddMaxEquality(y[i], vars_i)
        else:
            model.Add(y[i] == 0)

    total_couriers = sum(y[i] for i in I)
    model.Minimize(total_couriers)
    solver.Solve(model)

    # 4. Deterministic tie-breaking
    BASE = 1_000_000
    tie = sum((idle_couriers[i].id * BASE * BASE + _bundle_lex_code(candidate_bundles[j])) * x[(i, j)]
              for (i, j) in x)
    model.Minimize(tie)
    solver.Solve(model)

    # Extract solution
    assignments = []
    for (i, j), var in x.items():
        if solver.Value(var) == 1:
            assignments.append((idle_couriers[i].id, candidate_bundles[j]))

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

    if name not in ALGORITHMS:
        raise ValueError(f"Unknown algorithm: {name}. Available: {list(ALGORITHMS.keys())}")
    return ALGORITHMS[name]
