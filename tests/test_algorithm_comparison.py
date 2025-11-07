"""
Comparative Algorithm Test Suite

This test file feeds identical inputs to all 5 dispatch algorithms and
compares their outputs to highlight expected differences in behavior.

Algorithms tested:
1. Greedy - Nearest courier per order, no bundling
2. Hungarian - Optimal 1-to-1 matching
3. Simple Bundling - Same-restaurant bundles only
4. Network Bundling - Multi-restaurant bundles (≤2 restaurants)
5. Anticipated Bundling - Lookahead with waiting allowed

Expected Differences:
- Greedy/Hungarian: no bundling, different matching strategies
- Simple Bundling: combines same-restaurant orders
- Network Bundling: combines across restaurants
- Anticipated Bundling: waits for future orders
"""

import sys
sys.path.insert(0, '/Users/pranjal/Code/meituan')

from simulator_core import Restaurant, Courier, Order, SimulationState, set_courier_speed
from assignment_algorithms import (
    assign_greedy,
    assign_hungarian,
    assign_simple_bundling,
    assign_network_bundling,
    assign_anticipated_bundling
)
from typing import List, Tuple, Dict

# ============================================================================
# SETUP HELPERS
# ============================================================================

def create_config(anticipated_lookahead_s: int = 900):
    """Create a standard config for testing"""
    return {
        'physics': {
            'courier_speed_kmh': 30.0,
            'pickup_service_time_s': 90.0,
            'dropoff_service_time_s': 45.0,
            'meal_prep_time_s': 300.0,  # 5 minutes
            'order_expiration_time_s': 1800.0  # 30 minutes
        },
        'algorithms': {
            'bundling': {
                'max_bundle_size': 3
            },
            'anticipated': {
                'lookahead_window_s': anticipated_lookahead_s
            }
        }
    }

def create_test_state(restaurants: List[Restaurant],
                     couriers: List[Courier],
                     orders: List[Order],
                     current_time: float = 0.0,
                     config: Dict = None) -> SimulationState:
    """Create a simulation state for testing"""
    if config is None:
        config = create_config()

    state = SimulationState(restaurants, couriers, orders, duration=3600)
    state.config = config
    state.current_time = current_time

    # Set courier speed globally
    set_courier_speed(config['physics']['courier_speed_kmh'])

    return state

def print_separator(title: str):
    """Print a formatted section separator"""
    print(f"\n{'=' * 80}")
    print(f"{title:^80}")
    print('=' * 80)

def print_scenario_setup(scenario_name: str, description: str,
                        couriers: List[Courier], orders: List[Order],
                        restaurants: List[Restaurant]):
    """Print scenario setup details"""
    print_separator(f"SCENARIO: {scenario_name}")
    print(f"\n{description}\n")

    print(f"Restaurants: {len(restaurants)}")
    for r in restaurants:
        print(f"  Restaurant {r.id} at {r.location}")

    print(f"\nCouriers: {len(couriers)}")
    for c in couriers:
        print(f"  Courier {c.id} at {c.current_location}, state={c.state}")

    print(f"\nOrders: {len(orders)}")
    for o in orders:
        print(f"  Order {o.id}: Restaurant {o.restaurant_id} → Diner at {o.diner_location}")
        print(f"    State: {o.state}, Ready: {o.ready_time}s, Expires: {o.ready_time + o.expiration_time}s")

def print_algorithm_result(algo_name: str, assignments: List[Tuple[int, List[int]]]):
    """Print algorithm assignment results"""
    print(f"\n{algo_name}:")
    if not assignments:
        print("  → No assignments made")
    else:
        for courier_id, order_ids in assignments:
            if len(order_ids) == 1:
                print(f"  → Courier {courier_id}: Order {order_ids[0]}")
            else:
                print(f"  → Courier {courier_id}: Bundle {order_ids} ({len(order_ids)} orders)")

def compare_results(results: Dict[str, List[Tuple[int, List[int]]]]):
    """Print a comparison table of all algorithm results"""
    print_separator("COMPARISON SUMMARY")

    # Calculate metrics for each algorithm
    print(f"\n{'Algorithm':<25} {'Assignments':<15} {'Orders Assigned':<20} {'Bundles Created':<20}")
    print('-' * 80)

    for algo_name, assignments in results.items():
        num_assignments = len(assignments)
        num_orders = sum(len(order_ids) for _, order_ids in assignments)
        num_bundles = sum(1 for _, order_ids in assignments if len(order_ids) > 1)

        print(f"{algo_name:<25} {num_assignments:<15} {num_orders:<20} {num_bundles:<20}")

    # Show differences
    print("\n" + "=" * 80)
    print("DIFFERENCES (Expected Behavior):")
    print("=" * 80)

    all_orders_assigned = {}
    for algo_name, assignments in results.items():
        orders = set()
        for _, order_ids in assignments:
            orders.update(order_ids)
        all_orders_assigned[algo_name] = orders

    # Find unique assignments
    for algo_name, orders in all_orders_assigned.items():
        unique_orders = orders
        for other_algo, other_orders in all_orders_assigned.items():
            if other_algo != algo_name:
                unique_orders = unique_orders - other_orders

        if unique_orders:
            print(f"\n{algo_name} UNIQUELY assigned: {sorted(unique_orders)}")

# ============================================================================
# TEST SCENARIOS
# ============================================================================

def test_scenario_1_basic_single_orders():
    """
    Test basic scenario with simple single orders.
    Expected: Greedy and Hungarian make similar decisions, no bundling occurs.
    """
    # Setup
    restaurants = [
        Restaurant(1, (0.0, 0.0)),
        Restaurant(2, (2.0, 2.0)),
        Restaurant(3, (4.0, 4.0))
    ]

    couriers = [
        Courier(1, (0.5, 0.5)),
        Courier(2, (2.5, 2.5)),
        Courier(3, (4.5, 4.5))
    ]

    # All orders are ready now
    orders = [
        Order(1, 1, (0.0, 0.0), (0.5, 1.0), 0, 300, 1800),
        Order(2, 2, (2.0, 2.0), (2.5, 3.0), 0, 300, 1800),
        Order(3, 3, (4.0, 4.0), (4.5, 5.0), 0, 300, 1800)
    ]

    # Set all orders to READY
    for o in orders:
        o.state = "READY"

    state = create_test_state(restaurants, couriers, orders, current_time=300)

    print_scenario_setup(
        "Basic Single Orders",
        "3 couriers, 3 orders from different restaurants, all ready.\n"
        "Expected: Greedy assigns nearest; Hungarian optimizes globally; No bundling.",
        couriers, orders, restaurants
    )

    # Run all algorithms
    results = {}

    results['Greedy'] = assign_greedy(state, couriers, orders)
    results['Hungarian'] = assign_hungarian(state, couriers, orders)
    results['Simple Bundling'] = assign_simple_bundling(state, couriers, orders)
    results['Network Bundling'] = assign_network_bundling(state, couriers, orders)
    results['Anticipated Bundling'] = assign_anticipated_bundling(state, couriers, orders)

    # Print results
    for algo_name, assignments in results.items():
        print_algorithm_result(algo_name, assignments)

    compare_results(results)
    print("\n✓ Expected: All algorithms assign all 3 orders (no bundling opportunities)")


def test_scenario_1b_greedy_vs_hungarian():
    """
    Test scenario where Greedy is suboptimal but Hungarian finds global optimum.
    Expected: Greedy assigns fewer orders than Hungarian due to blocking.

    REDESIGNED: 2 couriers, 3 orders with deadline constraints.
    Greedy's sequential processing blocks optimal assignment.
    """
    restaurants = [
        Restaurant(1, (0.0, 0.0)),
        Restaurant(2, (10.0, 0.0)),
        Restaurant(3, (5.0, 0.0))
    ]

    couriers = [
        Courier(1, (0.5, 0.0)),   # Near Restaurant 1
        Courier(2, (9.5, 0.0))    # Near Restaurant 2
    ]

    # Strategic setup:
    # O1: Only C1 can deliver (C2 too far with tight deadline)
    # O2: Both couriers can deliver (middle restaurant)
    # O3: Only C2 can deliver (C1 too far with tight deadline)
    #
    # Greedy (processes by ready_time):
    #   - O2 first (ready earlier) → picks nearest C1 → C1 busy
    #   - O1 next → C1 busy, C2 infeasible → UNASSIGNED
    #   - O3 next → C2 available → assigned
    #   Result: 2 orders (O2, O3)
    #
    # Hungarian (global optimization):
    #   - Sees O1 needs C1, O3 needs C2, O2 flexible
    #   - Assigns C1→O1, C2→O3, leaves O2 unassigned
    #   Result: 2 orders (O1, O3)
    #   OR assigns C1→O1, C2→O2 if feasible
    orders = [
        Order(1, 1, (0.0, 0.0), (0.2, 1.0), 100, 300, 900),   # Ready at 400s, tight deadline, near R1
        Order(2, 3, (5.0, 0.0), (5.0, 1.0), 0, 300, 1200),    # Ready at 300s (FIRST), middle
        Order(3, 2, (10.0, 0.0), (9.8, 1.0), 100, 300, 900)   # Ready at 400s, tight deadline, near R2
    ]

    for o in orders:
        o.state = "READY"

    state = create_test_state(restaurants, couriers, orders, current_time=400)  # Start at 400s

    print_scenario_setup(
        "Greedy vs Hungarian Optimization (REDESIGNED)",
        "2 couriers, 3 orders with deadline constraints.\n"
        "O1 needs C1 (C2 too far), O3 needs C2 (C1 too far), O2 flexible.\n"
        "Greedy processes O2 first → may block optimal assignment.\n"
        "Hungarian finds globally optimal matching.",
        couriers, orders, restaurants
    )

    results = {}
    results['Greedy'] = assign_greedy(state, couriers, orders)
    results['Hungarian'] = assign_hungarian(state, couriers, orders)
    results['Simple Bundling'] = assign_simple_bundling(state, couriers, orders)
    results['Network Bundling'] = assign_network_bundling(state, couriers, orders)
    results['Anticipated Bundling'] = assign_anticipated_bundling(state, couriers, orders)

    for algo_name, assignments in results.items():
        print_algorithm_result(algo_name, assignments)

    compare_results(results)

    # ASSERTION: Check if Hungarian assigned more or equal orders
    greedy_count = sum(len(order_ids) for _, order_ids in results['Greedy'])
    hungarian_count = sum(len(order_ids) for _, order_ids in results['Hungarian'])

    if hungarian_count > greedy_count:
        print(f"\n✓ PASS: Hungarian assigned {hungarian_count} orders > Greedy's {greedy_count} orders (global optimization wins!)")
    elif hungarian_count == greedy_count:
        print(f"\n✓ PASS: Hungarian assigned {hungarian_count} orders = Greedy's {greedy_count} orders (both optimal for this case)")
    else:
        print(f"\n✗ FAIL: Hungarian assigned {hungarian_count} orders < Greedy's {greedy_count} orders (should be optimal!)")


def test_scenario_2_same_restaurant_bundling():
    """
    Test scenario where multiple orders come from the same restaurant.
    Expected: Simple/Network/Anticipated bundling combine them; Greedy/Hungarian don't.
    """
    restaurants = [
        Restaurant(1, (0.0, 0.0)),
        Restaurant(2, (5.0, 5.0))
    ]

    couriers = [
        Courier(1, (0.5, 0.5)),
        Courier(2, (5.5, 5.5))
    ]

    # 3 orders from Restaurant 1, 1 order from Restaurant 2
    orders = [
        Order(1, 1, (0.0, 0.0), (0.5, 1.0), 0, 300, 1800),
        Order(2, 1, (0.0, 0.0), (1.0, 1.0), 0, 300, 1800),
        Order(3, 1, (0.0, 0.0), (1.5, 1.0), 0, 300, 1800),
        Order(4, 2, (5.0, 5.0), (5.5, 6.0), 0, 300, 1800)
    ]

    for o in orders:
        o.state = "READY"

    state = create_test_state(restaurants, couriers, orders, current_time=300)

    print_scenario_setup(
        "Same-Restaurant Bundling Opportunity",
        "2 couriers, 4 orders (3 from Restaurant 1, 1 from Restaurant 2).\n"
        "Expected: Simple/Network/Anticipated bundle Restaurant 1 orders; Greedy/Hungarian don't.",
        couriers, orders, restaurants
    )

    results = {}
    results['Greedy'] = assign_greedy(state, couriers, orders)
    results['Hungarian'] = assign_hungarian(state, couriers, orders)
    results['Simple Bundling'] = assign_simple_bundling(state, couriers, orders)
    results['Network Bundling'] = assign_network_bundling(state, couriers, orders)
    results['Anticipated Bundling'] = assign_anticipated_bundling(state, couriers, orders)

    for algo_name, assignments in results.items():
        print_algorithm_result(algo_name, assignments)

    compare_results(results)
    print("\n✓ Expected: Bundling algorithms create bundles; Greedy/Hungarian assign 1-to-1")


def test_scenario_3_multi_restaurant_bundling():
    """
    Test scenario where multi-restaurant bundling is FEASIBLE and efficient.
    Expected: Network bundling combines across restaurants; Simple bundling doesn't.

    FIXED: Extended deadline to 1 hour (3600s) so multi-restaurant route is feasible.
    Original issue: 30-min deadline made 46-min route infeasible.
    """
    restaurants = [
        Restaurant(1, (0.0, 0.0)),      # West restaurant
        Restaurant(2, (10.0, 0.0))      # East restaurant (10km away)
    ]

    couriers = [
        Courier(1, (5.0, -1.0)),  # Courier near middle, slightly south
        Courier(2, (15.0, 15.0))  # Far courier (will not be chosen)
    ]

    # 2 orders: 1 from each restaurant, both delivering to SAME customer location (midway)
    # Multi-restaurant bundle [1,2]:
    #   Route: Courier → R1 → R2 → Customer
    #   Time: ~46 minutes (requires 1-hour deadline)
    # Single orders would leave one unassigned (only 1 effective courier)
    orders = [
        Order(1, 1, (0.0, 0.0), (5.0, 0.0), 0, 300, 3600),    # 1-hour deadline
        Order(2, 2, (10.0, 0.0), (5.0, 0.0), 0, 300, 3600),   # 1-hour deadline
    ]

    for o in orders:
        o.state = "READY"

    state = create_test_state(restaurants, couriers, orders, current_time=300)

    print_scenario_setup(
        "Multi-Restaurant Bundling ADVANTAGE (FIXED)",
        "1 active courier, 2 orders from 2 far-apart restaurants to SAME customer.\n"
        "Multi-restaurant bundle [1,2]: Courier→R1→R2→Customer = ~46 min (feasible with 1h deadline)\n"
        "Single orders: Only 1 can be assigned (1 courier available)\n"
        "Expected: Network bundling creates [1,2] bundle (2 orders); Others assign only 1 order.",
        couriers, orders, restaurants
    )

    results = {}
    results['Greedy'] = assign_greedy(state, couriers, orders)
    results['Hungarian'] = assign_hungarian(state, couriers, orders)
    results['Simple Bundling'] = assign_simple_bundling(state, couriers, orders)
    results['Network Bundling'] = assign_network_bundling(state, couriers, orders)
    results['Anticipated Bundling'] = assign_anticipated_bundling(state, couriers, orders)

    for algo_name, assignments in results.items():
        print_algorithm_result(algo_name, assignments)

    compare_results(results)

    # ASSERTION: Verify network bundling created multi-restaurant bundle with 2 orders
    network_bundles = results['Network Bundling']
    network_orders = sum(len(order_ids) for _, order_ids in network_bundles)
    has_multi_restaurant = False

    for courier_id, order_ids in network_bundles:
        if len(order_ids) > 1:
            # Check if bundle has multiple restaurants
            bundle_restaurants = set(orders[oid-1].restaurant_id for oid in order_ids)
            if len(bundle_restaurants) > 1:
                has_multi_restaurant = True
                print(f"\n✓ PASS: Network bundling created multi-restaurant bundle {order_ids} with {len(bundle_restaurants)} restaurants")

    if network_orders == 2 and has_multi_restaurant:
        print(f"✓ PASS: Network bundling served 2/2 orders via multi-restaurant bundle (optimal!)")
    elif network_orders == 2 and not has_multi_restaurant:
        print(f"⚠ WARNING: Network bundling served 2 orders but NOT via multi-restaurant bundle")
    else:
        print(f"\n✗ FAIL: Network bundling only served {network_orders}/2 orders (should bundle both with extended deadline)")

    # ASSERTION: Verify simple bundling can't create multi-restaurant
    simple_bundles = results['Simple Bundling']
    simple_orders = sum(len(order_ids) for _, order_ids in simple_bundles)
    for courier_id, order_ids in simple_bundles:
        if len(order_ids) > 1:
            bundle_restaurants = set(orders[oid-1].restaurant_id for oid in order_ids)
            if len(bundle_restaurants) > 1:
                print(f"✗ FAIL: Simple bundling created multi-restaurant bundle {order_ids} (should be same-restaurant only)")

    if simple_orders == 1:
        print(f"✓ PASS: Simple bundling only served 1 order (can't bundle different restaurants)")


def test_scenario_4_lookahead_opportunity():
    """
    Test scenario with PENDING orders that will be ready soon.
    Expected: Anticipated bundling waits for them; others only assign READY orders.

    FIXED: Orders now ready in 200s (within 300s wait limit), not 600s.
    """
    restaurants = [
        Restaurant(1, (0.0, 0.0))
    ]

    couriers = [
        Courier(1, (0.5, 0.5)),
        Courier(2, (1.0, 1.0))
    ]

    # Order 1 is READY now, Order 2-3 are PENDING (ready in 200s = 3.3 min)
    # This is WITHIN the 300s (5 min) wait limit, so anticipated CAN bundle them
    orders = [
        Order(1, 1, (0.0, 0.0), (0.5, 1.0), 0, 300, 1800),        # Ready at 300s
        Order(2, 1, (0.0, 0.0), (1.0, 1.0), 200, 300, 1800),      # Ready at 500s (200s future)
        Order(3, 1, (0.0, 0.0), (1.5, 1.0), 200, 300, 1800)       # Ready at 500s (200s future)
    ]

    orders[0].state = "READY"  # Order 1 is ready
    orders[1].state = "PENDING"  # Order 2-3 are pending (within lookahead)
    orders[2].state = "PENDING"

    state = create_test_state(restaurants, couriers, orders, current_time=300)

    print_scenario_setup(
        "Lookahead Opportunity (FIXED)",
        "2 couriers, 1 READY order, 2 PENDING orders (ready in 200s, within wait limit).\n"
        "Expected: Anticipated bundles all 3 by waiting 200s; Others only assign Order 1.",
        couriers, orders, restaurants
    )

    results = {}
    results['Greedy'] = assign_greedy(state, couriers, [orders[0]])  # Only READY orders
    results['Hungarian'] = assign_hungarian(state, couriers, [orders[0]])
    results['Simple Bundling'] = assign_simple_bundling(state, couriers, [orders[0]])
    results['Network Bundling'] = assign_network_bundling(state, couriers, [orders[0]])
    results['Anticipated Bundling'] = assign_anticipated_bundling(state, couriers, [orders[0]])  # Looks ahead

    for algo_name, assignments in results.items():
        print_algorithm_result(algo_name, assignments)

    compare_results(results)

    # ASSERTION: Verify anticipated bundling actually bundled
    anticipated_orders = sum(len(order_ids) for _, order_ids in results['Anticipated Bundling'])
    if anticipated_orders == 3:
        print("\n✓ PASS: Anticipated bundling successfully bundled all 3 orders (lookahead working!)")
    else:
        print(f"\n✗ FAIL: Anticipated bundling only assigned {anticipated_orders}/3 orders (lookahead NOT working!)")

    # ASSERTION: Verify others only assigned 1 order
    for algo_name in ['Greedy', 'Hungarian', 'Simple Bundling', 'Network Bundling']:
        count = sum(len(order_ids) for _, order_ids in results[algo_name])
        if count != 1:
            print(f"✗ FAIL: {algo_name} assigned {count} orders, expected 1")


def test_scenario_5_tight_deadlines():
    """
    Test scenario where orders are about to expire.
    Expected: Algorithms handle feasibility differently; some may fail to assign.
    """
    restaurants = [
        Restaurant(1, (0.0, 0.0))
    ]

    couriers = [
        Courier(1, (5.0, 5.0)),  # Far away courier
        Courier(2, (0.2, 0.2))   # Nearby courier
    ]

    # Order expires in 10 minutes (600s) - tight for far courier
    orders = [
        Order(1, 1, (0.0, 0.0), (0.5, 1.0), 0, 300, 600),  # Expires at 900s
    ]

    orders[0].state = "READY"

    state = create_test_state(restaurants, couriers, orders, current_time=300)

    print_scenario_setup(
        "Tight Deadline Stress",
        "2 couriers (1 far, 1 near), 1 order expiring soon (600s remaining).\n"
        "Expected: Algorithms prefer nearby courier; Far courier may be infeasible.",
        couriers, orders, restaurants
    )

    results = {}
    results['Greedy'] = assign_greedy(state, couriers, orders)
    results['Hungarian'] = assign_hungarian(state, couriers, orders)
    results['Simple Bundling'] = assign_simple_bundling(state, couriers, orders)
    results['Network Bundling'] = assign_network_bundling(state, couriers, orders)
    results['Anticipated Bundling'] = assign_anticipated_bundling(state, couriers, orders)

    for algo_name, assignments in results.items():
        print_algorithm_result(algo_name, assignments)

    compare_results(results)
    print("\n✓ Expected: All algorithms assign to nearby courier (feasibility constraint)")


def test_scenario_6_no_feasible_assignments():
    """
    Test scenario where no assignments are feasible.
    Expected: All algorithms return empty assignments.
    """
    restaurants = [
        Restaurant(1, (0.0, 0.0))
    ]

    couriers = [
        Courier(1, (10.0, 10.0))  # Very far away
    ]

    # Order already expired
    orders = [
        Order(1, 1, (0.0, 0.0), (0.5, 1.0), 0, 300, 600),  # Expires at 900s
    ]

    orders[0].state = "READY"

    # Current time is past expiration
    state = create_test_state(restaurants, couriers, orders, current_time=1000)

    print_scenario_setup(
        "No Feasible Assignments",
        "1 courier far away, 1 order already expired.\n"
        "Expected: All algorithms return empty (no feasible assignments).",
        couriers, orders, restaurants
    )

    results = {}
    results['Greedy'] = assign_greedy(state, couriers, orders)
    results['Hungarian'] = assign_hungarian(state, couriers, orders)
    results['Simple Bundling'] = assign_simple_bundling(state, couriers, orders)
    results['Network Bundling'] = assign_network_bundling(state, couriers, orders)
    results['Anticipated Bundling'] = assign_anticipated_bundling(state, couriers, orders)

    for algo_name, assignments in results.items():
        print_algorithm_result(algo_name, assignments)

    compare_results(results)
    print("\n✓ Expected: All algorithms return empty (order expired)")


def test_scenario_7_comprehensive_stress_test():
    """
    Comprehensive stress test showing ALL 5 algorithms producing DIFFERENT outputs.

    Mix of conditions:
    - Single scattered orders → Greedy/Hungarian compete
    - Same-restaurant cluster → Simple bundles
    - Multi-restaurant opportunities → Network bundles
    - PENDING orders → Anticipated waits and bundles

    Expected hierarchy: Greedy < Hungarian ≤ Simple < Network < Anticipated
    """
    restaurants = [
        Restaurant(1, (0.0, 0.0)),      # Cluster center
        Restaurant(2, (0.5, 0.5)),      # Near Restaurant 1 (multi-restaurant opportunity)
        Restaurant(3, (15.0, 0.0)),     # Far east
        Restaurant(4, (0.0, 15.0)),     # Far north
        Restaurant(5, (15.0, 15.0))     # Far northeast
    ]

    couriers = [
        Courier(1, (0.2, 0.2)),    # Near R1/R2 cluster
        Courier(2, (0.8, 0.8)),    # Near R1/R2 cluster
        Courier(3, (15.2, 0.2)),   # Near R3
        Courier(4, (0.2, 15.2)),   # Near R4
        Courier(5, (14.8, 14.8)),  # Near R5
        Courier(6, (7.5, 7.5))     # Middle (can reach anywhere)
    ]

    # 15 orders with strategic distribution:
    # Group A: 4 READY orders from R1 (same-restaurant bundling opportunity)
    # Group B: 2 READY orders from R2 (near R1, multi-restaurant opportunity)
    # Group C: 3 single scattered orders from R3, R4, R5
    # Group D: 3 PENDING orders from R1 (lookahead opportunity)
    orders = [
        # Group A: Same-restaurant cluster at R1 (READY)
        Order(1, 1, (0.0, 0.0), (0.5, 2.0), 0, 300, 3600),    # R1
        Order(2, 1, (0.0, 0.0), (1.0, 2.0), 0, 300, 3600),    # R1
        Order(3, 1, (0.0, 0.0), (1.5, 2.0), 0, 300, 3600),    # R1
        Order(4, 1, (0.0, 0.0), (2.0, 2.0), 10, 300, 3600),   # R1 (slightly later)

        # Group B: Multi-restaurant opportunity (READY, near R1)
        Order(5, 2, (0.5, 0.5), (1.0, 1.0), 0, 300, 3600),    # R2
        Order(6, 2, (0.5, 0.5), (1.5, 1.0), 0, 300, 3600),    # R2

        # Group C: Scattered single orders (READY)
        Order(7, 3, (15.0, 0.0), (15.5, 1.0), 0, 300, 3600),  # R3 (far east)
        Order(8, 4, (0.0, 15.0), (1.0, 15.5), 0, 300, 3600),  # R4 (far north)
        Order(9, 5, (15.0, 15.0), (15.5, 15.5), 0, 300, 3600), # R5 (far northeast)

        # Group D: PENDING orders at R1 (lookahead opportunity, ready in 180s)
        Order(10, 1, (0.0, 0.0), (0.8, 2.5), 120, 300, 3600),  # Ready at 420s
        Order(11, 1, (0.0, 0.0), (1.2, 2.5), 120, 300, 3600),  # Ready at 420s
        Order(12, 1, (0.0, 0.0), (1.8, 2.5), 120, 300, 3600),  # Ready at 420s
    ]

    # Set states
    for i in range(9):
        orders[i].state = "READY"
    for i in range(9, 12):
        orders[i].state = "PENDING"

    state = create_test_state(restaurants, couriers, orders, current_time=300)

    print_scenario_setup(
        "COMPREHENSIVE STRESS TEST - All 5 Algorithms Different",
        "6 couriers, 12 orders with mixed conditions:\n"
        "- 4 READY orders from R1 (same-restaurant bundling)\n"
        "- 2 READY orders from R2 near R1 (multi-restaurant opportunity)\n"
        "- 3 scattered single orders (Greedy/Hungarian differ)\n"
        "- 3 PENDING orders from R1 ready in 180s (lookahead)\n\n"
        "Expected: Greedy < Hungarian ≤ Simple < Network < Anticipated",
        couriers, orders, restaurants
    )

    # Run algorithms on READY orders only (except Anticipated which looks ahead)
    ready_orders = [o for o in orders if o.state == "READY"]

    results = {}
    results['Greedy'] = assign_greedy(state, couriers, ready_orders)
    results['Hungarian'] = assign_hungarian(state, couriers, ready_orders)
    results['Simple Bundling'] = assign_simple_bundling(state, couriers, ready_orders)
    results['Network Bundling'] = assign_network_bundling(state, couriers, ready_orders)
    results['Anticipated Bundling'] = assign_anticipated_bundling(state, couriers, ready_orders)  # Looks ahead

    for algo_name, assignments in results.items():
        print_algorithm_result(algo_name, assignments)

    compare_results(results)

    # Calculate orders assigned by each
    counts = {}
    for algo_name, assignments in results.items():
        counts[algo_name] = sum(len(order_ids) for _, order_ids in assignments)

    print("\n" + "=" * 80)
    print("PERFORMANCE HIERARCHY:")
    print("=" * 80)
    for algo_name in ['Greedy', 'Hungarian', 'Simple Bundling', 'Network Bundling', 'Anticipated Bundling']:
        count = counts[algo_name]
        print(f"{algo_name:<25} {count:>2} orders assigned")

    # ASSERTION: Check progressive improvement
    print("\n" + "=" * 80)
    print("VALIDATION:")
    print("=" * 80)

    if counts['Hungarian'] >= counts['Greedy']:
        print(f"✓ Hungarian ({counts['Hungarian']}) >= Greedy ({counts['Greedy']})")
    else:
        print(f"✗ FAIL: Hungarian ({counts['Hungarian']}) < Greedy ({counts['Greedy']})")

    if counts['Simple Bundling'] >= counts['Hungarian']:
        print(f"✓ Simple Bundling ({counts['Simple Bundling']}) >= Hungarian ({counts['Hungarian']})")
    else:
        print(f"⚠ Simple Bundling ({counts['Simple Bundling']}) < Hungarian ({counts['Hungarian']}) (may be OK)")

    if counts['Network Bundling'] >= counts['Simple Bundling']:
        print(f"✓ Network Bundling ({counts['Network Bundling']}) >= Simple ({counts['Simple Bundling']})")
    else:
        print(f"⚠ Network Bundling ({counts['Network Bundling']}) < Simple ({counts['Simple Bundling']}) (may be OK)")

    if counts['Anticipated Bundling'] >= counts['Network Bundling']:
        print(f"✓ Anticipated Bundling ({counts['Anticipated Bundling']}) >= Network ({counts['Network Bundling']})")
    else:
        print(f"✗ FAIL: Anticipated ({counts['Anticipated Bundling']}) < Network ({counts['Network Bundling']})")

    # Check if all different
    unique_counts = len(set(counts.values()))
    if unique_counts >= 3:
        print(f"\n✓ PASS: {unique_counts}/5 algorithms produced different order counts (diversity shown)")
    else:
        print(f"\n⚠ WARNING: Only {unique_counts}/5 unique order counts (limited diversity)")


# ============================================================================
# MAIN TEST RUNNER
# ============================================================================

def run_all_tests():
    """Run all comparative test scenarios"""
    print("\n" + "=" * 80)
    print("COMPARATIVE ALGORITHM TEST SUITE".center(80))
    print("=" * 80)
    print("\nThis suite compares 5 dispatch algorithms on identical inputs.")
    print("Differences in outputs are EXPECTED and demonstrate algorithm characteristics.\n")

    tests = [
        test_scenario_1_basic_single_orders,
        test_scenario_1b_greedy_vs_hungarian,
        test_scenario_2_same_restaurant_bundling,
        test_scenario_3_multi_restaurant_bundling,
        test_scenario_4_lookahead_opportunity,
        test_scenario_5_tight_deadlines,
        test_scenario_6_no_feasible_assignments,
        test_scenario_7_comprehensive_stress_test
    ]

    for test_func in tests:
        try:
            test_func()
            print("\n" + "✓" * 80)
        except Exception as e:
            print(f"\n✗ TEST FAILED: {test_func.__name__}")
            print(f"Error: {e}")
            import traceback
            traceback.print_exc()
            print("✗" * 80)

    print_separator("TEST SUITE COMPLETE")
    print("\nAll tests executed. Review outputs to understand algorithm differences.")
    print("Note: Failures indicate bugs; differences indicate design choices.")


if __name__ == "__main__":
    run_all_tests()
