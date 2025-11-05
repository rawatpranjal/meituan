"""
Test Suite for Network Bundling vs Simple Bundling

Algorithms Under Test:
- assign_simple_bundling: Line 288 in assignment_algorithms.py
- assign_network_bundling: Line 717 in assignment_algorithms.py
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from simulator_core import (
    SimulationState, Courier, Order, Restaurant,
    euclidean_distance, get_travel_time
)
from assignment_algorithms import (
    assign_simple_bundling,
    assign_network_bundling,
    calculate_route_duration,
    generate_bundle_candidates
)
import datetime


# ============================================================================
# TEST 1
# ============================================================================

def test_cross_street_pickup():
    """
    Setup:
    - Restaurant 0 at (2.0, 2.0)
    - Restaurant 1 at (2.05, 2.0)
    - Order 0: R0 → Diner at (2.0, 1.5)
    - Order 1: R1 → Diner at (2.05, 1.5)
    - Courier 0 at (0.0, 0.0)
    - Courier 1 at (0.1, 0.0)
    """
    print("\n" + "="*80)
    print("TEST 1")
    print("="*80)

    restaurants = [
        Restaurant(0, (2.0, 2.0)),
        Restaurant(1, (2.05, 2.0))
    ]

    couriers = [
        Courier(0, (0.0, 0.0)),
        Courier(1, (0.1, 0.0))
    ]

    orders = [
        Order(0, 0, (2.0, 2.0), (2.0, 1.5), 0.0),
        Order(1, 1, (2.05, 2.0), (2.05, 1.5), 0.0)
    ]

    for order in orders:
        order.ready_time = 0.0
        order.state = "READY"

    state = SimulationState(restaurants, couriers, orders, duration=3600)

    r_dist = euclidean_distance(restaurants[0].location, restaurants[1].location) * 1000
    d_dist = euclidean_distance(orders[0].diner_location, orders[1].diner_location) * 1000

    print(f"\nGeometry:")
    print(f"  Restaurant distance: {r_dist:.1f}m")
    print(f"  Diner distance: {d_dist:.1f}m")
    print(f"  Couriers: {len(couriers)}")
    print(f"  Orders: {len(orders)}")

    print("\n--- SIMPLE BUNDLING ---")
    simple_assignments = assign_simple_bundling(state, couriers, orders)
    print(f"Assignments: {len(simple_assignments)}")
    for c_id, o_ids in simple_assignments:
        print(f"  Courier {c_id} → Orders {o_ids}")
    simple_assigned_count = sum(len(o_ids) for _, o_ids in simple_assignments)
    print(f"Orders assigned: {simple_assigned_count}")

    print("\n--- NETWORK BUNDLING ---")
    bundles_generated = generate_bundle_candidates(orders, max_bundle_size=3)
    print(f"Bundles generated: {bundles_generated}")
    for bundle in bundles_generated:
        cost = calculate_route_duration(
            couriers[0].current_location,
            bundle,
            state,
            use_tsp_optimization=(len(bundle) > 1),
            include_service_times=True
        )
        rids = set(orders[oid].restaurant_id for oid in bundle)
        print(f"  Bundle {bundle} (restaurants: {rids}): {cost:.1f}s")

    network_assignments = assign_network_bundling(state, couriers, orders)
    print(f"Assignments: {len(network_assignments)}")
    for c_id, o_ids in network_assignments:
        print(f"  Courier {c_id} → Orders {o_ids}")
    network_assigned_count = sum(len(o_ids) for _, o_ids in network_assignments)
    print(f"Orders assigned: {network_assigned_count}")

    print("\n--- RESULTS ---")
    print(f"Simple: {simple_assigned_count} orders, {len(simple_assignments)} assignments")
    print(f"Network: {network_assigned_count} orders, {len(network_assignments)} assignments")

    assert network_assigned_count >= simple_assigned_count
    assert network_assigned_count == 2
    assert simple_assigned_count == 2

    print("PASS")


# ============================================================================
# TEST 2
# ============================================================================

def test_en_route_dropoff():
    """
    Setup:
    - Restaurant 0 at (1.0, 1.0)
    - Restaurant 1 at (5.0, 5.0)
    - Order 0: R0 → Diner at (5.0, 1.0)
    - Order 1: R1 → Diner at (5.0, 4.0)
    - Courier 0 at (1.0, 0.0)
    """
    print("\n" + "="*80)
    print("TEST 2")
    print("="*80)

    restaurants = [
        Restaurant(0, (1.0, 1.0)),
        Restaurant(1, (5.0, 5.0))
    ]

    couriers = [Courier(0, (1.0, 0.0))]

    orders = [
        Order(0, 0, (1.0, 1.0), (5.0, 1.0), 0.0),
        Order(1, 1, (5.0, 5.0), (5.0, 4.0), 0.0)
    ]

    for order in orders:
        order.ready_time = 0.0
        order.state = "READY"

    state = SimulationState(restaurants, couriers, orders, duration=3600)

    r_dist = euclidean_distance(restaurants[0].location, restaurants[1].location) * 1000
    d_dist = euclidean_distance(orders[0].diner_location, orders[1].diner_location) * 1000

    print(f"\nGeometry:")
    print(f"  Restaurant distance: {r_dist:.1f}m")
    print(f"  Diner distance: {d_dist:.1f}m")
    print(f"  Couriers: {len(couriers)}")
    print(f"  Orders: {len(orders)}")

    print("\n--- SIMPLE BUNDLING ---")
    simple_assignments = assign_simple_bundling(state, couriers, orders)
    print(f"Assignments: {len(simple_assignments)}")
    for c_id, o_ids in simple_assignments:
        print(f"  Courier {c_id} → Orders {o_ids}")
    simple_assigned_count = sum(len(o_ids) for _, o_ids in simple_assignments)
    print(f"Orders assigned: {simple_assigned_count}/{len(orders)}")

    print("\n--- NETWORK BUNDLING ---")
    network_assignments = assign_network_bundling(state, couriers, orders)
    print(f"Assignments: {len(network_assignments)}")
    for c_id, o_ids in network_assignments:
        print(f"  Courier {c_id} → Orders {o_ids}")
    network_assigned_count = sum(len(o_ids) for _, o_ids in network_assignments)
    print(f"Orders assigned: {network_assigned_count}/{len(orders)}")

    print("\n--- RESULTS ---")
    print(f"Simple: {simple_assigned_count} orders")
    print(f"Network: {network_assigned_count} orders")

    assert network_assigned_count >= simple_assigned_count

    print("PASS")


# ============================================================================
# TEST 3
# ============================================================================

def test_three_way_trade():
    """
    Setup:
    - Restaurant 0 at (1.0, 5.0)
    - Restaurant 1 at (3.0, 5.0)
    - Restaurant 2 at (5.0, 5.0)
    - Order 0: R0 → Diner at (1.0, 1.0)
    - Order 1: R1 → Diner at (3.0, 1.0)
    - Order 2: R2 → Diner at (5.0, 1.0)
    - Courier 0 at (0.0, 5.0)
    """
    print("\n" + "="*80)
    print("TEST 3")
    print("="*80)

    restaurants = [
        Restaurant(0, (1.0, 5.0)),
        Restaurant(1, (3.0, 5.0)),
        Restaurant(2, (5.0, 5.0))
    ]

    couriers = [Courier(0, (0.0, 5.0))]

    orders = [
        Order(0, 0, (1.0, 5.0), (1.0, 1.0), 0.0),
        Order(1, 1, (3.0, 5.0), (3.0, 1.0), 0.0),
        Order(2, 2, (5.0, 5.0), (5.0, 1.0), 0.0)
    ]

    for order in orders:
        order.ready_time = 0.0
        order.state = "READY"

    state = SimulationState(restaurants, couriers, orders, duration=3600)

    print(f"\nGeometry:")
    print(f"  Restaurants: line at y=5.0, x=1,3,5")
    print(f"  Diners: line at y=1.0, x=1,3,5")
    print(f"  Couriers: {len(couriers)}")
    print(f"  Orders: {len(orders)}")

    print("\n--- SIMPLE BUNDLING ---")
    simple_assignments = assign_simple_bundling(state, couriers, orders)
    print(f"Assignments: {len(simple_assignments)}")
    for c_id, o_ids in simple_assignments:
        print(f"  Courier {c_id} → Orders {o_ids}")
    simple_assigned_count = sum(len(o_ids) for _, o_ids in simple_assignments)
    print(f"Orders assigned: {simple_assigned_count}")
    print(f"Orders unassigned: {len(orders) - simple_assigned_count}")

    print("\n--- NETWORK BUNDLING ---")
    network_assignments = assign_network_bundling(state, couriers, orders)
    print(f"Assignments: {len(network_assignments)}")
    for c_id, o_ids in network_assignments:
        print(f"  Courier {c_id} → Orders {o_ids}")
    network_assigned_count = sum(len(o_ids) for _, o_ids in network_assignments)
    print(f"Orders assigned: {network_assigned_count}")
    print(f"Orders unassigned: {len(orders) - network_assigned_count}")

    print("\n--- RESULTS ---")
    print(f"Simple: {simple_assigned_count} orders")
    print(f"Network: {network_assigned_count} orders")

    assert network_assigned_count >= simple_assigned_count

    print("PASS")


# ============================================================================
# TEST 4
# ============================================================================

def test_intelligent_rejection():
    """
    Setup:
    - Restaurant 0 at (2.0, 2.0)
    - Restaurant 1 at (2.1, 2.0)
    - Order 0: R0 → Diner at (0.0, 5.0)
    - Order 1: R1 → Diner at (5.0, 0.0)
    - Courier 0 at (2.0, 1.0)
    - Courier 1 at (2.0, 3.0)
    """
    print("\n" + "="*80)
    print("TEST 4")
    print("="*80)

    restaurants = [
        Restaurant(0, (2.0, 2.0)),
        Restaurant(1, (2.1, 2.0))
    ]

    couriers = [
        Courier(0, (2.0, 1.0)),
        Courier(1, (2.0, 3.0))
    ]

    orders = [
        Order(0, 0, (2.0, 2.0), (0.0, 5.0), 0.0),
        Order(1, 1, (2.1, 2.0), (5.0, 0.0), 0.0)
    ]

    for order in orders:
        order.ready_time = 0.0
        order.state = "READY"

    state = SimulationState(restaurants, couriers, orders, duration=3600)

    r_dist = euclidean_distance(restaurants[0].location, restaurants[1].location) * 1000
    d_dist = euclidean_distance(orders[0].diner_location, orders[1].diner_location) * 1000

    print(f"\nGeometry:")
    print(f"  Restaurant distance: {r_dist:.1f}m")
    print(f"  Diner distance: {d_dist:.1f}m")
    print(f"  Couriers: {len(couriers)}")
    print(f"  Orders: {len(orders)}")

    bundled_cost_c0 = calculate_route_duration(
        couriers[0].current_location,
        [0, 1],
        state,
        use_tsp_optimization=True,
        include_service_times=True
    )

    single_cost_0 = calculate_route_duration(
        couriers[0].current_location,
        [0],
        state,
        use_tsp_optimization=False,
        include_service_times=True
    )

    single_cost_1 = calculate_route_duration(
        couriers[1].current_location,
        [1],
        state,
        use_tsp_optimization=False,
        include_service_times=True
    )

    combined_separate_cost = single_cost_0 + single_cost_1

    print(f"\nCost Calculation:")
    print(f"  Bundled route (C0 → [0,1]): {bundled_cost_c0:.1f}s")
    print(f"  Separate routes (C0→[0] + C1→[1]): {combined_separate_cost:.1f}s")
    print(f"  Bundling cost comparison: {bundled_cost_c0 - combined_separate_cost:+.1f}s")

    print("\n--- SIMPLE BUNDLING ---")
    simple_assignments = assign_simple_bundling(state, couriers, orders)
    print(f"Assignments: {len(simple_assignments)}")
    for c_id, o_ids in simple_assignments:
        print(f"  Courier {c_id} → Orders {o_ids}")
    simple_assigned_count = sum(len(o_ids) for _, o_ids in simple_assignments)
    simple_singles = sum(1 for _, o_ids in simple_assignments if len(o_ids) == 1)
    print(f"Orders assigned: {simple_assigned_count}")
    print(f"Single-order assignments: {simple_singles}")

    print("\n--- NETWORK BUNDLING ---")
    network_assignments = assign_network_bundling(state, couriers, orders)
    print(f"Assignments: {len(network_assignments)}")
    for c_id, o_ids in network_assignments:
        print(f"  Courier {c_id} → Orders {o_ids}")
    network_assigned_count = sum(len(o_ids) for _, o_ids in network_assignments)
    network_singles = sum(1 for _, o_ids in network_assignments if len(o_ids) == 1)
    network_bundles = sum(1 for _, o_ids in network_assignments if len(o_ids) > 1)
    print(f"Orders assigned: {network_assigned_count}")
    print(f"Single-order assignments: {network_singles}")
    print(f"Multi-order bundles: {network_bundles}")

    print("\n--- RESULTS ---")
    print(f"Simple: {simple_assigned_count} orders, {simple_singles} singles")
    print(f"Network: {network_assigned_count} orders, {network_singles} singles, {network_bundles} bundles")

    assert network_assigned_count == 2
    assert simple_assigned_count == 2

    print("PASS")


# ============================================================================
# TEST RUNNER
# ============================================================================

def run_all_tests():
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    log_path = f"/Users/pranjal/Code/meituan/tests/logs/test_network_bundling_{timestamp}.log"

    print("="*80)
    print("TEST SUITE: Network Bundling vs Simple Bundling")
    print("="*80)
    print(f"Log file: {log_path}")

    tests = [
        test_cross_street_pickup,
        test_en_route_dropoff,
        test_three_way_trade,
        test_intelligent_rejection
    ]

    passed = 0
    failed = 0
    errors = []

    for test in tests:
        try:
            test()
            passed += 1
        except AssertionError as e:
            print(f"\nFAIL: {test.__name__}")
            print(f"  {e}")
            errors.append((test.__name__, str(e)))
            failed += 1
        except Exception as e:
            print(f"\nERROR: {test.__name__}")
            print(f"  {e}")
            import traceback
            traceback.print_exc()
            errors.append((test.__name__, f"EXCEPTION: {e}"))
            failed += 1

    print("\n" + "="*80)
    print("SUMMARY")
    print("="*80)
    print(f"Total: {len(tests)}")
    print(f"Passed: {passed}")
    print(f"Failed: {failed}")

    if errors:
        print("\nFailed:")
        for test_name, error in errors:
            print(f"  {test_name}: {error[:200]}")

    return failed == 0


if __name__ == "__main__":
    import sys
    success = run_all_tests()
    sys.exit(0 if success else 1)
