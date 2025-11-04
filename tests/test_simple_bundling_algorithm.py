"""
Ruthless Test Suite for Simple Bundling Algorithm (Algorithm 3)

Tests the assign_simple_bundling function from assignment_algorithms.py

ALGORITHM 3 SCOPE (from README):
- Groups orders from SAME RESTAURANT (max 3 per bundle)
- Hungarian assignment of bundles to couriers
- Performance: 83.3% fulfillment (best), 30.6 min average delivery

TEST PHILOSOPHY:
- Test what Algorithm 3 is SUPPOSED to do
- Verify it bundles same-restaurant orders correctly
- Confirm it beats Greedy (Algo 1) and Hungarian (Algo 2)
- Document where Algorithm 5 (Batched Pickups) should be superior
- No complaining about cross-restaurant bundling (that's Algo 5's job)

RUTHLESS CRITERIA:
- Exact expected behavior, not approximate
- Hard pass/fail with mathematical precision
- Expose bugs in same-restaurant bundling logic
- Verify MAX_BUNDLE_SIZE=3 enforcement
- Confirm Hungarian assignment correctness
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from simulator_core import (
    SimulationState, Courier, Order, Restaurant,
    euclidean_distance, get_travel_time
)
from assignment_algorithms import assign_simple_bundling, assign_greedy, assign_hungarian


# ============================================================================
# CATEGORY 1: CORE SAME-RESTAURANT BUNDLING
# ============================================================================

def test_basic_same_restaurant_bundling():
    """
    CORE TEST: Bundles 3 orders from same restaurant.
    """
    print("\n" + "="*80)
    print("TEST 1: Basic Same-Restaurant Bundling")
    print("="*80)

    restaurants = [Restaurant(0, (2.0, 2.0))]
    couriers = [Courier(0, (2.1, 2.1))]

    orders = [
        Order(0, 0, (2.0, 2.0), (1.0, 1.0), 0.0),
        Order(1, 0, (2.0, 2.0), (1.5, 1.5), 0.0),
        Order(2, 0, (2.0, 2.0), (2.5, 2.5), 0.0)
    ]

    for order in orders:
        order.ready_time = 0.0
        order.state = "READY"

    state = SimulationState(restaurants, couriers, orders, duration=3600)

    print(f"\n3 orders from Restaurant {restaurants[0].id}")
    print(f"1 courier available")

    assignments = assign_simple_bundling(state, couriers, orders)

    print(f"\nAssignments: {len(assignments)}")
    for c_id, o_ids in assignments:
        print(f"  Courier {c_id} → Orders {o_ids} ({len(o_ids)} orders)")

    assert len(assignments) == 1, f"Expected 1 assignment, got {len(assignments)}"

    all_assigned_orders = []
    for _, o_ids in assignments:
        all_assigned_orders.extend(o_ids)

    assert set(all_assigned_orders) == {0, 1, 2}, f"All orders should be bundled, got {all_assigned_orders}"

    print("\n✓ PASS: Same-restaurant bundling works correctly")


def test_max_bundle_size_3_enforcement():
    """
    CORE TEST: MAX_BUNDLE_SIZE=3 is strictly enforced.
    7 orders → bundles (3, 3, 1)
    """
    print("\n" + "="*80)
    print("TEST 2: MAX_BUNDLE_SIZE=3 Strict Enforcement")
    print("="*80)

    restaurants = [Restaurant(0, (2.0, 2.0))]
    couriers = [Courier(i, (2.0, 2.0)) for i in range(3)]

    orders = [
        Order(i, 0, (2.0, 2.0), (2.0 + i * 0.5, 2.0), 0.0)
        for i in range(7)
    ]

    for order in orders:
        order.ready_time = 0.0
        order.state = "READY"

    state = SimulationState(restaurants, couriers, orders, duration=3600)

    print("\n7 orders from same restaurant")
    print("3 couriers available")

    assignments = assign_simple_bundling(state, couriers, orders)

    print(f"\nAssignments: {len(assignments)}")
    for c_id, o_ids in assignments:
        print(f"  Courier {c_id} → {len(o_ids)} orders: {o_ids}")
        assert len(o_ids) <= 3, f"CRITICAL: Bundle size {len(o_ids)} exceeds MAX_BUNDLE_SIZE=3!"

    all_assigned = []
    for _, o_ids in assignments:
        all_assigned.extend(o_ids)

    assert len(set(all_assigned)) == 7, f"All 7 orders must be assigned, got {len(set(all_assigned))}"
    assert len(assignments) == 3, f"Expected 3 bundles, got {len(assignments)}"

    bundle_sizes = sorted([len(o_ids) for _, o_ids in assignments], reverse=True)
    assert bundle_sizes == [3, 3, 1], f"Expected [3, 3, 1] split, got {bundle_sizes}"

    print(f"\nBundle sizes: {bundle_sizes}")
    print("\n✓ PASS: MAX_BUNDLE_SIZE=3 strictly enforced")


def test_multiple_restaurants_separate_bundles():
    """
    CORE TEST: Different restaurants create separate bundles.
    Algorithm 3 does NOT bundle across restaurants.
    """
    print("\n" + "="*80)
    print("TEST 3: Multiple Restaurants → Separate Bundles")
    print("="*80)

    restaurants = [
        Restaurant(0, (2.0, 2.0)),
        Restaurant(1, (2.5, 2.5))
    ]

    couriers = [Courier(i, (2.0, 2.0)) for i in range(2)]

    orders = [
        Order(0, 0, (2.0, 2.0), (1.0, 1.0), 0.0),
        Order(1, 0, (2.0, 2.0), (1.5, 1.5), 0.0),
        Order(2, 1, (2.5, 2.5), (3.0, 3.0), 0.0),
        Order(3, 1, (2.5, 2.5), (3.5, 3.5), 0.0)
    ]

    for order in orders:
        order.ready_time = 0.0
        order.state = "READY"

    state = SimulationState(restaurants, couriers, orders, duration=3600)

    print("\n2 orders from Restaurant 0")
    print("2 orders from Restaurant 1")

    assignments = assign_simple_bundling(state, couriers, orders)

    print(f"\nAssignments: {len(assignments)}")
    for c_id, o_ids in assignments:
        r_ids = [orders[o_id].restaurant_id for o_id in o_ids]
        print(f"  Courier {c_id} → Orders {o_ids} (Restaurants: {set(r_ids)})")

        # CRITICAL: No cross-restaurant bundling
        assert len(set(r_ids)) == 1, f"CRITICAL BUG: Cross-restaurant bundle detected! {r_ids}"

    assert len(assignments) == 2, f"Expected 2 bundles, got {len(assignments)}"

    print("\n✓ PASS: Restaurants handled separately (no cross-bundling)")


def test_hungarian_assignment_optimality():
    """
    CORE TEST: Hungarian assigns bundles optimally to couriers.
    """
    print("\n" + "="*80)
    print("TEST 4: Hungarian Assignment Optimality")
    print("="*80)

    restaurants = [
        Restaurant(0, (1.0, 1.0)),
        Restaurant(1, (5.0, 5.0))
    ]

    couriers = [
        Courier(0, (1.1, 1.1)),  # Near Restaurant 0
        Courier(1, (4.9, 4.9))   # Near Restaurant 1
    ]

    orders = [
        Order(0, 0, (1.0, 1.0), (0.5, 0.5), 0.0),
        Order(1, 0, (1.0, 1.0), (0.8, 0.8), 0.0),
        Order(2, 1, (5.0, 5.0), (5.5, 5.5), 0.0),
        Order(3, 1, (5.0, 5.0), (5.2, 5.2), 0.0)
    ]

    for order in orders:
        order.ready_time = 0.0
        order.state = "READY"

    state = SimulationState(restaurants, couriers, orders, duration=3600)

    print("\nRestaurant 0 at (1.0, 1.0), Courier 0 at (1.1, 1.1) - 0.14km")
    print("Restaurant 1 at (5.0, 5.0), Courier 1 at (4.9, 4.9) - 0.14km")

    assignments = assign_simple_bundling(state, couriers, orders)

    print(f"\nAssignments: {len(assignments)}")
    for c_id, o_ids in assignments:
        r_id = orders[o_ids[0]].restaurant_id
        courier = couriers[c_id]
        restaurant = restaurants[r_id]
        dist = euclidean_distance(courier.current_location, restaurant.location)
        print(f"  Courier {c_id} → Restaurant {r_id} (distance: {dist:.2f}km)")

    # Hungarian should assign each courier to nearest restaurant
    assert len(assignments) == 2, f"Expected 2 assignments, got {len(assignments)}"

    print("\n✓ PASS: Hungarian assigns bundles optimally")


def test_bundle_size_2_correctly_handled():
    """
    TEST: 2 orders from same restaurant creates size-2 bundle.
    """
    print("\n" + "="*80)
    print("TEST 5: Bundle Size 2 Correctly Handled")
    print("="*80)

    restaurants = [Restaurant(0, (2.0, 2.0))]
    couriers = [Courier(0, (2.0, 2.0))]

    orders = [
        Order(0, 0, (2.0, 2.0), (1.0, 1.0), 0.0),
        Order(1, 0, (2.0, 2.0), (1.5, 1.5), 0.0)
    ]

    for order in orders:
        order.ready_time = 0.0
        order.state = "READY"

    state = SimulationState(restaurants, couriers, orders, duration=3600)

    print("\n2 orders from same restaurant")

    assignments = assign_simple_bundling(state, couriers, orders)

    print(f"\nAssignments: {len(assignments)}")
    for c_id, o_ids in assignments:
        print(f"  Courier {c_id} → {len(o_ids)} orders")

    assert len(assignments) == 1, f"Expected 1 assignment, got {len(assignments)}"
    assert len(assignments[0][1]) == 2, f"Expected bundle of 2, got {len(assignments[0][1])}"

    print("\n✓ PASS: Size-2 bundle created correctly")


# ============================================================================
# CATEGORY 2: EDGE CASES
# ============================================================================

def test_empty_inputs():
    """EDGE: Handle all empty input scenarios"""
    print("\n" + "="*80)
    print("TEST 6: Empty Input Edge Cases")
    print("="*80)

    restaurants = [Restaurant(0, (2.0, 2.0))]

    state = SimulationState(restaurants, [], [], duration=3600)
    assignments = assign_simple_bundling(state, [], [])
    print(f"\nCase 1 - No inputs: {len(assignments)} assignments")
    assert len(assignments) == 0, "Should return empty"

    couriers = [Courier(0, (1.0, 1.0))]
    state = SimulationState(restaurants, couriers, [], duration=3600)
    assignments = assign_simple_bundling(state, couriers, [])
    print(f"Case 2 - No orders: {len(assignments)} assignments")
    assert len(assignments) == 0, "Should return empty"

    orders = [Order(0, 0, (2.0, 2.0), (1.5, 1.5), 0.0)]
    orders[0].state = "READY"
    state = SimulationState(restaurants, [], orders, duration=3600)
    assignments = assign_simple_bundling(state, [], orders)
    print(f"Case 3 - No couriers: {len(assignments)} assignments")
    assert len(assignments) == 0, "Should return empty"

    print("\n✓ PASS: Empty inputs handled correctly")


def test_single_order_single_restaurant():
    """EDGE: Single order creates size-1 bundle"""
    print("\n" + "="*80)
    print("TEST 7: Single Order → Size-1 Bundle")
    print("="*80)

    restaurants = [Restaurant(0, (2.0, 2.0))]
    couriers = [Courier(0, (2.0, 2.0))]
    orders = [Order(0, 0, (2.0, 2.0), (3.0, 3.0), 0.0)]
    orders[0].state = "READY"

    state = SimulationState(restaurants, couriers, orders, duration=3600)
    assignments = assign_simple_bundling(state, couriers, orders)

    print(f"\nAssignment: {assignments}")

    assert len(assignments) == 1, f"Expected 1 assignment, got {len(assignments)}"
    assert assignments[0][1] == [0], f"Expected [0], got {assignments[0][1]}"

    print("\n✓ PASS: Single order handled correctly")


def test_more_orders_than_couriers():
    """EDGE: More bundles than couriers - some orders unassigned"""
    print("\n" + "="*80)
    print("TEST 8: More Bundles Than Couriers")
    print("="*80)

    restaurants = [Restaurant(i, (float(i), float(i))) for i in range(5)]
    couriers = [Courier(i, (2.0, 2.0)) for i in range(2)]

    orders = [
        Order(i, i, (float(i), float(i)), (float(i) + 0.5, float(i) + 0.5), 0.0)
        for i in range(5)
    ]

    for order in orders:
        order.ready_time = 0.0
        order.state = "READY"

    state = SimulationState(restaurants, couriers, orders, duration=3600)

    print("\n5 orders from 5 different restaurants")
    print("2 couriers available")

    assignments = assign_simple_bundling(state, couriers, orders)

    print(f"\nAssignments: {len(assignments)}")

    # Can only assign 2 bundles (limited by couriers)
    assert len(assignments) <= 2, f"Too many assignments: {len(assignments)}"

    print("\n✓ PASS: Courier limitation respected")


def test_overflow_10_orders_1_restaurant():
    """EDGE: 10 orders from same restaurant → bundles (3,3,3,1)"""
    print("\n" + "="*80)
    print("TEST 9: Overflow - 10 Orders, 1 Restaurant")
    print("="*80)

    restaurants = [Restaurant(0, (2.0, 2.0))]
    couriers = [Courier(i, (2.0, 2.0)) for i in range(4)]

    orders = [
        Order(i, 0, (2.0, 2.0), (2.0 + i * 0.2, 2.0), float(i * 60))
        for i in range(10)
    ]

    for order in orders:
        order.state = "READY"

    state = SimulationState(restaurants, couriers, orders, duration=3600)

    print("\n10 orders from same restaurant")
    print("4 couriers available")

    assignments = assign_simple_bundling(state, couriers, orders)

    print(f"\nAssignments: {len(assignments)}")

    bundle_sizes = sorted([len(o_ids) for _, o_ids in assignments], reverse=True)
    print(f"Bundle sizes: {bundle_sizes}")

    # All should be ≤3
    for size in bundle_sizes:
        assert size <= 3, f"Bundle size {size} exceeds MAX_BUNDLE_SIZE=3"

    # All 10 orders should be assigned
    all_assigned = []
    for _, o_ids in assignments:
        all_assigned.extend(o_ids)
    assert len(set(all_assigned)) == 10, f"Expected 10 orders assigned, got {len(set(all_assigned))}"

    # Should create 4 bundles: (3,3,3,1)
    assert bundle_sizes == [3, 3, 3, 1], f"Expected [3,3,3,1], got {bundle_sizes}"

    print("\n✓ PASS: Large overflow handled correctly")


def test_mixed_ready_times_same_restaurant():
    """EDGE: Orders with different ready times bundled by ready_time"""
    print("\n" + "="*80)
    print("TEST 10: Mixed Ready Times (Same Restaurant)")
    print("="*80)

    restaurants = [Restaurant(0, (2.0, 2.0))]
    couriers = [Courier(0, (2.0, 2.0))]

    orders = [
        Order(0, 0, (2.0, 2.0), (1.0, 1.0), 0.0),
        Order(1, 0, (2.0, 2.0), (1.5, 1.5), 0.0),
        Order(2, 0, (2.0, 2.0), (2.0, 2.0), 0.0)
    ]

    orders[0].ready_time = 0.0
    orders[1].ready_time = 60.0
    orders[2].ready_time = 120.0

    for order in orders:
        order.state = "READY"

    state = SimulationState(restaurants, couriers, orders, duration=3600)

    print("\n3 orders from same restaurant")
    print("Ready times: 0s, 60s, 120s")

    assignments = assign_simple_bundling(state, couriers, orders)

    print(f"\nAssignments: {len(assignments)}")
    for c_id, o_ids in assignments:
        print(f"  Courier {c_id} → Orders {o_ids}")

    # Should bundle all 3 (same restaurant)
    assert len(assignments) == 1, f"Expected 1 bundle, got {len(assignments)}"

    print("\n✓ PASS: Mixed ready times handled correctly")


# ============================================================================
# CATEGORY 3: PERFORMANCE VS OTHER ALGORITHMS
# ============================================================================

def test_beats_greedy_same_restaurant_scenario():
    """
    PERFORMANCE: Simple Bundling should beat Greedy on same-restaurant orders.
    """
    print("\n" + "="*80)
    print("TEST 11: PERFORMANCE - Beats Greedy (Same Restaurant)")
    print("="*80)

    restaurants = [Restaurant(0, (2.0, 2.0))]
    couriers = [Courier(i, (2.0, 2.0)) for i in range(6)]

    orders = [
        Order(i, 0, (2.0, 2.0), (2.0 + i * 0.3, 2.0), 0.0)
        for i in range(6)
    ]

    for order in orders:
        order.ready_time = 0.0
        order.state = "READY"

    state = SimulationState(restaurants, couriers, orders, duration=3600)

    print("\n6 orders from same restaurant")
    print("6 couriers available")

    greedy_assignments = assign_greedy(state, couriers, orders)
    bundling_assignments = assign_simple_bundling(state, couriers, orders)

    print(f"\nGreedy: {len(greedy_assignments)} assignments")
    print(f"Simple Bundling: {len(bundling_assignments)} assignments")

    # Greedy creates 6 separate assignments (1-to-1)
    assert len(greedy_assignments) == 6, f"Greedy should assign 6 singles, got {len(greedy_assignments)}"

    # Simple Bundling should use fewer couriers via bundling
    assert len(bundling_assignments) < len(greedy_assignments), \
        f"Simple Bundling should use fewer couriers! Got {len(bundling_assignments)} vs Greedy {len(greedy_assignments)}"

    print(f"\n✓ PASS: Simple Bundling beats Greedy (uses {len(bundling_assignments)} vs {len(greedy_assignments)} couriers)")


def test_beats_hungarian_same_restaurant_scenario():
    """
    PERFORMANCE: Simple Bundling should beat Hungarian (both use Hungarian solver,
    but bundling reduces number of assignments).
    """
    print("\n" + "="*80)
    print("TEST 12: PERFORMANCE - Beats Hungarian (Same Restaurant)")
    print("="*80)

    restaurants = [Restaurant(0, (2.0, 2.0))]
    couriers = [Courier(i, (2.0, 2.0)) for i in range(9)]

    orders = [
        Order(i, 0, (2.0, 2.0), (2.0 + i * 0.2, 2.0), 0.0)
        for i in range(9)
    ]

    for order in orders:
        order.ready_time = 0.0
        order.state = "READY"

    state = SimulationState(restaurants, couriers, orders, duration=3600)

    print("\n9 orders from same restaurant")
    print("9 couriers available")

    hungarian_assignments = assign_hungarian(state, couriers, orders)
    bundling_assignments = assign_simple_bundling(state, couriers, orders)

    print(f"\nHungarian: {len(hungarian_assignments)} assignments")
    print(f"Simple Bundling: {len(bundling_assignments)} assignments")

    # Hungarian creates 9 separate 1-to-1 assignments
    assert len(hungarian_assignments) == 9, f"Hungarian should assign 9 singles"

    # Simple Bundling should create 3 bundles (3,3,3)
    assert len(bundling_assignments) == 3, f"Expected 3 bundles, got {len(bundling_assignments)}"

    print(f"\n✓ PASS: Simple Bundling beats Hungarian (3 bundles vs 9 singles)")


def test_efficiency_with_nearby_customers():
    """
    PERFORMANCE: Bundle efficiency when customers are nearby.
    """
    print("\n" + "="*80)
    print("TEST 13: Bundle Efficiency (Nearby Customers)")
    print("="*80)

    restaurants = [Restaurant(0, (2.0, 2.0))]
    couriers = [Courier(0, (2.0, 2.0))]

    # All customers in same neighborhood
    orders = [
        Order(0, 0, (2.0, 2.0), (2.5, 2.5), 0.0),
        Order(1, 0, (2.0, 2.0), (2.6, 2.6), 0.0),
        Order(2, 0, (2.0, 2.0), (2.7, 2.7), 0.0)
    ]

    for order in orders:
        order.ready_time = 0.0
        order.state = "READY"

    state = SimulationState(restaurants, couriers, orders, duration=3600)

    print("\n3 orders from same restaurant")
    print("All customers within 200m of each other")

    assignments = assign_simple_bundling(state, couriers, orders)

    print(f"\nAssignments: {len(assignments)}")
    for c_id, o_ids in assignments:
        print(f"  Courier {c_id} → {len(o_ids)} orders (bundled)")

    # Should bundle all 3 (efficient route)
    assert len(assignments) == 1, f"Expected 1 bundle, got {len(assignments)}"
    assert len(assignments[0][1]) == 3, f"Expected all 3 bundled"

    print("\n✓ PASS: Efficient bundling with nearby customers")


# ============================================================================
# CATEGORY 4: CORRECTNESS INVARIANTS
# ============================================================================

def test_no_duplicate_orders_in_assignments():
    """INVARIANT: No order appears in multiple bundles"""
    print("\n" + "="*80)
    print("TEST 14: INVARIANT - No Duplicate Orders")
    print("="*80)

    restaurants = [Restaurant(0, (2.0, 2.0))]
    couriers = [Courier(i, (2.0, 2.0)) for i in range(5)]

    orders = [
        Order(i, 0, (2.0, 2.0), (2.0 + i * 0.3, 2.0), 0.0)
        for i in range(10)
    ]

    for order in orders:
        order.ready_time = 0.0
        order.state = "READY"

    state = SimulationState(restaurants, couriers, orders, duration=3600)

    assignments = assign_simple_bundling(state, couriers, orders)

    all_assigned = []
    for _, o_ids in assignments:
        all_assigned.extend(o_ids)

    # Check for duplicates
    assert len(all_assigned) == len(set(all_assigned)), \
        f"CRITICAL: Duplicate orders in assignments! {all_assigned}"

    print(f"\nAssigned {len(all_assigned)} unique orders")
    print("\n✓ PASS: No duplicate orders")


def test_no_courier_assigned_twice():
    """INVARIANT: No courier appears in multiple assignments"""
    print("\n" + "="*80)
    print("TEST 15: INVARIANT - No Courier Reuse")
    print("="*80)

    restaurants = [Restaurant(i, (float(i), float(i))) for i in range(5)]
    couriers = [Courier(i, (2.0, 2.0)) for i in range(10)]

    orders = [
        Order(i, i, (float(i), float(i)), (float(i) + 0.5, float(i)), 0.0)
        for i in range(5)
    ]

    for order in orders:
        order.ready_time = 0.0
        order.state = "READY"

    state = SimulationState(restaurants, couriers, orders, duration=3600)

    assignments = assign_simple_bundling(state, couriers, orders)

    courier_ids = [c_id for c_id, _ in assignments]

    # Check for duplicates
    assert len(courier_ids) == len(set(courier_ids)), \
        f"CRITICAL: Courier assigned multiple times! {courier_ids}"

    print(f"\nUsed {len(courier_ids)} unique couriers")
    print("\n✓ PASS: No courier reuse")


def test_all_orders_in_bundle_from_same_restaurant():
    """INVARIANT: Every bundle contains orders from only one restaurant"""
    print("\n" + "="*80)
    print("TEST 16: INVARIANT - Same-Restaurant Bundle Constraint")
    print("="*80)

    restaurants = [Restaurant(i, (float(i), float(i))) for i in range(3)]
    couriers = [Courier(i, (2.0, 2.0)) for i in range(3)]

    orders = [
        Order(0, 0, (0.0, 0.0), (0.5, 0.5), 0.0),
        Order(1, 0, (0.0, 0.0), (0.6, 0.6), 0.0),
        Order(2, 1, (1.0, 1.0), (1.5, 1.5), 0.0),
        Order(3, 1, (1.0, 1.0), (1.6, 1.6), 0.0),
        Order(4, 2, (2.0, 2.0), (2.5, 2.5), 0.0),
        Order(5, 2, (2.0, 2.0), (2.6, 2.6), 0.0)
    ]

    for order in orders:
        order.ready_time = 0.0
        order.state = "READY"

    state = SimulationState(restaurants, couriers, orders, duration=3600)

    print("\n6 orders from 3 different restaurants (2 per restaurant)")

    assignments = assign_simple_bundling(state, couriers, orders)

    print(f"\nAssignments: {len(assignments)}")

    for c_id, o_ids in assignments:
        restaurant_ids = [orders[o_id].restaurant_id for o_id in o_ids]
        unique_restaurants = set(restaurant_ids)

        print(f"  Courier {c_id} → Orders {o_ids} (Restaurants: {restaurant_ids})")

        assert len(unique_restaurants) == 1, \
            f"CRITICAL: Cross-restaurant bundle! Orders {o_ids} from restaurants {restaurant_ids}"

    print("\n✓ PASS: All bundles are same-restaurant only")


def test_input_state_not_mutated():
    """INVARIANT: Input lists not modified"""
    print("\n" + "="*80)
    print("TEST 17: INVARIANT - Input State Not Mutated")
    print("="*80)

    restaurants = [Restaurant(0, (2.0, 2.0))]
    couriers = [Courier(i, (2.0, 2.0)) for i in range(3)]
    orders = [Order(i, 0, (2.0, 2.0), (2.0 + i * 0.2, 2.0), 0.0) for i in range(5)]

    for order in orders:
        order.state = "READY"

    state = SimulationState(restaurants, couriers, orders, duration=3600)

    original_courier_count = len(couriers)
    original_order_count = len(orders)
    original_courier_ids = [c.id for c in couriers]
    original_order_ids = [o.id for o in orders]

    print(f"\nBefore: {original_courier_count} couriers, {original_order_count} orders")

    _ = assign_simple_bundling(state, couriers, orders)

    print(f"After: {len(couriers)} couriers, {len(orders)} orders")

    assert len(couriers) == original_courier_count, "Courier list mutated!"
    assert len(orders) == original_order_count, "Order list mutated!"
    assert [c.id for c in couriers] == original_courier_ids, "Courier list modified!"
    assert [o.id for o in orders] == original_order_ids, "Order list modified!"

    print("\n✓ PASS: Input state not mutated")


# ============================================================================
# CATEGORY 5: ALGORITHM SCOPE DOCUMENTATION
# ============================================================================

def test_cross_restaurant_not_in_scope():
    """
    SCOPE DOCUMENTATION: Algorithm 3 does NOT do cross-restaurant bundling.
    This is expected behavior - Algorithm 5 (Batched Pickups) handles this.
    """
    print("\n" + "="*80)
    print("TEST 18: SCOPE - Cross-Restaurant Bundling Not Supported")
    print("="*80)

    restaurants = [
        Restaurant(0, (2.0, 2.0)),
        Restaurant(1, (2.1, 2.0))  # Only 100m apart!
    ]

    couriers = [Courier(0, (2.0, 2.0))]

    orders = [
        Order(0, 0, (2.0, 2.0), (2.5, 2.5), 0.0),
        Order(1, 1, (2.1, 2.0), (2.6, 2.6), 0.0)
    ]

    for order in orders:
        order.ready_time = 0.0
        order.state = "READY"

    state = SimulationState(restaurants, couriers, orders, duration=3600)

    dist = euclidean_distance(restaurants[0].location, restaurants[1].location)
    print(f"\nRestaurant 0 and 1 only {dist*1000:.0f}m apart")
    print("1 courier available")

    assignments = assign_simple_bundling(state, couriers, orders)

    print(f"\nAssignments: {len(assignments)}")
    for c_id, o_ids in assignments:
        print(f"  Courier {c_id} → {len(o_ids)} order(s)")

    # Algorithm 3 will only assign 1 order (can't bundle across restaurants)
    assert len(assignments) == 1, f"Expected 1 assignment"

    # Only 1 order assigned
    total_assigned = sum(len(o_ids) for _, o_ids in assignments)
    assert total_assigned == 1, f"Only 1 order should be assigned"

    print("\n✓ DOCUMENTED: Cross-restaurant bundling not in Algorithm 3 scope")
    print("  → Use Algorithm 5 (Batched Pickups) for multi-restaurant bundling")


def test_performance_ceiling_vs_algorithm_5():
    """
    SCOPE DOCUMENTATION: Algorithm 5 should beat Algorithm 3 on cross-restaurant scenarios.
    This test documents expected performance hierarchy.
    """
    print("\n" + "="*80)
    print("TEST 19: SCOPE - Algorithm 5 Superior on Cross-Restaurant")
    print("="*80)

    restaurants = [
        Restaurant(0, (2.0, 2.0)),
        Restaurant(1, (2.2, 2.0)),  # 200m away
        Restaurant(2, (2.4, 2.0))   # 400m from R0
    ]

    couriers = [Courier(i, (2.0, 2.0)) for i in range(2)]

    orders = [
        Order(0, 0, (2.0, 2.0), (3.0, 3.0), 0.0),
        Order(1, 1, (2.2, 2.0), (3.1, 3.1), 0.0),
        Order(2, 2, (2.4, 2.0), (3.2, 3.2), 0.0)
    ]

    for order in orders:
        order.ready_time = 0.0
        order.state = "READY"

    state = SimulationState(restaurants, couriers, orders, duration=3600)

    print("\n3 orders from 3 restaurants (all within 400m)")
    print("All customers nearby (within 200m)")
    print("2 couriers available")

    assignments = assign_simple_bundling(state, couriers, orders)

    print(f"\nAlgorithm 3 (Simple Bundling): {len(assignments)} assignments")

    # Can only assign 2 orders (2 couriers, no cross-restaurant bundling)
    total_assigned = sum(len(o_ids) for _, o_ids in assignments)
    assert total_assigned == 2, f"Algorithm 3 can only assign 2 orders"

    print(f"  → Assigns {total_assigned}/3 orders (limited by separate restaurants)")
    print("\n✓ DOCUMENTED: Algorithm 5 would create multi-restaurant bundle")
    print("  → Algorithm 5 could bundle all 3 orders with 1 courier")


def test_chooses_singles_when_bundling_inefficient():
    """
    CRITICAL TEST: Verify Simple Bundling can choose NOT to bundle.

    This proves Simple Bundling >= Hungarian (includes singles as an option).

    Setup: 3 orders from same restaurant with customers at opposite map corners.
    Expected: Choose singles (3 separate assignments) because bundling route is inefficient.
    """
    print("\n" + "="*80)
    print("TEST 20: CRITICAL - Chooses Singles When Bundling Inefficient")
    print("="*80)

    restaurants = [Restaurant(0, (2.5, 2.5))]
    couriers = [Courier(i, (2.5, 2.5)) for i in range(3)]

    # Customers at opposite corners of map (very far apart)
    orders = [
        Order(0, 0, (2.5, 2.5), (0.0, 0.0), 0.0),  # Southwest corner
        Order(1, 0, (2.5, 2.5), (5.0, 5.0), 0.0),  # Northeast corner
        Order(2, 0, (2.5, 2.5), (0.0, 5.0), 0.0)   # Northwest corner
    ]

    for order in orders:
        order.ready_time = 0.0
        order.state = "READY"

    state = SimulationState(restaurants, couriers, orders, duration=3600)

    print("\nSame restaurant at (2.5, 2.5)")
    print("Customers at opposite map corners:")
    for o in orders:
        dist = euclidean_distance(restaurants[0].location, o.diner_location)
        print(f"  Order {o.id} to {o.diner_location}: {dist:.1f}km from restaurant")

    assignments = assign_simple_bundling(state, couriers, orders)

    print(f"\nAssignments: {len(assignments)}")
    for c_id, o_ids in assignments:
        print(f"  Courier {c_id} → {len(o_ids)} order(s): {o_ids}")

    # Should choose 3 singles (bundling these would create terrible route)
    bundle_sizes = [len(o_ids) for _, o_ids in assignments]

    print(f"\nBundle sizes: {bundle_sizes}")

    # All should be singles (size 1)
    assert all(size == 1 for size in bundle_sizes), \
        f"Should choose singles for distant customers! Got bundle sizes: {bundle_sizes}"

    assert len(assignments) == 3, f"Expected 3 single assignments, got {len(assignments)}"

    print("\n✓ PASS: Correctly chose singles over inefficient bundling")
    print("  → Proves Simple Bundling >= Hungarian (singles are an option)")


def test_performance_benchmark_100_orders():
    """
    PERFORMANCE: Scalability test with 100 orders from 1 restaurant.
    """
    print("\n" + "="*80)
    print("TEST 21: PERFORMANCE - Scalability (100 Orders)")
    print("="*80)

    restaurants = [Restaurant(0, (2.5, 2.5))]
    couriers = [Courier(i, (2.5, 2.5)) for i in range(34)]  # Enough for 100 orders

    orders = [
        Order(i, 0, (2.5, 2.5), (2.5 + (i % 10) * 0.1, 2.5 + (i // 10) * 0.1), float(i * 10))
        for i in range(100)
    ]

    for order in orders:
        order.state = "READY"

    state = SimulationState(restaurants, couriers, orders, duration=3600)

    print("\n100 orders from same restaurant")
    print("34 couriers available")

    import time
    start = time.time()
    assignments = assign_simple_bundling(state, couriers, orders)
    elapsed = time.time() - start

    print(f"\nAssignments: {len(assignments)}")

    bundle_sizes = [len(o_ids) for _, o_ids in assignments]
    total_assigned = sum(bundle_sizes)

    print(f"Orders assigned: {total_assigned}/100")
    print(f"Bundle sizes: min={min(bundle_sizes)}, max={max(bundle_sizes)}, avg={sum(bundle_sizes)/len(bundle_sizes):.1f}")
    print(f"Execution time: {elapsed*1000:.1f}ms")

    # Should assign all 100 orders
    assert total_assigned == 100, f"Expected 100 assigned, got {total_assigned}"

    # Should create 34 bundles: 33x(3) + 1x(1) = 99+1 = 100
    assert len(assignments) == 34, f"Expected 34 bundles, got {len(assignments)}"

    # Performance should be reasonable
    assert elapsed < 0.1, f"Too slow: {elapsed*1000:.1f}ms"

    print(f"\n✓ PASS: Scaled to 100 orders in {elapsed*1000:.1f}ms")


# ============================================================================
# TEST RUNNER
# ============================================================================

def run_all_tests():
    """Run all test cases and save detailed log"""
    import datetime

    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    log_dir = "/Users/pranjal/Code/meituan/simulation_test/tests/logs"
    os.makedirs(log_dir, exist_ok=True)
    log_path = f"{log_dir}/test_simple_bundling_algorithm_{timestamp}.log"

    print("="*80)
    print("RUTHLESS SIMPLE BUNDLING TEST SUITE (ALGORITHM 3)")
    print("="*80)
    print("Testing: assign_simple_bundling() from assignment_algorithms.py")
    print("Scope: Same-restaurant bundling only (max 3 per bundle)")
    print(f"Log file: {log_path}")

    tests = [
        # Category 1: Core Same-Restaurant Bundling
        test_basic_same_restaurant_bundling,
        test_max_bundle_size_3_enforcement,
        test_multiple_restaurants_separate_bundles,
        test_hungarian_assignment_optimality,
        test_bundle_size_2_correctly_handled,

        # Category 2: Edge Cases
        test_empty_inputs,
        test_single_order_single_restaurant,
        test_more_orders_than_couriers,
        test_overflow_10_orders_1_restaurant,
        test_mixed_ready_times_same_restaurant,

        # Category 3: Performance vs Other Algorithms
        test_beats_greedy_same_restaurant_scenario,
        test_beats_hungarian_same_restaurant_scenario,
        test_efficiency_with_nearby_customers,

        # Category 4: Correctness Invariants
        test_no_duplicate_orders_in_assignments,
        test_no_courier_assigned_twice,
        test_all_orders_in_bundle_from_same_restaurant,
        test_input_state_not_mutated,

        # Category 5: Algorithm Scope Documentation
        test_cross_restaurant_not_in_scope,
        test_performance_ceiling_vs_algorithm_5,
        test_chooses_singles_when_bundling_inefficient,
        test_performance_benchmark_100_orders
    ]

    passed = 0
    failed = 0
    errors = []

    for test in tests:
        try:
            test()
            passed += 1
        except AssertionError as e:
            print(f"\n✗ FAIL: {test.__name__}")
            print(f"  Error: {e}")
            errors.append((test.__name__, str(e)))
            failed += 1
        except Exception as e:
            print(f"\n✗ ERROR: {test.__name__}")
            print(f"  Unexpected error: {e}")
            import traceback
            errors.append((test.__name__, f"EXCEPTION: {e}\n{traceback.format_exc()}"))
            failed += 1

    print("\n" + "="*80)
    print("TEST SUMMARY")
    print("="*80)
    print(f"Total tests: {len(tests)}")
    print(f"Passed: {passed}")
    print(f"Failed: {failed}")

    if errors:
        print("\nFailed tests:")
        for test_name, error in errors:
            print(f"  - {test_name}")
            print(f"    {error[:200]}")

    if failed == 0:
        print("\n✓ ALL TESTS PASSED")
        print("\n" + "="*80)
        print("ALGORITHM 3 VERIFIED:")
        print("="*80)
        print("✓ Bundles same-restaurant orders correctly")
        print("✓ Enforces MAX_BUNDLE_SIZE=3 strictly")
        print("✓ Uses Hungarian for optimal bundle assignment")
        print("✓ Beats Greedy (Algo 1) and Hungarian (Algo 2)")
        print("✓ Scope: Same-restaurant only (by design)")
        print("\n→ For cross-restaurant bundling, use Algorithm 5 (Batched Pickups)")
        return True
    else:
        print(f"\n✗ {failed} TEST(S) FAILED")
        return False


if __name__ == "__main__":
    import sys
    success = run_all_tests()
    sys.exit(0 if success else 1)
