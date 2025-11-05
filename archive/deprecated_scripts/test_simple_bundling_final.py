"""
Advanced Test Suite for Simple Bundling Algorithm (Final Corrected Version)
"""

import sys
import numpy as np
from dataclasses import dataclass
from typing import List, Tuple

# Import the algorithm under test
from assignment_algorithms import assign_simple_bundling
from simulator_core import Order, Courier, SimulationState, Restaurant


# ============================================================================
# TEST UTILITIES
# ============================================================================

def create_order(order_id: int, restaurant_id: int, restaurant_loc: Tuple[float, float],
                 customer_loc: Tuple[float, float]) -> Order:
    """Create a test order with specified locations."""
    order = Order(
        order_id=order_id,
        restaurant_id=restaurant_id,
        restaurant_location=restaurant_loc,
        diner_location=customer_loc,
        placement_time=0,
        meal_prep_time=0,  # Ready immediately for testing
        expiration_time=10000
    )
    # Force state to READY for testing
    order.state = 'READY'
    return order


def create_courier(courier_id: int, location: Tuple[float, float]) -> Courier:
    """Create a test courier at specified location."""
    courier = Courier(
        courier_id=courier_id,
        start_location=location
    )
    courier.state = 'IDLE'
    return courier


def create_state(orders: List[Order]) -> SimulationState:
    """Create minimal simulation state for testing."""
    # Create restaurants based on unique restaurant IDs in orders
    unique_restaurant_ids = set(o.restaurant_id for o in orders)
    restaurants = []
    for rid in unique_restaurant_ids:
        # Find the restaurant location from the first order at this restaurant
        loc = next(o.restaurant_location for o in orders if o.restaurant_id == rid)
        restaurants.append(Restaurant(
            restaurant_id=rid,
            location=loc
        ))

    # Create state with minimal required components
    state = SimulationState(
        restaurants=restaurants,
        couriers=[],
        order_schedule=orders,
        duration=10000
    )

    # Set config for route calculation
    state.config = {
        'courier_speed_kmh': 30,
        'pickup_service_time_s': 90,
        'dropoff_service_time_s': 45,
        'map_size_m': 5000
    }

    return state


def assert_assignment_equals(actual: List[Tuple[int, List[int]]],
                             expected: List[Tuple[int, List[int]]],
                             test_name: str):
    """Assert assignment matches expected, with detailed error reporting."""
    # Sort both for comparison
    actual_sorted = sorted(actual, key=lambda x: (x[0], tuple(x[1])))
    expected_sorted = sorted(expected, key=lambda x: (x[0], tuple(x[1])))

    print(f"  Expected: {expected_sorted}")
    print(f"  Actual:   {actual_sorted}")

    if actual_sorted == expected_sorted:
        print(f"✓ PASS: {test_name}")
        return True
    else:
        print(f"✗ FAIL: {test_name}")
        return False


# ============================================================================
# TEST 1: THE "VALUABLE BUT EXPENSIVE BUNDLE" TEST
# ============================================================================

def test_valuable_expensive_bundle():
    """
    Scenario: One 3-order bundle (high raw cost, low cost-per-order) vs
              one single order (low raw cost, high cost-per-order).
              Only one courier available.

    Expected: Algorithm chooses the 3-order bundle.
    """
    # Restaurant 0: Three tightly clustered orders
    orders = [
        create_order(0, restaurant_id=0, restaurant_loc=(2.0, 2.0), customer_loc=(1.0, 1.0)),
        create_order(1, restaurant_id=0, restaurant_loc=(2.0, 2.0), customer_loc=(1.1, 1.0)),
        create_order(2, restaurant_id=0, restaurant_loc=(2.0, 2.0), customer_loc=(1.2, 1.0)),
    ]

    # Restaurant 1: One distant single order (quick trip)
    orders.append(
        create_order(3, restaurant_id=1, restaurant_loc=(5.0, 5.0), customer_loc=(4.9, 4.9))
    )

    # One courier equidistant from both restaurants
    couriers = [create_courier(0, location=(2.5, 2.5))]

    state = create_state(orders)

    result = assign_simple_bundling(state, couriers, orders)

    # Expected: The 3-order bundle is chosen (more efficient despite higher raw cost)
    expected = [(0, [0, 1, 2])]

    return assert_assignment_equals(result, expected,
        "Test 1: Valuable but Expensive Bundle")


# ============================================================================
# TEST 2: THE "TWO COMPETING BUNDLES" TEST
# ============================================================================

def test_two_competing_bundles():
    """
    Scenario: Four orders at one restaurant. Two possible pairs:
              - Pair A [0,1]: Highly efficient (close destinations)
              - Pair B [2,3]: Less efficient (farther destinations)
              Only one courier available.

    Expected: Algorithm chooses Pair A.
    """
    # Restaurant 0: Four orders forming two potential pairs
    orders = [
        # Pair A: Very close destinations (highly efficient)
        create_order(0, restaurant_id=0, restaurant_loc=(2.0, 2.0), customer_loc=(1.0, 1.0)),
        create_order(1, restaurant_id=0, restaurant_loc=(2.0, 2.0), customer_loc=(1.1, 1.0)),

        # Pair B: Farther destinations (less efficient)
        create_order(2, restaurant_id=0, restaurant_loc=(2.0, 2.0), customer_loc=(4.0, 4.0)),
        create_order(3, restaurant_id=0, restaurant_loc=(2.0, 2.0), customer_loc=(4.1, 4.0)),
    ]

    # One courier
    couriers = [create_courier(0, location=(2.5, 2.5))]

    state = create_state(orders)

    result = assign_simple_bundling(state, couriers, orders)

    # Expected: Pair A is chosen (most efficient)
    # Note: Orders 2 and 3 are left unassigned
    expected = [(0, [0, 1])]

    return assert_assignment_equals(result, expected,
        "Test 2: Two Competing Bundles")


# ============================================================================
# TEST 3: THE "SUBOPTIMAL TRIPLE VS OPTIMAL PAIR" TEST
# ============================================================================

def test_suboptimal_triple_vs_optimal_pair():
    """
    Scenario: Three orders at one restaurant. A 3-order bundle is possible but
              geographically awkward (one outlier). A 2-order bundle is highly
              efficient. Only one courier available.

    Expected: Algorithm chooses the 2-order bundle.
    """
    # Restaurant 0: Three orders with one major outlier
    orders = [
        # Perfect pair: Very close destinations
        create_order(0, restaurant_id=0, restaurant_loc=(2.0, 2.0), customer_loc=(1.0, 1.0)),
        create_order(1, restaurant_id=0, restaurant_loc=(2.0, 2.0), customer_loc=(1.1, 1.0)),

        # Major outlier: Ruins the triple's efficiency
        create_order(2, restaurant_id=0, restaurant_loc=(2.0, 2.0), customer_loc=(5.0, 5.0)),
    ]

    # One courier
    couriers = [create_courier(0, location=(2.5, 2.5))]

    state = create_state(orders)

    result = assign_simple_bundling(state, couriers, orders)

    # Expected: The efficient pair [0,1] is chosen over the inefficient triple [0,1,2]
    # Note: Order 2 is left unassigned
    expected = [(0, [0, 1])]

    return assert_assignment_equals(result, expected,
        "Test 3: Suboptimal Triple vs Optimal Pair")


# ============================================================================
# TEST 4: THE "OPTIMAL MIXED ASSIGNMENT" TEST
# ============================================================================

def test_optimal_mixed_assignment():
    """
    Scenario: Four orders at one restaurant with clear efficient grouping.
              Two couriers available.
              Orders 0,1 form a perfect pair. Orders 2,3 form another pair.

    Expected: Two 2-order bundles.
    """
    # Restaurant 0: Four orders that naturally form two pairs
    orders = [
        # Pair 1: Very efficient (close destinations)
        create_order(0, restaurant_id=0, restaurant_loc=(2.0, 2.0), customer_loc=(1.0, 1.0)),
        create_order(1, restaurant_id=0, restaurant_loc=(2.0, 2.0), customer_loc=(1.1, 1.0)),

        # Pair 2: Also efficient (different area, but close to each other)
        create_order(2, restaurant_id=0, restaurant_loc=(2.0, 2.0), customer_loc=(3.0, 3.0)),
        create_order(3, restaurant_id=0, restaurant_loc=(2.0, 2.0), customer_loc=(3.1, 3.0)),
    ]

    # Two couriers
    couriers = [
        create_courier(0, location=(2.5, 2.5)),
        create_courier(1, location=(2.5, 2.5))
    ]

    state = create_state(orders)

    result = assign_simple_bundling(state, couriers, orders)

    print(f"  Result: {result}")

    # Expected: Two assignments, each with 2 orders (non-overlapping pairs)
    if len(result) != 2:
        print(f"✗ FAIL: Test 4: Optimal Mixed Assignment")
        print(f"  Expected 2 assignments, got {len(result)}: {result}")
        return False

    bundle_sizes = sorted([len(order_ids) for _, order_ids in result])
    expected_sizes = [2, 2]

    if bundle_sizes != expected_sizes:
        print(f"✗ FAIL: Test 4: Optimal Mixed Assignment")
        print(f"  Expected bundle sizes [2, 2], got {bundle_sizes}")
        return False

    # Verify all orders are assigned
    assigned_orders = set()
    for _, order_ids in result:
        assigned_orders.update(order_ids)

    if assigned_orders != {0, 1, 2, 3}:
        print(f"✗ FAIL: Test 4: Optimal Mixed Assignment")
        print(f"  Not all orders assigned. Got: {assigned_orders}")
        return False

    # Verify no overlaps
    order_lists = [order_ids for _, order_ids in result]
    all_orders = []
    for ol in order_lists:
        all_orders.extend(ol)

    if len(all_orders) != len(set(all_orders)):
        print(f"✗ FAIL: Test 4: Optimal Mixed Assignment")
        print(f"  Found overlapping assignments")
        return False

    print(f"✓ PASS: Test 4: Optimal Mixed Assignment")
    return True


# ============================================================================
# TEST RUNNER
# ============================================================================

def run_all_tests():
    """Run the complete advanced test suite and report results."""
    print("=" * 80)
    print("ADVANCED TEST SUITE: Simple Bundling (Final Corrected Version)")
    print("=" * 80)
    print()

    tests = [
        ("Valuable but Expensive Bundle", test_valuable_expensive_bundle),
        ("Two Competing Bundles", test_two_competing_bundles),
        ("Suboptimal Triple vs Optimal Pair", test_suboptimal_triple_vs_optimal_pair),
        ("Optimal Mixed Assignment", test_optimal_mixed_assignment),
    ]

    results = []
    for name, test_func in tests:
        print(f"Running: {name}...")
        try:
            passed = test_func()
            results.append((name, passed))
        except Exception as e:
            print(f"✗ FAIL: {name}")
            print(f"  Exception: {e}")
            import traceback
            traceback.print_exc()
            results.append((name, False))
        print()

    # Summary
    print("=" * 80)
    print("TEST SUMMARY")
    print("=" * 80)

    passed = sum(1 for _, p in results if p)
    total = len(results)

    for name, p in results:
        status = "✓ PASS" if p else "✗ FAIL"
        print(f"{status}: {name}")

    print()
    print(f"Results: {passed}/{total} tests passed")

    return passed == total


if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)
