"""
Constrained Bundling Algorithm Tests

Tests the assign_constrained_bundling function from assignment_algorithms.py
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from simulator_core import (
    SimulationState, Courier, Order, Restaurant,
    euclidean_distance, get_travel_time
)
from assignment_algorithms import assign_constrained_bundling, calculate_route_duration

MAX_ORDER_DURATION = 2400.0  # 40 minutes


# ============================================================================
# CATEGORY 1: CONSTRAINT VIOLATION TESTS
# ============================================================================

def test_1_1_unacceptable_single():
    """Test 1.1: The Unacceptable Single"""
    print("\n" + "="*80)
    print("TEST 1.1: The Unacceptable Single")
    print("="*80)

    restaurants = [Restaurant(0, (15.0, 15.0))]

    couriers = [Courier(0, (0.0, 0.0))]

    orders = [Order(0, 0, (15.0, 15.0), (16.0, 16.0), 0.0)]
    orders[0].state = "READY"
    orders[0].ready_time = 0.0

    state = SimulationState(restaurants, couriers, orders, duration=3600)

    print(f"\nCourier 0 at (0.0, 0.0)")
    print(f"Order 0 at restaurant (15.0, 15.0)")

    duration = calculate_route_duration(couriers[0].current_location, [orders[0].id], state, False, True)
    print(f"\nRoute duration: {duration:.1f}s")
    print(f"MAX_ORDER_DURATION: {MAX_ORDER_DURATION:.1f}s")

    result = assign_constrained_bundling(state, couriers, orders, MAX_ORDER_DURATION)

    print(f"\nOutput: {result}")

    assert result == []

    print("PASS")


def test_1_2_just_barely_unacceptable_bundle():
    """Test 1.2: The Just Barely Unacceptable Bundle"""
    print("\n" + "="*80)
    print("TEST 1.2: The Just Barely Unacceptable Bundle")
    print("="*80)

    restaurants = [Restaurant(0, (5.1, 5.1))]

    couriers = [Courier(0, (5.0, 5.0))]

    orders = [
        Order(0, 0, (5.1, 5.1), (5.5, 5.5), 0.0),
        Order(1, 0, (5.1, 5.1), (15.0, 15.0), 0.0)
    ]

    for order in orders:
        order.state = "READY"
        order.ready_time = 0.0

    state = SimulationState(restaurants, couriers, orders, duration=3600)

    print(f"\nCourier 0 at (5.0, 5.0)")
    print(f"Orders 0, 1 at restaurant (5.1, 5.1)")

    single_duration_0 = calculate_route_duration(couriers[0].current_location, [orders[0].id], state, False, True)
    single_duration_1 = calculate_route_duration(couriers[0].current_location, [orders[1].id], state, False, True)
    bundle_duration = calculate_route_duration(couriers[0].current_location, [orders[0].id, orders[1].id], state, True, True)

    print(f"\nSingle O0: {single_duration_0:.1f}s")
    print(f"Single O1: {single_duration_1:.1f}s")
    print(f"Bundle [O0, O1]: {bundle_duration:.1f}s")
    print(f"MAX_ORDER_DURATION: {MAX_ORDER_DURATION:.1f}s")

    result = assign_constrained_bundling(state, couriers, orders, MAX_ORDER_DURATION)

    print(f"\nOutput: {result}")

    assert len(result) == 1
    assert len(result[0][1]) == 1

    print(f"Assigned single order only: {result[0][1]}")
    print("PASS")


def test_1_3_acceptable_vs_unacceptable_bundle():
    """Test 1.3: The Acceptable Bundle vs. Unacceptable Bundle"""
    print("\n" + "="*80)
    print("TEST 1.3: The Acceptable Bundle vs. Unacceptable Bundle")
    print("="*80)

    restaurants = [Restaurant(0, (5.1, 5.1))]

    couriers = [Courier(0, (5.0, 5.0))]

    orders = [
        Order(0, 0, (5.1, 5.1), (5.2, 5.2), 0.0),
        Order(1, 0, (5.1, 5.1), (5.3, 5.3), 0.0),
        Order(2, 0, (5.1, 5.1), (15.0, 15.0), 0.0)
    ]

    for order in orders:
        order.state = "READY"
        order.ready_time = 0.0

    state = SimulationState(restaurants, couriers, orders, duration=3600)

    print(f"\nCourier 0 at (5.0, 5.0)")
    print(f"Orders 0, 1, 2 at restaurant (5.1, 5.1)")

    bundle_01 = calculate_route_duration(couriers[0].current_location, [orders[0].id, orders[1].id], state, True, True)
    bundle_02 = calculate_route_duration(couriers[0].current_location, [orders[0].id, orders[2].id], state, True, True)

    print(f"\nBundle [O0, O1]: {bundle_01:.1f}s")
    print(f"Bundle [O0, O2]: {bundle_02:.1f}s")
    print(f"MAX_ORDER_DURATION: {MAX_ORDER_DURATION:.1f}s")

    result = assign_constrained_bundling(state, couriers, orders, MAX_ORDER_DURATION)

    print(f"\nOutput: {result}")

    assert len(result) == 1
    assigned_orders = sorted(result[0][1])

    print(f"Assigned orders: {assigned_orders}")

    assert len(assigned_orders) == 2
    assert 0 in assigned_orders
    assert 1 in assigned_orders

    print("PASS")


# ============================================================================
# CATEGORY 2: OPTIMIZATION UNDER CONSTRAINTS TESTS
# ============================================================================

def test_2_1_only_valid_high_throughput_option():
    """Test 2.1: Choosing the Only Valid High-Throughput Option"""
    print("\n" + "="*80)
    print("TEST 2.1: Choosing the Only Valid High-Throughput Option")
    print("="*80)

    restaurants = [Restaurant(0, (1.0, 1.0))]

    couriers = [Courier(0, (0.0, 0.0))]

    orders = [
        Order(0, 0, (1.0, 1.0), (1.1, 1.1), 0.0),
        Order(1, 0, (1.0, 1.0), (1.2, 1.2), 0.0),
        Order(2, 0, (1.0, 1.0), (10.0, 10.0), 0.0)
    ]

    for order in orders:
        order.state = "READY"
        order.ready_time = 0.0

    state = SimulationState(restaurants, couriers, orders, duration=3600)

    print(f"\nCourier 0 at (0.0, 0.0)")
    print(f"Orders 0, 1, 2 at restaurant (1.0, 1.0)")

    bundle_01 = calculate_route_duration(couriers[0].current_location, [orders[0].id, orders[1].id], state, True, True)
    bundle_02 = calculate_route_duration(couriers[0].current_location, [orders[0].id, orders[2].id], state, True, True)
    bundle_12 = calculate_route_duration(couriers[0].current_location, [orders[1].id, orders[2].id], state, True, True)

    print(f"\nBundle [O0, O1]: {bundle_01:.1f}s")
    print(f"Bundle [O0, O2]: {bundle_02:.1f}s")
    print(f"Bundle [O1, O2]: {bundle_12:.1f}s")
    print(f"MAX_ORDER_DURATION: {MAX_ORDER_DURATION:.1f}s")

    result = assign_constrained_bundling(state, couriers, orders, MAX_ORDER_DURATION)

    print(f"\nOutput: {result}")

    assert len(result) == 1
    assigned_orders = sorted(result[0][1])

    print(f"Assigned orders: {assigned_orders}")

    assert len(assigned_orders) == 2

    print("PASS")


def test_2_2_cheaper_of_two_valid_bundles():
    """Test 2.2: Choosing the Cheaper of Two Valid Bundles"""
    print("\n" + "="*80)
    print("TEST 2.2: Choosing the Cheaper of Two Valid Bundles")
    print("="*80)

    restaurants = [Restaurant(0, (1.0, 1.0))]

    couriers = [Courier(0, (0.0, 0.0))]

    orders = [
        Order(0, 0, (1.0, 1.0), (1.1, 1.1), 0.0),
        Order(1, 0, (1.0, 1.0), (1.2, 1.2), 0.0),
        Order(2, 0, (1.0, 1.0), (8.0, 8.0), 0.0),
        Order(3, 0, (1.0, 1.0), (8.1, 8.1), 0.0)
    ]

    for order in orders:
        order.state = "READY"
        order.ready_time = 0.0

    state = SimulationState(restaurants, couriers, orders, duration=3600)

    print(f"\nCourier 0 at (0.0, 0.0)")
    print(f"Orders 0, 1, 2, 3 at restaurant (1.0, 1.0)")

    bundle_01 = calculate_route_duration(couriers[0].current_location, [orders[0].id, orders[1].id], state, True, True)
    bundle_23 = calculate_route_duration(couriers[0].current_location, [orders[2].id, orders[3].id], state, True, True)

    print(f"\nBundle [O0, O1]: {bundle_01:.1f}s (cheaper)")
    print(f"Bundle [O2, O3]: {bundle_23:.1f}s (more expensive)")
    print(f"MAX_ORDER_DURATION: {MAX_ORDER_DURATION:.1f}s")

    result = assign_constrained_bundling(state, couriers, orders, MAX_ORDER_DURATION)

    print(f"\nOutput: {result}")

    assert len(result) == 1
    assigned_orders = sorted(result[0][1])

    print(f"Assigned orders: {assigned_orders}")

    assert len(assigned_orders) == 2
    assert assigned_orders == [0, 1]

    print("PASS")


# ============================================================================
# CATEGORY 3: SYSTEM-LEVEL BEHAVIOR TESTS
# ============================================================================

def test_3_1_constraint_starvation():
    """Test 3.1: Constraint Starvation Scenario"""
    print("\n" + "="*80)
    print("TEST 3.1: Constraint Starvation Scenario")
    print("="*80)

    restaurants = [Restaurant(0, (20.0, 20.0))]

    couriers = [
        Courier(0, (0.0, 0.0)),
        Courier(1, (1.0, 1.0))
    ]

    orders = [
        Order(0, 0, (20.0, 20.0), (21.0, 21.0), 0.0),
        Order(1, 0, (20.0, 20.0), (22.0, 22.0), 0.0)
    ]

    for order in orders:
        order.state = "READY"
        order.ready_time = 0.0

    state = SimulationState(restaurants, couriers, orders, duration=10000)

    print(f"\nCouriers at (0.0, 0.0) and (1.0, 1.0)")
    print(f"Orders at distant restaurant (20.0, 20.0)")

    for i, c in enumerate(couriers):
        for j, o in enumerate(orders):
            duration = calculate_route_duration(c.current_location, [o.id], state, False, True)
            print(f"  C{i} → O{j}: {duration:.1f}s")

    print(f"\nMAX_ORDER_DURATION: {MAX_ORDER_DURATION:.1f}s")

    result = assign_constrained_bundling(state, couriers, orders, MAX_ORDER_DURATION)

    print(f"\nOutput: {result}")

    assert result == []

    print("Constraint starvation: no valid assignments")
    print("PASS")


def test_3_2_constraint_forces_suboptimal_courier():
    """Test 3.2: Constraint Forces a Suboptimal Courier"""
    print("\n" + "="*80)
    print("TEST 3.2: Constraint Forces a Suboptimal Courier")
    print("="*80)

    restaurants = [Restaurant(0, (5.0, 5.0))]

    couriers = [
        Courier(0, (0.0, 0.0)),
        Courier(1, (4.9, 4.9))
    ]

    orders = [Order(0, 0, (5.0, 5.0), (10.0, 10.0), 0.0)]
    orders[0].state = "READY"
    orders[0].ready_time = 0.0

    state = SimulationState(restaurants, couriers, orders, duration=3600)

    print(f"\nCourier 0 at (0.0, 0.0) - far from restaurant")
    print(f"Courier 1 at (4.9, 4.9) - very close to restaurant")
    print(f"Order 0 at restaurant (5.0, 5.0), dropoff at (10.0, 10.0)")

    duration_c0 = calculate_route_duration(couriers[0].current_location, [orders[0].id], state, False, True)
    duration_c1 = calculate_route_duration(couriers[1].current_location, [orders[0].id], state, False, True)

    print(f"\nC0 total duration: {duration_c0:.1f}s")
    print(f"C1 total duration: {duration_c1:.1f}s")
    print(f"MAX_ORDER_DURATION: {MAX_ORDER_DURATION:.1f}s")

    result = assign_constrained_bundling(state, couriers, orders, MAX_ORDER_DURATION)

    print(f"\nOutput: {result}")

    assert len(result) == 1

    assigned_courier = result[0][0]
    print(f"Assigned courier: {assigned_courier}")

    if duration_c0 > MAX_ORDER_DURATION and duration_c1 <= MAX_ORDER_DURATION:
        assert assigned_courier == 1
        print("Constraint forced suboptimal courier (farther but within time limit)")
    else:
        print(f"Both durations within limit, optimal choice made")

    print("PASS")


# ============================================================================
# CATEGORY 4: ADDITIONAL EDGE CASES
# ============================================================================

def test_4_1_no_couriers_no_orders():
    """Test 4.1: No Couriers, No Orders"""
    print("\n" + "="*80)
    print("TEST 4.1: No Couriers, No Orders")
    print("="*80)

    restaurants = [Restaurant(0, (2.0, 2.0))]

    couriers = []
    orders = []

    state = SimulationState(restaurants, couriers, orders, duration=3600)

    print(f"\nInput: 0 couriers, 0 orders")

    result = assign_constrained_bundling(state, couriers, orders, MAX_ORDER_DURATION)

    print(f"Output: {result}")

    assert result == []

    print("PASS")


def test_4_2_single_courier_single_order_under_limit():
    """Test 4.2: Single Courier, Single Order Under Limit"""
    print("\n" + "="*80)
    print("TEST 4.2: Single Courier, Single Order Under Limit")
    print("="*80)

    restaurants = [Restaurant(0, (2.0, 2.0))]

    couriers = [Courier(0, (1.0, 1.0))]

    orders = [Order(0, 0, (2.0, 2.0), (3.0, 3.0), 0.0)]
    orders[0].state = "READY"
    orders[0].ready_time = 0.0

    state = SimulationState(restaurants, couriers, orders, duration=3600)

    print(f"\nInput: 1 courier, 1 order")

    duration = calculate_route_duration(couriers[0].current_location, [orders[0].id], state, False, True)
    print(f"Duration: {duration:.1f}s")
    print(f"MAX_ORDER_DURATION: {MAX_ORDER_DURATION:.1f}s")

    result = assign_constrained_bundling(state, couriers, orders, MAX_ORDER_DURATION)

    print(f"\nOutput: {result}")

    assert len(result) == 1
    assert result[0] == (0, [0])

    print("PASS")


def test_4_3_exactly_at_threshold():
    """Test 4.3: Exactly at MAX_ORDER_DURATION Threshold"""
    print("\n" + "="*80)
    print("TEST 4.3: Exactly at MAX_ORDER_DURATION Threshold")
    print("="*80)

    restaurants = [Restaurant(0, (5.0, 5.0))]

    couriers = [Courier(0, (4.0, 4.0))]

    orders = [Order(0, 0, (5.0, 5.0), (6.0, 6.0), 0.0)]
    orders[0].state = "READY"
    orders[0].ready_time = 0.0

    state = SimulationState(restaurants, couriers, orders, duration=3600)

    print(f"\nCourier 0 at (4.0, 4.0)")
    print(f"Order 0 at restaurant (5.0, 5.0)")

    duration = calculate_route_duration(couriers[0].current_location, [orders[0].id], state, False, True)
    print(f"\nDuration: {duration:.1f}s")

    threshold = duration
    print(f"Setting MAX_ORDER_DURATION to exact duration: {threshold:.1f}s")

    result = assign_constrained_bundling(state, couriers, orders, threshold)

    print(f"\nOutput: {result}")

    assert len(result) == 1
    assert result[0] == (0, [0])

    print("Assignment made at exact threshold (inclusive)")
    print("PASS")


# ============================================================================
# TEST RUNNER
# ============================================================================

def run_all_tests():
    """Run all test cases"""

    tests = [
        test_1_1_unacceptable_single,
        test_1_2_just_barely_unacceptable_bundle,
        test_1_3_acceptable_vs_unacceptable_bundle,
        test_2_1_only_valid_high_throughput_option,
        test_2_2_cheaper_of_two_valid_bundles,
        test_3_1_constraint_starvation,
        test_3_2_constraint_forces_suboptimal_courier,
        test_4_1_no_couriers_no_orders,
        test_4_2_single_courier_single_order_under_limit,
        test_4_3_exactly_at_threshold
    ]

    passed = 0
    failed = 0
    errors = []

    print("="*80)
    print("CONSTRAINED BUNDLING ALGORITHM TEST SUITE")
    print("="*80)
    print(f"Total tests: {len(tests)}")
    print(f"MAX_ORDER_DURATION: {MAX_ORDER_DURATION:.1f}s (40 minutes)")

    for test in tests:
        try:
            test()
            passed += 1
        except AssertionError as e:
            print(f"\nFAIL: {test.__name__}")
            print(f"Error: {e}")
            errors.append((test.__name__, str(e)))
            failed += 1
        except Exception as e:
            print(f"\nERROR: {test.__name__}")
            print(f"Exception: {e}")
            errors.append((test.__name__, f"EXCEPTION: {e}"))
            failed += 1

    print("\n" + "="*80)
    print("TEST SUMMARY")
    print("="*80)
    print(f"Total: {len(tests)}")
    print(f"Passed: {passed}")
    print(f"Failed: {failed}")

    if errors:
        print("\nFailed tests:")
        for test_name, error in errors:
            print(f"  {test_name}: {error}")

    return failed == 0


if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)
