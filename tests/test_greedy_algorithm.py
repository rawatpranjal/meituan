"""
Greedy Algorithm Tests

Tests the assign_greedy function from assignment_algorithms.py
"""

import sys
import os
import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from simulator_core import (
    SimulationState, Courier, Order, Restaurant,
    euclidean_distance, get_travel_time
)
from assignment_algorithms import assign_greedy


# ============================================================================
# CATEGORY 1: BASIC FUNCTIONALITY TESTS
# ============================================================================

def test_1_1_simple_one_to_one():
    """Test 1.1: Simple One-to-One Assignment"""
    print("\n" + "="*80)
    print("TEST 1.1: Simple One-to-One Assignment")
    print("="*80)

    restaurants = [Restaurant(0, (2.0, 2.0))]
    couriers = [Courier(0, (1.0, 1.0))]
    orders = [Order(0, 0, (2.0, 2.0), (3.0, 3.0), 0.0)]
    orders[0].state = "READY"
    orders[0].ready_time = 0.0

    state = SimulationState(restaurants, couriers, orders, duration=3600)

    result = assign_greedy(state, couriers, orders)

    print(f"Input: 1 courier, 1 order")
    print(f"Output: {result}")

    assert len(result) == 1
    assert result[0] == (0, [0])

    print("PASS")


def test_1_2_nearest_courier_selection():
    """Test 1.2: Nearest Courier Selection"""
    print("\n" + "="*80)
    print("TEST 1.2: Nearest Courier Selection")
    print("="*80)

    restaurants = [Restaurant(0, (2.0, 2.0))]

    couriers = [
        Courier(0, (5.0, 5.0)),
        Courier(1, (2.1, 2.1)),
        Courier(2, (3.0, 3.0))
    ]

    orders = [Order(0, 0, (2.0, 2.0), (1.0, 1.0), 0.0)]
    orders[0].state = "READY"
    orders[0].ready_time = 0.0

    state = SimulationState(restaurants, couriers, orders, duration=3600)

    print("Courier distances to restaurant (2.0, 2.0):")
    for c in couriers:
        dist = euclidean_distance(c.current_location, restaurants[0].location)
        tt = get_travel_time(c.current_location, restaurants[0].location)
        print(f"  Courier {c.id}: {dist:.3f} km, {tt:.1f}s")

    result = assign_greedy(state, couriers, orders)

    print(f"Output: {result}")

    assert len(result) == 1
    assert result[0][0] == 1

    print("PASS")


def test_1_3_more_orders_than_couriers():
    """Test 1.3: More Orders Than Couriers"""
    print("\n" + "="*80)
    print("TEST 1.3: More Orders Than Couriers")
    print("="*80)

    restaurants = [Restaurant(0, (2.0, 2.0))]

    couriers = [
        Courier(0, (2.0, 2.0)),
        Courier(1, (2.0, 2.0)),
        Courier(2, (2.0, 2.0))
    ]

    orders = [
        Order(0, 0, (2.0, 2.0), (1.0, 1.0), 0.0),
        Order(1, 0, (2.0, 2.0), (1.5, 1.5), 0.0),
        Order(2, 0, (2.0, 2.0), (2.0, 2.0), 0.0),
        Order(3, 0, (2.0, 2.0), (2.5, 2.5), 0.0),
        Order(4, 0, (2.0, 2.0), (3.0, 3.0), 0.0)
    ]

    for i, order in enumerate(orders):
        order.state = "READY"
        order.ready_time = float(i * 60)

    state = SimulationState(restaurants, couriers, orders, duration=3600)

    print(f"Input: {len(couriers)} couriers, {len(orders)} orders")
    print("Order ready_times:")
    for o in orders:
        print(f"  Order {o.id}: ready_time={o.ready_time}")

    result = assign_greedy(state, couriers, orders)

    print(f"Output: {result}")
    print(f"Assignments made: {len(result)}")

    assigned_orders = sorted([o_ids[0] for _, o_ids in result])
    print(f"Orders assigned: {assigned_orders}")

    assert len(result) == 3
    assert assigned_orders == [0, 1, 2]

    print("PASS")


# ============================================================================
# CATEGORY 2: LOGICAL CORRECTNESS TESTS
# ============================================================================

def test_2_1_prioritization_by_ready_time():
    """Test 2.1: Prioritization by ready_time"""
    print("\n" + "="*80)
    print("TEST 2.1: Prioritization by ready_time")
    print("="*80)

    restaurants = [
        Restaurant(0, (1.0, 1.0)),
        Restaurant(1, (5.0, 5.0))
    ]

    couriers = [
        Courier(0, (1.1, 1.1)),
        Courier(1, (4.9, 4.9))
    ]

    orders = [
        Order(0, 0, (1.0, 1.0), (0.5, 0.5), 0.0),
        Order(1, 1, (5.0, 5.0), (5.5, 5.5), 0.0)
    ]

    orders[0].ready_time = 300.0
    orders[1].ready_time = 299.0
    orders[0].state = "READY"
    orders[1].state = "READY"

    state = SimulationState(restaurants, couriers, orders, duration=3600)

    print("Setup:")
    print(f"  Order 0: restaurant (1.0, 1.0), ready_time=300")
    print(f"  Order 1: restaurant (5.0, 5.0), ready_time=299")
    print(f"  Courier 0: location (1.1, 1.1) - close to Order 0")
    print(f"  Courier 1: location (4.9, 4.9) - close to Order 1")

    print("\nTravel times:")
    for o in orders:
        for c in couriers:
            tt = get_travel_time(c.current_location, o.restaurant_location)
            print(f"  Courier {c.id} to Order {o.id}: {tt:.1f}s")

    result = assign_greedy(state, couriers, orders)

    print(f"\nOutput: {result}")

    assignments_dict = {o_ids[0]: c_id for c_id, o_ids in result}
    print(f"Order 1 (ready first) assigned to: Courier {assignments_dict[1]}")
    print(f"Order 0 (ready second) assigned to: Courier {assignments_dict[0]}")

    assert len(result) == 2
    assert assignments_dict[1] == 1
    assert assignments_dict[0] == 0

    print("PASS")


def test_2_2_courier_uniqueness():
    """Test 2.2: Courier Uniqueness Guarantee"""
    print("\n" + "="*80)
    print("TEST 2.2: Courier Uniqueness Guarantee")
    print("="*80)

    restaurants = [Restaurant(0, (2.0, 2.0))]

    couriers = [Courier(0, (2.0, 2.0))]

    orders = [
        Order(0, 0, (2.0, 2.0), (1.0, 1.0), 0.0),
        Order(1, 0, (2.0, 2.0), (1.5, 1.5), 0.0)
    ]

    orders[0].ready_time = 100.0
    orders[1].ready_time = 200.0
    orders[0].state = "READY"
    orders[1].state = "READY"

    state = SimulationState(restaurants, couriers, orders, duration=3600)

    print(f"Input: 1 courier, 2 orders")
    print(f"  Order 0: ready_time={orders[0].ready_time}")
    print(f"  Order 1: ready_time={orders[1].ready_time}")

    result = assign_greedy(state, couriers, orders)

    print(f"Output: {result}")
    print(f"Assignments made: {len(result)}")

    if result:
        assigned_orders = [o_ids[0] for _, o_ids in result]
        print(f"Orders assigned: {assigned_orders}")

    assert len(result) == 1
    assert result[0][0] == 0
    assert result[0][1][0] == 0

    print("PASS")


# ============================================================================
# CATEGORY 3: EDGE CASE SCENARIOS
# ============================================================================

def test_3_1_no_idle_couriers():
    """Test 3.1: No Idle Couriers Available"""
    print("\n" + "="*80)
    print("TEST 3.1: No Idle Couriers Available")
    print("="*80)

    restaurants = [Restaurant(0, (2.0, 2.0))]

    couriers = []

    orders = [
        Order(0, 0, (2.0, 2.0), (1.0, 1.0), 0.0),
        Order(1, 0, (2.0, 2.0), (1.5, 1.5), 0.0)
    ]

    for order in orders:
        order.state = "READY"
        order.ready_time = 0.0

    state = SimulationState(restaurants, couriers, orders, duration=3600)

    print(f"Input: 0 couriers, {len(orders)} orders")

    result = assign_greedy(state, couriers, orders)

    print(f"Output: {result}")

    assert result == []

    print("PASS")


def test_3_2_no_ready_orders():
    """Test 3.2: No Ready Orders"""
    print("\n" + "="*80)
    print("TEST 3.2: No Ready Orders")
    print("="*80)

    restaurants = [Restaurant(0, (2.0, 2.0))]

    couriers = [
        Courier(0, (1.0, 1.0)),
        Courier(1, (2.0, 2.0)),
        Courier(2, (3.0, 3.0))
    ]

    orders = []

    state = SimulationState(restaurants, couriers, orders, duration=3600)

    print(f"Input: {len(couriers)} couriers, 0 orders")

    result = assign_greedy(state, couriers, orders)

    print(f"Output: {result}")

    assert result == []

    print("PASS")


def test_3_3_equal_travel_times():
    """Test 3.3: Equal Travel Times"""
    print("\n" + "="*80)
    print("TEST 3.3: Equal Travel Times")
    print("="*80)

    restaurants = [Restaurant(0, (2.0, 2.0))]

    couriers = [
        Courier(0, (2.0, 3.0)),
        Courier(1, (3.0, 2.0))
    ]

    orders = [Order(0, 0, (2.0, 2.0), (1.0, 1.0), 0.0)]
    orders[0].state = "READY"
    orders[0].ready_time = 0.0

    state = SimulationState(restaurants, couriers, orders, duration=3600)

    print("Courier distances to restaurant:")
    for c in couriers:
        dist = euclidean_distance(c.current_location, restaurants[0].location)
        tt = get_travel_time(c.current_location, restaurants[0].location)
        print(f"  Courier {c.id}: {dist:.3f} km, {tt:.1f}s")

    result = assign_greedy(state, couriers, orders)

    print(f"Output: {result}")

    assert len(result) == 1
    assert result[0][0] in [0, 1]
    assert result[0][1] == [0]

    print(f"Assigned Courier {result[0][0]} (deterministic tie-breaking)")
    print("PASS")


# ============================================================================
# CATEGORY 4: GREEDY BEHAVIOR LITMUS TEST
# ============================================================================

def test_4_1_distant_bait_scenario():
    """Test 4.1: The Distant Bait Scenario"""
    print("\n" + "="*80)
    print("TEST 4.1: The Distant Bait Scenario (Greedy Behavior)")
    print("="*80)

    restaurants = [
        Restaurant(0, (2.0, 2.0)),
        Restaurant(1, (10.0, 10.0))
    ]

    couriers = [
        Courier(0, (2.0, 2.0)),
        Courier(1, (10.0, 10.0))
    ]

    orders = [
        Order(0, 0, (2.0, 2.0), (3.0, 3.0), 0.0),
        Order(1, 1, (10.0, 10.0), (11.0, 11.0), 0.0)
    ]

    orders[0].ready_time = 300.0
    orders[1].ready_time = 299.0
    orders[0].state = "READY"
    orders[1].state = "READY"

    state = SimulationState(restaurants, couriers, orders, duration=3600)

    print("Setup:")
    print(f"  Courier 0: downtown (2.0, 2.0)")
    print(f"  Courier 1: suburban (10.0, 10.0)")
    print(f"  Order 0: downtown restaurant (2.0, 2.0), ready_time=300")
    print(f"  Order 1: suburban restaurant (10.0, 10.0), ready_time=299")

    print("\nTravel times:")
    for o in orders:
        for c in couriers:
            tt = get_travel_time(c.current_location, o.restaurant_location)
            print(f"  Courier {c.id} to Order {o.id}: {tt:.1f}s")

    result = assign_greedy(state, couriers, orders)

    print(f"\nOutput: {result}")

    assignments_dict = {o_ids[0]: c_id for c_id, o_ids in result}

    print("\nGreedy behavior verification:")
    print(f"  Order 1 (ready_time=299, processed first) → Courier {assignments_dict[1]}")
    print(f"  Order 0 (ready_time=300, processed second) → Courier {assignments_dict[0]}")

    assert len(result) == 2
    assert assignments_dict[1] == 1
    assert assignments_dict[0] == 0

    print("\nGreedy assigns based on ready_time order, not global optimality")
    print("PASS")


# ============================================================================
# TEST RUNNER
# ============================================================================

def run_all_tests():
    """Run all test cases"""

    tests = [
        test_1_1_simple_one_to_one,
        test_1_2_nearest_courier_selection,
        test_1_3_more_orders_than_couriers,
        test_2_1_prioritization_by_ready_time,
        test_2_2_courier_uniqueness,
        test_3_1_no_idle_couriers,
        test_3_2_no_ready_orders,
        test_3_3_equal_travel_times,
        test_4_1_distant_bait_scenario
    ]

    passed = 0
    failed = 0
    errors = []

    print("="*80)
    print("GREEDY ALGORITHM TEST SUITE")
    print("="*80)
    print(f"Total tests: {len(tests)}")

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
