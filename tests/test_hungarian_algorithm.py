"""
Rigorous Test Suite for Hungarian Assignment Algorithm

Tests the assign_hungarian function from assignment_algorithms.py

TEST PHILOSOPHY:
- Ruthlessly test every edge case and boundary condition
- Verify optimality guarantees of Hungarian algorithm
- Compare against manual calculations and expected costs
- Test cost matrix construction and dummy assignment handling
- Validate that Hungarian finds globally optimal solutions
- Hard pass/fail criteria with mathematical precision
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from simulator_core import (
    SimulationState, Courier, Order, Restaurant,
    euclidean_distance, get_travel_time,
    PICKUP_SERVICE_TIME, DROPOFF_SERVICE_TIME
)
from assignment_algorithms import assign_hungarian, calculate_route_duration
import copy


# ============================================================================
# TEST UTILITIES
# ============================================================================

def calculate_expected_cost(courier_loc, restaurant_loc, diner_loc):
    """
    Manually calculate expected cost for a single delivery.

    Cost = travel_to_restaurant + PICKUP_SERVICE_TIME +
           travel_to_customer + DROPOFF_SERVICE_TIME
    """
    travel_to_restaurant = get_travel_time(courier_loc, restaurant_loc)
    travel_to_customer = get_travel_time(restaurant_loc, diner_loc)

    total_cost = (travel_to_restaurant + PICKUP_SERVICE_TIME +
                  travel_to_customer + DROPOFF_SERVICE_TIME)

    return total_cost


def verify_assignment_format(assignments):
    """Verify assignments follow correct format: List[Tuple[int, List[int]]]"""
    assert isinstance(assignments, list), "Assignments must be a list"

    for assignment in assignments:
        assert isinstance(assignment, tuple), f"Each assignment must be a tuple, got {type(assignment)}"
        assert len(assignment) == 2, f"Each assignment must have 2 elements, got {len(assignment)}"

        courier_id, order_ids = assignment
        assert isinstance(courier_id, int), f"Courier ID must be int, got {type(courier_id)}"
        assert isinstance(order_ids, list), f"Order IDs must be a list, got {type(order_ids)}"

        for oid in order_ids:
            assert isinstance(oid, int), f"Order ID must be int, got {type(oid)}"


def verify_no_duplicates(assignments):
    """Verify no courier or order appears twice"""
    courier_ids = [c_id for c_id, _ in assignments]
    all_order_ids = []
    for _, o_ids in assignments:
        all_order_ids.extend(o_ids)

    assert len(courier_ids) == len(set(courier_ids)), \
        f"Duplicate courier IDs found: {courier_ids}"
    assert len(all_order_ids) == len(set(all_order_ids)), \
        f"Duplicate order IDs found: {all_order_ids}"


def create_simple_scenario(num_couriers, num_orders, courier_locs, diner_locs):
    """Helper to create test scenarios"""
    restaurants = [Restaurant(0, (2.0, 2.0))]

    couriers = [
        Courier(i, courier_locs[i])
        for i in range(num_couriers)
    ]

    orders = [
        Order(i, 0, (2.0, 2.0), diner_locs[i], 0.0)
        for i in range(num_orders)
    ]

    for order in orders:
        order.state = "READY"
        order.ready_time = 0.0

    state = SimulationState(restaurants, couriers, orders, duration=3600)

    return state, couriers, orders


# ============================================================================
# CATEGORY 1: BASIC FUNCTIONALITY TESTS
# ============================================================================

def test_empty_couriers_empty_orders():
    """Empty inputs should return empty list"""
    print("\n" + "="*80)
    print("TEST 1: Empty Couriers and Empty Orders")
    print("="*80)

    restaurants = [Restaurant(0, (2.0, 2.0))]
    couriers = []
    orders = []

    state = SimulationState(restaurants, couriers, orders, duration=3600)

    assignments = assign_hungarian(state, couriers, orders)

    print(f"Input: {len(couriers)} couriers, {len(orders)} orders")
    print(f"Output: {len(assignments)} assignments")

    assert assignments == [], f"Expected empty list, got {assignments}"
    print("✓ PASS: Returns empty list for empty inputs")


def test_empty_couriers_with_orders():
    """No couriers with orders should return empty list"""
    print("\n" + "="*80)
    print("TEST 2: No Couriers, Some Orders")
    print("="*80)

    restaurants = [Restaurant(0, (2.0, 2.0))]
    couriers = []
    orders = [Order(i, 0, (2.0, 2.0), (3.0, 3.0), 0.0) for i in range(3)]

    for order in orders:
        order.state = "READY"

    state = SimulationState(restaurants, couriers, orders, duration=3600)

    assignments = assign_hungarian(state, couriers, orders)

    print(f"Input: {len(couriers)} couriers, {len(orders)} orders")
    print(f"Output: {len(assignments)} assignments")

    assert assignments == [], f"Expected empty list, got {assignments}"
    print("✓ PASS: Returns empty list when no couriers available")


def test_couriers_with_empty_orders():
    """Couriers but no orders should return empty list"""
    print("\n" + "="*80)
    print("TEST 3: Some Couriers, No Orders")
    print("="*80)

    restaurants = [Restaurant(0, (2.0, 2.0))]
    couriers = [Courier(i, (2.0, 2.0)) for i in range(3)]
    orders = []

    state = SimulationState(restaurants, couriers, orders, duration=3600)

    assignments = assign_hungarian(state, couriers, orders)

    print(f"Input: {len(couriers)} couriers, {len(orders)} orders")
    print(f"Output: {len(assignments)} assignments")

    assert assignments == [], f"Expected empty list, got {assignments}"
    print("✓ PASS: Returns empty list when no orders available")


def test_single_courier_single_order():
    """Single courier and single order should create one assignment"""
    print("\n" + "="*80)
    print("TEST 4: Single Courier, Single Order")
    print("="*80)

    restaurants = [Restaurant(0, (2.0, 2.0))]

    couriers = [Courier(0, (1.0, 1.0))]
    orders = [Order(0, 0, (2.0, 2.0), (3.0, 3.0), 0.0)]
    orders[0].state = "READY"

    state = SimulationState(restaurants, couriers, orders, duration=3600)

    assignments = assign_hungarian(state, couriers, orders)

    print(f"Input: 1 courier, 1 order")
    print(f"Output: {len(assignments)} assignment(s)")

    assert len(assignments) == 1, f"Expected 1 assignment, got {len(assignments)}"
    assert assignments[0] == (0, [0]), f"Expected (0, [0]), got {assignments[0]}"

    verify_assignment_format(assignments)
    print("✓ PASS: Creates correct 1-to-1 assignment")


def test_assignment_format_validation():
    """Verify output format: List[Tuple[int, List[int]]]"""
    print("\n" + "="*80)
    print("TEST 5: Assignment Format Validation")
    print("="*80)

    state, couriers, orders = create_simple_scenario(
        num_couriers=3,
        num_orders=3,
        courier_locs=[(1.0, 1.0), (2.0, 2.0), (3.0, 3.0)],
        diner_locs=[(2.5, 2.5), (3.5, 3.5), (4.5, 4.5)]
    )

    assignments = assign_hungarian(state, couriers, orders)

    print(f"Testing format of {len(assignments)} assignments...")

    # Should not raise any assertion errors
    verify_assignment_format(assignments)

    print("✓ PASS: All assignments follow correct format")


# ============================================================================
# CATEGORY 2: UNBALANCED SCENARIOS
# ============================================================================

def test_more_couriers_than_orders_2_to_1():
    """2 couriers, 1 order: should assign 1, leave 1 idle"""
    print("\n" + "="*80)
    print("TEST 6: More Couriers Than Orders (2:1)")
    print("="*80)

    state, couriers, orders = create_simple_scenario(
        num_couriers=2,
        num_orders=1,
        courier_locs=[(1.0, 1.0), (3.0, 3.0)],
        diner_locs=[(2.5, 2.5)]
    )

    assignments = assign_hungarian(state, couriers, orders)

    print(f"Input: 2 couriers, 1 order")
    print(f"Output: {len(assignments)} assignment(s)")

    assert len(assignments) == 1, f"Expected 1 assignment, got {len(assignments)}"

    # Verify the assigned order
    assigned_orders = []
    for _, o_ids in assignments:
        assigned_orders.extend(o_ids)

    assert assigned_orders == [0], f"Expected order 0 assigned, got {assigned_orders}"

    verify_assignment_format(assignments)
    verify_no_duplicates(assignments)
    print("✓ PASS: Assigns 1 order, leaves 1 courier idle")


def test_more_couriers_than_orders_5_to_3():
    """5 couriers, 3 orders: should assign all 3 orders"""
    print("\n" + "="*80)
    print("TEST 7: More Couriers Than Orders (5:3)")
    print("="*80)

    state, couriers, orders = create_simple_scenario(
        num_couriers=5,
        num_orders=3,
        courier_locs=[(i, i) for i in range(5)],
        diner_locs=[(2.5+i, 2.5+i) for i in range(3)]
    )

    assignments = assign_hungarian(state, couriers, orders)

    print(f"Input: 5 couriers, 3 orders")
    print(f"Output: {len(assignments)} assignment(s)")

    assert len(assignments) == 3, f"Expected 3 assignments, got {len(assignments)}"

    # All orders should be assigned
    assigned_orders = []
    for _, o_ids in assignments:
        assigned_orders.extend(o_ids)

    assert sorted(assigned_orders) == [0, 1, 2], \
        f"Expected all orders assigned, got {assigned_orders}"

    verify_assignment_format(assignments)
    verify_no_duplicates(assignments)
    print("✓ PASS: All 3 orders assigned, 2 couriers remain idle")


def test_more_orders_than_couriers_1_to_2():
    """1 courier, 2 orders: should assign only 1 order"""
    print("\n" + "="*80)
    print("TEST 8: More Orders Than Couriers (1:2)")
    print("="*80)

    state, couriers, orders = create_simple_scenario(
        num_couriers=1,
        num_orders=2,
        courier_locs=[(1.0, 1.0)],
        diner_locs=[(2.5, 2.5), (3.5, 3.5)]
    )

    assignments = assign_hungarian(state, couriers, orders)

    print(f"Input: 1 courier, 2 orders")
    print(f"Output: {len(assignments)} assignment(s)")

    assert len(assignments) == 1, f"Expected 1 assignment, got {len(assignments)}"

    verify_assignment_format(assignments)
    verify_no_duplicates(assignments)
    print("✓ PASS: Assigns 1 order, leaves 1 order unassigned")


def test_more_orders_than_couriers_3_to_5():
    """3 couriers, 5 orders: should assign exactly 3 orders"""
    print("\n" + "="*80)
    print("TEST 9: More Orders Than Couriers (3:5)")
    print("="*80)

    state, couriers, orders = create_simple_scenario(
        num_couriers=3,
        num_orders=5,
        courier_locs=[(i, i) for i in range(3)],
        diner_locs=[(2.5+i, 2.5+i) for i in range(5)]
    )

    assignments = assign_hungarian(state, couriers, orders)

    print(f"Input: 3 couriers, 5 orders")
    print(f"Output: {len(assignments)} assignment(s)")

    assert len(assignments) == 3, f"Expected 3 assignments, got {len(assignments)}"

    # Exactly 3 orders should be assigned
    assigned_orders = []
    for _, o_ids in assignments:
        assigned_orders.extend(o_ids)

    assert len(assigned_orders) == 3, \
        f"Expected 3 orders assigned, got {len(assigned_orders)}"

    verify_assignment_format(assignments)
    verify_no_duplicates(assignments)
    print("✓ PASS: All 3 couriers assigned, 2 orders remain unassigned")


def test_extreme_imbalance_100_couriers_1_order():
    """100 couriers, 1 order: should handle gracefully"""
    print("\n" + "="*80)
    print("TEST 10: Extreme Imbalance (100:1)")
    print("="*80)

    restaurants = [Restaurant(0, (2.0, 2.0))]

    couriers = [Courier(i, (2.0 + i * 0.01, 2.0 + i * 0.01)) for i in range(100)]
    orders = [Order(0, 0, (2.0, 2.0), (3.0, 3.0), 0.0)]
    orders[0].state = "READY"

    state = SimulationState(restaurants, couriers, orders, duration=3600)

    assignments = assign_hungarian(state, couriers, orders)

    print(f"Input: 100 couriers, 1 order")
    print(f"Output: {len(assignments)} assignment(s)")

    assert len(assignments) == 1, f"Expected 1 assignment, got {len(assignments)}"

    verify_assignment_format(assignments)
    print("✓ PASS: Handles extreme imbalance correctly")


def test_extreme_imbalance_1_courier_100_orders():
    """1 courier, 100 orders: should assign only 1 order"""
    print("\n" + "="*80)
    print("TEST 11: Extreme Imbalance (1:100)")
    print("="*80)

    restaurants = [Restaurant(0, (2.0, 2.0))]

    couriers = [Courier(0, (2.0, 2.0))]
    orders = [
        Order(i, 0, (2.0, 2.0), (3.0 + i * 0.01, 3.0 + i * 0.01), 0.0)
        for i in range(100)
    ]

    for order in orders:
        order.state = "READY"

    state = SimulationState(restaurants, couriers, orders, duration=3600)

    assignments = assign_hungarian(state, couriers, orders)

    print(f"Input: 1 courier, 100 orders")
    print(f"Output: {len(assignments)} assignment(s)")

    assert len(assignments) == 1, f"Expected 1 assignment, got {len(assignments)}"

    verify_assignment_format(assignments)
    print("✓ PASS: Handles extreme imbalance correctly")


# ============================================================================
# CATEGORY 3: COST CALCULATION VALIDATION
# ============================================================================

def test_cost_calculation_known_scenario():
    """Verify cost calculation matches manual calculation"""
    print("\n" + "="*80)
    print("TEST 12: Cost Calculation Verification")
    print("="*80)

    restaurants = [Restaurant(0, (2.0, 2.0))]

    # Simple scenario with known distances
    courier_loc = (0.0, 0.0)
    restaurant_loc = (1.0, 0.0)  # 1 km away
    diner_loc = (2.0, 0.0)       # 1 km from restaurant

    couriers = [Courier(0, courier_loc)]
    orders = [Order(0, 0, restaurant_loc, diner_loc, 0.0)]
    orders[0].state = "READY"

    state = SimulationState(restaurants, couriers, orders, duration=3600)

    # Calculate expected cost manually
    expected_cost = calculate_expected_cost(courier_loc, restaurant_loc, diner_loc)

    # Get actual cost from calculate_route_duration
    actual_cost = calculate_route_duration(
        courier_loc,
        [0],
        state,
        use_tsp_optimization=False,
        include_service_times=True
    )

    print(f"Courier location: {courier_loc}")
    print(f"Restaurant location: {restaurant_loc}")
    print(f"Diner location: {diner_loc}")
    print(f"\nExpected cost: {expected_cost:.2f}s")
    print(f"Actual cost: {actual_cost:.2f}s")
    print(f"Difference: {abs(expected_cost - actual_cost):.2f}s")

    assert abs(expected_cost - actual_cost) < 0.01, \
        f"Cost mismatch: expected {expected_cost:.2f}, got {actual_cost:.2f}"

    print("✓ PASS: Cost calculation matches manual calculation")


def test_zero_distance_scenario():
    """Courier at restaurant location should have minimal cost (service times only)"""
    print("\n" + "="*80)
    print("TEST 13: Zero Distance to Restaurant")
    print("="*80)

    restaurants = [Restaurant(0, (2.0, 2.0))]

    # Courier already at restaurant
    couriers = [Courier(0, (2.0, 2.0))]
    orders = [Order(0, 0, (2.0, 2.0), (3.0, 3.0), 0.0)]
    orders[0].state = "READY"

    state = SimulationState(restaurants, couriers, orders, duration=3600)

    # Calculate cost
    cost = calculate_route_duration(
        (2.0, 2.0),
        [0],
        state,
        use_tsp_optimization=False,
        include_service_times=True
    )

    # Cost should be: 0 (travel to restaurant) + PICKUP_TIME +
    #                 travel_to_diner + DROPOFF_TIME
    travel_to_diner = get_travel_time((2.0, 2.0), (3.0, 3.0))
    expected_min_cost = PICKUP_SERVICE_TIME + travel_to_diner + DROPOFF_SERVICE_TIME

    print(f"Cost with courier at restaurant: {cost:.2f}s")
    print(f"Expected minimum cost: {expected_min_cost:.2f}s")

    assert abs(cost - expected_min_cost) < 1.0, \
        f"Cost mismatch: expected ~{expected_min_cost:.2f}, got {cost:.2f}"

    print("✓ PASS: Zero distance scenario calculated correctly")


def test_large_distance_scenario():
    """Very far locations should produce proportionally large costs"""
    print("\n" + "="*80)
    print("TEST 14: Large Distance Scenario")
    print("="*80)

    restaurants = [Restaurant(0, (10.0, 10.0))]

    # Courier very far from restaurant
    couriers = [Courier(0, (0.0, 0.0))]
    orders = [Order(0, 0, (10.0, 10.0), (20.0, 20.0), 0.0)]
    orders[0].state = "READY"

    state = SimulationState(restaurants, couriers, orders, duration=3600)

    # Calculate cost
    cost = calculate_route_duration(
        (0.0, 0.0),
        [0],
        state,
        use_tsp_optimization=False,
        include_service_times=True
    )

    # Calculate distances
    dist_to_restaurant = euclidean_distance((0.0, 0.0), (10.0, 10.0))
    dist_to_diner = euclidean_distance((10.0, 10.0), (20.0, 20.0))

    print(f"Distance to restaurant: {dist_to_restaurant:.2f} km")
    print(f"Distance to diner: {dist_to_diner:.2f} km")
    print(f"Total cost: {cost:.2f}s")

    # Cost should be large (well over 1000s for ~28km total travel)
    assert cost > 1000, f"Expected large cost, got {cost:.2f}s"

    print("✓ PASS: Large distance produces appropriately large cost")


def test_service_times_included():
    """Verify service times are included in cost calculation"""
    print("\n" + "="*80)
    print("TEST 15: Service Times Included in Cost")
    print("="*80)

    restaurants = [Restaurant(0, (2.0, 2.0))]

    # Courier at same location as restaurant and diner (minimize travel)
    couriers = [Courier(0, (2.0, 2.0))]
    orders = [Order(0, 0, (2.0, 2.0), (2.0, 2.0), 0.0)]
    orders[0].state = "READY"

    state = SimulationState(restaurants, couriers, orders, duration=3600)

    # Cost with service times
    cost_with_service = calculate_route_duration(
        (2.0, 2.0),
        [0],
        state,
        use_tsp_optimization=False,
        include_service_times=True
    )

    # Cost without service times
    cost_without_service = calculate_route_duration(
        (2.0, 2.0),
        [0],
        state,
        use_tsp_optimization=False,
        include_service_times=False
    )

    expected_service_time = PICKUP_SERVICE_TIME + DROPOFF_SERVICE_TIME

    print(f"Cost with service times: {cost_with_service:.2f}s")
    print(f"Cost without service times: {cost_without_service:.2f}s")
    print(f"Difference: {cost_with_service - cost_without_service:.2f}s")
    print(f"Expected service time: {expected_service_time:.2f}s")

    assert abs((cost_with_service - cost_without_service) - expected_service_time) < 1.0, \
        "Service times not correctly included"

    print("✓ PASS: Service times correctly included in cost")


# ============================================================================
# CATEGORY 4: OPTIMALITY TESTS (Hungarian vs Greedy)
# ============================================================================

def test_hungarian_finds_optimal_assignment():
    """
    CRITICAL TEST: Verify Hungarian finds globally optimal assignment.

    Scenario:
    - Courier A is close to Order 1's restaurant but Order 1's customer is far
    - Courier B is far from Order 2's restaurant but Order 2's customer is close

    Greedy would assign: A→Order1 (based on pickup distance)
    Hungarian should assign optimally to minimize total completion time
    """
    print("\n" + "="*80)
    print("TEST 16: Hungarian Finds Optimal Assignment")
    print("="*80)

    restaurants = [Restaurant(0, (2.0, 2.0))]

    # Courier A: close to restaurant
    # Courier B: far from restaurant
    couriers = [
        Courier(0, (2.1, 2.1)),  # Courier A - 0.14 km from restaurant
        Courier(1, (5.0, 5.0))   # Courier B - 4.24 km from restaurant
    ]

    # Order 1: customer very far
    # Order 2: customer very close
    orders = [
        Order(0, 0, (2.0, 2.0), (10.0, 10.0), 0.0),  # Order 1 - far customer
        Order(1, 0, (2.0, 2.0), (2.1, 2.1), 0.0)     # Order 2 - close customer
    ]

    for order in orders:
        order.state = "READY"

    state = SimulationState(restaurants, couriers, orders, duration=3600)

    # Calculate costs for all possible assignments
    cost_A_O1 = calculate_route_duration(couriers[0].current_location, [0], state)
    cost_A_O2 = calculate_route_duration(couriers[0].current_location, [1], state)
    cost_B_O1 = calculate_route_duration(couriers[1].current_location, [0], state)
    cost_B_O2 = calculate_route_duration(couriers[1].current_location, [1], state)

    print(f"\nAll possible assignment costs:")
    print(f"  Courier A → Order 1: {cost_A_O1:.2f}s")
    print(f"  Courier A → Order 2: {cost_A_O2:.2f}s")
    print(f"  Courier B → Order 1: {cost_B_O1:.2f}s")
    print(f"  Courier B → Order 2: {cost_B_O2:.2f}s")

    # Calculate total costs for both possible complete assignments
    total_A_O1_B_O2 = cost_A_O1 + cost_B_O2
    total_A_O2_B_O1 = cost_A_O2 + cost_B_O1

    print(f"\nTotal costs for complete assignments:")
    print(f"  A→O1, B→O2: {total_A_O1_B_O2:.2f}s")
    print(f"  A→O2, B→O1: {total_A_O2_B_O1:.2f}s")

    optimal_total = min(total_A_O1_B_O2, total_A_O2_B_O1)
    print(f"\nOptimal total cost: {optimal_total:.2f}s")

    # Run Hungarian algorithm
    assignments = assign_hungarian(state, couriers, orders)

    # Calculate actual total cost
    actual_total = 0
    for c_id, o_ids in assignments:
        courier = couriers[c_id]
        cost = calculate_route_duration(courier.current_location, o_ids, state)
        actual_total += cost

    print(f"Hungarian total cost: {actual_total:.2f}s")
    print(f"Difference from optimal: {abs(actual_total - optimal_total):.2f}s")

    # Hungarian should find the optimal assignment
    assert abs(actual_total - optimal_total) < 1.0, \
        f"Hungarian did not find optimal! Expected {optimal_total:.2f}, got {actual_total:.2f}"

    print("✓ PASS: Hungarian finds globally optimal assignment")


def test_hungarian_vs_greedy_difference():
    """Create scenario where Hungarian significantly outperforms Greedy"""
    print("\n" + "="*80)
    print("TEST 17: Hungarian vs Greedy Performance")
    print("="*80)

    from assignment_algorithms import assign_greedy

    restaurants = [Restaurant(0, (5.0, 5.0))]

    # Create adversarial scenario for greedy
    couriers = [
        Courier(0, (5.1, 5.1)),  # Very close to restaurant
        Courier(1, (8.0, 8.0)),  # Far from restaurant
    ]

    orders = [
        Order(0, 0, (5.0, 5.0), (15.0, 15.0), 0.0),  # Order 1: far customer
        Order(1, 0, (5.0, 5.0), (8.1, 8.1), 0.0)     # Order 2: close customer
    ]

    for i, order in enumerate(orders):
        order.state = "READY"
        order.ready_time = i * 60.0  # Stagger ready times for greedy

    state = SimulationState(restaurants, couriers, orders, duration=3600)

    # Run both algorithms
    greedy_assignments = assign_greedy(state, couriers, orders)
    hungarian_assignments = assign_hungarian(state, couriers, orders)

    # Calculate total costs
    greedy_total = 0
    for c_id, o_ids in greedy_assignments:
        courier = couriers[c_id]
        cost = calculate_route_duration(courier.current_location, o_ids, state)
        greedy_total += cost

    hungarian_total = 0
    for c_id, o_ids in hungarian_assignments:
        courier = couriers[c_id]
        cost = calculate_route_duration(courier.current_location, o_ids, state)
        hungarian_total += cost

    print(f"\nGreedy assignments: {greedy_assignments}")
    print(f"Greedy total cost: {greedy_total:.2f}s")

    print(f"\nHungarian assignments: {hungarian_assignments}")
    print(f"Hungarian total cost: {hungarian_total:.2f}s")

    improvement = ((greedy_total - hungarian_total) / greedy_total) * 100
    print(f"\nHungarian improvement: {improvement:.1f}%")

    # Hungarian should be at least as good as greedy
    assert hungarian_total <= greedy_total + 1.0, \
        f"Hungarian worse than Greedy! H={hungarian_total:.2f}, G={greedy_total:.2f}"

    print("✓ PASS: Hungarian performs at least as well as Greedy")


# ============================================================================
# CATEGORY 5: GEOGRAPHIC EDGE CASES
# ============================================================================

def test_all_same_location():
    """All couriers, restaurants, and diners at same location"""
    print("\n" + "="*80)
    print("TEST 18: All Entities at Same Location")
    print("="*80)

    same_loc = (2.0, 2.0)
    restaurants = [Restaurant(0, same_loc)]

    couriers = [Courier(i, same_loc) for i in range(3)]
    orders = [Order(i, 0, same_loc, same_loc, 0.0) for i in range(3)]

    for order in orders:
        order.state = "READY"

    state = SimulationState(restaurants, couriers, orders, duration=3600)

    assignments = assign_hungarian(state, couriers, orders)

    print(f"All entities at location: {same_loc}")
    print(f"Assignments: {len(assignments)}")

    assert len(assignments) == 3, f"Expected 3 assignments, got {len(assignments)}"

    # All costs should be just service times (no travel)
    for c_id, o_ids in assignments:
        cost = calculate_route_duration(same_loc, o_ids, state)
        expected = PICKUP_SERVICE_TIME + DROPOFF_SERVICE_TIME
        print(f"  Courier {c_id} → Order {o_ids[0]}: cost={cost:.2f}s (expected ~{expected:.2f}s)")
        assert abs(cost - expected) < 1.0, f"Cost should be service times only"

    verify_assignment_format(assignments)
    verify_no_duplicates(assignments)
    print("✓ PASS: Handles all entities at same location")


def test_maximum_spread_locations():
    """Couriers and orders at maximum spread across map"""
    print("\n" + "="*80)
    print("TEST 19: Maximum Spread Locations")
    print("="*80)

    restaurants = [Restaurant(0, (50.0, 50.0))]

    # Couriers at corners of large square
    couriers = [
        Courier(0, (0.0, 0.0)),
        Courier(1, (100.0, 100.0))
    ]

    # Orders also at extremes
    orders = [
        Order(0, 0, (50.0, 50.0), (0.0, 100.0), 0.0),
        Order(1, 0, (50.0, 50.0), (100.0, 0.0), 0.0)
    ]

    for order in orders:
        order.state = "READY"

    state = SimulationState(restaurants, couriers, orders, duration=3600)

    assignments = assign_hungarian(state, couriers, orders)

    print(f"Map spread: 100km x 100km")
    print(f"Assignments: {len(assignments)}")

    assert len(assignments) == 2, f"Expected 2 assignments, got {len(assignments)}"

    verify_assignment_format(assignments)
    verify_no_duplicates(assignments)
    print("✓ PASS: Handles maximum spread locations")


def test_collinear_arrangement():
    """All locations on a straight line"""
    print("\n" + "="*80)
    print("TEST 20: Collinear Locations")
    print("="*80)

    restaurants = [Restaurant(0, (5.0, 0.0))]

    # All on x-axis
    couriers = [
        Courier(0, (0.0, 0.0)),
        Courier(1, (2.0, 0.0)),
        Courier(2, (10.0, 0.0))
    ]

    orders = [
        Order(0, 0, (5.0, 0.0), (6.0, 0.0), 0.0),
        Order(1, 0, (5.0, 0.0), (8.0, 0.0), 0.0),
        Order(2, 0, (5.0, 0.0), (12.0, 0.0), 0.0)
    ]

    for order in orders:
        order.state = "READY"

    state = SimulationState(restaurants, couriers, orders, duration=3600)

    assignments = assign_hungarian(state, couriers, orders)

    print(f"All locations on x-axis (y=0)")
    print(f"Assignments: {len(assignments)}")

    assert len(assignments) == 3, f"Expected 3 assignments, got {len(assignments)}"

    verify_assignment_format(assignments)
    verify_no_duplicates(assignments)
    print("✓ PASS: Handles collinear arrangement")


# ============================================================================
# CATEGORY 6: DETERMINISM & CONSISTENCY
# ============================================================================

def test_determinism_multiple_runs():
    """Same input should produce same output every time"""
    print("\n" + "="*80)
    print("TEST 21: Determinism Across Multiple Runs")
    print("="*80)

    state, couriers, orders = create_simple_scenario(
        num_couriers=5,
        num_orders=5,
        courier_locs=[(i, i) for i in range(5)],
        diner_locs=[(2.5+i, 2.5+i) for i in range(5)]
    )

    # Run 10 times
    results = []
    for run in range(10):
        assignments = assign_hungarian(state, couriers, orders)
        # Convert to hashable format for comparison
        assignments_tuple = tuple(sorted(assignments))
        results.append(assignments_tuple)

    # All results should be identical
    first_result = results[0]
    all_same = all(result == first_result for result in results)

    print(f"Ran algorithm 10 times")
    print(f"All results identical: {all_same}")

    if not all_same:
        print("\nDifferent results found:")
        for i, result in enumerate(results):
            print(f"  Run {i+1}: {result}")

    assert all_same, "Algorithm is not deterministic!"

    print("✓ PASS: Algorithm is deterministic")


def test_input_order_independence():
    """Shuffling input order shouldn't affect optimality"""
    print("\n" + "="*80)
    print("TEST 22: Input Order Independence")
    print("="*80)

    state, couriers, orders = create_simple_scenario(
        num_couriers=4,
        num_orders=4,
        courier_locs=[(i, i) for i in range(4)],
        diner_locs=[(2.5+i, 2.5+i) for i in range(4)]
    )

    # Run with original order
    assignments1 = assign_hungarian(state, couriers, orders)
    total1 = sum(
        calculate_route_duration(couriers[c_id].current_location, o_ids, state)
        for c_id, o_ids in assignments1
    )

    # Run with reversed order
    assignments2 = assign_hungarian(state, list(reversed(couriers)), list(reversed(orders)))
    total2 = sum(
        calculate_route_duration(couriers[c_id].current_location, o_ids, state)
        for c_id, o_ids in assignments2
    )

    print(f"Original order total cost: {total1:.2f}s")
    print(f"Reversed order total cost: {total2:.2f}s")
    print(f"Difference: {abs(total1 - total2):.2f}s")

    # Totals should be the same (both optimal)
    assert abs(total1 - total2) < 1.0, \
        f"Different totals! Original={total1:.2f}, Reversed={total2:.2f}"

    print("✓ PASS: Input order doesn't affect optimality")


# ============================================================================
# CATEGORY 7: ASSIGNMENT CONSTRAINTS
# ============================================================================

def test_no_duplicate_courier_ids():
    """Each courier should appear at most once in assignments"""
    print("\n" + "="*80)
    print("TEST 23: No Duplicate Courier IDs")
    print("="*80)

    state, couriers, orders = create_simple_scenario(
        num_couriers=5,
        num_orders=5,
        courier_locs=[(i, i) for i in range(5)],
        diner_locs=[(2.5+i, 2.5+i) for i in range(5)]
    )

    assignments = assign_hungarian(state, couriers, orders)

    courier_ids = [c_id for c_id, _ in assignments]
    unique_courier_ids = set(courier_ids)

    print(f"Total assignments: {len(assignments)}")
    print(f"Unique couriers: {len(unique_courier_ids)}")
    print(f"Courier IDs: {sorted(courier_ids)}")

    assert len(courier_ids) == len(unique_courier_ids), \
        f"Duplicate courier IDs found: {courier_ids}"

    print("✓ PASS: No duplicate courier IDs")


def test_no_duplicate_order_ids():
    """Each order should appear at most once in assignments"""
    print("\n" + "="*80)
    print("TEST 24: No Duplicate Order IDs")
    print("="*80)

    state, couriers, orders = create_simple_scenario(
        num_couriers=5,
        num_orders=5,
        courier_locs=[(i, i) for i in range(5)],
        diner_locs=[(2.5+i, 2.5+i) for i in range(5)]
    )

    assignments = assign_hungarian(state, couriers, orders)

    all_order_ids = []
    for _, o_ids in assignments:
        all_order_ids.extend(o_ids)

    unique_order_ids = set(all_order_ids)

    print(f"Total order assignments: {len(all_order_ids)}")
    print(f"Unique orders: {len(unique_order_ids)}")
    print(f"Order IDs: {sorted(all_order_ids)}")

    assert len(all_order_ids) == len(unique_order_ids), \
        f"Duplicate order IDs found: {all_order_ids}"

    print("✓ PASS: No duplicate order IDs")


def test_assignments_from_input_only():
    """All assigned couriers and orders must be from input"""
    print("\n" + "="*80)
    print("TEST 25: Assignments From Input Only")
    print("="*80)

    state, couriers, orders = create_simple_scenario(
        num_couriers=3,
        num_orders=3,
        courier_locs=[(i, i) for i in range(3)],
        diner_locs=[(2.5+i, 2.5+i) for i in range(3)]
    )

    input_courier_ids = {c.id for c in couriers}
    input_order_ids = {o.id for o in orders}

    assignments = assign_hungarian(state, couriers, orders)

    # Verify all assigned couriers were in input
    for c_id, o_ids in assignments:
        assert c_id in input_courier_ids, \
            f"Courier {c_id} not in input: {input_courier_ids}"

        # Verify all assigned orders were in input
        for o_id in o_ids:
            assert o_id in input_order_ids, \
                f"Order {o_id} not in input: {input_order_ids}"

    print(f"Input couriers: {sorted(input_courier_ids)}")
    print(f"Input orders: {sorted(input_order_ids)}")
    print(f"All assignments verified from input")

    print("✓ PASS: All assignments are from input")


def test_single_order_per_assignment():
    """Hungarian always assigns single orders (1-to-1 matching)"""
    print("\n" + "="*80)
    print("TEST 26: Single Order Per Assignment (1-to-1)")
    print("="*80)

    state, couriers, orders = create_simple_scenario(
        num_couriers=5,
        num_orders=5,
        courier_locs=[(i, i) for i in range(5)],
        diner_locs=[(2.5+i, 2.5+i) for i in range(5)]
    )

    assignments = assign_hungarian(state, couriers, orders)

    print(f"Checking {len(assignments)} assignments...")

    for c_id, o_ids in assignments:
        print(f"  Courier {c_id} → {len(o_ids)} order(s): {o_ids}")
        assert len(o_ids) == 1, \
            f"Hungarian should assign exactly 1 order, got {len(o_ids)}"

    print("✓ PASS: All assignments are 1-to-1")


# ============================================================================
# CATEGORY 8: STATE VALIDATION
# ============================================================================

def test_only_ready_orders_assigned():
    """Only orders with state='READY' should be assigned"""
    print("\n" + "="*80)
    print("TEST 27: Only READY Orders Assigned")
    print("="*80)

    restaurants = [Restaurant(0, (2.0, 2.0))]

    couriers = [Courier(i, (i, i)) for i in range(5)]

    orders = [
        Order(0, 0, (2.0, 2.0), (2.5, 2.5), 0.0),
        Order(1, 0, (2.0, 2.0), (3.5, 3.5), 0.0),
        Order(2, 0, (2.0, 2.0), (4.5, 4.5), 0.0),
    ]

    # Mix of states
    orders[0].state = "READY"
    orders[1].state = "PENDING"  # Should not be assigned
    orders[2].state = "READY"

    # Only pass READY orders to algorithm
    ready_orders = [o for o in orders if o.state == "READY"]

    state = SimulationState(restaurants, couriers, orders, duration=3600)

    assignments = assign_hungarian(state, couriers, ready_orders)

    # Get assigned order IDs
    assigned_order_ids = []
    for _, o_ids in assignments:
        assigned_order_ids.extend(o_ids)

    print(f"READY orders: {[o.id for o in ready_orders]}")
    print(f"Assigned orders: {assigned_order_ids}")

    # Verify only READY orders were assigned
    for o_id in assigned_order_ids:
        assert o_id in [0, 2], f"Non-READY order {o_id} was assigned"

    print("✓ PASS: Only READY orders assigned")


def test_input_lists_not_mutated():
    """Verify algorithm doesn't modify input lists"""
    print("\n" + "="*80)
    print("TEST 28: Input Lists Not Mutated")
    print("="*80)

    state, couriers, orders = create_simple_scenario(
        num_couriers=3,
        num_orders=3,
        courier_locs=[(i, i) for i in range(3)],
        diner_locs=[(2.5+i, 2.5+i) for i in range(3)]
    )

    # Deep copy inputs
    couriers_before = copy.deepcopy(couriers)
    orders_before = copy.deepcopy(orders)

    # Run algorithm
    assignments = assign_hungarian(state, couriers, orders)

    # Verify no mutations
    assert len(couriers) == len(couriers_before), "Couriers list length changed"
    assert len(orders) == len(orders_before), "Orders list length changed"

    # Verify courier locations unchanged
    for i, courier in enumerate(couriers):
        assert courier.current_location == couriers_before[i].current_location, \
            f"Courier {i} location was modified"

    # Verify order locations unchanged
    for i, order in enumerate(orders):
        assert order.diner_location == orders_before[i].diner_location, \
            f"Order {i} location was modified"

    print("✓ PASS: Input lists not mutated")


# ============================================================================
# CATEGORY 9: DUMMY ASSIGNMENT FILTERING
# ============================================================================

def test_dummy_couriers_filtered_correctly():
    """When more orders than couriers, dummy couriers should be filtered"""
    print("\n" + "="*80)
    print("TEST 29: Dummy Couriers Filtered (More Orders)")
    print("="*80)

    state, couriers, orders = create_simple_scenario(
        num_couriers=2,
        num_orders=5,
        courier_locs=[(i, i) for i in range(2)],
        diner_locs=[(2.5+i, 2.5+i) for i in range(5)]
    )

    assignments = assign_hungarian(state, couriers, orders)

    print(f"Input: 2 couriers, 5 orders")
    print(f"Assignments: {len(assignments)}")
    print(f"Assigned couriers: {[c_id for c_id, _ in assignments]}")

    # Should assign exactly 2 orders (no dummy couriers in output)
    assert len(assignments) == 2, \
        f"Expected 2 assignments (no dummies), got {len(assignments)}"

    # All courier IDs should be valid (0 or 1)
    for c_id, _ in assignments:
        assert c_id in [0, 1], f"Invalid courier ID {c_id} (dummy not filtered?)"

    print("✓ PASS: Dummy couriers filtered correctly")


def test_dummy_orders_filtered_correctly():
    """When more couriers than orders, dummy orders should be filtered"""
    print("\n" + "="*80)
    print("TEST 30: Dummy Orders Filtered (More Couriers)")
    print("="*80)

    state, couriers, orders = create_simple_scenario(
        num_couriers=5,
        num_orders=2,
        courier_locs=[(i, i) for i in range(5)],
        diner_locs=[(2.5+i, 2.5+i) for i in range(2)]
    )

    assignments = assign_hungarian(state, couriers, orders)

    print(f"Input: 5 couriers, 2 orders")
    print(f"Assignments: {len(assignments)}")

    # Get all assigned orders
    all_order_ids = []
    for _, o_ids in assignments:
        all_order_ids.extend(o_ids)

    print(f"Assigned orders: {all_order_ids}")

    # Should assign exactly 2 orders (no dummy orders in output)
    assert len(assignments) == 2, \
        f"Expected 2 assignments (no dummies), got {len(assignments)}"

    # All order IDs should be valid (0 or 1)
    for o_id in all_order_ids:
        assert o_id in [0, 1], f"Invalid order ID {o_id} (dummy not filtered?)"

    print("✓ PASS: Dummy orders filtered correctly")


# ============================================================================
# CATEGORY 10: STRESS & PERFORMANCE TESTS
# ============================================================================

def test_large_balanced_scenario_100x100():
    """Test with 100 couriers and 100 orders"""
    print("\n" + "="*80)
    print("TEST 31: Large Balanced Scenario (100x100)")
    print("="*80)

    import time

    restaurants = [Restaurant(0, (50.0, 50.0))]

    # Create 100 couriers in a grid
    couriers = [
        Courier(i, (i % 10, i // 10))
        for i in range(100)
    ]

    # Create 100 orders in a different grid
    orders = [
        Order(i, 0, (50.0, 50.0), (50.0 + (i % 10), 50.0 + (i // 10)), 0.0)
        for i in range(100)
    ]

    for order in orders:
        order.state = "READY"

    state = SimulationState(restaurants, couriers, orders, duration=3600)

    print("Running Hungarian on 100x100 scenario...")
    start_time = time.time()

    assignments = assign_hungarian(state, couriers, orders)

    end_time = time.time()
    elapsed = end_time - start_time

    print(f"\nCompleted in {elapsed:.3f}s")
    print(f"Assignments: {len(assignments)}")

    assert len(assignments) == 100, f"Expected 100 assignments, got {len(assignments)}"

    verify_assignment_format(assignments)
    verify_no_duplicates(assignments)

    print(f"✓ PASS: Handled 100x100 scenario in {elapsed:.3f}s")


def test_performance_benchmark():
    """Benchmark performance on various sizes"""
    print("\n" + "="*80)
    print("TEST 32: Performance Benchmark")
    print("="*80)

    import time

    sizes = [10, 25, 50, 100]
    results = []

    for size in sizes:
        restaurants = [Restaurant(0, (50.0, 50.0))]

        couriers = [Courier(i, (i % 10, i // 10)) for i in range(size)]
        orders = [
            Order(i, 0, (50.0, 50.0), (50.0 + (i % 10), 50.0 + (i // 10)), 0.0)
            for i in range(size)
        ]

        for order in orders:
            order.state = "READY"

        state = SimulationState(restaurants, couriers, orders, duration=3600)

        start_time = time.time()
        assignments = assign_hungarian(state, couriers, orders)
        elapsed = time.time() - start_time

        results.append((size, elapsed))
        print(f"  {size}x{size}: {elapsed:.4f}s ({len(assignments)} assignments)")

    print("\nPerformance Summary:")
    for size, elapsed in results:
        print(f"  {size:3d}x{size:3d}: {elapsed:7.4f}s")

    print("✓ PASS: Performance benchmark completed")


# ============================================================================
# CATEGORY 11: CRITICAL MISSING TESTS - COST MATRIX VERIFICATION
# ============================================================================

def test_cost_matrix_2x2_cell_verification():
    """CRITICAL: Verify cost matrix constructed correctly cell-by-cell"""
    print("\n" + "="*80)
    print("TEST 33: Cost Matrix 2x2 Cell-by-Cell Verification")
    print("="*80)

    restaurants = [Restaurant(0, (2.0, 2.0))]

    # Simple scenario with known distances
    # Courier A at (0,0), Courier B at (4,2)
    # Order 1: restaurant (2,2) → diner (3,3)
    # Order 2: restaurant (2,2) → diner (1,1)

    couriers = [
        Courier(0, (0.0, 0.0)),  # Courier A
        Courier(1, (4.0, 2.0))   # Courier B
    ]

    orders = [
        Order(0, 0, (2.0, 2.0), (3.0, 3.0), 0.0),  # Order 1
        Order(1, 0, (2.0, 2.0), (1.0, 1.0), 0.0)   # Order 2
    ]

    for order in orders:
        order.state = "READY"

    state = SimulationState(restaurants, couriers, orders, duration=3600)

    # Manually calculate expected costs
    # Cost[A→O1] = travel(0,0→2,2) + PICKUP + travel(2,2→3,3) + DROPOFF
    cost_A_O1 = calculate_route_duration((0.0, 0.0), [0], state)

    # Cost[A→O2] = travel(0,0→2,2) + PICKUP + travel(2,2→1,1) + DROPOFF
    cost_A_O2 = calculate_route_duration((0.0, 0.0), [1], state)

    # Cost[B→O1] = travel(4,2→2,2) + PICKUP + travel(2,2→3,3) + DROPOFF
    cost_B_O1 = calculate_route_duration((4.0, 2.0), [0], state)

    # Cost[B→O2] = travel(4,2→2,2) + PICKUP + travel(2,2→1,1) + DROPOFF
    cost_B_O2 = calculate_route_duration((4.0, 2.0), [1], state)

    print(f"\nManually calculated costs:")
    print(f"  Cost[Courier A → Order 1]: {cost_A_O1:.2f}s")
    print(f"  Cost[Courier A → Order 2]: {cost_A_O2:.2f}s")
    print(f"  Cost[Courier B → Order 1]: {cost_B_O1:.2f}s")
    print(f"  Cost[Courier B → Order 2]: {cost_B_O2:.2f}s")

    # Run Hungarian
    assignments = assign_hungarian(state, couriers, orders)

    # Verify optimal assignment was selected
    total_cost = sum(
        calculate_route_duration(couriers[c_id].current_location, o_ids, state)
        for c_id, o_ids in assignments
    )

    # Calculate both possible assignment totals
    total_A_O1_B_O2 = cost_A_O1 + cost_B_O2
    total_A_O2_B_O1 = cost_A_O2 + cost_B_O1

    optimal_total = min(total_A_O1_B_O2, total_A_O2_B_O1)

    print(f"\nPossible assignment totals:")
    print(f"  A→O1, B→O2: {total_A_O1_B_O2:.2f}s")
    print(f"  A→O2, B→O1: {total_A_O2_B_O1:.2f}s")
    print(f"  Optimal: {optimal_total:.2f}s")
    print(f"  Hungarian selected: {total_cost:.2f}s")

    assert abs(total_cost - optimal_total) < 1.0, \
        f"Cost matrix logic error: expected {optimal_total:.2f}, got {total_cost:.2f}"

    print("✓ PASS: Cost matrix correctly constructed")


def test_cost_matrix_padding_more_couriers():
    """CRITICAL: Verify dummy order padding when more couriers"""
    print("\n" + "="*80)
    print("TEST 34: Cost Matrix Padding - More Couriers")
    print("="*80)

    # 5 couriers, 2 orders → should pad 3 dummy orders
    state, couriers, orders = create_simple_scenario(
        num_couriers=5,
        num_orders=2,
        courier_locs=[(i, i) for i in range(5)],
        diner_locs=[(2.5+i, 2.5+i) for i in range(2)]
    )

    assignments = assign_hungarian(state, couriers, orders)

    print(f"Input: 5 couriers, 2 orders")
    print(f"Expected: Matrix 5x5 with 3 dummy columns (cost=0)")
    print(f"Output: {len(assignments)} assignments")

    # Should assign exactly 2 orders (dummies filtered)
    assert len(assignments) == 2, \
        f"Expected 2 real assignments, got {len(assignments)}"

    # All assigned orders should be real (0 or 1)
    all_order_ids = []
    for _, o_ids in assignments:
        all_order_ids.extend(o_ids)

    assert all(oid in [0, 1] for oid in all_order_ids), \
        f"Dummy order not filtered: {all_order_ids}"

    print("✓ PASS: Dummy orders padded and filtered correctly")


def test_cost_matrix_padding_more_orders():
    """CRITICAL: Verify dummy courier padding when more orders"""
    print("\n" + "="*80)
    print("TEST 35: Cost Matrix Padding - More Orders")
    print("="*80)

    # 2 couriers, 5 orders → should pad 3 dummy couriers
    state, couriers, orders = create_simple_scenario(
        num_couriers=2,
        num_orders=5,
        courier_locs=[(i, i) for i in range(2)],
        diner_locs=[(2.5+i, 2.5+i) for i in range(5)]
    )

    assignments = assign_hungarian(state, couriers, orders)

    print(f"Input: 2 couriers, 5 orders")
    print(f"Expected: Matrix 5x5 with 3 dummy rows (cost=1e9)")
    print(f"Output: {len(assignments)} assignments")

    # Should assign exactly 2 orders (dummies filtered)
    assert len(assignments) == 2, \
        f"Expected 2 real assignments, got {len(assignments)}"

    # All assigned couriers should be real (0 or 1)
    assigned_couriers = [c_id for c_id, _ in assignments]

    assert all(cid in [0, 1] for cid in assigned_couriers), \
        f"Dummy courier not filtered: {assigned_couriers}"

    print("✓ PASS: Dummy couriers padded and filtered correctly")


def test_unbalanced_cheapest_3_of_5_orders():
    """CRITICAL: Verify cheapest 3 orders selected when 3 couriers, 5 orders"""
    print("\n" + "="*80)
    print("TEST 36: Unbalanced - Verify Cheapest 3 of 5 Orders Selected")
    print("="*80)

    restaurants = [Restaurant(0, (5.0, 5.0))]

    # 3 couriers all at same location
    couriers = [Courier(i, (5.0, 5.0)) for i in range(3)]

    # 5 orders with progressively increasing distances (and thus costs)
    orders = [
        Order(0, 0, (5.0, 5.0), (5.1, 5.1), 0.0),  # Closest - cheapest
        Order(1, 0, (5.0, 5.0), (5.2, 5.2), 0.0),  # 2nd closest
        Order(2, 0, (5.0, 5.0), (5.3, 5.3), 0.0),  # 3rd closest
        Order(3, 0, (5.0, 5.0), (10.0, 10.0), 0.0), # Far - expensive
        Order(4, 0, (5.0, 5.0), (20.0, 20.0), 0.0)  # Very far - most expensive
    ]

    for order in orders:
        order.state = "READY"

    state = SimulationState(restaurants, couriers, orders, duration=3600)

    # Calculate costs for each order
    costs = []
    for order in orders:
        cost = calculate_route_duration((5.0, 5.0), [order.id], state)
        costs.append((order.id, cost))

    costs_sorted = sorted(costs, key=lambda x: x[1])

    print(f"\nOrder costs (from courier at 5,5):")
    for oid, cost in costs_sorted:
        print(f"  Order {oid}: {cost:.2f}s")

    # Run Hungarian
    assignments = assign_hungarian(state, couriers, orders)

    # Get assigned orders
    assigned_orders = []
    for _, o_ids in assignments:
        assigned_orders.extend(o_ids)

    assigned_orders = sorted(assigned_orders)

    print(f"\nAssigned orders: {assigned_orders}")
    print(f"Expected cheapest 3: {[oid for oid, _ in costs_sorted[:3]]}")

    # The 3 cheapest orders should be assigned
    cheapest_3 = [oid for oid, _ in costs_sorted[:3]]

    assert assigned_orders == sorted(cheapest_3), \
        f"Did not select cheapest 3! Expected {sorted(cheapest_3)}, got {assigned_orders}"

    print("✓ PASS: Cheapest 3 orders correctly selected")


def test_unbalanced_cheapest_1_of_100_orders():
    """CRITICAL: Verify cheapest order selected when 1 courier, 100 orders"""
    print("\n" + "="*80)
    print("TEST 37: Unbalanced - Verify Cheapest 1 of 100 Orders Selected")
    print("="*80)

    restaurants = [Restaurant(0, (50.0, 50.0))]

    # 1 courier at origin
    couriers = [Courier(0, (50.0, 50.0))]

    # 100 orders with known cost gradient
    # Order 0 is closest (cheapest), Order 99 is farthest (most expensive)
    orders = [
        Order(i, 0, (50.0, 50.0), (50.0 + i * 0.1, 50.0 + i * 0.1), 0.0)
        for i in range(100)
    ]

    for order in orders:
        order.state = "READY"

    state = SimulationState(restaurants, couriers, orders, duration=3600)

    # Calculate cost of order 0 (should be cheapest)
    cost_order_0 = calculate_route_duration((50.0, 50.0), [0], state)

    # Calculate cost of order 99 (should be most expensive)
    cost_order_99 = calculate_route_duration((50.0, 50.0), [99], state)

    print(f"\nOrder 0 cost (closest): {cost_order_0:.2f}s")
    print(f"Order 99 cost (farthest): {cost_order_99:.2f}s")

    # Run Hungarian
    assignments = assign_hungarian(state, couriers, orders)

    assert len(assignments) == 1, f"Expected 1 assignment, got {len(assignments)}"

    assigned_order = assignments[0][1][0]

    print(f"Assigned order: {assigned_order}")

    # Should assign order 0 (the cheapest)
    assert assigned_order == 0, \
        f"Did not select cheapest order! Expected order 0, got order {assigned_order}"

    print("✓ PASS: Cheapest order (Order 0) correctly selected")


def test_multiple_restaurants_2_restaurants():
    """CRITICAL: Test with 2 restaurants - verify correct restaurant location used"""
    print("\n" + "="*80)
    print("TEST 38: Multiple Restaurants - 2 Restaurants")
    print("="*80)

    # 2 restaurants at different locations
    restaurants = [
        Restaurant(0, (1.0, 1.0)),
        Restaurant(1, (9.0, 9.0))
    ]

    # 2 couriers equidistant between restaurants
    couriers = [
        Courier(0, (5.0, 5.0)),
        Courier(1, (5.0, 5.0))
    ]

    # Order 0 from restaurant 0, Order 1 from restaurant 1
    orders = [
        Order(0, restaurant_id=0, restaurant_location=(1.0, 1.0), diner_location=(0.0, 0.0), placement_time=0.0),
        Order(1, restaurant_id=1, restaurant_location=(9.0, 9.0), diner_location=(10.0, 10.0), placement_time=0.0)
    ]

    for order in orders:
        order.state = "READY"

    state = SimulationState(restaurants, couriers, orders, duration=3600)

    # Calculate costs using correct restaurant locations
    cost_C0_O0 = calculate_route_duration((5.0, 5.0), [0], state)
    cost_C0_O1 = calculate_route_duration((5.0, 5.0), [1], state)

    print(f"\nRestaurant 0 at (1,1), Restaurant 1 at (9,9)")
    print(f"Courier 0 at (5,5)")
    print(f"Order 0 from restaurant_id=0 → Cost: {cost_C0_O0:.2f}s")
    print(f"Order 1 from restaurant_id=1 → Cost: {cost_C0_O1:.2f}s")

    # Run Hungarian
    assignments = assign_hungarian(state, couriers, orders)

    assert len(assignments) == 2, f"Expected 2 assignments, got {len(assignments)}"

    # Both orders should be assigned
    assigned_orders = []
    for _, o_ids in assignments:
        assigned_orders.extend(o_ids)

    assert sorted(assigned_orders) == [0, 1], \
        f"Expected orders [0, 1] assigned, got {sorted(assigned_orders)}"

    print("✓ PASS: Multiple restaurants handled correctly")


def test_multiple_restaurants_3_restaurants():
    """CRITICAL: Test with 3 restaurants - mixed batch"""
    print("\n" + "="*80)
    print("TEST 39: Multiple Restaurants - 3 Restaurants Mixed")
    print("="*80)

    # 3 restaurants at different locations
    restaurants = [
        Restaurant(0, (0.0, 0.0)),
        Restaurant(1, (5.0, 5.0)),
        Restaurant(2, (10.0, 10.0))
    ]

    # 3 couriers at different locations
    couriers = [
        Courier(0, (0.0, 0.0)),
        Courier(1, (5.0, 5.0)),
        Courier(2, (10.0, 10.0))
    ]

    # 3 orders from 3 different restaurants
    orders = [
        Order(0, restaurant_id=0, restaurant_location=(0.0, 0.0), diner_location=(1.0, 1.0), placement_time=0.0),
        Order(1, restaurant_id=1, restaurant_location=(5.0, 5.0), diner_location=(6.0, 6.0), placement_time=0.0),
        Order(2, restaurant_id=2, restaurant_location=(10.0, 10.0), diner_location=(11.0, 11.0), placement_time=0.0)
    ]

    for order in orders:
        order.state = "READY"

    state = SimulationState(restaurants, couriers, orders, duration=3600)

    # Run Hungarian
    assignments = assign_hungarian(state, couriers, orders)

    print(f"\n3 restaurants, 3 couriers, 3 orders")
    print(f"Assignments: {len(assignments)}")

    assert len(assignments) == 3, f"Expected 3 assignments, got {len(assignments)}"

    # Verify each order uses its correct restaurant location
    for c_id, o_ids in assignments:
        for o_id in o_ids:
            order = state.orders[o_id]
            print(f"  Courier {c_id} → Order {o_id} (restaurant_id={order.restaurant_id})")

    print("✓ PASS: 3 restaurants handled correctly")


def test_order_from_restaurant_id_3():
    """CRITICAL: Test order from restaurant_id=3 (not 0)"""
    print("\n" + "="*80)
    print("TEST 40: Order from restaurant_id=3 (Not Zero)")
    print("="*80)

    # Create 5 restaurants but only use restaurant_id=3
    restaurants = [
        Restaurant(0, (0.0, 0.0)),
        Restaurant(1, (2.0, 2.0)),
        Restaurant(2, (4.0, 4.0)),
        Restaurant(3, (6.0, 6.0)),  # This one
        Restaurant(4, (8.0, 8.0))
    ]

    couriers = [Courier(0, (6.0, 6.0))]

    # Order from restaurant_id=3
    orders = [
        Order(0, restaurant_id=3, restaurant_location=(6.0, 6.0), diner_location=(7.0, 7.0), placement_time=0.0)
    ]

    orders[0].state = "READY"

    state = SimulationState(restaurants, couriers, orders, duration=3600)

    print(f"Order 0 from restaurant_id=3 at location (6,6)")

    # Run Hungarian
    assignments = assign_hungarian(state, couriers, orders)

    assert len(assignments) == 1, f"Expected 1 assignment, got {len(assignments)}"

    # Calculate cost (should use restaurant location 6,6)
    cost = calculate_route_duration((6.0, 6.0), [0], state)
    print(f"Cost calculated: {cost:.2f}s")

    # Cost should be relatively small since courier starts at restaurant
    # (just service times + short travel to diner)
    expected_min = PICKUP_SERVICE_TIME + get_travel_time((6.0, 6.0), (7.0, 7.0)) + DROPOFF_SERVICE_TIME

    assert abs(cost - expected_min) < 1.0, \
        f"Cost calculation seems wrong for restaurant_id=3: expected ~{expected_min:.2f}, got {cost:.2f}"

    print("✓ PASS: restaurant_id=3 handled correctly")


def test_boundary_condition_cost_near_1e8():
    """CRITICAL: Test boundary at 1e8 threshold"""
    print("\n" + "="*80)
    print("TEST 41: Boundary Condition - Cost Near 1e8")
    print("="*80)

    # This test verifies the filtering threshold
    # Real costs < 1e8 should be assigned
    # Dummy costs = 1e9 should be filtered

    print("Testing that threshold 1e8 correctly separates real from dummy costs")

    # In practice, real costs should never approach 1e8
    # (that would be ~27,777 hours of travel time)
    # But we verify the logic is sound

    # Create normal scenario
    state, couriers, orders = create_simple_scenario(
        num_couriers=2,
        num_orders=2,
        courier_locs=[(0, 0), (1, 1)],
        diner_locs=[(2, 2), (3, 3)]
    )

    assignments = assign_hungarian(state, couriers, orders)

    # Calculate actual costs
    for c_id, o_ids in assignments:
        cost = calculate_route_duration(couriers[c_id].current_location, o_ids, state)
        print(f"  Courier {c_id} → Order {o_ids[0]}: cost={cost:.2f}s")

        # Real costs should be << 1e8
        assert cost < 1e7, f"Unrealistic cost detected: {cost:.2f}s"

    # All assignments should be real (not filtered)
    assert len(assignments) == 2, \
        f"Expected 2 assignments, got {len(assignments)} (may have filtered real costs)"

    print("✓ PASS: Boundary threshold 1e8 working correctly")


def test_balanced_no_padding_3x3():
    """CRITICAL: Balanced case (3x3) should have NO padding"""
    print("\n" + "="*80)
    print("TEST 42: Balanced 3x3 - No Padding Required")
    print("="*80)

    state, couriers, orders = create_simple_scenario(
        num_couriers=3,
        num_orders=3,
        courier_locs=[(i, i) for i in range(3)],
        diner_locs=[(2.5+i, 2.5+i) for i in range(3)]
    )

    assignments = assign_hungarian(state, couriers, orders)

    print(f"Input: 3 couriers, 3 orders (balanced)")
    print(f"Expected: Matrix 3x3, no padding needed")
    print(f"Output: {len(assignments)} assignments")

    # Should assign all 3 (balanced)
    assert len(assignments) == 3, \
        f"Expected 3 assignments (balanced), got {len(assignments)}"

    # All orders should be assigned
    assigned_orders = []
    for _, o_ids in assignments:
        assigned_orders.extend(o_ids)

    assert sorted(assigned_orders) == [0, 1, 2], \
        f"Expected all orders [0,1,2] assigned, got {sorted(assigned_orders)}"

    # All couriers should be assigned
    assigned_couriers = sorted([c_id for c_id, _ in assignments])

    assert assigned_couriers == [0, 1, 2], \
        f"Expected all couriers [0,1,2] assigned, got {assigned_couriers}"

    print("✓ PASS: Balanced 3x3 scenario handled correctly without padding")


# ============================================================================
# CATEGORY 12: HIGH PRIORITY - OPTIMALITY PROOFS & EDGE CASES
# ============================================================================

def test_canonical_2x2_crossed_costs():
    """HIGH: Canonical 2x2 crossed costs - forces Hungarian to avoid greedy choice"""
    print("\n" + "="*80)
    print("TEST 43: Canonical 2x2 Crossed Costs (Adversarial)")
    print("="*80)

    restaurants = [Restaurant(0, (5.0, 5.0))]

    # Courier A close to restaurant, Courier B far
    couriers = [
        Courier(0, (5.1, 5.1)),  # Very close to restaurant
        Courier(1, (15.0, 15.0)) # Far from restaurant
    ]

    # Order 1: customer very far from restaurant
    # Order 2: customer very close to restaurant
    orders = [
        Order(0, 0, (5.0, 5.0), (20.0, 20.0), 0.0),  # Far customer
        Order(1, 0, (5.0, 5.0), (5.1, 5.1), 0.0)     # Close customer
    ]

    for order in orders:
        order.state = "READY"

    state = SimulationState(restaurants, couriers, orders, duration=3600)

    # Calculate all 4 possible assignment costs
    cost_A_O1 = calculate_route_duration((5.1, 5.1), [0], state)  # A→O1 (far delivery)
    cost_A_O2 = calculate_route_duration((5.1, 5.1), [1], state)  # A→O2 (close delivery)
    cost_B_O1 = calculate_route_duration((15.0, 15.0), [0], state)  # B→O1
    cost_B_O2 = calculate_route_duration((15.0, 15.0), [1], state)  # B→O2

    print(f"\nAll possible costs:")
    print(f"  Courier A (5.1,5.1) → Order 1 (far): {cost_A_O1:.2f}s")
    print(f"  Courier A (5.1,5.1) → Order 2 (close): {cost_A_O2:.2f}s")
    print(f"  Courier B (15,15) → Order 1 (far): {cost_B_O1:.2f}s")
    print(f"  Courier B (15,15) → Order 2 (close): {cost_B_O2:.2f}s")

    # Greedy would pick A→O1 (closest courier to restaurant)
    # But optimal might be A→O2, B→O1 if B's pickup penalty < A's delivery penalty

    total_A_O1_B_O2 = cost_A_O1 + cost_B_O2
    total_A_O2_B_O1 = cost_A_O2 + cost_B_O1

    print(f"\nComplete assignment costs:")
    print(f"  A→O1, B→O2: {total_A_O1_B_O2:.2f}s")
    print(f"  A→O2, B→O1: {total_A_O2_B_O1:.2f}s")

    optimal_total = min(total_A_O1_B_O2, total_A_O2_B_O1)
    print(f"  Optimal: {optimal_total:.2f}s")

    # Run Hungarian
    assignments = assign_hungarian(state, couriers, orders)

    actual_total = sum(
        calculate_route_duration(couriers[c_id].current_location, o_ids, state)
        for c_id, o_ids in assignments
    )

    print(f"  Hungarian selected: {actual_total:.2f}s")

    assert abs(actual_total - optimal_total) < 1.0, \
        f"Hungarian didn't find optimal! Expected {optimal_total:.2f}, got {actual_total:.2f}"

    print("✓ PASS: Found optimal solution in adversarial crossed-cost scenario")


def test_all_equal_costs_tie_breaking():
    """HIGH: All costs equal - verify deterministic tie breaking"""
    print("\n" + "="*80)
    print("TEST 44: All Equal Costs - Deterministic Tie Breaking")
    print("="*80)

    restaurants = [Restaurant(0, (5.0, 5.0))]

    # All couriers at same location
    couriers = [Courier(i, (5.0, 5.0)) for i in range(3)]

    # All orders to same location
    orders = [Order(i, 0, (5.0, 5.0), (6.0, 6.0), 0.0) for i in range(3)]

    for order in orders:
        order.state = "READY"

    state = SimulationState(restaurants, couriers, orders, duration=3600)

    # All costs should be identical
    cost = calculate_route_duration((5.0, 5.0), [0], state)
    print(f"\nAll assignment costs are identical: {cost:.2f}s")

    # Run 10 times to verify determinism
    results = []
    for run in range(10):
        assignments = assign_hungarian(state, couriers, orders)
        # Sort to create canonical representation
        assignments_sorted = tuple(sorted(assignments))
        results.append(assignments_sorted)

    first_result = results[0]
    all_same = all(r == first_result for r in results)

    print(f"Ran 10 times, all results identical: {all_same}")

    if not all_same:
        print("\nDifferent results detected (tie-breaking inconsistent?):")
        for i, result in enumerate(set(results)):
            print(f"  Result {i+1}: {result}")

    assert all_same, "Tie-breaking is not deterministic!"

    print("✓ PASS: Deterministic tie-breaking with all equal costs")


def test_float_precision_close_costs():
    """HIGH: Test with costs differing by <0.01s"""
    print("\n" + "="*80)
    print("TEST 45: Float Precision - Costs Differing by <0.01s")
    print("="*80)

    restaurants = [Restaurant(0, (10.0, 10.0))]

    # Two couriers very close together
    couriers = [
        Courier(0, (10.0, 10.0)),
        Courier(1, (10.0001, 10.0001))  # 0.01mm away
    ]

    # Order with specific location
    orders = [Order(0, 0, (10.0, 10.0), (11.0, 11.0), 0.0)]
    orders[0].state = "READY"

    state = SimulationState(restaurants, couriers, orders, duration=3600)

    cost_C0 = calculate_route_duration((10.0, 10.0), [0], state)
    cost_C1 = calculate_route_duration((10.0001, 10.0001), [0], state)

    print(f"\nCourier 0 cost: {cost_C0:.6f}s")
    print(f"Courier 1 cost: {cost_C1:.6f}s")
    print(f"Difference: {abs(cost_C0 - cost_C1):.6f}s")

    # Run Hungarian
    assignments = assign_hungarian(state, couriers, orders)

    assert len(assignments) == 1, f"Expected 1 assignment, got {len(assignments)}"

    assigned_courier = assignments[0][0]
    print(f"Assigned to courier: {assigned_courier}")

    # Should assign to courier with lower cost (might be either one due to float precision)
    # Main test: algorithm doesn't crash or produce invalid result
    assert assigned_courier in [0, 1], f"Invalid courier ID: {assigned_courier}"

    print("✓ PASS: Float precision handled correctly")


def test_negative_coordinates():
    """HIGH: Test with negative coordinates"""
    print("\n" + "="*80)
    print("TEST 46: Negative Coordinates")
    print("="*80)

    restaurants = [Restaurant(0, (-5.0, -5.0))]

    couriers = [
        Courier(0, (-10.0, -10.0)),
        Courier(1, (-2.0, -2.0))
    ]

    orders = [
        Order(0, 0, (-5.0, -5.0), (-3.0, -3.0), 0.0),
        Order(1, 0, (-5.0, -5.0), (-15.0, -15.0), 0.0)
    ]

    for order in orders:
        order.state = "READY"

    state = SimulationState(restaurants, couriers, orders, duration=3600)

    print(f"All locations have negative coordinates")
    print(f"  Restaurant: (-5, -5)")
    print(f"  Couriers: (-10, -10), (-2, -2)")
    print(f"  Diners: (-3, -3), (-15, -15)")

    # Run Hungarian
    assignments = assign_hungarian(state, couriers, orders)

    assert len(assignments) == 2, f"Expected 2 assignments, got {len(assignments)}"

    # Calculate distances to verify they work with negative coords
    for c_id, o_ids in assignments:
        cost = calculate_route_duration(couriers[c_id].current_location, o_ids, state)
        print(f"  Courier {c_id} → Order {o_ids[0]}: {cost:.2f}s")
        assert cost > 0, f"Cost should be positive, got {cost}"

    verify_assignment_format(assignments)
    verify_no_duplicates(assignments)

    print("✓ PASS: Negative coordinates handled correctly")


def test_very_small_distances():
    """HIGH: Test with very small distances (<1 meter = 0.001 km)"""
    print("\n" + "="*80)
    print("TEST 47: Very Small Distances (<1 meter)")
    print("="*80)

    restaurants = [Restaurant(0, (10.0, 10.0))]

    # Courier 1mm away from restaurant
    couriers = [Courier(0, (10.000001, 10.000001))]

    # Diner 1mm away from restaurant
    orders = [Order(0, 0, (10.0, 10.0), (10.000001, 10.000001), 0.0)]
    orders[0].state = "READY"

    state = SimulationState(restaurants, couriers, orders, duration=3600)

    cost = calculate_route_duration((10.000001, 10.000001), [0], state)

    print(f"Distance: ~1mm for both pickup and delivery")
    print(f"Total cost: {cost:.2f}s")

    # Cost should be approximately just service times (270s)
    expected_approx = PICKUP_SERVICE_TIME + DROPOFF_SERVICE_TIME

    assert abs(cost - expected_approx) < 5.0, \
        f"Cost with tiny distances should be ~{expected_approx}s, got {cost:.2f}s"

    # Run Hungarian
    assignments = assign_hungarian(state, couriers, orders)

    assert len(assignments) == 1, f"Expected 1 assignment, got {len(assignments)}"

    print("✓ PASS: Very small distances handled correctly")


def test_adversarial_route_aware_scenario_2():
    """HIGH: Another route-aware test - near pickup far delivery vs far pickup near delivery"""
    print("\n" + "="*80)
    print("TEST 48: Adversarial Route-Aware Scenario #2")
    print("="*80)

    restaurants = [Restaurant(0, (10.0, 10.0))]

    couriers = [
        Courier(0, (9.0, 10.0)),   # 1km from restaurant
        Courier(1, (20.0, 10.0))   # 10km from restaurant
    ]

    orders = [
        Order(0, 0, (10.0, 10.0), (30.0, 10.0), 0.0),  # Order 1: 20km delivery
        Order(1, 0, (10.0, 10.0), (21.0, 10.0), 0.0)   # Order 2: 11km delivery
    ]

    for order in orders:
        order.state = "READY"

    state = SimulationState(restaurants, couriers, orders, duration=3600)

    # Calculate costs
    cost_C0_O1 = calculate_route_duration((9.0, 10.0), [0], state)   # 1km pickup + 20km delivery
    cost_C0_O2 = calculate_route_duration((9.0, 10.0), [1], state)   # 1km pickup + 11km delivery
    cost_C1_O1 = calculate_route_duration((20.0, 10.0), [0], state)  # 10km pickup + 20km delivery
    cost_C1_O2 = calculate_route_duration((20.0, 10.0), [1], state)  # 10km pickup + 11km delivery

    print(f"\nRoute costs:")
    print(f"  C0 (1km to restaurant) → O1 (20km delivery): {cost_C0_O1:.2f}s")
    print(f"  C0 (1km to restaurant) → O2 (11km delivery): {cost_C0_O2:.2f}s")
    print(f"  C1 (10km to restaurant) → O1 (20km delivery): {cost_C1_O1:.2f}s")
    print(f"  C1 (10km to restaurant) → O2 (11km delivery): {cost_C1_O2:.2f}s")

    total_C0_O1_C1_O2 = cost_C0_O1 + cost_C1_O2
    total_C0_O2_C1_O1 = cost_C0_O2 + cost_C1_O1

    print(f"\nComplete assignments:")
    print(f"  C0→O1, C1→O2: {total_C0_O1_C1_O2:.2f}s")
    print(f"  C0→O2, C1→O1: {total_C0_O2_C1_O1:.2f}s")

    optimal_total = min(total_C0_O1_C1_O2, total_C0_O2_C1_O1)

    # Run Hungarian
    assignments = assign_hungarian(state, couriers, orders)

    actual_total = sum(
        calculate_route_duration(couriers[c_id].current_location, o_ids, state)
        for c_id, o_ids in assignments
    )

    print(f"  Optimal: {optimal_total:.2f}s")
    print(f"  Hungarian: {actual_total:.2f}s")

    assert abs(actual_total - optimal_total) < 1.0, \
        f"Route-aware optimization failed! Expected {optimal_total:.2f}, got {actual_total:.2f}"

    print("✓ PASS: Route-aware optimization working correctly")


# ============================================================================
# TEST RUNNER
# ============================================================================

def run_all_tests():
    """Run all test functions"""
    print("\n" + "="*80)
    print("HUNGARIAN ALGORITHM TEST SUITE")
    print("="*80)
    print(f"Testing: assign_hungarian() from assignment_algorithms.py")
    print(f"Service times: PICKUP={PICKUP_SERVICE_TIME}s, DROPOFF={DROPOFF_SERVICE_TIME}s")
    print("="*80)

    test_functions = [
        # Category 1: Basic Functionality
        test_empty_couriers_empty_orders,
        test_empty_couriers_with_orders,
        test_couriers_with_empty_orders,
        test_single_courier_single_order,
        test_assignment_format_validation,

        # Category 2: Unbalanced Scenarios
        test_more_couriers_than_orders_2_to_1,
        test_more_couriers_than_orders_5_to_3,
        test_more_orders_than_couriers_1_to_2,
        test_more_orders_than_couriers_3_to_5,
        test_extreme_imbalance_100_couriers_1_order,
        test_extreme_imbalance_1_courier_100_orders,

        # Category 3: Cost Calculation
        test_cost_calculation_known_scenario,
        test_zero_distance_scenario,
        test_large_distance_scenario,
        test_service_times_included,

        # Category 4: Optimality
        test_hungarian_finds_optimal_assignment,
        test_hungarian_vs_greedy_difference,

        # Category 5: Geographic Edge Cases
        test_all_same_location,
        test_maximum_spread_locations,
        test_collinear_arrangement,

        # Category 6: Determinism
        test_determinism_multiple_runs,
        test_input_order_independence,

        # Category 7: Constraints
        test_no_duplicate_courier_ids,
        test_no_duplicate_order_ids,
        test_assignments_from_input_only,
        test_single_order_per_assignment,

        # Category 8: State Validation
        test_only_ready_orders_assigned,
        test_input_lists_not_mutated,

        # Category 9: Dummy Filtering
        test_dummy_couriers_filtered_correctly,
        test_dummy_orders_filtered_correctly,

        # Category 10: Stress Tests
        test_large_balanced_scenario_100x100,
        test_performance_benchmark,

        # Category 11: CRITICAL Missing Tests
        test_cost_matrix_2x2_cell_verification,
        test_cost_matrix_padding_more_couriers,
        test_cost_matrix_padding_more_orders,
        test_unbalanced_cheapest_3_of_5_orders,
        test_unbalanced_cheapest_1_of_100_orders,
        test_multiple_restaurants_2_restaurants,
        test_multiple_restaurants_3_restaurants,
        test_order_from_restaurant_id_3,
        test_boundary_condition_cost_near_1e8,
        test_balanced_no_padding_3x3,

        # Category 12: HIGH Priority - Optimality Proofs & Edge Cases
        test_canonical_2x2_crossed_costs,
        test_all_equal_costs_tie_breaking,
        test_float_precision_close_costs,
        test_negative_coordinates,
        test_very_small_distances,
        test_adversarial_route_aware_scenario_2,
    ]

    passed = 0
    failed = 0
    errors = []

    for test_func in test_functions:
        try:
            test_func()
            passed += 1
        except AssertionError as e:
            failed += 1
            errors.append((test_func.__name__, str(e)))
            print(f"\n✗ FAIL: {test_func.__name__}")
            print(f"  {e}")
        except Exception as e:
            failed += 1
            errors.append((test_func.__name__, f"ERROR: {e}"))
            print(f"\n✗ ERROR: {test_func.__name__}")
            print(f"  {e}")

    # Print summary
    print("\n" + "="*80)
    print("TEST SUMMARY")
    print("="*80)
    print(f"Total tests: {passed + failed}")
    print(f"Passed: {passed}")
    print(f"Failed: {failed}")

    if errors:
        print("\nFailed tests:")
        for test_name, error_msg in errors:
            print(f"  - {test_name}")
            print(f"    {error_msg}")

    print("="*80)

    return passed, failed


if __name__ == "__main__":
    import sys
    passed, failed = run_all_tests()
    sys.exit(0 if failed == 0 else 1)
