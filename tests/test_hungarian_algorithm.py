"""
Hungarian Algorithm Tests

Tests the assign_hungarian function from assignment_algorithms.py
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from simulator_core import (
    SimulationState, Courier, Order, Restaurant,
    euclidean_distance, get_travel_time
)
from assignment_algorithms import assign_hungarian, calculate_route_duration


# Give long routes time to finish in tests that are not about deadline failure
LONG_EXP = 10_000.0       # ~2.78 hours
ULTRA_EXP = 100_000.0     # used for the "extremely far" test


# ============================================================================
# CATEGORY 1: BASIC OPTIMALITY TEST
# ============================================================================

def test_1_1_criss_cross_scenario():
    """Test 1.1: The Criss-Cross Scenario"""
    print("\n" + "="*80)
    print("TEST 1.1: The Criss-Cross Scenario (Global Optimality)")
    print("="*80)

    restaurants = [
        Restaurant(0, (1.1, 1.1)),
        Restaurant(1, (10.1, 10.1))
    ]

    couriers = [
        Courier(0, (1.0, 1.0)),
        Courier(1, (10.0, 10.0))
    ]

    orders = [
        Order(0, 0, (1.1, 1.1), (2.0, 2.0), 0.0),
        Order(1, 1, (10.1, 10.1), (11.0, 11.0), 0.0)
    ]

    for order in orders:
        order.state = "READY"
        order.ready_time = 0.0

    state = SimulationState(restaurants, couriers, orders, duration=3600)

    print("\nSetup:")
    print(f"  Courier 0 at (1.0, 1.0)")
    print(f"  Courier 1 at (10.0, 10.0)")
    print(f"  Order 0 at restaurant (1.1, 1.1)")
    print(f"  Order 1 at restaurant (10.1, 10.1)")

    print("\nCost matrix:")
    for i, c in enumerate(couriers):
        for j, o in enumerate(orders):
            cost = calculate_route_duration(c.current_location, [o.id], state, False, True)
            print(f"  C{i} → O{j}: {cost:.1f}s")

    result = assign_hungarian(state, couriers, orders)

    print(f"\nOutput: {result}")

    assignments_dict = {o_ids[0]: c_id for c_id, o_ids in result}

    print(f"  Order 0 assigned to: Courier {assignments_dict[0]}")
    print(f"  Order 1 assigned to: Courier {assignments_dict[1]}")

    assert len(result) == 2
    assert assignments_dict[0] == 0
    assert assignments_dict[1] == 1

    print("PASS")


# ============================================================================
# CATEGORY 2: UNBALANCED SCENARIO TESTS
# ============================================================================

def test_2_1_courier_surplus():
    """Test 2.1: Courier Surplus"""
    print("\n" + "="*80)
    print("TEST 2.1: Courier Surplus")
    print("="*80)

    restaurants = [Restaurant(0, (1.1, 1.1))]

    couriers = [
        Courier(0, (1.0, 1.0)),
        Courier(1, (2.0, 2.0)),
        Courier(2, (10.0, 10.0))
    ]

    orders = [Order(0, 0, (1.1, 1.1), (2.0, 2.0), 0.0)]
    orders[0].state = "READY"
    orders[0].ready_time = 0.0

    state = SimulationState(restaurants, couriers, orders, duration=3600)

    print(f"\nInput: {len(couriers)} couriers, {len(orders)} order")
    print("Courier distances to restaurant:")
    for c in couriers:
        dist = euclidean_distance(c.current_location, restaurants[0].location)
        print(f"  Courier {c.id}: {dist:.3f} km")

    result = assign_hungarian(state, couriers, orders)

    print(f"\nOutput: {result}")

    assert len(result) == 1
    assert result[0][0] == 0
    assert result[0][1] == [0]

    print("PASS")


def test_2_2_order_surplus():
    """Test 2.2: Order Surplus"""
    print("\n" + "="*80)
    print("TEST 2.2: Order Surplus")
    print("="*80)

    restaurants = [
        Restaurant(0, (1.1, 1.1)),
        Restaurant(1, (10.0, 10.0))
    ]

    couriers = [Courier(0, (1.0, 1.0))]

    orders = [
        Order(0, 0, (1.1, 1.1), (2.0, 2.0), 0.0),
        Order(1, 1, (10.0, 10.0), (11.0, 11.0), 0.0)
    ]

    for order in orders:
        order.state = "READY"
        order.ready_time = 0.0

    state = SimulationState(restaurants, couriers, orders, duration=3600)

    print(f"\nInput: {len(couriers)} courier, {len(orders)} orders")
    print("Order costs:")
    for o in orders:
        cost = calculate_route_duration(couriers[0].current_location, [o.id], state, False, True)
        print(f"  Order {o.id}: {cost:.1f}s")

    result = assign_hungarian(state, couriers, orders)

    print(f"\nOutput: {result}")

    assert len(result) == 1
    assert result[0][0] == 0
    assert result[0][1][0] == 0

    print("PASS")


# ============================================================================
# CATEGORY 3: EDGE CASE SCENARIOS
# ============================================================================

def test_3_1_no_couriers_many_orders():
    """Test 3.1: No Couriers, Many Orders"""
    print("\n" + "="*80)
    print("TEST 3.1: No Couriers, Many Orders")
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

    print(f"\nInput: {len(couriers)} couriers, {len(orders)} orders")

    result = assign_hungarian(state, couriers, orders)

    print(f"Output: {result}")

    assert result == []

    print("PASS")


def test_3_2_many_couriers_no_orders():
    """Test 3.2: Many Couriers, No Orders"""
    print("\n" + "="*80)
    print("TEST 3.2: Many Couriers, No Orders")
    print("="*80)

    restaurants = [Restaurant(0, (2.0, 2.0))]

    couriers = [
        Courier(0, (1.0, 1.0)),
        Courier(1, (2.0, 2.0)),
        Courier(2, (3.0, 3.0))
    ]

    orders = []

    state = SimulationState(restaurants, couriers, orders, duration=3600)

    print(f"\nInput: {len(couriers)} couriers, {len(orders)} orders")

    result = assign_hungarian(state, couriers, orders)

    print(f"Output: {result}")

    assert result == []

    print("PASS")


def test_3_3_one_courier_one_order():
    """Test 3.3: One Courier, One Order"""
    print("\n" + "="*80)
    print("TEST 3.3: One Courier, One Order")
    print("="*80)

    restaurants = [Restaurant(0, (2.0, 2.0))]

    couriers = [Courier(0, (1.0, 1.0))]

    orders = [Order(0, 0, (2.0, 2.0), (3.0, 3.0), 0.0, expiration_time=LONG_EXP)]
    orders[0].state = "READY"
    orders[0].ready_time = 0.0

    state = SimulationState(restaurants, couriers, orders, duration=3600)

    print(f"\nInput: 1 courier, 1 order")

    result = assign_hungarian(state, couriers, orders)

    print(f"Output: {result}")

    assert len(result) == 1
    assert result[0] == (0, [0])

    print("PASS")


# ============================================================================
# CATEGORY 4: INTELLIGENCE LITMUS TEST (VS. GREEDY)
# ============================================================================

def test_4_1_strategic_dilemma():
    """Test 4.1: The Strategic Dilemma"""
    print("\n" + "="*80)
    print("TEST 4.1: The Strategic Dilemma (Hungarian vs Greedy)")
    print("="*80)

    restaurants = [
        Restaurant(0, (5.0, 5.0)),
        Restaurant(1, (5.2, 5.2))
    ]

    couriers = [
        Courier(0, (5.0, 5.1)),
        Courier(1, (10.0, 10.0))
    ]

    orders = [
        Order(0, 0, (5.0, 5.0), (6.0, 6.0), 0.0, expiration_time=LONG_EXP),
        Order(1, 1, (5.2, 5.2), (6.2, 6.2), 0.0, expiration_time=LONG_EXP)
    ]

    orders[0].ready_time = 300.0
    orders[1].ready_time = 299.0
    orders[0].state = "READY"
    orders[1].state = "READY"

    state = SimulationState(restaurants, couriers, orders, duration=3600)

    print("\nSetup:")
    print(f"  Courier 0 at (5.0, 5.1)")
    print(f"  Courier 1 at (10.0, 10.0)")
    print(f"  Order 0 at (5.0, 5.0), ready_time=300")
    print(f"  Order 1 at (5.2, 5.2), ready_time=299")

    print("\nCost matrix:")
    total_cost_option_a = 0
    total_cost_option_b = 0

    cost_c0_o0 = calculate_route_duration(couriers[0].current_location, [orders[0].id], state, False, True)
    cost_c0_o1 = calculate_route_duration(couriers[0].current_location, [orders[1].id], state, False, True)
    cost_c1_o0 = calculate_route_duration(couriers[1].current_location, [orders[0].id], state, False, True)
    cost_c1_o1 = calculate_route_duration(couriers[1].current_location, [orders[1].id], state, False, True)

    print(f"  C0 → O0: {cost_c0_o0:.1f}s")
    print(f"  C0 → O1: {cost_c0_o1:.1f}s")
    print(f"  C1 → O0: {cost_c1_o0:.1f}s")
    print(f"  C1 → O1: {cost_c1_o1:.1f}s")

    total_cost_option_a = cost_c0_o0 + cost_c1_o1
    total_cost_option_b = cost_c0_o1 + cost_c1_o0

    print(f"\nOption A (C0→O0, C1→O1): Total cost = {total_cost_option_a:.1f}s")
    print(f"Option B (C0→O1, C1→O0): Total cost = {total_cost_option_b:.1f}s")

    result = assign_hungarian(state, couriers, orders)

    print(f"\nOutput: {result}")

    assignments_dict = {o_ids[0]: c_id for c_id, o_ids in result}
    actual_total = cost_c0_o0 + cost_c1_o1 if assignments_dict[0] == 0 else cost_c0_o1 + cost_c1_o0

    print(f"\nHungarian chose: Total cost = {actual_total:.1f}s")
    print(f"Hungarian minimizes total system cost (global optimum)")

    assert len(result) == 2
    assert actual_total == min(total_cost_option_a, total_cost_option_b)

    print("PASS")


# ============================================================================
# CATEGORY 5: SYMMETRY AND TIE-BREAKING TESTS
# ============================================================================

def test_5_1_symmetrical_courier_choice():
    """Test 5.1: Symmetrical Courier Choice"""
    print("\n" + "="*80)
    print("TEST 5.1: Symmetrical Courier Choice")
    print("="*80)

    restaurants = [Restaurant(0, (5.0, 5.0))]

    couriers = [
        Courier(0, (5.0, 4.0)),
        Courier(1, (5.0, 6.0))
    ]

    orders = [Order(0, 0, (5.0, 5.0), (6.0, 6.0), 0.0)]
    orders[0].state = "READY"
    orders[0].ready_time = 0.0

    state = SimulationState(restaurants, couriers, orders, duration=3600)

    print("\nSetup: 1 order equidistant from 2 couriers")
    print(f"  Order 0 at (5.0, 5.0)")
    print(f"  Courier 0 at (5.0, 4.0) - distance = 1.0")
    print(f"  Courier 1 at (5.0, 6.0) - distance = 1.0")

    result = assign_hungarian(state, couriers, orders)

    print(f"\nOutput: {result}")

    assert len(result) == 1
    assert result[0][0] in [0, 1]
    assert result[0][1] == [0]

    print(f"Assigned Courier {result[0][0]} (deterministic tie-breaking)")
    print("PASS")


def test_5_2_symmetrical_order_choice():
    """Test 5.2: Symmetrical Order Choice"""
    print("\n" + "="*80)
    print("TEST 5.2: Symmetrical Order Choice")
    print("="*80)

    restaurants = [
        Restaurant(0, (5.0, 4.0)),
        Restaurant(1, (5.0, 6.0))
    ]

    couriers = [Courier(0, (5.0, 5.0))]

    orders = [
        Order(0, 0, (5.0, 4.0), (6.0, 6.0), 0.0, expiration_time=LONG_EXP),
        Order(1, 1, (5.0, 6.0), (7.0, 7.0), 0.0, expiration_time=LONG_EXP)
    ]

    for order in orders:
        order.state = "READY"
        order.ready_time = 0.0

    state = SimulationState(restaurants, couriers, orders, duration=3600)

    print("\nSetup: 1 courier equidistant from 2 orders")
    print(f"  Courier 0 at (5.0, 5.0)")
    print(f"  Order 0 at (5.0, 4.0) - distance = 1.0")
    print(f"  Order 1 at (5.0, 6.0) - distance = 1.0")

    result = assign_hungarian(state, couriers, orders)

    print(f"\nOutput: {result}")

    assert len(result) == 1
    assert result[0][0] == 0
    assert result[0][1][0] in [0, 1]

    print(f"Assigned Order {result[0][1][0]} (deterministic tie-breaking)")
    print("PASS")


# ============================================================================
# CATEGORY 6: ZERO-COST AND CO-LOCATED SCENARIOS
# ============================================================================

def test_6_1_colocated_courier_and_pickup():
    """Test 6.1: Co-Located Courier and Pickup"""
    print("\n" + "="*80)
    print("TEST 6.1: Co-Located Courier and Pickup")
    print("="*80)

    restaurants = [
        Restaurant(0, (5.0, 5.0)),
        Restaurant(1, (10.1, 10.1))
    ]

    couriers = [
        Courier(0, (5.0, 5.0)),
        Courier(1, (10.0, 10.0))
    ]

    orders = [
        Order(0, 0, (5.0, 5.0), (6.0, 6.0), 0.0),
        Order(1, 1, (10.1, 10.1), (11.0, 11.0), 0.0)
    ]

    for order in orders:
        order.state = "READY"
        order.ready_time = 0.0

    state = SimulationState(restaurants, couriers, orders, duration=3600)

    print("\nSetup:")
    print(f"  Courier 0 at (5.0, 5.0) - EXACT match with Order 0")
    print(f"  Courier 1 at (10.0, 10.0) - very close to Order 1")

    print("\nCosts:")
    for i, c in enumerate(couriers):
        for j, o in enumerate(orders):
            cost = calculate_route_duration(c.current_location, [o.id], state, False, True)
            print(f"  C{i} → O{j}: {cost:.1f}s")

    result = assign_hungarian(state, couriers, orders)

    print(f"\nOutput: {result}")

    assignments_dict = {o_ids[0]: c_id for c_id, o_ids in result}

    assert len(result) == 2
    assert assignments_dict[0] == 0
    assert assignments_dict[1] == 1

    print("PASS")


def test_6_2_all_orders_at_one_restaurant():
    """Test 6.2: All Orders at One Restaurant (Pizzeria Pileup)"""
    print("\n" + "="*80)
    print("TEST 6.2: All Orders at One Restaurant")
    print("="*80)

    restaurants = [Restaurant(0, (5.0, 5.0))]

    couriers = [
        Courier(0, (5.1, 5.1)),
        Courier(1, (5.2, 5.2)),
        Courier(2, (6.0, 6.0)),
        Courier(3, (7.0, 7.0)),
        Courier(4, (10.0, 10.0))
    ]

    orders = [
        Order(0, 0, (5.0, 5.0), (1.0, 1.0), 0.0, expiration_time=LONG_EXP),
        Order(1, 0, (5.0, 5.0), (2.0, 2.0), 0.0, expiration_time=LONG_EXP),
        Order(2, 0, (5.0, 5.0), (3.0, 3.0), 0.0, expiration_time=LONG_EXP)
    ]

    for order in orders:
        order.state = "READY"
        order.ready_time = 0.0

    state = SimulationState(restaurants, couriers, orders, duration=3600)

    print(f"\nInput: {len(couriers)} couriers, {len(orders)} orders (all at same restaurant)")
    print("Courier distances to restaurant (5.0, 5.0):")
    courier_distances = []
    for c in couriers:
        dist = euclidean_distance(c.current_location, restaurants[0].location)
        courier_distances.append((c.id, dist))
        print(f"  Courier {c.id}: {dist:.3f} km")

    courier_distances.sort(key=lambda x: x[1])
    closest_three = [c_id for c_id, _ in courier_distances[:3]]

    print(f"\nClosest 3 couriers: {closest_three}")

    result = assign_hungarian(state, couriers, orders)

    print(f"\nOutput: {result}")

    assigned_couriers = sorted([c_id for c_id, _ in result])

    print(f"Assigned couriers: {assigned_couriers}")

    assert len(result) == 3
    assert assigned_couriers == sorted(closest_three)

    print("PASS")


# ============================================================================
# CATEGORY 7: OBJECTIVE FUNCTION ROBUSTNESS
# ============================================================================

def test_7_1_extremely_unattractive_assignment():
    """Test 7.1: The Extremely Unattractive but Necessary Assignment"""
    print("\n" + "="*80)
    print("TEST 7.1: Extremely Unattractive but Necessary Assignment")
    print("="*80)

    restaurants = [Restaurant(0, (100.0, 100.0))]

    couriers = [Courier(0, (0.0, 0.0))]

    orders = [Order(0, 0, (100.0, 100.0), (101.0, 101.0), 0.0, expiration_time=ULTRA_EXP)]
    orders[0].state = "READY"
    orders[0].ready_time = 0.0

    state = SimulationState(restaurants, couriers, orders, duration=36000)

    print("\nSetup:")
    print(f"  Courier 0 at (0.0, 0.0)")
    print(f"  Order 0 at (100.0, 100.0) - VERY far away")

    cost = calculate_route_duration(couriers[0].current_location, [orders[0].id], state, False, True)
    print(f"\nCost: {cost:.1f}s")

    result = assign_hungarian(state, couriers, orders)

    print(f"\nOutput: {result}")

    assert len(result) == 1
    assert result[0] == (0, [0])

    print("Hungarian assigns despite massive cost (fulfillment priority)")
    print("PASS")


def test_7_2_identical_costs_for_all_pairings():
    """Test 7.2: Identical Costs for All Pairings"""
    print("\n" + "="*80)
    print("TEST 7.2: Identical Costs for All Pairings")
    print("="*80)

    restaurants = [
        Restaurant(0, (1.0, 0.0)),
        Restaurant(1, (-1.0, 0.0))
    ]

    couriers = [
        Courier(0, (0.0, 1.0)),
        Courier(1, (0.0, -1.0))
    ]

    orders = [
        Order(0, 0, (1.0, 0.0), (2.0, 2.0), 0.0, expiration_time=LONG_EXP),
        Order(1, 1, (-1.0, 0.0), (-2.0, -2.0), 0.0, expiration_time=LONG_EXP)
    ]

    for order in orders:
        order.state = "READY"
        order.ready_time = 0.0

    state = SimulationState(restaurants, couriers, orders, duration=3600)

    print("\nSetup: Diamond shape - all courier-order distances equal")
    print("Cost matrix:")
    for i, c in enumerate(couriers):
        for j, o in enumerate(orders):
            dist = euclidean_distance(c.current_location, o.restaurant_location)
            print(f"  C{i} → O{j}: {dist:.3f} km")

    result = assign_hungarian(state, couriers, orders)

    print(f"\nOutput: {result}")

    assert len(result) == 2

    assigned_couriers = [c_id for c_id, _ in result]
    assigned_orders = [o_ids[0] for _, o_ids in result]

    assert len(set(assigned_couriers)) == 2
    assert len(set(assigned_orders)) == 2

    print("Hungarian produces valid assignment despite equal costs")
    print("PASS")


# ============================================================================
# CATEGORY 8: COMPLEX CONSTRAINT SCENARIOS
# ============================================================================

def test_8_1_the_sacrificial_order():
    """Test 8.1: The Sacrificial Order"""
    print("\n" + "="*80)
    print("TEST 8.1: The Sacrificial Order")
    print("="*80)

    restaurants = [
        Restaurant(0, (5.1, 5.1)),
        Restaurant(1, (6.1, 6.1)),
        Restaurant(2, (20.0, 20.0))
    ]

    couriers = [
        Courier(0, (5.0, 5.0)),
        Courier(1, (6.0, 6.0))
    ]

    orders = [
        Order(0, 0, (5.1, 5.1), (5.5, 5.5), 0.0),
        Order(1, 1, (6.1, 6.1), (6.5, 6.5), 0.0),
        Order(2, 2, (20.0, 20.0), (21.0, 21.0), 0.0)
    ]

    for order in orders:
        order.state = "READY"
        order.ready_time = 0.0

    state = SimulationState(restaurants, couriers, orders, duration=3600)

    print(f"\nInput: {len(couriers)} couriers, {len(orders)} orders")
    print("Order costs:")
    for o in orders:
        cost_c0 = calculate_route_duration(couriers[0].current_location, [o.id], state, False, True)
        cost_c1 = calculate_route_duration(couriers[1].current_location, [o.id], state, False, True)
        print(f"  Order {o.id}: C0={cost_c0:.1f}s, C1={cost_c1:.1f}s")

    result = assign_hungarian(state, couriers, orders)

    print(f"\nOutput: {result}")

    assignments_dict = {o_ids[0]: c_id for c_id, o_ids in result}
    assigned_orders = sorted(assignments_dict.keys())

    print(f"Assigned orders: {assigned_orders}")

    assert len(result) == 2
    assert 0 in assigned_orders
    assert 1 in assigned_orders
    assert 2 not in assigned_orders

    print("Order 2 correctly left unassigned (sacrificial)")
    print("PASS")


# ============================================================================
# CATEGORY 9: DEADLINE FEASIBILITY & MANHATTAN TIE-BREAK TESTS
# ============================================================================

from simulator_core import PICKUP_SERVICE_TIME, DROPOFF_SERVICE_TIME

def test_9_1_deadline_infeasible_skip():
    """Test 9.1: Skip assignment when finish time exceeds deadline."""
    print("\n" + "="*80)
    print("TEST 9.1: Deadline Infeasible → Skip")
    print("="*80)

    restaurants = [Restaurant(0, (2.0, 2.0))]
    couriers = [Courier(0, (2.0, 2.0))]
    # Expiration = 1s while service alone is 150+120=270s
    orders = [Order(0, 0, (2.0, 2.0), (2.0, 2.0), placement_time=0.0, expiration_time=1.0)]
    orders[0].state = "READY"
    orders[0].ready_time = 0.0

    state = SimulationState(restaurants, couriers, orders, duration=3600)
    result = assign_hungarian(state, couriers, orders)
    print(f"Output: {result}")

    assert result == []
    print("PASS")


def test_9_2_deadline_edge_equal_ok():
    """Test 9.2: Assign when finish time exactly equals deadline."""
    print("\n" + "="*80)
    print("TEST 9.2: Deadline Edge Case (finish == deadline) → Assign")
    print("="*80)

    restaurants = [Restaurant(0, (2.0, 2.0))]
    couriers = [Courier(0, (2.0, 2.0))]

    # Zero travel times; finish = pickup(150) + dropoff(120) = 270s
    orders = [Order(
        0, 0, (2.0, 2.0), (2.0, 2.0), placement_time=0.0,
        expiration_time=PICKUP_SERVICE_TIME + DROPOFF_SERVICE_TIME
    )]
    orders[0].state = "READY"
    orders[0].ready_time = 0.0

    state = SimulationState(restaurants, couriers, orders, duration=3600)
    result = assign_hungarian(state, couriers, orders)
    print(f"Output: {result}")

    assert len(result) == 1 and result[0] == (0, [0])
    print("PASS")


def test_9_3_manhattan_overrides_euclidean():
    """Test 9.3: Manhattan tie-break chooses different courier than Euclidean."""
    print("\n" + "="*80)
    print("TEST 9.3: Manhattan Overrides Euclidean")
    print("="*80)

    restaurants = [Restaurant(0, (0.0, 0.0))]
    # C0: Manhattan=1.0, C1: Manhattan=1.2 (but Euclidean≈0.8485)
    couriers = [Courier(0, (1.0, 0.0)), Courier(1, (0.6, 0.6))]
    orders = [Order(0, 0, (0.0, 0.0), (0.0, 0.0), placement_time=0.0, expiration_time=10_000.0)]
    orders[0].state = "READY"
    orders[0].ready_time = 0.0

    state = SimulationState(restaurants, couriers, orders, duration=3600)
    result = assign_hungarian(state, couriers, orders)
    print(f"Output: {result}")

    assert len(result) == 1 and result[0][0] == 0 and result[0][1] == [0]
    print("PASS")


def test_9_4_manhattan_tie_lowest_id():
    """Test 9.4: When Manhattan times tie, choose lowest courier ID."""
    print("\n" + "="*80)
    print("TEST 9.4: Manhattan Tie → Lowest Courier ID")
    print("="*80)

    restaurants = [Restaurant(0, (2.0, 2.0))]
    # Both have Manhattan distance 1.0
    couriers = [Courier(0, (2.0, 3.0)), Courier(1, (3.0, 2.0))]
    orders = [Order(0, 0, (2.0, 2.0), (2.0, 2.0), placement_time=0.0, expiration_time=10_000.0)]
    orders[0].state = "READY"
    orders[0].ready_time = 0.0

    state = SimulationState(restaurants, couriers, orders, duration=3600)
    result = assign_hungarian(state, couriers, orders)
    print(f"Output: {result}")

    assert len(result) == 1 and result[0][0] == 0 and result[0][1] == [0]
    print("PASS")


def test_9_5_cardinality_then_pickup_time():
    """Test 9.5: Maximize cardinality first, then minimize pickup time."""
    print("\n" + "="*80)
    print("TEST 9.5: Cardinality First, Then Pickup Time")
    print("="*80)

    restaurants = [Restaurant(0, (1.0, 1.0)), Restaurant(1, (5.0, 5.0))]
    # Two couriers, two orders - should assign both
    couriers = [Courier(0, (1.0, 1.0)), Courier(1, (5.0, 5.0))]
    orders = [
        Order(0, 0, (1.0, 1.0), (2.0, 2.0), 0.0, expiration_time=10_000.0),
        Order(1, 1, (5.0, 5.0), (6.0, 6.0), 0.0, expiration_time=10_000.0)
    ]
    for o in orders:
        o.state = "READY"
        o.ready_time = 0.0

    state = SimulationState(restaurants, couriers, orders, duration=3600)
    result = assign_hungarian(state, couriers, orders)
    print(f"Output: {result}")

    assert len(result) == 2
    assigned_couriers = {c_id for c_id, _ in result}
    assigned_orders = {o_ids[0] for _, o_ids in result}
    assert assigned_couriers == {0, 1}
    assert assigned_orders == {0, 1}
    print("PASS")


# ============================================================================
# TEST RUNNER
# ============================================================================

def run_all_tests():
    """Run all test cases"""

    tests = [
        test_1_1_criss_cross_scenario,
        test_2_1_courier_surplus,
        test_2_2_order_surplus,
        test_3_1_no_couriers_many_orders,
        test_3_2_many_couriers_no_orders,
        test_3_3_one_courier_one_order,
        test_4_1_strategic_dilemma,
        test_5_1_symmetrical_courier_choice,
        test_5_2_symmetrical_order_choice,
        test_6_1_colocated_courier_and_pickup,
        test_6_2_all_orders_at_one_restaurant,
        test_7_1_extremely_unattractive_assignment,
        test_7_2_identical_costs_for_all_pairings,
        test_8_1_the_sacrificial_order,
        test_9_1_deadline_infeasible_skip,
        test_9_2_deadline_edge_equal_ok,
        test_9_3_manhattan_overrides_euclidean,
        test_9_4_manhattan_tie_lowest_id,
        test_9_5_cardinality_then_pickup_time
    ]

    passed = 0
    failed = 0
    errors = []

    print("="*80)
    print("HUNGARIAN ALGORITHM TEST SUITE")
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
