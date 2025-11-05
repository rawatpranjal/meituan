"""
UNIFIED TEST SUITE FOR ANTICIPATED BUNDLING (CP-SAT MAXIMIZATION)

This suite validates the CP-SAT maximization framework where:
    Score = (n × 1M) - EffectiveCost
    EffectiveCost = RouteDuration + α·T_wait + β·T_delay - γ·StalenessBonus

KEY PRINCIPLE: THROUGHPUT FIRST, EFFICIENCY SECOND
- A 3-order bundle almost always beats a 2-order bundle
- A 2-order bundle almost always beats a 1-order assignment
- Temporal penalties influence choices WITHIN the same bundle size
- PRIORITY_MULTIPLIER = 1,000,000 dominates most cost differences

TEST CATEGORIES:
1. Temporal Edge Cases (lookahead boundaries)
2. Throughput Prioritization (bundle formation driven by n × 1M)
3. Temporal Penalties Within Bundle Size (penalties matter when comparing same n)
4. Pathological Geometry (route efficiency still matters)
5. Bundling Antipatterns (when NOT to bundle)
6. Lookahead Blindness (window enforcement)
7. Cost Function Balance (distance vs penalties)
8. Starvation & Fairness (FIFO with staleness bonus)
9. Scale & Thresholds (combinatorial vs geographic)
10. Invariants (no duplicates, constraints enforced)
11. Anticipatory Horizon (proactive dispatch)
"""

import sys
import os
import datetime
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from simulator_core import (
    SimulationState, Courier, Order, Restaurant,
    euclidean_distance, get_travel_time
)
from assignment_algorithms import assign_anticipated_bundling

# Algorithm parameters
LOOKAHEAD_WINDOW = 300
ALPHA_PENALTY = 0.5
BETA_PENALTY = 0.3
STALENESS_BONUS = 0.4
MAX_STALENESS_BONUS = 140

test_results = []

def record_test(test_name, passed, expected, actual, reason=""):
    test_results.append({
        'name': test_name,
        'passed': passed,
        'expected': expected,
        'actual': actual,
        'reason': reason
    })

def print_test_header(category, test_num, test_name):
    print("\n" + "="*80)
    print(f"{category} - TEST {test_num}: {test_name}")
    print("="*80)


# ============================================================================
# CATEGORY 1: TEMPORAL EDGE CASES
# ============================================================================

def test_1_1_order_at_exact_lookahead_boundary():
    print_test_header("TEMPORAL EDGE CASES", "1.1", "Order at exact lookahead boundary (t=300s)")

    restaurants = [Restaurant(0, (1.0, 0.0))]
    couriers = [Courier(0, (0.0, 0.0))]
    orders = [Order(0, 0, (1.0, 0.0), (2.0, 0.0), 0.0)]
    orders[0].ready_time = 300.0
    orders[0].state = "PENDING"

    state = SimulationState(restaurants, couriers, orders, duration=3600)
    state.current_time = 0.0

    print(f"\nScenario: Order ready at t=300s (EXACTLY at boundary)")
    print(f"Condition: ready_time <= current_time + LOOKAHEAD_WINDOW")
    print(f"Evaluation: 300 <= 0 + 300 → TRUE")

    assignments = assign_anticipated_bundling(state, couriers, orders)

    expected = "C0 assigned to O0 (order at boundary included)"
    actual = f"Assignments: {assignments}"
    passed = len(assignments) == 1 and assignments[0][0] == 0 and 0 in assignments[0][1]

    print(f"\nExpected: {expected}")
    print(f"Actual: {actual}")
    print(f"Result: {'PASS' if passed else 'FAIL'}")

    record_test("1.1_exact_boundary", passed, expected, actual)
    return passed


def test_1_2_order_one_second_before_boundary():
    print_test_header("TEMPORAL EDGE CASES", "1.2", "Order 1 second before boundary (t=299s)")

    restaurants = [Restaurant(0, (1.0, 0.0))]
    couriers = [Courier(0, (0.0, 0.0))]
    orders = [Order(0, 0, (1.0, 0.0), (2.0, 0.0), 0.0)]
    orders[0].ready_time = 299.0
    orders[0].state = "PENDING"

    state = SimulationState(restaurants, couriers, orders, duration=3600)
    state.current_time = 0.0

    assignments = assign_anticipated_bundling(state, couriers, orders)

    expected = "C0 assigned to O0"
    actual = f"Assignments: {assignments}"
    passed = len(assignments) == 1

    print(f"\nExpected: {expected}")
    print(f"Actual: {actual}")
    print(f"Result: {'PASS' if passed else 'FAIL'}")

    record_test("1.2_before_boundary", passed, expected, actual)
    return passed


def test_1_3_order_one_second_after_boundary():
    print_test_header("TEMPORAL EDGE CASES", "1.3", "Order 1 second after boundary (t=301s)")

    restaurants = [Restaurant(0, (1.0, 0.0))]
    couriers = [Courier(0, (0.0, 0.0))]
    orders = [Order(0, 0, (1.0, 0.0), (2.0, 0.0), 0.0)]
    orders[0].ready_time = 301.0
    orders[0].state = "PENDING"

    state = SimulationState(restaurants, couriers, orders, duration=3600)
    state.current_time = 0.0

    print(f"\nScenario: Order ready at t=301s (outside lookahead [0, 300])")
    print(f"Expected: No assignment (order invisible)")

    assignments = assign_anticipated_bundling(state, couriers, orders)

    expected = "No assignments (order outside lookahead)"
    actual = f"Assignments: {assignments}"
    passed = len(assignments) == 0

    print(f"\nExpected: {expected}")
    print(f"Actual: {actual}")
    print(f"Result: {'PASS' if passed else 'FAIL'}")

    record_test("1.3_after_boundary", passed, expected, actual)
    return passed


def test_1_4_throughput_beats_perfect_sync():
    """
    FIXED: Throughput prioritization means bundle formed despite penalties
    """
    print_test_header("TEMPORAL EDGE CASES", "1.4", "Throughput beats perfect sync (MAXIMIZATION)")

    restaurants = [
        Restaurant(0, (1.0, 0.0)),
        Restaurant(1, (1.1, 0.0))
    ]
    couriers = [Courier(0, (0.0, 0.0))]

    order_a = Order(0, 0, (1.0, 0.0), (2.0, 0.0), 0.0)
    order_a.ready_time = 180.0
    order_a.state = "PENDING"

    order_b = Order(1, 1, (1.1, 0.0), (2.1, 0.0), 0.0)
    order_b.ready_time = 120.0
    order_b.state = "PENDING"

    orders = [order_a, order_b]

    state = SimulationState(restaurants, couriers, orders, duration=3600)
    state.current_time = 0.0

    travel_time = get_travel_time((0.0, 0.0), (1.0, 0.0))
    print(f"\nScenario:")
    print(f"  Courier travel time: {travel_time:.1f}s")
    print(f"  Order A ready: 180s → T_wait = 0s (perfect sync)")
    print(f"  Order B ready: 120s → T_wait = 60s (courier waits)")
    print(f"  Bundle score: (2 × 1M) - cost > Single score: (1 × 1M) - cost")
    print(f"  Expected: Bundle [A,B] formed (throughput prioritization)")

    assignments = assign_anticipated_bundling(state, couriers, orders)

    expected = "C0 assigned to [O0, O1] bundle (throughput beats zero-penalty single)"
    actual = f"Assignments: {assignments}"

    passed = False
    if assignments:
        assigned_ids = assignments[0][1]
        passed = 0 in assigned_ids and 1 in assigned_ids

    print(f"\nExpected: {expected}")
    print(f"Actual: {actual}")
    print(f"Result: {'PASS' if passed else 'FAIL'}")

    if not passed:
        print(f"\nFAILURE REASON: Throughput prioritization should form bundle")

    record_test("1.4_throughput_beats_sync", passed, expected, actual)
    return passed


# ============================================================================
# CATEGORY 2: THROUGHPUT PRIORITIZATION
# ============================================================================

def test_2_1_throughput_beats_massive_delay():
    """
    FIXED: Bundle formed despite massive T_delay penalty
    """
    print_test_header("THROUGHPUT PRIORITIZATION", "2.1", "Throughput beats massive T_delay")

    restaurants = [Restaurant(0, (5.0, 0.0))]
    couriers = [Courier(0, (0.0, 0.0))]

    order_a = Order(0, 0, (5.0, 0.0), (6.0, 0.0), 0.0)
    order_a.ready_time = 500.0
    order_a.state = "PENDING"

    order_b = Order(1, 0, (5.0, 0.0), (6.0, 0.1), -500.0)
    order_b.ready_time = 0.1
    order_b.state = "READY"

    orders = [order_a, order_b]

    state = SimulationState(restaurants, couriers, orders, duration=3600)
    state.current_time = 500.0

    travel_time = get_travel_time((0.0, 0.0), (5.0, 0.0))

    print(f"\nScenario at t=500s:")
    print(f"  Order A: Ready at t=500s (fresh, zero penalties)")
    print(f"  Order B: Ready at t=0.1s (stale for 499.9s)")
    print(f"  Bundle T_delay penalty: {BETA_PENALTY * 499.9:.1f}s")
    print(f"  But throughput bonus: 2M - 1M = 1,000,000")
    print(f"  Bundle wins despite massive delay penalty")

    assignments = assign_anticipated_bundling(state, couriers, orders)

    expected = "C0 assigned to [O0, O1] bundle (throughput beats massive T_delay)"
    actual = f"Assignments: {assignments}"

    passed = False
    if assignments:
        assigned_ids = assignments[0][1]
        passed = 0 in assigned_ids and 1 in assigned_ids

    print(f"\nExpected: {expected}")
    print(f"Actual: {actual}")
    print(f"Result: {'PASS' if passed else 'FAIL'}")

    if not passed:
        print(f"\nFAILURE REASON: Throughput prioritization should overcome delay penalty")

    record_test("2.1_throughput_beats_delay", passed, expected, actual)
    return passed


def test_2_2_throughput_prioritizes_bundle():
    """
    FIXED: With one courier, bundle is chosen over single despite delay penalty
    """
    print_test_header("THROUGHPUT PRIORITIZATION", "2.2", "Throughput prioritizes bundle")

    restaurants = [Restaurant(0, (1.0, 0.0))]
    couriers = [Courier(0, (0.0, 0.0))]  # Only ONE courier

    order_a = Order(0, 0, (1.0, 0.0), (2.0, 0.0), 0.0)
    order_a.ready_time = 0.0
    order_a.state = "READY"

    order_b = Order(1, 0, (1.0, 0.0), (2.0, 0.1), 0.0)
    order_b.ready_time = 200.0
    order_b.state = "PENDING"

    orders = [order_a, order_b]

    state = SimulationState(restaurants, couriers, orders, duration=3600)
    state.current_time = 0.0

    print(f"\nScenario:")
    print(f"  1 courier, 2 orders at same restaurant")
    print(f"  Order A ready now, Order B ready in 200s")
    print(f"  Bundle T_delay for A: {BETA_PENALTY*200:.1f}s")
    print(f"  Bundle score: 2M - (route + 60) > Single score: 1M - route")
    print(f"  Expected: Bundle [A,B] formed")

    assignments = assign_anticipated_bundling(state, couriers, orders)

    expected = "One 2-order bundle assignment"
    actual = f"Assignments: {assignments}"
    passed = False
    if assignments and len(assignments) == 1:
        passed = len(assignments[0][1]) == 2

    print(f"\nExpected: {expected}")
    print(f"Actual: {actual}")
    print(f"Result: {'PASS' if passed else 'FAIL'}")

    if not passed:
        print(f"\nFAILURE REASON: Throughput prioritization should form bundle")

    record_test("2.2_bundle_prioritized", passed, expected, actual)
    return passed


def test_2_3_all_options_have_high_cost():
    print_test_header("THROUGHPUT PRIORITIZATION", "2.3", "All options have high cost")

    restaurants = [Restaurant(0, (50.0, 0.0))]
    couriers = [Courier(0, (0.0, 0.0))]

    order = Order(0, 0, (50.0, 0.0), (51.0, 0.0), 0.0)
    order.ready_time = 0.0
    order.state = "READY"

    orders = [order]

    state = SimulationState(restaurants, couriers, orders, duration=3600)
    state.current_time = 300.0

    travel_time = get_travel_time((0.0, 0.0), (50.0, 0.0))

    print(f"\nScenario:")
    print(f"  Very far restaurant (50km)")
    print(f"  Travel time: {travel_time:.1f}s")
    print(f"  T_delay: {BETA_PENALTY*300:.1f}s")
    print(f"  All options terrible, but must still assign")

    assignments = assign_anticipated_bundling(state, couriers, orders)

    expected = "C0 assigned to O0 (least-bad choice)"
    actual = f"Assignments: {assignments}"
    passed = len(assignments) == 1

    print(f"\nExpected: {expected}")
    print(f"Actual: {actual}")
    print(f"Result: {'PASS' if passed else 'FAIL'}")

    record_test("2.3_all_high_cost", passed, expected, actual)
    return passed


# ============================================================================
# CATEGORY 3: PATHOLOGICAL GEOMETRY
# ============================================================================

def test_3_1_opposite_ends_courier_middle():
    print_test_header("PATHOLOGICAL GEOMETRY", "3.1", "Orders at opposite ends")

    restaurants = [
        Restaurant(0, (0.0, 0.0)),
        Restaurant(1, (100.0, 0.0))
    ]
    couriers = [
        Courier(0, (50.0, 0.0)),
        Courier(1, (50.0, 0.1))
    ]

    order_a = Order(0, 0, (0.0, 0.0), (0.0, 1.0), 0.0)
    order_a.ready_time = 10.0
    order_a.state = "READY"

    order_b = Order(1, 1, (100.0, 0.0), (100.0, 1.0), 0.0)
    order_b.ready_time = 10.0
    order_b.state = "READY"

    orders = [order_a, order_b]

    state = SimulationState(restaurants, couriers, orders, duration=3600)
    state.current_time = 10.0

    print(f"\nScenario:")
    print(f"  Orders at opposite ends (0km and 100km)")
    print(f"  Courier at middle (50km)")
    print(f"  Bundle route: 50 + 100 = 150km (massive backtracking)")
    print(f"  Separate: 2 × 50 = 100km (parallel)")
    print(f"  With 2 couriers, separate is better")

    assignments = assign_anticipated_bundling(state, couriers, orders)

    expected = "Two separate assignments"
    actual = f"Assignments: {assignments}"

    bundled = False
    if assignments:
        for _, order_ids in assignments:
            if len(order_ids) > 1:
                bundled = True

    passed = not bundled and len(assignments) == 2

    print(f"\nExpected: {expected}")
    print(f"Actual: {actual}")
    print(f"Result: {'PASS' if passed else 'FAIL'}")

    record_test("3.1_opposite_ends", passed, expected, actual)
    return passed


def test_3_2_throughput_beats_high_route_cost():
    """
    FIXED: With one courier, bundle formed despite divergent deliveries
    """
    print_test_header("PATHOLOGICAL GEOMETRY", "3.2", "Throughput beats high route cost")

    restaurants = [
        Restaurant(0, (10.0, 0.0)),
        Restaurant(1, (10.1, 0.0))
    ]
    couriers = [Courier(0, (9.0, 0.0))]  # Only ONE courier

    order_a = Order(0, 0, (10.0, 0.0), (10.0, 50.0), 0.0)
    order_a.ready_time = 10.0
    order_a.state = "READY"

    order_b = Order(1, 1, (10.1, 0.0), (10.0, -50.0), 0.0)
    order_b.ready_time = 10.0
    order_b.state = "READY"

    orders = [order_a, order_b]

    state = SimulationState(restaurants, couriers, orders, duration=3600)
    state.current_time = 10.0

    print(f"\nScenario:")
    print(f"  Restaurants close (100m apart)")
    print(f"  Deliveries 100km apart (50km north, 50km south)")
    print(f"  Bundle route: massive detour (150km+)")
    print(f"  But with 1 courier: Bundle score (2M - huge_cost) > Single score (1M - cost)")
    print(f"  Throughput prioritization forms bundle despite inefficiency")

    assignments = assign_anticipated_bundling(state, couriers, orders)

    expected = "Bundle [O0, O1] formed (throughput beats route cost)"
    actual = f"Assignments: {assignments}"

    passed = False
    if assignments:
        assigned_ids = assignments[0][1]
        passed = 0 in assigned_ids and 1 in assigned_ids

    print(f"\nExpected: {expected}")
    print(f"Actual: {actual}")
    print(f"Result: {'PASS' if passed else 'FAIL'}")

    if not passed:
        print(f"\nFAILURE REASON: Throughput prioritization should form bundle")

    record_test("3.2_throughput_beats_route_cost", passed, expected, actual)
    return passed


# ============================================================================
# CATEGORY 4: BUNDLING ANTIPATTERNS
# ============================================================================

def test_4_1_same_restaurant_staggered_ready_times():
    print_test_header("BUNDLING ANTIPATTERNS", "4.1", "Staggered ready times")

    restaurants = [Restaurant(0, (5.0, 0.0))]
    couriers = [
        Courier(0, (4.0, 0.0)),
        Courier(1, (4.0, 0.1)),
        Courier(2, (4.0, 0.2))
    ]

    order_0 = Order(0, 0, (5.0, 0.0), (6.0, 0.0), 0.0)
    order_0.ready_time = 0.0
    order_0.state = "READY"

    order_1 = Order(1, 0, (5.0, 0.0), (6.0, 0.1), 0.0)
    order_1.ready_time = 600.0
    order_1.state = "PENDING"

    order_2 = Order(2, 0, (5.0, 0.0), (6.0, 0.2), 0.0)
    order_2.ready_time = 1200.0
    order_2.state = "PENDING"

    orders = [order_0, order_1, order_2]

    state = SimulationState(restaurants, couriers, orders, duration=3600)
    state.current_time = 0.0

    print(f"\nScenario:")
    print(f"  3 orders, 3 couriers")
    print(f"  Orders ready at t=0, 600, 1200")
    print(f"  3-bundle T_delay: {BETA_PENALTY*(1200+600):.1f}s")
    print(f"  With multiple couriers available, at least serve O0")

    assignments = assign_anticipated_bundling(state, couriers, orders)

    expected = "At least one assignment (O0 served)"
    actual = f"Assignments: {assignments}"
    passed = len(assignments) >= 1

    print(f"\nExpected: {expected}")
    print(f"Actual: {actual}")
    print(f"Result: {'PASS' if passed else 'FAIL'}")

    record_test("4.1_staggered_times", passed, expected, actual)
    return passed


def test_4_2_multi_restaurant_backtracking():
    print_test_header("BUNDLING ANTIPATTERNS", "4.2", "Multi-restaurant backtracking")

    restaurants = [
        Restaurant(0, (1.0, 0.0)),
        Restaurant(1, (0.0, 1.0))
    ]
    couriers = [
        Courier(0, (0.0, 0.0)),
        Courier(1, (0.0, 0.05))
    ]

    order_a = Order(0, 0, (1.0, 0.0), (2.0, 0.0), 0.0)
    order_a.ready_time = 10.0
    order_a.state = "READY"

    order_b = Order(1, 1, (0.0, 1.0), (0.0, 2.0), 0.0)
    order_b.ready_time = 10.0
    order_b.state = "READY"

    orders = [order_a, order_b]

    state = SimulationState(restaurants, couriers, orders, duration=3600)
    state.current_time = 10.0

    print(f"\nScenario:")
    print(f"  2 couriers, 2 orders at different restaurants")
    print(f"  Separate assignments more efficient")

    assignments = assign_anticipated_bundling(state, couriers, orders)

    expected = "Two separate assignments"
    actual = f"Assignments: {assignments}"

    bundled = False
    if assignments:
        for _, order_ids in assignments:
            if len(order_ids) > 1:
                bundled = True

    passed = not bundled

    print(f"\nExpected: {expected}")
    print(f"Actual: {actual}")
    print(f"Result: {'PASS' if passed else 'FAIL'}")

    record_test("4.2_backtracking", passed, expected, actual)
    return passed


# ============================================================================
# CATEGORY 5: LOOKAHEAD BLINDNESS
# ============================================================================

def test_5_1_perfect_order_outside_window():
    print_test_header("LOOKAHEAD BLINDNESS", "5.1", "Perfect order outside window")

    restaurants = [
        Restaurant(0, (5.0, 0.0)),
        Restaurant(1, (1.0, 0.0))
    ]
    couriers = [Courier(0, (0.0, 0.0))]

    order_mediocre = Order(0, 0, (5.0, 0.0), (6.0, 0.0), 0.0)
    order_mediocre.ready_time = 299.0
    order_mediocre.state = "PENDING"

    order_perfect = Order(1, 1, (1.0, 0.0), (2.0, 0.0), 0.0)
    order_perfect.ready_time = 301.0
    order_perfect.state = "PENDING"

    orders = [order_mediocre, order_perfect]

    state = SimulationState(restaurants, couriers, orders, duration=3600)
    state.current_time = 0.0

    print(f"\nScenario:")
    print(f"  Order 0: 5km away, ready at t=299s (inside window)")
    print(f"  Order 1: 1km away, ready at t=301s (outside window)")
    print(f"  Expected: Assign Order 0 (only visible)")

    assignments = assign_anticipated_bundling(state, couriers, orders)

    expected = "C0 assigned to Order 0"
    actual = f"Assignments: {assignments}"

    passed = False
    if assignments:
        assigned_ids = assignments[0][1]
        passed = 0 in assigned_ids and 1 not in assigned_ids

    print(f"\nExpected: {expected}")
    print(f"Actual: {actual}")
    print(f"Result: {'PASS' if passed else 'FAIL'}")

    record_test("5.1_outside_window", passed, expected, actual)
    return passed


# ============================================================================
# CATEGORY 6: COST FUNCTION BALANCE
# ============================================================================

def test_6_1_short_distance_high_penalties_vs_long_distance_zero_penalties():
    print_test_header("COST FUNCTION", "6.1", "Short+penalties vs Long+zero penalties")

    restaurants = [
        Restaurant(0, (1.0, 0.0)),
        Restaurant(1, (5.0, 0.0))
    ]
    couriers = [Courier(0, (0.0, 0.0))]

    order_a = Order(0, 0, (1.0, 0.0), (1.5, 0.0), 0.0)
    order_a.ready_time = 0.0
    order_a.state = "READY"

    travel_to_b = get_travel_time((0.0, 0.0), (5.0, 0.0))
    order_b = Order(1, 1, (5.0, 0.0), (5.5, 0.0), 0.0)
    order_b.ready_time = travel_to_b
    order_b.state = "PENDING"

    orders = [order_a, order_b]

    state = SimulationState(restaurants, couriers, orders, duration=3600)
    state.current_time = 200.0

    travel_a = get_travel_time((0.0, 0.0), (1.0, 0.0))
    travel_b = get_travel_time((0.0, 0.0), (5.0, 0.0))

    delay_a = 200.0
    penalty_a = BETA_PENALTY * delay_a

    print(f"\nScenario at t=200s:")
    print(f"  Route A: {travel_a:.1f}s, T_delay: {delay_a:.1f}s, Penalty: {penalty_a:.1f}s")
    print(f"  Route B: {travel_b:.1f}s, T_delay: 0s, Penalty: 0s")
    print(f"  Effective cost A: {travel_a+penalty_a:.1f}s")
    print(f"  Effective cost B: {travel_b:.1f}s")

    if travel_a + penalty_a < travel_b:
        expected = "Route A (shorter effective cost)"
        expected_order = 0
    else:
        expected = "Route B (longer but zero penalties)"
        expected_order = 1

    assignments = assign_anticipated_bundling(state, couriers, orders)

    actual = f"Assignments: {assignments}"

    passed = False
    if assignments:
        assigned_ids = assignments[0][1]
        passed = expected_order in assigned_ids

    print(f"\nExpected: {expected}")
    print(f"Actual: {actual}")
    print(f"Result: {'PASS' if passed else 'FAIL'}")

    record_test("6.1_distance_vs_penalties", passed, expected, actual)
    return passed


# ============================================================================
# CATEGORY 7: STARVATION & FAIRNESS
# ============================================================================

def test_7_1_old_ready_order_vs_fresh_ready_order():
    print_test_header("STARVATION & FAIRNESS", "7.1", "Old READY vs Fresh READY")

    restaurants = [Restaurant(0, (5.0, 0.0))]
    couriers = [Courier(0, (4.0, 0.0))]

    order_old = Order(0, 0, (5.0, 0.0), (6.0, 0.0), 0.0)
    order_old.ready_time = 0.0
    order_old.state = "READY"

    order_fresh = Order(1, 0, (5.0, 0.0), (6.0, 0.1), 0.0)
    order_fresh.ready_time = 350.0
    order_fresh.state = "READY"

    orders = [order_old, order_fresh]

    state = SimulationState(restaurants, couriers, orders, duration=3600)
    state.current_time = 400.0

    print(f"\nScenario at t=400s:")
    print(f"  Order 0: Waiting 400s → staleness bonus ≈ 140s (capped)")
    print(f"  Order 1: Waiting 50s → staleness bonus ≈ 20s")
    print(f"  Both eligible for bundle due to throughput priority")
    print(f"  Acceptable: Single Order 0 OR Bundle [0,1]")

    assignments = assign_anticipated_bundling(state, couriers, orders)

    expected = "Order 0 included (older order)"
    actual = f"Assignments: {assignments}"

    passed = False
    if assignments:
        assigned_ids = assignments[0][1]
        passed = 0 in assigned_ids  # Order 0 must be included

    print(f"\nExpected: {expected}")
    print(f"Actual: {actual}")
    print(f"Result: {'PASS' if passed else 'FAIL'}")

    record_test("7.1_fifo_fairness", passed, expected, actual)
    return passed


# ============================================================================
# CATEGORY 8: SCALE & THRESHOLDS
# ============================================================================

def test_8_1_exactly_17_orders_combinatorial_mode():
    print_test_header("SCALE & THRESHOLDS", "8.1", "Exactly 17 orders (threshold)")

    restaurants = [Restaurant(i, (i*0.5, 0.0)) for i in range(17)]
    couriers = [Courier(i, (i*0.5, 0.1)) for i in range(17)]

    orders = []
    for i in range(17):
        order = Order(i, i, (i*0.5, 0.0), (i*0.5, 1.0), 0.0)
        order.ready_time = 10.0
        order.state = "READY"
        orders.append(order)

    state = SimulationState(restaurants, couriers, orders, duration=3600)
    state.current_time = 10.0

    print(f"\nScenario: 17 orders (at threshold)")
    print(f"Expected: Combinatorial mode, assignments made")

    try:
        assignments = assign_anticipated_bundling(state, couriers, orders)
        passed = len(assignments) > 0
        actual = f"{len(assignments)} assignments made"
    except Exception as e:
        passed = False
        actual = f"ERROR: {str(e)}"

    expected = "Assignments made successfully"

    print(f"\nExpected: {expected}")
    print(f"Actual: {actual}")
    print(f"Result: {'PASS' if passed else 'FAIL'}")

    record_test("8.1_threshold_17", passed, expected, actual)
    return passed


def test_8_2_exactly_18_orders_geographic_mode():
    print_test_header("SCALE & THRESHOLDS", "8.2", "Exactly 18 orders (geographic mode)")

    restaurants = [Restaurant(i, (i*0.5, 0.0)) for i in range(18)]
    couriers = [Courier(i, (i*0.5, 0.1)) for i in range(18)]

    orders = []
    for i in range(18):
        order = Order(i, i, (i*0.5, 0.0), (i*0.5, 1.0), 0.0)
        order.ready_time = 10.0
        order.state = "READY"
        orders.append(order)

    state = SimulationState(restaurants, couriers, orders, duration=3600)
    state.current_time = 10.0

    print(f"\nScenario: 18 orders (above threshold)")
    print(f"Expected: Geographic clustering mode, assignments made")

    try:
        assignments = assign_anticipated_bundling(state, couriers, orders)
        passed = len(assignments) > 0
        actual = f"{len(assignments)} assignments made"
    except Exception as e:
        passed = False
        actual = f"ERROR: {str(e)}"

    expected = "Assignments made successfully"

    print(f"\nExpected: {expected}")
    print(f"Actual: {actual}")
    print(f"Result: {'PASS' if passed else 'FAIL'}")

    record_test("8.2_geographic_mode_18", passed, expected, actual)
    return passed


# ============================================================================
# CATEGORY 9: INVARIANTS
# ============================================================================

def test_9_1_no_duplicate_order_assignments():
    print_test_header("INVARIANTS", "9.1", "No duplicate order assignments")

    restaurants = [Restaurant(0, (5.0, 0.0))]
    couriers = [
        Courier(0, (4.0, 0.0)),
        Courier(1, (4.1, 0.0)),
        Courier(2, (4.2, 0.0))
    ]

    order = Order(0, 0, (5.0, 0.0), (6.0, 0.0), 0.0)
    order.ready_time = 10.0
    order.state = "READY"

    orders = [order]

    state = SimulationState(restaurants, couriers, orders, duration=3600)
    state.current_time = 10.0

    assignments = assign_anticipated_bundling(state, couriers, orders)

    assigned_order_ids = []
    for _, order_ids in assignments:
        assigned_order_ids.extend(order_ids)

    duplicates = len(assigned_order_ids) != len(set(assigned_order_ids))

    expected = "Order 0 assigned to exactly one courier"
    actual = f"Assignments: {assignments}"
    passed = not duplicates and len(assignments) <= 1

    print(f"\nExpected: {expected}")
    print(f"Actual: {actual}")
    print(f"Result: {'PASS' if passed else 'FAIL'}")

    if not passed:
        print(f"\nCRITICAL INVARIANT VIOLATION")

    record_test("9.1_no_duplicates", passed, expected, actual)
    return passed


def test_9_2_one_bundle_per_courier():
    print_test_header("INVARIANTS", "9.2", "One bundle per courier")

    restaurants = [Restaurant(0, (5.0, 0.0))]
    couriers = [Courier(0, (4.0, 0.0))]

    order_a = Order(0, 0, (5.0, 0.0), (6.0, 0.0), 0.0)
    order_a.ready_time = 10.0
    order_a.state = "READY"

    order_b = Order(1, 0, (5.0, 0.0), (6.0, 0.1), 0.0)
    order_b.ready_time = 10.0
    order_b.state = "READY"

    orders = [order_a, order_b]

    state = SimulationState(restaurants, couriers, orders, duration=3600)
    state.current_time = 10.0

    assignments = assign_anticipated_bundling(state, couriers, orders)

    courier_0_count = sum(1 for cid, _ in assignments if cid == 0)

    expected = "Courier 0 appears once"
    actual = f"Assignments: {assignments}, Courier 0 appears {courier_0_count} times"
    passed = courier_0_count <= 1

    print(f"\nExpected: {expected}")
    print(f"Actual: {actual}")
    print(f"Result: {'PASS' if passed else 'FAIL'}")

    if not passed:
        print(f"\nCRITICAL INVARIANT VIOLATION")

    record_test("9.2_one_bundle_per_courier", passed, expected, actual)
    return passed


def test_9_3_skip_already_assigned_orders():
    print_test_header("INVARIANTS", "9.3", "Skip already-assigned orders")

    restaurants = [Restaurant(0, (5.0, 0.0))]
    couriers = [
        Courier(0, (4.0, 0.0)),
        Courier(1, (4.1, 0.0))
    ]

    order_a = Order(0, 0, (5.0, 0.0), (6.0, 0.0), 0.0)
    order_a.ready_time = 10.0
    order_a.state = "ASSIGNED"
    order_a.assigned_courier_id = 999

    order_b = Order(1, 0, (5.0, 0.0), (6.0, 0.1), 0.0)
    order_b.ready_time = 10.0
    order_b.state = "READY"

    orders = [order_a, order_b]

    state = SimulationState(restaurants, couriers, orders, duration=3600)
    state.current_time = 10.0

    assignments = assign_anticipated_bundling(state, couriers, orders)

    assigned_ids = []
    for _, order_ids in assignments:
        assigned_ids.extend(order_ids)

    expected = "Only Order 1 assigned"
    actual = f"Assignments: {assignments}"
    passed = 0 not in assigned_ids and 1 in assigned_ids

    print(f"\nExpected: {expected}")
    print(f"Actual: {actual}")
    print(f"Result: {'PASS' if passed else 'FAIL'}")

    record_test("9.3_skip_assigned", passed, expected, actual)
    return passed


# ============================================================================
# CATEGORY 10: ANTICIPATORY HORIZON (from maximization suite)
# ============================================================================

def test_10_1_proactive_dispatch():
    print_test_header("ANTICIPATORY HORIZON", "10.1", "Proactive dispatch")

    restaurants = [Restaurant(0, (3.0, 0.0))]
    couriers = [Courier(0, (0.0, 0.0))]

    order = Order(0, 0, (3.0, 0.0), (4.0, 0.0), 0.0)
    order.ready_time = 180.0
    order.state = "PENDING"

    orders = [order]

    state = SimulationState(restaurants, couriers, orders, duration=3600)
    state.current_time = 0.0

    travel_time = get_travel_time((0.0, 0.0), (3.0, 0.0))

    print(f"\nScenario:")
    print(f"  Order ready at t=180s")
    print(f"  Courier travel time: {travel_time:.1f}s")
    print(f"  Expected: Proactive assignment")

    assignments = assign_anticipated_bundling(state, couriers, orders)

    expected = "C0 assigned to O0 (proactive)"
    actual = f"Assignments: {assignments}"
    passed = len(assignments) == 1

    print(f"\nExpected: {expected}")
    print(f"Actual: {actual}")
    print(f"Result: {'PASS' if passed else 'FAIL'}")

    record_test("10.1_proactive_dispatch", passed, expected, actual)
    return passed


# ============================================================================
# TEST RUNNER
# ============================================================================

def run_all_tests():
    print("\n" + "="*80)
    print("UNIFIED TEST SUITE FOR ANTICIPATED BUNDLING (CP-SAT MAXIMIZATION)")
    print("="*80)
    print(f"\nFramework: Score = (n × 1M) - EffectiveCost")
    print(f"Principle: THROUGHPUT FIRST, EFFICIENCY SECOND\n")

    test_functions = [
        # Category 1: Temporal Edge Cases
        test_1_1_order_at_exact_lookahead_boundary,
        test_1_2_order_one_second_before_boundary,
        test_1_3_order_one_second_after_boundary,
        test_1_4_throughput_beats_perfect_sync,  # FIXED

        # Category 2: Throughput Prioritization
        test_2_1_throughput_beats_massive_delay,  # FIXED
        test_2_2_throughput_prioritizes_bundle,   # FIXED
        test_2_3_all_options_have_high_cost,

        # Category 3: Pathological Geometry
        test_3_1_opposite_ends_courier_middle,
        test_3_2_throughput_beats_high_route_cost,  # FIXED

        # Category 4: Bundling Antipatterns
        test_4_1_same_restaurant_staggered_ready_times,
        test_4_2_multi_restaurant_backtracking,

        # Category 5: Lookahead Blindness
        test_5_1_perfect_order_outside_window,

        # Category 6: Cost Function
        test_6_1_short_distance_high_penalties_vs_long_distance_zero_penalties,

        # Category 7: Starvation & Fairness
        test_7_1_old_ready_order_vs_fresh_ready_order,

        # Category 8: Scale & Thresholds
        test_8_1_exactly_17_orders_combinatorial_mode,
        test_8_2_exactly_18_orders_geographic_mode,

        # Category 9: Invariants
        test_9_1_no_duplicate_order_assignments,
        test_9_2_one_bundle_per_courier,
        test_9_3_skip_already_assigned_orders,

        # Category 10: Anticipatory Horizon
        test_10_1_proactive_dispatch,
    ]

    passed_count = 0
    failed_count = 0

    for test_func in test_functions:
        try:
            result = test_func()
            if result:
                passed_count += 1
            else:
                failed_count += 1
        except Exception as e:
            print(f"\n{'='*80}")
            print(f"EXCEPTION in {test_func.__name__}: {str(e)}")
            print(f"{'='*80}")
            failed_count += 1
            record_test(test_func.__name__, False, "No exception", f"Exception: {str(e)}",
                       "Test raised unexpected exception")

    print("\n" + "="*80)
    print("FINAL RESULTS")
    print("="*80)
    print(f"\nTotal Tests: {len(test_functions)}")
    print(f"Passed: {passed_count}")
    print(f"Failed: {failed_count}")
    print(f"Pass Rate: {100*passed_count/len(test_functions):.1f}%")

    print(f"\n{'='*80}")
    print("DETAILED FAILURE ANALYSIS")
    print(f"{'='*80}\n")

    failures = [r for r in test_results if not r['passed']]

    if not failures:
        print("NO FAILURES - All tests passed!\n")
    else:
        for i, failure in enumerate(failures, 1):
            print(f"\nFAILURE #{i}: {failure['name']}")
            print(f"  Expected: {failure['expected']}")
            print(f"  Actual: {failure['actual']}")
            print(f"  Reason: {failure['reason']}")

    return passed_count, failed_count


if __name__ == "__main__":
    import sys

    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    log_file = f"test_anticipated_bundling_unified_{timestamp}.log"

    class Tee:
        def __init__(self, *files):
            self.files = files
        def write(self, data):
            for f in self.files:
                f.write(data)
                f.flush()
        def flush(self):
            for f in self.files:
                f.flush()

    with open(log_file, 'w') as f:
        original_stdout = sys.stdout
        sys.stdout = Tee(sys.stdout, f)

        passed, failed = run_all_tests()

        sys.stdout = original_stdout

    print(f"\n{'='*80}")
    print(f"Test output saved to: {log_file}")
    print(f"{'='*80}\n")
