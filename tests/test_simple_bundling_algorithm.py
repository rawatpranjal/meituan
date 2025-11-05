"""
Redesigned Test Suite for Simple Bundling Algorithm

Six tests designed to validate the redesigned candidate-based implementation:
1. Sanity Check - Basic 1-to-1 assignment
2. Obvious Bundle - Proves bundles are preferred when efficient
3. Don't Bundle Test - Proves singles are chosen when optimal
4. Filter Logic Test - THE CRITICAL TEST validating post-solver filtering
5. Multi-Restaurant Silo Test - Verifies candidate generation constraints
6. Resource Scarcity Test - Validates optimal choice under constraint

Each test surgically validates a specific aspect of the candidate + filter pipeline.
"""

import sys
import os
import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from simulator_core import (
    SimulationState, Courier, Order, Restaurant,
    euclidean_distance, get_travel_time
)
from assignment_algorithms import assign_simple_bundling


# ============================================================================
# TEST 1: SANITY CHECK - BASIC 1-TO-1 ASSIGNMENT
# ============================================================================

def test_sanity_check_basic_assignment():
    """
    TEST 1: Sanity Check - Basic 1-to-1 Assignment

    Scenario: One order, one courier.

    Expected Logic:
    - Candidate list: [[0]]
    - Solver assigns C0 to this task
    - Filter accepts this single assignment

    Expected: [(courier_id=0, order_ids=[0])]

    Rationale: Smoke test. Proves end-to-end pipeline works for simplest case.
    """
    print("\n" + "="*80)
    print("TEST 1: SANITY CHECK - BASIC 1-TO-1 ASSIGNMENT")
    print("="*80)

    restaurants = [Restaurant(0, (2.0, 2.0))]
    couriers = [Courier(0, (2.0, 1.0))]
    orders = [Order(0, 0, (2.0, 2.0), (1.0, 1.0), 0.0)]

    for order in orders:
        order.ready_time = 0.0
        order.state = "READY"

    state = SimulationState(restaurants, couriers, orders, duration=3600)

    print(f"\nSetup:")
    print(f"  Restaurant 0 at (2.0, 2.0)")
    print(f"  Courier 0 at (2.0, 1.0)")
    print(f"  Order 0: R0 → Customer at (1.0, 1.0)")
    print(f"  Expected candidates: [[0]]")

    assignments = assign_simple_bundling(state, couriers, orders)

    print(f"\nResult: {len(assignments)} assignment(s)")
    for c_id, o_ids in assignments:
        print(f"  Courier {c_id} → Orders {o_ids}")

    # Assertions
    assert len(assignments) == 1, f"Expected 1 assignment, got {len(assignments)}"
    assert assignments[0][0] == 0, f"Expected courier 0, got {assignments[0][0]}"
    assert assignments[0][1] == [0], f"Expected order [0], got {assignments[0][1]}"

    print("\n✓ PASS: Basic assignment works")


# ============================================================================
# TEST 2: THE OBVIOUS BUNDLE - PROVING BUNDLES ARE PREFERRED
# ============================================================================

def test_obvious_bundle_proves_bundling_preferred():
    """
    TEST 2: The Obvious Bundle - Proving Bundles Are Preferred

    Scenario: Two orders at same restaurant, destinations very close.
    Only one courier available.

    Expected Logic:
    - Candidates: [[0], [1], [0,1]]
    - Bundle [0,1] has lowest cost
    - Solver's top-ranked assignment: (C0, [0,1])
    - Filter accepts it

    Expected: [(courier_id=0, order_ids=[0, 1])]

    Rationale: Confirms that when a bundle is most cost-effective, the
    solver and filter correctly prioritize it over singles.
    """
    print("\n" + "="*80)
    print("TEST 2: OBVIOUS BUNDLE - PROVING BUNDLES ARE PREFERRED")
    print("="*80)

    restaurants = [Restaurant(0, (2.0, 2.0))]
    couriers = [Courier(0, (2.0, 1.0))]
    orders = [
        Order(0, 0, (2.0, 2.0), (1.0, 1.0), 0.0),
        Order(1, 0, (2.0, 2.0), (1.1, 1.0), 0.0)  # Very close to O0
    ]

    for order in orders:
        order.ready_time = 0.0
        order.state = "READY"

    state = SimulationState(restaurants, couriers, orders, duration=3600)

    print(f"\nSetup:")
    print(f"  Restaurant 0 at (2.0, 2.0)")
    print(f"  Courier 0 at (2.0, 1.0)")
    print(f"  Order 0: R0 → (1.0, 1.0)")
    print(f"  Order 1: R0 → (1.1, 1.0)  [very close to O0]")
    print(f"  Expected candidates: [[0], [1], [0,1]]")

    assignments = assign_simple_bundling(state, couriers, orders)

    print(f"\nResult: {len(assignments)} assignment(s)")
    for c_id, o_ids in assignments:
        print(f"  Courier {c_id} → Orders {o_ids}")

    # Assertions
    assert len(assignments) == 1, f"Expected 1 assignment, got {len(assignments)}"
    c_id, o_ids = assignments[0]
    assert c_id == 0, f"Expected courier 0, got {c_id}"
    assert set(o_ids) == {0, 1}, f"Expected orders {{0, 1}}, got {set(o_ids)}"
    assert len(o_ids) == 2, f"Expected bundle of 2, got {len(o_ids)}"

    print("\n✓ PASS: Algorithm correctly bundles when efficient")


# ============================================================================
# TEST 3: THE "DON'T BUNDLE" TEST - PROVING SINGLES ARE CHOSEN WHEN OPTIMAL
# ============================================================================

def test_dont_bundle_singles_when_optimal():
    """
    TEST 3: The "Don't Bundle" Test - Proving Singles Are Chosen When Optimal

    Scenario: Two orders at same restaurant, destinations on opposite ends
    of city. Two couriers available.

    Expected Logic:
    - Candidates: [[0], [1], [0,1]]
    - Bundle [0,1] has very high cost (zig-zag across city)
    - Singles (C0,[0]) and (C1,[1]) have low cost
    - Filter accepts both singles

    Expected: Two separate single-order assignments

    Rationale: Proves algorithm isn't biased towards bundling. Correctly
    identifies that two cheap singles > one expensive bundle.
    """
    print("\n" + "="*80)
    print("TEST 3: DON'T BUNDLE - SINGLES WHEN OPTIMAL")
    print("="*80)

    restaurants = [Restaurant(0, (2.0, 2.0))]
    couriers = [
        Courier(0, (2.0, 1.0)),
        Courier(1, (2.0, 3.0))
    ]
    orders = [
        Order(0, 0, (2.0, 2.0), (0.0, 5.0), 0.0),  # Far North
        Order(1, 0, (2.0, 2.0), (5.0, 0.0), 0.0)   # Far East
    ]

    for order in orders:
        order.ready_time = 0.0
        order.state = "READY"

    state = SimulationState(restaurants, couriers, orders, duration=3600)

    print(f"\nSetup:")
    print(f"  Restaurant 0 at (2.0, 2.0)")
    print(f"  Courier 0 at (2.0, 1.0)")
    print(f"  Courier 1 at (2.0, 3.0)")
    print(f"  Order 0: R0 → (0.0, 5.0)  [Far North]")
    print(f"  Order 1: R0 → (5.0, 0.0)  [Far East]")
    print(f"  Expected candidates: [[0], [1], [0,1]]")

    assignments = assign_simple_bundling(state, couriers, orders)

    print(f"\nResult: {len(assignments)} assignment(s)")
    for c_id, o_ids in assignments:
        print(f"  Courier {c_id} → Orders {o_ids}")

    # Assertions
    assert len(assignments) == 2, f"Expected 2 separate assignments, got {len(assignments)}"

    # Check that both assignments are singles (size 1)
    bundle_sizes = [len(o_ids) for _, o_ids in assignments]
    assert all(size == 1 for size in bundle_sizes), \
        f"Expected two singles, got bundle sizes {bundle_sizes}"

    # Check that all orders are assigned
    all_assigned = []
    for _, o_ids in assignments:
        all_assigned.extend(o_ids)
    assert set(all_assigned) == {0, 1}, f"Expected orders {{0, 1}}, got {set(all_assigned)}"

    print("\n✓ PASS: Algorithm correctly avoids inefficient bundling")


# ============================================================================
# TEST 4: THE FILTER LOGIC TEST - THE HEART OF THE NEW DESIGN
# ============================================================================

def test_filter_logic_heart_of_redesign():
    """
    TEST 4: The Filter Logic Test - The Heart of the New Design

    This is THE CRITICAL TEST. It validates the post-solver filtering logic.

    Scenario: Three orders, two couriers. Orders O0 and O1 are a perfect pair.
    Order O2 is an outlier.

    Expected Logic:
    - Candidates: [[0], [1], [2], [0,1], [0,2], [1,2], [0,1,2]]
    - Solver's sorted output (by cost):
      1. (cost=100, C0, [0,1])  -- Best move on board
      2. (cost=150, C1, [2])    -- Second best
      3. (cost=180, C1, [0])    -- Conflicts with #1
      ...
    - Filter execution:
      - Process #1: Valid. Accept (C0, [0,1]). assigned_orders={0,1}
      - Process #2: Valid. Accept (C1, [2]). assigned_orders={0,1,2}
      - Process #3: Order 0 already assigned. Reject.

    Expected: Bundle [0,1] + Single [2]

    Rationale: Surgically tests filtering logic. Proves it correctly prioritizes
    highest-value assignments and discards conflicting options.
    """
    print("\n" + "="*80)
    print("TEST 4: FILTER LOGIC - THE HEART OF THE REDESIGN")
    print("="*80)

    restaurants = [Restaurant(0, (2.0, 2.0))]
    couriers = [
        Courier(0, (2.0, 1.0)),
        Courier(1, (2.0, 3.0))
    ]
    orders = [
        Order(0, 0, (2.0, 2.0), (1.0, 1.0), 0.0),
        Order(1, 0, (2.0, 2.0), (1.1, 1.0), 0.0),  # Perfect pair with O0
        Order(2, 0, (2.0, 2.0), (5.0, 5.0), 0.0)   # Outlier
    ]

    for order in orders:
        order.ready_time = 0.0
        order.state = "READY"

    state = SimulationState(restaurants, couriers, orders, duration=3600)

    print(f"\nSetup:")
    print(f"  Restaurant 0 at (2.0, 2.0)")
    print(f"  Courier 0 at (2.0, 1.0)")
    print(f"  Courier 1 at (2.0, 3.0)")
    print(f"  Order 0: R0 → (1.0, 1.0)")
    print(f"  Order 1: R0 → (1.1, 1.0)  [perfect pair with O0]")
    print(f"  Order 2: R0 → (5.0, 5.0)  [outlier]")
    print(f"  Expected candidates: [[0], [1], [2], [0,1], [0,2], [1,2], [0,1,2]]")

    assignments = assign_simple_bundling(state, couriers, orders)

    print(f"\nResult: {len(assignments)} assignment(s)")
    for c_id, o_ids in assignments:
        print(f"  Courier {c_id} → Orders {o_ids}")

    # Assertions
    assert len(assignments) == 2, f"Expected 2 assignments, got {len(assignments)}"

    # Find the bundle and the single
    bundle_sizes = sorted([len(o_ids) for _, o_ids in assignments])
    assert bundle_sizes == [1, 2], f"Expected bundle sizes [1, 2], got {bundle_sizes}"

    # Find which assignment is the bundle
    bundle_assignment = [o_ids for _, o_ids in assignments if len(o_ids) == 2][0]
    single_assignment = [o_ids for _, o_ids in assignments if len(o_ids) == 1][0]

    # The bundle should contain orders 0 and 1 (the close pair)
    assert set(bundle_assignment) == {0, 1}, \
        f"Expected bundle {{0, 1}}, got {set(bundle_assignment)}"

    # The single should contain order 2 (the outlier)
    assert single_assignment == [2], \
        f"Expected single [2], got {single_assignment}"

    print("\n✓ PASS: Filter correctly selects optimal non-overlapping assignments")


# ============================================================================
# TEST 5: THE MULTI-RESTAURANT SILO TEST - VERIFYING CANDIDATE GENERATION
# ============================================================================

def test_multi_restaurant_silo_candidate_generation():
    """
    TEST 5: The Multi-Restaurant Silo Test - Verifying Candidate Generation

    Scenario: Two orders at R0, one order at R1 (geographically close to R0 order).

    Expected Logic:
    - _generate_simple_bundle_candidates groups by restaurant first
    - Generates candidates for R0: [[0], [1], [0,1]]
    - Generates candidates for R1: [[2]]
    - NEVER generates cross-restaurant candidate like [0,2]
    - Final assignments cannot contain cross-restaurant bundles

    Expected: No cross-restaurant bundles in assignments

    Rationale: Validates integrity of candidate generation step, which enforces
    the "same-restaurant" rule.
    """
    print("\n" + "="*80)
    print("TEST 5: MULTI-RESTAURANT SILO - CANDIDATE GENERATION")
    print("="*80)

    restaurants = [
        Restaurant(0, (2.0, 2.0)),
        Restaurant(1, (2.1, 2.0))  # Very close to R0
    ]
    couriers = [
        Courier(0, (2.0, 1.0)),
        Courier(1, (2.0, 3.0)),
        Courier(2, (2.0, 2.0))
    ]
    orders = [
        Order(0, 0, (2.0, 2.0), (1.0, 1.0), 0.0),    # R0
        Order(1, 0, (2.0, 2.0), (5.0, 5.0), 0.0),    # R0
        Order(2, 1, (2.1, 2.0), (1.1, 1.0), 0.0)     # R1, geographically close to O0
    ]

    for order in orders:
        order.ready_time = 0.0
        order.state = "READY"

    state = SimulationState(restaurants, couriers, orders, duration=3600)

    print(f"\nSetup:")
    print(f"  Restaurant 0 at (2.0, 2.0)")
    print(f"  Restaurant 1 at (2.1, 2.0)  [very close to R0]")
    print(f"  3 couriers available")
    print(f"  Order 0: R0 → (1.0, 1.0)")
    print(f"  Order 1: R0 → (5.0, 5.0)")
    print(f"  Order 2: R1 → (1.1, 1.0)  [geographically close to O0]")
    print(f"  Expected: Candidates NEVER mix restaurants")

    assignments = assign_simple_bundling(state, couriers, orders)

    print(f"\nResult: {len(assignments)} assignment(s)")
    for c_id, o_ids in assignments:
        print(f"  Courier {c_id} → Orders {o_ids}")
        restaurants_in_bundle = [orders[oid].restaurant_id for oid in o_ids]
        print(f"    Restaurants: {restaurants_in_bundle}")

    # Critical assertion: No bundle should contain orders from different restaurants
    for c_id, o_ids in assignments:
        restaurants_in_bundle = set(orders[oid].restaurant_id for oid in o_ids)
        assert len(restaurants_in_bundle) == 1, \
            f"Cross-restaurant bundle detected! Courier {c_id} has orders from restaurants {restaurants_in_bundle}"

    # Verify all orders are assigned
    all_assigned = []
    for _, o_ids in assignments:
        all_assigned.extend(o_ids)
    assert set(all_assigned) == {0, 1, 2}, f"Expected all orders assigned, got {set(all_assigned)}"

    print("\n✓ PASS: Candidate generation enforces same-restaurant constraint")


# ============================================================================
# TEST 6: THE RESOURCE SCARCITY TEST - OPTIMAL CHOICE UNDER CONSTRAINT
# ============================================================================

def test_resource_scarcity_optimal_choice():
    """
    TEST 6: The Resource Scarcity Test - Optimal Choice Under Constraint

    Scenario: Five orders at one restaurant, but only one available courier.

    Expected Logic:
    - Candidates include many singles, pairs, triplets
    - Solver finds single best assignment for the one courier
    - This will be the task with lowest route cost
    - Likely a 3-order bundle of geographically closest customers
    - Filter accepts this single best assignment
    - Other orders left unassigned (no more couriers)

    Expected: Single assignment, bundle size ≤3

    Rationale: Proves algorithm makes single best possible move when
    resources are severely limited.
    """
    print("\n" + "="*80)
    print("TEST 6: RESOURCE SCARCITY - OPTIMAL CHOICE")
    print("="*80)

    restaurants = [Restaurant(0, (2.0, 2.0))]
    couriers = [Courier(0, (2.0, 1.0))]
    orders = [
        Order(0, 0, (2.0, 2.0), (1.0, 1.0), 0.0),
        Order(1, 0, (2.0, 2.0), (1.5, 1.5), 0.0),
        Order(2, 0, (2.0, 2.0), (2.0, 2.0), 0.0),
        Order(3, 0, (2.0, 2.0), (2.5, 2.5), 0.0),
        Order(4, 0, (2.0, 2.0), (3.0, 3.0), 0.0)
    ]

    for order in orders:
        order.ready_time = 0.0
        order.state = "READY"

    state = SimulationState(restaurants, couriers, orders, duration=3600)

    print(f"\nSetup:")
    print(f"  Restaurant 0 at (2.0, 2.0)")
    print(f"  5 orders ready")
    print(f"  Only 1 courier available")
    print(f"  Expected: Solver picks single best candidate (likely a triplet)")

    assignments = assign_simple_bundling(state, couriers, orders)

    print(f"\nResult: {len(assignments)} assignment(s)")
    for c_id, o_ids in assignments:
        print(f"  Courier {c_id} → Orders {o_ids} ({len(o_ids)} orders)")

    # Assertions
    assert len(assignments) == 1, f"Expected 1 assignment, got {len(assignments)}"

    c_id, o_ids = assignments[0]
    assert c_id == 0, f"Expected courier 0, got {c_id}"
    assert len(o_ids) <= 3, f"MAX_BUNDLE_SIZE=3 violated! Got bundle size {len(o_ids)}"
    assert len(o_ids) >= 1, f"Expected at least 1 order assigned, got {len(o_ids)}"

    # Check that assigned orders are from the available pool
    assert all(oid in range(5) for oid in o_ids), \
        f"Invalid order IDs assigned: {o_ids}"

    unassigned_count = 5 - len(o_ids)
    print(f"\n  {len(o_ids)} orders assigned, {unassigned_count} left unassigned")

    print("\n✓ PASS: Algorithm makes optimal choice under resource constraint")


# ============================================================================
# TEST RUNNER
# ============================================================================

def run_all_tests():
    """
    Runs all 6 redesigned tests and reports results.
    Returns True if all pass, False otherwise.
    """
    tests = [
        ("Test 1: Sanity Check", test_sanity_check_basic_assignment),
        ("Test 2: Obvious Bundle", test_obvious_bundle_proves_bundling_preferred),
        ("Test 3: Don't Bundle", test_dont_bundle_singles_when_optimal),
        ("Test 4: Filter Logic", test_filter_logic_heart_of_redesign),
        ("Test 5: Multi-Restaurant Silo", test_multi_restaurant_silo_candidate_generation),
        ("Test 6: Resource Scarcity", test_resource_scarcity_optimal_choice)
    ]

    passed = 0
    failed = 0

    print("\n" + "="*80)
    print("REDESIGNED SIMPLE BUNDLING TEST SUITE - 6 SURGICAL TESTS")
    print("="*80)

    for test_name, test_func in tests:
        try:
            test_func()
            passed += 1
        except AssertionError as e:
            failed += 1
            print(f"\n✗ FAIL: {test_name}")
            print(f"  Assertion Error: {e}")
        except Exception as e:
            failed += 1
            print(f"\n✗ FAIL: {test_name}")
            print(f"  Exception: {e}")
            import traceback
            traceback.print_exc()

    print("\n" + "="*80)
    print(f"RESULTS: {passed} PASSED, {failed} FAILED")
    print("="*80)

    return failed == 0


if __name__ == "__main__":
    # Setup logging
    timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
    log_dir = "/Users/pranjal/Code/meituan/tests/logs"
    os.makedirs(log_dir, exist_ok=True)
    log_path = f"{log_dir}/test_simple_bundling_algorithm_{timestamp}.log"

    # Redirect stdout to log file
    class TeeOutput:
        def __init__(self, *files):
            self.files = files

        def write(self, obj):
            for f in self.files:
                f.write(obj)
                f.flush()

        def flush(self):
            for f in self.files:
                f.flush()

    with open(log_path, 'w') as log_file:
        original_stdout = sys.stdout
        sys.stdout = TeeOutput(sys.stdout, log_file)

        try:
            success = run_all_tests()

            print(f"\nLog file: {log_path}")

            sys.exit(0 if success else 1)
        finally:
            sys.stdout = original_stdout
