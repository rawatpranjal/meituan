"""
Debug Script for Simple Bundling Algorithm

This script traces through the algorithm execution step-by-step to identify
why bundles are not being created when they should be efficient.

Reproduces Test 2 (Obvious Bundle) with detailed logging.
"""

import sys
import numpy as np
from scipy.optimize import linear_sum_assignment

sys.path.insert(0, '/Users/pranjal/Code/meituan')

from simulator_core import SimulationState, Courier, Order, Restaurant, calculate_route_duration
from assignment_algorithms import _generate_simple_bundle_candidates


def debug_obvious_bundle_case():
    """
    Reproduce Test 2: Two orders at same restaurant, very close destinations.
    One courier. Bundle should be more efficient than singles.
    """
    print("="*80)
    print("DEBUG: OBVIOUS BUNDLE CASE")
    print("="*80)

    # Setup
    restaurants = [Restaurant(0, (2.0, 2.0))]
    couriers = [Courier(0, (2.0, 1.0))]
    orders = [
        Order(0, 0, (2.0, 2.0), (1.0, 1.0), 0.0),
        Order(1, 0, (2.0, 2.0), (1.1, 1.0), 0.0)
    ]

    for order in orders:
        order.ready_time = 0.0
        order.state = "READY"

    state = SimulationState(restaurants, couriers, orders, duration=3600)

    print("\nSetup:")
    print(f"  Restaurant 0: {restaurants[0].location}")
    print(f"  Courier 0: {couriers[0].current_location}")
    print(f"  Order 0: R0 → {orders[0].customer_location}")
    print(f"  Order 1: R0 → {orders[1].customer_location}")

    # Step 1: Generate candidates
    print("\n" + "="*80)
    print("STEP 1: GENERATE CANDIDATES")
    print("="*80)

    candidate_bundles = _generate_simple_bundle_candidates(orders, max_bundle_size=3)

    print(f"\nGenerated {len(candidate_bundles)} candidates:")
    for i, bundle in enumerate(candidate_bundles):
        print(f"  Candidate {i}: {bundle}")

    # Step 2: Build cost matrix
    print("\n" + "="*80)
    print("STEP 2: BUILD COST MATRIX")
    print("="*80)

    num_couriers = len(couriers)
    num_bundles = len(candidate_bundles)
    cost_matrix = np.zeros((num_couriers, num_bundles))

    print(f"\nCost matrix dimensions: {num_couriers} couriers × {num_bundles} bundles")
    print("\nCalculating costs:")

    for i, courier in enumerate(couriers):
        for j, bundle_order_ids in enumerate(candidate_bundles):
            cost = calculate_route_duration(
                courier.current_location,
                bundle_order_ids,
                state,
                use_tsp_optimization=(len(bundle_order_ids) > 1),
                include_service_times=True
            )
            cost_matrix[i, j] = cost

            print(f"  Courier {i} → Candidate {j} {bundle_order_ids}: {cost:.2f} seconds")

    print(f"\nCost matrix:")
    print(cost_matrix)

    # Step 3: Solve with Hungarian algorithm
    print("\n" + "="*80)
    print("STEP 3: HUNGARIAN ALGORITHM")
    print("="*80)

    row_ind, col_ind = linear_sum_assignment(cost_matrix)

    print(f"\nHungarian algorithm output:")
    print(f"  row_ind: {row_ind}")
    print(f"  col_ind: {col_ind}")

    # Step 4: Extract potential assignments
    print("\n" + "="*80)
    print("STEP 4: EXTRACT POTENTIAL ASSIGNMENTS")
    print("="*80)

    potential_assignments = []
    for r, c in zip(row_ind, col_ind):
        if r < num_couriers and c < num_bundles:
            cost = cost_matrix[r, c]
            courier_id = couriers[r].id
            order_ids = candidate_bundles[c]
            potential_assignments.append((cost, courier_id, order_ids))
            print(f"  Assignment: Courier {courier_id} → Orders {order_ids}, Cost: {cost:.2f}")

    # Step 5: Sort by cost
    print("\n" + "="*80)
    print("STEP 5: SORT BY COST")
    print("="*80)

    potential_assignments.sort(key=lambda x: x[0])

    print("\nSorted potential assignments (best first):")
    for i, (cost, courier_id, order_ids) in enumerate(potential_assignments):
        print(f"  #{i+1}: Courier {courier_id} → Orders {order_ids}, Cost: {cost:.2f}")

    # Step 6: Filter for non-overlapping
    print("\n" + "="*80)
    print("STEP 6: FILTER FOR NON-OVERLAPPING")
    print("="*80)

    final_assignments = []
    assigned_orders = set()

    for cost, courier_id, order_ids in potential_assignments:
        overlapping = [oid for oid in order_ids if oid in assigned_orders]

        if overlapping:
            print(f"  REJECT: Courier {courier_id} → {order_ids} (overlaps with {overlapping})")
        else:
            print(f"  ACCEPT: Courier {courier_id} → {order_ids}")
            final_assignments.append((courier_id, order_ids))
            for oid in order_ids:
                assigned_orders.add(oid)

    # Final result
    print("\n" + "="*80)
    print("FINAL RESULT")
    print("="*80)

    print(f"\nFinal assignments: {len(final_assignments)}")
    for courier_id, order_ids in final_assignments:
        print(f"  Courier {courier_id} → Orders {order_ids}")

    print(f"\nOrders assigned: {sorted(assigned_orders)}")
    print(f"Orders unassigned: {sorted(set(range(len(orders))) - assigned_orders)}")

    # Analysis
    print("\n" + "="*80)
    print("ANALYSIS")
    print("="*80)

    if len(final_assignments) == 1 and len(final_assignments[0][1]) == 2:
        print("\n✓ SUCCESS: Algorithm correctly created a bundle")
    elif len(final_assignments) == 1 and len(final_assignments[0][1]) == 1:
        print("\n✗ FAILURE: Algorithm created a single instead of a bundle")
        print("\nDiagnostic questions:")
        print(f"  1. Was the bundle candidate generated? {[0,1] in candidate_bundles or [1,0] in candidate_bundles}")
        print(f"  2. Cost of single [0]: {cost_matrix[0][candidate_bundles.index([0])]:.2f}")
        print(f"  3. Cost of single [1]: {cost_matrix[0][candidate_bundles.index([1])]:.2f}")
        bundle_idx = candidate_bundles.index([0,1]) if [0,1] in candidate_bundles else candidate_bundles.index([1,0])
        print(f"  4. Cost of bundle [0,1]: {cost_matrix[0][bundle_idx]:.2f}")
        print(f"  5. Which candidate did Hungarian select? Candidate {col_ind[0]} = {candidate_bundles[col_ind[0]]}")
        print(f"  6. Why? Because cost {cost_matrix[0][col_ind[0]]:.2f} is the minimum")
    else:
        print(f"\n? UNEXPECTED: Got {len(final_assignments)} assignments")


if __name__ == "__main__":
    debug_obvious_bundle_case()
