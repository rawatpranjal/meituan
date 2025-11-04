"""
Diagnose why Network Bundling delivers 1 fewer order than Simple Bundling
in the Popup Problem scenario.

This should not happen - Network Bundling is strictly more capable.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from simulator_core import SimulationState, Courier, Order, Restaurant, euclidean_distance
from assignment_algorithms import (
    assign_simple_bundling,
    assign_network_bundling,
    generate_geographic_bundles,
    _find_best_partition
)

print("="*80)
print("DIAGNOSTIC: Network vs Simple Bundling Performance Gap")
print("="*80)

# Create a test scenario that mimics burst conditions
restaurants = [
    Restaurant(0, (1.0, 1.0)),
    Restaurant(1, (1.6, 1.0)),  # 600m from R0 (within 750m)
    Restaurant(2, (2.5, 2.5)),  # Far from others
]

couriers = [
    Courier(0, (1.5, 1.5)),
    Courier(1, (2.0, 2.0)),
    Courier(2, (3.0, 3.0)),
]

# Burst scenario: 6 orders from 2 nearby restaurants
orders = [
    # Restaurant 0 (3 orders)
    Order(0, 0, (1.0, 1.0), (0.5, 0.5), 0.0),   # Close customer
    Order(1, 0, (1.0, 1.0), (0.8, 1.2), 0.0),   # Close customer
    Order(2, 0, (1.0, 1.0), (1.2, 0.8), 0.0),   # Close customer

    # Restaurant 1 (3 orders) - 600m from R0
    Order(3, 1, (1.6, 1.0), (2.0, 0.5), 0.0),   # Nearby customer
    Order(4, 1, (1.6, 1.0), (2.2, 1.0), 0.0),   # Nearby customer
    Order(5, 1, (1.6, 1.0), (1.8, 1.5), 0.0),   # Nearby customer
]

for order in orders:
    order.ready_time = 0.0
    order.state = "READY"

state = SimulationState(restaurants, couriers, orders, duration=3600)
state.current_time = 0.0

print("\nScenario Setup:")
print(f"  3 couriers, 6 orders from 2 nearby restaurants (600m apart)")
print(f"  Restaurant 0 at (1.0, 1.0): 3 orders")
print(f"  Restaurant 1 at (1.6, 1.0): 3 orders")
print(f"  Distance between restaurants: {euclidean_distance((1.0, 1.0), (1.6, 1.0)):.3f} km = 600m")

# Test Simple Bundling
print("\n" + "="*80)
print("SIMPLE BUNDLING ANALYSIS")
print("="*80)

simple_assignments = assign_simple_bundling(state, couriers, orders)

print(f"\nAssignments: {len(simple_assignments)}")
total_orders_simple = 0
for courier_id, order_ids in simple_assignments:
    print(f"  Courier {courier_id} ← Orders {order_ids} (size {len(order_ids)})")
    total_orders_simple += len(order_ids)

    # Show restaurant breakdown
    restaurants_in_bundle = set(state.orders[oid].restaurant_id for oid in order_ids)
    if len(restaurants_in_bundle) > 1:
        print(f"    ⚠ MULTI-RESTAURANT BUNDLE: {restaurants_in_bundle}")

print(f"\nTotal orders assigned: {total_orders_simple}/6")

# Test Network Bundling
print("\n" + "="*80)
print("NETWORK BUNDLING ANALYSIS")
print("="*80)

# First, see what bundles are generated
bundle_candidates = generate_geographic_bundles(orders, max_bundle_size=3)
print(f"\nBundle candidates generated: {len(bundle_candidates)}")

multi_restaurant_count = 0
for bundle_ids in bundle_candidates:
    restaurants_in_bundle = set(state.orders[oid].restaurant_id for oid in bundle_ids)
    if len(bundle_ids) > 1:
        if len(restaurants_in_bundle) > 1:
            print(f"  Bundle {bundle_ids}: Multi-restaurant {restaurants_in_bundle} (size {len(bundle_ids)})")
            multi_restaurant_count += 1
        else:
            print(f"  Bundle {bundle_ids}: Same-restaurant R{list(restaurants_in_bundle)[0]} (size {len(bundle_ids)})")

print(f"\nMulti-restaurant bundles: {multi_restaurant_count}")

network_assignments = assign_network_bundling(state, couriers, orders)

print(f"\nAssignments: {len(network_assignments)}")
total_orders_network = 0
for courier_id, order_ids in network_assignments:
    print(f"  Courier {courier_id} ← Orders {order_ids} (size {len(order_ids)})")
    total_orders_network += len(order_ids)

    # Show restaurant breakdown
    restaurants_in_bundle = set(state.orders[oid].restaurant_id for oid in order_ids)
    if len(restaurants_in_bundle) > 1:
        print(f"    ✓ MULTI-RESTAURANT BUNDLE: {restaurants_in_bundle}")

print(f"\nTotal orders assigned: {total_orders_network}/6")

# Compare
print("\n" + "="*80)
print("COMPARISON")
print("="*80)
print(f"Simple Bundling:  {total_orders_simple}/6 orders assigned")
print(f"Network Bundling: {total_orders_network}/6 orders assigned")

if total_orders_simple > total_orders_network:
    print(f"\n⚠ ANOMALY: Simple Bundling assigned {total_orders_simple - total_orders_network} more order(s)!")
    print("   This should not happen - Network should be >= Simple")
elif total_orders_network > total_orders_simple:
    print(f"\n✓ Expected: Network assigned {total_orders_network - total_orders_simple} more order(s)")
else:
    print("\n✓ Tie: Both assigned same number of orders")

print("\n" + "="*80)
print("HYPOTHESIS")
print("="*80)
print("""
Possible causes for Network underperformance:

1. **Multi-restaurant bundle overhead**: Extra pickup stops (90s each) make
   multi-restaurant bundles slower than separate same-restaurant bundles.

2. **Geographic clustering too permissive**: 750m radius might include
   restaurants that are close but create inefficient routes.

3. **Partition optimization difference**: Simple Bundling's _find_best_partition
   explicitly tries multiple strategies (singles, max bundles, balanced) and
   picks the best. Network Bundling generates all geographic candidates and
   lets Hungarian choose, which may miss better partitions.

4. **Cost calculation mismatch**: If the cost function doesn't properly account
   for multi-pickup overhead, Network might choose bundles that look cheap but
   are actually slow.
""")
