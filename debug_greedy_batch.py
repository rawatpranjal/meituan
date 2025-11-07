#!/usr/bin/env python3

import pickle
from simulator_core import get_travel_time

# Load state
with open('outputs/quick_test/states/greedy.pkl', 'rb') as f:
    state = pickle.load(f)

# Get snapshot at t=2400
snapshot = state.timeline[2400]
now = 2400

# Get READY orders and IDLE couriers from snapshot
ready_orders_data = [(oid, o) for oid, o in snapshot['orders'].items() if o['state'] == 'READY']
idle_couriers_data = [(cid, c) for cid, c in snapshot['couriers'].items()
                      if len(c.get('assigned_order_ids', [])) == 0]

print(f"At t={now}s:")
print(f"  READY orders: {len(ready_orders_data)}")
print(f"  IDLE couriers: {len(idle_couriers_data)}\n")

# Simulate greedy algorithm logic
pickup_service = 90
dropoff_service = 45

# Sort orders by ready_time (like greedy does)
ready_orders_sorted = sorted(ready_orders_data, key=lambda x: x[1]['ready_time'])

assignments_made = 0
available_couriers = list(idle_couriers_data)

for order_id, order_data in ready_orders_sorted[:5]:  # Check first 5 orders
    print(f"\nOrder {order_id}:")
    print(f"  ready_time: {order_data['ready_time']:.0f}s")
    print(f"  expiration: {order_data.get('expiration_time', 1800):.0f}s")
    deadline = order_data['ready_time'] + order_data.get('expiration_time', 1800)
    print(f"  deadline: {deadline:.0f}s")
    print(f"  time_left: {deadline - now:.0f}s ({(deadline-now)/60:.1f} min)")

    rest_loc = order_data['restaurant_location']
    cust_loc = order_data['diner_location']

    # Check feasibility with each courier
    feasible_count = 0
    for courier_id, courier_data in available_couriers[:3]:  # Check first 3 couriers
        cour_loc = courier_data['current_location']

        # Calculate travel times (Manhattan distance, 30 km/h)
        dist_to_rest = abs(cour_loc[0] - rest_loc[0]) + abs(cour_loc[1] - rest_loc[1])
        dist_to_cust = abs(rest_loc[0] - cust_loc[0]) + abs(rest_loc[1] - cust_loc[1])

        t_to_pickup = (dist_to_rest / 30.0) * 3600  # seconds
        t_pickup_to_dropoff = (dist_to_cust / 30.0) * 3600  # seconds

        finish_time = now + t_to_pickup + pickup_service + t_pickup_to_dropoff + dropoff_service

        is_feasible = finish_time <= deadline

        print(f"    Courier {courier_id}:")
        print(f"      Travel to rest: {t_to_pickup:.0f}s ({t_to_pickup/60:.1f} min)")
        print(f"      Travel to cust: {t_pickup_to_dropoff:.0f}s ({t_pickup_to_dropoff/60:.1f} min)")
        print(f"      finish_time: {finish_time:.0f}s")
        print(f"      Feasible: {'YES ✓' if is_feasible else 'NO ✗'} (finish={finish_time:.0f}s vs deadline={deadline:.0f}s)")

        if is_feasible:
            feasible_count += 1

    print(f"  Total feasible couriers: {feasible_count}/{len(available_couriers)}")

    if feasible_count > 0:
        assignments_made += 1

print(f"\n{'='*70}")
print(f"Expected assignments: {assignments_made}")
print(f"Actual assignments made by greedy at t=2400: 0")
print(f"\n⚠️  BUG CONFIRMED if expected > 0")
