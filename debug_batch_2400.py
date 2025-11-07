#!/usr/bin/env python3

import pickle
import sys

# Load cached state for greedy algorithm
print("Loading cached state...")
with open('outputs/quick_test/states/greedy.pkl', 'rb') as f:
    state = pickle.load(f)

# Find snapshot at t=2400s (40 minutes)
target_time = 2400
snapshot = state.timeline[target_time]

print(f"\n{'='*70}")
print(f"SNAPSHOT AT t={target_time}s (40 minutes)")
print(f"{'='*70}\n")

# Analyze orders by state
orders_by_state = {}
for order_id, order_data in snapshot['orders'].items():
    state_name = order_data['state']
    if state_name not in orders_by_state:
        orders_by_state[state_name] = []
    orders_by_state[state_name].append(order_id)

print("ORDER STATE DISTRIBUTION:")
for state_name, order_ids in sorted(orders_by_state.items()):
    print(f"  {state_name}: {len(order_ids)} orders")
    if state_name in ['READY', 'PENDING']:
        for oid in order_ids[:5]:  # Show first 5
            order = snapshot['orders'][oid]
            ready_time = order['ready_time']
            expiration = order.get('expiration_time', 1800)
            deadline = ready_time + expiration
            print(f"    Order {oid}: ready_time={ready_time:.0f}s, deadline={deadline:.0f}s, "
                  f"restaurant={order['restaurant_location']}, "
                  f"customer={order['diner_location']}")
        if len(order_ids) > 5:
            print(f"    ... and {len(order_ids)-5} more")

# Analyze couriers
print(f"\nCOURIER STATUS:")
idle_couriers = []
busy_couriers = []

for courier_id, courier_data in snapshot['couriers'].items():
    location = courier_data['current_location']
    assigned_orders = courier_data.get('assigned_order_ids', [])

    if len(assigned_orders) == 0:
        idle_couriers.append(courier_id)
        print(f"  Courier {courier_id}: IDLE at {location}")
    else:
        busy_couriers.append(courier_id)
        print(f"  Courier {courier_id}: BUSY with {len(assigned_orders)} orders at {location}")

print(f"\nSUMMARY:")
print(f"  IDLE couriers: {len(idle_couriers)}")
print(f"  BUSY couriers: {len(busy_couriers)}")
print(f"  READY orders: {len(orders_by_state.get('READY', []))}")
print(f"  PENDING orders: {len(orders_by_state.get('PENDING', []))}")

# The key question: Why aren't READY orders matched to IDLE couriers?
ready_count = len(orders_by_state.get('READY', []))
idle_count = len(idle_couriers)

print(f"\n{'='*70}")
print(f"ANALYSIS:")
print(f"{'='*70}")

if ready_count == 0 and idle_count > 0:
    print(f"✓ No READY orders available")
    print(f"  -> {idle_count} couriers idle because no orders to assign")
    print(f"  -> This confirms the temporal gap theory")
    pending_count = len(orders_by_state.get('PENDING', []))
    if pending_count > 0:
        print(f"  -> {pending_count} PENDING orders exist (food still cooking)")
        print(f"  -> These will become READY in next few minutes")
elif ready_count > 0 and idle_count > 0:
    print(f"⚠️  MISMATCH DETECTED!")
    print(f"  -> {ready_count} READY orders available")
    print(f"  -> {idle_count} couriers idle")
    print(f"  -> WHY AREN'T THEY MATCHED?")
    print(f"\nPossible reasons:")
    print(f"  1. All READY orders are infeasible (too far, deadline too tight)")
    print(f"  2. Assignment algorithm bug")
    print(f"  3. Orders just became READY but assignment already happened")

    # Check feasibility
    print(f"\nFEASIBILITY CHECK:")
    for order_id in orders_by_state.get('READY', [])[:3]:
        order = snapshot['orders'][order_id]
        ready_time = order['ready_time']
        expiration = order.get('expiration_time', 1800)
        deadline = ready_time + expiration
        time_left = deadline - target_time

        rest_loc = order['restaurant_location']
        cust_loc = order['diner_location']

        print(f"\n  Order {order_id}:")
        print(f"    Time until deadline: {time_left:.0f}s ({time_left/60:.1f} min)")
        print(f"    Restaurant: {rest_loc}")
        print(f"    Customer: {cust_loc}")

        # Check distance from each idle courier
        for courier_id in idle_couriers[:3]:
            courier = snapshot['couriers'][courier_id]
            cour_loc = courier['current_location']

            # Manhattan distance
            dist_to_rest = abs(cour_loc[0] - rest_loc[0]) + abs(cour_loc[1] - rest_loc[1])
            dist_to_cust = abs(rest_loc[0] - cust_loc[0]) + abs(rest_loc[1] - cust_loc[1])

            # Time calculation (30 km/h = 0.5 km/min)
            travel_time = (dist_to_rest + dist_to_cust) * 2  # minutes
            service_time = (90 + 45) / 60  # 2.25 minutes
            total_time = travel_time + service_time

            feasible = total_time * 60 < time_left

            print(f"    Courier {courier_id} at {cour_loc}:")
            print(f"      Distance: {dist_to_rest:.1f}km to restaurant, {dist_to_cust:.1f}km to customer")
            print(f"      Estimated time: {total_time:.1f} min")
            print(f"      Feasible: {'YES ✓' if feasible else 'NO ✗ (too slow)'}")
elif ready_count == 0 and idle_count == 0:
    print(f"✓ No READY orders and no IDLE couriers")
    print(f"  -> All couriers busy, system at capacity")
else:
    print(f"✓ All READY orders assigned")
    print(f"  -> System working correctly")
