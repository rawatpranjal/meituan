#!/usr/bin/env python3

import pickle

# Load cached state
with open('outputs/quick_test/states/greedy.pkl', 'rb') as f:
    state = pickle.load(f)

# Check snapshots around t=2400s
times = [2399, 2400, 2401, 2405, 2410]

for t in times:
    snapshot = state.timeline[t]

    # Count orders and couriers
    ready_orders = [o for o in snapshot['orders'].values() if o['state'] == 'READY']
    idle_couriers = [c for c in snapshot['couriers'].values()
                     if len(c.get('assigned_order_ids', [])) == 0]
    assigned_orders = [o for o in snapshot['orders'].values() if o['state'] == 'ASSIGNED']

    print(f"t={t}s: READY={len(ready_orders)}, IDLE_COURIERS={len(idle_couriers)}, ASSIGNED={len(assigned_orders)}")

print("\n" + "="*70)
print("INTERPRETATION:")
print("="*70)

snap_2400 = state.timeline[2400]
snap_2401 = state.timeline[2401]

ready_2400 = len([o for o in snap_2400['orders'].values() if o['state'] == 'READY'])
ready_2401 = len([o for o in snap_2401['orders'].values() if o['state'] == 'READY'])
assigned_2401 = len([o for o in snap_2401['orders'].values() if o['state'] == 'ASSIGNED'])

if ready_2400 > 30 and ready_2401 > 30:
    print("\n⚠️  PROBLEM DETECTED:")
    print(f"   Batch at t=2400s did NOT make assignments!")
    print(f"   35 READY orders remained unmatched")
    print(f"   This suggests the assignment algorithm returned ZERO matches")
    print(f"\n   Possible causes:")
    print(f"   1. Greedy algorithm bug")
    print(f"   2. Feasibility filter is too strict")
    print(f"   3. Algorithm logic error")
elif ready_2401 < ready_2400:
    print("\n✓ Assignments were made at t=2400s")
    print(f"   READY orders decreased from {ready_2400} to {ready_2401}")
    print(f"   {assigned_2401} orders now ASSIGNED")
    print(f"   Snapshot at t=2400 was taken BEFORE assignments")
else:
    print("\n? Unclear - need more investigation")
