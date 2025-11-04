"""
ARCHIVED: Relay Bundling Algorithm (Algorithm 6)

This algorithm has been retired from active use and moved to archive.

The relay bundling strategy attempted to improve efficiency through cross-zone
courier handoffs, but added complexity without sufficient performance gains.
"""

from typing import List, Tuple, Optional
from simulator_core import SimulationState, Courier, Order


# ============================================================================
# ALGORITHM 6: RELAY BUNDLING (OPPORTUNISTIC HANDOFFS)
# ============================================================================

def get_zone(location: Tuple[float, float]) -> str:
    """Divide 5x5km grid into 4 quadrants."""
    x, y = location
    if x < 2.5 and y < 2.5: return "SW"
    if x >= 2.5 and y < 2.5: return "SE"
    if x < 2.5 and y >= 2.5: return "NW"
    return "NE"


def calculate_handoff_point(pickup_loc: Tuple[float, float],
                            dropoff_loc: Tuple[float, float]) -> Tuple[float, float]:
    """Calculate intersection with zone boundary (grid midlines at 2.5km)."""
    px, py = pickup_loc
    dx, dy = dropoff_loc

    # Check if crosses vertical boundary (x=2.5)
    if (px < 2.5 and dx >= 2.5) or (px >= 2.5 and dx < 2.5):
        # Interpolate y coordinate at x=2.5
        t = (2.5 - px) / (dx - px)
        handoff_y = py + t * (dy - py)
        return (2.5, handoff_y)

    # Check if crosses horizontal boundary (y=2.5)
    if (py < 2.5 and dy >= 2.5) or (py >= 2.5 and dy < 2.5):
        # Interpolate x coordinate at y=2.5
        t = (2.5 - py) / (dy - py)
        handoff_x = px + t * (dx - px)
        return (handoff_x, 2.5)

    # If no zone crossing, return midpoint
    return ((px + dx) / 2, (py + dy) / 2)


def assign_relay_bundling(state: SimulationState, idle_couriers: List[Courier],
                          ready_orders: List[Order]) -> List[Tuple[int, List[int], Optional[dict]]]:
    """
    Algorithm 6: Opportunistic Relay Heuristic

    Strategy:
    1. Run Simple Bundling Route-Aware to get initial assignments
    2. Identify cross-zone deliveries (different pickup/dropoff zones)
    3. Check for idle couriers in destination zone
    4. Create relay handoff at zone boundary
    5. Split delivery: C1 takes to handoff, C2 completes delivery

    Returns:
        List of (courier_id, [order_ids], relay_info) assignments
        where relay_info = {
            'handoff_location': (x, y),
            'relay_courier_id': courier2_id,
            'is_first_leg': bool  # True for pickup->handoff, False for handoff->delivery
        }
    """
    # Import here to avoid circular dependency
    from assignment_algorithms import assign_simple_bundling

    if not idle_couriers or not ready_orders:
        return []

    # Step 1: Run Simple Bundling to get base assignments
    base_assignments = assign_simple_bundling(state, idle_couriers, ready_orders)

    if not base_assignments:
        return []

    # Track which couriers are used and which orders are assigned
    used_couriers = set()
    assigned_orders = {}  # order_id -> courier_id

    for courier_id, order_ids in base_assignments:
        used_couriers.add(courier_id)
        for oid in order_ids:
            assigned_orders[oid] = courier_id

    # Find remaining idle couriers (not used by base assignment)
    remaining_idle = [c for c in idle_couriers if c.id not in used_couriers]

    # Step 2: Scan for relay opportunities
    relay_assignments = []
    relayed_orders = set()

    for courier_id, order_ids in base_assignments:
        courier = next(c for c in idle_couriers if c.id == courier_id)

        # Check each order for relay potential (only single orders for simplicity)
        if len(order_ids) == 1:
            order_id = order_ids[0]
            order = state.orders[order_id]

            pickup_zone = get_zone(order.restaurant_location)
            dropoff_zone = get_zone(order.diner_location)

            # Check if cross-zone delivery
            if pickup_zone != dropoff_zone:
                # Find idle courier in destination zone
                relay_courier = None
                for rc in remaining_idle:
                    if get_zone(rc.current_location) == dropoff_zone:
                        relay_courier = rc
                        break

                if relay_courier:
                    # Create relay assignment
                    handoff_point = calculate_handoff_point(
                        order.restaurant_location,
                        order.diner_location
                    )

                    # First leg: original courier takes to handoff
                    relay_info_1 = {
                        'handoff_location': handoff_point,
                        'relay_courier_id': relay_courier.id,
                        'is_first_leg': True,
                        'relay_order_id': order_id
                    }
                    relay_assignments.append((courier_id, [order_id], relay_info_1))

                    # Second leg: relay courier completes delivery
                    relay_info_2 = {
                        'handoff_location': handoff_point,
                        'relay_courier_id': courier_id,  # Original courier
                        'is_first_leg': False,
                        'relay_order_id': order_id
                    }
                    relay_assignments.append((relay_courier.id, [order_id], relay_info_2))

                    # Mark as relayed and remove relay courier from available pool
                    relayed_orders.add(order_id)
                    remaining_idle.remove(relay_courier)
                    used_couriers.add(relay_courier.id)

                    # Log relay event
                    state.log_event('RELAY_CREATED',
                                  f'Relay handoff created: C{courier_id} → C{relay_courier.id} for order {order_id}',
                                  courier_1_id=courier_id,
                                  courier_2_id=relay_courier.id,
                                  order_id=order_id,
                                  handoff_location=handoff_point)

                    print(f"[RELAY] Created handoff: Courier {courier_id} → {relay_courier.id} at {handoff_point}")

    # Step 3: Combine relay and non-relay assignments
    final_assignments = []

    # Add relay assignments
    final_assignments.extend(relay_assignments)

    # Add non-relayed assignments from base
    for courier_id, order_ids in base_assignments:
        # Filter out relayed orders
        non_relayed = [oid for oid in order_ids if oid not in relayed_orders]
        if non_relayed:
            final_assignments.append((courier_id, non_relayed, None))

    return final_assignments
