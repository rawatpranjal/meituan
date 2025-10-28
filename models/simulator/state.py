"""
Courier State Management

Manages the stateful courier information throughout the simulation.
Each courier tracks: status, location, and when they become available.
"""

def initialize_courier_states(first_dispatch_snapshot, waybill_lookup, timeline_logger=None):
    """
    Initialize courier states from the first historical dispatch moment

    Args:
        first_dispatch_snapshot: List of courier dicts from dispatch_rider at t=0
        waybill_lookup: Dictionary mapping order_id to order details
        timeline_logger: Optional CourierTimelineLogger instance

    Returns:
        Dictionary mapping courier_id to state dict
    """
    courier_states = {}

    for courier_row in first_dispatch_snapshot:
        courier_id = courier_row['courier_id']
        waybills_str = courier_row.get('courier_waybills', '[]')

        # Determine if courier is busy from historical data
        # Couriers with existing waybills are already busy
        is_busy = waybills_str and waybills_str != '[]' and len(waybills_str) > 2

        initial_state = 'BUSY' if is_busy else 'AVAILABLE'
        courier_states[courier_id] = {
            'status': initial_state,
            'becomes_available_at': courier_row['dispatch_time'] + 600 if is_busy else 0,
            'lat': courier_row['rider_lat'],
            'lng': courier_row['rider_lng']
        }

        # Log initial state
        # Note: Couriers initialized as AVAILABLE may immediately transition to BUSY
        # at the same timestamp if assigned in first dispatch cycle (0-second idle time)
        if timeline_logger:
            timeline_logger.log_state_transition(
                courier_row['dispatch_time'],
                courier_id,
                initial_state,
                'initialized'
            )

    return courier_states


def get_available_couriers(current_time, courier_states, timeline_logger=None):
    """
    Get list of couriers who are available at the current time

    Args:
        current_time: Current simulation timestamp
        courier_states: Dictionary of all courier states
        timeline_logger: Optional CourierTimelineLogger instance

    Returns:
        List of dicts with available courier info (courier_id, lat, lng)
    """
    available = []

    for courier_id, state in courier_states.items():
        # Courier is available if they're AVAILABLE or their busy period has ended
        if state['status'] == 'AVAILABLE' or state['becomes_available_at'] <= current_time:
            # Update status if they just became free
            if state['status'] == 'BUSY' and state['becomes_available_at'] <= current_time:
                state['status'] = 'AVAILABLE'
                # Log transition to AVAILABLE
                if timeline_logger:
                    timeline_logger.log_state_transition(
                        current_time,
                        courier_id,
                        'AVAILABLE',
                        'completed_delivery'
                    )

            available.append({
                'courier_id': courier_id,
                'rider_lat': state['lat'],
                'rider_lng': state['lng']
            })

    return available


def update_courier_after_assignment(courier_id, courier_states, dispatch_time,
                                    delivery_location, task_duration, timeline_logger=None):
    """
    Update courier state after successful assignment

    Args:
        courier_id: ID of the courier
        courier_states: Dictionary of all courier states
        dispatch_time: When the assignment was made
        delivery_location: Tuple of (lat, lng) for delivery destination
        task_duration: How long the task will take (in seconds)
        timeline_logger: Optional CourierTimelineLogger instance
    """
    courier_states[courier_id]['status'] = 'BUSY'
    courier_states[courier_id]['becomes_available_at'] = dispatch_time + task_duration
    courier_states[courier_id]['lat'] = delivery_location[0]
    courier_states[courier_id]['lng'] = delivery_location[1]

    # Log transition to BUSY
    if timeline_logger:
        timeline_logger.log_state_transition(
            dispatch_time,
            courier_id,
            'BUSY',
            'assigned_order'
        )


def get_courier_state_summary(courier_states, current_time):
    """
    Get summary statistics about courier states

    Args:
        courier_states: Dictionary of all courier states
        current_time: Current simulation timestamp

    Returns:
        Dictionary with summary stats
    """
    total = len(courier_states)
    available = sum(1 for s in courier_states.values()
                   if s['status'] == 'AVAILABLE' or s['becomes_available_at'] <= current_time)
    busy = total - available

    return {
        'total_couriers': total,
        'available': available,
        'busy': busy,
        'utilization': busy / total if total > 0 else 0
    }
