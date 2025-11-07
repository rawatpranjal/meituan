
import numpy as np
from datetime import datetime
from copy import deepcopy
from typing import List, Tuple, Dict, Optional

# ============================================================================
# PHYSICS HELPERS - MANHATTAN ONLY
# ============================================================================

# Global state for physics calculations
_courier_speed_m_per_s = None

def manhattan_distance(loc1: Tuple[float, float], loc2: Tuple[float, float]) -> float:

    return abs(loc1[0] - loc2[0]) + abs(loc1[1] - loc2[1])

def set_courier_speed(courier_speed_kmh: float):

    global _courier_speed_m_per_s
    _courier_speed_m_per_s = (courier_speed_kmh * 1000.0) / 3600.0

def get_distance(loc1: Tuple[float, float], loc2: Tuple[float, float]) -> float:

    return manhattan_distance(loc1, loc2)

def get_travel_time(loc1: Tuple[float, float], loc2: Tuple[float, float]) -> float:

    distance_km = get_distance(loc1, loc2)
    distance_m = distance_km * 1000.0
    if _courier_speed_m_per_s is None:
        raise ValueError("Courier speed not configured - call set_courier_speed() first")
    return distance_m / _courier_speed_m_per_s

def simulate_bundle_timeline(courier_location: Tuple[float, float],
                            order_ids: List[int],
                            state,
                            current_time: float) -> Dict:

    if not order_ids:
        return {'feasible': True, 'events': [], 'total_duration': 0}

    if not hasattr(state, 'config') or state.config is None:
        raise ValueError("State must have config for timeline simulation")

    # Get physics parameters
    pickup_service_time = state.config['physics']['pickup_service_time_s']
    dropoff_service_time = state.config['physics']['dropoff_service_time_s']

    # Get orders
    orders = [state.orders[oid] for oid in order_ids]

    # Group orders by restaurant
    by_restaurant = {}
    for order in orders:
        rest_id = order.restaurant_id
        if rest_id not in by_restaurant:
            by_restaurant[rest_id] = []
        by_restaurant[rest_id].append(order)

    events = []
    time_cursor = current_time
    location = courier_location

    # Phase 1: Visit restaurants (TSP if multiple)
    restaurant_visits = list(by_restaurant.keys())

    if len(restaurant_visits) > 1:
        # Multi-restaurant: Use TSP for optimal restaurant visiting order
        # For simplicity, using nearest-neighbor heuristic for >8 restaurants
        if len(restaurant_visits) <= 8:
            # Exact TSP via brute force
            from itertools import permutations
            best_route = None
            best_time = float('inf')

            for perm in permutations(restaurant_visits):
                test_time = 0
                test_loc = location
                for rest_id in perm:
                    rest_loc = state.restaurants[rest_id].location
                    test_time += get_travel_time(test_loc, rest_loc)
                    test_loc = rest_loc
                if test_time < best_time:
                    best_time = test_time
                    best_route = list(perm)
            restaurant_visits = best_route
        else:
            # Nearest neighbor for >8 restaurants
            remaining = set(restaurant_visits)
            ordered = []
            current = location

            while remaining:
                nearest = min(remaining,
                            key=lambda r: get_distance(current, state.restaurants[r].location))
                ordered.append(nearest)
                current = state.restaurants[nearest].location
                remaining.remove(nearest)
            restaurant_visits = ordered

    # Visit each restaurant
    for rest_id in restaurant_visits:
        restaurant = state.restaurants[rest_id]
        rest_orders = by_restaurant[rest_id]

        # Travel to restaurant
        travel_time = get_travel_time(location, restaurant.location)
        time_cursor += travel_time

        events.append({
            'type': 'arrive_restaurant',
            'time': time_cursor,
            'restaurant_id': rest_id,
            'location': restaurant.location
        })

        # Check if we need to wait for orders to be ready
        max_ready_time = max(o.ready_time for o in rest_orders)
        if time_cursor < max_ready_time:
            wait_time = max_ready_time - time_cursor
            # Cap waiting at 5 minutes (300 seconds)
            if wait_time > 300:
                return {
                    'feasible': False,
                    'reason': f'Would need to wait {wait_time}s at restaurant {rest_id}',
                    'events': events
                }
            time_cursor = max_ready_time
            events.append({
                'type': 'wait_for_ready',
                'time': time_cursor,
                'wait_duration': wait_time
            })

        # Pickup service time (once per restaurant)
        time_cursor += pickup_service_time

        events.append({
            'type': 'pickup_complete',
            'time': time_cursor,
            'order_ids': [o.id for o in rest_orders]
        })

        location = restaurant.location

    # Phase 2: Deliver orders (TSP for optimal delivery sequence)
    delivery_sequence = orders.copy()

    if len(delivery_sequence) > 1:
        # TSP for delivery sequence
        if len(delivery_sequence) <= 8:
            # Exact TSP
            from itertools import permutations
            best_seq = None
            best_time = float('inf')

            for perm in permutations(delivery_sequence):
                test_time = 0
                test_loc = location
                for order in perm:
                    test_time += get_travel_time(test_loc, order.diner_location)
                    test_loc = order.diner_location
                if test_time < best_time:
                    best_time = test_time
                    best_seq = list(perm)
            delivery_sequence = best_seq
        else:
            # Nearest neighbor
            remaining = set(delivery_sequence)
            ordered = []
            current = location

            while remaining:
                nearest = min(remaining,
                            key=lambda o: get_distance(current, o.diner_location))
                ordered.append(nearest)
                current = nearest.diner_location
                remaining.remove(nearest)
            delivery_sequence = ordered

    # Deliver each order
    for order in delivery_sequence:
        # Travel to customer
        travel_time = get_travel_time(location, order.diner_location)
        time_cursor += travel_time

        # Dropoff service time
        time_cursor += dropoff_service_time

        # Check per-order deadline
        deadline = order.ready_time + order.expiration_time
        if time_cursor > deadline:
            return {
                'feasible': False,
                'reason': f'Order {order.id} would expire (delivery at {time_cursor} > deadline {deadline})',
                'events': events
            }

        events.append({
            'type': 'delivery_complete',
            'time': time_cursor,
            'order_id': order.id,
            'location': order.diner_location,
            'deadline': deadline,
            'margin': deadline - time_cursor
        })

        location = order.diner_location

    return {
        'feasible': True,
        'events': events,
        'total_duration': time_cursor - current_time,
        'final_time': time_cursor
    }

def calculate_predicted_delivery_time(courier, order_ids: List[int],
                                      state) -> float:

    if not order_ids:
        return state.current_time

    # Use the new timeline simulator
    timeline = simulate_bundle_timeline(
        courier.current_location,
        order_ids,
        state,
        state.current_time
    )

    if timeline['feasible']:
        return timeline['final_time']
    else:
        # Return a very large time if infeasible
        state.log_event('BUNDLE_INFEASIBLE',
                       f'Bundle infeasible: {timeline.get("reason", "unknown")}',
                       courier_id=courier.id,
                       order_ids=order_ids)
        return float('inf')  # Infinite time = guaranteed rejection

# ============================================================================
# CORE CLASSES
# ============================================================================

class Restaurant:

    def __init__(self, restaurant_id: int, location: Tuple[float, float]):
        self.id = restaurant_id
        self.location = location  # (x, y) in km

    def to_dict(self):
        return {
            'id': self.id,
            'location': self.location
        }

class Order:

    def __init__(self, order_id: int, restaurant_id: int, restaurant_location: Tuple[float, float],
                 diner_location: Tuple[float, float], placement_time: float, meal_prep_time: Optional[float] = None,
                 expiration_time: Optional[float] = None):
        self.id = order_id
        self.restaurant_id = restaurant_id
        self.restaurant_location = restaurant_location
        self.diner_location = diner_location
        self.placement_time = placement_time
        # Meal prep time must be provided from config
        if meal_prep_time is None:
            raise ValueError("meal_prep_time is required - no hardcoded defaults allowed")
        self.meal_prep_time = meal_prep_time
        self.ready_time = placement_time + self.meal_prep_time
        # Expiration time must be provided from config
        if expiration_time is None:
            raise ValueError("expiration_time is required - no hardcoded defaults allowed")
        self.expiration_time = expiration_time

        # State tracking
        self.state = "PENDING"  # PENDING -> READY -> ASSIGNED -> PICKED_UP -> DELIVERED
        self.assigned_courier_id = None
        self.assignment_time = None
        self.pickup_time = None
        self.delivery_time = None

    def to_dict(self):
        return {
            'id': self.id,
            'restaurant_id': self.restaurant_id,
            'restaurant_location': self.restaurant_location,
            'diner_location': self.diner_location,
            'placement_time': self.placement_time,
            'ready_time': self.ready_time,
            'state': self.state,
            'assigned_courier_id': self.assigned_courier_id,
            'assignment_time': self.assignment_time,
            'pickup_time': self.pickup_time,
            'delivery_time': self.delivery_time
        }

class Courier:

    def __init__(self, courier_id: int, start_location: Tuple[float, float],
                 shift_start: float = 0.0, shift_end: float = 3600.0):
        self.id = courier_id
        self.start_location = start_location
        self.current_location = start_location
        self.shift_start = shift_start
        self.shift_end = shift_end

        # State tracking
        self.state = "IDLE"  # IDLE, DRIVING_TO_PICKUP, AT_PICKUP, DRIVING_TO_DROPOFF
        self.assigned_order_ids = []  # List of order IDs (for bundling)
        self.current_route = []  # List of (location, action, order_id) tuples
        self.next_destination = None
        self.arrival_time_at_destination = None

        # Multi-restaurant support
        self.pickup_route = []  # List of restaurant pickups to make
        self.current_pickup_index = 0  # Current position in pickup route

        # Metrics
        self.total_distance_traveled = 0.0
        self.total_idle_time = 0.0
        self.total_deliveries = 0
        self.last_state_change_time = 0.0

    def to_dict(self):
        return {
            'id': self.id,
            'start_location': self.start_location,
            'current_location': self.current_location,
            'state': self.state,
            'assigned_order_ids': list(self.assigned_order_ids),
            'current_route': list(self.current_route),
            'next_destination': self.next_destination,
            'arrival_time_at_destination': self.arrival_time_at_destination,
            'total_distance_traveled': self.total_distance_traveled,
            'total_idle_time': self.total_idle_time,
            'total_deliveries': self.total_deliveries
        }

class SimulationState:

    def __init__(self, restaurants: List[Restaurant], couriers: List[Courier],
                 order_schedule: List[Order], duration: int):
        self.current_time = 0.0
        self.duration = duration  # Store simulation duration
        self.config = None  # Will be set by run_simulation if config-based scenario
        self.restaurants = {r.id: r for r in restaurants}
        self.couriers = {c.id: c for c in couriers}
        self.orders = {o.id: o for o in order_schedule}
        self.order_schedule = sorted(order_schedule, key=lambda o: o.placement_time)

        # Timeline for replay
        self.timeline = []
        self.events_log = []

        # Performance metrics
        self.metrics = {
            'orders_delivered': 0,
            'orders_in_transit': 0,
            'orders_unassigned': 0,
            'orders_out_of_scope': 0,
            'total_delivery_time': 0.0,
            'total_ready_to_door_time': 0.0,
            'total_distance_traveled': 0.0,
            'total_courier_idle_time': 0.0,
            'bundles_created': 0,
            'total_bundle_size': 0,
            'unserved_orders': 0  # Deprecated - kept for compatibility
        }

    def log_event(self, event_type: str, description: str, **kwargs):

        event = {
            'time': self.current_time,
            'type': event_type,
            'description': description,
            **kwargs
        }
        self.events_log.append(event)

    def snapshot(self):

        return {
            'time': self.current_time,
            'couriers': {cid: c.to_dict() for cid, c in self.couriers.items()},
            'orders': {oid: o.to_dict() for oid, o in self.orders.items()},
            'metrics': deepcopy(self.metrics)
        }

    def get_idle_couriers(self) -> List[Courier]:

        return [c for c in self.couriers.values() if c.state == "IDLE"]

    def get_ready_orders(self) -> List[Order]:

        return [o for o in self.orders.values()
                if o.state == "READY" and self.current_time >= o.ready_time]

    def compute_final_metrics(self):

        # Categorize orders by final state
        delivered_orders = [o for o in self.orders.values() if o.state == "DELIVERED"]
        in_transit_orders = [o for o in self.orders.values() if o.state == "PICKED_UP"]
        assigned_orders = [o for o in self.orders.values() if o.state == "ASSIGNED"]
        ready_unassigned = [o for o in self.orders.values() if o.state == "READY"]
        pending_orders = [o for o in self.orders.values() if o.state == "PENDING"]

        # Separate out-of-scope orders (would never be ready in simulation time)
        out_of_scope = [o for o in pending_orders if o.ready_time > self.duration]
        truly_pending = [o for o in pending_orders if o.ready_time <= self.duration]

        self.metrics['orders_delivered'] = len(delivered_orders)
        self.metrics['orders_in_transit'] = len(in_transit_orders) + len(assigned_orders)
        self.metrics['orders_unassigned'] = len(ready_unassigned) + len(truly_pending)
        self.metrics['orders_out_of_scope'] = len(out_of_scope)

        # Deprecated metric for compatibility
        self.metrics['unserved_orders'] = len(self.orders) - len(delivered_orders)

        # === CUSTOMER-FACING METRICS ===
        # Fulfillment rate
        total_valid_orders = len(self.orders) - len(out_of_scope)
        self.metrics['fulfillment_rate_pct'] = (len(delivered_orders) / total_valid_orders * 100) if total_valid_orders > 0 else 0

        # Click-to-door times
        if delivered_orders:
            click_to_door_times = [o.delivery_time - o.placement_time for o in delivered_orders]
            self.metrics['avg_click_to_door_time'] = np.mean(click_to_door_times)
            self.metrics['p90_click_to_door_time'] = np.percentile(click_to_door_times, 90) if len(click_to_door_times) > 1 else click_to_door_times[0]

            # Ready-to-door (freshness)
            self.metrics['avg_ready_to_door_time'] = self.metrics['total_ready_to_door_time'] / len(delivered_orders)

            # Legacy metric names for compatibility
            self.metrics['avg_delivery_time'] = self.metrics['avg_click_to_door_time']
            self.metrics['avg_ready_to_door_time_sec'] = self.metrics['avg_ready_to_door_time']
        else:
            self.metrics['avg_click_to_door_time'] = 0
            self.metrics['p90_click_to_door_time'] = 0
            self.metrics['avg_delivery_time'] = 0
            self.metrics['avg_ready_to_door_time'] = 0

        # Pickup wait time (restaurant freshness)
        pickup_wait_times = []
        for order in delivered_orders:
            if order.pickup_time and order.ready_time:
                wait_time = order.pickup_time - order.ready_time
                pickup_wait_times.append(wait_time)

        self.metrics['avg_pickup_wait_time'] = np.mean(pickup_wait_times) if pickup_wait_times else 0

        # === COURIER-FACING METRICS ===
        # Courier utilization
        total_courier_time = sum(self.duration for _ in self.couriers.values())
        if total_courier_time > 0:
            active_time = total_courier_time - self.metrics['total_courier_idle_time']
            self.metrics['courier_utilization_pct'] = (active_time / total_courier_time) * 100
            self.metrics['courier_utilization'] = self.metrics['courier_utilization_pct']  # Legacy
        else:
            self.metrics['courier_utilization_pct'] = 0
            self.metrics['courier_utilization'] = 0

        # Distance and productivity
        self.metrics['total_distance_traveled_km'] = sum(c.total_distance_traveled for c in self.couriers.values())
        self.metrics['total_distance_traveled'] = self.metrics['total_distance_traveled_km']  # Legacy

        # Orders per courier hour
        total_courier_hours = total_courier_time / 3600
        self.metrics['avg_orders_per_courier_hour'] = len(delivered_orders) / total_courier_hours if total_courier_hours > 0 else 0

        # === PLATFORM-FACING METRICS ===
        # System throughput
        simulation_hours = self.duration / 3600
        self.metrics['system_throughput_orders_per_hour'] = len(delivered_orders) / simulation_hours if simulation_hours > 0 else 0

        # Bundle metrics
        if self.metrics['bundles_created'] > 0:
            self.metrics['avg_bundle_size'] = self.metrics['total_bundle_size'] / self.metrics['bundles_created']
        else:
            self.metrics['avg_bundle_size'] = 0

# ============================================================================
# SIMULATION ENGINE
# ============================================================================

def run_simulation(scenario: Dict, assignment_algorithm, algorithm_name: str) -> SimulationState:

    # Extract config if present (for new config-based scenarios)
    config = scenario.get('config', None)

    if config:
        # Set up courier speed from config
        courier_speed = config['physics']['courier_speed_kmh']
        set_courier_speed(courier_speed)

        # Get parameters from config
        sim_duration = int(scenario.get('duration', config['scenario']['duration_hours'] * 3600))
        batch_interval = config['physics']['batch_interval_s']
        pickup_service_time = config['physics']['pickup_service_time_s']
        dropoff_service_time = config['physics']['dropoff_service_time_s']
    else:
        # Fail fast - config is required
        raise ValueError("Config is required - no hardcoded defaults allowed")

    # Initialize simulation state
    state = SimulationState(
        restaurants=deepcopy(scenario['restaurants']),
        couriers=deepcopy(scenario['couriers']),
        order_schedule=deepcopy(scenario['order_schedule']),
        duration=sim_duration
    )

    # Store config in state for access by algorithms
    if config:
        state.config = config

    state.log_event('SIMULATION_START', f'Starting simulation with {algorithm_name}',
                    algorithm=algorithm_name)

    # Track when to run batch assignments
    next_batch_time = batch_interval

    # Track order placement queue
    pending_order_placements = list(state.order_schedule)

    # Main simulation loop
    for t in range(0, sim_duration + 1):
        state.current_time = t

        # 1. PROCESS ORDER PLACEMENTS
        while pending_order_placements and pending_order_placements[0].placement_time <= t:
            order = pending_order_placements.pop(0)
            state.log_event('ORDER_PLACED', f'Order {order.id} placed',
                           order_id=order.id, restaurant_id=order.restaurant_id)

        # 2. UPDATE ORDER STATES (PENDING -> READY -> EXPIRED)
        for order in state.orders.values():
            if order.state == "PENDING" and t >= order.ready_time:
                order.state = "READY"
                state.log_event('ORDER_READY', f'Order {order.id} is ready for pickup',
                               order_id=order.id)

            # Check if READY orders have expired (not matched within timeout)
            if order.state == "READY" and t >= order.ready_time + order.expiration_time:
                order.state = "EXPIRED"
                state.metrics['orders_expired'] = state.metrics.get('orders_expired', 0) + 1
                state.log_event('ORDER_EXPIRED', f'Order {order.id} expired (not matched within {order.expiration_time}s)',
                               order_id=order.id)

        # 3. UPDATE COURIER STATES (check for arrivals)
        for courier in state.couriers.values():
            if courier.state != "IDLE" and courier.arrival_time_at_destination is not None:
                if t >= courier.arrival_time_at_destination:
                    # Courier has arrived at destination
                    prev_location = courier.current_location
                    courier.current_location = courier.next_destination

                    # Update distance traveled
                    distance = get_distance(prev_location, courier.current_location)
                    courier.total_distance_traveled += distance

                    if courier.state == "DRIVING_TO_PICKUP":
                        # Check if all orders at this restaurant are ready
                        current_pickup = courier.pickup_route[courier.current_pickup_index]
                        max_ready_time = max(state.orders[oid].ready_time
                                           for oid in current_pickup['order_ids'])

                        if t < max_ready_time:
                            # Need to wait for orders to be ready
                            wait_time = max_ready_time - t
                            if wait_time > 300:  # Cap waiting at 5 minutes
                                state.log_event('EXCESSIVE_WAIT_WARNING',
                                              f'Courier {courier.id} would wait {wait_time:.0f}s > 300s cap',
                                              courier_id=courier.id,
                                              wait_time=wait_time)
                                # Still wait but log the issue
                            courier.state = "WAITING_AT_PICKUP"
                            courier.arrival_time_at_destination = max_ready_time  # Wait until orders ready
                            state.log_event('WAITING_AT_PICKUP',
                                          f'Courier {courier.id} waiting {wait_time:.0f}s for orders to be ready',
                                          courier_id=courier.id,
                                          wait_time=wait_time,
                                          ready_at=max_ready_time)
                        else:
                            # Orders ready - start pickup service immediately
                            courier.state = "AT_PICKUP"
                            courier.arrival_time_at_destination = t + pickup_service_time

                            state.log_event('PICKUP_SERVICE_START',
                                           f'Courier {courier.id} starting pickup service ({pickup_service_time}s)',
                                           courier_id=courier.id,
                                           service_time=pickup_service_time)

                    elif courier.state == "WAITING_AT_PICKUP" and t >= courier.arrival_time_at_destination:
                        # Wait complete - start pickup service
                        courier.state = "AT_PICKUP"
                        courier.arrival_time_at_destination = t + pickup_service_time

                        state.log_event('PICKUP_SERVICE_START',
                                       f'Courier {courier.id} starting pickup service after wait ({pickup_service_time}s)',
                                       courier_id=courier.id,
                                       service_time=pickup_service_time)

                    elif courier.state == "AT_PICKUP" and t >= courier.arrival_time_at_destination:
                        # Pickup service completed - mark orders FROM THIS RESTAURANT as picked up
                        current_pickup = courier.pickup_route[courier.current_pickup_index]
                        for order_id in current_pickup['order_ids']:
                            order = state.orders[order_id]
                            order.state = "PICKED_UP"
                            order.pickup_time = t
                            state.log_event('ORDER_PICKED_UP', f'Order {order_id} picked up by courier {courier.id}',
                                           order_id=order_id, courier_id=courier.id)

                        # Check if there are more restaurants to visit
                        courier.current_pickup_index += 1
                        if courier.current_pickup_index < len(courier.pickup_route):
                            # Go to next restaurant
                            next_pickup = courier.pickup_route[courier.current_pickup_index]
                            courier.state = "DRIVING_TO_PICKUP"
                            courier.next_destination = next_pickup['location']
                            travel_time = get_travel_time(courier.current_location, courier.next_destination)
                            courier.arrival_time_at_destination = t + travel_time

                            state.log_event('MULTI_RESTAURANT_ROUTE',
                                          f'Courier {courier.id} heading to restaurant {next_pickup["restaurant_id"]}',
                                          courier_id=courier.id,
                                          restaurant_id=next_pickup['restaurant_id'],
                                          pickup_index=courier.current_pickup_index,
                                          total_pickups=len(courier.pickup_route))

                        # All pickups complete, start deliveries
                        elif courier.assigned_order_ids:
                            # Import TSP optimizer (lazy import to avoid circular dependency)
                            from assignment_algorithms import optimize_delivery_sequence

                            # Get all dropoff locations
                            dropoff_locations = [state.orders[oid].diner_location
                                               for oid in courier.assigned_order_ids]

                            # Optimize delivery sequence if multiple dropoffs
                            if len(dropoff_locations) > 1:
                                optimized_sequence = optimize_delivery_sequence(
                                    courier.current_location, dropoff_locations)

                                # Reorder assigned_order_ids based on optimal sequence
                                original_order_ids = courier.assigned_order_ids[:]
                                courier.assigned_order_ids = [original_order_ids[i]
                                                             for i in optimized_sequence]

                                # Build optimized route
                                courier.current_route = []
                                for idx in optimized_sequence:
                                    courier.current_route.append({
                                        'type': 'DROPOFF',
                                        'location': dropoff_locations[idx],
                                        'order_id': original_order_ids[idx]
                                    })

                                state.log_event('ROUTE_OPTIMIZED',
                                              f'Courier {courier.id} route optimized: '
                                              f'{len(dropoff_locations)} dropoffs reordered',
                                              courier_id=courier.id,
                                              original_sequence=original_order_ids,
                                              optimized_sequence=courier.assigned_order_ids)
                            else:
                                # Single delivery, no optimization needed
                                courier.current_route = [{
                                    'type': 'DROPOFF',
                                    'location': dropoff_locations[0],
                                    'order_id': courier.assigned_order_ids[0]
                                }]

                            # Start heading to first optimized dropoff
                            first_order = state.orders[courier.assigned_order_ids[0]]
                            courier.state = "DRIVING_TO_DROPOFF"
                            courier.next_destination = first_order.diner_location
                            travel_time = get_travel_time(courier.current_location, courier.next_destination)
                            courier.arrival_time_at_destination = t + travel_time

                            state.log_event('COURIER_DELIVERING',
                                           f'Courier {courier.id} heading to first dropoff (optimized)',
                                           courier_id=courier.id,
                                           order_ids=courier.assigned_order_ids)

                    elif courier.state == "DRIVING_TO_DROPOFF":
                        # Delivered an order
                        delivered_order_id = courier.assigned_order_ids.pop(0)
                        delivered_order = state.orders[delivered_order_id]
                        delivered_order.state = "DELIVERED"
                        delivered_order.delivery_time = t
                        courier.total_deliveries += 1

                        # Update metrics
                        state.metrics['orders_delivered'] += 1
                        click_to_door = t - delivered_order.placement_time
                        ready_to_door = t - delivered_order.ready_time
                        state.metrics['total_delivery_time'] += click_to_door
                        state.metrics['total_ready_to_door_time'] += ready_to_door

                        state.log_event('ORDER_DELIVERED',
                                       f'Order {delivered_order_id} delivered by courier {courier.id}',
                                       order_id=delivered_order_id,
                                       courier_id=courier.id,
                                       click_to_door=click_to_door,
                                       ready_to_door=ready_to_door)

                        # Check if there are more orders to deliver
                        if courier.assigned_order_ids:
                            next_order = state.orders[courier.assigned_order_ids[0]]
                            courier.next_destination = next_order.diner_location
                            # Include dropoff service time before traveling to next location
                            travel_time = get_travel_time(courier.current_location, courier.next_destination)
                            courier.arrival_time_at_destination = t + dropoff_service_time + travel_time
                        else:
                            # No more orders - return to IDLE
                            courier.state = "IDLE"
                            courier.next_destination = None
                            courier.arrival_time_at_destination = None
                            courier.last_state_change_time = t

                            state.log_event('COURIER_IDLE', f'Courier {courier.id} is now idle',
                                           courier_id=courier.id)

        # 4. RUN BATCH ASSIGNMENT at regular intervals
        if t >= next_batch_time:
            idle_couriers = state.get_idle_couriers()

            # ========================================================================
            # ALGORITHM-SPECIFIC INPUT: Differentiate based on algorithm capability
            # ========================================================================
            # Anticipated algorithms need to see ALL orders (including PENDING) to
            # exercise their lookahead capability. Reactive algorithms only see READY orders.

            if algorithm_name == 'anticipated_bundling':
                # Anticipatory algorithm: pass ALL orders, it filters internally
                orders_for_assignment = list(state.orders.values())
            else:
                # Reactive algorithms: only see orders that are already READY
                orders_for_assignment = state.get_ready_orders()

            # For backward compatibility, keep ready_orders for validation logic
            ready_orders = state.get_ready_orders()

            if idle_couriers and orders_for_assignment:
                # Run assignment algorithm with appropriate order list
                assignments = assignment_algorithm(state, idle_couriers, orders_for_assignment)

                # Process assignments
                for assignment in assignments:
                    # Regular assignment: (courier_id, order_ids)
                    courier_id, order_ids = assignment

                    # ============================================================
                    # VALIDATION 1: Verify courier is actually idle
                    # ============================================================
                    idle_courier_ids = [c.id for c in idle_couriers]
                    if courier_id not in idle_courier_ids:
                        state.log_event('INVALID_ASSIGNMENT_REJECTED',
                                       f'Algorithm attempted to assign busy courier {courier_id} '
                                       f'(current state: {state.couriers[courier_id].state}). '
                                       f'Assignment rejected.',
                                       courier_id=courier_id,
                                       courier_state=state.couriers[courier_id].state,
                                       order_ids=order_ids)
                        continue  # Skip this invalid assignment

                    # ============================================================
                    # VALIDATION 2: Verify order states are valid for this algorithm
                    # ============================================================
                    invalid_orders = []
                    for oid in order_ids:
                        order = state.orders[oid]

                        # Algorithm-specific validation rules
                        if algorithm_name == 'anticipated_bundling':
                            # Anticipatory: allow PENDING or READY, but must be unassigned
                            if order.state not in ["PENDING", "READY"]:
                                invalid_orders.append((oid, f'state is {order.state}, expected PENDING or READY'))
                            elif order.state == "ASSIGNED":
                                invalid_orders.append((oid, 'already assigned to another courier'))
                        else:
                            # Reactive: must be in ready_orders list and state READY
                            ready_order_ids = [o.id for o in ready_orders]
                            if oid not in ready_order_ids:
                                invalid_orders.append((oid, 'not in ready_orders'))
                            elif order.state != "READY":
                                invalid_orders.append((oid, f'state is {order.state}'))

                    if invalid_orders:
                        state.log_event('INVALID_ORDER_ASSIGNMENT_REJECTED',
                                       f'Algorithm attempted to assign invalid orders to courier {courier_id}. '
                                       f'Invalid orders: {invalid_orders}. Assignment rejected.',
                                       courier_id=courier_id,
                                       invalid_orders=invalid_orders,
                                       order_ids=order_ids)
                        continue  # Skip this invalid assignment

                    courier = state.couriers[courier_id]

                    # ============================================================
                    # VALIDATION 4: DEADLINE FEASIBILITY GATEKEEPER
                    # ============================================================
                    # THE HARD CONSTRAINT: Never assign an order you cannot deliver on time.
                    # This validation enforces the business rule centrally for ALL algorithms.

                    # Calculate predicted delivery time for this bundle
                    predicted_delivery_time = calculate_predicted_delivery_time(
                        courier, order_ids, state
                    )

                    # Find the strictest deadline in the bundle
                    # (All orders must be delivered before their deadline expires)
                    strictest_deadline = min(
                        state.orders[oid].ready_time + state.orders[oid].expiration_time
                        for oid in order_ids
                    )

                    if predicted_delivery_time > strictest_deadline:
                        # Assignment is IMPOSSIBLE - the courier cannot deliver on time
                        # Reject this assignment to prevent order expiration
                        state.log_event(
                            'DEADLINE_INFEASIBLE_ASSIGNMENT_REJECTED',
                            f'Algorithm proposed impossible assignment for courier {courier_id}. '
                            f'Predicted delivery @ t={predicted_delivery_time:.0f}s exceeds '
                            f'strictest deadline @ t={strictest_deadline:.0f}s. '
                            f'Orders: {order_ids}. Assignment rejected.',
                            courier_id=courier_id,
                            order_ids=order_ids,
                            predicted_delivery=predicted_delivery_time,
                            strictest_deadline=strictest_deadline,
                            margin=predicted_delivery_time - strictest_deadline
                        )
                        continue  # Skip this impossible assignment

                    # ============================================================
                    # VALIDATION 3: Final safety check on courier state
                    # ============================================================
                    if courier.state != "IDLE":
                        state.log_event('COURIER_STATE_CONFLICT',
                                       f'Courier {courier_id} state conflict: expected IDLE, '
                                       f'but state is {courier.state}. Assignment rejected.',
                                       courier_id=courier_id,
                                       expected_state="IDLE",
                                       actual_state=courier.state)
                        continue  # Skip this invalid assignment

                    courier.assigned_order_ids = list(order_ids)

                    # Build multi-restaurant route plan
                    orders = [state.orders[oid] for oid in order_ids]

                    # Group orders by restaurant
                    by_restaurant = {}
                    for order in orders:
                        rest_id = order.restaurant_id
                        if rest_id not in by_restaurant:
                            by_restaurant[rest_id] = []
                        by_restaurant[rest_id].append(order.id)

                    # Build route with restaurant visits
                    courier.pickup_route = []
                    for rest_id, rest_order_ids in by_restaurant.items():
                        courier.pickup_route.append({
                            'restaurant_id': rest_id,
                            'location': state.restaurants[rest_id].location,
                            'order_ids': rest_order_ids
                        })

                    # Set initial state to go to first restaurant
                    courier.state = "DRIVING_TO_PICKUP"
                    courier.current_pickup_index = 0
                    first_pickup = courier.pickup_route[0]
                    courier.next_destination = first_pickup['location']
                    travel_time = get_travel_time(courier.current_location, courier.next_destination)
                    courier.arrival_time_at_destination = t + travel_time

                    # Update idle time before assignment
                    if courier.last_state_change_time > 0:
                        idle_duration = t - courier.last_state_change_time
                        courier.total_idle_time += idle_duration
                        state.metrics['total_courier_idle_time'] += idle_duration

                    # Mark orders as assigned
                    for order_id in order_ids:
                        order = state.orders[order_id]
                        order.state = "ASSIGNED"
                        order.assigned_courier_id = courier_id
                        order.assignment_time = t

                    # Track bundles (only count multi-order assignments as bundles)
                    if len(order_ids) > 1:
                        state.metrics['bundles_created'] += 1
                        state.metrics['total_bundle_size'] += len(order_ids)

                    state.log_event('ASSIGNMENT_MADE',
                                   f'Courier {courier_id} assigned orders {order_ids}',
                                   courier_id=courier_id,
                                   order_ids=order_ids,
                                   bundle_size=len(order_ids))

            next_batch_time += batch_interval

        # 5. SNAPSHOT STATE (every second for smooth animation)
        if t % 1 == 0:  # Changed from 10 to 1 for smooth movement
            state.timeline.append(state.snapshot())

    # Update idle time for couriers still idle at end
    for courier in state.couriers.values():
        if courier.state == "IDLE" and courier.last_state_change_time < sim_duration:
            idle_duration = sim_duration - courier.last_state_change_time
            courier.total_idle_time += idle_duration
            state.metrics['total_courier_idle_time'] += idle_duration

    # Compute final metrics
    state.compute_final_metrics()

    state.log_event('SIMULATION_END', 'Simulation completed',
                    final_metrics=state.metrics)

    return state
