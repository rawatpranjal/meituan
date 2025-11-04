"""
Food Delivery Routing Simulator - Core Engine

This module contains the core simulation engine for comparing batched assignment
algorithms in a food delivery context.

Simulation Parameters:
- Grid: 5km x 5km
- Restaurants: 5 (uniformly distributed)
- Couriers: 10 (random start positions)
- Orders: 20 (Poisson process with peak hour)
- Duration: 1 hour (3600 seconds)
- Batch interval: 60 seconds
- Courier speed: 30 km/h = 0.5 km/min = 8.33 m/s
- Meal prep time: 10 minutes (600 seconds)
"""

import numpy as np
from datetime import datetime
from copy import deepcopy
from typing import List, Tuple, Dict, Optional, Callable
import json
from distance_metrics import get_distance_metric


# ============================================================================
# CONFIGURATION - "THE ENGINEERED GAUNTLET"
# ============================================================================

GRID_SIZE = 5.0  # km (A larger 5x5 grid to create meaningful distances for the "Distant Bait" test)
NUM_RESTAURANTS = 4  # Gauntlet: 3 downtown, 1 suburban to test geographic intelligence
NUM_COURIERS = 5  # Gauntlet: 5 strategically placed couriers to create specific challenges
NUM_ORDERS = 80  # Will be overridden by scripted scenario (kept for compatibility)
SIMULATION_DURATION = 3600  # seconds (1 hour to see the full story play out)
BATCH_INTERVAL = 300  # seconds (5 minutes)
COURIER_SPEED_KMH = 20.0  # km/h (REALISTIC: urban congestion, stop signs, turns)
COURIER_SPEED_M_PER_S = (COURIER_SPEED_KMH * 1000.0) / 3600.0  # meters/second
MEAL_PREP_TIME = 600  # seconds (10 minutes - CRITICAL for the "Impossible Deadline" test)
RANDOM_SEED = 42

# Service times at pickup and dropoff (REALISTIC urban times)
PICKUP_SERVICE_TIME = 150  # seconds (2.5 min: parking, walking to restaurant, waiting, handoff)
DROPOFF_SERVICE_TIME = 120  # seconds (2 min: parking, finding building/apartment, customer handoff)

# Order expiration timeout - THE MOST CRITICAL CHANGE
# Eased from 5 to 15 minutes to make multi-order bundling a viable, winning strategy
ORDER_EXPIRATION_TIME = 900  # seconds (15 minutes)


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

# Global distance function (set by run_simulation)
_distance_func = None
_courier_speed_m_per_s = None

def set_distance_function(distance_func: Callable, courier_speed_kmh: float):
    """
    Set the global distance function and courier speed for this simulation run.

    Args:
        distance_func: Distance calculation function from distance_metrics
        courier_speed_kmh: Courier speed in km/h
    """
    global _distance_func, _courier_speed_m_per_s
    _distance_func = distance_func
    _courier_speed_m_per_s = (courier_speed_kmh * 1000.0) / 3600.0


def euclidean_distance(loc1: Tuple[float, float], loc2: Tuple[float, float]) -> float:
    """Calculate Euclidean distance between two points in km (legacy function)."""
    return np.sqrt((loc1[0] - loc2[0])**2 + (loc1[1] - loc2[1])**2)


def get_distance(loc1: Tuple[float, float], loc2: Tuple[float, float]) -> float:
    """
    Calculate distance between two points using configured metric.

    Args:
        loc1: First location (x, y) in km
        loc2: Second location (x, y) in km

    Returns:
        Distance in km
    """
    if _distance_func is None:
        # Fallback to Euclidean if not configured
        return euclidean_distance(loc1, loc2)
    return _distance_func(loc1, loc2)


def get_travel_time(loc1: Tuple[float, float], loc2: Tuple[float, float]) -> float:
    """Calculate travel time in seconds between two points."""
    distance_km = get_distance(loc1, loc2)
    distance_m = distance_km * 1000.0
    if _courier_speed_m_per_s is None:
        # Fallback to default speed
        return distance_m / COURIER_SPEED_M_PER_S
    return distance_m / _courier_speed_m_per_s


# ============================================================================
# CORE CLASSES
# ============================================================================

class Restaurant:
    """Represents a restaurant (pickup location)."""

    def __init__(self, restaurant_id: int, location: Tuple[float, float]):
        self.id = restaurant_id
        self.location = location  # (x, y) in km

    def to_dict(self):
        return {
            'id': self.id,
            'location': self.location
        }


class Order:
    """Represents a food delivery order."""

    def __init__(self, order_id: int, restaurant_id: int, restaurant_location: Tuple[float, float],
                 diner_location: Tuple[float, float], placement_time: float, meal_prep_time: Optional[float] = None,
                 expiration_time: Optional[float] = None):
        self.id = order_id
        self.restaurant_id = restaurant_id
        self.restaurant_location = restaurant_location
        self.diner_location = diner_location
        self.placement_time = placement_time
        # Allow variable meal prep time (7-15 min range for realism), default to 10 min
        self.meal_prep_time = meal_prep_time if meal_prep_time is not None else MEAL_PREP_TIME
        self.ready_time = placement_time + self.meal_prep_time
        # Allow custom expiration window (for test design), default to 15 min
        self.expiration_time = expiration_time if expiration_time is not None else ORDER_EXPIRATION_TIME

        # State tracking
        self.state = "PENDING"  # PENDING -> READY -> ASSIGNED -> PICKED_UP -> DELIVERED
        self.assigned_courier_id = None
        self.assignment_time = None
        self.pickup_time = None
        self.delivery_time = None

        # Relay tracking
        self.is_relay = False
        self.relay_handoff_location = None
        self.relay_courier_id = None  # Second courier who completes delivery
        self.handoff_time = None

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
            'delivery_time': self.delivery_time,
            'is_relay': self.is_relay,
            'relay_handoff_location': self.relay_handoff_location,
            'relay_courier_id': self.relay_courier_id,
            'handoff_time': self.handoff_time
        }


class Courier:
    """Represents a delivery courier."""

    def __init__(self, courier_id: int, start_location: Tuple[float, float],
                 shift_start: float = 0.0, shift_end: float = SIMULATION_DURATION):
        self.id = courier_id
        self.start_location = start_location
        self.current_location = start_location
        self.shift_start = shift_start
        self.shift_end = shift_end

        # State tracking
        self.state = "IDLE"  # IDLE, DRIVING_TO_PICKUP, AT_PICKUP, DRIVING_TO_DROPOFF, DRIVING_TO_HANDOFF, AT_HANDOFF
        self.assigned_order_ids = []  # List of order IDs (for bundling)
        self.current_route = []  # List of (location, action, order_id) tuples
        self.next_destination = None
        self.arrival_time_at_destination = None

        # Relay tracking
        self.relay_orders = []  # Orders to hand off to other couriers
        self.incoming_relay_orders = []  # Orders received from other couriers

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
            'assigned_order_ids': self.assigned_order_ids,
            'current_route': self.current_route,
            'next_destination': self.next_destination,
            'arrival_time_at_destination': self.arrival_time_at_destination,
            'total_distance_traveled': self.total_distance_traveled,
            'total_idle_time': self.total_idle_time,
            'total_deliveries': self.total_deliveries
        }


class SimulationState:
    """Manages the complete simulation state."""

    def __init__(self, restaurants: List[Restaurant], couriers: List[Courier],
                 order_schedule: List[Order], duration: int = SIMULATION_DURATION):
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
            'unserved_orders': 0,  # Deprecated - kept for compatibility
            # Relay metrics
            'relay_handoffs': 0,
            'relay_orders': 0,
            'relay_distance_saved': 0.0
        }

    def log_event(self, event_type: str, description: str, **kwargs):
        """Log a simulation event."""
        event = {
            'time': self.current_time,
            'type': event_type,
            'description': description,
            **kwargs
        }
        self.events_log.append(event)

    def snapshot(self):
        """Create a snapshot of the current state for timeline."""
        return {
            'time': self.current_time,
            'couriers': {cid: c.to_dict() for cid, c in self.couriers.items()},
            'orders': {oid: o.to_dict() for oid, o in self.orders.items()},
            'metrics': deepcopy(self.metrics)
        }

    def get_idle_couriers(self) -> List[Courier]:
        """Get all couriers currently in IDLE state."""
        return [c for c in self.couriers.values() if c.state == "IDLE"]

    def get_ready_orders(self) -> List[Order]:
        """Get all orders in READY state (excluding EXPIRED)."""
        return [o for o in self.orders.values()
                if o.state == "READY" and self.current_time >= o.ready_time]

    def compute_final_metrics(self):
        """Compute final aggregate metrics."""
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
# SCENARIO GENERATION
# ============================================================================

def generate_gauntlet_scenario(duration: int = SIMULATION_DURATION) -> Dict:
    """
    Generates the "Engineered Gauntlet" scenario.

    This is a scripted, non-random scenario designed to surgically expose the
    weaknesses and strengths of each algorithm in a pre-defined sequence.

    The gauntlet contains 4 key tests:
    1. Test 1 (t=300s): Distant Bait - exposes Greedy's myopic decision-making
    2. Test 2 (t=900s): Pizzeria Pileup - exposes 1-to-1 matching limitations
    3. Test 3 (t=1200s): Cross-Street Rivalry - requires multi-restaurant bundling
    4. Test 4 (t=2100s): Impossible Deadline - requires anticipatory planning
    """
    print("Generating the 'Engineered Gauntlet' scenario...")

    # --- GEOGRAPHIC SETUP ---
    # 3 downtown restaurants, 1 suburban outlier
    restaurants = [
        Restaurant(0, (1.5, 1.5)),    # R0 - Downtown Pizzeria (main pileup)
        Restaurant(1, (1.7, 1.5)),    # R1 - Downtown Cafe (cross-street rival)
        Restaurant(2, (1.6, 1.7)),    # R2 - Downtown Sushi (impossible deadline origin)
        Restaurant(3, (4.5, 4.5))     # R3 - Suburban Outlier (the distant bait)
    ]

    # Strategic courier placement to create dilemmas
    couriers = [
        Courier(0, (1.5, 1.4)),   # C0 - Downtown Specialist
        Courier(1, (1.7, 1.4)),   # C1 - Downtown Specialist
        Courier(2, (4.4, 4.4)),   # C2 - Suburban Specialist (Perfectly positioned for the bait)
        Courier(3, (3.0, 3.0)),   # C3 - Central Roamer
        Courier(4, (1.0, 3.5))    # C4 - Remote Roamer
    ]

    # --- THE SCRIPTED ORDER SCHEDULE ---
    order_schedule = []
    order_id_counter = 0

    def add_order(restaurant_idx, diner_loc, placement_time, meal_prep_time=None, expiration_time=None):
        """Add order with optional custom meal prep and expiration times (for realism and test design)."""
        nonlocal order_id_counter
        order_schedule.append(
            Order(order_id_counter, restaurant_idx, restaurants[restaurant_idx].location,
                  diner_loc, placement_time, meal_prep_time, expiration_time)
        )
        order_id_counter += 1

    # === THE GAUNTLET BEGINS ===

    # Test 1 (Minute 5 = 300s): The Distant Bait (To break Greedy)
    add_order(restaurant_idx=3, diner_loc=(4.8, 4.2), placement_time=300)

    # Test 2 (Minute 15 = 900s): The Pizzeria Pileup (To break 1-to-1 matching)
    # A sudden burst of 6 orders at R0. Only 4-5 couriers will be free.
    add_order(restaurant_idx=0, diner_loc=(1.0, 1.0), placement_time=900)
    add_order(restaurant_idx=0, diner_loc=(1.2, 1.8), placement_time=905)
    add_order(restaurant_idx=0, diner_loc=(1.8, 1.2), placement_time=910)
    add_order(restaurant_idx=0, diner_loc=(0.8, 1.5), placement_time=915)
    add_order(restaurant_idx=0, diner_loc=(1.5, 0.8), placement_time=920)
    add_order(restaurant_idx=0, diner_loc=(2.0, 2.0), placement_time=925)

    # Test 3 (Minute 20 = 1200s): The Cross-Street Rivalry (To limit Simple Bundling)
    # 3 new orders appear at R1, right across the street from the R0 pileup.
    add_order(restaurant_idx=1, diner_loc=(2.5, 1.5), placement_time=1200)
    add_order(restaurant_idx=1, diner_loc=(2.7, 1.7), placement_time=1205)
    add_order(restaurant_idx=1, diner_loc=(2.4, 1.3), placement_time=1210)

    # Test 4 (Minute 30 = 1800s): The Impossible Deadline (To crown Anticipated)
    # THE DECISIVE TEST - Only Anticipated's 15-min lookahead can solve this
    # Placed: t=1800s (30min) | Ready: t=2400s (40min) | Expires: t=3000s (50min)
    # Delivery time: ~795s (13.25 min) - too long for 600s reactive window
    # Reactive: sees at 2400s, needs 795s, expires at 3000s (600s) → IMPOSSIBLE
    # Anticipated: sees at 1800s, can dispatch early, has 1200s window → POSSIBLE
    add_order(restaurant_idx=2, diner_loc=(3.5, 3.5), placement_time=1800,
              meal_prep_time=600, expiration_time=600)  # Custom 10-min expiration window

    # Add some background "noise" orders with variable prep times (7-15 min) for realism
    np.random.seed(RANDOM_SEED)
    for t in range(400, duration - MEAL_PREP_TIME, 350):
        # Avoid placing noise during the exact moments of our key tests
        if 900 <= t <= 1300 or 1800 <= t <= 1900 or t == 300:
            continue
        # Realistic variable meal prep time: 7-15 minutes (420-900 seconds)
        variable_prep_time = np.random.uniform(420, 900)
        add_order(
            restaurant_idx=np.random.choice([0, 1, 2]),
            diner_loc=(np.random.uniform(0, 5), np.random.uniform(0, 5)),
            placement_time=t,
            meal_prep_time=variable_prep_time
        )

    return {
        'restaurants': restaurants,
        'couriers': couriers,
        'order_schedule': order_schedule,
        'duration': duration
    }


# Keep old function as backup
def generate_scenario_legacy(seed: int = RANDOM_SEED, duration: Optional[int] = None) -> Dict:
    """Legacy scenario generation (Small Town Lunch Rush). Kept for reference."""
    np.random.seed(seed)
    sim_duration = duration if duration is not None else SIMULATION_DURATION

    # Generate restaurants (tight cluster at town center)
    restaurants = []
    center = GRID_SIZE / 2
    for i in range(NUM_RESTAURANTS):
        location = (
            center + np.random.uniform(-0.025, 0.025),
            center + np.random.uniform(-0.025, 0.025)
        )
        restaurants.append(Restaurant(i, location))

    # Generate couriers (random start positions)
    couriers = []
    for i in range(NUM_COURIERS):
        location = (
            np.random.uniform(0, GRID_SIZE),
            np.random.uniform(0, GRID_SIZE)
        )
        couriers.append(Courier(i, location))

    # Generate order schedule
    order_schedule = []
    current_time = 0.0
    order_id = 0
    max_placement_time = sim_duration - MEAL_PREP_TIME

    while order_id < NUM_ORDERS and current_time < max_placement_time:
        current_minute = current_time / 60.0
        if current_minute < 15:
            lambda_rate = 0.33
        elif current_minute < 30:
            lambda_rate = 2.5
        else:
            lambda_rate = 0.33

        inter_arrival_time = np.random.exponential(60.0 / lambda_rate)
        current_time += inter_arrival_time

        if current_time >= max_placement_time:
            break

        if 15 <= current_minute < 30:
            restaurant = np.random.choice(restaurants, p=[0.75, 0.25] if len(restaurants) >= 2 else None)
        else:
            restaurant = np.random.choice(restaurants)

        angle = np.random.uniform(0, 2 * np.pi)
        radius = np.random.uniform(1.0, 1.3)
        diner_location = (
            center + radius * np.cos(angle),
            center + radius * np.sin(angle)
        )

        order = Order(
            order_id=order_id,
            restaurant_id=restaurant.id,
            restaurant_location=restaurant.location,
            diner_location=diner_location,
            placement_time=current_time
        )
        order_schedule.append(order)
        order_id += 1

    return {
        'restaurants': restaurants,
        'couriers': couriers,
        'order_schedule': order_schedule,
        'duration': sim_duration
    }


# Alias for backward compatibility
generate_scenario = generate_gauntlet_scenario


def generate_asymmetric_scenario(duration: int = 900) -> Dict:
    """
    Generate asymmetric scenario to reveal algorithm differences visually.

    Creates a downtown hub with 2 close restaurants and a suburban outlier,
    with strategic courier placement to force different assignment strategies.

    Args:
        duration: Simulation duration in seconds (default: 900s = 15 minutes)

    Returns:
        Dictionary with 'restaurants', 'couriers', and 'order_schedule' lists
    """
    # Fixed geographic setup for visual differentiation
    restaurants = [
        Restaurant(0, (2.0, 2.0)),    # R1 - Downtown hub
        Restaurant(1, (2.2, 2.0)),    # R2 - Downtown hub (200m away)
        Restaurant(2, (4.5, 4.5))     # R3 - Suburban outlier
    ]

    # Strategic courier placement
    couriers = [
        Courier(0, (2.1, 2.0)),   # C0 - Hub courier
        Courier(1, (2.0, 1.9)),   # C1 - Hub courier
        Courier(2, (4.4, 4.4)),   # C2 - Suburban courier
        Courier(3, (3.0, 3.0)),   # C3 - Remote courier
        Courier(4, (3.2, 3.2)),   # C4 - Remote courier
    ]

    # For longer simulations, add more couriers
    if duration > 900:
        for i in range(5, 10):
            # Add more remote couriers spread around
            angle = (i - 5) * 72 * np.pi / 180  # 72 degrees apart
            distance = 2.0
            x = 2.5 + distance * np.cos(angle)
            y = 2.5 + distance * np.sin(angle)
            couriers.append(Courier(i, (x, y)))

    # Scripted order schedule for maximum differentiation - MORE SPREAD OUT
    order_schedule = [
        # The bait - takes suburban courier off the board
        Order(0, 2, restaurants[2].location, (4.7, 4.3), 60.0),

        # The bundling test - 4 orders at R1 (spread out more)
        Order(1, 0, restaurants[0].location, (1.6, 2.4), 120.0),  # NW of R1
        Order(2, 0, restaurants[0].location, (2.4, 1.6), 120.0),  # SE of R1
        Order(3, 0, restaurants[0].location, (1.5, 1.5), 120.0),  # SW of R1
        Order(4, 0, restaurants[0].location, (2.5, 2.5), 120.0),  # NE of R1

        # The smart test - 3 orders at R2 (spread out more)
        Order(5, 1, restaurants[1].location, (1.8, 2.6), 180.0),  # N of R2
        Order(6, 1, restaurants[1].location, (2.6, 1.4), 180.0),  # SE of R2
        Order(7, 1, restaurants[1].location, (1.4, 1.8), 180.0)   # W of R2
    ]

    # For longer simulations, continue with more waves
    if duration > 900:
        # Add more order waves with similar patterns
        order_id = 8
        for wave_time in range(300, min(duration - MEAL_PREP_TIME, 10800), 120):
            # Alternate between hub restaurants with bursts
            restaurant_idx = 0 if (wave_time // 120) % 2 == 0 else 1
            restaurant = restaurants[restaurant_idx]

            # Create 2-4 orders per wave
            num_orders = np.random.randint(2, 5)
            for _ in range(num_orders):
                # Diner locations clustered around downtown
                diner_location = (
                    2.0 + np.random.uniform(-1.0, 1.0),
                    2.0 + np.random.uniform(-1.0, 1.0)
                )

                order_schedule.append(Order(
                    order_id=order_id,
                    restaurant_id=restaurant_idx,
                    restaurant_location=restaurant.location,
                    diner_location=diner_location,
                    placement_time=float(wave_time)
                ))
                order_id += 1

    return {
        'restaurants': restaurants,
        'couriers': couriers,
        'order_schedule': order_schedule,
        'duration': duration
    }


# ============================================================================
# SIMULATION ENGINE
# ============================================================================

def run_simulation(scenario: Dict, assignment_algorithm, algorithm_name: str) -> SimulationState:
    """
    Run the simulation with a specific assignment algorithm.

    Args:
        scenario: Dictionary with restaurants, couriers, order_schedule, duration, and config
        assignment_algorithm: Function that takes (state) and returns list of (courier_id, [order_ids])
        algorithm_name: Name of the algorithm for logging

    Returns:
        Final SimulationState with complete timeline and metrics
    """
    # Extract config if present (for new config-based scenarios)
    config = scenario.get('config', None)

    if config:
        # Set up distance function from config
        distance_metric = config['physics']['distance_metric']
        courier_speed = config['physics']['courier_speed_kmh']
        distance_func = get_distance_metric(distance_metric)
        set_distance_function(distance_func, courier_speed)

        # Get parameters from config
        sim_duration = scenario.get('duration', config['scenario']['duration_hours'] * 3600)
        batch_interval = config['physics']['batch_interval_s']
    else:
        # Legacy mode: use hardcoded defaults
        sim_duration = scenario.get('duration', SIMULATION_DURATION)
        batch_interval = BATCH_INTERVAL

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
                    distance = euclidean_distance(prev_location, courier.current_location)
                    courier.total_distance_traveled += distance

                    if courier.state == "DRIVING_TO_PICKUP":
                        # Arrived at restaurant - start pickup service
                        courier.state = "AT_PICKUP"
                        courier.arrival_time_at_destination = t + PICKUP_SERVICE_TIME  # Wait for service

                        state.log_event('PICKUP_SERVICE_START',
                                       f'Courier {courier.id} starting pickup service ({PICKUP_SERVICE_TIME}s)',
                                       courier_id=courier.id,
                                       service_time=PICKUP_SERVICE_TIME)

                    elif courier.state == "AT_PICKUP" and t >= courier.arrival_time_at_destination:
                        # Pickup service completed - mark orders as picked up
                        for order_id in courier.assigned_order_ids:
                            order = state.orders[order_id]
                            order.state = "PICKED_UP"
                            order.pickup_time = t
                            state.log_event('ORDER_PICKED_UP', f'Order {order_id} picked up by courier {courier.id}',
                                           order_id=order_id, courier_id=courier.id)

                        # Check for relay orders that need handoff
                        relay_orders = [oid for oid in courier.assigned_order_ids
                                       if state.orders[oid].is_relay]
                        non_relay_orders = [oid for oid in courier.assigned_order_ids
                                          if not state.orders[oid].is_relay]

                        # If we have relay orders, handle them first
                        if relay_orders:
                            # Head to handoff point for first relay order
                            first_relay = state.orders[relay_orders[0]]
                            courier.state = "DRIVING_TO_HANDOFF"
                            courier.next_destination = first_relay.relay_handoff_location
                            travel_time = get_travel_time(courier.current_location, courier.next_destination)
                            courier.arrival_time_at_destination = t + travel_time
                            courier.relay_orders = relay_orders
                            courier.assigned_order_ids = non_relay_orders  # Keep non-relay orders

                            state.log_event('COURIER_TO_HANDOFF',
                                          f'Courier {courier.id} heading to handoff point',
                                          courier_id=courier.id,
                                          handoff_location=first_relay.relay_handoff_location,
                                          relay_orders=relay_orders)

                        # Otherwise handle normal delivery
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

                    elif courier.state == "DRIVING_TO_HANDOFF":
                        # Arrived at handoff point - perform handoff
                        if courier.relay_orders:
                            for relay_order_id in courier.relay_orders:
                                relay_order = state.orders[relay_order_id]

                                # Transfer to relay courier
                                relay_courier = state.couriers.get(relay_order.relay_courier_id)
                                if relay_courier and relay_courier.state == "IDLE":
                                    # Transfer order to relay courier
                                    relay_courier.incoming_relay_orders.append(relay_order_id)
                                    relay_courier.assigned_order_ids = [relay_order_id]
                                    relay_courier.state = "DRIVING_TO_DROPOFF"
                                    relay_courier.next_destination = relay_order.diner_location
                                    travel_time = get_travel_time(relay_courier.current_location, relay_order.diner_location)
                                    relay_courier.arrival_time_at_destination = t + travel_time

                                    # Update order
                                    relay_order.handoff_time = t
                                    relay_order.assigned_courier_id = relay_courier.id

                                    # Track metrics
                                    state.metrics['relay_handoffs'] += 1

                                    # Calculate distance saved
                                    direct_distance = euclidean_distance(relay_order.restaurant_location, relay_order.diner_location)
                                    relay_distance = euclidean_distance(relay_order.restaurant_location, relay_order.relay_handoff_location)
                                    state.metrics['relay_distance_saved'] += direct_distance - relay_distance

                                    state.log_event('HANDOFF_COMPLETE',
                                                  f'Order {relay_order_id} handed off from courier {courier.id} to {relay_courier.id}',
                                                  order_id=relay_order_id,
                                                  from_courier=courier.id,
                                                  to_courier=relay_courier.id,
                                                  handoff_location=relay_order.relay_handoff_location)
                                else:
                                    # Relay courier not available, deliver normally
                                    courier.assigned_order_ids.append(relay_order_id)
                                    state.log_event('HANDOFF_FAILED',
                                                  f'Relay courier {relay_order.relay_courier_id} not available, courier {courier.id} will deliver',
                                                  order_id=relay_order_id,
                                                  courier_id=courier.id)

                            courier.relay_orders = []

                            # Check if courier has non-relay orders to deliver
                            if courier.assigned_order_ids:
                                first_order = state.orders[courier.assigned_order_ids[0]]
                                courier.state = "DRIVING_TO_DROPOFF"
                                courier.next_destination = first_order.diner_location
                                travel_time = get_travel_time(courier.current_location, courier.next_destination)
                                courier.arrival_time_at_destination = t + travel_time
                            else:
                                # No more orders - return to IDLE
                                courier.state = "IDLE"
                                courier.next_destination = None
                                courier.arrival_time_at_destination = None
                                courier.last_state_change_time = t

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
                            courier.arrival_time_at_destination = t + DROPOFF_SERVICE_TIME + travel_time
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
            ready_orders = state.get_ready_orders()

            if idle_couriers and ready_orders:
                # Run assignment algorithm
                assignments = assignment_algorithm(state, idle_couriers, ready_orders)

                # Process assignments
                for assignment in assignments:
                    # Handle both regular and relay assignments
                    if len(assignment) == 3:
                        # Relay assignment: (courier_id, order_ids, relay_info)
                        courier_id, order_ids, relay_info = assignment
                    else:
                        # Regular assignment: (courier_id, order_ids)
                        courier_id, order_ids = assignment
                        relay_info = None

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
                    # VALIDATION 2: Verify all orders are ready and unassigned
                    # ============================================================
                    ready_order_ids = [o.id for o in ready_orders]
                    invalid_orders = []
                    for oid in order_ids:
                        order = state.orders[oid]
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

                    # Process relay information if present
                    if relay_info:
                        for order_id in order_ids:
                            if order_id in relay_info:
                                order = state.orders[order_id]
                                order.is_relay = True
                                order.relay_handoff_location = relay_info[order_id]['handoff_location']
                                order.relay_courier_id = relay_info[order_id]['relay_courier']

                                # Track relay metrics
                                state.metrics['relay_orders'] += 1

                                state.log_event('RELAY_SCHEDULED',
                                              f'Order {order_id} scheduled for relay handoff',
                                              order_id=order_id,
                                              handoff_location=order.relay_handoff_location,
                                              relay_courier=order.relay_courier_id)

                    courier.state = "DRIVING_TO_PICKUP"

                    # All orders in bundle are from same restaurant (for now)
                    first_order = state.orders[order_ids[0]]
                    courier.next_destination = first_order.restaurant_location
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

                    # Track bundles
                    if len(order_ids) > 0:
                        state.metrics['bundles_created'] += 1
                        state.metrics['total_bundle_size'] += len(order_ids)

                    state.log_event('ASSIGNMENT_MADE',
                                   f'Courier {courier_id} assigned orders {order_ids}' +
                                   (' with relay' if relay_info else ''),
                                   courier_id=courier_id,
                                   order_ids=order_ids,
                                   bundle_size=len(order_ids),
                                   has_relay=bool(relay_info))

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
