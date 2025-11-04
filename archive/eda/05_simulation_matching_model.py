"""
Batch Matching Simulation: Comparing Greedy, Hungarian, VRP-base, and VRP-fresh

Simulates a single dispatch wave with 50 orders and 40 couriers distributed
across a 3x3 spatial grid, comparing four assignment algorithms:
1. Batch Greedy (order-first heuristic, 1-to-1)
2. Batch Hungarian (optimal 1-to-1 matching)
3. Batch VRP-base (pickup-delivery routing, no freshness constraint)
4. Batch VRP-fresh (pickup-delivery routing with ride-time ≤ T_max)

Objective: Maximize served orders, then minimize total route distance
Cost: L_ij = d(courier_j, restaurant_i) + d(restaurant_i, customer_i)
Freshness: Ride time (pickup → delivery) ≤ T_MAX (VRP-fresh only)
"""

import numpy as np
import matplotlib.pyplot as plt
from scipy.optimize import linear_sum_assignment
from scipy.spatial import distance_matrix
import sys
import time
from datetime import datetime
from collections import defaultdict

# OR-Tools imports
try:
    from ortools.constraint_solver import routing_enums_pb2
    from ortools.constraint_solver import pywrapcp
except ImportError:
    print("ERROR: OR-Tools not installed. Install with: pip3 install --break-system-packages ortools")
    sys.exit(1)

# Setup logging
log_file = f"/Users/pranjal/Code/meituan/eda/logs/05_simulation_matching_model_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
log = open(log_file, 'w')
sys.stdout = log

print("=" * 80)
print("BATCH MATCHING SIMULATION")
print("Comparing Batch Greedy, Hungarian, VRP-base, and VRP-fresh")
print("=" * 80)

# ============================================================================
# SECTION 1: SIMULATION PARAMETERS
# ============================================================================
print("\n[SECTION 1] SIMULATION PARAMETERS")
print("-" * 80)

CITY_SIZE = 10.0  # km (10x10 km city)
GRID_CELLS = 3    # 3x3 grid
NUM_RESTAURANTS = 8  # More restaurants for realistic clustering
NUM_ORDERS = 80  # Scaled up to show bundling benefits
NUM_COURIERS = 30  # Scaled up (2.67:1 ratio maintains undersupply)
COURIER_CAPACITY = 3  # K=3 for VRP (more realistic than 5)
COURIER_SPEED_KMH = 30  # 30 km/h average speed
T_MAX_MINUTES = 30  # Maximum ride time for freshness (30 minutes - relaxed)
RANDOM_SEED = 42
BIG_M = 1000000.0  # Penalty for unserved orders in Hungarian

# Convert parameters
COURIER_SPEED_KM_PER_MIN = COURIER_SPEED_KMH / 60.0  # 0.5 km/min
T_MAX_SECONDS = T_MAX_MINUTES * 60  # 1200 seconds

np.random.seed(RANDOM_SEED)

print(f"City size: {CITY_SIZE}km x {CITY_SIZE}km")
print(f"Grid: {GRID_CELLS}x{GRID_CELLS} cells ({CITY_SIZE/GRID_CELLS:.2f}km per cell)")
print(f"Restaurants: {NUM_RESTAURANTS}")
print(f"Orders: {NUM_ORDERS}")
print(f"Couriers: {NUM_COURIERS}")
print(f"Courier capacity (VRP): K={COURIER_CAPACITY} orders")
print(f"Courier speed: {COURIER_SPEED_KMH} km/h ({COURIER_SPEED_KM_PER_MIN:.2f} km/min)")
print(f"Max ride time (VRP-fresh): T_max={T_MAX_MINUTES} min ({T_MAX_SECONDS} sec)")
print(f"Random seed: {RANDOM_SEED}")

# ============================================================================
# SECTION 2: GENERATING SYNTHETIC DATA
# ============================================================================
print("\n[SECTION 2] GENERATING SYNTHETIC DATA")
print("-" * 80)

# Generate restaurant locations (green stars)
restaurant_x = np.random.uniform(0, CITY_SIZE, NUM_RESTAURANTS)
restaurant_y = np.random.uniform(0, CITY_SIZE, NUM_RESTAURANTS)
restaurant_locations = np.column_stack([restaurant_x, restaurant_y])
print(f"Generated {NUM_RESTAURANTS} restaurant locations")

# Generate orders (each assigned to a restaurant)
order_restaurant_idx = np.random.randint(0, NUM_RESTAURANTS, NUM_ORDERS)
order_restaurants = restaurant_locations[order_restaurant_idx]  # Pickup locations

# Generate customer locations (uniform random, independent of restaurants)
customer_x = np.random.uniform(0, CITY_SIZE, NUM_ORDERS)
customer_y = np.random.uniform(0, CITY_SIZE, NUM_ORDERS)
customer_locations = np.column_stack([customer_x, customer_y])
print(f"Generated {NUM_ORDERS} orders with random customer locations")

# Count orders per restaurant
restaurant_order_counts = np.bincount(order_restaurant_idx, minlength=NUM_RESTAURANTS)
print(f"Orders per restaurant: min={restaurant_order_counts.min()}, max={restaurant_order_counts.max()}, avg={restaurant_order_counts.mean():.1f}")

# Generate courier start positions (red triangles)
courier_x = np.random.uniform(0, CITY_SIZE, NUM_COURIERS)
courier_y = np.random.uniform(0, CITY_SIZE, NUM_COURIERS)
courier_locations = np.column_stack([courier_x, courier_y])
print(f"Generated {NUM_COURIERS} couriers at random start positions")

# Compute full-route cost matrix L_ij = d(courier_j, restaurant_i) + d(restaurant_i, customer_i)
courier_to_restaurant = distance_matrix(courier_locations, order_restaurants)
restaurant_to_customer = np.linalg.norm(order_restaurants - customer_locations, axis=1)
full_route_cost = courier_to_restaurant + restaurant_to_customer[np.newaxis, :]

print(f"Full-route cost matrix shape: {full_route_cost.shape}")
print(f"Cost range: min={full_route_cost.min():.2f}km, max={full_route_cost.max():.2f}km, mean={full_route_cost.mean():.2f}km")

# Grid cell assignment for visualization
def get_grid_cell(x, y):
    col = int(x / (CITY_SIZE / GRID_CELLS))
    row = int(y / (CITY_SIZE / GRID_CELLS))
    col = min(col, GRID_CELLS - 1)
    row = min(row, GRID_CELLS - 1)
    return (row, col)

orders_per_cell = defaultdict(int)
for i in range(NUM_ORDERS):
    cell = get_grid_cell(customer_x[i], customer_y[i])
    orders_per_cell[cell] += 1

couriers_per_cell = defaultdict(int)
for j in range(NUM_COURIERS):
    cell = get_grid_cell(courier_x[j], courier_y[j])
    couriers_per_cell[cell] += 1

print(f"Customer distribution: {dict(orders_per_cell)}")
print(f"Courier distribution: {dict(couriers_per_cell)}")

# ============================================================================
# SECTION 3: ALGORITHM 1 - BATCH GREEDY (ORDER-FIRST HEURISTIC)
# ============================================================================
print("\n[SECTION 3] ALGORITHM 1: BATCH GREEDY (ORDER-FIRST HEURISTIC)")
print("-" * 80)

greedy_start = time.time()

# Initialize unassigned orders and available couriers
unassigned_orders = list(range(NUM_ORDERS))
available_couriers = list(range(NUM_COURIERS))
greedy_assignments = []

# Greedy matching: pick order, assign to nearest courier by full route cost
while unassigned_orders and available_couriers:
    # Pick first order (oldest in real system)
    order_idx = unassigned_orders[0]

    # Find courier with minimum full route cost for this order
    best_courier = None
    best_cost = float('inf')
    for courier_idx in available_couriers:
        cost = full_route_cost[courier_idx, order_idx]
        if cost < best_cost:
            best_cost = cost
            best_courier = courier_idx

    # Make assignment
    greedy_assignments.append((order_idx, best_courier, best_cost))
    unassigned_orders.remove(order_idx)
    available_couriers.remove(best_courier)

greedy_time = (time.time() - greedy_start) * 1000  # ms

# Compute metrics
greedy_served = len(greedy_assignments)
greedy_total_dist = sum(cost for _, _, cost in greedy_assignments)
greedy_avg_dist = greedy_total_dist / greedy_served if greedy_served > 0 else 0
greedy_assign_rate = (greedy_served / NUM_ORDERS) * 100

# Compute pickup and delivery leg distances
greedy_pickup_dists = []
greedy_delivery_dists = []
for order_idx, courier_idx, _ in greedy_assignments:
    pickup_dist = courier_to_restaurant[courier_idx, order_idx]
    delivery_dist = restaurant_to_customer[order_idx]
    greedy_pickup_dists.append(pickup_dist)
    greedy_delivery_dists.append(delivery_dist)

greedy_avg_pickup = np.mean(greedy_pickup_dists) if greedy_pickup_dists else 0
greedy_avg_delivery = np.mean(greedy_delivery_dists) if greedy_delivery_dists else 0

print(f"Assignments: {greedy_served}")
print(f"Total route distance: {greedy_total_dist:.2f} km")
print(f"Average distance per order: {greedy_avg_dist:.2f} km")
print(f"Average pickup leg: {greedy_avg_pickup:.2f} km")
print(f"Average delivery leg: {greedy_avg_delivery:.2f} km")
print(f"Assignment rate: {greedy_assign_rate:.1f}%")
print(f"Computation time: {greedy_time:.2f} ms")

# ============================================================================
# SECTION 4: ALGORITHM 2 - HUNGARIAN (OPTIMAL 1-TO-1 WITH BIG-M)
# ============================================================================
print("\n[SECTION 4] ALGORITHM 2: HUNGARIAN (OPTIMAL 1-TO-1 MATCHING)")
print("-" * 80)

hungarian_start = time.time()

# Build cost matrix with dummy expansion
# full_route_cost is (couriers, orders) = (NUM_COURIERS, NUM_ORDERS)
m = NUM_ORDERS
n = NUM_COURIERS

if m > n:
    # More orders than couriers: add dummy couriers (rows) with penalty BIG_M
    dummy_rows = m - n
    dummy_cost = np.full((dummy_rows, m), BIG_M)
    cost_matrix = np.vstack([full_route_cost, dummy_cost])
    print(f"Added {dummy_rows} dummy couriers (penalty={BIG_M})")
elif m < n:
    # More couriers than orders: add dummy orders (cols) with cost 0
    dummy_cols = n - m
    dummy_cost = np.zeros((n, dummy_cols))
    cost_matrix = np.hstack([full_route_cost, dummy_cost])
    print(f"Added {dummy_cols} dummy orders (cost=0)")
else:
    # Balanced: no dummies needed
    cost_matrix = full_route_cost
    print("Balanced problem (m=n), no dummies needed")

print(f"Cost matrix shape for Hungarian: {cost_matrix.shape}")

# Run Hungarian algorithm
row_ind, col_ind = linear_sum_assignment(cost_matrix)

hungarian_time = (time.time() - hungarian_start) * 1000  # ms

# Extract real assignments (filter out dummy assignments)
hungarian_assignments = []
for courier_idx, order_idx in zip(row_ind, col_ind):
    # Check if this is a real assignment (not involving dummies)
    if courier_idx < NUM_COURIERS and order_idx < NUM_ORDERS:
        cost = full_route_cost[courier_idx, order_idx]
        if cost < BIG_M:  # Not a penalty assignment
            hungarian_assignments.append((order_idx, courier_idx, cost))

# Compute metrics
hungarian_served = len(hungarian_assignments)
hungarian_total_dist = sum(cost for _, _, cost in hungarian_assignments)
hungarian_avg_dist = hungarian_total_dist / hungarian_served if hungarian_served > 0 else 0
hungarian_assign_rate = (hungarian_served / NUM_ORDERS) * 100

# Compute pickup and delivery leg distances
hungarian_pickup_dists = []
hungarian_delivery_dists = []
for order_idx, courier_idx, _ in hungarian_assignments:
    pickup_dist = courier_to_restaurant[courier_idx, order_idx]
    delivery_dist = restaurant_to_customer[order_idx]
    hungarian_pickup_dists.append(pickup_dist)
    hungarian_delivery_dists.append(delivery_dist)

hungarian_avg_pickup = np.mean(hungarian_pickup_dists) if hungarian_pickup_dists else 0
hungarian_avg_delivery = np.mean(hungarian_delivery_dists) if hungarian_delivery_dists else 0

print(f"Assignments: {hungarian_served}")
print(f"Unserved orders: {NUM_ORDERS - hungarian_served}")
print(f"Total route distance: {hungarian_total_dist:.2f} km")
print(f"Average distance per order: {hungarian_avg_dist:.2f} km")
print(f"Average pickup leg: {hungarian_avg_pickup:.2f} km")
print(f"Average delivery leg: {hungarian_avg_delivery:.2f} km")
print(f"Assignment rate: {hungarian_assign_rate:.1f}%")
print(f"Computation time: {hungarian_time:.2f} ms")

# Compute optimality gap
if hungarian_total_dist > 0:
    optimality_gap = ((greedy_total_dist - hungarian_total_dist) / hungarian_total_dist) * 100
    print(f"Greedy optimality gap: +{optimality_gap:.1f}% worse than Hungarian")

# ============================================================================
# SECTION 5a: ALGORITHM 3 - VRP-BASE (MULTI-VEHICLE WITH CAPACITY K=3)
# ============================================================================
print("\n[SECTION 5a] ALGORITHM 3: VRP-BASE (MULTI-VEHICLE WITH CAPACITY)")
print("-" * 80)

vrp_base_start = time.time()

print(f"VRP-base solving ALL {NUM_ORDERS} orders with {NUM_COURIERS} couriers (capacity K={COURIER_CAPACITY})")

# Build locations: courier starts (depots), then all pickups, then all deliveries
locations = []
depot_indices = []
for courier_idx in range(NUM_COURIERS):
    locations.append(courier_locations[courier_idx])
    depot_indices.append(len(locations) - 1)

# Add all pickups
pickup_start_idx = len(locations)
for order_idx in range(NUM_ORDERS):
    locations.append(order_restaurants[order_idx])

# Add all deliveries
delivery_start_idx = len(locations)
for order_idx in range(NUM_ORDERS):
    locations.append(customer_locations[order_idx])

locations_array = np.array(locations)
num_nodes = len(locations)

# Distance matrix
route_dist_matrix = distance_matrix(locations_array, locations_array)
route_dist_matrix_int = (route_dist_matrix * 1000).astype(int)

# Create routing index manager: num_nodes, num_vehicles, starts, ends
manager = pywrapcp.RoutingIndexManager(num_nodes, NUM_COURIERS, depot_indices, depot_indices)
routing = pywrapcp.RoutingModel(manager)

# Define cost callback
def distance_callback_base(from_index, to_index):
    from_node = manager.IndexToNode(from_index)
    to_node = manager.IndexToNode(to_index)
    return route_dist_matrix_int[from_node][to_node]

transit_callback_index = routing.RegisterTransitCallback(distance_callback_base)
routing.SetArcCostEvaluatorOfAllVehicles(transit_callback_index)

# Add small fixed cost per vehicle (reduced to allow natural courier allocation)
routing.SetFixedCostOfAllVehicles(1000)  # 1km equivalent fixed cost per courier

# Add capacity dimension (each order = 1 unit, courier capacity = K)
def demand_callback(from_index):
    from_node = manager.IndexToNode(from_index)
    # Demand is 1 at pickup, -1 at delivery, 0 at depot
    if from_node < NUM_COURIERS:  # Depot
        return 0
    elif from_node < pickup_start_idx + NUM_ORDERS:  # Pickup
        return 1
    else:  # Delivery
        return -1

demand_callback_index = routing.RegisterUnaryTransitCallback(demand_callback)
routing.AddDimensionWithVehicleCapacity(
    demand_callback_index,
    0,  # null capacity slack
    [COURIER_CAPACITY] * NUM_COURIERS,  # vehicle capacities
    True,  # start cumul to zero
    'Capacity'
)

# Add pickup-delivery constraints
for order_idx in range(NUM_ORDERS):
    pickup_node = pickup_start_idx + order_idx
    delivery_node = delivery_start_idx + order_idx
    pickup_index = manager.NodeToIndex(pickup_node)
    delivery_index = manager.NodeToIndex(delivery_node)

    routing.AddPickupAndDelivery(pickup_index, delivery_index)
    routing.solver().Add(
        routing.VehicleVar(pickup_index) == routing.VehicleVar(delivery_index)
    )

# Set search parameters
search_parameters = pywrapcp.DefaultRoutingSearchParameters()
search_parameters.first_solution_strategy = (
    routing_enums_pb2.FirstSolutionStrategy.PARALLEL_CHEAPEST_INSERTION
)
search_parameters.time_limit.seconds = 60  # 60 second limit
search_parameters.local_search_metaheuristic = (
    routing_enums_pb2.LocalSearchMetaheuristic.GUIDED_LOCAL_SEARCH
)

# Solve
solution = routing.SolveWithParameters(search_parameters)

vrp_base_assignments = []
vrp_base_total_distance = 0
vrp_base_couriers_used = 0
courier_order_map_base = {}  # courier_idx -> list of order_idx

if solution:
    for vehicle_id in range(NUM_COURIERS):
        index = routing.Start(vehicle_id)
        route_distance = 0
        courier_orders = []

        while not routing.IsEnd(index):
            node = manager.IndexToNode(index)
            next_index = solution.Value(routing.NextVar(index))
            next_node = manager.IndexToNode(next_index)

            # Check if this node is a pickup
            if pickup_start_idx <= node < pickup_start_idx + NUM_ORDERS:
                order_idx = node - pickup_start_idx
                courier_orders.append(order_idx)
                vrp_base_assignments.append((order_idx, vehicle_id, 0))

            route_distance += route_dist_matrix[node][next_node]
            index = next_index

        if len(courier_orders) > 0:
            vrp_base_couriers_used += 1
            courier_order_map_base[vehicle_id] = courier_orders

        vrp_base_total_distance += route_distance

vrp_base_time = (time.time() - vrp_base_start) * 1000  # ms

# Compute metrics
vrp_base_served = len(vrp_base_assignments)
vrp_base_avg_dist = vrp_base_total_distance / vrp_base_served if vrp_base_served > 0 else 0
vrp_base_assign_rate = (vrp_base_served / NUM_ORDERS) * 100
vrp_base_avg_per_courier = vrp_base_total_distance / vrp_base_couriers_used if vrp_base_couriers_used > 0 else 0

# Compute bundle statistics
bundle_sizes_base = [len(orders) for orders in courier_order_map_base.values()]
avg_bundle_size_base = np.mean(bundle_sizes_base) if bundle_sizes_base else 0

print(f"Assignments: {vrp_base_served}")
print(f"Couriers used: {vrp_base_couriers_used}")
print(f"Avg orders per courier: {avg_bundle_size_base:.2f}")
print(f"Total route distance: {vrp_base_total_distance:.2f} km")
print(f"Average distance per order: {vrp_base_avg_dist:.2f} km")
print(f"Average route per courier: {vrp_base_avg_per_courier:.2f} km")
print(f"Assignment rate: {vrp_base_assign_rate:.1f}%")
print(f"Computation time: {vrp_base_time:.2f} ms")

# ============================================================================
# SECTION 5b: ALGORITHM 4 - VRP-FRESH (MULTI-VEHICLE WITH CAPACITY + FRESHNESS)
# ============================================================================
print("\n[SECTION 5b] ALGORITHM 4: VRP-FRESH (WITH RIDE-TIME CONSTRAINT)")
print("-" * 80)

vrp_fresh_start = time.time()

print(f"VRP-fresh solving ALL {NUM_ORDERS} orders with {NUM_COURIERS} couriers (capacity K={COURIER_CAPACITY})")
print(f"Freshness constraint: ride_time ≤ {T_MAX_MINUTES} minutes ({T_MAX_SECONDS} seconds)")

# Reuse the same location structure as VRP-base
# locations, depot_indices, pickup_start_idx, delivery_start_idx already defined

# Distance and time matrices
route_time_matrix = route_dist_matrix / COURIER_SPEED_KM_PER_MIN * 60  # seconds
route_time_matrix_int = route_time_matrix.astype(int)

# Create routing index manager
manager_fresh = pywrapcp.RoutingIndexManager(num_nodes, NUM_COURIERS, depot_indices, depot_indices)
routing_fresh = pywrapcp.RoutingModel(manager_fresh)

# Define cost callback
def distance_callback_fresh(from_index, to_index):
    from_node = manager_fresh.IndexToNode(from_index)
    to_node = manager_fresh.IndexToNode(to_index)
    return route_dist_matrix_int[from_node][to_node]

# Define time callback
def time_callback_fresh(from_index, to_index):
    from_node = manager_fresh.IndexToNode(from_index)
    to_node = manager_fresh.IndexToNode(to_index)
    return route_time_matrix_int[from_node][to_node]

transit_callback_index_fresh = routing_fresh.RegisterTransitCallback(distance_callback_fresh)
routing_fresh.SetArcCostEvaluatorOfAllVehicles(transit_callback_index_fresh)

# Add small fixed cost per vehicle (reduced to allow natural courier allocation)
routing_fresh.SetFixedCostOfAllVehicles(1000)  # 1km equivalent fixed cost per courier

# Add capacity dimension
def demand_callback_fresh(from_index):
    from_node = manager_fresh.IndexToNode(from_index)
    if from_node < NUM_COURIERS:
        return 0
    elif from_node < pickup_start_idx + NUM_ORDERS:
        return 1
    else:
        return -1

demand_callback_index_fresh = routing_fresh.RegisterUnaryTransitCallback(demand_callback_fresh)
routing_fresh.AddDimensionWithVehicleCapacity(
    demand_callback_index_fresh,
    0,
    [COURIER_CAPACITY] * NUM_COURIERS,
    True,
    'Capacity'
)

# Add time dimension
time_callback_index_fresh = routing_fresh.RegisterTransitCallback(time_callback_fresh)
routing_fresh.AddDimension(
    time_callback_index_fresh,
    0,  # no slack
    7200,  # max time per route (2 hours)
    True,  # start cumul to zero
    'Time'
)
time_dimension_fresh = routing_fresh.GetDimensionOrDie('Time')

# Add pickup-delivery constraints with freshness
for order_idx in range(NUM_ORDERS):
    pickup_node = pickup_start_idx + order_idx
    delivery_node = delivery_start_idx + order_idx
    pickup_index = manager_fresh.NodeToIndex(pickup_node)
    delivery_index = manager_fresh.NodeToIndex(delivery_node)

    routing_fresh.AddPickupAndDelivery(pickup_index, delivery_index)
    routing_fresh.solver().Add(
        routing_fresh.VehicleVar(pickup_index) == routing_fresh.VehicleVar(delivery_index)
    )

    # Freshness constraint: ride_time ≤ T_MAX
    pickup_time = time_dimension_fresh.CumulVar(pickup_index)
    delivery_time = time_dimension_fresh.CumulVar(delivery_index)
    routing_fresh.solver().Add(delivery_time - pickup_time <= T_MAX_SECONDS)

# Set search parameters
search_parameters_fresh = pywrapcp.DefaultRoutingSearchParameters()
search_parameters_fresh.first_solution_strategy = (
    routing_enums_pb2.FirstSolutionStrategy.PARALLEL_CHEAPEST_INSERTION
)
search_parameters_fresh.time_limit.seconds = 60  # Increased time limit
search_parameters_fresh.local_search_metaheuristic = (
    routing_enums_pb2.LocalSearchMetaheuristic.GUIDED_LOCAL_SEARCH
)

# Solve
solution_fresh = routing_fresh.SolveWithParameters(search_parameters_fresh)

vrp_fresh_assignments = []
vrp_fresh_total_distance = 0
vrp_fresh_couriers_used = 0
vrp_fresh_ride_times = []
courier_order_map_fresh = {}

if solution_fresh:
    for vehicle_id in range(NUM_COURIERS):
        index = routing_fresh.Start(vehicle_id)
        route_distance = 0
        courier_orders = []

        while not routing_fresh.IsEnd(index):
            node = manager_fresh.IndexToNode(index)
            next_index = solution_fresh.Value(routing_fresh.NextVar(index))
            next_node = manager_fresh.IndexToNode(next_index)

            # Check if this node is a pickup
            if pickup_start_idx <= node < pickup_start_idx + NUM_ORDERS:
                order_idx = node - pickup_start_idx
                courier_orders.append(order_idx)

                # Get ride time for this order
                pickup_index = manager_fresh.NodeToIndex(pickup_start_idx + order_idx)
                delivery_index = manager_fresh.NodeToIndex(delivery_start_idx + order_idx)
                pickup_time = solution_fresh.Value(time_dimension_fresh.CumulVar(pickup_index))
                delivery_time = solution_fresh.Value(time_dimension_fresh.CumulVar(delivery_index))
                ride_time = delivery_time - pickup_time

                vrp_fresh_ride_times.append(ride_time)
                vrp_fresh_assignments.append((order_idx, vehicle_id, ride_time))

            route_distance += route_dist_matrix[node][next_node]
            index = next_index

        if len(courier_orders) > 0:
            vrp_fresh_couriers_used += 1
            courier_order_map_fresh[vehicle_id] = courier_orders

        vrp_fresh_total_distance += route_distance

vrp_fresh_time = (time.time() - vrp_fresh_start) * 1000  # ms

# Compute metrics
vrp_fresh_served = len(vrp_fresh_assignments)
vrp_fresh_avg_dist = vrp_fresh_total_distance / vrp_fresh_served if vrp_fresh_served > 0 else 0
vrp_fresh_assign_rate = (vrp_fresh_served / NUM_ORDERS) * 100
vrp_fresh_avg_per_courier = vrp_fresh_total_distance / vrp_fresh_couriers_used if vrp_fresh_couriers_used > 0 else 0

# Bundle statistics
bundle_sizes_fresh = [len(orders) for orders in courier_order_map_fresh.values()]
avg_bundle_size_fresh = np.mean(bundle_sizes_fresh) if bundle_sizes_fresh else 0

# Ride time statistics
vrp_fresh_median_ride = np.median(vrp_fresh_ride_times) if vrp_fresh_ride_times else 0
vrp_fresh_p95_ride = np.percentile(vrp_fresh_ride_times, 95) if vrp_fresh_ride_times else 0
vrp_fresh_max_ride = max(vrp_fresh_ride_times) if vrp_fresh_ride_times else 0

print(f"Assignments: {vrp_fresh_served}")
print(f"Couriers used: {vrp_fresh_couriers_used}")
print(f"Avg orders per courier: {avg_bundle_size_fresh:.2f}")
print(f"Total route distance: {vrp_fresh_total_distance:.2f} km")
print(f"Average distance per order: {vrp_fresh_avg_dist:.2f} km")
print(f"Average route per courier: {vrp_fresh_avg_per_courier:.2f} km")
print(f"Assignment rate: {vrp_fresh_assign_rate:.1f}%")
print(f"Median ride time: {vrp_fresh_median_ride:.0f} sec ({vrp_fresh_median_ride/60:.1f} min)")
print(f"P95 ride time: {vrp_fresh_p95_ride:.0f} sec ({vrp_fresh_p95_ride/60:.1f} min)")
print(f"Max ride time: {vrp_fresh_max_ride:.0f} sec ({vrp_fresh_max_ride/60:.1f} min) [should be ≤ {T_MAX_SECONDS}]")
print(f"Computation time: {vrp_fresh_time:.2f} ms")

# ============================================================================
# SECTION 6: CREATING VISUALIZATION (2x2 GRID)
# ============================================================================
print("\n[SECTION 6] CREATING VISUALIZATION (2x2 GRID)")
print("-" * 80)

timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
viz_file = f"/Users/pranjal/Code/meituan/eda/visualizations/05_simulation_matching_{timestamp}.png"

fig, axes = plt.subplots(2, 2, figsize=(20, 18))

# Color palette for couriers
colors = plt.cm.tab20(np.linspace(0, 1, max(NUM_COURIERS, 20)))

# Helper function to draw grid
def draw_grid(ax):
    cell_size = CITY_SIZE / GRID_CELLS
    for i in range(GRID_CELLS + 1):
        ax.axhline(y=i * cell_size, color='gray', linestyle='--', linewidth=0.5, alpha=0.3)
        ax.axvline(x=i * cell_size, color='gray', linestyle='--', linewidth=0.5, alpha=0.3)

# Panel 1: Batch Greedy (top-left)
ax1 = axes[0, 0]
ax1.set_title('Batch Greedy (Order-First Heuristic)', fontsize=12, fontweight='bold')
ax1.set_xlabel('X (km)')
ax1.set_ylabel('Y (km)')
ax1.set_xlim(0, CITY_SIZE)
ax1.set_ylim(0, CITY_SIZE)
draw_grid(ax1)

ax1.scatter(restaurant_x, restaurant_y, c='green', marker='*', s=150,
           label='Restaurants', zorder=5, edgecolors='black', linewidths=0.5)
ax1.scatter(courier_x, courier_y, c='red', marker='^', s=60,
           label='Couriers', zorder=4, edgecolors='black', linewidths=0.5)
ax1.scatter(customer_x, customer_y, c='lightblue', marker='o', s=40,
           label='Customers', zorder=3, edgecolors='black', linewidths=0.5)

for order_idx, courier_idx, _ in greedy_assignments:
    ax1.plot([courier_x[courier_idx], order_restaurants[order_idx][0]],
            [courier_y[courier_idx], order_restaurants[order_idx][1]],
            color=colors[courier_idx % len(colors)], linewidth=1.2, alpha=0.5, zorder=2)
    ax1.plot([order_restaurants[order_idx][0], customer_x[order_idx]],
            [order_restaurants[order_idx][1], customer_y[order_idx]],
            color=colors[courier_idx % len(colors)], linewidth=1.2, alpha=0.5,
            linestyle='--', zorder=2)

ax1.legend(loc='upper right', fontsize=8)
ax1.text(0.02, 0.98, f"Served: {greedy_served}/{NUM_ORDERS}\nTotal: {greedy_total_dist:.1f} km\nAvg: {greedy_avg_dist:.2f} km/order",
        transform=ax1.transAxes, fontsize=9, verticalalignment='top',
        bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))

# Panel 2: Hungarian (top-right)
ax2 = axes[0, 1]
ax2.set_title('Hungarian (Optimal 1-to-1)', fontsize=12, fontweight='bold')
ax2.set_xlabel('X (km)')
ax2.set_ylabel('Y (km)')
ax2.set_xlim(0, CITY_SIZE)
ax2.set_ylim(0, CITY_SIZE)
draw_grid(ax2)

ax2.scatter(restaurant_x, restaurant_y, c='green', marker='*', s=150,
           label='Restaurants', zorder=5, edgecolors='black', linewidths=0.5)
ax2.scatter(courier_x, courier_y, c='red', marker='^', s=60,
           label='Couriers', zorder=4, edgecolors='black', linewidths=0.5)
ax2.scatter(customer_x, customer_y, c='lightblue', marker='o', s=40,
           label='Customers', zorder=3, edgecolors='black', linewidths=0.5)

for order_idx, courier_idx, _ in hungarian_assignments:
    ax2.plot([courier_x[courier_idx], order_restaurants[order_idx][0]],
            [courier_y[courier_idx], order_restaurants[order_idx][1]],
            color=colors[courier_idx % len(colors)], linewidth=1.2, alpha=0.5, zorder=2)
    ax2.plot([order_restaurants[order_idx][0], customer_x[order_idx]],
            [order_restaurants[order_idx][1], customer_y[order_idx]],
            color=colors[courier_idx % len(colors)], linewidth=1.2, alpha=0.5,
            linestyle='--', zorder=2)

ax2.legend(loc='upper right', fontsize=8)
ax2.text(0.02, 0.98, f"Served: {hungarian_served}/{NUM_ORDERS}\nTotal: {hungarian_total_dist:.1f} km\nAvg: {hungarian_avg_dist:.2f} km/order",
        transform=ax2.transAxes, fontsize=9, verticalalignment='top',
        bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))

# Panel 3: VRP-base (bottom-left)
ax3 = axes[1, 0]
ax3.set_title('VRP-base (No Freshness Constraint)', fontsize=12, fontweight='bold')
ax3.set_xlabel('X (km)')
ax3.set_ylabel('Y (km)')
ax3.set_xlim(0, CITY_SIZE)
ax3.set_ylim(0, CITY_SIZE)
draw_grid(ax3)

ax3.scatter(restaurant_x, restaurant_y, c='green', marker='*', s=150,
           label='Restaurants', zorder=5, edgecolors='black', linewidths=0.5)
ax3.scatter(courier_x, courier_y, c='red', marker='^', s=60,
           label='Couriers', zorder=4, edgecolors='black', linewidths=0.5)
ax3.scatter(customer_x, customer_y, c='lightblue', marker='o', s=40,
           label='Customers', zorder=3, edgecolors='black', linewidths=0.5)

for order_idx, courier_idx, _ in vrp_base_assignments:
    ax3.plot([courier_x[courier_idx], order_restaurants[order_idx][0]],
            [courier_y[courier_idx], order_restaurants[order_idx][1]],
            color=colors[courier_idx % len(colors)], linewidth=1.2, alpha=0.5, zorder=2)
    ax3.plot([order_restaurants[order_idx][0], customer_x[order_idx]],
            [order_restaurants[order_idx][1], customer_y[order_idx]],
            color=colors[courier_idx % len(colors)], linewidth=1.2, alpha=0.5,
            linestyle='--', zorder=2)

ax3.legend(loc='upper right', fontsize=8)
ax3.text(0.02, 0.98, f"Served: {vrp_base_served}/{NUM_ORDERS}\nTotal: {vrp_base_total_distance:.1f} km\nAvg: {vrp_base_avg_dist:.2f} km/order",
        transform=ax3.transAxes, fontsize=9, verticalalignment='top',
        bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))

# Panel 4: VRP-fresh (bottom-right)
ax4 = axes[1, 1]
ax4.set_title(f'VRP-fresh (Ride Time ≤ {T_MAX_MINUTES} min)', fontsize=12, fontweight='bold')
ax4.set_xlabel('X (km)')
ax4.set_ylabel('Y (km)')
ax4.set_xlim(0, CITY_SIZE)
ax4.set_ylim(0, CITY_SIZE)
draw_grid(ax4)

ax4.scatter(restaurant_x, restaurant_y, c='green', marker='*', s=150,
           label='Restaurants', zorder=5, edgecolors='black', linewidths=0.5)
ax4.scatter(courier_x, courier_y, c='red', marker='^', s=60,
           label='Couriers', zorder=4, edgecolors='black', linewidths=0.5)
ax4.scatter(customer_x, customer_y, c='lightblue', marker='o', s=40,
           label='Customers', zorder=3, edgecolors='black', linewidths=0.5)

for order_idx, courier_idx, ride_time in vrp_fresh_assignments:
    ax4.plot([courier_x[courier_idx], order_restaurants[order_idx][0]],
            [courier_y[courier_idx], order_restaurants[order_idx][1]],
            color=colors[courier_idx % len(colors)], linewidth=1.2, alpha=0.5, zorder=2)
    ax4.plot([order_restaurants[order_idx][0], customer_x[order_idx]],
            [order_restaurants[order_idx][1], customer_y[order_idx]],
            color=colors[courier_idx % len(colors)], linewidth=1.2, alpha=0.5,
            linestyle='--', zorder=2)

ax4.legend(loc='upper right', fontsize=8)
ax4.text(0.02, 0.98, f"Served: {vrp_fresh_served}/{NUM_ORDERS}\nTotal: {vrp_fresh_total_distance:.1f} km\nMedian ride: {vrp_fresh_median_ride/60:.1f} min\nP95 ride: {vrp_fresh_p95_ride/60:.1f} min",
        transform=ax4.transAxes, fontsize=9, verticalalignment='top',
        bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.5))

plt.tight_layout()
plt.savefig(viz_file, dpi=150, bbox_inches='tight')
print(f"Visualization saved to: {viz_file}")

# ============================================================================
# SECTION 7: METRICS COMPARISON
# ============================================================================
print("\n[SECTION 7] METRICS COMPARISON")
print("=" * 80)
print()
print(f"{'Algorithm':<25} {'Served':>8} {'Total Dist':>12} {'Avg Dist':>10} {'Median Ride':>13} {'P95 Ride':>10} {'Rate':>7} {'Time (ms)':>12}")
print("-" * 110)
print(f"{'Batch Greedy':<25} {greedy_served:>8} {greedy_total_dist:>11.2f}km {greedy_avg_dist:>9.2f}km {'N/A':>13} {'N/A':>10} {greedy_assign_rate:>6.1f}% {greedy_time:>11.2f}ms")
print(f"{'Hungarian (optimal)':<25} {hungarian_served:>8} {hungarian_total_dist:>11.2f}km {hungarian_avg_dist:>9.2f}km {'N/A':>13} {'N/A':>10} {hungarian_assign_rate:>6.1f}% {hungarian_time:>11.2f}ms")
print(f"{'VRP-base':<25} {vrp_base_served:>8} {vrp_base_total_distance:>11.2f}km {vrp_base_avg_dist:>9.2f}km {'N/A':>13} {'N/A':>10} {vrp_base_assign_rate:>6.1f}% {vrp_base_time:>11.2f}ms")
print(f"{'VRP-fresh (T≤20min)':<25} {vrp_fresh_served:>8} {vrp_fresh_total_distance:>11.2f}km {vrp_fresh_avg_dist:>9.2f}km {vrp_fresh_median_ride/60:>12.1f}min {vrp_fresh_p95_ride/60:>9.1f}min {vrp_fresh_assign_rate:>6.1f}% {vrp_fresh_time:>11.2f}ms")
print()
print("=" * 80)
print("KEY INSIGHTS")
print("=" * 80)

if hungarian_total_dist > 0 and greedy_total_dist > 0:
    gap = ((greedy_total_dist - hungarian_total_dist) / hungarian_total_dist) * 100
    print(f"1. Greedy vs Hungarian: Greedy is {gap:+.1f}% from optimal")

if hungarian_served > 0:
    print(f"2. Assignment Rate: {hungarian_served}/{NUM_ORDERS} orders served ({hungarian_assign_rate:.1f}%)")

if hungarian_total_dist > 0 and vrp_base_total_distance > 0:
    vrp_base_diff = ((vrp_base_total_distance - hungarian_total_dist) / hungarian_total_dist) * 100
    print(f"3. VRP-base vs Hungarian: {vrp_base_diff:+.1f}% distance")

if vrp_base_total_distance > 0 and vrp_fresh_total_distance > 0:
    fresh_cost = ((vrp_fresh_total_distance - vrp_base_total_distance) / vrp_base_total_distance) * 100
    print(f"4. Freshness Cost: VRP-fresh is {fresh_cost:+.1f}% longer than VRP-base")

if vrp_fresh_ride_times:
    violations = sum(1 for rt in vrp_fresh_ride_times if rt > T_MAX_SECONDS)
    print(f"5. Freshness Violations: {violations}/{len(vrp_fresh_ride_times)} orders exceed T_max (should be 0)")

print(f"6. Computation Speed: Greedy ({greedy_time:.1f}ms) < Hungarian ({hungarian_time:.1f}ms) < VRP-base ({vrp_base_time:.1f}ms) < VRP-fresh ({vrp_fresh_time:.1f}ms)")

print()
print("=" * 80)
print("Simulation complete. Output files:")
print(f"  - Log: {log_file}")
print(f"  - Visualization: {viz_file}")
print("=" * 80)

# Close log file
log.close()

# Print to stdout (since we redirected earlier)
sys.stdout = sys.__stdout__
print(f"Simulation complete! Check {log_file} for details.")
