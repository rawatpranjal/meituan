"""
Clean, Simple Visualization for Food Delivery Simulation

ONLY:
- Red squares for restaurants (always visible)
- Blue circles for houses (only when order exists)
- Colored triangles for couriers
- No text, no labels, no clutter
"""

import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation, PillowWriter
from matplotlib.patches import Rectangle, Circle, Polygon
import numpy as np
from simulator_core import (
    Restaurant, Courier, Order, run_simulation, GRID_SIZE,
    NUM_RESTAURANTS, NUM_COURIERS, SIMULATION_DURATION, NUM_ORDERS, LAMBDA
)
from assignment_algorithms import get_algorithm
import math


# Configuration (use values from simulator_core.py)
TARGET_ORDERS = NUM_ORDERS  # Use configured value from simulator_core
SIMULATION_HOURS = SIMULATION_DURATION / 3600  # Convert seconds to hours
ANIMATION_SPEED = 10  # 10x speed (6 hours in 36 seconds)

# Visual sizes (smaller for better movement visibility)
RESTAURANT_SIZE = 0.1
HOUSE_SIZE = 0.08
COURIER_SIZE = 0.12
GRID_SPACING = 0.5  # Finer grid every 0.5 km

# Colors for couriers (10 distinct colors)
COURIER_COLORS = [
    '#FF0000',  # Red
    '#0000FF',  # Blue
    '#00FF00',  # Green
    '#FF00FF',  # Magenta
    '#FFAA00',  # Orange
    '#00FFFF',  # Cyan
    '#800080',  # Purple
    '#FF1493',  # Pink
    '#32CD32',  # Lime
    '#FFD700',  # Gold
]

# Algorithm display names (professional technical terminology)
ALGORITHM_DISPLAY_NAMES = {
    'greedy': 'Greedy',
    'hungarian': 'Optimal Single-Order Matching',
    'simple_bundling': 'Single-Pickup Bundling',
    'route_cost_bundling': '[DEPRECATED] Route Cost Bundling',
    'batched_pickups': 'Batched Pickups',
    'relay_bundling': 'Relay Bundling',
    'anticipated_bundling': 'Anticipated Bundling'
}

# Algorithm filename mapping (for descriptive GIF names)
ALGORITHM_FILENAMES = {
    'greedy': 'greedy_baseline',
    'hungarian': 'hungarian_route_aware',
    'simple_bundling': 'simple_bundling_route_aware',
    'route_cost_bundling': 'deprecated_route_cost',
    'batched_pickups': 'batched_pickups_network',
    'relay_bundling': 'relay_bundling_handoffs',
    'anticipated_bundling': 'anticipated_bundling_lookahead'
}


def generate_dense_continuous_scenario(duration=SIMULATION_DURATION, seed=42):
    """Generate scenario with uniform continuous demand over 1 hour."""

    # Set random seed for reproducible scenarios
    np.random.seed(seed)

    # Create restaurants uniformly distributed
    restaurants = []
    positions = [
        (1.0, 1.0), (3.0, 1.0), (4.5, 1.0),
        (1.0, 3.0), (2.5, 2.5), (4.0, 3.0),
        (1.0, 4.5), (3.0, 4.5)
    ]
    for i in range(NUM_RESTAURANTS):
        if i < len(positions):
            restaurants.append(Restaurant(i, positions[i]))
        else:
            # Random placement if we need more
            x = np.random.uniform(0.5, GRID_SIZE - 0.5)
            y = np.random.uniform(0.5, GRID_SIZE - 0.5)
            restaurants.append(Restaurant(i, (x, y)))

    # Create couriers uniformly distributed
    couriers = []
    courier_positions = [
        (0.5, 0.5), (2.0, 0.5), (3.5, 0.5), (4.5, 0.5),
        (0.5, 2.5), (2.5, 2.0), (4.5, 2.5),
        (0.5, 4.0), (2.0, 4.0), (4.5, 4.5)
    ]
    for i in range(NUM_COURIERS):
        if i < len(courier_positions):
            couriers.append(Courier(i, courier_positions[i]))
        else:
            x = np.random.uniform(0.5, 4.5)
            y = np.random.uniform(0.5, 4.5)
            couriers.append(Courier(i, (x, y)))

    # Generate orders with Poisson process (FLAT DEMAND - use LAMBDA from simulator_core)
    orders = []
    order_id = 0

    # Use flat demand configuration from simulator_core.py
    lambda_rate = LAMBDA  # Uniform demand throughout simulation

    # Generate orders using Poisson process
    current_time = 0.0
    buffer = 300  # 5 min buffer (enough for MEAL_PREP_TIME=180s + margin)
    while current_time < duration - buffer and order_id < TARGET_ORDERS + 50:
        # Sample inter-arrival time from exponential distribution
        # Convert lambda from orders/minute to orders/second
        lambda_per_second = lambda_rate / 60.0
        inter_arrival_time = np.random.exponential(1.0 / lambda_per_second)

        current_time += inter_arrival_time

        if current_time >= duration - buffer:
            break

        # Pick random restaurant
        restaurant = np.random.choice(restaurants)

        # Random customer location
        diner_x = np.random.uniform(0.5, GRID_SIZE - 0.5)
        diner_y = np.random.uniform(0.5, GRID_SIZE - 0.5)
        diner_location = (diner_x, diner_y)

        order = Order(
            order_id=order_id,
            restaurant_id=restaurant.id,
            restaurant_location=restaurant.location,
            diner_location=diner_location,
            placement_time=float(current_time)
        )
        orders.append(order)
        order_id += 1

    return {
        'restaurants': restaurants,
        'couriers': couriers,
        'order_schedule': orders,
        'duration': duration
    }


def draw_clean_visualization(ax, snapshot, restaurants, couriers_colors):
    """Draw clean visualization with only essential elements."""

    # Clear axis
    ax.clear()
    ax.set_xlim(0, GRID_SIZE)
    ax.set_ylim(0, GRID_SIZE)
    ax.set_aspect('equal')
    ax.axis('off')

    # White background
    ax.set_facecolor('#FFFFFF')

    # Draw grid lines - FINER GRID every 0.5km
    grid_points = np.arange(0, GRID_SIZE + GRID_SPACING, GRID_SPACING)
    for i, pos in enumerate(grid_points):
        # Make 1km lines thicker than 0.5km lines
        if i % 2 == 0:  # Every 1km
            ax.axhline(y=pos, color='#666666', linewidth=1.2, alpha=0.8)
            ax.axvline(x=pos, color='#666666', linewidth=1.2, alpha=0.8)
        else:  # Every 0.5km
            ax.axhline(y=pos, color='#AAAAAA', linewidth=0.8, alpha=0.5)
            ax.axvline(x=pos, color='#AAAAAA', linewidth=0.8, alpha=0.5)

    # Draw restaurants (red squares) - always visible
    for restaurant in restaurants:
        r_x, r_y = restaurant.location
        rect = Rectangle((r_x - RESTAURANT_SIZE/2, r_y - RESTAURANT_SIZE/2),
                        RESTAURANT_SIZE, RESTAURANT_SIZE,
                        facecolor='#FF4444', edgecolor='#CC0000',
                        linewidth=2, zorder=10)
        ax.add_patch(rect)

    # Draw houses (blue circles) - only for active orders
    drawn_houses = set()
    for order_id, order_data in snapshot['orders'].items():
        if order_data['state'] in ['READY', 'ASSIGNED', 'PICKED_UP']:
            diner_loc = tuple(order_data['diner_location'])
            if diner_loc not in drawn_houses:
                drawn_houses.add(diner_loc)
                h_x, h_y = diner_loc
                circle = Circle((h_x, h_y), HOUSE_SIZE/2,
                              facecolor='#4444FF', edgecolor='#0000CC',
                              linewidth=2, zorder=8)
                ax.add_patch(circle)

    # Draw relay handoff points (purple diamonds)
    for o_id, order_data in snapshot['orders'].items():
        if order_data.get('is_relay') and order_data.get('relay_handoff_location'):
            handoff_loc = order_data['relay_handoff_location']
            # Only show if order is in transit and not yet delivered
            if order_data.get('state') in ['ASSIGNED', 'PICKED_UP']:
                # Draw purple diamond for handoff point
                diamond_size = 0.15
                diamond_points = [
                    (handoff_loc[0], handoff_loc[1] + diamond_size/2),  # Top
                    (handoff_loc[0] + diamond_size/2, handoff_loc[1]),  # Right
                    (handoff_loc[0], handoff_loc[1] - diamond_size/2),  # Bottom
                    (handoff_loc[0] - diamond_size/2, handoff_loc[1])   # Left
                ]
                diamond = Polygon(diamond_points, facecolor='#9370DB',
                                edgecolor='#663399', linewidth=2, zorder=15)
                ax.add_patch(diamond)

    # Draw couriers and their routes
    for c_id, courier_data in snapshot['couriers'].items():
        c_idx = int(c_id)
        if c_idx >= NUM_COURIERS:
            continue

        loc = courier_data['current_location']
        color = COURIER_COLORS[c_idx % len(COURIER_COLORS)]

        # Check if courier recently received new assignment (within last 2 seconds)
        current_time = snapshot['time']
        recently_assigned = False
        assigned_order_ids = courier_data.get('assigned_order_ids', [])
        for order_id in assigned_order_ids:
            if order_id in snapshot['orders']:
                order = snapshot['orders'][order_id]
                assignment_time = order.get('assignment_time')
                if assignment_time is not None:
                    time_since_assignment = current_time - assignment_time
                    if time_since_assignment <= 2.0:  # Highlight for 2 seconds
                        recently_assigned = True
                        break

        # Draw highlight glow if recently assigned
        if recently_assigned:
            # Outer glow (larger, more transparent)
            outer_glow = Circle(loc, COURIER_SIZE * 1.5,
                              facecolor=color, edgecolor='none',
                              alpha=0.2, zorder=18)
            ax.add_patch(outer_glow)
            # Inner glow (smaller, more visible)
            inner_glow = Circle(loc, COURIER_SIZE * 1.0,
                              facecolor=color, edgecolor='none',
                              alpha=0.35, zorder=19)
            ax.add_patch(inner_glow)

        # Draw courier as triangle
        if courier_data['state'] == 'IDLE':
            size = COURIER_SIZE * 0.8
            alpha = 0.5
        else:
            size = COURIER_SIZE
            alpha = 1.0

        # Triangle pointing in direction of movement
        if courier_data.get('next_destination'):
            dest = courier_data['next_destination']
            dx = dest[0] - loc[0]
            dy = dest[1] - loc[1]
            angle = math.atan2(dy, dx)
        else:
            angle = 0

        # Create triangle points
        triangle_points = [
            (loc[0] + size/2 * math.cos(angle), loc[1] + size/2 * math.sin(angle)),
            (loc[0] + size/3 * math.cos(angle + 2.4), loc[1] + size/3 * math.sin(angle + 2.4)),
            (loc[0] + size/3 * math.cos(angle - 2.4), loc[1] + size/3 * math.sin(angle - 2.4))
        ]

        triangle = Polygon(triangle_points, facecolor=color,
                          edgecolor='black', linewidth=2,
                          alpha=alpha, zorder=20)
        ax.add_patch(triangle)

        # Draw batch-persistent assignment arrows ONLY for last batch interval
        BATCH_INTERVAL = 300  # seconds (5 minutes) - arrows persist for full batch duration

        if not assigned_order_ids:
            continue  # Skip couriers with no assignments

        # FILTER: Only show arrows for orders assigned within last BATCH_INTERVAL (5 minutes)
        # This ensures arrows persist for 5 full minutes even if order is delivered
        recent_assigned_orders = []
        for order_id in assigned_order_ids:
            if order_id in snapshot['orders']:
                order = snapshot['orders'][order_id]
                assignment_time = order.get('assignment_time')
                if assignment_time is not None:
                    time_since_assignment = current_time - assignment_time
                    if time_since_assignment <= BATCH_INTERVAL:
                        recent_assigned_orders.append(order_id)

        if not recent_assigned_orders:
            continue  # No recent assignments to visualize

        # Build complete route: courier → pickups → deliveries
        # Show routes for ALL orders assigned in current batch (even if already delivered)
        waypoints = [loc]  # Start at courier location

        # Collect all pending pickups (restaurants for non-picked-up orders)
        pending_pickups = []
        for order_id in recent_assigned_orders:
            if order_id in snapshot['orders']:
                order = snapshot['orders'][order_id]
                if order.get('state') in ['ASSIGNED', 'READY']:
                    pending_pickups.append(order['restaurant_location'])

        # Collect all pending deliveries (customers for picked-up orders)
        pending_deliveries = []
        for order_id in recent_assigned_orders:
            if order_id in snapshot['orders']:
                order = snapshot['orders'][order_id]
                if order.get('state') == 'PICKED_UP':
                    pending_deliveries.append(order['diner_location'])

        # For delivered orders in current batch, show delivery location to maintain route visibility
        for order_id in recent_assigned_orders:
            if order_id in snapshot['orders']:
                order = snapshot['orders'][order_id]
                if order.get('state') == 'DELIVERED':
                    # Add delivered order's customer location to show completed delivery
                    pending_deliveries.append(order['diner_location'])

        # Add waypoints in order: pickups first, then deliveries
        waypoints.extend(pending_pickups)
        waypoints.extend(pending_deliveries)

        if len(waypoints) <= 1:
            continue  # No route to draw

        # Calculate line thickness based on bundle size
        num_packages = len(assigned_order_ids)
        linewidth = 4.0 + (num_packages * 1.2)

        # Draw connected segments between consecutive waypoints
        for i in range(len(waypoints) - 1):
            start = waypoints[i]
            end = waypoints[i + 1]

            # L-shaped Manhattan routing
            mid_point = (end[0], start[1])

            # Horizontal segment
            ax.plot([start[0], mid_point[0]], [start[1], mid_point[1]],
                   color=color, linewidth=linewidth, alpha=1.0,
                   linestyle='-', zorder=15)

            # Vertical segment
            ax.plot([mid_point[0], end[0]], [mid_point[1], end[1]],
                   color=color, linewidth=linewidth, alpha=1.0,
                   linestyle='-', zorder=15)

            # Draw waypoint marker at each stop (except courier start position)
            if i > 0:  # Skip first waypoint (courier location)
                # Small circle marker at waypoint
                waypoint_marker = Circle(waypoints[i], 0.08,
                                        facecolor=color, edgecolor='black',
                                        linewidth=2.0, alpha=0.9, zorder=17)
                ax.add_patch(waypoint_marker)

        # Arrow head at final destination
        final_dest = waypoints[-1]
        second_last = waypoints[-2]

        # Determine arrow direction
        if abs(final_dest[1] - second_last[1]) > 0.01:
            arrow_dy = 0.35 if final_dest[1] > second_last[1] else -0.35
            ax.arrow(final_dest[0], final_dest[1] - arrow_dy, 0, arrow_dy * 0.7,
                    head_width=0.20, head_length=0.15,
                    fc=color, ec='black', alpha=1.0, linewidth=3, zorder=16)
        elif abs(final_dest[0] - second_last[0]) > 0.01:
            arrow_dx = 0.35 if final_dest[0] > second_last[0] else -0.35
            ax.arrow(final_dest[0] - arrow_dx, final_dest[1], arrow_dx * 0.7, 0,
                    head_width=0.20, head_length=0.15,
                    fc=color, ec='black', alpha=1.0, linewidth=3, zorder=16)


def create_algorithm_animation(algo_name, scenario, output_path):
    """Create clean animation for a single algorithm."""

    print(f"\nCreating clean animation for {algo_name}...")

    # Run simulation
    state = run_simulation(scenario, get_algorithm(algo_name), algo_name)

    # Setup figure
    fig, ax = plt.subplots(figsize=(10, 10))

    # Define courier colors
    couriers_colors = {}
    for i in range(NUM_COURIERS):
        couriers_colors[str(i)] = COURIER_COLORS[i % len(COURIER_COLORS)]

    def update(frame):
        if frame >= len(state.timeline):
            return ax,

        snapshot = state.timeline[frame]
        time = snapshot['time']
        metrics = snapshot['metrics']

        # Draw clean visualization
        restaurants = list(state.restaurants.values())
        draw_clean_visualization(ax, snapshot, restaurants, couriers_colors)

        # Title at top center - use display name
        display_name = ALGORITHM_DISPLAY_NAMES.get(algo_name, algo_name.replace('_', ' ').title())
        ax.text(GRID_SIZE/2, GRID_SIZE + 0.3,
               display_name,
               fontsize=14, fontweight='bold', ha='center')

        # Time and progress at top right (moved down to avoid overlap)
        total_minutes = int(time // 60)
        hours = total_minutes // 60
        minutes = total_minutes % 60
        progress_hour = min(hours + 1, SIMULATION_HOURS)

        ax.text(GRID_SIZE - 0.2, GRID_SIZE - 0.15,
               f'Hour {progress_hour} of {SIMULATION_HOURS} | {hours:02d}:{minutes:02d}',
               fontsize=12, ha='right')

        # Legend at top-left corner
        legend_text = (
            '■ Restaurant  ● Customer\n'
            '▲ Courier  ⋯→ Route\n'
            '◆ Handoff Point'
        )
        ax.text(0.15, GRID_SIZE - 0.15, legend_text,
               fontsize=8, verticalalignment='top', horizontalalignment='left',
               bbox=dict(boxstyle='round', facecolor='white', alpha=0.85,
                       edgecolor='#666666', linewidth=1.5),
               zorder=25)

        # Two-Row Dashboard - Compute running averages from incremental metrics

        # Get base counters (always available)
        orders_delivered = metrics.get('orders_delivered', 0)
        total_orders = len(snapshot['orders'])
        bundles_created = metrics.get('bundles_created', 0)

        # Current snapshot counts
        orders_in_transit = sum(1 for o in snapshot['orders'].values() if o['state'] in ['ASSIGNED', 'PICKED_UP'])

        # ROW 1: Customer-Facing Metrics (computed from incremental counters)

        # Avg Delivery Time (minutes) - compute from total_delivery_time
        if orders_delivered > 0:
            avg_delivery_mins = (metrics.get('total_delivery_time', 0) / orders_delivered) / 60
        else:
            avg_delivery_mins = 0

        # Avg Freshness Time (ready to door, minutes) - compute from total_ready_to_door_time
        if orders_delivered > 0:
            avg_freshness_mins = (metrics.get('total_ready_to_door_time', 0) / orders_delivered) / 60
        else:
            avg_freshness_mins = 0

        # ROW 2: Operational Metrics

        # Total Distance - sum from all couriers (couriers are dicts in snapshot)
        total_distance_km = sum(c['total_distance_traveled'] for c in snapshot['couriers'].values())

        # Avg Bundle Size - compute from total_bundle_size
        if bundles_created > 0:
            avg_bundle_size = metrics.get('total_bundle_size', 0) / bundles_created
        else:
            avg_bundle_size = 0

        # Total Idle Time (hours)
        total_idle_hours = metrics.get('total_courier_idle_time', 0) / 3600

        # Relay metrics
        relay_handoffs = metrics.get('relay_handoffs', 0)
        relay_orders = metrics.get('relay_orders', 0)

        # Create two-row dashboard text
        row1_text = (
            f'Delivered: {orders_delivered}/{total_orders}  |  '
            f'Avg Delivery: {avg_delivery_mins:.1f}m  |  '
            f'Avg Freshness: {avg_freshness_mins:.1f}m  |  '
            f'In Transit: {orders_in_transit}'
        )

        # Show relay metrics for relay algorithm, otherwise show idle time
        if algo_name == 'relay_bundling' and relay_orders > 0:
            row2_text = (
                f'Distance: {total_distance_km:.1f}km  |  '
                f'Bundles: {bundles_created}  |  '
                f'Avg Bundle Size: {avg_bundle_size:.2f}  |  '
                f'Relays: {relay_handoffs}/{relay_orders}'
            )
        else:
            row2_text = (
                f'Distance: {total_distance_km:.1f}km  |  '
                f'Bundles: {bundles_created}  |  '
                f'Avg Bundle Size: {avg_bundle_size:.2f}  |  '
                f'Idle Time: {total_idle_hours:.1f}h'
            )

        # Position Row 1 directly below grid
        ax.text(GRID_SIZE/2, -0.15, row1_text,
               fontsize=10, ha='center', verticalalignment='top',
               bbox=dict(boxstyle='round', facecolor='#E8F4F8',
                       edgecolor='#2E86AB', linewidth=2, pad=0.4))

        # Position Row 2 below Row 1
        ax.text(GRID_SIZE/2, -0.40, row2_text,
               fontsize=10, ha='center', verticalalignment='top',
               bbox=dict(boxstyle='round', facecolor='#FFF4E6',
                       edgecolor='#FF8C00', linewidth=2, pad=0.4))

        return ax,

    # Create animation - sample every 30 seconds for 6 hours
    # Sample frames: take every 30th frame (30 seconds of simulation)
    sampled_frames = list(range(0, len(state.timeline), 30))[:720]  # Max 720 frames (6hrs at 30s intervals)

    def update_sampled(idx):
        return update(sampled_frames[idx])

    # Animation with speed control
    fps = 10 * ANIMATION_SPEED  # Base 10fps * speed multiplier
    interval = 1000 / fps  # Convert to milliseconds

    anim = FuncAnimation(fig, update_sampled, frames=len(sampled_frames),
                        interval=interval, blit=False)

    # Save GIF
    print(f"  Saving to {output_path}...")
    print(f"  Animation: {len(sampled_frames)} frames at {fps} fps = {len(sampled_frames)/fps:.1f} seconds")
    writer = PillowWriter(fps=fps)
    anim.save(output_path, writer=writer, dpi=80)
    print(f"  Saved!")

    plt.close(fig)
    return state.metrics


# Main execution
if __name__ == "__main__":
    print("=" * 60)
    print("CREATING CLEAN, SIMPLE VISUALIZATIONS")
    print("=" * 60)

    # Generate dense continuous scenario for 6 hours
    scenario = generate_dense_continuous_scenario()
    print(f"Generated scenario: {SIMULATION_HOURS} hours simulation")
    print(f"  {len(scenario['restaurants'])} restaurants")
    print(f"  {len(scenario['couriers'])} couriers")
    print(f"  {len(scenario['order_schedule'])} orders (target: {TARGET_ORDERS})")
    print(f"  Animation speed: {ANIMATION_SPEED}x")

    # Algorithms to visualize - clean 5-algorithm progression
    # Each adds one major capability: None → Route Awareness → Bundling → Network → Anticipatory
    algorithms = ['greedy', 'hungarian', 'simple_bundling', 'batched_pickups', 'anticipated_bundling']

    # Create animations for each algorithm
    all_metrics = {}
    for algo_name in algorithms:
        # Use descriptive filenames
        filename = ALGORITHM_FILENAMES[algo_name]
        output_path = f'gifs/{filename}.gif'
        metrics = create_algorithm_animation(algo_name, scenario, output_path)
        all_metrics[algo_name] = metrics

    print("\n" + "=" * 60)
    print("PERFORMANCE COMPARISON")
    print("=" * 60)

    for algo_name, metrics in all_metrics.items():
        print(f"\n{algo_name.upper()}:")
        print(f"  Orders Delivered: {metrics['orders_delivered']}")
        print(f"  Total Distance: {metrics['total_distance_traveled']:.1f} km")
        print(f"  Bundles Created: {metrics['bundles_created']}")
        print(f"  Avg Bundle Size: {metrics.get('avg_bundle_size', 0):.2f}")

    print("\n" + "=" * 60)
    print("✓ ALL ANIMATIONS CREATED!")
    print("=" * 60)
    print("\nGIF files created:")
    for algo in algorithms:
        filename = ALGORITHM_FILENAMES[algo]
        print(f"  - gifs/{filename}.gif")