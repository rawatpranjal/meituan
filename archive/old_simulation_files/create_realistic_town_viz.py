"""
Realistic Town Visualization for Food Delivery Simulation

Creates a realistic small-town environment with:
- Street grid layout
- Buildings (restaurants and houses)
- Smooth courier movement along streets
- Visual differentiation between algorithms
"""

import matplotlib.pyplot as plt
import matplotlib.patches as patches
from matplotlib.animation import FuncAnimation, PillowWriter
from matplotlib.patches import FancyBboxPatch, Circle, Rectangle, Polygon
import numpy as np
from simulator_core import generate_asymmetric_scenario, run_simulation, SIMULATION_DURATION
from assignment_algorithms import get_algorithm
import math
from typing import List, Tuple, Dict


# Town Configuration
TOWN_SIZE = 6  # 6x6 km town
BLOCK_SIZE = 0.5  # Each city block is 500m
STREET_WIDTH = 0.05  # 50m wide streets
BUILDING_SIZE = 0.08  # Buildings are 80m wide

# Colors
STREET_COLOR = '#E8E8E8'
SIDEWALK_COLOR = '#D0D0D0'
BUILDING_COLOR = '#B8B8B8'
RESTAURANT_COLOR = '#FF6B6B'
HOUSE_COLOR = '#4ECDC4'
PARK_COLOR = '#95E77E'
COURIER_COLORS = ['#FF4444', '#4444FF', '#44FF44', '#FF44FF', '#FFAA00']


def create_town_layout(ax, restaurants, orders_snapshot):
    """Create a realistic town layout with streets and buildings."""

    # Clear and set up axis
    ax.clear()
    ax.set_xlim(0, TOWN_SIZE)
    ax.set_ylim(0, TOWN_SIZE)
    ax.set_aspect('equal')
    ax.axis('off')

    # Set background color (light ground color)
    ax.set_facecolor('#F5F5F0')

    # Draw street grid
    for i in range(int(TOWN_SIZE / BLOCK_SIZE) + 1):
        # Horizontal streets
        street_y = i * BLOCK_SIZE
        street = Rectangle((0, street_y - STREET_WIDTH/2), TOWN_SIZE, STREET_WIDTH,
                          facecolor=STREET_COLOR, edgecolor='#CCCCCC', linewidth=0.5)
        ax.add_patch(street)

        # Vertical streets
        street_x = i * BLOCK_SIZE
        street = Rectangle((street_x - STREET_WIDTH/2, 0), STREET_WIDTH, TOWN_SIZE,
                          facecolor=STREET_COLOR, edgecolor='#CCCCCC', linewidth=0.5)
        ax.add_patch(street)

    # Draw street lines (center lines)
    for i in range(int(TOWN_SIZE / BLOCK_SIZE) + 1):
        # Dashed center lines
        ax.plot([0, TOWN_SIZE], [i * BLOCK_SIZE, i * BLOCK_SIZE],
               'y-', linewidth=0.5, alpha=0.3, dashes=[5, 5])
        ax.plot([i * BLOCK_SIZE, i * BLOCK_SIZE], [0, TOWN_SIZE],
               'y-', linewidth=0.5, alpha=0.3, dashes=[5, 5])

    # Add some parks/green areas for visual variety
    park_locations = [(1.5, 1.5), (4.5, 4.5), (1.5, 4.5)]
    for park_x, park_y in park_locations:
        park = FancyBboxPatch((park_x - 0.3, park_y - 0.3), 0.6, 0.6,
                              boxstyle="round,pad=0.05",
                              facecolor=PARK_COLOR, edgecolor='#7BC65D',
                              linewidth=2, alpha=0.7)
        ax.add_patch(park)
        # Add some trees
        for _ in range(3):
            tree_x = park_x + np.random.uniform(-0.2, 0.2)
            tree_y = park_y + np.random.uniform(-0.2, 0.2)
            tree = Circle((tree_x, tree_y), 0.03,
                         facecolor='#5DA04E', edgecolor='#4A8940', linewidth=1)
            ax.add_patch(tree)

    # Draw restaurants as actual buildings - BIGGER
    for r_idx, restaurant in enumerate(restaurants):
        r_x, r_y = restaurant.location

        # Restaurant building (larger, distinctive)
        building_size = BUILDING_SIZE * 2.5  # Make it bigger
        building = FancyBboxPatch((r_x - building_size/2, r_y - building_size/2),
                                 building_size, building_size,
                                 boxstyle="round,pad=0.01",
                                 facecolor=RESTAURANT_COLOR,
                                 edgecolor='#D94444',
                                 linewidth=4, zorder=10)
        ax.add_patch(building)

        # Restaurant label - simple text, no emoji
        ax.text(r_x, r_y, f'R{r_idx+1}', fontsize=18, ha='center', va='center',
               color='white', fontweight='bold', zorder=11)

    # Draw customer houses (only for active orders) - BIGGER
    drawn_houses = set()
    for order_id, order_data in orders_snapshot.items():
        if order_data['state'] in ['READY', 'ASSIGNED', 'PICKED_UP']:
            diner_loc = tuple(order_data['diner_location'])
            if diner_loc not in drawn_houses:
                drawn_houses.add(diner_loc)
                h_x, h_y = diner_loc

                # Draw house - bigger
                house_size = BUILDING_SIZE * 1.8
                house = Rectangle((h_x - house_size/2, h_y - house_size/2),
                                house_size, house_size,
                                facecolor=HOUSE_COLOR, edgecolor='#3BA8A0',
                                linewidth=3, zorder=8)
                ax.add_patch(house)

                # House label - simple text, no emoji
                ax.text(h_x, h_y, 'H', fontsize=14, ha='center', va='center',
                       color='white', fontweight='bold', zorder=9)

    # Add street names for orientation
    street_names = ['Main St', 'Oak Ave', 'Elm St', 'Park Blvd', '1st Ave', '2nd Ave']
    for i, name in enumerate(street_names[:int(TOWN_SIZE / BLOCK_SIZE)]):
        # Horizontal street names
        ax.text(0.1, i * BLOCK_SIZE + 0.02, name, fontsize=6,
               style='italic', alpha=0.5)
        # Vertical street names
        if i < len(street_names):
            ax.text(i * BLOCK_SIZE + 0.02, TOWN_SIZE - 0.1,
                   f'{i+1}th St', fontsize=6, style='italic', alpha=0.5, rotation=90)


def draw_courier_vehicle(ax, x, y, heading, color, courier_id, state, num_packages=0):
    """Draw a delivery vehicle (bike/scooter) at the given position."""

    # Vehicle body - MUCH BIGGER
    if state == 'IDLE':
        vehicle_size = 0.15  # Increased from 0.06
        alpha = 0.7
    else:
        vehicle_size = 0.18  # Increased from 0.08
        alpha = 1.0

    # Calculate rotation angle from heading
    angle = math.atan2(heading[1], heading[0]) * 180 / math.pi

    # Draw vehicle as oriented triangle (bike shape)
    vehicle_points = [
        (x - vehicle_size/2, y - vehicle_size/3),
        (x + vehicle_size/2, y),
        (x - vehicle_size/2, y + vehicle_size/3)
    ]

    # Rotate points around center
    angle_rad = angle * math.pi / 180
    cos_a = math.cos(angle_rad)
    sin_a = math.sin(angle_rad)

    rotated_points = []
    for px, py in vehicle_points:
        # Translate to origin
        px -= x
        py -= y
        # Rotate
        new_x = px * cos_a - py * sin_a
        new_y = px * sin_a + py * cos_a
        # Translate back
        rotated_points.append((new_x + x, new_y + y))

    vehicle = Polygon(rotated_points, facecolor=color,
                     edgecolor='black', linewidth=3, alpha=alpha, zorder=20)
    ax.add_patch(vehicle)

    # Courier number - BIGGER
    ax.text(x, y, f'{courier_id}', fontsize=14, color='white',
           fontweight='bold', ha='center', va='center', zorder=21)

    # Draw packages on vehicle - simple circles instead of emoji
    if num_packages > 0:
        package_offset = vehicle_size * 0.6
        package_x = x - math.sin(angle_rad) * package_offset
        package_y = y + math.cos(angle_rad) * package_offset

        # Draw simple package boxes
        for i in range(min(num_packages, 3)):
            offset = i * 0.03
            # Simple brown box
            box = Rectangle((package_x + offset - 0.02, package_y + offset - 0.02),
                          0.04, 0.04, facecolor='#8B4513',
                          edgecolor='#654321', linewidth=2, zorder=19-i)
            ax.add_patch(box)

        if num_packages > 1:
            # Bundle badge
            ax.text(package_x, package_y - 0.08, f'{num_packages}x',
                   fontsize=12, color='red', fontweight='bold',
                   bbox=dict(boxstyle='round', facecolor='yellow',
                           edgecolor='red', linewidth=2))


def calculate_street_path(start, end):
    """Calculate path along streets (Manhattan distance)."""
    # For now, simple L-shaped path
    # In reality, would use A* or similar for actual street routing
    path = [start]

    # Go horizontal first, then vertical
    intermediate = (end[0], start[1])
    path.append(intermediate)
    path.append(end)

    return path


def interpolate_position(start, end, progress):
    """Smoothly interpolate between two positions."""
    x = start[0] + (end[0] - start[0]) * progress
    y = start[1] + (end[1] - start[1]) * progress
    return (x, y)


def create_algorithm_animation(algo_name, scenario, output_path):
    """Create a realistic animation for a single algorithm."""

    print(f"\nCreating realistic animation for {algo_name}...")

    # Run simulation
    state = run_simulation(scenario, get_algorithm(algo_name), algo_name)

    # Setup figure
    fig, ax = plt.subplots(figsize=(12, 12))

    # Store courier paths for smooth movement
    courier_paths = {}
    courier_previous_pos = {}

    def update(frame):
        if frame >= len(state.timeline):
            return ax,

        snapshot = state.timeline[frame]
        time = snapshot['time']

        # Draw town layout
        restaurants = list(state.restaurants.values())
        create_town_layout(ax, restaurants, snapshot['orders'])

        # Title with time
        minutes = int(time // 60)
        seconds = int(time % 60)
        title_map = {
            'greedy': 'GREEDY - First Come First Serve',
            'hungarian': 'HUNGARIAN - Optimal Matching',
            'simple_bundling': 'SIMPLE BUNDLING - Group Nearby Orders',
            'route_cost_bundling': 'ROUTE COST - Smart Multi-Stop Routes'
        }

        ax.text(TOWN_SIZE/2, TOWN_SIZE + 0.1,
               f'{title_map.get(algo_name, algo_name)}',
               fontsize=18, fontweight='bold', ha='center')
        ax.text(TOWN_SIZE/2, TOWN_SIZE + 0.05,
               f'Time: {minutes:02d}:{seconds:02d}',
               fontsize=14, ha='center')

        # Draw ready orders at restaurants
        restaurant_orders = {}
        for order_id, order_data in snapshot['orders'].items():
            if order_data['state'] == 'READY':
                r_id = order_data.get('restaurant_id', 0)
                if r_id not in restaurant_orders:
                    restaurant_orders[r_id] = 0
                restaurant_orders[r_id] += 1

        # Show order count badges at restaurants
        for r_id, count in restaurant_orders.items():
            if count > 0 and r_id < len(restaurants):
                r = restaurants[r_id]
                # Order ready indicator
                ax.text(r.location[0] + BUILDING_SIZE, r.location[1] + BUILDING_SIZE,
                       f'{count} orders ready!',
                       fontsize=10, color='white', fontweight='bold',
                       bbox=dict(boxstyle='round', facecolor='orange',
                               edgecolor='darkorange', linewidth=2))

        # Draw couriers
        for c_id, courier_data in snapshot['couriers'].items():
            c_idx = int(c_id)
            if c_idx >= 5:  # Limit to 5 couriers for clarity
                continue

            loc = courier_data['current_location']
            color = COURIER_COLORS[c_idx % len(COURIER_COLORS)]

            # Calculate heading (direction of movement)
            if c_id not in courier_previous_pos:
                courier_previous_pos[c_id] = loc
                heading = (1, 0)  # Default facing right
            else:
                prev = courier_previous_pos[c_id]
                dx = loc[0] - prev[0]
                dy = loc[1] - prev[1]
                if abs(dx) > 0.001 or abs(dy) > 0.001:
                    heading = (dx, dy)
                else:
                    heading = (1, 0)
                courier_previous_pos[c_id] = loc

            # Get number of packages
            num_packages = len(courier_data.get('assigned_order_ids', []))

            # Draw vehicle
            draw_courier_vehicle(ax, loc[0], loc[1], heading, color,
                               c_id, courier_data['state'], num_packages)

            # Draw planned route
            if courier_data.get('next_destination'):
                dest = courier_data['next_destination']

                # Calculate street path
                path = calculate_street_path(loc, dest)

                # Draw path along streets
                for i in range(len(path) - 1):
                    start = path[i]
                    end = path[i + 1]

                    # Thicker line for bundles
                    linewidth = 4 if num_packages > 1 else 2

                    ax.plot([start[0], end[0]], [start[1], end[1]],
                           color=color, linestyle='--', linewidth=linewidth,
                           alpha=0.5, zorder=5)

                # Draw destination marker
                ax.plot(dest[0], dest[1], 'X', color=color, markersize=15,
                       markeredgecolor='black', markeredgewidth=1)

        # Metrics panel
        metrics = snapshot['metrics']

        # Create metrics box - no emoji
        metrics_text = (
            f"PERFORMANCE METRICS\n"
            f"Orders Delivered: {metrics['orders_delivered']}/8\n"
            f"Distance Traveled: {metrics['total_distance_traveled']:.1f} km\n"
            f"Bundles Created: {metrics['bundles_created']}\n"
            f"Avg Bundle Size: {metrics.get('avg_bundle_size', 0):.1f}"
        )

        # Different background color based on algorithm efficiency
        if algo_name in ['simple_bundling', 'route_cost_bundling']:
            bg_color = 'lightgreen' if metrics['bundles_created'] > 0 else 'lightblue'
        else:
            bg_color = 'lightblue'

        ax.text(0.1, TOWN_SIZE - 0.2, metrics_text,
               fontsize=11, fontweight='bold',
               bbox=dict(boxstyle='round', facecolor=bg_color,
                       edgecolor='black', linewidth=2, alpha=0.9))

        # Time phase indicator - no emoji
        if time < 120:
            phase = "Orders Coming In"
            phase_color = 'blue'
        elif 120 <= time < 420:
            phase = "Preparing Meals"
            phase_color = 'orange'
        elif 420 <= time < 540:
            phase = "Peak Delivery Rush!"
            phase_color = 'red'
        else:
            phase = "Completing Deliveries"
            phase_color = 'green'

        ax.text(TOWN_SIZE - 0.5, TOWN_SIZE - 0.2, phase,
               fontsize=12, color=phase_color, fontweight='bold',
               bbox=dict(boxstyle='round', facecolor='white',
                       edgecolor=phase_color, linewidth=2))

        return ax,

    # Create animation
    num_frames = min(600, len(state.timeline))  # 10 minutes max
    anim = FuncAnimation(fig, update, frames=num_frames, interval=50, blit=False)

    # Save GIF
    print(f"  Saving to {output_path}...")
    writer = PillowWriter(fps=20)  # Smooth 20 fps
    anim.save(output_path, writer=writer, dpi=80)
    print(f"  Saved!")

    plt.close(fig)
    return state.metrics


# Main execution
if __name__ == "__main__":
    print("=" * 60)
    print("CREATING REALISTIC TOWN VISUALIZATIONS")
    print("=" * 60)

    # Generate scenario
    scenario = generate_asymmetric_scenario(duration=600)

    # Algorithms to visualize
    algorithms = ['greedy', 'hungarian', 'simple_bundling', 'route_cost_bundling']

    # Create animations for each algorithm
    all_metrics = {}
    for algo_name in algorithms:
        output_path = f'gifs/realistic_{algo_name}.gif'
        metrics = create_algorithm_animation(algo_name, scenario, output_path)
        all_metrics[algo_name] = metrics

    # Print comparison
    print("\n" + "=" * 60)
    print("ALGORITHM PERFORMANCE COMPARISON")
    print("=" * 60)

    for algo_name, metrics in all_metrics.items():
        print(f"\n{algo_name.upper()}:")
        print(f"  Orders Delivered: {metrics['orders_delivered']}")
        print(f"  Total Distance: {metrics['total_distance_traveled']:.1f} km")
        print(f"  Bundles Created: {metrics['bundles_created']}")
        print(f"  Avg Bundle Size: {metrics.get('avg_bundle_size', 0):.2f}")
        print(f"  Courier Utilization: {metrics['courier_utilization']:.1f}%")

    print("\n" + "=" * 60)
    print("✅ ALL REALISTIC ANIMATIONS CREATED!")
    print("=" * 60)
    print("\nGIF files created:")
    for algo in algorithms:
        print(f"  - gifs/realistic_{algo}.gif")