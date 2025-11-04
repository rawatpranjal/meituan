"""
Create individual GIF for each algorithm with zoomed view on hub area.
"""

import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation, PillowWriter
from simulator_core import generate_asymmetric_scenario, run_simulation
from assignment_algorithms import get_algorithm
import numpy as np

# Configuration for zoomed view
ZOOM_XLIM = (1.0, 3.5)  # Focus on hub area
ZOOM_YLIM = (1.0, 3.5)

print("Creating individual GIFs for each algorithm...")

# Generate scenario
scenario = generate_asymmetric_scenario(duration=600)
algorithms = ['greedy', 'hungarian', 'simple_bundling', 'route_cost_bundling', 'batched_pickups']

# Run all simulations first
results = {}
for algo_name in algorithms:
    print(f"  Running {algo_name}...")
    state = run_simulation(scenario, get_algorithm(algo_name), algo_name)
    results[algo_name] = state

# Create individual GIF for each algorithm
for algo_idx, algo_name in enumerate(algorithms):
    print(f"\nCreating GIF for {algo_name}...")

    # Create figure for this algorithm
    fig, ax = plt.subplots(figsize=(10, 10))

    # Algorithm title mapping
    title_map = {
        'greedy': 'Greedy Algorithm (Baseline)',
        'hungarian': 'Hungarian Algorithm (Optimal 1-to-1)',
        'simple_bundling': 'Simple Bundling (Volume Focus)',
        'route_cost_bundling': 'Route Cost Bundling (Efficiency)',
        'batched_pickups': 'Batched Pickups (Multi-Restaurant)'
    }

    def update(frame):
        ax.clear()

        # Set zoomed view
        ax.set_xlim(ZOOM_XLIM)
        ax.set_ylim(ZOOM_YLIM)
        ax.set_aspect('equal')
        ax.grid(True, alpha=0.2)
        ax.set_xlabel('X (km)', fontsize=12)
        ax.set_ylabel('Y (km)', fontsize=12)

        if frame >= len(results[algo_name].timeline):
            return ax,

        snapshot = results[algo_name].timeline[frame]
        time = snapshot['time']

        ax.set_title(f'{title_map[algo_name]}\nTime: {int(time)}s', fontsize=14, fontweight='bold')

        # Draw restaurants
        restaurants = scenario['restaurants']
        for r_idx, r in enumerate(restaurants):
            # Only draw if in view
            if ZOOM_XLIM[0] <= r.location[0] <= ZOOM_XLIM[1] and ZOOM_YLIM[0] <= r.location[1] <= ZOOM_YLIM[1]:
                ax.scatter(r.location[0], r.location[1], marker='*', s=800, c='green',
                          edgecolor='black', linewidth=2, zorder=10)

                # Label hub restaurants
                if r_idx < 2:
                    ax.text(r.location[0], r.location[1]-0.08, f'R{r_idx+1}\n(Hub)',
                           ha='center', fontsize=10, fontweight='bold')

        # Count ready orders at each restaurant
        restaurant_ready = {0: 0, 1: 0, 2: 0}
        for order_id, order_data in snapshot['orders'].items():
            if order_data['state'] == 'READY':
                r_id = order_data.get('restaurant_id', 0)
                if r_id in restaurant_ready:
                    restaurant_ready[r_id] += 1

        # Show ready order counts at restaurants
        for r_idx, count in restaurant_ready.items():
            if count > 0 and r_idx < len(restaurants):
                r = restaurants[r_idx]
                if ZOOM_XLIM[0] <= r.location[0] <= ZOOM_XLIM[1]:
                    # Stack yellow dots to represent ready orders
                    for i in range(min(count, 5)):  # Max 5 dots to avoid clutter
                        offset = 0.05 * (i + 1)
                        ax.scatter(r.location[0] + offset, r.location[1],
                                 marker='o', s=100, c='yellow',
                                 edgecolor='orange', linewidth=1, zorder=9)

        # Draw orders being delivered
        for order_id, order_data in snapshot['orders'].items():
            if order_data['state'] in ['ASSIGNED', 'PICKED_UP', 'DELIVERED']:
                diner_loc = order_data['diner_location']
                # Only draw if in view
                if ZOOM_XLIM[0] <= diner_loc[0] <= ZOOM_XLIM[1] and ZOOM_YLIM[0] <= diner_loc[1] <= ZOOM_YLIM[1]:
                    color_map = {'ASSIGNED': 'orange', 'PICKED_UP': 'cyan', 'DELIVERED': 'lime'}
                    ax.scatter(diner_loc[0], diner_loc[1], marker='o', s=150,
                             c=color_map.get(order_data['state'], 'gray'),
                             edgecolor='black', linewidth=1, alpha=0.7, zorder=5)

        # Draw couriers with movement animation
        colors = ['blue', 'red', 'purple', 'darkgreen', 'brown']
        for c_id, courier_data in snapshot['couriers'].items():
            c_idx = int(c_id)
            loc = courier_data['current_location']

            # Only draw if in view or has destination in view
            if (ZOOM_XLIM[0]-0.5 <= loc[0] <= ZOOM_XLIM[1]+0.5 and
                ZOOM_YLIM[0]-0.5 <= loc[1] <= ZOOM_YLIM[1]+0.5):

                color = colors[c_idx % len(colors)]

                # Courier state affects appearance
                if courier_data['state'] == 'IDLE':
                    marker_size = 400
                    alpha = 0.5
                else:
                    marker_size = 500
                    alpha = 1.0

                # Draw courier
                ax.scatter(loc[0], loc[1], marker='^', s=marker_size, c=color,
                          edgecolor='black', linewidth=2, alpha=alpha, zorder=8)

                # Courier label
                ax.text(loc[0], loc[1]+0.06, f'C{c_id}', ha='center',
                       fontsize=9, color=color, fontweight='bold')

                # Draw route if traveling
                if courier_data.get('next_destination'):
                    dest = courier_data['next_destination']
                    num_orders = len(courier_data.get('assigned_order_ids', []))

                    # Thicker line for bundles
                    linewidth = 4 if num_orders > 1 else 2

                    # Draw path
                    ax.plot([loc[0], dest[0]], [loc[1], dest[1]],
                           color=color, linewidth=linewidth, alpha=0.6, linestyle='--')

                    # Arrow for direction
                    ax.annotate('', xy=(dest[0], dest[1]), xytext=(loc[0], loc[1]),
                               arrowprops=dict(arrowstyle='->', color=color,
                                             lw=linewidth, alpha=0.6))

                    # Show bundle info if multiple orders
                    if num_orders > 1:
                        mid_x = (loc[0] + dest[0]) / 2
                        mid_y = (loc[1] + dest[1]) / 2
                        if ZOOM_XLIM[0] <= mid_x <= ZOOM_XLIM[1]:
                            ax.text(mid_x, mid_y, f'{num_orders} orders',
                                   fontsize=11, fontweight='bold', color=color,
                                   bbox=dict(boxstyle='round', facecolor='white',
                                           edgecolor=color, linewidth=2))

        # Metrics overlay
        metrics = snapshot['metrics']
        metrics_text = (
            f"Delivered: {metrics['orders_delivered']}/8\n"
            f"Bundles: {metrics['bundles_created']}\n"
            f"Avg Bundle: {metrics.get('avg_bundle_size', 0):.1f}\n"
            f"Distance: {metrics.get('total_distance_traveled', 0):.1f} km"
        )
        ax.text(0.02, 0.98, metrics_text, transform=ax.transAxes,
               fontsize=12, verticalalignment='top', fontweight='bold',
               bbox=dict(boxstyle='round', facecolor='lightblue', alpha=0.9))

        # Add phase indicator
        phase = "Off-Peak"
        if 360 <= time <= 480:
            phase = "PEAK HOUR"
            phase_color = 'red'
        elif 480 < time <= 600:
            phase = "Clearing"
            phase_color = 'orange'
        else:
            phase_color = 'green'

        ax.text(0.98, 0.98, phase, transform=ax.transAxes,
               ha='right', fontsize=14, color=phase_color, fontweight='bold',
               bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))

        return ax,

    # Create animation (60 frames = 600s simulation, every 10s)
    anim = FuncAnimation(fig, update, frames=60, interval=200, blit=False)

    # Save GIF
    output_path = f'gifs/individual_{algo_name}_zoomed.gif'
    print(f"  Saving to {output_path}...")
    writer = PillowWriter(fps=5)
    anim.save(output_path, writer=writer, dpi=100)
    print(f"  Saved!")

    plt.close(fig)

print("\nAll individual GIFs created successfully!")
print("\nGIF files created:")
for algo in algorithms:
    print(f"  - gifs/individual_{algo}_zoomed.gif")