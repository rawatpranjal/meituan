"""
Create smooth, highly zoomed GIFs showing courier movement second by second.
"""

import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation, PillowWriter
from simulator_core import generate_asymmetric_scenario, run_simulation
from assignment_algorithms import get_algorithm
import numpy as np

# TIGHT zoom on just the hub area
ZOOM_XLIM = (1.3, 2.7)  # Very tight focus on hub
ZOOM_YLIM = (1.3, 2.7)

print("Creating smooth, zoomed GIFs with second-by-second movement...")

# Generate scenario - need enough time for orders to be ready
scenario = generate_asymmetric_scenario(duration=600)  # 10 minutes to allow for meal prep
algorithms = ['greedy', 'hungarian', 'simple_bundling', 'route_cost_bundling']

# Run simulations
results = {}
for algo_name in algorithms:
    print(f"  Running {algo_name}...")
    state = run_simulation(scenario, get_algorithm(algo_name), algo_name)
    results[algo_name] = state
    print(f"    Timeline frames: {len(state.timeline)}")

# Create individual GIF for each algorithm
for algo_idx, algo_name in enumerate(algorithms):
    print(f"\nCreating smooth GIF for {algo_name}...")

    fig, ax = plt.subplots(figsize=(10, 10))

    title_map = {
        'greedy': 'Greedy (No Bundling)',
        'hungarian': 'Hungarian (Optimal 1-to-1)',
        'simple_bundling': 'Simple Bundling (BUNDLES!)',
        'route_cost_bundling': 'Route Cost (Smart Routes)'
    }

    # Store courier trails for smooth movement visualization
    courier_trails = {}

    def update(frame):
        ax.clear()

        # TIGHT zoom on hub
        ax.set_xlim(ZOOM_XLIM)
        ax.set_ylim(ZOOM_YLIM)
        ax.set_aspect('equal')
        ax.grid(True, alpha=0.1)
        ax.set_xlabel('X (km)', fontsize=12)
        ax.set_ylabel('Y (km)', fontsize=12)

        if frame >= len(results[algo_name].timeline):
            return ax,

        snapshot = results[algo_name].timeline[frame]
        time = snapshot['time']

        # Title with time in MM:SS format
        minutes = int(time // 60)
        seconds = int(time % 60)
        ax.set_title(f'{title_map[algo_name]}\nTime: {minutes:02d}:{seconds:02d}',
                    fontsize=16, fontweight='bold')

        # Draw restaurants LARGE
        restaurants = scenario['restaurants']
        for r_idx, r in enumerate(restaurants):
            if ZOOM_XLIM[0] <= r.location[0] <= ZOOM_XLIM[1] and ZOOM_YLIM[0] <= r.location[1] <= ZOOM_YLIM[1]:
                ax.scatter(r.location[0], r.location[1], marker='*', s=1200, c='green',
                          edgecolor='black', linewidth=3, zorder=10)

                # Big restaurant labels
                label = f'R{r_idx+1}'
                if r_idx < 2:
                    label += '\n(HUB)'
                ax.text(r.location[0], r.location[1]-0.06, label,
                       ha='center', fontsize=12, fontweight='bold', color='darkgreen')

        # Count and show ready orders at restaurants
        restaurant_ready = {0: 0, 1: 0, 2: 0}
        for order_id, order_data in snapshot['orders'].items():
            if order_data['state'] == 'READY':
                r_id = order_data.get('restaurant_id', 0)
                if r_id in restaurant_ready:
                    restaurant_ready[r_id] += 1

        # Show ready orders as stacked yellow dots
        for r_idx, count in restaurant_ready.items():
            if count > 0 and r_idx < len(restaurants):
                r = restaurants[r_idx]
                if ZOOM_XLIM[0] <= r.location[0] <= ZOOM_XLIM[1]:
                    # Draw a circle of yellow dots around restaurant
                    for i in range(min(count, 8)):  # Max 8 dots
                        angle = (i / 8) * 2 * np.pi
                        offset_x = 0.08 * np.cos(angle)
                        offset_y = 0.08 * np.sin(angle)
                        ax.scatter(r.location[0] + offset_x, r.location[1] + offset_y,
                                 marker='o', s=150, c='yellow',
                                 edgecolor='orange', linewidth=2, zorder=9)

                    # Show count
                    if count > 0:
                        ax.text(r.location[0] + 0.12, r.location[1] + 0.12, f'{count}',
                               fontsize=14, fontweight='bold', color='red',
                               bbox=dict(boxstyle='circle', facecolor='yellow', edgecolor='red'))

        # Draw order destinations (diner locations)
        for order_id, order_data in snapshot['orders'].items():
            if order_data['state'] in ['ASSIGNED', 'PICKED_UP', 'DELIVERED']:
                diner_loc = order_data['diner_location']
                if ZOOM_XLIM[0] <= diner_loc[0] <= ZOOM_XLIM[1] and ZOOM_YLIM[0] <= diner_loc[1] <= ZOOM_YLIM[1]:
                    color_map = {'ASSIGNED': 'orange', 'PICKED_UP': 'cyan', 'DELIVERED': 'lime'}
                    ax.scatter(diner_loc[0], diner_loc[1], marker='H', s=200,  # Hexagon for houses
                             c=color_map.get(order_data['state'], 'gray'),
                             edgecolor='black', linewidth=2, alpha=0.8, zorder=5)

        # Draw couriers with smooth movement
        colors = ['blue', 'red', 'purple', 'darkgreen', 'brown']
        for c_id, courier_data in snapshot['couriers'].items():
            c_idx = int(c_id)
            loc = courier_data['current_location']

            # Store trail for this courier
            if c_id not in courier_trails:
                courier_trails[c_id] = []

            # Only draw if in view
            if (ZOOM_XLIM[0]-0.2 <= loc[0] <= ZOOM_XLIM[1]+0.2 and
                ZOOM_YLIM[0]-0.2 <= loc[1] <= ZOOM_YLIM[1]+0.2):

                color = colors[c_idx % len(colors)]

                # Draw courier trail (last 10 positions)
                if len(courier_trails[c_id]) > 0:
                    trail = courier_trails[c_id][-10:]  # Last 10 positions
                    for i, trail_pos in enumerate(trail):
                        alpha = 0.1 + (i / len(trail)) * 0.3  # Fade trail
                        ax.scatter(trail_pos[0], trail_pos[1], marker='o',
                                 s=20, c=color, alpha=alpha, zorder=3)

                courier_trails[c_id].append(loc)

                # Courier state affects appearance
                if courier_data['state'] == 'IDLE':
                    marker_size = 600
                    alpha = 0.5
                    marker = 'D'  # Diamond when idle
                else:
                    marker_size = 800
                    alpha = 1.0
                    marker = '^'  # Triangle when active

                # Draw courier BIG
                ax.scatter(loc[0], loc[1], marker=marker, s=marker_size, c=color,
                          edgecolor='black', linewidth=3, alpha=alpha, zorder=8)

                # Courier label
                ax.text(loc[0], loc[1]+0.05, f'C{c_id}', ha='center',
                       fontsize=11, color='white', fontweight='bold',
                       bbox=dict(boxstyle='round', facecolor=color, alpha=0.8))

                # Draw route if traveling
                if courier_data.get('next_destination'):
                    dest = courier_data['next_destination']
                    num_orders = len(courier_data.get('assigned_order_ids', []))

                    # THICK line for bundles
                    linewidth = 6 if num_orders > 1 else 3

                    # Animated dashed line
                    dash_offset = (frame * 2) % 20  # Animate the dash
                    ax.plot([loc[0], dest[0]], [loc[1], dest[1]],
                           color=color, linewidth=linewidth, alpha=0.7,
                           linestyle='--', dashes=[10, 5], dash_capstyle='round')

                    # Big arrow
                    ax.annotate('', xy=(dest[0], dest[1]), xytext=(loc[0], loc[1]),
                               arrowprops=dict(arrowstyle='->', color=color,
                                             lw=linewidth, alpha=0.7))

                    # Show bundle size prominently
                    if num_orders > 1:
                        mid_x = (loc[0] + dest[0]) / 2
                        mid_y = (loc[1] + dest[1]) / 2
                        if ZOOM_XLIM[0] <= mid_x <= ZOOM_XLIM[1]:
                            ax.text(mid_x, mid_y, f'BUNDLE\n{num_orders} ORDERS',
                                   fontsize=13, fontweight='bold', color='white',
                                   bbox=dict(boxstyle='round', facecolor=color,
                                           edgecolor='yellow', linewidth=3))

        # BIG metrics overlay
        metrics = snapshot['metrics']
        metrics_text = (
            f"Delivered: {metrics['orders_delivered']}/8\n"
            f"Bundles: {metrics['bundles_created']}\n"
            f"Bundle Size: {metrics.get('avg_bundle_size', 0):.1f}"
        )
        ax.text(0.02, 0.98, metrics_text, transform=ax.transAxes,
               fontsize=14, verticalalignment='top', fontweight='bold',
               bbox=dict(boxstyle='round', facecolor='lightblue', alpha=0.9))

        # Phase indicator
        if 60 <= time <= 180:
            phase = "⚠️ ORDERS ARRIVING"
            phase_color = 'orange'
        elif 180 < time <= 240:
            phase = "🔥 PEAK RUSH"
            phase_color = 'red'
        else:
            phase = "✓ Delivering"
            phase_color = 'green'

        ax.text(0.98, 0.98, phase, transform=ax.transAxes,
               ha='right', fontsize=16, color=phase_color, fontweight='bold',
               bbox=dict(boxstyle='round', facecolor='white', edgecolor=phase_color, linewidth=2))

        return ax,

    # Create animation with MANY frames for smooth movement
    num_frames = min(600, len(results[algo_name].timeline))  # Every second for up to 10 minutes
    anim = FuncAnimation(fig, update, frames=num_frames, interval=100, blit=False)

    # Save GIF
    output_path = f'gifs/smooth_{algo_name}.gif'
    print(f"  Saving to {output_path}...")
    writer = PillowWriter(fps=10)  # 10 fps for smooth playback
    anim.save(output_path, writer=writer, dpi=80)
    print(f"  Saved!")

    plt.close(fig)

print("\nAll smooth GIFs created successfully!")
print("\nSmooth GIF files created:")
for algo in algorithms:
    print(f"  - gifs/smooth_{algo}.gif")