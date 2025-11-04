"""
Create GIF showing just the KEY decision moment (seconds 360-540) when orders become ready.
Super zoomed, smooth movement, clear bundling visualization.
"""

import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation, PillowWriter
from simulator_core import generate_asymmetric_scenario, run_simulation
from assignment_algorithms import get_algorithm
import numpy as np

# SUPER TIGHT zoom on just the hub restaurants
ZOOM_XLIM = (1.8, 2.4)  # SUPER tight - just the two hub restaurants
ZOOM_YLIM = (1.7, 2.3)

print("Creating KEY MOMENT GIF (seconds 360-540 when action happens)...")

# Generate scenario
scenario = generate_asymmetric_scenario(duration=600)

# Run only Simple Bundling to show the bundling effect clearly
algo_name = 'simple_bundling'
print(f"  Running {algo_name}...")
state = run_simulation(scenario, get_algorithm(algo_name), algo_name)
print(f"    Timeline frames: {len(state.timeline)}")

# Create the GIF
fig, ax = plt.subplots(figsize=(12, 12))

def update(frame_idx):
    ax.clear()

    # Map frame_idx to actual time (start at 360s)
    actual_frame = 360 + frame_idx
    if actual_frame >= len(state.timeline):
        return ax,

    snapshot = state.timeline[actual_frame]
    time = snapshot['time']

    # SUPER tight zoom
    ax.set_xlim(ZOOM_XLIM)
    ax.set_ylim(ZOOM_YLIM)
    ax.set_aspect('equal')
    ax.grid(True, alpha=0.05)
    ax.set_facecolor('#f8f8f8')  # Light gray background

    # Title with clear time
    minutes = int(time // 60)
    seconds = int(time % 60)
    ax.set_title(f'SIMPLE BUNDLING - Watch the Bundle Form!\nTime: {minutes:02d}:{seconds:02d}',
                fontsize=18, fontweight='bold')

    # Draw hub restaurants BIG
    restaurants = scenario['restaurants'][:2]  # Just hub restaurants
    for r_idx, r in enumerate(restaurants):
        ax.scatter(r.location[0], r.location[1], marker='*', s=2000, c='green',
                  edgecolor='black', linewidth=4, zorder=10)

        ax.text(r.location[0], r.location[1], f'R{r_idx+1}',
               ha='center', va='center', fontsize=14, fontweight='bold', color='white')

    # Count ready orders at each restaurant
    restaurant_ready = {0: 0, 1: 0}
    for order_id, order_data in snapshot['orders'].items():
        if order_data['state'] == 'READY':
            r_id = order_data.get('restaurant_id', 0)
            if r_id < 2:
                restaurant_ready[r_id] += 1

    # Show ready orders as BIG yellow circles around restaurants
    for r_idx, count in restaurant_ready.items():
        if count > 0:
            r = restaurants[r_idx]
            # Make a circle of yellow orders
            for i in range(count):
                angle = (i / max(4, count)) * 2 * np.pi
                radius = 0.12
                x = r.location[0] + radius * np.cos(angle)
                y = r.location[1] + radius * np.sin(angle)
                ax.scatter(x, y, marker='o', s=300, c='yellow',
                         edgecolor='darkorange', linewidth=3, zorder=9)

            # Big count badge
            if count > 0:
                ax.text(r.location[0] + 0.15, r.location[1] + 0.15, str(count),
                       fontsize=20, fontweight='bold', color='red',
                       bbox=dict(boxstyle='circle', facecolor='yellow',
                               edgecolor='red', linewidth=3))

    # Draw assigned/picked up orders at diner locations
    for order_id, order_data in snapshot['orders'].items():
        if order_data['state'] in ['ASSIGNED', 'PICKED_UP']:
            diner_loc = order_data['diner_location']
            if (ZOOM_XLIM[0] <= diner_loc[0] <= ZOOM_XLIM[1] and
                ZOOM_YLIM[0] <= diner_loc[1] <= ZOOM_YLIM[1]):
                color = 'cyan' if order_data['state'] == 'PICKED_UP' else 'orange'
                ax.scatter(diner_loc[0], diner_loc[1], marker='s', s=250,
                         c=color, edgecolor='black', linewidth=2, alpha=0.8, zorder=5)

    # Draw couriers
    colors = ['blue', 'red', 'purple', 'darkgreen']
    for c_id, courier_data in snapshot['couriers'].items():
        c_idx = int(c_id)
        if c_idx >= 4:  # Skip remote couriers for clarity
            continue

        loc = courier_data['current_location']

        # Check if in view
        if (ZOOM_XLIM[0]-0.3 <= loc[0] <= ZOOM_XLIM[1]+0.3 and
            ZOOM_YLIM[0]-0.3 <= loc[1] <= ZOOM_YLIM[1]+0.3):

            color = colors[c_idx % len(colors)]

            # Big courier marker
            if courier_data['state'] == 'IDLE':
                marker_size = 800
                alpha = 0.6
            else:
                marker_size = 1000
                alpha = 1.0

            ax.scatter(loc[0], loc[1], marker='^', s=marker_size, c=color,
                     edgecolor='white', linewidth=3, alpha=alpha, zorder=8)

            # Courier label
            ax.text(loc[0], loc[1], f'C{c_id}', ha='center', va='center',
                   fontsize=12, color='white', fontweight='bold')

            # Draw assignment if exists
            if courier_data.get('next_destination'):
                dest = courier_data['next_destination']
                num_orders = len(courier_data.get('assigned_order_ids', []))

                # VERY thick line for bundles
                linewidth = 8 if num_orders > 1 else 4

                # Animated arrow
                ax.annotate('', xy=(dest[0], dest[1]),
                           xytext=(loc[0], loc[1]),
                           arrowprops=dict(arrowstyle='->', color=color,
                                         lw=linewidth, alpha=0.8))

                # BUNDLE indicator
                if num_orders > 1:
                    # Draw a big badge showing bundle
                    mid_x = (loc[0] + dest[0]) / 2
                    mid_y = (loc[1] + dest[1]) / 2
                    ax.text(mid_x, mid_y, f'BUNDLE!\n{num_orders} orders',
                           fontsize=16, fontweight='bold', color='white',
                           ha='center', va='center',
                           bbox=dict(boxstyle='round', facecolor=color,
                                   edgecolor='yellow', linewidth=4))

    # Big metrics
    metrics = snapshot['metrics']
    avg_bundle = metrics.get('avg_bundle_size', 0)
    if avg_bundle > 1.5:
        bundle_text = f"✓ BUNDLING ACTIVE!"
        bundle_color = 'green'
    else:
        bundle_text = "No bundling yet..."
        bundle_color = 'gray'

    metrics_text = (
        f"Bundles Created: {metrics.get('bundles_created', 0)}\n"
        f"Avg Bundle Size: {avg_bundle:.1f}\n"
        f"{bundle_text}"
    )
    ax.text(0.02, 0.98, metrics_text, transform=ax.transAxes,
           fontsize=16, verticalalignment='top', fontweight='bold',
           bbox=dict(boxstyle='round', facecolor='lightblue', alpha=0.95))

    # Time phase indicator
    if time < 420:
        phase = "Waiting for orders..."
        phase_color = 'gray'
    elif 420 <= time < 480:
        phase = "R1 ORDERS READY!"
        phase_color = 'orange'
    elif 480 <= time < 540:
        phase = "R2 ORDERS READY!"
        phase_color = 'red'
    else:
        phase = "DELIVERING!"
        phase_color = 'green'

    ax.text(0.98, 0.02, phase, transform=ax.transAxes,
           ha='right', fontsize=18, color=phase_color, fontweight='bold')

    return ax,

# Create animation for the key period (360-540 seconds = 180 frames)
num_frames = 180
anim = FuncAnimation(fig, update, frames=num_frames, interval=50, blit=False)

# Save GIF
output_path = 'gifs/key_moment_bundling.gif'
print(f"  Saving to {output_path}...")
writer = PillowWriter(fps=20)  # 20 fps for very smooth
anim.save(output_path, writer=writer, dpi=72)
print(f"  Saved KEY MOMENT GIF!")

plt.close(fig)

print("\n✓ Created key_moment_bundling.gif showing the critical bundling decision!")