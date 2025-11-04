#!/usr/bin/env python3
"""
Create showcase GIFs focused on peak period with slower animation speed.
These shorter GIFs are ideal for presentations and documentation.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from create_clean_simple_viz import *
from simulator_core import SimulationState
from assignment_algorithms import *
import numpy as np

# Override animation speed for showcase
SHOWCASE_ANIMATION_SPEED = 5  # 5x speed (slower than full 10x)


def find_peak_hour(timeline):
    """Find the busiest 1-hour window in the simulation."""
    if not timeline:
        return 0, min(3600, len(timeline))

    max_activity = 0
    peak_start = 0
    window_size = min(3600, len(timeline))  # 1 hour in seconds

    # Calculate activity for each 1-hour window
    for start_idx in range(max(1, len(timeline) - window_size + 1)):
        end_idx = min(start_idx + window_size, len(timeline))
        window = timeline[start_idx:end_idx]

        # Activity = orders in transit + orders delivered in this window
        activity = 0
        for snapshot in window:
            orders_in_transit = sum(
                1 for o in snapshot['orders'].values()
                if o.get('state') in ['ASSIGNED', 'PICKED_UP']
            )
            activity += orders_in_transit

        if activity > max_activity:
            max_activity = activity
            peak_start = start_idx

    peak_end = min(peak_start + window_size, len(timeline))

    # Convert to hours for display
    peak_start_mins = peak_start // 60
    peak_end_mins = peak_end // 60
    print(f"  Peak period detected: minutes {peak_start_mins}-{peak_end_mins} ({max_activity} total activity)")

    return peak_start, peak_end


def create_showcase_animation(algo_name, scenario, output_path, peak_start, peak_end):
    """Create a showcase animation for the peak period only."""

    # Run simulation
    print(f"\nCreating showcase animation for {algo_name}...")
    state = SimulationState(scenario)

    assignment_func = {
        'greedy': assign_greedy,
        'hungarian': assign_hungarian,
        'simple_bundling': assign_simple_bundling,
        'route_cost_bundling': assign_route_cost_bundling,
        'batched_pickups': assign_batched_pickups
    }[algo_name]

    while state.current_time < state.duration:
        state.step(assignment_func)

    # Extract peak period from timeline
    peak_timeline = state.timeline[peak_start:peak_end]

    if not peak_timeline:
        print(f"  Warning: No timeline data for peak period")
        return state.metrics

    # Create figure
    fig, ax = plt.subplots(figsize=(12, 10))

    def update(frame_idx):
        ax.clear()
        if frame_idx >= len(peak_timeline):
            return ax,

        snapshot = peak_timeline[frame_idx]
        metrics = snapshot['metrics']
        time = peak_start + frame_idx  # Adjust time to actual simulation time

        # Grid setup
        ax.set_xlim(-0.5, GRID_SIZE + 0.5)
        ax.set_ylim(-1.0, GRID_SIZE + 0.7)
        ax.set_aspect('equal')
        ax.axis('off')
        ax.set_facecolor('#F5F5F5')

        # Title with "SHOWCASE" label
        display_name = ALGORITHM_DISPLAY_NAMES.get(algo_name, algo_name.replace('_', ' ').title())
        ax.text(GRID_SIZE/2, GRID_SIZE + 0.3,
               f'[PEAK HOUR] {display_name}',
               fontsize=14, fontweight='bold', ha='center', color='#D32F2F')

        # Time display (adjusted position)
        total_minutes = int(time // 60)
        hours = total_minutes // 60
        minutes = total_minutes % 60
        ax.text(GRID_SIZE - 0.2, GRID_SIZE - 0.15,
               f'{hours:02d}:{minutes:02d}',
               fontsize=12, ha='right')

        # Legend
        legend_text = (
            '■ Restaurant  ● Customer\n'
            '▲ Courier  ⋯→ Route'
        )
        ax.text(0.15, GRID_SIZE - 0.15, legend_text,
               fontsize=8, verticalalignment='top', horizontalalignment='left',
               bbox=dict(boxstyle='round', facecolor='white', alpha=0.85,
                       edgecolor='#666666', linewidth=1.5),
               zorder=25)

        # Draw restaurants
        for rid, restaurant in snapshot['restaurants'].items():
            x, y = restaurant['location']
            ax.add_patch(plt.Rectangle((x-0.15, y-0.15), 0.3, 0.3,
                                      facecolor='#FFA500', edgecolor='#FF8C00',
                                      linewidth=2, zorder=10))
            ax.text(x, y, 'R', fontsize=12, ha='center', va='center',
                   color='white', fontweight='bold', zorder=11)

        # Draw orders and couriers
        for oid, order in snapshot['orders'].items():
            if order['state'] in ['PENDING', 'ASSIGNED', 'PICKED_UP', 'DELIVERED']:
                cx, cy = order['customer_location']

                if order['state'] == 'DELIVERED':
                    color = '#90EE90'
                    edge_color = '#228B22'
                elif order['state'] in ['ASSIGNED', 'PICKED_UP']:
                    color = '#87CEEB'
                    edge_color = '#4682B4'
                else:
                    color = '#FFE4B5'
                    edge_color = '#DEB887'

                ax.add_patch(plt.Circle((cx, cy), 0.12,
                                       facecolor=color, edgecolor=edge_color,
                                       linewidth=1.5, zorder=5))

        # Draw couriers and routes
        for cid, courier in snapshot['couriers'].items():
            x, y = courier['current_location']
            color = COURIER_COLORS[cid % len(COURIER_COLORS)]

            ax.add_patch(plt.Polygon([(x, y+0.2), (x-0.15, y-0.15), (x+0.15, y-0.15)],
                                    facecolor=color, edgecolor='black',
                                    linewidth=2, zorder=20))

            # Draw route if exists
            if courier.get('route') and len(courier['route']) > 0:
                route_x = [x]
                route_y = [y]
                for stop in courier['route']:
                    sx, sy = stop['location']
                    route_x.append(sx)
                    route_y.append(sy)

                ax.plot(route_x, route_y, '--', color=color, alpha=0.6,
                       linewidth=2, zorder=3)

                for sx, sy in zip(route_x[1:], route_y[1:]):
                    ax.plot(sx, sy, 'o', color=color, markersize=6, zorder=4)

        # Dashboard (simplified for showcase)
        orders_delivered = metrics.get('orders_delivered', 0)
        total_orders = len(snapshot['orders'])
        orders_in_transit = sum(1 for o in snapshot['orders'].values()
                               if o.get('state') in ['ASSIGNED', 'PICKED_UP'])

        total_distance_km = sum(c.get('total_distance_traveled', 0)
                              for c in snapshot['couriers'].values())

        dashboard_text = (
            f'Delivered: {orders_delivered}/{total_orders}  |  '
            f'In Transit: {orders_in_transit}  |  '
            f'Distance: {total_distance_km:.1f}km'
        )

        ax.text(GRID_SIZE/2, -0.15, dashboard_text,
               fontsize=11, ha='center', verticalalignment='top',
               bbox=dict(boxstyle='round', facecolor='#FFE4E1',
                       edgecolor='#DC143C', linewidth=2, pad=0.4))

        return ax,

    # Sample frames for showcase (every 10 seconds instead of 30)
    sample_interval = 10
    sampled_indices = list(range(0, len(peak_timeline), sample_interval))

    def update_sampled(idx):
        return update(sampled_indices[idx] if idx < len(sampled_indices) else len(peak_timeline)-1)

    # Animation settings for showcase
    fps = 10 * SHOWCASE_ANIMATION_SPEED
    interval = 1000 / fps

    anim = FuncAnimation(fig, update_sampled, frames=len(sampled_indices),
                        interval=interval, blit=False)

    # Save showcase GIF
    print(f"  Saving showcase to {output_path}...")
    writer = PillowWriter(fps=fps)
    duration_seconds = len(sampled_indices) / fps
    print(f"  Animation: {len(sampled_indices)} frames at {fps} fps = {duration_seconds:.1f} seconds")
    anim.save(output_path, writer=writer, dpi=80)
    print(f"  Saved!")

    plt.close(fig)
    return state.metrics


if __name__ == "__main__":
    print("=" * 60)
    print("CREATING SHOWCASE GIFS (PEAK HOUR ONLY)")
    print("=" * 60)

    # Generate same scenario as full animation
    scenario = generate_dense_continuous_scenario()
    print(f"Using same scenario: {SIMULATION_HOURS} hours simulation")
    print(f"  {len(scenario['restaurants'])} restaurants")
    print(f"  {len(scenario['couriers'])} couriers")
    print(f"  Animation speed: {SHOWCASE_ANIMATION_SPEED}x")

    # First, run one simulation to find peak hour
    print("\nFinding peak hour...")
    state = SimulationState(scenario)
    assignment_func = assign_simple_bundling  # Use any algorithm for detection
    while state.current_time < state.duration:
        state.step(assignment_func)

    peak_start, peak_end = find_peak_hour(state.timeline)

    # Create showcase animations for clean 4-algorithm progression
    algorithms = ['greedy', 'hungarian', 'simple_bundling', 'batched_pickups']

    for algo_name in algorithms:
        filename = ALGORITHM_FILENAMES[algo_name]
        output_path = f'gifs/showcase_{filename}.gif'
        create_showcase_animation(algo_name, scenario, output_path, peak_start, peak_end)

    print("\n" + "=" * 60)
    print("✓ ALL SHOWCASE GIFS CREATED!")
    print("=" * 60)
    print("\nShowcase GIF files created (peak hour only):")
    for algo in algorithms:
        filename = ALGORITHM_FILENAMES[algo]
        print(f"  - gifs/showcase_{filename}.gif")
    print("\n")