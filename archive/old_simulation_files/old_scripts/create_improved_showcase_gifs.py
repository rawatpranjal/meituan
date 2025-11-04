#!/usr/bin/env python3
"""
Improved Showcase GIFs - Focus on High-Activity Matching Windows

Key improvements:
1. Identify 30-minute window with highest matching activity (new orders + assignments)
2. Slower animation (2x speed) to appreciate matching process
3. Finer sampling (every 5 seconds) for smoother animation
4. Visual highlights for new assignments
5. Clearer visualization elements
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation, PillowWriter
from matplotlib.patches import Rectangle, Circle, Polygon
import numpy as np
from simulator_core import run_simulation, GRID_SIZE, SIMULATION_DURATION
from assignment_algorithms import get_algorithm
from create_clean_simple_viz import (
    COURIER_COLORS, ALGORITHM_DISPLAY_NAMES, ALGORITHM_FILENAMES,
    generate_dense_continuous_scenario, NUM_COURIERS
)
import math

# Improved animation parameters for 1-hour simulation
ANIMATION_SPEED = 2  # 2x speed (slower for better visibility)
SAMPLE_INTERVAL = 20  # Sample every 20 seconds (balanced for 1-hour → 9-second GIF)

# Enhanced visual sizes
RESTAURANT_SIZE = 0.12  # Slightly larger
HOUSE_SIZE = 0.10
COURIER_SIZE = 0.15
HIGHLIGHT_ALPHA = 0.3  # For assignment highlights


def find_peak_matching_window(timeline, window_minutes=30):
    """
    Find the most active window for matching activity.

    Activity score = new orders + new assignments + orders in transit
    This identifies periods where the matching process is most visible.

    Optimized version: samples every 60 seconds instead of every second.
    """
    if not timeline:
        return 0, min(window_minutes * 60, len(timeline))

    window_size = window_minutes * 60  # Convert to seconds
    max_score = 0
    peak_start = 0

    # Sample every 60 seconds for speed (analyzing 21600 snapshots is too slow)
    sample_interval = 60
    print(f"  Analyzing {len(timeline)} seconds of simulation (sampling every {sample_interval}s)...")

    # Calculate activity score for sampled windows
    for start_idx in range(0, len(timeline) - window_size + 1, sample_interval):
        end_idx = min(start_idx + window_size, len(timeline))

        # Sample snapshots within this window (every 60 seconds)
        sampled_snapshots = timeline[start_idx:end_idx:sample_interval]

        if not sampled_snapshots:
            continue

        # Count activity metrics (simplified for speed)
        max_in_transit = 0
        total_assignments = 0
        total_orders_seen = 0

        for snapshot in sampled_snapshots:
            # Orders in transit at this point
            in_transit = sum(1 for o in snapshot['orders'].values()
                           if o.get('state') in ['ASSIGNED', 'PICKED_UP'])
            max_in_transit = max(max_in_transit, in_transit)

            # Total assignments made so far
            total_assignments = snapshot['metrics'].get('bundles_created', 0)

            # Total orders seen
            total_orders_seen = len(snapshot['orders'])

        # Activity score: prioritize periods with high concurrent activity
        score = (max_in_transit * 10) + (total_assignments * 2) + (total_orders_seen * 0.5)

        if score > max_score:
            max_score = score
            peak_start = start_idx

    peak_end = min(peak_start + window_size, len(timeline))

    # Convert to time for display
    peak_start_mins = peak_start // 60
    peak_end_mins = peak_end // 60
    duration_mins = (peak_end - peak_start) // 60

    # Calculate stats for the selected window
    window = timeline[peak_start:peak_end]
    total_assignments = 0
    total_deliveries = 0
    for snapshot in window:
        total_assignments += snapshot['metrics'].get('bundles_created', 0)
        total_deliveries += snapshot['metrics'].get('orders_delivered', 0)

    print(f"  ✓ Peak window: {peak_start_mins:.1f}-{peak_end_mins:.1f} min ({duration_mins} min duration)")
    print(f"    Activity score: {max_score:.0f}")
    print(f"    Assignments in window: {total_assignments}")
    print(f"    Deliveries in window: {total_deliveries}")

    return peak_start, peak_end


def draw_enhanced_visualization(ax, snapshot, restaurants):
    """Draw enhanced visualization with clean arrows."""

    # Clear axis
    ax.clear()
    ax.set_xlim(0, GRID_SIZE)
    ax.set_ylim(0, GRID_SIZE)
    ax.set_aspect('equal')
    ax.axis('off')
    ax.set_facecolor('#FAFAFA')

    # Draw grid lines with better contrast
    grid_points = np.arange(0, GRID_SIZE + 0.5, 0.5)
    for i, pos in enumerate(grid_points):
        if i % 2 == 0:  # 1km lines
            ax.axhline(y=pos, color='#555555', linewidth=1.4, alpha=0.6)
            ax.axvline(x=pos, color='#555555', linewidth=1.4, alpha=0.6)
        else:  # 0.5km lines
            ax.axhline(y=pos, color='#999999', linewidth=0.9, alpha=0.4)
            ax.axvline(x=pos, color='#999999', linewidth=0.9, alpha=0.4)

    # Draw restaurants (enhanced with shadow)
    for restaurant in restaurants:
        r_x, r_y = restaurant.location
        # Shadow
        shadow = Rectangle((r_x - RESTAURANT_SIZE/2 + 0.02, r_y - RESTAURANT_SIZE/2 - 0.02),
                          RESTAURANT_SIZE, RESTAURANT_SIZE,
                          facecolor='#CCCCCC', edgecolor='none',
                          linewidth=0, zorder=9, alpha=0.5)
        ax.add_patch(shadow)
        # Restaurant
        rect = Rectangle((r_x - RESTAURANT_SIZE/2, r_y - RESTAURANT_SIZE/2),
                        RESTAURANT_SIZE, RESTAURANT_SIZE,
                        facecolor='#FF3333', edgecolor='#AA0000',
                        linewidth=2.5, zorder=10)
        ax.add_patch(rect)

    # Draw customer locations with state-based styling
    drawn_houses = set()
    for order_id, order_data in snapshot['orders'].items():
        if order_data['state'] in ['READY', 'ASSIGNED', 'PICKED_UP']:
            diner_loc = tuple(order_data['diner_location'])
            if diner_loc not in drawn_houses:
                drawn_houses.add(diner_loc)
                h_x, h_y = diner_loc

                # Customer marker
                circle = Circle((h_x, h_y), HOUSE_SIZE/2,
                              facecolor='#3333FF', edgecolor='#0000AA',
                              linewidth=2.5, zorder=8)
                ax.add_patch(circle)

    # Draw relay handoff points (purple diamonds)
    for order_id, order_data in snapshot['orders'].items():
        if order_data.get('is_relay') and order_data.get('relay_handoff_location'):
            handoff_loc = order_data['relay_handoff_location']
            if order_data.get('state') in ['ASSIGNED', 'PICKED_UP']:
                diamond_size = 0.18
                diamond_points = [
                    (handoff_loc[0], handoff_loc[1] + diamond_size/2),
                    (handoff_loc[0] + diamond_size/2, handoff_loc[1]),
                    (handoff_loc[0], handoff_loc[1] - diamond_size/2),
                    (handoff_loc[0] - diamond_size/2, handoff_loc[1])
                ]
                diamond = Polygon(diamond_points, facecolor='#9370DB',
                                edgecolor='#663399', linewidth=2.5, zorder=15)
                ax.add_patch(diamond)

    # Draw assignment arrows ONLY for orders assigned in last batch interval (BATCH PERSISTENCE)
    current_time = snapshot['time']
    BATCH_INTERVAL = 300  # seconds (5 minutes) - arrows persist for full batch duration

    for c_id, courier_data in snapshot['couriers'].items():
        c_idx = int(c_id)
        if c_idx >= NUM_COURIERS:
            continue

        loc = courier_data['current_location']
        color = COURIER_COLORS[c_idx % len(COURIER_COLORS)]
        assigned_order_ids = courier_data.get('assigned_order_ids', [])

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

        # Thin dotted lines for elegant routing visualization
        linewidth = 1.5
        linestyle = ':'  # Dotted line

        # Draw connected segments between consecutive waypoints
        for i in range(len(waypoints) - 1):
            start = waypoints[i]
            end = waypoints[i + 1]

            # L-shaped Manhattan routing
            mid_point = (end[0], start[1])

            # Horizontal segment
            ax.plot([start[0], mid_point[0]], [start[1], mid_point[1]],
                   color=color, linewidth=linewidth, alpha=0.8,
                   linestyle=linestyle, zorder=15)

            # Vertical segment
            ax.plot([mid_point[0], end[0]], [mid_point[1], end[1]],
                   color=color, linewidth=linewidth, alpha=0.8,
                   linestyle=linestyle, zorder=15)

            # Draw small waypoint markers at each stop (except courier start position)
            if i > 0:  # Skip first waypoint (courier location)
                waypoint_marker = Circle(waypoints[i], 0.05,
                                        facecolor=color, edgecolor='black',
                                        linewidth=1.0, alpha=0.7, zorder=17)
                ax.add_patch(waypoint_marker)


def create_improved_showcase_animation(algo_name, scenario, output_path):
    """Create a 15-frame animation of a 15-minute simulation."""

    print(f"\nCreating 15-minute showcase for {algo_name}...")

    # Run 15-minute simulation
    assignment_func = get_algorithm(algo_name)
    scenario['duration'] = 900  # 15 minutes
    state = run_simulation(scenario, assignment_func, algo_name)

    # Sample every 60 seconds for 15 frames total
    timeline = state.timeline
    sampled_indices = list(range(0, len(timeline), 60))

    if not sampled_indices:
        print(f"  ⚠ Warning: No timeline data to sample.")
        return state.metrics

    fig, ax = plt.subplots(figsize=(12, 12))
    restaurants = list(state.restaurants.values())
    
    assignment_arrows = []

    def update(frame_idx):
        nonlocal assignment_arrows
        snapshot = timeline[frame_idx]
        current_time = snapshot['time']

        # Clear arrows only at the start of a new batch
        if int(current_time) % 300 == 0:
            assignment_arrows = []
            for courier_data in snapshot['couriers'].values():
                assigned_order_ids = courier_data.get('assigned_order_ids', [])
                if assigned_order_ids:
                    first_order_id = assigned_order_ids[0]
                    if first_order_id in snapshot['orders']:
                        order = snapshot['orders'][first_order_id]
                        assignment_time = order.get('assignment_time')
                        if assignment_time and (current_time - assignment_time) < 5:
                            assignment_arrows.append({
                                'courier_loc': courier_data['current_location'],
                                'resto_loc': order['restaurant_location'],
                                'color': COURIER_COLORS[int(courier_data['id']) % len(COURIER_COLORS)]
                            })

        draw_enhanced_visualization(ax, snapshot, restaurants, assignment_arrows)

        display_name = ALGORITHM_DISPLAY_NAMES.get(algo_name, algo_name.replace('_', ' ').title())
        ax.set_title(f'{display_name} - 15-Minute Simulation', fontsize=16, fontweight='bold')

        return ax,

    # Create animation with 1 frame per minute
    anim = FuncAnimation(fig, lambda i: update(sampled_indices[i]), frames=len(sampled_indices), blit=False)

    # Save GIF
    print(f"  💾 Saving to {output_path}...")
    writer = PillowWriter(fps=1)  # 1 frame per second in the GIF
    anim.save(output_path, writer=writer, dpi=100)
    print(f"  ✅ Saved! {len(sampled_indices)} frames.")

    plt.close(fig)
    return state.metrics


if __name__ == "__main__":
    print("=" * 70)
    print("CREATING 15-MINUTE SIMULATION GIFS (15 FRAMES)")
    print("=" * 70)

    # Generate a scenario for a 15-minute simulation
    scenario = generate_dense_continuous_scenario(duration=900)
    
    algorithms = ['greedy', 'hungarian', 'simple_bundling', 'batched_pickups']

    print(f"\n🔄 Processing {len(algorithms)} algorithms...")

    all_metrics = {}
    for i, algo_name in enumerate(algorithms, 1):
        print(f"\n[{i}/{len(algorithms)}] {algo_name.upper()}")
        filename = ALGORITHM_FILENAMES[algo_name]
        output_path = f'/Users/pranjal/Code/meituan/simulation_test/gifs/showcase_{filename}_15min.gif'
        
        metrics = create_improved_showcase_animation(algo_name, scenario, output_path)
        all_metrics[algo_name] = metrics

    print("\n" + "=" * 70)
    print("✅ ALL 15-MINUTE GIFS CREATED!")
    print("=" * 70)
