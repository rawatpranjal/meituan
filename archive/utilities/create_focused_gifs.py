#!/usr/bin/env python3
"""
Focused GIF Generation - Shows only the 7 most distinct sequential batches
Highlights key moments where algorithms make dramatically different decisions
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation, PillowWriter
from matplotlib.patches import Rectangle, Circle, Polygon
import numpy as np
import json
from simulator_core import run_simulation, GRID_SIZE, SIMULATION_DURATION, generate_scenario, NUM_COURIERS
from assignment_algorithms import get_algorithm
from export_detailed_logs import export_detailed_log
import math
import os

# Load top distinct batches from analysis
with open('analysis/top_distinct_batches.json', 'r') as f:
    distinct_batch_data = json.load(f)
    DISTINCT_BATCHES = distinct_batch_data['top_batches']
    BATCH_TIMES = distinct_batch_data['batch_times']

# Configuration
FRAMES = len(DISTINCT_BATCHES)  # One frame per distinct batch
FPS = 0.33  # 3 seconds per frame (1/3 fps = 3 sec/frame)
BATCH_INTERVAL = 300  # Batches occur every 300 seconds

# Visual sizes
RESTAURANT_SIZE = 0.12
HOUSE_SIZE = 0.10
COURIER_SIZE = 0.15

# Courier colors (distinct colors for each courier)
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

# Algorithm display names
ALGORITHM_DISPLAY_NAMES = {
    'greedy': 'Greedy',
    'hungarian': 'Optimal Single-Order Matching',
    'simple_bundling': 'Single-Pickup Bundling',
    'network_bundling': 'Network Bundling',
    'anticipated_bundling': 'Anticipated Network Bundling'
}

# Algorithm filename mapping
ALGORITHM_FILENAMES = {
    'greedy': 'greedy_baseline',
    'hungarian': 'hungarian_route_aware',
    'simple_bundling': 'simple_bundling_route_aware',
    'network_bundling': 'network_bundling',
    'anticipated_bundling': 'anticipated_bundling_network'
}


def draw_visualization(ax, snapshot, restaurants, total_duration):
    """Draw complete visualization matching screenshot layout."""

    # Clear axis
    ax.clear()
    # Zoom in to central area - crop edges to remove corner whitespace
    zoom_margin = 0.1  # Tight crop on central map area
    ax.set_xlim(-zoom_margin, GRID_SIZE + zoom_margin)
    ax.set_ylim(-zoom_margin, GRID_SIZE + 0.5)  # Extra space for title only (no dashboard)
    ax.set_aspect('equal')
    ax.axis('off')
    ax.set_facecolor('#FFFFFF')

    # Draw grid lines - thin gray
    grid_points = np.arange(0, GRID_SIZE + 0.5, 0.5)
    for i, pos in enumerate(grid_points):
        if i % 2 == 0:  # 1km lines
            ax.axhline(y=pos, color='#CCCCCC', linewidth=1.0, alpha=0.8)
            ax.axvline(x=pos, color='#CCCCCC', linewidth=1.0, alpha=0.8)
        else:  # 0.5km lines
            ax.axhline(y=pos, color='#DDDDDD', linewidth=0.7, alpha=0.6)
            ax.axvline(x=pos, color='#DDDDDD', linewidth=0.7, alpha=0.6)

    # Draw restaurants (red squares)
    for restaurant in restaurants:
        r_x, r_y = restaurant.location
        rect = Rectangle((r_x - RESTAURANT_SIZE/2, r_y - RESTAURANT_SIZE/2),
                        RESTAURANT_SIZE, RESTAURANT_SIZE,
                        facecolor='#DD0000', edgecolor='#880000',
                        linewidth=2, zorder=10)
        ax.add_patch(rect)

    # Draw customer locations (blue circles) - only for active orders
    drawn_houses = set()
    for order_id, order_data in snapshot['orders'].items():
        if order_data['state'] in ['READY', 'ASSIGNED', 'PICKED_UP']:
            diner_loc = tuple(order_data['diner_location'])
            if diner_loc not in drawn_houses:
                drawn_houses.add(diner_loc)
                h_x, h_y = diner_loc
                circle = Circle((h_x, h_y), HOUSE_SIZE/2,
                              facecolor='#0000DD', edgecolor='#000088',
                              linewidth=2, zorder=8)
                ax.add_patch(circle)

    # Draw relay handoff points (purple diamonds)
    for order_id, order_data in snapshot['orders'].items():
        if order_data.get('is_relay') and order_data.get('relay_handoff_location'):
            handoff_loc = order_data['relay_handoff_location']
            if order_data.get('state') in ['ASSIGNED', 'PICKED_UP']:
                diamond_size = 0.15
                diamond_points = [
                    (handoff_loc[0], handoff_loc[1] + diamond_size/2),
                    (handoff_loc[0] + diamond_size/2, handoff_loc[1]),
                    (handoff_loc[0], handoff_loc[1] - diamond_size/2),
                    (handoff_loc[0] - diamond_size/2, handoff_loc[1])
                ]
                diamond = Polygon(diamond_points, facecolor='#9370DB',
                                edgecolor='#663399', linewidth=2, zorder=15)
                ax.add_patch(diamond)

    # Draw routes with arrow persistence (5-minute window)
    current_time = snapshot['time']
    BATCH_INTERVAL = 300  # 5 minutes

    for c_id, courier_data in snapshot['couriers'].items():
        c_idx = int(c_id)
        if c_idx >= NUM_COURIERS:
            continue

        loc = courier_data['current_location']
        color = COURIER_COLORS[c_idx % len(COURIER_COLORS)]

        # FIX: Find ALL orders assigned to this courier in last 5 minutes
        # Search through ALL orders in snapshot, not just assigned_order_ids
        # This captures orders that were delivered quickly but assignment should still show
        recent_assigned_orders = []
        for order_id, order_data in snapshot['orders'].items():
            if order_data.get('assigned_courier_id') == c_id:
                assignment_time = order_data.get('assignment_time')
                if assignment_time is not None:
                    time_since_assignment = current_time - assignment_time
                    if time_since_assignment <= BATCH_INTERVAL:
                        recent_assigned_orders.append(order_id)

        if not recent_assigned_orders:
            continue

        # Build complete route: courier → pickups → deliveries
        waypoints = [loc]

        # Pending pickups and deliveries
        pending_pickups = []
        pending_deliveries = []

        for order_id in recent_assigned_orders:
            if order_id in snapshot['orders']:
                order = snapshot['orders'][order_id]
                state = order.get('state')

                # ASSIGNED orders: show COMPLETE route (pickup + delivery)
                if state in ['ASSIGNED', 'READY']:
                    pending_pickups.append(order['restaurant_location'])
                    pending_deliveries.append(order['diner_location'])  # FIX: Add customer to show complete route

                # PICKED_UP orders: show delivery leg only
                elif state == 'PICKED_UP':
                    pending_deliveries.append(order['diner_location'])

                # DELIVERED orders: persist route for 5-min window
                elif state == 'DELIVERED':
                    pending_deliveries.append(order['diner_location'])

        waypoints.extend(pending_pickups)
        waypoints.extend(pending_deliveries)

        if len(waypoints) <= 1:
            continue

        # Draw thin dotted route lines
        linewidth = 1.5
        linestyle = '--'  # Dashed line

        for i in range(len(waypoints) - 1):
            start = waypoints[i]
            end = waypoints[i + 1]

            # L-shaped Manhattan routing
            mid_point = (end[0], start[1])

            # Horizontal segment
            ax.plot([start[0], mid_point[0]], [start[1], mid_point[1]],
                   color=color, linewidth=linewidth, alpha=0.7,
                   linestyle=linestyle, zorder=15)

            # Vertical segment
            ax.plot([mid_point[0], end[0]], [mid_point[1], end[1]],
                   color=color, linewidth=linewidth, alpha=0.7,
                   linestyle=linestyle, zorder=15)

            # Small waypoint markers
            if i > 0:
                waypoint_marker = Circle(waypoints[i], 0.04,
                                        facecolor=color, edgecolor='black',
                                        linewidth=1.0, alpha=0.6, zorder=17)
                ax.add_patch(waypoint_marker)

    # Draw couriers (black triangles)
    for c_id, courier_data in snapshot['couriers'].items():
        c_idx = int(c_id)
        if c_idx >= NUM_COURIERS:
            continue

        loc = courier_data['current_location']

        # Determine direction
        if courier_data.get('next_destination'):
            dest = courier_data['next_destination']
            dx = dest[0] - loc[0]
            dy = dest[1] - loc[1]
            angle = math.atan2(dy, dx)
        else:
            angle = 0

        # Triangle size
        size = COURIER_SIZE

        # Create directional triangle
        triangle_points = [
            (loc[0] + size/2 * math.cos(angle),
             loc[1] + size/2 * math.sin(angle)),
            (loc[0] + size/3 * math.cos(angle + 2.4),
             loc[1] + size/3 * math.sin(angle + 2.4)),
            (loc[0] + size/3 * math.cos(angle - 2.4),
             loc[1] + size/3 * math.sin(angle - 2.4))
        ]

        # Black triangle with white edge
        triangle = Polygon(triangle_points, facecolor='black',
                          edgecolor='white', linewidth=1.5,
                          alpha=1.0, zorder=20)
        ax.add_patch(triangle)

    # Draw legend (top-left corner)
    legend_text = (
        '■ Restaurant  ● Customer\n'
        '▲ Courier  ⋯→ Route'
    )
    ax.text(0.15, GRID_SIZE - 0.15, legend_text,
           fontsize=12, verticalalignment='top', horizontalalignment='left',
           bbox=dict(boxstyle='round,pad=0.4', facecolor='white', alpha=0.9,
                   edgecolor='#666666', linewidth=1.5),
           zorder=25)

    # Draw time (top-right corner)
    total_minutes = int(current_time // 60)
    hours = total_minutes // 60
    minutes = total_minutes % 60
    total_hours = int(total_duration // 3600)

    time_text = f'Hour {hours + 1} of {total_hours} | {hours:02d}:{minutes:02d}'
    ax.text(GRID_SIZE - 0.15, GRID_SIZE - 0.15, time_text,
           fontsize=14, ha='right', va='top', fontweight='bold',
           bbox=dict(boxstyle='round,pad=0.4', facecolor='white',
                   edgecolor='#666666', linewidth=1.5),
           zorder=25)

    # Dashboard removed - focused on visualization only


def create_consolidated_gif(algo_name, scenario, output_path, logs_dir='logs'):
    """Create focused GIF showing only the most distinct batches."""

    print(f"\nCreating focused GIF for {algo_name}...")

    # Run simulation
    assignment_func = get_algorithm(algo_name)
    state = run_simulation(scenario, assignment_func, algo_name)

    # Export detailed log for AI analysis
    log_filename = ALGORITHM_FILENAMES[algo_name]
    detailed_log_path = f'{logs_dir}/{log_filename}_detailed_log.txt'
    overlap_matrix_path = f'{logs_dir}/assignment_overlap_matrix.txt'
    print(f"  📝 Exporting detailed log...")
    export_detailed_log(state, algo_name, detailed_log_path, overlap_matrix_path)

    # Sample only at the distinct batch times
    timeline = state.timeline
    sampled_indices = [t for t in BATCH_TIMES if t < len(timeline)]

    if not sampled_indices:
        print(f"  ⚠ Warning: No timeline data at batch times")
        return state.metrics

    print(f"  Timeline: {len(timeline)} seconds → {len(sampled_indices)} frames (batches {DISTINCT_BATCHES})")
    print(f"  Times: {[f'{t//60}min' for t in sampled_indices]}")

    # Create figure (compact size without dashboard)
    fig, ax = plt.subplots(figsize=(10, 11))
    plt.tight_layout(pad=0.5)  # Minimize whitespace
    restaurants = list(state.restaurants.values())

    def update(idx):
        if idx >= len(sampled_indices):
            return ax,

        frame_idx = sampled_indices[idx]
        snapshot = timeline[frame_idx]

        # Draw visualization
        draw_visualization(ax, snapshot, restaurants, SIMULATION_DURATION)

        # Add title at top with batch number
        display_name = ALGORITHM_DISPLAY_NAMES.get(algo_name, algo_name)
        batch_num = DISTINCT_BATCHES[idx]
        batch_time_min = frame_idx // 60
        title = f"{display_name}\n[Batch {batch_num} @ {batch_time_min} min]"
        ax.text(GRID_SIZE/2, GRID_SIZE + 0.35, title,
               fontsize=18, fontweight='bold', ha='center')

        return ax,

    # Create animation
    anim = FuncAnimation(fig, update, frames=len(sampled_indices),
                        interval=1000/FPS, blit=False)

    # Save GIF
    print(f"  💾 Saving to {output_path}...")
    writer = PillowWriter(fps=FPS)
    anim.save(output_path, writer=writer, dpi=100, savefig_kwargs={'bbox_inches': 'tight', 'pad_inches': 0.1})
    plt.close(fig)

    print(f"  ✅ Saved! {len(sampled_indices)} frames @ {FPS} fps = {len(sampled_indices)/FPS:.0f} seconds")

    return state.metrics


if __name__ == "__main__":
    # Parse command-line arguments
    import argparse
    parser = argparse.ArgumentParser(description='Create focused GIFs for all algorithms')
    parser.add_argument('--output-dir', type=str, default='.',
                       help='Output directory for gifs/ and logs/ folders (default: current directory)')
    parser.add_argument('--scenario', type=str, default=None,
                       help='Path to scenario config YAML (optional, defaults to legacy scenario)')
    args = parser.parse_args()

    print("=" * 70)
    print("CREATING FOCUSED GIFS")
    print(f"{len(DISTINCT_BATCHES)} frames (batches {DISTINCT_BATCHES}) @ {FPS} fps = {len(DISTINCT_BATCHES)/FPS:.0f}-second GIFs")
    print(f"Showing sequential batches at minutes: {[b*5 for b in DISTINCT_BATCHES]}")
    print("=" * 70)

    # Load or generate scenario
    if args.scenario:
        from config_loader import load_config
        from scenario_generators import ScenarioFactory
        config = load_config(args.scenario)
        factory = ScenarioFactory(config)
        scenario = factory.create_scenario()
    else:
        # Generate scenario (Small Town Lunch Rush - legacy)
        scenario = generate_scenario()

    print(f"\n📋 Scenario Configuration:")
    print(f"  • {len(scenario['restaurants'])} restaurants")
    print(f"  • {len(scenario['couriers'])} couriers")
    print(f"  • {len(scenario['order_schedule'])} orders")
    duration_hours = scenario['duration'] / 3600
    print(f"  • Duration: {duration_hours:.1f} hour(s)")

    # Clean 5-Algorithm Hierarchy
    algorithms = ['greedy', 'hungarian', 'simple_bundling', 'network_bundling',
                  'anticipated_bundling']

    print(f"\n🔄 Processing {len(algorithms)} algorithms...")

    # Create output directories
    gifs_dir = f'{args.output_dir}/gifs'
    logs_dir = f'{args.output_dir}/logs'
    os.makedirs(gifs_dir, exist_ok=True)
    os.makedirs(logs_dir, exist_ok=True)

    # Generate GIFs
    all_metrics = {}
    for i, algo_name in enumerate(algorithms, 1):
        print(f"\n[{i}/{len(algorithms)}] {algo_name.upper()}")
        print("-" * 70)

        filename = ALGORITHM_FILENAMES[algo_name]
        output_path = f'{gifs_dir}/{filename}_focused.gif'

        metrics = create_consolidated_gif(algo_name, scenario, output_path, logs_dir)
        all_metrics[algo_name] = metrics

    print("\n" + "=" * 70)
    print("✅ ALL FOCUSED GIFS CREATED!")
    print("=" * 70)
    print(f"\n📁 Output: {gifs_dir}/")
    for algo in algorithms:
        filename = ALGORITHM_FILENAMES[algo]
        print(f"  • {filename}_focused.gif")

    print("\n📊 Performance Summary:")
    print("-" * 70)
    for algo_name, metrics in all_metrics.items():
        display_name = ALGORITHM_DISPLAY_NAMES.get(algo_name, algo_name)
        delivered = metrics['orders_delivered']
        distance = metrics['total_distance_traveled']
        bundles = metrics['bundles_created']

        print(f"\n{display_name}:")
        print(f"  Delivered: {delivered} orders")
        print(f"  Distance: {distance:.1f} km")
        print(f"  Bundles: {bundles}")
