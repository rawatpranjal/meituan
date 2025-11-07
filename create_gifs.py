#!/usr/bin/env python3

import sys
import os
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation, PillowWriter
from matplotlib.patches import Rectangle, Circle, Polygon
import numpy as np
import pickle
import math
import argparse

from config_loader import load_config
from scenario_generators.scenario_factory import ScenarioFactory
from simulator_core import run_simulation
from assignment_algorithms import (
    assign_greedy,
    assign_hungarian,
    assign_simple_bundling,
    assign_network_bundling,
    assign_anticipated_bundling
)

FRAMES = 12
FPS = 0.5
SAMPLE_INTERVAL = 300

RESTAURANT_SIZE = 0.15
HOUSE_SIZE = 0.12
COURIER_SIZE = 0.18

COURIER_COLORS = [
    '#FF0000', '#0000FF', '#00FF00', '#FF00FF',
    '#FFAA00', '#00FFFF', '#800080', '#FF1493',
    '#32CD32', '#FFD700'
]

ALGORITHM_NAMES = {
    'greedy': 'Greedy Assignment',
    'hungarian': 'Hungarian Algorithm',
    'simple_bundling': 'Simple Bundling',
    'network_bundling': 'Network Bundling',
    'anticipated_bundling': 'Anticipated Bundling'
}

def get_algorithm(name):
    algorithms = {
        'greedy': assign_greedy,
        'hungarian': assign_hungarian,
        'simple_bundling': assign_simple_bundling,
        'network_bundling': assign_network_bundling,
        'anticipated_bundling': assign_anticipated_bundling
    }
    return algorithms[name]

def draw_visualization(ax, snapshot, restaurants, grid_size, total_duration, num_couriers):
    ax.clear()
    zoom_margin = 0.1
    ax.set_xlim(-zoom_margin, grid_size + zoom_margin)
    ax.set_ylim(-0.95, grid_size + 0.5)
    ax.set_aspect('equal')
    ax.axis('off')
    ax.set_facecolor('#FFFFFF')

    grid_points = np.arange(0, grid_size + 0.5, 0.5)
    for i, pos in enumerate(grid_points):
        if i % 2 == 0:
            ax.axhline(y=pos, color='#CCCCCC', linewidth=1.0, alpha=0.8)
            ax.axvline(x=pos, color='#CCCCCC', linewidth=1.0, alpha=0.8)
        else:
            ax.axhline(y=pos, color='#DDDDDD', linewidth=0.7, alpha=0.6)
            ax.axvline(x=pos, color='#DDDDDD', linewidth=0.7, alpha=0.6)

    for restaurant in restaurants:
        r_x, r_y = restaurant.location
        rect = Rectangle((r_x - RESTAURANT_SIZE/2, r_y - RESTAURANT_SIZE/2),
                        RESTAURANT_SIZE, RESTAURANT_SIZE,
                        facecolor='#DD0000', edgecolor='#880000',
                        linewidth=2, zorder=10)
        ax.add_patch(rect)

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

    current_time = snapshot['time']
    BATCH_INTERVAL = 300

    for c_id, courier_data in snapshot['couriers'].items():
        c_idx = int(c_id)
        if c_idx >= num_couriers:
            continue

        loc = courier_data['current_location']
        color = COURIER_COLORS[c_idx % len(COURIER_COLORS)]

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

        waypoints = [loc]
        pending_pickups = []
        pending_deliveries = []

        for order_id in recent_assigned_orders:
            if order_id in snapshot['orders']:
                order = snapshot['orders'][order_id]
                state = order.get('state')

                if state in ['ASSIGNED', 'READY']:
                    pending_pickups.append(order['restaurant_location'])
                    pending_deliveries.append(order['diner_location'])
                elif state == 'PICKED_UP':
                    pending_deliveries.append(order['diner_location'])
                elif state == 'DELIVERED':
                    pending_deliveries.append(order['diner_location'])

        waypoints.extend(pending_pickups)
        waypoints.extend(pending_deliveries)

        if len(waypoints) <= 1:
            continue

        linewidth = 2.0
        linestyle = '--'

        for i in range(len(waypoints) - 1):
            start = waypoints[i]
            end = waypoints[i + 1]
            mid_point = (end[0], start[1])

            ax.plot([start[0], mid_point[0]], [start[1], mid_point[1]],
                   color=color, linewidth=linewidth, alpha=0.7,
                   linestyle=linestyle, zorder=15)
            ax.plot([mid_point[0], end[0]], [mid_point[1], end[1]],
                   color=color, linewidth=linewidth, alpha=0.7,
                   linestyle=linestyle, zorder=15)

            if i > 0:
                waypoint_marker = Circle(waypoints[i], 0.04,
                                        facecolor=color, edgecolor='black',
                                        linewidth=1.0, alpha=0.6, zorder=17)
                ax.add_patch(waypoint_marker)

    for c_id, courier_data in snapshot['couriers'].items():
        c_idx = int(c_id)
        if c_idx >= num_couriers:
            continue

        loc = courier_data['current_location']

        if courier_data.get('next_destination'):
            dest = courier_data['next_destination']
            dx = dest[0] - loc[0]
            dy = dest[1] - loc[1]
            angle = math.atan2(dy, dx)
        else:
            angle = 0

        size = COURIER_SIZE
        triangle_points = [
            (loc[0] + size/2 * math.cos(angle),
             loc[1] + size/2 * math.sin(angle)),
            (loc[0] + size/3 * math.cos(angle + 2.4),
             loc[1] + size/3 * math.sin(angle + 2.4)),
            (loc[0] + size/3 * math.cos(angle - 2.4),
             loc[1] + size/3 * math.sin(angle - 2.4))
        ]
        triangle = Polygon(triangle_points, facecolor='black',
                          edgecolor='white', linewidth=1.5,
                          alpha=1.0, zorder=20)
        ax.add_patch(triangle)

    legend_text = (
        '■ Restaurant  ● Customer\n'
        '▲ Courier  ⋯→ Route'
    )
    ax.text(0.15, grid_size - 0.15, legend_text,
           fontsize=10, verticalalignment='top', horizontalalignment='left',
           bbox=dict(boxstyle='round,pad=0.4', facecolor='white', alpha=0.9,
                   edgecolor='#666666', linewidth=1.5),
           zorder=25)

    total_minutes = int(current_time // 60)
    hours = total_minutes // 60
    minutes = total_minutes % 60
    total_hours = int(total_duration // 3600)

    time_text = f'Hour {hours + 1} of {total_hours} | {hours:02d}:{minutes:02d}'
    ax.text(grid_size - 0.15, grid_size - 0.15, time_text,
           fontsize=12, ha='right', va='top', fontweight='bold',
           bbox=dict(boxstyle='round,pad=0.4', facecolor='white',
                   edgecolor='#666666', linewidth=1.5),
           zorder=25)

    metrics = snapshot['metrics']
    delivered_orders = [o for o in snapshot['orders'].values() if o['state'] == 'DELIVERED']
    out_of_scope = [o for o in snapshot['orders'].values()
                    if o['state'] == 'PENDING' and o['ready_time'] > total_duration]
    total_valid_orders = len(snapshot['orders']) - len(out_of_scope)

    fulfillment_rate = (len(delivered_orders) / total_valid_orders * 100) if total_valid_orders > 0 else 0

    if delivered_orders:
        click_to_door_times = [(o['delivery_time'] - o['placement_time']) for o in delivered_orders]
        avg_click_to_door = np.mean(click_to_door_times) / 60
        p90_click_to_door = np.percentile(click_to_door_times, 90) / 60 if len(click_to_door_times) > 1 else click_to_door_times[0] / 60
    else:
        avg_click_to_door = 0
        p90_click_to_door = 0

    elapsed_hours = current_time / 3600
    throughput = len(delivered_orders) / elapsed_hours if elapsed_hours > 0 else 0

    total_courier_hours = (len(snapshot['couriers']) * current_time) / 3600
    orders_per_courier_hr = len(delivered_orders) / total_courier_hours if total_courier_hours > 0 else 0

    freshness = (metrics.get('total_ready_to_door_time', 0) / len(delivered_orders)) / 60 if delivered_orders else 0

    bundles_created = metrics.get('bundles_created', 0)
    avg_bundle_size = metrics.get('total_bundle_size', 0) / bundles_created if bundles_created > 0 else 0

    total_courier_time = len(snapshot['couriers']) * current_time
    active_time = total_courier_time - metrics.get('total_courier_idle_time', 0)
    utilization = (active_time / total_courier_time * 100) if total_courier_time > 0 else 0

    total_distance = sum(c['total_distance_traveled'] for c in snapshot['couriers'].values())

    tier1_text = (
        f'Fulfillment: {fulfillment_rate:.1f}%  |  '
        f'Avg Click-to-Door: {avg_click_to_door:.1f}min  |  '
        f'P90 Click-to-Door: {p90_click_to_door:.1f}min'
    )
    ax.text(grid_size/2, -0.15, tier1_text,
           fontsize=11, ha='center', va='top', fontweight='bold',
           bbox=dict(boxstyle='round,pad=0.5', facecolor='#FFEBEE',
                   edgecolor='#D32F2F', linewidth=2.5))

    tier2_text = (
        f'System Throughput: {throughput:.1f} ord/hr  |  '
        f'Orders/Courier-Hour: {orders_per_courier_hr:.2f}  |  '
        f'Freshness: {freshness:.1f}min'
    )
    ax.text(grid_size/2, -0.42, tier2_text,
           fontsize=11, ha='center', va='top',
           bbox=dict(boxstyle='round,pad=0.5', facecolor='#E3F2FD',
                   edgecolor='#1976D2', linewidth=2))

    tier3_text = (
        f'Avg Bundle Size: {avg_bundle_size:.2f}  |  '
        f'Utilization: {utilization:.1f}%  |  '
        f'Distance: {total_distance:.1f}km'
    )
    ax.text(grid_size/2, -0.69, tier3_text,
           fontsize=11, ha='center', va='top',
           bbox=dict(boxstyle='round,pad=0.5', facecolor='#F5F5F5',
                   edgecolor='#757575', linewidth=2))

def create_gif(algo_name, scenario, state, output_path, grid_size, num_couriers):
    print(f"Creating GIF for {algo_name}...")

    timeline = state.timeline
    total_duration = int(scenario['duration'])
    sampled_indices = list(range(0, min(len(timeline), total_duration), SAMPLE_INTERVAL))

    if not sampled_indices:
        print(f"  Warning: No timeline data")
        return

    print(f"  Timeline: {len(timeline)} seconds → {len(sampled_indices)} frames")

    fig, ax = plt.subplots(figsize=(14, 16))
    plt.tight_layout(pad=0.5)
    restaurants = list(state.restaurants.values())

    def update(idx):
        if idx >= len(sampled_indices):
            return ax,

        frame_idx = sampled_indices[idx]
        snapshot = timeline[frame_idx]

        draw_visualization(ax, snapshot, restaurants, grid_size, total_duration, num_couriers)

        display_name = ALGORITHM_NAMES.get(algo_name, algo_name)
        ax.text(grid_size/2, grid_size + 0.35, display_name,
               fontsize=16, fontweight='bold', ha='center')

        return ax,

    anim = FuncAnimation(fig, update, frames=len(sampled_indices),
                        interval=1000/FPS, blit=False)

    print(f"  Saving to {output_path}...")
    writer = PillowWriter(fps=FPS)
    anim.save(output_path, writer=writer, dpi=120, savefig_kwargs={'bbox_inches': 'tight', 'pad_inches': 0.1})
    plt.close(fig)

    print(f"  Done! {len(sampled_indices)} frames @ {FPS} fps = {len(sampled_indices)/FPS:.0f} seconds")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Create GIFs for all algorithms')
    parser.add_argument('--force-rerun', action='store_true',
                       help='Force re-run simulations (ignore cached states)')
    args = parser.parse_args()

    print("=" * 70)
    print("CREATING GIFS FOR QUICK TEST SCENARIO")
    print("12 frames (one per 5-min batch) @ 0.5 fps = 24-second GIFs")
    print("=" * 70)

    config_path = 'scenarios/quick_test.yaml'
    config = load_config(config_path)
    factory = ScenarioFactory(config)

    grid_size = config['physics']['map_size_m'] / 1000.0
    num_couriers = config['couriers']['count']

    print(f"\nScenario: {config['scenario']['name']}")
    print(f"  Orders: {config['demand']['total_orders']}")
    print(f"  Couriers: {num_couriers}")
    print(f"  Duration: {config['scenario']['duration_hours']} hour(s)")

    algorithms = ['greedy', 'hungarian', 'simple_bundling', 'network_bundling', 'anticipated_bundling']

    os.makedirs('outputs/quick_test/states', exist_ok=True)
    os.makedirs('outputs/quick_test/gifs', exist_ok=True)

    for i, algo_name in enumerate(algorithms, 1):
        print(f"\n[{i}/{len(algorithms)}] {algo_name.upper()}")
        print("-" * 70)

        state_file = f'outputs/quick_test/states/{algo_name}.pkl'

        if os.path.exists(state_file) and not args.force_rerun:
            print(f"  Loading cached state from {state_file}")
            with open(state_file, 'rb') as f:
                state = pickle.load(f)
        else:
            print(f"  Running simulation...")
            scenario = factory.create_scenario()
            assignment_func = get_algorithm(algo_name)
            state = run_simulation(scenario, assignment_func, algo_name)

            print(f"  Caching state to {state_file}")
            with open(state_file, 'wb') as f:
                pickle.dump(state, f)

        scenario = factory.create_scenario()
        output_path = f'outputs/quick_test/gifs/{algo_name}.gif'
        create_gif(algo_name, scenario, state, output_path, grid_size, num_couriers)

    print("\n" + "=" * 70)
    print("ALL GIFS CREATED!")
    print("=" * 70)
    print(f"\nOutput: outputs/quick_test/gifs/")
    for algo in algorithms:
        print(f"  • {algo}.gif")
