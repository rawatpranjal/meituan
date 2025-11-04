#!/usr/bin/env python3
"""
Run All Algorithms with Individual and Consolidated Logging

Executes all 5 routing algorithms on the same scenario and creates:
1. Individual detailed logs for each algorithm
2. One consolidated log showing batch-by-batch comparison
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from datetime import datetime
from simulator_core import run_simulation, GRID_SIZE, SIMULATION_DURATION
from assignment_algorithms import get_algorithm
from create_clean_simple_viz import generate_dense_continuous_scenario
from export_detailed_logs import export_detailed_log
from collections import defaultdict

ALGORITHMS = ['greedy', 'hungarian', 'simple_bundling', 'batched_pickups', 'anticipated_bundling']

ALGORITHM_DISPLAY_NAMES = {
    'greedy': 'Greedy',
    'hungarian': 'Optimal Single-Order Matching',
    'simple_bundling': 'Single-Pickup Bundling',
    'batched_pickups': 'Batched Pickups',
    'anticipated_bundling': 'Anticipated Bundling'
}

ALGORITHM_FILENAMES = {
    'greedy': 'greedy_baseline',
    'hungarian': 'hungarian_route_aware',
    'simple_bundling': 'simple_bundling_route_aware',
    'batched_pickups': 'batched_pickups_network',
    'anticipated_bundling': 'anticipated_bundling_lookahead'
}

BATCH_INTERVAL = 300  # 5 minutes


def extract_batch_data(state, algo_name):
    """Extract batch-by-batch assignment data from simulation state."""
    batches = defaultdict(list)

    # Group assignments by batch (every 300 seconds)
    for order_id, order in state.orders.items():
        if order.assigned_courier_id is not None and order.assignment_time is not None:
            batch_num = int(order.assignment_time // BATCH_INTERVAL)

            batches[batch_num].append({
                'order_id': order_id,
                'courier_id': order.assigned_courier_id,
                'assignment_time': order.assignment_time,
                'restaurant_id': order.restaurant_id,
                'restaurant_location': order.restaurant_location,
                'diner_location': order.diner_location,
                'bundle_size': 1  # Will be updated for bundles
            })

    # Group assignments into bundles (same courier, same time)
    bundle_batches = {}
    for batch_num, assignments in batches.items():
        # Group by (courier_id, assignment_time)
        bundle_groups = defaultdict(list)
        for assignment in assignments:
            key = (assignment['courier_id'], assignment['assignment_time'])
            bundle_groups[key].append(assignment)

        # Create bundle records
        bundles = []
        for (courier_id, assignment_time), group in bundle_groups.items():
            bundle = {
                'courier_id': courier_id,
                'assignment_time': assignment_time,
                'order_ids': [a['order_id'] for a in group],
                'bundle_size': len(group),
                'restaurant_ids': [a['restaurant_id'] for a in group],
                'restaurant_locations': [a['restaurant_location'] for a in group],
                'diner_locations': [a['diner_location'] for a in group]
            }
            bundles.append(bundle)

        bundle_batches[batch_num] = bundles

    return bundle_batches


def format_bundle(bundle):
    """Format a bundle for display."""
    courier_id = bundle['courier_id']
    order_ids = bundle['order_ids']
    bundle_size = bundle['bundle_size']

    if bundle_size == 1:
        return f"    Courier {courier_id} ← Order {order_ids[0]}"
    else:
        order_str = ', '.join(map(str, order_ids))
        return f"    Courier {courier_id} ← [{order_str}] ({bundle_size} orders)"


def run_all_algorithms_with_logging():
    """Run all algorithms and create consolidated log."""

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_path = f'logs/consolidated_all_algorithms_{timestamp}.log'

    print("=" * 80)
    print("RUNNING ALL ALGORITHMS WITH CONSOLIDATED LOGGING")
    print("=" * 80)
    print(f"\nLog file: {log_path}")

    # Generate scenario
    scenario = generate_dense_continuous_scenario()

    print(f"\n📋 Scenario Configuration:")
    print(f"  • {len(scenario['restaurants'])} restaurants")
    print(f"  • {len(scenario['couriers'])} couriers")
    print(f"  • {len(scenario['order_schedule'])} orders")
    print(f"  • Duration: {SIMULATION_DURATION // 60} minutes")

    # Run all algorithms
    print(f"\n🔄 Running {len(ALGORITHMS)} algorithms...")

    algorithm_states = {}
    algorithm_batches = {}

    for i, algo_name in enumerate(ALGORITHMS, 1):
        print(f"  [{i}/{len(ALGORITHMS)}] {ALGORITHM_DISPLAY_NAMES[algo_name]}...", end=' ')

        assignment_func = get_algorithm(algo_name)
        state = run_simulation(scenario, assignment_func, algo_name)

        algorithm_states[algo_name] = state
        algorithm_batches[algo_name] = extract_batch_data(state, algo_name)

        # Export individual detailed log for this algorithm
        log_filename = ALGORITHM_FILENAMES[algo_name]
        individual_log_path = f'logs/{log_filename}_detailed_{timestamp}.log'
        export_detailed_log(state, algo_name, individual_log_path, None)

        delivered = state.metrics['orders_delivered']
        print(f"✓ ({delivered} orders delivered)")

    # Write consolidated log
    print(f"\n📝 Writing consolidated log to {log_path}...")

    with open(log_path, 'w') as f:
        # Header
        f.write("=" * 80 + "\n")
        f.write("CONSOLIDATED ALGORITHM COMPARISON LOG\n")
        f.write("=" * 80 + "\n")
        f.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        f.write(f"\nScenario:\n")
        f.write(f"  • Restaurants: {len(scenario['restaurants'])}\n")
        f.write(f"  • Couriers: {len(scenario['couriers'])}\n")
        f.write(f"  • Orders: {len(scenario['order_schedule'])}\n")
        f.write(f"  • Duration: {SIMULATION_DURATION // 60} minutes\n")
        f.write("\n")

        # Get all batch numbers across all algorithms
        all_batches = set()
        for batches in algorithm_batches.values():
            all_batches.update(batches.keys())

        # Write batch-by-batch comparison
        for batch_num in sorted(all_batches):
            time_sec = batch_num * BATCH_INTERVAL
            time_min = time_sec // 60

            f.write("=" * 80 + "\n")
            f.write(f"BATCH {batch_num} @ t={time_sec}s ({time_min}:00)\n")
            f.write("=" * 80 + "\n\n")

            for algo_name in ALGORITHMS:
                display_name = ALGORITHM_DISPLAY_NAMES[algo_name]
                f.write(f"[{display_name.upper()}]\n")

                if batch_num in algorithm_batches[algo_name]:
                    bundles = algorithm_batches[algo_name][batch_num]
                    f.write(f"  Assignments: {len(bundles)}\n")

                    for bundle in bundles:
                        f.write(format_bundle(bundle) + "\n")

                    # Summary stats
                    total_orders = sum(b['bundle_size'] for b in bundles)
                    avg_bundle_size = total_orders / len(bundles) if bundles else 0
                    f.write(f"  → {total_orders} orders via {len(bundles)} assignments (avg bundle: {avg_bundle_size:.2f})\n")
                else:
                    f.write("  No assignments\n")

                f.write("\n")

        # Final summary
        f.write("\n" + "=" * 80 + "\n")
        f.write("FINAL PERFORMANCE SUMMARY\n")
        f.write("=" * 80 + "\n\n")

        for algo_name in ALGORITHMS:
            display_name = ALGORITHM_DISPLAY_NAMES[algo_name]
            metrics = algorithm_states[algo_name].metrics

            f.write(f"{display_name}:\n")
            f.write(f"  Orders Delivered: {metrics['orders_delivered']}\n")
            f.write(f"  Total Distance: {metrics['total_distance_traveled']:.1f} km\n")
            f.write(f"  Bundles Created: {metrics['bundles_created']}\n")

            if metrics['bundles_created'] > 0:
                avg_bundle_size = metrics['total_bundle_size'] / metrics['bundles_created']
                f.write(f"  Avg Bundle Size: {avg_bundle_size:.2f}\n")

            # Calculate fulfillment rate
            total_orders = len(scenario['order_schedule'])
            fulfillment = (metrics['orders_delivered'] / total_orders * 100) if total_orders > 0 else 0
            f.write(f"  Fulfillment Rate: {fulfillment:.1f}%\n")

            f.write("\n")

    print(f"✅ Consolidated log created: {log_path}")

    # Print summary
    print("\n" + "=" * 80)
    print("PERFORMANCE SUMMARY")
    print("=" * 80)

    for algo_name in ALGORITHMS:
        display_name = ALGORITHM_DISPLAY_NAMES[algo_name]
        metrics = algorithm_states[algo_name].metrics
        delivered = metrics['orders_delivered']
        distance = metrics['total_distance_traveled']

        print(f"\n{display_name}:")
        print(f"  Delivered: {delivered} orders")
        print(f"  Distance: {distance:.1f} km")

    print("\n" + "=" * 80)
    print("✓ ALL LOGS CREATED")
    print("=" * 80)
    print(f"\n📁 Individual Logs:")
    for algo_name in ALGORITHMS:
        log_filename = ALGORITHM_FILENAMES[algo_name]
        individual_log_path = f'logs/{log_filename}_detailed_{timestamp}.log'
        print(f"  • {individual_log_path}")

    print(f"\n📁 Consolidated Log:")
    print(f"  • {log_path}")
    print("=" * 80)

    return log_path


if __name__ == "__main__":
    log_path = run_all_algorithms_with_logging()
