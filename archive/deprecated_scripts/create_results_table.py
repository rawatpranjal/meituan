#!/usr/bin/env python3
"""
Create Results Comparison Table

Generates a formatted comparison table from the most recent simulation run.
"""

import json
import re
from datetime import datetime

ALGORITHMS = ['greedy', 'hungarian', 'simple_bundling', 'network_bundling', 'anticipated_bundling']

ALGORITHM_DISPLAY_NAMES = {
    'greedy': 'Greedy',
    'hungarian': 'Optimal Single-Order Matching',
    'simple_bundling': 'Single-Pickup Bundling',
    'network_bundling': 'Network Bundling',
    'anticipated_bundling': 'Anticipated Network Bundling'
}

ALGORITHM_FILENAMES = {
    'greedy': 'greedy_baseline',
    'hungarian': 'hungarian_route_aware',
    'simple_bundling': 'simple_bundling_route_aware',
    'network_bundling': 'network_bundling',
    'anticipated_bundling': 'anticipated_bundling_network'
}


def parse_detailed_log(log_path):
    """Extract metrics from detailed log file."""
    with open(log_path, 'r') as f:
        content = f.read()

    metrics = {}

    # Extract fulfillment rate
    match = re.search(r'Fulfillment Rate:\s*([\d.]+)%', content)
    if match:
        metrics['fulfillment_rate'] = float(match.group(1))

    # Extract orders delivered
    match = re.search(r'Delivered:\s*(\d+)', content)
    if match:
        metrics['orders_delivered'] = int(match.group(1))

    # Extract total distance
    match = re.search(r'Total Distance:\s*([\d.]+)\s*km', content)
    if match:
        metrics['total_distance'] = float(match.group(1))

    # Extract average bundle size
    match = re.search(r'Avg Bundle Size:\s*([\d.]+)', content)
    if match:
        metrics['avg_bundle_size'] = float(match.group(1))

    # Extract average delivery time (Click-to-Door)
    match = re.search(r'Avg Click-to-Door:\s*([\d.]+)\s*minutes', content)
    if match:
        metrics['avg_delivery_time'] = float(match.group(1))

    # Extract courier utilization
    match = re.search(r'Courier Utilization:\s*([\d.]+)%', content)
    if match:
        metrics['courier_utilization'] = float(match.group(1))

    # Extract total orders from simulation parameters
    match = re.search(r'Total Orders:\s*(\d+)', content)
    if match:
        metrics['total_orders'] = int(match.group(1))

    # Calculate bundles created (count lines with "✓ Courier")
    bundles = len(re.findall(r'✓ Courier \d+ ← Orders', content))
    if bundles > 0:
        metrics['bundles_created'] = bundles

    return metrics


def create_comparison_table(timestamp):
    """Create comparison table from latest run."""

    print("=" * 100)
    print("ALGORITHM PERFORMANCE COMPARISON TABLE")
    print("=" * 100)
    print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Run timestamp: {timestamp}")
    print()

    # Collect metrics for all algorithms
    all_metrics = {}
    total_orders = None

    for algo_name in ALGORITHMS:
        filename = ALGORITHM_FILENAMES[algo_name]
        log_path = f'logs/{filename}_detailed_{timestamp}.log'

        try:
            metrics = parse_detailed_log(log_path)
            all_metrics[algo_name] = metrics

            if total_orders is None and 'total_orders' in metrics:
                total_orders = metrics['total_orders']
        except FileNotFoundError:
            print(f"Warning: Log file not found: {log_path}")
            all_metrics[algo_name] = {}

    # Calculate fulfillment rates if not already present
    for algo_name, metrics in all_metrics.items():
        if 'fulfillment_rate' not in metrics and 'orders_delivered' in metrics and total_orders:
            metrics['fulfillment_rate'] = (metrics['orders_delivered'] / total_orders) * 100

    # Print table header
    print(f"{'Algorithm':<35} | {'Orders':<10} | {'Fulfill%':<10} | {'Distance':<12} | {'Bundles':<10} | {'Avg Bundle':<12} | {'Delivery Time':<15}")
    print("-" * 130)

    # Print rows
    for algo_name in ALGORITHMS:
        display_name = ALGORITHM_DISPLAY_NAMES[algo_name]
        metrics = all_metrics.get(algo_name, {})

        orders = metrics.get('orders_delivered', '?')
        fulfillment = f"{metrics.get('fulfillment_rate', 0):.1f}%" if 'fulfillment_rate' in metrics else '?'
        distance = f"{metrics.get('total_distance', 0):.1f} km" if 'total_distance' in metrics else '?'
        bundles = metrics.get('bundles_created', '?')
        avg_bundle = f"{metrics.get('avg_bundle_size', 0):.2f}" if 'avg_bundle_size' in metrics else '?'
        delivery_time = f"{metrics.get('avg_delivery_time', 0):.1f} min" if 'avg_delivery_time' in metrics else '?'

        print(f"{display_name:<35} | {str(orders):<10} | {fulfillment:<10} | {distance:<12} | {str(bundles):<10} | {avg_bundle:<12} | {delivery_time:<15}")

    print("=" * 130)

    # Find best performers
    if all_metrics:
        best_fulfillment = max((m.get('orders_delivered', 0), algo) for algo, m in all_metrics.items())

        distance_metrics = [(m.get('total_distance', float('inf')), algo) for algo, m in all_metrics.items() if 'total_distance' in m]
        if distance_metrics:
            best_distance = min(distance_metrics)
        else:
            best_distance = None

        print(f"\n✓ Best Fulfillment: {ALGORITHM_DISPLAY_NAMES[best_fulfillment[1]]} ({best_fulfillment[0]} orders)")

        if best_distance:
            print(f"✓ Most Efficient Distance: {ALGORITHM_DISPLAY_NAMES[best_distance[1]]} ({best_distance[0]:.1f} km)")

    # Calculate improvement over Greedy
    if 'greedy' in all_metrics and all_metrics['greedy'].get('orders_delivered'):
        greedy_orders = all_metrics['greedy']['orders_delivered']

        print(f"\nImprovement over Greedy ({greedy_orders} orders):")
        print("-" * 50)

        for algo_name in ALGORITHMS:
            if algo_name == 'greedy':
                continue

            metrics = all_metrics.get(algo_name, {})
            orders = metrics.get('orders_delivered')

            if orders:
                improvement = ((orders - greedy_orders) / greedy_orders) * 100
                display_name = ALGORITHM_DISPLAY_NAMES[algo_name]
                print(f"  {display_name:<35}: +{improvement:>5.1f}% ({orders} orders)")

    print("\n" + "=" * 100)


def create_comparison_table_from_txt():
    """Create comparison table from .txt log files (no timestamp)."""

    print("=" * 100)
    print("ALGORITHM PERFORMANCE COMPARISON TABLE")
    print("=" * 100)
    print(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("Run: Latest (detailed_log.txt files)")
    print()

    all_metrics = {}

    for algo_name in ALGORITHMS:
        filename = ALGORITHM_FILENAMES[algo_name]
        log_path = f'logs/{filename}_detailed_log.txt'

        try:
            metrics = parse_detailed_log(log_path)
            all_metrics[algo_name] = metrics

        except FileNotFoundError:
            print(f"Warning: {log_path} not found")
            continue
        except Exception as e:
            print(f"Error parsing {log_path}: {e}")
            continue

    if not all_metrics:
        print("Error: No valid metrics found")
        return

    # Print table
    print(f"{'Algorithm':<35} | {'Orders':<10} | {'Fulfill%':<10} | {'Distance':<12} | {'Bundles':<10} | {'Avg Bundle':<12} | {'Delivery Time':<15}")
    print("-" * 130)

    for algo_name in ALGORITHMS:
        if algo_name not in all_metrics:
            continue

        m = all_metrics[algo_name]
        print(f"{ALGORITHM_DISPLAY_NAMES[algo_name]:<35} | {m['orders_delivered']:<10} | {m['fulfillment_rate']:<10} | {m['total_distance']:<12} | {m['bundles_created']:<10} | {m['avg_bundle_size']:<12} | {m['avg_delivery_time']:<15}")

    print("=" * 130)
    print()

    # Find best performers
    best_fulfillment = max(all_metrics.values(), key=lambda x: x['orders_delivered'])
    best_distance = min(all_metrics.values(), key=lambda x: float(str(x['total_distance']).replace(' km', '')))

    best_fulfillment_algo = [name for name, m in all_metrics.items() if m['orders_delivered'] == best_fulfillment['orders_delivered']][0]
    best_distance_algo = [name for name, m in all_metrics.items() if m['total_distance'] == best_distance['total_distance']][0]

    print(f"✓ Best Fulfillment: {ALGORITHM_DISPLAY_NAMES[best_fulfillment_algo]} ({best_fulfillment['orders_delivered']} orders)")
    print(f"✓ Most Efficient Distance: {ALGORITHM_DISPLAY_NAMES[best_distance_algo]} ({best_distance['total_distance']})")
    print()

    # Show improvement over baseline (Greedy)
    if 'greedy' in all_metrics:
        baseline_orders = all_metrics['greedy']['orders_delivered']
        print(f"Improvement over Greedy ({baseline_orders} orders):")
        print("-" * 50)

        for algo_name in ALGORITHMS:
            if algo_name == 'greedy' or algo_name not in all_metrics:
                continue

            orders = all_metrics[algo_name]['orders_delivered']
            improvement = ((orders - baseline_orders) / baseline_orders) * 100
            print(f"  {ALGORITHM_DISPLAY_NAMES[algo_name]:<35}: + {improvement:.1f}% ({orders} orders)")

    print()
    print("=" * 100)


if __name__ == "__main__":
    import sys
    import glob

    # Check for timestamped .log files first, then fall back to .txt files
    log_files_with_timestamp = glob.glob('logs/greedy_baseline_detailed_*.log')
    txt_files = glob.glob('logs/greedy_baseline_detailed_log.txt')

    if txt_files:
        # Use .txt files (no timestamp)
        print("Using latest detailed_log.txt files\n")
        create_comparison_table_from_txt()
    elif log_files_with_timestamp:
        # Use timestamped .log files
        latest_file = max(log_files_with_timestamp)
        parts = latest_file.replace('.log', '').split('_')
        timestamp = f"{parts[-2]}_{parts[-1]}"
        print(f"Using timestamp: {timestamp}\n")
        create_comparison_table(timestamp)
    else:
        print("Error: No log files found")
        sys.exit(1)
