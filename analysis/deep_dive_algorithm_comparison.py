#!/usr/bin/env python3
"""
Deep Dive Algorithm Comparison with Event-Level Analysis

This script runs all 5 algorithms on the same scenario and extracts detailed
event logs to analyze granular differences in assignment decisions, bundle
formations, and performance across demand periods.
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from simulator_core import run_simulation, generate_scenario
from assignment_algorithms import get_algorithm
from create_clean_simple_viz import generate_dense_continuous_scenario
import json
import csv
from datetime import datetime
from collections import defaultdict

# Algorithms to compare
ALGORITHMS = ['greedy', 'hungarian', 'simple_bundling', 'batched_pickups', 'anticipated_bundling']

# Demand period definitions (in seconds)
DEMAND_PERIODS = {
    'low_early': (0, 3600),          # Hour 0-1: Low demand
    'peak': (3600, 7200),            # Hour 1-2: Peak demand (160 orders/hr)
    'average': (7200, 21600)         # Hour 2-6: Average demand
}


def save_events_to_csv(events, algorithm_name, output_dir='analysis/event_data'):
    """Save event log to CSV file."""
    os.makedirs(output_dir, exist_ok=True)

    filepath = os.path.join(output_dir, f'events_{algorithm_name}.csv')

    with open(filepath, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(['time', 'type', 'description', 'details'])

        for event in events:
            time = event['time']
            event_type = event['type']
            description = event['description']

            # Extract additional details
            details = {k: v for k, v in event.items() if k not in ['time', 'type', 'description']}
            details_str = json.dumps(details)

            writer.writerow([time, event_type, description, details_str])

    print(f"  Saved {len(events)} events to {filepath}")
    return filepath


def analyze_assignments_by_period(events, algorithm_name):
    """Analyze assignment patterns by demand period."""

    period_stats = {period: {
        'assignments': 0,
        'orders_assigned': [],
        'bundle_sizes': [],
        'couriers_used': set()
    } for period in DEMAND_PERIODS}

    for event in events:
        time = event['time']

        # Determine period
        period_name = None
        for pname, (start, end) in DEMAND_PERIODS.items():
            if start <= time < end:
                period_name = pname
                break

        if period_name and event['type'] == 'ASSIGNMENT_MADE':
            period_stats[period_name]['assignments'] += 1

            order_ids = event.get('order_ids', [])
            period_stats[period_name]['orders_assigned'].extend(order_ids)
            period_stats[period_name]['bundle_sizes'].append(event.get('bundle_size', 1))
            period_stats[period_name]['couriers_used'].add(event.get('courier_id'))

    # Compute summary statistics
    summary = {}
    for period_name, stats in period_stats.items():
        summary[period_name] = {
            'num_assignments': stats['assignments'],
            'orders_assigned': len(stats['orders_assigned']),
            'avg_bundle_size': sum(stats['bundle_sizes']) / len(stats['bundle_sizes']) if stats['bundle_sizes'] else 0,
            'num_couriers_used': len(stats['couriers_used'])
        }

    return summary


def extract_relay_handoffs(events):
    """Extract relay handoff information."""
    handoffs = []

    for event in events:
        if event['type'] in ['RELAY_SCHEDULED', 'HANDOFF_COMPLETE']:
            handoffs.append({
                'time': event['time'],
                'type': event['type'],
                'order_id': event.get('order_id'),
                'from_courier': event.get('from_courier'),
                'to_courier': event.get('to_courier', event.get('relay_courier')),
                'handoff_location': event.get('handoff_location')
            })

    return handoffs


def extract_bundle_formations(events):
    """Extract bundle formation patterns."""
    bundles = []

    for event in events:
        if event['type'] == 'ASSIGNMENT_MADE':
            bundles.append({
                'time': event['time'],
                'courier_id': event.get('courier_id'),
                'order_ids': event.get('order_ids', []),
                'bundle_size': event.get('bundle_size', 1),
                'has_relay': event.get('has_relay', False)
            })

    return bundles


def run_comparative_analysis():
    """Run all algorithms and collect detailed event data."""

    print("="*70)
    print("DEEP DIVE ALGORITHM COMPARISON")
    print("="*70)

    # Generate scenario (same for all algorithms)
    print("\nGenerating scenario...")
    scenario = generate_dense_continuous_scenario()
    print(f"  Duration: {scenario['duration']/3600:.1f} hours")
    print(f"  Restaurants: {len(scenario['restaurants'])}")
    print(f"  Couriers: {len(scenario['couriers'])}")
    print(f"  Orders: {len(scenario['order_schedule'])}")

    # Store all results
    all_results = {}

    # Run each algorithm
    for algo_name in ALGORITHMS:
        print(f"\n{'='*70}")
        print(f"Running {algo_name.upper()}...")
        print(f"{'='*70}")

        # Run simulation
        assignment_func = get_algorithm(algo_name)
        state = run_simulation(scenario, assignment_func, algo_name)

        # Extract events
        print(f"  Extracting event data...")
        events = state.events_log

        # Save events to CSV
        save_events_to_csv(events, algo_name)

        # Analyze by period
        print(f"  Analyzing by demand period...")
        period_analysis = analyze_assignments_by_period(events, algo_name)

        # Extract specialized data
        print(f"  Extracting bundle formations...")
        bundles = extract_bundle_formations(events)

        # Save bundle data
        bundle_file = f'analysis/event_data/bundles_{algo_name}.json'
        with open(bundle_file, 'w') as f:
            json.dump(bundles, f, indent=2)
        print(f"  Saved {len(bundles)} bundles to {bundle_file}")

        # Extract relay handoffs if applicable
        if algo_name == 'relay_bundling':
            print(f"  Extracting relay handoffs...")
            handoffs = extract_relay_handoffs(events)

            handoff_file = 'analysis/event_data/relay_handoffs.json'
            with open(handoff_file, 'w') as f:
                json.dump(handoffs, f, indent=2)
            print(f"  Saved {len(handoffs)} handoff events to {handoff_file}")

        # Store results
        all_results[algo_name] = {
            'metrics': state.metrics,
            'period_analysis': period_analysis,
            'num_events': len(events),
            'num_bundles': len(bundles)
        }

    # Save comparative summary
    summary_file = 'analysis/comparative_summary.json'
    with open(summary_file, 'w') as f:
        json.dump(all_results, f, indent=2)
    print(f"\n{'='*70}")
    print(f"Saved comparative summary to {summary_file}")

    # Print summary table
    print(f"\n{'='*70}")
    print("COMPARATIVE SUMMARY BY DEMAND PERIOD")
    print(f"{'='*70}\n")

    for period_name in ['low_early', 'peak', 'average']:
        print(f"\n{period_name.upper().replace('_', ' ')} PERIOD:")
        print(f"{'Algorithm':<20} {'Assignments':<15} {'Orders':<10} {'Avg Bundle':<12} {'Couriers'}")
        print("-" * 70)

        for algo_name in ALGORITHMS:
            stats = all_results[algo_name]['period_analysis'][period_name]
            print(f"{algo_name:<20} {stats['num_assignments']:<15} "
                  f"{stats['orders_assigned']:<10} {stats['avg_bundle_size']:<12.2f} "
                  f"{stats['num_couriers_used']}")

    print(f"\n{'='*70}")
    print("OVERALL PERFORMANCE")
    print(f"{'='*70}\n")
    print(f"{'Algorithm':<20} {'Delivered':<12} {'Distance':<12} {'Bundles':<10} {'Avg Size'}")
    print("-" * 70)

    for algo_name in ALGORITHMS:
        metrics = all_results[algo_name]['metrics']
        print(f"{algo_name:<20} {metrics['orders_delivered']:<12} "
              f"{metrics['total_distance_traveled']:<12.1f} "
              f"{metrics['bundles_created']:<10} "
              f"{metrics.get('avg_bundle_size', 0):<10.2f}")

    print(f"\n{'='*70}")
    print("✓ ANALYSIS COMPLETE")
    print(f"{'='*70}")
    print("\nEvent data saved to: analysis/event_data/")
    print("Comparative summary: analysis/comparative_summary.json")

    return all_results


if __name__ == "__main__":
    results = run_comparative_analysis()
