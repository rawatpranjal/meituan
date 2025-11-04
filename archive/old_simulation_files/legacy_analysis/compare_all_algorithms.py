#!/usr/bin/env python3
"""
Comprehensive Algorithm Comparison
Runs all 6 algorithms and reports final metrics across all 9 business metrics
"""

import sys
from datetime import datetime
from simulator_core import run_simulation
from assignment_algorithms import get_algorithm
from create_clean_simple_viz import generate_dense_continuous_scenario

# Algorithm names
ALGORITHMS = [
    'greedy',
    'hungarian',
    'simple_bundling',
    'batched_pickups',
    'anticipated_bundling'  # Workhorse
]

DISPLAY_NAMES = {
    'greedy': 'Greedy',
    'hungarian': 'Optimal Single-Order Matching',
    'simple_bundling': 'Single-Pickup Bundling',
    'batched_pickups': 'Batched Pickups',
    'relay_bundling': 'Relay Bundling',
    'anticipated_bundling': 'Anticipated Bundling'
}

def run_comparison():
    """Run all algorithms and collect metrics."""

    print("=" * 100)
    print("COMPREHENSIVE ALGORITHM COMPARISON")
    print("All 9 Business Metrics Across 6 Routing Strategies")
    print("=" * 100)

    # Generate shared scenario with fixed seed for reproducibility
    print("\nGenerating scenario...")
    scenario = generate_dense_continuous_scenario(seed=42)
    print(f"  • {len(scenario['restaurants'])} restaurants")
    print(f"  • {len(scenario['couriers'])} couriers")
    print(f"  • {len(scenario['order_schedule'])} orders")
    print(f"  • Duration: 1 hour")

    # Run all algorithms
    results = {}
    for i, algo_name in enumerate(ALGORITHMS, 1):
        print(f"\n[{i}/{len(ALGORITHMS)}] Running {algo_name}...")
        assignment_func = get_algorithm(algo_name)
        state = run_simulation(scenario, assignment_func, algo_name)
        results[algo_name] = state.metrics
        print(f"  ✓ {state.metrics['orders_delivered']} orders delivered")

    # Print comprehensive comparison table
    print("\n" + "=" * 100)
    print("TIER 1: MISSION-CRITICAL METRICS")
    print("=" * 100)
    print(f"{'Algorithm':<30} {'Fulfillment %':<15} {'Avg Click-Door':<18} {'P90 Click-Door':<18}")
    print("-" * 100)

    for algo_name in ALGORITHMS:
        m = results[algo_name]
        display_name = DISPLAY_NAMES[algo_name]
        fulfillment = m.get('fulfillment_rate_pct', 0)
        avg_ctd = m.get('avg_click_to_door_time', 0) / 60  # Convert to minutes
        p90_ctd = m.get('p90_click_to_door_time', 0) / 60
        print(f"{display_name:<30} {fulfillment:>13.1f}%  {avg_ctd:>15.1f}min  {p90_ctd:>15.1f}min")

    print("\n" + "=" * 100)
    print("TIER 2: OPERATIONAL EFFICIENCY METRICS")
    print("=" * 100)
    print(f"{'Algorithm':<30} {'Throughput':<18} {'Ord/Courier-Hr':<18} {'Freshness':<15}")
    print("-" * 100)

    for algo_name in ALGORITHMS:
        m = results[algo_name]
        display_name = DISPLAY_NAMES[algo_name]
        throughput = m.get('system_throughput_orders_per_hour', 0)
        productivity = m.get('avg_orders_per_courier_hour', 0)
        freshness = m.get('avg_ready_to_door_time', 0) / 60
        print(f"{display_name:<30} {throughput:>12.1f} ord/hr  {productivity:>15.2f}  {freshness:>12.1f}min")

    print("\n" + "=" * 100)
    print("TIER 3: DIAGNOSTIC METRICS")
    print("=" * 100)
    print(f"{'Algorithm':<30} {'Avg Bundle Size':<18} {'Utilization':<15} {'Distance':<15}")
    print("-" * 100)

    for algo_name in ALGORITHMS:
        m = results[algo_name]
        display_name = DISPLAY_NAMES[algo_name]
        bundle_size = m.get('avg_bundle_size', 0)
        utilization = m.get('courier_utilization_pct', 0)
        distance = m.get('total_distance_traveled_km', 0)
        print(f"{display_name:<30} {bundle_size:>15.2f}  {utilization:>12.1f}%  {distance:>12.1f}km")

    print("\n" + "=" * 100)
    print("ANALYSIS SUMMARY")
    print("=" * 100)

    # Find best performers for each metric
    best_fulfillment = max(ALGORITHMS, key=lambda a: results[a].get('fulfillment_rate_pct', 0))
    best_click_to_door = min(ALGORITHMS, key=lambda a: results[a].get('avg_click_to_door_time', float('inf')))
    best_throughput = max(ALGORITHMS, key=lambda a: results[a].get('system_throughput_orders_per_hour', 0))
    best_productivity = max(ALGORITHMS, key=lambda a: results[a].get('avg_orders_per_courier_hour', 0))
    best_freshness = min(ALGORITHMS, key=lambda a: results[a].get('avg_ready_to_door_time', float('inf')))

    print(f"\n🏆 Best Fulfillment Rate: {DISPLAY_NAMES[best_fulfillment]} ({results[best_fulfillment]['fulfillment_rate_pct']:.1f}%)")
    print(f"🏆 Best Click-to-Door Time: {DISPLAY_NAMES[best_click_to_door]} ({results[best_click_to_door]['avg_click_to_door_time']/60:.1f}min)")
    print(f"🏆 Best System Throughput: {DISPLAY_NAMES[best_throughput]} ({results[best_throughput]['system_throughput_orders_per_hour']:.1f} ord/hr)")
    print(f"🏆 Best Courier Productivity: {DISPLAY_NAMES[best_productivity]} ({results[best_productivity]['avg_orders_per_courier_hour']:.2f} ord/courier-hr)")
    print(f"🏆 Best Freshness: {DISPLAY_NAMES[best_freshness]} ({results[best_freshness]['avg_ready_to_door_time']/60:.1f}min)")

    print("\n" + "=" * 100)

    return results

if __name__ == "__main__":
    results = run_comparison()
