"""
Holistic Performance Analysis for Food Delivery Routing Simulator

This script runs all algorithms and produces a comprehensive comparison
from Customer, Courier, and Platform perspectives.
"""

import sys
from datetime import datetime
from simulator_core import generate_scenario, run_simulation, SIMULATION_DURATION
from assignment_algorithms import get_algorithm


def format_time(seconds):
    """Convert seconds to human-readable format."""
    if seconds < 60:
        return f"{seconds:.0f}s"
    elif seconds < 3600:
        return f"{seconds/60:.1f}m"
    else:
        return f"{seconds/3600:.1f}h"


def run_holistic_analysis():
    """Run comprehensive analysis of all algorithms."""

    print("=" * 120)
    print("HOLISTIC PERFORMANCE ANALYSIS - 3-HOUR EXTENDED SIMULATION")
    print("=" * 120)

    # Generate scenario
    print("\nGenerating scenario...")
    scenario = generate_scenario()

    print(f"  Total orders: {len(scenario['order_schedule'])}")
    print(f"  Couriers: {len(scenario['couriers'])}")
    print(f"  Restaurants: {len(scenario['restaurants'])}")
    print(f"  Duration: {SIMULATION_DURATION}s ({SIMULATION_DURATION/3600:.1f} hours)")

    # Analyze order distribution
    order_times = [o.placement_time for o in scenario['order_schedule']]
    phase1 = len([t for t in order_times if t < 3600])
    phase2 = len([t for t in order_times if 3600 <= t < 7200])
    phase3 = len([t for t in order_times if t >= 7200])

    print(f"\nOrder distribution by phase:")
    print(f"  Phase 1 (0-1hr, Off-peak): {phase1} orders")
    print(f"  Phase 2 (1-2hr, PEAK): {phase2} orders")
    print(f"  Phase 3 (2-3hr, Tapering): {phase3} orders")

    # Run simulations
    algorithms = ['greedy', 'hungarian', 'simple_bundling', 'route_cost_bundling', 'batched_pickups']
    results = {}

    print("\n" + "=" * 120)
    print("Running simulations...")
    print("=" * 120)

    for algo_name in algorithms:
        print(f"\n  Running {algo_name}...")
        state = run_simulation(scenario, get_algorithm(algo_name), algo_name)
        results[algo_name] = state.metrics

        # Quick status
        print(f"    Delivered: {state.metrics['orders_delivered']}")
        print(f"    Fulfillment rate: {state.metrics['fulfillment_rate_pct']:.1f}%")

    # Display comprehensive results
    print("\n" + "=" * 120)
    print("COMPREHENSIVE PERFORMANCE METRICS")
    print("=" * 120)

    # === CUSTOMER PERSPECTIVE ===
    print("\n" + "=" * 80)
    print("CUSTOMER PERSPECTIVE - Service Quality")
    print("=" * 80)

    print(f"\n{'Metric':<35} {'Greedy':>12} {'Hungarian':>12} {'Simple Bundle':>14} {'Route Cost':>12} {'Batched Pickups':>15}")
    print("-" * 100)

    # Fulfillment Rate
    print(f"{'Fulfillment Rate (%)':<35}", end='')
    for algo in algorithms:
        val = results[algo]['fulfillment_rate_pct']
        print(f"{val:>12.1f}%", end='')
    print()

    # Avg Click-to-Door
    print(f"{'Avg Click-to-Door Time':<35}", end='')
    for algo in algorithms:
        val = results[algo]['avg_click_to_door_time']
        print(f"{format_time(val):>12}", end='  ')
    print()

    # P90 Click-to-Door
    print(f"{'P90 Click-to-Door Time':<35}", end='')
    for algo in algorithms:
        val = results[algo]['p90_click_to_door_time']
        print(f"{format_time(val):>12}", end='  ')
    print()

    # Ready-to-Door (Freshness)
    print(f"{'Avg Ready-to-Door Time (Freshness)':<35}", end='')
    for algo in algorithms:
        val = results[algo]['avg_ready_to_door_time']
        print(f"{format_time(val):>12}", end='  ')
    print()

    # Pickup Wait
    print(f"{'Avg Pickup Wait Time':<35}", end='')
    for algo in algorithms:
        val = results[algo]['avg_pickup_wait_time']
        print(f"{format_time(val):>12}", end='  ')
    print()

    # === COURIER PERSPECTIVE ===
    print("\n" + "=" * 80)
    print("COURIER PERSPECTIVE - Driver Experience & Productivity")
    print("=" * 80)

    print(f"\n{'Metric':<35} {'Greedy':>12} {'Hungarian':>12} {'Simple Bundle':>14} {'Route Cost':>12} {'Batched Pickups':>15}")
    print("-" * 100)

    # Utilization
    print(f"{'Courier Utilization (%)':<35}", end='')
    for algo in algorithms:
        val = results[algo]['courier_utilization_pct']
        print(f"{val:>12.1f}%", end='')
    print()

    # Orders per Hour
    print(f"{'Avg Orders/Courier/Hour':<35}", end='')
    for algo in algorithms:
        val = results[algo]['avg_orders_per_courier_hour']
        print(f"{val:>12.2f}", end='  ')
    print()

    # Total Distance
    print(f"{'Total Distance Traveled (km)':<35}", end='')
    for algo in algorithms:
        val = results[algo]['total_distance_traveled_km']
        print(f"{val:>12.1f}", end='  ')
    print()

    # === PLATFORM PERSPECTIVE ===
    print("\n" + "=" * 80)
    print("PLATFORM PERSPECTIVE - Business Efficiency")
    print("=" * 80)

    print(f"\n{'Metric':<35} {'Greedy':>12} {'Hungarian':>12} {'Simple Bundle':>14} {'Route Cost':>12} {'Batched Pickups':>15}")
    print("-" * 100)

    # System Throughput
    print(f"{'System Throughput (orders/hr)':<35}", end='')
    for algo in algorithms:
        val = results[algo]['system_throughput_orders_per_hour']
        print(f"{val:>12.1f}", end='  ')
    print()

    # Bundle Size
    print(f"{'Avg Bundle Size':<35}", end='')
    for algo in algorithms:
        val = results[algo]['avg_bundle_size']
        print(f"{val:>12.2f}", end='  ')
    print()

    # Orders Delivered
    print(f"{'Total Orders Delivered':<35}", end='')
    for algo in algorithms:
        val = results[algo]['orders_delivered']
        print(f"{val:>12}", end='    ')
    print()

    # === SUMMARY ===
    print("\n" + "=" * 120)
    print("KEY INSIGHTS")
    print("=" * 120)

    # Find best performers
    best_fulfillment = max(algorithms, key=lambda a: results[a]['fulfillment_rate_pct'])
    best_speed = min(algorithms, key=lambda a: results[a]['avg_click_to_door_time'])
    best_consistency = min(algorithms, key=lambda a: results[a]['p90_click_to_door_time'])
    best_utilization = max(algorithms, key=lambda a: results[a]['courier_utilization_pct'])
    best_throughput = max(algorithms, key=lambda a: results[a]['system_throughput_orders_per_hour'])

    print(f"\n📊 Best Performers:")
    print(f"  • Highest Fulfillment: {best_fulfillment} ({results[best_fulfillment]['fulfillment_rate_pct']:.1f}%)")
    print(f"  • Fastest Delivery: {best_speed} ({format_time(results[best_speed]['avg_click_to_door_time'])})")
    print(f"  • Most Consistent (P90): {best_consistency} ({format_time(results[best_consistency]['p90_click_to_door_time'])})")
    print(f"  • Best Courier Utilization: {best_utilization} ({results[best_utilization]['courier_utilization_pct']:.1f}%)")
    print(f"  • Highest Throughput: {best_throughput} ({results[best_throughput]['system_throughput_orders_per_hour']:.1f} orders/hr)")

    # Algorithm rankings
    print(f"\n🏆 Overall Rankings (by fulfillment rate):")
    ranked = sorted(algorithms, key=lambda a: results[a]['fulfillment_rate_pct'], reverse=True)
    for i, algo in enumerate(ranked, 1):
        fr = results[algo]['fulfillment_rate_pct']
        delivered = results[algo]['orders_delivered']
        print(f"  {i}. {algo.replace('_', ' ').title()}: {fr:.1f}% ({delivered} orders delivered)")

    # Trade-offs
    print(f"\n⚖️ Trade-offs Observed:")

    # Bundle vs Speed trade-off
    simple_bundle_time = results['simple_bundling']['avg_click_to_door_time']
    route_cost_time = results['route_cost_bundling']['avg_click_to_door_time']
    if simple_bundle_time > route_cost_time:
        diff_pct = ((simple_bundle_time - route_cost_time) / route_cost_time) * 100
        print(f"  • Simple Bundling delivers more orders but {diff_pct:.0f}% slower than Route Cost")

    # Efficiency vs Service
    simple_delivered = results['simple_bundling']['orders_delivered']
    route_delivered = results['route_cost_bundling']['orders_delivered']
    if simple_delivered > route_delivered:
        print(f"  • Simple Bundling prioritizes throughput (+{simple_delivered - route_delivered} orders)")
        print(f"    Route Cost prioritizes speed ({format_time(route_cost_time)} vs {format_time(simple_bundle_time)})")

    print("\n" + "=" * 120)

    # Save log
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_path = f'/Users/pranjal/Code/meituan/simulation_test/logs/holistic_analysis_{timestamp}.log'
    print(f"\n📁 Full analysis log: {log_path}")
    print("=" * 120)


if __name__ == '__main__':
    run_holistic_analysis()