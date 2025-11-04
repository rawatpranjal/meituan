"""
Main Runner for Food Delivery Routing Simulator

This script runs all 4 assignment algorithms on the same scenario and
launches an interactive dashboard for visualization and comparison.

Usage:
    python main.py [--export-gif] [--no-dashboard]
"""

import sys
import argparse
import json
from datetime import datetime
from simulator_core import generate_scenario, generate_asymmetric_scenario, run_simulation, SIMULATION_DURATION
from assignment_algorithms import get_algorithm
from dashboard import run_dashboard, print_comparison_report


def save_results_to_log(results, log_path):
    """Save detailed simulation results to log file."""
    with open(log_path, 'w') as f:
        f.write("=" * 80 + "\n")
        f.write("FOOD DELIVERY ROUTING SIMULATION - DETAILED LOG\n")
        f.write("=" * 80 + "\n\n")

        f.write(f"Simulation Duration: {SIMULATION_DURATION}s ({SIMULATION_DURATION/60:.1f} min)\n")
        f.write(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")

        for algo_name, state in results.items():
            f.write("\n" + "=" * 80 + "\n")
            f.write(f"ALGORITHM: {algo_name.upper()}\n")
            f.write("=" * 80 + "\n\n")

            # Final metrics
            f.write("FINAL METRICS:\n")
            f.write("-" * 80 + "\n")
            for metric, value in state.metrics.items():
                f.write(f"  {metric}: {value}\n")

            # Event log
            f.write(f"\nEVENT LOG ({len(state.events_log)} events):\n")
            f.write("-" * 80 + "\n")
            for event in state.events_log[:50]:  # First 50 events
                f.write(f"  [{event['time']:6.1f}s] {event['type']}: {event['description']}\n")

            if len(state.events_log) > 50:
                f.write(f"  ... ({len(state.events_log) - 50} more events)\n")

        f.write("\n" + "=" * 80 + "\n")
        f.write("END OF LOG\n")
        f.write("=" * 80 + "\n")


def main():
    """Main execution function."""
    parser = argparse.ArgumentParser(description='Food Delivery Routing Simulator')
    parser.add_argument('--export-gif', action='store_true',
                       help='Export animation to GIF file')
    parser.add_argument('--no-dashboard', action='store_true',
                       help='Skip interactive dashboard (only print report)')
    parser.add_argument('--algorithms', nargs='+',
                       choices=['greedy', 'hungarian', 'simple_bundling', 'route_cost_bundling', 'batched_pickups'],
                       default=['greedy', 'hungarian', 'simple_bundling', 'route_cost_bundling', 'batched_pickups'],
                       help='Algorithms to run (default: all)')
    parser.add_argument('--duration', type=int, default=None,
                       help='Simulation duration in seconds (default: 10800 for full 3-hour, use 900-1500 for GIF export)')

    args = parser.parse_args()

    print("=" * 80)
    print("FOOD DELIVERY ROUTING SIMULATOR")
    print("=" * 80)

    # Step 1: Generate scenario - Use asymmetric scenario for visual differentiation
    print("\nGenerating asymmetric scenario for visual differentiation...")
    scenario = generate_asymmetric_scenario(duration=args.duration if args.duration else 900)

    print(f"  - Restaurants: {len(scenario['restaurants'])}")
    print(f"  - Couriers: {len(scenario['couriers'])}")
    print(f"  - Orders: {len(scenario['order_schedule'])}")
    if args.duration:
        print(f"  - Duration: {args.duration}s ({args.duration/60:.1f} min)")

    # Print order arrival distribution
    order_times = [o.placement_time for o in scenario['order_schedule']]
    print(f"  - Order arrivals: {min(order_times):.1f}s to {max(order_times):.1f}s")

    # Step 2: Run simulations for each algorithm
    results = {}

    for algo_name in args.algorithms:
        print(f"\nRunning simulation with {algo_name}...")
        algorithm_func = get_algorithm(algo_name)

        state = run_simulation(scenario, algorithm_func, algo_name)
        results[algo_name] = state

        print(f"  - Orders delivered: {state.metrics['orders_delivered']}/{len(scenario['order_schedule'])}")
        print(f"  - Avg delivery time: {state.metrics['avg_delivery_time']:.1f}s")
        print(f"  - Total distance: {state.metrics['total_distance_traveled']:.1f}km")
        print(f"  - Bundles created: {state.metrics['bundles_created']}")

    # Step 3: Save results to log
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    log_path = f'/Users/pranjal/Code/meituan/simulation_test/logs/06_batch_simulator_{timestamp}.log'

    print(f"\nSaving detailed results to {log_path}...")
    save_results_to_log(results, log_path)

    # Step 4: Print comparison report
    print_comparison_report(results)

    # Step 5: Launch dashboard
    if not args.no_dashboard:
        print("\nLaunching interactive dashboard...")
        print("  - Use spacebar to play/pause")
        print("  - Use left/right arrows to step through frames")
        print("  - Use +/- to change playback speed")
        print("  - Close window to exit")

        run_dashboard(results, export_gif=args.export_gif)
    else:
        print("\nSkipping dashboard (--no-dashboard flag set)")

    print("\n" + "=" * 80)
    print("SIMULATION COMPLETE")
    print(f"Log file: {log_path}")
    print("=" * 80)


if __name__ == '__main__':
    main()
