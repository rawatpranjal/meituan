#!/usr/bin/env python3

import sys
import time
from datetime import datetime
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

def test_algorithm(name, assign_func, cfg):
    print(f"\n{'='*60}")
    print(f"Testing {name} algorithm")
    print('='*60)

    # Create scenario
    factory = ScenarioFactory(cfg)
    scenario = factory.create_scenario()
    scenario['config'] = cfg

    print(f"Running with {len(scenario['order_schedule'])} orders...")
    start_time = time.time()

    # Run simulation
    state = run_simulation(scenario, assign_func, name)

    elapsed = time.time() - start_time
    print(f"Completed in {elapsed:.1f} seconds")

    # Extract metrics
    metrics = state.metrics
    total_orders = len(scenario['order_schedule'])
    print(f"\nResults:")
    print(f"  Orders delivered: {metrics['orders_delivered']} / {total_orders}")
    print(f"  Orders expired: {metrics.get('orders_expired', 0)}")
    print(f"  Bundles created: {metrics.get('bundles_created', 0)}")
    print(f"  Total distance: {round(metrics.get('total_distance_traveled', 0), 1)} km")
    print(f"  Avg delivery time: {round(metrics.get('avg_delivery_time', 0) / 60, 1)} min")
    print(f"  Fulfillment rate: {round(metrics.get('fulfillment_rate_pct', 0), 1)}%")

    # Algorithm-specific checks
    if name == 'network_bundling':
        print(f"  Note: Network bundling can use up to 2 restaurants per bundle")
    elif name == 'anticipated_bundling':
        print(f"  Note: Anticipated bundling uses lookahead and can wait at pickup")

    return metrics

def main():
    print(f"\nTest started at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Working directory: /Users/pranjal/Code/meituan")

    # Always use the manageable test scenario
    scenario_file = 'scenarios/quick_test.yaml'
    print(f"Using scenario: {scenario_file}")

    # Load config
    cfg = load_config(scenario_file)
    print(f"Configuration: {cfg['demand']['total_orders']} orders, {cfg['couriers']['count']} couriers, {cfg['scenario']['duration_hours']} hours")

    # Test all algorithms
    algorithms = [
        ('greedy', assign_greedy),
        ('hungarian', assign_hungarian),
        ('simple_bundling', assign_simple_bundling),
        ('network_bundling', assign_network_bundling),
        ('anticipated_bundling', assign_anticipated_bundling)
    ]

    results = {}
    for name, func in algorithms:
        try:
            metrics = test_algorithm(name, func, cfg)
            results[name] = metrics
        except Exception as e:
            print(f"\nERROR in {name}: {e}")
            import traceback
            traceback.print_exc()
            results[name] = None

    # Summary comparison
    print(f"\n{'='*60}")
    print("SUMMARY COMPARISON")
    print('='*60)
    print(f"{'Algorithm':<20} {'Delivered':<12} {'Distance':<12} {'Fulfillment %':<12}")
    print('-'*60)

    for name, _ in algorithms:
        if results[name]:
            m = results[name]
            print(f"{name:<20} {m['orders_delivered']:<12} {round(m.get('total_distance_traveled',0),1):<12} {round(m.get('fulfillment_rate_pct',0),1):<12}")
        else:
            print(f"{name:<20} {'ERROR':<12} {'-':<12} {'-':<12}")

    print(f"\nTest completed at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    return 0

if __name__ == '__main__':
    sys.exit(main())