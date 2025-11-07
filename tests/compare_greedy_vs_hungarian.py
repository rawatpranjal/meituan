#!/usr/bin/env python3
"""Compare Greedy vs Hungarian algorithms across all scenarios"""
import sys
import os
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config_loader import load_config
from scenario_generators import ScenarioFactory
from simulator_core import run_simulation
from assignment_algorithms import get_algorithm

# Import detailed log exporter
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'archive', 'utilities'))
from export_detailed_logs import export_detailed_log

# Setup logging
timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
log_dir = os.path.join(os.path.dirname(__file__), "logs")
os.makedirs(log_dir, exist_ok=True)
log_file_path = os.path.join(log_dir, f"comparison_greedy_vs_hungarian_{timestamp}.log")

class Tee:
    def __init__(self, *files):
        self.files = files
    def write(self, obj):
        for f in self.files:
            f.write(obj)
            f.flush()
    def flush(self):
        for f in self.files:
            f.flush()

log_file = open(log_file_path, 'w')
original_stdout = sys.stdout
sys.stdout = Tee(sys.stdout, log_file)

try:
    scenarios = [
        "scenarios/downtown_crush.yaml",
        "scenarios/river_divide.yaml",
        "scenarios/popup_problem.yaml",
        "scenarios/impossible_deadline.yaml"
    ]

    algorithms = {
        'greedy': 'Greedy',
        'hungarian': 'Hungarian'
    }

    print("="*80)
    print("GREEDY vs HUNGARIAN - COMPREHENSIVE COMPARISON")
    print("="*80)
    print(f"Testing {len(scenarios)} scenarios with {len(algorithms)} algorithms")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    all_results = {}

    for config_path in scenarios:
        config = load_config(config_path)
        scenario_name = config['scenario']['name']

        print("="*80)
        print(f"SCENARIO: {scenario_name.upper()}")
        print("="*80)
        print()

        all_results[scenario_name] = {}

        for algo_key, algo_display in algorithms.items():
            print(f"Running {algo_display}...")

            factory = ScenarioFactory(config)
            scenario = factory.create_scenario()

            assignment_func = get_algorithm(algo_key)
            state = run_simulation(scenario, assignment_func, algo_key)

            # Export detailed log
            detailed_log_path = os.path.join(log_dir, f"{algo_key}_{scenario_name}_detailed_{timestamp}.log")
            export_detailed_log(state, algo_key, detailed_log_path)

            all_results[scenario_name][algo_key] = {
                'orders_delivered': state.metrics['orders_delivered'],
                'orders_in_transit': state.metrics['orders_in_transit'],
                'orders_unassigned': state.metrics['orders_unassigned'],
                'orders_out_of_scope': state.metrics['orders_out_of_scope'],
                'fulfillment_rate_pct': state.metrics['fulfillment_rate_pct'],
                'total_distance_km': state.metrics['total_distance_traveled'],
                'avg_click_to_door_min': state.metrics.get('avg_click_to_door_time', 0)/60,
                'p90_click_to_door_min': state.metrics.get('p90_click_to_door_time', 0)/60,
                'avg_ready_to_door_min': state.metrics.get('avg_ready_to_door_time', 0)/60,
                'courier_utilization_pct': state.metrics['courier_utilization_pct'],
                'bundles_created': state.metrics['bundles_created'],
                'avg_bundle_size': state.metrics['avg_bundle_size'],
                'system_throughput': state.metrics.get('system_throughput_orders_per_hour', 0),
                'orders_per_courier_hour': state.metrics.get('avg_orders_per_courier_hour', 0),
                'log_path': detailed_log_path
            }

            print(f"  ✓ {algo_display}: {state.metrics['orders_delivered']} delivered ({state.metrics['fulfillment_rate_pct']:.1f}%)")
            print(f"    Log: {detailed_log_path}")

        print()

    # Print comprehensive comparison tables
    print("="*80)
    print("COMPREHENSIVE COMPARISON REPORT")
    print("="*80)
    print()

    for scenario_name in all_results.keys():
        print("="*80)
        print(f"SCENARIO: {scenario_name.upper()}")
        print("="*80)

        greedy = all_results[scenario_name]['greedy']
        hungarian = all_results[scenario_name]['hungarian']

        print()
        print("FULFILLMENT METRICS:")
        print("-"*80)
        print(f"{'Metric':<35} {'Greedy':<15} {'Hungarian':<15} {'Improvement'}")
        print("-"*80)
        print(f"{'Orders Delivered':<35} {greedy['orders_delivered']:<15} {hungarian['orders_delivered']:<15} {hungarian['orders_delivered']-greedy['orders_delivered']:+d}")
        print(f"{'Fulfillment Rate':<35} {greedy['fulfillment_rate_pct']:<15.1f}% {hungarian['fulfillment_rate_pct']:<15.1f}% {hungarian['fulfillment_rate_pct']-greedy['fulfillment_rate_pct']:+.1f}%")

        if greedy['orders_delivered'] > 0:
            improvement_pct = ((hungarian['orders_delivered'] - greedy['orders_delivered']) / greedy['orders_delivered'] * 100)
            print(f"{'Delivery Improvement':<35} {'-':<15} {'-':<15} {improvement_pct:+.1f}%")

        print()
        print("TIMING METRICS:")
        print("-"*80)
        print(f"{'Metric':<35} {'Greedy':<15} {'Hungarian':<15} {'Difference'}")
        print("-"*80)
        print(f"{'Avg Click-to-Door (min)':<35} {greedy['avg_click_to_door_min']:<15.1f} {hungarian['avg_click_to_door_min']:<15.1f} {hungarian['avg_click_to_door_min']-greedy['avg_click_to_door_min']:+.1f}")
        print(f"{'P90 Click-to-Door (min)':<35} {greedy['p90_click_to_door_min']:<15.1f} {hungarian['p90_click_to_door_min']:<15.1f} {hungarian['p90_click_to_door_min']-greedy['p90_click_to_door_min']:+.1f}")
        print(f"{'Avg Ready-to-Door (min)':<35} {greedy['avg_ready_to_door_min']:<15.1f} {hungarian['avg_ready_to_door_min']:<15.1f} {hungarian['avg_ready_to_door_min']-greedy['avg_ready_to_door_min']:+.1f}")

        print()
        print("EFFICIENCY METRICS:")
        print("-"*80)
        print(f"{'Metric':<35} {'Greedy':<15} {'Hungarian':<15} {'Difference'}")
        print("-"*80)
        print(f"{'Total Distance (km)':<35} {greedy['total_distance_km']:<15.2f} {hungarian['total_distance_km']:<15.2f} {hungarian['total_distance_km']-greedy['total_distance_km']:+.2f}")
        print(f"{'Courier Utilization (%)':<35} {greedy['courier_utilization_pct']:<15.1f} {hungarian['courier_utilization_pct']:<15.1f} {hungarian['courier_utilization_pct']-greedy['courier_utilization_pct']:+.1f}")
        print(f"{'System Throughput (ord/hr)':<35} {greedy['system_throughput']:<15.1f} {hungarian['system_throughput']:<15.1f} {hungarian['system_throughput']-greedy['system_throughput']:+.1f}")
        print(f"{'Orders/Courier-Hour':<35} {greedy['orders_per_courier_hour']:<15.2f} {hungarian['orders_per_courier_hour']:<15.2f} {hungarian['orders_per_courier_hour']-greedy['orders_per_courier_hour']:+.2f}")

        print()
        print("BUNDLING METRICS:")
        print("-"*80)
        print(f"{'Metric':<35} {'Greedy':<15} {'Hungarian':<15} {'Difference'}")
        print("-"*80)
        print(f"{'Bundles Created':<35} {greedy['bundles_created']:<15} {hungarian['bundles_created']:<15} {hungarian['bundles_created']-greedy['bundles_created']:+d}")
        print(f"{'Avg Bundle Size':<35} {greedy['avg_bundle_size']:<15.2f} {hungarian['avg_bundle_size']:<15.2f} {hungarian['avg_bundle_size']-greedy['avg_bundle_size']:+.2f}")

        print()
        print("ORDER BREAKDOWN:")
        print("-"*80)
        print(f"{'Status':<35} {'Greedy':<15} {'Hungarian':<15}")
        print("-"*80)
        print(f"{'In Transit':<35} {greedy['orders_in_transit']:<15} {hungarian['orders_in_transit']:<15}")
        print(f"{'Unassigned':<35} {greedy['orders_unassigned']:<15} {hungarian['orders_unassigned']:<15}")
        print(f"{'Out of Scope':<35} {greedy['orders_out_of_scope']:<15} {hungarian['orders_out_of_scope']:<15}")
        print()

    # Summary table
    print("="*80)
    print("SUMMARY ACROSS ALL SCENARIOS")
    print("="*80)
    print()
    print(f"{'Scenario':<25} {'Algorithm':<12} {'Delivered':<12} {'Fulfill%':<10} {'Distance':<12}")
    print("-"*80)
    for scenario_name in all_results.keys():
        for algo_key, algo_display in algorithms.items():
            result = all_results[scenario_name][algo_key]
            print(f"{scenario_name:<25} {algo_display:<12} {result['orders_delivered']:<12} {result['fulfillment_rate_pct']:<10.1f} {result['total_distance_km']:<12.2f}")
    print("="*80)

    print()
    print(f"Completed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Log file: {log_file_path}")

finally:
    sys.stdout = original_stdout
    log_file.close()
    print(f"\nLog saved to: {log_file_path}")
