#!/usr/bin/env python3
"""Test greedy algorithm on all scenarios"""
import sys
import os
from datetime import datetime

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from config_loader import load_config
from scenario_generators import ScenarioFactory
from simulator_core import run_simulation
from assignment_algorithms import get_algorithm

# Import detailed log exporter
import sys
sys.path.insert(0, os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))), 'archive', 'utilities'))
from export_detailed_logs import export_detailed_log

# Setup logging
log_dir = os.path.join(os.path.dirname(__file__), "logs")
os.makedirs(log_dir, exist_ok=True)
log_file_path = os.path.join(log_dir, "test_greedy_all_scenarios.log")

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

    print("="*80)
    print("GREEDY ALGORITHM - ALL SCENARIOS TEST")
    print("="*80)
    print(f"Testing {len(scenarios)} scenarios")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    all_results = {}

    for i, config_path in enumerate(scenarios, 1):
        print("="*80)
        print(f"SCENARIO {i}/{len(scenarios)}: {config_path}")
        print("="*80)

        config = load_config(config_path)
        factory = ScenarioFactory(config)
        scenario = factory.create_scenario()

        assignment_func = get_algorithm('greedy')
        state = run_simulation(scenario, assignment_func, 'greedy')

        scenario_name = config['scenario']['name']

        # Export detailed batch-by-batch log
        detailed_log_path = os.path.join(log_dir, f"greedy_{scenario_name}_detailed.log")
        export_detailed_log(state, 'greedy', detailed_log_path)
        print(f"Detailed log: {detailed_log_path}")
        all_results[scenario_name] = {
            'config': config_path,
            'duration_hours': config['scenario']['duration_hours'],
            'total_orders': config['demand']['total_orders'],
            'restaurants': config['restaurants']['count'],
            'couriers': config['couriers']['count'],
            'orders_delivered': state.metrics['orders_delivered'],
            'orders_in_transit': state.metrics['orders_in_transit'],
            'orders_unassigned': state.metrics['orders_unassigned'],
            'orders_out_of_scope': state.metrics['orders_out_of_scope'],
            'fulfillment_rate_pct': state.metrics['fulfillment_rate_pct'],
            'total_distance_km': state.metrics['total_distance_traveled'],
            'avg_click_to_door_min': state.metrics.get('avg_click_to_door_time', 0)/60,
            'p90_click_to_door_min': state.metrics.get('p90_click_to_door_time', 0)/60,
            'courier_utilization_pct': state.metrics['courier_utilization_pct'],
            'bundles_created': state.metrics['bundles_created'],
            'avg_bundle_size': state.metrics['avg_bundle_size']
        }

        print()
        print("-"*80)
        print("RESULTS")
        print("-"*80)
        print(f"Scenario: {scenario_name}")
        print(f"Orders delivered: {state.metrics['orders_delivered']}")
        print(f"Orders in transit: {state.metrics['orders_in_transit']}")
        print(f"Orders unassigned: {state.metrics['orders_unassigned']}")
        print(f"Orders out of scope: {state.metrics['orders_out_of_scope']}")
        print(f"Fulfillment rate: {state.metrics['fulfillment_rate_pct']:.1f}%")
        print(f"Total distance: {state.metrics['total_distance_traveled']:.2f} km")
        print(f"Avg click-to-door: {state.metrics.get('avg_click_to_door_time', 0)/60:.1f} min")
        print(f"P90 click-to-door: {state.metrics.get('p90_click_to_door_time', 0)/60:.1f} min")
        print(f"Courier utilization: {state.metrics['courier_utilization_pct']:.1f}%")
        print(f"Bundles created: {state.metrics['bundles_created']}")
        print(f"Avg bundle size: {state.metrics['avg_bundle_size']:.2f}")
        print()

    print("="*80)
    print("SUMMARY - GREEDY ALGORITHM ACROSS ALL SCENARIOS")
    print("="*80)
    print(f"{'Scenario':<25} {'Orders':<10} {'Delivered':<12} {'Fulfill%':<10} {'Distance':<12}")
    print("-"*80)
    for scenario_name, result in all_results.items():
        print(f"{scenario_name:<25} {result['total_orders']:<10} {result['orders_delivered']:<12} {result['fulfillment_rate_pct']:<10.1f} {result['total_distance_km']:<12.2f}")
    print("="*80)
    print(f"\nCompleted: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Log file: {log_file_path}")

finally:
    sys.stdout = original_stdout
    log_file.close()
    print(f"\nLog saved to: {log_file_path}")
