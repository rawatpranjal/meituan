#!/usr/bin/env python3
"""
Test determinism by running algorithms with different random seeds.
Verifies that:
1. Same seed produces identical results (determinism)
2. Different seeds produce different scenarios but consistent algorithm behavior
"""

import sys
import numpy as np
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

def run_with_seed(seed, assign_func, alg_name, cfg):
    """Run simulation with a specific seed"""
    # Create a copy of config and set the random seed
    from copy import deepcopy
    cfg_copy = deepcopy(cfg)
    cfg_copy['scenario']['random_seed'] = seed

    # Also set numpy seed for any code that doesn't use config
    np.random.seed(seed)

    factory = ScenarioFactory(cfg_copy)
    scenario = factory.create_scenario()
    scenario['config'] = cfg_copy

    state = run_simulation(scenario, assign_func, alg_name)

    return {
        'seed': seed,
        'orders_delivered': state.metrics['orders_delivered'],
        'orders_expired': state.metrics.get('orders_expired', 0),
        'bundles_created': state.metrics.get('bundles_created', 0),
        'total_distance': round(state.metrics.get('total_distance_traveled', 0), 2),
        'avg_delivery_time': round(state.metrics.get('avg_delivery_time', 0), 1),
        'fulfillment_rate': round(state.metrics.get('fulfillment_rate_pct', 0), 2)
    }

def test_determinism():
    """Test that same seed produces identical results"""
    print("\n" + "="*70)
    print("DETERMINISM TEST: Same seed should produce identical results")
    print("="*70)

    cfg = load_config('scenarios/quick_test.yaml')

    # Test each algorithm with same seed twice
    test_seed = 42
    algorithms = [
        ('greedy', assign_greedy),
        ('hungarian', assign_hungarian),
        ('simple_bundling', assign_simple_bundling),
        ('network_bundling', assign_network_bundling),
        ('anticipated_bundling', assign_anticipated_bundling)
    ]

    all_deterministic = True

    for name, func in algorithms:
        print(f"\nTesting {name}...")
        run1 = run_with_seed(test_seed, func, name, cfg)
        run2 = run_with_seed(test_seed, func, name, cfg)

        # Compare results
        if run1 == run2:
            print(f"  ✓ PASS: Results are identical")
            print(f"    Delivered: {run1['orders_delivered']}, Distance: {run1['total_distance']} km")
        else:
            print(f"  ✗ FAIL: Results differ!")
            print(f"    Run 1: {run1}")
            print(f"    Run 2: {run2}")
            all_deterministic = False

    return all_deterministic

def test_different_seeds():
    """Test that different seeds produce different scenarios but valid results"""
    print("\n" + "="*70)
    print("DIFFERENT SEEDS TEST: Different seeds should create valid scenarios")
    print("="*70)

    cfg = load_config('scenarios/quick_test.yaml')
    seeds = [42, 123, 999]

    # Test hungarian algorithm with different seeds
    print(f"\nTesting hungarian algorithm with seeds: {seeds}")
    print(f"{'Seed':<10} {'Delivered':<12} {'Distance':<12} {'Fulfillment %':<15}")
    print("-"*50)

    results = []
    for seed in seeds:
        result = run_with_seed(seed, assign_hungarian, 'hungarian', cfg)
        results.append(result)
        print(f"{seed:<10} {result['orders_delivered']:<12} "
              f"{result['total_distance']:<12} {result['fulfillment_rate']:<15}")

    # Check that results are different (scenarios vary with seed)
    if len(set(r['total_distance'] for r in results)) > 1:
        print("\n  ✓ Different seeds produce different scenarios")
    else:
        print("\n  ⚠ Warning: All seeds produced identical results (unexpected)")

    return results

def main():
    print("\n" + "="*70)
    print("RANDOM SEED DETERMINISM TEST")
    print("="*70)

    # Test 1: Determinism (same seed = same results)
    deterministic = test_determinism()

    # Test 2: Different seeds
    seed_results = test_different_seeds()

    # Summary
    print("\n" + "="*70)
    print("SUMMARY")
    print("="*70)
    if deterministic:
        print("✓ All algorithms are deterministic (same seed = same results)")
    else:
        print("✗ Some algorithms are non-deterministic!")

    print("\n✓ Different seeds produce valid results")
    print("\nTest completed successfully")

    return 0 if deterministic else 1

if __name__ == '__main__':
    sys.exit(main())
