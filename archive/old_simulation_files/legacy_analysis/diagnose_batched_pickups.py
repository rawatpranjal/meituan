#!/usr/bin/env python3
"""
Diagnose why Batched Pickups still has lower bundle sizes than Simple Bundling.
"""

import sys
from simulator_core import run_simulation
from assignment_algorithms import get_algorithm
from create_clean_simple_viz import generate_dense_continuous_scenario

def diagnose_batched_pickups():
    """Run detailed diagnostic comparison."""

    print("=" * 100)
    print("BATCHED PICKUPS DIAGNOSTIC")
    print("=" * 100)

    # Generate shared scenario with fixed seed
    scenario = generate_dense_continuous_scenario(seed=42)
    print(f"\nScenario: {len(scenario['order_schedule'])} orders, {len(scenario['couriers'])} couriers")

    # Run Simple Bundling
    print("\n" + "-" * 100)
    print("SIMPLE BUNDLING (Same-Restaurant Only)")
    print("-" * 100)
    simple_bundling = get_algorithm('simple_bundling')
    state1 = run_simulation(scenario, simple_bundling, 'simple_bundling')

    print(f"  Delivered: {state1.metrics['orders_delivered']}/{len(scenario['order_schedule'])}")
    print(f"  Bundle Size: {state1.metrics['avg_bundle_size']:.2f}")
    print(f"  Fulfillment: {state1.metrics['fulfillment_rate_pct']:.1f}%")
    print(f"  Throughput: {state1.metrics['system_throughput_orders_per_hour']:.1f} ord/hr")

    # Run Batched Pickups
    print("\n" + "-" * 100)
    print("BATCHED PICKUPS (Multi-Restaurant Allowed)")
    print("-" * 100)
    batched_pickups = get_algorithm('batched_pickups')
    state2 = run_simulation(scenario, batched_pickups, 'batched_pickups')

    print(f"  Delivered: {state2.metrics['orders_delivered']}/{len(scenario['order_schedule'])}")
    print(f"  Bundle Size: {state2.metrics['avg_bundle_size']:.2f}")
    print(f"  Fulfillment: {state2.metrics['fulfillment_rate_pct']:.1f}%")
    print(f"  Throughput: {state2.metrics['system_throughput_orders_per_hour']:.1f} ord/hr")

    # Analyze the difference
    print("\n" + "=" * 100)
    print("ANALYSIS")
    print("=" * 100)

    bundle_diff = state2.metrics['avg_bundle_size'] - state1.metrics['avg_bundle_size']
    fulfillment_diff = state2.metrics['fulfillment_rate_pct'] - state1.metrics['fulfillment_rate_pct']

    print(f"\nBundle Size Difference: {bundle_diff:+.2f}")
    print(f"  Simple Bundling: {state1.metrics['avg_bundle_size']:.2f}")
    print(f"  Batched Pickups: {state2.metrics['avg_bundle_size']:.2f}")

    print(f"\nFulfillment Difference: {fulfillment_diff:+.1f}%")
    print(f"  Simple Bundling: {state1.metrics['fulfillment_rate_pct']:.1f}%")
    print(f"  Batched Pickups: {state2.metrics['fulfillment_rate_pct']:.1f}%")

    # Check if multi-restaurant capability helps
    if bundle_diff < 0:
        print("\n⚠️ PROBLEM: Batched Pickups has LOWER bundle size despite allowing multi-restaurant")
        print("   Possible causes:")
        print("   1. Multi-restaurant bundles have higher route costs")
        print("   2. Too many bundle combinations confusing the Hungarian solver")
        print("   3. Bundle explosion making optimization harder")
    elif bundle_diff > 0.1:
        print("\n✅ SUCCESS: Batched Pickups achieves higher bundle sizes")
    else:
        print("\n⚠️ MARGINAL: Batched Pickups only slightly better at bundling")

    print("\n" + "=" * 100)

if __name__ == "__main__":
    diagnose_batched_pickups()
