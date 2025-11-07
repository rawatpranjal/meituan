#!/usr/bin/env python3

import sys
import os
import subprocess
import json
from datetime import datetime

# Add current directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from config_loader import load_config, get_scenario_name, get_output_directory, save_config_snapshot
from scenario_generators import ScenarioFactory

def create_output_directories(output_dir: str):

    os.makedirs(f"{output_dir}/logs", exist_ok=True)
    os.makedirs(f"{output_dir}/gifs", exist_ok=True)
    os.makedirs(f"{output_dir}/analysis", exist_ok=True)
    print(f"✓ Created output directories: {output_dir}/")

def run_scenario(config_path: str):

    print("=" * 80)
    print("RUNNING SCENARIO FROM CONFIG")
    print("=" * 80)
    print(f"Config: {config_path}")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    # Load and validate config
    try:
        config = load_config(config_path)
    except Exception as e:
        print(f"❌ Error loading config: {e}")
        sys.exit(1)

    scenario_name = get_scenario_name(config)
    output_dir = get_output_directory(config)

    print(f"Scenario: {scenario_name}")
    print(f"Output Directory: {output_dir}")
    print()

    # Create output directories
    create_output_directories(output_dir)

    # Generate scenario
    print("=" * 80)
    print("GENERATING SCENARIO")
    print("=" * 80)
    try:
        factory = ScenarioFactory(config)
        print(factory.get_scenario_summary())
        print()
        scenario = factory.create_scenario()
    except Exception as e:
        print(f"❌ Error generating scenario: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    # Save config snapshot for provenance
    metadata_path = f"{output_dir}/metadata.json"
    save_config_snapshot(config, metadata_path)
    print(f"✓ Saved config snapshot: {metadata_path}")
    print()

    # Run simulation for each algorithm
    print("=" * 80)
    print("RUNNING SIMULATIONS")
    print("=" * 80)

    from simulator_core import run_simulation
    from assignment_algorithms import get_algorithm

    algorithms = ['greedy', 'hungarian', 'simple_bundling', 'network_bundling', 'anticipated_bundling']
    algorithm_display_names = {
        'greedy': 'Greedy',
        'hungarian': 'Optimal Single-Order Matching',
        'simple_bundling': 'Single-Pickup Bundling',
        'network_bundling': 'Network Bundling',
        'anticipated_bundling': 'Anticipated Network Bundling'
    }

    results = {}

    for i, algo_name in enumerate(algorithms, 1):
        print(f"\n[{i}/{len(algorithms)}] Running {algorithm_display_names[algo_name]}...")
        print("-" * 80)

        try:
            assignment_func = get_algorithm(algo_name)
            state = run_simulation(scenario, assignment_func, algo_name)

            # Extract results
            results[algo_name] = {
                'orders_delivered': state.metrics['orders_delivered'],
                'total_distance': round(state.metrics['total_distance_traveled'], 2),
                'bundles_created': state.metrics['bundles_created'],
                'avg_delivery_time': round(state.metrics['total_delivery_time'] / max(state.metrics['orders_delivered'], 1) / 60, 1)
            }

            print(f"  ✓ Delivered: {results[algo_name]['orders_delivered']} orders")
            print(f"  ✓ Distance: {results[algo_name]['total_distance']} km")

        except Exception as e:
            print(f"  ❌ Error running {algo_name}: {e}")
            import traceback
            traceback.print_exc()
            results[algo_name] = {'error': str(e)}

    # Update metadata with results
    with open(metadata_path, 'r') as f:
        metadata = json.load(f)
    metadata['run_timestamp'] = datetime.now().isoformat()
    metadata['results'] = results
    with open(metadata_path, 'w') as f:
        json.dump(metadata, f, indent=2)

    print()
    print("=" * 80)
    print("✅ SCENARIO COMPLETE")
    print("=" * 80)
    print(f"Finished: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    print(f"📁 Outputs saved to: {output_dir}/")
    print(f"   ├── logs/ (detailed execution logs)")
    print(f"   ├── gifs/ (animated visualizations)")
    print(f"   ├── analysis/ (batch analysis)")
    print(f"   └── metadata.json (config + results)")
    print()

    # Summary table
    print("=" * 80)
    print("RESULTS SUMMARY")
    print("=" * 80)
    print(f"{'Algorithm':<40} {'Delivered':<12} {'Distance':<12} {'Bundles'}")
    print("-" * 80)
    for algo_name, result in results.items():
        if 'error' not in result:
            display_name = algorithm_display_names[algo_name]
            delivered = result['orders_delivered']
            distance = result['total_distance']
            bundles = result['bundles_created']
            print(f"{display_name:<40} {delivered:<12} {distance:<12} {bundles}")
    print("=" * 80)

    # Generate visualizations
    print()
    print("=" * 80)
    print("GENERATING VISUALIZATIONS")
    print("=" * 80)
    print(f"Creating GIFs for scenario: {scenario_name}")
    print(f"Output: {output_dir}/gifs/")
    print()

    try:
        # Create consolidated GIFs (full simulation)
        print("Creating consolidated GIFs (60 frames)...")
        result = subprocess.run(
            ['python3', 'create_consolidated_gifs.py',
             '--output-dir', output_dir,
             '--scenario', config_path],
            capture_output=True,
            text=True,
            timeout=600
        )
        if result.returncode == 0:
            print("✓ Consolidated GIFs created successfully")
        else:
            print(f"⚠ Warning: Consolidated GIF creation had issues")
            print(result.stderr[:200])

        # Create focused GIFs (key moments only)
        # Note: This requires analysis/top_distinct_batches.json to exist
        if os.path.exists('analysis/top_distinct_batches.json'):
            print("Creating focused GIFs (key moments)...")
            result = subprocess.run(
                ['python3', 'create_focused_gifs.py',
                 '--output-dir', output_dir,
                 '--scenario', config_path],
                capture_output=True,
                text=True,
                timeout=600
            )
            if result.returncode == 0:
                print("✓ Focused GIFs created successfully")
            else:
                print(f"⚠ Warning: Focused GIF creation had issues")
                print(result.stderr[:200])
        else:
            print("⚠ Skipping focused GIFs (requires analysis/top_distinct_batches.json)")

    except subprocess.TimeoutExpired:
        print("⚠ Warning: Visualization generation timed out")
    except Exception as e:
        print(f"⚠ Warning: Could not generate visualizations: {e}")

    print()
    print("=" * 80)
    print("✅ SCENARIO COMPLETE WITH VISUALIZATIONS")
    print("=" * 80)

def main():
    if len(sys.argv) < 2:
        print("Usage: python3 run_scenario.py <config_path>")
        print()
        print("Available scenarios:")
        print("  scenarios/downtown_crush.yaml     - Intense concentrated demand")
        print("  scenarios/river_divide.yaml       - Geographic bottleneck")
        print("  scenarios/popup_problem.yaml      - Unpredictable bursts")
        print()
        sys.exit(1)

    config_path = sys.argv[1]

    if not os.path.exists(config_path):
        print(f"❌ Error: Config file not found: {config_path}")
        sys.exit(1)

    run_scenario(config_path)

if __name__ == '__main__':
    main()
