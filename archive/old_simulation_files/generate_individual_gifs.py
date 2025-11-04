"""
Generate Individual Algorithm GIFs

Creates separate GIF animations for each algorithm with configurable
duration and playback speed for better observation of differences.
"""

import sys
import os
import argparse
import json
from datetime import datetime
from copy import deepcopy
import numpy as np
from simulator_core import (
    generate_scenario, run_simulation,
    SIMULATION_DURATION, SimulationState
)
from assignment_algorithms import get_algorithm
from dashboard import SimulationDashboard
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation, PillowWriter


def generate_short_scenario(duration=600, num_orders=30, num_couriers=5):
    """Generate a shorter scenario for demo purposes"""

    # Save original values
    import simulator_core
    original_duration = simulator_core.SIMULATION_DURATION
    original_num_orders = simulator_core.NUM_ORDERS
    original_num_couriers = simulator_core.NUM_COURIERS

    # Temporarily override for generation
    simulator_core.SIMULATION_DURATION = duration
    simulator_core.NUM_ORDERS = num_orders
    simulator_core.NUM_COURIERS = num_couriers

    # Generate scenario with specified parameters
    scenario = generate_scenario()

    # Restore original values
    simulator_core.SIMULATION_DURATION = original_duration
    simulator_core.NUM_ORDERS = original_num_orders
    simulator_core.NUM_COURIERS = original_num_couriers

    return scenario


class IndividualDashboard(SimulationDashboard):
    """Modified dashboard for single algorithm visualization"""

    def __init__(self, results: dict, algorithm_name: str):
        """Initialize with single algorithm result"""
        # Filter to just one algorithm
        self.single_result = {algorithm_name: results[algorithm_name]}
        self.algorithm_name = algorithm_name
        super().__init__(self.single_result)

    def setup_figure(self):
        """Override to create single panel instead of 2x2 grid"""
        self.fig = plt.figure(figsize=(14, 10), facecolor='white')

        # Single subplot for the algorithm
        self.axes = {
            self.algorithm_name: self.fig.add_subplot(111)
        }

        # Set up the single axis
        ax = self.axes[self.algorithm_name]
        ax.set_xlim(0, 5000)
        ax.set_ylim(0, 5000)
        ax.set_aspect('equal')
        ax.grid(True, alpha=0.3, linestyle='--')
        ax.set_xlabel('Distance (meters)', fontsize=12)
        ax.set_ylabel('Distance (meters)', fontsize=12)
        ax.set_title(f'{self.algorithm_name.replace("_", " ").title()} - Food Delivery Routing',
                    fontsize=14, fontweight='bold', pad=15)

    def export_gif(self, output_path: str, fps: int = 5):
        """Export animation to GIF with configurable FPS"""

        print(f"Exporting {self.algorithm_name} to GIF at {fps} fps...")

        # Get timeline from results
        timeline = self.results[self.algorithm_name].timeline

        # Create animation
        def update_wrapper(frame):
            self.current_frame = frame
            return self.update_frame(frame)

        anim = FuncAnimation(
            self.fig,
            update_wrapper,
            frames=len(timeline),
            interval=100,  # Not used in save
            blit=False
        )

        # Save with custom FPS (slower playback)
        writer = PillowWriter(fps=fps)
        anim.save(output_path, writer=writer)
        print(f"  Saved: {output_path}")

        plt.close(self.fig)

        return output_path


def generate_individual_gif(scenario, algorithm_name, duration=600, fps=5):
    """Generate GIF for a single algorithm"""

    print(f"\nGenerating GIF for {algorithm_name}...")

    # Run simulation
    algorithm_func = get_algorithm(algorithm_name)

    # Temporarily override simulation duration
    import simulator_core
    original_duration = simulator_core.SIMULATION_DURATION
    simulator_core.SIMULATION_DURATION = duration

    state = run_simulation(scenario, algorithm_func, algorithm_name)
    results = {algorithm_name: state}

    # Restore original duration
    simulator_core.SIMULATION_DURATION = original_duration

    print(f"  Simulation complete: {state.metrics['orders_delivered']} orders delivered")

    # Create individual dashboard
    dashboard = IndividualDashboard(results, algorithm_name)

    # Export GIF with slow playback
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_path = f'gifs/{algorithm_name}_{duration}s_fps{fps}.gif'

    # Ensure output directory exists
    os.makedirs('gifs', exist_ok=True)

    dashboard.export_gif(output_path, fps=fps)

    return output_path, state.metrics


def main():
    parser = argparse.ArgumentParser(description='Generate individual algorithm GIFs')
    parser.add_argument('--duration', type=int, default=600,
                       help='Simulation duration in seconds (default: 600)')
    parser.add_argument('--fps', type=int, default=5,
                       help='GIF frames per second (default: 5)')
    parser.add_argument('--orders', type=int, default=30,
                       help='Number of orders (default: 30)')
    parser.add_argument('--couriers', type=int, default=5,
                       help='Number of couriers (default: 5)')
    parser.add_argument('--algorithms', nargs='+',
                       choices=['greedy', 'hungarian', 'simple_bundling',
                               'route_cost_bundling', 'batched_pickups'],
                       default=['greedy', 'hungarian', 'simple_bundling',
                               'route_cost_bundling', 'batched_pickups'],
                       help='Algorithms to generate GIFs for')

    args = parser.parse_args()

    print("=" * 80)
    print("INDIVIDUAL ALGORITHM GIF GENERATOR")
    print("=" * 80)
    print(f"Configuration:")
    print(f"  Duration: {args.duration} seconds")
    print(f"  FPS: {args.fps} frames per second")
    print(f"  Orders: {args.orders}")
    print(f"  Couriers: {args.couriers}")
    print(f"  Algorithms: {', '.join(args.algorithms)}")

    # Generate scenario
    print("\nGenerating scenario...")
    scenario = generate_short_scenario(
        duration=args.duration,
        num_orders=args.orders,
        num_couriers=args.couriers
    )

    print(f"  Generated {len(scenario['order_schedule'])} orders")
    print(f"  {len(scenario['couriers'])} couriers")
    print(f"  {len(scenario['restaurants'])} restaurants")

    # Generate GIF for each algorithm
    results_summary = {}

    for algorithm_name in args.algorithms:
        output_path, metrics = generate_individual_gif(
            scenario,
            algorithm_name,
            duration=args.duration,
            fps=args.fps
        )

        results_summary[algorithm_name] = {
            'gif_path': output_path,
            'orders_delivered': metrics['orders_delivered'],
            'avg_delivery_time': metrics.get('avg_delivery_time', 0),
            'fulfillment_rate': metrics.get('fulfillment_rate_pct', 0)
        }

    # Print summary
    print("\n" + "=" * 80)
    print("GENERATION COMPLETE")
    print("=" * 80)

    print("\nSummary:")
    for algo, results in results_summary.items():
        print(f"\n{algo.replace('_', ' ').title()}:")
        print(f"  File: {results['gif_path']}")
        print(f"  Orders delivered: {results['orders_delivered']}")
        print(f"  Fulfillment rate: {results['fulfillment_rate']:.1f}%")

    print("\nAll GIFs saved to: gifs/")
    print("=" * 80)


if __name__ == '__main__':
    main()