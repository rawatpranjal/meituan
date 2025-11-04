"""
Decision Snapshot Visualizer for Food Delivery Routing Algorithms

Creates static frame comparisons showing each algorithm's assignment decisions
at key batch moments. Focuses on the "forensic breakdown" of decision-making.
"""

import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches as patches
from matplotlib.patches import FancyBboxPatch, Circle, FancyArrowPatch
from matplotlib.lines import Line2D
import os
from datetime import datetime
from typing import List, Tuple, Dict
from dataclasses import dataclass
from copy import deepcopy

# Import simulation components
from simulator_core import (
    Order, Courier, Restaurant, SimulationState,
    euclidean_distance, COURIER_SPEED_M_PER_S,
    MEAL_PREP_TIME, PICKUP_SERVICE_TIME, DROPOFF_SERVICE_TIME
)
from assignment_algorithms import get_algorithm


# Configuration for controlled scenarios
GRID_SIZE = 5000  # 5km x 5km grid
SNAPSHOT_TIMES = [60, 120, 180, 240, 300]  # Key decision points
OUTPUT_DIR = 'snapshots'


@dataclass
class ControlledScenario:
    """Represents a controlled test scenario for comparison"""
    name: str
    description: str
    orders: List[Order]
    couriers: List[Courier]
    restaurants: List[Restaurant]
    time: float


def create_scenario_t180():
    """
    Create the specific scenario described for t=180s.
    6 orders, 3 couriers, strategic placement to showcase algorithm differences.
    """
    # Create restaurants
    restaurants = [
        Restaurant(restaurant_id=1, location=(1000, 3500)),  # R1 - Northwest
        Restaurant(restaurant_id=2, location=(4000, 3500)),  # R2 - Northeast
        Restaurant(restaurant_id=3, location=(2500, 1500)),  # R3 - South center (cluster)
        Restaurant(restaurant_id=4, location=(2800, 1800)),  # R4 - Near R3
    ]

    # Create orders with strategic placement
    # Note: ready_time is calculated automatically as placement_time + MEAL_PREP_TIME
    orders = [
        # Single orders at different restaurants
        Order(order_id=1, restaurant_id=1, restaurant_location=(1000, 3500),
              diner_location=(800, 4200), placement_time=50),
        Order(order_id=2, restaurant_id=2, restaurant_location=(4000, 3500),
              diner_location=(4200, 4000), placement_time=80),

        # Cluster of 3 orders at same restaurant (R3)
        Order(order_id=3, restaurant_id=3, restaurant_location=(2500, 1500),
              diner_location=(2000, 1000), placement_time=100),
        Order(order_id=4, restaurant_id=3, restaurant_location=(2500, 1500),
              diner_location=(3000, 1200), placement_time=110),
        Order(order_id=5, restaurant_id=3, restaurant_location=(2500, 1500),
              diner_location=(2200, 900), placement_time=120),

        # Single order at nearby restaurant (R4)
        Order(order_id=6, restaurant_id=4, restaurant_location=(2800, 1800),
              diner_location=(3200, 2200), placement_time=130),
    ]

    # Set all orders to READY state for t=180 scenario
    for order in orders:
        order.state = "READY"

    # Create couriers at strategic positions
    couriers = [
        Courier(courier_id=1, start_location=(500, 2500)),   # C1 - West
        Courier(courier_id=2, start_location=(4500, 2500)),  # C2 - East
        Courier(courier_id=3, start_location=(2500, 2500)),  # C3 - Center
    ]

    # Set all couriers to IDLE state
    for courier in couriers:
        courier.state = "IDLE"
        courier.current_location = courier.start_location

    return ControlledScenario(
        name="t180_decision",
        description="6 orders ready, 3 idle couriers - showcases bundling strategies",
        orders=orders,
        couriers=couriers,
        restaurants=restaurants,
        time=180.0
    )


def create_scenario_t60():
    """Create a simpler scenario for t=60s - early batch"""
    restaurants = [
        Restaurant(restaurant_id=1, location=(1500, 1500)),
        Restaurant(restaurant_id=2, location=(3500, 3500)),
    ]

    orders = [
        Order(order_id=1, restaurant_id=1, restaurant_location=(1500, 1500),
              diner_location=(1000, 2000), placement_time=10),
        Order(order_id=2, restaurant_id=1, restaurant_location=(1500, 1500),
              diner_location=(2000, 1800), placement_time=20),
        Order(order_id=3, restaurant_id=2, restaurant_location=(3500, 3500),
              diner_location=(4000, 3000), placement_time=15),
    ]

    for order in orders:
        order.state = "READY"

    couriers = [
        Courier(courier_id=1, start_location=(1000, 1000)),
        Courier(courier_id=2, start_location=(4000, 4000)),
    ]

    for courier in couriers:
        courier.state = "IDLE"
        courier.current_location = courier.start_location

    return ControlledScenario(
        name="t60_decision",
        description="3 orders ready, 2 idle couriers - simple bundling case",
        orders=orders,
        couriers=couriers,
        restaurants=restaurants,
        time=60.0
    )


class SnapshotVisualizer:
    """Handles visualization of decision snapshots"""

    def __init__(self):
        self.colors = {
            'courier1': '#2E86AB',  # Blue
            'courier2': '#A23B72',  # Purple
            'courier3': '#F18F01',  # Orange
            'restaurant': '#2ECC71', # Green
            'unassigned': '#FFC107', # Yellow
            'assigned': '#FF5722',   # Red-orange
            'delivered': '#4CAF50',  # Green
        }

        # Ensure output directory exists
        os.makedirs(OUTPUT_DIR, exist_ok=True)

    def draw_snapshot(self, scenario: ControlledScenario, assignments: List[Tuple[int, List[int]]],
                     algorithm_name: str, save_path: str = None):
        """Draw a single decision snapshot for one algorithm"""

        fig, ax = plt.subplots(figsize=(12, 10), dpi=100)

        # Set up the plot
        ax.set_xlim(0, GRID_SIZE)
        ax.set_ylim(0, GRID_SIZE)
        ax.set_aspect('equal')
        ax.grid(True, alpha=0.2, linestyle='--')

        # Title and labels
        ax.set_title(f'{algorithm_name.replace("_", " ").title()} - Decision at t={scenario.time:.0f}s',
                    fontsize=16, fontweight='bold', pad=20)
        ax.set_xlabel('Distance (meters)', fontsize=12)
        ax.set_ylabel('Distance (meters)', fontsize=12)

        # Create assignment mapping
        assigned_orders = set()
        courier_assignments = {}
        for courier_id, order_ids in assignments:
            courier_assignments[courier_id] = order_ids
            assigned_orders.update(order_ids)

        # Draw restaurants
        for restaurant in scenario.restaurants:
            ax.scatter(restaurant.location[0], restaurant.location[1],
                      s=300, marker='*', color=self.colors['restaurant'],
                      edgecolors='darkgreen', linewidths=2, zorder=5)
            ax.annotate(f'R{restaurant.id}',
                       xy=restaurant.location,
                       xytext=(5, 5), textcoords='offset points',
                       fontsize=10, fontweight='bold')

        # Draw couriers
        for courier in scenario.couriers:
            color = self.colors[f'courier{courier.id}'] if courier.id <= 3 else 'gray'
            ax.scatter(courier.current_location[0], courier.current_location[1],
                      s=200, marker='^', color=color,
                      edgecolors='black', linewidths=2, zorder=6)
            ax.annotate(f'C{courier.id}',
                       xy=courier.current_location,
                       xytext=(5, -15), textcoords='offset points',
                       fontsize=10, fontweight='bold')

        # Draw orders and their assignments
        for order in scenario.orders:
            # Determine order color based on assignment status
            if order.id in assigned_orders:
                # Find which courier is assigned
                for courier_id, order_ids in courier_assignments.items():
                    if order.id in order_ids:
                        color = self.colors[f'courier{courier_id}']
                        break
            else:
                color = self.colors['unassigned']

            # Draw order at restaurant location (ready for pickup)
            circle = Circle(order.restaurant_location, radius=80,
                          color=color, alpha=0.7, zorder=3)
            ax.add_patch(circle)
            ax.annotate(f'O{order.id}',
                       xy=order.restaurant_location,
                       xytext=(0, 0), textcoords='offset points',
                       fontsize=9, ha='center', va='center',
                       fontweight='bold', color='white')

            # Draw diner location (destination)
            ax.scatter(order.diner_location[0], order.diner_location[1],
                      s=50, marker='o', color=color, alpha=0.5,
                      edgecolors='black', linewidths=1, zorder=2)

        # Draw assignment routes
        for courier_id, order_ids in assignments:
            if not order_ids:
                continue

            courier = next(c for c in scenario.couriers if c.id == courier_id)
            color = self.colors[f'courier{courier_id}']

            # Group orders by restaurant for visualization
            orders_by_restaurant = {}
            for order_id in order_ids:
                order = next(o for o in scenario.orders if o.id == order_id)
                if order.restaurant_id not in orders_by_restaurant:
                    orders_by_restaurant[order.restaurant_id] = []
                orders_by_restaurant[order.restaurant_id].append(order)

            # Draw routes
            current_pos = courier.current_location

            # Draw pickup routes
            for restaurant_id, restaurant_orders in orders_by_restaurant.items():
                restaurant_loc = restaurant_orders[0].restaurant_location

                # Draw line from courier to restaurant
                ax.plot([current_pos[0], restaurant_loc[0]],
                       [current_pos[1], restaurant_loc[1]],
                       color=color, linewidth=2, alpha=0.7,
                       linestyle='-', zorder=4)

                # Draw arrow
                ax.annotate('', xy=restaurant_loc,
                          xytext=current_pos,
                          arrowprops=dict(arrowstyle='->', color=color,
                                        lw=2, alpha=0.7))

                # If bundle, draw thicker line and add bundle annotation
                if len(restaurant_orders) > 1:
                    ax.plot([current_pos[0], restaurant_loc[0]],
                           [current_pos[1], restaurant_loc[1]],
                           color=color, linewidth=4, alpha=0.3,
                           linestyle='-', zorder=3)

                    # Add bundle label
                    bundle_ids = ', '.join([f'O{o.id}' for o in restaurant_orders])
                    mid_point = ((current_pos[0] + restaurant_loc[0])/2,
                               (current_pos[1] + restaurant_loc[1])/2)
                    ax.annotate(f'{{{bundle_ids}}}',
                              xy=mid_point,
                              xytext=(10, 10), textcoords='offset points',
                              fontsize=10, fontweight='bold',
                              bbox=dict(boxstyle='round,pad=0.3',
                                      facecolor=color, alpha=0.3))

                # Draw delivery routes (dashed lines)
                for order in restaurant_orders:
                    ax.plot([restaurant_loc[0], order.diner_location[0]],
                           [restaurant_loc[1], order.diner_location[1]],
                           color=color, linewidth=1.5, alpha=0.5,
                           linestyle='--', zorder=2)

        # Add metrics box
        self._add_metrics_box(ax, scenario, assignments, algorithm_name)

        # Add legend
        self._add_legend(ax)

        # Save figure
        if save_path:
            plt.savefig(save_path, dpi=150, bbox_inches='tight',
                       facecolor='white', edgecolor='none')

        return fig, ax

    def _add_metrics_box(self, ax, scenario, assignments, algorithm_name):
        """Add metrics box to the plot"""

        # Calculate metrics
        total_orders = len(scenario.orders)
        assigned_orders = sum(len(order_ids) for _, order_ids in assignments)
        unassigned_orders = total_orders - assigned_orders
        num_bundles = len(assignments)
        avg_bundle_size = assigned_orders / num_bundles if num_bundles > 0 else 0

        # Identify bundle types
        singles = sum(1 for _, orders in assignments if len(orders) == 1)
        doubles = sum(1 for _, orders in assignments if len(orders) == 2)
        triples = sum(1 for _, orders in assignments if len(orders) == 3)
        larger = sum(1 for _, orders in assignments if len(orders) > 3)

        # Create metrics text
        metrics_text = (
            f'Orders Assigned: {assigned_orders}/{total_orders}\n'
            f'Unassigned: {unassigned_orders}\n'
            f'Bundles Created: {num_bundles}\n'
            f'Avg Bundle Size: {avg_bundle_size:.1f}\n'
            f'Distribution: {singles}×1, {doubles}×2, {triples}×3'
        )

        if larger > 0:
            metrics_text += f', {larger}×4+'

        # Add text box
        props = dict(boxstyle='round,pad=0.5', facecolor='white',
                    edgecolor='gray', alpha=0.9)
        ax.text(0.02, 0.98, metrics_text, transform=ax.transAxes,
               fontsize=11, verticalalignment='top',
               bbox=props, family='monospace')

    def _add_legend(self, ax):
        """Add legend to the plot"""

        legend_elements = [
            Line2D([0], [0], marker='*', color='w',
                  markerfacecolor=self.colors['restaurant'],
                  markersize=12, label='Restaurant'),
            Line2D([0], [0], marker='^', color='w',
                  markerfacecolor='gray',
                  markersize=10, label='Courier'),
            Line2D([0], [0], marker='o', color='w',
                  markerfacecolor=self.colors['unassigned'],
                  markersize=10, label='Unassigned Order'),
            Line2D([0], [0], marker='o', color='w',
                  markerfacecolor=self.colors['courier1'],
                  markersize=10, label='Assigned Order'),
            Line2D([0], [0], color='black', linewidth=2,
                  label='Pickup Route'),
            Line2D([0], [0], color='black', linewidth=2,
                  linestyle='--', label='Delivery Route'),
        ]

        ax.legend(handles=legend_elements, loc='upper right',
                 framealpha=0.9, fontsize=10)


def run_algorithm_on_scenario(scenario: ControlledScenario,
                             algorithm_name: str) -> List[Tuple[int, List[int]]]:
    """Run a specific algorithm on a controlled scenario"""

    # Create simulation state with proper initialization
    state = SimulationState(
        restaurants=scenario.restaurants,
        couriers=[deepcopy(c) for c in scenario.couriers],
        order_schedule=[deepcopy(o) for o in scenario.orders]
    )

    # Get algorithm function
    algorithm_func = get_algorithm(algorithm_name)

    # Get idle couriers and ready orders
    idle_couriers = [c for c in state.couriers.values() if c.state == "IDLE"]
    ready_orders = [o for o in state.orders.values() if o.state == "READY"]

    # Run assignment algorithm
    assignments = algorithm_func(state, idle_couriers, ready_orders)

    return assignments


def generate_comparison_grid(scenario: ControlledScenario):
    """Generate a large, high-resolution comparison grid showing all algorithms side by side"""

    algorithms = ['greedy', 'hungarian', 'simple_bundling',
                 'route_cost_bundling', 'batched_pickups']

    # Create large, high-resolution figure
    fig = plt.figure(figsize=(35, 28), dpi=200)  # Very large, high DPI for zooming
    fig.suptitle(f'Algorithm Comparison - Decision at t={scenario.time:.0f}s\n{scenario.description}',
                fontsize=20, fontweight='bold', y=0.98)

    # Create grid layout (2 rows, 3 columns for 5 algorithms)
    positions = [(0, 0), (0, 1), (0, 2), (1, 0), (1, 1)]

    visualizer = SnapshotVisualizer()

    for idx, algorithm_name in enumerate(algorithms):
        # Run algorithm
        assignments = run_algorithm_on_scenario(scenario, algorithm_name)

        # Create subplot
        row, col = positions[idx]
        ax = plt.subplot2grid((2, 3), (row, col), fig=fig)

        # Set up the subplot
        ax.set_xlim(0, GRID_SIZE)
        ax.set_ylim(0, GRID_SIZE)
        ax.set_aspect('equal')
        ax.grid(True, alpha=0.2, linestyle='--')
        ax.set_xlabel('Distance (meters)', fontsize=10)
        ax.set_ylabel('Distance (meters)', fontsize=10)
        ax.set_title(algorithm_name.replace("_", " ").title(),
                    fontsize=14, fontweight='bold', pad=10)

        # Draw restaurants
        for restaurant in scenario.restaurants:
            ax.scatter(restaurant.location[0], restaurant.location[1],
                      s=250, marker='*', color=visualizer.colors['restaurant'],
                      edgecolors='darkgreen', linewidths=2, zorder=5)
            ax.annotate(f'R{restaurant.id}',
                       xy=restaurant.location,
                       xytext=(5, 5), textcoords='offset points',
                       fontsize=9, fontweight='bold')

        # Draw couriers
        for courier in scenario.couriers:
            color = visualizer.colors[f'courier{courier.id}'] if courier.id <= 3 else 'gray'
            ax.scatter(courier.current_location[0], courier.current_location[1],
                      s=150, marker='^', color=color,
                      edgecolors='black', linewidths=2, zorder=6)
            ax.annotate(f'C{courier.id}',
                       xy=courier.current_location,
                       xytext=(5, -15), textcoords='offset points',
                       fontsize=9, fontweight='bold')

        # Create assignment mapping
        assigned_orders = set()
        courier_assignments = {}
        for courier_id, order_ids in assignments:
            courier_assignments[courier_id] = order_ids
            assigned_orders.update(order_ids)

        # Draw orders
        for order in scenario.orders:
            # Determine color
            if order.id in assigned_orders:
                for courier_id, order_ids in courier_assignments.items():
                    if order.id in order_ids:
                        color = visualizer.colors[f'courier{courier_id}']
                        break
            else:
                color = visualizer.colors['unassigned']

            # Draw order at restaurant (ready for pickup)
            circle = Circle(order.restaurant_location, radius=70,
                          color=color, alpha=0.7, zorder=3)
            ax.add_patch(circle)
            ax.annotate(f'O{order.id}',
                       xy=order.restaurant_location,
                       xytext=(0, 0), textcoords='offset points',
                       fontsize=8, ha='center', va='center',
                       fontweight='bold', color='white')

            # Draw diner location
            ax.scatter(order.diner_location[0], order.diner_location[1],
                      s=40, marker='o', color=color, alpha=0.5,
                      edgecolors='black', linewidths=1, zorder=2)

        # Draw assignment routes
        for courier_id, order_ids in assignments:
            if not order_ids:
                continue

            courier = next(c for c in scenario.couriers if c.id == courier_id)
            color = visualizer.colors[f'courier{courier_id}']

            # Group orders by restaurant
            orders_by_restaurant = {}
            for order_id in order_ids:
                order = next(o for o in scenario.orders if o.id == order_id)
                if order.restaurant_id not in orders_by_restaurant:
                    orders_by_restaurant[order.restaurant_id] = []
                orders_by_restaurant[order.restaurant_id].append(order)

            current_pos = courier.current_location

            # Draw pickup routes
            for restaurant_id, restaurant_orders in orders_by_restaurant.items():
                restaurant_loc = restaurant_orders[0].restaurant_location

                # Line to restaurant
                ax.plot([current_pos[0], restaurant_loc[0]],
                       [current_pos[1], restaurant_loc[1]],
                       color=color, linewidth=2, alpha=0.7,
                       linestyle='-', zorder=4)

                # Arrow
                ax.annotate('', xy=restaurant_loc,
                          xytext=current_pos,
                          arrowprops=dict(arrowstyle='->', color=color,
                                        lw=2, alpha=0.7))

                # Bundle indicator
                if len(restaurant_orders) > 1:
                    bundle_ids = ', '.join([f'O{o.id}' for o in restaurant_orders])
                    mid_point = ((current_pos[0] + restaurant_loc[0])/2,
                               (current_pos[1] + restaurant_loc[1])/2)
                    ax.annotate(f'{{{bundle_ids}}}',
                              xy=mid_point,
                              xytext=(10, 10), textcoords='offset points',
                              fontsize=8, fontweight='bold',
                              bbox=dict(boxstyle='round,pad=0.3',
                                      facecolor=color, alpha=0.3))

                # Delivery routes (dashed)
                for order in restaurant_orders:
                    ax.plot([restaurant_loc[0], order.diner_location[0]],
                           [restaurant_loc[1], order.diner_location[1]],
                           color=color, linewidth=1.5, alpha=0.5,
                           linestyle='--', zorder=2)

        # Add metrics box
        total_orders = len(scenario.orders)
        assigned = sum(len(order_ids) for _, order_ids in assignments)
        unassigned = total_orders - assigned
        num_bundles = len(assignments)

        metrics_text = (
            f'Assigned: {assigned}/{total_orders}\n'
            f'Unassigned: {unassigned}\n'
            f'Bundles: {num_bundles}'
        )

        props = dict(boxstyle='round,pad=0.3', facecolor='white',
                    edgecolor='gray', alpha=0.9)
        ax.text(0.02, 0.98, metrics_text, transform=ax.transAxes,
               fontsize=9, verticalalignment='top',
               bbox=props, family='monospace')

    plt.tight_layout()

    # Save high-resolution comparison grid
    save_path = os.path.join(OUTPUT_DIR, f'algorithm_comparison_{scenario.name}.png')
    plt.savefig(save_path, dpi=300, bbox_inches='tight',
               facecolor='white', edgecolor='none')
    print(f"\nSaved high-resolution comparison grid:")
    print(f"  File: {save_path}")
    print(f"  Resolution: 300 DPI")
    print(f"  Size: {fig.get_size_inches()[0]:.0f}\" x {fig.get_size_inches()[1]:.0f}\"")

    return fig


def create_scenario_differentiation():
    """
    Create a scenario specifically designed to make all 5 algorithms
    give different solutions.

    Strategic placement:
    - 5 orders, 3 couriers
    - Orders positioned to force different bundling decisions
    - Geographic layout creates clear trade-offs
    """
    # Create restaurants at strategic positions
    restaurants = [
        Restaurant(restaurant_id=1, location=(1000, 4000)),  # R1 - Northwest
        Restaurant(restaurant_id=2, location=(4000, 4000)),  # R2 - Northeast
        Restaurant(restaurant_id=3, location=(2300, 2000)),  # R3 - Center-south
        Restaurant(restaurant_id=4, location=(2600, 2200)),  # R4 - Near R3
        Restaurant(restaurant_id=5, location=(3500, 1500)),  # R5 - Southeast
    ]

    # Create orders strategically
    orders = [
        # O1 at R1 - Northwest corner
        Order(order_id=1, restaurant_id=1, restaurant_location=(1000, 4000),
              diner_location=(800, 4500), placement_time=50),

        # O2 at R2 - Northeast corner
        Order(order_id=2, restaurant_id=2, restaurant_location=(4000, 4000),
              diner_location=(4300, 3500), placement_time=60),

        # O3, O4 at R3 - Same restaurant (center-south)
        Order(order_id=3, restaurant_id=3, restaurant_location=(2300, 2000),
              diner_location=(2000, 1500), placement_time=70),
        Order(order_id=4, restaurant_id=3, restaurant_location=(2300, 2000),
              diner_location=(2600, 1700), placement_time=80),

        # O5 at R4 - Near R3 (different restaurant but geographically close)
        Order(order_id=5, restaurant_id=4, restaurant_location=(2600, 2200),
              diner_location=(3000, 2500), placement_time=90),
    ]

    # Set all orders to READY
    for order in orders:
        order.state = "READY"

    # Position couriers strategically
    couriers = [
        Courier(courier_id=1, start_location=(500, 4500)),    # C1 - Near R1/O1
        Courier(courier_id=2, start_location=(4500, 3500)),   # C2 - Near R2/O2
        Courier(courier_id=3, start_location=(2500, 2500)),   # C3 - Center, near R3/R4 cluster
    ]

    for courier in couriers:
        courier.state = "IDLE"
        courier.current_location = courier.start_location

    return ControlledScenario(
        name="differentiation",
        description="5 orders, 3 couriers - designed to differentiate all algorithms",
        orders=orders,
        couriers=couriers,
        restaurants=restaurants,
        time=200.0
    )


def main():
    """Main function to generate comparison grid only"""

    print("=" * 80)
    print("ALGORITHM COMPARISON GRID GENERATOR")
    print("=" * 80)

    # Create the differentiation scenario
    scenario = create_scenario_differentiation()

    print(f"\nScenario: {scenario.name}")
    print(f"Description: {scenario.description}")
    print(f"Orders: {len(scenario.orders)}")
    print(f"Couriers: {len(scenario.couriers)}")
    print(f"Restaurants: {len(scenario.restaurants)}")

    # Generate high-resolution comparison grid
    print(f"\nGenerating high-resolution comparison grid...")
    generate_comparison_grid(scenario)

    print("\n" + "=" * 80)
    print(f"Comparison grid saved to: {OUTPUT_DIR}/")
    print("=" * 80)


if __name__ == '__main__':
    main()