"""
Interactive Dashboard for Food Delivery Simulation

This module provides an interactive visualization dashboard that allows:
- 2x2 grid showing all 4 algorithms side-by-side
- Playback controls (play/pause, timeline scrubber)
- Speed controls (0.5x, 1x, 2x, 5x, 10x)
- Real-time metrics overlay
- Export to GIF
"""

import numpy as np
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.widgets import Slider, Button
from matplotlib.animation import FuncAnimation, PillowWriter
import json
from datetime import datetime
from typing import Dict, List
from simulator_core import SimulationState, GRID_SIZE, SIMULATION_DURATION


# ============================================================================
# DASHBOARD CLASS
# ============================================================================

class SimulationDashboard:
    """Interactive dashboard for visualizing simulation results."""

    def __init__(self, simulation_results: Dict[str, SimulationState]):
        """
        Initialize dashboard with simulation results.

        Args:
            simulation_results: Dict mapping algorithm names to SimulationState objects
        """
        self.results = simulation_results
        self.algorithm_names = list(simulation_results.keys())

        # Playback state
        self.current_frame = 0
        self.is_playing = False
        self.playback_speed = 1.0
        self.max_frames = min(len(results.timeline) for results in simulation_results.values())

        # Color schemes - use only 5 distinct colors for couriers
        courier_palette = ['blue', 'red', 'green', 'purple', 'orange']
        self.courier_colors = courier_palette
        self.order_state_colors = {
            'READY': 'yellow',
            'ASSIGNED': 'orange',
            'PICKED_UP': 'cyan',
            'DELIVERED': 'green'
        }

        # Setup figure
        self.setup_figure()

    def setup_figure(self):
        """Create the dashboard figure with 2x3 grid layout for 5 algorithms."""
        self.fig = plt.figure(figsize=(20, 14))  # Larger size for better readability

        # Create 2x3 grid for 5 algorithm visualizations
        positions = [(0, 0), (0, 1), (0, 2), (1, 0), (1, 1)]
        self.axes = []

        for i, pos in enumerate(positions):
            if i >= len(self.algorithm_names):
                break

            ax = plt.subplot2grid((2, 3), pos)
            # TIGHT zoom on hub area
            ax.set_xlim(1.3, 2.7)  # Very tight focus on hub
            ax.set_ylim(1.3, 2.7)
            ax.set_xlabel('X (km)', fontsize=12)
            ax.set_ylabel('Y (km)', fontsize=12)
            ax.set_aspect('equal')
            ax.grid(True, alpha=0.3)
            ax.set_title(self.algorithm_names[i].replace('_', ' ').title(),
                        fontsize=14, fontweight='bold')

            self.axes.append(ax)

        # Add timeline slider
        slider_ax = plt.axes([0.15, 0.02, 0.7, 0.02])
        self.timeline_slider = Slider(
            slider_ax, 'Time',
            0, self.max_frames - 1,
            valinit=0,
            valstep=1
        )
        self.timeline_slider.on_changed(self.on_timeline_change)

        # Add play/pause button
        play_ax = plt.axes([0.05, 0.02, 0.08, 0.03])
        self.play_button = Button(play_ax, 'Play')
        self.play_button.on_clicked(self.toggle_play)

        # Add speed controls
        speed_ax = plt.axes([0.88, 0.02, 0.1, 0.03])
        self.speed_text = self.fig.text(0.88, 0.05, f'Speed: {self.playback_speed}x',
                                       fontsize=10, ha='left')

        plt.subplots_adjust(left=0.05, right=0.95, top=0.95, bottom=0.08, hspace=0.25, wspace=0.18)

    def render_frame(self, frame_idx: int):
        """Render a specific frame of the simulation."""
        for ax_idx, (algo_name, ax) in enumerate(zip(self.algorithm_names, self.axes)):
            if frame_idx >= len(self.results[algo_name].timeline):
                continue

            ax.clear()
            ax.set_xlim(-0.5, GRID_SIZE + 0.5)
            ax.set_ylim(-0.5, GRID_SIZE + 0.5)
            ax.set_xlabel('X (km)')
            ax.set_ylabel('Y (km)')
            ax.set_aspect('equal')
            ax.grid(True, alpha=0.3)
            ax.set_title(algo_name.replace('_', ' ').title(), fontsize=12, fontweight='bold')

            # Get snapshot
            snapshot = self.results[algo_name].timeline[frame_idx]
            current_time = snapshot['time']

            # Draw restaurants (green stars)
            restaurants = list(self.results[algo_name].restaurants.values())
            for restaurant in restaurants:
                ax.plot(restaurant.location[0], restaurant.location[1],
                       marker='*', color='green', markersize=30,  # Increased from 20 to 30
                       markeredgecolor='black', markeredgewidth=1, zorder=10)

            # Draw orders (colored circles at diner locations) - skip PENDING to reduce clutter
            for order_id, order_data in snapshot['orders'].items():
                state = order_data['state']
                # Skip PENDING orders to reduce visual clutter
                if state == 'PENDING':
                    continue

                diner_loc = order_data['diner_location']
                color = self.order_state_colors.get(state, 'gray')

                ax.plot(diner_loc[0], diner_loc[1],
                       marker='o', color=color, markersize=12,  # Increased from 8 to 12
                       markeredgecolor='black', markeredgewidth=0.5, zorder=5)

            # Draw couriers (colored triangles)
            for courier_id, courier_data in snapshot['couriers'].items():
                loc = courier_data['current_location']
                state = courier_data['state']
                color_idx = int(courier_id) % len(self.courier_colors)
                color = self.courier_colors[color_idx]

                # Different marker based on state
                if state == 'IDLE':
                    marker = '^'
                    alpha = 0.5
                else:
                    marker = '^'
                    alpha = 1.0

                ax.plot(loc[0], loc[1],
                       marker=marker, color=color, markersize=18,  # Increased from 12 to 18
                       markeredgecolor='black', markeredgewidth=1,
                       alpha=alpha, zorder=8)

                # Draw batch-persistent assignment arrows ONLY for last 30 seconds
                BATCH_INTERVAL = 30  # seconds - arrows persist for this duration
                assigned_order_ids = courier_data.get('assigned_order_ids', [])

                # Draw arrows ONLY for orders assigned in the last 30 seconds
                for order_id in assigned_order_ids:
                    if order_id not in snapshot['orders']:
                        continue

                    order = snapshot['orders'][order_id]
                    assignment_time = order.get('assignment_time')

                    # CRITICAL: Skip if no assignment time or if assigned more than 30 seconds ago
                    if assignment_time is None:
                        continue
                    time_since_assignment = current_time - assignment_time
                    if time_since_assignment > BATCH_INTERVAL:
                        continue  # Arrow expires after 30 seconds!

                    # Determine destination based on order state
                    order_state = order.get('state', 'ASSIGNED')
                    if order_state == 'PICKED_UP':
                        # Arrow to customer
                        dest = order['diner_location']
                    else:
                        # Arrow to restaurant (ASSIGNED or READY state)
                        dest = order['restaurant_location']

                    # SUPER VISIBLE ARROWS - thick, solid, bright
                    num_orders = len(assigned_order_ids)
                    linewidth = 5.0 + (num_orders * 1.5)  # Much thicker

                    ax.plot([loc[0], dest[0]], [loc[1], dest[1]],
                           color=color, linestyle='-', linewidth=linewidth,
                           alpha=1.0, zorder=15)  # Solid, full opacity, higher z-order

            # Add metrics overlay
            metrics = snapshot['metrics']
            metrics_text = (
                f"Time: {int(current_time)}s ({int(current_time/60)}m)\n"
                f"Orders Delivered: {metrics['orders_delivered']}\n"
                f"Bundles: {metrics['bundles_created']}\n"
                f"Avg Bundle Size: {metrics.get('avg_bundle_size', 0):.2f}\n"
                f"Distance: {metrics['total_distance_traveled']:.1f} km"
            )

            ax.text(0.02, 0.98, metrics_text,
                   transform=ax.transAxes,
                   fontsize=12,  # Increased from 9 to 12
                   verticalalignment='top',
                   bbox=dict(boxstyle='round', facecolor='wheat', alpha=0.8))

            # Legend
            legend_elements = [
                mpatches.Patch(color='green', label='Restaurant'),
                mpatches.Patch(color='yellow', label='Order Ready'),
                mpatches.Patch(color='orange', label='Order Assigned'),
                mpatches.Patch(color='cyan', label='Order Picked Up'),
                mpatches.Patch(color='lime', label='Order Delivered')
            ]
            ax.legend(handles=legend_elements, loc='upper right', fontsize=7)

        self.fig.canvas.draw_idle()

    def on_timeline_change(self, val):
        """Handle timeline slider change."""
        self.current_frame = int(val)
        self.render_frame(self.current_frame)

    def toggle_play(self, event):
        """Toggle play/pause."""
        self.is_playing = not self.is_playing
        if self.is_playing:
            self.play_button.label.set_text('Pause')
            self.animate()
        else:
            self.play_button.label.set_text('Play')

    def animate(self):
        """Animate playback."""
        if not self.is_playing:
            return

        self.current_frame += 1
        if self.current_frame >= self.max_frames:
            self.current_frame = 0

        self.timeline_slider.set_val(self.current_frame)
        self.render_frame(self.current_frame)

        # Schedule next frame
        interval = max(1, int(50 / self.playback_speed))  # ms
        self.fig.canvas.manager.window.after(interval, self.animate)

    def change_speed(self, multiplier: float):
        """Change playback speed."""
        self.playback_speed = multiplier
        self.speed_text.set_text(f'Speed: {self.playback_speed}x')
        self.fig.canvas.draw_idle()

    def export_gif(self, output_path: str, fps: int = 5):
        """
        Export animation to GIF at moderate resolution.

        Args:
            output_path: Path to save GIF
            fps: Frames per second (default: 5 for smaller files)
        """
        print(f"Exporting animation to {output_path}...")
        print(f"  Total frames: {self.max_frames}")
        print(f"  Frame rate: {fps} fps")
        print(f"  Resolution: {self.fig.get_size_inches()[0]:.0f}\" x {self.fig.get_size_inches()[1]:.0f}\" at 100 DPI")

        anim = FuncAnimation(
            self.fig,
            lambda frame: self.render_frame(frame),
            frames=self.max_frames,
            interval=1000/fps,
            repeat=True
        )

        # Use higher DPI for better readability
        writer = PillowWriter(fps=fps)
        anim.save(output_path, writer=writer, dpi=100)

        print(f"Animation saved to {output_path}")

    def show(self):
        """Display the dashboard."""
        # Render initial frame
        self.render_frame(0)

        # Add keyboard shortcuts
        def on_key(event):
            if event.key == ' ':  # Space bar
                self.toggle_play(None)
            elif event.key == 'right':
                self.current_frame = min(self.current_frame + 1, self.max_frames - 1)
                self.timeline_slider.set_val(self.current_frame)
            elif event.key == 'left':
                self.current_frame = max(self.current_frame - 1, 0)
                self.timeline_slider.set_val(self.current_frame)
            elif event.key == '+' or event.key == '=':
                self.change_speed(min(self.playback_speed * 2, 10))
            elif event.key == '-':
                self.change_speed(max(self.playback_speed / 2, 0.5))

        self.fig.canvas.mpl_connect('key_press_event', on_key)

        plt.show()


# ============================================================================
# MAIN EXECUTION
# ============================================================================

def run_dashboard(results: Dict[str, SimulationState], export_gif: bool = False):
    """
    Launch the interactive dashboard.

    Args:
        results: Dictionary mapping algorithm names to SimulationState objects
        export_gif: Whether to export animation to GIF
    """
    dashboard = SimulationDashboard(results)

    if export_gif:
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        gif_path = f'/Users/pranjal/Code/meituan/simulation_test/gifs/simulation_{timestamp}.gif'
        dashboard.export_gif(gif_path, fps=10)

    dashboard.show()


def print_comparison_report(results: Dict[str, SimulationState]):
    """
    Print a text-based comparison report of all algorithms.

    Args:
        results: Dictionary mapping algorithm names to SimulationState objects
    """
    print("\n" + "=" * 100)
    print("SIMULATION COMPARISON REPORT")
    print("=" * 100)

    # Header
    print(f"\n{'Algorithm':<25} {'Delivered':>10} {'Avg Delivery':>15} {'Avg Ready-Door':>15} "
          f"{'Utilization':>12} {'Distance':>12} {'Bundles':>10}")
    print("-" * 100)

    # Results for each algorithm
    for algo_name, state in results.items():
        metrics = state.metrics
        print(f"{algo_name.replace('_', ' ').title():<25} "
              f"{metrics['orders_delivered']:>10} "
              f"{metrics['avg_delivery_time']:>14.1f}s "
              f"{metrics['avg_ready_to_door_time']:>14.1f}s "
              f"{metrics['courier_utilization']:>11.1f}% "
              f"{metrics['total_distance_traveled']:>11.1f}km "
              f"{metrics['bundles_created']:>10}")

    print("\n" + "=" * 100)
    print("KEY INSIGHTS")
    print("=" * 100)

    # Find best performer for each metric
    best_delivery_time = min(results.items(), key=lambda x: x[1].metrics['avg_delivery_time'])
    best_distance = min(results.items(), key=lambda x: x[1].metrics['total_distance_traveled'])
    best_utilization = max(results.items(), key=lambda x: x[1].metrics['courier_utilization'])

    print(f"\n1. Fastest Delivery: {best_delivery_time[0]} "
          f"({best_delivery_time[1].metrics['avg_delivery_time']:.1f}s avg)")
    print(f"2. Most Efficient Distance: {best_distance[0]} "
          f"({best_distance[1].metrics['total_distance_traveled']:.1f}km total)")
    print(f"3. Best Courier Utilization: {best_utilization[0]} "
          f"({best_utilization[1].metrics['courier_utilization']:.1f}%)")

    # Bundling effectiveness
    bundling_algos = {name: state for name, state in results.items()
                     if 'bundling' in name.lower()}
    if bundling_algos:
        print(f"\n4. Bundling Effectiveness:")
        for name, state in bundling_algos.items():
            avg_bundle = state.metrics.get('avg_bundle_size', 0)
            print(f"   - {name}: {avg_bundle:.2f} orders/bundle")

    print("\n" + "=" * 100)


if __name__ == '__main__':
    print("This module is meant to be imported. Run main.py to execute the simulation.")
