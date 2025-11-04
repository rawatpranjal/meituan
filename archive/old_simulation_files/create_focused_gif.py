"""
Create a focused GIF showing only the critical decision period (first 500s).
"""

import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation, PillowWriter
from simulator_core import generate_asymmetric_scenario, run_simulation
from assignment_algorithms import get_algorithm

print("Creating focused GIF (first 500s)...")

# Generate scenario and run simulations
scenario = generate_asymmetric_scenario(duration=500)
algorithms = ['greedy', 'hungarian', 'simple_bundling', 'route_cost_bundling']

results = {}
for algo_name in algorithms:
    print(f"  Running {algo_name}...")
    state = run_simulation(scenario, get_algorithm(algo_name), algo_name)
    results[algo_name] = state

# Create figure
fig, axes = plt.subplots(2, 2, figsize=(16, 12))
fig.suptitle('Algorithm Comparison - Critical Decision Period', fontsize=16, fontweight='bold')

# Setup axes
ax_flat = axes.flatten()
titles = ['Greedy', 'Hungarian', 'Simple Bundling', 'Route Cost']

for ax, title in zip(ax_flat, titles):
    ax.set_title(title, fontsize=14, fontweight='bold')
    ax.set_xlim(0, 5)
    ax.set_ylim(0, 5)
    ax.set_aspect('equal')
    ax.grid(True, alpha=0.2)

def update(frame):
    for ax, algo_name in zip(ax_flat, algorithms):
        ax.clear()
        ax.set_title(titles[algorithms.index(algo_name)], fontsize=14, fontweight='bold')
        ax.set_xlim(0, 5)
        ax.set_ylim(0, 5)
        ax.set_aspect('equal')
        ax.grid(True, alpha=0.2)
        
        if frame >= len(results[algo_name].timeline):
            continue
            
        snapshot = results[algo_name].timeline[frame]
        time = snapshot['time']
        
        # Draw restaurants
        for r in scenario['restaurants']:
            ax.scatter(r.location[0], r.location[1], marker='*', s=400, c='green',
                      edgecolor='black', linewidth=2, zorder=10)
        
        # Draw couriers and routes
        colors = ['blue', 'red', 'purple', 'darkgreen', 'brown']
        for c_id, courier_data in snapshot['couriers'].items():
            c_idx = int(c_id)
            loc = courier_data['current_location']
            color = colors[c_idx % len(colors)]
            
            ax.scatter(loc[0], loc[1], marker='^', s=250, c=color,
                      edgecolor='black', linewidth=1.5, alpha=0.8, zorder=8)
            
            if courier_data.get('next_destination'):
                dest = courier_data['next_destination']
                num_orders = len(courier_data.get('assigned_order_ids', []))
                linewidth = 3 if num_orders > 1 else 1.5
                
                ax.plot([loc[0], dest[0]], [loc[1], dest[1]],
                       color=color, linewidth=linewidth, alpha=0.6, linestyle='--')
        
        # Show time and metrics
        metrics = snapshot['metrics']
        text = f"t={int(time)}s | Del: {metrics['orders_delivered']} | Bundles: {metrics['bundles_created']}"
        ax.text(0.5, 0.02, text, transform=ax.transAxes, ha='center',
               fontsize=10, bbox=dict(boxstyle='round', facecolor='white', alpha=0.8))

    fig.suptitle(f'Algorithm Comparison - t={int(snapshot["time"])}s', fontsize=16, fontweight='bold')
    return ax_flat

# Create animation
anim = FuncAnimation(fig, update, frames=50, interval=100, blit=False)

# Save as GIF
writer = PillowWriter(fps=5)
output_path = 'gifs/focused_comparison_500s.gif'
print(f"  Saving to {output_path}...")
anim.save(output_path, writer=writer, dpi=80)
print(f"  Saved focused GIF to {output_path}")
