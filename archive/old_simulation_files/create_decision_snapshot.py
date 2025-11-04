"""
Create a static decision snapshot showing algorithm differences at critical moment.
"""

import matplotlib.pyplot as plt
import matplotlib.patches as patches
from simulator_core import generate_asymmetric_scenario, run_simulation
from assignment_algorithms import get_algorithm

# Generate scenario and run all algorithms
scenario = generate_asymmetric_scenario(duration=900)
algorithms = ['greedy', 'hungarian', 'simple_bundling', 'route_cost_bundling']

# Run simulations
print("Generating decision snapshot...")
results = {}
for algo_name in algorithms:
    print(f"  Running {algo_name}...")
    state = run_simulation(scenario, get_algorithm(algo_name), algo_name)
    results[algo_name] = state

# Create figure showing decision moment at t=480s (after all orders ready)
fig, axes = plt.subplots(2, 2, figsize=(16, 12))
fig.suptitle('Algorithm Comparison: Assignment Decisions at t=480s (All Orders Ready)', fontsize=16, fontweight='bold')

ax_flat = axes.flatten()
titles = ['Greedy (Baseline)', 'Hungarian (Optimal 1-to-1)', 'Simple Bundling (Volume Focus)', 'Route Cost (Efficiency Focus)']

for idx, (algo_name, ax) in enumerate(zip(algorithms, ax_flat)):
    ax.set_title(titles[idx], fontsize=14, fontweight='bold')
    ax.set_xlim(0, 5)
    ax.set_ylim(0, 5)
    ax.set_aspect('equal')
    ax.grid(True, alpha=0.2)
    ax.set_xlabel('X (km)', fontsize=11)
    ax.set_ylabel('Y (km)', fontsize=11)

    # Get snapshot at t=480s (frame 48)
    frame = min(48, len(results[algo_name].timeline) - 1)
    snapshot = results[algo_name].timeline[frame]

    # Draw restaurants with labels
    restaurants = scenario['restaurants']
    for idx, r in enumerate(restaurants):
        ax.scatter(r.location[0], r.location[1], marker='*', s=500, c='green',
                  edgecolor='black', linewidth=2, zorder=10, label='Restaurant' if idx == 0 else None)

        # Add restaurant label
        if idx < 2:  # Hub restaurants
            ax.text(r.location[0], r.location[1]-0.15, f'R{idx+1}\n(Hub)', ha='center', fontsize=9, fontweight='bold')
        else:  # Suburban restaurant
            ax.text(r.location[0], r.location[1]-0.15, f'R{idx+1}\n(Suburban)', ha='center', fontsize=9, fontweight='bold')

    # Count ready/unassigned orders at each restaurant
    restaurant_orders = {0: 0, 1: 0, 2: 0}
    for order_id, order_data in snapshot['orders'].items():
        if order_data['state'] == 'READY':
            restaurant_orders[order_data.get('restaurant_id', 0)] += 1

    # Show order counts at restaurants
    for r_idx, count in restaurant_orders.items():
        if count > 0 and r_idx < len(restaurants):
            r = restaurants[r_idx]
            ax.text(r.location[0]+0.2, r.location[1], f'{count} ready',
                   fontsize=10, color='orange', fontweight='bold',
                   bbox=dict(boxstyle='round', facecolor='yellow', alpha=0.5))

    # Draw couriers and their assignments
    colors = ['blue', 'red', 'purple', 'darkgreen', 'brown']
    for c_id, courier_data in snapshot['couriers'].items():
        c_idx = int(c_id)
        loc = courier_data['current_location']
        color = colors[c_idx % len(colors)]

        # Determine courier type
        if c_idx < 2:
            c_label = f'C{c_id}\n(Hub)'
        elif c_idx == 2:
            c_label = f'C{c_id}\n(Suburban)'
        else:
            c_label = f'C{c_id}\n(Remote)'

        # Draw courier
        ax.scatter(loc[0], loc[1], marker='^', s=300, c=color,
                  edgecolor='black', linewidth=2, alpha=0.8, zorder=8)
        ax.text(loc[0], loc[1]+0.12, c_label, ha='center', fontsize=8, color=color, fontweight='bold')

        # Draw assignment line if courier is assigned
        if courier_data.get('next_destination'):
            dest = courier_data['next_destination']
            num_orders = len(courier_data.get('assigned_order_ids', []))
            linewidth = 4 if num_orders > 1 else 2

            # Draw assignment line
            ax.plot([loc[0], dest[0]], [loc[1], dest[1]],
                   color=color, linewidth=linewidth, alpha=0.7, linestyle='--')

            # Add arrow to show direction
            ax.annotate('', xy=(dest[0], dest[1]), xytext=(loc[0], loc[1]),
                       arrowprops=dict(arrowstyle='->', color=color, lw=linewidth, alpha=0.7))

            # Show bundle size if > 1
            if num_orders > 1:
                mid_x, mid_y = (loc[0] + dest[0])/2, (loc[1] + dest[1])/2
                ax.text(mid_x, mid_y, f'BUNDLE\n{num_orders} orders',
                       fontsize=11, fontweight='bold', color=color,
                       bbox=dict(boxstyle='round', facecolor='white', edgecolor=color, linewidth=2))

    # Show key metrics
    metrics = snapshot['metrics']
    delivered = metrics['orders_delivered']
    bundles = metrics['bundles_created']
    avg_bundle = metrics.get('avg_bundle_size', 1.0)

    metrics_text = (
        f"Delivered: {delivered}/8\n"
        f"Bundles: {bundles}\n"
        f"Avg bundle: {avg_bundle:.1f} orders"
    )
    ax.text(0.02, 0.98, metrics_text, transform=ax.transAxes,
           fontsize=11, verticalalignment='top', fontweight='bold',
           bbox=dict(boxstyle='round', facecolor='lightblue', alpha=0.9))

    # Add algorithm assessment
    if algo_name == 'simple_bundling' and avg_bundle > 1.5:
        ax.text(0.5, 0.05, '✓ Bundling Success!', transform=ax.transAxes,
               ha='center', fontsize=12, color='green', fontweight='bold')
    elif algo_name in ['greedy', 'hungarian'] and bundles > 7:
        ax.text(0.5, 0.05, '✗ No bundling capability', transform=ax.transAxes,
               ha='center', fontsize=12, color='red', fontweight='bold')

plt.tight_layout()
plt.savefig('/Users/pranjal/Code/meituan/simulation_test/decision_snapshot.png', dpi=150, bbox_inches='tight')
print("\nSaved decision snapshot to decision_snapshot.png")
plt.show()