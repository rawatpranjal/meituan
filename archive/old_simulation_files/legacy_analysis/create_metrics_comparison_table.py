"""
Generate comprehensive metrics comparison table for all algorithms.

Reads comparative_summary.json and creates a professional PNG table showing
all performance metrics across the 5 assignment algorithms.
"""

import json
import sys
from datetime import datetime
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
from matplotlib.patches import Rectangle
import numpy as np

# Setup logging
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
log_path = f'/Users/pranjal/Code/meituan/simulation_test/logs/create_comparison_table_{timestamp}.log'

def log(message):
    """Log to both stdout and file."""
    print(message)
    with open(log_path, 'a') as f:
        f.write(message + '\n')

def format_value(value, metric_name):
    """Format values based on metric type."""
    # Percentage metrics
    if '_pct' in metric_name or 'utilization' in metric_name or 'rate' in metric_name:
        return f'{value:.1f}%'
    # Time metrics (convert to minutes)
    elif 'time' in metric_name and 'throughput' not in metric_name:
        return f'{value/60:.1f} min'
    # Distance metrics
    elif 'distance' in metric_name or '_km' in metric_name:
        return f'{value:.1f} km'
    # Throughput metrics
    elif 'throughput' in metric_name or 'per_hour' in metric_name:
        return f'{value:.2f}/hr'
    # Count metrics
    elif 'orders' in metric_name or 'bundles' in metric_name or 'handoffs' in metric_name:
        return f'{int(value)}'
    # Bundle size
    elif 'bundle_size' in metric_name:
        return f'{value:.2f}'
    else:
        return f'{value:.2f}'

def get_best_worst(values, higher_is_better):
    """Get indices of best and worst values (handles ties)."""
    if higher_is_better:
        best_value = max(values)
        worst_value = min(values)
    else:
        best_value = min(values)
        worst_value = max(values)

    # Find all indices matching best/worst values (handles ties)
    best_indices = [i for i, v in enumerate(values) if v == best_value]
    worst_indices = [i for i, v in enumerate(values) if v == worst_value]

    return best_indices, worst_indices

def main():
    """Main execution function."""
    log("=" * 80)
    log("CREATING COMPREHENSIVE METRICS COMPARISON TABLE")
    log("=" * 80)
    log(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")

    # Load data
    json_path = '/Users/pranjal/Code/meituan/simulation_test/analysis/comparative_summary.json'
    log(f"Loading data from: {json_path}")

    with open(json_path, 'r') as f:
        data = json.load(f)

    log(f"Loaded data for {len(data)} algorithms\n")

    # Define algorithms and their display names (auto-detect from JSON)
    algorithms = list(data.keys())
    algo_display_names = {
        'greedy': 'Greedy',
        'hungarian': 'Optimal\nSingle-Order\nMatching',
        'simple_bundling': 'Single-Pickup\nBundling',
        'batched_pickups': 'Batched\nPickups',
        'relay_bundling': 'Relay\nBundling',
        'anticipated_bundling': 'Anticipated\nBundling'
    }

    # Define metrics to display, organized by category
    # Format: (metric_key, display_name, higher_is_better)
    metric_categories = {
        'Customer Metrics': [
            ('fulfillment_rate_pct', 'Fulfillment Rate', True),
            ('avg_click_to_door_time', 'Avg Click-to-Door Time', False),
            ('p90_click_to_door_time', 'P90 Click-to-Door Time', False),
            ('avg_ready_to_door_time', 'Avg Ready-to-Door Time', False),
            ('avg_pickup_wait_time', 'Avg Pickup Wait Time', False),
        ],
        'Courier Metrics': [
            ('courier_utilization_pct', 'Courier Utilization', True),
            ('avg_orders_per_courier_hour', 'Orders per Courier-Hour', True),
            ('total_distance_traveled_km', 'Total Distance Traveled', False),
        ],
        'Platform Metrics': [
            ('system_throughput_orders_per_hour', 'System Throughput', True),
            ('orders_delivered', 'Orders Delivered', True),
            ('bundles_created', 'Bundles Created', None),  # No preference
            ('avg_bundle_size', 'Avg Bundle Size', True),
        ]
    }

    # Create figure
    fig, ax = plt.subplots(figsize=(16, 12))
    ax.axis('tight')
    ax.axis('off')

    # Prepare table data
    table_data = []
    row_colors = []

    # Add title row
    header = ['Metric Category / Name'] + [algo_display_names[a] for a in algorithms]
    table_data.append(header)
    row_colors.append(['#2C3E50'] * len(header))  # Dark blue header

    # Build table rows with color coding
    cell_colors = [['#2C3E50'] * len(header)]  # Header row

    for category, metrics in metric_categories.items():
        # Add category header row
        category_row = [category] + [''] * len(algorithms)
        table_data.append(category_row)
        cell_colors.append(['#34495E'] + ['#ECF0F1'] * len(algorithms))  # Dark gray + light gray

        # Add metric rows
        for metric_key, display_name, higher_is_better in metrics:
            # Extract values
            values = []
            for algo in algorithms:
                value = data[algo]['metrics'][metric_key]
                values.append(value)

            # Format values
            formatted_values = [format_value(v, metric_key) for v in values]

            # Create row
            row = [display_name] + formatted_values
            table_data.append(row)

            # Determine cell colors (highlight best/worst, handles ties)
            row_cell_colors = ['#ECF0F1']  # Light gray for metric name

            if higher_is_better is not None:
                best_indices, worst_indices = get_best_worst(values, higher_is_better)
                for i in range(len(algorithms)):
                    if i in best_indices:
                        row_cell_colors.append('#27AE60')  # Green for best (all tied bests)
                    elif i in worst_indices:
                        row_cell_colors.append('#E74C3C')  # Red for worst (all tied worsts)
                    else:
                        row_cell_colors.append('#FFD700')  # Yellow for middle performers
            else:
                row_cell_colors.extend(['#FFFFFF'] * len(algorithms))

            cell_colors.append(row_cell_colors)

    # Create table
    table = ax.table(
        cellText=table_data,
        cellColours=cell_colors,
        cellLoc='center',
        loc='center',
        colWidths=[0.25] + [0.15] * len(algorithms)
    )

    # Style the table
    table.auto_set_font_size(False)
    table.set_fontsize(13)
    table.scale(1, 2.8)

    # Style header row
    for i in range(len(header)):
        cell = table[(0, i)]
        cell.set_text_props(weight='bold', color='white', fontsize=14)

    # Style category rows
    row_idx = 1
    for category in metric_categories:
        cell = table[(row_idx, 0)]
        cell.set_text_props(weight='bold', color='white', fontsize=13)
        row_idx += len(metric_categories[category]) + 1

    # Style metric name column
    for i in range(1, len(table_data)):
        cell = table[(i, 0)]
        cell.set_text_props(ha='left', fontsize=12)

    # Add title
    fig.suptitle('Food Delivery Algorithm Performance Comparison',
                 fontsize=18, fontweight='bold', y=0.995)

    # Add legend
    legend_elements = [
        mpatches.Patch(facecolor='#27AE60', label='Best Performance'),
        mpatches.Patch(facecolor='#E74C3C', label='Worst Performance'),
        mpatches.Patch(facecolor='#FFD700', label='Middle Performance')
    ]
    ax.legend(handles=legend_elements, loc='lower center', ncol=3,
             bbox_to_anchor=(0.5, -0.005), frameon=False, fontsize=13)

    # Save figure
    output_path = '/Users/pranjal/Code/meituan/simulation_test/algorithm_comparison_comprehensive_metrics.png'
    plt.tight_layout(rect=[0, 0, 1, 0.99])
    plt.savefig(output_path, dpi=300, bbox_inches='tight', facecolor='white')

    log(f"\nTable generated successfully!")
    log(f"Output saved to: {output_path}")

    # Log summary statistics
    log("\n" + "=" * 80)
    log("SUMMARY STATISTICS")
    log("=" * 80)

    for algo in algorithms:
        log(f"\n{algo_display_names[algo].replace(chr(10), ' ')}:")
        metrics = data[algo]['metrics']
        log(f"  Fulfillment Rate: {metrics['fulfillment_rate_pct']:.1f}%")
        log(f"  Avg Delivery Time: {metrics['avg_click_to_door_time']/60:.1f} min")
        log(f"  Orders Delivered: {metrics['orders_delivered']}")
        log(f"  Courier Utilization: {metrics['courier_utilization_pct']:.1f}%")
        log(f"  Avg Bundle Size: {metrics['avg_bundle_size']:.2f}")

    log("\n" + "=" * 80)
    log("COMPLETE")
    log("=" * 80)

    return output_path

if __name__ == '__main__':
    try:
        output_path = main()
        print(f"\n{'=' * 80}")
        print("FILES GENERATED")
        print(f"{'=' * 80}")
        print(f"PNG Table: {output_path}")
        print(f"Log File:  {log_path}")
        print(f"{'=' * 80}")
    except Exception as e:
        log(f"\nERROR: {str(e)}")
        import traceback
        log(traceback.format_exc())
        sys.exit(1)
