"""
Frame Generator

Creates individual frames for dispatch cycle visualization.
"""

import matplotlib.pyplot as plt
from datetime import datetime
import polars as pl


def generate_dispatch_frame(dispatch_time, assignments_df, mode, save_path=None, zoom_bounds=None, show_mode='all'):
    """
    Generate a single visualization frame for a dispatch moment

    Args:
        dispatch_time: Unix timestamp for this dispatch cycle
        assignments_df: Polars DataFrame with assignment log data for this dispatch
        mode: 'baseline' or 'actual' - determines which assignments to visualize
        save_path: Path to save PNG file (optional)
        zoom_bounds: Tuple of (lat_min, lat_max, lng_min, lng_max) for zooming (optional)
        show_mode: 'all' (show everything) or 'active' (fade matched pairs) - controls visual hierarchy

    Returns:
        matplotlib figure if save_path is None, otherwise None
    """
    fig, ax = plt.subplots(figsize=(12, 10))

    # Filter to only assigned orders for this mode
    if mode == 'baseline':
        # Show baseline assignments
        assigned = assignments_df.filter(pl.col('is_assigned_by_baseline') == True)
        unassigned = assignments_df.filter(pl.col('is_assigned_by_baseline') == False)

        if show_mode == 'active':
            # ACTIVE MODE: Fade matched pairs, highlight unmatched

            # Plot MATCHED couriers (faded red triangles)
            if assigned.shape[0] > 0:
                courier_lats = assigned['baseline_courier_lat'].drop_nulls().to_list()
                courier_lngs = assigned['baseline_courier_lng'].drop_nulls().to_list()
                if courier_lats and courier_lngs:
                    ax.scatter(courier_lats, courier_lngs, c='lightcoral', marker='^', s=15, alpha=0.25,
                              label=f'Matched Couriers ({len(courier_lats)})', zorder=1)

            # Plot MATCHED orders (faded blue circles)
            if assigned.shape[0] > 0:
                matched_order_lats = assigned['order_pickup_lat'].drop_nulls().to_list()
                matched_order_lngs = assigned['order_pickup_lng'].drop_nulls().to_list()
                if matched_order_lats and matched_order_lngs:
                    ax.scatter(matched_order_lats, matched_order_lngs, c='lightblue', marker='o', s=20, alpha=0.25,
                              label=f'Matched Orders ({len(matched_order_lats)})', zorder=1)

            # Plot UNMATCHED orders (bright, prominent)
            if unassigned.shape[0] > 0:
                unmatched_order_lats = unassigned['order_pickup_lat'].drop_nulls().to_list()
                unmatched_order_lngs = unassigned['order_pickup_lng'].drop_nulls().to_list()
                if unmatched_order_lats and unmatched_order_lngs:
                    ax.scatter(unmatched_order_lats, unmatched_order_lngs, c='blue', marker='o', s=80, alpha=0.9,
                              label=f'Unmatched Orders ({len(unmatched_order_lats)})', zorder=3, edgecolors='navy', linewidths=1.5)

            # Draw assignment lines (faded gray)
            for row in assigned.iter_rows(named=True):
                if (row['baseline_courier_lat'] is not None and
                    row['baseline_courier_lng'] is not None and
                    row['order_pickup_lat'] is not None and
                    row['order_pickup_lng'] is not None):
                    ax.plot([row['baseline_courier_lat'], row['order_pickup_lat']],
                           [row['baseline_courier_lng'], row['order_pickup_lng']],
                           'gray', alpha=0.15, linewidth=0.3, zorder=0)

        else:
            # ALL MODE: Show everything equally (original behavior)

            # Plot available couriers (red triangles)
            if assigned.shape[0] > 0:
                courier_lats = assigned['baseline_courier_lat'].drop_nulls().to_list()
                courier_lngs = assigned['baseline_courier_lng'].drop_nulls().to_list()
                if courier_lats and courier_lngs:
                    ax.scatter(courier_lats, courier_lngs, c='red', marker='^', s=30, alpha=0.6,
                              label=f'Available Couriers ({len(courier_lats)})')

            # Plot waiting orders (blue circles)
            order_lats = assignments_df['order_pickup_lat'].drop_nulls().to_list()
            order_lngs = assignments_df['order_pickup_lng'].drop_nulls().to_list()
            if order_lats and order_lngs:
                ax.scatter(order_lats, order_lngs, c='blue', marker='o', s=40, alpha=0.7,
                          label=f'Waiting Orders ({len(order_lats)})')

            # Draw assignment lines (gray)
            for row in assigned.iter_rows(named=True):
                if (row['baseline_courier_lat'] is not None and
                    row['baseline_courier_lng'] is not None and
                    row['order_pickup_lat'] is not None and
                    row['order_pickup_lng'] is not None):
                    ax.plot([row['baseline_courier_lat'], row['order_pickup_lat']],
                           [row['baseline_courier_lng'], row['order_pickup_lng']],
                           'gray', alpha=0.3, linewidth=0.5)

        title_prefix = "Baseline Assignments"
        num_assignments = assigned.shape[0]

    elif mode == 'actual':
        # Show actual assignments
        assigned = assignments_df.filter(pl.col('actual_assigned_courier_id').is_not_null())

        # Plot all historical couriers (gray triangles)
        if assigned.shape[0] > 0:
            courier_lats = assigned['actual_courier_lat'].drop_nulls().to_list()
            courier_lngs = assigned['actual_courier_lng'].drop_nulls().to_list()
            if courier_lats and courier_lngs:
                ax.scatter(courier_lats, courier_lngs, c='gray', marker='^', s=30, alpha=0.4,
                          label=f'All Couriers ({len(courier_lats)})')

        # Plot waiting orders (blue circles)
        order_lats = assignments_df['order_pickup_lat'].drop_nulls().to_list()
        order_lngs = assignments_df['order_pickup_lng'].drop_nulls().to_list()
        if order_lats and order_lngs:
            ax.scatter(order_lats, order_lngs, c='blue', marker='o', s=40, alpha=0.7,
                      label=f'Waiting Orders ({len(order_lats)})')

        # Draw assignment lines (purple)
        for row in assigned.iter_rows(named=True):
            if (row['actual_courier_lat'] is not None and
                row['actual_courier_lng'] is not None and
                row['order_pickup_lat'] is not None and
                row['order_pickup_lng'] is not None):
                ax.plot([row['actual_courier_lat'], row['order_pickup_lat']],
                       [row['actual_courier_lng'], row['order_pickup_lng']],
                       'purple', alpha=0.7, linewidth=0.5)

        title_prefix = "Actual (Meituan) Assignments"
        num_assignments = assigned.shape[0]

    else:
        raise ValueError(f"Invalid mode: {mode}. Must be 'baseline' or 'actual'")

    # Format plot
    timestamp_str = datetime.fromtimestamp(dispatch_time).strftime('%H:%M:%S')
    ax.set_title(f'{title_prefix}\nDispatch @ {timestamp_str}\n'
                f'{assignments_df.shape[0]} Orders, {num_assignments} Assignments',
                fontsize=14)
    ax.set_xlabel('X Coordinate (shifted grid)', fontsize=12)
    ax.set_ylabel('Y Coordinate (shifted grid)', fontsize=12)
    ax.legend(loc='best', fontsize=10)
    ax.grid(True, alpha=0.3)

    # Apply zoom if specified
    if zoom_bounds:
        lat_min, lat_max, lng_min, lng_max = zoom_bounds
        ax.set_xlim(lat_min, lat_max)
        ax.set_ylim(lng_min, lng_max)

    plt.tight_layout()

    if save_path:
        plt.savefig(save_path, dpi=120, bbox_inches='tight')
        plt.close(fig)
        return None
    else:
        return fig


def generate_comparison_frame(dispatch_time, assignments_df, save_path=None, zoom_bounds=None, show_mode='all'):
    """
    Generate a side-by-side comparison frame (baseline vs actual)

    Args:
        dispatch_time: Unix timestamp for this dispatch cycle
        assignments_df: Polars DataFrame with assignment log data for this dispatch
        save_path: Path to save PNG file (optional)
        zoom_bounds: Tuple of (lat_min, lat_max, lng_min, lng_max) for zooming (optional)
        show_mode: 'all' (show everything) or 'active' (fade matched pairs) - controls visual hierarchy

    Returns:
        matplotlib figure if save_path is None, otherwise None
    """
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(24, 10))

    # Left subplot: Baseline
    plt.sca(ax1)
    _plot_single_mode(ax1, dispatch_time, assignments_df, 'baseline', zoom_bounds, show_mode)

    # Right subplot: Actual
    plt.sca(ax2)
    _plot_single_mode(ax2, dispatch_time, assignments_df, 'actual', zoom_bounds, show_mode)

    plt.tight_layout()

    if save_path:
        plt.savefig(save_path, dpi=120, bbox_inches='tight')
        plt.close(fig)
        return None
    else:
        return fig


def _plot_single_mode(ax, dispatch_time, assignments_df, mode, zoom_bounds=None, show_mode='all'):
    """
    Internal helper to plot a single mode on a given axes

    Args:
        ax: matplotlib axes object
        dispatch_time: Unix timestamp
        assignments_df: Polars DataFrame
        mode: 'baseline' or 'actual'
        zoom_bounds: Tuple of (lat_min, lat_max, lng_min, lng_max) for zooming (optional)
        show_mode: 'all' (show everything) or 'active' (fade matched pairs) - controls visual hierarchy
    """
    if mode == 'baseline':
        assigned = assignments_df.filter(pl.col('is_assigned_by_baseline') == True)
        unassigned = assignments_df.filter(pl.col('is_assigned_by_baseline') == False)

        if show_mode == 'active':
            # ACTIVE MODE: Fade matched pairs, highlight unmatched

            # Plot MATCHED couriers (faded)
            if assigned.shape[0] > 0:
                courier_lats = assigned['baseline_courier_lat'].drop_nulls().to_list()
                courier_lngs = assigned['baseline_courier_lng'].drop_nulls().to_list()
                if courier_lats and courier_lngs:
                    ax.scatter(courier_lats, courier_lngs, c='lightcoral', marker='^', s=15, alpha=0.25,
                              label=f'Matched Couriers ({len(courier_lats)})', zorder=1)

            # Plot MATCHED orders (faded)
            if assigned.shape[0] > 0:
                matched_order_lats = assigned['order_pickup_lat'].drop_nulls().to_list()
                matched_order_lngs = assigned['order_pickup_lng'].drop_nulls().to_list()
                if matched_order_lats and matched_order_lngs:
                    ax.scatter(matched_order_lats, matched_order_lngs, c='lightblue', marker='o', s=20, alpha=0.25,
                              label=f'Matched Orders ({len(matched_order_lats)})', zorder=1)

            # Plot UNMATCHED orders (bright, prominent)
            if unassigned.shape[0] > 0:
                unmatched_order_lats = unassigned['order_pickup_lat'].drop_nulls().to_list()
                unmatched_order_lngs = unassigned['order_pickup_lng'].drop_nulls().to_list()
                if unmatched_order_lats and unmatched_order_lngs:
                    ax.scatter(unmatched_order_lats, unmatched_order_lngs, c='blue', marker='o', s=80, alpha=0.9,
                              label=f'Unmatched Orders ({len(unmatched_order_lats)})', zorder=3, edgecolors='navy', linewidths=1.5)

            # Draw assignment lines (faded)
            for row in assigned.iter_rows(named=True):
                if (row['baseline_courier_lat'] is not None and
                    row['baseline_courier_lng'] is not None):
                    ax.plot([row['baseline_courier_lat'], row['order_pickup_lat']],
                           [row['baseline_courier_lng'], row['order_pickup_lng']],
                           'gray', alpha=0.15, linewidth=0.3, zorder=0)

        else:
            # ALL MODE: Show everything equally

            # Plot couriers
            if assigned.shape[0] > 0:
                courier_lats = assigned['baseline_courier_lat'].drop_nulls().to_list()
                courier_lngs = assigned['baseline_courier_lng'].drop_nulls().to_list()
                if courier_lats and courier_lngs:
                    ax.scatter(courier_lats, courier_lngs, c='red', marker='^', s=30, alpha=0.6,
                              label=f'Available Couriers ({len(courier_lats)})')

            # Plot orders
            order_lats = assignments_df['order_pickup_lat'].drop_nulls().to_list()
            order_lngs = assignments_df['order_pickup_lng'].drop_nulls().to_list()
            if order_lats and order_lngs:
                ax.scatter(order_lats, order_lngs, c='blue', marker='o', s=40, alpha=0.7,
                          label=f'Waiting Orders ({len(order_lats)})')

            # Draw assignment lines
            for row in assigned.iter_rows(named=True):
                if (row['baseline_courier_lat'] is not None and
                    row['baseline_courier_lng'] is not None):
                    ax.plot([row['baseline_courier_lat'], row['order_pickup_lat']],
                           [row['baseline_courier_lng'], row['order_pickup_lng']],
                           'gray', alpha=0.3, linewidth=0.5)

        title_prefix = "Baseline Assignments"
        num_assignments = assigned.shape[0]

    else:  # actual
        assigned = assignments_df.filter(pl.col('actual_assigned_courier_id').is_not_null())
        unassigned = assignments_df.filter(pl.col('actual_assigned_courier_id').is_null())

        if show_mode == 'active':
            # ACTIVE MODE: Fade matched pairs, highlight unmatched

            # Plot MATCHED couriers (faded)
            if assigned.shape[0] > 0:
                courier_lats = assigned['actual_courier_lat'].drop_nulls().to_list()
                courier_lngs = assigned['actual_courier_lng'].drop_nulls().to_list()
                if courier_lats and courier_lngs:
                    ax.scatter(courier_lats, courier_lngs, c='lightgray', marker='^', s=15, alpha=0.25,
                              label=f'Matched Couriers ({len(courier_lats)})', zorder=1)

            # Plot MATCHED orders (faded)
            if assigned.shape[0] > 0:
                matched_order_lats = assigned['order_pickup_lat'].drop_nulls().to_list()
                matched_order_lngs = assigned['order_pickup_lng'].drop_nulls().to_list()
                if matched_order_lats and matched_order_lngs:
                    ax.scatter(matched_order_lats, matched_order_lngs, c='plum', marker='o', s=20, alpha=0.3,
                              label=f'Matched Orders ({len(matched_order_lats)})', zorder=1)

            # Plot UNMATCHED orders (bright, prominent)
            if unassigned.shape[0] > 0:
                unmatched_order_lats = unassigned['order_pickup_lat'].drop_nulls().to_list()
                unmatched_order_lngs = unassigned['order_pickup_lng'].drop_nulls().to_list()
                if unmatched_order_lats and unmatched_order_lngs:
                    ax.scatter(unmatched_order_lats, unmatched_order_lngs, c='blue', marker='o', s=80, alpha=0.9,
                              label=f'Unmatched Orders ({len(unmatched_order_lats)})', zorder=3, edgecolors='navy', linewidths=1.5)

            # Draw assignment lines (faded)
            for row in assigned.iter_rows(named=True):
                if (row['actual_courier_lat'] is not None and
                    row['actual_courier_lng'] is not None):
                    ax.plot([row['actual_courier_lat'], row['order_pickup_lat']],
                           [row['actual_courier_lng'], row['order_pickup_lng']],
                           'purple', alpha=0.2, linewidth=0.3, zorder=0)

        else:
            # ALL MODE: Show everything equally

            # Plot couriers
            if assigned.shape[0] > 0:
                courier_lats = assigned['actual_courier_lat'].drop_nulls().to_list()
                courier_lngs = assigned['actual_courier_lng'].drop_nulls().to_list()
                if courier_lats and courier_lngs:
                    ax.scatter(courier_lats, courier_lngs, c='gray', marker='^', s=30, alpha=0.4,
                              label=f'All Couriers ({len(courier_lats)})')

            # Plot orders
            order_lats = assignments_df['order_pickup_lat'].drop_nulls().to_list()
            order_lngs = assignments_df['order_pickup_lng'].drop_nulls().to_list()
            if order_lats and order_lngs:
                ax.scatter(order_lats, order_lngs, c='blue', marker='o', s=40, alpha=0.7,
                          label=f'Waiting Orders ({len(order_lats)})')

            # Draw assignment lines
            for row in assigned.iter_rows(named=True):
                if (row['actual_courier_lat'] is not None and
                    row['actual_courier_lng'] is not None):
                    ax.plot([row['actual_courier_lat'], row['order_pickup_lat']],
                           [row['actual_courier_lng'], row['order_pickup_lng']],
                           'purple', alpha=0.7, linewidth=0.5)

        title_prefix = "Actual (Meituan) Assignments"
        num_assignments = assigned.shape[0]

    # Format subplot
    timestamp_str = datetime.fromtimestamp(dispatch_time).strftime('%H:%M:%S')
    ax.set_title(f'{title_prefix}\nDispatch @ {timestamp_str}\n'
                f'{assignments_df.shape[0]} Orders, {num_assignments} Assignments',
                fontsize=14)
    ax.set_xlabel('X Coordinate (shifted grid)', fontsize=12)
    ax.set_ylabel('Y Coordinate (shifted grid)', fontsize=12)
    ax.legend(loc='best', fontsize=10)
    ax.grid(True, alpha=0.3)

    # Apply zoom if specified
    if zoom_bounds:
        lat_min, lat_max, lng_min, lng_max = zoom_bounds
        ax.set_xlim(lat_min, lat_max)
        ax.set_ylim(lng_min, lng_max)
