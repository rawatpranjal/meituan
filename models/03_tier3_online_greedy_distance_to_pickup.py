"""
Online Greedy Model (Tier 3)
First-Come First-Served Greedy Assignment
Cost Function: Distance to Pickup

This model processes orders greedily within each batch,
assigning each order immediately to the closest available courier.
"""

import polars as pl
import numpy as np
import matplotlib.pyplot as plt
import sys
import os
import random
from datetime import datetime

# Import simulator modules
sys.path.insert(0, '/Users/pranjal/Code/meituan/models/simulator')
from physics import AVERAGE_TASK_DURATION, GLOBAL_REJECTION_PROBABILITY
from state import (initialize_courier_states, get_available_couriers,
                   update_courier_after_assignment, get_courier_state_summary)
from assignment_strategy import Tier3OnlineGreedy
from logger import SimulationLogger
from courier_timeline_logger import CourierTimelineLogger

# Import cost function module
sys.path.insert(0, '/Users/pranjal/Code/meituan/models')
from cost import DistanceToPickup

# ============================================================================
# CONFIGURATION
# ============================================================================

DATA_PATH = "/Users/pranjal/Code/meituan/data/INFORMS.org/meituan_informs_data/"
LOGS_DIR = "/Users/pranjal/Code/meituan/models/logs"
MODEL_NAME = "03_tier3_online_greedy_distance_to_pickup"

random.seed(42)
np.random.seed(42)

# Setup stdout logging
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
log_file = f"{LOGS_DIR}/{MODEL_NAME}_execution_{timestamp}.log"
log = open(log_file, 'w')
sys.stdout = log

print("="*80)
print("ONLINE GREEDY MODEL (TIER 3) - First-Come First-Served")
print("="*80)

# ============================================================================
# VISUALIZATION FUNCTIONS
# ============================================================================

def create_playbook_visualization(dispatch_time, waiting_orders, available_couriers,
                                   assignments, waybill_lookup, save_path):
    """
    Create 2D scatter plot showing courier-order assignments (Playbook View)

    Args:
        dispatch_time: Unix timestamp
        waiting_orders: List of order dicts
        available_couriers: List of courier dicts
        assignments: List of (order, courier, cost) tuples
        waybill_lookup: Order location lookup
        save_path: Path to save PNG
    """
    plt.figure(figsize=(12, 10))

    # Plot all available couriers (red triangles)
    courier_x = [c['rider_lat'] for c in available_couriers]
    courier_y = [c['rider_lng'] for c in available_couriers]
    plt.scatter(courier_x, courier_y, c='red', marker='^', s=30, alpha=0.6,
               label=f'Available Couriers ({len(available_couriers)})')

    # Plot all waiting orders at restaurant locations (blue circles)
    order_x = []
    order_y = []
    for o in waiting_orders:
        if o['order_id'] in waybill_lookup:
            order_x.append(waybill_lookup[o['order_id']]['sender_lat'])
            order_y.append(waybill_lookup[o['order_id']]['sender_lng'])
    plt.scatter(order_x, order_y, c='blue', marker='o', s=40, alpha=0.7,
               label=f'Waiting Orders ({len(waiting_orders)})')

    # Draw assignment connections (gray lines)
    for order, courier, cost in assignments:
        if order['order_id'] in waybill_lookup:
            order_loc_x = waybill_lookup[order['order_id']]['sender_lat']
            order_loc_y = waybill_lookup[order['order_id']]['sender_lng']
            courier_loc_x = courier['rider_lat']
            courier_loc_y = courier['rider_lng']

            plt.plot([courier_loc_x, order_loc_x], [courier_loc_y, order_loc_y],
                    'gray', alpha=0.3, linewidth=0.5)

    # Format plot
    plt.xlabel('X Coordinate (shifted grid)', fontsize=12)
    plt.ylabel('Y Coordinate (shifted grid)', fontsize=12)
    plt.title(f'Playbook View - Dispatch Moment {datetime.fromtimestamp(dispatch_time)}\n'
              f'{len(waiting_orders)} Orders, {len(available_couriers)} Couriers, '
              f'{len(assignments)} Assignments', fontsize=14)
    plt.legend(loc='best', fontsize=10)
    plt.grid(True, alpha=0.3)
    plt.tight_layout()

    plt.savefig(save_path, dpi=150, bbox_inches='tight')
    plt.close()

    print(f"  Saved playbook visualization: {save_path}")


def create_journey_detail_visualization(dispatch_time, waiting_orders, available_couriers,
                                        assignments, waybill_lookup, save_path, sample_size=15):
    """
    Create detailed view showing complete journey paths for a sample of assignments

    Shows L-shaped paths: Courier → Pickup (Restaurant) → Delivery (Customer)
    Samples from dense spatial regions for better visualization
    """
    if not assignments:
        print("  No assignments to visualize")
        return

    # Find dense region for sampling
    pickup_locations = []
    valid_assignment_indices = []

    for idx, (order, courier, cost) in enumerate(assignments):
        order_id = order['order_id']
        if order_id in waybill_lookup:
            pickup_x = waybill_lookup[order_id]['sender_lat']
            pickup_y = waybill_lookup[order_id]['sender_lng']
            pickup_locations.append((pickup_x, pickup_y))
            valid_assignment_indices.append(idx)

    if not pickup_locations:
        print(f"  Warning: No valid assignments found for visualization")
        return

    # Find the center of mass of all pickups
    pickup_array = np.array(pickup_locations)
    center_x = np.median(pickup_array[:, 0])
    center_y = np.median(pickup_array[:, 1])

    # Calculate distances from each pickup to the center
    distances_to_center = []
    for i, (px, py) in enumerate(pickup_locations):
        dist = np.sqrt((px - center_x)**2 + (py - center_y)**2)
        distances_to_center.append((dist, valid_assignment_indices[i]))

    # Sort by distance and take the closest ones (densest region)
    distances_to_center.sort(key=lambda x: x[0])
    n_assignments = min(sample_size, len(distances_to_center))
    sample_indices = [idx for _, idx in distances_to_center[:n_assignments]]

    plt.figure(figsize=(14, 12))

    # Collect coordinates for zoom calculation
    all_x = []
    all_y = []

    # For each sampled assignment, draw the complete journey
    for idx in sample_indices:
        order, courier, cost = assignments[idx]
        order_id = order['order_id']

        if order_id not in waybill_lookup:
            continue

        # Get locations
        courier_x = courier['rider_lat']
        courier_y = courier['rider_lng']

        pickup_x = waybill_lookup[order_id]['sender_lat']
        pickup_y = waybill_lookup[order_id]['sender_lng']

        delivery_x = waybill_lookup[order_id]['recipient_lat']
        delivery_y = waybill_lookup[order_id]['recipient_lng']

        # Collect for zoom
        all_x.extend([courier_x, pickup_x, delivery_x])
        all_y.extend([courier_y, pickup_y, delivery_y])

        # Draw journey path (L-shaped)
        # Courier → Pickup (solid line)
        plt.plot([courier_x, pickup_x], [courier_y, pickup_y],
                'b-', alpha=0.6, linewidth=2, label='To Pickup' if idx == sample_indices[0] else '')

        # Pickup → Delivery (dashed line)
        plt.plot([pickup_x, delivery_x], [pickup_y, delivery_y],
                'g--', alpha=0.6, linewidth=2, label='To Delivery' if idx == sample_indices[0] else '')

        # Plot points
        plt.scatter(courier_x, courier_y, c='red', marker='^', s=150,
                   edgecolors='black', linewidths=1, zorder=5)
        plt.scatter(pickup_x, pickup_y, c='blue', marker='o', s=150,
                   edgecolors='black', linewidths=1, zorder=5)
        plt.scatter(delivery_x, delivery_y, c='green', marker='s', s=150,
                   edgecolors='black', linewidths=1, zorder=5)

    # Add legend markers (only once)
    plt.scatter([], [], c='red', marker='^', s=150, edgecolors='black', linewidths=1, label='Courier Start')
    plt.scatter([], [], c='blue', marker='o', s=150, edgecolors='black', linewidths=1, label='Pickup (Restaurant)')
    plt.scatter([], [], c='green', marker='s', s=150, edgecolors='black', linewidths=1, label='Delivery (Customer)')

    # Zoom to sample region with padding
    if all_x and all_y:
        x_margin = (max(all_x) - min(all_x)) * 0.1
        y_margin = (max(all_y) - min(all_y)) * 0.1
        plt.xlim(min(all_x) - x_margin, max(all_x) + x_margin)
        plt.ylim(min(all_y) - y_margin, max(all_y) + y_margin)

    # Format
    plt.xlabel('X Coordinate', fontsize=13, fontweight='bold')
    plt.ylabel('Y Coordinate', fontsize=13, fontweight='bold', rotation=0,
               ha='right', va='center', labelpad=30)
    plt.title(f'Journey Detail Visualization - Dense Region Sample\n'
              f'{n_assignments} Assignments from Dispatch {datetime.fromtimestamp(dispatch_time).strftime("%H:%M:%S")}',
              fontsize=14, fontweight='bold', pad=15)
    plt.legend(loc='upper right', fontsize=11, framealpha=0.95, edgecolor='black')
    plt.grid(True, alpha=0.25, linestyle='--')
    plt.tight_layout()

    # Save with higher DPI for better quality
    plt.savefig(save_path, dpi=200, bbox_inches='tight')
    plt.close()

    print(f"  Saved journey detail visualization: {save_path}")


# ============================================================================
# MAIN SIMULATION
# ============================================================================

def run_simulation_with_viz():
    """
    Run the stateful simulation using simulator modules,
    with visualization generation for first 3 dispatch moments
    """
    # --- 1. SETUP & DATA LOADING ---
    print("\n[SECTION 1] DATA LOADING")
    print("-"*80)

    waybill = pl.read_csv(f"{DATA_PATH}all_waybill_info_meituan_0322.csv")
    dispatch_rider = pl.read_csv(f"{DATA_PATH}dispatch_rider_meituan.csv")
    dispatch_waybill = pl.read_csv(f"{DATA_PATH}dispatch_waybill_meituan.csv")

    print(f"Loaded {waybill.shape[0]:,} waybills")
    print(f"Loaded {dispatch_rider.shape[0]:,} dispatch rider records")
    print(f"Loaded {dispatch_waybill.shape[0]:,} dispatch waybill records")

    # Build waybill lookup dictionary
    print("\nBuilding waybill lookup dictionary...")
    waybill_subset = waybill.select(['order_id', 'sender_lat', 'sender_lng',
                                      'recipient_lat', 'recipient_lng', 'courier_id',
                                      'platform_order_time'])
    order_ids = waybill_subset['order_id'].to_list()
    sender_lats = waybill_subset['sender_lat'].to_list()
    sender_lngs = waybill_subset['sender_lng'].to_list()
    recipient_lats = waybill_subset['recipient_lat'].to_list()
    recipient_lngs = waybill_subset['recipient_lng'].to_list()
    actual_courier_ids = waybill_subset['courier_id'].to_list()
    platform_order_times = waybill_subset['platform_order_time'].to_list()

    waybill_lookup = {
        order_ids[i]: {
            'sender_lat': sender_lats[i],
            'sender_lng': sender_lngs[i],
            'recipient_lat': recipient_lats[i],
            'recipient_lng': recipient_lngs[i],
            'actual_courier_id': actual_courier_ids[i],
            'platform_order_time': platform_order_times[i]
        }
        for i in range(len(order_ids))
    }
    print(f"Lookup contains {len(waybill_lookup):,} orders")

    # --- 2. INITIALIZE SIMULATION STATE ---
    print("\n[SECTION 2] INITIALIZATION")
    print("-"*80)

    # Get sorted list of dispatch times
    dispatch_times = dispatch_waybill['dispatch_time'].unique().sort().to_list()
    print(f"Total dispatch moments to process: {len(dispatch_times)}")

    # Initialize cost function
    cost_function = DistanceToPickup()
    print(f"Cost function: {cost_function.get_name()} - {cost_function.get_description()}")

    # Initialize assignment strategy
    assignment_strategy = Tier3OnlineGreedy(cost_function)
    print(f"Assignment strategy: Tier 3 Online Greedy (FCFS)")

    # Initialize loggers
    logger = SimulationLogger(LOGS_DIR, MODEL_NAME, cost_function.get_name())
    timeline_logger = CourierTimelineLogger(LOGS_DIR, MODEL_NAME)
    print(f"\nMetrics logging:")
    print(f"  Assignment log: {logger.assignment_log_path}")
    print(f"  Cycle summary: {logger.cycle_summary_path}")
    print(f"  Courier timeline: {timeline_logger.get_log_path()}")

    # Initialize courier states from first dispatch moment
    first_dispatch_time = dispatch_times[0]
    first_dispatch_couriers = dispatch_rider.filter(
        pl.col('dispatch_time') == first_dispatch_time
    ).to_dicts()

    courier_states = initialize_courier_states(first_dispatch_couriers, waybill_lookup, timeline_logger)
    print(f"Initialized {len(courier_states):,} couriers")

    # Physics constants
    print(f"\nPhysics constants (calibrated from historical data):")
    print(f"  AVERAGE_TASK_DURATION = {AVERAGE_TASK_DURATION}s ({AVERAGE_TASK_DURATION/60:.1f} min)")
    print(f"  GLOBAL_REJECTION_PROBABILITY = {GLOBAL_REJECTION_PROBABILITY:.4f}")

    # --- 3. MAIN DISCRETE-TIME LOOP ---
    print("\n" + "="*80)
    print("[SECTION 3] RUNNING ASSIGNMENT ALGORITHM")
    print("="*80)

    total_orders_processed = 0
    total_assignments_proposed = 0
    total_assignments_accepted = 0
    total_rejections = 0

    for dispatch_idx, dispatch_time in enumerate(dispatch_times):
        print(f"\n[Dispatch {dispatch_idx+1}/{len(dispatch_times)}] Time: {dispatch_time} ({datetime.fromtimestamp(dispatch_time)})")

        # --- A. GET WAITING ORDERS ---
        waiting_orders_df = dispatch_waybill.filter(pl.col('dispatch_time') == dispatch_time)
        waiting_orders = waiting_orders_df.to_dicts()
        n_orders = len(waiting_orders)
        total_orders_processed += n_orders

        # --- B. GET AVAILABLE COURIERS ---
        available_couriers = get_available_couriers(dispatch_time, courier_states, timeline_logger)
        n_couriers = len(available_couriers)

        # Build courier location lookup for this dispatch moment (for logging actual assignments)
        dispatch_couriers_df = dispatch_rider.filter(pl.col('dispatch_time') == dispatch_time)
        courier_location_lookup = {
            row['courier_id']: (row['rider_lat'], row['rider_lng'])
            for row in dispatch_couriers_df.select(['courier_id', 'rider_lat', 'rider_lng']).iter_rows(named=True)
        }

        print(f"  Waiting orders: {n_orders}")
        print(f"  Available couriers: {n_couriers}")

        if n_orders == 0 or n_couriers == 0:
            print("  Skipping (no orders or no couriers)")
            continue

        # --- C. MAKE ASSIGNMENTS ---
        print(f"  Building cost matrix ({n_orders} x {n_couriers})...")
        print(f"  Solving assignment...")
        proposed_assignments = assignment_strategy.make_assignments(
            waiting_orders, available_couriers, waybill_lookup
        )
        n_proposed = len(proposed_assignments)
        total_assignments_proposed += n_proposed

        # --- D. GENERATE VISUALIZATIONS (First 3 dispatches) ---
        if dispatch_idx < 3:
            viz_filename = f"{LOGS_DIR}/{MODEL_NAME}_playbook_dispatch_{dispatch_idx+1}_{dispatch_time}.png"
            create_playbook_visualization(
                dispatch_time, waiting_orders, available_couriers,
                proposed_assignments, waybill_lookup, viz_filename
            )

            journey_filename = f"{LOGS_DIR}/{MODEL_NAME}_journey_dispatch_{dispatch_idx+1}_{dispatch_time}.png"
            create_journey_detail_visualization(
                dispatch_time, waiting_orders, available_couriers,
                proposed_assignments, waybill_lookup, journey_filename, sample_size=15
            )

        # --- E. PROCESS ASSIGNMENTS & REJECTIONS ---
        accepted_assignments = []
        rejected_assignments = []
        cycle_total_cost = 0

        for order, courier, cost in proposed_assignments:
            # Simulate courier acceptance/rejection
            if random.random() < GLOBAL_REJECTION_PROBABILITY:
                # Rejected!
                rejected_assignments.append((order, courier, cost))
                total_rejections += 1
            else:
                # Accepted!
                accepted_assignments.append((order, courier, cost))
                cycle_total_cost += cost
                total_assignments_accepted += 1

                # Update courier state
                order_id = order['order_id']
                delivery_location = (
                    waybill_lookup[order_id]['recipient_lat'],
                    waybill_lookup[order_id]['recipient_lng']
                )
                update_courier_after_assignment(
                    courier['courier_id'], courier_states, dispatch_time,
                    delivery_location, AVERAGE_TASK_DURATION, timeline_logger
                )

        n_accepted = len(accepted_assignments)
        n_rejected = len(rejected_assignments)

        print(f"  Assignments made: {n_accepted}/{n_orders}")
        print(f"  Total distance to pickups: {cycle_total_cost:.0f} grid units")
        if n_accepted > 0:
            print(f"  Avg distance per assignment: {cycle_total_cost/n_accepted:.0f} grid units")

        # Show sample assignments
        if n_accepted > 0:
            print(f"  Sample assignments:")
            for i in range(min(3, n_accepted)):
                order, courier, cost = accepted_assignments[i]
                print(f"    Order {order['order_id']} → Courier {courier['courier_id']} (distance: {cost:.0f} grid units)")

        # --- F. LOG METRICS ---
        assigned_order_ids = {a[0]['order_id'] for a in accepted_assignments}
        proposed_order_ids = {a[0]['order_id'] for a in proposed_assignments}
        num_matches = 0

        for order in waiting_orders:
            order_id = order['order_id']

            if order_id not in waybill_lookup:
                continue

            pickup_lat = waybill_lookup[order_id]['sender_lat']
            pickup_lng = waybill_lookup[order_id]['sender_lng']
            actual_courier_id = waybill_lookup[order_id].get('actual_courier_id')
            platform_order_time = waybill_lookup[order_id].get('platform_order_time')

            # Find if this order was in our assignments
            courier = None
            cost = None
            rank = None
            is_assigned = False
            was_accepted = False

            # Check if proposed
            for o, c, cst in proposed_assignments:
                if o['order_id'] == order_id:
                    courier = c
                    cost = cst
                    is_assigned = True
                    was_accepted = order_id in assigned_order_ids
                    rank = None  # TODO: Calculate rank
                    break

            is_match = (courier and was_accepted and
                       courier['courier_id'] == actual_courier_id)
            if is_match:
                num_matches += 1

            # Get courier locations for logging
            baseline_courier_lat = courier['rider_lat'] if courier else None
            baseline_courier_lng = courier['rider_lng'] if courier else None

            actual_courier_lat = None
            actual_courier_lng = None
            if actual_courier_id and actual_courier_id in courier_location_lookup:
                actual_courier_lat, actual_courier_lng = courier_location_lookup[actual_courier_id]

            logger.log_assignment(
                dispatch_time, order, courier, cost, rank,
                is_assigned, was_accepted, actual_courier_id, is_match,
                n_orders, n_couriers, pickup_lat, pickup_lng, platform_order_time,
                baseline_courier_lat, baseline_courier_lng,
                actual_courier_lat, actual_courier_lng
            )

        # Log cycle summary
        agreement_rate = num_matches / n_orders if n_orders > 0 else 0
        logger.log_cycle_summary(
            dispatch_time, n_orders, n_couriers,
            n_proposed, n_accepted, n_rejected,
            cycle_total_cost, agreement_rate
        )

        logger.flush()

        print(f"  Updated availability for {n_accepted} assigned couriers")

    # --- 4. FINAL SUMMARY ---
    print("\n" + "="*80)
    print("[SECTION 4] BASELINE PERFORMANCE EVALUATION")
    print("="*80)

    print("\n[4.1] Assignment Metrics")
    print("-"*80)
    print(f"Total waiting orders: {total_orders_processed:,}")
    print(f"Total assignments proposed: {total_assignments_proposed:,}")
    print(f"Total assignments accepted: {total_assignments_accepted:,}")
    print(f"Total rejections: {total_rejections:,}")
    print(f"Assignment rate: {total_assignments_accepted/total_orders_processed*100:.2f}%")
    print(f"Acceptance rate: {total_assignments_accepted/total_assignments_proposed*100:.2f}%" if total_assignments_proposed > 0 else "N/A")

    print("\n" + "="*80)
    print("ONLINE GREEDY MODEL COMPLETE (TIER 3)")
    print("="*80)

    print("\nKEY FINDINGS:")
    print("- Tier 3: Greedy First-Come First-Served assignment")
    print("- Processes orders within batch greedily (no optimization)")
    print("- Cost = Euclidean distance from courier to restaurant")
    print("- Each order assigned to closest available courier immediately")
    print("- Stateful simulation with calibrated physics")
    print("- Courier rejection model included (13.11% rejection rate)")
    print("- Distance in grid units (coordinates shifted but not scaled)")

    print("\nVISUALIZATIONS:")
    print("Playbook Views (all assignments):")
    print(f"  - {MODEL_NAME}_playbook_dispatch_1_*.png, _2_*.png, _3_*.png")
    print("  - Red triangles: Available couriers")
    print("  - Blue circles: Waiting orders (at restaurant pickup locations)")
    print("  - Gray lines: Assignment connections (courier → order)")
    print("\nJourney Details (sample L-shaped paths):")
    print(f"  - {MODEL_NAME}_journey_dispatch_1_*.png, _2_*.png, _3_*.png")
    print("  - Shows complete path: Courier (red △) → Pickup (blue ○) → Delivery (green □)")
    print("  - Blue solid lines: Courier to pickup, Green dashed lines: Pickup to delivery")
    print("  - Zoomed to ~15 sampled assignments for clarity")

    # Close loggers
    logger.close()
    timeline_logger.close()
    log_paths = logger.get_log_paths()
    timeline_path = timeline_logger.get_log_path()

    # Close stdout log
    sys.stdout = sys.__stdout__
    log.close()

    print(f"\nLog saved to: {log_file}")
    print(f"Assignment log: {log_paths['assignment_log']}")
    print(f"Cycle summary: {log_paths['cycle_summary']}")
    print(f"Courier timeline: {timeline_path}")


if __name__ == "__main__":
    run_simulation_with_viz()
