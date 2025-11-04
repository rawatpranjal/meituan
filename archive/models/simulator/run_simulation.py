"""
Discrete-Time Simulator - Main Runner

Orchestrates the entire simulation using historical order arrivals
and stateful courier tracking.
"""

import polars as pl
import numpy as np
import random
import sys
import os

# Import our simulator modules
from physics import AVERAGE_TASK_DURATION, GLOBAL_REJECTION_PROBABILITY
from state import (initialize_courier_states, get_available_couriers,
                   update_courier_after_assignment, get_courier_state_summary)
from assignment_strategy import Tier1Baseline
from logger import SimulationLogger

# ============================================================================
# CONFIGURATION
# ============================================================================

DATA_PATH = "/Users/pranjal/Code/meituan/data/INFORMS.org/meituan_informs_data/"
LOGS_DIR = "/Users/pranjal/Code/meituan/models/logs"
MODEL_NAME = "tier1_baseline_simulator"

# Random seed for reproducibility of courier rejections
random.seed(42)
np.random.seed(42)

# ============================================================================
# MAIN SIMULATION
# ============================================================================

def run_simulation():
    """
    Run the discrete-time simulation
    """
    print("="*80)
    print("DISCRETE-TIME SIMULATOR - Tier 1 Baseline")
    print("="*80)

    # --- 1. SETUP & DATA LOADING ---
    print("\n[SETUP] Loading historical data...")
    waybill = pl.read_csv(f"{DATA_PATH}all_waybill_info_meituan_0322.csv")
    dispatch_rider = pl.read_csv(f"{DATA_PATH}dispatch_rider_meituan.csv")
    dispatch_waybill = pl.read_csv(f"{DATA_PATH}dispatch_waybill_meituan.csv")

    print(f"Loaded {waybill.shape[0]:,} waybill records")
    print(f"Loaded {dispatch_rider.shape[0]:,} courier snapshots")
    print(f"Loaded {dispatch_waybill.shape[0]:,} dispatch orders")

    # Build waybill lookup dictionary
    print("\nBuilding waybill lookup...")
    waybill_subset = waybill.select(['order_id', 'sender_lat', 'sender_lng',
                                      'recipient_lat', 'recipient_lng', 'courier_id'])
    order_ids = waybill_subset['order_id'].to_list()
    sender_lats = waybill_subset['sender_lat'].to_list()
    sender_lngs = waybill_subset['sender_lng'].to_list()
    recipient_lats = waybill_subset['recipient_lat'].to_list()
    recipient_lngs = waybill_subset['recipient_lng'].to_list()
    actual_courier_ids = waybill_subset['courier_id'].to_list()

    waybill_lookup = {
        order_ids[i]: {
            'sender_lat': sender_lats[i],
            'sender_lng': sender_lngs[i],
            'recipient_lat': recipient_lats[i],
            'recipient_lng': recipient_lngs[i],
            'actual_courier_id': actual_courier_ids[i]
        }
        for i in range(len(order_ids))
    }
    print(f"Lookup contains {len(waybill_lookup):,} orders")

    # --- 2. INITIALIZE SIMULATION STATE ---
    print("\n[INITIALIZATION] Setting up initial state...")

    # Get sorted list of dispatch times
    dispatch_times = dispatch_waybill['dispatch_time'].unique().sort().to_list()
    print(f"Total dispatch moments to process: {len(dispatch_times)}")

    # Initialize courier states from first dispatch moment
    first_dispatch_time = dispatch_times[0]
    first_dispatch_couriers = dispatch_rider.filter(
        pl.col('dispatch_time') == first_dispatch_time
    ).to_dicts()

    courier_states = initialize_courier_states(first_dispatch_couriers, waybill_lookup)
    print(f"Initialized {len(courier_states):,} couriers")

    # Initialize assignment strategy
    assignment_strategy = Tier1Baseline()
    print(f"Assignment strategy: Tier 1 Baseline (Distance to Pickup)")

    # Initialize logger
    logger = SimulationLogger(LOGS_DIR, MODEL_NAME)
    print(f"Metrics logging initialized")
    print(f"  Assignment log: {logger.assignment_log_path}")
    print(f"  Cycle summary: {logger.cycle_summary_path}")

    # Physics constants
    print(f"\nPhysics constants:")
    print(f"  AVERAGE_TASK_DURATION = {AVERAGE_TASK_DURATION}s ({AVERAGE_TASK_DURATION/60:.1f} min)")
    print(f"  GLOBAL_REJECTION_PROBABILITY = {GLOBAL_REJECTION_PROBABILITY:.4f}")

    # --- 3. MAIN DISCRETE-TIME LOOP ---
    print("\n" + "="*80)
    print("[SIMULATION] Starting discrete-time loop...")
    print("="*80)

    total_orders_processed = 0
    total_assignments_proposed = 0
    total_assignments_accepted = 0
    total_rejections = 0

    for dispatch_idx, dispatch_time in enumerate(dispatch_times):
        print(f"\n[Dispatch {dispatch_idx+1}/{len(dispatch_times)}] Time: {dispatch_time}")

        # --- A. GET WAITING ORDERS ---
        waiting_orders_df = dispatch_waybill.filter(pl.col('dispatch_time') == dispatch_time)
        waiting_orders = waiting_orders_df.to_dicts()
        n_orders = len(waiting_orders)
        total_orders_processed += n_orders

        # --- B. GET AVAILABLE COURIERS ---
        available_couriers = get_available_couriers(dispatch_time, courier_states)
        n_couriers = len(available_couriers)

        courier_summary = get_courier_state_summary(courier_states, dispatch_time)

        print(f"  Orders: {n_orders}, Available couriers: {n_couriers}")
        print(f"  Courier utilization: {courier_summary['utilization']*100:.1f}% busy")

        if n_orders == 0 or n_couriers == 0:
            print("  Skipping (no orders or no couriers)")
            continue

        # --- C. MAKE ASSIGNMENTS ---
        proposed_assignments = assignment_strategy.make_assignments(
            waiting_orders, available_couriers, waybill_lookup
        )
        n_proposed = len(proposed_assignments)
        total_assignments_proposed += n_proposed

        print(f"  Proposed assignments: {n_proposed}/{n_orders}")

        # --- D. PROCESS ASSIGNMENTS & REJECTIONS ---
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
                    delivery_location, AVERAGE_TASK_DURATION
                )

        n_accepted = len(accepted_assignments)
        n_rejected = len(rejected_assignments)

        print(f"  Accepted: {n_accepted}, Rejected: {n_rejected}")
        print(f"  Avg cost per accepted assignment: {cycle_total_cost/n_accepted if n_accepted > 0 else 0:.0f} grid units")

        # --- E. LOG METRICS ---
        # Log each order (assigned, rejected, or unassigned)
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
                    # Check if accepted
                    was_accepted = order_id in assigned_order_ids
                    # TODO: Calculate rank
                    rank = None
                    break

            is_match = (courier and was_accepted and
                       courier['courier_id'] == actual_courier_id)
            if is_match:
                num_matches += 1

            logger.log_assignment(
                dispatch_time, order, courier, cost, rank,
                is_assigned, was_accepted, actual_courier_id, is_match,
                n_orders, n_couriers, pickup_lat, pickup_lng
            )

        # Log cycle summary
        agreement_rate = num_matches / n_orders if n_orders > 0 else 0
        logger.log_cycle_summary(
            dispatch_time, n_orders, n_couriers,
            n_proposed, n_accepted, n_rejected,
            cycle_total_cost, agreement_rate
        )

        logger.flush()

        print(f"  Agreement with actual: {agreement_rate*100:.1f}%")

    # --- 4. FINAL SUMMARY ---
    print("\n" + "="*80)
    print("[SUMMARY] Simulation Complete")
    print("="*80)
    print(f"Total orders processed: {total_orders_processed:,}")
    print(f"Proposed assignments: {total_assignments_proposed:,}")
    print(f"Accepted assignments: {total_assignments_accepted:,}")
    print(f"Rejections: {total_rejections:,}")
    print(f"Overall assignment rate: {total_assignments_accepted/total_orders_processed*100:.2f}%")
    print(f"Overall acceptance rate: {total_assignments_accepted/total_assignments_proposed*100:.2f}%" if total_assignments_proposed > 0 else "N/A")

    # Close logger
    logger.close()
    log_paths = logger.get_log_paths()
    print(f"\nLogs saved:")
    print(f"  {log_paths['assignment_log']}")
    print(f"  {log_paths['cycle_summary']}")


if __name__ == "__main__":
    run_simulation()
