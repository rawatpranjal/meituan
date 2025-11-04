"""
Stakeholder Scorecard Analysis

Computes the three essential metrics for evaluating ride-matching systems:
1. Platform: System-Wide Travel Inefficiency (Total Detour Distance)
2. Customer: Median Wait for Assignment Time
3. Courier: Assignments Per Idle Hour

This analysis module is designed to answer the key question:
"Who benefits and who gets hurt?" when comparing different matching algorithms.
"""

import polars as pl
import sys
from datetime import datetime
from pathlib import Path


def compute_stakeholder_scorecard(assignment_log_path, cycle_summary_path,
                                   courier_timeline_path, output_log_path):
    """
    Compute three-metric stakeholder scorecard from simulation logs

    Args:
        assignment_log_path: Path to assignment log CSV
        cycle_summary_path: Path to cycle summary CSV
        courier_timeline_path: Path to courier timeline CSV
        output_log_path: Path to save scorecard analysis

    Returns:
        dict: Scorecard metrics
    """
    # Setup logging
    log = open(output_log_path, 'w')
    sys.stdout = log

    print("="*80)
    print("STAKEHOLDER SCORECARD ANALYSIS")
    print("="*80)
    print(f"Analysis timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Assignment log: {Path(assignment_log_path).name}")
    print(f"Cycle summary: {Path(cycle_summary_path).name}")
    print(f"Courier timeline: {Path(courier_timeline_path).name}")
    print()

    # ========================================================================
    # LOAD DATA
    # ========================================================================
    print("[LOADING DATA]")
    print("-"*80)

    assignments = pl.read_csv(assignment_log_path)
    cycles = pl.read_csv(cycle_summary_path)
    timeline = pl.read_csv(courier_timeline_path)

    print(f"Assignment records: {assignments.shape[0]:,}")
    print(f"Dispatch cycles: {cycles.shape[0]:,}")
    print(f"Courier state transitions: {timeline.shape[0]:,}")
    print()

    # ========================================================================
    # METRIC 1: PLATFORM - SYSTEM-WIDE TRAVEL INEFFICIENCY
    # ========================================================================
    print("="*80)
    print("[METRIC 1] PLATFORM: System-Wide Travel Inefficiency")
    print("="*80)
    print()
    print("Definition: Total detour distance across all assignments")
    print("Lower is better - represents efficient use of courier resources")
    print()

    # Sum all assignment costs (distance to pickup for Tier 1)
    total_cost = cycles.select(pl.col("total_cost_of_cycle").sum()).item()

    # Count accepted assignments
    n_assignments = cycles.select(pl.col("num_accepted_assignments").sum()).item()

    avg_cost_per_assignment = total_cost / n_assignments if n_assignments > 0 else 0

    print(f"Total Cost (Grid Units):           {total_cost:,.0f}")
    print(f"Total Accepted Assignments:        {n_assignments:,}")
    print(f"Average Cost Per Assignment:       {avg_cost_per_assignment:,.2f}")
    print()

    # ========================================================================
    # METRIC 2: CUSTOMER - MEDIAN WAIT FOR ASSIGNMENT TIME
    # ========================================================================
    print("="*80)
    print("[METRIC 2] CUSTOMER: Median Wait for Assignment Time")
    print("="*80)
    print()
    print("Definition: Time from order creation to assignment (50th percentile)")
    print("Lower is better - customers want quick matches")
    print()

    # Filter to accepted assignments with valid wait time data
    accepted_with_wait = assignments.filter(
        (pl.col("was_accepted") == True) &
        (pl.col("wait_for_assignment_seconds").is_not_null())
    )

    if accepted_with_wait.shape[0] > 0:
        wait_stats = accepted_with_wait.select([
            pl.col("wait_for_assignment_seconds").median().alias("median"),
            pl.col("wait_for_assignment_seconds").mean().alias("mean"),
            pl.col("wait_for_assignment_seconds").min().alias("min"),
            pl.col("wait_for_assignment_seconds").max().alias("max"),
            pl.col("wait_for_assignment_seconds").quantile(0.25).alias("q25"),
            pl.col("wait_for_assignment_seconds").quantile(0.75).alias("q75"),
            pl.col("wait_for_assignment_seconds").quantile(0.90).alias("q90"),
        ]).to_dicts()[0]

        median_wait_seconds = wait_stats['median']
        median_wait_minutes = median_wait_seconds / 60

        print(f"Median Wait (seconds):             {median_wait_seconds:,.0f}")
        print(f"Median Wait (minutes):             {median_wait_minutes:,.2f}")
        print(f"Mean Wait (seconds):               {wait_stats['mean']:,.0f}")
        print(f"Min Wait (seconds):                {wait_stats['min']:,.0f}")
        print(f"Max Wait (seconds):                {wait_stats['max']:,.0f}")
        print(f"25th Percentile (seconds):         {wait_stats['q25']:,.0f}")
        print(f"75th Percentile (seconds):         {wait_stats['q75']:,.0f}")
        print(f"90th Percentile (seconds):         {wait_stats['q90']:,.0f}")
        print()
    else:
        median_wait_seconds = None
        print("ERROR: No valid wait time data available")
        print()

    # ========================================================================
    # METRIC 3: COURIER - ASSIGNMENTS PER IDLE HOUR
    # ========================================================================
    print("="*80)
    print("[METRIC 3] COURIER: Assignments Per Idle Hour")
    print("="*80)
    print()
    print("Definition: Total assignments / Total courier idle time (in hours)")
    print("Higher is better - couriers want frequent work during availability")
    print()

    # Calculate idle time from timeline
    # Group by courier and calculate time spent in AVAILABLE state
    couriers_with_states = timeline.with_columns([
        pl.col("timestamp").alias("event_time")
    ])

    # Sort by courier and timestamp
    couriers_sorted = couriers_with_states.sort(["courier_id", "event_time"])

    # For each courier, calculate time between state changes
    # A courier's idle time = time spent in AVAILABLE state

    # Get unique couriers
    unique_couriers = timeline.select(pl.col("courier_id").unique())["courier_id"].to_list()

    total_idle_seconds = 0

    for courier_id in unique_couriers:
        courier_events = couriers_sorted.filter(pl.col("courier_id") == courier_id)

        events_list = courier_events.to_dicts()

        # Calculate time in each state
        for i in range(len(events_list) - 1):
            current_event = events_list[i]
            next_event = events_list[i + 1]

            # Time in current state
            time_in_state = next_event['event_time'] - current_event['event_time']

            # If current state is AVAILABLE, count as idle time
            if current_event['new_state'] == 'AVAILABLE':
                total_idle_seconds += time_in_state

    total_idle_hours = total_idle_seconds / 3600

    assignments_per_idle_hour = n_assignments / total_idle_hours if total_idle_hours > 0 else 0

    print(f"Total Idle Time (seconds):         {total_idle_seconds:,.0f}")
    print(f"Total Idle Time (hours):           {total_idle_hours:,.2f}")
    print(f"Total Accepted Assignments:        {n_assignments:,}")
    print(f"Assignments Per Idle Hour:         {assignments_per_idle_hour:,.3f}")
    print()

    # ========================================================================
    # SCORECARD SUMMARY
    # ========================================================================
    print("="*80)
    print("STAKEHOLDER SCORECARD SUMMARY")
    print("="*80)
    print()

    scorecard = {
        'platform_total_cost': total_cost,
        'platform_avg_cost_per_assignment': avg_cost_per_assignment,
        'customer_median_wait_seconds': median_wait_seconds,
        'customer_median_wait_minutes': median_wait_minutes if median_wait_seconds else None,
        'courier_idle_hours': total_idle_hours,
        'courier_assignments_per_idle_hour': assignments_per_idle_hour
    }

    print("┌" + "─"*78 + "┐")
    print("│ Stakeholder │ Metric                          │ Value                │")
    print("├" + "─"*78 + "┤")
    print(f"│ PLATFORM    │ Total Detour Distance           │ {total_cost:>15,.0f} units │")
    print(f"│             │ Avg Cost Per Assignment         │ {avg_cost_per_assignment:>15,.2f} units │")
    print("├" + "─"*78 + "┤")
    if median_wait_seconds is not None:
        print(f"│ CUSTOMER    │ Median Wait for Assignment      │ {median_wait_seconds:>15,.0f} sec    │")
        print(f"│             │                                 │ {median_wait_minutes:>15,.2f} min    │")
    else:
        print(f"│ CUSTOMER    │ Median Wait for Assignment      │ {'N/A':>20} │")
    print("├" + "─"*78 + "┤")
    print(f"│ COURIER     │ Assignments Per Idle Hour       │ {assignments_per_idle_hour:>15,.3f}      │")
    print(f"│             │ Total Idle Time                 │ {total_idle_hours:>15,.2f} hrs  │")
    print("└" + "─"*78 + "┘")
    print()

    print("="*80)
    print("INTERPRETATION GUIDE")
    print("="*80)
    print()
    print("PLATFORM (Total Detour Distance):")
    print("  - Lower = More efficient use of courier fleet")
    print("  - Represents total 'wasted' travel to pickup locations")
    print()
    print("CUSTOMER (Median Wait Time):")
    print("  - Lower = Faster service")
    print("  - Measures responsiveness of the matching algorithm")
    print()
    print("COURIER (Assignments Per Idle Hour):")
    print("  - Higher = More earning opportunity")
    print("  - Measures how 'liquid' the marketplace is for couriers")
    print()

    print("="*80)
    print("SCORECARD ANALYSIS COMPLETE")
    print("="*80)

    log.close()
    print(f"Scorecard saved to: {output_log_path}", file=sys.__stdout__)

    return scorecard


if __name__ == "__main__":
    # Default: analyze the most recent run
    LOGS_DIR = "/Users/pranjal/Code/meituan/models/logs"
    MODEL_NAME = "01_tier1_bipartite_distance_to_pickup"

    # Update this timestamp to match your most recent model run
    RUN_TIMESTAMP = "20251027_192202"  # TODO: Update to latest run

    assignment_log = f"{LOGS_DIR}/{MODEL_NAME}_assignment_log_{RUN_TIMESTAMP}.csv"
    cycle_summary = f"{LOGS_DIR}/{MODEL_NAME}_cycle_summary_{RUN_TIMESTAMP}.csv"
    courier_timeline = f"{LOGS_DIR}/{MODEL_NAME}_courier_timeline_{RUN_TIMESTAMP}.csv"

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_log = f"{LOGS_DIR}/{MODEL_NAME}_scorecard_{timestamp}.log"

    scorecard = compute_stakeholder_scorecard(
        assignment_log, cycle_summary, courier_timeline, output_log
    )
