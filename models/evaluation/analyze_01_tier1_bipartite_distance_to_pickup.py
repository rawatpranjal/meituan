"""
Analysis of 01_tier1_bipartite_distance_to_pickup Model Output
Tier 1: Static bipartite matching with distance-to-pickup cost function
Analyzes assignment logs and cycle summaries to evaluate model performance
"""

import polars as pl
import sys
from datetime import datetime
from pathlib import Path

# ============================================================================
# CONFIGURATION
# ============================================================================

def analyze_model_run(assignment_log_path, cycle_summary_path, output_log_path):
    """
    Analyze a single model run and save results to log

    Args:
        assignment_log_path: Path to assignment log CSV
        cycle_summary_path: Path to cycle summary CSV
        output_log_path: Path to save analysis log
    """

    # Setup logging
    log = open(output_log_path, 'w')
    sys.stdout = log

    print("="*80)
    print("MODEL ANALYSIS: 01_tier1_bipartite_distance_to_pickup")
    print("Tier 1: Static Bipartite Matching | Cost: Distance to Pickup")
    print("="*80)
    print(f"Analysis timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"Assignment log: {Path(assignment_log_path).name}")
    print(f"Cycle summary: {Path(cycle_summary_path).name}")
    print()

    # ========================================================================
    # SECTION 1: LOAD DATA
    # ========================================================================
    print("[SECTION 1] DATA LOADING")
    print("-"*80)

    assignments = pl.read_csv(assignment_log_path)
    cycles = pl.read_csv(cycle_summary_path)

    print(f"Assignment records: {assignments.shape[0]:,}")
    print(f"Dispatch cycles: {cycles.shape[0]:,}")
    print()

    # ========================================================================
    # SECTION 2: OVERALL PERFORMANCE METRICS
    # ========================================================================
    print("[SECTION 2] OVERALL PERFORMANCE METRICS")
    print("-"*80)

    # From assignment log
    total_orders = assignments.shape[0]
    total_assigned = assignments.filter(pl.col("is_assigned_by_baseline") == True).shape[0]
    total_accepted = assignments.filter(pl.col("was_accepted") == True).shape[0]
    total_rejected = total_assigned - total_accepted

    assignment_rate = (total_assigned / total_orders) * 100
    acceptance_rate = (total_accepted / total_assigned) * 100 if total_assigned > 0 else 0
    overall_success_rate = (total_accepted / total_orders) * 100

    print(f"Total orders processed: {total_orders:,}")
    print(f"Orders assigned by model: {total_assigned:,}")
    print(f"Assignments accepted: {total_accepted:,}")
    print(f"Assignments rejected: {total_rejected:,}")
    print()
    print(f"Assignment rate: {assignment_rate:.2f}%")
    print(f"Acceptance rate: {acceptance_rate:.2f}%")
    print(f"Overall success rate: {overall_success_rate:.2f}%")
    print()

    # Match with actual assignments
    matches = assignments.filter(pl.col("is_match_with_actual") == True).shape[0]
    match_rate = (matches / total_orders) * 100
    print(f"Matches with actual assignments: {matches:,}")
    print(f"Match rate: {match_rate:.2f}%")
    print()

    # ========================================================================
    # SECTION 3: COST ANALYSIS
    # ========================================================================
    print("[SECTION 3] COST ANALYSIS")
    print("-"*80)

    assigned_records = assignments.filter(pl.col("is_assigned_by_baseline") == True)

    if assigned_records.shape[0] > 0:
        cost_stats = assigned_records.select([
            pl.col("baseline_cost").mean().alias("mean"),
            pl.col("baseline_cost").median().alias("median"),
            pl.col("baseline_cost").std().alias("std"),
            pl.col("baseline_cost").min().alias("min"),
            pl.col("baseline_cost").max().alias("max"),
            pl.col("baseline_cost").quantile(0.25).alias("q25"),
            pl.col("baseline_cost").quantile(0.75).alias("q75"),
            pl.col("baseline_cost").quantile(0.90).alias("q90"),
            pl.col("baseline_cost").quantile(0.95).alias("q95"),
        ]).to_dicts()[0]

        print(f"Cost statistics (distance to pickup in grid units):")
        print(f"  Mean: {cost_stats['mean']:.2f}")
        print(f"  Median: {cost_stats['median']:.2f}")
        print(f"  Std Dev: {cost_stats['std']:.2f}")
        print(f"  Min: {cost_stats['min']:.2f}")
        print(f"  Max: {cost_stats['max']:.2f}")
        print(f"  25th percentile: {cost_stats['q25']:.2f}")
        print(f"  75th percentile: {cost_stats['q75']:.2f}")
        print(f"  90th percentile: {cost_stats['q90']:.2f}")
        print(f"  95th percentile: {cost_stats['q95']:.2f}")
        print()

        # Cost by acceptance
        accepted_costs = assigned_records.filter(pl.col("was_accepted") == True)
        rejected_costs = assigned_records.filter(pl.col("was_accepted") == False)

        if accepted_costs.shape[0] > 0:
            avg_accepted_cost = accepted_costs.select(pl.col("baseline_cost").mean()).item()
            print(f"Average cost for accepted assignments: {avg_accepted_cost:.2f}")

        if rejected_costs.shape[0] > 0:
            avg_rejected_cost = rejected_costs.select(pl.col("baseline_cost").mean()).item()
            print(f"Average cost for rejected assignments: {avg_rejected_cost:.2f}")

            if accepted_costs.shape[0] > 0:
                cost_diff = avg_rejected_cost - avg_accepted_cost
                print(f"Cost difference (rejected - accepted): {cost_diff:.2f}")
        print()

    # ========================================================================
    # SECTION 4: CYCLE-LEVEL ANALYSIS
    # ========================================================================
    print("[SECTION 4] CYCLE-LEVEL ANALYSIS")
    print("-"*80)

    print(f"Number of dispatch cycles: {cycles.shape[0]}")
    print()

    # Supply-demand analysis
    supply_demand_stats = cycles.select([
        pl.col("supply_demand_ratio").mean().alias("mean"),
        pl.col("supply_demand_ratio").median().alias("median"),
        pl.col("supply_demand_ratio").min().alias("min"),
        pl.col("supply_demand_ratio").max().alias("max"),
    ]).to_dicts()[0]

    print(f"Supply-Demand Ratio (couriers/orders):")
    print(f"  Mean: {supply_demand_stats['mean']:.2f}")
    print(f"  Median: {supply_demand_stats['median']:.2f}")
    print(f"  Min: {supply_demand_stats['min']:.2f}")
    print(f"  Max: {supply_demand_stats['max']:.2f}")
    print()

    # Identify supply-constrained cycles (ratio < 1.0)
    constrained = cycles.filter(pl.col("supply_demand_ratio") < 1.0)
    print(f"Supply-constrained cycles (ratio < 1.0): {constrained.shape[0]}/{cycles.shape[0]}")
    if constrained.shape[0] > 0:
        constrained_assignment_rate = constrained.select(pl.col("assignment_rate").mean()).item()
        print(f"  Avg assignment rate in constrained cycles: {constrained_assignment_rate:.2%}")

    # Identify supply-abundant cycles (ratio >= 2.0)
    abundant = cycles.filter(pl.col("supply_demand_ratio") >= 2.0)
    print(f"Supply-abundant cycles (ratio >= 2.0): {abundant.shape[0]}/{cycles.shape[0]}")
    if abundant.shape[0] > 0:
        abundant_assignment_rate = abundant.select(pl.col("assignment_rate").mean()).item()
        print(f"  Avg assignment rate in abundant cycles: {abundant_assignment_rate:.2%}")
    print()

    # Assignment rate variation across cycles
    assignment_rate_stats = cycles.select([
        pl.col("assignment_rate").mean().alias("mean"),
        pl.col("assignment_rate").std().alias("std"),
        pl.col("assignment_rate").min().alias("min"),
        pl.col("assignment_rate").max().alias("max"),
    ]).to_dicts()[0]

    print(f"Assignment Rate Across Cycles:")
    print(f"  Mean: {assignment_rate_stats['mean']:.2%}")
    print(f"  Std Dev: {assignment_rate_stats['std']:.4f}")
    print(f"  Min: {assignment_rate_stats['min']:.2%}")
    print(f"  Max: {assignment_rate_stats['max']:.2%}")
    print()

    # Acceptance rate variation
    acceptance_rate_stats = cycles.select([
        pl.col("acceptance_rate").mean().alias("mean"),
        pl.col("acceptance_rate").std().alias("std"),
        pl.col("acceptance_rate").min().alias("min"),
        pl.col("acceptance_rate").max().alias("max"),
    ]).to_dicts()[0]

    print(f"Acceptance Rate Across Cycles:")
    print(f"  Mean: {acceptance_rate_stats['mean']:.2%}")
    print(f"  Std Dev: {acceptance_rate_stats['std']:.4f}")
    print(f"  Min: {acceptance_rate_stats['min']:.2%}")
    print(f"  Max: {acceptance_rate_stats['max']:.2%}")
    print()

    # Cost efficiency across cycles
    cost_per_cycle_stats = cycles.select([
        pl.col("avg_cost_per_assignment").mean().alias("mean"),
        pl.col("avg_cost_per_assignment").median().alias("median"),
        pl.col("avg_cost_per_assignment").std().alias("std"),
        pl.col("avg_cost_per_assignment").min().alias("min"),
        pl.col("avg_cost_per_assignment").max().alias("max"),
    ]).to_dicts()[0]

    print(f"Average Cost Per Assignment Across Cycles:")
    print(f"  Mean: {cost_per_cycle_stats['mean']:.2f}")
    print(f"  Median: {cost_per_cycle_stats['median']:.2f}")
    print(f"  Std Dev: {cost_per_cycle_stats['std']:.2f}")
    print(f"  Min: {cost_per_cycle_stats['min']:.2f}")
    print(f"  Max: {cost_per_cycle_stats['max']:.2f}")
    print()

    # ========================================================================
    # SECTION 5: TEMPORAL PATTERNS
    # ========================================================================
    print("[SECTION 5] TEMPORAL PATTERNS")
    print("-"*80)

    # Add datetime column for temporal analysis
    cycles_with_dt = cycles.with_columns([
        pl.from_epoch("dispatch_time", time_unit="s").alias("datetime")
    ])

    # Extract date
    cycles_with_dt = cycles_with_dt.with_columns([
        pl.col("datetime").dt.date().alias("date")
    ])

    # Group by date
    daily_stats = cycles_with_dt.group_by("date").agg([
        pl.count().alias("num_cycles"),
        pl.col("num_orders_in_batch").sum().alias("total_orders"),
        pl.col("num_proposed_assignments").sum().alias("total_proposed"),
        pl.col("num_accepted_assignments").sum().alias("total_accepted"),
        pl.col("assignment_rate").mean().alias("avg_assignment_rate"),
        pl.col("acceptance_rate").mean().alias("avg_acceptance_rate"),
    ]).sort("date")

    print("Daily Performance Summary:")
    print(daily_stats)
    print()

    # ========================================================================
    # SECTION 6: BATCH SIZE ANALYSIS
    # ========================================================================
    print("[SECTION 6] BATCH SIZE ANALYSIS")
    print("-"*80)

    batch_size_stats = cycles.select([
        pl.col("num_orders_in_batch").mean().alias("mean"),
        pl.col("num_orders_in_batch").median().alias("median"),
        pl.col("num_orders_in_batch").min().alias("min"),
        pl.col("num_orders_in_batch").max().alias("max"),
    ]).to_dicts()[0]

    print(f"Orders per dispatch cycle:")
    print(f"  Mean: {batch_size_stats['mean']:.1f}")
    print(f"  Median: {batch_size_stats['median']:.1f}")
    print(f"  Min: {batch_size_stats['min']}")
    print(f"  Max: {batch_size_stats['max']}")
    print()

    courier_pool_stats = cycles.select([
        pl.col("num_available_couriers").mean().alias("mean"),
        pl.col("num_available_couriers").median().alias("median"),
        pl.col("num_available_couriers").min().alias("min"),
        pl.col("num_available_couriers").max().alias("max"),
    ]).to_dicts()[0]

    print(f"Available couriers per dispatch cycle:")
    print(f"  Mean: {courier_pool_stats['mean']:.1f}")
    print(f"  Median: {courier_pool_stats['median']:.1f}")
    print(f"  Min: {courier_pool_stats['min']}")
    print(f"  Max: {courier_pool_stats['max']}")
    print()

    # ========================================================================
    # SECTION 7: KEY INSIGHTS
    # ========================================================================
    print("="*80)
    print("[SECTION 7] KEY INSIGHTS")
    print("="*80)

    print("\n1. OVERALL MODEL PERFORMANCE:")
    print(f"   - Successfully assigned {overall_success_rate:.1f}% of orders")
    print(f"   - Assignment rate: {assignment_rate:.1f}%")
    print(f"   - Acceptance rate: {acceptance_rate:.1f}%")

    print("\n2. COST EFFICIENCY:")
    if assigned_records.shape[0] > 0:
        print(f"   - Median distance to pickup: {cost_stats['median']:.0f} grid units")
        print(f"   - 95th percentile distance: {cost_stats['q95']:.0f} grid units")

    print("\n3. SUPPLY-DEMAND DYNAMICS:")
    print(f"   - {constrained.shape[0]} cycles were supply-constrained (ratio < 1.0)")
    print(f"   - {abundant.shape[0]} cycles had abundant supply (ratio >= 2.0)")
    print(f"   - Median supply-demand ratio: {supply_demand_stats['median']:.2f}")

    print("\n4. MATCH WITH ACTUAL ASSIGNMENTS:")
    print(f"   - Only {match_rate:.2f}% of assignments matched actual system")
    print(f"   - This suggests the actual system uses different optimization criteria")

    print()
    print("="*80)
    print("ANALYSIS COMPLETE")
    print("="*80)

    log.close()
    print(f"Analysis saved to: {output_log_path}", file=sys.__stdout__)


if __name__ == "__main__":
    # Default: analyze the most recent run
    LOGS_DIR = "/Users/pranjal/Code/meituan/models/logs"
    MODEL_NAME = "01_tier1_bipartite_distance_to_pickup"

    # Use most recent files (can be parameterized)
    # TODO: Update these timestamps to match your most recent model run
    RUN_TIMESTAMP = "20251027_181214"  # Update this to analyze a specific run

    assignment_log = f"{LOGS_DIR}/{MODEL_NAME}_assignment_log_{RUN_TIMESTAMP}.csv"
    cycle_summary = f"{LOGS_DIR}/{MODEL_NAME}_cycle_summary_{RUN_TIMESTAMP}.csv"

    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    output_log = f"{LOGS_DIR}/{MODEL_NAME}_analysis_{timestamp}.log"

    analyze_model_run(assignment_log, cycle_summary, output_log)
