"""
Calibrate Physics Constants from Historical Data

This script performs a one-time analysis of the historical waybill data
to derive the "laws of physics" for our simulation:
1. AVERAGE_TASK_DURATION - typical time from assignment to delivery completion
2. GLOBAL_REJECTION_PROBABILITY - overall courier rejection rate

These constants will be used by the simulator to model realistic courier behavior.
"""

import polars as pl
import sys

data_path = "/Users/pranjal/Code/meituan/data/INFORMS.org/meituan_informs_data/"

print("="*80)
print("PHYSICS CALIBRATION - Deriving Constants from Historical Data")
print("="*80)

# ============================================================================
# Load Historical Data
# ============================================================================
print("\nLoading historical waybill data...")
waybill = pl.read_csv(f"{data_path}all_waybill_info_meituan_0322.csv")
print(f"Loaded {waybill.shape[0]:,} waybill records")

# ============================================================================
# Calibrate 1: AVERAGE_TASK_DURATION
# ============================================================================
print("\n" + "-"*80)
print("CALIBRATING: AVERAGE_TASK_DURATION")
print("-"*80)

# Filter for valid, successfully delivered orders
valid_deliveries = waybill.filter(
    (pl.col("is_courier_grabbed") == 1) &  # Successfully accepted
    (pl.col("grab_time") > 0) &             # Valid grab time
    (pl.col("arrive_time") > 0) &          # Valid arrival time
    (pl.col("arrive_time") > pl.col("grab_time"))  # Logical ordering
)

print(f"Valid delivered orders: {valid_deliveries.shape[0]:,} ({100*valid_deliveries.shape[0]/waybill.shape[0]:.1f}% of total)")

# Calculate task durations
durations = valid_deliveries.select([
    (pl.col("arrive_time") - pl.col("grab_time")).alias("task_duration")
])

# Use median as it's robust to outliers
avg_task_duration = durations["task_duration"].median()
mean_task_duration = durations["task_duration"].mean()
p25 = durations["task_duration"].quantile(0.25)
p75 = durations["task_duration"].quantile(0.75)

print(f"\nTask Duration Statistics (in seconds):")
print(f"  Median (P50): {avg_task_duration:.0f}s ({avg_task_duration/60:.1f} min)")
print(f"  Mean: {mean_task_duration:.0f}s ({mean_task_duration/60:.1f} min)")
print(f"  25th percentile: {p25:.0f}s ({p25/60:.1f} min)")
print(f"  75th percentile: {p75:.0f}s ({p75/60:.1f} min)")

print(f"\n CALIBRATED: AVERAGE_TASK_DURATION = {avg_task_duration:.0f} seconds")

# ============================================================================
# Calibrate 2: GLOBAL_REJECTION_PROBABILITY
# ============================================================================
print("\n" + "-"*80)
print("CALIBRATING: GLOBAL_REJECTION_PROBABILITY")
print("-"*80)

total_attempts = waybill.shape[0]
rejected_attempts = waybill.filter(pl.col("is_courier_grabbed") == 0).shape[0]
accepted_attempts = waybill.filter(pl.col("is_courier_grabbed") == 1).shape[0]

rejection_probability = rejected_attempts / total_attempts

print(f"\nCourier Response Statistics:")
print(f"  Total dispatch attempts: {total_attempts:,}")
print(f"  Accepted (grabbed): {accepted_attempts:,} ({100*accepted_attempts/total_attempts:.2f}%)")
print(f"  Rejected (not grabbed): {rejected_attempts:,} ({100*rejected_attempts/total_attempts:.2f}%)")

print(f"\n CALIBRATED: GLOBAL_REJECTION_PROBABILITY = {rejection_probability:.4f}")

# ============================================================================
# Write Calibrated Constants to physics.py
# ============================================================================
print("\n" + "="*80)
print("Writing calibrated constants to physics.py...")
print("="*80)

physics_code = f'''"""
Physics Constants and Helper Functions

These constants were calibrated from historical data using calibrate_physics.py.
They define the "laws of physics" for the discrete-time simulator.
"""

from math import sqrt

# ============================================================================
# CALIBRATED CONSTANTS (from historical data)
# ============================================================================

# Average time (in seconds) from order assignment to delivery completion
# Calibrated as median(arrive_time - grab_time) for successfully delivered orders
AVERAGE_TASK_DURATION = {avg_task_duration:.0f}  # seconds ({avg_task_duration/60:.1f} minutes)

# Probability that a courier will reject an assignment
# Calibrated as count(is_courier_grabbed==0) / total_orders
GLOBAL_REJECTION_PROBABILITY = {rejection_probability:.4f}  # {rejection_probability*100:.2f}%

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def euclidean_distance(x1, y1, x2, y2):
    """
    Calculate Euclidean distance for planar grid coordinates

    Args:
        x1, y1: First point coordinates (shifted grid units)
        x2, y2: Second point coordinates (shifted grid units)

    Returns:
        Distance in grid units
    """
    return sqrt((x2 - x1)**2 + (y2 - y1)**2)
'''

with open('/Users/pranjal/Code/meituan/models/simulator/physics.py', 'w') as f:
    f.write(physics_code)

print(" Physics constants written successfully")
print(f"\nCalibration complete. Constants:")
print(f"  AVERAGE_TASK_DURATION = {avg_task_duration:.0f}s")
print(f"  GLOBAL_REJECTION_PROBABILITY = {rejection_probability:.4f}")
