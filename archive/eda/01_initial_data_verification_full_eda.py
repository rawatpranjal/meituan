"""
EDA based on PDF Ground Truth
- Background and Data Description (Tables 1-5)
- Supplementary Document (Section 2 clarifications)
"""

import polars as pl
import sys
from datetime import datetime

# Setup logging
log_file = f"/Users/pranjal/Code/meituan/eda/logs/01_initial_data_verification_full_eda_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
log = open(log_file, 'w')
sys.stdout = log

print("="*80)
print("MEITUAN INFORMS DATA VALIDATION")
print("Based on: TSL-Meituan challenge_background and data_20240321.pdf")
print("="*80)

data_path = "/Users/pranjal/Code/meituan/data/INFORMS.org/meituan_informs_data/"

# Load datasets
print("\n[1] LOADING DATASETS")
print("-"*80)

waybill = pl.read_csv(f"{data_path}all_waybill_info_meituan_0322.csv")
courier_wave = pl.read_csv(f"{data_path}courier_wave_info_meituan.csv")
dispatch_rider = pl.read_csv(f"{data_path}dispatch_rider_meituan.csv")
dispatch_waybill = pl.read_csv(f"{data_path}dispatch_waybill_meituan.csv")

print(f"all_waybill_info_meituan_0322.csv: {waybill.shape[0]:,} rows, {waybill.shape[1]} columns")
print(f"courier_wave_info_meituan.csv: {courier_wave.shape[0]:,} rows, {courier_wave.shape[1]} columns")
print(f"dispatch_rider_meituan.csv: {dispatch_rider.shape[0]:,} rows, {dispatch_rider.shape[1]} columns")
print(f"dispatch_waybill_meituan.csv: {dispatch_waybill.shape[0]:,} rows, {dispatch_waybill.shape[1]} columns")

# Validate row counts (PDF Section 1)
print("\n[2] ROW COUNT VALIDATION")
print("-"*80)
print("Expected from PDF Section 1:")
print("- Waybill data: 654,343 rows")
print("- Courier wave data: 206,748 rows")
print("- Dispatch rider data: 62,044 rows")
print("- Dispatch waybill data: 15,921 rows")
print()
print("Actual:")
print(f"- Waybill data: {waybill.shape[0]:,} rows - {'MATCH' if waybill.shape[0] == 654343 else 'MISMATCH'}")
print(f"- Courier wave data: {courier_wave.shape[0]:,} rows - {'MATCH' if courier_wave.shape[0] == 206748 else 'MISMATCH'}")
print(f"- Dispatch rider data: {dispatch_rider.shape[0]:,} rows - {'MATCH' if dispatch_rider.shape[0] == 62044 else 'MISMATCH'}")
print(f"- Dispatch waybill data: {dispatch_waybill.shape[0]:,} rows - {'MATCH' if dispatch_waybill.shape[0] == 15921 else 'MISMATCH'}")

# Validate columns (PDF Tables 1 & 2)
print("\n[3] WAYBILL COLUMNS VALIDATION (PDF Tables 1 & 2)")
print("-"*80)
expected_waybill_cols = [
    'order_id', 'waybill_id', 'dt', 'da_id', 'sender_lat', 'sender_lng',
    'recipient_lat', 'recipient_lng', 'poi_id', 'platform_order_time',
    'estimate_arrived_time', 'estimate_meal_prepare_time', 'order_push_time',
    'dispatch_time', 'courier_id', 'grab_lat', 'grab_lng', 'is_courier_grabbed',
    'grab_time', 'fetch_time', 'arrive_time', 'is_prebook', 'is_weekend'
]
actual_waybill_cols = waybill.columns
print(f"Expected columns (PDF Tables 1 & 2): {len(expected_waybill_cols)}")
print(f"Actual columns: {len(actual_waybill_cols)}")
print()
missing = set(expected_waybill_cols) - set(actual_waybill_cols)
extra = set(actual_waybill_cols) - set(expected_waybill_cols)
if missing:
    print(f"Missing columns: {missing}")
if extra:
    print(f"Extra columns: {extra}")
if not missing and not extra:
    print("Column names: MATCH")

# Validate courier wave columns (PDF Table 3)
print("\n[4] COURIER WAVE COLUMNS VALIDATION (PDF Table 3)")
print("-"*80)
expected_courier_cols = ['dt', 'courier_id', 'wave_id', 'wave_start_time', 'wave_end_time', 'order_ids']
actual_courier_cols = courier_wave.columns
print(f"Expected columns (PDF Table 3): {len(expected_courier_cols)}")
print(f"Actual columns: {len(actual_courier_cols)}")
print()
missing = set(expected_courier_cols) - set(actual_courier_cols)
extra = set(actual_courier_cols) - set(expected_courier_cols)
if missing:
    print(f"Missing columns: {missing}")
if extra:
    print(f"Extra columns: {extra}")
if not missing and not extra:
    print("Column names: MATCH")

# Validate dispatch rider columns (PDF Table 5)
print("\n[5] DISPATCH RIDER COLUMNS VALIDATION (PDF Table 5)")
print("-"*80)
expected_dispatch_rider_cols = ['dt', 'dispatch_time', 'courier_id', 'rider_lat', 'rider_lng', 'courier_waybills']
actual_dispatch_rider_cols = dispatch_rider.columns
print(f"Expected columns (PDF Table 5): {len(expected_dispatch_rider_cols)}")
print(f"Actual columns: {len(actual_dispatch_rider_cols)}")
print()
missing = set(expected_dispatch_rider_cols) - set(actual_dispatch_rider_cols)
extra = set(actual_dispatch_rider_cols) - set(expected_dispatch_rider_cols)
if missing:
    print(f"Missing columns: {missing}")
if extra:
    print(f"Extra columns: {extra}")
if not missing and not extra:
    print("Column names: MATCH")

# Validate dispatch waybill columns (PDF Table 4)
print("\n[6] DISPATCH WAYBILL COLUMNS VALIDATION (PDF Table 4)")
print("-"*80)
expected_dispatch_waybill_cols = ['dt', 'dispatch_time', 'order_id']
actual_dispatch_waybill_cols = dispatch_waybill.columns
print(f"Expected columns (PDF Table 4): {len(expected_dispatch_waybill_cols)}")
print(f"Actual columns: {len(actual_dispatch_waybill_cols)}")
print()
missing = set(expected_dispatch_waybill_cols) - set(actual_dispatch_waybill_cols)
extra = set(actual_dispatch_waybill_cols) - set(expected_dispatch_waybill_cols)
if missing:
    print(f"Missing columns: {missing}")
if extra:
    print(f"Extra columns: {extra}")
if not missing and not extra:
    print("Column names: MATCH")

# Data Quality Checks (Supplementary PDF Section 2)
print("\n[7] DATA QUALITY CHECKS (Supplementary PDF Section 2)")
print("-"*80)

print("\n[7.1] Coordinate Shifting (Section 2.1)")
print("PDF: Coordinates are shifted (not scaled) for privacy protection")
print(f"Sender lat range: [{waybill['sender_lat'].min():.6f}, {waybill['sender_lat'].max():.6f}]")
print(f"Sender lng range: [{waybill['sender_lng'].min():.6f}, {waybill['sender_lng'].max():.6f}]")
print(f"Recipient lat range: [{waybill['recipient_lat'].min():.6f}, {waybill['recipient_lat'].max():.6f}]")
print(f"Recipient lng range: [{waybill['recipient_lng'].min():.6f}, {waybill['recipient_lng'].max():.6f}]")

print("\n[7.2] Wave Start Time Issues (Section 2.2)")
print("PDF: wave_start_time may be incorrect; use earliest grab_time instead")
# Group by wave to check
wave_check = waybill.filter(pl.col('is_courier_grabbed') == 1).join(
    courier_wave,
    on=['dt', 'courier_id'],
    how='inner'
)
wave_check = wave_check.with_columns([
    (pl.col('grab_time') < pl.col('wave_start_time')).alias('grab_before_wave_start')
])
issues = wave_check['grab_before_wave_start'].sum()
print(f"Waybills with grab_time < wave_start_time: {issues:,}")

print("\n[7.3] Dispatch Time Checkpoints (Section 2.3)")
print("PDF: dispatch_time is checkpoint when system outputs candidates")
print("PDF: Not actual assignment time")
print(f"Unique dispatch_time values in dispatch_waybill: {dispatch_waybill['dispatch_time'].n_unique():,}")
print(f"Unique dispatch_time values in dispatch_rider: {dispatch_rider['dispatch_time'].n_unique():,}")

print("\n[7.4] Rejected Waybills (Section 2.4)")
print("PDF: Waybills with is_courier_grabbed=0 have zeros in time fields")
rejected = waybill.filter(pl.col('is_courier_grabbed') == 0)
print(f"Total rejected waybills: {rejected.shape[0]:,}")
print(f"Rejected with grab_time=0: {(rejected['grab_time'] == 0).sum():,}")
print(f"Rejected with fetch_time=0: {(rejected['fetch_time'] == 0).sum():,}")
print(f"Rejected with arrive_time=0: {(rejected['arrive_time'] == 0).sum():,}")

print("\n[7.5] estimate_meal_prepare_time (Section 2.5)")
print("PDF: When order placed, meal preparation time not started")
print("PDF: Correction: meal preparation starts at platform_order_time")
prep_stats = waybill.select([
    pl.col('estimate_meal_prepare_time').min().alias('min'),
    pl.col('estimate_meal_prepare_time').max().alias('max'),
    pl.col('estimate_meal_prepare_time').mean().alias('mean'),
    pl.col('estimate_meal_prepare_time').median().alias('median')
])
print(f"Min: {prep_stats['min'][0]}")
print(f"Max: {prep_stats['max'][0]}")
print(f"Mean: {prep_stats['mean'][0]:.2f}")
print(f"Median: {prep_stats['median'][0]:.2f}")

print("\n[7.6] da_id Overlaps (Section 2.6)")
print("PDF: Multiple da_id within same time period")
print(f"Unique da_id values: {waybill['da_id'].n_unique():,}")
dt_da_combos = waybill.group_by(['dt', 'da_id']).agg(pl.len().alias('count'))
print(f"Unique (dt, da_id) combinations: {dt_da_combos.shape[0]:,}")

print("\n[7.7] Wave ID Uniqueness (Section 2.7)")
print("PDF: wave_id is unique identifier within (dt, courier_id)")
wave_unique = courier_wave.group_by(['dt', 'courier_id', 'wave_id']).agg(pl.len().alias('count'))
duplicates = wave_unique.filter(pl.col('count') > 1)
print(f"Total wave records: {courier_wave.shape[0]:,}")
print(f"Unique (dt, courier_id, wave_id): {wave_unique.shape[0]:,}")
print(f"Duplicate wave IDs: {duplicates.shape[0]:,}")

# Basic Statistics
print("\n[8] BASIC STATISTICS")
print("-"*80)

print("\n[8.1] Waybill Data")
print(f"Unique order_id: {waybill['order_id'].n_unique():,}")
print(f"Unique waybill_id: {waybill['waybill_id'].n_unique():,}")
print(f"Unique courier_id: {waybill['courier_id'].n_unique():,}")
print(f"Unique poi_id: {waybill['poi_id'].n_unique():,}")
print(f"Orders grabbed (is_courier_grabbed=1): {(waybill['is_courier_grabbed'] == 1).sum():,}")
print(f"Orders rejected (is_courier_grabbed=0): {(waybill['is_courier_grabbed'] == 0).sum():,}")
print(f"Prebook orders (is_prebook=1): {(waybill['is_prebook'] == 1).sum():,}")
print(f"Weekend orders (is_weekend=1): {(waybill['is_weekend'] == 1).sum():,}")

print("\n[8.2] Courier Wave Data")
print(f"Unique courier_id: {courier_wave['courier_id'].n_unique():,}")
print(f"Unique wave_id: {courier_wave['wave_id'].n_unique():,}")
print(f"Unique (courier_id, wave_id): {courier_wave.group_by(['courier_id', 'wave_id']).agg(pl.len()).shape[0]:,}")

print("\n[8.3] Date Coverage")
print(f"Waybill dt values: {sorted(waybill['dt'].unique().to_list())}")
print(f"Courier wave dt values: {sorted(courier_wave['dt'].unique().to_list())}")
print(f"Dispatch rider dt values: {sorted(dispatch_rider['dt'].unique().to_list())}")
print(f"Dispatch waybill dt values: {sorted(dispatch_waybill['dt'].unique().to_list())}")

print("\n" + "="*80)
print("VALIDATION COMPLETE")
print("="*80)

sys.stdout = sys.__stdout__
log.close()
print(f"\nLog saved to: {log_file}")
