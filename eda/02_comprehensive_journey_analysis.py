"""
Comprehensive Journey Analysis
Based on PDF Tables 1-5 and Supplementary Document Section 2
"""

import polars as pl
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
import sys
from datetime import datetime

# Setup logging
log_file = f"/Users/pranjal/Code/meituan/eda/02_comprehensive_journey_analysis_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
log = open(log_file, 'w')
sys.stdout = log

print("="*80)
print("COMPREHENSIVE JOURNEY ANALYSIS")
print("="*80)

data_path = "/Users/pranjal/Code/meituan/data/INFORMS.org/meituan_informs_data/"

# ============================================================================
# SECTION 1: SETUP & DATA LOADING
# ============================================================================
print("\n[SECTION 1] DATA LOADING")
print("-"*80)

waybill = pl.read_csv(f"{data_path}all_waybill_info_meituan_0322.csv")
courier_wave = pl.read_csv(f"{data_path}courier_wave_info_meituan.csv")
dispatch_rider = pl.read_csv(f"{data_path}dispatch_rider_meituan.csv")
dispatch_waybill = pl.read_csv(f"{data_path}dispatch_waybill_meituan.csv")

print(f"Loaded {waybill.shape[0]:,} waybills")
print(f"Loaded {courier_wave.shape[0]:,} courier waves")
print(f"Loaded {dispatch_rider.shape[0]:,} dispatch rider records")
print(f"Loaded {dispatch_waybill.shape[0]:,} dispatch waybill records")

# ============================================================================
# SECTION 2: PHASE 1 - HIGH-LEVEL EDA
# ============================================================================

print("\n" + "="*80)
print("[SECTION 2] PHASE 1 - HIGH-LEVEL EDA")
print("="*80)

# 2.1 Data Structure & Integrity
print("\n[2.1] DATA STRUCTURE & INTEGRITY")
print("-"*80)

print("\nWaybill Data:")
print(f"  Shape: {waybill.shape}")
print(f"  Columns: {waybill.columns}")
print(f"  Null counts: {waybill.null_count().transpose()}")

print("\nCourier Wave Data:")
print(f"  Shape: {courier_wave.shape}")
print(f"  Columns: {courier_wave.columns}")
print(f"  Null counts: {courier_wave.null_count().transpose()}")

print("\nDispatch Rider Data:")
print(f"  Shape: {dispatch_rider.shape}")
print(f"  Null counts: {dispatch_rider.null_count().transpose()}")

print("\nDispatch Waybill Data:")
print(f"  Shape: {dispatch_waybill.shape}")
print(f"  Null counts: {dispatch_waybill.null_count().transpose()}")

# Check zero values (PDF Section 2.4: rejected waybills have zeros)
print("\nZero Value Analysis (Rejected Orders - PDF Section 2.4):")
zero_grab = (waybill['grab_time'] == 0).sum()
zero_fetch = (waybill['fetch_time'] == 0).sum()
zero_arrive = (waybill['arrive_time'] == 0).sum()
print(f"  grab_time = 0: {zero_grab:,}")
print(f"  fetch_time = 0: {zero_fetch:,}")
print(f"  arrive_time = 0: {zero_arrive:,}")

# 2.2 Summary Statistics
print("\n[2.2] SUMMARY STATISTICS")
print("-"*80)

print("\nID Cardinalities:")
print(f"  Unique order_ids: {waybill['order_id'].n_unique():,}")
print(f"  Unique waybill_ids: {waybill['waybill_id'].n_unique():,}")
print(f"  Unique courier_ids (waybill): {waybill['courier_id'].n_unique():,}")
print(f"  Unique courier_ids (wave): {courier_wave['courier_id'].n_unique():,}")
print(f"  Unique poi_ids: {waybill['poi_id'].n_unique():,}")
print(f"  Unique da_ids: {waybill['da_id'].n_unique():,}")

print("\nCoordinate Ranges (PDF Section 2.1: shifted coordinates):")
print(f"  sender_lat: [{waybill['sender_lat'].min():.2f}, {waybill['sender_lat'].max():.2f}]")
print(f"  sender_lng: [{waybill['sender_lng'].min():.2f}, {waybill['sender_lng'].max():.2f}]")
print(f"  recipient_lat: [{waybill['recipient_lat'].min():.2f}, {waybill['recipient_lat'].max():.2f}]")
print(f"  recipient_lng: [{waybill['recipient_lng'].min():.2f}, {waybill['recipient_lng'].max():.2f}]")

print("\nTimestamp Ranges (Unix timestamps):")
grabbed_orders = waybill.filter(pl.col('is_courier_grabbed') == 1)
print(f"  platform_order_time: [{grabbed_orders['platform_order_time'].min()}, {grabbed_orders['platform_order_time'].max()}]")
print(f"  grab_time: [{grabbed_orders['grab_time'].min()}, {grabbed_orders['grab_time'].max()}]")
print(f"  fetch_time: [{grabbed_orders['fetch_time'].min()}, {grabbed_orders['fetch_time'].max()}]")
print(f"  arrive_time: [{grabbed_orders['arrive_time'].min()}, {grabbed_orders['arrive_time'].max()}]")

# 2.3 Time Distribution
print("\n[2.3] TIME DISTRIBUTION")
print("-"*80)

# Convert timestamps to datetime for grabbed orders
grabbed_with_time = grabbed_orders.with_columns([
    pl.from_epoch(pl.col('platform_order_time')).alias('order_datetime'),
    pl.from_epoch(pl.col('grab_time')).alias('grab_datetime'),
])

# Extract hour of day
grabbed_with_time = grabbed_with_time.with_columns([
    pl.col('order_datetime').dt.hour().alias('order_hour'),
    pl.col('order_datetime').dt.weekday().alias('order_weekday'),
])

# Hour of day distribution
hour_dist = grabbed_with_time.group_by('order_hour').agg(pl.len().alias('count')).sort('order_hour')
print("\nOrder Count by Hour of Day:")
for row in hour_dist.iter_rows():
    print(f"  Hour {row[0]:02d}: {row[1]:,} orders")

# Weekday distribution (Polars weekday: 1=Monday, 7=Sunday)
weekday_dist = grabbed_with_time.group_by('order_weekday').agg(pl.len().alias('count')).sort('order_weekday')
weekday_names = {1: 'Monday', 2: 'Tuesday', 3: 'Wednesday', 4: 'Thursday', 5: 'Friday', 6: 'Saturday', 7: 'Sunday'}
print("\nOrder Count by Weekday:")
for row in weekday_dist.iter_rows():
    weekday_name = weekday_names.get(row[0], f'Unknown({row[0]})')
    print(f"  {weekday_name}: {row[1]:,} orders")

# ============================================================================
# SECTION 3: METRIC ENGINEERING
# ============================================================================

print("\n" + "="*80)
print("[SECTION 3] METRIC ENGINEERING")
print("="*80)

# 3.1 Courier Metrics
print("\n[3.1] COURIER METRICS")
print("-"*80)

# Parse order_ids from courier_wave (comma-separated string to list length)
courier_wave = courier_wave.with_columns([
    pl.col('order_ids').str.split(',').list.len().alias('orders_per_wave')
])

# Calculate wave duration
courier_wave = courier_wave.with_columns([
    (pl.col('wave_end_time') - pl.col('wave_start_time')).alias('wave_duration_seconds')
])

# Calculate efficiency (orders per hour)
courier_wave = courier_wave.with_columns([
    (pl.col('orders_per_wave') / (pl.col('wave_duration_seconds') / 3600.0)).alias('orders_per_hour')
])

print("\nWave Duration Statistics (seconds):")
print(f"  Min: {courier_wave['wave_duration_seconds'].min():,}")
print(f"  Max: {courier_wave['wave_duration_seconds'].max():,}")
print(f"  Mean: {courier_wave['wave_duration_seconds'].mean():.2f}")
print(f"  Median: {courier_wave['wave_duration_seconds'].median():.2f}")

print("\nOrders Per Wave Statistics:")
print(f"  Min: {courier_wave['orders_per_wave'].min()}")
print(f"  Max: {courier_wave['orders_per_wave'].max()}")
print(f"  Mean: {courier_wave['orders_per_wave'].mean():.2f}")
print(f"  Median: {courier_wave['orders_per_wave'].median():.2f}")

print("\nCourier Efficiency (orders per hour):")
valid_efficiency = courier_wave.filter(pl.col('wave_duration_seconds') > 0)
print(f"  Min: {valid_efficiency['orders_per_hour'].min():.2f}")
print(f"  Max: {valid_efficiency['orders_per_hour'].max():.2f}")
print(f"  Mean: {valid_efficiency['orders_per_hour'].mean():.2f}")
print(f"  Median: {valid_efficiency['orders_per_hour'].median():.2f}")

# 3.2 Order Metrics
print("\n[3.2] ORDER METRICS")
print("-"*80)

# Calculate order lifecycle metrics for grabbed orders
order_metrics = grabbed_orders.with_columns([
    (pl.col('arrive_time') - pl.col('platform_order_time')).alias('total_delivery_time'),
    (pl.col('grab_time') - pl.col('order_push_time')).alias('wait_for_assignment_time'),
    (pl.col('fetch_time') - pl.col('grab_time')).alias('food_wait_time'),
    (pl.col('arrive_time') - pl.col('fetch_time')).alias('last_mile_time'),
    (pl.col('arrive_time') <= pl.col('estimate_arrived_time')).alias('on_time'),
    (pl.when(pl.col('arrive_time') > pl.col('estimate_arrived_time'))
     .then(pl.col('arrive_time') - pl.col('estimate_arrived_time'))
     .otherwise(0)).alias('lateness_seconds')
])

print("\nTotal Delivery Time (seconds):")
print(f"  Min: {order_metrics['total_delivery_time'].min():,}")
print(f"  Max: {order_metrics['total_delivery_time'].max():,}")
print(f"  Mean: {order_metrics['total_delivery_time'].mean():.2f}")
print(f"  Median: {order_metrics['total_delivery_time'].median():.2f}")

print("\nWait for Assignment Time (seconds):")
print(f"  Min: {order_metrics['wait_for_assignment_time'].min():,}")
print(f"  Max: {order_metrics['wait_for_assignment_time'].max():,}")
print(f"  Mean: {order_metrics['wait_for_assignment_time'].mean():.2f}")
print(f"  Median: {order_metrics['wait_for_assignment_time'].median():.2f}")

print("\nFood Wait Time (seconds):")
print(f"  Min: {order_metrics['food_wait_time'].min():,}")
print(f"  Max: {order_metrics['food_wait_time'].max():,}")
print(f"  Mean: {order_metrics['food_wait_time'].mean():.2f}")
print(f"  Median: {order_metrics['food_wait_time'].median():.2f}")

print("\nLast Mile Time (seconds):")
print(f"  Min: {order_metrics['last_mile_time'].min():,}")
print(f"  Max: {order_metrics['last_mile_time'].max():,}")
print(f"  Mean: {order_metrics['last_mile_time'].mean():.2f}")
print(f"  Median: {order_metrics['last_mile_time'].median():.2f}")

print("\nOn-Time Performance:")
on_time_count = order_metrics['on_time'].sum()
total_count = order_metrics.shape[0]
print(f"  On-time deliveries: {on_time_count:,} ({on_time_count/total_count*100:.2f}%)")
print(f"  Late deliveries: {total_count - on_time_count:,} ({(total_count - on_time_count)/total_count*100:.2f}%)")

late_orders = order_metrics.filter(~pl.col('on_time'))
print(f"\nLateness for Late Orders (seconds):")
print(f"  Min: {late_orders['lateness_seconds'].min():,}")
print(f"  Max: {late_orders['lateness_seconds'].max():,}")
print(f"  Mean: {late_orders['lateness_seconds'].mean():.2f}")
print(f"  Median: {late_orders['lateness_seconds'].median():.2f}")

# 3.3 Restaurant Metrics
print("\n[3.3] RESTAURANT METRICS")
print("-"*80)

# Calculate rejection count per order
rejection_count = waybill.group_by('order_id').agg(pl.len().alias('waybill_count'))
multi_waybill = rejection_count.filter(pl.col('waybill_count') > 1)
print(f"\nOrder Rejection Analysis:")
print(f"  Orders with single waybill: {rejection_count.filter(pl.col('waybill_count') == 1).shape[0]:,}")
print(f"  Orders with multiple waybills (rejections): {multi_waybill.shape[0]:,}")
print(f"  Max waybills per order: {rejection_count['waybill_count'].max()}")

# Restaurant prep time (approximation: fetch_time - order_push_time)
restaurant_metrics = grabbed_orders.with_columns([
    (pl.col('fetch_time') - pl.col('order_push_time')).alias('actual_prep_time')
])

restaurant_stats = restaurant_metrics.group_by('poi_id').agg([
    pl.len().alias('order_count'),
    pl.col('actual_prep_time').mean().alias('avg_prep_time'),
    pl.col('actual_prep_time').median().alias('median_prep_time'),
    pl.col('actual_prep_time').std().alias('std_prep_time')
]).sort('order_count', descending=True)

print(f"\nRestaurant Performance:")
print(f"  Total restaurants: {restaurant_stats.shape[0]:,}")
print(f"\nTop 10 Busiest Restaurants:")
for i, row in enumerate(restaurant_stats.head(10).iter_rows()):
    print(f"  {i+1}. POI {row[0]}: {row[1]:,} orders, avg prep: {row[2]:.0f}s, median: {row[3]:.0f}s")

# ============================================================================
# SECTION 4: PHASE 2 - JOURNEY ANALYSIS
# ============================================================================

print("\n" + "="*80)
print("[SECTION 4] PHASE 2 - JOURNEY ANALYSIS")
print("="*80)

# 4.1 Courier Journeys (Aggregate)
print("\n[4.1] COURIER JOURNEYS - AGGREGATE VIEW")
print("-"*80)

print("\nWave Duration Distribution (in hours):")
duration_bins = [0, 1800, 3600, 7200, 10800, 14400, float('inf')]
duration_labels = ['0-30min', '30min-1h', '1-2h', '2-3h', '3-4h', '4h+']
for i in range(len(duration_bins)-1):
    count = courier_wave.filter(
        (pl.col('wave_duration_seconds') >= duration_bins[i]) &
        (pl.col('wave_duration_seconds') < duration_bins[i+1])
    ).shape[0]
    print(f"  {duration_labels[i]}: {count:,} waves")

print("\nOrders Per Wave Distribution:")
orders_bins = [0, 5, 10, 15, 20, 25, float('inf')]
orders_labels = ['1-5', '6-10', '11-15', '16-20', '21-25', '26+']
for i in range(len(orders_bins)-1):
    count = courier_wave.filter(
        (pl.col('orders_per_wave') >= orders_bins[i]) &
        (pl.col('orders_per_wave') < orders_bins[i+1])
    ).shape[0]
    print(f"  {orders_labels[i]} orders: {count:,} waves")

# 4.2 Sample Courier Journeys
print("\n[4.2] SAMPLE COURIER JOURNEYS (n=5)")
print("-"*80)

# Select 5 random couriers with substantial activity
courier_activity = courier_wave.group_by('courier_id').agg([
    pl.len().alias('wave_count'),
    pl.col('orders_per_wave').sum().alias('total_orders')
]).filter(pl.col('total_orders') >= 50).sort('total_orders', descending=True)

sample_couriers = courier_activity.head(5)['courier_id'].to_list()

print(f"\nSelected sample couriers: {sample_couriers}")

for i, courier_id in enumerate(sample_couriers):
    print(f"\nCourier {courier_id}:")
    courier_waves = courier_wave.filter(pl.col('courier_id') == courier_id)
    courier_orders = waybill.filter(
        (pl.col('courier_id') == courier_id) &
        (pl.col('is_courier_grabbed') == 1)
    )

    print(f"  Total waves: {courier_waves.shape[0]}")
    print(f"  Total orders delivered: {courier_orders.shape[0]}")
    print(f"  Avg wave duration: {courier_waves['wave_duration_seconds'].mean()/3600:.2f} hours")
    print(f"  Avg orders per wave: {courier_waves['orders_per_wave'].mean():.2f}")
    print(f"  Avg efficiency: {courier_waves['orders_per_hour'].mean():.2f} orders/hour")

# 4.3 Sample Order Journeys
print("\n[4.3] SAMPLE ORDER JOURNEYS (n=10)")
print("-"*80)

# Select 10 random grabbed orders
sample_orders = order_metrics.sample(n=10, seed=42)

print("\nSample Order Lifecycle Breakdown:")
print(f"{'Order ID':<15} {'Total(s)':<10} {'Wait(s)':<10} {'Food(s)':<10} {'Delivery(s)':<12} {'On-Time':<8}")
print("-"*75)
for row in sample_orders.select([
    'order_id', 'total_delivery_time', 'wait_for_assignment_time',
    'food_wait_time', 'last_mile_time', 'on_time'
]).iter_rows():
    print(f"{row[0]:<15} {row[1]:<10} {row[2]:<10} {row[3]:<10} {row[4]:<12} {str(row[5]):<8}")

# 4.4 Sample Restaurant Procedures
print("\n[4.4] SAMPLE RESTAURANT PROCEDURES (n=5)")
print("-"*80)

# Select top 5 restaurants by order volume
sample_restaurants = restaurant_stats.head(5)

print("\nTop 5 Busiest Restaurants Performance:")
print(f"{'POI ID':<10} {'Orders':<10} {'Avg Prep(s)':<12} {'Median Prep(s)':<15} {'Std Dev(s)':<12}")
print("-"*65)
for row in sample_restaurants.iter_rows():
    print(f"{row[0]:<10} {row[1]:<10} {row[2]:<12.0f} {row[3]:<15.0f} {row[4]:<12.0f}")

# 4.5 Order Procedures (Perfect, Rejected, Delayed)
print("\n[4.5] ORDER PROCEDURES - EDGE CASES")
print("-"*80)

# Perfect order: fast assignment
fast_assignment = order_metrics.sort('wait_for_assignment_time').head(1)
print("\nPerfect Order (fastest assignment):")
for row in fast_assignment.select([
    'order_id', 'waybill_id', 'wait_for_assignment_time',
    'food_wait_time', 'last_mile_time', 'total_delivery_time', 'on_time'
]).iter_rows():
    print(f"  Order ID: {row[0]}")
    print(f"  Waybill ID: {row[1]}")
    print(f"  Wait for assignment: {row[2]}s")
    print(f"  Food wait: {row[3]}s")
    print(f"  Last mile: {row[4]}s")
    print(f"  Total delivery: {row[5]}s")
    print(f"  On-time: {row[6]}")

# Rejected order: multiple waybills
rejected_example = waybill.join(
    rejection_count.filter(pl.col('waybill_count') > 1).head(1),
    on='order_id',
    how='inner'
).sort('waybill_id')

print("\nRejected Order Example (multiple waybills):")
for row in rejected_example.select([
    'order_id', 'waybill_id', 'courier_id', 'is_courier_grabbed',
    'order_push_time', 'grab_time'
]).iter_rows():
    grabbed_status = "GRABBED" if row[3] == 1 else "REJECTED"
    print(f"  Order ID: {row[0]}, Waybill: {row[1]}, Courier: {row[2]}, Status: {grabbed_status}")

# Delayed order: long wait for assignment
delayed_example = order_metrics.sort('wait_for_assignment_time', descending=True).head(1)
print("\nDelayed Order (longest wait for assignment):")
for row in delayed_example.select([
    'order_id', 'waybill_id', 'wait_for_assignment_time',
    'total_delivery_time', 'on_time', 'lateness_seconds'
]).iter_rows():
    print(f"  Order ID: {row[0]}")
    print(f"  Waybill ID: {row[1]}")
    print(f"  Wait for assignment: {row[2]}s ({row[2]/60:.1f} minutes)")
    print(f"  Total delivery time: {row[3]}s")
    print(f"  On-time: {row[4]}")
    if not row[4]:
        print(f"  Lateness: {row[5]}s ({row[5]/60:.1f} minutes)")

print("\n" + "="*80)
print("ANALYSIS COMPLETE")
print("="*80)

# Close log
sys.stdout = sys.__stdout__
log.close()
print(f"\nLog saved to: {log_file}")
