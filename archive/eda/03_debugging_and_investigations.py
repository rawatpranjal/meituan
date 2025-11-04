"""
Debugging and Deep Investigations
Based on idiosyncrasies found in EDA 02
"""

import polars as pl
import numpy as np
import sys
from datetime import datetime
from math import radians, sin, cos, sqrt, atan2

# Setup logging
log_file = f"/Users/pranjal/Code/meituan/eda/logs/03_debugging_and_investigations_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
log = open(log_file, 'w')
sys.stdout = log

print("="*80)
print("DEBUGGING AND DEEP INVESTIGATIONS")
print("="*80)

data_path = "/Users/pranjal/Code/meituan/data/INFORMS.org/meituan_informs_data/"

# ============================================================================
# SECTION 1: SETUP & DATA LOADING
# ============================================================================
print("\n[SECTION 1] DATA LOADING & METRIC ENGINEERING")
print("-"*80)

waybill = pl.read_csv(f"{data_path}all_waybill_info_meituan_0322.csv")
courier_wave = pl.read_csv(f"{data_path}courier_wave_info_meituan.csv")
dispatch_rider = pl.read_csv(f"{data_path}dispatch_rider_meituan.csv")
dispatch_waybill = pl.read_csv(f"{data_path}dispatch_waybill_meituan.csv")

print(f"Loaded {waybill.shape[0]:,} waybills")
print(f"Loaded {courier_wave.shape[0]:,} courier waves")
print(f"Loaded {dispatch_rider.shape[0]:,} dispatch rider records")
print(f"Loaded {dispatch_waybill.shape[0]:,} dispatch waybill records")

# Re-engineer metrics from EDA 02
grabbed_orders = waybill.filter(pl.col('is_courier_grabbed') == 1)

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

courier_wave = courier_wave.with_columns([
    pl.col('order_ids').str.split(',').list.len().alias('orders_per_wave'),
    (pl.col('wave_end_time') - pl.col('wave_start_time')).alias('wave_duration_seconds')
])

courier_wave = courier_wave.with_columns([
    (pl.col('orders_per_wave') / (pl.col('wave_duration_seconds') / 3600.0)).alias('orders_per_hour')
])

print("\nMetrics re-engineered successfully")

# ============================================================================
# SECTION 2: DEBUGGING IDIOSYNCRASIES
# ============================================================================

print("\n" + "="*80)
print("[SECTION 2] DEBUGGING IDIOSYNCRASIES")
print("="*80)

# 2.1 Negative Durations Investigation
print("\n[2.1] NEGATIVE DURATIONS INVESTIGATION")
print("-"*80)

impossible_orders = order_metrics.filter(pl.col('total_delivery_time') < 0)
print(f"\nFound {impossible_orders.shape[0]:,} orders with negative total_delivery_time")

if impossible_orders.shape[0] > 0:
    prebook_count = impossible_orders.filter(pl.col('is_prebook') == 1).shape[0]
    prebook_pct = prebook_count / impossible_orders.shape[0] * 100
    print(f"Pre-book orders among impossible orders: {prebook_count:,} ({prebook_pct:.2f}%)")

    # Check overall prebook rate for comparison
    total_prebook = order_metrics.filter(pl.col('is_prebook') == 1).shape[0]
    total_prebook_pct = total_prebook / order_metrics.shape[0] * 100
    print(f"Pre-book orders overall: {total_prebook:,} ({total_prebook_pct:.2f}%)")

    # Inspect one example
    print("\nExample Impossible Order (with timestamps):")
    example = impossible_orders.head(1).select([
        'order_id', 'waybill_id', 'is_prebook',
        'platform_order_time', 'order_push_time', 'grab_time',
        'fetch_time', 'arrive_time', 'estimate_arrived_time',
        'total_delivery_time'
    ])

    for row in example.iter_rows(named=True):
        print(f"\n  Order ID: {row['order_id']}")
        print(f"  Waybill ID: {row['waybill_id']}")
        print(f"  Is Pre-book: {row['is_prebook']}")
        print(f"  platform_order_time: {row['platform_order_time']} ({datetime.fromtimestamp(row['platform_order_time'])})")
        print(f"  order_push_time: {row['order_push_time']} ({datetime.fromtimestamp(row['order_push_time'])})")
        print(f"  grab_time: {row['grab_time']} ({datetime.fromtimestamp(row['grab_time'])})")
        print(f"  fetch_time: {row['fetch_time']} ({datetime.fromtimestamp(row['fetch_time'])})")
        print(f"  arrive_time: {row['arrive_time']} ({datetime.fromtimestamp(row['arrive_time'])})")
        print(f"  estimate_arrived_time: {row['estimate_arrived_time']} ({datetime.fromtimestamp(row['estimate_arrived_time'])})")
        print(f"  Calculated total_delivery_time: {row['total_delivery_time']}s")

    print("\nConclusion: Checking if platform_order_time is future timestamp issue")

# 2.2 Week-Long Wait Investigation
print("\n[2.2] WEEK-LONG WAIT INVESTIGATION (Order 24526)")
print("-"*80)

long_wait_order = waybill.filter(pl.col('order_id') == 24526)

if long_wait_order.shape[0] > 0:
    print(f"\nFound {long_wait_order.shape[0]} waybill(s) for order 24526")

    for row in long_wait_order.select([
        'order_id', 'waybill_id', 'is_courier_grabbed',
        'platform_order_time', 'order_push_time', 'estimate_arrived_time',
        'grab_time', 'fetch_time', 'arrive_time'
    ]).iter_rows(named=True):
        print(f"\n  Waybill ID: {row['waybill_id']}")
        print(f"  Is Grabbed: {row['is_courier_grabbed']}")
        print(f"  platform_order_time: {row['platform_order_time']} ({datetime.fromtimestamp(row['platform_order_time'])})")
        print(f"  order_push_time: {row['order_push_time']} ({datetime.fromtimestamp(row['order_push_time'])})")
        print(f"  estimate_arrived_time: {row['estimate_arrived_time']} ({datetime.fromtimestamp(row['estimate_arrived_time'])})")
        print(f"  grab_time: {row['grab_time']} ({datetime.fromtimestamp(row['grab_time']) if row['grab_time'] > 0 else 'N/A'})")

        if row['is_courier_grabbed'] == 1:
            wait_time = row['grab_time'] - row['order_push_time']
            arrive_vs_estimate = row['arrive_time'] - row['estimate_arrived_time']
            print(f"  Wait for assignment: {wait_time}s ({wait_time/60:.1f} minutes)")
            print(f"  Arrive vs Estimate: {arrive_vs_estimate}s ({'EARLY' if arrive_vs_estimate <= 0 else 'LATE'})")

    print("\nConclusion: Timestamp validation for extreme outlier")
else:
    print("\nOrder 24526 not found in dataset")

# 2.3 Hyper-Efficient Courier Investigation
print("\n[2.3] HYPER-EFFICIENT COURIER INVESTIGATION")
print("-"*80)

fastest_wave = courier_wave.filter(pl.col('wave_duration_seconds') > 0).sort('orders_per_hour', descending=True).head(1)

print("\nFastest Wave (by documented metrics):")
for row in fastest_wave.select([
    'dt', 'courier_id', 'wave_id', 'orders_per_wave',
    'wave_duration_seconds', 'orders_per_hour', 'order_ids'
]).iter_rows(named=True):
    print(f"  Date: {row['dt']}")
    print(f"  Courier: {row['courier_id']}")
    print(f"  Wave: {row['wave_id']}")
    print(f"  Documented orders_per_wave: {row['orders_per_wave']}")
    print(f"  Documented wave_duration: {row['wave_duration_seconds']}s ({row['wave_duration_seconds']/60:.1f} min)")
    print(f"  Documented efficiency: {row['orders_per_hour']:.2f} orders/hour")

    # Get actual order timings
    # Handle potential JSON format like "[123,456]" or just "123,456"
    order_ids_str = row['order_ids'].strip('[]')
    order_ids_list = order_ids_str.split(',')
    order_ids_int = [int(oid.strip()) for oid in order_ids_list if oid.strip()]

    wave_orders = waybill.filter(
        (pl.col('order_id').is_in(order_ids_int)) &
        (pl.col('is_courier_grabbed') == 1)
    )

    if wave_orders.shape[0] > 0:
        actual_start = wave_orders['grab_time'].min()
        actual_end = wave_orders['arrive_time'].max()
        actual_duration = actual_end - actual_start
        actual_efficiency = row['orders_per_wave'] / (actual_duration / 3600.0) if actual_duration > 0 else 0

        print(f"\n  Actual wave start (earliest grab): {actual_start} ({datetime.fromtimestamp(actual_start)})")
        print(f"  Actual wave end (latest arrive): {actual_end} ({datetime.fromtimestamp(actual_end)})")
        print(f"  Actual wave duration: {actual_duration}s ({actual_duration/60:.1f} min)")
        print(f"  Actual efficiency: {actual_efficiency:.2f} orders/hour")
        print(f"\n  Discrepancy: {abs(row['wave_duration_seconds'] - actual_duration)}s ({abs(row['wave_duration_seconds'] - actual_duration)/60:.1f} min)")

print("\nConclusion: Validating PDF Section 2.2 (wave_start_time may be incorrect)")

# 2.4 Restaurant Prep Time Variance
print("\n[2.4] RESTAURANT PREP TIME VARIANCE INVESTIGATION")
print("-"*80)

restaurant_metrics = grabbed_orders.with_columns([
    (pl.col('fetch_time') - pl.col('order_push_time')).alias('actual_prep_time')
])

restaurant_stats = restaurant_metrics.group_by('poi_id').agg([
    pl.len().alias('order_count'),
    pl.col('actual_prep_time').mean().alias('avg_prep_time'),
    pl.col('actual_prep_time').median().alias('median_prep_time'),
    pl.col('actual_prep_time').std().alias('std_prep_time')
]).filter(pl.col('order_count') >= 100).sort('std_prep_time', descending=True)

high_variance_poi = restaurant_stats.head(1)['poi_id'][0]

print(f"\nAnalyzing POI {high_variance_poi} (highest prep time variance):")

poi_stats = restaurant_stats.filter(pl.col('poi_id') == high_variance_poi)
for row in poi_stats.iter_rows(named=True):
    print(f"  Total orders: {row['order_count']:,}")
    print(f"  Avg prep time: {row['avg_prep_time']:.0f}s ({row['avg_prep_time']/60:.1f} min)")
    print(f"  Median prep time: {row['median_prep_time']:.0f}s ({row['median_prep_time']/60:.1f} min)")
    print(f"  Std deviation: {row['std_prep_time']:.0f}s ({row['std_prep_time']/60:.1f} min)")

# Get distribution
poi_orders = restaurant_metrics.filter(pl.col('poi_id') == high_variance_poi)

# Add food_wait_time and last_mile_time for outlier analysis
poi_orders = poi_orders.with_columns([
    (pl.col('fetch_time') - pl.col('grab_time')).alias('food_wait_time'),
    (pl.col('arrive_time') - pl.col('fetch_time')).alias('last_mile_time'),
    (pl.col('arrive_time') <= pl.col('estimate_arrived_time')).alias('on_time')
])

percentiles = poi_orders.select([
    pl.col('actual_prep_time').quantile(0.25).alias('p25'),
    pl.col('actual_prep_time').quantile(0.50).alias('p50'),
    pl.col('actual_prep_time').quantile(0.75).alias('p75'),
    pl.col('actual_prep_time').quantile(0.90).alias('p90'),
    pl.col('actual_prep_time').quantile(0.99).alias('p99'),
])

print("\nPrep Time Distribution:")
for row in percentiles.iter_rows(named=True):
    print(f"  25th percentile: {row['p25']:.0f}s")
    print(f"  50th percentile: {row['p50']:.0f}s")
    print(f"  75th percentile: {row['p75']:.0f}s")
    print(f"  90th percentile: {row['p90']:.0f}s")
    print(f"  99th percentile: {row['p99']:.0f}s")

# Top 5 outliers
outliers = poi_orders.sort('actual_prep_time', descending=True).head(5)
print("\nTop 5 Longest Prep Times for this POI:")
for row in outliers.select([
    'order_id', 'actual_prep_time', 'food_wait_time', 'last_mile_time', 'on_time'
]).iter_rows(named=True):
    print(f"  Order {row['order_id']}: prep={row['actual_prep_time']}s, food_wait={row['food_wait_time']}s, delivery={row['last_mile_time']}s, on_time={row['on_time']}")

print("\nConclusion: Prep time distribution heavily right-skewed by outliers")

# ============================================================================
# SECTION 3: FOLLOW-UP INVESTIGATIONS
# ============================================================================

print("\n" + "="*80)
print("[SECTION 3] FOLLOW-UP INVESTIGATIONS")
print("="*80)

# 3.1 Anatomy of a Dispatch Cycle
print("\n[3.1] ANATOMY OF A DISPATCH CYCLE")
print("-"*80)

# Helper function: Haversine distance
def haversine_distance(lat1, lon1, lat2, lon2):
    """Calculate distance in meters between two lat/lng points"""
    R = 6371000  # Earth radius in meters

    lat1_rad, lon1_rad = radians(lat1), radians(lon1)
    lat2_rad, lon2_rad = radians(lat2), radians(lon2)

    dlat = lat2_rad - lat1_rad
    dlon = lon2_rad - lon1_rad

    a = sin(dlat/2)**2 + cos(lat1_rad) * cos(lat2_rad) * sin(dlon/2)**2
    c = 2 * atan2(sqrt(a), sqrt(1-a))

    return R * c

# Pick a representative dispatch moment
sample_dispatch_time = dispatch_waybill['dispatch_time'].mode()[0]

print(f"\nAnalyzing dispatch_time: {sample_dispatch_time} ({datetime.fromtimestamp(sample_dispatch_time)})")

# Get waiting orders
waiting_orders = dispatch_waybill.filter(pl.col('dispatch_time') == sample_dispatch_time)
print(f"Waiting orders: {waiting_orders.shape[0]}")

# Get available couriers
available_couriers = dispatch_rider.filter(pl.col('dispatch_time') == sample_dispatch_time)
print(f"Available couriers: {available_couriers.shape[0]}")

# Supply/demand ratio
if waiting_orders.shape[0] > 0:
    supply_demand_ratio = available_couriers.shape[0] / waiting_orders.shape[0]
    print(f"Supply/Demand Ratio: {supply_demand_ratio:.2f} (couriers per waiting order)")

# Enrich waiting orders with locations
waiting_with_loc = waiting_orders.join(
    waybill.select(['order_id', 'sender_lat', 'sender_lng']),
    on='order_id',
    how='left'
)

# Sample 3 orders for detailed analysis
sample_waiting = waiting_with_loc.head(3)

print("\nDetailed Assignment Analysis for 3 Sample Orders:")

for order_row in sample_waiting.iter_rows(named=True):
    order_id = order_row['order_id']
    order_lat = order_row['sender_lat']
    order_lng = order_row['sender_lng']

    print(f"\n  Order {order_id} at location ({order_lat:.2f}, {order_lng:.2f}):")

    # Calculate distance to all available couriers
    distances = []
    for courier_row in available_couriers.iter_rows(named=True):
        dist = haversine_distance(
            order_lat, order_lng,
            courier_row['rider_lat'], courier_row['rider_lng']
        )
        distances.append({
            'courier_id': courier_row['courier_id'],
            'distance': dist,
            'current_load': courier_row['courier_waybills']
        })

    # Sort by distance
    distances_sorted = sorted(distances, key=lambda x: x['distance'])

    print(f"    Top 3 closest couriers:")
    for i, d in enumerate(distances_sorted[:3]):
        print(f"      {i+1}. Courier {d['courier_id']}: {d['distance']:.0f}m away, load: {d['current_load'][:50]}...")

    # Find who was actually assigned
    actual_assignment = waybill.filter(
        (pl.col('order_id') == order_id) &
        (pl.col('is_courier_grabbed') == 1)
    )

    if actual_assignment.shape[0] > 0:
        assigned_courier = actual_assignment['courier_id'][0]
        print(f"    Actually assigned to: Courier {assigned_courier}")

        # Find rank of assigned courier
        rank = next((i+1 for i, d in enumerate(distances_sorted) if d['courier_id'] == assigned_courier), None)
        if rank:
            print(f"    Rank by distance: {rank} out of {len(distances_sorted)}")

# 3.2 Profile of a "Rejected" Order
print("\n[3.2] PROFILE OF A REJECTED ORDER")
print("-"*80)

# Create rejection flag
waybill_with_rejection = waybill.with_columns([
    (pl.col('is_courier_grabbed') == 0).alias('is_rejected')
])

# Calculate delivery distance (Haversine)
# For efficiency, sample 10,000 orders
sample_for_distance = waybill_with_rejection.sample(n=min(10000, waybill_with_rejection.shape[0]), seed=42)

# Calculate distances using numpy for speed
sender_lats = sample_for_distance['sender_lat'].to_numpy()
sender_lngs = sample_for_distance['sender_lng'].to_numpy()
recipient_lats = sample_for_distance['recipient_lat'].to_numpy()
recipient_lngs = sample_for_distance['recipient_lng'].to_numpy()

distances = []
for i in range(len(sender_lats)):
    dist = haversine_distance(sender_lats[i], sender_lngs[i], recipient_lats[i], recipient_lngs[i])
    distances.append(dist)

sample_for_distance = sample_for_distance.with_columns([
    pl.Series('delivery_distance', distances)
])

# Compare rejected vs accepted
rejected_sample = sample_for_distance.filter(pl.col('is_rejected') == True)
accepted_sample = sample_for_distance.filter(pl.col('is_rejected') == False)

print(f"\nSample size: {sample_for_distance.shape[0]:,} orders")
print(f"Rejected: {rejected_sample.shape[0]:,} ({rejected_sample.shape[0]/sample_for_distance.shape[0]*100:.2f}%)")
print(f"Accepted: {accepted_sample.shape[0]:,} ({accepted_sample.shape[0]/sample_for_distance.shape[0]*100:.2f}%)")

print("\nDelivery Distance Comparison:")
print(f"  Rejected orders - Mean: {rejected_sample['delivery_distance'].mean():.0f}m, Median: {rejected_sample['delivery_distance'].median():.0f}m")
print(f"  Accepted orders - Mean: {accepted_sample['delivery_distance'].mean():.0f}m, Median: {accepted_sample['delivery_distance'].median():.0f}m")

# Time of day comparison
sample_with_hour = sample_for_distance.with_columns([
    pl.from_epoch(pl.col('order_push_time')).dt.hour().alias('order_hour')
])

print("\nRejection Rate by Hour of Day:")
hourly_rejection = sample_with_hour.group_by('order_hour').agg([
    pl.len().alias('total'),
    pl.col('is_rejected').sum().alias('rejected')
]).with_columns([
    (pl.col('rejected') / pl.col('total') * 100).alias('rejection_rate')
]).sort('order_hour')

for row in hourly_rejection.iter_rows(named=True):
    print(f"  Hour {row['order_hour']:02d}: {row['rejection_rate']:.2f}% rejected ({row['rejected']}/{row['total']})")

# 3.3 "Superstar" vs "Average" Courier
print("\n[3.3] SUPERSTAR VS AVERAGE COURIER COMPARISON")
print("-"*80)

# Calculate corrected wave metrics using actual grab/arrive times
# We'll parse order_ids from courier_wave and look them up in waybill
print("\nCalculating corrected wave durations...")

corrected_waves = []

for wave_row in courier_wave.iter_rows(named=True):
    order_ids_str = wave_row['order_ids'].strip('[]')
    order_ids_list = order_ids_str.split(',')
    order_ids_int = [int(oid.strip()) for oid in order_ids_list if oid.strip()]

    wave_orders = waybill.filter(
        (pl.col('order_id').is_in(order_ids_int)) &
        (pl.col('is_courier_grabbed') == 1)
    )

    if wave_orders.shape[0] > 0:
        actual_start = wave_orders['grab_time'].min()
        actual_end = wave_orders['arrive_time'].max()
        actual_duration = actual_end - actual_start

        if actual_duration > 0:
            corrected_waves.append({
                'courier_id': wave_row['courier_id'],
                'wave_id': wave_row['wave_id'],
                'orders_per_wave': wave_row['orders_per_wave'],
                'corrected_duration': actual_duration,
                'corrected_efficiency': wave_row['orders_per_wave'] / (actual_duration / 3600.0)
            })

    # Show progress every 10000 waves
    if len(corrected_waves) % 10000 == 0:
        print(f"  Processed {len(corrected_waves):,} waves...")

wave_corrected = pl.DataFrame(corrected_waves)
print(f"Successfully calculated corrected metrics for {wave_corrected.shape[0]:,} waves")

# Segment couriers by efficiency
courier_efficiency = wave_corrected.group_by('courier_id').agg([
    pl.col('corrected_efficiency').mean().alias('avg_efficiency'),
    pl.len().alias('wave_count'),
    pl.col('orders_per_wave').sum().alias('total_orders')
]).filter(pl.col('wave_count') >= 10)

# Calculate percentiles
p90 = courier_efficiency['avg_efficiency'].quantile(0.90)
p50 = courier_efficiency['avg_efficiency'].quantile(0.50)
p10 = courier_efficiency['avg_efficiency'].quantile(0.10)

print(f"\nEfficiency Percentiles (corrected):")
print(f"  90th percentile (Superstars): {p90:.2f} orders/hour")
print(f"  50th percentile (Average): {p50:.2f} orders/hour")
print(f"  10th percentile (Struggling): {p10:.2f} orders/hour")

superstars = courier_efficiency.filter(pl.col('avg_efficiency') >= p90)
average = courier_efficiency.filter((pl.col('avg_efficiency') >= p10) & (pl.col('avg_efficiency') <= p50))
struggling = courier_efficiency.filter(pl.col('avg_efficiency') <= p10)

print(f"\nSegment Sizes:")
print(f"  Superstars (top 10%): {superstars.shape[0]} couriers")
print(f"  Average (middle): {average.shape[0]} couriers")
print(f"  Struggling (bottom 10%): {struggling.shape[0]} couriers")

# Compare characteristics
for segment_name, segment_df in [('Superstars', superstars), ('Average', average), ('Struggling', struggling)]:
    print(f"\n{segment_name} Characteristics:")
    print(f"  Avg efficiency: {segment_df['avg_efficiency'].mean():.2f} orders/hour")
    print(f"  Avg total orders: {segment_df['total_orders'].mean():.1f}")
    print(f"  Avg waves worked: {segment_df['wave_count'].mean():.1f}")

# 3.4 Ripple Effect of a Single Delay
print("\n[3.4] RIPPLE EFFECT OF A SINGLE DELAY")
print("-"*80)

# Calculate 90th percentile of food_wait_time
p90_food_wait = order_metrics['food_wait_time'].quantile(0.90)
print(f"\n90th percentile food_wait_time: {p90_food_wait:.0f}s ({p90_food_wait/60:.1f} min)")

# For each wave, check if any order had extreme food wait
wave_order_mapping = waybill.filter(pl.col('is_courier_grabbed') == 1).select([
    'dt', 'courier_id', 'order_id', 'grab_time', 'fetch_time', 'arrive_time', 'estimate_arrived_time'
])

# Join with courier_wave to get wave_id
wave_order_mapping = wave_order_mapping.join(
    courier_wave.select(['dt', 'courier_id', 'wave_id', 'order_ids']),
    on=['dt', 'courier_id'],
    how='left'
)

# Calculate metrics per order
wave_order_mapping = wave_order_mapping.with_columns([
    (pl.col('fetch_time') - pl.col('grab_time')).alias('food_wait_time'),
    (pl.col('arrive_time') <= pl.col('estimate_arrived_time')).alias('on_time'),
    (pl.when(pl.col('arrive_time') > pl.col('estimate_arrived_time'))
     .then(pl.col('arrive_time') - pl.col('estimate_arrived_time'))
     .otherwise(0)).alias('lateness_seconds')
])

# Identify "patient zero" waves
wave_max_food_wait = wave_order_mapping.group_by(['dt', 'courier_id', 'wave_id']).agg([
    pl.col('food_wait_time').max().alias('max_food_wait'),
    pl.col('lateness_seconds').mean().alias('avg_lateness'),
    pl.len().alias('order_count')
]).filter(pl.col('order_count') >= 2)  # Only waves with multiple orders

infected_waves = wave_max_food_wait.filter(pl.col('max_food_wait') > p90_food_wait)
healthy_waves = wave_max_food_wait.filter(pl.col('max_food_wait') <= p90_food_wait)

print(f"\nWave Analysis:")
print(f"  Waves with 2+ orders: {wave_max_food_wait.shape[0]:,}")
print(f"  'Infected' waves (max food_wait > {p90_food_wait:.0f}s): {infected_waves.shape[0]:,}")
print(f"  'Healthy' waves: {healthy_waves.shape[0]:,}")

print(f"\nAverage Lateness Comparison:")
print(f"  Infected waves avg lateness: {infected_waves['avg_lateness'].mean():.1f}s ({infected_waves['avg_lateness'].mean()/60:.1f} min)")
print(f"  Healthy waves avg lateness: {healthy_waves['avg_lateness'].mean():.1f}s ({healthy_waves['avg_lateness'].mean()/60:.1f} min)")
print(f"  Ripple effect: {infected_waves['avg_lateness'].mean() - healthy_waves['avg_lateness'].mean():.1f}s additional lateness")

print("\n" + "="*80)
print("DEBUGGING AND INVESTIGATIONS COMPLETE")
print("="*80)

# Close log
sys.stdout = sys.__stdout__
log.close()
print(f"\nLog saved to: {log_file}")
