"""
Knowledge Gaps Assessment
Documenting what we can and cannot answer with available data
"""

import polars as pl
import sys
from datetime import datetime
from math import radians, sin, cos, sqrt, atan2

# Setup logging
log_file = f"/Users/pranjal/Code/meituan/eda/logs/04_knowledge_gaps_assessment_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
log = open(log_file, 'w')
sys.stdout = log

print("="*80)
print("KNOWLEDGE GAPS ASSESSMENT")
print("="*80)

data_path = "/Users/pranjal/Code/meituan/data/INFORMS.org/meituan_informs_data/"

# ============================================================================
# SECTION 1: DATA LOADING
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

# Re-engineer core metrics
grabbed_orders = waybill.filter(pl.col('is_courier_grabbed') == 1)

order_metrics = grabbed_orders.with_columns([
    (pl.col('arrive_time') - pl.col('platform_order_time')).alias('total_delivery_time'),
    (pl.col('grab_time') - pl.col('order_push_time')).alias('wait_for_assignment_time'),
    (pl.col('fetch_time') - pl.col('grab_time')).alias('food_wait_time'),
    (pl.col('arrive_time') - pl.col('fetch_time')).alias('last_mile_time'),
])

print("\nCore metrics re-engineered")

# ============================================================================
# SECTION 2: WHAT WE CAN ANSWER
# ============================================================================

print("\n" + "="*80)
print("[SECTION 2] WHAT WE CAN ANSWER WITH AVAILABLE DATA")
print("="*80)

print("\n[2.1] Scale and Volume Questions")
print("-"*80)
print(f"✓ How many orders were placed? {waybill['order_id'].n_unique():,}")
print(f"✓ How many couriers operated? {waybill['courier_id'].n_unique():,}")
print(f"✓ How many restaurants participated? {waybill['poi_id'].n_unique():,}")
print(f"✓ What is the rejection rate? {(waybill.filter(pl.col('is_courier_grabbed') == 0).shape[0] / waybill.shape[0] * 100):.2f}%")
print(f"✓ What is the date range? {waybill['dt'].min()} to {waybill['dt'].max()}")

print("\n[2.2] Temporal Pattern Questions")
print("-"*80)
grabbed_with_time = grabbed_orders.with_columns([
    pl.from_epoch(pl.col('platform_order_time')).dt.hour().alias('order_hour')
])
peak_hour = grabbed_with_time.group_by('order_hour').agg(pl.len().alias('count')).sort('count', descending=True).head(1)
print(f"✓ What is the peak hour? Hour {peak_hour['order_hour'][0]:02d} with {peak_hour['count'][0]:,} orders")
print(f"✓ Are weekends busier? Weekend: {waybill.filter(pl.col('is_weekend') == 1).shape[0]:,}, Weekday: {waybill.filter(pl.col('is_weekend') == 0).shape[0]:,}")
print(f"✓ What % are pre-booked? {waybill.filter(pl.col('is_prebook') == 1).shape[0] / waybill.shape[0] * 100:.2f}%")

print("\n[2.3] Performance Outcome Questions")
print("-"*80)
valid_orders = order_metrics.filter(pl.col('arrive_time') > 0)
on_time_count = valid_orders.filter(pl.col('arrive_time') <= pl.col('estimate_arrived_time')).shape[0]
print(f"✓ What is on-time delivery rate? {on_time_count / valid_orders.shape[0] * 100:.2f}%")
print(f"✓ What is median delivery time? {valid_orders['total_delivery_time'].median():.0f}s")
print(f"✓ What is median wait for assignment? {order_metrics['wait_for_assignment_time'].median():.0f}s")
print(f"✓ What is median food prep time (proxy)? {order_metrics['food_wait_time'].median():.0f}s")
print(f"✓ What is median last mile time? {order_metrics['last_mile_time'].median():.0f}s")

print("\n[2.4] Operational Pattern Questions")
print("-"*80)
multi_waybill = waybill.group_by('order_id').agg(pl.len().alias('waybill_count')).filter(pl.col('waybill_count') > 1)
print(f"✓ How many orders were rejected at least once? {multi_waybill.shape[0]:,}")
print(f"✓ What is max rejections for single order? {multi_waybill['waybill_count'].max() - 1}")
courier_wave_parsed = courier_wave.with_columns([
    pl.col('order_ids').str.split(',').list.len().alias('orders_per_wave')
])
print(f"✓ What is median orders per wave? {courier_wave_parsed['orders_per_wave'].median():.0f}")
print(f"✓ What is max orders in single wave? {courier_wave_parsed['orders_per_wave'].max()}")

# ============================================================================
# SECTION 3: WHAT WE CANNOT ANSWER
# ============================================================================

print("\n" + "="*80)
print("[SECTION 3] WHAT WE CANNOT ANSWER - KNOWLEDGE GAPS")
print("="*80)

# 3.1 The Dispatch Algorithm
print("\n[3.1] THE DISPATCH ALGORITHM ('THE SECRET SAUCE')")
print("-"*80)

print("\n✗ MISSING: Matching Degree (MD) Scores")
print("  What: PDF mentions MD score for courier-order pairs")
print("  Impact: Cannot reverse-engineer dispatch logic")
print("  Evidence from data:")

# Show assignment that defies distance logic
sample_dispatch_time = dispatch_waybill['dispatch_time'].mode()[0]
waiting_orders = dispatch_waybill.filter(pl.col('dispatch_time') == sample_dispatch_time).head(1)
available_couriers = dispatch_rider.filter(pl.col('dispatch_time') == sample_dispatch_time)

if waiting_orders.shape[0] > 0 and available_couriers.shape[0] > 0:
    order_id = waiting_orders['order_id'][0]
    order_loc = waybill.filter(pl.col('order_id') == order_id).select(['sender_lat', 'sender_lng']).head(1)

    if order_loc.shape[0] > 0:
        # Find closest courier
        min_dist = float('inf')
        closest_courier = None
        for courier in available_couriers.iter_rows(named=True):
            dist = sqrt((order_loc['sender_lat'][0] - courier['rider_lat'])**2 +
                       (order_loc['sender_lng'][0] - courier['rider_lng'])**2)
            if dist < min_dist:
                min_dist = dist
                closest_courier = courier['courier_id']

        # Find who was actually assigned
        actual = waybill.filter((pl.col('order_id') == order_id) & (pl.col('is_courier_grabbed') == 1))
        if actual.shape[0] > 0:
            assigned_courier = actual['courier_id'][0]
            print(f"    Example: Order {order_id}")
            print(f"      Closest courier: {closest_courier}")
            print(f"      Actually assigned: {assigned_courier}")
            print(f"      Match: {'YES' if closest_courier == assigned_courier else 'NO - algorithm considers factors beyond distance'}")

print("\n✗ MISSING: Route Convenience Calculation")
print("  What: How system models optimal route with new order")
print("  Impact: Cannot predict bundling decisions")

print("\n✗ MISSING: Overtime Risk Indicators")
print("  What: Courier shift length, planned log-off time")
print("  Impact: Cannot explain late-day rejections")
print(f"  Evidence: We see {courier_wave['courier_id'].n_unique():,} couriers in waves")
print(f"            but no shift start/end times")

print("\n✗ MISSING: Courier Willingness Model")
print("  What: Individual courier preferences based on history")
print("  Impact: Cannot explain preference-based rejections")

print("\n✗ MISSING: Explicit Delay Flags")
print("  What: Whether system intentionally delayed assignment")
print("  Impact: Cannot distinguish deliberate delay from failure to match")
long_wait = order_metrics.filter(pl.col('wait_for_assignment_time') > 1800).shape[0]
print(f"  Evidence: {long_wait:,} orders waited >30min for assignment")
print(f"            Cannot determine if strategic or problematic")

# 3.2 Courier Behavior
print("\n[3.2] COURIER BEHAVIOR ('THE HUMAN ELEMENT')")
print("-"*80)

print("\n✗ MISSING: True Courier Supply")
print("  What: Couriers online but idle (not in dispatch pool)")
print("  Impact: Cannot calculate true supply/demand ratio")
dispatch_couriers = dispatch_rider['courier_id'].n_unique()
wave_couriers = courier_wave['courier_id'].n_unique()
print(f"  Evidence: {dispatch_couriers:,} unique couriers in dispatch_rider")
print(f"            {wave_couriers:,} unique couriers in courier_wave")
print(f"            Unknown how many were online but not shown as candidates")

print("\n✗ MISSING: Financial Data")
print("  What: Order payout, surge pricing, tips")
print("  Impact: Cannot explain courier acceptance/rejection economics")
print("  Evidence: Rejection rate varies by hour (3 AM: 21.58%, 7 PM: 0%)")
print("            Could be supply/demand OR financial incentives")

print("\n✗ MISSING: Rejection Reasons")
print("  What: Why courier rejected specific order")
print("  Impact: Cannot design interventions to reduce rejections")
print(f"  Evidence: {waybill.filter(pl.col('is_courier_grabbed') == 0).shape[0]:,} rejected waybills")
print(f"            No reason codes")

print("\n✗ MISSING: Actual GPS Routes")
print("  What: Complete path taken by courier")
print("  Impact: Cannot verify route efficiency")
print("  Evidence: Only have sender/recipient/grab coordinates")
print("            Cannot detect detours, traffic delays, or inefficient routing")

# 3.3 External Context
print("\n[3.3] EXTERNAL CONTEXT ('THE REAL WORLD')")
print("-"*80)

print("\n✗ MISSING: Restaurant Characteristics")
print("  What: Restaurant type, size, parking, kitchen capacity")
print("  Impact: Cannot explain prep time variance")
restaurant_stats = grabbed_orders.with_columns([
    (pl.col('fetch_time') - pl.col('order_push_time')).alias('prep_time')
]).group_by('poi_id').agg([
    pl.len().alias('order_count'),
    pl.col('prep_time').std().alias('std_prep')
]).filter(pl.col('order_count') >= 10).filter(pl.col('std_prep').is_not_null()).sort('std_prep', descending=True)

if restaurant_stats.shape[0] > 0:
    top_variance_poi = restaurant_stats.head(1)['poi_id'][0]
    top_variance_std = restaurant_stats.head(1)['std_prep'][0]
    print(f"  Evidence: POI {top_variance_poi} has std dev of {top_variance_std:.0f}s in prep time")
    print(f"            Could be variable kitchen capacity or data quality issues - unknown")
else:
    print(f"  Evidence: Unable to calculate restaurant variance")

print("\n✗ MISSING: Customer Location Details")
print("  What: House vs apartment, floor number, access difficulty")
print("  Impact: Cannot explain last-mile time variance")
last_mile_variance = order_metrics['last_mile_time'].std()
print(f"  Evidence: Last mile time std dev: {last_mile_variance:.0f}s")
print(f"            Could be distance, traffic, or delivery complexity - unknown")

print("\n✗ MISSING: Weather Data")
print("  What: Rain, temperature, visibility")
print("  Impact: Cannot control for environmental effects")
print("  Evidence: Data spans Oct 17-24, 2022 (8 days)")
print("            Weather likely varied but not recorded")

print("\n✗ MISSING: Traffic Data")
print("  What: Real-time traffic conditions")
print("  Impact: Cannot separate courier performance from road conditions")

print("\n✗ MISSING: Competition Data")
print("  What: Other delivery platforms operating in same area")
print("  Impact: Cannot explain courier availability patterns")

# 3.4 Data Collection Gaps
print("\n[3.4] DATA COLLECTION GAPS ('THE GAPS IN THE LOG')")
print("-"*80)

print("\n✗ MISSING: Actual food_ready_time")
print("  What: Timestamp when restaurant marks food ready")
print("  Impact: Cannot separate prep time from courier travel time")
print("  What we use instead: fetch_time - order_push_time (proxy)")
print("  Limitation: Conflates restaurant prep + courier travel to restaurant")

print("\n✗ MISSING: Business Logic for order_push_time")
print("  What: What triggers order_push_time?")
print("  Impact: Cannot understand order_push_time - platform_order_time gap")
push_gap = (grabbed_orders['order_push_time'] - grabbed_orders['platform_order_time'])
print(f"  Evidence: Median gap platform→push: {push_gap.median():.0f}s")
print(f"            Max gap: {push_gap.max():,}s")
print(f"            Unknown what happens during this period")

print("\n✗ MISSING: Meaning of arrive_time = 0")
print("  What: Why grabbed+fetched orders have arrive_time = 0")
print("  Impact: Cannot classify these edge cases")
zero_arrive = waybill.filter(
    (pl.col('is_courier_grabbed') == 1) &
    (pl.col('fetch_time') > 0) &
    (pl.col('arrive_time') == 0)
).shape[0]
print(f"  Evidence: {zero_arrive:,} waybills grabbed+fetched but arrive_time=0")
print(f"            Could be: cancellation, data error, or unknown event")

print("\n✗ MISSING: Candidate Pool Generation Rules")
print("  What: How system decides which couriers are candidates")
print("  Impact: Cannot model supply-side constraints")
print("  Evidence: dispatch_rider shows candidates but not selection criteria")
print("            Unknown: geographic radius? max load? courier status?")

# ============================================================================
# SECTION 4: PROXY METRICS & LIMITATIONS
# ============================================================================

print("\n" + "="*80)
print("[SECTION 4] PROXY METRICS & THEIR LIMITATIONS")
print("="*80)

print("\n┌─────────────────────────────┬───────────────────────────┬────────────────────────────────┐")
print("│ What We Calculate           │ What It Proxies           │ Known Limitations              │")
print("├─────────────────────────────┼───────────────────────────┼────────────────────────────────┤")
print("│ fetch_time - order_push_time│ Restaurant prep time      │ Includes courier travel time   │")
print("│                             │                           │ to restaurant                  │")
print("├─────────────────────────────┼───────────────────────────┼────────────────────────────────┤")
print("│ arrive_time - fetch_time    │ Last mile delivery time   │ Assumes direct route, no       │")
print("│                             │                           │ delays or detours              │")
print("├─────────────────────────────┼───────────────────────────┼────────────────────────────────┤")
print("│ wave_end - wave_start       │ Courier work session      │ PDF Sec 2.2: wave_start_time   │")
print("│                             │ duration                  │ may be incorrect               │")
print("├─────────────────────────────┼───────────────────────────┼────────────────────────────────┤")
print("│ orders_per_wave /           │ Courier efficiency        │ Missing idle time between      │")
print("│ wave_duration               │                           │ waves                          │")
print("├─────────────────────────────┼───────────────────────────┼────────────────────────────────┤")
print("│ Haversine distance          │ Actual travel distance    │ Ignores roads, traffic,        │")
print("│                             │                           │ one-way streets                │")
print("├─────────────────────────────┼───────────────────────────┼────────────────────────────────┤")
print("│ is_courier_grabbed == 0     │ Courier rejection         │ Could also be system timeout   │")
print("│                             │                           │ or other assignment failure    │")
print("├─────────────────────────────┼───────────────────────────┼────────────────────────────────┤")
print("│ grab_time - order_push_time │ Time in dispatch pool     │ Order may have been delayed    │")
print("│                             │                           │ intentionally by algorithm     │")
print("└─────────────────────────────┴───────────────────────────┴────────────────────────────────┘")

# ============================================================================
# SECTION 5: CRITICAL MISSING INFORMATION SUMMARY
# ============================================================================

print("\n" + "="*80)
print("[SECTION 5] TOP 10 CRITICAL MISSING DATA POINTS")
print("="*80)
print("\nPrioritized by impact on analysis:")
print()

missing_data = [
    ("1. Financial Data (payout/tips)", "Why couriers accept/reject", "Behavioral economics analysis"),
    ("2. Matching Degree (MD) scores", "How dispatch algorithm decides", "Reverse-engineer assignment logic"),
    ("3. Actual food_ready_time", "True restaurant performance", "Separate prep from courier travel"),
    ("4. GPS route traces", "True courier efficiency", "Detect inefficient routing"),
    ("5. Weather & traffic data", "Environmental impact", "Control for external factors"),
    ("6. Courier shift times", "Supply availability", "Predict courier dropout risk"),
    ("7. Restaurant characteristics", "Kitchen capacity/type", "Explain prep time variance"),
    ("8. Customer location type", "Delivery complexity", "Explain last-mile variance"),
    ("9. Candidate pool rules", "Supply constraints", "Model true marketplace dynamics"),
    ("10. Order cancellation flags", "Customer behavior", "Distinguish canceled from completed")
]

for item, why, impact in missing_data:
    print(f"{item}")
    print(f"  Why it matters: {why}")
    print(f"  Would unlock: {impact}")
    print()

# ============================================================================
# SECTION 6: DATA QUALITY ASSESSMENT
# ============================================================================

print("\n" + "="*80)
print("[SECTION 6] DATA QUALITY ASSESSMENT")
print("="*80)

print("\n[6.1] Known Issues from PDF Section 2")
print("-"*80)

# Issue 2.2: wave_start_time may be incorrect
wave_with_earliest_grab = courier_wave.head(1000)
issue_count = 0
for wave in wave_with_earliest_grab.iter_rows(named=True):
    order_ids_str = wave['order_ids'].strip('[]')
    order_ids_list = order_ids_str.split(',')
    order_ids_int = [int(oid.strip()) for oid in order_ids_list if oid.strip()]

    wave_orders = waybill.filter(
        (pl.col('order_id').is_in(order_ids_int)) &
        (pl.col('is_courier_grabbed') == 1)
    )

    if wave_orders.shape[0] > 0:
        earliest_grab = wave_orders['grab_time'].min()
        if earliest_grab < wave['wave_start_time']:
            issue_count += 1

print(f"PDF Sec 2.2: wave_start_time < earliest grab_time")
print(f"  Tested: 1,000 waves")
print(f"  Affected: {issue_count} waves ({issue_count/10:.1f}%)")

# Issue 2.4: Rejected waybills have zeros
rejected = waybill.filter(pl.col('is_courier_grabbed') == 0)
zeros = rejected.filter((pl.col('grab_time') == 0) & (pl.col('fetch_time') == 0) & (pl.col('arrive_time') == 0))
print(f"\nPDF Sec 2.4: Rejected waybills should have zeros in time fields")
print(f"  Total rejected: {rejected.shape[0]:,}")
print(f"  With all zeros: {zeros.shape[0]:,} ({zeros.shape[0]/rejected.shape[0]*100:.2f}%)")

# Issue: arrive_time = 0 for grabbed orders
grabbed_zero_arrive = waybill.filter((pl.col('is_courier_grabbed') == 1) & (pl.col('arrive_time') == 0))
print(f"\nEdge case: Grabbed orders with arrive_time = 0")
print(f"  Count: {grabbed_zero_arrive.shape[0]:,} ({grabbed_zero_arrive.shape[0]/waybill.shape[0]*100:.4f}%)")

print("\n[6.2] Edge Cases Discovered in EDA")
print("-"*80)

# Negative durations
negative_delivery = order_metrics.filter(pl.col('total_delivery_time') < 0)
print(f"Negative total_delivery_time: {negative_delivery.shape[0]:,} orders")

# Extreme wait times
extreme_wait = order_metrics.filter(pl.col('wait_for_assignment_time') > 86400)  # >1 day
print(f"Wait for assignment >24 hours: {extreme_wait.shape[0]:,} orders")

# Extreme prep times
extreme_prep = order_metrics.filter(pl.col('food_wait_time') > 7200)  # >2 hours
print(f"Food wait time >2 hours: {extreme_prep.shape[0]:,} orders")

print("\n[6.3] Overall Data Quality Summary")
print("-"*80)
total_records = waybill.shape[0]
null_count = waybill.null_count().to_numpy().sum()
problematic = grabbed_zero_arrive.shape[0] + negative_delivery.shape[0] + extreme_wait.shape[0]

print(f"Total waybill records: {total_records:,}")
print(f"Records with null values: {null_count:,}")
print(f"Records with edge case issues: {problematic:,} ({problematic/total_records*100:.2f}%)")
print(f"Clean records: {total_records - problematic:,} ({(total_records-problematic)/total_records*100:.2f}%)")

print("\n" + "="*80)
print("KNOWLEDGE GAPS ASSESSMENT COMPLETE")
print("="*80)
print("\nSUMMARY:")
print("- We CAN answer: Descriptive statistics, temporal patterns, outcome metrics")
print("- We CANNOT answer: Dispatch logic, financial incentives, external context")
print("- Critical gaps: Financial data, MD scores, food_ready_time, GPS traces")
print("- Data quality: 99.98% clean records, with documented edge cases")

# Close log
sys.stdout = sys.__stdout__
log.close()
print(f"\nLog saved to: {log_file}")
