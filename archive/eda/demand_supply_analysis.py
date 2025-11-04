"""
Demand-Supply Analysis for Meituan Dispatch Data

Analyzes demand (orders) and supply (couriers) dynamics across dispatch moments.
"""

import polars as pl
import matplotlib.pyplot as plt
import sys
from datetime import datetime

# Paths
DATA_PATH = "/Users/pranjal/Code/meituan/data/INFORMS.org/meituan_informs_data/"
OUTPUT_DIR = "/Users/pranjal/Code/meituan/eda"

# Setup logging
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
log_file = f"{OUTPUT_DIR}/demand_supply_analysis_{timestamp}.log"
log = open(log_file, 'w')
sys.stdout = log

print("="*80)
print("DEMAND-SUPPLY ANALYSIS")
print("="*80)
print(f"Analysis timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
print()

# ============================================================================
# SECTION 1: DATA LOADING
# ============================================================================
print("[SECTION 1] DATA LOADING")
print("-"*80)

dispatch_waybill = pl.read_csv(f"{DATA_PATH}dispatch_waybill_meituan.csv")
dispatch_rider = pl.read_csv(f"{DATA_PATH}dispatch_rider_meituan.csv")

print(f"Orders data: {dispatch_waybill.shape[0]:,} rows")
print(f"Couriers data: {dispatch_rider.shape[0]:,} rows")
print()

# ============================================================================
# SECTION 2: OVERALL STATISTICS
# ============================================================================
print("[SECTION 2] OVERALL STATISTICS")
print("-"*80)

# Demand
total_orders = dispatch_waybill.shape[0]
unique_dispatch_moments = dispatch_waybill['dispatch_time'].n_unique()

print(f"Total orders: {total_orders:,}")
print(f"Unique dispatch moments: {unique_dispatch_moments}")
print(f"Average orders per dispatch: {total_orders/unique_dispatch_moments:.1f}")
print()

# Supply
total_courier_snapshots = dispatch_rider.shape[0]
unique_couriers = dispatch_rider['courier_id'].n_unique()

print(f"Total courier snapshots: {total_courier_snapshots:,}")
print(f"Unique couriers: {unique_couriers:,}")
print(f"Average couriers per dispatch: {total_courier_snapshots/unique_dispatch_moments:.1f}")
print()

# ============================================================================
# SECTION 3: PER-DISPATCH STATISTICS
# ============================================================================
print("[SECTION 3] PER-DISPATCH STATISTICS")
print("-"*80)

# Aggregate demand per dispatch
demand_per_dispatch = dispatch_waybill.group_by('dispatch_time').agg([
    pl.len().alias('num_orders')
]).sort('dispatch_time')

# Aggregate supply per dispatch
supply_per_dispatch = dispatch_rider.group_by('dispatch_time').agg([
    pl.len().alias('num_couriers')
]).sort('dispatch_time')

# Join demand and supply
demand_supply = demand_per_dispatch.join(supply_per_dispatch, on='dispatch_time', how='inner')

# Calculate supply-demand ratio
demand_supply = demand_supply.with_columns([
    (pl.col('num_couriers') / pl.col('num_orders')).alias('supply_demand_ratio')
])

# Add datetime
demand_supply = demand_supply.with_columns([
    pl.from_epoch('dispatch_time', time_unit='s').alias('datetime')
])

print("Demand-Supply per Dispatch Moment:")
print(demand_supply)
print()

# Statistics
demand_stats = demand_supply.select([
    pl.col('num_orders').mean().alias('mean_orders'),
    pl.col('num_orders').median().alias('median_orders'),
    pl.col('num_orders').min().alias('min_orders'),
    pl.col('num_orders').max().alias('max_orders'),
    pl.col('num_orders').std().alias('std_orders'),
]).to_dicts()[0]

supply_stats = demand_supply.select([
    pl.col('num_couriers').mean().alias('mean_couriers'),
    pl.col('num_couriers').median().alias('median_couriers'),
    pl.col('num_couriers').min().alias('min_couriers'),
    pl.col('num_couriers').max().alias('max_couriers'),
    pl.col('num_couriers').std().alias('std_couriers'),
]).to_dicts()[0]

ratio_stats = demand_supply.select([
    pl.col('supply_demand_ratio').mean().alias('mean_ratio'),
    pl.col('supply_demand_ratio').median().alias('median_ratio'),
    pl.col('supply_demand_ratio').min().alias('min_ratio'),
    pl.col('supply_demand_ratio').max().alias('max_ratio'),
    pl.col('supply_demand_ratio').std().alias('std_ratio'),
]).to_dicts()[0]

print("Demand Statistics:")
print(f"  Mean: {demand_stats['mean_orders']:.1f}")
print(f"  Median: {demand_stats['median_orders']:.1f}")
print(f"  Min: {demand_stats['min_orders']}")
print(f"  Max: {demand_stats['max_orders']}")
print(f"  Std Dev: {demand_stats['std_orders']:.1f}")
print()

print("Supply Statistics:")
print(f"  Mean: {supply_stats['mean_couriers']:.1f}")
print(f"  Median: {supply_stats['median_couriers']:.1f}")
print(f"  Min: {supply_stats['min_couriers']}")
print(f"  Max: {supply_stats['max_couriers']}")
print(f"  Std Dev: {supply_stats['std_couriers']:.1f}")
print()

print("Supply-Demand Ratio Statistics:")
print(f"  Mean: {ratio_stats['mean_ratio']:.2f}")
print(f"  Median: {ratio_stats['median_ratio']:.2f}")
print(f"  Min: {ratio_stats['min_ratio']:.2f}")
print(f"  Max: {ratio_stats['max_ratio']:.2f}")
print(f"  Std Dev: {ratio_stats['std_ratio']:.2f}")
print()

# ============================================================================
# SECTION 4: TEMPORAL PATTERNS
# ============================================================================
print("[SECTION 4] TEMPORAL PATTERNS")
print("-"*80)

# Add temporal features
demand_supply_temporal = demand_supply.with_columns([
    pl.col('datetime').dt.hour().alias('hour'),
    pl.col('datetime').dt.weekday().alias('weekday'),
    pl.col('datetime').dt.date().alias('date'),
])

print("Temporal breakdown:")
print(demand_supply_temporal.select(['datetime', 'hour', 'weekday', 'date', 'num_orders', 'num_couriers', 'supply_demand_ratio']))
print()

# Group by date
daily_summary = demand_supply_temporal.group_by('date').agg([
    pl.col('num_orders').sum().alias('total_orders'),
    pl.col('num_couriers').sum().alias('total_courier_snapshots'),
    pl.col('supply_demand_ratio').mean().alias('avg_supply_demand_ratio'),
    pl.len().alias('num_dispatch_moments')
]).sort('date')

print("Daily Summary:")
print(daily_summary)
print()

# ============================================================================
# SECTION 5: SUPPLY-DEMAND IMBALANCE
# ============================================================================
print("[SECTION 5] SUPPLY-DEMAND IMBALANCE")
print("-"*80)

# Identify supply-constrained periods (ratio < 1.0)
constrained = demand_supply.filter(pl.col('supply_demand_ratio') < 1.0)
print(f"Supply-constrained dispatch moments (ratio < 1.0): {constrained.shape[0]}/{demand_supply.shape[0]}")
if constrained.shape[0] > 0:
    print("Constrained moments:")
    print(constrained.select(['dispatch_time', 'datetime', 'num_orders', 'num_couriers', 'supply_demand_ratio']))
print()

# Identify oversupply periods (ratio > 3.0)
oversupply = demand_supply.filter(pl.col('supply_demand_ratio') > 3.0)
print(f"Oversupply dispatch moments (ratio > 3.0): {oversupply.shape[0]}/{demand_supply.shape[0]}")
if oversupply.shape[0] > 0:
    print("Oversupply moments:")
    print(oversupply.select(['dispatch_time', 'datetime', 'num_orders', 'num_couriers', 'supply_demand_ratio']))
print()

# Balanced periods
balanced = demand_supply.filter(
    (pl.col('supply_demand_ratio') >= 1.0) &
    (pl.col('supply_demand_ratio') <= 3.0)
)
print(f"Balanced dispatch moments (1.0 <= ratio <= 3.0): {balanced.shape[0]}/{demand_supply.shape[0]}")
print()

# ============================================================================
# SECTION 6: VISUALIZATIONS
# ============================================================================
print("[SECTION 6] GENERATING VISUALIZATIONS")
print("-"*80)

# Convert to pandas for plotting
demand_supply_pd = demand_supply.to_pandas()

# Plot 1: Time series of demand and supply
fig, axes = plt.subplots(3, 1, figsize=(14, 10))

# Demand over time
axes[0].plot(demand_supply_pd['datetime'], demand_supply_pd['num_orders'],
            'b-o', linewidth=2, markersize=6, label='Orders')
axes[0].set_ylabel('Number of Orders', fontsize=12, fontweight='bold')
axes[0].set_title('Demand Over Time', fontsize=14, fontweight='bold')
axes[0].grid(True, alpha=0.3)
axes[0].legend()

# Supply over time
axes[1].plot(demand_supply_pd['datetime'], demand_supply_pd['num_couriers'],
            'r-o', linewidth=2, markersize=6, label='Couriers')
axes[1].set_ylabel('Number of Couriers', fontsize=12, fontweight='bold')
axes[1].set_title('Supply Over Time', fontsize=14, fontweight='bold')
axes[1].grid(True, alpha=0.3)
axes[1].legend()

# Supply-demand ratio over time
axes[2].plot(demand_supply_pd['datetime'], demand_supply_pd['supply_demand_ratio'],
            'g-o', linewidth=2, markersize=6, label='Supply/Demand Ratio')
axes[2].axhline(y=1.0, color='red', linestyle='--', linewidth=1, label='Ratio = 1.0 (Balanced)')
axes[2].set_xlabel('Time', fontsize=12, fontweight='bold')
axes[2].set_ylabel('Supply/Demand Ratio', fontsize=12, fontweight='bold')
axes[2].set_title('Supply-Demand Ratio Over Time', fontsize=14, fontweight='bold')
axes[2].grid(True, alpha=0.3)
axes[2].legend()

plt.tight_layout()
timeseries_plot = f"{OUTPUT_DIR}/demand_supply_timeseries_{timestamp}.png"
plt.savefig(timeseries_plot, dpi=150, bbox_inches='tight')
plt.close()
print(f"Saved: {timeseries_plot}")

# Plot 2: Distribution histogram
fig, axes = plt.subplots(1, 3, figsize=(16, 5))

# Demand distribution
axes[0].hist(demand_supply_pd['num_orders'], bins=15, color='blue', alpha=0.7, edgecolor='black')
axes[0].axvline(demand_stats['mean_orders'], color='red', linestyle='--', linewidth=2, label=f"Mean: {demand_stats['mean_orders']:.1f}")
axes[0].set_xlabel('Number of Orders', fontsize=12)
axes[0].set_ylabel('Frequency', fontsize=12)
axes[0].set_title('Demand Distribution', fontsize=14, fontweight='bold')
axes[0].legend()
axes[0].grid(True, alpha=0.3, axis='y')

# Supply distribution
axes[1].hist(demand_supply_pd['num_couriers'], bins=15, color='red', alpha=0.7, edgecolor='black')
axes[1].axvline(supply_stats['mean_couriers'], color='blue', linestyle='--', linewidth=2, label=f"Mean: {supply_stats['mean_couriers']:.1f}")
axes[1].set_xlabel('Number of Couriers', fontsize=12)
axes[1].set_ylabel('Frequency', fontsize=12)
axes[1].set_title('Supply Distribution', fontsize=14, fontweight='bold')
axes[1].legend()
axes[1].grid(True, alpha=0.3, axis='y')

# Ratio distribution
axes[2].hist(demand_supply_pd['supply_demand_ratio'], bins=15, color='green', alpha=0.7, edgecolor='black')
axes[2].axvline(1.0, color='red', linestyle='--', linewidth=2, label='Balanced (ratio=1.0)')
axes[2].axvline(ratio_stats['mean_ratio'], color='blue', linestyle='--', linewidth=2, label=f"Mean: {ratio_stats['mean_ratio']:.2f}")
axes[2].set_xlabel('Supply/Demand Ratio', fontsize=12)
axes[2].set_ylabel('Frequency', fontsize=12)
axes[2].set_title('Supply-Demand Ratio Distribution', fontsize=14, fontweight='bold')
axes[2].legend()
axes[2].grid(True, alpha=0.3, axis='y')

plt.tight_layout()
dist_plot = f"{OUTPUT_DIR}/demand_supply_distributions_{timestamp}.png"
plt.savefig(dist_plot, dpi=150, bbox_inches='tight')
plt.close()
print(f"Saved: {dist_plot}")

# ============================================================================
# SECTION 7: KEY FINDINGS
# ============================================================================
print()
print("="*80)
print("[SECTION 7] KEY FINDINGS")
print("="*80)
print()

print("1. OVERALL SUPPLY-DEMAND BALANCE:")
print(f"   - Average supply-demand ratio: {ratio_stats['mean_ratio']:.2f}")
print(f"   - System is {'oversupplied' if ratio_stats['mean_ratio'] > 1.5 else 'balanced' if ratio_stats['mean_ratio'] > 0.8 else 'supply-constrained'}")
print()

print("2. TEMPORAL PATTERNS:")
print(f"   - Data spans {daily_summary.shape[0]} days")
print(f"   - {unique_dispatch_moments} dispatch moments total")
print(f"   - Average {unique_dispatch_moments / daily_summary.shape[0]:.1f} dispatch moments per day")
print()

print("3. IMBALANCE PERIODS:")
print(f"   - Supply-constrained: {constrained.shape[0]}/{demand_supply.shape[0]} dispatch moments ({constrained.shape[0]/demand_supply.shape[0]*100:.1f}%)")
print(f"   - Oversupply: {oversupply.shape[0]}/{demand_supply.shape[0]} dispatch moments ({oversupply.shape[0]/demand_supply.shape[0]*100:.1f}%)")
print(f"   - Balanced: {balanced.shape[0]}/{demand_supply.shape[0]} dispatch moments ({balanced.shape[0]/demand_supply.shape[0]*100:.1f}%)")
print()

print("4. VARIABILITY:")
print(f"   - Demand CV: {demand_stats['std_orders']/demand_stats['mean_orders']:.2%}")
print(f"   - Supply CV: {supply_stats['std_couriers']/supply_stats['mean_couriers']:.2%}")
print(f"   - Ratio CV: {ratio_stats['std_ratio']/ratio_stats['mean_ratio']:.2%}")
print()

print("="*80)
print("ANALYSIS COMPLETE")
print("="*80)

# Close log
sys.stdout = sys.__stdout__
log.close()

print(f"\nAnalysis complete. Files saved:")
print(f"Log: {log_file}")
print(f"Plots: {timeseries_plot}")
print(f"       {dist_plot}")
