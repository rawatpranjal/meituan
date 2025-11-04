# Evaluation Module Documentation

## Overview

This module analyzes simulation output logs to compute performance metrics.

## Scripts

### analyze_01_tier1_bipartite_distance_to_pickup.py

Performs detailed analysis of Tier 1 baseline model simulation results.

**Input Files:**
- Assignment log CSV
- Cycle summary CSV

**Configuration:**
- `LOGS_DIR`: `/Users/pranjal/Code/meituan/models/logs`
- `MODEL_NAME`: `01_tier1_bipartite_distance_to_pickup`
- `RUN_TIMESTAMP`: Timestamp of simulation run to analyze

**Output:**
- Text log file: `{MODEL_NAME}_analysis_{timestamp}.log`

**Sections:**

1. Data Loading
   - Record counts from CSVs

2. Overall Performance Metrics
   - Assignment rate = assigned_orders / total_orders
   - Acceptance rate = accepted_assignments / proposed_assignments
   - Match rate = matches_with_actual / total_orders

3. Cost Analysis
   - Statistics on baseline_cost column (mean, median, std, percentiles)
   - Cost comparison: accepted vs rejected assignments

4. Cycle-Level Analysis
   - Supply-demand ratio statistics
   - Assignment rates in supply-constrained vs supply-abundant cycles
   - Acceptance rate variation across cycles

5. Temporal Patterns
   - Daily aggregations of assignments and costs

6. Batch Size Analysis
   - Statistics on orders per dispatch cycle
   - Statistics on couriers per dispatch cycle

7. Key Insights
   - Text summary of findings

### stakeholder_scorecard.py

Computes three-metric scorecard from simulation logs.

**Input Files:**
- Assignment log CSV
- Cycle summary CSV
- Courier timeline CSV

**Configuration:**
- `LOGS_DIR`: `/Users/pranjal/Code/meituan/models/logs`
- `MODEL_NAME`: `01_tier1_bipartite_distance_to_pickup`
- `RUN_TIMESTAMP`: Timestamp of simulation run to analyze

**Output:**
- Text log file: `{MODEL_NAME}_scorecard_{timestamp}.log`

**Metrics Computed:**

#### Metric 1: Platform - System-Wide Travel Inefficiency

**Source:** cycle_summary.csv → total_cost_of_cycle column

**Calculation:**
```python
platform_total_cost = sum(cycle_summary['total_cost_of_cycle'])
avg_cost_per_assignment = platform_total_cost / total_accepted_assignments
```

**Output:**
- Total Cost (Grid Units)
- Average Cost Per Assignment

#### Metric 2: Customer - Median Wait for Assignment Time

**Source:** assignment_log.csv → wait_for_assignment_seconds column

**Calculation:**
```python
# Filter to accepted assignments with non-null wait times
wait_times = assignment_log
    .filter(was_accepted == True)
    .filter(wait_for_assignment_seconds.is_not_null())
    .select('wait_for_assignment_seconds')

median_wait = wait_times.median()
```

**Output:**
- Median Wait (seconds)
- Median Wait (minutes)
- Distribution statistics (mean, min, max, percentiles)

#### Metric 3: Courier - Assignments Per Idle Hour

**Source:** courier_timeline.csv → timestamp, new_state columns

**Calculation:**
```python
# For each courier, sum time in AVAILABLE state
total_idle_seconds = 0
for courier in unique_couriers:
    events = timeline.filter(courier_id == courier).sort('timestamp')
    for i in range(len(events) - 1):
        if events[i]['new_state'] == 'AVAILABLE':
            idle_time = events[i+1]['timestamp'] - events[i]['timestamp']
            total_idle_seconds += idle_time

total_idle_hours = total_idle_seconds / 3600
assignments_per_idle_hour = total_accepted_assignments / total_idle_hours
```

**Output:**
- Total Idle Time (seconds and hours)
- Assignments Per Idle Hour

**Scorecard Format:**

Table with 3 stakeholder rows:
- Platform: Total detour distance, average cost per assignment
- Customer: Median wait time in seconds and minutes
- Courier: Assignments per idle hour, total idle time

## Usage

Both scripts are configured to analyze specific model runs via hardcoded RUN_TIMESTAMP variable. Update this variable to match the simulation run to be analyzed.

Execute from evaluation directory:
```bash
cd /Users/pranjal/Code/meituan/models/evaluation
python3 analyze_01_tier1_bipartite_distance_to_pickup.py
python3 stakeholder_scorecard.py
```
