# Archive

This folder contains legacy code and data from previous iterations of the food delivery optimization project.

## Contents

### `models/` (189 MB)
Previous batch dispatch system implementation using real Meituan INFORMS data:
- **Tier 1 Models**: Bipartite matching baseline (Model 01)
- **Tier 2 Models**: Batch VRP with production-ready clustering (Model 02)
- Cost function architecture for experimentation
- Legacy simulation components

**Technologies**: Polars, scipy, NetworkX, OR-Tools

### `data/` (249 MB)
Meituan INFORMS Dataset from TSL Challenge:
- `all_waybill_info_meituan_0322.csv` - 654K orders
- `courier_wave_info_meituan.csv` - Courier wave data
- `dispatch_rider_meituan.csv` - Dispatch rider details
- `dispatch_waybill_meituan.csv` - Dispatch mappings

**Source**: https://github.com/meituan/meituan_informs_data
**License**: CC BY-NC 4.0 (Non-commercial academic use only)

### `eda/` (6.8 MB)
Exploratory data analysis scripts and visualizations:
- Statistical analysis of order patterns
- Geographic distribution analysis
- Courier utilization studies
- Demand pattern characterization

### `visualizations/` (16 MB)
GIF animations from batch dispatch system showing:
- Model 01, 02, 03 performance comparisons
- Courier routing patterns
- Batch optimization processes

### `gifs/` (540 KB)
Legacy showcase GIF files from initial prototypes

### `old_simulation_files/`
Archived simulation test outputs and logs from development iterations:
- Old GIF files from algorithm development
- Historical log files
- Deprecated scripts
- Legacy analysis reports

### Extra Documentation Files
- `ALGORITHM_COMPARISON_TABLE.md` - Detailed algorithm comparison metrics
- `FIXES_RESULTS.md` - Bug fixes and performance improvements documentation
- `PERFORMANCE_ANALYSIS.md` - Deep-dive performance analysis

## Current Project

The current active project is the **Food Delivery Simulation** at the repository root, featuring:
- 5 algorithms (Greedy → Hungarian → Simple Bundling → Network Bundling → Anticipated)
- 3 test scenarios (Downtown Crush, Popup Problem, River Divide)
- Configurable YAML-based scenario system
- Real-time visualization and metrics

See the main [README.md](../README.md) for current project documentation.

## Attribution

Real-data analysis in this archive was supported by data provided by Meituan.

**Citation**:
```
This research was supported by data provided by Meituan.
Dataset: TSL-Meituan Data-Driven Research Challenge
Source: https://github.com/meituan/meituan_informs_data
License: CC BY-NC 4.0
```
