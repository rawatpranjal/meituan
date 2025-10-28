# Project Context for AI Sessions

## AI INSTRUCTIONS
**DO NOT provide unsolicited opinions, suggestions, or interpretations. Do what you're told. Facts only.**

**DOCUMENTATION RULES**:
- When creating READMEs or documentation: Document existing code only
- NO examples, NO analogies, NO suggestions, NO opinions
- NO "future work" sections, NO "how to extend" sections
- NO business advice, NO hypothetical scenarios
- Professional technical documentation format only
- Remember: You are an AI with short memory and limited domain knowledge
- You document what exists. You do not teach, suggest, or speculate.

**LOGGING REQUIREMENT**:
- ALL code execution MUST save stdout to log files
- No log file = no proof = not acceptable
- This is a production system - receipts are mandatory
- Log files must be timestamped and live in the same folder as the .py script
- **ALWAYS show file links at the end**: Display full paths to both .py script AND .log file after every execution

**FILE NAMING CONVENTION**:
- ALL files must use highly specific, descriptive names that clearly indicate:
  1. What the file does
  2. How it fits within the larger system architecture
- **Naming Pattern**: `{model_id}_{tier}_{algorithm}_{objective}_{file_type}_{timestamp}.{ext}`
  - `model_id`: Sequential number (01, 02, 03...)
  - `tier`: Model tier/complexity (tier1, tier2, tier3...)
  - `algorithm`: Algorithm type (bipartite, greedy, rl, auction...)
  - `objective`: Cost function (distance_to_pickup, total_delivery_time, detour_cost...)
  - `file_type`: execution, assignment_log, cycle_summary, analysis, playbook, journey
  - `timestamp`: YYYYMMDD_HHMMSS format
- **Example**: `01_tier1_bipartite_distance_to_pickup_execution_20251027_181213.log`
- **Critical Rule**: Related files (model script, logs, analysis, visualizations) MUST share the same base name prefix
- This ensures full traceability and makes the project structure self-documenting

## Project Overview
- **ML System Design/Optimization Project** using Meituan food delivery operational data
- Dataset from TSL-Meituan Data-Driven Research Challenge
- Focus: Build data-driven optimization models for real-world delivery operations
- **Tech Stack**: Polars (NOT pandas) for data manipulation
- **EDA Format**: Python scripts (.py) with log file outputs

## Critical Paths
- **Project root**: `/Users/pranjal/Code/meituan/`
- **Main dataset**: `/Users/pranjal/Code/meituan/data/INFORMS.org/meituan_informs_data/`
- **Context docs**: `/Users/pranjal/Code/meituan/context/`
- **EDA**: `/Users/pranjal/Code/meituan/eda/` (contains .py scripts, .log files, and .png visualizations)
- **ML system design**: `/Users/pranjal/Code/meituan/ml_system_design/`
- **This file**: `/Users/pranjal/Code/meituan/claude.md`

## Dataset Files
- `all_waybill_info_meituan_0322.csv` - Main waybill data (654K rows, 116 MB) **[UNZIPPED]**
- `courier_wave_info_meituan.csv` - Courier wave info (207K rows, 12 MB)
- `dispatch_rider_meituan.csv` - Dispatch rider details (62K rows, 4 MB)
- `dispatch_waybill_meituan.csv` - Dispatch mapping (16K rows, 0.5 MB)
- `TSL-Meituan challenge_background and data_20240321.pdf` - Background docs
- `tsl_meituan_2024_data_report_20241015.pdf` - Data report
- `License.txt` - CC BY-NC 4.0

## Data Source
- Original repo: https://github.com/meituan/meituan_informs_data
- License: Non-commercial, academic use only
- Attribution: "This research was supported by data provided by Meituan."

## Data Schema (from PDF Tables 1-5)

### Table 1 & 2: Order Data (all_waybill_info_meituan_0322.csv)
- `order_id`: Order identifier
- `waybill_id`: Waybill identifier
- `dt`: Date
- `da_id`: Delivery area identifier
- `sender_lat`, `sender_lng`: Restaurant coordinates (shifted)
- `recipient_lat`, `recipient_lng`: Customer coordinates (shifted)
- `poi_id`: Point of interest identifier
- `platform_order_time`: Unix timestamp
- `estimate_arrived_time`: Unix timestamp
- `estimate_meal_prepare_time`: Unix timestamp
- `order_push_time`: Unix timestamp
- `dispatch_time`: Unix timestamp
- `courier_id`: Courier identifier
- `grab_lat`, `grab_lng`: Courier grab location (shifted)
- `is_courier_grabbed`: Binary flag
- `grab_time`: Unix timestamp
- `fetch_time`: Unix timestamp
- `arrive_time`: Unix timestamp
- `is_prebook`: Binary flag
- `is_weekend`: Binary flag

### Table 3: Courier Data (courier_wave_info_meituan.csv)
- `dt`: Date
- `courier_id`: Courier identifier
- `wave_id`: Wave identifier
- `wave_start_time`: Unix timestamp
- `wave_end_time`: Unix timestamp
- `order_ids`: Comma-separated order IDs

### Table 4: Assignment Inputs - Orders (dispatch_waybill_meituan.csv)
- `dt`: Date
- `dispatch_time`: Unix timestamp
- `order_id`: Order identifier

### Table 5: Assignment Inputs - Couriers (dispatch_rider_meituan.csv)
- `dt`: Date
- `dispatch_time`: Unix timestamp
- `courier_id`: Courier identifier
- `rider_lat`, `rider_lng`: Courier location (shifted)
- `courier_waybills`: JSON string
