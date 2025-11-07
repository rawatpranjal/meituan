# CLAUDE.md — Execution, Logging, and Artifact Index

## 1) Scope and Objective
Defines non-negotiable execution rules, file naming, logging requirements, directory map, and the authoritative index of all produced artifacts for the Food Delivery Simulation.

## 2) Non-Negotiable Rules
- Do not include opinions, speculation, or advice.
- Execute tests and print raw results only. Print function outputs. No narratives.
- If tests fail, let them. Do not change the tests unless very explicitly told to do so. 
- Read full logs and full test outputs. Do not skim.
- Do not create new code unless explicitly instructed.
- Documentation references existing code only. No examples. No analogies. No “future work.”
- All code execution must capture stdout to a timestamped log file in the same directory as the executing `.py` script.
- After every execution, display absolute paths to the `.py` script and the `.log` file.
- Ask for explicit read/write permission before any file access or mutation.

## 3) Directory Map (authoritative)
- Project root: `/Users/pranjal/Code/meituan/`
- Core simulation: `/Users/pranjal/Code/meituan/simulator_core.py`
- Algorithms: `/Users/pranjal/Code/meituan/assignment_algorithms.py`
- Scenarios (YAML): `/Users/pranjal/Code/meituan/scenarios/`
- Outputs (metrics, GIFs, logs): `/Users/pranjal/Code/meituan/outputs/`
- Context (research): `/Users/pranjal/Code/meituan/context/`
- Archive (legacy & data): `/Users/pranjal/Code/meituan/archive/`
- This file: `/Users/pranjal/Code/meituan/claude.md`

## 4) File Naming Convention
**Pattern**: `{model_id}_{tier}_{algorithm}_{objective}_{file_type}_{timestamp}.{ext}`  
- `model_id`: `01`, `02`, `03`, …  
- `tier`: `tier1`, `tier2`, `tier3`, …  
- `algorithm`: `greedy`, `hungarian`, `simple_bundling`, `network_bundling`, `anticipated_bundling`, or other exact token used in code  
- `objective`: `distance_to_pickup`, `total_delivery_time`, `detour_cost`, …  
- `file_type`: `execution`, `assignment_log`, `cycle_summary`, `analysis`, `playbook`, `journey`  
- `timestamp`: `YYYYMMDD_HHMMSS`  
**Critical rule**: related files (script, logs, metrics, visualizations) must share the same base prefix.

## 5) Project Overview (factual)
- Food Delivery Simulation with 5 dispatch algorithms across 3 test scenarios.
- Tech: NumPy, SciPy, Matplotlib, Pillow, PyYAML.
- Outputs: animated GIFs, JSON metrics, detailed logs.

## 6) Test Scenarios
1. Downtown Crush — concentrated demand (400 orders, 3 hours)  
2. Popup Problem — bursty demand (350 orders, 4 hours)  
3. River Divide — geographic bottleneck (300 orders, 3 hours)

Scenario configurations live in `/Users/pranjal/Code/meituan/scenarios/*.yaml`.

## 7) Algorithms
1. Greedy — nearest-courier baseline  
2. Hungarian — bipartite optimal matching  
3. Simple Bundling — same-restaurant bundling  
4. Network Bundling — multi-restaurant clustering  
5. Anticipated Bundling — lookahead optimization

Algorithm implementations live in `/Users/pranjal/Code/meituan/assignment_algorithms.py`.

## 8) Execution Protocol (receipts required)
1. **Permission gate**: obtain explicit confirmation for read/write to the paths in Section 3.  
2. **Prepare run ID**: set `{model_id}`, `{tier}`, `{algorithm}`, `{objective}`, `{timestamp}` once per run.  
3. **Log binding**: bind `stdout` of the execution to a log file named with the shared base prefix.  
   - Log file location: same directory as the executing `.py` script.  
   - Log must include start time, end time, script absolute path, scenario YAML path, seed (if any), commit hash (if applicable), and full stdout.  
4. **Print raw results**: tests must print function return values and metrics. No commentary.  
5. **Display absolute paths** after completion:  
   - Executed script path  
   - Generated log file path  
6. **Artifact registration**: append a new row to the Artifact Index (Section 11) with absolute paths.

## 9) Key Entry Points
- Main runner: `run_scenario.py`  
- Scenarios: `scenarios/*.yaml`  
- Outputs: `outputs/*/metadata.json` (metrics), `outputs/*/gifs/*.gif` (visualizations)

## 10) Logging Requirements
- Timestamp format: `YYYYMMDD_HHMMSS`.  
- Log filename uses the shared base prefix from Section 4 and `file_type=execution` or `assignment_log` as applicable.  
- Log content must capture:
  - Start and end timestamps
  - Host environment summary
  - Absolute script path
  - Absolute log path
  - Scenario YAML path
  - Algorithm identifier
  - Objective identifier
  - Random seed (if used)
  - Commit hash or code snapshot reference (if available)
  - Full stdout (including printed function outputs and metrics)
  - Non-zero exit codes or exceptions with full tracebacks

## 11) Artifact Index (append one row per execution)
Columns:  
`run_timestamp | model_id | tier | algorithm | objective | scenario_yaml | script_path | log_path | metrics_json | gif_path | commit | status_code | duration_sec`

Location: `/Users/pranjal/Code/meituan/outputs/artifact_index.csv`  
- This index is authoritative for receipts.  
- All paths must be absolute.

## 12) Outputs
- Metrics JSON: `outputs/*/metadata.json`  
- Visualizations: `outputs/*/gifs/*.gif`  
- Logs: colocated with the executing `.py` script

## 13) Archive (reference only)
- Data lives under `/Users/pranjal/Code/meituan/archive/`  
- Selected files:
  - `all_waybill_info_meituan_0322.csv` (654K rows, 116 MB) [uncompressed]
  - `courier_wave_info_meituan.csv`
  - `dispatch_rider_meituan.csv`
  - `dispatch_waybill_meituan.csv`
  - `TSL-Meituan challenge_background and data_20240321.pdf`
  - `tsl_meituan_2024_data_report_20241015.pdf`
  - `License.txt` (CC BY-NC 4.0)

## 14) Data Schema (archived source summary)
**Orders** (`all_waybill_info_meituan_0322.csv`):  
`order_id, waybill_id, dt, da_id, sender_lat, sender_lng, recipient_lat, recipient_lng, poi_id, platform_order_time, estimate_arrived_time, estimate_meal_prepare_time, order_push_time, dispatch_time, courier_id, grab_lat, grab_lng, is_courier_grabbed, grab_time, fetch_time, arrive_time, is_prebook, is_weekend`

**Courier Waves** (`courier_wave_info_meituan.csv`):  
`dt, courier_id, wave_id, wave_start_time, wave_end_time, order_ids`

**Assignment Inputs — Orders** (`dispatch_waybill_meituan.csv`):  
`dt, dispatch_time, order_id`

**Assignment Inputs — Couriers** (`dispatch_rider_meituan.csv`):  
`dt, dispatch_time, courier_id, rider_lat, rider_lng, courier_waybills`

## 15) Links and Attributions
- Original data repository: https://github.com/meituan/meituan_informs_data  
- License: Non-commercial, academic use only. Attribution: “This research was supported by data provided by Meituan.”

