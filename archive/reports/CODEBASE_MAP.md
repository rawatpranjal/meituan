# Meituan Food Delivery Simulation - Codebase Map

## Active Project Structure

### Entry Point
```
run_scenario.py (254 lines)
├── Entry: python3 run_scenario.py scenarios/<scenario>.yaml
├── Loads: config_loader.py
├── Generates: scenario_generators/
├── Simulates: simulator_core.py + assignment_algorithms.py
└── Visualizes: create_consolidated_gifs.py + create_focused_gifs.py
```

### Core Pipeline (2,308 lines)
```
simulator_core.py (1,063 lines)
├── SimulationState class
├── Courier, Order, Restaurant classes
├── run_simulation() main engine
└── [DEPRECATED] generate_gauntlet_scenario()
    generate_scenario_legacy()
    generate_asymmetric_scenario()

assignment_algorithms.py (991 lines)
├── assign_greedy() - Algorithm 1
├── assign_hungarian() - Algorithm 2
├── assign_simple_bundling() - Algorithm 3
├── assign_network_bundling() - Algorithm 4
├── assign_anticipated_bundling() - Algorithm 5
└── Supporting: calculate_route_duration(), optimize_delivery_sequence()

config_loader.py (211 lines)
├── load_config() - YAML parsing
├── Validation and defaults
└── [UNUSED] compare_metrics()

distance_metrics.py (91 lines)
├── euclidean_distance()
├── manhattan_distance()
└── get_distance_metric() factory function
```

### Scenario Generation (951 lines)
```
scenario_generators/
├── __init__.py (26 lines) - ScenarioFactory
├── demand_generators.py (483 lines) - Order generation
├── layout_generators.py (318 lines) - Restaurant/courier placement
└── scenario_factory.py (124 lines) - Main factory class
```

### Visualization Pipeline
```
create_consolidated_gifs.py
├── Called by: run_scenario.py (line 189)
└── Creates: 60-frame animated GIFs + calls export_detailed_logs()

create_focused_gifs.py
├── Called by: run_scenario.py (line 207)
└── Creates: 7-frame key moment GIFs + calls export_detailed_logs()

export_detailed_logs.py
├── Called by: create_consolidated_gifs.py
├── Called by: create_focused_gifs.py
└── Purpose: Converts simulation events to human-readable logs
```

### Test Suite (7 files, ~250 test functions)
```
tests/
├── test_greedy_algorithm.py (20 tests) ✓ ACTIVE
├── test_hungarian_algorithm.py (48 tests) ✓ ACTIVE
├── test_simple_bundling_algorithm.py (6 tests) ✓ ACTIVE
├── test_network_bundling_algorithm.py (28 tests) ✓ ACTIVE [Comprehensive]
├── test_network_bundling.py (4 tests) ✓ ACTIVE [Comparative]
├── test_anticipated_bundling.py (20 tests) ✓ ACTIVE
└── diagnose_network_vs_simple.py ✓ ACTIVE [Diagnostic]
```

---

## Unused / Deprecated Code

### Standalone Scripts NOT Called by Active Pipeline
```
analyze_batch_distinctness.py (9.6 KB) ✗ UNUSED
├── Called by: run_small_town_simulation.py only
├── Purpose: Identifies distinct batch decisions
└── Issue: References analysis/event_data/ which isn't generated

create_results_table.py (9.9 KB) ✗ UNUSED
├── Called by: run_small_town_simulation.py only
├── Purpose: Creates performance comparison table
└── Issue: Parses old log format (now uses metadata.json)

run_small_town_simulation.py (3.4 KB) ✗ DEPRECATED
├── Purpose: Old orchestration script for "Small Town" scenario
├── Called by: None (manual entry point, superseded)
└── Issue: Calls old scripts (analyze_batch, create_results)
```

### Untracked Debug/Test Files
```
debug_simple_bundling.py (6.4 KB) ✗ UNTRACKED
├── Git status: ?? (never committed)
├── Purpose: Manual debugging script
└── Issue: Not integrated into test suite

test_simple_bundling_final.py (11.7 KB) ✗ UNTRACKED
├── Git status: ?? (never committed)
├── Purpose: Test suite (corrected version)
└── Issue: Duplicates tests/test_simple_bundling_algorithm.py
```

### Deprecated Functions in Active Files
```
simulator_core.py:
├── generate_gauntlet_scenario() - NOT CALLED
├── generate_scenario_legacy() - NOT CALLED
├── generate_asymmetric_scenario() - NOT CALLED
└── generate_scenario alias (backward compat) - NOT CALLED

config_loader.py:
└── compare_metrics() - NOT CALLED
```

---

## Archive Folder (462 MB, 66 Python files)

```
archive/
├── data/ - Original Meituan dataset (non-commercial academic license)
├── eda/ - Old exploratory data analysis (6 files)
├── models/ - Old batch assignment models
│   ├── Tier 1: Bipartite matching
│   ├── Tier 2: Batch VRP
│   └── Tier 3: Online greedy
└── old_simulation_files/ - Legacy visualization system
    ├── Various GIF generation approaches
    └── Old script orchestration
```

---

## Dependency Graph: What's Actually Used

```
ENTRY POINT
    ↓
run_scenario.py ─────────────────────────────────┐
    ├─→ config_loader.py                         │
    ├─→ scenario_generators/                     │
    │   ├→ demand_generators.py                  │
    │   └→ layout_generators.py                  │
    ├─→ simulator_core.py                        │
    │   ├→ distance_metrics.py                   │
    │   └→ [imports Assignment Algorithms]       │
    ├─→ assignment_algorithms.py                 │
    │   ├→ simulator_core.py                     │
    │   └→ distance_metrics.py                   │
    │                                            │
    ├─→ [SUBPROCESS] create_consolidated_gifs.py │
    │   ├→ simulator_core.py                     │
    │   ├→ assignment_algorithms.py              │
    │   └─→ export_detailed_logs.py              │
    │                                            │
    └─→ [SUBPROCESS] create_focused_gifs.py      │
        ├→ simulator_core.py                     │
        ├→ assignment_algorithms.py              │
        └─→ export_detailed_logs.py              │
```

---

## Cleanup Recommendations

### SAFE TO REMOVE (No Dependencies)
```
✗ debug_simple_bundling.py (delete or archive)
✗ test_simple_bundling_final.py (delete or archive)
✗ analyze_batch_distinctness.py (archive)
✗ create_results_table.py (archive)
✗ run_small_town_simulation.py (archive)
```

### SAFE TO REMOVE FROM simulator_core.py
```
✗ generate_gauntlet_scenario() [3-4 lines]
✗ generate_scenario_legacy() [80 lines]
✗ generate_asymmetric_scenario() [90 lines]
✗ generate_scenario alias
```

### SAFE TO REMOVE FROM config_loader.py
```
✗ compare_metrics() [~10 lines, unused utility]
```

### KEEP (Different Purposes)
```
✓ tests/test_network_bundling_algorithm.py [Comprehensive unit tests]
✓ tests/test_network_bundling.py [Comparative/diagnostic tests]
```

---

## Statistics

| Category | Count | Lines | Status |
|----------|-------|-------|--------|
| Core Pipeline Files | 3 | 2,308 | ✓ Active |
| Support Libraries | 2 | 302 | ✓ Active |
| Scenario Generators | 4 | 951 | ✓ Active |
| Test Files | 7 | ~2,000 | ✓ Active |
| Visualization Scripts | 3 | ~2,000 | ✓ Used by pipeline |
| Unused Standalone | 4 | ~700 | ✗ Unused |
| Untracked Debug/Test | 2 | ~500 | ✗ Untracked |
| **Total Active** | **20** | **~5,500** | ✓ |
| Archived Files | 66 | ~10,000+ | Archive |
| Archive Size | - | **462 MB** | - |

---

## Files Currently Used by run_scenario.py

```
✓ config_loader.py
✓ scenario_generators/ (all 4 modules)
✓ simulator_core.py
✓ assignment_algorithms.py
✓ distance_metrics.py
✓ create_consolidated_gifs.py (subprocess)
✓ create_focused_gifs.py (subprocess)
✓ export_detailed_logs.py (called by GIF scripts)
```

All other Python files in root directory are **UNUSED** by the main pipeline.

