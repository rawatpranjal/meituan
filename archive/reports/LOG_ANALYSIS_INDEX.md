# Log File Analysis - Complete Documentation

This directory contains comprehensive analysis of all log files in the Meituan Food Delivery Simulation repository.

## Files Generated

### 1. LOG_SUMMARY_TABLE.txt (Quick Reference)
**File Size:** 12 KB | **Lines:** 155
**Purpose:** Fast lookup table for log categories and recommended actions

Contains:
- Quick stats summary
- Organized table of all 138 log files by category
- Active production results (15 files, 762 KB) - KEEP
- Intermediate/debug logs (28 files, 194 KB) - DELETE
- Deprecated archive logs (87 files, 188 MB) - DELETE
- Git staged deletions overview
- Recommended git commands to execute

**Use this file when:** You need a quick overview or want to find specific log categories

---

### 2. LOG_ANALYSIS_REPORT.txt (Comprehensive Analysis)
**File Size:** 24 KB | **Lines:** 561
**Purpose:** Detailed analysis with context, architecture, and decision-making rationale

Contains 10 major sections:
1. Executive Summary
2. Active Log Locations & Usage (with full file listings)
3. Deprecated/Archive Log Locations (with generation context)
4. Deleted Files in Git (pending commit)
5. Log Generation Architecture (how logs are created)
6. Storage Analysis (disk usage breakdown)
7. Git Status Summary
8. Log Retention Policy Recommendations (4 tiers)
9. Deletion Checklist (executable commands)
10. Total Cleanup Impact (before/after stats)
11. Recent Commit Timeline
12. Conclusion & Recommendations

**Use this file when:** You need detailed context about why logs exist, how they're generated, or to understand the full cleanup strategy

---

## Key Findings Summary

### Current State
- **138 total log files** across entire repository
- **~189 MB total** storage used by logs
- **21 MB** in active production (keep)
- **168 MB** in deprecated/intermediate (delete)

### Active Production Results (KEEP)
- **Location:** `outputs/{scenario}/logs/*.txt`
- **Count:** 15 files, 762 KB
- **Scenarios:** downtown_crush, popup_problem, river_divide
- **Algorithms:** 5 per scenario (greedy, hungarian, simple_bundling, network_bundling, anticipated_bundling)
- **Git Status:** Modified (M) - tracked, ready to commit
- **Generated:** Nov 5, 2025
- **Usage:** Core documentation of algorithm performance

### Intermediate/Debug Logs (DELETE)
- **Location:** `logs/*.log`, `tests/logs/*.log`, `tests/*.log`
- **Count:** 28 files, 194 KB
- **Git Status:** Untracked (??)
- **Generated:** Nov 4-5, 2025
- **Reason:** Development/testing only, not used in final output
- **Safe to Delete:** YES

### Deprecated Archive (DELETE)
- **Locations:**
  - `archive/eda/logs/` (20 files, 156 KB)
  - `archive/models/logs/` (37 files, **188 MB** - largest)
  - `archive/old_simulation_files/old_logs/` (29 files, 240 KB)
- **Total:** 86 files, 188.4 MB
- **Status:** Code deprecated, replaced by new architecture
- **Safe to Delete:** YES

### Git Staged Deletions
- **Count:** ~90 files marked " D" (space-D in git status)
- **Content:** Old versions of scenario results (_before_fixes, _v2_after_*_fix)
- **Action:** Just need to commit to finalize
- **Command:** `git commit -m "Cleanup: Remove intermediate versions"`

---

## How Logs Are Generated

```
run_scenario.py (entry point)
 ├─> Runs simulator for each algorithm
 ├─> Calls create_consolidated_gifs.py
 │   ├─> Generates 60-frame GIFs
 │   ├─> Calls export_detailed_logs.py
 │   │   └─> Writes to outputs/{scenario}/logs/{algorithm}_detailed_log.txt
 │   └─> Saves intermediate logs to logs/{scenario}.log (temporary)
 ├─> Calls create_focused_gifs.py (7-frame GIFs)
 └─> Calls create_results_table.py (comparison table)

tests/test_*.py (development)
 └─> Generates logs to tests/logs/test_*.log (not used in output)
```

---

## Recommended Actions

### Immediate (Safe, No Risk)
1. Delete intermediate logs:
   ```bash
   rm logs/*.log
   rm tests/logs/*.log
   rm tests/*.log
   ```

2. Commit production results:
   ```bash
   git add outputs/*/logs/*.txt
   git commit -m "Update: Final scenario results with improved algorithms"
   ```

3. Finalize staged deletions:
   ```bash
   git commit -m "Cleanup: Remove intermediate versions and debug logs"
   ```

### Important (Space Savings)
4. Delete deprecated archives (188 MB recovery):
   ```bash
   rm -rf archive/eda/logs/*
   rm -rf archive/models/logs/*
   rm -rf archive/old_simulation_files/old_logs/*
   ```

### Optional (Code Cleanliness)
5. Add log files to .gitignore:
   ```bash
   echo "logs/*.log" >> .gitignore
   echo "tests/logs/*.log" >> .gitignore
   echo "tests/*.log" >> .gitignore
   git add .gitignore
   git commit -m "Config: Add log files to gitignore"
   ```

---

## Log Retention Policy (Recommended)

| Tier | Location | Retention | Reason |
|------|----------|-----------|--------|
| TIER 1 | `outputs/*/logs/*.txt` | FOREVER | Core documentation of algorithm performance |
| TIER 2 | `logs/*.log`, `tests/logs/*.log` | 1 week | Development debugging, historical value unclear |
| TIER 3 | `archive/*/logs/*` | DELETE NOW | Code deprecated, logs no longer needed |
| TIER 4 | `*.txt` (analysis docs) | CONDITIONAL | Review content, keep if useful reference |

---

## Storage Impact

### Before Cleanup
- Active: 762 KB
- Intermediate: 194 KB
- Deprecated: 188 MB
- **TOTAL: 189 MB**

### After Cleanup
- Active: 762 KB
- Intermediate: 0 B (deleted)
- Deprecated: 0 B (deleted)
- **TOTAL: 762 KB**

### Savings: 188.2 MB (99.6% reduction in logs)

---

## Git Status Interpretation

```
M  = Modified (tracked file, changes ready to commit)
?? = Untracked (file in working directory, not in git)
 D = Deleted (staged for deletion, ready to commit with space prefix)
```

Current state:
- 15 files marked "M" (production logs) - ready to commit
- 38 files marked "??" (development logs) - can delete or ignore
- ~90 files marked " D" (old versions) - ready to commit for deletion

---

## Timestamps & Development Timeline

### Oct 27 - Oct 29
- Initial EDA (Exploratory Data Analysis)
- Development of tier-based model system
- Multiple iterations, many test runs

### Oct 27 - Nov 3
- Old simulation visualization iterations
- Dashboard and GIF generation experiments
- Multiple versions stored (_before_fixes, _v2_after_*_fix patterns)

### Nov 2 - Nov 5
- Final architecture stabilized
- New simulator_core.py, export_detailed_logs.py, run_scenario.py
- Clean 5-algorithm system (greedy, hungarian, simple_bundling, network_bundling, anticipated_bundling)
- Final runs on Nov 5 generated latest logs

---

## Files Referenced in This Analysis

**Report Files Created:**
1. `/Users/pranjal/Code/meituan/LOG_ANALYSIS_REPORT.txt` - Main detailed analysis
2. `/Users/pranjal/Code/meituan/LOG_SUMMARY_TABLE.txt` - Quick reference table
3. `/Users/pranjal/Code/meituan/LOG_ANALYSIS_INDEX.md` - This file (index)

**Key Source Files:**
- `/Users/pranjal/Code/meituan/run_scenario.py` - Entry point
- `/Users/pranjal/Code/meituan/export_detailed_logs.py` - Log generation
- `/Users/pranjal/Code/meituan/simulator_core.py` - Core simulation engine
- `/Users/pranjal/Code/meituan/assignment_algorithms.py` - Algorithm implementations

---

## How to Use These Reports

### Quick Lookup
- Start with **LOG_SUMMARY_TABLE.txt**
- Find your category of interest in the tables
- See file count, size, and recommended action

### Deep Dive
- Read **LOG_ANALYSIS_REPORT.txt** for complete context
- Understand why logs exist and how they're generated
- Learn the retention policy reasoning
- Follow the execution checklist

### Reference
- Keep this **LOG_ANALYSIS_INDEX.md** as navigation
- Links between all three documents
- Quick command reference for cleanup

---

## Next Steps

1. Review both analysis files (suggested reading order: TABLE → REPORT → this INDEX)
2. Verify recommendations align with your project goals
3. Execute recommended actions in order (safest first)
4. Monitor git history to ensure cleanup was successful

---

**Generated:** 2025-11-05
**Total Files Analyzed:** 138
**Repository:** `/Users/pranjal/Code/meituan/`
**Analysis Scope:** All log files, git status, storage usage, generation architecture

