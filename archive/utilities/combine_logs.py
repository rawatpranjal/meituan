#!/usr/bin/env python3
"""
Combine all detailed log files from simulation scenarios.
Usage: python3 combine_logs.py [mode]
  mode = 'all' (default): combines all scenarios into one file
  mode = 'by_scenario': creates one file per scenario
  mode = 'by_algorithm': creates one file per algorithm
"""
import sys
from pathlib import Path
from datetime import datetime

OUTPUT_DIR = Path("outputs")
SCENARIOS = ["downtown_crush", "popup_problem", "river_divide"]
ALGORITHMS = [
    "greedy_baseline",
    "hungarian_route_aware",
    "simple_bundling_route_aware",
    "network_bundling",
    "anticipated_bundling_network"
]

def combine_all():
    """Combine all logs from all scenarios into one file."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"combined_all_logs_{timestamp}.txt"

    with open(output_file, 'w') as out:
        out.write("=" * 80 + "\n")
        out.write("COMBINED LOGS: ALL SCENARIOS AND ALGORITHMS\n")
        out.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
        out.write("=" * 80 + "\n\n")

        for scenario in SCENARIOS:
            out.write("\n" + "=" * 80 + "\n")
            out.write(f"SCENARIO: {scenario.upper()}\n")
            out.write("=" * 80 + "\n\n")

            for algorithm in ALGORITHMS:
                log_file = OUTPUT_DIR / scenario / "logs" / f"{algorithm}_detailed_log.txt"

                if log_file.exists():
                    out.write("-" * 80 + "\n")
                    out.write(f"Algorithm: {algorithm}\n")
                    out.write("-" * 80 + "\n")
                    out.write(log_file.read_text())
                    out.write("\n\n")
                else:
                    out.write(f"[Log file not found: {log_file}]\n\n")

    print(f"Combined all logs: {output_file}")
    print(f"Total size: {Path(output_file).stat().st_size:,} bytes")
    return output_file

def combine_by_scenario():
    """Create one combined log file per scenario."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_files = []

    for scenario in SCENARIOS:
        output_file = f"combined_{scenario}_logs_{timestamp}.txt"

        with open(output_file, 'w') as out:
            out.write("=" * 80 + "\n")
            out.write(f"COMBINED LOGS: {scenario.upper()}\n")
            out.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            out.write("=" * 80 + "\n\n")

            for algorithm in ALGORITHMS:
                log_file = OUTPUT_DIR / scenario / "logs" / f"{algorithm}_detailed_log.txt"

                if log_file.exists():
                    out.write("-" * 80 + "\n")
                    out.write(f"Algorithm: {algorithm}\n")
                    out.write("-" * 80 + "\n")
                    out.write(log_file.read_text())
                    out.write("\n\n")
                else:
                    out.write(f"[Log file not found: {log_file}]\n\n")

        output_files.append(output_file)
        print(f"Combined {scenario} logs: {output_file}")
        print(f"  Size: {Path(output_file).stat().st_size:,} bytes")

    return output_files

def combine_by_algorithm():
    """Create one combined log file per algorithm."""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_files = []

    for algorithm in ALGORITHMS:
        output_file = f"combined_{algorithm}_logs_{timestamp}.txt"

        with open(output_file, 'w') as out:
            out.write("=" * 80 + "\n")
            out.write(f"COMBINED LOGS: {algorithm.upper()}\n")
            out.write(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n")
            out.write("=" * 80 + "\n\n")

            for scenario in SCENARIOS:
                log_file = OUTPUT_DIR / scenario / "logs" / f"{algorithm}_detailed_log.txt"

                if log_file.exists():
                    out.write("-" * 80 + "\n")
                    out.write(f"Scenario: {scenario}\n")
                    out.write("-" * 80 + "\n")
                    out.write(log_file.read_text())
                    out.write("\n\n")
                else:
                    out.write(f"[Log file not found: {log_file}]\n\n")

        output_files.append(output_file)
        print(f"Combined {algorithm} logs: {output_file}")
        print(f"  Size: {Path(output_file).stat().st_size:,} bytes")

    return output_files

if __name__ == "__main__":
    mode = sys.argv[1] if len(sys.argv) > 1 else "all"

    if mode == "all":
        combine_all()
    elif mode == "by_scenario":
        combine_by_scenario()
    elif mode == "by_algorithm":
        combine_by_algorithm()
    else:
        print(f"Unknown mode: {mode}")
        print("Usage: python3 combine_logs.py [all|by_scenario|by_algorithm]")
        sys.exit(1)
