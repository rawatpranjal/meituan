#!/usr/bin/env python3
"""
Master script for "Small Town Lunch Rush" simulation.

Executes complete simulation pipeline in single command:
1. Clean up old logs, GIFs, and analysis files
2. Generate consolidated GIFs (60 frames + detailed logs)
3. Analyze batch distinctness
4. Generate focused GIFs (7 frames)
5. Generate results comparison table

Each run deletes previous outputs and creates fresh files - no accumulation.

Usage:
    python3 run_small_town_simulation.py
"""

import subprocess
import sys
import os
import glob
from datetime import datetime

# Scripts to run in sequence
SCRIPTS = [
    ('create_consolidated_gifs.py', 'Generating consolidated 60-frame GIFs and detailed logs'),
    ('analyze_batch_distinctness.py', 'Analyzing batch distinctness for focused view'),
    ('create_focused_gifs.py', 'Generating focused 7-frame GIFs'),
    ('create_results_table.py', 'Generating performance comparison table'),
]

def cleanup_old_outputs():
    """Delete all old logs and GIFs before starting new run."""
    print('🗑️  Cleaning up old outputs...')

    # Clean logs directory
    log_files = glob.glob('logs/*.log') + glob.glob('logs/*.txt')
    for log_file in log_files:
        try:
            os.remove(log_file)
            print(f'  Deleted: {log_file}')
        except Exception as e:
            print(f'  Warning: Could not delete {log_file}: {e}')

    # Clean gifs directory
    gif_files = glob.glob('gifs/*.gif')
    for gif_file in gif_files:
        try:
            os.remove(gif_file)
            print(f'  Deleted: {gif_file}')
        except Exception as e:
            print(f'  Warning: Could not delete {gif_file}: {e}')

    # Clean analysis directory
    analysis_files = glob.glob('analysis/*.json')
    for analysis_file in analysis_files:
        try:
            os.remove(analysis_file)
            print(f'  Deleted: {analysis_file}')
        except Exception as e:
            print(f'  Warning: Could not delete {analysis_file}: {e}')

    print(f'✓ Cleanup complete: {len(log_files)} logs, {len(gif_files)} GIFs, {len(analysis_files)} analysis files removed')
    print()

def main():
    print('=' * 70)
    print('SMALL TOWN LUNCH RUSH SIMULATION')
    print('=' * 70)
    print(f'Started: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    print()

    # Clean up old outputs first
    cleanup_old_outputs()

    for i, (script, description) in enumerate(SCRIPTS, 1):
        print(f'\n[{i}/{len(SCRIPTS)}] {description}')
        print('-' * 70)
        print(f'Running: {script}')
        print()

        result = subprocess.run([sys.executable, script], capture_output=False)

        if result.returncode != 0:
            print()
            print('=' * 70)
            print(f'❌ ERROR: {script} failed with exit code {result.returncode}')
            print('=' * 70)
            sys.exit(1)

        print()
        print(f'✓ {script} completed successfully')

    print()
    print('=' * 70)
    print('✅ ALL SIMULATION OUTPUTS GENERATED SUCCESSFULLY')
    print('=' * 70)
    print(f'Finished: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    print()
    print('📁 Output locations:')
    print('  • GIFs: gifs/')
    print('  • Detailed logs: logs/*_detailed_log.txt')
    print('  • Performance table: stdout (above)')
    print()

if __name__ == '__main__':
    main()
