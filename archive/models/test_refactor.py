#!/usr/bin/env python3
"""
Test script to validate the refactored dual-mode dispatch system.

This script:
1. Tests both batch and real-time modes
2. Tests all strategies (greedy, batch_greedy, hungarian)
3. Compares results with original models where possible
4. Validates key system invariants
"""
import subprocess
import sys
import os
import pandas as pd
from pathlib import Path
import logging
from datetime import datetime

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def run_simulation(mode, strategy, bundling='off', micro_batch_sec=10):
    """Run a simulation with specified parameters."""
    cmd = [
        'python', '-m', 'models.run',
        '--mode', mode,
        '--strategy', strategy,
        '--bundling', bundling,
        '--cost', 'distance_to_pickup',
        '--seed', '42'
    ]

    if mode == 'realtime':
        cmd.extend(['--micro-batch-sec', str(micro_batch_sec)])

    logger.info(f"Running: {' '.join(cmd)}")

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            cwd='/Users/pranjal/Code/meituan'
        )

        if result.returncode != 0:
            logger.error(f"Command failed with return code {result.returncode}")
            logger.error(f"STDERR: {result.stderr}")
            return None

        # Parse output to get log file paths
        output_lines = result.stdout.split('\n')
        log_files = {}

        for line in output_lines:
            if 'Assignment log:' in line:
                log_files['assignment'] = line.split(': ')[1].strip()
            elif 'Cycle summary:' in line:
                log_files['cycle'] = line.split(': ')[1].strip()
            elif 'Manifest:' in line:
                log_files['manifest'] = line.split(': ')[1].strip()

        return log_files

    except Exception as e:
        logger.error(f"Error running simulation: {e}")
        return None


def validate_invariants(assignment_log_path, cycle_log_path):
    """Validate key system invariants."""
    invariants = {
        'conservation_of_orders': True,
        'courier_uniqueness': True,
        'time_monotonicity': True,
        'rejection_rate': True
    }

    try:
        # Read logs
        assignments = pd.read_csv(assignment_log_path)
        cycles = pd.read_csv(cycle_log_path)

        # Invariant 1: Conservation of orders
        # orders_in = assigned + rejected + deferred
        for _, cycle in cycles.iterrows():
            total_in = cycle['num_orders_in_batch']
            assigned = cycle['num_accepted_assignments']
            rejected = cycle['num_rejections']
            deferred = cycle.get('num_deferred_out', 0)

            if abs((assigned + rejected + deferred) - total_in) > total_in * 0.01:  # 1% tolerance
                logger.warning(f"Conservation violation: {total_in} != {assigned} + {rejected} + {deferred}")
                invariants['conservation_of_orders'] = False

        # Invariant 2: Courier uniqueness (no courier assigned twice in same cycle)
        for dispatch_time in assignments['dispatch_time'].unique():
            cycle_assignments = assignments[
                (assignments['dispatch_time'] == dispatch_time) &
                (assignments['was_accepted'] == True)
            ]
            courier_counts = cycle_assignments['baseline_assigned_courier_id'].value_counts()

            # Check if any courier appears more than expected (bundling allows multiple)
            if 'bundle_size' in cycle_assignments.columns:
                max_bundle = cycle_assignments['bundle_size'].max()
            else:
                max_bundle = 1

            if (courier_counts > max_bundle * 1.5).any():  # Allow some bundling
                logger.warning(f"Courier uniqueness violation at time {dispatch_time}")
                invariants['courier_uniqueness'] = False

        # Invariant 3: Rejection rate should be around 13.11%
        total_proposed = cycles['num_proposed_assignments'].sum()
        total_rejected = cycles['num_rejections'].sum()
        if total_proposed > 0:
            actual_rejection_rate = total_rejected / total_proposed
            expected_rejection_rate = 0.1311

            if abs(actual_rejection_rate - expected_rejection_rate) > 0.02:  # 2% tolerance
                logger.warning(f"Rejection rate deviation: {actual_rejection_rate:.4f} vs {expected_rejection_rate:.4f}")
                invariants['rejection_rate'] = False

        logger.info(f"Invariant validation results: {invariants}")
        return all(invariants.values())

    except Exception as e:
        logger.error(f"Error validating invariants: {e}")
        return False


def compare_with_baseline(new_log_path, baseline_pattern):
    """Compare new results with original model results."""
    try:
        # Find most recent baseline log
        log_dir = Path('/Users/pranjal/Code/meituan/models/logs')
        baseline_files = list(log_dir.glob(baseline_pattern))

        if not baseline_files:
            logger.warning(f"No baseline files found matching {baseline_pattern}")
            return None

        baseline_file = max(baseline_files, key=lambda f: f.stat().st_mtime)
        logger.info(f"Comparing with baseline: {baseline_file}")

        # Read both logs
        new_df = pd.read_csv(new_log_path)
        baseline_df = pd.read_csv(baseline_file)

        # Compare key metrics
        metrics = {}

        if 'assignment_rate' in new_df.columns and 'assignment_rate' in baseline_df.columns:
            metrics['assignment_rate_diff'] = abs(
                new_df['assignment_rate'].mean() - baseline_df['assignment_rate'].mean()
            )

        if 'avg_cost_per_assignment' in new_df.columns and 'avg_cost_per_assignment' in baseline_df.columns:
            metrics['avg_cost_diff'] = abs(
                new_df['avg_cost_per_assignment'].mean() - baseline_df['avg_cost_per_assignment'].mean()
            )

        logger.info(f"Comparison metrics: {metrics}")

        # Check if differences are within acceptable range
        acceptable = True
        if metrics.get('assignment_rate_diff', 0) > 0.05:  # 5% tolerance
            logger.warning("Assignment rate differs significantly from baseline")
            acceptable = False

        if metrics.get('avg_cost_diff', 0) > 1.0:  # Cost tolerance
            logger.warning("Average cost differs significantly from baseline")
            acceptable = False

        return acceptable

    except Exception as e:
        logger.error(f"Error comparing with baseline: {e}")
        return None


def main():
    """Run comprehensive validation tests."""
    logger.info("="*60)
    logger.info("REFACTOR VALIDATION TEST SUITE")
    logger.info("="*60)

    test_results = {}

    # Test 1: Batch mode with Hungarian (should match Model 01)
    logger.info("\nTest 1: Batch mode with Hungarian strategy")
    log_files = run_simulation('batch', 'hungarian')
    if log_files:
        passed = validate_invariants(log_files['assignment'], log_files['cycle'])
        baseline_match = compare_with_baseline(
            log_files['cycle'],
            '01_tier1_bipartite_distance_to_pickup_cycle_summary_*.csv'
        )
        test_results['batch_hungarian'] = passed and (baseline_match is not False)
    else:
        test_results['batch_hungarian'] = False

    # Test 2: Batch mode with Greedy
    logger.info("\nTest 2: Batch mode with Greedy strategy")
    log_files = run_simulation('batch', 'greedy')
    if log_files:
        passed = validate_invariants(log_files['assignment'], log_files['cycle'])
        test_results['batch_greedy'] = passed
    else:
        test_results['batch_greedy'] = False

    # Test 3: Batch mode with Batch-Greedy
    logger.info("\nTest 3: Batch mode with Batch-Greedy strategy")
    log_files = run_simulation('batch', 'batch_greedy')
    if log_files:
        passed = validate_invariants(log_files['assignment'], log_files['cycle'])
        test_results['batch_greedy_smart'] = passed
    else:
        test_results['batch_greedy_smart'] = False

    # Test 4: Real-time mode with Greedy
    logger.info("\nTest 4: Real-time mode with Greedy strategy")
    log_files = run_simulation('realtime', 'greedy', micro_batch_sec=10)
    if log_files:
        passed = validate_invariants(log_files['assignment'], log_files['cycle'])
        test_results['realtime_greedy'] = passed
    else:
        test_results['realtime_greedy'] = False

    # Test 5: Batch mode with bundling
    logger.info("\nTest 5: Batch mode with Hungarian + bundling")
    log_files = run_simulation('batch', 'hungarian', bundling='on')
    if log_files:
        passed = validate_invariants(log_files['assignment'], log_files['cycle'])
        test_results['batch_hungarian_bundling'] = passed
    else:
        test_results['batch_hungarian_bundling'] = False

    # Print summary
    logger.info("\n" + "="*60)
    logger.info("TEST SUMMARY")
    logger.info("="*60)

    for test_name, passed in test_results.items():
        status = "✓ PASSED" if passed else "✗ FAILED"
        logger.info(f"{test_name}: {status}")

    total_passed = sum(1 for p in test_results.values() if p)
    total_tests = len(test_results)

    logger.info(f"\nTotal: {total_passed}/{total_tests} tests passed")

    if total_passed == total_tests:
        logger.info("\n🎉 All tests passed! The refactored system is working correctly.")
        return 0
    else:
        logger.warning(f"\n⚠️ {total_tests - total_passed} tests failed. Review the logs for details.")
        return 1


if __name__ == '__main__':
    sys.exit(main())