#!/usr/bin/env python3
"""
Assignment Overlap Analysis

Compares assignment decisions across different routing algorithms to identify
which strategies make similar vs. different decisions.

Overlap is defined as: same courier gets same set of orders (strict matching).
"""

from typing import Dict, List, Set, Tuple
from simulator_core import run_simulation
from assignment_algorithms import get_algorithm
from create_clean_simple_viz import generate_dense_continuous_scenario
import numpy as np

ALGORITHMS = ['greedy', 'hungarian', 'simple_bundling', 'batched_pickups', 'anticipated_bundling']

DISPLAY_NAMES = {
    'greedy': 'Greedy',
    'hungarian': 'Optimal Single-Order Matching',
    'simple_bundling': 'Single-Pickup Bundling',
    'batched_pickups': 'Batched Pickups',
    'anticipated_bundling': 'Anticipated Bundling'
}


def extract_batch_assignments(state) -> Dict[int, Dict[int, List[int]]]:
    """
    Extract assignment decisions from simulation events log.

    Returns:
        Dict[batch_num, Dict[courier_id, List[order_ids]]]
    """
    batch_assignments = {}

    for event in state.events_log:
        if event['type'] == 'ASSIGNMENT_MADE':
            event_time = event['time']
            batch_num = int(event_time // 300)  # 5-minute batches

            if batch_num not in batch_assignments:
                batch_assignments[batch_num] = {}

            courier_id = event['courier_id']
            order_ids = tuple(sorted(event['order_ids']))  # Sort for comparison

            batch_assignments[batch_num][courier_id] = order_ids

    return batch_assignments


def calculate_batch_overlap(assignments_a: Dict[int, List[int]],
                           assignments_b: Dict[int, List[int]]) -> float:
    """
    Calculate overlap percentage for a single batch.

    Overlap = (number of identical courier-order assignments) / (total assignments)
    """
    if not assignments_a and not assignments_b:
        return 100.0  # Both made no assignments

    if not assignments_a or not assignments_b:
        return 0.0  # One made assignments, other didn't

    # Count identical assignments (same courier, same orders)
    identical_count = 0
    total_assignments = max(len(assignments_a), len(assignments_b))

    for courier_id in assignments_a:
        if courier_id in assignments_b:
            if assignments_a[courier_id] == assignments_b[courier_id]:
                identical_count += 1

    return (identical_count / total_assignments) * 100 if total_assignments > 0 else 0.0


def calculate_pairwise_overlap(algo_assignments: Dict[str, Dict[int, Dict[int, List[int]]]]) -> Dict[Tuple[str, str], float]:
    """
    Calculate average overlap across all batches for each algorithm pair.

    Returns:
        Dict[(algo1, algo2), overlap_percentage]
    """
    overlap_matrix = {}

    # Get all batches that exist across any algorithm
    all_batches = set()
    for assignments in algo_assignments.values():
        all_batches.update(assignments.keys())

    # Calculate pairwise overlap
    for i, algo1 in enumerate(ALGORITHMS):
        for j, algo2 in enumerate(ALGORITHMS):
            if i == j:
                overlap_matrix[(algo1, algo2)] = 100.0  # Self-overlap is 100%
                continue

            # Calculate batch-by-batch overlap
            batch_overlaps = []
            for batch_num in sorted(all_batches):
                assignments_a = algo_assignments[algo1].get(batch_num, {})
                assignments_b = algo_assignments[algo2].get(batch_num, {})

                overlap = calculate_batch_overlap(assignments_a, assignments_b)
                batch_overlaps.append(overlap)

            # Average across all batches
            avg_overlap = np.mean(batch_overlaps) if batch_overlaps else 0.0
            overlap_matrix[(algo1, algo2)] = avg_overlap

    return overlap_matrix


def generate_overlap_report(overlap_matrix: Dict[Tuple[str, str], float], output_path: str):
    """Generate formatted overlap matrix report."""

    with open(output_path, 'w') as f:
        f.write("=" * 100 + "\n")
        f.write("ASSIGNMENT OVERLAP MATRIX\n")
        f.write("Percentage of Identical Assignments (Same Courier + Same Orders)\n")
        f.write("=" * 100 + "\n\n")

        f.write("How to read this matrix:\n")
        f.write("- 100% = Algorithms made identical decisions\n")
        f.write("- 0% = Algorithms made completely different decisions\n")
        f.write("- Diagonal is always 100% (algorithm compared with itself)\n\n")

        # Header row
        f.write(f"{'Algorithm':<25}")
        for algo in ALGORITHMS:
            f.write(f"{DISPLAY_NAMES[algo]:>15}")
        f.write("\n")
        f.write("-" * 100 + "\n")

        # Data rows
        for algo1 in ALGORITHMS:
            f.write(f"{DISPLAY_NAMES[algo1]:<25}")
            for algo2 in ALGORITHMS:
                overlap = overlap_matrix[(algo1, algo2)]
                f.write(f"{overlap:>14.1f}%")
            f.write("\n")

        f.write("\n" + "=" * 100 + "\n")
        f.write("KEY INSIGHTS\n")
        f.write("=" * 100 + "\n\n")

        # Find most similar pair (excluding self-comparison)
        max_overlap = 0
        max_pair = None
        for (algo1, algo2), overlap in overlap_matrix.items():
            if algo1 != algo2 and overlap > max_overlap:
                max_overlap = overlap
                max_pair = (algo1, algo2)

        if max_pair:
            f.write(f"Most Similar Algorithms:\n")
            f.write(f"  {DISPLAY_NAMES[max_pair[0]]} ↔ {DISPLAY_NAMES[max_pair[1]]}: {max_overlap:.1f}% overlap\n\n")

        # Find least similar pairs
        min_overlap = 100
        min_pair = None
        for (algo1, algo2), overlap in overlap_matrix.items():
            if algo1 != algo2 and overlap < min_overlap:
                min_overlap = overlap
                min_pair = (algo1, algo2)

        if min_pair:
            f.write(f"Most Different Algorithms:\n")
            f.write(f"  {DISPLAY_NAMES[min_pair[0]]} ↔ {DISPLAY_NAMES[min_pair[1]]}: {min_overlap:.1f}% overlap\n\n")

        # Analyze each algorithm's average similarity to others
        f.write("Average Overlap with Other Algorithms:\n")
        for algo in ALGORITHMS:
            overlaps = [overlap_matrix[(algo, other)]
                       for other in ALGORITHMS if other != algo]
            avg = np.mean(overlaps)
            f.write(f"  {DISPLAY_NAMES[algo]}: {avg:.1f}%")
            if avg > 50:
                f.write(" (more conventional)")
            elif avg < 30:
                f.write(" (highly distinctive)")
            f.write("\n")

    print(f"  ✓ Overlap matrix saved: {output_path}")


def main():
    """Run overlap analysis for all algorithms."""

    print("=" * 100)
    print("ASSIGNMENT OVERLAP ANALYSIS")
    print("=" * 100)

    # Generate shared scenario
    print("\nGenerating scenario...")
    scenario = generate_dense_continuous_scenario()
    print(f"  • {len(scenario['order_schedule'])} orders")

    # Run all algorithms and extract assignments
    print("\nRunning algorithms and extracting assignments...")
    algo_assignments = {}

    for algo_name in ALGORITHMS:
        print(f"  [{algo_name}] Running simulation...")
        assignment_func = get_algorithm(algo_name)
        state = run_simulation(scenario, assignment_func, algo_name)

        # Extract batch assignments
        batch_assignments = extract_batch_assignments(state)
        algo_assignments[algo_name] = batch_assignments

        print(f"    ✓ {len(batch_assignments)} batches with assignments")

    # Calculate pairwise overlap
    print("\nCalculating pairwise overlap...")
    overlap_matrix = calculate_pairwise_overlap(algo_assignments)

    # Generate report
    output_path = 'logs/assignment_overlap_matrix.txt'
    generate_overlap_report(overlap_matrix, output_path)

    # Print summary to console
    print("\n" + "=" * 100)
    print("OVERLAP MATRIX SUMMARY")
    print("=" * 100)
    print(f"{'Algorithm':<25}", end="")
    for algo in ALGORITHMS:
        print(f"{DISPLAY_NAMES[algo]:>15}", end="")
    print()
    print("-" * 100)

    for algo1 in ALGORITHMS:
        print(f"{DISPLAY_NAMES[algo1]:<25}", end="")
        for algo2 in ALGORITHMS:
            overlap = overlap_matrix[(algo1, algo2)]
            print(f"{overlap:>14.1f}%", end="")
        print()

    print("\n✅ Overlap analysis complete!")
    print(f"   Report saved to: {output_path}")


if __name__ == "__main__":
    main()
