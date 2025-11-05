#!/usr/bin/env python3
"""
Analyze Batch Distinctness Across Algorithms

Identifies batches where the 5 routing algorithms make the most distinct
assignment decisions. Used to select key frames for focused GIF generation.
"""

import json
import numpy as np
from collections import defaultdict
from typing import Dict, List, Tuple

ALGORITHMS = ['greedy', 'hungarian', 'simple_bundling', 'batched_pickups', 'anticipated_bundling']

DISPLAY_NAMES = {
    'greedy': 'Greedy',
    'hungarian': 'Optimal Single-Order Matching',
    'simple_bundling': 'Single-Pickup Bundling',
    'batched_pickups': 'Batched Pickups',
    'anticipated_bundling': 'Anticipated Bundling'
}


def load_bundle_data(algo_name: str) -> List[Dict]:
    """Load bundle data for an algorithm."""
    filepath = f'analysis/event_data/bundles_{algo_name}.json'
    try:
        with open(filepath, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"Warning: {filepath} not found")
        return []


def group_by_batch(bundles: List[Dict]) -> Dict[int, List[Dict]]:
    """Group bundles by batch number (time // 300)."""
    batches = defaultdict(list)
    for bundle in bundles:
        batch_num = int(bundle['time'] // 300)
        batches[batch_num] = batches.get(batch_num, []) + [bundle]
    return dict(batches)


def calculate_batch_distinctness(batch_num: int, algo_batches: Dict[str, Dict[int, List[Dict]]]) -> Dict:
    """
    Calculate distinctness score for a single batch across all algorithms.

    Returns dict with:
    - overlap_score: How different the assignments are (0=identical, 1=completely different)
    - bundle_variance: Variance in bundle sizes
    - assignment_count_variance: Variance in number of assignments
    - total_distinctness: Combined score
    """

    # Extract assignments for this batch from each algorithm
    batch_data = {}
    for algo in ALGORITHMS:
        if batch_num in algo_batches[algo]:
            batch_data[algo] = algo_batches[algo][batch_num]
        else:
            batch_data[algo] = []

    # Skip if batch doesn't exist for any algorithm
    if not any(batch_data.values()):
        return None

    # Metric 1: Assignment overlap (courier-order pairs)
    assignment_sets = {}
    for algo, bundles in batch_data.items():
        # Create set of (courier_id, tuple of order_ids) for comparison
        pairs = set()
        for bundle in bundles:
            courier = bundle['courier_id']
            orders = tuple(sorted(bundle['order_ids']))
            pairs.add((courier, orders))
        assignment_sets[algo] = pairs

    # Calculate pairwise overlap
    overlaps = []
    for i, algo1 in enumerate(ALGORITHMS):
        for j, algo2 in enumerate(ALGORITHMS):
            if i < j:  # Only compare each pair once
                set1 = assignment_sets[algo1]
                set2 = assignment_sets[algo2]
                if set1 or set2:
                    intersection = len(set1 & set2)
                    union = len(set1 | set2)
                    overlap = intersection / union if union > 0 else 1.0
                    overlaps.append(overlap)

    avg_overlap = np.mean(overlaps) if overlaps else 1.0
    overlap_score = 1.0 - avg_overlap  # Higher = more distinct

    # Metric 2: Bundle size variance
    bundle_sizes = []
    for algo, bundles in batch_data.items():
        for bundle in bundles:
            bundle_sizes.append(bundle['bundle_size'])

    bundle_variance = np.var(bundle_sizes) if len(bundle_sizes) > 1 else 0.0

    # Metric 3: Assignment count variance
    assignment_counts = [len(bundles) for bundles in batch_data.values()]
    count_variance = np.var(assignment_counts) if len(assignment_counts) > 1 else 0.0

    # Metric 4: Order selection diversity (how many different orders chosen)
    all_orders = set()
    for bundles in batch_data.values():
        for bundle in bundles:
            all_orders.update(bundle['order_ids'])
    order_diversity = len(all_orders)

    # Combined distinctness score
    # Weights: overlap is most important, then count variance, then bundle variance
    total_distinctness = (
        overlap_score * 10.0 +  # Primary signal
        count_variance * 2.0 +   # Secondary signal
        bundle_variance * 1.0 +  # Tertiary signal
        order_diversity * 0.1    # Context signal
    )

    return {
        'batch_num': batch_num,
        'time_sec': batch_num * 300,
        'overlap_score': overlap_score,
        'bundle_variance': bundle_variance,
        'count_variance': count_variance,
        'order_diversity': order_diversity,
        'total_distinctness': total_distinctness,
        'assignment_counts': assignment_counts,
        'bundle_sizes': bundle_sizes
    }


def analyze_all_batches() -> List[Dict]:
    """Analyze distinctness for all batches across all algorithms."""

    print("="*80)
    print("BATCH DISTINCTNESS ANALYSIS")
    print("="*80)

    # Load data for all algorithms
    print("\nLoading bundle data...")
    algo_bundles = {}
    for algo in ALGORITHMS:
        bundles = load_bundle_data(algo)
        algo_bundles[algo] = group_by_batch(bundles)
        print(f"  {DISPLAY_NAMES[algo]}: {len(bundles)} total bundles")

    # Get all batch numbers
    all_batches = set()
    for batches in algo_bundles.values():
        all_batches.update(batches.keys())

    print(f"\nTotal batches found: {len(all_batches)}")
    print(f"Batch range: {min(all_batches)} to {max(all_batches)}\n")

    # Analyze each batch
    print("Analyzing distinctness scores...")
    batch_scores = []
    for batch_num in sorted(all_batches):
        score = calculate_batch_distinctness(batch_num, algo_bundles)
        if score:
            batch_scores.append(score)

    # Sort by distinctness score
    batch_scores.sort(key=lambda x: x['total_distinctness'], reverse=True)

    return batch_scores


def find_best_sequential_window(batch_scores: List[Dict], window_size: int = 5) -> List[int]:
    """
    Find the best sequential window of N batches with maximum total distinctness.

    Returns list of batch numbers in sequential order.
    """

    # Create lookup dict by batch number
    scores_by_batch = {s['batch_num']: s for s in batch_scores}
    all_batch_nums = sorted(scores_by_batch.keys())

    best_window = None
    best_score = 0

    # Try each possible sequential window
    for start_idx in range(len(all_batch_nums) - window_size + 1):
        window_batches = all_batch_nums[start_idx:start_idx + window_size]

        # Check if batches are sequential (no gaps)
        is_sequential = all(
            window_batches[i+1] == window_batches[i] + 1
            for i in range(len(window_batches) - 1)
        )

        if is_sequential:
            # Calculate total distinctness for this window
            window_score = sum(scores_by_batch[b]['total_distinctness'] for b in window_batches)

            if window_score > best_score:
                best_score = window_score
                best_window = window_batches

    return best_window, best_score


def print_results(batch_scores: List[Dict], window_size: int = 5):
    """Print analysis results."""

    print("\n" + "="*80)
    print(f"TOP INDIVIDUAL DISTINCT BATCHES (for reference)")
    print("="*80)

    for i, score in enumerate(batch_scores[:window_size], 1):
        time_min = score['time_sec'] / 60
        print(f"\n{i}. BATCH {score['batch_num']} @ t={score['time_sec']}s ({time_min:.1f} min)")
        print(f"   Total Distinctness Score: {score['total_distinctness']:.2f}")

    # Find best sequential window
    sequential_window, total_score = find_best_sequential_window(batch_scores, window_size)

    print("\n" + "="*80)
    print(f"BEST SEQUENTIAL {window_size}-BATCH WINDOW")
    print("="*80)
    print(f"\nTotal Combined Distinctness: {total_score:.2f}")
    print(f"\nSequential Batches: {sequential_window}")

    # Print details for each batch in the window
    scores_by_batch = {s['batch_num']: s for s in batch_scores}
    for batch_num in sequential_window:
        score = scores_by_batch[batch_num]
        time_min = score['time_sec'] / 60
        print(f"\n  BATCH {batch_num} @ {time_min:.0f}min:")
        print(f"    Distinctness: {score['total_distinctness']:.2f}")
        print(f"    Overlap: {score['overlap_score']:.3f}")
        print(f"    Assignments: {score['assignment_counts']}")
        print(f"    Bundle Sizes: {score['bundle_sizes']}")

    # Save sequential window to file
    output = {
        'top_batches': sequential_window,
        'batch_times': [b * 300 for b in sequential_window],
        'total_distinctness': total_score,
        'detailed_scores': [scores_by_batch[b] for b in sequential_window]
    }

    with open('analysis/top_distinct_batches.json', 'w') as f:
        json.dump(output, f, indent=2)

    print("\n" + "="*80)
    print("RECOMMENDED SEQUENTIAL BATCHES FOR FOCUSED GIF")
    print("="*80)
    print(f"\nBatch Numbers: {sequential_window}")
    print(f"Times (seconds): {[b * 300 for b in sequential_window]}")
    print(f"Times (minutes): {[b * 5 for b in sequential_window]}")
    print(f"\nSaved to: analysis/top_distinct_batches.json")

    return sequential_window


def main():
    """Main execution."""
    batch_scores = analyze_all_batches()
    top_batches = print_results(batch_scores, window_size=7)

    print("\n" + "="*80)
    print("✓ ANALYSIS COMPLETE")
    print("="*80)
    print("\nUse these batch numbers for focused GIF generation:")
    print(f"  {', '.join(map(str, top_batches))}")

    return top_batches


if __name__ == "__main__":
    main()
