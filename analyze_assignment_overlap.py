#!/usr/bin/env python3
"""
Analyze assignment overlap between different algorithms.
Shows correlation matrix of how often different algorithms assign the same courier to the same order.
"""

import pickle
import numpy as np
import pandas as pd
from pathlib import Path

def load_assignments(pickle_path):
    """Load order→courier assignments from a pickle file"""
    with open(pickle_path, 'rb') as f:
        state = pickle.load(f)

    assignments = {}
    for order_id, order in state.orders.items():
        if order.assigned_courier_id is not None:
            assignments[order_id] = order.assigned_courier_id

    return assignments

def calculate_overlap(assignments1, assignments2):
    """Calculate percentage of orders assigned to same courier in both algorithms"""
    # Find common orders (delivered by both algorithms)
    common_orders = set(assignments1.keys()) & set(assignments2.keys())

    if len(common_orders) == 0:
        return 0.0

    # Count matching assignments
    matches = sum(1 for order_id in common_orders
                  if assignments1[order_id] == assignments2[order_id])

    overlap_rate = (matches / len(common_orders)) * 100
    return overlap_rate

def main():
    print("\n" + "="*70)
    print("ASSIGNMENT OVERLAP ANALYSIS")
    print("="*70)

    # Define algorithms and their pickle files
    algorithms = [
        'greedy',
        'hungarian',
        'simple_bundling',
        'network_bundling',
        'anticipated_bundling'
    ]

    base_path = Path('outputs/quick_test/states')

    # Load all assignments
    print("\nLoading assignment data...")
    all_assignments = {}
    for alg in algorithms:
        pickle_file = base_path / f'{alg}.pkl'
        if pickle_file.exists():
            all_assignments[alg] = load_assignments(pickle_file)
            print(f"  {alg}: {len(all_assignments[alg])} orders delivered")
        else:
            print(f"  WARNING: {pickle_file} not found")
            return 1

    # Calculate pairwise overlap matrix
    print("\nCalculating pairwise assignment overlap...")
    n = len(algorithms)
    overlap_matrix = np.zeros((n, n))

    for i, alg1 in enumerate(algorithms):
        for j, alg2 in enumerate(algorithms):
            overlap_matrix[i, j] = calculate_overlap(
                all_assignments[alg1],
                all_assignments[alg2]
            )

    # Display as DataFrame for better formatting
    df = pd.DataFrame(
        overlap_matrix,
        index=algorithms,
        columns=algorithms
    )

    print("\n" + "="*70)
    print("ASSIGNMENT OVERLAP MATRIX (%)")
    print("="*70)
    print("\nShows percentage of orders assigned to same courier by algorithm pairs")
    print("(Only considers orders successfully delivered by both algorithms)\n")
    print(df.round(1).to_string())

    # Additional statistics
    print("\n" + "="*70)
    print("PAIRWISE OVERLAP STATISTICS")
    print("="*70)

    # Get upper triangle (exclude diagonal)
    pairs = []
    for i, alg1 in enumerate(algorithms):
        for j, alg2 in enumerate(algorithms):
            if i < j:  # Upper triangle only
                overlap = overlap_matrix[i, j]
                pairs.append((alg1, alg2, overlap))

    # Sort by overlap rate
    pairs.sort(key=lambda x: x[2], reverse=True)

    print(f"\n{'Algorithm 1':<22} {'Algorithm 2':<22} {'Overlap %':<12}")
    print("-"*60)
    for alg1, alg2, overlap in pairs:
        print(f"{alg1:<22} {alg2:<22} {overlap:>10.1f}%")

    # Key findings
    print("\n" + "="*70)
    print("KEY FINDINGS")
    print("="*70)

    highest = pairs[0]
    lowest = pairs[-1]

    print(f"\nMost similar assignment strategies:")
    print(f"  {highest[0]} ↔ {highest[1]}: {highest[2]:.1f}% overlap")

    print(f"\nMost different assignment strategies:")
    print(f"  {lowest[0]} ↔ {lowest[1]}: {lowest[2]:.1f}% overlap")

    avg_overlap = np.mean([p[2] for p in pairs])
    print(f"\nAverage pairwise overlap: {avg_overlap:.1f}%")

    # Analyze by algorithm type
    print("\n" + "="*70)
    print("OVERLAP BY ALGORITHM TYPE")
    print("="*70)

    # Bundling vs non-bundling
    bundling_algs = {'simple_bundling', 'network_bundling', 'anticipated_bundling'}
    non_bundling_algs = {'greedy', 'hungarian'}

    # Within bundling algorithms
    bundling_pairs = [p for p in pairs if p[0] in bundling_algs and p[1] in bundling_algs]
    if bundling_pairs:
        avg_bundling = np.mean([p[2] for p in bundling_pairs])
        print(f"\nAverage overlap within bundling algorithms: {avg_bundling:.1f}%")

    # Within non-bundling algorithms
    non_bundling_pairs = [p for p in pairs if p[0] in non_bundling_algs and p[1] in non_bundling_algs]
    if non_bundling_pairs:
        avg_non_bundling = np.mean([p[2] for p in non_bundling_pairs])
        print(f"Average overlap within non-bundling algorithms: {avg_non_bundling:.1f}%")

    # Between bundling and non-bundling
    cross_pairs = [p for p in pairs if
                   (p[0] in bundling_algs and p[1] in non_bundling_algs) or
                   (p[0] in non_bundling_algs and p[1] in bundling_algs)]
    if cross_pairs:
        avg_cross = np.mean([p[2] for p in cross_pairs])
        print(f"Average overlap between bundling and non-bundling: {avg_cross:.1f}%")

    # Try to create heatmap visualization
    try:
        import matplotlib.pyplot as plt
        import seaborn as sns

        plt.figure(figsize=(10, 8))
        sns.heatmap(df, annot=True, fmt='.1f', cmap='RdYlGn',
                    vmin=0, vmax=100, cbar_kws={'label': 'Overlap %'})
        plt.title('Assignment Overlap Matrix\n(% of orders assigned to same courier)',
                  fontsize=14, pad=20)
        plt.xlabel('Algorithm', fontsize=12)
        plt.ylabel('Algorithm', fontsize=12)
        plt.tight_layout()

        output_file = 'outputs/quick_test/assignment_overlap_heatmap.png'
        plt.savefig(output_file, dpi=150, bbox_inches='tight')
        print(f"\n✓ Heatmap saved to: {output_file}")

    except ImportError:
        print("\n(matplotlib/seaborn not available - skipping heatmap visualization)")

    print("\n" + "="*70)
    print("Analysis complete!")
    print("="*70)

    return 0

if __name__ == '__main__':
    import sys
    sys.exit(main())
