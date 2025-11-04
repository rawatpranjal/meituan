"""
Create GIFs from Assignment Logs

Command-line script to generate animated GIFs from simulation assignment logs.
"""

import argparse
import os
import sys
from animator import create_single_gif, create_comparison_gif, create_all_gifs


def main():
    parser = argparse.ArgumentParser(
        description='Generate animated GIFs from dispatch simulation logs',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Create side-by-side comparison GIF
  python create_gifs.py --assignment-log logs/assignment_log.csv --mode comparison

  # Create baseline-only GIF
  python create_gifs.py --assignment-log logs/assignment_log.csv --mode baseline

  # Create all GIF types
  python create_gifs.py --assignment-log logs/assignment_log.csv --mode all

  # Custom output directory and frame duration
  python create_gifs.py --assignment-log logs/assignment_log.csv --mode all --output-dir gifs/ --duration 1.5
        """
    )

    parser.add_argument('--assignment-log', required=True,
                       help='Path to assignment log CSV file')
    parser.add_argument('--mode', required=True,
                       choices=['baseline', 'actual', 'comparison', 'all'],
                       help='Type of GIF to create')
    parser.add_argument('--output-dir', default=None,
                       help='Output directory (default: same as assignment log)')
    parser.add_argument('--duration', type=float, default=2.5,
                       help='Duration per frame in seconds (default: 2.5)')
    parser.add_argument('--zoom', action='store_true',
                       help='Zoom to dense central region (default: False)')
    parser.add_argument('--show-mode', default='active', choices=['all', 'active'],
                       help='Visual hierarchy mode: "all" shows everything equally, "active" fades matched pairs (default: active)')

    args = parser.parse_args()

    # Validate assignment log exists
    if not os.path.exists(args.assignment_log):
        print(f"Error: Assignment log not found: {args.assignment_log}")
        sys.exit(1)

    # Determine output directory
    if args.output_dir:
        output_dir = args.output_dir
        os.makedirs(output_dir, exist_ok=True)
    else:
        output_dir = os.path.dirname(args.assignment_log)

    # Extract base name for output files
    base_name = os.path.basename(args.assignment_log).replace('_assignment_log_', '_').replace('.csv', '')

    print("="*80)
    print("GIF ANIMATION GENERATOR")
    print("="*80)
    print(f"Assignment log: {args.assignment_log}")
    print(f"Output directory: {output_dir}")
    print(f"Mode: {args.mode}")
    print(f"Duration: {args.duration}s per frame")
    print(f"Show mode: {args.show_mode}")
    print(f"Zoom: {args.zoom}")

    # Calculate zoom bounds if requested
    zoom_bounds = None
    if args.zoom:
        import polars as pl
        import numpy as np

        df = pl.read_csv(args.assignment_log)
        first_dispatch = df.filter(pl.col('dispatch_time') == df['dispatch_time'].min())

        lats = first_dispatch['order_pickup_lat'].to_list()
        lngs = first_dispatch['order_pickup_lng'].to_list()

        center_lat = np.median(lats)
        center_lng = np.median(lngs)

        lat_range = max(lats) - min(lats)
        lng_range = max(lngs) - min(lngs)

        zoom_factor = 0.15
        zoom_lat = lat_range * zoom_factor
        zoom_lng = lng_range * zoom_factor

        zoom_bounds = (
            center_lat - zoom_lat/2,
            center_lat + zoom_lat/2,
            center_lng - zoom_lng/2,
            center_lng + zoom_lng/2
        )
        print(f"Zoom bounds: lat=[{zoom_bounds[0]:.0f}, {zoom_bounds[1]:.0f}], lng=[{zoom_bounds[2]:.0f}, {zoom_bounds[3]:.0f}]")

    print()

    try:
        if args.mode == 'all':
            outputs = create_all_gifs(args.assignment_log, output_dir, args.duration, zoom_bounds, args.show_mode)
            print("\n" + "="*80)
            print("SUCCESS")
            print("="*80)
            print("Generated GIFs:")
            for mode, path in outputs.items():
                print(f"  {mode}: {path}")

        elif args.mode == 'comparison':
            output_path = os.path.join(output_dir, f'{base_name}_comparison.gif')
            create_comparison_gif(args.assignment_log, output_path, args.duration, zoom_bounds, args.show_mode)
            print("\n" + "="*80)
            print("SUCCESS")
            print("="*80)
            print(f"Comparison GIF: {output_path}")

        else:  # baseline or actual
            output_path = os.path.join(output_dir, f'{base_name}_{args.mode}.gif')
            create_single_gif(args.assignment_log, args.mode, output_path, args.duration, zoom_bounds, args.show_mode)
            print("\n" + "="*80)
            print("SUCCESS")
            print("="*80)
            print(f"{args.mode.capitalize()} GIF: {output_path}")

    except Exception as e:
        print("\n" + "="*80)
        print("ERROR")
        print("="*80)
        print(f"Failed to create GIF: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
