"""
GIF Animator

Creates animated GIFs from assignment log data.
"""

import os
import polars as pl
import imageio
from frame_generator import generate_dispatch_frame, generate_comparison_frame


def create_single_gif(assignment_log_path, mode, output_path, duration=2.5, zoom_bounds=None, show_mode='active'):
    """
    Create a GIF animation for a single mode (baseline or actual)

    Args:
        assignment_log_path: Path to assignment log CSV
        mode: 'baseline' or 'actual'
        output_path: Path to save GIF file
        duration: Duration per frame in seconds (default 2.5)
        zoom_bounds: Tuple of (lat_min, lat_max, lng_min, lng_max) for zooming (optional)
        show_mode: 'all' (show everything) or 'active' (fade matched pairs) - controls visual hierarchy (default 'active')

    Returns:
        output_path on success
    """
    # Load assignment log
    assignment_log = pl.read_csv(assignment_log_path)

    # Get sorted list of dispatch times
    dispatch_times = sorted(assignment_log['dispatch_time'].unique().to_list())

    # Create temp directory for frames
    temp_dir = os.path.join(os.path.dirname(output_path), f'.frames_{mode}')
    os.makedirs(temp_dir, exist_ok=True)

    frame_files = []

    try:
        # Generate frames for each dispatch moment
        for i, dispatch_time in enumerate(dispatch_times):
            cycle_data = assignment_log.filter(pl.col('dispatch_time') == dispatch_time)
            frame_path = os.path.join(temp_dir, f'frame_{i:03d}.png')

            generate_dispatch_frame(dispatch_time, cycle_data, mode, save_path=frame_path, zoom_bounds=zoom_bounds, show_mode=show_mode)
            frame_files.append(frame_path)

        # Stitch frames into GIF
        with imageio.get_writer(output_path, mode='I', duration=duration, loop=0) as writer:
            for frame_path in frame_files:
                image = imageio.imread(frame_path)
                writer.append_data(image)

        print(f"GIF saved: {output_path}")
        print(f"  Mode: {mode}")
        print(f"  Frames: {len(frame_files)}")
        print(f"  Duration: {duration}s per frame")

        return output_path

    finally:
        # Cleanup temp frames
        for frame_path in frame_files:
            if os.path.exists(frame_path):
                os.remove(frame_path)
        if os.path.exists(temp_dir):
            os.rmdir(temp_dir)


def create_comparison_gif(assignment_log_path, output_path, duration=2.5, zoom_bounds=None, show_mode='active'):
    """
    Create a side-by-side comparison GIF (baseline vs actual)

    Args:
        assignment_log_path: Path to assignment log CSV
        output_path: Path to save GIF file
        duration: Duration per frame in seconds (default 2.5)
        zoom_bounds: Tuple of (lat_min, lat_max, lng_min, lng_max) for zooming (optional)
        show_mode: 'all' (show everything) or 'active' (fade matched pairs) - controls visual hierarchy (default 'active')

    Returns:
        output_path on success
    """
    # Load assignment log
    assignment_log = pl.read_csv(assignment_log_path)

    # Get sorted list of dispatch times
    dispatch_times = sorted(assignment_log['dispatch_time'].unique().to_list())

    # Create temp directory for frames
    temp_dir = os.path.join(os.path.dirname(output_path), '.frames_comparison')
    os.makedirs(temp_dir, exist_ok=True)

    frame_files = []

    try:
        # Generate frames for each dispatch moment
        for i, dispatch_time in enumerate(dispatch_times):
            cycle_data = assignment_log.filter(pl.col('dispatch_time') == dispatch_time)
            frame_path = os.path.join(temp_dir, f'frame_{i:03d}.png')

            generate_comparison_frame(dispatch_time, cycle_data, save_path=frame_path, zoom_bounds=zoom_bounds, show_mode=show_mode)
            frame_files.append(frame_path)

        # Stitch frames into GIF
        with imageio.get_writer(output_path, mode='I', duration=duration, loop=0) as writer:
            for frame_path in frame_files:
                image = imageio.imread(frame_path)
                writer.append_data(image)

        print(f"Comparison GIF saved: {output_path}")
        print(f"  Frames: {len(frame_files)}")
        print(f"  Duration: {duration}s per frame")

        return output_path

    finally:
        # Cleanup temp frames
        for frame_path in frame_files:
            if os.path.exists(frame_path):
                os.remove(frame_path)
        if os.path.exists(temp_dir):
            os.rmdir(temp_dir)


def create_all_gifs(assignment_log_path, output_dir, duration=2.5, zoom_bounds=None, show_mode='active'):
    """
    Create all GIF types (baseline, actual, comparison)

    Args:
        assignment_log_path: Path to assignment log CSV
        output_dir: Directory to save GIF files
        duration: Duration per frame in seconds (default 2.5)
        zoom_bounds: Tuple of (lat_min, lat_max, lng_min, lng_max) for zooming (optional)
        show_mode: 'all' (show everything) or 'active' (fade matched pairs) - controls visual hierarchy (default 'active')

    Returns:
        Dictionary mapping GIF type to output path
    """
    os.makedirs(output_dir, exist_ok=True)

    # Extract base name from assignment log
    base_name = os.path.basename(assignment_log_path).replace('_assignment_log_', '_').replace('.csv', '')

    outputs = {}

    # Baseline GIF
    baseline_path = os.path.join(output_dir, f'{base_name}_baseline.gif')
    create_single_gif(assignment_log_path, 'baseline', baseline_path, duration, zoom_bounds, show_mode)
    outputs['baseline'] = baseline_path

    # Actual GIF
    actual_path = os.path.join(output_dir, f'{base_name}_actual.gif')
    create_single_gif(assignment_log_path, 'actual', actual_path, duration, zoom_bounds, show_mode)
    outputs['actual'] = actual_path

    # Comparison GIF
    comparison_path = os.path.join(output_dir, f'{base_name}_comparison.gif')
    create_comparison_gif(assignment_log_path, comparison_path, duration, zoom_bounds, show_mode)
    outputs['comparison'] = comparison_path

    print(f"\nAll GIFs created in: {output_dir}")
    return outputs
