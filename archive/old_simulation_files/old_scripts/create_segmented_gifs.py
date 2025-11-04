#!/usr/bin/env python3
"""
Segmented GIF Generation - Break 1-hour simulation into 6×10-minute segments
Each segment is ~10 seconds, clearly showing 2 batch cycles
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation, PillowWriter
from matplotlib.patches import Rectangle, Circle, Polygon
import numpy as np
from simulator_core import run_simulation, GRID_SIZE, SIMULATION_DURATION
from assignment_algorithms import get_algorithm
from create_clean_simple_viz import (
    COURIER_COLORS, ALGORITHM_DISPLAY_NAMES, ALGORITHM_FILENAMES,
    generate_dense_continuous_scenario, NUM_COURIERS
)
from create_improved_showcase_gifs import draw_enhanced_visualization
import math

# Segmented animation parameters
SEGMENT_DURATION = 720  # 12 minutes per segment (shows 2-3 batch cycles)
SAMPLE_INTERVAL = 10  # Sample every 10 seconds
ANIMATION_FPS = 12  # 12 fps for smooth playback
NUM_SEGMENTS = 5  # 5 segments for 1-hour simulation

print("=" * 70)
print("CREATING SEGMENTED SHOWCASE GIFS")
print("1-Hour Simulation → 5 × 12-Minute Segments")
print("Each segment shows 2-3 batch cycles clearly")
print("=" * 70)

# Generate scenario
scenario = generate_dense_continuous_scenario()
print(f"\n📋 Scenario Configuration:")
print(f"  • {len(scenario['restaurants'])} restaurants")
print(f"  • {len(scenario['couriers'])} couriers")
print(f"  • {len(scenario['order_schedule'])} orders")
print(f"  • Duration: 1 hour")
print(f"\n🎬 Segment Settings:")
print(f"  • {NUM_SEGMENTS} segments × {SEGMENT_DURATION//60} minutes each")
print(f"  • Sample interval: every {SAMPLE_INTERVAL} seconds")
print(f"  • FPS: {ANIMATION_FPS}")
print(f"  • Each segment: ~{(SEGMENT_DURATION/SAMPLE_INTERVAL)/ANIMATION_FPS:.0f} seconds")

# Algorithms to showcase
algorithms = ['greedy', 'hungarian', 'simple_bundling', 'batched_pickups',
              'relay_bundling', 'anticipated_bundling']

print(f"\n🔄 Processing {len(algorithms)} algorithms × {NUM_SEGMENTS} segments...")

# Create output directory
os.makedirs('gifs/segments', exist_ok=True)

for algo_idx, algo_name in enumerate(algorithms, 1):
    print(f"\n[{algo_idx}/{len(algorithms)}] {algo_name.upper()}")
    print("-" * 70)
    
    # Run simulation once
    assignment_func = get_algorithm(algo_name)
    state = run_simulation(scenario, assignment_func, algo_name)
    
    # Generate each segment
    for seg_idx in range(NUM_SEGMENTS):
        seg_start = seg_idx * SEGMENT_DURATION
        seg_end = min((seg_idx + 1) * SEGMENT_DURATION, SIMULATION_DURATION)
        
        print(f"  Segment {seg_idx+1}: {seg_start//60:2d}-{seg_end//60:2d} min", end=" ... ")
        
        # Extract segment timeline
        segment_timeline = state.timeline[seg_start:seg_end]
        
        if not segment_timeline:
            print("⚠ No data")
            continue
        
        # Create figure
        fig, ax = plt.subplots(figsize=(12, 12))
        restaurants = list(state.restaurants.values())
        
        def update(frame_idx):
            if frame_idx >= len(segment_timeline):
                return ax,
            
            snapshot = segment_timeline[frame_idx]
            draw_enhanced_visualization(ax, snapshot, restaurants)
            
            # Add segment label
            display_name = ALGORITHM_DISPLAY_NAMES.get(algo_name, algo_name)
            ax.text(GRID_SIZE/2, GRID_SIZE + 0.35,
                   f'{display_name} - Segment {seg_idx+1}/5',
                   fontsize=16, fontweight='bold', ha='center')
            
            ax.text(GRID_SIZE/2, GRID_SIZE + 0.15,
                   f'[{seg_start//60}-{seg_end//60} minutes]',
                   fontsize=11, ha='center', color='#D32F2F', style='italic')
            
            return ax,
        
        # Sample frames
        sampled_indices = list(range(0, len(segment_timeline), SAMPLE_INTERVAL))
        
        def update_sampled(idx):
            actual_idx = sampled_indices[idx] if idx < len(sampled_indices) else len(segment_timeline) - 1
            return update(actual_idx)
        
        # Create animation
        anim = FuncAnimation(fig, update_sampled, frames=len(sampled_indices),
                            interval=1000/ANIMATION_FPS, blit=False)
        
        # Save GIF
        filename = ALGORITHM_FILENAMES[algo_name]
        output_path = f'gifs/segments/{filename}_segment{seg_idx+1}.gif'
        
        writer = PillowWriter(fps=ANIMATION_FPS)
        anim.save(output_path, writer=writer, dpi=100)
        plt.close(fig)
        
        print(f"✅ {len(sampled_indices)} frames")

print("\n" + "=" * 70)
print("✅ ALL SEGMENTED GIFS CREATED!")
print("=" * 70)
print(f"\n📁 Output: gifs/segments/")
print(f"   {len(algorithms)} algorithms × {NUM_SEGMENTS} segments = {len(algorithms)*NUM_SEGMENTS} GIFs")
