#!/usr/bin/env python3
"""Quick test of detailed log generation."""

from simulator_core import run_simulation
from assignment_algorithms import get_algorithm
from create_clean_simple_viz import generate_dense_continuous_scenario
from export_detailed_logs import export_detailed_log

# Generate scenario
scenario = generate_dense_continuous_scenario()

# Run Hungarian algorithm
print("Running Hungarian algorithm...")
assignment_func = get_algorithm('hungarian')
state = run_simulation(scenario, assignment_func, 'hungarian')

# Export detailed log
print("Exporting detailed log...")
export_detailed_log(state, 'hungarian', 'logs/test_hungarian_detailed_log.txt')

print("\n✅ Test complete!")
print(f"   Log saved to: logs/test_hungarian_detailed_log.txt")
print(f"   Delivered: {state.metrics['orders_delivered']} orders")
