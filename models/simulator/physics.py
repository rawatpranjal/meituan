"""
Physics Constants and Helper Functions

These constants were calibrated from historical data using calibrate_physics.py.
They define the "laws of physics" for the discrete-time simulator.
"""

from math import sqrt

# ============================================================================
# CALIBRATED CONSTANTS (from historical data)
# ============================================================================

# Average time (in seconds) from order assignment to delivery completion
# Calibrated as median(arrive_time - grab_time) for successfully delivered orders
AVERAGE_TASK_DURATION = 1451  # seconds (24.2 minutes)

# Probability that a courier will reject an assignment
# Calibrated as count(is_courier_grabbed==0) / total_orders
GLOBAL_REJECTION_PROBABILITY = 0.1311  # 13.11%

# ============================================================================
# HELPER FUNCTIONS
# ============================================================================

def euclidean_distance(x1, y1, x2, y2):
    """
    Calculate Euclidean distance for planar grid coordinates

    Args:
        x1, y1: First point coordinates (shifted grid units)
        x2, y2: Second point coordinates (shifted grid units)

    Returns:
        Distance in grid units
    """
    return sqrt((x2 - x1)**2 + (y2 - y1)**2)
