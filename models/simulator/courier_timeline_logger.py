"""
Courier Timeline Logger

Tracks courier state transitions over time to enable calculation of idle time
and other courier-centric metrics during evaluation.
"""

import csv
from datetime import datetime


class CourierTimelineLogger:
    """
    Logs courier state transitions throughout the simulation

    Each row represents a state change event for a courier
    """

    def __init__(self, log_dir, model_name):
        """
        Initialize timeline logger

        Args:
            log_dir: Directory to save log file
            model_name: Name of the model (for filename)
        """
        self.log_dir = log_dir
        self.model_name = model_name
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')

        # Create courier timeline log
        self.timeline_log_path = f"{log_dir}/{model_name}_courier_timeline_{timestamp}.csv"
        self.timeline_csv = open(self.timeline_log_path, 'w', newline='')
        self.timeline_writer = csv.writer(self.timeline_csv)
        self.timeline_writer.writerow([
            'timestamp', 'courier_id', 'event_type', 'new_state', 'reason'
        ])

    def log_state_transition(self, timestamp, courier_id, new_state, reason):
        """
        Log a courier state transition

        Args:
            timestamp: Unix timestamp when transition occurred
            courier_id: ID of the courier
            new_state: New state (AVAILABLE, BUSY)
            reason: Why the transition happened (e.g., 'assigned_order', 'completed_delivery', 'initialized')
        """
        event_type = 'state_change'

        self.timeline_writer.writerow([
            timestamp, courier_id, event_type, new_state, reason
        ])

    def flush(self):
        """Flush CSV buffer to disk"""
        self.timeline_csv.flush()

    def close(self):
        """Close CSV file"""
        self.timeline_csv.close()

    def get_log_path(self):
        """Get path to timeline log file"""
        return self.timeline_log_path
