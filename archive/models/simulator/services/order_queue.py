"""
Order queue service for managing order priorities and backlogs.
"""
import heapq
from typing import List, Dict, Optional, Tuple, Any
import logging

logger = logging.getLogger(__name__)


class OrderQueue:
    """
    Priority queue for managing orders based on platform_order_time.

    Features:
    - Priority by platform_order_time (oldest first)
    - Backlog management
    - Wait time tracking
    - Order lifecycle tracking
    """

    def __init__(self):
        """Initialize empty order queue."""
        self.queue = []  # Min-heap by platform_order_time
        self.backlog = []  # Orders that couldn't be assigned
        self.order_stats = {}  # Track per-order statistics
        self.total_orders_added = 0
        self.total_orders_assigned = 0
        self.total_orders_deferred = 0

    def add_order(self, order: Dict, waybill_lookup: Dict):
        """
        Add an order to the queue.

        Args:
            order: Order dict with at least 'order_id'
            waybill_lookup: Lookup for full order details including platform_order_time
        """
        order_id = order['order_id']
        if order_id in waybill_lookup:
            platform_time = waybill_lookup[order_id]['platform_order_time']
            heapq.heappush(self.queue, (platform_time, order))

            # Track order statistics
            self.order_stats[order_id] = {
                'added_at': platform_time,
                'attempts': 0,
                'status': 'queued'
            }
            self.total_orders_added += 1
        else:
            logger.warning(f"Order {order_id} not found in waybill lookup")

    def add_orders_batch(self, orders: List[Dict], waybill_lookup: Dict):
        """
        Add multiple orders to the queue.

        Args:
            orders: List of order dicts
            waybill_lookup: Lookup for full order details
        """
        for order in orders:
            self.add_order(order, waybill_lookup)

    def get_orders_until(self, time_threshold: int) -> List[Dict]:
        """
        Get all orders with platform_order_time <= threshold.

        Orders are removed from queue and returned in priority order.

        Args:
            time_threshold: Maximum platform_order_time to include

        Returns:
            List of orders ready for assignment
        """
        ready_orders = []

        while self.queue and self.queue[0][0] <= time_threshold:
            platform_time, order = heapq.heappop(self.queue)
            ready_orders.append(order)

            # Update statistics
            order_id = order['order_id']
            if order_id in self.order_stats:
                self.order_stats[order_id]['attempts'] += 1
                self.order_stats[order_id]['status'] = 'processing'

        return ready_orders

    def get_next_n_orders(self, n: int) -> List[Dict]:
        """
        Get next n orders from queue (for batch processing).

        Args:
            n: Number of orders to retrieve

        Returns:
            List of up to n orders
        """
        orders = []
        for _ in range(min(n, len(self.queue))):
            platform_time, order = heapq.heappop(self.queue)
            orders.append(order)

            # Update statistics
            order_id = order['order_id']
            if order_id in self.order_stats:
                self.order_stats[order_id]['attempts'] += 1
                self.order_stats[order_id]['status'] = 'processing'

        return orders

    def add_to_backlog(self, orders: List[Dict]):
        """
        Add unassigned orders to backlog.

        Args:
            orders: Orders that couldn't be assigned
        """
        self.backlog.extend(orders)
        for order in orders:
            order_id = order['order_id']
            if order_id in self.order_stats:
                self.order_stats[order_id]['status'] = 'backlog'
            self.total_orders_deferred += 1

    def get_backlog_orders(self) -> List[Dict]:
        """
        Get and clear backlog orders.

        Returns:
            List of backlog orders (oldest first)
        """
        backlog = self.backlog
        self.backlog = []

        # Update status
        for order in backlog:
            order_id = order['order_id']
            if order_id in self.order_stats:
                self.order_stats[order_id]['status'] = 'retry'

        return backlog

    def mark_assigned(self, order_ids: List[str], assignment_time: int):
        """
        Mark orders as successfully assigned.

        Args:
            order_ids: List of assigned order IDs
            assignment_time: Time when assigned
        """
        for order_id in order_ids:
            if order_id in self.order_stats:
                stats = self.order_stats[order_id]
                stats['status'] = 'assigned'
                stats['assigned_at'] = assignment_time
                stats['wait_time'] = assignment_time - stats['added_at']
                self.total_orders_assigned += 1

    def get_wait_time_stats(self) -> Dict[str, float]:
        """
        Calculate wait time statistics for assigned orders.

        Returns:
            Dict with avg, min, max, median wait times
        """
        wait_times = []
        for order_id, stats in self.order_stats.items():
            if stats['status'] == 'assigned' and 'wait_time' in stats:
                wait_times.append(stats['wait_time'])

        if not wait_times:
            return {
                'avg': 0,
                'min': 0,
                'max': 0,
                'median': 0,
                'count': 0
            }

        wait_times.sort()
        n = len(wait_times)

        return {
            'avg': sum(wait_times) / n,
            'min': wait_times[0],
            'max': wait_times[-1],
            'median': wait_times[n // 2],
            'count': n
        }

    def get_queue_length(self) -> int:
        """Get current queue length."""
        return len(self.queue)

    def get_backlog_length(self) -> int:
        """Get current backlog length."""
        return len(self.backlog)

    def get_summary_stats(self) -> Dict[str, Any]:
        """
        Get comprehensive queue statistics.

        Returns:
            Dictionary with queue metrics
        """
        status_counts = {}
        for stats in self.order_stats.values():
            status = stats['status']
            status_counts[status] = status_counts.get(status, 0) + 1

        return {
            'total_added': self.total_orders_added,
            'total_assigned': self.total_orders_assigned,
            'total_deferred': self.total_orders_deferred,
            'current_queue_length': self.get_queue_length(),
            'current_backlog_length': self.get_backlog_length(),
            'status_counts': status_counts,
            'wait_time_stats': self.get_wait_time_stats()
        }

    def clear(self):
        """Clear all queues and reset statistics."""
        self.queue = []
        self.backlog = []
        self.order_stats = {}
        self.total_orders_added = 0
        self.total_orders_assigned = 0
        self.total_orders_deferred = 0