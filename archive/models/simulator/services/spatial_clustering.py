"""
Spatial clustering service for partitioning orders and couriers into grid cells.
Enables processing large batches by dividing them into tractable local problems.
"""
from typing import Dict, List, Tuple, Set
import logging
import numpy as np
from collections import defaultdict

logger = logging.getLogger(__name__)


class SpatialClusterer:
    """
    Grid-based spatial clustering for orders and couriers.

    Uses grid cells to partition the delivery area, with halo regions
    for courier visibility across boundaries.
    """

    def __init__(self,
                 grid_size: float = 25000,
                 halo_radius: float = 40000,
                 max_orders_per_cluster: int = 200):
        """
        Initialize spatial clusterer.

        Args:
            grid_size: Size of grid cells in microdegrees (default 25000 ≈ 2.8km)
            halo_radius: Radius for courier visibility in microdegrees (default 40000 ≈ 4.4km)
            max_orders_per_cluster: Maximum orders per cluster for MILP tractability
        """
        self.grid_size = grid_size
        self.halo_radius = halo_radius
        self.max_orders_per_cluster = max_orders_per_cluster

        logger.info(f"SpatialClusterer initialized: grid_size={grid_size}, "
                   f"halo_radius={halo_radius}, max_orders={max_orders_per_cluster}")

    def _get_grid_cell(self, lat: float, lng: float) -> Tuple[int, int]:
        """
        Get grid cell index for a coordinate.

        Args:
            lat: Latitude in microdegrees
            lng: Longitude in microdegrees

        Returns:
            (row, col) grid indices
        """
        row = int(lat / self.grid_size)
        col = int(lng / self.grid_size)
        return (row, col)

    def _get_cell_center(self, cell: Tuple[int, int]) -> Tuple[float, float]:
        """
        Get center coordinates of a grid cell.

        Args:
            cell: (row, col) grid indices

        Returns:
            (lat, lng) center coordinates in microdegrees
        """
        row, col = cell
        lat = (row + 0.5) * self.grid_size
        lng = (col + 0.5) * self.grid_size
        return (lat, lng)

    def _compute_distance(self, lat1: float, lng1: float,
                         lat2: float, lng2: float) -> float:
        """
        Compute Euclidean distance between two points.

        Args:
            lat1, lng1: First point in microdegrees
            lat2, lng2: Second point in microdegrees

        Returns:
            Distance in microdegrees
        """
        return np.sqrt((lat1 - lat2) ** 2 + (lng1 - lng2) ** 2)

    def cluster_orders(self, orders: List[Dict],
                      waybill_lookup: Dict) -> Dict[Tuple[int, int], List[Dict]]:
        """
        Partition orders into spatial grid cells.

        Args:
            orders: List of order dictionaries
            waybill_lookup: Full order details keyed by order_id

        Returns:
            Dictionary mapping grid cells to lists of orders
        """
        clusters = defaultdict(list)

        for order in orders:
            order_id = order['order_id']
            order_details = waybill_lookup.get(order_id, {})

            # Use restaurant location for clustering (sender coordinates)
            sender_lat = order_details.get('sender_lat', 0)
            sender_lng = order_details.get('sender_lng', 0)

            if sender_lat == 0 or sender_lng == 0:
                logger.warning(f"Order {order_id} has invalid coordinates, skipping")
                continue

            # Assign to grid cell
            cell = self._get_grid_cell(sender_lat, sender_lng)
            clusters[cell].append(order)

        # Split large clusters if necessary
        final_clusters = {}
        for cell, cell_orders in clusters.items():
            if len(cell_orders) <= self.max_orders_per_cluster:
                final_clusters[cell] = cell_orders
            else:
                # Split into sub-clusters
                logger.info(f"Cell {cell} has {len(cell_orders)} orders, splitting...")
                sub_clusters = self._split_large_cluster(cell, cell_orders, waybill_lookup)
                for sub_idx, sub_orders in enumerate(sub_clusters):
                    # Create sub-cell identifier
                    sub_cell = (cell[0], cell[1], sub_idx)
                    final_clusters[sub_cell] = sub_orders

        logger.info(f"Created {len(final_clusters)} clusters from {len(orders)} orders")
        for cell, cell_orders in final_clusters.items():
            logger.debug(f"  Cluster {cell}: {len(cell_orders)} orders")

        return final_clusters

    def _split_large_cluster(self, cell: Tuple[int, int],
                            orders: List[Dict],
                            waybill_lookup: Dict) -> List[List[Dict]]:
        """
        Split a large cluster into smaller sub-clusters.

        Uses K-means-like approach to create balanced sub-clusters.

        Args:
            cell: Original grid cell
            orders: Orders in the cell
            waybill_lookup: Full order details

        Returns:
            List of sub-clusters
        """
        n_clusters = (len(orders) + self.max_orders_per_cluster - 1) // self.max_orders_per_cluster

        # Get order coordinates
        coords = []
        for order in orders:
            order_details = waybill_lookup.get(order['order_id'], {})
            lat = order_details.get('sender_lat', 0)
            lng = order_details.get('sender_lng', 0)
            coords.append([lat, lng])

        coords = np.array(coords)

        # Simple K-means clustering
        centroids = coords[np.random.choice(len(coords), n_clusters, replace=False)]

        for _ in range(10):  # Max 10 iterations
            # Assign to nearest centroid
            assignments = []
            for coord in coords:
                distances = [np.linalg.norm(coord - centroid) for centroid in centroids]
                assignments.append(np.argmin(distances))

            # Update centroids
            new_centroids = []
            for k in range(n_clusters):
                cluster_coords = coords[np.array(assignments) == k]
                if len(cluster_coords) > 0:
                    new_centroids.append(cluster_coords.mean(axis=0))
                else:
                    new_centroids.append(centroids[k])

            centroids = np.array(new_centroids)

        # Create sub-clusters
        sub_clusters = [[] for _ in range(n_clusters)]
        for order, assignment in zip(orders, assignments):
            sub_clusters[assignment].append(order)

        return [sc for sc in sub_clusters if sc]  # Filter empty clusters

    def get_couriers_for_cluster(self, cluster_center: Tuple[float, float],
                                couriers: List[Dict],
                                halo_radius: float = None) -> List[Dict]:
        """
        Get couriers within halo radius of cluster center.

        Args:
            cluster_center: (lat, lng) center of cluster in microdegrees
            couriers: List of courier dictionaries with location
            halo_radius: Override default halo radius

        Returns:
            List of couriers within radius
        """
        if halo_radius is None:
            halo_radius = self.halo_radius

        center_lat, center_lng = cluster_center
        nearby_couriers = []

        for courier in couriers:
            # Courier location from dict
            courier_lat = courier.get('lat', 0)
            courier_lng = courier.get('lng', 0)

            if courier_lat == 0 or courier_lng == 0:
                continue

            # Check distance
            distance = self._compute_distance(
                center_lat, center_lng,
                courier_lat, courier_lng
            )

            if distance <= halo_radius:
                nearby_couriers.append(courier)

        return nearby_couriers

    def get_cluster_priority(self, cluster: Tuple,
                           orders: List[Dict],
                           waybill_lookup: Dict) -> float:
        """
        Compute priority score for processing order of clusters.

        Higher priority = process first.
        Based on order age and cluster size.

        Args:
            cluster: Cluster identifier
            orders: Orders in cluster
            waybill_lookup: Full order details

        Returns:
            Priority score (higher = more urgent)
        """
        if not orders:
            return 0.0

        # Average order age (older = higher priority)
        total_age = 0
        valid_count = 0

        for order in orders:
            order_details = waybill_lookup.get(order['order_id'], {})
            order_time = order_details.get('platform_order_time', 0)
            if order_time > 0:
                # Assuming current_time is max of all order times
                # In practice, would use actual current timestamp
                total_age += order_time
                valid_count += 1

        avg_age = total_age / valid_count if valid_count > 0 else 0

        # Cluster size factor (larger clusters = higher priority)
        size_factor = len(orders)

        # Combined priority (can be tuned)
        priority = -avg_age + size_factor * 0.1  # Negative age so older = higher

        return priority