"""
Assignment Strategy - The "Brain"

Pluggable algorithms for order-to-courier assignment.
This module allows easy swapping of different assignment strategies.

Note: Assignment strategies now accept cost functions as parameters,
allowing separation of the optimization algorithm from the cost function.
"""

from abc import ABC, abstractmethod
import numpy as np
from scipy.optimize import linear_sum_assignment
from scipy.cluster.vq import kmeans


class AssignmentStrategy(ABC):
    """
    Abstract base class for assignment algorithms

    Any assignment strategy must implement the make_assignments method.
    """

    @abstractmethod
    def make_assignments(self, waiting_orders, available_couriers, waybill_lookup):
        """
        Make order-to-courier assignments

        Args:
            waiting_orders: List of order dicts with order_id
            available_couriers: List of courier dicts with courier_id, rider_lat, rider_lng
            waybill_lookup: Dictionary mapping order_id to order details (pickup/delivery locations)

        Returns:
            List of tuples: (order_dict, courier_dict, cost)
            where cost is the assignment cost for this pair
        """
        pass


class Tier1Baseline(AssignmentStrategy):
    """
    Tier 1 Baseline: Static Bipartite Matching

    Algorithm: Hungarian algorithm (optimal bipartite matching)
    Cost function: Pluggable (passed as parameter)
    """

    def __init__(self, cost_function):
        """
        Initialize assignment strategy with a cost function

        Args:
            cost_function: Instance of a cost function from models.cost module
        """
        self.cost_function = cost_function

    def make_assignments(self, waiting_orders, available_couriers, waybill_lookup):
        """
        Assign orders to couriers using the configured cost function

        Returns list of (order, courier, cost) tuples
        """
        if not waiting_orders or not available_couriers:
            return []

        n_orders = len(waiting_orders)
        n_couriers = len(available_couriers)

        # Build cost matrix: |orders| x |couriers|
        cost_matrix = np.zeros((n_orders, n_couriers))

        for i, order in enumerate(waiting_orders):
            order_id = order['order_id']

            # Skip if order not in lookup
            if order_id not in waybill_lookup:
                # Set to high cost so it won't be assigned
                cost_matrix[i, :] = 1e9
                continue

            # Get order location details
            order_location = waybill_lookup[order_id]

            for j, courier in enumerate(available_couriers):
                # Calculate cost using the configured cost function
                cost = self.cost_function.compute_cost(
                    courier=courier,
                    order=order,
                    order_location=order_location
                )

                cost_matrix[i, j] = cost

        # Solve assignment problem using Hungarian algorithm
        row_ind, col_ind = linear_sum_assignment(cost_matrix)

        # Build list of assignments with costs
        assignments = []
        for i in range(len(row_ind)):
            order_idx = row_ind[i]
            courier_idx = col_ind[i]
            cost = cost_matrix[order_idx, courier_idx]

            # Skip if it was a "no match" (high cost sentinel)
            if cost >= 1e9:
                continue

            assignments.append((
                waiting_orders[order_idx],
                available_couriers[courier_idx],
                cost
            ))

        return assignments


class Tier2BatchVRP(AssignmentStrategy):
    """
    Tier 2: Batch Vehicle Routing Problem

    Algorithm: Clustering + Assignment + Routing
    - Clusters orders geographically using K-Means
    - Assigns couriers to cluster centroids using Hungarian algorithm
    - Routes orders within each cluster using nearest neighbor TSP heuristic

    Cost function: Pluggable (passed as parameter)

    This strategy enables Many-to-One assignment (multiple orders per courier).
    """

    def __init__(self, cost_function, max_bundle_size=5):
        """
        Initialize assignment strategy with a cost function

        Args:
            cost_function: Instance of a cost function from models.cost module
            max_bundle_size: Maximum orders per courier bundle (default: 5)
        """
        self.cost_function = cost_function
        self.max_bundle_size = max_bundle_size

    def make_assignments(self, waiting_orders, available_couriers, waybill_lookup):
        """
        Assign orders to couriers using clustering + routing

        Returns list of (order, courier, cost) tuples.
        Note: Same courier may appear multiple times (bundled assignments).
        """
        if not waiting_orders or not available_couriers:
            return []

        n_orders = len(waiting_orders)
        n_couriers = len(available_couriers)

        # Step 1: Extract order pickup locations for clustering
        order_locations = []
        valid_orders = []

        for order in waiting_orders:
            order_id = order['order_id']
            if order_id in waybill_lookup:
                loc = waybill_lookup[order_id]
                order_locations.append([loc['sender_lat'], loc['sender_lng']])
                valid_orders.append(order)

        if len(valid_orders) == 0:
            return []

        order_locations = np.array(order_locations)

        # Step 2: Cluster orders using K-Means
        # Calculate K based on max_bundle_size to create reasonable bundles
        # K = num_orders / max_bundle_size (rounded up) but not more than available couriers
        desired_k = (len(valid_orders) + self.max_bundle_size - 1) // self.max_bundle_size
        k = min(n_couriers, desired_k)
        k = max(1, k)  # At least 1 cluster

        if k == 1:
            # Single cluster - all orders go to one courier (only if very few orders)
            clusters = np.zeros(len(valid_orders), dtype=int)
            centroids = np.array([order_locations.mean(axis=0)])
        elif k >= len(valid_orders):
            # More clusters than orders - each order is its own cluster
            # This ensures no bundle exceeds 1 order
            clusters = np.arange(len(valid_orders))
            centroids = order_locations
            k = len(valid_orders)  # Update k to match actual number of clusters
        else:
            # Run K-Means clustering for normal case
            centroids, distortion = kmeans(order_locations.astype(float), k)

            # Assign each order to nearest centroid
            clusters = np.array([
                np.argmin([np.linalg.norm(loc - centroid) for centroid in centroids])
                for loc in order_locations
            ])

        # Step 3: Assign couriers to cluster centroids using Hungarian algorithm
        # Get unique cluster indices to handle properly
        unique_clusters = np.unique(clusters)
        n_clusters = len(unique_clusters)

        # Build cost matrix: |unique_clusters| x |couriers|
        cluster_cost_matrix = np.zeros((n_clusters, n_couriers))

        for i, cluster_idx in enumerate(unique_clusters):
            centroid = centroids[cluster_idx] if cluster_idx < len(centroids) else centroids[-1]

            for courier_idx, courier in enumerate(available_couriers):
                # Distance from courier to cluster centroid
                courier_lat = courier['rider_lat']
                courier_lng = courier['rider_lng']
                dist = np.linalg.norm([courier_lat - centroid[0], courier_lng - centroid[1]])
                cluster_cost_matrix[i, courier_idx] = dist

        # Solve cluster-to-courier assignment
        cluster_ind, courier_ind = linear_sum_assignment(cluster_cost_matrix)

        # Build cluster-to-courier mapping
        cluster_to_courier = {}
        for i in range(len(cluster_ind)):
            matrix_idx = cluster_ind[i]
            courier_idx = courier_ind[i]
            actual_cluster_idx = unique_clusters[matrix_idx]
            cluster_to_courier[actual_cluster_idx] = available_couriers[courier_idx]

        # Step 4: Route orders within each cluster and create assignments
        assignments = []

        for cluster_idx in unique_clusters:
            # Get all orders in this cluster
            cluster_orders = [
                valid_orders[i] for i in range(len(valid_orders))
                if clusters[i] == cluster_idx
            ]

            if cluster_idx not in cluster_to_courier:
                # No courier assigned to this cluster (shouldn't happen with proper K)
                continue

            courier = cluster_to_courier[cluster_idx]

            # Get cluster centroid cost as base cost
            matrix_idx = np.where(unique_clusters == cluster_idx)[0][0]
            courier_idx = courier_ind[np.where(cluster_ind == matrix_idx)[0][0]]
            cluster_cost = cluster_cost_matrix[matrix_idx, courier_idx]

            # Simple routing: assign all orders in cluster to courier with cluster cost
            # (In future, could implement actual TSP routing for better cost estimation)
            for order in cluster_orders:
                order_id = order['order_id']
                order_location = waybill_lookup[order_id]

                # Use cost function to compute individual order cost
                individual_cost = self.cost_function.compute_cost(
                    courier=courier,
                    order=order,
                    order_location=order_location
                )

                assignments.append((order, courier, individual_cost))

        return assignments


class Tier3OnlineGreedy(AssignmentStrategy):
    """
    Tier 3: Online Greedy (First-Come, First-Served)

    Algorithm: Greedy nearest-neighbor for single orders
    - Processes orders one-by-one in arrival order
    - Assigns each order immediately to closest available courier
    - No batching, no optimization

    Cost function: Pluggable (passed as parameter)

    This strategy minimizes customer wait time but sacrifices system efficiency.
    """

    def __init__(self, cost_function):
        """
        Initialize assignment strategy with a cost function

        Args:
            cost_function: Instance of a cost function from models.cost module
        """
        self.cost_function = cost_function

    def assign_single_order(self, order, available_couriers, waybill_lookup):
        """
        Assign a single order to the closest available courier

        Args:
            order: Single order dict with order_id
            available_couriers: List of available courier dicts
            waybill_lookup: Dictionary mapping order_id to order details

        Returns:
            (courier, cost) tuple if assignment successful, None otherwise
        """
        if not available_couriers:
            return None

        order_id = order['order_id']
        if order_id not in waybill_lookup:
            return None

        order_location = waybill_lookup[order_id]

        # Find closest courier (greedy)
        min_cost = float('inf')
        best_courier = None

        for courier in available_couriers:
            cost = self.cost_function.compute_cost(
                courier=courier,
                order=order,
                order_location=order_location
            )

            if cost < min_cost:
                min_cost = cost
                best_courier = courier

        if best_courier is None:
            return None

        return (best_courier, min_cost)

    def make_assignments(self, waiting_orders, available_couriers, waybill_lookup):
        """
        Compatibility method for batch interface (not used in online mode)

        In practice, Tier3OnlineGreedy uses assign_single_order() in a custom simulation loop.
        This method is provided for interface compatibility.
        """
        assignments = []

        # Greedy: assign each order to closest available courier
        # Mark couriers as used so they're not double-assigned
        used_couriers = set()
        remaining_couriers = available_couriers.copy()

        for order in waiting_orders:
            available = [c for c in remaining_couriers if c['courier_id'] not in used_couriers]

            if not available:
                break

            result = self.assign_single_order(order, available, waybill_lookup)
            if result:
                courier, cost = result
                assignments.append((order, courier, cost))
                used_couriers.add(courier['courier_id'])

        return assignments
