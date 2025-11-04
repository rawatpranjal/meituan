"""
Clustered MILP strategy with spatial partitioning for scalable many-to-one matching.
Uses grid-based clustering to divide large problems into tractable sub-problems.
"""
from typing import List, Dict, Tuple, Optional, Any
import logging
import time

# OR-Tools import
try:
    from ortools.sat.python import cp_model
except ImportError:
    raise ImportError(
        "OR-Tools is required for Clustered MILP strategy. "
        "Install with: pip3 install --break-system-packages ortools"
    )

from . import BaseStrategy
from ..simulator.services.spatial_clustering import SpatialClusterer
from ..simulator.services.capacity_tokens import CapacityTokenManager

logger = logging.getLogger(__name__)


class ClusteredMILPStrategy(BaseStrategy):
    """
    Clustered MILP strategy for large-scale dispatch.

    Key features:
    - Spatial clustering to partition orders into grid cells
    - Per-cluster MILP solving with capacity constraints
    - Capacity token management to prevent courier double-booking
    - Sequential processing with largest clusters first
    """

    def __init__(self, cost_function,
                 grid_size: float = 25000,
                 halo_radius: float = 40000,
                 max_orders_per_cluster: int = 200,
                 max_orders_per_courier: int = 5,
                 milp_time_limit_sec: int = 8,
                 milp_gap: float = 0.05,
                 max_candidates_per_order: int = 50):
        """
        Initialize clustered MILP strategy.

        Args:
            cost_function: Cost function for scoring assignments
            grid_size: Size of grid cells in microdegrees (default 25000 ≈ 2.8km)
            halo_radius: Radius for courier visibility (default 40000 ≈ 4.4km)
            max_orders_per_cluster: Max orders per MILP instance (default 200)
            max_orders_per_courier: Max orders per courier (default 5)
            milp_time_limit_sec: Time limit per cluster MILP (default 8)
            milp_gap: Optimality gap for MILP (default 0.05 = 5%)
            max_candidates_per_order: Max candidate couriers per order (default 50)
        """
        super().__init__(cost_function)

        # Clustering parameters
        self.spatial_clusterer = SpatialClusterer(
            grid_size=grid_size,
            halo_radius=halo_radius,
            max_orders_per_cluster=max_orders_per_cluster
        )

        # Capacity management
        self.capacity_manager = CapacityTokenManager(
            max_orders_per_courier=max_orders_per_courier
        )

        # MILP parameters
        self.milp_time_limit_sec = milp_time_limit_sec
        self.milp_gap = milp_gap
        self.max_candidates_per_order = max_candidates_per_order
        self.max_orders_per_courier = max_orders_per_courier

    def get_name(self) -> str:
        """Return strategy name for logging."""
        return "clustered_milp_vrp"

    def make_assignments(
        self,
        waiting_orders: List[Dict],
        available_couriers: List[Dict],
        waybill_lookup: Dict,
        candidates: Optional[List[Tuple]] = None,
        bundle_mapping: Optional[Dict] = None
    ) -> List[Tuple[Dict, Dict, float]]:
        """
        Make assignments using clustered MILP optimization.

        Args:
            waiting_orders: Orders/bundles to assign
            available_couriers: Available couriers
            waybill_lookup: Full order details
            candidates: Optional pre-computed candidates (not used in clustering)
            bundle_mapping: Optional bundle mapping

        Returns:
            List of (order, courier, cost) tuples
        """
        if not waiting_orders or not available_couriers:
            return []

        start_time = time.time()
        total_assignments = []

        logger.info(f"Clustered MILP starting: {len(waiting_orders)} orders, "
                   f"{len(available_couriers)} couriers")

        # Initialize capacity manager
        self.capacity_manager.initialize(available_couriers)

        # Cluster orders spatially
        clusters = self.spatial_clusterer.cluster_orders(
            waiting_orders, waybill_lookup
        )
        logger.info(f"Created {len(clusters)} spatial clusters")

        # Sort clusters by priority (largest first for better utilization)
        sorted_clusters = sorted(
            clusters.items(),
            key=lambda x: len(x[1]),
            reverse=True
        )

        # Process each cluster
        for cluster_idx, (cluster_id, cluster_orders) in enumerate(sorted_clusters):
            if not cluster_orders:
                continue

            # Get cluster center for courier filtering
            if len(cluster_id) == 2:
                # Standard grid cell
                cluster_center = self.spatial_clusterer._get_cell_center(cluster_id)
            else:
                # Sub-cluster (has third index)
                base_cell = (cluster_id[0], cluster_id[1])
                cluster_center = self.spatial_clusterer._get_cell_center(base_cell)

            # Get nearby couriers with available capacity
            nearby_couriers = self.spatial_clusterer.get_couriers_for_cluster(
                cluster_center, available_couriers
            )
            available_nearby = self.capacity_manager.filter_couriers_with_capacity(
                nearby_couriers, min_capacity=1
            )

            if not available_nearby:
                logger.warning(f"Cluster {cluster_id}: No available couriers nearby, "
                             f"{len(cluster_orders)} orders unassigned")
                continue

            logger.info(f"Cluster {cluster_idx+1}/{len(clusters)} (ID: {cluster_id}): "
                       f"{len(cluster_orders)} orders, {len(available_nearby)} couriers")

            # Solve MILP for this cluster
            cluster_assignments = self._solve_cluster_milp(
                cluster_orders,
                available_nearby,
                waybill_lookup,
                cluster_id
            )

            # Reserve capacity for assignments
            for order, courier, cost in cluster_assignments:
                order_id = order['order_id']
                courier_id = courier['courier_id']

                # Reserve capacity
                if self.capacity_manager.reserve(courier_id, [order_id]):
                    total_assignments.append((order, courier, cost))
                else:
                    logger.warning(f"Could not reserve capacity for courier {courier_id}, "
                                 f"skipping assignment")

        # Log final statistics
        elapsed = time.time() - start_time
        self.capacity_manager.log_utilization()
        logger.info(f"Clustered MILP completed in {elapsed:.2f}s: "
                   f"{len(total_assignments)}/{len(waiting_orders)} orders assigned "
                   f"({len(total_assignments)/len(waiting_orders)*100:.1f}%)")

        return total_assignments

    def _solve_cluster_milp(self,
                           orders: List[Dict],
                           couriers: List[Dict],
                           waybill_lookup: Dict,
                           cluster_id: Any) -> List[Tuple[Dict, Dict, float]]:
        """
        Solve MILP for a single cluster.

        Args:
            orders: Orders in this cluster
            couriers: Available couriers for this cluster (with capacity)
            waybill_lookup: Full order details
            cluster_id: Cluster identifier for logging

        Returns:
            List of (order, courier, cost) tuples
        """
        if not orders or not couriers:
            return []

        logger.debug(f"Solving MILP for cluster {cluster_id}: "
                    f"{len(orders)} orders, {len(couriers)} couriers")

        # Create model
        model = cp_model.CpModel()

        # Build cost matrix
        costs = {}
        valid_pairs = []

        for order in orders:
            order_id = order['order_id']
            order_details = waybill_lookup.get(order_id, {})

            # Limit candidates per order
            courier_count = 0
            for courier in couriers:
                if courier_count >= self.max_candidates_per_order:
                    break

                courier_id = courier['courier_id']

                # Check capacity before adding to valid pairs
                if not self.capacity_manager.has_capacity(courier_id):
                    continue

                # Compute cost
                cost = self.cost_function.compute_cost(
                    courier, order, order_details
                )

                valid_pairs.append((order_id, courier_id))
                # Scale to prevent overflow
                costs[(order_id, courier_id)] = int(cost / 1000)
                courier_count += 1

        if not valid_pairs:
            logger.warning(f"Cluster {cluster_id}: No valid pairs found")
            return []

        # Create decision variables
        x = {}
        for order_id, courier_id in valid_pairs:
            x[(order_id, courier_id)] = model.NewBoolVar(f'x_{order_id}_{courier_id}')

        # Constraint 1: Each order assigned to at most one courier
        for order in orders:
            order_id = order['order_id']
            order_assignments = [
                x[(oid, cid)] for oid, cid in valid_pairs
                if oid == order_id
            ]
            if order_assignments:
                model.Add(sum(order_assignments) <= 1)

        # Constraint 2: Courier capacity constraints (accounting for existing assignments)
        for courier in couriers:
            courier_id = courier['courier_id']
            available_capacity = self.capacity_manager.get_available_capacity(courier_id)

            courier_assignments = [
                x[(oid, cid)] for oid, cid in valid_pairs
                if cid == courier_id
            ]
            if courier_assignments and available_capacity > 0:
                model.Add(sum(courier_assignments) <= available_capacity)

        # Objective: Minimize cost + penalty for unassigned
        BIG_PENALTY = 1000000

        # Add unassigned variables
        unassigned = {}
        for order in orders:
            order_id = order['order_id']
            unassigned[order_id] = model.NewBoolVar(f'unassigned_{order_id}')

            # Order is either assigned or unassigned
            order_assignments = [
                x[(oid, cid)] for oid, cid in valid_pairs
                if oid == order_id
            ]
            if order_assignments:
                model.Add(sum(order_assignments) + unassigned[order_id] == 1)

        # Build objective
        objective_terms = []

        # Assignment costs
        for (order_id, courier_id) in valid_pairs:
            objective_terms.append(x[(order_id, courier_id)] * costs[(order_id, courier_id)])

        # Unassigned penalties
        for order_id in unassigned:
            objective_terms.append(unassigned[order_id] * BIG_PENALTY)

        model.Minimize(sum(objective_terms))

        # Solve
        solver = cp_model.CpSolver()
        solver.parameters.max_time_in_seconds = self.milp_time_limit_sec
        solver.parameters.relative_gap_limit = self.milp_gap
        solver.parameters.log_search_progress = False

        status = solver.Solve(model)

        # Extract solution
        assignments = []

        if status == cp_model.OPTIMAL or status == cp_model.FEASIBLE:
            # Map IDs back to objects
            order_map = {o['order_id']: o for o in orders}
            courier_map = {c['courier_id']: c for c in couriers}

            assigned_count = 0
            for (order_id, courier_id) in valid_pairs:
                if solver.Value(x[(order_id, courier_id)]) == 1:
                    order = order_map[order_id]
                    courier = courier_map[courier_id]
                    cost = costs[(order_id, courier_id)] * 1000.0  # Scale back
                    assignments.append((order, courier, cost))
                    assigned_count += 1

            logger.debug(f"Cluster {cluster_id} MILP: {assigned_count}/{len(orders)} assigned, "
                        f"status={solver.StatusName(status)}")
        else:
            logger.warning(f"Cluster {cluster_id} MILP failed: {solver.StatusName(status)}")

        return assignments

    def assign_single_order(
        self,
        order: Dict,
        available_couriers: List[Dict],
        waybill_lookup: Dict
    ) -> Optional[Tuple[Dict, float]]:
        """
        Assign a single order (for real-time mode compatibility).

        For single orders, falls back to greedy selection.

        Args:
            order: Single order to assign
            available_couriers: Available couriers
            waybill_lookup: Full order details

        Returns:
            (courier, cost) tuple if successful, None otherwise
        """
        if not available_couriers:
            return None

        # For single order, use simple greedy with capacity check
        order_details = waybill_lookup.get(order['order_id'], {})

        best_courier = None
        best_cost = float('inf')

        for courier in available_couriers:
            # Check capacity
            if not self.capacity_manager.has_capacity(courier['courier_id']):
                continue

            cost = self.cost_function.compute_cost(courier, order, order_details)
            if cost < best_cost:
                best_cost = cost
                best_courier = courier

        if best_courier:
            # Reserve capacity
            if self.capacity_manager.reserve(best_courier['courier_id'], [order['order_id']]):
                return (best_courier, best_cost)

        return None