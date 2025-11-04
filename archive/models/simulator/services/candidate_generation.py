"""
Candidate generation service for creating sparse courier-order pairs.
"""
from typing import List, Dict, Tuple, Optional, Set, Any
import logging
from ..physics import euclidean_distance

logger = logging.getLogger(__name__)


class CandidateGenerator:
    """
    Generate sparse candidate pairs between couriers and orders.

    This reduces the problem size by only considering feasible pairs
    based on distance, time windows, or other constraints.
    """

    def __init__(
        self,
        max_pickup_radius: float = 10.0,
        max_candidates_per_order: int = 20,
        max_candidates_per_courier: int = 50
    ):
        """
        Initialize candidate generator with constraints.

        Args:
            max_pickup_radius: Maximum distance for courier-to-pickup (grid units)
            max_candidates_per_order: Max couriers to consider per order
            max_candidates_per_courier: Max orders to consider per courier
        """
        self.max_pickup_radius = max_pickup_radius
        self.max_candidates_per_order = max_candidates_per_order
        self.max_candidates_per_courier = max_candidates_per_courier

    def generate(
        self,
        orders: List[Dict],
        couriers: List[Dict],
        waybill_lookup: Dict,
        time_windows: Optional[Dict] = None
    ) -> List[Tuple[str, str, float]]:
        """
        Generate sparse candidate pairs.

        Args:
            orders: List of orders to assign
            couriers: List of available couriers
            waybill_lookup: Full order details
            time_windows: Optional time window constraints

        Returns:
            List of (order_id, courier_id, distance) tuples
        """
        candidates = []

        # Pre-compute order locations
        order_locations = {}
        for order in orders:
            order_id = order['order_id']
            if order_id in waybill_lookup:
                details = waybill_lookup[order_id]
                order_locations[order_id] = (
                    details['sender_lat'],
                    details['sender_lng']
                )

        # Generate candidates for each order
        for order in orders:
            order_id = order['order_id']
            if order_id not in order_locations:
                continue

            order_lat, order_lng = order_locations[order_id]

            # Calculate distance to all couriers
            courier_distances = []
            for courier in couriers:
                courier_id = courier['courier_id']
                courier_lat = courier['lat']
                courier_lng = courier['lng']

                distance = euclidean_distance(
                    courier_lat, courier_lng,
                    order_lat, order_lng
                )

                # Apply radius filter
                if distance <= self.max_pickup_radius:
                    courier_distances.append((courier_id, distance))

            # Sort by distance and take top K
            courier_distances.sort(key=lambda x: x[1])
            top_couriers = courier_distances[:self.max_candidates_per_order]

            # Add candidates
            for courier_id, distance in top_couriers:
                candidates.append((order_id, courier_id, distance))

        # Apply per-courier limit
        candidates = self._apply_courier_limit(candidates)

        logger.info(f"Generated {len(candidates)} candidate pairs from "
                   f"{len(orders)} orders and {len(couriers)} couriers")

        return candidates

    def generate_with_bundles(
        self,
        bundles: List[Dict],
        bundle_mapping: Dict[str, List[Dict]],
        couriers: List[Dict],
        waybill_lookup: Dict
    ) -> List[Tuple[str, str, float]]:
        """
        Generate candidates for bundled orders.

        Args:
            bundles: List of bundle units
            bundle_mapping: Maps bundle_id to list of orders
            couriers: Available couriers
            waybill_lookup: Full order details

        Returns:
            List of (bundle_id, courier_id, distance) tuples
        """
        candidates = []

        for bundle in bundles:
            bundle_id = bundle['order_id']  # Bundle uses first order's ID as identifier

            # Get bundle centroid or use first order location
            if bundle_id in bundle_mapping:
                orders_in_bundle = bundle_mapping[bundle_id]
                bundle_lat, bundle_lng = self._compute_bundle_centroid(
                    orders_in_bundle, waybill_lookup
                )
            else:
                # Single order "bundle"
                if bundle_id in waybill_lookup:
                    details = waybill_lookup[bundle_id]
                    bundle_lat = details['sender_lat']
                    bundle_lng = details['sender_lng']
                else:
                    continue

            # Find nearest couriers
            courier_distances = []
            for courier in couriers:
                courier_id = courier['courier_id']
                distance = euclidean_distance(
                    courier['lat'], courier['lng'],
                    bundle_lat, bundle_lng
                )

                if distance <= self.max_pickup_radius:
                    courier_distances.append((courier_id, distance))

            # Sort and limit
            courier_distances.sort(key=lambda x: x[1])
            top_couriers = courier_distances[:self.max_candidates_per_order]

            for courier_id, distance in top_couriers:
                candidates.append((bundle_id, courier_id, distance))

        return self._apply_courier_limit(candidates)

    def _compute_bundle_centroid(
        self,
        orders: List[Dict],
        waybill_lookup: Dict
    ) -> Tuple[float, float]:
        """
        Compute geographic centroid of orders in a bundle.

        Args:
            orders: Orders in the bundle
            waybill_lookup: Full order details

        Returns:
            (centroid_lat, centroid_lng)
        """
        total_lat = 0
        total_lng = 0
        count = 0

        for order in orders:
            order_id = order['order_id']
            if order_id in waybill_lookup:
                details = waybill_lookup[order_id]
                total_lat += details['sender_lat']
                total_lng += details['sender_lng']
                count += 1

        if count > 0:
            return (total_lat / count, total_lng / count)
        else:
            return (0, 0)

    def _apply_courier_limit(
        self,
        candidates: List[Tuple[str, str, float]]
    ) -> List[Tuple[str, str, float]]:
        """
        Limit the number of candidates per courier.

        Args:
            candidates: All candidate pairs

        Returns:
            Filtered candidates respecting per-courier limit
        """
        # Count candidates per courier
        courier_counts = {}
        for order_id, courier_id, distance in candidates:
            courier_counts[courier_id] = courier_counts.get(courier_id, 0) + 1

        # Filter if any courier exceeds limit
        filtered = []
        courier_added = {}

        # Sort by distance to prioritize closer pairs
        candidates.sort(key=lambda x: x[2])

        for order_id, courier_id, distance in candidates:
            current_count = courier_added.get(courier_id, 0)
            if current_count < self.max_candidates_per_courier:
                filtered.append((order_id, courier_id, distance))
                courier_added[courier_id] = current_count + 1

        return filtered

    def get_candidate_matrix(
        self,
        candidates: List[Tuple[str, str, float]],
        orders: List[Dict],
        couriers: List[Dict]
    ) -> Dict[str, Dict[str, float]]:
        """
        Convert candidate list to nested dict for easy lookup.

        Args:
            candidates: List of (order_id, courier_id, distance) tuples
            orders: List of orders
            couriers: List of couriers

        Returns:
            Nested dict: matrix[order_id][courier_id] = distance
        """
        matrix = {}

        for order in orders:
            order_id = order['order_id']
            matrix[order_id] = {}

        for order_id, courier_id, distance in candidates:
            if order_id in matrix:
                matrix[order_id][courier_id] = distance

        return matrix

    def validate_candidates(
        self,
        candidates: List[Tuple[str, str, float]],
        orders: List[Dict],
        couriers: List[Dict]
    ) -> Dict[str, Any]:
        """
        Validate and analyze candidate generation quality.

        Args:
            candidates: Generated candidates
            orders: Original orders
            couriers: Original couriers

        Returns:
            Dict with validation metrics
        """
        order_ids = {o['order_id'] for o in orders}
        courier_ids = {c['courier_id'] for c in couriers}

        # Track coverage
        orders_with_candidates = set()
        couriers_with_candidates = set()
        distances = []

        for order_id, courier_id, distance in candidates:
            orders_with_candidates.add(order_id)
            couriers_with_candidates.add(courier_id)
            distances.append(distance)

        # Calculate metrics
        order_coverage = len(orders_with_candidates) / len(order_ids) if order_ids else 0
        courier_coverage = len(couriers_with_candidates) / len(courier_ids) if courier_ids else 0

        return {
            'num_candidates': len(candidates),
            'order_coverage': order_coverage,
            'courier_coverage': courier_coverage,
            'orders_without_candidates': len(order_ids) - len(orders_with_candidates),
            'couriers_without_candidates': len(courier_ids) - len(couriers_with_candidates),
            'avg_distance': sum(distances) / len(distances) if distances else 0,
            'min_distance': min(distances) if distances else 0,
            'max_distance': max(distances) if distances else 0,
            'sparsity': len(candidates) / (len(orders) * len(couriers)) if orders and couriers else 0
        }