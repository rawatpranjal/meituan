"""
Bundling service for grouping same-restaurant orders.
Replaces the clustering logic from Model 02 with simpler same-restaurant bundling.
"""
from typing import List, Dict, Tuple, Optional, Any
import logging
from collections import defaultdict

logger = logging.getLogger(__name__)


class BundlingService:
    """
    Service for creating order bundles based on same-restaurant policy.

    This is simpler than Model 02's K-Means clustering - we only bundle
    orders from the same restaurant (same poi_id) with overlapping time windows.
    """

    def __init__(
        self,
        max_bundle_size: int = 5,
        time_window_overlap: int = 300  # 5 minutes
    ):
        """
        Initialize bundling service.

        Args:
            max_bundle_size: Maximum orders per bundle
            time_window_overlap: Required time overlap for bundling (seconds)
        """
        self.max_bundle_size = max_bundle_size
        self.time_window_overlap = time_window_overlap

    def create_bundles(
        self,
        orders: List[Dict],
        waybill_lookup: Dict
    ) -> Tuple[List[Dict], Dict[str, List[Dict]]]:
        """
        Create bundles from orders using same-restaurant policy.

        Args:
            orders: List of orders to potentially bundle
            waybill_lookup: Full order details

        Returns:
            Tuple of:
                - assignable_units: List of units (bundles or single orders)
                - bundle_mapping: Dict mapping unit_id -> list of orders
        """
        # Group orders by restaurant (poi_id)
        restaurant_groups = self._group_by_restaurant(orders, waybill_lookup)

        assignable_units = []
        bundle_mapping = {}

        for poi_id, restaurant_orders in restaurant_groups.items():
            if len(restaurant_orders) == 1:
                # Single order, no bundling
                assignable_units.append(restaurant_orders[0])
            else:
                # Try to bundle orders from same restaurant
                bundles = self._create_restaurant_bundles(
                    restaurant_orders, waybill_lookup
                )

                for bundle in bundles:
                    if len(bundle) > 1:
                        # Multi-order bundle
                        # Use first order's ID as bundle identifier
                        bundle_unit = bundle[0].copy()
                        bundle_unit['is_bundle'] = True
                        bundle_unit['bundle_size'] = len(bundle)

                        assignable_units.append(bundle_unit)
                        bundle_mapping[bundle_unit['order_id']] = bundle
                    else:
                        # Single order (couldn't be bundled)
                        assignable_units.append(bundle[0])

        logger.info(f"Created {len(assignable_units)} units from {len(orders)} orders "
                   f"({len(bundle_mapping)} bundles)")

        return assignable_units, bundle_mapping

    def _group_by_restaurant(
        self,
        orders: List[Dict],
        waybill_lookup: Dict
    ) -> Dict[str, List[Dict]]:
        """
        Group orders by restaurant (poi_id).

        Args:
            orders: Orders to group
            waybill_lookup: Full order details

        Returns:
            Dict mapping poi_id -> list of orders
        """
        restaurant_groups = defaultdict(list)

        for order in orders:
            order_id = order['order_id']
            if order_id in waybill_lookup:
                poi_id = waybill_lookup[order_id].get('poi_id')
                if poi_id:
                    restaurant_groups[poi_id].append(order)
                else:
                    # No restaurant ID, treat as single order
                    restaurant_groups[f"single_{order_id}"].append(order)

        return dict(restaurant_groups)

    def _create_restaurant_bundles(
        self,
        restaurant_orders: List[Dict],
        waybill_lookup: Dict
    ) -> List[List[Dict]]:
        """
        Create bundles from orders of the same restaurant.

        Args:
            restaurant_orders: Orders from the same restaurant
            waybill_lookup: Full order details

        Returns:
            List of bundles (each bundle is a list of orders)
        """
        # Sort orders by platform_order_time
        sorted_orders = sorted(
            restaurant_orders,
            key=lambda o: waybill_lookup[o['order_id']]['platform_order_time']
        )

        bundles = []
        current_bundle = []
        bundle_start_time = None
        bundle_end_time = None

        for order in sorted_orders:
            order_details = waybill_lookup[order['order_id']]
            order_time = order_details['platform_order_time']

            if not current_bundle:
                # Start new bundle
                current_bundle = [order]
                bundle_start_time = order_time
                bundle_end_time = order_time + self.time_window_overlap
            else:
                # Check if order fits in current bundle
                if (len(current_bundle) < self.max_bundle_size and
                    order_time <= bundle_end_time):
                    # Add to current bundle
                    current_bundle.append(order)
                    # Extend bundle window if needed
                    bundle_end_time = max(bundle_end_time,
                                        order_time + self.time_window_overlap)
                else:
                    # Current bundle is full or time window exceeded
                    bundles.append(current_bundle)
                    # Start new bundle
                    current_bundle = [order]
                    bundle_start_time = order_time
                    bundle_end_time = order_time + self.time_window_overlap

        # Add final bundle
        if current_bundle:
            bundles.append(current_bundle)

        return bundles

    def get_bundle_metrics(
        self,
        bundle_mapping: Dict[str, List[Dict]],
        waybill_lookup: Dict
    ) -> Dict[str, Any]:
        """
        Calculate metrics for created bundles.

        Args:
            bundle_mapping: Bundle ID to orders mapping
            waybill_lookup: Full order details

        Returns:
            Dict with bundle metrics
        """
        if not bundle_mapping:
            return {
                'num_bundles': 0,
                'avg_bundle_size': 0,
                'max_bundle_size': 0,
                'total_bundled_orders': 0,
                'bundle_size_distribution': {}
            }

        bundle_sizes = []
        total_orders = 0
        size_distribution = defaultdict(int)

        for bundle_id, orders in bundle_mapping.items():
            size = len(orders)
            bundle_sizes.append(size)
            total_orders += size
            size_distribution[size] += 1

        return {
            'num_bundles': len(bundle_mapping),
            'avg_bundle_size': sum(bundle_sizes) / len(bundle_sizes),
            'max_bundle_size': max(bundle_sizes),
            'min_bundle_size': min(bundle_sizes),
            'total_bundled_orders': total_orders,
            'bundle_size_distribution': dict(size_distribution)
        }

    def calculate_bundle_savings(
        self,
        bundle_mapping: Dict[str, List[Dict]],
        waybill_lookup: Dict,
        travel_time_func=None
    ) -> Dict[str, float]:
        """
        Calculate potential savings from bundling.

        Args:
            bundle_mapping: Bundle ID to orders mapping
            waybill_lookup: Full order details
            travel_time_func: Function to calculate travel time

        Returns:
            Dict with savings metrics
        """
        if not bundle_mapping:
            return {
                'total_trips_saved': 0,
                'avg_trips_saved_per_bundle': 0,
                'bundling_efficiency': 0
            }

        total_individual_trips = 0
        total_bundled_trips = len(bundle_mapping)

        for bundle_id, orders in bundle_mapping.items():
            total_individual_trips += len(orders)

        trips_saved = total_individual_trips - total_bundled_trips
        efficiency = trips_saved / total_individual_trips if total_individual_trips > 0 else 0

        return {
            'total_trips_saved': trips_saved,
            'avg_trips_saved_per_bundle': trips_saved / len(bundle_mapping) if bundle_mapping else 0,
            'bundling_efficiency': efficiency,
            'total_individual_trips': total_individual_trips,
            'total_bundled_trips': total_bundled_trips
        }

    def validate_bundles(
        self,
        bundle_mapping: Dict[str, List[Dict]],
        waybill_lookup: Dict
    ) -> List[str]:
        """
        Validate that bundles meet constraints.

        Args:
            bundle_mapping: Bundle ID to orders mapping
            waybill_lookup: Full order details

        Returns:
            List of validation errors (empty if valid)
        """
        errors = []

        for bundle_id, orders in bundle_mapping.items():
            # Check size constraint
            if len(orders) > self.max_bundle_size:
                errors.append(f"Bundle {bundle_id} exceeds max size: "
                            f"{len(orders)} > {self.max_bundle_size}")

            # Check same-restaurant constraint
            poi_ids = set()
            for order in orders:
                order_id = order['order_id']
                if order_id in waybill_lookup:
                    poi_id = waybill_lookup[order_id].get('poi_id')
                    if poi_id:
                        poi_ids.add(poi_id)

            if len(poi_ids) > 1:
                errors.append(f"Bundle {bundle_id} contains orders from "
                            f"{len(poi_ids)} different restaurants")

            # Check time window overlap
            times = []
            for order in orders:
                order_id = order['order_id']
                if order_id in waybill_lookup:
                    times.append(waybill_lookup[order_id]['platform_order_time'])

            if times:
                time_span = max(times) - min(times)
                if time_span > self.time_window_overlap * 2:
                    errors.append(f"Bundle {bundle_id} time span too large: "
                                f"{time_span}s > {self.time_window_overlap * 2}s")

        return errors