#!/usr/bin/env python3
"""
Export Detailed Simulation Logs in AI-Readable Format

Converts simulation events_log into comprehensive text files that capture:
- Batch-by-batch assignment decisions
- Courier and order locations
- Full assignment context and rationale
- State transitions and delivery completions
"""

from typing import Dict, List
from datetime import timedelta


def format_time(seconds: float) -> str:
    """Convert seconds to HH:MM:SS format."""
    td = timedelta(seconds=int(seconds))
    hours = td.seconds // 3600
    minutes = (td.seconds % 3600) // 60
    secs = td.seconds % 60
    return f"{hours:02d}:{minutes:02d}:{secs:02d}"


def format_location(loc: tuple) -> str:
    """Format location as (x, y)."""
    if loc:
        return f"({loc[0]:.2f}, {loc[1]:.2f})"
    return "(unknown)"


def export_detailed_log(state, algorithm_name: str, output_path: str, overlap_matrix_path: str = None):
    """
    Export comprehensive simulation log in AI-readable format.

    Args:
        state: SimulationState object with events_log and timeline
        algorithm_name: Name of the algorithm used
        output_path: Path to save the detailed log file
        overlap_matrix_path: Optional path to assignment overlap matrix file
    """

    with open(output_path, 'w') as f:
        # Header
        f.write("=" * 100 + "\n")
        f.write(f"DETAILED SIMULATION LOG: {algorithm_name.upper()}\n")
        f.write("AI-Readable Assignment History and Decision Trace\n")
        f.write("=" * 100 + "\n\n")

        f.write(f"Simulation Parameters:\n")
        f.write(f"  - Duration: {state.duration}s ({state.duration/3600:.1f} hours)\n")
        f.write(f"  - Couriers: {len(state.couriers)}\n")
        f.write(f"  - Restaurants: {len(state.restaurants)}\n")
        f.write(f"  - Total Orders: {len(state.orders)}\n")
        f.write(f"  - Algorithm: {algorithm_name}\n\n")

        # Group events by batch (every 300 seconds)
        batches = {}
        assignment_events = []
        delivery_events = []

        for event in state.events_log:
            event_time = event['time']
            event_type = event['type']

            if event_type == 'ASSIGNMENT_MADE':
                assignment_events.append(event)
                # Group by batch (round to nearest 300s)
                batch_num = int(event_time // 300)
                if batch_num not in batches:
                    batches[batch_num] = []
                batches[batch_num].append(event)
            elif event_type == 'ORDER_DELIVERED':
                delivery_events.append(event)

        # Write batch-by-batch assignments
        f.write("=" * 100 + "\n")
        f.write("BATCH ASSIGNMENT HISTORY\n")
        f.write("=" * 100 + "\n\n")

        for batch_num in sorted(batches.keys()):
            batch_time = batch_num * 300
            f.write("\n" + "=" * 100 + "\n")
            f.write(f"BATCH {batch_num + 1} @ t={batch_time}s ({format_time(batch_time)})\n")
            f.write("=" * 100 + "\n\n")

            # Get snapshot at this batch time to show state
            snapshot = None
            if batch_time < len(state.timeline):
                snapshot = state.timeline[batch_time]

            if snapshot:
                # Show available couriers
                idle_couriers = [c for c in snapshot['couriers'].values() if c['state'] == 'IDLE']
                f.write(f"AVAILABLE COURIERS ({len(idle_couriers)}):\n")
                for courier in idle_couriers:
                    loc = format_location(courier['current_location'])
                    f.write(f"  - Courier {courier['id']} @ {loc} - IDLE\n")
                f.write("\n")

                # Show ready orders
                ready_orders = [o for o in snapshot['orders'].values() if o['state'] == 'READY']
                f.write(f"READY ORDERS ({len(ready_orders)}):\n")
                for order in ready_orders[:10]:  # Limit to first 10 for readability
                    resto_loc = format_location(order['restaurant_location'])
                    cust_loc = format_location(order['diner_location'])
                    ready_time = format_time(order['ready_time'])
                    f.write(f"  - Order {order['id']}: Restaurant @ {resto_loc} → Customer @ {cust_loc} ")
                    f.write(f"[Ready since {ready_time}]\n")
                if len(ready_orders) > 10:
                    f.write(f"  ... and {len(ready_orders) - 10} more orders\n")
                f.write("\n")

            # Show assignments made in this batch
            batch_assignments = batches[batch_num]
            f.write(f"ASSIGNMENTS MADE ({len(batch_assignments)}):\n")

            for event in batch_assignments:
                courier_id = event['courier_id']
                order_ids = event['order_ids']
                bundle_size = event['bundle_size']

                # Get courier and order details from state
                courier = state.couriers.get(courier_id)

                f.write(f"\n✓ Courier {courier_id} ← Orders {order_ids} (Bundle size: {bundle_size})\n")

                if courier and snapshot:
                    courier_snap = snapshot['couriers'].get(str(courier_id))
                    if courier_snap:
                        courier_loc = format_location(courier_snap['current_location'])
                        f.write(f"  Courier location: {courier_loc}\n")

                # Show order details
                for order_id in order_ids:
                    order = state.orders.get(order_id)
                    if order:
                        resto_loc = format_location(order.restaurant_location)
                        cust_loc = format_location(order.diner_location)
                        f.write(f"  - Order {order_id}: {resto_loc} → {cust_loc}\n")

            # Batch summary
            if snapshot:
                in_transit = sum(1 for o in snapshot['orders'].values()
                               if o['state'] in ['ASSIGNED', 'PICKED_UP'])
                still_waiting = sum(1 for o in snapshot['orders'].values() if o['state'] == 'READY')
                delivered = sum(1 for o in snapshot['orders'].values() if o['state'] == 'DELIVERED')

                f.write(f"\nBATCH SUMMARY:\n")
                f.write(f"  - Orders assigned this batch: {len(batch_assignments)}\n")
                f.write(f"  - Orders in transit: {in_transit}\n")
                f.write(f"  - Orders still waiting: {still_waiting}\n")
                f.write(f"  - Orders delivered so far: {delivered}\n")

        # Write delivery completion log
        f.write("\n\n" + "=" * 100 + "\n")
        f.write("DELIVERY COMPLETION LOG\n")
        f.write("=" * 100 + "\n\n")

        for event in delivery_events:
            order_id = event.get('order_id')
            courier_id = event.get('courier_id')
            delivery_time = event['time']

            order = state.orders.get(order_id)
            if order:
                click_to_door = delivery_time - order.placement_time
                ready_to_door = delivery_time - order.ready_time

                f.write(f"✓ Order {order_id} delivered by Courier {courier_id} @ t={delivery_time:.0f}s ({format_time(delivery_time)})\n")
                f.write(f"  - Click-to-door: {click_to_door/60:.1f} minutes\n")
                f.write(f"  - Ready-to-door (freshness): {ready_to_door/60:.1f} minutes\n")
                f.write(f"  - Route: {format_location(order.restaurant_location)} → {format_location(order.diner_location)}\n\n")

        # Final summary
        f.write("\n" + "=" * 100 + "\n")
        f.write("FINAL SIMULATION SUMMARY\n")
        f.write("=" * 100 + "\n\n")

        metrics = state.metrics
        f.write(f"TIER 1 - Mission-Critical:\n")
        f.write(f"  - Fulfillment Rate: {metrics.get('fulfillment_rate_pct', 0):.1f}%\n")
        f.write(f"  - Avg Click-to-Door: {metrics.get('avg_click_to_door_time', 0)/60:.1f} minutes\n")
        f.write(f"  - P90 Click-to-Door: {metrics.get('p90_click_to_door_time', 0)/60:.1f} minutes\n\n")

        f.write(f"TIER 2 - Operational Efficiency:\n")
        f.write(f"  - System Throughput: {metrics.get('system_throughput_orders_per_hour', 0):.1f} orders/hr\n")
        f.write(f"  - Orders/Courier-Hour: {metrics.get('avg_orders_per_courier_hour', 0):.2f}\n")
        f.write(f"  - Freshness: {metrics.get('avg_ready_to_door_time', 0)/60:.1f} minutes\n\n")

        f.write(f"TIER 3 - Diagnostic:\n")
        f.write(f"  - Avg Bundle Size: {metrics.get('avg_bundle_size', 0):.2f}\n")
        f.write(f"  - Courier Utilization: {metrics.get('courier_utilization_pct', 0):.1f}%\n")
        f.write(f"  - Total Distance: {metrics.get('total_distance_traveled_km', 0):.1f} km\n\n")

        f.write(f"Orders Breakdown:\n")
        f.write(f"  - Delivered: {metrics.get('orders_delivered', 0)}\n")
        f.write(f"  - In Transit: {metrics.get('orders_in_transit', 0)}\n")
        f.write(f"  - Unassigned: {metrics.get('orders_unassigned', 0)}\n")
        f.write(f"  - Out of Scope: {metrics.get('orders_out_of_scope', 0)}\n")

        # Append assignment overlap matrix if provided
        if overlap_matrix_path:
            try:
                import os
                if os.path.exists(overlap_matrix_path):
                    f.write("\n\n" + "=" * 100 + "\n")
                    f.write("ASSIGNMENT OVERLAP ANALYSIS\n")
                    f.write("=" * 100 + "\n\n")

                    with open(overlap_matrix_path, 'r') as overlap_file:
                        overlap_content = overlap_file.read()
                        f.write(overlap_content)
            except Exception as e:
                f.write(f"\n\n(Note: Could not include overlap matrix: {e})\n")

    print(f"  ✓ Detailed log saved: {output_path}")


if __name__ == "__main__":
    print("This module provides logging utilities for simulation analysis.")
    print("Import and use export_detailed_log(state, algorithm_name, output_path)")
