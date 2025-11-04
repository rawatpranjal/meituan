#!/usr/bin/env python3
"""
Test script for Workhorse (Anticipated Bundling) algorithm.
Tests the upgraded implementation with System Intensity and value maximization.
"""

import sys
import numpy as np
from simulator_core import (
    SimulationState, Restaurant, Order, Courier,
    MEAL_PREP_TIME, BATCH_INTERVAL, ORDER_EXPIRATION_TIME
)
from assignment_algorithms import assign_anticipated_bundling


def create_test_scenario():
    """Create a simple test scenario to validate Workhorse algorithm."""

    # Create restaurants
    restaurants = [
        Restaurant(restaurant_id=1, location=(1.0, 1.0)),
        Restaurant(restaurant_id=2, location=(4.0, 4.0)),
        Restaurant(restaurant_id=3, location=(2.5, 2.5)),
    ]

    # Create couriers (varying locations)
    couriers = [
        Courier(courier_id=1, start_location=(0.5, 0.5)),  # Near restaurant 1
        Courier(courier_id=2, start_location=(4.5, 4.5)),  # Near restaurant 2
        Courier(courier_id=3, start_location=(2.5, 2.5)),  # Near restaurant 3
        Courier(courier_id=4, start_location=(1.5, 3.0)),  # Central
        Courier(courier_id=5, start_location=(3.0, 1.5)),  # Central
    ]

    # Current time
    current_time = 1000.0

    # Create test orders with varying ready times
    # Scenario: High demand (12 orders, 5 couriers = Z_t = 2.4)
    orders = []

    # Cluster 1: Restaurant 1 - 4 orders (ready soon)
    for i in range(4):
        order = Order(
            order_id=100 + i,
            restaurant_id=1,
            restaurant_location=(1.0, 1.0),
            diner_location=(1.0 + i*0.3, 2.0 + i*0.2),
            placement_time=current_time - 300 + i*30
        )
        order.state = "PENDING"
        orders.append(order)

    # Cluster 2: Restaurant 2 - 5 orders (ready very soon)
    for i in range(5):
        order = Order(
            order_id=200 + i,
            restaurant_id=2,
            restaurant_location=(4.0, 4.0),
            diner_location=(3.5 - i*0.2, 4.0 + i*0.3),
            placement_time=current_time - 200 + i*20
        )
        order.state = "PENDING"
        orders.append(order)

    # Cluster 3: Restaurant 3 - 3 orders (ready further out)
    for i in range(3):
        order = Order(
            order_id=300 + i,
            restaurant_id=3,
            restaurant_location=(2.5, 2.5),
            diner_location=(2.0 + i*0.5, 3.0 - i*0.3),
            placement_time=current_time - 100 + i*40
        )
        order.state = "PENDING"
        orders.append(order)

    # Initialize simulation state
    state = SimulationState(restaurants, couriers, orders)
    state.current_time = current_time

    return state, couriers, orders


def test_workhorse_algorithm():
    """Test the Workhorse algorithm with various scenarios."""

    print("="*80)
    print("WORKHORSE ALGORITHM TEST")
    print("="*80)
    print()

    # Test 1: High demand scenario (Z_t > 2.0)
    print("TEST 1: High Demand Scenario (Z_t = 2.4)")
    print("-"*80)

    state, couriers, orders = create_test_scenario()

    print(f"Setup:")
    print(f"  - Orders: {len(orders)} (12 orders)")
    print(f"  - Couriers: {len(couriers)} (5 couriers)")
    print(f"  - Expected Z_t: {len(orders)/len(couriers):.2f}")
    print(f"  - Expected target bundle size: ~2.4 (clamped to 2.4)")
    print()

    # Run algorithm with different theta values
    for theta in [0.05, 0.1, 0.2]:
        print(f"\n  Testing with theta={theta}")
        print(f"  {'-'*60}")

        # Reset courier states
        for courier in couriers:
            courier.state = "IDLE"
            courier.assigned_orders = []

        # Run Workhorse
        assignments = assign_anticipated_bundling(state, couriers, [], theta=theta)

        print(f"  Assignments made: {len(assignments)}")

        total_orders_assigned = 0
        bundle_sizes = []

        for courier_id, order_ids in assignments:
            bundle_size = len(order_ids)
            total_orders_assigned += bundle_size
            bundle_sizes.append(bundle_size)

            # Get restaurant info and check same-restaurant constraint
            restaurant_ids = [state.orders[oid].restaurant_id for oid in order_ids]
            unique_restaurants = set(restaurant_ids)

            if len(unique_restaurants) > 1:
                print(f"    Courier {courier_id}: {bundle_size} orders from MULTIPLE restaurants {unique_restaurants} (Orders: {order_ids}) ⚠️ VIOLATION")
            else:
                restaurant_id = restaurant_ids[0]
                print(f"    Courier {courier_id}: {bundle_size} orders from Restaurant {restaurant_id} (Orders: {order_ids})")

        if bundle_sizes:
            avg_bundle_size = sum(bundle_sizes) / len(bundle_sizes)
            print(f"\n  Summary:")
            print(f"    Orders assigned: {total_orders_assigned}/{len(orders)}")
            print(f"    Average bundle size: {avg_bundle_size:.2f}")
            print(f"    Bundle size distribution: {bundle_sizes}")
        else:
            print(f"  No assignments made!")

    print("\n" + "="*80)
    print()

    # Test 2: Low demand scenario (Z_t < 1.0)
    print("TEST 2: Low Demand Scenario (Z_t = 0.6)")
    print("-"*80)

    # Create low demand: 3 orders, 5 couriers
    state.current_time = 2000.0
    low_demand_orders = [
        Order(order_id=401, restaurant_id=1, restaurant_location=(1.0, 1.0),
              diner_location=(1.5, 2.0), placement_time=1800.0),
        Order(order_id=402, restaurant_id=2, restaurant_location=(4.0, 4.0),
              diner_location=(3.5, 4.5), placement_time=1850.0),
        Order(order_id=403, restaurant_id=3, restaurant_location=(2.5, 2.5),
              diner_location=(2.0, 3.0), placement_time=1900.0),
    ]

    for order in low_demand_orders:
        order.state = "PENDING"

    state.orders = {o.id: o for o in low_demand_orders}

    # Reset couriers
    for courier in couriers:
        courier.state = "IDLE"
        courier.assigned_orders = []

    print(f"Setup:")
    print(f"  - Orders: {len(low_demand_orders)} (3 orders)")
    print(f"  - Couriers: {len(couriers)} (5 couriers)")
    print(f"  - Expected Z_t: {len(low_demand_orders)/len(couriers):.2f}")
    print(f"  - Expected target bundle size: ~1.0")
    print()

    assignments = assign_anticipated_bundling(state, couriers, [], theta=0.1)

    print(f"Assignments made: {len(assignments)}")

    for courier_id, order_ids in assignments:
        bundle_size = len(order_ids)
        first_order = state.orders[order_ids[0]]
        restaurant_id = first_order.restaurant_id
        print(f"  Courier {courier_id}: {bundle_size} orders from Restaurant {restaurant_id} (Orders: {order_ids})")

    if assignments:
        bundle_sizes = [len(order_ids) for _, order_ids in assignments]
        avg_bundle_size = sum(bundle_sizes) / len(bundle_sizes)
        print(f"\nSummary:")
        print(f"  Average bundle size: {avg_bundle_size:.2f} (should be ~1.0 for low demand)")

    print("\n" + "="*80)
    print("TEST COMPLETE")
    print("="*80)


if __name__ == "__main__":
    test_workhorse_algorithm()
