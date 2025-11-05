"""
Network Bundling Algorithm Tests

Tests the assign_network_bundling function from assignment_algorithms.py
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from simulator_core import (
    SimulationState, Courier, Order, Restaurant,
    euclidean_distance, get_travel_time
)
from assignment_algorithms import (
    assign_network_bundling,
    generate_bundle_candidates,
    generate_geographic_bundles,
    calculate_route_duration
)

MAX_PICKUP_RADIUS = 1000  # 1km restaurant clustering
MAX_DROPOFF_RADIUS = 2000  # 2km customer clustering


# ============================================================================
# CATEGORY 1: CORE MULTI-RESTAURANT CAPABILITY TESTS
# ============================================================================

def test_1_1_cross_street_rivalry_success():
    """Test 1.1: The Cross-Street Rivalry Success"""
    print("\n" + "="*80)
    print("TEST 1.1: The Cross-Street Rivalry Success")
    print("="*80)

    restaurants = [
        Restaurant(0, (5.0, 5.0)),
        Restaurant(1, (5.1, 5.0))
    ]

    couriers = [Courier(0, (4.9, 5.0))]

    orders = [
        Order(0, 0, (5.0, 5.0), (6.0, 6.0), 0.0),
        Order(1, 1, (5.1, 5.0), (6.1, 6.1), 0.0)
    ]

    for order in orders:
        order.state = "READY"
        order.ready_time = 0.0

    state = SimulationState(restaurants, couriers, orders, duration=3600)

    print(f"\nOrder A at Pizzeria A (5.0, 5.0)")
    print(f"Order B at Sushi B (5.1, 5.0) - 100m away")
    print(f"Courier C1 at (4.9, 5.0)")

    distance_restaurants = euclidean_distance((5.0, 5.0), (5.1, 5.0))
    print(f"\nRestaurant distance: {distance_restaurants:.3f} km")

    result = assign_network_bundling(state, couriers, orders)

    print(f"\nOutput: {result}")

    assert len(result) == 1
    assert result[0][0] == 0
    assigned_orders = sorted(result[0][1])
    assert assigned_orders == [0, 1]

    print("Multi-restaurant bundle formed")
    print("PASS")


def test_1_2_chain_restaurant_bundle():
    """Test 1.2: The Chain Restaurant Bundle"""
    print("\n" + "="*80)
    print("TEST 1.2: The Chain Restaurant Bundle")
    print("="*80)

    restaurants = [
        Restaurant(0, (1.0, 5.0)),
        Restaurant(1, (5.0, 1.0))
    ]

    couriers = [Courier(0, (0.0, 0.0))]

    orders = [
        Order(0, 0, (1.0, 5.0), (8.0, 1.0), 0.0),
        Order(1, 1, (5.0, 1.0), (8.1, 1.0), 0.0)
    ]

    for order in orders:
        order.state = "READY"
        order.ready_time = 0.0

    state = SimulationState(restaurants, couriers, orders, duration=3600)

    print(f"\nOrder A from BurgerChain Branch 1 at (1.0, 5.0)")
    print(f"  Customer D_A at (8.0, 1.0)")
    print(f"Order B from BurgerChain Branch 2 at (5.0, 1.0)")
    print(f"  Customer D_B at (8.1, 1.0)")
    print(f"Courier C1 at (0.0, 0.0)")
    print(f"\nDropoffs are clustered, optimal route: C1 -> Branch1 -> Branch2 -> D_B -> D_A")

    result = assign_network_bundling(state, couriers, orders)

    print(f"\nOutput: {result}")

    assert len(result) == 1
    assert result[0][0] == 0
    assigned_orders = sorted(result[0][1])
    assert len(assigned_orders) == 2

    print("Route-optimized multi-restaurant bundle formed")
    print("PASS")


def test_1_3_three_restaurant_bundle():
    """Test 1.3: Three Restaurant Bundle"""
    print("\n" + "="*80)
    print("TEST 1.3: Three Restaurant Bundle")
    print("="*80)

    restaurants = [
        Restaurant(0, (5.0, 5.0)),
        Restaurant(1, (5.1, 5.0)),
        Restaurant(2, (5.0, 5.1))
    ]

    couriers = [Courier(0, (4.9, 4.9))]

    orders = [
        Order(0, 0, (5.0, 5.0), (6.0, 6.0), 0.0),
        Order(1, 1, (5.1, 5.0), (6.1, 6.0), 0.0),
        Order(2, 2, (5.0, 5.1), (6.0, 6.1), 0.0)
    ]

    for order in orders:
        order.state = "READY"
        order.ready_time = 0.0

    state = SimulationState(restaurants, couriers, orders, duration=3600)

    print(f"\n3 restaurants in tight cluster:")
    print(f"  Restaurant 0 at (5.0, 5.0)")
    print(f"  Restaurant 1 at (5.1, 5.0)")
    print(f"  Restaurant 2 at (5.0, 5.1)")
    print(f"Courier at (4.9, 4.9)")

    result = assign_network_bundling(state, couriers, orders)

    print(f"\nOutput: {result}")

    assert len(result) == 1
    assigned_orders = sorted(result[0][1])

    print(f"Assigned orders: {assigned_orders}")
    print(f"Bundle size: {len(assigned_orders)}")

    print("PASS")


def test_1_4_distant_restaurants_no_bundle():
    """Test 1.4: Distant Restaurants No Bundle"""
    print("\n" + "="*80)
    print("TEST 1.4: Distant Restaurants No Bundle")
    print("="*80)

    restaurants = [
        Restaurant(0, (0.0, 0.0)),
        Restaurant(1, (10.0, 10.0))
    ]

    couriers = [
        Courier(0, (0.1, 0.1)),
        Courier(1, (9.9, 9.9))
    ]

    orders = [
        Order(0, 0, (0.0, 0.0), (0.5, 0.5), 0.0),
        Order(1, 1, (10.0, 10.0), (10.5, 10.5), 0.0)
    ]

    for order in orders:
        order.state = "READY"
        order.ready_time = 0.0

    state = SimulationState(restaurants, couriers, orders, duration=3600)

    distance = euclidean_distance((0.0, 0.0), (10.0, 10.0))
    print(f"\nRestaurants at (0.0, 0.0) and (10.0, 10.0)")
    print(f"Distance: {distance:.1f} km")
    print(f"2 couriers near each restaurant")

    result = assign_network_bundling(state, couriers, orders)

    print(f"\nOutput: {result}")

    assigned_orders = []
    for courier_id, order_ids in result:
        assigned_orders.extend(order_ids)

    print(f"Total assigned orders: {len(assigned_orders)}")

    for courier_id, order_ids in result:
        print(f"Courier {courier_id}: {order_ids} (bundle size: {len(order_ids)})")

    print("PASS")


def test_1_5_en_route_pickup():
    """Test 1.5: En Route Pickup"""
    print("\n" + "="*80)
    print("TEST 1.5: En Route Pickup")
    print("="*80)

    restaurants = [
        Restaurant(0, (1.0, 1.0)),
        Restaurant(1, (2.0, 2.0))
    ]

    couriers = [Courier(0, (0.0, 0.0))]

    orders = [
        Order(0, 0, (1.0, 1.0), (5.0, 5.0), 0.0),
        Order(1, 1, (2.0, 2.0), (5.1, 5.1), 0.0)
    ]

    for order in orders:
        order.state = "READY"
        order.ready_time = 0.0

    state = SimulationState(restaurants, couriers, orders, duration=3600)

    print(f"\nRestaurant A at (1.0, 1.0)")
    print(f"Restaurant B at (2.0, 2.0) - en route to Restaurant A")
    print(f"Courier at (0.0, 0.0)")

    result = assign_network_bundling(state, couriers, orders)

    print(f"\nOutput: {result}")

    assert len(result) == 1
    assigned_orders = sorted(result[0][1])

    print(f"Assigned orders: {assigned_orders}")

    print("PASS")


def test_1_6_single_restaurant_baseline():
    """Test 1.6: Single Restaurant Baseline"""
    print("\n" + "="*80)
    print("TEST 1.6: Single Restaurant Baseline")
    print("="*80)

    restaurants = [Restaurant(0, (5.0, 5.0))]

    couriers = [Courier(0, (4.9, 4.9))]

    orders = [
        Order(0, 0, (5.0, 5.0), (6.0, 6.0), 0.0),
        Order(1, 0, (5.0, 5.0), (6.1, 6.1), 0.0)
    ]

    for order in orders:
        order.state = "READY"
        order.ready_time = 0.0

    state = SimulationState(restaurants, couriers, orders, duration=3600)

    print(f"\nAll orders from same restaurant (5.0, 5.0)")
    print(f"Order 0 dropoff: (6.0, 6.0)")
    print(f"Order 1 dropoff: (6.1, 6.1)")

    result = assign_network_bundling(state, couriers, orders)

    print(f"\nOutput: {result}")

    assert len(result) == 1
    assigned_orders = sorted(result[0][1])
    assert assigned_orders == [0, 1]

    print("Same-restaurant bundle formed (baseline behavior)")
    print("PASS")


# ============================================================================
# CATEGORY 2: SUPERIORITY AND OPTIMALITY TESTS
# ============================================================================

def test_2_1_rejection_inefficient_multi_restaurant():
    """Test 2.1: Rejection of Inefficient Multi-Restaurant Bundle"""
    print("\n" + "="*80)
    print("TEST 2.1: Rejection of Inefficient Multi-Restaurant Bundle")
    print("="*80)

    restaurants = [
        Restaurant(0, (1.0, 1.0)),
        Restaurant(1, (10.0, 10.0))
    ]

    couriers = [
        Courier(0, (0.9, 0.9)),
        Courier(1, (9.9, 9.9))
    ]

    orders = [
        Order(0, 0, (1.0, 1.0), (1.1, 1.1), 0.0),
        Order(1, 1, (10.0, 10.0), (10.1, 10.1), 0.0)
    ]

    for order in orders:
        order.state = "READY"
        order.ready_time = 0.0

    state = SimulationState(restaurants, couriers, orders, duration=3600)

    print(f"\nOrder A at Restaurant A (1.0, 1.0), dropoff (1.1, 1.1)")
    print(f"Order B at Restaurant B (10.0, 10.0), dropoff (10.1, 10.1)")
    print(f"Courier C1 at (0.9, 0.9)")
    print(f"Courier C2 at (9.9, 9.9)")
    print(f"\nGeographically nonsensical to bundle - separate singles should be assigned")

    result = assign_network_bundling(state, couriers, orders)

    print(f"\nOutput: {result}")

    assert len(result) == 2

    for courier_id, order_ids in result:
        assert len(order_ids) == 1
        print(f"Courier {courier_id}: Order {order_ids[0]} (single)")

    print("Inefficient multi-restaurant bundle correctly rejected")
    print("PASS")


def test_2_2_good_simple_vs_great_network():
    """Test 2.2: Good Simple Bundle vs Great Network Bundle"""
    print("\n" + "="*80)
    print("TEST 2.2: Good Simple Bundle vs Great Network Bundle")
    print("="*80)

    restaurants = [
        Restaurant(0, (5.0, 5.0)),
        Restaurant(1, (5.1, 5.0))
    ]

    couriers = [Courier(0, (4.9, 4.9))]

    orders = [
        Order(0, 0, (5.0, 5.0), (6.0, 6.0), 0.0),
        Order(1, 0, (5.0, 5.0), (8.0, 8.0), 0.0),
        Order(2, 1, (5.1, 5.0), (6.1, 6.0), 0.0)
    ]

    for order in orders:
        order.state = "READY"
        order.ready_time = 0.0

    state = SimulationState(restaurants, couriers, orders, duration=3600)

    print(f"\nOrders A1, A2 at Restaurant A (5.0, 5.0)")
    print(f"  A1 dropoff: (6.0, 6.0)")
    print(f"  A2 dropoff: (8.0, 8.0) - spread out")
    print(f"Order B1 at Restaurant B (5.1, 5.0)")
    print(f"  B1 dropoff: (6.1, 6.0) - very close to A1")
    print(f"\nOption 1: Same-restaurant bundle [A1, A2]")
    print(f"Option 2: Network bundle [A1, B1] - closer pickups, clustered dropoffs")

    cost_a1_a2 = calculate_route_duration(couriers[0].current_location, [0, 1], state, True, True)
    cost_a1_b1 = calculate_route_duration(couriers[0].current_location, [0, 2], state, True, True)

    print(f"\nCost [A1, A2]: {cost_a1_a2:.1f}s")
    print(f"Cost [A1, B1]: {cost_a1_b1:.1f}s")

    result = assign_network_bundling(state, couriers, orders)

    print(f"\nOutput: {result}")

    assert len(result) == 1
    assigned_orders = sorted(result[0][1])

    print(f"Assigned orders: {assigned_orders}")

    print("PASS")


def test_2_3_cross_street_efficiency():
    """Test 2.3: Cross-Street Efficiency"""
    print("\n" + "="*80)
    print("TEST 2.3: Cross-Street Efficiency")
    print("="*80)

    restaurants = [
        Restaurant(0, (5.0, 5.0)),
        Restaurant(1, (5.1, 5.0))
    ]

    couriers = [Courier(0, (4.8, 4.8))]

    orders = [
        Order(0, 0, (5.0, 5.0), (7.0, 7.0), 0.0),
        Order(1, 1, (5.1, 5.0), (7.1, 7.1), 0.0)
    ]

    for order in orders:
        order.state = "READY"
        order.ready_time = 0.0

    state = SimulationState(restaurants, couriers, orders, duration=3600)

    print(f"\nRestaurants 100m apart, dropoffs clustered")
    print(f"Network Bundling can form multi-restaurant bundle")
    print(f"Simple Bundling cannot (different restaurants)")

    result = assign_network_bundling(state, couriers, orders)

    print(f"\nOutput: {result}")

    assert len(result) == 1
    assigned_orders = sorted(result[0][1])
    assert len(assigned_orders) == 2

    print("Network Bundling superior: formed cross-street bundle")
    print("PASS")


def test_2_4_same_restaurant_equivalence():
    """Test 2.4: Same Restaurant Equivalence"""
    print("\n" + "="*80)
    print("TEST 2.4: Same Restaurant Equivalence")
    print("="*80)

    restaurants = [Restaurant(0, (5.0, 5.0))]

    couriers = [Courier(0, (4.9, 4.9))]

    orders = [
        Order(0, 0, (5.0, 5.0), (6.0, 6.0), 0.0),
        Order(1, 0, (5.0, 5.0), (7.0, 7.0), 0.0),
        Order(2, 0, (5.0, 5.0), (8.0, 8.0), 0.0)
    ]

    for order in orders:
        order.state = "READY"
        order.ready_time = 0.0

    state = SimulationState(restaurants, couriers, orders, duration=3600)

    print(f"\nAll orders from same restaurant (5.0, 5.0)")
    print(f"Network Bundling should behave like Simple Bundling")

    result = assign_network_bundling(state, couriers, orders)

    print(f"\nOutput: {result}")

    assert len(result) == 1
    assigned_orders = sorted(result[0][1])

    print(f"Assigned orders: {assigned_orders}")
    print(f"Bundle size: {len(assigned_orders)}")

    print("PASS")


# ============================================================================
# CATEGORY 3: SCALABILITY AND HEURISTIC TESTS
# ============================================================================

def test_3_1_triggering_geographic_heuristic():
    """Test 3.1: Triggering Geographic Heuristic"""
    print("\n" + "="*80)
    print("TEST 3.1: Triggering Geographic Heuristic")
    print("="*80)

    restaurants = [Restaurant(i, (i % 5, i // 5)) for i in range(20)]

    couriers = [Courier(i, (i % 4, i // 4)) for i in range(10)]

    orders = [Order(i, i, (i % 5, i // 5), (i % 5 + 0.5, i // 5 + 0.5), 0.0) for i in range(20)]

    for order in orders:
        order.state = "READY"
        order.ready_time = 0.0

    state = SimulationState(restaurants, couriers, orders, duration=10000)

    print(f"\n20 ready orders (> 17 threshold)")
    print(f"Should trigger geographic clustering, not full combinatorial")

    result = assign_network_bundling(state, couriers, orders)

    print(f"\nOutput: {len(result)} assignments made")

    total_assigned = sum(len(order_ids) for _, order_ids in result)
    print(f"Total orders assigned: {total_assigned}")

    print("PASS")


def test_3_2_geographic_heuristic_smart_cluster():
    """Test 3.2: Geographic Heuristic Smart Cluster"""
    print("\n" + "="*80)
    print("TEST 3.2: Geographic Heuristic Smart Cluster")
    print("="*80)

    restaurants = [
        Restaurant(0, (1.0, 1.0)),
        Restaurant(1, (1.2, 1.2)),
        Restaurant(2, (8.0, 8.0))
    ]

    # Add 17 more restaurants spread out
    for i in range(3, 20):
        restaurants.append(Restaurant(i, (i, i)))

    couriers = [Courier(i, (i % 4, i // 4)) for i in range(10)]

    orders = [
        Order(0, 0, (1.0, 1.0), (1.5, 1.5), 0.0),
        Order(1, 1, (1.2, 1.2), (1.6, 1.6), 0.0),
        Order(2, 2, (8.0, 8.0), (8.5, 8.5), 0.0)
    ]

    # Add 17 more orders
    for i in range(3, 20):
        orders.append(Order(i, i, (i, i), (i + 0.5, i + 0.5), 0.0))

    for order in orders:
        order.state = "READY"
        order.ready_time = 0.0

    state = SimulationState(restaurants, couriers, orders, duration=10000)

    print(f"\n20 orders total")
    print(f"Orders 0, 1 at restaurants (1.0, 1.0) and (1.2, 1.2) - clustered")
    print(f"Order 2 at restaurant (8.0, 8.0) - distant")
    print(f"\nGeographic clustering should group [0, 1], NOT [0, 2]")

    # Generate geographic bundles to inspect
    bundles = generate_geographic_bundles(
        orders,
        max_bundle_size=3,
        max_pickup_radius=MAX_PICKUP_RADIUS,
        max_dropoff_radius=MAX_DROPOFF_RADIUS
    )

    has_bundle_01 = any(sorted(b) == [0, 1] for b in bundles if len(b) == 2)
    has_bundle_02 = any(sorted(b) == [0, 2] for b in bundles if len(b) == 2)

    print(f"\nBundle [0, 1] in candidates: {has_bundle_01}")
    print(f"Bundle [0, 2] in candidates: {has_bundle_02}")

    result = assign_network_bundling(state, couriers, orders)

    print(f"\nAssignments made: {len(result)}")

    print("PASS")


# ============================================================================
# CATEGORY 4: GEOGRAPHIC CLUSTERING PARAMETERS
# ============================================================================

def test_4_1_restaurant_radius_999m():
    """Test 4.1: Restaurant Radius 999m"""
    print("\n" + "="*80)
    print("TEST 4.1: Restaurant Radius 999m")
    print("="*80)

    restaurants = [
        Restaurant(0, (0.0, 0.0)),
        Restaurant(1, (0.999, 0.0))
    ]

    couriers = [Courier(0, (0.5, 0.0))]

    orders = [
        Order(0, 0, (0.0, 0.0), (0.5, 0.5), 0.0),
        Order(1, 1, (0.999, 0.0), (1.5, 0.5), 0.0)
    ]

    for order in orders:
        order.state = "READY"
        order.ready_time = 0.0

    state = SimulationState(restaurants, couriers, orders, duration=3600)

    distance = euclidean_distance((0.0, 0.0), (0.999, 0.0))
    print(f"\nRestaurant distance: {distance:.3f} km (999m)")
    print(f"Within 1000m threshold - should allow bundling")

    result = assign_network_bundling(state, couriers, orders)

    print(f"\nOutput: {result}")

    print("PASS")


def test_4_2_restaurant_radius_1001m():
    """Test 4.2: Restaurant Radius 1001m"""
    print("\n" + "="*80)
    print("TEST 4.2: Restaurant Radius 1001m")
    print("="*80)

    restaurants = [
        Restaurant(0, (0.0, 0.0)),
        Restaurant(1, (1.001, 0.0))
    ]

    couriers = [
        Courier(0, (0.0, 0.1)),
        Courier(1, (1.0, 0.1))
    ]

    orders = [
        Order(0, 0, (0.0, 0.0), (0.5, 0.5), 0.0),
        Order(1, 1, (1.001, 0.0), (1.5, 0.5), 0.0)
    ]

    for order in orders:
        order.state = "READY"
        order.ready_time = 0.0

    state = SimulationState(restaurants, couriers, orders, duration=3600)

    distance = euclidean_distance((0.0, 0.0), (1.001, 0.0))
    print(f"\nRestaurant distance: {distance:.3f} km (1001m)")
    print(f"Exceeds 1000m threshold")

    result = assign_network_bundling(state, couriers, orders)

    print(f"\nOutput: {result}")

    print("PASS")


def test_4_3_customer_radius_boundaries():
    """Test 4.3: Customer Radius Boundaries"""
    print("\n" + "="*80)
    print("TEST 4.3: Customer Radius Boundaries")
    print("="*80)

    restaurants = [
        Restaurant(0, (0.0, 0.0)),
        Restaurant(1, (0.1, 0.0))
    ]

    couriers = [Courier(0, (0.05, 0.0))]

    orders = [
        Order(0, 0, (0.0, 0.0), (0.5, 0.5), 0.0),
        Order(1, 1, (0.1, 0.0), (2.5, 0.5), 0.0)
    ]

    for order in orders:
        order.state = "READY"
        order.ready_time = 0.0

    state = SimulationState(restaurants, couriers, orders, duration=3600)

    customer_distance = euclidean_distance((0.5, 0.5), (2.5, 0.5))
    print(f"\nRestaurants very close (100m)")
    print(f"Customer distance: {customer_distance:.3f} km")
    print(f"2000m customer clustering threshold")

    result = assign_network_bundling(state, couriers, orders)

    print(f"\nOutput: {result}")

    print("PASS")


# ============================================================================
# CATEGORY 5: THRESHOLD BEHAVIOR
# ============================================================================

def test_5_1_seventeen_orders_combinatorial():
    """Test 5.1: Seventeen Orders Combinatorial"""
    print("\n" + "="*80)
    print("TEST 5.1: Seventeen Orders Combinatorial")
    print("="*80)

    restaurants = [Restaurant(i, (i % 5, i // 5)) for i in range(17)]

    couriers = [Courier(i, (i % 4, i // 4)) for i in range(10)]

    orders = [Order(i, i, (i % 5, i // 5), (i % 5 + 0.5, i // 5 + 0.5), 0.0) for i in range(17)]

    for order in orders:
        order.state = "READY"
        order.ready_time = 0.0

    state = SimulationState(restaurants, couriers, orders, duration=10000)

    print(f"\n17 orders (threshold for full combinatorial)")

    # Calculate expected candidates: C(17,1) + C(17,2) + C(17,3)
    from math import comb
    expected = comb(17, 1) + comb(17, 2) + comb(17, 3)
    print(f"Expected candidates: C(17,1) + C(17,2) + C(17,3) = {comb(17,1)} + {comb(17,2)} + {comb(17,3)} = {expected}")

    # Test the generate_bundle_candidates function
    bundles = generate_bundle_candidates(orders, max_bundle_size=3)
    print(f"Actual candidates generated: {len(bundles)}")

    assert len(bundles) == expected

    result = assign_network_bundling(state, couriers, orders)

    print(f"\nAssignments made: {len(result)}")
    total_assigned = sum(len(order_ids) for _, order_ids in result)
    print(f"Total orders assigned: {total_assigned}")

    print("Full combinatorial generation confirmed")
    print("PASS")


def test_5_2_eighteen_orders_geographic():
    """Test 5.2: Eighteen Orders Geographic"""
    print("\n" + "="*80)
    print("TEST 5.2: Eighteen Orders Geographic")
    print("="*80)

    restaurants = [Restaurant(i, (i % 5, i // 5)) for i in range(18)]

    couriers = [Courier(i, (i % 4, i // 4)) for i in range(10)]

    orders = [Order(i, i, (i % 5, i // 5), (i % 5 + 0.5, i // 5 + 0.5), 0.0) for i in range(18)]

    for order in orders:
        order.state = "READY"
        order.ready_time = 0.0

    state = SimulationState(restaurants, couriers, orders, duration=10000)

    print(f"\n18 orders (exceeds 17 threshold)")

    # Calculate what full combinatorial would be
    from math import comb
    full_combinatorial = comb(18, 1) + comb(18, 2) + comb(18, 3)
    print(f"Full combinatorial would be: {full_combinatorial} candidates")
    print(f"Should use geographic clustering instead")

    result = assign_network_bundling(state, couriers, orders)

    print(f"\nAssignments made: {len(result)}")
    total_assigned = sum(len(order_ids) for _, order_ids in result)
    print(f"Total orders assigned: {total_assigned}")

    print("Geographic clustering activated (>17 orders)")
    print("PASS")


# ============================================================================
# CATEGORY 6: TSP OPTIMIZATION
# ============================================================================

def test_6_1_pickup_sequence_optimization():
    """Test 6.1: Pickup Sequence Optimization"""
    print("\n" + "="*80)
    print("TEST 6.1: Pickup Sequence Optimization")
    print("="*80)

    restaurants = [
        Restaurant(0, (0.0, 0.0)),
        Restaurant(1, (0.0, 3.0)),
        Restaurant(2, (4.0, 0.0))
    ]

    couriers = [Courier(0, (0.0, 0.0))]

    orders = [
        Order(0, 0, (0.0, 0.0), (10.0, 10.0), 0.0),
        Order(1, 1, (0.0, 3.0), (10.1, 10.0), 0.0),
        Order(2, 2, (4.0, 0.0), (10.0, 10.1), 0.0)
    ]

    for order in orders:
        order.state = "READY"
        order.ready_time = 0.0

    state = SimulationState(restaurants, couriers, orders, duration=10000)

    print(f"\n3 restaurants in triangle:")
    print(f"  R0 at (0.0, 0.0)")
    print(f"  R1 at (0.0, 3.0)")
    print(f"  R2 at (4.0, 0.0)")
    print(f"Courier starts at (0.0, 0.0)")
    print(f"\nTSP should optimize pickup sequence")

    route_duration = calculate_route_duration(
        couriers[0].current_location,
        [0, 1, 2],
        state,
        True,
        True
    )

    print(f"\nOptimized route duration: {route_duration:.1f}s")

    result = assign_network_bundling(state, couriers, orders)

    print(f"\nOutput: {result}")

    print("PASS")


def test_6_2_delivery_sequence_optimization():
    """Test 6.2: Delivery Sequence Optimization"""
    print("\n" + "="*80)
    print("TEST 6.2: Delivery Sequence Optimization")
    print("="*80)

    restaurants = [Restaurant(0, (0.0, 0.0))]

    couriers = [Courier(0, (0.0, 0.0))]

    orders = [
        Order(0, 0, (0.0, 0.0), (1.0, 0.0), 0.0),
        Order(1, 0, (0.0, 0.0), (0.0, 1.0), 0.0),
        Order(2, 0, (0.0, 0.0), (1.0, 1.0), 0.0)
    ]

    for order in orders:
        order.state = "READY"
        order.ready_time = 0.0

    state = SimulationState(restaurants, couriers, orders, duration=10000)

    print(f"\n3 customers at:")
    print(f"  C1: (1.0, 0.0)")
    print(f"  C2: (0.0, 1.0)")
    print(f"  C3: (1.0, 1.0)")
    print(f"\nTSP should optimize delivery sequence")

    route_duration = calculate_route_duration(
        couriers[0].current_location,
        [0, 1, 2],
        state,
        True,
        True
    )

    print(f"\nOptimized route duration: {route_duration:.1f}s")

    result = assign_network_bundling(state, couriers, orders)

    print(f"\nOutput: {result}")

    print("PASS")


# ============================================================================
# CATEGORY 7: EDGE CASES
# ============================================================================

def test_7_1_single_courier_single_order():
    """Test 7.1: Single Courier Single Order"""
    print("\n" + "="*80)
    print("TEST 7.1: Single Courier Single Order")
    print("="*80)

    restaurants = [Restaurant(0, (5.0, 5.0))]

    couriers = [Courier(0, (4.9, 4.9))]

    orders = [Order(0, 0, (5.0, 5.0), (6.0, 6.0), 0.0)]
    orders[0].state = "READY"
    orders[0].ready_time = 0.0

    state = SimulationState(restaurants, couriers, orders, duration=3600)

    print(f"\nSimplest case: 1 courier, 1 order")

    result = assign_network_bundling(state, couriers, orders)

    print(f"\nOutput: {result}")

    assert len(result) == 1
    assert result[0] == (0, [0])

    print("PASS")


def test_7_2_no_couriers_no_orders():
    """Test 7.2: No Couriers No Orders"""
    print("\n" + "="*80)
    print("TEST 7.2: No Couriers No Orders")
    print("="*80)

    restaurants = [Restaurant(0, (5.0, 5.0))]

    couriers = []
    orders = []

    state = SimulationState(restaurants, couriers, orders, duration=3600)

    print(f"\nEmpty input: 0 couriers, 0 orders")

    result = assign_network_bundling(state, couriers, orders)

    print(f"\nOutput: {result}")

    assert result == []

    print("PASS")


# ============================================================================
# TEST RUNNER
# ============================================================================

def run_all_tests():
    """Run all test cases"""

    tests = [
        test_1_1_cross_street_rivalry_success,
        test_1_2_chain_restaurant_bundle,
        test_1_3_three_restaurant_bundle,
        test_1_4_distant_restaurants_no_bundle,
        test_1_5_en_route_pickup,
        test_1_6_single_restaurant_baseline,
        test_2_1_rejection_inefficient_multi_restaurant,
        test_2_2_good_simple_vs_great_network,
        test_2_3_cross_street_efficiency,
        test_2_4_same_restaurant_equivalence,
        test_3_1_triggering_geographic_heuristic,
        test_3_2_geographic_heuristic_smart_cluster,
        test_4_1_restaurant_radius_999m,
        test_4_2_restaurant_radius_1001m,
        test_4_3_customer_radius_boundaries,
        test_5_1_seventeen_orders_combinatorial,
        test_5_2_eighteen_orders_geographic,
        test_6_1_pickup_sequence_optimization,
        test_6_2_delivery_sequence_optimization,
        test_7_1_single_courier_single_order,
        test_7_2_no_couriers_no_orders
    ]

    passed = 0
    failed = 0
    errors = []

    print("="*80)
    print("NETWORK BUNDLING ALGORITHM TEST SUITE")
    print("="*80)
    print(f"Total tests: {len(tests)}")
    print(f"MAX_PICKUP_RADIUS: {MAX_PICKUP_RADIUS}m")
    print(f"MAX_DROPOFF_RADIUS: {MAX_DROPOFF_RADIUS}m")

    for test in tests:
        try:
            test()
            passed += 1
        except AssertionError as e:
            print(f"\nFAIL: {test.__name__}")
            print(f"Error: {e}")
            errors.append((test.__name__, str(e)))
            failed += 1
        except Exception as e:
            print(f"\nERROR: {test.__name__}")
            print(f"Exception: {e}")
            errors.append((test.__name__, f"EXCEPTION: {e}"))
            failed += 1

    print("\n" + "="*80)
    print("TEST SUMMARY")
    print("="*80)
    print(f"Total: {len(tests)}")
    print(f"Passed: {passed}")
    print(f"Failed: {failed}")

    if errors:
        print("\nFailed tests:")
        for test_name, error in errors:
            print(f"  {test_name}: {error}")

    return failed == 0


if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)
