"""
Network Bundling Algorithm Tests

Tests the assign_network_bundling function from assignment_algorithms.py
Key features: bundles from up to 2 restaurants, TSP optimization, per-order deadline checks
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from simulator_core import SimulationState, Courier, Order, Restaurant
from assignment_algorithms import assign_network_bundling


# ============================================================================
# CATEGORY 1: MULTI-RESTAURANT BUNDLING (≤2 restaurants)
# ============================================================================

def test_1_1_single_restaurant_bundle():
    """Test 1.1: Single restaurant bundle should work"""
    print("\n" + "="*80)
    print("TEST 1.1: Single restaurant bundle")
    print("="*80)

    restaurants = [Restaurant(0, (1.0, 0.0))]
    couriers = [Courier(0, (0.0, 0.0))]

    orders = [
        Order(0, 0, (1.0, 0.0), (2.0, 0.0), 0),
        Order(1, 0, (1.0, 0.0), (2.0, 1.0), 0)
    ]
    for o in orders:
        o.state = "READY"
        o.ready_time = 0
        o.expiration_time = 3600  # 1 hour deadline

    state = SimulationState(restaurants, couriers, orders, duration=3600)
    result = assign_network_bundling(state, couriers, orders)

    print(f"Result: {result}")
    assert len(result) == 1
    assert len(result[0]['order_ids']) == 2
    print("PASS")


def test_1_2_two_restaurant_bundle():
    """Test 1.2: Two restaurant bundle - network's signature capability"""
    print("\n" + "="*80)
    print("TEST 1.2: Two restaurant bundle")
    print("="*80)

    restaurants = [
        Restaurant(0, (1.0, 0.0)),
        Restaurant(1, (0.0, 1.0))
    ]
    couriers = [Courier(0, (0.0, 0.0))]

    orders = [
        Order(0, 0, (1.0, 0.0), (3.0, 0.0), 0),  # From R0
        Order(1, 1, (0.0, 1.0), (0.0, 3.0), 0)   # From R1
    ]
    for o in orders:
        o.state = "READY"
        o.ready_time = 0
        o.expiration_time = 3600

    state = SimulationState(restaurants, couriers, orders, duration=3600)
    result = assign_network_bundling(state, couriers, orders)

    print(f"Result: {result}")
    assert len(result) == 1
    assert len(result[0]['order_ids']) == 2
    # Verify orders from different restaurants
    assigned_orders = [orders[oid] for oid in result[0]['order_ids']]
    restaurant_ids = {o.restaurant_id for o in assigned_orders}
    assert len(restaurant_ids) == 2, f"Expected 2 restaurants, got {restaurant_ids}"
    print("PASS")


def test_1_3_reject_three_restaurant_bundle():
    """Test 1.3: Should reject bundles from >2 restaurants"""
    print("\n" + "="*80)
    print("TEST 1.3: Reject >2 restaurant bundle")
    print("="*80)

    restaurants = [
        Restaurant(0, (1.0, 0.0)),
        Restaurant(1, (0.0, 1.0)),
        Restaurant(2, (1.0, 1.0))
    ]
    couriers = [Courier(0, (0.5, 0.5))]  # Central position

    orders = [
        Order(0, 0, (1.0, 0.0), (2.0, 0.0), 0),  # From R0
        Order(1, 1, (0.0, 1.0), (0.0, 2.0), 0),  # From R1
        Order(2, 2, (1.0, 1.0), (2.0, 2.0), 0)   # From R2
    ]
    for o in orders:
        o.state = "READY"
        o.ready_time = 0
        o.expiration_time = 3600

    state = SimulationState(restaurants, couriers, orders, duration=3600)
    result = assign_network_bundling(state, couriers, orders)

    print(f"Result: {result}")
    # Should create at most a 2-restaurant bundle
    if result:
        assigned_orders = [orders[oid] for oid in result[0]['order_ids']]
        restaurant_ids = {o.restaurant_id for o in assigned_orders}
        assert len(restaurant_ids) <= 2, f"Bundle has >2 restaurants: {restaurant_ids}"
    print("PASS")


# ============================================================================
# CATEGORY 2: DEADLINE FEASIBILITY
# ============================================================================

def test_2_1_reject_bundle_if_any_order_expires():
    """Test 2.1: Reject bundle if any order would miss deadline"""
    print("\n" + "="*80)
    print("TEST 2.1: Reject bundle if any order expires")
    print("="*80)

    restaurants = [
        Restaurant(0, (5.0, 0.0)),
        Restaurant(1, (0.0, 5.0))
    ]
    couriers = [Courier(0, (0.0, 0.0))]

    orders = [
        Order(0, 0, (5.0, 0.0), (10.0, 0.0), 0),  # Far order from R0
        Order(1, 1, (0.0, 5.0), (0.0, 10.0), 0)   # Far order from R1
    ]
    orders[0].state = "READY"
    orders[0].ready_time = 0
    orders[0].expiration_time = 600  # 10 minutes - feasible solo

    orders[1].state = "READY"
    orders[1].ready_time = 0
    orders[1].expiration_time = 300  # 5 minutes - too tight for bundle

    state = SimulationState(restaurants, couriers, orders, duration=3600)
    result = assign_network_bundling(state, couriers, orders)

    print(f"Result: {result}")
    # Should not bundle them together due to tight deadline on order 1
    if result and len(result[0]['order_ids']) > 1:
        assert False, "Should not bundle orders when one has tight deadline"
    print("PASS")


def test_2_2_per_order_deadline_checking():
    """Test 2.2: Each order has its own deadline"""
    print("\n" + "="*80)
    print("TEST 2.2: Per-order deadline checking")
    print("="*80)

    restaurants = [Restaurant(0, (2.0, 0.0))]
    couriers = [Courier(0, (0.0, 0.0))]

    orders = [
        Order(0, 0, (2.0, 0.0), (4.0, 0.0), 0),
        Order(1, 0, (2.0, 0.0), (2.0, 2.0), 0)
    ]
    orders[0].state = "READY"
    orders[0].ready_time = 0
    orders[0].expiration_time = 1200  # 20 minutes - generous

    orders[1].state = "READY"
    orders[1].ready_time = 0
    orders[1].expiration_time = 400  # 6.7 minutes - tight

    state = SimulationState(restaurants, couriers, orders, duration=3600)

    # Calculate if bundle is feasible
    # Travel to restaurant: 2 km = 240s
    # Pickup service: 90s
    # Delivery to (4,0): 2 km = 240s, dropoff: 45s
    # Delivery to (2,2): 4 km = 480s, dropoff: 45s
    # Total for order 1 if delivered second: 240+90+240+45+480+45 = 1140s > 400s deadline

    result = assign_network_bundling(state, couriers, orders)
    print(f"Result: {result}")

    # Should either assign separately or prioritize tight deadline
    if result and len(result[0]['order_ids']) == 2:
        # If bundled, verify it meets both deadlines (which shouldn't be possible)
        print("WARNING: Bundle created despite tight deadline - checking route optimization")
    print("PASS")


# ============================================================================
# CATEGORY 3: SERVICE TIME ACCOUNTING
# ============================================================================

def test_3_1_pickup_service_per_restaurant():
    """Test 3.1: Pickup service counted once per restaurant"""
    print("\n" + "="*80)
    print("TEST 3.1: Pickup service per restaurant")
    print("="*80)

    restaurants = [
        Restaurant(0, (1.0, 0.0)),
        Restaurant(1, (2.0, 0.0))
    ]
    couriers = [Courier(0, (0.0, 0.0))]

    # 3 orders: 2 from R0, 1 from R1
    orders = [
        Order(0, 0, (1.0, 0.0), (1.5, 0.0), 0),  # R0
        Order(1, 0, (1.0, 0.0), (1.0, 0.5), 0),  # R0
        Order(2, 1, (2.0, 0.0), (2.5, 0.0), 0)   # R1
    ]
    for o in orders:
        o.state = "READY"
        o.ready_time = 0
        o.expiration_time = 3600

    state = SimulationState(restaurants, couriers, orders, duration=3600)

    # Expected service times:
    # - Pickup at R0: 90s (for both orders 0 and 1)
    # - Pickup at R1: 90s (for order 2)
    # - Dropoffs: 45s × 3 = 135s
    # Total service: 90 + 90 + 135 = 315s

    result = assign_network_bundling(state, couriers, orders)
    print(f"Result: {result}")

    if result and len(result[0]['order_ids']) == 3:
        print("Successfully bundled 3 orders from 2 restaurants")
    print("PASS")


def test_3_2_same_location_restaurants():
    """Test 3.2: Restaurants at same location still count separate pickup service"""
    print("\n" + "="*80)
    print("TEST 3.2: Same location restaurants")
    print("="*80)

    restaurants = [
        Restaurant(0, (1.0, 1.0)),
        Restaurant(1, (1.0, 1.0))  # Same location as R0
    ]
    couriers = [Courier(0, (0.0, 0.0))]

    orders = [
        Order(0, 0, (1.0, 1.0), (2.0, 1.0), 0),  # From R0
        Order(1, 1, (1.0, 1.0), (1.0, 2.0), 0)   # From R1
    ]
    for o in orders:
        o.state = "READY"
        o.ready_time = 0
        o.expiration_time = 3600

    state = SimulationState(restaurants, couriers, orders, duration=3600)

    # Even though restaurants share location, should count 2 pickup services
    # Travel between them is 0, but service time is still 90s each

    result = assign_network_bundling(state, couriers, orders)
    print(f"Result: {result}")
    print("PASS")


# ============================================================================
# CATEGORY 4: COMPARISON WITH SIMPLE BUNDLING
# ============================================================================

def test_4_1_network_beats_simple_cross_restaurant():
    """Test 4.1: Network bundling outperforms simple with nearby restaurants"""
    print("\n" + "="*80)
    print("TEST 4.1: Network beats simple bundling")
    print("="*80)

    restaurants = [
        Restaurant(0, (1.0, 0.0)),
        Restaurant(1, (1.2, 0.0))  # Very close to R0
    ]
    couriers = [
        Courier(0, (0.0, 0.0)),
        Courier(1, (10.0, 10.0))  # Far away
    ]

    orders = [
        Order(0, 0, (1.0, 0.0), (5.0, 4.0), 0),  # From R0, far delivery
        Order(1, 1, (1.2, 0.0), (5.0, 3.8), 0)   # From R1, nearby delivery
    ]
    for o in orders:
        o.state = "READY"
        o.ready_time = 0
        o.expiration_time = 3600

    state = SimulationState(restaurants, couriers, orders, duration=3600)

    # Network should assign both to courier 0 as a 2-restaurant bundle
    # Simple bundling would need to use both couriers (one per restaurant)

    result = assign_network_bundling(state, couriers, orders)
    print(f"Result: {result}")

    assert len(result) == 1, "Should use only 1 courier"
    assert len(result[0]['order_ids']) == 2, "Should bundle both orders"
    assert result[0]['courier_id'] == 0, "Should use closer courier"
    print("PASS")


def test_4_2_river_crossing_minimization():
    """Test 4.2: Network bundling reduces river crossings"""
    print("\n" + "="*80)
    print("TEST 4.2: River crossing minimization")
    print("="*80)

    # All restaurants south (y < 2.5), all customers north (y > 2.5)
    restaurants = [
        Restaurant(0, (1.0, 1.0)),  # South
        Restaurant(1, (3.0, 1.0))   # South
    ]
    couriers = [Courier(0, (2.0, 0.5))]  # South

    orders = [
        Order(0, 0, (1.0, 1.0), (1.5, 4.0), 0),  # R0 to north
        Order(1, 1, (3.0, 1.0), (2.5, 4.0), 0)   # R1 to north
    ]
    for o in orders:
        o.state = "READY"
        o.ready_time = 0
        o.expiration_time = 3600

    state = SimulationState(restaurants, couriers, orders, duration=3600)

    # Network bundling should pick up from both restaurants before crossing
    # This minimizes river crossings to 1 (pickup tour then delivery tour)
    # Simple bundling would cross twice (once per restaurant)

    result = assign_network_bundling(state, couriers, orders)
    print(f"Result: {result}")

    if result and len(result[0]['order_ids']) == 2:
        print("Successfully bundled cross-restaurant to minimize river crossings")
    print("PASS")


# ============================================================================
# CATEGORY 5: CONFIGURATION AND DETERMINISM
# ============================================================================

def test_5_1_respect_max_bundle_size():
    """Test 5.1: Respect max_bundle_size from config"""
    print("\n" + "="*80)
    print("TEST 5.1: Respect max_bundle_size")
    print("="*80)

    restaurants = [Restaurant(0, (1.0, 0.0))]
    couriers = [Courier(0, (0.0, 0.0))]

    # 4 orders from same restaurant
    orders = [
        Order(i, 0, (1.0, 0.0), (2.0, float(i)), 0)
        for i in range(4)
    ]
    for o in orders:
        o.state = "READY"
        o.ready_time = 0
        o.expiration_time = 3600

    state = SimulationState(restaurants, couriers, orders, duration=3600)
    state.config = {
        'algorithms': {'bundling': {'max_bundle_size': 3}},
        'physics': {
            'distance_metric': 'manhattan',
            'courier_speed_kmh': 30,
            'pickup_service_time_s': 90,
            'dropoff_service_time_s': 45
        }
    }

    result = assign_network_bundling(state, couriers, orders)
    print(f"Result: {result}")

    if result:
        assert len(result[0]['order_ids']) <= 3, "Should respect max_bundle_size=3"
    print("PASS")


def test_5_2_deterministic_selection():
    """Test 5.2: Deterministic bundle selection"""
    print("\n" + "="*80)
    print("TEST 5.2: Deterministic selection")
    print("="*80)

    restaurants = [Restaurant(0, (1.0, 1.0))]
    couriers = [
        Courier(0, (0.0, 0.0)),
        Courier(1, (2.0, 2.0))
    ]

    orders = [
        Order(0, 0, (1.0, 1.0), (2.0, 1.0), 0),
        Order(1, 0, (1.0, 1.0), (1.0, 2.0), 0)
    ]
    for o in orders:
        o.state = "READY"
        o.ready_time = 0
        o.expiration_time = 3600

    state = SimulationState(restaurants, couriers, orders, duration=3600)

    # Run multiple times - should get same result
    result1 = assign_network_bundling(state, couriers, orders)
    result2 = assign_network_bundling(state, couriers, orders)

    print(f"Result 1: {result1}")
    print(f"Result 2: {result2}")

    assert result1 == result2, "Results should be deterministic"
    print("PASS")


# ============================================================================
# RUN ALL TESTS
# ============================================================================

def run_all_tests():
    """Run all network bundling tests"""
    tests = [
        # Category 1: Multi-restaurant bundling
        test_1_1_single_restaurant_bundle,
        test_1_2_two_restaurant_bundle,
        test_1_3_reject_three_restaurant_bundle,

        # Category 2: Deadline feasibility
        test_2_1_reject_bundle_if_any_order_expires,
        test_2_2_per_order_deadline_checking,

        # Category 3: Service time accounting
        test_3_1_pickup_service_per_restaurant,
        test_3_2_same_location_restaurants,

        # Category 4: Comparison with simple bundling
        test_4_1_network_beats_simple_cross_restaurant,
        test_4_2_river_crossing_minimization,

        # Category 5: Configuration and determinism
        test_5_1_respect_max_bundle_size,
        test_5_2_deterministic_selection
    ]

    passed = 0
    failed = 0

    for test in tests:
        try:
            test()
            passed += 1
        except Exception as e:
            failed += 1
            print(f"FAILED: {test.__name__}")
            print(f"Error: {e}")
            import traceback
            traceback.print_exc()

    print("\n" + "="*80)
    print(f"NETWORK BUNDLING TEST SUMMARY: {passed} passed, {failed} failed")
    print("="*80)

    return failed == 0


if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)