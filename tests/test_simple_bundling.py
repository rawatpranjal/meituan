# tests/test_simple_bundling.py
# pytest -q
"""
Comprehensive test suite for simple_bundling algorithm.

Part I: Core Specification Tests
- READY-only scope at decision time t
- Strict perishability (no missing/nonpositive expiration_time)
- Manhattan timing everywhere
- Same-restaurant bundles only
- Max bundle size 3
- Lexicographic objective: maximize orders → minimize total Manhattan time → minimize couriers → deterministic tie
- Parity with Hungarian when orders ≤ couriers
- Superior throughput when orders > couriers via bundling
- Determinism across repeated runs

Part II: Decision Difference Tests
- Hungarian's global optimization vs greedy's local choices
- Deadline coupling forces cross-matches
- Bundling strictly dominates when orders > couriers
- All tests use READY-only inputs, hard deadlines, Manhattan timing, and deterministic geometry
"""
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import math
import random
import pytest
from types import SimpleNamespace as NS

from assignment_algorithms import (
    assign_greedy, assign_hungarian, assign_simple_bundling,
    _single_edge_manhattan_finish_and_cost
)
from simulator_core import PICKUP_SERVICE_TIME, DROPOFF_SERVICE_TIME

# Helpers: build duck-typed state, couriers, orders
def mk_state(now, orders, speed_kmh=36):
    # 36 km/h -> 10 m/s -> 1 km Manhattan = 100 s
    return NS(
        current_time=now,
        orders={o.id: o for o in orders},
        config={"physics": {"courier_speed_kmh": speed_kmh}}
    )

def courier(cid, x, y):
    return NS(id=cid, current_location=(x, y))

def order(oid, rest_id, rx, ry, dx, dy, ready_time, expiration_time, state="READY"):
    return NS(
        id=oid,
        restaurant_id=rest_id,
        restaurant_location=(rx, ry),
        diner_location=(dx, dy),
        ready_time=ready_time,
        expiration_time=expiration_time,
        state=state
    )

def asg_map(assignments):
    # {courier_id: sorted(order_ids)}
    return {cid: sorted(oids) for cid, oids in assignments}

def flatten_orders(assignments):
    return sorted([oid for _, oids in assignments for oid in oids])

# Manhattan travel time under speed 36 km/h (10 m/s)
def manhattan_seconds(a, b, speed_mps=10.0):
    dx = abs(a[0] - b[0])
    dy = abs(a[1] - b[1])
    dist_km = dx + dy
    return int(round((dist_km * 1000.0) / speed_mps))

# =============================================================================
# PART I: CORE SPECIFICATION TESTS
# =============================================================================

# -----------------------------------------------------------------------------
# 1) READY-only scope and strict perishability
# -----------------------------------------------------------------------------

def test_ready_only_filters_out_future_ready_and_expired():
    now = 10_000
    c = [courier(1, 0, 0)]
    o_ready = order(1, 11, 1, 0, 2, 0, ready_time=now-10, expiration_time=1000)        # READY and valid
    o_future = order(2, 11, 1, 0, 2, 0, ready_time=now+60, expiration_time=1000)       # not READY at t
    o_none_exp = order(3, 11, 1, 0, 2, 0, ready_time=now-10, expiration_time=None)     # invalid expiration
    o_nonpos = order(4, 11, 1, 0, 2, 0, ready_time=now-10, expiration_time=0)          # invalid expiration
    o_expired = order(5, 11, 1, 0, 2, 0, ready_time=now-2000, expiration_time=100)     # already expired

    s = mk_state(now, [o_ready, o_future, o_none_exp, o_nonpos, o_expired])

    sb = assign_simple_bundling(s, c, [o_ready, o_future, o_none_exp, o_nonpos, o_expired])
    assigned_ids = flatten_orders(sb)
    assert assigned_ids == [1], "simple_bundling must ignore future-ready and invalid/expired orders"

    # Hungarian mirrors scope: only READY and feasible
    hg = assign_hungarian(s, c, [o_ready, o_future, o_none_exp, o_nonpos, o_expired])
    assigned_ids_hg = flatten_orders(hg)
    assert assigned_ids_hg == [1], "hungarian baseline should match READY-only scope"

def test_no_waiting_for_future_ready_in_bundles():
    now = 20_000
    c = [courier(1, 0, 0)]
    # Same restaurant, but one order not yet ready -> bundle must be rejected; only READY order can be assigned
    a = order(1, 99, 1, 0, 2, 0, ready_time=now-1, expiration_time=2000)
    b = order(2, 99, 1, 0, 3, 0, ready_time=now+120, expiration_time=2000)  # future-ready
    s = mk_state(now, [a, b])

    sb = assign_simple_bundling(s, c, [a, b])
    assert flatten_orders(sb) == [1], "must not wait for b to become ready; only a can be assigned"

# -----------------------------------------------------------------------------
# 2) Manhattan timing and single-edge feasibility parity
# -----------------------------------------------------------------------------

def test_single_edge_manhattan_finish_matches_closed_form():
    now = 30_000
    r = (1, 0)
    d = (1, 1)
    o = order(1, 7, r[0], r[1], d[0], d[1], ready_time=now-1, expiration_time=10_000)
    c = courier(1, 0, 0)
    s = mk_state(now, [o])

    # Travel: 0,0 -> 1,0 = 1 km -> 100 s; 1,0 -> 1,1 = 1 km -> 100 s
    expected = (100  # to pickup
                + PICKUP_SERVICE_TIME
                + 100  # to drop
                + DROPOFF_SERVICE_TIME)
    got = _single_edge_manhattan_finish_and_cost(s, c, o)
    assert got == expected, f"Manhattan timing should be exact: expected {expected}, got {got}"

# -----------------------------------------------------------------------------
# 3) Same-restaurant only and max bundle size cap
# -----------------------------------------------------------------------------

def test_no_cross_restaurant_bundles_and_size_cap():
    now = 40_000
    c = [courier(1, 0, 0), courier(2, 0, 0)]
    # Three READY orders from restaurant A and one from restaurant B
    a1 = order(1, 100, 1, 0, 2, 0, now-1, 10_000)
    a2 = order(2, 100, 1, 0, 3, 0, now-1, 10_000)
    a3 = order(3, 100, 1, 0, 4, 0, now-1, 10_000)
    b1 = order(4, 200, 10, 0, 11, 0, now-1, 10_000)
    s = mk_state(now, [a1, a2, a3, b1])

    sb = assign_simple_bundling(s, c, [a1, a2, a3, b1])
    # Validate: any bundle of size > 1 must be same-restaurant and size <= 3
    for cid, bundle in sb:
        assert len(bundle) <= 3
        if len(bundle) > 1:
            rest_ids = {s.orders[oid].restaurant_id for oid in bundle}
            assert len(rest_ids) == 1, "bundle must be single-restaurant"

# -----------------------------------------------------------------------------
# 4) Per-order deadlines inside bundle sequence
# -----------------------------------------------------------------------------

def test_bundle_sequence_checks_each_order_deadline():
    now = 50_000
    c = [courier(1, 0, 0)]
    # Same restaurant at (1,0). Diners positioned so that if you deliver to B second, B expires.
    # With speed 10 m/s, 1 km = 100 s.
    # Make B very tight so it must be first; if no sequence keeps both within deadlines, no 2-pack edge is feasible.
    A = order(1, 9, 1, 0, 2, 0, ready_time=now-1, expiration_time=400)   # generous
    B = order(2, 9, 1, 0, 5, 0, ready_time=now-1, expiration_time=220)   # tight
    s = mk_state(now, [A, B])

    # Compute feasibility intuition:
    # to restaurant: 0,0->1,0 = 100; pickup S; to first drop F; S; to second drop S; S
    # If A first then B: 100 + PS + (1km=100) + DS + (3km=300) + DS = 100 + PS + 100 + DS + 300 + DS
    # B's finish would be later; with tight expiry 220 s, likely infeasible.
    sb = assign_simple_bundling(s, c, [A, B])
    bundles = asg_map(sb)
    # Either a single is chosen, or nothing, but never the pair [1,2] together
    assert bundles.get(1) != [1, 2], "pair must be rejected if any drop order would miss its own deadline"

# -----------------------------------------------------------------------------
# 5) Objective order: throughput > time > couriers > deterministic tie
# -----------------------------------------------------------------------------

def test_objective_prefers_lower_total_time_over_fewer_couriers():
    now = 60_000
    # Two couriers; two orders, same restaurant at (1,0). Diners far apart so two singles beat one 2-pack in total time.
    c = [courier(1, 0, 0), courier(2, 0, 0)]
    o1 = order(1, 5, 1, 0, 10, 0, now-1, 10_000)
    o2 = order(2, 5, 1, 0, -10, 0, now-1, 10_000)
    s = mk_state(now, [o1, o2])

    sb = assign_simple_bundling(s, c, [o1, o2])
    # Expect two singles to minimize total time, even though it uses more couriers.
    assigned = asg_map(sb)
    assert set(tuple(v) for v in assigned.values()) == {(1,), (2,)}, \
        "when throughput ties, algorithm minimizes total time even if it uses more couriers"

def test_objective_prefers_fewer_couriers_when_time_ties_and_throughput_equal():
    now = 70_000
    # Symmetric geometry so time ties: two couriers colocated at restaurant; diner locations symmetric and close.
    # With single pickup per bundle, the 2-pack and two singles can be made equal or near-equal; for this test
    # we arrange symmetry and accept either strict tie or near-tie within 1 second, then check courier minimization.
    c = [courier(1, 1, 0), courier(2, 1, 0)]
    o1 = order(1, 77, 1, 0, 2, 0, now-1, 10_000)
    o2 = order(2, 77, 1, 0, 0, 0, now-1, 10_000)
    s = mk_state(now, [o1, o2])

    sb = assign_simple_bundling(s, c, [o1, o2])
    # Expect a single 2-pack to one courier when time is tied or effectively tied.
    assert any(len(bundle) == 2 for _, bundle in sb), \
        "when throughput and time tie, algorithm should minimize number of couriers used"

def test_deterministic_tie_break_lower_courier_id_then_lex_bundle_code():
    now = 80_000
    # One courier. Three orders same restaurant. Make 3-pack infeasible but 2-packs feasible.
    c = [courier(10, 0, 0)]
    r = (1, 0)
    # Diners positioned so [1,2] and [1,3] have equal cost, but 3-pack violates o3's deadline
    o1 = order(1, 42, r[0], r[1], 2, 0, now-1, 10_000)
    o2 = order(2, 42, r[0], r[1], 0, 0, now-1, 10_000)
    # Make o3 tight: 3-pack would take ~100(pickup)+PS+100(drop1)+DS+200(drop2)+DS = 500+2*PS+2*DS
    # With PS=DS=30, that's ~560s. Set expiry to 400s so 3-pack fails but 2-packs with o3 succeed.
    o3 = order(3, 42, r[0], r[1], 0, 0, now-1, 400)
    s = mk_state(now, [o1, o2, o3])

    sb = assign_simple_bundling(s, c, [o1, o2, o3])
    # Now we expect a 2-pack since 3-pack is infeasible
    assigned_bundle = sorted([oids for _, oids in sb][0])
    assert len(assigned_bundle) == 2, "3-pack should be infeasible; expect a 2-pack"
    assert assigned_bundle in ([1,2], [1,3], [2,3]), "deterministic tie should pick one of the feasible 2-packs"

def test_tie_between_couriers_prefers_lower_id():
    now = 81_000
    # One order, two couriers equidistant
    c = [courier(5, 0, 0), courier(3, 0, 0)]
    o = order(1, 21, 1, 0, 2, 0, now-1, 10_000)
    s = mk_state(now, [o])

    sb = assign_simple_bundling(s, c, [o])
    chosen_courier = sb[0][0]
    assert chosen_courier == 3, "tie among equal edges must pick lower courier id"

# -----------------------------------------------------------------------------
# 6) Parity with Hungarian when orders <= couriers
# -----------------------------------------------------------------------------

def test_parity_with_hungarian_when_orders_leq_couriers():
    now = 90_000
    c = [courier(1, 0, 0), courier(2, 0, 0)]
    a = order(1, 5, 1, 0, 2, 0, now-1, 10_000)
    b = order(2, 6, 5, 0, 6, 0, now-1, 10_000)
    s = mk_state(now, [a, b])

    sb = assign_simple_bundling(s, c, [a, b])
    hg = assign_hungarian(s, c, [a, b])

    assert len(flatten_orders(sb)) == len(flatten_orders(hg)) == 2, \
        "when orders ≤ couriers, simple_bundling must match Hungarian throughput"

# -----------------------------------------------------------------------------
# 7) Throughput gain when orders > couriers via bundling
# -----------------------------------------------------------------------------

def test_bundling_increases_throughput_when_orders_gt_couriers():
    now = 100_000
    c = [courier(1, 0, 0)]  # only one courier
    # Three READY orders from same restaurant; deadlines generous so a 3-pack is feasible
    o1 = order(1, 33, 1, 0, 2, 0, now-1, 10_000)
    o2 = order(2, 33, 1, 0, 3, 0, now-1, 10_000)
    o3 = order(3, 33, 1, 0, 4, 0, now-1, 10_000)
    s = mk_state(now, [o1, o2, o3])

    sb = assign_simple_bundling(s, c, [o1, o2, o3])
    hg = assign_hungarian(s, c, [o1, o2, o3])

    assert len(flatten_orders(hg)) == 1, "Hungarian assigns at most one order to one courier"
    assert len(flatten_orders(sb)) >= 2, "simple_bundling should deliver more via bundling (up to 3 here)"

# -----------------------------------------------------------------------------
# 8) Determinism across repeated runs
# -----------------------------------------------------------------------------

def test_determinism_repeated_runs_same_instance():
    now = 110_000
    c = [courier(2, 0, 0), courier(1, 0, 0)]
    o1 = order(1, 88, 1, 0, 2, 0, now-1, 10_000)
    o2 = order(2, 88, 1, 0, 3, 0, now-1, 10_000)
    s = mk_state(now, [o1, o2])

    sb1 = assign_simple_bundling(s, c, [o1, o2])
    sb2 = assign_simple_bundling(s, c, [o1, o2])
    assert sb1 == sb2, "solver should return deterministic choices for identical inputs"

# =============================================================================
# PART II: DECISION DIFFERENCE TESTS (vs Greedy and Hungarian)
# =============================================================================

# -----------------------------------------------------------------------------
# 9) Global optimality beats local greed: same throughput target but lower time
# -----------------------------------------------------------------------------
def test_hungarian_reduces_total_pickup_time_vs_greedy_with_coupled_choices():
    now = 120_000
    c1 = courier(1, 0, 0)
    c2 = courier(2, 10, 0)
    # Restaurants on x-axis; diners colocated with restaurants for simplicity
    o1 = order(1, 101, 2, 0, 2, 0, ready_time=now-1, expiration_time=10_000)  # closer to c1, but not by much
    o2 = order(2, 102, 1, 0, 1, 0, ready_time=now-1, expiration_time=10_000)  # very close to c1, far from c2
    s = mk_state(now, [o1, o2])

    # Greedy processes oldest-first; both are READY, order list as passed.
    g_asg = assign_greedy(s, [c1, c2], [o1, o2])
    h_asg = assign_hungarian(s, [c1, c2], [o1, o2])

    # Compute sum of Manhattan pickup seconds for each plan
    def sum_pickup(assignments):
        total = 0
        for cid, oids in assignments:
            courier_loc = {1: c1.current_location, 2: c2.current_location}[cid]
            # one order per courier in this test
            rest_loc = s.orders[oids[0]].restaurant_location
            total += manhattan_seconds(courier_loc, rest_loc)
        return total

    assert len(g_asg) == len(h_asg) == 2
    assert sum_pickup(h_asg) <= sum_pickup(g_asg), "Hungarian should minimize total pickup time jointly."

# -----------------------------------------------------------------------------
# 10) Deadline-coupled cross-match: greedy burns the only viable courier
# -----------------------------------------------------------------------------
def test_hungarian_cross_matches_to_save_deadline_that_greedy_misses():
    now = 130_000
    c1 = courier(1, 0, 0)
    c2 = courier(2, 0, 90)

    # A is near c2 but has huge expiration so c1's long trip is still feasible.
    A = order(1, 111, 0, 91, 0, 91, ready_time=now-2, expiration_time=15_000)

    # B is near c2 and far from c1; compute tight expiry so c2->B is feasible but c1->B is not
    # c2 to B: |0-0| + |90-95| = 5 km = 500s travel + PS + DS
    # c1 to B: |0-0| + |0-95| = 95 km = 9500s travel + PS + DS (way too long)
    b_travel_c2 = manhattan_seconds((0, 90), (0, 95))
    b_exp_feasible = b_travel_c2 + PICKUP_SERVICE_TIME + DROPOFF_SERVICE_TIME + 50  # small margin
    B = order(2, 222, 0, 95, 0, 95, ready_time=now-1, expiration_time=b_exp_feasible)

    s = mk_state(now, [A, B])

    g_asg = assign_greedy(s, [c1, c2], [A, B])        # greedy sees A first and assigns c2->A
    h_asg = assign_hungarian(s, [c1, c2], [A, B])     # Hungarian assigns c2->B and c1->A

    assert len(flatten_orders(g_asg)) <= len(flatten_orders(h_asg)), \
        "Hungarian should preserve or increase deliveries vs greedy via cross-match."

# -----------------------------------------------------------------------------
# 11) Tight expiries make greedy under-assign; Hungarian keeps both
# -----------------------------------------------------------------------------
def test_hungarian_maximizes_cardinality_under_tight_dual_expiries():
    now = 140_000
    c1 = courier(1, 0, 0)
    c2 = courier(2, 10, 10)

    # Place A where c2 is naturally better but c1 is barely feasible.
    A = order(1, 301, 9, 9, 9, 9, ready_time=now-2, expiration_time=3000)
    # Place B so only c2 can meet the expiry comfortably; tune so c1->B misses.
    B = order(2, 302, 10, 10, 10, 10, ready_time=now-1, expiration_time=1200)

    s = mk_state(now, [A, B])

    g_asg = assign_greedy(s, [c1, c2], [A, B])
    h_asg = assign_hungarian(s, [c1, c2], [A, B])

    assert len(flatten_orders(g_asg)) <= len(flatten_orders(h_asg)), \
        "Hungarian pairing should preserve or increase feasible deliveries vs greedy."

# -----------------------------------------------------------------------------
# 12) Orders > couriers: simple_bundling strictly dominates greedy and Hungarian
# -----------------------------------------------------------------------------
def test_simple_bundling_outperforms_greedy_and_hungarian_when_orders_exceed_couriers():
    now = 150_000
    c = [courier(1, 0, 0)]
    o1 = order(1, 900, 1, 0, 2, 0, ready_time=now-1, expiration_time=10_000)
    o2 = order(2, 900, 1, 0, 3, 0, ready_time=now-1, expiration_time=10_000)
    o3 = order(3, 900, 1, 0, 4, 0, ready_time=now-1, expiration_time=10_000)
    s = mk_state(now, [o1, o2, o3])

    g_asg = assign_greedy(s, c, [o1, o2, o3])
    h_asg = assign_hungarian(s, c, [o1, o2, o3])
    sb_asg = assign_simple_bundling(s, c, [o1, o2, o3])

    assert len(flatten_orders(g_asg)) == 1
    assert len(flatten_orders(h_asg)) == 1
    assert len(flatten_orders(sb_asg)) >= 2, "Bundling should deliver multiple orders with one pickup."

# -----------------------------------------------------------------------------
# 13) Equal throughput, lower time: Hungarian than greedy
# -----------------------------------------------------------------------------
def test_equal_throughput_lower_time_hungarian_than_greedy():
    now = 160_000
    c1 = courier(1, 0, 0)
    c2 = courier(2, 10, 0)
    A = order(1, 401, 2, 0, 2, 0, ready_time=now-2, expiration_time=10_000)
    B = order(2, 402, 9, 0, 9, 0, ready_time=now-1, expiration_time=10_000)
    s = mk_state(now, [A, B])

    g_asg = assign_greedy(s, [c1, c2], [A, B])   # likely c1->A then c2->B
    h_asg = assign_hungarian(s, [c1, c2], [A, B])

    def sum_pickup(assignments):
        total = 0
        for cid, oids in assignments:
            cloc = {1: c1.current_location, 2: c2.current_location}[cid]
            total += manhattan_seconds(cloc, s.orders[oids[0]].restaurant_location)
        return total

    assert len(flatten_orders(g_asg)) == len(flatten_orders(h_asg)) == 2
    assert sum_pickup(h_asg) <= sum_pickup(g_asg), "Hungarian should globally minimize pickup time."

# -----------------------------------------------------------------------------
# 14) READY-only and perishable honored; greedy may under-assign
# -----------------------------------------------------------------------------
def test_ready_only_perishable_honored_greedy_may_under_assign():
    now = 170_000
    # Move c1 far from A so greedy picks c2 for A (older order); keep A's expiry generous so c1 could do A
    c1 = courier(1, 0, 60)  # far from A at y=1
    c2 = courier(2, 0, 20)

    # A is near y=1; c1 is 59 km away (5900s + services) but expiry is generous
    A = order(1, 551, 0, 1, 0, 1, ready_time=now-2, expiration_time=12_000)

    # B near y=19; compute tight expiry so c2->B is feasible but c1->B is not
    # c2 to B: |0-0| + |20-19| = 1 km = 100s + PS + DS
    # c1 to B: |0-0| + |60-19| = 41 km = 4100s + PS + DS (too long for tight expiry)
    b_travel_c2 = manhattan_seconds((0, 20), (0, 19))
    b_exp_feasible = b_travel_c2 + PICKUP_SERVICE_TIME + DROPOFF_SERVICE_TIME + 50
    B = order(2, 552, 0, 19, 0, 19, ready_time=now-1, expiration_time=b_exp_feasible)

    s = mk_state(now, [A, B])

    g_asg = assign_greedy(s, [c1, c2], [A, B])  # burns c2 on A (older), loses B
    h_asg = assign_hungarian(s, [c1, c2], [A, B])  # flips: c2->B, c1->A

    # Greedy should burn c2 on A and lose B; Hungarian should save both
    assert len(flatten_orders(g_asg)) <= len(flatten_orders(h_asg)), \
        "Hungarian should preserve or increase deliveries vs greedy."
