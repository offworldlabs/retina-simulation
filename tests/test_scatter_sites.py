"""Scatter layout: the fleet nobody designed.

Ring and dual both buy geometry on purpose — receivers placed to surround the
airspace, or paired on one mast to share a focus.  A community deployment does
neither: sites land where operators live, on whatever illuminator comes in
best, pointed by hand.  These tests pin the properties that make the layout a
fair test of the solver rather than a second ring under another name.
"""

import math
from collections import Counter

from retina_simulation.generator import (
    _KNOWN_METROS,
    _haversine_km,
    generate_fleet,
)


def _scatter(**kw):
    kw.setdefault("n_nodes", 16)
    kw.setdefault("metro", "gvl")
    kw.setdefault("n_cluster", 12)
    kw.setdefault("n_clusters", 1)
    kw.setdefault("use_tower_api", False)
    kw.setdefault("seed", 42)
    kw.setdefault("layout", "scatter")
    fleet = generate_fleet(**kw)
    return fleet, [n for n in fleet if "SCAT" in n["node_id"]]


GVL = _KNOWN_METROS["gvl"]


class TestScatterStructure:
    def test_the_layout_produces_the_requested_node_budget(self):
        _fleet, scat = _scatter(n_cluster=12)
        assert len(scat) == 12

    def test_no_ring_nodes_survive(self):
        """Scatter replaces the ring; a ring alongside it would supply the
        designed geometry the layout exists to do without."""
        fleet, _scat = _scatter()
        assert not [n for n in fleet if "RING" in n["node_id"]]

    def test_every_site_stays_inside_the_metro(self):
        """A site outside the metro never sees an aircraft — the traffic model
        is metro-scoped, so it would be a permanently dark node."""
        _fleet, scat = _scatter()
        radius_km = GVL["radius_nm"] * 1.852
        for n in scat:
            assert _haversine_km(n["rx_lat"], n["rx_lon"], GVL["lat"], GVL["lon"]) <= radius_km

    def test_sites_carry_an_explicit_hand_aimed_azimuth(self):
        _fleet, scat = _scatter()
        assert all("beam_azimuth_deg" in n for n in scat)
        assert all(0.0 <= n["beam_azimuth_deg"] < 360.0 for n in scat)


class TestUndesignedGeometry:
    def test_sites_do_not_share_one_illuminator(self):
        """The ring's shared TX is the thing that keeps every node's Doppler
        inside one association gate.  A real fleet has no such luxury."""
        _fleet, scat = _scatter()
        towers = Counter(n["tx_callsign"] for n in scat)
        assert len(towers) >= 3
        # And no single tower carries the fleet the way the ring TX does.
        assert towers.most_common(1)[0][1] < len(scat) * 0.75

    def test_illuminators_are_nearby_ones(self):
        """Weighting is 1/d^2 — an operator uses the station that comes in
        best, not one 200 km away."""
        _fleet, scat = _scatter()
        for n in scat:
            baseline = _haversine_km(n["rx_lat"], n["rx_lon"], n["tx_lat"], n["tx_lon"])
            assert baseline <= 75.0

    def test_aim_is_not_uniformly_at_the_core(self):
        """The ring aims every beam at the core exactly.  Here most point
        roughly there and some point elsewhere, so beams do not all intersect."""
        _fleet, scat = _scatter()
        errs = []
        for n in scat:
            to_core = (
                math.degrees(
                    math.atan2(
                        math.sin(math.radians(GVL["lon"] - n["rx_lon"])) * math.cos(math.radians(GVL["lat"])),
                        math.cos(math.radians(n["rx_lat"])) * math.sin(math.radians(GVL["lat"]))
                        - math.sin(math.radians(n["rx_lat"]))
                        * math.cos(math.radians(GVL["lat"]))
                        * math.cos(math.radians(GVL["lon"] - n["rx_lon"])),
                    )
                )
                % 360.0
            )
            errs.append(abs((n["beam_azimuth_deg"] - to_core + 180.0) % 360.0 - 180.0))
        assert max(errs) > 45.0  # somebody points well off-core
        assert sum(e < 45.0 for e in errs) >= len(errs) // 2  # most do not

    def test_reach_varies_but_beamwidth_is_uniform(self):
        """Reach varies per site (60 km is what a good setup achieves, not an
        average one), but every antenna is the same 42-degree Yagi — width
        jitter was removed deliberately."""
        _fleet, scat = _scatter()
        reaches = {n["max_bistatic_range_km"] for n in scat}
        widths = {n["beam_width_deg"] for n in scat}
        assert len(reaches) > 1
        assert widths == {42.0}
        assert max(n["max_bistatic_range_km"] for n in scat) <= 60.0

    def test_max_range_and_bistatic_limit_agree(self):
        """Consumers that have not been taught the bistatic rule read
        max_range_km; they must not see a different number."""
        _fleet, scat = _scatter()
        for n in scat:
            assert n["max_range_km"] == n["max_bistatic_range_km"]

    def test_sites_clump_rather_than_spacing_evenly(self):
        """A ring's nearest-neighbour distances are all equal by construction.
        Clumping is what puts near-parallel range gradients in the fleet, which
        is the geometry the solver actually has to cope with."""
        _fleet, scat = _scatter()
        nearest = []
        for a in scat:
            nearest.append(
                min(_haversine_km(a["rx_lat"], a["rx_lon"], b["rx_lat"], b["rx_lon"]) for b in scat if b is not a)
            )
        assert max(nearest) / max(min(nearest), 0.1) > 3.0


class TestDeterminism:
    def test_same_seed_same_fleet(self):
        a, _ = _scatter(seed=7)
        b, _ = _scatter(seed=7)
        assert a == b

    def test_different_seed_different_placement(self):
        _fa, sa = _scatter(seed=7)
        _fb, sb = _scatter(seed=8)
        assert [(n["rx_lat"], n["rx_lon"]) for n in sa] != [(n["rx_lat"], n["rx_lon"]) for n in sb]


class TestLayoutFleetFaults:
    """Stage-1 fixes: short/empty fleets and ring cells for ringless layouts."""

    def test_scatter_without_metro_raises_instead_of_a_short_fleet(self):
        import pytest

        with pytest.raises(ValueError, match="requires --metro"):
            generate_fleet(n_nodes=16, n_cluster=12, layout="scatter", use_tower_api=False, seed=42)

    def test_dual_without_metro_raises_instead_of_a_short_fleet(self):
        import pytest

        with pytest.raises(ValueError, match="requires --metro"):
            generate_fleet(n_nodes=16, n_cluster=12, layout="dual", use_tower_api=False, seed=42)

    def test_scatter_budget_is_not_gated_on_the_ring_table(self):
        """A metro with no _RING_TXS entry used to zero n_cluster for scatter,
        which has no rings at all."""
        fleet, scat = _scatter(n_clusters=0)
        assert len(scat) == 12
        assert len(fleet) == 16

    def test_coverage_cells_do_not_describe_rings_for_ringless_layouts(self):
        from retina_simulation.generator import coverage_cells

        cells = coverage_cells(n_cluster=12, n_clusters=1, metro="gvl", layout="scatter")
        assert len(cells) == 1
        assert cells[0]["ring_id"] == "synth-SCATTER"
        assert abs(cells[0]["core_lat"] - GVL["lat"]) < 1e-9
        # And without a metro there is nothing to describe.
        assert coverage_cells(n_cluster=12, layout="scatter") == []

    def test_ring_layout_cells_are_unchanged(self):
        from retina_simulation.generator import coverage_cells

        ring = coverage_cells(n_cluster=12, n_clusters=1, metro="gvl", layout="ring")
        default = coverage_cells(n_cluster=12, n_clusters=1, metro="gvl")
        assert ring == default

    def test_solo_nodes_declare_the_bistatic_limit(self):
        fleet = generate_fleet(n_nodes=30, n_cluster=8, n_clusters=1, use_tower_api=False, seed=42)
        solos = [n for n in fleet if "SOLO" in n["node_id"]]
        assert solos, "expected solo nodes in a nationwide fleet"
        for n in solos:
            assert n.get("max_bistatic_range_km") == n["max_range_km"]

    def test_empty_fleet_summary_does_not_crash(self):
        from retina_simulation.generator import fleet_summary

        s = fleet_summary([])
        assert s["total_nodes"] == 0


class TestVerticalRateDecays:
    def test_vel_up_decays_toward_level_flight(self):
        import random

        from retina_simulation.world import SimulationWorld

        random.seed(3)
        w = SimulationWorld()
        w.step(1.0)  # spawn traffic
        tagged = list(w.aircraft)
        assert tagged
        for ac in tagged:
            ac.vel_up = 0.003  # force the worst-case spawn climb rate
            ac.alt_km = 8.0
            # Outlive the 600 s window: the subject is vel_up decay, and
            # whether seed 3's lifetime rolls happen to retire the tagged
            # aircraft first is RNG-stream trivia (spawn-separation resampling
            # legitimately consumes extra draws).
            ac.lifetime_s = 10_000.0
        for _ in range(600):  # 600 s of simulation
            w.step(1.0)
        survivors = [ac for ac in w.aircraft if ac in tagged]
        assert survivors
        # vel_up used to integrate forever; every aircraft saturated at the
        # 15 km ceiling.  With the decay nobody is pinned there.
        assert all(abs(ac.vel_up) < 0.001 for ac in survivors)
        assert all(ac.alt_km < 15.0 for ac in survivors)
