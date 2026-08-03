"""Detection range limited on bistatic range rather than RX distance.

A monostatic limit compares only the RX->target leg against max_range_km,
ignoring the transmitter entirely.  That is not what a passive radar is
limited by: the delay measures (RX->target) + (target->TX) - baseline, and
that sum is what sets received power via the bistatic radar equation.  The
difference is not cosmetic — at a fixed RX distance the bistatic range varies
by more than the whole budget depending on which way the target lies relative
to the transmitter.

Nodes without max_bistatic_range_km keep the monostatic rule so real hardware
(which only ever carries max_range_km) is unaffected.
"""

import math

from retina_simulation.world import (
    NodeConfig,
    SimulatedAircraft,
    SimulationWorld,
    _haversine_km,
)

_RX_LAT, _RX_LON = 34.85, -82.39
_BASELINE_KM = 40.0
_TX_LON = _RX_LON + _BASELINE_KM / (111.32 * math.cos(math.radians(_RX_LAT)))


def _node(max_bistatic_range_km):
    # Beam aimed west, wide open, so only the range rule can reject.
    return NodeConfig(
        node_id="bistatic-node",
        rx_lat=_RX_LAT, rx_lon=_RX_LON, rx_alt_ft=0.0,
        tx_lat=_RX_LAT, tx_lon=_TX_LON, tx_alt_ft=0.0,
        beam_azimuth_deg=270.0, beam_width_deg=200.0,
        max_range_km=50.0,
        max_bistatic_range_km=max_bistatic_range_km,
    )


def _aircraft(bearing_deg, range_km):
    br = math.radians(bearing_deg)
    return SimulatedAircraft(
        object_id="probe",
        lat=_RX_LAT + math.degrees((range_km * math.cos(br)) / 6371.0),
        lon=_RX_LON + math.degrees(
            (range_km * math.sin(br)) / (6371.0 * math.cos(math.radians(_RX_LAT)))
        ),
        alt_km=0.0,
        vel_east=0.0, vel_north=0.0, vel_up=0.0,
        heading_deg=0.0, speed_km_s=0.2,
    )


def _bistatic_km(ac):
    r_rx = _haversine_km(_RX_LAT, _RX_LON, ac.lat, ac.lon)
    r_tx = _haversine_km(_RX_LAT, _TX_LON, ac.lat, ac.lon)
    return r_rx + r_tx - _BASELINE_KM


class TestBistaticRangeGate:
    def test_rejects_on_tx_leg_where_monostatic_accepts(self):
        """The whole point: 35 km from the RX, inside max_range_km=50, but
        directly away from the TX so the bistatic range is 70 km."""
        world = SimulationWorld()
        ac = _aircraft(270.0, 35.0)
        assert _bistatic_km(ac) > 60.0
        assert world._aircraft_in_detection_cone(ac, _node(None)) is True
        assert world._aircraft_in_detection_cone(ac, _node(60.0)) is False

    def test_accepts_inside_the_bistatic_budget(self):
        world = SimulationWorld()
        ac = _aircraft(270.0, 20.0)
        assert _bistatic_km(ac) < 60.0
        assert world._aircraft_in_detection_cone(ac, _node(60.0)) is True

    def test_same_rx_range_different_tx_leg(self):
        """Bistatic range must vary with bearing at a fixed RX distance —
        otherwise the gate has silently stayed monostatic."""
        near_tx = _aircraft(330.0, 28.0)
        away_tx = _aircraft(270.0, 28.0)
        r_near = _haversine_km(_RX_LAT, _RX_LON, near_tx.lat, near_tx.lon)
        r_away = _haversine_km(_RX_LAT, _RX_LON, away_tx.lat, away_tx.lon)
        assert abs(r_near - r_away) < 0.1, "probes must be at equal RX range"
        assert _bistatic_km(near_tx) < _bistatic_km(away_tx)

    def test_absent_key_keeps_monostatic_behaviour(self):
        """Real hardware carries only max_range_km and must be untouched."""
        world = SimulationWorld()
        mono = _node(None)
        assert mono.max_bistatic_range_km is None
        assert world._aircraft_in_detection_cone(_aircraft(270.0, 45.0), mono) is True
        assert world._aircraft_in_detection_cone(_aircraft(270.0, 55.0), mono) is False

    def test_beam_still_applies_under_the_bistatic_rule(self):
        """Range is not the only gate — an out-of-beam target stays rejected."""
        world = SimulationWorld()
        node = _node(60.0)
        node.beam_width_deg = 40.0
        assert world._aircraft_in_detection_cone(_aircraft(90.0, 10.0), node) is False


class TestEveryNodeDeclaresABistaticLimit:
    """A circle on the receiver is never a bistatic node's true footprint.

    The ring, solo and dual paths all declared max_bistatic_range_km; the
    generic region-node path did not, so those nodes alone kept gating and
    rendering as circles — visible on staging as three synthetic nodes
    reporting bistatic=None while every other node reported 60.0.
    """

    def test_metro_fleet_is_uniformly_bistatic(self):
        from retina_simulation.generator import generate_fleet

        fleet = generate_fleet(n_nodes=15, metro="gvl", n_cluster=10,
                               n_clusters=1, use_tower_api=False, seed=42)
        missing = [n["node_id"] for n in fleet
                   if n.get("max_bistatic_range_km") is None]
        assert not missing, f"nodes still monostatic: {missing}"

    def test_the_limit_matches_the_declared_range(self):
        from retina_simulation.generator import generate_fleet

        fleet = generate_fleet(n_nodes=15, metro="gvl", n_cluster=10,
                               n_clusters=1, use_tower_api=False, seed=7)
        for n in fleet:
            assert n["max_bistatic_range_km"] == n["max_range_km"], n["node_id"]
