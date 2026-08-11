"""Coverage-ring generation — aimed Yagi receivers encircling a metro core.

Each ring receiver shares one illuminator, aims its beam at the core (or
broadside to TX), and emits an explicit beam_azimuth_deg. The union of inward
beams must contain the core airspace.
"""

import random

import pytest

from retina_simulation import generator
from retina_simulation.generator import (
    _RING_TXS,
    _bearing_between,
    _generate_coverage_ring,
)
from retina_simulation.world import NodeConfig, SimulatedAircraft, SimulationWorld

pytest.importorskip("shapely")

_TX = _RING_TXS[0]
_TX_LAT, _TX_LON, _TX_ALT_FT, _FC_HZ, _CALLSIGN, _CORE_LAT, _CORE_LON = _TX
_TX_TUPLE = (_TX_LAT, _TX_LON, _TX_ALT_FT, _FC_HZ, _CALLSIGN)


def _ring(n=6, aim="core", seed=1):
    random.seed(seed)
    return _generate_coverage_ring(n, _CORE_LAT, _CORE_LON, _TX_TUPLE, aim=aim)


def _angular_diff(a, b):
    return abs((a - b + 180) % 360 - 180)


class TestRingShape:
    def test_returns_exactly_n_nodes(self):
        assert len(_ring(n=7)) == 7

    def test_all_nodes_share_tx_frequency_and_callsign(self):
        nodes = _ring(n=6)
        assert {n["fc_hz"] for n in nodes} == {_FC_HZ}
        assert {n["tx_callsign"] for n in nodes} == {_CALLSIGN}
        assert all(n["tx_lat"] == _TX_LAT and n["tx_lon"] == _TX_LON for n in nodes)

    def test_every_node_carries_float_beam_azimuth(self):
        for node in _ring(n=6):
            assert "beam_azimuth_deg" in node
            assert isinstance(node["beam_azimuth_deg"], float)

    def test_node_ids_use_prefix(self):
        ids = [n["node_id"] for n in _generate_coverage_ring(
            3, _CORE_LAT, _CORE_LON, _TX_TUPLE, prefix="synth-RING2")]
        assert ids == ["synth-RING2-0001", "synth-RING2-0002", "synth-RING2-0003"]


class TestRingAim:
    def test_core_aim_points_each_node_at_core(self):
        for node in _ring(n=8, aim="core"):
            bearing_to_core = _bearing_between(
                node["rx_lat"], node["rx_lon"], _CORE_LAT, _CORE_LON)
            assert _angular_diff(node["beam_azimuth_deg"], bearing_to_core) < 1.0

    def test_broadside_aim_is_baseline_plus_ninety(self):
        for node in _ring(n=8, aim="broadside"):
            expected = (_bearing_between(
                node["rx_lat"], node["rx_lon"], _TX_LAT, _TX_LON) + 90.0) % 360.0
            assert node["beam_azimuth_deg"] == round(expected, 2)


class TestCoreInsideEveryBeam:
    def test_core_airspace_covered_by_all_ring_beams(self):
        nodes = _ring(n=6, aim="core")
        world = SimulationWorld()
        aircraft = SimulatedAircraft(
            object_id="core-target", lat=_CORE_LAT, lon=_CORE_LON, alt_km=8.0,
            vel_east=0.0, vel_north=0.0, vel_up=0.0,
            heading_deg=0.0, speed_km_s=0.2,
        )
        for node in nodes:
            cfg = NodeConfig(
                node_id=node["node_id"],
                rx_lat=node["rx_lat"], rx_lon=node["rx_lon"],
                tx_lat=_TX_LAT, tx_lon=_TX_LON, fc_hz=_FC_HZ,
                beam_azimuth_deg=node["beam_azimuth_deg"],
                beam_width_deg=node["beam_width_deg"],
                max_range_km=node["max_range_km"],
            )
            world.add_node(cfg)
            assert world._aircraft_in_detection_cone(aircraft, world.nodes[node["node_id"]]), (
                f"core outside beam of {node['node_id']} "
                f"(az={node['beam_azimuth_deg']})"
            )


class TestRingWaterRejection:
    def test_ring_nodes_avoid_water(self, monkeypatch):
        real_is_water = generator._is_on_water

        def _wedge_is_water(lat, lon):
            if real_is_water(lat, lon):
                return True
            return _bearing_between(_CORE_LAT, _CORE_LON, lat, lon) < 90.0

        monkeypatch.setattr(generator, "_is_on_water", _wedge_is_water)
        nodes = _ring(n=8, aim="core")
        assert len(nodes) == 8
        for node in nodes:
            assert not generator._is_on_water(node["rx_lat"], node["rx_lon"]), (
                f"{node['node_id']} placed on water at "
                f"({node['rx_lat']}, {node['rx_lon']})"
            )
