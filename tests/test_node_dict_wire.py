"""Wire contract for beam_azimuth_deg in serialized node dicts.

Broadside nodes (metro/solo) drop the key entirely so the backend solver's
`float(node_cfg["beam_azimuth_deg"])` is never reached with a null. Ring nodes
carry an explicit float aim.
"""

import pytest

from retina_simulation.generator import generate_fleet, GeneratedNodeConfig, _node_dict

pytest.importorskip("shapely")


def _fleet():
    return generate_fleet(
        n_nodes=60, seed=42, use_tower_api=False, n_cluster=8, n_clusters=5,
    )


def _split(nodes):
    ring = [n for n in nodes if n["node_id"].startswith("synth-RING")]
    non_ring = [n for n in nodes if not n["node_id"].startswith("synth-RING")]
    return ring, non_ring


class TestNodeDictWireContract:
    def test_metro_and_solo_nodes_omit_beam_azimuth_key(self):
        _, non_ring = _split(_fleet())
        assert non_ring
        for node in non_ring:
            assert "beam_azimuth_deg" not in node, (
                f"{node['node_id']} leaked a beam_azimuth_deg key"
            )

    def test_ring_nodes_carry_float_beam_azimuth(self):
        ring, _ = _split(_fleet())
        assert ring
        for node in ring:
            assert isinstance(node["beam_azimuth_deg"], float)

    def test_backend_float_cast_never_hits_none(self):
        ring, non_ring = _split(_fleet())
        for node in ring:
            assert float(node["beam_azimuth_deg"]) == node["beam_azimuth_deg"]
        for node in non_ring:
            assert node.get("beam_azimuth_deg") is None


class TestNodeDictSerializer:
    def test_none_azimuth_dropped(self):
        node = GeneratedNodeConfig(
            node_id="synth-US-0001", rx_lat=33.9, rx_lon=-84.6, rx_alt_ft=900,
            tx_lat=33.7, tx_lon=-84.3, tx_alt_ft=1600, fc_hz=195e6,
        )
        assert "beam_azimuth_deg" not in _node_dict(node)

    def test_explicit_azimuth_kept(self):
        node = GeneratedNodeConfig(
            node_id="synth-RING-0001", rx_lat=33.9, rx_lon=-84.6, rx_alt_ft=900,
            tx_lat=33.7, tx_lon=-84.3, tx_alt_ft=1600, fc_hz=195e6,
            beam_azimuth_deg=212.5,
        )
        assert _node_dict(node)["beam_azimuth_deg"] == 212.5
