"""Coverage-cell descriptors — first-class metro cells derived from the ring spec.

coverage_cells must take its core straight from the ring spec (the true airport),
never reconstruct it from receiver positions, and must agree with the node count
generate_fleet actually emits per ring. _active_rings is the shared source of
truth both paths consume.
"""

from collections import Counter

import pytest

from retina_simulation.generator import (
    _RING_TXS,
    _active_rings,
    coverage_cells,
    generate_fleet,
)

pytest.importorskip("shapely")


class TestCoverageCellsMultiRing:
    def test_returns_one_cell_per_active_ring(self):
        cells = coverage_cells(30, 5)
        assert len(cells) == 5

    def test_ring_ids_are_numbered(self):
        cells = coverage_cells(30, 5)
        assert [c["ring_id"] for c in cells] == [
            "synth-RING1", "synth-RING2", "synth-RING3", "synth-RING4", "synth-RING5",
        ]

    def test_cores_come_from_spec_not_node_positions(self):
        cells = coverage_cells(30, 5)
        for cell, spec in zip(cells, _RING_TXS):
            assert (cell["core_lat"], cell["core_lon"]) == spec[5:7]

    def test_ops_weight_sums_to_total_budget(self):
        cells = coverage_cells(30, 5)
        assert sum(c["ops_weight"] for c in cells) == 30.0

    def test_ops_weight_split_evenly_when_divisible(self):
        cells = coverage_cells(30, 5)
        assert all(c["ops_weight"] == 6.0 for c in cells)

    def test_default_radius_km(self):
        cells = coverage_cells(30, 5)
        assert all(c["radius_km"] == 70.0 for c in cells)

    def test_illuminator_is_spec_callsign(self):
        cells = coverage_cells(30, 5)
        for cell, spec in zip(cells, _RING_TXS):
            assert cell["illuminator"] == spec[4]


class TestCoverageCellsSingleRing:
    def test_single_ring_uses_unnumbered_id(self):
        cells = coverage_cells(8, 1)
        assert len(cells) == 1
        assert cells[0]["ring_id"] == "synth-RING"

    def test_single_ring_core_from_first_spec(self):
        cells = coverage_cells(8, 1)
        assert (cells[0]["core_lat"], cells[0]["core_lon"]) == _RING_TXS[0][5:7]

    def test_single_ring_ops_weight_is_full_budget(self):
        cells = coverage_cells(8, 1)
        assert cells[0]["ops_weight"] == 8.0


class TestUnevenSplit:
    def test_remainder_goes_to_leading_rings(self):
        cells = coverage_cells(7, 3)
        assert [c["ops_weight"] for c in cells] == [3.0, 2.0, 2.0]

    def test_more_clusters_than_specs_caps_at_spec_count(self):
        cells = coverage_cells(30, 99)
        assert len(cells) == len(_RING_TXS)

    def test_fewer_receivers_than_clusters_drops_empty_rings(self):
        cells = coverage_cells(2, 5)
        assert len(cells) == 2
        assert all(c["ops_weight"] == 1.0 for c in cells)


class TestEmptyBudget:
    def test_zero_receivers_yields_no_cells(self):
        assert coverage_cells(0, 5) == []

    def test_zero_clusters_yields_no_cells(self):
        assert coverage_cells(30, 0) == []

    def test_negative_receivers_yields_no_cells(self):
        assert coverage_cells(-5, 3) == []


class TestInjectableSpec:
    def test_custom_spec_core_and_illuminator(self):
        spec = [(40.0, -100.0, 1000, 100e6, "X-RING", 40.5, -100.5)]
        cells = coverage_cells(6, 1, ring_spec=spec)
        assert len(cells) == 1
        assert (cells[0]["core_lat"], cells[0]["core_lon"]) == (40.5, -100.5)
        assert cells[0]["illuminator"] == "X-RING"

    def test_custom_traffic_radius(self):
        cells = coverage_cells(8, 1, traffic_radius_km=120.0)
        assert cells[0]["radius_km"] == 120.0


class TestActiveRingsConsistency:
    def test_active_rings_matches_coverage_cells(self):
        rings = list(_active_rings(30, 5))
        cells = coverage_cells(30, 5)
        assert [r[0] for r in rings] == [c["ring_id"] for c in cells]
        assert [float(r[2]) for r in rings] == [c["ops_weight"] for c in cells]

    def test_generated_node_counts_match_cell_ops_weight(self):
        nodes = generate_fleet(
            n_nodes=120, n_cluster=30, n_clusters=5,
            use_tower_api=False, seed=7,
        )
        cells = coverage_cells(30, 5)
        counts = Counter()
        for node in nodes:
            for cell in cells:
                if node["node_id"].startswith(cell["ring_id"] + "-"):
                    counts[cell["ring_id"]] += 1
                    break
        for cell in cells:
            assert counts[cell["ring_id"]] == cell["ops_weight"]

    def test_empty_active_rings_when_no_budget(self):
        assert list(_active_rings(0, 5)) == []
        assert list(_active_rings(30, 0)) == []
