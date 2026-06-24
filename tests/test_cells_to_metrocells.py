"""Cell-dict → MetroCell conversion in the orchestrator.

_cells_to_metrocells reads core_lat/core_lon/radius_km/ops_weight straight from
each first-class cell descriptor. Cores must survive unchanged (no centroid
re-derivation), and absent optional keys fall back to MetroCell defaults.
"""

import pytest

from retina_simulation.orchestrator import _cells_to_metrocells
from retina_simulation.generator import coverage_cells, _RING_TXS
from retina_simulation.world import MetroCell

pytest.importorskip("shapely")


class TestFromCoverageCells:
    def test_one_metrocell_per_cell(self):
        cells = coverage_cells(30, 5)
        metro = _cells_to_metrocells(cells)
        assert len(metro) == 5
        assert all(isinstance(m, MetroCell) for m in metro)

    def test_cores_passed_through_from_spec(self):
        cells = coverage_cells(30, 5)
        metro = _cells_to_metrocells(cells)
        for m, spec in zip(metro, _RING_TXS):
            assert (m.core_lat, m.core_lon) == spec[5:7]

    def test_radius_and_ops_weight_carried_over(self):
        cells = coverage_cells(30, 5)
        metro = _cells_to_metrocells(cells)
        for m, c in zip(metro, cells):
            assert m.radius_km == c["radius_km"]
            assert m.ops_weight == c["ops_weight"]


class TestDefaults:
    def test_missing_radius_defaults_to_seventy(self):
        metro = _cells_to_metrocells([{"core_lat": 40.0, "core_lon": -100.0}])
        assert metro[0].radius_km == 70.0

    def test_missing_ops_weight_defaults_to_one(self):
        metro = _cells_to_metrocells([{"core_lat": 40.0, "core_lon": -100.0}])
        assert metro[0].ops_weight == 1.0

    def test_explicit_keys_override_defaults(self):
        metro = _cells_to_metrocells([{
            "core_lat": 12.5, "core_lon": -34.5,
            "radius_km": 42.0, "ops_weight": 9.0,
        }])
        assert metro[0].core_lat == 12.5
        assert metro[0].core_lon == -34.5
        assert metro[0].radius_km == 42.0
        assert metro[0].ops_weight == 9.0


class TestEmpty:
    def test_no_cells_yields_no_metrocells(self):
        assert _cells_to_metrocells([]) == []
