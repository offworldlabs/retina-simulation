"""Hub-radial spawn routing through metro cells.

With no cells the world keeps the legacy node-anchored spawn. With a cell at
frac_metro_traffic=1.0 every spawn flies an arrival/departure/overflight radial
through the core. The object-type rolls must be untouched by the refactor.
"""

import random
from collections import Counter

from retina_simulation.world import (
    SimulationWorld, NodeConfig, MetroCell, _haversine_km,
)

_CORE_LAT, _CORE_LON = 32.8968, -97.0380


def _world_with_cell(radius_km=70.0):
    world = SimulationWorld()
    world.add_node(NodeConfig(node_id="anchor"))
    world.metro_cells = [MetroCell(
        core_lat=_CORE_LAT, core_lon=_CORE_LON, radius_km=radius_km)]
    world.frac_metro_traffic = 1.0
    return world


class TestFallbackSpawn:
    def test_no_cells_uses_node_anchored_route(self):
        random.seed(0)
        world = SimulationWorld()
        world.add_node(NodeConfig(node_id="anchor"))
        lat, lon, route = world._choose_spawn_pose()
        assert len(route) >= 2
        assert route[0] == (lat, lon)


class TestRadialRoutes:
    def test_arrivals_end_at_core(self):
        random.seed(11)
        world = _world_with_cell()
        cell = world.metro_cells[0]
        for _ in range(200):
            _, _, route = world._radial_pose(cell, "arrival")
            end_to_core = _haversine_km(
                route[-1][0], route[-1][1], _CORE_LAT, _CORE_LON)
            assert end_to_core < 12.0

    def test_departures_start_at_core(self):
        random.seed(12)
        world = _world_with_cell()
        cell = world.metro_cells[0]
        for _ in range(200):
            _, _, route = world._radial_pose(cell, "departure")
            start_to_core = _haversine_km(
                route[0][0], route[0][1], _CORE_LAT, _CORE_LON)
            assert start_to_core < 12.0

    def test_overflights_cross_the_cell_edge_to_edge(self):
        random.seed(13)
        world = _world_with_cell(radius_km=70.0)
        cell = world.metro_cells[0]
        for _ in range(200):
            _, _, route = world._radial_pose(cell, "overflight")
            d_entry = _haversine_km(route[0][0], route[0][1], _CORE_LAT, _CORE_LON)
            d_exit = _haversine_km(route[-1][0], route[-1][1], _CORE_LAT, _CORE_LON)
            assert abs(d_entry - 70.0) < 1.0
            assert abs(d_exit - 70.0) < 1.0


class TestObjectTypeDistributionPreserved:
    def test_type_fractions_match_after_radial_refactor(self):
        random.seed(123)
        world = _world_with_cell()
        n_spawn = 2000
        counts = Counter()
        for _ in range(n_spawn):
            ac = world._spawn_aircraft(mode="adsb")
            if ac.is_anomalous:
                counts["anomalous"] += 1
            elif ac.object_type == "drone":
                counts["drone"] += 1
            elif ac.has_adsb:
                counts["commercial"] += 1
            else:
                counts["dark"] += 1

        assert abs(counts["anomalous"] / n_spawn - world.frac_anomalous) < 0.03
        assert abs(counts["drone"] / n_spawn - world.frac_drone) < 0.03
        assert abs(counts["dark"] / n_spawn - world.frac_dark) < 0.04
        commercial_frac = 1.0 - world.frac_anomalous - world.frac_drone - world.frac_dark
        assert abs(counts["commercial"] / n_spawn - commercial_frac) < 0.04


class TestTrainingBatchWithCells:
    def test_labeled_records_still_produced(self):
        random.seed(7)
        world = _world_with_cell()
        world.min_aircraft = 8
        records = world.generate_training_batch(n_frames=20, dt=0.5, mode="adsb")
        assert records
        assert any(r["ground_truth"] for r in records)
        assert any("adsb" in r for r in records)
