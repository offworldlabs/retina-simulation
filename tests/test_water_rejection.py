"""Receiver water-rejection during fleet generation (shapely + Natural Earth).

Both the real RX position and the frontend display-fuzzed position must be on
land, since nodes are drawn at the fuzzed location (see _node_display_fuzz).
"""

import pytest

from retina_simulation import generator
from retina_simulation.generator import _is_on_water, _node_display_fuzz, generate_fleet

pytest.importorskip("shapely")


@pytest.fixture(autouse=True)
def _require_shapely_path():
    # Fail loudly if shapely/data is missing rather than silently testing the
    # US-only bounding-box fallback.
    assert generator._get_land_check() is not None, "shapely land-check unavailable"


class TestIsOnWater:
    @pytest.mark.parametrize("lat,lon", [
        (37.50, -122.70),  # Pacific off Pacifica — the synth-US-0008 ocean bug
        (-33.85, 151.25),  # Sydney Harbour
        (50.5, 0.0),       # English Channel
        (25.7, -79.5),     # Atlantic off Miami
        (43.5, -87.0),     # mid Lake Michigan (inland lake)
    ])
    def test_water_points(self, lat, lon):
        assert _is_on_water(lat, lon)

    @pytest.mark.parametrize("lat,lon", [
        (37.60, -122.42),  # SF peninsula
        (37.46, -122.43),  # Half Moon Bay
        (33.75, -84.39),   # Atlanta
        (41.88, -87.63),   # Chicago lakeshore
        (-33.87, 151.21),  # Sydney CBD
    ])
    def test_land_points(self, lat, lon):
        assert not _is_on_water(lat, lon)


class TestFleetOffWater:
    @pytest.mark.parametrize("regions,clusters", [(["us"], 5), (["eu", "au"], 0)])
    def test_real_and_display_positions_on_land(self, regions, clusters):
        nodes = generate_fleet(
            n_nodes=80, regions=regions, seed=11, use_tower_api=False,
            n_cluster=8 if clusters else 0, n_clusters=clusters,
        )
        assert nodes
        bad = []
        for n in nodes:
            if _is_on_water(n["rx_lat"], n["rx_lon"]):
                bad.append((n["node_id"], "real", n["rx_lat"], n["rx_lon"]))
            dlat, dlon = _node_display_fuzz(n["node_id"])
            if _is_on_water(n["rx_lat"] + dlat, n["rx_lon"] + dlon):
                bad.append((n["node_id"], "display", n["rx_lat"], n["rx_lon"]))
        assert not bad, f"{len(bad)} node positions on water: {bad[:5]}"
