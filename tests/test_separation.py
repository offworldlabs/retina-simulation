"""Traffic separation: spawn-pose resampling and in-trail speed modulation.

The hub-radial planner converges metro spawns on the same ~2 km core, so
without separation the fleet routinely flew pairs inside the solver's
association gates.  Spawn poses now resample away from live traffic
(best-effort), and in-flight conflicts slow the later-created aircraft
toward 70% of cruise until clear.  Anomalous aircraft are exempt — erratic
close approaches are the anomaly signature, not a bug.
"""

import random

from retina_simulation.world import (
    MetroCell,
    NodeConfig,
    SimulatedAircraft,
    SimulationWorld,
    _haversine_km,
)

_CORE_LAT, _CORE_LON = 34.85, -82.39


def _world():
    world = SimulationWorld()
    world.add_node(NodeConfig(node_id="anchor"))
    world.metro_cells = [MetroCell(core_lat=_CORE_LAT, core_lon=_CORE_LON, radius_km=70.0)]
    world.frac_metro_traffic = 1.0
    return world


def _plane(oid, lat, lon, alt_km=8.0, speed=0.2, created_at=0.0, **kw):
    return SimulatedAircraft(
        object_id=oid,
        lat=lat,
        lon=lon,
        alt_km=alt_km,
        vel_east=0.0,
        vel_north=speed,
        vel_up=0.0,
        heading_deg=0.0,
        speed_km_s=speed,
        base_speed_km_s=speed,
        created_at=created_at,
        waypoints=[(lat, lon), (lat + 2.0, lon)],
        waypoint_idx=1,
        **kw,
    )


class TestNearestTraffic:
    def test_empty_world_is_unconstrained(self):
        assert _world()._nearest_traffic_km(_CORE_LAT, _CORE_LON) == float("inf")

    def test_reports_closest_of_several(self):
        world = _world()
        world.aircraft = [
            _plane("a", _CORE_LAT + 0.5, _CORE_LON),
            _plane("b", _CORE_LAT + 0.1, _CORE_LON),
        ]
        d = world._nearest_traffic_km(_CORE_LAT, _CORE_LON)
        assert abs(d - _haversine_km(_CORE_LAT, _CORE_LON, _CORE_LAT + 0.1, _CORE_LON)) < 1e-6


class TestSpawnSeparation:
    def test_spawns_avoid_live_traffic(self):
        """With a 70 km cell and one blocker parked on the core, resampled
        spawns should clear min_separation_km essentially always — the pose
        space is enormous relative to one 5 km bubble."""
        random.seed(42)
        world = _world()
        world.aircraft = [_plane("blocker", _CORE_LAT, _CORE_LON)]
        clear = 0
        for _ in range(30):
            ac = world._spawn_aircraft(mode="adsb")
            if _haversine_km(ac.lat, ac.lon, _CORE_LAT, _CORE_LON) >= world.min_separation_km:
                clear += 1
        assert clear >= 28  # best-effort: allow the odd saturated roll

    def test_best_effort_never_blocks_spawn(self):
        """A blocker on every candidate pose still yields an aircraft —
        separation degrades, spawning never deadlocks (step() spawns in a
        while-loop up to min_aircraft)."""
        random.seed(7)
        world = _world()
        world.metro_cells[0].radius_km = 1.0  # pose space smaller than the bubble
        world.aircraft = [_plane("blocker", _CORE_LAT, _CORE_LON)]
        ac = world._spawn_aircraft(mode="adsb")
        assert ac is not None

    def test_spawned_aircraft_carry_cruise_speed(self):
        random.seed(3)
        ac = _world()._spawn_aircraft(mode="adsb")
        assert ac.base_speed_km_s == ac.speed_km_s > 0


class TestInTrailModulation:
    def test_trailing_conflict_slows_toward_seventy_percent(self):
        world = _world()
        lead = _plane("lead", _CORE_LAT, _CORE_LON, created_at=0.0)
        trail = _plane("trail", _CORE_LAT + 0.01, _CORE_LON, created_at=10.0)
        world.aircraft = [lead, trail]
        for _ in range(30):
            world._enforce_separation(1.0)
        assert trail.speed_km_s < 0.72 * trail.base_speed_km_s
        assert lead.speed_km_s > 0.95 * lead.base_speed_km_s

    def test_vertical_separation_is_no_conflict(self):
        world = _world()
        lead = _plane("lead", _CORE_LAT, _CORE_LON, alt_km=6.0, created_at=0.0)
        trail = _plane("trail", _CORE_LAT + 0.01, _CORE_LON, alt_km=9.0, created_at=10.0)
        world.aircraft = [lead, trail]
        world._enforce_separation(1.0)
        assert trail.speed_km_s == trail.base_speed_km_s

    def test_recovers_to_cruise_when_clear(self):
        world = _world()
        trail = _plane("trail", _CORE_LAT, _CORE_LON, created_at=10.0)
        trail.speed_km_s = 0.7 * trail.base_speed_km_s
        world.aircraft = [trail]
        for _ in range(30):
            world._enforce_separation(1.0)
        assert trail.speed_km_s > 0.99 * trail.base_speed_km_s

    def test_anomalous_and_drones_exempt(self):
        world = _world()
        lead = _plane("lead", _CORE_LAT, _CORE_LON, created_at=0.0)
        anom = _plane("anom", _CORE_LAT + 0.005, _CORE_LON, created_at=5.0, is_anomalous=True, object_type="anomalous")
        drone = _plane("drone", _CORE_LAT + 0.005, _CORE_LON, created_at=6.0, object_type="drone")
        world.aircraft = [lead, anom, drone]
        world._enforce_separation(1.0)
        assert anom.speed_km_s == anom.base_speed_km_s
        assert drone.speed_km_s == drone.base_speed_km_s

    def test_step_reduces_close_pairs_over_time(self):
        """End-to-end: a running world holds materially fewer sub-5 km
        same-level pairs than the pre-separation planner produced."""
        random.seed(1234)
        world = _world()
        world.min_aircraft, world.max_aircraft = 20, 25
        for _ in range(600):
            world.step(1.0, mode="adsb")
        flow = [ac for ac in world.aircraft if not ac.is_anomalous and ac.object_type == "aircraft"]
        conflicts = sum(
            1
            for i, a in enumerate(flow)
            for b in flow[i + 1 :]
            if abs(a.alt_km - b.alt_km) < world.min_vertical_sep_km
            and _haversine_km(a.lat, a.lon, b.lat, b.lon) < world.min_separation_km
        )
        # Not zero — crossing flows transit each other's bubbles while the
        # modulation sequences them — but stacking is gone.
        assert conflicts <= 2
