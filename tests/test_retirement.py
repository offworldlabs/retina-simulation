"""Aircraft retirement must happen at the region edge, never mid-scene.

Staging measurement (2 min, 1 Hz feed sampling): every ground-truth vanish
was a lifetime expiry, and lifetimes uniform(180, 900) s expire aircraft
wherever they happen to be — including directly over the metro core, where a
dot blinking out reads as a tracking bug.  Expiry marks the aircraft and
reroutes it toward the edge (_route_out); it keeps flying until it clears
retire_edge_km.  The old 2x-lifetime hard cap fired regardless of position,
so metro-routed traffic — which rarely crosses the edge on its own — still
vanished mid-view; non-drones now get exit_grace_s of outbound flight and
only a genuinely stuck aircraft hits the backstop.  Drones keep the 2x cap:
they loop low and slow and are expected to churn.
"""

from retina_simulation.world import SimulationWorld, _haversine_km


def _world():
    w = SimulationWorld(center_lat=35.0, center_lon=-82.4)
    w.min_aircraft = 0
    w.max_aircraft = 0
    return w


def _plant(w, lat, lon, created_at=0.0, lifetime_s=100.0):
    ac = w._spawn_aircraft("adsb")
    ac.lat, ac.lon = lat, lon
    ac.created_at = created_at
    ac.lifetime_s = lifetime_s
    w.aircraft = [ac]
    return ac


class TestEdgeGatedRetirement:
    def test_unexpired_aircraft_is_kept(self):
        w = _world()
        _plant(w, 35.0, -82.4, lifetime_s=1000.0)
        w._time = 100.0
        w.step(0.0, mode="adsb")
        assert len(w.aircraft) == 1

    def test_expired_but_central_aircraft_keeps_flying(self):
        w = _world()
        _plant(w, 35.05, -82.35)  # a few km from center
        w._time = 150.0  # expired (100 s lifetime), inside the exit grace
        w.step(0.0, mode="adsb")
        assert len(w.aircraft) == 1

    def test_expired_aircraft_at_the_edge_is_retired(self):
        w = _world()
        _plant(w, 35.0, -81.5)  # ~82 km east of center — beyond retire_edge_km
        w._time = 150.0
        w.step(0.0, mode="adsb")
        assert w.aircraft == []

    def test_exit_grace_backstop_retires_a_stuck_aircraft_anywhere(self):
        w = _world()
        _plant(w, 35.0, -82.4)  # dead center
        w._time = 100.0 + w.exit_grace_s + 1.0
        w.step(0.0, mode="adsb")
        assert w.aircraft == []

    def test_expired_central_aircraft_survives_the_old_2x_cap(self):
        w = _world()
        _plant(w, 35.0, -82.4)
        w._time = 201.0  # past the old 2x cap, well inside lifetime + grace
        w.step(0.0, mode="adsb")
        assert len(w.aircraft) == 1


class TestFlyOutRetirement:
    def test_expiry_reroutes_toward_the_edge(self):
        w = _world()
        ac = _plant(w, 35.05, -82.35)  # northeast of center
        ac.waypoints = [(35.0, -82.4), (35.1, -82.3)]  # looping metro route
        ac.waypoint_idx = 0
        w._time = 150.0
        w.step(0.0, mode="adsb")

        assert ac.departing is True
        assert len(ac.waypoints) == 1
        wp_lat, wp_lon = ac.waypoints[0]
        # The single exit waypoint sits beyond the retire edge...
        assert _haversine_km(w.center_lat, w.center_lon, wp_lat, wp_lon) > w.retire_edge_km
        # ...on the bearing away from center (northeast, like the aircraft).
        assert wp_lat > w.center_lat and wp_lon > w.center_lon

    def test_reroute_fires_once(self):
        w = _world()
        ac = _plant(w, 35.05, -82.35)
        w._time = 150.0
        w.step(0.0, mode="adsb")
        first_route = list(ac.waypoints)
        w.step(1.0, mode="adsb")
        assert ac.waypoints == first_route  # not re-planned every step

    def test_an_aircraft_at_the_exact_center_departs_on_its_heading(self):
        w = _world()
        ac = _plant(w, 35.0, -82.4)  # bearing from center undefined
        ac.heading_deg = 90.0
        w._time = 150.0
        w.step(0.0, mode="adsb")
        wp_lat, wp_lon = ac.waypoints[0]
        assert wp_lon > w.center_lon  # due east, per the heading
        assert abs(wp_lat - w.center_lat) < 0.05

    def test_a_drone_keeps_the_2x_cap(self):
        w = _world()
        ac = _plant(w, 35.0, -82.4)
        ac.object_type = "drone"
        w._time = 201.0  # past 2x lifetime
        w.step(0.0, mode="adsb")
        assert w.aircraft == []
