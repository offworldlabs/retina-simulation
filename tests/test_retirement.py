"""Aircraft retirement must happen at the region edge, never mid-scene.

Staging measurement (2 min, 1 Hz feed sampling): every ground-truth vanish
was a lifetime expiry, and lifetimes uniform(180, 900) s expire aircraft
wherever they happen to be — including directly over the metro core, where a
dot blinking out reads as a tracking bug.  Expiry now marks the aircraft for
retirement; it keeps flying until it clears retire_edge_km (or hits the
2x-lifetime hard cap for slow/looping routes).
"""

from retina_simulation.world import SimulationWorld


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
        w._time = 150.0  # expired (100 s lifetime), below the 200 s hard cap
        w.step(0.0, mode="adsb")
        assert len(w.aircraft) == 1

    def test_expired_aircraft_at_the_edge_is_retired(self):
        w = _world()
        _plant(w, 35.0, -81.5)  # ~82 km east of center — beyond retire_edge_km
        w._time = 150.0
        w.step(0.0, mode="adsb")
        assert w.aircraft == []

    def test_double_lifetime_hard_cap_retires_anywhere(self):
        w = _world()
        _plant(w, 35.0, -82.4)  # dead center
        w._time = 201.0  # past 2x lifetime
        w.step(0.0, mode="adsb")
        assert w.aircraft == []
