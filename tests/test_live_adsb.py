"""Live ADS-B seeding: real feed aircraft in the simulated world.

Feed rows (adsb.retina.fm, adsb.lol shape) become world aircraft the synthetic
nodes echo.  Two independent knobs: frac_live_dark casts a stable share of the
live aircraft as dark (mirrored without their transponder), and min/max_aircraft
+ frac_dark keep governing the synthetic spawns layered on top, untouched by
how many live aircraft the feed happens to report.
"""

import io
import json
import math

from retina_simulation.live_adsb import LiveAdsbClient, parse_point_response
from retina_simulation.orchestrator import build_adsb_push_payload, build_ground_truth_payload
from retina_simulation.world import NodeConfig, SimulationWorld


def _row(hex_code="ab1388", **overrides) -> dict:
    base = {
        "hex": hex_code,
        "flight": "N81227",
        "lat": 34.88,
        "lon": -82.40,
        "alt_baro": 11500.0,
        "gs": 126.0,
        "track": 80.0,
        "baro_rate": 0.0,
        "captured_at": 1000.0,
    }
    base.update(overrides)
    return base


_FT_PER_KM = 1.0 / 0.0003048  # feed altitudes are feet; the world thinks in km


def _world(**knobs) -> SimulationWorld:
    w = SimulationWorld(center_lat=34.85, center_lon=-82.39)
    w.frac_anomalous = 0.0
    w.frac_drone = 0.0
    w.frac_dark = 0.0
    w.min_aircraft = 0
    w.max_aircraft = 0
    for k, v in knobs.items():
        setattr(w, k, v)
    return w


# ── parse_point_response ─────────────────────────────────────────────────────


class TestParsePointResponse:
    def test_normalises_tar1090_rows(self):
        data = {
            "ac": [
                {
                    "hex": "AB1388",
                    "flight": "N81227  ",
                    "lat": 34.88,
                    "lon": -82.4,
                    "alt_baro": 11500,
                    "gs": 126.0,
                    "track": 80.0,
                    "baro_rate": -192,
                    "seen_pos": 11.0,
                },
            ]
        }
        rows = parse_point_response(data, fetched_at=2000.0)
        assert len(rows) == 1
        r = rows[0]
        assert r["hex"] == "ab1388"
        assert r["flight"] == "N81227"
        assert r["captured_at"] == 2000.0 - 11.0
        assert r["baro_rate"] == -192
        assert r["on_ground"] is False

    def test_drops_positionless_and_altitudeless_rows(self):
        data = {
            "ac": [
                {"hex": "bbbbbb", "alt_baro": 3000},
                {"hex": "", "lat": 34.0, "lon": -82.0, "alt_baro": 3000},
                {"hex": "dddddd", "lat": 34.0, "lon": -82.0},  # no alt_baro at all
                {"hex": "eeeeee", "lat": 34.0, "lon": -82.0, "alt_baro": None},
                {"hex": "cccccc", "lat": 34.0, "lon": -82.0, "alt_baro": 3000},
            ]
        }
        assert [r["hex"] for r in parse_point_response(data, 0.0)] == ["cccccc"]

    def test_ground_rows_are_emitted_flagged(self):
        # adsb.lol spells it "ground"; adsb.retina.fm serves a numeric 0 (and a
        # negative altitude is no more airborne than 0 is).
        data = {
            "ac": [
                {"hex": "aaaaaa", "lat": 34.0, "lon": -82.0, "alt_baro": "ground", "gs": 5},
                {"hex": "bbbbbb", "lat": 34.0, "lon": -82.0, "alt_baro": 0},
                {"hex": "cccccc", "lat": 34.0, "lon": -82.0, "alt_baro": -75},
                {"hex": "dddddd", "lat": 34.0, "lon": -82.0, "alt_baro": 3000},
            ]
        }
        rows = parse_point_response(data, 0.0)
        assert [(r["hex"], r["on_ground"], r["alt_baro"]) for r in rows] == [
            ("aaaaaa", True, 0.0),
            ("bbbbbb", True, 0.0),
            ("cccccc", True, 0.0),
            ("dddddd", False, 3000.0),
        ]


class TestLiveAdsbClient:
    def test_builds_point_url_and_serves_last_good_on_failure(self, monkeypatch):
        calls = []

        class _Resp(io.BytesIO):
            headers = {}

            def __enter__(self):
                return self

            def __exit__(self, *a):
                return False

        def fake_urlopen(req, timeout=0):
            calls.append(req.full_url)
            if len(calls) == 1:
                return _Resp(json.dumps({"ac": [{"hex": "ab1388", "lat": 1.0, "lon": 2.0, "alt_baro": 100}]}).encode())
            raise OSError("boom")

        monkeypatch.setattr("retina_simulation.live_adsb.urllib.request.urlopen", fake_urlopen)
        c = LiveAdsbClient(
            [{"name": "Greenville", "lat": 34.852, "lon": -82.394, "radius_nm": 60}], base_url="https://adsb.retina.fm/"
        )
        first = c.fetch_all()
        assert calls[0] == "https://adsb.retina.fm/v2/point/34.852/-82.394/60"
        assert [r["hex"] for r in first] == ["ab1388"]
        assert c.last_status["Greenville"] is True
        second = c.fetch_all()  # network fails → last good result stands in
        assert [r["hex"] for r in second] == ["ab1388"]
        assert c.last_status["Greenville"] is False


# ── ingest ───────────────────────────────────────────────────────────────────


class TestIngest:
    def test_creates_live_aircraft_with_real_kinematics(self):
        w = _world()
        stats = w.ingest_live_aircraft([_row()], now_wall=1000.0)
        assert stats == {"created": 1, "updated": 0, "landed": 0, "alt_rejected": 0, "live": 1}
        ac = w.live_aircraft["ab1388"]
        assert ac.source == "live"
        assert ac.object_id == "live-ab1388"
        assert ac.live_hex == "ab1388"
        assert ac.has_adsb is True and ac.adsb_hex == "ab1388"
        assert ac.adsb_callsign == "N81227"
        assert math.isclose(ac.speed_km_s, 126.0 * 1.852 / 3600.0)
        assert math.isclose(ac.alt_km, 11500.0 * 0.0003048)
        assert ac.heading_deg == 80.0
        assert ac in w.aircraft

    def test_extrapolates_a_stale_fix_to_now(self):
        w = _world()
        # Due north at 360 kt = 0.1852 km/s; a 10 s old fix should sit ~1.85 km north.
        w.ingest_live_aircraft([_row(track=0.0, gs=360.0, captured_at=990.0)], now_wall=1000.0)
        ac = w.live_aircraft["ab1388"]
        dlat_km = (ac.lat - 34.88) * 111.19
        assert 1.7 < dlat_km < 2.0

    def test_updates_in_place_and_keeps_id(self):
        w = _world()
        w.ingest_live_aircraft([_row()], now_wall=1000.0)
        first = w.live_aircraft["ab1388"]
        stats = w.ingest_live_aircraft([_row(lat=34.9, gs=200.0)], now_wall=1005.0)
        assert stats == {"created": 0, "updated": 1, "landed": 0, "alt_rejected": 0, "live": 1}
        assert w.live_aircraft["ab1388"] is first
        assert first.lat > 34.88
        assert len([a for a in w.aircraft if a.source == "live"]) == 1

    def test_expires_when_feed_goes_quiet_but_coasts_first(self):
        w = _world(live_stale_s=30.0)
        w.ingest_live_aircraft([_row(track=90.0, gs=360.0)], now_wall=1000.0)
        ac = w.live_aircraft["ab1388"]
        lon0 = ac.lon
        for _ in range(10):
            w.step(1.0, mode="adsb")
        assert "ab1388" in w.live_aircraft
        assert ac.lon > lon0  # dead-reckoned east while the feed was silent
        for _ in range(25):
            w.step(1.0, mode="adsb")
        assert "ab1388" not in w.live_aircraft
        assert all(a.source != "live" for a in w.aircraft)

    def test_ground_row_retires_the_aircraft_at_once(self):
        w = _world()
        w.ingest_live_aircraft([_row()], now_wall=1000.0)
        stats = w.ingest_live_aircraft([_row(on_ground=True, alt_baro=0.0)], now_wall=1000.0)
        assert stats["landed"] == 1 and stats["live"] == 0
        assert "ab1388" not in w.live_aircraft
        assert all(a.source != "live" for a in w.aircraft)

    def test_ground_row_never_creates_an_unknown_aircraft(self):
        w = _world()
        stats = w.ingest_live_aircraft([_row("ffffff", on_ground=True, alt_baro=0.0)], now_wall=1000.0)
        assert stats == {"created": 0, "updated": 0, "landed": 0, "alt_rejected": 0, "live": 0}
        assert not w.live_aircraft and not w.aircraft

    def test_a_fix_older_than_the_stale_window_is_ignored(self):
        w = _world(live_stale_s=60.0)
        stats = w.ingest_live_aircraft([_row(captured_at=1000.0)], now_wall=1061.0)
        assert stats["created"] == 0 and not w.live_aircraft

        w.ingest_live_aircraft([_row()], now_wall=1000.0)
        ac = w.live_aircraft["ab1388"]
        seen_s, lat0 = ac.live_seen_s, ac.lat
        stats = w.ingest_live_aircraft([_row(lat=40.0, captured_at=940.0)], now_wall=1001.0)
        assert stats["updated"] == 0
        assert ac.lat == lat0 and ac.live_seen_s == seen_s

    def test_expiry_is_keyed_on_the_fix_age_not_the_poll(self):
        # A 50 s old fix in a 60 s window has 10 s of life left, however fresh
        # the poll that carried it was.
        w = _world(live_stale_s=60.0)
        w.ingest_live_aircraft([_row(captured_at=950.0)], now_wall=1000.0)
        ac = w.live_aircraft["ab1388"]
        assert math.isclose(ac.live_seen_s, w._time - 50.0)
        for _ in range(9):
            w.step(1.0, mode="adsb")
        assert "ab1388" in w.live_aircraft
        for _ in range(3):
            w.step(1.0, mode="adsb")
        assert "ab1388" not in w.live_aircraft

    def test_a_fresh_fix_keeps_an_aircraft_alive(self):
        w = _world(live_stale_s=60.0)
        w.ingest_live_aircraft([_row(captured_at=950.0)], now_wall=1000.0)
        for _ in range(9):
            w.step(1.0, mode="adsb")
        w.ingest_live_aircraft([_row(captured_at=1009.0)], now_wall=1009.0)
        for _ in range(12):
            w.step(1.0, mode="adsb")
        assert "ab1388" in w.live_aircraft

    def test_clear_removes_every_live_aircraft(self):
        w = _world(min_aircraft=3)
        w.step(1.0, mode="adsb")
        w.ingest_live_aircraft([_row("aaaaaa"), _row("bbbbbb")], now_wall=1000.0)
        assert len(w.aircraft) == 5
        assert w.clear_live_aircraft() == 2
        assert len(w.aircraft) == 3 and w.synthetic_count() == 3


# ── altitude jump guard ──────────────────────────────────────────────────────


class TestAltitudeJumpGuard:
    """The feed serves another aircraft's cruise level in a low GA row.

    Observed live on N6389R: alt_baro +23k…+33k ft over truth, flipping between
    polls, so the pinned altitude the server solved at was kilometres wrong.
    """

    @staticmethod
    def _poll(w: SimulationWorld, wall: float, alt_km: float, **overrides) -> dict:
        return w.ingest_live_aircraft(
            [_row(alt_baro=alt_km * _FT_PER_KM, baro_rate=0.0, captured_at=wall, **overrides)], now_wall=wall
        )

    def test_a_single_bad_reading_is_rejected_and_the_next_good_one_accepted(self):
        w = _world()
        self._poll(w, 1000.0, 1.2)
        ac = w.live_aircraft["ab1388"]
        w.step(5.0, mode="adsb")
        assert self._poll(w, 1005.0, 1.2)["alt_rejected"] == 0

        w.step(5.0, mode="adsb")
        stats = self._poll(w, 1010.0, 11.2, lat=34.95)
        assert stats["alt_rejected"] == 1
        assert math.isclose(ac.alt_km, 1.2, rel_tol=1e-6)
        assert ac.live_alt_reject_s is not None
        assert math.isclose(ac.lat, 34.95, abs_tol=1e-6)  # the rest of the row still applies

        w.step(5.0, mode="adsb")
        assert self._poll(w, 1015.0, 1.2)["alt_rejected"] == 0
        assert math.isclose(ac.alt_km, 1.2, rel_tol=1e-6)
        assert ac.live_alt_reject_s is None

    def test_a_disagreement_that_lasts_a_minute_is_adopted(self):
        w = _world()
        self._poll(w, 1000.0, 1.2)
        ac = w.live_aircraft["ab1388"]
        wall = 1000.0
        for _ in range(12):  # 60 s of world time, all rejected
            w.step(5.0, mode="adsb")
            wall += 5.0
            self._poll(w, wall, 11.2)
        assert math.isclose(ac.alt_km, 1.2, rel_tol=1e-6)

        w.step(5.0, mode="adsb")
        wall += 5.0
        assert self._poll(w, wall, 11.2)["alt_rejected"] == 0
        assert math.isclose(ac.alt_km, 11.2, rel_tol=1e-6)
        assert ac.live_alt_reject_s is None

    def test_a_flip_flopping_feed_is_never_adopted(self):
        # N6389R replayed (2026-09-10): the feed disagreed with the coasted
        # altitude for 97 s straight, but across FOUR different corrupt values.
        # A run that does not agree with itself is the feed serving other
        # aircraft's levels, so nothing in it may ever be adopted — however
        # long it lasts — and the returning truth must be taken at once.
        w = _world()
        self._poll(w, 1000.0, 1.2)
        ac = w.live_aircraft["ab1388"]
        wall = 1000.0
        rejected = 0
        for alt_km, polls in ((11.2, 5), (3.3, 2), (11.1, 10), (9.2, 3)):  # 100 s, four values
            for _ in range(polls):
                w.step(5.0, mode="adsb")
                wall += 5.0
                rejected += self._poll(w, wall, alt_km)["alt_rejected"]
                assert math.isclose(ac.alt_km, 1.2, rel_tol=1e-6)  # never left the coasted truth
        assert rejected == 20  # every reading counted, none adopted

        w.step(5.0, mode="adsb")
        wall += 5.0
        assert self._poll(w, wall, 1.2)["alt_rejected"] == 0  # truth back, accepted immediately
        assert math.isclose(ac.alt_km, 1.2, rel_tol=1e-6)
        assert ac.live_alt_reject_s is None

    def test_a_corrupt_spawn_altitude_is_recovered_after_a_minute(self):
        # The adopt path exists for a wrong HELD altitude: the aircraft was
        # created from a corrupt row, so the feed's consistent truth is the
        # side that keeps getting rejected.
        w = _world()
        self._poll(w, 1000.0, 11.2)
        ac = w.live_aircraft["ab1388"]
        assert math.isclose(ac.alt_km, 11.2, rel_tol=1e-6)
        wall = 1000.0
        for _ in range(12):  # 60 s of consistent truth, all rejected
            w.step(5.0, mode="adsb")
            wall += 5.0
            assert self._poll(w, wall, 1.2)["alt_rejected"] == 1
        assert math.isclose(ac.alt_km, 11.2, rel_tol=1e-6)  # not adopted early

        w.step(5.0, mode="adsb")
        wall += 5.0
        assert self._poll(w, wall, 1.2)["alt_rejected"] == 0
        assert math.isclose(ac.alt_km, 1.2, rel_tol=1e-6)
        assert ac.live_alt_reject_s is None

    def test_a_normal_descent_is_never_rejected(self):
        w = _world()
        alt = 3.0
        self._poll(w, 1000.0, alt)
        ac = w.live_aircraft["ab1388"]
        wall = 1000.0
        for _ in range(10):  # 0.1 km per 5 s poll ≈ 1,200 ft/min
            w.step(5.0, mode="adsb")
            wall += 5.0
            alt -= 0.1
            assert self._poll(w, wall, alt)["alt_rejected"] == 0
        assert math.isclose(ac.alt_km, 2.0, rel_tol=1e-6)


# ── independence from the synthetic knobs ────────────────────────────────────


class TestSyntheticIndependence:
    def test_live_aircraft_do_not_count_toward_min_max_aircraft(self):
        w = _world(min_aircraft=5, max_aircraft=5)
        w.ingest_live_aircraft([_row(f"{i:06x}") for i in range(20)], now_wall=1000.0)
        w.step(1.0, mode="adsb")
        assert w.synthetic_count() == 5  # spawned up to min despite 20 live aircraft
        assert len(w.aircraft) == 25

    def test_live_aircraft_never_route_out_or_retire(self):
        w = _world()
        w.ingest_live_aircraft([_row()], now_wall=1000.0)
        ac = w.live_aircraft["ab1388"]
        for _ in range(50):
            ac.live_seen_s = w._time  # feed keeps reporting it
            w.step(60.0, mode="adsb")  # 50 minutes, far past any lifetime
        assert ac in w.aircraft
        assert ac.departing is False

    def test_separation_never_slows_a_live_aircraft(self):
        w = _world()
        w.ingest_live_aircraft(
            [_row("aaaaaa", lat=34.88, lon=-82.40), _row("bbbbbb", lat=34.881, lon=-82.40)], now_wall=1000.0
        )
        speeds = {h: ac.speed_km_s for h, ac in w.live_aircraft.items()}
        for _ in range(5):
            for ac in w.live_aircraft.values():
                ac.live_seen_s = w._time
            w.step(1.0, mode="adsb")
        for h, ac in w.live_aircraft.items():
            assert math.isclose(ac.speed_km_s, speeds[h])


# ── dark cast ────────────────────────────────────────────────────────────────


class TestLiveDarkCast:
    def test_zero_and_one_are_absolute(self):
        w = _world(frac_live_dark=0.0)
        assert not any(w.live_role_is_dark(f"{i:06x}") for i in range(200))
        w.frac_live_dark = 1.0
        assert all(w.live_role_is_dark(f"{i:06x}") for i in range(200))

    def test_share_is_roughly_the_fraction_and_stable(self):
        w = _world(frac_live_dark=0.3)
        hexes = [f"{i * 7919:06x}" for i in range(1000)]
        dark = [h for h in hexes if w.live_role_is_dark(h)]
        assert 240 < len(dark) < 360
        assert dark == [h for h in hexes if w.live_role_is_dark(h)]  # deterministic

    def test_raising_the_knob_only_adds_dark_aircraft(self):
        w = _world(frac_live_dark=0.2)
        hexes = [f"{i * 7919:06x}" for i in range(500)]
        low = {h for h in hexes if w.live_role_is_dark(h)}
        w.frac_live_dark = 0.5
        high = {h for h in hexes if w.live_role_is_dark(h)}
        assert low <= high

    def test_dark_cast_strips_transponder_but_keeps_hex_for_recast(self):
        w = _world(frac_live_dark=1.0)
        w.ingest_live_aircraft([_row()], now_wall=1000.0)
        ac = w.live_aircraft["ab1388"]
        assert ac.has_adsb is False and ac.adsb_hex is None
        assert ac.live_hex == "ab1388"
        # Slider back to 0 → transponder restored on the aircraft already flying.
        assert w.set_frac_live_dark(0.0) == 0
        assert ac.has_adsb is True and ac.adsb_hex == "ab1388"

    def test_set_frac_recasts_in_flight_aircraft(self):
        w = _world(frac_live_dark=0.0)
        w.ingest_live_aircraft([_row(f"{i * 7919:06x}") for i in range(300)], now_wall=1000.0)
        assert all(ac.has_adsb for ac in w.live_aircraft.values())
        n_dark = w.set_frac_live_dark(0.5)
        assert 120 < n_dark < 180
        assert n_dark == sum(1 for ac in w.live_aircraft.values() if not ac.has_adsb)

    def test_dark_live_aircraft_is_untagged_in_frames_and_pushes(self):
        w = _world(frac_live_dark=1.0)
        node = NodeConfig(
            node_id="n1",
            rx_lat=34.85,
            rx_lon=-82.39,
            tx_lat=34.80,
            tx_lon=-82.30,
            beam_width_deg=360.0,
            max_range_km=200.0,
        )
        w.add_node(node)
        w.ingest_live_aircraft([_row()], now_wall=1000.0)
        # Frames: many draws so the p_miss roll cannot hide a tagged echo.
        tagged = 0
        for _ in range(50):
            frame = w.generate_detections_for_node("n1", 0)
            tagged += sum(1 for e in frame.get("adsb", []) if e)
        assert tagged == 0
        summary = w.get_aircraft_summary()
        assert summary[0]["source"] == "live"
        assert build_adsb_push_payload(summary) == []
        gt = build_ground_truth_payload(summary)
        assert gt[0]["hex"] == "live-ab1388"
        assert gt[0]["has_adsb"] is False
        assert gt[0]["source"] == "live"

    def test_adsb_live_aircraft_is_tagged_with_its_real_hex(self):
        w = _world(frac_live_dark=0.0)
        node = NodeConfig(
            node_id="n1",
            rx_lat=34.85,
            rx_lon=-82.39,
            tx_lat=34.80,
            tx_lon=-82.30,
            beam_width_deg=360.0,
            max_range_km=200.0,
        )
        w.add_node(node)
        w.ingest_live_aircraft([_row()], now_wall=1000.0)
        tags = []
        for _ in range(50):
            tags += [e for e in w.generate_detections_for_node("n1", 0).get("adsb", []) if e]
        assert tags and all(t["hex"] == "ab1388" and t["flight"] == "N81227" for t in tags)
        summary = w.get_aircraft_summary()
        push = build_adsb_push_payload(summary)
        assert push[0]["hex"] == "ab1388" and push[0]["flight"] == "N81227"
        assert build_ground_truth_payload(summary)[0]["hex"] == "ab1388"

    def test_synthetic_spawns_default_source_sim(self):
        w = _world(min_aircraft=2)
        w.step(1.0, mode="adsb")
        assert {ac.source for ac in w.aircraft} == {"sim"}
        assert all(e["source"] == "sim" for e in build_ground_truth_payload(w.get_aircraft_summary()))
