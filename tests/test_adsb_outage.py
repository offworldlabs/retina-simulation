"""Transponder outages (frac_adsb_outage).

An ADS-B aircraft that goes silent mid-flight is the only way to exercise the
backend's known-track hold: the node track stays claimed to a hex after the
broadcast stops.  Before this knob the simulator tagged every has_adsb
aircraft in every frame and pushed it every second, so the hold could never be
entered.  Default is 0.0 — nothing changes for anyone who does not set it.
"""

from retina_simulation.orchestrator import build_adsb_push_payload, build_ground_truth_payload
from retina_simulation.world import (
    _ADSB_OUTAGE_DURATION_S,
    _ADSB_OUTAGE_START_S,
    NodeConfig,
    SimulatedAircraft,
    SimulationWorld,
)


def _world(frac_outage: float) -> SimulationWorld:
    w = SimulationWorld(center_lat=33.9, center_lon=-84.6)
    w.frac_anomalous = 0.0
    w.frac_drone = 0.0
    w.frac_dark = 0.0  # every spawn is a transponder aircraft
    w.frac_adsb_outage = frac_outage
    return w


def _summary(**overrides) -> dict:
    base = {
        "id": "obj-0001",
        "lat": 34.85,
        "lon": -82.4,
        "alt_km": 9.5,
        "heading": 270.0,
        "speed_ms": 230.0,
        "has_adsb": True,
        "is_anomalous": False,
        "object_type": "aircraft",
        "adsb_hex": "a1b2c3",
        "adsb_callsign": "ABC1234",
        "adsb_silent": False,
        "anomaly_event": None,
    }
    base.update(overrides)
    return base


class TestOutageScheduling:
    def test_knob_off_schedules_nothing(self):
        w = _world(0.0)
        for _ in range(30):
            ac = w._spawn_aircraft(mode="adsb")
            assert ac.adsb_outage_start_s is None
            assert ac.adsb_outage_end_s is None
            assert ac.adsb_silent is False

    def test_knob_at_one_schedules_every_adsb_aircraft(self):
        w = _world(1.0)
        for _ in range(30):
            ac = w._spawn_aircraft(mode="adsb")
            assert ac.has_adsb
            assert ac.adsb_outage_start_s is not None
            offset = ac.adsb_outage_start_s - ac.created_at
            assert _ADSB_OUTAGE_START_S[0] <= offset <= _ADSB_OUTAGE_START_S[1]
            duration = ac.adsb_outage_end_s - ac.adsb_outage_start_s
            assert _ADSB_OUTAGE_DURATION_S[0] <= duration <= _ADSB_OUTAGE_DURATION_S[1]

    def test_dark_aircraft_never_get_an_outage(self):
        # No transponder means nothing to lose; a dark aircraft with an outage
        # window would be a silent no-op that muddies the counts.
        w = _world(1.0)
        w.frac_dark = 1.0
        for _ in range(20):
            ac = w._spawn_aircraft(mode="adsb")
            assert ac.has_adsb is False
            assert ac.adsb_outage_start_s is None

    def test_silence_window_is_closed_at_the_end(self):
        ac = SimulatedAircraft(
            object_id="obj-1",
            lat=0.0,
            lon=0.0,
            alt_km=9.0,
            vel_east=0.0,
            vel_north=0.0,
            vel_up=0.0,
            heading_deg=0.0,
            speed_km_s=0.2,
            has_adsb=True,
            adsb_hex="a1b2c3",
            adsb_outage_start_s=100.0,
            adsb_outage_end_s=200.0,
        )
        ac.sim_now_s = 99.0
        assert ac.adsb_silent is False
        ac.sim_now_s = 100.0
        assert ac.adsb_silent is True
        ac.sim_now_s = 199.9
        assert ac.adsb_silent is True
        ac.sim_now_s = 200.0
        assert ac.adsb_silent is False


class TestRuntimeReRoll:
    def test_raising_the_knob_rolls_aircraft_already_in_the_air(self):
        w = _world(0.0)
        for _ in range(20):
            w.aircraft.append(w._spawn_aircraft(mode="adsb"))
        assert all(ac.adsb_outage_start_s is None for ac in w.aircraft)

        w.frac_adsb_outage = 1.0
        n = w.schedule_adsb_outages()
        assert n == 20
        assert all(ac.adsb_outage_start_s is not None for ac in w.aircraft)

    def test_re_roll_leaves_already_scheduled_aircraft_alone(self):
        # The config poll runs every 5 s; re-rolling the same aircraft on each
        # poll would keep pushing its outage into the future forever.
        w = _world(1.0)
        for _ in range(10):
            w.aircraft.append(w._spawn_aircraft(mode="adsb"))
        before = [(ac.adsb_outage_start_s, ac.adsb_outage_end_s) for ac in w.aircraft]
        assert w.schedule_adsb_outages() == 0
        after = [(ac.adsb_outage_start_s, ac.adsb_outage_end_s) for ac in w.aircraft]
        assert before == after

    def test_re_roll_is_a_no_op_with_the_knob_off(self):
        w = _world(0.0)
        for _ in range(10):
            w.aircraft.append(w._spawn_aircraft(mode="adsb"))
        assert w.schedule_adsb_outages() == 0


class TestFrameTagging:
    def _frame_hexes(self, w: SimulationWorld) -> list:
        frame = w.generate_detections_for_node("n1", timestamp_ms=0)
        return frame.get("adsb", [])

    def test_tag_present_before_and_after_but_none_during(self):
        w = _world(0.0)
        w.add_node(NodeConfig(node_id="n1", max_range_km=400.0, beam_width_deg=360.0))
        ac = w._spawn_aircraft(mode="adsb")
        # Park the aircraft on top of the node so the cone/miss roll cannot
        # drop it; retry a few frames to ride out the SNR-dependent miss.
        ac.lat, ac.lon, ac.alt_km = 33.94, -84.65, 9.0
        ac.adsb_outage_start_s = 100.0
        ac.adsb_outage_end_s = 200.0
        w.aircraft = [ac]

        def tags_for(now: float) -> list:
            ac.sim_now_s = now
            seen = []
            for _ in range(40):
                seen.extend(self._frame_hexes(w))
            return [t for t in seen if t is not None]

        assert tags_for(50.0), "transponder must be tagged before the outage"
        assert tags_for(150.0) == [], "transponder must be untagged during the outage"
        assert tags_for(250.0), "transponder must be tagged again after the outage"

    def test_silent_aircraft_still_produces_radar_detections(self):
        # The echo is unchanged — only the ADS-B tag goes away.  A silent
        # aircraft that also vanished from delay/doppler would be a dark
        # aircraft, not a transponder outage.
        w = _world(0.0)
        w.add_node(NodeConfig(node_id="n1", max_range_km=400.0, beam_width_deg=360.0))
        ac = w._spawn_aircraft(mode="adsb")
        ac.lat, ac.lon, ac.alt_km = 33.94, -84.65, 9.0
        ac.adsb_outage_start_s, ac.adsb_outage_end_s = 0.0, 200.0
        ac.sim_now_s = 100.0
        w.aircraft = [ac]
        assert any(w.generate_detections_for_node("n1", 0)["delay"] for _ in range(40))


class TestSummaryAndPayloads:
    def test_summary_carries_the_flag(self):
        w = _world(0.0)
        ac = w._spawn_aircraft(mode="adsb")
        ac.adsb_outage_start_s, ac.adsb_outage_end_s = 10.0, 20.0
        w.aircraft = [ac]

        ac.sim_now_s = 5.0
        assert w.get_aircraft_summary()[0]["adsb_silent"] is False
        ac.sim_now_s = 15.0
        assert w.get_aircraft_summary()[0]["adsb_silent"] is True

    def test_ground_truth_payload_passes_the_flag_through(self):
        out = build_ground_truth_payload([_summary(adsb_silent=True)])
        assert out[0]["adsb_silent"] is True
        # has_adsb stays True: the aircraft HAS a transponder, it is off.
        assert out[0]["has_adsb"] is True

    def test_ground_truth_flag_defaults_false_for_older_summaries(self):
        summary = _summary()
        del summary["adsb_silent"]
        assert build_ground_truth_payload([summary])[0]["adsb_silent"] is False

    def test_push_payload_skips_silent_aircraft(self):
        out = build_adsb_push_payload([_summary(adsb_silent=True)])
        assert out == []

    def test_push_payload_keeps_broadcasting_aircraft(self):
        out = build_adsb_push_payload([_summary(adsb_silent=False)])
        assert [e["hex"] for e in out] == ["a1b2c3"]
