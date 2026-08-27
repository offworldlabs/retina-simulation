"""ADS-B push payload schema (build_adsb_push_payload).

The push is the simulated ADS-B broadcast, so it must carry transponder
aircraft only.  The old inline loop substituted the object id for a missing
hex, which minted a fake transponder per dark aircraft on the server — every
dark solve then keyed mn-adsb-* and the server's dark lane stayed empty.
"""

from retina_simulation.orchestrator import build_adsb_push_payload


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
        "anomaly_event": None,
    }
    base.update(overrides)
    return base


class TestBuildAdsbPushPayload:
    def test_transponder_aircraft_pushed_under_its_hex(self):
        out = build_adsb_push_payload([_summary()])
        assert len(out) == 1
        entry = out[0]
        assert entry["hex"] == "a1b2c3"
        assert entry["lat"] == 34.85
        assert entry["alt_baro"] == round(9500 / 0.3048)
        assert entry["gs"] == round(230.0 * 1.94384, 1)
        assert entry["track"] == 270.0

    def test_dark_aircraft_not_pushed(self):
        """The regression: an aircraft with no adsb_hex must not appear at
        all — above all not under its object id."""
        out = build_adsb_push_payload([_summary(has_adsb=False, adsb_hex=None, adsb_callsign=None)])
        assert out == []

    def test_mixed_fleet_keeps_only_transponder_aircraft(self):
        out = build_adsb_push_payload(
            [
                _summary(),
                _summary(id="obj-0002", has_adsb=False, adsb_hex=None),
                _summary(id="obj-0003", adsb_hex="d4e5f6"),
            ]
        )
        assert [e["hex"] for e in out] == ["a1b2c3", "d4e5f6"]


class TestBuildRealAdsbBody:
    def test_body_declares_the_real_world(self):
        """The server's claiming stage keys on this tag; an untagged relay
        hands every synthetic node a pool of real-aircraft decoys to bind
        its echoes to — the ghost planes of 2026-08-27."""
        from retina_simulation.orchestrator import build_real_adsb_body

        payload = [{"hex": "a97cf2", "lat": 34.84, "lon": -82.35}]
        body = build_real_adsb_body(payload)
        assert body["source"] == "real"
        assert body["aircraft"] == payload
        assert isinstance(body["ts_ms"], int)
