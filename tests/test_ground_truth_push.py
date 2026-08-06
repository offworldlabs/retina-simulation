"""Ground-truth push payload schema (build_ground_truth_payload).

The server's debug map shows simulated parameters per ground-truth object, so
the push payload must carry the ADS-B/anomaly attributes the world already
tracks — a silent field drop here is invisible until someone clicks a dot.
"""

from retina_simulation.orchestrator import build_ground_truth_payload


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


class TestBuildGroundTruthPayload:
    def test_passes_through_adsb_and_anomaly_fields(self):
        out = build_ground_truth_payload([_summary(anomaly_event="hijack",
                                                   is_anomalous=True)])
        assert len(out) == 1
        entry = out[0]
        assert entry["hex"] == "a1b2c3"
        assert entry["has_adsb"] is True
        assert entry["adsb_callsign"] == "ABC1234"
        assert entry["anomaly_event"] == "hijack"
        assert entry["is_anomalous"] is True
        assert entry["alt_m"] == 9500.0

    def test_dark_object_falls_back_to_object_id(self):
        out = build_ground_truth_payload([_summary(
            adsb_hex=None, adsb_callsign=None, has_adsb=False)])
        assert out[0]["hex"] == "obj-0001"
        assert out[0]["has_adsb"] is False
        assert out[0]["adsb_callsign"] is None

    def test_entry_without_hex_or_id_is_dropped(self):
        out = build_ground_truth_payload([_summary(adsb_hex=None, id="")])
        assert out == []

    def test_missing_optional_keys_default(self):
        # A summary from an older world build without the new keys must not
        # crash and must default sanely.
        minimal = {"id": "obj-2", "lat": 1.0, "lon": 2.0, "alt_km": 3.0}
        out = build_ground_truth_payload([minimal])
        assert out[0]["has_adsb"] is False
        assert out[0]["adsb_callsign"] is None
        assert out[0]["anomaly_event"] is None
        assert out[0]["object_type"] == "aircraft"
