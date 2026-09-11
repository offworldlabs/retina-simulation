"""Live ADS-B client — pulls real aircraft from adsb.retina.fm to seed the world.

The endpoint speaks the adsb.lol ``/v2/point/{lat}/{lon}/{radius_nm}`` shape
(tar1090 aircraft objects under ``ac``), so the parsing here is the same as
the backend's adsb.lol client; it is a separate, dependency-free copy because
this package must not import the backend, and because the two feeds serve
different purposes: the backend polls adsb.lol for *external truth* to score
nodes against, while this client feeds ``SimulationWorld.ingest_live_aircraft``
so that the synthetic nodes fly real trajectories.

Rows come back normalised to what the world needs, with ``captured_at`` as an
absolute wall-clock time (``seen_pos`` resolved against the fetch), so a
dead-reckoned position can be extrapolated to "now" on ingest.
"""

import gzip
import json
import logging
import time
import urllib.error
import urllib.request

log = logging.getLogger(__name__)

DEFAULT_BASE_URL = "https://adsb.retina.fm"
_TIMEOUT_S = 8
_USER_AGENT = "retina-simulation/1.0 (+https://github.com/offworldlabs/retina-simulation)"
# One last-good result per area may stand in for a failed fetch this long.
# Longer than the world's own staleness window (LIVE_STALE_S) is pointless:
# the world would drop the aircraft anyway before the cache stopped serving.
_CACHE_MAX_AGE_S = 90.0


def _num(v, default=0.0) -> float:
    """Coerce a tar1090 numeric field; sentinels like ``"ground"`` → default."""
    if isinstance(v, bool):
        return default
    if isinstance(v, (int, float)):
        return float(v)
    return default


def parse_point_response(data: dict, fetched_at: float) -> list[dict]:
    """Normalise one ``/v2/point`` payload into world-ready rows.

    Rows without a position, or whose ``alt_baro`` is neither numeric nor the
    ``"ground"`` sentinel, are dropped — there is nothing to fly.

    A row IS emitted for an aircraft on the ground, flagged ``on_ground`` with
    ``alt_baro`` 0.0.  adsb.retina.fm encodes ground as a numeric 0 (adsb.lol
    says ``"ground"``), so both spellings — and any non-positive altitude —
    count.  The world does not put a parked aircraft in the air; it uses the
    ground row as the one positive signal that an aircraft has LANDED, and
    retires it on the spot (see SimulationWorld.ingest_live_aircraft).  Dropping
    those rows instead left landed aircraft "flying" for another staleness
    window (146 s observed).
    """
    rows = []
    for ac in data.get("ac", []) or []:
        if not isinstance(ac, dict):
            continue
        hex_code = str(ac.get("hex") or "").strip().lower()
        lat = ac.get("lat")
        lon = ac.get("lon")
        if not hex_code or not isinstance(lat, (int, float)) or not isinstance(lon, (int, float)):
            continue
        alt_baro = ac.get("alt_baro")
        if alt_baro == "ground":
            on_ground, alt_ft = True, 0.0
        elif isinstance(alt_baro, (int, float)) and not isinstance(alt_baro, bool):
            on_ground = alt_baro <= 0
            alt_ft = 0.0 if on_ground else float(alt_baro)
        else:
            continue
        seen_pos = ac.get("seen_pos")
        captured_at = fetched_at - seen_pos if isinstance(seen_pos, (int, float)) else fetched_at
        rows.append(
            {
                "hex": hex_code,
                "flight": (ac.get("flight") or "").strip(),
                "lat": float(lat),
                "lon": float(lon),
                "alt_baro": alt_ft,  # ft, 0.0 when on_ground
                "on_ground": on_ground,
                "gs": _num(ac.get("gs")),  # knots
                "track": _num(ac.get("track")),  # deg
                "baro_rate": _num(ac.get("baro_rate")),  # ft/min
                "captured_at": captured_at,  # epoch s
            }
        )
    return rows


class LiveAdsbClient:
    """Polls adsb.retina.fm (or any adsb.lol-shaped server) for one or more areas."""

    def __init__(self, areas: list[dict], base_url: str = DEFAULT_BASE_URL):
        """
        Args:
            areas: dicts with name, lat, lon and optional radius_nm (default 80).
            base_url: server root; ``/v2/point/...`` is appended.
        """
        self.base_url = base_url.rstrip("/")
        self.areas = [a for a in areas if isinstance(a, dict) and "lat" in a and "lon" in a]
        self._cache: dict[str, list[dict]] = {}
        self._cache_ts: dict[str, float] = {}
        self.last_status: dict[str, bool] = {}
        self.last_error: str | None = None

    def _url(self, area: dict) -> str:
        return f"{self.base_url}/v2/point/{area['lat']}/{area['lon']}/{area.get('radius_nm', 80)}"

    def _get(self, url: str) -> dict:
        req = urllib.request.Request(
            url,
            headers={
                "Accept": "application/json",
                "Accept-Encoding": "gzip",
                "User-Agent": _USER_AGENT,
            },
        )
        with urllib.request.urlopen(req, timeout=_TIMEOUT_S) as resp:  # noqa: S310 — https URL built from config
            raw = resp.read()
            if (resp.headers or {}).get("Content-Encoding") == "gzip":
                raw = gzip.decompress(raw)
        return json.loads(raw)

    def _last_good(self, name: str) -> list[dict]:
        ts = self._cache_ts.get(name)
        if ts is None or time.monotonic() - ts > _CACHE_MAX_AGE_S:
            self._cache.pop(name, None)
            self._cache_ts.pop(name, None)
            return []
        return self._cache.get(name, [])

    def fetch_area(self, area: dict) -> list[dict]:
        name = str(area.get("name") or f"{area['lat']},{area['lon']}")
        try:
            data = self._get(self._url(area))
            rows = parse_point_response(data, time.time())
        except (urllib.error.URLError, OSError, ValueError) as e:
            self.last_status[name] = False
            self.last_error = str(e)
            return self._last_good(name)
        self._cache[name] = rows
        self._cache_ts[name] = time.monotonic()
        self.last_status[name] = True
        self.last_error = None
        return rows

    def fetch_all(self) -> list[dict]:
        """Every configured area, deduplicated by hex (first area wins)."""
        seen: set[str] = set()
        out: list[dict] = []
        for area in self.areas:
            for row in self.fetch_area(area):
                if row["hex"] in seen:
                    continue
                seen.add(row["hex"])
                out.append(row)
        return out
