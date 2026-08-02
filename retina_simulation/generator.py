"""
Fleet Generator — Generates 100-1000 realistic synthetic node configurations
spread across multiple geographic regions, each paired with a real broadcast
transmitter tower.

Usage:
    python fleet_generator.py --nodes 200 --output fleet_config.json
    python fleet_generator.py --nodes 1000 --regions us,eu,au
"""

import argparse
import json
import logging
import math
import random
import sys
from dataclasses import dataclass, asdict
from typing import Optional

# ── Broadcast tower databases by region ──────────────────────────────────────
# Each tower: (lat, lon, alt_ft, freq_hz, callsign)
# Modeled after real VHF/UHF broadcast transmitters suitable for passive radar

# Effective radiated power, dBm, keyed by callsign.  Kept beside the tower
# tuples rather than inside them so the 5-element unpacking used throughout
# this module stays valid.  Only populated for sites taken from real FCC
# records; illuminator selection falls back to _DEFAULT_EIRP_DBM otherwise.
#
# The spread here is 65 dB — Caesars Head at 92.2 dBm against Spartanburg at
# 27.0 — so a fleet that ignores EIRP treats a 0.5 W transmitter as the equal
# of a megawatt one.
_TOWER_EIRP_DBM = {
    "WYFF": 92.2,        # Caesars Head
    "WMYA-TV": 90.9,     # Fountain Inn
    "WNTV": 84.7,        # Paris Mountain — Tower Finder's top pick for the metro
    "WLOS": 83.7,        # Mt Pisgah
    "WSPA-TV": 77.4,     # Hogback Mtn
    "BLP00776": 57.0,    # near the core, low power
    "W07DT-D": 50.0,     # Tryon NC
    "BLP01065": 27.0,    # Spartanburg — geometrically valuable, radiologically weak
}
_DEFAULT_EIRP_DBM = 80.0

_TOWERS_US = [
    # East Coast
    (33.75667, -84.33184, 1600, 195_000_000, "WSB-TV"),    # Atlanta
    (35.23064, -80.84313, 1540, 575_000_000, "WBTV"),       # Charlotte
    (38.93460, -77.07920, 1380, 585_000_000, "WRC-TV"),     # Washington DC
    (40.74843, -73.98566, 1776, 191_000_000, "WCBS-TV"),    # New York
    (42.35370, -71.06010, 1200, 575_000_000, "WBZ-TV"),     # Boston
    (39.95233, -75.16379, 1600, 563_000_000, "KYW-TV"),     # Philadelphia
    (25.79590, -80.28700, 1000, 191_000_000, "WTVJ"),       # Miami
    (28.54082, -81.37916, 1350, 551_000_000, "WESH"),       # Orlando
    (27.97450, -82.45720, 1400, 539_000_000, "WFLA"),       # Tampa
    (30.33270, -81.65560, 1200, 575_000_000, "WJXT"),       # Jacksonville
    (36.85260, -75.97820, 1300, 539_000_000, "WAVY"),       # Norfolk
    (35.78700, -78.78170, 1500, 563_000_000, "WRAL"),       # Raleigh
    # ── Greenville SC ────────────────────────────────────────────────────────
    # Real FCC facilities from the Tower Finder illuminator search, one entry
    # per *distinct site*.  Twenty stations serve this market but they share
    # only eight masts — Paris Mountain alone carries WNTV, WRET-TV, WGGS-TV,
    # W10AJ-D and five LPTVs.  Co-sited transmitters are worthless as a
    # bistatic pair (identical geometry, identical ellipse), so the table lists
    # sites and the strongest station at each.
    # alt is the radiating centre AMSL (ground + antenna height), in feet.
    (34.941222, -82.410278, 3315, 183_000_000, "WNTV"),       # Paris Mountain
    (34.647500, -82.270000, 1847, 599_000_000, "WMYA-TV"),    # Fountain Inn — south
    (35.170194, -82.290500, 5437, 201_000_000, "WSPA-TV"),    # Hogback Mtn
    (35.111944, -82.606389, 5058, 569_000_000, "WYFF"),       # Caesars Head
    (35.222222, -82.549444, 5220, 213_000_000, "WLOS"),       # Mt Pisgah
    (34.970111, -81.948391,  794, 195_000_000, "BLP01065"),   # Spartanburg — east
    (35.266278, -82.244111, 3186, 177_000_000, "W07DT-D"),    # Tryon NC
    (34.875111, -82.338211,  984, 183_000_000, "BLP00776"),   # near the core
    # Midwest
    (41.87150, -87.62440, 1650, 191_000_000, "WBBM-TV"),   # Chicago
    (42.33140, -83.04580, 1200, 551_000_000, "WXYZ-TV"),   # Detroit
    (39.96110, -82.99880, 1400, 563_000_000, "WCMH"),      # Columbus
    (39.76910, -86.15800, 1350, 575_000_000, "WISH-TV"),   # Indianapolis
    (44.97750, -93.26490, 1500, 585_000_000, "WCCO-TV"),   # Minneapolis
    (38.62720, -90.19780, 1300, 551_000_000, "KMOV"),       # St Louis
    (39.09970, -94.57860, 1450, 539_000_000, "KCTV"),       # Kansas City
    (41.25220, -95.99780, 1350, 575_000_000, "KETV"),       # Omaha
    # South
    (29.76330, -95.36320, 1300, 191_000_000, "KHOU"),       # Houston
    (32.78060, -96.80060, 1600, 575_000_000, "WFAA"),       # Dallas
    (29.42410, -98.49360, 1200, 563_000_000, "KENS"),       # San Antonio
    (30.26710, -97.74310, 1400, 551_000_000, "KVUE"),       # Austin
    (36.16270, -86.78160, 1350, 539_000_000, "WSMV"),       # Nashville
    (35.14950, -90.04890, 1200, 575_000_000, "WMC-TV"),    # Memphis
    (32.29560, -90.18480, 1100, 563_000_000, "WLBT"),      # Jackson MS
    (30.45080, -91.18720, 1150, 551_000_000, "WAFB"),      # Baton Rouge
    # West
    (34.05220, -118.24370, 1600, 191_000_000, "KABC-TV"),  # Los Angeles
    (37.77490, -122.41940, 1500, 575_000_000, "KGO-TV"),   # San Francisco
    (47.60620, -122.33210, 1400, 585_000_000, "KOMO-TV"),  # Seattle
    (45.52350, -122.67620, 1300, 551_000_000, "KGW"),       # Portland
    (33.44840, -112.07400, 1200, 563_000_000, "KPHO-TV"),  # Phoenix
    (36.17490, -115.13740, 1150, 539_000_000, "KLAS-TV"),  # Las Vegas
    (39.73920, -104.99030, 1400, 575_000_000, "KCNC-TV"),  # Denver
    (40.76080, -111.89100, 1300, 563_000_000, "KSL-TV"),   # Salt Lake City
    (32.71570, -117.16110, 1100, 551_000_000, "KFMB-TV"),  # San Diego
    (36.74770, -119.77260, 1200, 539_000_000, "KFSN-TV"),  # Fresno
]

_TOWERS_EU = [
    # UK
    (51.50740, -0.12780, 1000, 690_000_000, "Crystal Palace"),
    (53.47620, -2.22320, 1060, 706_000_000, "Winter Hill"),
    (52.68660, -2.43120, 1140, 730_000_000, "Sutton Coldfield"),
    (55.95320, -3.18830, 980, 674_000_000, "Black Hill"),
    (51.45150, -2.59270, 920, 698_000_000, "Mendip"),
    # Germany
    (52.52060, 13.40490, 1200, 482_000_000, "Berlin Alexanderplatz"),
    (48.13510, 11.58200, 960, 514_000_000, "München Olympiaturm"),
    (50.11090, 8.68210, 1100, 498_000_000, "Frankfurt Europaturm"),
    (53.55110, 9.99370, 1040, 530_000_000, "Hamburg Heinrich-Hertz"),
    (51.22770, 6.77350, 980, 546_000_000, "Düsseldorf Rheinturm"),
    # France
    (48.85830, 2.29450, 1050, 474_000_000, "Tour Eiffel"),
    (43.29650, 5.36980, 900, 490_000_000, "Marseille Grande Étoile"),
    (45.76400, 4.83570, 950, 506_000_000, "Lyon Fourvière"),
    (44.83780, -0.57950, 880, 522_000_000, "Bordeaux Bouliac"),
    # Spain / Italy
    (40.41680, -3.70380, 950, 538_000_000, "Madrid Torrespaña"),
    (41.38790, 2.16990, 1000, 554_000_000, "Barcelona Collserola"),
    (41.90270, 12.49630, 920, 570_000_000, "Roma Monte Mario"),
    (45.46430, 9.18950, 980, 586_000_000, "Milano Valassina"),
    # Scandinavia
    (59.33260, 18.06490, 1100, 602_000_000, "Stockholm Kaknäs"),
    (60.17100, 24.93750, 860, 618_000_000, "Helsinki Pasila"),
]

_TOWERS_AU = [
    (-33.86880, 151.20930, 1200, 226_500_000, "Sydney TCN-9"),
    (-37.81360, 144.96310, 1100, 182_250_000, "Melbourne GTV-9"),
    (-27.46980, 153.02510, 1000, 191_625_000, "Brisbane QTQ-9"),
    (-34.92850, 138.60070, 950, 209_250_000, "Adelaide NWS-9"),
    (-31.95050, 115.86040, 900, 196_250_000, "Perth STW-9"),
    (-42.88260, 147.32710, 850, 182_250_000, "Hobart TVT-6"),
    (-35.28100, 149.13000, 920, 226_500_000, "Canberra CTC-10"),
    (-19.25900, 146.81690, 800, 196_250_000, "Townsville TNQ-7"),
    (-12.46340, 130.84560, 780, 191_625_000, "Darwin DTQ-8"),
    (-33.42990, 149.10050, 860, 209_250_000, "Orange CBN-8"),
]

# ── Rural / isolated towers for "solo node" testing ──────────────────────────
# These are real US broadcast transmitters in areas with no nearby metro clusters.
# Each is at least ~150 km from the nearest _TOWERS_US entry, so nodes here
# can never share coverage with a metro node and will always produce solo arcs.
_TOWERS_SOLO_US = [
    # Great Plains / High Plains
    (44.07500, -103.22830, 1100, 551_000_000, "KEVN-Rapid City"),     # SD
    (46.87190, -113.99300, 1050, 563_000_000, "KPAX-Missoula"),       # MT
    (43.03540, -108.05270, 1000, 539_000_000, "KCWY-Casper"),         # WY
    (48.23000, -101.29600,  980, 575_000_000, "KMOT-Minot"),          # ND
    (38.81130,  -99.32640,  960, 551_000_000, "KAYS-Hays"),           # KS central
    (32.44180, -104.22840,  900, 563_000_000, "KCAV-Carlsbad"),       # NM SE
    # Desert Southwest
    (35.19900, -111.65100, 1200, 563_000_000, "KNAZ-Flagstaff"),      # AZ
    (40.83870, -115.76270,  920, 539_000_000, "KELK-Elko"),           # NV
    (31.87220, -106.42920,  880, 539_000_000, "KTSM-El Paso"),        # TX border
    (36.90000, -111.50000, 1100, 539_000_000, "KPGE-Page-AZ"),        # AZ NE
    (39.50000, -119.80000,  950, 551_000_000, "KRNV-Reno"),           # NV
    # South / Central
    (34.74020,  -92.28990,  920, 563_000_000, "KATV-Little Rock"),    # AR
    (37.68610,  -97.33010,  940, 551_000_000, "KWCH-Wichita"),        # KS
    # Pacific NW interior
    (42.55000, -114.46000,  980, 575_000_000, "KXTF-Twin Falls"),     # ID
    # Rural Midwest / Great Lakes
    (46.48730,  -84.35670,  900, 563_000_000, "KBSF-Sault Ste Marie"),# MI UP
    (46.78650,  -92.10350,  940, 551_000_000, "KDLH-Duluth"),         # MN
    (47.92500,  -97.03260,  920, 563_000_000, "WDAY-Fargo"),          # ND
    (43.54960,  -96.72960,  930, 539_000_000, "KSFY-Sioux Falls"),    # SD
    (46.37000,  -94.87000,  920, 539_000_000, "KBRJ-Brainerd"),       # MN lakes
    (45.00000,  -85.50000,  880, 563_000_000, "WPBN-Traverse"),       # MI north
    # East
    (44.06000,  -76.15000,  850, 563_000_000, "WWTI-Watertown"),      # NY
    (37.30000,  -79.50000,  880, 563_000_000, "WSLS-Roanoke"),        # VA
]


# ── Coverage-ring metros ──────────────────────────────────────────────────────
# Each entry: shared low-band (VHF) illuminator + the airspace core (airport) the
# receiver ring is built around and aims at. VHF keeps target Doppler inside the
# analytics 90 Hz association gate for ~18 km baselines; the TX sits 12-30 km off
# the core for a non-degenerate bistatic angle. Cores reuse world._US_WAYPOINTS.
# (tx_lat, tx_lon, tx_alt_ft, fc_hz, callsign, core_lat, core_lon)
_RING_TXS = [
    (32.78060, -96.80060, 1600, 195_000_000, "WFAA-RING", 32.8968, -97.0380),   # DFW
    (41.87810, -87.62980, 1500, 197_000_000, "WMAQ-RING", 41.9742, -87.9073),   # ORD Chicago
    (33.74900, -84.38800, 1050, 199_000_000, "WSB-RING",  33.6407, -84.4277),   # ATL
    (39.73920, -104.99030, 5300, 201_000_000, "KCNC-RING", 39.8561, -104.6737), # DEN
    (39.09970, -94.57860, 900, 203_000_000, "KMBC-RING",  39.2976, -94.7139),   # MCI Kansas City
    # WSPA-TV, RF ch 11 (201 MHz) on Hogback Mtn, 31 km NNW of GSP.  It shares
    # 201 MHz with the DEN ring above, which is harmless: rings never overlap
    # geographically.  (An earlier version of this note said the market had no
    # other VHF station — it has several; see _TOWERS_US.  The ring uses this
    # one because it is the strongest VHF site with a clear line to the core.)
    (35.170194, -82.290500, 5437, 201_000_000, "WSPA-RING", 34.8957, -82.2189),  # GSP Greenville SC
]


# ── Metro areas ───────────────────────────────────────────────────────────────
# Shared by the generator's --metro scoping and the orchestrator's --metros
# post-generation filter, so the two can never disagree about where a metro is.
_KNOWN_METROS = {
    "atl": {"name": "Atlanta", "lat": 33.749, "lon": -84.388, "radius_nm": 80},
    "gvl": {"name": "Greenville", "lat": 34.852, "lon": -82.394, "radius_nm": 60},
    "clt": {"name": "Charlotte", "lat": 35.227, "lon": -80.843, "radius_nm": 70},
    "nyc": {"name": "New York", "lat": 40.748, "lon": -73.986, "radius_nm": 80},
    "dca": {"name": "Washington DC", "lat": 38.935, "lon": -77.079, "radius_nm": 70},
    "chi": {"name": "Chicago", "lat": 41.872, "lon": -87.624, "radius_nm": 80},
    "den": {"name": "Denver", "lat": 39.739, "lon": -104.990, "radius_nm": 80},
    "lax": {"name": "Los Angeles", "lat": 34.052, "lon": -118.244, "radius_nm": 80},
    "dfw": {"name": "Dallas-Fort Worth", "lat": 32.897, "lon": -97.038, "radius_nm": 80},
    "kc": {"name": "Kansas City", "lat": 39.298, "lon": -94.714, "radius_nm": 70},
}

_NM_TO_KM = 1.852


def _haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Return great-circle distance in km between two lat/lon points."""
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2) ** 2
         + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2))
         * math.sin(dlon / 2) ** 2)
    return R * 2 * math.asin(math.sqrt(max(0.0, min(1.0, a))))


def _bearing_between(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Initial bearing point1→point2, degrees from north clockwise.

    Matches world._bearing_deg so a beam aimed here lands in world's cone check.
    """
    dlon = math.radians(lon2 - lon1)
    lat1r, lat2r = math.radians(lat1), math.radians(lat2)
    x = math.sin(dlon) * math.cos(lat2r)
    y = (math.cos(lat1r) * math.sin(lat2r)
         - math.sin(lat1r) * math.cos(lat2r) * math.cos(dlon))
    return math.degrees(math.atan2(x, y)) % 360


# Rural US bounding boxes used when the named solo pool needs extending.
# Each box: (lat_min, lat_max, lon_min, lon_max)
_RURAL_BOXES_US = [
    (38.0, 49.0, -116.0, -96.0),   # Northern Great Plains (MT/ND/SD/NE/WY)
    (32.0, 42.0, -108.0, -96.0),   # Southern Great Plains (KS/OK/TX panhandle/NM)
    (32.0, 42.0, -117.0, -108.0),  # Desert Southwest (AZ/NV/UT/western NM)
    (40.0, 50.0, -122.0, -111.0),  # Pacific NW interior (eastern OR/WA/ID)
    (35.0, 46.0, -111.0, -103.0),  # Rockies (CO/WY/MT eastern slope)
    (38.0, 47.0, -99.0,  -88.0),   # Rural Midwest (IA/MN/WI/IL away from cities)
]
_RURAL_FREQS = [539_000_000, 551_000_000, 563_000_000, 575_000_000, 585_000_000]


def _extend_solo_pool(
    current_pool: list,
    n_needed: int,
    avoid_positions: list[tuple[float, float]],
    min_sep_km: float = 180.0,
    max_attempts: int = 200_000,
) -> list:
    """Extend *current_pool* to at least *n_needed* entries.

    Named towers in *current_pool* are gated with the same *min_sep_km* check
    against *avoid_positions* and each other before being accepted.  New
    synthetic towers are then placed in rural US bounding boxes via
    rejection-sampling so every accepted position is at least *min_sep_km*
    from all others.
    Returns the (possibly extended) pool.
    """
    pool = []
    busy = list(avoid_positions)          # positions already taken

    # Gate named entries — apply the same minimum-separation rule so named
    # towers that are too close to each other or to metro positions are skipped.
    for item in current_pool:
        lat, lon = item[0], item[1]
        if not any(_haversine_km(lat, lon, p[0], p[1]) < min_sep_km for p in busy):
            pool.append(item)
            busy.append((lat, lon))

    attempts = 0
    while len(pool) < n_needed and attempts < max_attempts:
        box = random.choice(_RURAL_BOXES_US)
        lat = random.uniform(box[0], box[1])
        lon = random.uniform(box[2], box[3])

        # Reject if too close to any known position
        if any(_haversine_km(lat, lon, p[0], p[1]) < min_sep_km for p in busy):
            attempts += 1
            continue

        alt_ft = random.uniform(800, 1400)
        fc = random.choice(_RURAL_FREQS)
        callsign = f"RURAL-{len(pool) + 1:04d}"
        pool.append((lat, lon, alt_ft, fc, callsign))
        busy.append((lat, lon))
        attempts += 1

    return pool


@dataclass
class GeneratedNodeConfig:
    """A generated node in the fleet."""
    node_id: str
    rx_lat: float
    rx_lon: float
    rx_alt_ft: float
    tx_lat: float
    tx_lon: float
    tx_alt_ft: float
    fc_hz: float
    fs_hz: float = 2_000_000.0
    beam_width_deg: float = 40.0
    max_range_km: float = 50.0
    region: str = "us"
    tx_callsign: str = ""
    beam_azimuth_deg: Optional[float] = None   # explicit Yagi aim; None → broadside
    # Bistatic range limit: (RX→target) + (target→TX) − baseline.  That sum is
    # what the delay measures and what sets received power, so it is the
    # physical detection limit; max_range_km is a monostatic approximation
    # retained for hardware nodes.  None → omitted from the wire format.
    max_bistatic_range_km: Optional[float] = None


def _node_dict(node: GeneratedNodeConfig) -> dict:
    """Serialize a node, omitting unset optional geometry keys.

    The backend solver does `float(node_cfg["beam_azimuth_deg"])` whenever the
    key is present, so a null on the wire would crash it. Dropping the key for
    broadside nodes makes the backend fall back to its own broadside auto-aim.
    max_bistatic_range_km is dropped for the same reason and so that its
    absence unambiguously means "use the monostatic rule".
    """
    d = asdict(node)
    for _optional in ("beam_azimuth_deg", "max_bistatic_range_km"):
        if d.get(_optional) is None:
            d.pop(_optional, None)
    return d


def _jitter(val: float, sigma: float) -> float:
    """Add Gaussian jitter to a value."""
    return val + random.gauss(0, sigma)


_DISPLAY_FUZZ_DEG = 0.0036


def _node_display_fuzz(node_id: str) -> tuple[float, float]:
    """Match the frontend's deterministic RX privacy offset."""
    h1 = 0xDEADBEEF
    h2 = 0x41C6CE57
    for ch in node_id:
        code = ord(ch)
        h1 = ((h1 ^ code) * 2654435761) & 0xFFFFFFFF
        h2 = ((h2 ^ code) * 1597334677) & 0xFFFFFFFF

    h1 = ((h1 ^ (h1 >> 16)) * 2246822507) & 0xFFFFFFFF
    h1 = ((h1 ^ (h1 >> 13)) * 3266489909) & 0xFFFFFFFF
    h1 ^= h1 >> 16
    h2 = ((h2 ^ (h2 >> 16)) * 2246822507) & 0xFFFFFFFF
    h2 = ((h2 ^ (h2 >> 13)) * 3266489909) & 0xFFFFFFFF
    h2 ^= h2 >> 16

    n1 = (h1 / 0x100000000) * 2 - 1
    n2 = (h2 / 0x100000000) * 2 - 1
    return n1 * _DISPLAY_FUZZ_DEG, n2 * _DISPLAY_FUZZ_DEG


# ── Water / ocean rejection ──────────────────────────────────────────────────
# Simple bounding-box check for known US water bodies.  Positions inside any
# of these boxes are rejected so synthetic receiver sites don't end up in the
# ocean, Great Lakes, or large bays.

_WATER_BOXES: list[tuple[float, float, float, float]] = [
    # (lat_min, lat_max, lon_min, lon_max)
    # Great Lakes
    (41.5, 49.0, -92.5, -76.0),   # approximate Great Lakes bounding box
    # Gulf of Mexico (open water — broad nearshore strip)
    (18.0, 30.5, -98.0, -80.0),
    # Atlantic east of Florida / Florida Straits / Bahamas
    # Closes the -80W→-72W gap that makes nodes appear in the ocean near Miami
    (24.0, 31.0, -81.0, -72.0),
    # Atlantic Ocean — open ocean east of the eastern seaboard
    (24.0, 47.5, -72.0, -60.0),
    # Pacific Ocean — truly offshore strip (cities are east of -117°W)
    (32.0, 49.0, -130.0, -125.0),
    # Tampa Bay (Gulf box doesn't have enough land-point density to cover the ~30km width)
    (27.35, 28.1, -82.85, -82.2),
    # Charlotte Harbor / Pine Island Sound FL
    (26.5, 27.1, -82.35, -81.85),
    # Sarasota Bay / Little Sarasota Bay FL
    (27.1, 27.55, -82.75, -82.5),
    # Lake Pontchartrain LA (35km wide — nearby land points can't bridge it)
    (30.05, 30.45, -90.55, -89.65),
    # Corpus Christi Bay TX
    (27.7, 27.95, -97.5, -97.05),
    # Matagorda Bay TX
    (28.45, 28.8, -96.75, -96.15),
    # Pamlico Sound / Albemarle Sound NC
    (35.0, 36.1, -76.85, -75.65),
    # Chesapeake Bay
    (36.8, 39.5, -76.5, -75.8),
    # Puget Sound
    (47.0, 48.8, -123.5, -122.0),
    # San Francisco Bay
    (37.4, 38.2, -122.5, -121.9),
    # Long Island Sound
    (40.8, 41.4, -73.8, -71.8),
    # Delaware Bay
    (38.7, 39.6, -75.6, -74.9),
    # Mobile Bay / Pensacola Bay
    (30.0, 30.8, -88.2, -87.3),
    # Galveston Bay / Houston Ship Channel
    (29.3, 29.9, -95.1, -94.4),
]

# Known land points near coasts — locations within the water bounding boxes
# that are actually on land (e.g. cities on Great Lakes shores).
# If an RX position is within 5 km of a land point, it's allowed.
_COASTAL_LAND_POINTS: list[tuple[float, float]] = [
    # Great Lakes cities
    (41.88, -87.63),   # Chicago
    (42.33, -83.05),   # Detroit
    (41.50, -81.69),   # Cleveland
    (43.16, -79.24),   # Niagara Falls
    (42.89, -78.88),   # Buffalo
    (44.98, -93.27),   # Minneapolis
    (43.04, -87.91),   # Milwaukee
    (42.96, -85.67),   # Grand Rapids
    (46.79, -92.10),   # Duluth
    (43.05, -89.40),   # Madison WI
    (44.51, -88.01),   # Green Bay WI
    (42.96, -82.45),   # Port Huron MI
    (44.27, -85.60),   # Cadillac MI
    (42.26, -85.59),   # Kalamazoo MI
    (41.66, -83.56),   # Toledo OH / Maumee Bay
    (43.96, -77.96),   # Oswego NY
    (44.70, -75.48),   # Ogdensburg NY
    (43.45, -76.51),   # Oswego / Pulaski NY
    # Gulf Coast cities
    (29.76, -95.36),   # Houston
    (30.27, -97.74),   # Austin
    (30.45, -91.19),   # Baton Rouge
    (30.00, -90.07),   # New Orleans
    (27.95, -82.46),   # Tampa
    (25.76, -80.19),   # Miami
    (28.54, -81.38),   # Orlando
    (30.33, -81.66),   # Jacksonville
    (27.77, -82.64),   # St. Petersburg
    (29.42, -98.49),   # San Antonio
    (30.39, -87.69),   # Pensacola
    (30.22, -92.02),   # Lafayette
    (29.95, -90.07),   # New Orleans Lakeshore
    (30.69, -88.04),   # Mobile AL
    (29.70, -95.01),   # Pasadena TX
    (29.55, -95.13),   # League City TX
    # Tampa Bay shores (box: 27.35-28.1°N, -82.85 to -82.4°W)
    # NOTE: only inland / peninsula cities — do NOT add right-on-shore suburbs
    # (Ruskin, Apollo Beach, Gibsonton) because at any positive radius their
    # circle extends into the bay and exempts mid-bay positions.
    (27.97, -82.80),   # Clearwater FL
    (28.02, -82.77),   # Dunedin FL
    (27.99, -82.69),   # Safety Harbor FL
    (27.94, -82.29),   # Brandon FL
    (27.87, -82.33),   # Riverview FL
    (27.52, -82.57),   # Palmetto FL
    (27.50, -82.57),   # Bradenton FL
    (27.34, -82.54),   # Sarasota FL
    # Charlotte Harbor shores (box: 26.5-27.1°N, -82.35 to -81.85°W)
    (27.09, -82.43),   # Venice FL (north edge)
    (26.93, -82.05),   # Port Charlotte FL
    (26.63, -81.87),   # Cape Coral FL (east)
    (26.71, -81.93),   # Punta Gorda FL
    # Sarasota Bay (box: 27.1-27.55°N, -82.75 to -82.5°W)
    (27.34, -82.54),   # Sarasota FL (already above, reuses)
    (27.48, -82.57),   # North Port FL
    # Lake Pontchartrain shores (box: 30.05-30.45°N, -90.55 to -89.65°W)
    (30.07, -89.93),   # Slidell LA (east shore)
    (30.43, -90.10),   # Mandeville LA (north shore)
    (30.20, -90.23),   # Metairie / Kenner LA (south shore)
    (30.18, -89.75),   # Bay St. Louis MS
    # Corpus Christi Bay (box: 27.7-27.95°N, -97.5 to -97.05°W)
    (27.80, -97.40),   # Corpus Christi TX
    (27.73, -97.14),   # Portland TX
    (27.86, -97.08),   # Ingleside TX
    # Matagorda Bay (box: 28.45-28.8°N, -96.75 to -96.15°W)
    (28.69, -96.00),   # El Campo / Bay City TX
    (28.60, -96.10),   # Palacios TX
    (28.72, -96.67),   # Bay City area
    # Pamlico / Albemarle Sound (box: 35.0-36.1°N, -76.85 to -75.65°W)
    (35.54, -77.07),   # Greenville NC (west)
    (35.10, -76.89),   # New Bern NC (southwest)
    (36.07, -76.77),   # Elizabeth City NC (north)
    (36.00, -75.68),   # Kill Devil Hills / OBX NC (east shore)
    (35.26, -75.71),   # Ocracoke Island NC (southeast)
    # Florida Atlantic coast cities (for FL Atlantic water box)
    (26.12, -80.14),   # Fort Lauderdale FL
    (26.36, -80.08),   # Boca Raton FL
    (26.72, -80.05),   # West Palm Beach FL
    (27.20, -80.25),   # Stuart / Treasure Coast FL
    (27.64, -80.40),   # Vero Beach FL
    (28.08, -80.61),   # Melbourne / Brevard County FL
    (28.45, -80.79),   # Cocoa / Rockledge FL
    (28.61, -80.82),   # Titusville / Merritt Island FL
    (29.03, -80.93),   # New Smyrna Beach FL
    (29.21, -81.00),   # Daytona Beach FL
    (29.89, -81.31),   # St. Augustine FL
    (30.28, -81.39),   # Jacksonville Beach FL
    # Eastern seaboard
    (38.91, -77.04),   # Washington DC
    (39.95, -75.16),   # Philadelphia
    (40.71, -74.01),   # New York
    (42.36, -71.06),   # Boston
    (36.85, -75.98),   # Norfolk
    (32.78, -79.93),   # Charleston SC
    (34.22, -77.91),   # Wilmington NC
    (33.45, -75.96),   # outer banks NC (off coast; excluded)
    (38.32, -75.09),   # Ocean City MD
    (39.94, -74.07),   # Toms River NJ
    (40.92, -72.64),   # Long Island NY (east)
    (41.27, -72.89),   # New Haven CT
    (41.46, -71.31),   # Providence RI
    (43.66, -70.25),   # Portland ME
    (44.80, -68.77),   # Bangor ME
    (44.42, -73.14),   # Burlington VT (Lake Champlain)
    (43.09, -76.15),   # Syracuse NY
    (42.45, -76.51),   # Ithaca NY
    (44.18, -76.49),   # Kingston ON / Wolfe Island
    # Pacific coast
    (34.05, -118.24),  # Los Angeles
    (37.77, -122.42),  # San Francisco
    (47.61, -122.33),  # Seattle
    (45.52, -122.68),  # Portland
    (33.45, -117.61),  # San Clemente
    (32.72, -117.16),  # San Diego
    (36.60, -121.89),  # Monterey CA
    (35.38, -120.85),  # San Luis Obispo CA
    (34.41, -119.69),  # Santa Barbara CA
    (33.99, -118.46),  # Santa Monica CA
    (37.97, -122.52),  # San Rafael CA
    (37.52, -122.05),  # Fremont CA
    (37.34, -121.88),  # San Jose CA
    (38.58, -121.49),  # Sacramento CA (inland, near Sacramento River delta)
    (48.52, -122.62),  # Anacortes WA
    (48.11, -122.76),  # Port Townsend WA
    (46.14, -123.83),  # Astoria OR
    (44.63, -124.06),  # Newport OR
    (42.32, -122.87),  # Medford OR
]


_LAND_CHECK = None
_LAND_CHECK_LOADED = False


def _get_land_check():
    """Lazily build a global coastline water-check from bundled Natural Earth data.

    Uses shapely + Natural Earth polygons (10 m land, 50 m lakes): a point is
    water if it lies outside every land polygon OR inside a lake polygon.  This
    covers the whole world (oceans, seas, bays, and the Great Lakes), unlike the
    US-only bounding boxes.  Loading costs ~120 MB RAM and ~0.5 s, so it is
    deferred to the first call.  Returns None when shapely or the data files are
    unavailable, in which case callers fall back to the bounding boxes.
    """
    global _LAND_CHECK, _LAND_CHECK_LOADED
    if _LAND_CHECK_LOADED:
        return _LAND_CHECK
    _LAND_CHECK_LOADED = True

    try:
        from pathlib import Path
        from shapely.geometry import shape, Point
        from shapely import STRtree
    except ImportError:
        logging.getLogger(__name__).warning(
            "shapely not installed; receiver water-rejection falls back to "
            "US-only bounding boxes (no global coastline)."
        )
        return None

    data_dir = Path(__file__).parent / "data"
    try:
        def _load(name):
            with open(data_dir / name) as f:
                return [shape(feat["geometry"]) for feat in json.load(f)["features"]]
        land = _load("ne_10m_land.geojson")
        lakes = _load("ne_50m_lakes.geojson")
    except (OSError, ValueError) as exc:
        logging.getLogger(__name__).warning(
            "Natural Earth data unavailable (%s); water-rejection falls back "
            "to bounding boxes.", exc,
        )
        return None

    land_tree = STRtree(land)
    lake_tree = STRtree(lakes)

    def _is_water(lat: float, lon: float) -> bool:
        p = Point(lon, lat)
        if not any(land[i].covers(p) for i in land_tree.query(p)):
            return True  # outside all land → ocean/sea
        return any(lakes[i].covers(p) for i in lake_tree.query(p))  # inland lake

    _LAND_CHECK = _is_water
    return _LAND_CHECK


def _is_on_water(lat: float, lon: float) -> bool:
    """True if (lat, lon) is over water (ocean, sea, or large lake).

    Primary check is shapely + Natural Earth polygons (global coastline, lakes
    included).  Falls back to the US bounding boxes below when shapely or the
    bundled data is unavailable.
    """
    check = _get_land_check()
    if check is not None:
        return check(lat, lon)
    return _is_on_water_boxes(lat, lon)


def _is_on_water_boxes(lat: float, lon: float) -> bool:
    """Bounding-box fallback for US oceans, the Great Lakes, and large bays."""
    for lat_min, lat_max, lon_min, lon_max in _WATER_BOXES:
        if lat_min <= lat <= lat_max and lon_min <= lon <= lon_max:
            # Check if near a known coastal land point.
            # 6km radius: tight enough that shore-city circles don't bridge wide bays.
            for land_lat, land_lon in _COASTAL_LAND_POINTS:
                if _haversine_km(lat, lon, land_lat, land_lon) < 6.0:
                    return False
            return True
    return False


def _candidate_is_safe(
    lat: float,
    lon: float,
    display_node_id: str | None = None,
) -> bool:
    if _is_on_water(lat, lon):
        return False
    if not display_node_id:
        return True
    dlat, dlon = _node_display_fuzz(display_node_id)
    return not _is_on_water(lat + dlat, lon + dlon)


def _place_rx_on_land(
    tx_lat: float, tx_lon: float,
    dist_min_km: float = 5.0, dist_max_km: float = 40.0,
    max_attempts: int = 80,
    display_node_id: str | None = None,
) -> tuple[float, float]:
    """Place an RX position near a tower, rejecting water locations."""
    R = 6371.0
    for _ in range(max_attempts):
        distance_km = random.uniform(dist_min_km, dist_max_km)
        bearing_rad = random.uniform(0, 2 * math.pi)
        dlat = (distance_km * math.cos(bearing_rad)) / R
        dlon = (distance_km * math.sin(bearing_rad)) / (
            R * math.cos(math.radians(tx_lat))
        )
        rx_lat = tx_lat + math.degrees(dlat)
        rx_lon = tx_lon + math.degrees(dlon)
        if _candidate_is_safe(rx_lat, rx_lon, display_node_id):
            return (round(rx_lat, 6), round(rx_lon, 6))
    for step_km in range(5, 155, 5):
        for bearing_deg in range(0, 360, 15):
            bearing_rad = math.radians(bearing_deg)
            dlat = (step_km * math.cos(bearing_rad)) / R
            dlon = (step_km * math.sin(bearing_rad)) / (
                R * math.cos(math.radians(tx_lat))
            )
            rx_lat = tx_lat + math.degrees(dlat)
            rx_lon = tx_lon + math.degrees(dlon)
            if _candidate_is_safe(rx_lat, rx_lon, display_node_id):
                return (round(rx_lat, 6), round(rx_lon, 6))
    if _candidate_is_safe(tx_lat, tx_lon, display_node_id):
        return (round(tx_lat, 6), round(tx_lon, 6))
    coastal_points = sorted(
        _COASTAL_LAND_POINTS,
        key=lambda point: _haversine_km(tx_lat, tx_lon, point[0], point[1]),
    )
    for land_lat, land_lon in coastal_points:
        if _candidate_is_safe(land_lat, land_lon, display_node_id):
            return (round(land_lat, 6), round(land_lon, 6))
    return (round(tx_lat, 6), round(tx_lon, 6))


def _subtended_deg(from_lat, from_lon, a, b) -> float:
    """Angle between two towers as seen from a point, in degrees.

    This — not the towers' bearing separation from the metro centre — is what
    conditions a two-illuminator fix.  The bistatic range gradient is
    b = u_tx + u_rx, so two measurements are independent to the extent their
    transmitters lie in different directions *from the target*.  Two towers on
    a similar bearing from the metro core but far apart in range still subtend
    a usable angle across most of the coverage area.
    """
    ba = _bearing_between(from_lat, from_lon, a[0], a[1])
    bb = _bearing_between(from_lat, from_lon, b[0], b[1])
    return abs((ba - bb + 180.0) % 360.0 - 180.0)


def _beam_footprint(rx_lat, rx_lon, beam_azimuth_deg, beam_width_deg, max_range_km):
    """Sample points across a receiver's beam, for evaluating pair geometry.

    Deliberately samples the *edges* as well as the centre: a pair can condition
    well at boresight and collapse at the beam edge, and selecting on a single
    representative point would bake that blind spot in.
    """
    R = 6371.0
    pts = []
    half = beam_width_deg / 2.0
    for frac in (0.35, 0.7, 1.0):
        for off in (-half, -half / 2, 0.0, half / 2, half):
            br = math.radians((beam_azimuth_deg + off) % 360.0)
            d = max_range_km * frac
            pts.append((
                rx_lat + math.degrees((d * math.cos(br)) / R),
                rx_lon + math.degrees(
                    (d * math.sin(br)) / (R * math.cos(math.radians(rx_lat)))
                ),
            ))
    return pts


def _pick_illuminator_pair(rx_lat, rx_lon, beam_azimuth_deg, beam_width_deg,
                           max_range_km, towers, min_eirp_dbm):
    """Choose the two towers giving the best-conditioned pair for this receiver.

    Scored on the *worst* subtended angle across the beam footprint rather than
    the mean, because a pair that degenerates anywhere in the footprint is
    unreliable there.  Towers are already one-per-site in _TOWERS_US; co-sited
    transmitters would share an ellipse and be worthless as a pair.

    Returns (tower_a, tower_b) or None when nothing clears min_eirp_dbm.
    """
    usable = [t for t in towers if _TOWER_EIRP_DBM.get(t[4], _DEFAULT_EIRP_DBM) >= min_eirp_dbm]
    if len(usable) < 2:
        return None
    probes = _beam_footprint(rx_lat, rx_lon, beam_azimuth_deg, beam_width_deg, max_range_km)
    best, best_score = None, -1.0
    for i in range(len(usable)):
        for j in range(i + 1, len(usable)):
            a, b = usable[i], usable[j]
            if _haversine_km(a[0], a[1], b[0], b[1]) < 1.0:
                continue  # same mast
            worst = min(_subtended_deg(p[0], p[1], a, b) for p in probes)
            if worst > best_score:
                best, best_score = (a, b), worst
    return best


def _generate_dual_sites(
    n_sites: int,
    core_lat: float,
    core_lon: float,
    towers: list[tuple],
    metro_radius_km: float,
    prefix: str = "synth-GVL",
    beam_width_deg: float = 41.0,
    max_bistatic_range_km: float = 60.0,
    min_eirp_dbm: float = 40.0,
    aim: str = "core",
    aim_jitter_deg: float = 30.0,
) -> list[dict]:
    """Generate n_sites receivers, each running two nodes on two illuminators.

    This is how a real passive-radar site is built: one antenna, one RX
    position, several receiver chains tuned to different transmitters.  The two
    nodes therefore share rx position, altitude, beam azimuth, beam width and
    range — they differ only in which tower they listen to.

    The geometric payoff is that the two bistatic ellipses share a focus (the
    common RX), so they intersect in at most two points and the beam almost
    always excludes one.  A single site localises on its own, with the residual
    ambiguity bounded by the antenna pattern rather than by a second receiver
    tens of km away.

    Position is well determined this way; velocity is not.  One pair gives two
    Doppler projections for three velocity components, so it stays
    under-determined unless the level-flight assumption is applied (which the
    association stage does).  Two pairs — four nodes — give eight residuals
    against five unknowns, and only then do the solver's residual gates regain
    the discriminating power they lack at n=2.

    Sites are placed at random across the metro rather than ringed around a
    core, so their beams overlap each other far less than a ring's do.
    """
    if n_sites <= 0 or len(towers) < 2:
        return []

    nodes = []
    for i in range(n_sites):
        node_id = f"{prefix}-{i + 1:04d}"
        rx_lat, rx_lon = _place_rx_on_land(
            core_lat, core_lon,
            dist_min_km=5.0,
            dist_max_km=max(10.0, metro_radius_km * 0.85),
            display_node_id=node_id,
        )
        # Aim.  "random" spreads sectors and minimises inter-site overlap, but
        # a beam pointed away from the traffic sees nothing: with 85% of
        # aircraft routed through the metro core, random aiming left most sites
        # idle and collapsed the solve rate by an order of magnitude.
        #
        # "core" aims at the core with jitter, which is also what a real
        # operator would do — receivers are sited to cover the airspace of
        # interest.  Inter-site overlap is not the enemy here the way it is for
        # a ring: each dual site already self-solves from its own two
        # illuminators, so a second site overlapping it upgrades the fix to
        # four nodes rather than manufacturing a two-node ambiguity.
        if aim == "random":
            beam_azimuth = random.uniform(0.0, 360.0)
        else:
            beam_azimuth = (
                _bearing_between(rx_lat, rx_lon, core_lat, core_lon)
                + random.uniform(-aim_jitter_deg, aim_jitter_deg)
            ) % 360.0
        pair = _pick_illuminator_pair(
            rx_lat, rx_lon, beam_azimuth, beam_width_deg,
            max_bistatic_range_km, towers, min_eirp_dbm,
        )
        if pair is None:
            continue
        rx_alt_ft = round(random.uniform(100, 1500), 1)
        for suffix, tower in zip("ab", pair):
            tx_lat, tx_lon, tx_alt_ft, fc_hz, callsign = tower
            node = GeneratedNodeConfig(
                node_id=f"{node_id}{suffix}",
                rx_lat=round(rx_lat, 6),
                rx_lon=round(rx_lon, 6),
                rx_alt_ft=rx_alt_ft,
                tx_lat=tx_lat,
                tx_lon=tx_lon,
                tx_alt_ft=tx_alt_ft,
                fc_hz=fc_hz,
                fs_hz=2_000_000,
                beam_width_deg=round(beam_width_deg, 1),
                max_range_km=round(max_bistatic_range_km, 1),
                region="us",
                tx_callsign=callsign,
                beam_azimuth_deg=round(beam_azimuth, 2),
                max_bistatic_range_km=round(max_bistatic_range_km, 1),
            )
            nodes.append(_node_dict(node))
    return nodes


def _generate_metro_solo(
    n: int,
    core_lat: float,
    core_lon: float,
    towers: list[tuple],
    metro_radius_km: float,
    prefix: str = "synth-SOLO",
    beam_width_deg: float = 40.0,
    max_bistatic_range_km: float = 60.0,
    ring_radius_km: float = 18.0,
    start_bearing_deg: float = 30.0,
) -> list[dict]:
    """Generate n isolated receivers on the metro rim, each aimed outward.

    These exist to keep the single-node ellipse-arc path exercised.  Every
    receiver in the coverage ring aims *inward* at the core, so their beams all
    intersect and essentially every detection associates into a multinode
    solve — leaving the single-node arc code with almost no live coverage.

    Isolation here is by beam geometry, not distance: the metro is far too
    small for the nationwide pool's 400 km separation.  Each solo RX sits near
    the rim and points away from the core, so its sector cannot intersect the
    inward-aimed ring beams no matter how the range circles overlap.  The
    association overlap zone is computed from beam sectors
    (compute_overlap_zone), so this is the property that actually decides
    whether detections stay single-node.

    Each also takes its own illuminator rather than the shared ring TX, which
    puts its Doppler outside the association gate — a second, independent
    reason not to pair.
    """
    if n <= 0 or not towers:
        return []

    R = 6371.0
    # Sit between the ring envelope and the metro edge.  Far enough out that
    # the outward beam looks away from ring airspace; inside the metro radius
    # so the node stays on the map with the rest of the fleet.
    rim_km = max(ring_radius_km * 2.0, metro_radius_km * 0.80)

    nodes = []
    for i in range(n):
        # Offset from the ring's own start bearing so a solo node never lands
        # on top of a ring receiver.
        bearing_deg = (start_bearing_deg + 360.0 * i / max(n, 1)) % 360.0
        bearing_rad = math.radians(bearing_deg)
        dlat = (rim_km * math.cos(bearing_rad)) / R
        dlon = (rim_km * math.sin(bearing_rad)) / (R * math.cos(math.radians(core_lat)))
        rx_lat = core_lat + math.degrees(dlat)
        rx_lon = core_lon + math.degrees(dlon)

        node_id = f"{prefix}-{i + 1:04d}"
        if not _candidate_is_safe(rx_lat, rx_lon, node_id):
            rx_lat, rx_lon = _place_rx_on_land(
                core_lat, core_lon,
                dist_min_km=rim_km - 10,
                dist_max_km=rim_km + 10,
                display_node_id=node_id,
            )

        # Pick the illuminator *furthest* from the core among the metro towers:
        # its baseline points away from ring airspace, so the bistatic ellipse
        # opens outward too rather than folding back over the mesh.
        tower = max(
            towers,
            key=lambda t: _haversine_km(t[0], t[1], rx_lat, rx_lon),
        )
        tx_lat, tx_lon, tx_alt_ft, fc_hz, callsign = tower

        # Aim directly away from the core — the isolation property.
        beam_azimuth = (_bearing_between(rx_lat, rx_lon, core_lat, core_lon) + 180.0) % 360.0

        node = GeneratedNodeConfig(
            node_id=node_id,
            rx_lat=round(rx_lat, 6),
            rx_lon=round(rx_lon, 6),
            rx_alt_ft=round(random.uniform(100, 1500), 1),
            tx_lat=tx_lat,
            tx_lon=tx_lon,
            tx_alt_ft=tx_alt_ft,
            fc_hz=fc_hz,
            fs_hz=2_000_000,
            beam_width_deg=round(beam_width_deg, 1),
            max_range_km=round(max_bistatic_range_km, 1),
            region="us",
            tx_callsign=callsign,
            beam_azimuth_deg=round(beam_azimuth, 2),
            max_bistatic_range_km=round(max_bistatic_range_km, 1),
        )
        nodes.append(_node_dict(node))

    return nodes


def _generate_coverage_ring(
    n: int,
    core_lat: float,
    core_lon: float,
    tx_tower: tuple,
    prefix: str = "synth-RING",
    radius_km: float = 18.0,
    beam_width_deg: float = 50.0,
    max_range_km: float = 60.0,
    aim: str = "core",
    start_bearing_deg: float = 0.0,
) -> list[dict]:
    """Generate n receivers on a ring around a metro core, all sharing one TX.

    Each RX sits radius_km from the core at an evenly spaced bearing and (for
    aim="core") points its Yagi at the core. The union of inward beams covers
    the core airspace from diverse look angles, so the bistatic range gradients
    span well — low GDOP and observable velocity even at n=2, unlike a
    co-located cluster whose gradients are near-parallel. One shared low-band
    illuminator keeps per-node Doppler inside the analytics association gate.
    """
    tx_lat, tx_lon, tx_alt_ft, fc_hz, callsign = tx_tower
    R = 6371.0

    nodes = []
    for i in range(n):
        bearing_rad = math.radians((start_bearing_deg + 360.0 * i / n) % 360.0)
        dlat = (radius_km * math.cos(bearing_rad)) / R
        dlon = (radius_km * math.sin(bearing_rad)) / (
            R * math.cos(math.radians(core_lat))
        )
        rx_lat = core_lat + math.degrees(dlat)
        rx_lon = core_lon + math.degrees(dlon)

        node_id = f"{prefix}-{i + 1:04d}"
        if not _candidate_is_safe(rx_lat, rx_lon, node_id):
            rx_lat, rx_lon = _place_rx_on_land(
                core_lat, core_lon,
                dist_min_km=max(5.0, radius_km - 5),
                dist_max_km=radius_km + 5,
                display_node_id=node_id,
            )

        if aim == "broadside":
            beam_azimuth = (_bearing_between(rx_lat, rx_lon, tx_lat, tx_lon) + 90.0) % 360.0
        else:
            beam_azimuth = _bearing_between(rx_lat, rx_lon, core_lat, core_lon)

        node = GeneratedNodeConfig(
            node_id=node_id,
            rx_lat=round(rx_lat, 6),
            rx_lon=round(rx_lon, 6),
            rx_alt_ft=round(random.uniform(100, 1500), 1),
            tx_lat=tx_lat,
            tx_lon=tx_lon,
            tx_alt_ft=tx_alt_ft,
            fc_hz=fc_hz,
            fs_hz=2_000_000,
            beam_width_deg=round(beam_width_deg, 1),
            max_range_km=round(max_range_km, 1),
            region="us",
            tx_callsign=callsign,
            beam_azimuth_deg=round(beam_azimuth, 2),
            # Ring receivers model real passive-radar reach, so they are limited
            # on bistatic range.  max_range_km stays for consumers that have not
            # been taught the bistatic rule.
            max_bistatic_range_km=round(max_range_km, 1),
        )
        nodes.append(_node_dict(node))

    return nodes


def _resolve_metro(metro: str) -> dict:
    """Resolve a metro code (e.g. "gvl") to its descriptor in _KNOWN_METROS."""
    key = metro.strip().lower()
    if key not in _KNOWN_METROS:
        raise ValueError(
            f"Unknown metro {metro!r} (available: {', '.join(sorted(_KNOWN_METROS))})"
        )
    return _KNOWN_METROS[key]


def _towers_in_metro(towers: list, metro: dict) -> list:
    """Towers within the metro's own radius of its centre."""
    radius_km = metro["radius_nm"] * _NM_TO_KM
    return [
        t for t in towers
        if _haversine_km(t[0], t[1], metro["lat"], metro["lon"]) <= radius_km
    ]


def _rings_in_metro(ring_spec: list, metro: dict) -> list:
    """Ring specs whose airspace core lies inside the metro.

    Cores are matched (not the illuminators) because the core is what the ring
    and its coverage cell are built around — a TX can legitimately sit outside
    the metro radius while lighting an airspace inside it.
    """
    radius_km = metro["radius_nm"] * _NM_TO_KM
    return [
        s for s in ring_spec
        if _haversine_km(s[5], s[6], metro["lat"], metro["lon"]) <= radius_km
    ]


def _active_rings(n_cluster: int, n_clusters: int, ring_spec: list = _RING_TXS):
    """Yield (ring_id, spec, size) for each ring the budget actually produces.

    Single source of truth for which metros get rings and how many receivers
    each — used by both the node generator and coverage_cells so the cells can
    never disagree with the nodes (no reverse-engineering from node positions).
    """
    if n_cluster <= 0 or n_clusters <= 0:
        return
    k = min(n_clusters, len(ring_spec))
    base, rem = divmod(n_cluster, k)
    for ci in range(k):
        size = base + (1 if ci < rem else 0)
        if size <= 0:
            continue
        ring_id = "synth-RING" if k == 1 else f"synth-RING{ci + 1}"
        yield ring_id, ring_spec[ci], size


def coverage_cells(
    n_cluster: int = 8,
    n_clusters: int = 1,
    ring_spec: list = _RING_TXS,
    traffic_radius_km: float = 70.0,
    metro: Optional[str] = None,
) -> list[dict]:
    """First-class metro-cell descriptors for the active rings.

    The cell core is the airspace centre from the ring spec (the true airport),
    never reconstructed from receiver positions — so water-displaced receivers
    cannot drift the hub-radial aim point. ops_weight defaults to ring size; a
    caller with real traffic figures can override the spec to inject ops/yr.

    When *metro* is set the spec is narrowed to that metro's rings with the same
    filter generate_fleet uses, so the cells always describe the nodes that were
    actually generated.
    """
    if metro:
        ring_spec = _rings_in_metro(ring_spec, _resolve_metro(metro))
        n_clusters = min(n_clusters, len(ring_spec))
    cells = []
    for ring_id, spec, size in _active_rings(n_cluster, n_clusters, ring_spec):
        tx_lat, tx_lon, tx_alt_ft, fc_hz, callsign, core_lat, core_lon = spec
        cells.append({
            "ring_id": ring_id,
            "core_lat": core_lat,
            "core_lon": core_lon,
            "radius_km": traffic_radius_km,
            "ops_weight": float(size),
            "illuminator": callsign,
        })
    return cells


def generate_fleet(
    n_nodes: int = 200,
    regions: Optional[list[str]] = None,
    seed: int = 42,
    solo_fraction: float = 0.10,
    use_tower_api: bool = True,
    n_cluster: int = 8,
    n_clusters: int = 1,
    ring_radius_km: float = 18.0,
    ring_beam_width_deg: float = 50.0,
    ring_max_range_km: float = 60.0,
    ring_aim: str = "core",
    ring_spec: list = _RING_TXS,
    metro: Optional[str] = None,
    layout: str = "ring",
    illuminator_band: str = "any",
    dual_min_eirp_dbm: float = 40.0,
    dual_aim: str = "core",
) -> list[dict]:
    """Generate a fleet of synthetic node configurations.

    Nodes are distributed across the requested regions, each associated
    with a nearby broadcast tower as its illumination source. Receivers
    are placed 5-40 km from the tower (realistic for passive radar).

    When use_tower_api is True (default), each metro area queries the
    Tower Search API (towers.retina.fm) to get multiple real towers,
    so nodes in the same city use DIFFERENT towers. Falls back to the
    hardcoded tower list when the API is unreachable or returns no results.

    A fraction of nodes (solo_fraction, default 10%) are placed at isolated
    rural towers far from any other nodes, useful for testing single-node
    ellipse-arc function without overlapping detection zones.

    n_cluster receivers (default 8) form coverage rings — split across n_clusters
    metros, each a ring around the metro core aimed inward and sharing one low-band
    illuminator. Diverse look angles give low-GDOP, velocity-observable multinode
    fixes. Ring slots are carved out of the metro allocation so total stays n_nodes.

    Args:
        n_nodes: Total nodes to generate (100-1000).
        regions: List of regions to distribute across ["us", "eu", "au"].
        seed: Random seed for reproducibility.
        solo_fraction: Fraction of nodes allocated as solo/isolated (0.0-1.0).
        use_tower_api: Query towers.retina.fm for diverse per-node towers.
        n_cluster: Total coverage-ring receivers (split across n_clusters metros).
        n_clusters: Number of metro coverage rings.
        ring_radius_km: Receiver ring radius around each metro core.
        ring_beam_width_deg: Yagi half-power beamwidth for ring receivers.
        ring_max_range_km: Detection range for ring receivers.
        ring_aim: "core" (aim at metro core) or "broadside" (perp to TX).
        ring_spec: Metro ring table (defaults to _RING_TXS); inject to add metros
            or change illuminators without editing library source.
        metro: Restrict the whole fleet to one metro area (a _KNOWN_METROS code
            such as "gvl"). Towers and rings outside that metro's radius are
            dropped and solo/rural placement is disabled, so every node lands in
            the one metro. None (default) keeps the continent-wide behaviour.

    Returns:
        List of node config dicts ready for fleet_config.json.
    """
    if regions is None:
        regions = ["us"]

    random.seed(seed)

    metro_area = _resolve_metro(metro) if metro else None

    tower_db = {
        "us": _TOWERS_US,
        "eu": _TOWERS_EU,
        "au": _TOWERS_AU,
    }

    # Solo towers — only available for US region (where rural towers are defined).
    #
    # The nationwide pool separates receivers by 400 km, which cannot apply
    # inside a metro, so --metro uses metro-scoped solo placement instead (see
    # _metro_solo_nodes below).  Both exist for the same reason: solo receivers
    # are the only way to exercise the single-node ellipse-arc path.  Without
    # them every detection lands in an overlap zone and associates into a
    # multinode solve — measured on the Greenville fleet as 15 of 16 nodes
    # overlapping 1-11 neighbours, and single-node arcs nearly absent.
    solo_towers = _TOWERS_SOLO_US if ("us" in regions and not metro_area) else []

    # Distribute nodes across regions proportionally to tower count
    available_towers = []
    for region in regions:
        towers = tower_db.get(region, [])
        if metro_area:
            towers = _towers_in_metro(towers, metro_area)
        for t in towers:
            available_towers.append((region, t))

    if not available_towers:
        if metro_area:
            raise ValueError(
                f"No towers within {metro_area['radius_nm']} nm of "
                f"{metro_area['name']} for regions: {regions}"
            )
        raise ValueError(f"No towers available for regions: {regions}")

    if metro_area:
        ring_spec = _rings_in_metro(ring_spec, metro_area)
        n_clusters = min(n_clusters, len(ring_spec))

    # ── Pre-fetch real towers from Tower API for metro areas ──────────────────
    # Each metro area gets multiple real towers so nodes in the same city
    # use DIFFERENT transmitters instead of all sharing the same one.
    metro_api_towers: dict[str, list[dict]] = {}
    def _cache_key(lat, lon):
        return f"{lat:.4f},{lon:.4f}"
    if use_tower_api:
        try:
            try:
                from retina_simulation.tower_resolver import lookup_metro_towers
            except ImportError:
                from tower_resolver import lookup_metro_towers
            # Only the towers actually in play — under --metro this is a handful
            # of centres instead of every metro on the continent.
            all_metro_centers = [t for _region, t in available_towers]
            metro_api_towers_raw = lookup_metro_towers(all_metro_centers, radius_km=80, limit=50)
            # Map back to (lat, lon) → tower list
            for t in all_metro_centers:
                key = _cache_key(t[0], t[1])
                if key in metro_api_towers_raw and metro_api_towers_raw[key]:
                    metro_api_towers[key] = metro_api_towers_raw[key]
        except Exception as exc:
            import logging
            logging.warning("Tower API lookup failed, using hardcoded towers: %s", exc)

    # Allocate solo and cluster node counts, carving both from metro allocation
    if solo_towers:
        n_solo = max(1, round(n_nodes * solo_fraction))
    elif metro_area:
        # Metro-scoped solo receivers, placed on the rim and aimed outward.
        n_solo = max(1, round(n_nodes * solo_fraction))
    else:
        n_solo = 0
    # No rings survived the metro filter → give their budget back to metro nodes
    # instead of silently generating fewer nodes than asked for.
    n_cluster = max(0, n_cluster) if n_clusters > 0 else 0
    n_metro = max(0, n_nodes - n_solo - n_cluster)

    # Track how many times each API tower has been used (per metro) for
    # round-robin distribution — avoids all nodes sharing one tower.
    _metro_tower_idx: dict[str, int] = {}

    nodes = []
    # --- Metro nodes (clustered near metro towers) ---
    for i in range(n_metro):
        region, tower = random.choice(available_towers)
        tx_lat, tx_lon, tx_alt_ft, fc_hz, callsign = tower

        # Try to use a DIFFERENT real tower from the Tower API for this node
        key = _cache_key(tx_lat, tx_lon)
        api_towers = metro_api_towers.get(key, [])
        if api_towers:
            idx = _metro_tower_idx.get(key, 0)
            t = api_towers[idx % len(api_towers)]
            _metro_tower_idx[key] = idx + 1
            tx_lat = t["tx_lat"]
            tx_lon = t["tx_lon"]
            tx_alt_ft = t["tx_alt_ft"]
            fc_hz = t["fc_hz"]
            callsign = t["tx_callsign"]

        # Metro-scoped fleets are named for the metro (synth-GVL-0001) rather
        # than the continent, so node IDs say where they actually are.
        region_prefix = metro.strip().upper() if metro else region.upper()
        node_id = f"synth-{region_prefix}-{i + 1:04d}"
        rx_lat, rx_lon = _place_rx_on_land(
            tx_lat,
            tx_lon,
            dist_min_km=5,
            dist_max_km=40,
            display_node_id=node_id,
        )
        rx_alt_ft = random.uniform(100, 2000)

        node_fc = fc_hz + random.choice([-500000, 0, 0, 0, 500000])
        beam_width = random.uniform(35, 45)
        max_range = random.uniform(35, 55)

        node = GeneratedNodeConfig(
            node_id=node_id,
            rx_lat=rx_lat,
            rx_lon=rx_lon,
            rx_alt_ft=round(rx_alt_ft, 1),
            tx_lat=tx_lat,
            tx_lon=tx_lon,
            tx_alt_ft=tx_alt_ft,
            fc_hz=node_fc,
            fs_hz=2_000_000,
            beam_width_deg=round(beam_width, 1),
            max_range_km=round(max_range, 1),
            region=region,
            tx_callsign=callsign,
        )
        nodes.append(_node_dict(node))

    # --- Solo nodes (isolated — strictly one node per unique tower position) ---
    if n_solo > 0 and metro_area:
        # Metro-scoped: isolation comes from aiming away from the core, not
        # from the nationwide pool's 400 km separation (impossible in a metro).
        nodes.extend(_generate_metro_solo(
            n=n_solo,
            core_lat=metro_area["lat"],
            core_lon=metro_area["lon"],
            towers=[t for _region, t in available_towers] or _TOWERS_US,
            metro_radius_km=metro_area["radius_nm"] * _NM_TO_KM,
            ring_radius_km=ring_radius_km,
            max_bistatic_range_km=ring_max_range_km,
        ))
    elif n_solo > 0:
        # All US positions that must be avoided when extending the pool
        # Avoid positions: metro towers only.  Named solo towers are gated
        # inside _extend_solo_pool with the same min_sep check so they are
        # also enforced to be at least min_sep_km from each other.
        # 400 km > 2 × 140 km fleet range → solo coverage circles never
        # touch each other or any metro cluster, making them visually isolated.
        us_metro_occ: list[tuple[float, float]] = [
            (t[0], t[1]) for t in _TOWERS_US
        ]
        solo_pool = _extend_solo_pool(
            list(solo_towers),
            n_solo,
            avoid_positions=us_metro_occ,
            min_sep_km=400.0,
        )
        random.shuffle(solo_pool)

        for j in range(min(n_solo, len(solo_pool))):
            tower = solo_pool[j]          # strict: never re-use a tower index
            tx_lat, tx_lon, tx_alt_ft, fc_hz, callsign = tower

            node_id = f"synth-SOLO-{j + 1:04d}"
            rx_lat, rx_lon = _place_rx_on_land(
                tx_lat,
                tx_lon,
                dist_min_km=8,
                dist_max_km=35,
                display_node_id=node_id,
            )
            rx_alt_ft = random.uniform(100, 1500)

            beam_width = random.uniform(35, 45)
            max_range = random.uniform(35, 55)

            node = GeneratedNodeConfig(
                node_id=node_id,
                rx_lat=round(rx_lat, 6),
                rx_lon=round(rx_lon, 6),
                rx_alt_ft=round(rx_alt_ft, 1),
                tx_lat=tx_lat,
                tx_lon=tx_lon,
                tx_alt_ft=tx_alt_ft,
                fc_hz=fc_hz,
                fs_hz=2_000_000,
                beam_width_deg=round(beam_width, 1),
                max_range_km=round(max_range, 1),
                region="us",
                tx_callsign=callsign,
            )
            nodes.append(_node_dict(node))

    # --- Coverage-ring nodes (dedicated multi-node detection group) ----------
    # Each metro gets a ring of RX around its airspace core, every RX aimed at
    # the core and sharing one low-band illuminator. Diverse look angles give
    # low GDOP / observable velocity (unlike a co-located cluster), and the
    # shared VHF TX keeps Doppler inside the association gate. Spreading the
    # budget across metros puts overlap coverage where traffic actually flies.
    ring_nodes = []
    if layout == "dual":
        # Dual-illuminator sites replace the coverage ring entirely: they are
        # two different answers to the same problem.  A ring buys geometry by
        # surrounding the airspace with receivers that all overlap; a dual site
        # buys it at the receiver, from two illuminators sharing one antenna.
        if metro_area:
            _dual_towers = [t for _r, t in available_towers] or _TOWERS_US
            if illuminator_band == "vhf":
                # All-VHF is a legitimate configuration to test on its own:
                # VHF is the better illuminator on physics, and restricting to
                # one band removes the cross-band question from the result.
                _vhf = [t for t in _dual_towers if t[3] < 300e6]
                if len(_vhf) >= 2:
                    _dual_towers = _vhf
            ring_nodes.extend(_generate_dual_sites(
                n_sites=max(0, n_cluster) // 2,
                core_lat=metro_area["lat"],
                core_lon=metro_area["lon"],
                towers=_dual_towers,
                metro_radius_km=metro_area["radius_nm"] * _NM_TO_KM,
                prefix=f"synth-{metro.strip().upper()}-DUAL" if metro else "synth-DUAL",
                beam_width_deg=ring_beam_width_deg,
                max_bistatic_range_km=ring_max_range_km,
                min_eirp_dbm=dual_min_eirp_dbm,
                aim=dual_aim,
            ))
        return nodes + ring_nodes
    for ring_id, spec, size in _active_rings(n_cluster, n_clusters, ring_spec):
        tx_lat, tx_lon, tx_alt_ft, fc_hz, callsign, core_lat, core_lon = spec
        ring_nodes.extend(_generate_coverage_ring(
            n=size,
            core_lat=core_lat,
            core_lon=core_lon,
            tx_tower=(tx_lat, tx_lon, tx_alt_ft, fc_hz, callsign),
            prefix=ring_id,
            radius_km=ring_radius_km,
            beam_width_deg=ring_beam_width_deg,
            max_range_km=ring_max_range_km,
            aim=ring_aim,
        ))
    nodes = ring_nodes + nodes   # prepend so ring IDs are first

    return nodes


def fleet_summary(nodes: list[dict]) -> dict:
    """Compute a summary of the fleet configuration."""
    from collections import Counter
    regions = Counter(n["region"] for n in nodes)
    towers = Counter(n["tx_callsign"] for n in nodes)
    return {
        "total_nodes": len(nodes),
        "regions": dict(regions),
        "unique_towers": len(towers),
        "towers_by_usage": dict(towers.most_common(20)),
        "lat_range": (
            round(min(n["rx_lat"] for n in nodes), 4),
            round(max(n["rx_lat"] for n in nodes), 4),
        ),
        "lon_range": (
            round(min(n["rx_lon"] for n in nodes), 4),
            round(max(n["rx_lon"] for n in nodes), 4),
        ),
    }


def main():
    parser = argparse.ArgumentParser(description="Generate fleet of synthetic node configs")
    parser.add_argument("--nodes", type=int, default=200, help="Number of nodes (100-1000)")
    parser.add_argument("--regions", type=str, default="us", help="Comma-separated regions: us,eu,au")
    parser.add_argument("--metro", type=str, default=None,
                        choices=sorted(_KNOWN_METROS),
                        help="Restrict the whole fleet to one metro area (drops solo/rural nodes)")
    parser.add_argument("--output", type=str, default="fleet_config.json", help="Output file path")
    parser.add_argument("--seed", type=int, default=42, help="Random seed")
    parser.add_argument("--layout", choices=("ring", "dual"), default="ring",
                        help="ring: receivers circling a core, all aimed inward. "
                             "dual: receivers scattered across the metro, each "
                             "running two nodes on two illuminators from one "
                             "antenna (n-cluster is the node budget, so half "
                             "that many sites).")
    parser.add_argument("--illuminator-band", choices=("any", "vhf"), default="any",
                        help="restrict dual-site illuminators to VHF")
    parser.add_argument("--dual-min-eirp-dbm", type=float, default=40.0,
                        help="floor on the weaker illuminator of a dual pair")
    parser.add_argument("--n-cluster", "--n-ring", dest="n_cluster", type=int, default=30,
                        help="Total coverage-ring receivers (split across rings)")
    parser.add_argument("--n-clusters", "--n-rings", dest="n_clusters", type=int, default=5,
                        help="Number of metro coverage rings (more = multinode spread across "
                             "the map). Capped at the number of rings that survive --metro.")
    parser.add_argument("--ring-radius-km", type=float, default=18.0,
                        help="Receiver ring radius around each metro core")
    parser.add_argument("--ring-beam-width-deg", type=float, default=50.0,
                        help="Yagi half-power beamwidth for ring receivers")
    parser.add_argument("--ring-max-range-km", type=float, default=60.0,
                        help="Detection range for ring receivers")
    parser.add_argument("--ring-aim", type=str, default="core", choices=["core", "broadside"],
                        help="Aim ring beams at the metro core or broadside to TX")
    args = parser.parse_args()

    regions = [r.strip().lower() for r in args.regions.split(",")]
    nodes = generate_fleet(n_nodes=args.nodes, regions=regions, seed=args.seed,
                           n_cluster=args.n_cluster, n_clusters=args.n_clusters,
                           layout=args.layout,
                           illuminator_band=args.illuminator_band,
                           dual_min_eirp_dbm=args.dual_min_eirp_dbm,
                           dual_aim=args.dual_aim,
                           ring_radius_km=args.ring_radius_km,
                           ring_beam_width_deg=args.ring_beam_width_deg,
                           ring_max_range_km=args.ring_max_range_km,
                           ring_aim=args.ring_aim,
                           metro=args.metro)
    cells = coverage_cells(n_cluster=args.n_cluster, n_clusters=args.n_clusters,
                           metro=args.metro)
    summary = fleet_summary(nodes)

    config = {
        "fleet": {
            "generated_at": __import__("datetime").datetime.now(__import__("datetime").timezone.utc).isoformat(),
            "summary": summary,
        },
        "nodes": nodes,
        "cells": cells,
    }

    with open(args.output, "w") as f:
        json.dump(config, f, indent=2)

    print(f"Generated {len(nodes)} nodes → {args.output}", file=sys.stderr)
    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
