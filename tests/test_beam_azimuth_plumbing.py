"""NodeConfig.beam_azimuth_deg resolution in world.add_node.

None means auto-aim broadside to the RX->TX baseline; an explicit value is an
aimed Yagi and must be preserved. The detection cone follows whichever aim was
resolved.
"""

import math

from retina_simulation.world import (
    SimulationWorld, NodeConfig, SimulatedAircraft, _bearing_deg,
)

_RX_LAT, _RX_LON = 33.939182, -84.651910
_TX_LAT, _TX_LON = 33.75667, -84.331844


def _broadside_azimuth():
    return (_bearing_deg(_RX_LAT, _RX_LON, _TX_LAT, _TX_LON) + 90.0) % 360.0


def _aircraft_at_bearing(bearing_deg, dist_km=20.0):
    br = math.radians(bearing_deg)
    lat = _RX_LAT + math.degrees((dist_km * math.cos(br)) / 6371.0)
    lon = _RX_LON + math.degrees(
        (dist_km * math.sin(br)) / (6371.0 * math.cos(math.radians(_RX_LAT)))
    )
    return SimulatedAircraft(
        object_id="probe", lat=lat, lon=lon, alt_km=8.0,
        vel_east=0.0, vel_north=0.0, vel_up=0.0,
        heading_deg=0.0, speed_km_s=0.2,
    )


def _node(beam_azimuth_deg, beam_width_deg=41.0):
    return NodeConfig(
        node_id="plumb-node",
        rx_lat=_RX_LAT, rx_lon=_RX_LON,
        tx_lat=_TX_LAT, tx_lon=_TX_LON,
        beam_azimuth_deg=beam_azimuth_deg,
        beam_width_deg=beam_width_deg, max_range_km=50.0,
    )


class TestBeamAzimuthResolution:
    def test_none_resolves_to_broadside_float(self):
        world = SimulationWorld()
        world.add_node(_node(beam_azimuth_deg=None))
        resolved = world.nodes["plumb-node"].beam_azimuth_deg
        assert isinstance(resolved, float)
        assert abs(resolved - _broadside_azimuth()) < 1e-9

    def test_explicit_azimuth_is_preserved(self):
        world = SimulationWorld()
        world.add_node(_node(beam_azimuth_deg=123.0))
        assert world.nodes["plumb-node"].beam_azimuth_deg == 123.0


class TestDetectionConeRespectsAim:
    def test_on_aim_aircraft_detected(self):
        world = SimulationWorld()
        world.add_node(_node(beam_azimuth_deg=90.0, beam_width_deg=40.0))
        node = world.nodes["plumb-node"]
        assert world._aircraft_in_detection_cone(_aircraft_at_bearing(90.0), node)

    def test_off_aim_aircraft_outside_beamwidth_rejected(self):
        world = SimulationWorld()
        world.add_node(_node(beam_azimuth_deg=90.0, beam_width_deg=40.0))
        node = world.nodes["plumb-node"]
        assert not world._aircraft_in_detection_cone(_aircraft_at_bearing(180.0), node)
