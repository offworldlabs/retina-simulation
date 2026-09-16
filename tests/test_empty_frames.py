"""A node that sees nothing still reports.

The server marks its aircraft feed dirty on every frame it processes, whatever
the frame carried, and that is what keeps the map's websocket fed between
detections. A node that goes quiet when its beam is empty therefore reads as a
dead connection rather than a quiet sky.
"""

import asyncio

from retina_simulation.orchestrator import FleetOrchestrator

_NODE_ID = "synth-GVL-TEST-0001"
_NODE_CONFIG = {"node_id": _NODE_ID, "rx_lat": 34.85, "rx_lon": -82.39}


class _StubConnection:
    def __init__(self):
        self.connected = True
        self.sent: list[dict] = []

    async def send_detection(self, frame):
        self.sent.append(frame)

    async def send_heartbeat(self):
        pass


class _EmptyWorld:
    """A world in which nothing is ever in anyone's beam."""

    nodes: dict = {}
    aircraft: list = []

    def step(self, dt, mode=None):
        pass

    def get_aircraft_summary(self):
        return []

    def generate_detections_for_node(self, node_id, timestamp_ms):
        return {"timestamp": timestamp_ms, "delay": [], "doppler": [], "snr": []}


def _run_briefly(conn):
    """One or two ticks: the loop paces itself, so the count is not fixed."""
    fleet = FleetOrchestrator([_NODE_CONFIG])
    fleet.world = _EmptyWorld()
    fleet.connections = {_NODE_ID: conn}
    asyncio.run(fleet.run_simulation_loop(duration_s=0.001))
    return fleet


def test_a_node_with_an_empty_beam_still_sends_a_frame():
    conn = _StubConnection()

    _run_briefly(conn)

    assert conn.sent
    assert all(frame["delay"] == [] for frame in conn.sent)


def test_an_empty_frame_counts_as_a_frame_and_not_as_a_detection():
    conn = _StubConnection()

    fleet = _run_briefly(conn)

    assert conn.sent
    assert fleet._stats["total_frames"] == len(conn.sent)
    assert fleet._stats["total_detections"] == 0
