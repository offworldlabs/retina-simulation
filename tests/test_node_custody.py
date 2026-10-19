"""Chain of custody in node.py on retina-custody's windowed hash chain.

DETECTION frames go out unsigned. Each closed window goes out as a CHAIN_ENTRY
with its frame batch, in the shape the custody spec's section 2 defines, and
must verify against the node's key and the previous entry. A restart inside
the last committed window opens no window before that one has ended.
"""

import _thread
import json
import os
import random
import socket
import sys
import threading
import time
from datetime import datetime, timezone

import pytest
from retina_custody import iq_buffer
from retina_custody.crypto_backend import SoftwareCryptoBackend
from retina_custody.hash_chain import DEFAULT_WINDOW_S, HashChainBuilder, HashChainVerifier
from retina_custody.models import HashChainEntry
from retina_custody.tsa_client import TimestampManager

from retina_simulation import node
from retina_simulation.node import NodeConfig, SyntheticNodeGenerator

WINDOW_S = 60
T0 = 1_800_000_000  # a multiple of WINDOW_S
NID = "synth-custody-01"

# Section 2 entry fields, with TSA off (no tsa_token or ots_proof).
ENTRY_KEYS = {
    "node_id",
    "window_start",
    "window_end",
    "prev_hash",
    "detections_hash",
    "n_detections",
    "node_config_hash",
    "firmware_version",
    "timestamp_utc",
    "entry_hash",
    "signature",
    "signing_mode",
}
CHAIN_ENTRY_KEYS = {"type", "node_id", "entry", "batch"}
SIGNING_FIELDS = {"signature", "payload_hash", "signing_mode", "public_key_fingerprint"}


class FakeClock:
    def __init__(self, t: float):
        self.t = t

    def __call__(self) -> float:
        return self.t

    def advance(self, s: float):
        self.t += s


@pytest.fixture()
def crypto(tmp_path):
    return SoftwareCryptoBackend(key_file=str(tmp_path / "key.json"))


@pytest.fixture()
def clock():
    return FakeClock(T0 + 1)


@pytest.fixture()
def builder(crypto, tmp_path, clock):
    return HashChainBuilder(
        NID, crypto, {"rx_lat": 1.0}, chain_dir=str(tmp_path / "chain"), window_s=WINDOW_S, clock=clock
    )


@pytest.fixture()
def generator():
    random.seed(7)
    return SyntheticNodeGenerator(NodeConfig(node_id=NID), mode="adsb")


def _wire(msg: dict) -> dict:
    """The message as the server parses it off the line."""
    line = node._encode_msg(msg)
    assert line.endswith(b"\n") and line.count(b"\n") == 1
    return json.loads(line)


def _verify(msg: dict, pem: str, expected_prev: str = "") -> tuple[bool, str]:
    wire = _wire(msg)
    entry = HashChainEntry.from_dict(wire["entry"])
    batch = wire["batch"].encode("utf-8") if "batch" in wire else None
    return HashChainVerifier(lambda nid: pem if nid == NID else None).verify_entry(entry, batch, expected_prev)


def _closed_entry(builder, generator) -> HashChainEntry:
    node._record_frame(builder, NID, generator.generate_frame(T0 * 1000))
    entry, _ = builder.close_window()
    return entry


def _iso(ts: float) -> str:
    return datetime.fromtimestamp(ts, tz=timezone.utc).strftime(node.ISO_UTC)


def _ts(iso: str) -> float:
    return datetime.strptime(iso, node.ISO_UTC).replace(tzinfo=timezone.utc).timestamp()


# ── Messages ──────────────────────────────────────────────────────────────────


def test_detection_message_is_the_unsigned_frame(builder, generator):
    frame = generator.generate_frame(T0 * 1000)
    detection, chain = node._record_frame(builder, NID, frame)

    assert detection == {"type": "DETECTION", "node_id": NID, "data": frame}
    assert chain == []
    wire = _wire(detection)
    assert SIGNING_FIELDS.isdisjoint(wire)
    assert SIGNING_FIELDS.isdisjoint(wire["data"])
    assert builder.pending_detections == 1


def test_closed_windows_go_out_as_chain_entries_that_verify_in_sequence(builder, generator, clock, crypto):
    pem = crypto.get_public_key_pem()
    ts = (T0 + 1) * 1000

    sent_w1 = []
    for i in range(5):
        detection, chain = node._record_frame(builder, NID, generator.generate_frame(ts + i * 500))
        assert chain == []
        sent_w1.append(detection["data"])
    # Window 1 closes on the timer, as from the 60-second check.
    clock.advance(WINDOW_S)
    [first] = node._chain_msgs(NID, builder.close_if_due())
    assert node._chain_msgs(NID, builder.close_if_due()) == []

    sent_w2 = []
    for i in range(3):
        detection, chain = node._record_frame(builder, NID, generator.generate_frame(ts + 60_000 + i * 500))
        assert chain == []
        sent_w2.append(detection["data"])
    # Window 2 is closed by the first frame that arrives after it ends.
    clock.advance(WINDOW_S)
    _, chain = node._record_frame(builder, NID, generator.generate_frame(ts + 120_000))
    [second] = chain
    assert builder.pending_detections == 1

    for msg, frames in ((first, sent_w1), (second, sent_w2)):
        assert set(msg) == CHAIN_ENTRY_KEYS
        assert msg["type"] == "CHAIN_ENTRY"
        assert msg["node_id"] == NID
        assert set(msg["entry"]) == ENTRY_KEYS
        assert msg["entry"]["node_id"] == NID
        assert msg["entry"]["n_detections"] == len(frames)
        # The batch is the window's frames exactly as their DETECTIONs carried them.
        assert json.loads(msg["batch"]) == frames

    assert first["entry"]["prev_hash"] == "genesis"
    assert second["entry"]["prev_hash"] == first["entry"]["entry_hash"]
    assert second["entry"]["window_start"] == first["entry"]["window_end"]
    assert _verify(first, pem) == (True, "ok")
    assert _verify(second, pem, expected_prev=first["entry"]["entry_hash"]) == (True, "ok")


def test_oversize_batch_is_left_out_and_the_line_fits(builder, generator, capsys):
    entry = _closed_entry(builder, generator)
    huge = b"[" + b"1," * (node.TCP_LINE_MAX // 2) + b"1]"

    msg = node._chain_entry_msg(NID, entry, huge)

    assert set(msg) == {"type", "node_id", "entry"}
    assert msg["entry"] == entry.to_dict()
    assert len(node._encode_msg(msg)) <= node.TCP_LINE_MAX
    err = capsys.readouterr().err
    assert "without the batch" in err
    assert entry.window_start in err


def test_size_rule_boundary_is_the_encoded_line(builder, generator):
    entry = _closed_entry(builder, generator)
    overhead = len(node._encode_msg({"type": "CHAIN_ENTRY", "node_id": NID, "entry": entry.to_dict(), "batch": ""}))
    fits = b"1" * (node.TCP_LINE_MAX - overhead)

    at_limit = node._chain_entry_msg(NID, entry, fits)
    assert len(node._encode_msg(at_limit)) == node.TCP_LINE_MAX
    assert at_limit["batch"] == fits.decode("ascii")
    assert "batch" not in node._chain_entry_msg(NID, entry, fits + b"1")


def test_size_rule_counts_json_escaping(builder, generator):
    entry = _closed_entry(builder, generator)
    # Under the limit as bytes, but every quote goes on the wire as \"
    quoted = b'"' * (node.TCP_LINE_MAX * 2 // 3)
    assert len(quoted) < node.TCP_LINE_MAX

    assert "batch" not in node._chain_entry_msg(NID, entry, quoted)


def _server_overflows(stream: bytes, first_read: int) -> bool:
    """retina-server's TCP read loop (backend/services/tcp_handler.py): append a
    read of up to 4 KiB, disconnect once the buffer passes 1 MiB, then split
    off complete lines."""
    buf = b""
    pos, size = 0, first_read
    while pos < len(stream):
        buf += stream[pos : pos + size]
        pos += size
        size = 4096
        if len(buf) > 1_048_576:
            return True
        while b"\n" in buf:
            _, buf = buf.split(b"\n", 1)
    return False


def test_line_limit_survives_the_servers_worst_read(builder, generator):
    entry = _closed_entry(builder, generator)
    overhead = len(node._encode_msg({"type": "CHAIN_ENTRY", "node_id": NID, "entry": entry.to_dict(), "batch": ""}))
    line = node._encode_msg(node._chain_entry_msg(NID, entry, b"1" * (node.TCP_LINE_MAX - overhead)))
    assert len(line) == node.TCP_LINE_MAX
    detection = node._encode_msg({"type": "DETECTION", "node_id": NID, "data": generator.generate_frame(T0 * 1000)})
    following = detection * (8192 // len(detection) + 1)

    def worst(line: bytes) -> bool:
        # A read that starts at the newline carries 4095 bytes of what follows.
        first = (len(line) - 1) % 4096 or 4096
        return _server_overflows(line + following, first)

    assert not worst(line)
    # Two bytes longer and the same read trips the server's limit, so this is
    # the read that binds.
    assert worst(line[:-1] + b"11\n")


# ── Wiring ────────────────────────────────────────────────────────────────────


def test_chain_builder_logs_beside_the_key_and_takes_window_and_tsa(tmp_path, monkeypatch, crypto):
    monkeypatch.setattr(node, "_COVERAGE_DATA_DIR", str(tmp_path / "coverage_data"))

    timed = node._chain_builder(NID, crypto, {}, window_s=30, tsa=True)
    assert timed.window_s == 30
    assert isinstance(timed.timestamper, TimestampManager)
    assert os.path.isdir(tmp_path / "coverage_data" / "chains" / NID)
    assert node._key_file(NID) == str(tmp_path / "coverage_data" / "keys" / NID / "synthetic_key.json")

    plain = node._chain_builder(NID, crypto, {})
    assert plain.window_s == DEFAULT_WINDOW_S
    assert plain.timestamper is None


def test_chain_window_flag_rejects_zero(monkeypatch, capsys):
    monkeypatch.setattr(sys, "argv", ["node", "--chain-window-s", "0"])
    with pytest.raises(SystemExit) as exc:
        node.main()
    assert exc.value.code == 2
    assert "--chain-window-s" in capsys.readouterr().err


def test_listener_prints_ack_with_its_window_and_leaves_sends_unbounded(capsys):
    a, b = socket.socketpair()
    config = NodeConfig(node_id=NID)
    stop = threading.Event()
    awaiting = node._AwaitingAcks()
    awaiting.add({"entry_hash": "e" * 64, "window_start": "2026-10-19T14:00:00Z"})
    listener = threading.Thread(target=node._listener_loop, args=(a, config, stop, awaiting), daemon=True)
    listener.start()
    ack = node._encode_msg(
        {
            "type": "CHAIN_ENTRY_ACK",
            "node_id": NID,
            "entry_hash": "e" * 64,
            "verified": True,
            "reason": "ok",
            "batch_verified": True,
        }
    )

    # Split mid-message: the first half must wait for the rest.
    b.sendall(ack[:40])
    time.sleep(0.05)
    b.sendall(ack[40:] + node._encode_msg({"type": "CONFIG_REQUEST", "node_id": NID}))
    b.settimeout(5)
    with b.makefile("rb") as f:
        reply = json.loads(f.readline())
    assert reply["type"] == "CONFIG"
    assert reply["config_hash"] == node._config_hash(config)
    # A poll timeout on the socket would also cap how long a send may take.
    assert a.gettimeout() is None

    b.close()
    listener.join(timeout=5)
    assert not listener.is_alive()
    a.close()
    err = capsys.readouterr().err
    assert f"CHAIN_ENTRY_ACK ({NID} window=2026-10-19T14:00:00Z, verified=True, batch_verified=True, reason=ok)" in err
    assert len(awaiting) == 0


def _listen(acks: list[dict], awaiting: "node._AwaitingAcks") -> None:
    """Run the listener over these acks until the server side closes."""
    a, b = socket.socketpair()
    listener = threading.Thread(
        target=node._listener_loop, args=(a, NodeConfig(node_id=NID), threading.Event(), awaiting), daemon=True
    )
    listener.start()
    b.sendall(b"".join(node._encode_msg(ack) for ack in acks))
    b.close()
    listener.join(timeout=5)
    assert not listener.is_alive()
    a.close()


def test_rejected_ack_prints_its_reason_and_answers_the_oldest_waiting_entry(capsys):
    awaiting = node._AwaitingAcks()
    awaiting.add({"entry_hash": "1" * 64, "window_start": "2026-10-19T14:00:00Z"})
    awaiting.add({"entry_hash": "2" * 64, "window_start": "2026-10-19T14:15:00Z"})
    # retina-server's ack for an entry it could not take: no entry_hash to match by.
    rejected = {
        "type": "CHAIN_ENTRY_ACK",
        "status": "rejected",
        "node_id": None,
        "entry_hash": None,
        "verified": False,
        "reason": "no HELLO on this connection",
        "batch_verified": None,
    }
    stored = {
        "type": "CHAIN_ENTRY_ACK",
        "status": "stored",
        "node_id": NID,
        "entry_hash": "2" * 64,
        "verified": True,
        "reason": "ok",
        "batch_verified": True,
    }

    _listen([rejected, stored], awaiting)

    err = capsys.readouterr().err
    assert f"CHAIN_ENTRY_ACK ({NID} window=2026-10-19T14:00:00Z, rejected: no HELLO on this connection)" in err
    assert f"CHAIN_ENTRY_ACK ({NID} window=2026-10-19T14:15:00Z, verified=True" in err
    assert "window=?" not in err
    assert len(awaiting) == 0


def test_rejected_ack_with_nothing_waiting_names_no_window(capsys):
    awaiting = node._AwaitingAcks()
    _listen(
        [{"type": "CHAIN_ENTRY_ACK", "status": "rejected", "entry_hash": None, "reason": "malformed entry: x"}],
        awaiting,
    )
    assert f"CHAIN_ENTRY_ACK ({NID} window=?, rejected: malformed entry: x)" in capsys.readouterr().err
    assert len(awaiting) == 0


@pytest.mark.parametrize(
    ("key_ack", "printed", "not_printed"),
    [
        (
            {"type": "KEY_ACK", "node_id": NID, "fingerprint": "f" * 64, "status": "registered"},
            "← KEY_ACK — key registered",
            "not registered",
        ),
        (
            {"type": "KEY_ACK", "status": "rejected", "node_id": None, "reason": "no HELLO for this node"},
            "! KEY_ACK rejected: no HELLO for this node — key not registered",
            "key registered",
        ),
    ],
    ids=["registered", "rejected"],
)
def test_handshake_prints_the_key_ack_status(crypto, capsys, key_ack, printed, not_printed):
    a, b = socket.socketpair()

    def server():
        with b, b.makefile("rb") as f:
            for raw in f:
                msg = json.loads(raw)
                if msg["type"] == "CONFIG":
                    b.sendall(node._encode_msg({"type": "CONFIG_ACK", "config_hash": msg["config_hash"]}))
                elif msg["type"] == "REGISTER_KEY":
                    b.sendall(node._encode_msg(key_ack))
                    return

    peer = threading.Thread(target=server, daemon=True)
    peer.start()
    a.settimeout(5)
    # A rejected key is reported, not fatal: detections still flow.
    assert node._perform_handshake(a, NodeConfig(node_id=NID), crypto_backend=crypto) is True
    peer.join(timeout=5)
    a.close()
    err = capsys.readouterr().err
    assert printed in err
    assert not_printed not in err


def test_a_slow_send_on_one_socket_does_not_hold_up_another():
    slow, slow_peer = socket.socketpair()
    fast, fast_peer = socket.socketpair()
    assert node._send_lock(slow) is node._send_lock(slow)
    assert node._send_lock(slow) is not node._send_lock(fast)
    # Far more than the socket buffers hold, so sendall blocks until the peer reads.
    big = {"type": "CHAIN_ENTRY", "batch": "1" * (8 << 20)}
    sender = threading.Thread(target=node._send_msg, args=(slow, big), daemon=True)
    sender.start()
    deadline = time.monotonic() + 5
    while not node._send_lock(slow).locked() and time.monotonic() < deadline:
        time.sleep(0.01)
    assert node._send_lock(slow).locked()

    done = threading.Event()
    threading.Thread(target=lambda: (node._send_msg(fast, {"type": "HEARTBEAT"}), done.set()), daemon=True).start()
    assert done.wait(2), "the other socket's send waited for the slow one"
    assert sender.is_alive()
    fast_peer.settimeout(5)
    with fast_peer.makefile("rb") as f:
        assert json.loads(f.readline()) == {"type": "HEARTBEAT"}

    slow_peer.settimeout(5)
    with slow_peer.makefile("rb") as f:
        assert json.loads(f.readline()) == big
    sender.join(timeout=5)
    assert not sender.is_alive()
    for s in (slow, slow_peer, fast, fast_peer):
        s.close()


# ── Restart inside a committed window ─────────────────────────────────────────

HOLD_NOTE = "holding the chain until"


def _start(crypto, chain_dir, clock, window_s: int = WINDOW_S) -> "node._NodeChain":
    """The node's chain as a fresh process builds it, resuming from the log in chain_dir."""
    builder = HashChainBuilder(NID, crypto, {"rx_lat": 1.0}, chain_dir=str(chain_dir), window_s=window_s, clock=clock)
    return node._NodeChain(NID, builder, str(chain_dir / node.CHAIN_LOG), clock=clock)


def _stream(chain, generator, clock, until: float) -> tuple[list[dict], list[dict]]:
    """A frame every 0.5 s until `until`, each followed by the timer check, as the runners do.

    Returns the frames sent as DETECTIONs and the CHAIN_ENTRY messages produced.
    """
    sent, entries = [], []
    while clock() < until:
        detection, closed = chain.record(generator.generate_frame(int(clock() * 1000)))
        sent.append(detection["data"])
        entries += closed + chain.close_if_due()
        clock.advance(0.5)
    return sent, entries


def test_restart_inside_a_window_opens_no_overlapping_window(crypto, tmp_path, generator, capsys):
    pem = crypto.get_public_key_pem()
    chain_dir = tmp_path / "chain"
    clock = FakeClock(T0 + 5)

    # Run 1 streams from T0+5 to T0+20, then Ctrl-C commits the open window.
    run1 = _start(crypto, chain_dir, clock)
    _stream(run1, generator, clock, T0 + 20)
    [last] = run1.close_window()
    last = last["entry"]
    assert (last["window_start"], last["window_end"]) == (_iso(T0), _iso(T0 + WINDOW_S))

    # Run 2 restarts 10 s later, inside that window, and streams past the next one's end.
    clock.advance(10)
    run2 = _start(crypto, chain_dir, clock)
    held, entries = _stream(run2, generator, clock, T0 + WINDOW_S)
    assert held and entries == []
    assert run2.builder.pending_detections == 0
    sent, entries = _stream(run2, generator, clock, T0 + 2 * WINDOW_S + 1)

    [first] = entries
    entry = first["entry"]
    assert _ts(entry["window_start"]) >= _ts(last["window_end"])
    assert entry["prev_hash"] == last["entry_hash"]
    assert _verify(first, pem, expected_prev=last["entry_hash"]) == (True, "ok")
    # The held frames are in no batch; the window holds the frames after the hold.
    batched = json.loads(first["batch"])
    assert all(frame not in batched for frame in held)
    assert batched == sent[: len(batched)]
    assert len(batched) == WINDOW_S * 2
    err = capsys.readouterr().err
    assert err.count(f"{NID}: {HOLD_NOTE} {_iso(T0 + WINDOW_S)}") == 1


@pytest.mark.parametrize("restart_at", [T0 + WINDOW_S, T0 + WINDOW_S + 5], ids=["at the end", "after the end"])
def test_no_hold_without_a_log_or_once_the_last_window_has_ended(crypto, tmp_path, generator, capsys, restart_at):
    chain_dir = tmp_path / "chain"
    clock = FakeClock(T0 + 5)

    # No log: the first frame opens a window.
    run1 = _start(crypto, chain_dir, clock)
    run1.record(generator.generate_frame(int(clock() * 1000)))
    assert run1.builder.pending_detections == 1
    [last] = run1.close_window()
    assert last["entry"]["window_end"] == _iso(T0 + WINDOW_S)

    clock.t = restart_at
    run2 = _start(crypto, chain_dir, clock)
    run2.record(generator.generate_frame(int(clock() * 1000)))
    assert run2.builder.pending_detections == 1
    assert HOLD_NOTE not in capsys.readouterr().err


@pytest.mark.parametrize(
    "junk",
    ['{"node_id": "synth-custody-01", "window_st', '{"window_end": "2099-01-01T00:00:00Z"}', "", "not json"],
    ids=["torn write", "missing fields", "blank", "not json"],
)
def test_hold_falls_back_past_unparseable_lines_as_the_builder_does(crypto, tmp_path, generator, capsys, junk):
    chain_dir = tmp_path / "chain"
    clock = FakeClock(T0 + 5)
    run1 = _start(crypto, chain_dir, clock)
    run1.record(generator.generate_frame(int(clock() * 1000)))
    [last] = run1.close_window()
    with open(chain_dir / node.CHAIN_LOG, "a") as f:
        f.write(junk + "\n")

    # Inside the committed window: held, per the last entry the builder resumes from.
    clock.t = T0 + 30
    run2 = _start(crypto, chain_dir, clock)
    assert run2.builder.prev_hash == last["entry"]["entry_hash"]
    run2.record(generator.generate_frame(int(clock() * 1000)))
    assert run2.builder.pending_detections == 0
    clock.t = T0 + WINDOW_S
    run2.record(generator.generate_frame(int(clock() * 1000)))
    assert run2.builder.pending_detections == 1
    assert f"{NID}: {HOLD_NOTE} {_iso(T0 + WINDOW_S)}" in capsys.readouterr().err


@pytest.mark.parametrize("junk", ["[1]", "null", '"text"', "7"])
def test_resume_point_skips_json_that_is_not_an_object(builder, generator, tmp_path, junk):
    # HashChainBuilder itself raises on such a line (from_dict needs a dict),
    # so this reads the log directly.
    _closed_entry(builder, generator)
    log = tmp_path / "chain" / node.CHAIN_LOG
    with open(log, "a") as f:
        f.write(junk + "\n")
    assert node._chain_resume_at(str(log), WINDOW_S) == T0 + WINDOW_S


def test_resume_point_is_the_next_boundary_of_the_new_window_size(builder, generator, tmp_path):
    _closed_entry(builder, generator)
    log = str(tmp_path / "chain" / node.CHAIN_LOG)
    assert node._chain_resume_at(log, WINDOW_S) == T0 + WINDOW_S
    assert node._chain_resume_at(log, 900) == T0 + 900  # T0 is a multiple of 900
    assert node._chain_resume_at(log, 7) == 1_800_000_062  # first multiple of 7 at or after T0 + 60
    assert node._chain_resume_at(str(tmp_path / "missing" / node.CHAIN_LOG), WINDOW_S) is None


# ── Runners against a local TCP stub ──────────────────────────────────────────


class _StubServer:
    """Just enough of retina-server's TCP side for the custody path.

    Does the handshake, keeps the key each node registers, checks every
    CHAIN_ENTRY the way the server does (against that key and the last verified
    entry) and acknowledges it. Once every node has had a window acknowledged it
    interrupts the main thread, as Ctrl-C would, so the runner shuts down.
    """

    def __init__(self, n_nodes: int):
        self.n_nodes = n_nodes
        self.srv = socket.create_server(("127.0.0.1", 0))
        self.port = self.srv.getsockname()[1]
        self.msgs: dict[str, list[dict]] = {}
        self.acks: list[dict] = []
        self.unparsed = 0
        self.timed_out = False
        self._keys: dict[str, str] = {}
        self._lock = threading.Lock()
        self._interrupted = False
        self._threads: list[threading.Thread] = []
        self._watchdog = threading.Timer(15, self._interrupt, kwargs={"timed_out": True})

    def start(self):
        self._watchdog.start()
        threading.Thread(target=self._accept, daemon=True).start()

    def stop(self):
        self._watchdog.cancel()
        self.srv.close()
        for t in self._threads:
            t.join(timeout=5)

    def _interrupt(self, timed_out: bool = False):
        with self._lock:
            if self._interrupted:
                return
            self._interrupted = True
            self.timed_out = timed_out
        _thread.interrupt_main()

    def _accept(self):
        for _ in range(self.n_nodes):
            conn, _ = self.srv.accept()
            t = threading.Thread(target=self._serve, args=(conn,), daemon=True)
            self._threads.append(t)
            t.start()

    def _send(self, conn, msg):
        conn.sendall(node._encode_msg(msg))

    def _serve(self, conn):
        conn.settimeout(15)
        node_id = None
        tail = ""
        verifier = HashChainVerifier(self._keys.get)
        with conn, conn.makefile("rb") as f:
            for raw in f:  # until the node half-closes
                if not raw.strip():
                    continue
                try:
                    msg = json.loads(raw)
                except ValueError:
                    self.unparsed += 1
                    continue
                kind = msg.get("type")
                if kind == "HELLO":
                    node_id = msg["node_id"]
                    self.msgs[node_id] = []
                self.msgs[node_id].append(msg)
                if kind == "CONFIG":
                    self._send(conn, {"type": "CONFIG_ACK", "config_hash": msg["config_hash"]})
                elif kind == "REGISTER_KEY":
                    self._keys[msg["node_id"]] = msg["public_key_pem"]
                    self._send(conn, {"type": "KEY_ACK", "node_id": msg["node_id"], "status": "registered"})
                elif kind == "CHAIN_ENTRY":
                    entry = HashChainEntry.from_dict(msg["entry"])
                    batch = msg["batch"].encode("utf-8") if "batch" in msg else None
                    verified, reason = verifier.verify_entry(entry, batch, tail)
                    if verified:
                        tail = entry.entry_hash
                    ack = {
                        "type": "CHAIN_ENTRY_ACK",
                        "node_id": node_id,
                        "entry_hash": entry.entry_hash,
                        "verified": verified,
                        "reason": reason,
                        "batch_verified": verified if batch is not None else None,
                    }
                    with self._lock:
                        self.acks.append(ack)
                        everyone = len({a["node_id"] for a in self.acks}) == self.n_nodes
                    self._send(conn, ack)
                    if everyone:
                        self._interrupt()


def _commit_last_window(nid: str) -> HashChainEntry:
    """The window a previous run committed on exit, as node.py does, ending 1-2 s from now.

    Uses the node's own key and chain log, with a 1 s window like the runs below.
    The margin keeps the hold longer than the run's start-up on a slow machine.
    """
    crypto = SoftwareCryptoBackend(key_file=node._key_file(nid))
    at = time.time() + 1.0
    chain_dir = node._custody_dir("chains", nid)
    builder = HashChainBuilder(nid, crypto, {}, chain_dir=chain_dir, window_s=1, clock=lambda: at)
    builder.add_detection({"timestamp": int(at * 1000)})
    entry, _ = builder.close_window()
    return entry


def _assert_custody_traffic(stub: _StubServer, nid: str, coverage_dir, err: str, prior: HashChainEntry | None = None):
    msgs = stub.msgs[nid]
    detections = [m for m in msgs if m["type"] == "DETECTION"]
    entries = [m for m in msgs if m["type"] == "CHAIN_ENTRY"]

    assert detections
    for m in detections:
        assert set(m) == {"type", "node_id", "data"}
        assert m["node_id"] == nid
        assert SIGNING_FIELDS.isdisjoint(m["data"])

    # One window closed by a frame mid-run, then the open one closed at shutdown.
    assert len(entries) == 2
    for m in entries:
        assert set(m) == CHAIN_ENTRY_KEYS
        assert set(m["entry"]) == ENTRY_KEYS
    assert entries[1]["entry"]["prev_hash"] == entries[0]["entry"]["entry_hash"]
    acks = [a for a in stub.acks if a["node_id"] == nid]
    assert [(a["verified"], a["reason"], a["batch_verified"]) for a in acks] == [(True, "ok", True)] * 2

    # A restart links to the last committed window and opens none before it ends.
    first = entries[0]["entry"]
    assert first["prev_hash"] == (prior.entry_hash if prior else "genesis")
    if prior:
        assert _ts(first["window_start"]) >= _ts(prior.window_end)
        assert err.count(f"{nid}: {HOLD_NOTE} {prior.window_end}") == 1
    else:
        assert f"{nid}: {HOLD_NOTE}" not in err

    # Frames sent during the hold are in no batch. Every frame after it is, in
    # order. Ctrl-C can land after a frame went into the chain and before its
    # DETECTION went out.
    sent = [m["data"] for m in detections]
    batched = [frame for m in entries for frame in json.loads(m["batch"])]
    n_held = sent.index(batched[0])
    assert n_held > 0 if prior else n_held == 0
    assert all(frame not in batched for frame in sent[:n_held])
    assert batched[: len(sent) - n_held] == sent[n_held:]
    assert len(batched) - (len(sent) - n_held) in (0, 1)

    # The listener printed both verdicts, the last one after the half-close.
    for m in entries:
        assert f"CHAIN_ENTRY_ACK ({nid} window={m['entry']['window_start']}, verified=True" in err

    log = coverage_dir / "chains" / nid / node.CHAIN_LOG
    assert [json.loads(line)["entry_hash"] for line in log.read_text().splitlines()] == [
        *([prior.entry_hash] if prior else []),
        *(m["entry"]["entry_hash"] for m in entries),
    ]
    assert (coverage_dir / "keys" / nid / "synthetic_key.json").is_file()


def _run_until_interrupted(stub: _StubServer, run):
    stub.start()
    try:
        run()
    except KeyboardInterrupt:
        pytest.fail("the interrupt escaped the runner")
    finally:
        stub.stop()
    assert not stub.timed_out
    assert stub.unparsed == 0


class _NoIQBuffer:
    def __init__(self, **_kwargs):
        pass

    def start(self):
        pass

    def stop(self):
        pass


# restart: the node's last run committed a window that has not ended yet.
@pytest.mark.parametrize("restart", [False, True], ids=["fresh", "restart"])
def test_single_node_runner_sends_verifiable_windows_over_tcp(tmp_path, monkeypatch, capsys, restart):
    coverage = tmp_path / "coverage_data"
    monkeypatch.setattr(node, "_COVERAGE_DATA_DIR", str(coverage))
    # The real buffer's stop() waits up to a second for its fill thread, long
    # enough to hide whether shutdown waits for the server's last ack.
    monkeypatch.setattr(iq_buffer, "IQCircularBuffer", _NoIQBuffer)
    random.seed(11)
    generator = SyntheticNodeGenerator(NodeConfig(node_id=NID), mode="adsb")
    prior = _commit_last_window(NID) if restart else None
    stub = _StubServer(n_nodes=1)

    _run_until_interrupted(
        stub, lambda: node._stream_tcp(generator, "127.0.0.1", stub.port, interval_ms=20, chain_window_s=1)
    )

    _assert_custody_traffic(stub, NID, coverage, capsys.readouterr().err, prior)


@pytest.mark.parametrize("restart", [False, True], ids=["fresh", "restart"])
def test_multi_node_runner_sends_verifiable_windows_over_tcp(tmp_path, monkeypatch, capsys, restart):
    coverage = tmp_path / "coverage_data"
    monkeypatch.setattr(node, "_COVERAGE_DATA_DIR", str(coverage))
    random.seed(13)
    ids = ["synth-custody-a", "synth-custody-b"]
    nodes_config = tmp_path / "nodes.json"
    nodes_config.write_text(json.dumps({"nodes": [{"node_id": nid} for nid in ids]}))
    priors = {nid: _commit_last_window(nid) if restart else None for nid in ids}
    stub = _StubServer(n_nodes=len(ids))

    _run_until_interrupted(
        stub,
        lambda: node._stream_multi_node_tcp(
            str(nodes_config), "127.0.0.1", stub.port, mode="adsb", interval_ms=20, chain_window_s=1
        ),
    )

    err = capsys.readouterr().err
    for nid in ids:
        _assert_custody_traffic(stub, nid, coverage, err, priors[nid])
