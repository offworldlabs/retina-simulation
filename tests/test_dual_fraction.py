"""dual_fraction: a slice of the ring/scatter budget additionally run as
dual-illuminator sites (see generator._generate_dual_sites), independent of
--layout dual (already all-dual there).

Also covers the orchestrator's scene-change detection: n_nodes/dual_fraction
are baked into the fleet at container boot, so the only way to apply a
change is a full regeneration — _poll_simulation_config exits the process
via orchestrator.stop() when the backend-reported scene drifts from the
container's own stamped scene.
"""

import asyncio
import json
import re

import pytest

from retina_simulation.generator import generate_fleet

_DUAL_ID_RE = re.compile(r"-DUAL-\d{4}[ab]$")


def _fleet(**kw):
    kw.setdefault("use_tower_api", False)
    kw.setdefault("seed", 42)
    return generate_fleet(**kw)


class TestDualFractionCarve:
    def test_dual_ids_appear_in_rx_sharing_pairs_scatter_gvl(self):
        fleet = _fleet(
            n_nodes=30, metro="gvl", n_cluster=30, n_clusters=1,
            layout="scatter", dual_fraction=0.4,
        )
        dual_nodes = [n for n in fleet if _DUAL_ID_RE.search(n["node_id"])]
        # n_dual_sites = round(30 * 0.4 / 2) = 6 sites -> up to 12 nodes,
        # clamped to (n_cluster=30 after solo carve) + n_metro budget.
        assert dual_nodes
        assert len(dual_nodes) == 12

        sites = {}
        for n in dual_nodes:
            sites.setdefault(n["node_id"][:-1], []).append(n)
        assert len(sites) == 6
        for a, b in sites.values():
            # Same antenna, same mast — only the transmitter differs (mirrors
            # test_dual_sites.py's rx-sharing assertion for layout="dual").
            assert (a["rx_lat"], a["rx_lon"], a["rx_alt_ft"]) == \
                   (b["rx_lat"], b["rx_lon"], b["rx_alt_ft"])
            assert (a["tx_lat"], a["tx_lon"]) != (b["tx_lat"], b["tx_lon"])

    def test_dual_ids_appear_in_ring_layout_too(self):
        # Spec: "append dual sites after the existing layout output (scatter
        # AND ring branches)" — ring is the default layout.
        fleet = _fleet(
            n_nodes=30, metro="gvl", n_cluster=30, n_clusters=1,
            layout="ring", dual_fraction=0.4,
        )
        dual_nodes = [n for n in fleet if _DUAL_ID_RE.search(n["node_id"])]
        assert dual_nodes

    def test_dual_fraction_ignored_for_layout_dual(self):
        # layout="dual" is already all-dual; dual_fraction must be a no-op,
        # not an additional carve on top of an already-fully-dual cluster.
        without = _fleet(
            n_nodes=16, metro="gvl", n_cluster=16, n_clusters=1, layout="dual",
        )
        with_frac = _fleet(
            n_nodes=16, metro="gvl", n_cluster=16, n_clusters=1, layout="dual",
            dual_fraction=0.5,
        )
        assert without == with_frac


class TestDeterminismRegression:
    def test_dual_fraction_zero_reproduces_todays_scene(self):
        """dual_fraction=0.0 must not perturb the RNG stream of pre-existing
        nodes — sites are appended AFTER the existing layout output, so a
        zero carve means zero extra RNG draws and an identical fleet."""
        baseline = _fleet(n_nodes=50)
        explicit_zero = _fleet(n_nodes=50, dual_fraction=0.0)
        assert explicit_zero == baseline

    def test_dual_fraction_zero_with_metro_layout_ring(self):
        baseline = _fleet(n_nodes=30, metro="gvl", n_cluster=30, n_clusters=1, layout="ring")
        explicit_zero = _fleet(
            n_nodes=30, metro="gvl", n_cluster=30, n_clusters=1, layout="ring",
            dual_fraction=0.0,
        )
        assert explicit_zero == baseline

    def test_same_seed_same_dual_fraction_is_deterministic(self):
        a = _fleet(n_nodes=30, metro="gvl", n_cluster=30, n_clusters=1,
                   layout="scatter", dual_fraction=0.4)
        b = _fleet(n_nodes=30, metro="gvl", n_cluster=30, n_clusters=1,
                   layout="scatter", dual_fraction=0.4)
        assert a == b


class TestDualFractionClampAndGuards:
    def test_dual_fraction_one_clamps_to_available_budget(self):
        # n_dual_nodes is clamped to n_cluster + n_metro — it can never
        # exceed the whole layout's node budget even at dual_fraction=1.0.
        fleet = _fleet(
            n_nodes=30, metro="gvl", n_cluster=30, n_clusters=1,
            layout="scatter", dual_fraction=1.0,
        )
        dual_nodes = [n for n in fleet if _DUAL_ID_RE.search(n["node_id"])]
        non_solo_non_dual = [
            n for n in fleet
            if not _DUAL_ID_RE.search(n["node_id"]) and "SOLO" not in n["node_id"]
        ]
        # Whole cluster budget (30, after the metro-scoped solo carve leaves
        # it untouched) went to dual sites; nothing left for scatter nodes.
        assert len(dual_nodes) <= 30
        assert non_solo_non_dual == []

    def test_dual_fraction_without_metro_raises(self):
        with pytest.raises(ValueError):
            _fleet(n_nodes=30, layout="ring", dual_fraction=0.3)


class _StubWorld:
    frac_anomalous = 0.0
    frac_drone = 0.0
    frac_dark = 0.0
    min_aircraft = 1
    max_aircraft = 1


class _StubOrchestrator:
    def __init__(self, max_range_km=0.0):
        self._running = True
        self.world = _StubWorld()
        self.stop_calls = 0
        # The orchestrator itself holds the running value — max_range_km
        # needs no scene stamp, unlike n_nodes/dual_fraction below.
        self.max_range_km = max_range_km

    async def stop(self):
        self.stop_calls += 1
        self._running = False


class _FakeResponse:
    def __init__(self, payload):
        self._body = json.dumps(payload).encode()

    def read(self):
        return self._body

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False


def _run_one_poll(monkeypatch, orchestrator, scene, cfg):
    """Run _poll_simulation_config for exactly one fetch cycle.

    Patches urllib.request.urlopen (imported locally inside the polled
    function) to return `cfg` once, then flips orchestrator._running off so
    the `while orchestrator._running` loop exits on its next check — needed
    for the "no diff" cases where the function itself never returns early.
    """
    import urllib.request

    from retina_simulation.orchestrator import _poll_simulation_config

    def _fake_urlopen(url, context=None, timeout=None):
        orchestrator._running = False
        return _FakeResponse(cfg)

    monkeypatch.setattr(urllib.request, "urlopen", _fake_urlopen)
    asyncio.run(_poll_simulation_config(
        orchestrator, "http://validation.example", interval_s=0.0, scene=scene,
    ))


class TestScenePollDetection:
    def test_stop_called_on_scene_diff(self, monkeypatch):
        orch = _StubOrchestrator()
        scene = {"n_nodes": 30, "dual_fraction": 0.0}
        cfg = {"_updated_at": 1.0, "n_nodes": 32, "dual_fraction": 0.2}
        _run_one_poll(monkeypatch, orch, scene, cfg)
        assert orch.stop_calls == 1

    def test_stop_called_on_dual_fraction_diff_alone(self, monkeypatch):
        orch = _StubOrchestrator()
        scene = {"n_nodes": 30, "dual_fraction": 0.0}
        cfg = {"_updated_at": 1.0, "n_nodes": 30, "dual_fraction": 0.2}
        _run_one_poll(monkeypatch, orch, scene, cfg)
        assert orch.stop_calls == 1

    def test_stop_not_called_when_scene_matches(self, monkeypatch):
        orch = _StubOrchestrator()
        scene = {"n_nodes": 30, "dual_fraction": 0.0}
        cfg = {"_updated_at": 1.0, "n_nodes": 30, "dual_fraction": 0.0}
        _run_one_poll(monkeypatch, orch, scene, cfg)
        assert orch.stop_calls == 0

    def test_stop_not_called_within_float_tolerance(self, monkeypatch):
        orch = _StubOrchestrator()
        scene = {"n_nodes": 30, "dual_fraction": 0.2}
        cfg = {"_updated_at": 1.0, "n_nodes": 30, "dual_fraction": 0.2 + 1e-9}
        _run_one_poll(monkeypatch, orch, scene, cfg)
        assert orch.stop_calls == 0

    def test_stop_not_called_when_scene_is_empty(self, monkeypatch):
        # Absent scene stamp (stale volume, in-process generation fallback)
        # -> no comparison, no restart, regardless of how different cfg is.
        orch = _StubOrchestrator()
        cfg = {"_updated_at": 1.0, "n_nodes": 99, "dual_fraction": 0.9}
        _run_one_poll(monkeypatch, orch, None, cfg)
        assert orch.stop_calls == 0

    def test_stop_not_called_when_config_omits_scene_keys(self, monkeypatch):
        # Backend never had a PUT for n_nodes/dual_fraction (only-if-set
        # pattern) -> absent keys -> no comparison.
        orch = _StubOrchestrator()
        scene = {"n_nodes": 30, "dual_fraction": 0.0}
        cfg = {"_updated_at": 1.0}
        _run_one_poll(monkeypatch, orch, scene, cfg)
        assert orch.stop_calls == 0


class TestScenePollMaxRangeKm:
    """max_range_km is a scene key too — applying it requires regenerating
    every node's config, so it goes through the same restart path as
    n_nodes/dual_fraction. Unlike those two it needs no scene stamp: the
    orchestrator itself holds the running value, so the comparison runs even
    when `scene` is None."""

    def test_stop_called_on_max_range_km_diff(self, monkeypatch):
        orch = _StubOrchestrator(max_range_km=0.0)
        cfg = {"_updated_at": 1.0, "max_range_km": 150.0}
        _run_one_poll(monkeypatch, orch, None, cfg)
        assert orch.stop_calls == 1

    def test_stop_called_on_max_range_km_diff_with_scene_present(self, monkeypatch):
        # Runs independent of the scene stamp -- present-but-matching scene
        # keys must not suppress the max_range_km comparison.
        orch = _StubOrchestrator(max_range_km=0.0)
        scene = {"n_nodes": 30, "dual_fraction": 0.0}
        cfg = {"_updated_at": 1.0, "n_nodes": 30, "dual_fraction": 0.0, "max_range_km": 200.0}
        _run_one_poll(monkeypatch, orch, scene, cfg)
        assert orch.stop_calls == 1

    def test_stop_not_called_when_max_range_km_matches(self, monkeypatch):
        orch = _StubOrchestrator(max_range_km=100.0)
        cfg = {"_updated_at": 1.0, "max_range_km": 100.0}
        _run_one_poll(monkeypatch, orch, None, cfg)
        assert orch.stop_calls == 0

    def test_stop_not_called_within_float_tolerance(self, monkeypatch):
        orch = _StubOrchestrator(max_range_km=100.0)
        cfg = {"_updated_at": 1.0, "max_range_km": 100.0 + 1e-9}
        _run_one_poll(monkeypatch, orch, None, cfg)
        assert orch.stop_calls == 0

    def test_stop_not_called_when_max_range_km_absent(self, monkeypatch):
        # Never PUT (only-if-set pattern) -> absent key -> no comparison,
        # even though the running value is nonzero.
        orch = _StubOrchestrator(max_range_km=75.0)
        cfg = {"_updated_at": 1.0}
        _run_one_poll(monkeypatch, orch, None, cfg)
        assert orch.stop_calls == 0

    def test_works_with_scene_stamp_none_and_no_scene_diff(self, monkeypatch):
        # scene=None must not itself raise or skip the max_range_km check --
        # only n_nodes/dual_fraction are guarded by the stamp.
        orch = _StubOrchestrator(max_range_km=50.0)
        cfg = {"_updated_at": 1.0, "max_range_km": 50.0}
        _run_one_poll(monkeypatch, orch, None, cfg)
        assert orch.stop_calls == 0
