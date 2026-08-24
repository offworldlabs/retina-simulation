"""Dual-illuminator sites: one antenna, one RX, two transmitters.

This is how a real passive-radar site is built, and the geometric payoff is
that the two bistatic ellipses share a focus (the common RX).  They therefore
intersect in at most two points and the beam almost always excludes one, so a
single site localises on its own with the residual ambiguity bounded by the
antenna pattern rather than by a second receiver tens of km away.
"""

import math

from retina_simulation.generator import (
    _subtended_deg,
    generate_fleet,
)


def _dual_sites(**kw):
    kw.setdefault("n_nodes", 16)
    kw.setdefault("metro", "gvl")
    kw.setdefault("n_cluster", 16)
    kw.setdefault("n_clusters", 1)
    kw.setdefault("use_tower_api", False)
    kw.setdefault("seed", 42)
    kw.setdefault("layout", "dual")
    fleet = generate_fleet(**kw)
    sites = {}
    for n in fleet:
        if "DUAL" in n["node_id"]:
            sites.setdefault(n["node_id"][:-1], []).append(n)
    return fleet, sites


class TestDualSiteStructure:
    def test_every_site_emits_exactly_two_nodes(self):
        _fleet, sites = _dual_sites()
        assert sites
        assert all(len(v) == 2 for v in sites.values())

    def test_the_pair_shares_one_receiver(self):
        """Same antenna, same mast — only the transmitter differs."""
        _fleet, sites = _dual_sites()
        for a, b in sites.values():
            assert (a["rx_lat"], a["rx_lon"], a["rx_alt_ft"]) == (b["rx_lat"], b["rx_lon"], b["rx_alt_ft"])

    def test_the_pair_shares_one_detection_area(self):
        _fleet, sites = _dual_sites()
        for a, b in sites.values():
            assert a["beam_azimuth_deg"] == b["beam_azimuth_deg"]
            assert a["beam_width_deg"] == b["beam_width_deg"]
            assert a["max_bistatic_range_km"] == b["max_bistatic_range_km"]

    def test_the_pair_uses_two_different_transmitters(self):
        """Two receivers on one mast would share an ellipse and be useless."""
        _fleet, sites = _dual_sites()
        for a, b in sites.values():
            assert (a["tx_lat"], a["tx_lon"]) != (b["tx_lat"], b["tx_lon"])
            sep = math.hypot((a["tx_lat"] - b["tx_lat"]) * 111.32, (a["tx_lon"] - b["tx_lon"]) * 91.0)
            assert sep > 1.0, "co-sited transmitters give an identical ellipse"

    def test_vhf_band_restriction_is_honoured(self):
        _fleet, sites = _dual_sites(illuminator_band="vhf")
        for pair in sites.values():
            for n in pair:
                assert n["fc_hz"] < 300e6

    def test_sites_are_scattered_not_ringed(self):
        """A ring puts every receiver the same distance from the core; these
        are drawn across the metro, which is what keeps inter-site overlap
        low."""
        _fleet, sites = _dual_sites()
        core = (34.852, -82.394)
        d = [math.hypot((p[0]["rx_lat"] - core[0]) * 111.32, (p[0]["rx_lon"] - core[1]) * 91.0) for p in sites.values()]
        assert max(d) - min(d) > 20.0


class TestIlluminatorSelection:
    def test_pairs_are_geometrically_usable(self):
        """Selection maximises the worst subtended angle over the beam
        footprint; a pair whose transmitters lie in nearly the same direction
        gives two near-parallel ellipses and a degenerate intersection."""
        _fleet, sites = _dual_sites()
        angles = [
            _subtended_deg(a["rx_lat"], a["rx_lon"], (a["tx_lat"], a["tx_lon"]), (b["tx_lat"], b["tx_lon"]))
            for a, b in sites.values()
        ]
        assert sum(angles) / len(angles) > 25.0

    def test_eirp_floor_excludes_weak_illuminators(self):
        """Spartanburg (27 dBm) is geometrically valuable but radiologically
        weak; a high floor must drop it."""
        _fleet, sites = _dual_sites(dual_min_eirp_dbm=75.0)
        used = {n["tx_callsign"] for pair in sites.values() for n in pair}
        assert "BLP01065" not in used
        assert "W07DT-D" not in used


class TestAim:
    def test_core_aim_points_beams_at_the_traffic(self):
        """Random aiming starved the layout — with most aircraft routed through
        the metro core, a randomly-pointed beam sees nothing and the solve rate
        collapsed by an order of magnitude."""
        _fleet, sites = _dual_sites(dual_aim="core")
        core = (34.852, -82.394)
        off = []
        for pair in sites.values():
            n = pair[0]
            want = (
                math.degrees(
                    math.atan2((core[1] - n["rx_lon"]) * math.cos(math.radians(core[0])), core[0] - n["rx_lat"])
                )
                % 360.0
            )
            off.append(abs((n["beam_azimuth_deg"] - want + 180) % 360 - 180))
        assert sum(off) / len(off) < 45.0

    def test_random_aim_is_still_available(self):
        _fleet, sites = _dual_sites(dual_aim="random")
        assert sites


class TestGeneratorCLI:
    """main() must be able to supply every argument generate_fleet takes.

    --dual-aim was added to generate_fleet and to the offline bench but never
    registered on the generator's own parser, while main() passed
    args.dual_aim regardless.  Nothing caught it: every test and the bench call
    generate_fleet() directly, and the CLI runs only in the fleet container's
    entrypoint — so the break surfaced as a staging deploy coming up with zero
    synthetic nodes.
    """

    def _parser_dests(self):
        import argparse
        from unittest import mock

        import retina_simulation.generator as gen

        captured = {}
        real_parse = argparse.ArgumentParser.parse_args

        def _capture(self, *a, **kw):
            captured["dests"] = {act.dest for act in self._actions}
            raise SystemExit(0)  # stop before generating a fleet

        with mock.patch.object(argparse.ArgumentParser, "parse_args", _capture):
            try:
                gen.main()
            except SystemExit:
                pass
        assert real_parse is argparse.ArgumentParser.parse_args
        return captured["dests"]

    def test_every_arg_main_reads_is_registered(self):
        import ast
        import inspect

        import retina_simulation.generator as gen

        src = inspect.getsource(gen.main)
        tree = ast.parse(src.lstrip())
        used = {
            node.attr
            for node in ast.walk(tree)
            if isinstance(node, ast.Attribute) and isinstance(node.value, ast.Name) and node.value.id == "args"
        }
        missing = used - self._parser_dests()
        assert not missing, f"main() reads unregistered args: {sorted(missing)}"

    def test_dual_aim_is_registered_and_defaults_to_core(self):
        import argparse
        from unittest import mock

        import retina_simulation.generator as gen

        seen = {}

        def _capture(self, *a, **kw):
            seen["ns"] = argparse.Namespace(**{act.dest: act.default for act in self._actions})
            raise SystemExit(0)

        with mock.patch.object(argparse.ArgumentParser, "parse_args", _capture):
            try:
                gen.main()
            except SystemExit:
                pass
        assert seen["ns"].dual_aim == "core"
