"""Vulture dead-code whitelist.

Read by tools/check-dead-code.sh. Every name here is one vulture reports as
dead but which must not be deleted — or which nobody has decided about yet.
The distinction matters, so the two live in separate sections.

Add to CONTRACTS only when the name is genuinely referenced by something
vulture cannot see: a framework calling in, a wire format, a config key. Real
dead code should be deleted, not whitelisted.

The UNREVIEWED section is a backlog, not an exemption. Each entry is code that
appears genuinely unreachable and needs a decision — delete it, or wire up
whatever was left unfinished. The gate is green with these listed so that it
starts catching NEW dead code immediately; working through them is separate.
"""
# ruff: noqa: B018, F821
# B018 — bare-name expressions are how vulture whitelists work.
# F821 — these names are defined in other modules; only vulture reads this file.

_ = type("_", (), {})()

# ── Contracts: referenced by something vulture cannot see ─────────────────────
# ssl.SSLContext attribute; assignment configures the context
#   retina_simulation/tower_resolver.py:125
_.check_hostname
# ssl.SSLContext attribute; assignment configures the context
#   retina_simulation/tower_resolver.py:126
_.verify_mode

# ── UNREVIEWED: appears dead, needs a decision (delete, or finish wiring) ──────
# TODO: no reference found anywhere in the estate
#   retina_simulation/generator.py:277  (unused function)
_jitter
# TODO: no reference found anywhere in the estate
#   retina_simulation/world.py:133  (unused function)
config_hash
# TODO: no reference found anywhere in the estate
#   retina_simulation/world.py:894  (unused method)
_.export_training_ndjson
# TODO: no reference found anywhere in the estate
#   retina_simulation/orchestrator.py:92  (unused attribute)
_.frames_sent
# TODO: no reference found anywhere in the estate
#   retina_simulation/orchestrator.py:91  (unused attribute)
_.handshake_ok
# TODO: no reference found anywhere in the estate
#   retina_simulation/node.py:69  (unused variable)
min_doppler
# TODO: no reference found anywhere in the estate
#   retina_simulation/node.py:82  (unused variable)
target_id
# TODO: no reference found anywhere in the estate
#   retina_simulation/generator.py:260  (unused variable)
tx_callsign
