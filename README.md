# retina-simulation

Fleet simulator and synthetic node generator for the RETINA passive radar
network. It generates realistic synthetic node configurations and streams
synthetic detections to a RETINA server, so the server's pipeline can be
tested at fleet scale without radar hardware. retina-server uses it as a git
submodule under `libs/retina-simulation`.

## Modules

All modules are in `retina_simulation/`.

| Module | Purpose |
|---|---|
| `generator.py` | Generates synthetic node configurations across regions, each paired with a real broadcast transmitter |
| `world.py` | `SimulationWorld`: one shared set of simulated aircraft, observed by many synthetic nodes, each from its own geometry |
| `node.py` | A synthetic node that speaks the RETINA node protocol over TCP (or HTTP) and streams signed detections |
| `orchestrator.py` | Runs a fleet of synthetic nodes against a server using a shared `SimulationWorld` |
| `tower_resolver.py` | Looks up real transmitters near each node from the FCC broadcast API and caches the result |
| `live_adsb.py` | Pulls live aircraft from an adsb.lol-compatible endpoint to seed the world |

Each of `generator`, `node` and `orchestrator` has a command-line interface:

```bash
python -m retina_simulation.generator --help
python -m retina_simulation.node --help
python -m retina_simulation.orchestrator --help
```

## Development

Requires Python 3.10 or later. `retina-custody` is not on a package index, so
install it from GitHub first:

```bash
pip install "retina-custody @ git+https://github.com/offworldlabs/retina-custody@v0.1.0"
pip install -e '.[dev]'
pytest
```

CI (`.github/workflows/ci.yml`) also runs `pre-commit run --all-files`, which
covers ruff and the dead-code check.

## Data

- `retina_simulation/data/ne_10m_land.geojson` and `ne_50m_lakes.geojson` are
  from [Natural Earth](https://www.naturalearthdata.com/), which is in the
  public domain.
- `retina_simulation/metro_tower_cache.json` holds transmitter records derived
  from FCC broadcast data.

## License

Copyright (C) 2026 Offworld Labs

This program is free software: you can redistribute it and/or modify it
under the terms of the GNU Affero General Public License as published by
the Free Software Foundation, either version 3 of the License, or (at your
option) any later version.

This program is distributed in the hope that it will be useful, but
WITHOUT ANY WARRANTY; without even the implied warranty of
MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero
General Public License for more details.

The full licence text is in [LICENSE](LICENSE).
