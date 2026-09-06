# Mega Home

A [HACS](https://hacs.xyz/) custom integration that makes the Mega Manager resident app work
**locally** — including in a flat that has no internet connection at all.

## What it does

Home Assistant already runs in the flat, two metres from the resident. Until now the app went
the long way round: the structure came from Mega Manager's database, states and commands were
proxied through the manager's cloud connection, and even the scenario icons were images served
by the manager. Cut the internet and the app was gone.

This integration removes the manager from the runtime path:

- it **pulls one config** from the manager (floors, rooms, tiles with their `entity_id`s,
  scenarios) and **caches it on disk**, together with the scenario icons as files;
- it **serves the app** at `http://<home-assistant>:8123/mega-home`;
- it answers **states and commands** from `hass` directly, and **pushes state changes** to the
  app over one SSE stream (`/mega-home/api/events`) — the tile changes when the relay clicks, not
  on the next poll; nothing to lose when the line goes down.

The manager stays the source of the configuration; it just is not needed to run the home.

## The cache is not a fallback

A finished installation may stay offline **permanently**, so an unreachable manager is an
ordinary state, not an incident:

- the config is loaded from disk **before** the first network call, so Home Assistant restarts
  fine with no line;
- polling backs off from 15 minutes to an hour instead of hammering, and logs one line per
  state change rather than one per failure;
- if the manager is down, the previously fetched config keeps being served — the resident sees
  nothing at all.

Only a home that has *never* synchronised has nothing to serve.

## Installation

1. Add this repository to HACS as a custom repository (category: Integration) and install
   **Mega Home**, then restart Home Assistant.
2. Settings → Devices & services → **Add integration** → *Mega Home*.
3. Enter:
   - **Manager address** — the manager's inbound port, e.g. `http://manager.example.com:8055`
     (that is the port the object talks to; the interface port is a different one);
   - **Object token** — the object's webhook token from its card in Mega Manager.

The address and the token are checked against the manager before the entry is created.

> HACS installs from **releases**. A commit on `main` with no release will not reach an object.

## Endpoints

| Path | What it is |
|---|---|
| `/mega-home` | the app bundle, downloaded from the manager (a static "connecting" page until the first download — releases carry no copy) |
| `/mega-home/icons/<icon>_300.png` | scenario icons, cached locally |
| `/mega-home/api/config` | the cached home config |
| `/mega-home/api/states` | current state of every tile |
| `/mega-home/api/command` | `POST {id, command, value?}` |
| `/mega-home/api/scenario` | `POST {id}` |
| `/mega-home/api/photos` | room backgrounds this home holds, as `{roomId: version}` |
| `/mega-home/api/photo/{room}` | `GET` the background, `POST` a JPEG to replace it, `DELETE` to remove it |

Home Assistant's own interface is untouched: it keeps `/` and its panels, and this integration
only adds routes under its own prefix.

### ⚠ There is no authentication yet

The endpoints above are currently open to anyone who can reach Home Assistant on the local
network. That is deliberate and temporary — authentication is the next phase — but it means
**this must not be put on a customer object in this state**.

## Action

`mega_home.sync` — fetch the configuration now, for the installer who has just changed the
project in the manager and does not want to wait for the next poll.

## Diagnostics

Settings → Devices & services → Mega Home → ⋮ → *Download diagnostics* reports the config
version, when the last successful sync happened, the last error, the current poll interval and
how many tiles have no `entity_id` yet (the usual reason a tile shows "no data").

## Tests

`pytest tests` — no Home Assistant installation needed. `tests/conftest.py` stubs the two names
the modules under test actually use and registers `mega_home` as a package without running its
`__init__.py`, so a checkout plus `pip install pytest` is the whole setup. Running the
integration in docker against a real manager stays the other half of the check: it covers what a
stub cannot (route registration, Home Assistant's own view plumbing), and neither replaces the
other.

## Requirements

Home Assistant 2025.7 or newer (the integration uses `async_register_static_paths`; the
blocking `register_static_path` it replaced was removed in 2025.7).
