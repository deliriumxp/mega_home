# Mega Home: design notes

Decisions and the reasons behind them. The product-level plan lives in the manager repository
(`docs/local-ha-app.md`); this file records what was decided *here* and what the platform
actually does — several things in it were found by running the integration, not by reading.

## Why an integration and not an add-on

Add-ons only exist on Home Assistant OS and Supervised installations. An integration works on
Container and Core too, which is what objects actually run. An add-on would only be worth it if
the remote-access phase needs its own process; the integration would stay the config source
even then.

## Why the cache is the source of truth

The requirement is not "works offline sometimes" — an object may be handed over and never see
the internet again. So:

- the config is loaded from `Store` **before** the first network call, and a failed first fetch
  is only fatal for a home that has never synchronised (there is genuinely nothing to serve);
- a failed poll raises `UpdateFailed`, which marks the entry as failing **without** discarding
  `coordinator.data` — the previous config keeps being served;
- the poll interval doubles from 15 minutes up to an hour on failure and resets on success, so
  a permanently offline object is not hammering a dead address forever.

`DataUpdateCoordinator` already logs the first failure and stays quiet afterwards, which is the
"one line per state change" the installers asked for.

## Version first, body second

Every poll asks `/inbound/home-config/version` and only fetches the body when the hash moved.
The manager computes that hash over the canonical config, so it changes exactly when the
content does.

The manager records the visit: a version poll updates only "last seen", while the version
itself is recorded when the body is really delivered (or confirmed by a 304). That is what the
object's card in the manager shows, and it is the only feedback there is — this is a pull
model, and a push would need to reach the object from outside, which is a later phase.

⚠ **"Version first, body second" applies to the CONFIG only — the app bundle is checked on
every poll regardless** (`_async_sync_bundle`, fixed 2026-09-05). The bundle check used to sit
past the early return, so on the common path — nothing changed in the composition, so the
version matches — the poll returned before ever looking at the bundle. A new interface could
then only arrive as an `app_changed` frame over the live link, and a home whose link was down
kept serving an old bundle forever. Seen on a real object: the manager had published a new
interface, the flat kept showing the copy packaged in the release, and nothing anywhere said
so. The poll is the safety net for BOTH; it was one for the config only. Regression test:
`tests/test_coordinator.py`.

⚠ **The bundle store is created by the coordinator's constructor, not by
`async_setup_entry`** (fixed 2026-09-05, one release after the above). It used to be created in
setup — and *after* the first refresh — so at startup `self.bundle` was None and the check was
skipped in silence. A new interface then could not appear before the next poll tick fifteen
minutes later, and if that tick found nothing changed under the old ordering, never. Found on a
real object from diagnostics: `app_checked_at: null` with `last_update_success: true`, i.e. "did
not try", not "could not". Order of initialisation now decides nothing; `async_setup_entry` only
calls `async_load()` (what is already on disk), and it does that before the first refresh so the
poll does not re-download a bundle the object already holds. Regression test:
`test_хранилище_бандла_есть_сразу_после_создания`.

⚠ **A bundle that cannot be fetched is reported, not swallowed.** `BundleStore.async_sync`
still never raises (the poll and the live link both call it and neither may break), but it now
records `last_error`; the coordinator logs it at warning level once per state change and
diagnostics carries `app_error` / `app_checked_at`. Before that, a home the interface never
reached looked perfectly healthy — the app worked, the log was silent, diagnostics said
nothing — and the cause had to be found by comparing screenshots.

## The address is reconfigurable

⚠ **A "reconfigure" step exists on purpose** (added 0.1.3). The manager address changes more than
once in the life of an object: it moves behind a reverse proxy and back out, the domain changes,
or the object is pointed straight at the inbound port because the proxy will not pass a WebSocket
upgrade. Without the step the only way to change it was to delete the entry and add it again,
which loses the entry and has to be explained over the phone.

Re-adding the same object still works and still updates the address — one entry per object token
— and `verify_ssl` now travels in that update too. It did not before, so moving an object from a
proxy to port 8055 with a self-signed certificate changed the address and silently kept
validating the certificate.

## Icons are files

Scenario icons used to be images served by the manager. On an object without internet that is a
blank tile, so the coordinator downloads each icon named in the config into
`.storage/mega_home_icons/` and they are served from there. Downloads are skipped for files
that already exist, and one failed icon never fails a sync — a home is usable without a
picture.

## Room photos are the home's own data (0.1.4)

The resident picks a background photo per room in the app's settings. Unlike everything else
this integration serves, these files are **not synchronised from the manager** — the home is
where they are created and the only place that holds them
(`.storage/mega_home_photos/`, `photos.py`, `api/photos` + `api/photo/{room}`).

Why here and not in the browser or in the manager: a background is decoration of the HOME.
Uploaded from a phone it has to appear on the hallway tablet too, and it must survive a browser
data wipe. (The theme, the interface scale and the blur/brightness of the background stay device
settings and live in the browser on purpose — a wall tablet and a phone are looked at from
different distances.)

- **The file name is a hash of the room id**, never the id itself. Room ids come off the wire
  from the manager, and hashing means there is nothing to validate and no way out of the
  directory.
- **Only a room the current config knows can be written.** That is the bound on the endpoint:
  the HTTP surface still has no authentication, so without it anyone on the local network could
  fill the object's disk. Same reason for the 4 MB cap and the JPEG magic-byte check — the app
  re-encodes whatever the resident picked before uploading, so one format is enough.
- ⚠ **The version in the image URL is a hash of the CONTENT, not the modification time.** The
  image is served `immutable`, so a repeated version means the resident replaces a background
  and never sees it change. The first implementation used `st_mtime_ns` and failed its own test
  on the first run: two saves in a row produced the same value.
- Photos of rooms that later disappear from the config are **not** deleted: a transient config
  glitch must not destroy the resident's pictures. `room_photos` in diagnostics is the count on
  disk, which is also how "this flat has no photos" is told apart from "the app is not showing
  them".

## Platform findings (verified against a running Home Assistant, 2026-09-04)

- **`hass.http.async_register_static_paths([StaticPathConfig(url, path, cache)])`** is the only
  supported way to serve files; the older `register_static_path` did blocking I/O in the event
  loop and was removed in 2025.7.
- **aiohttp's static resource answers `403` for a directory**, not the index file: `/mega-home`
  and `/mega-home/` both returned "403: Forbidden" while `/mega-home/index.html` served fine.
  Both spellings are therefore registered as redirects onto the entry point — and, because a
  static prefix swallows everything under it, those plain routes have to be registered *before*
  the static path.
- **Two static prefixes do not fall through to each other.** aiohttp resolves to the first
  resource whose prefix matches and serves (or 404s) from that one, so `/mega-home/icons` must
  be registered before `/mega-home`, or every icon would be looked for inside the bundle.
- **`DataUpdateCoordinator` has no `last_update_success_time`.** It tracks *whether* the last
  refresh succeeded, not when — the timestamp diagnostics report is kept by this integration
  (`last_success_at`). Writing that attribute from memory produced a 500 on the diagnostics
  download, caught only by fetching it.
- **Home Assistant's own frontend does not swallow our prefix.** Its `IndexView.resolve()`
  explicitly declines paths whose first segment is not a registered panel, which is why a
  third-party prefix is reachable at all; verified live — `/` kept answering while
  `/mega-home` served us.
- **Service calls can raise.** `climate.set_temperature` does not exist on an installation with
  no climate integration loaded, and `async_call` raises `ServiceNotFound` even with
  `blocking=False`. The command view turns that into a readable answer instead of a bare 500.

## One resident, two ways in — one `ops.py` (0.1.5)

A resident at home reaches this integration over HTTP. A resident who is AWAY cannot: their
phone talks to the manager, and the manager forwards the question down the live link this
integration already holds (`remote-access.md` in the manager repo). Both paths must give the
same data and the same refusal wording, so the operations themselves live in `ops.py` and know
nothing about transport: they return plain data and raise `OpError(message, status)`.

⚠ Before this, the rules lived inside the view handlers, welded to `web.Request`. The link
would have had to fake HTTP requests or grow a second copy of "which service does this command
map to" — the kind of pair that drifts apart silently and is only noticed as "the app works at
home but not outside".

⚠ **The house always answers.** A refusal travels as an ordinary frame with `ok: false` and the
status; an unexpected error becomes a generic message (internals are for the log, not for the
resident) — never a dropped frame. The manager waits with a three second timeout, so silence
would turn every "device not found" into "the house is offline", three seconds later, for a
resident standing in that house.

## Polling has to be switched on by hand (0.1.6)

`DataUpdateCoordinator` runs its timer only while it HAS LISTENERS, and listeners are entities.
This integration creates none (`PLATFORMS` is empty — it is a transport, a cache and a file
server), so after the first refresh at startup the home never asked the manager again. "Polling
as the safety net" existed only on paper: the config arrived by push alone, and a home whose
link was down lived on its cache until Home Assistant restarted.

Found on a live object: `last_success_at` and `app_checked_at` ten hours old, with
`update_interval_seconds: 900` and `last_update_success: true` — the diagnostics said "did not
try", not "could not". `MegaHomeCoordinator.keep_polling()` registers one empty listener, tied
to the config entry's unload.

## The states projection is a shared contract

`entity_view()` in `ops.py` produces exactly what the manager's
`smart-home-view.util.ts` produces (brightness as a percentage, cover position, the
capabilities list, `available`, `updatedAt` in epoch milliseconds). The app's screens read that
shape and know nothing about which side answered.

The duplication is deliberate and unavoidable: the entire point of the phase is that the
manager is not in the runtime path. It is a contract between two repositories — change it here
and there in the same breath.

## Registration happens once per Home Assistant run

Views and static paths live on the aiohttp application, which outlives a config entry reload,
and registering the same route twice raises. So `async_register_http()` runs once, guarded by a
flag in `hass.data`, and the views look the coordinator up per request instead of capturing it.
That also means an unloaded entry does not break the routes — they simply answer "not
synchronised yet".

## Not done here

- **Authentication.** Views are `requires_auth = False` and anyone on the local network can
  reach them. Deliberate for development, unacceptable on a customer object.
- **The app bundle.** `www/` currently holds a placeholder page; the real Angular build lands
  in the next phase, together with a self-hosted font (the manager's `index.html` pulls Inter
  from Google Fonts, which is another thing that does not exist on an offline object).
- **SPA deep links.** With the bundle served as a static prefix, a cold load of a sub-path will
  not fall back to `index.html`. That decision belongs with the bundle.
