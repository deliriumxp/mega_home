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

## Icons are files

Scenario icons used to be images served by the manager. On an object without internet that is a
blank tile, so the coordinator downloads each icon named in the config into
`.storage/mega_home_icons/` and they are served from there. Downloads are skipped for files
that already exist, and one failed icon never fails a sync — a home is usable without a
picture.

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

## The states projection is a shared contract

`_entity_view()` in `http.py` produces exactly what the manager's
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
