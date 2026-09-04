"""Test harness: just enough Home Assistant to import the module under test.

⚠ The real `homeassistant` package is a hundred megabytes of dependencies, and
what these tests touch of it is two names: the `HomeAssistant` type (only ever
an annotation — `from __future__ import annotations` keeps it unevaluated) and
the `STORAGE_DIR` constant. Stubbing them keeps `pytest` runnable on a plain
checkout; Home Assistant itself is exercised by running the integration in
docker, which is a different kind of check and does not replace this one.

⚠ `mega_home` is registered here as a package with a `__path__` but WITHOUT its
real `__init__.py`. That is deliberate: importing the package for real drags in
config entries, the HTTP layer and the coordinator, so a test of one file would
depend on stubs for all of them. Submodules (`mega_home.bundle` and the
`.api` / `.const` they import) load normally.
"""

from __future__ import annotations

import sys
import types
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def _module(name: str, **attributes: object) -> types.ModuleType:
    module = types.ModuleType(name)
    for key, value in attributes.items():
        setattr(module, key, value)
    sys.modules[name] = module
    return module


class _HomeAssistant:  # noqa: D101 - annotation-only stand-in
    pass


class _Store:  # noqa: D101 - the coordinator only calls save/load on it
    def __init__(self, *args: object, **kwargs: object) -> None:
        self._data: object = None

    def __class_getitem__(cls, _item: object) -> type[_Store]:
        return cls

    async def async_save(self, data: object) -> None:
        self._data = data

    async def async_load(self) -> object:
        return self._data


class _DataUpdateCoordinator:  # noqa: D101 - stand-in for the HA base class
    def __class_getitem__(cls, _item: object) -> type[_DataUpdateCoordinator]:
        return cls

    def __init__(
        self,
        hass: object,
        logger: object,
        *,
        config_entry: object = None,
        name: str = "",
        update_interval: object = None,
    ) -> None:
        self.hass = hass
        self.logger = logger
        self.config_entry = config_entry
        self.name = name
        self.update_interval = update_interval
        self.data: object = None
        self.last_update_success = True


class _UpdateFailed(Exception):  # noqa: D101 - raised by the coordinator
    pass


class _ConfigEntry:  # noqa: D101 - annotation-only stand-in
    def __class_getitem__(cls, _item: object) -> type[_ConfigEntry]:
        return cls


_module("homeassistant")
_module("homeassistant.core", HomeAssistant=_HomeAssistant)
_module("homeassistant.config_entries", ConfigEntry=_ConfigEntry)
_module("homeassistant.helpers")
_module("homeassistant.helpers.storage", STORAGE_DIR=".storage", Store=_Store)
_module(
    "homeassistant.helpers.update_coordinator",
    DataUpdateCoordinator=_DataUpdateCoordinator,
    UpdateFailed=_UpdateFailed,
)
_module("homeassistant.util")


def _utcnow() -> object:
    from datetime import datetime, timezone

    return datetime.now(timezone.utc)


_module("homeassistant.util.dt", utcnow=_utcnow)

_package = types.ModuleType("mega_home")
_package.__path__ = [str(ROOT / "custom_components" / "mega_home")]
sys.modules["mega_home"] = _package
