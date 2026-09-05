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
        # ⚠ Таймер настоящего координатора работает, только пока есть слушатели —
        # ради этого свойства и держим их здесь (см. `keep_polling`).
        self.listeners: list[object] = []

    def async_add_listener(self, callback: object, context: object = None):
        self.listeners.append(callback)

        def unsubscribe() -> None:
            self.listeners.remove(callback)

        return unsubscribe


class _UpdateFailed(Exception):  # noqa: D101 - raised by the coordinator
    pass


class _ConfigEntry:  # noqa: D101 - annotation-only stand-in
    def __class_getitem__(cls, _item: object) -> type[_ConfigEntry]:
        return cls


class _State:  # noqa: D101 - one Home Assistant state, as `ops` reads it
    def __init__(self, state: str, attributes: dict | None = None, last_updated=None) -> None:
        from datetime import datetime, timezone

        self.state = state
        self.attributes = attributes or {}
        self.last_updated = last_updated or datetime.now(timezone.utc)


class _ServiceNotFound(Exception):  # noqa: D101 - raised by hass.services
    pass


_module("homeassistant")
_module("homeassistant.core", HomeAssistant=_HomeAssistant, State=_State)
_module("homeassistant.exceptions", ServiceNotFound=_ServiceNotFound)
class _ConfigFlow:  # noqa: D101 - stand-in for the HA base class
    def __init_subclass__(cls, **kwargs: object) -> None:
        super().__init_subclass__()


_module(
    "homeassistant.config_entries",
    ConfigEntry=_ConfigEntry,
    ConfigFlow=_ConfigFlow,
    ConfigFlowResult=dict,
)
# `voluptuous` и клиентская сессия нужны только для импорта модуля потока:
# проверяется в нём функция разбора адреса, а не формы Home Assistant.
class _Invalid(Exception):  # noqa: D101 - voluptuous' own rejection
    pass


_module(
    "voluptuous",
    Schema=lambda *a, **k: None,
    Required=lambda *a, **k: None,
    Optional=lambda *a, **k: None,
    Invalid=_Invalid,
)
_module("homeassistant.helpers.aiohttp_client", async_get_clientsession=lambda *a, **k: None)
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
