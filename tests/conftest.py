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


_module("homeassistant")
_module("homeassistant.core", HomeAssistant=_HomeAssistant)
_module("homeassistant.helpers")
_module("homeassistant.helpers.storage", STORAGE_DIR=".storage")

_package = types.ModuleType("mega_home")
_package.__path__ = [str(ROOT / "custom_components" / "mega_home")]
sys.modules["mega_home"] = _package
