"""Обновление по кнопке менеджера (`ha_update.py`): файлы с менеджера, HA перезапускается."""

from __future__ import annotations

import asyncio
import hashlib
import json
from pathlib import Path
from typing import Any

import pytest

from fake_host import FakeSource
from mega_home import ha_update
from mega_home.core import ops
from mega_home.core.api import ManagerError
from mega_home.core.ops_base import OpError


class _Hass:
    def __init__(self) -> None:
        self.calls: list[tuple[str, str]] = []
        self.tasks: list[str] = []

    class _Services:
        def __init__(self, hass: _Hass) -> None:
            self.hass = hass

        async def async_call(self, domain, service, data, blocking=False):  # noqa: ANN001, ANN201
            self.hass.calls.append((domain, service))

    @property
    def services(self) -> _Hass._Services:
        return _Hass._Services(self)

    def async_create_background_task(self, coro, name):  # noqa: ANN001, ANN201
        self.tasks.append(name)
        coro.close()

    async def async_add_executor_job(self, func, *args):  # noqa: ANN001, ANN201
        return func(*args)


class _Client:
    """Менеджер, раздающий пакет `files` версии `version`."""

    def __init__(self, version: str, files: dict[str, bytes], broken: str | None = None) -> None:
        self.version = version
        self.files = files
        self.broken = broken
        self.fetched: list[str] = []

    async def async_integration_manifest(self) -> dict[str, Any]:
        if self.version == "unavailable":
            raise ManagerError("502")
        return {
            "version": self.version,
            "files": [
                {"path": path, "sha256": hashlib.sha256(data).hexdigest(), "bytes": len(data)}
                for path, data in self.files.items()
            ],
        }

    async def async_integration_file(self, path: str) -> bytes:
        self.fetched.append(path)
        return b"garbage" if path == self.broken else self.files[path]


def _package(tmp_path: Path, version: str) -> Path:
    package = tmp_path / "custom_components" / "mega_home"
    package.mkdir(parents=True)
    (package / "manifest.json").write_text(json.dumps({"version": version}), "utf-8")
    (package / "old.py").write_text("old", "utf-8")
    return package


def _new(version: str) -> dict[str, bytes]:
    return {
        "manifest.json": json.dumps({"version": version}).encode(),
        "core/new.py": b"new",
    }


def _run(hass: _Hass, client: _Client, package: Path, wanted: str | None = None) -> dict:
    return asyncio.run(ha_update.async_self_update(hass, client, wanted, package))


def test_новая_версия_скачивается_встаёт_на_место_и_перезапускает(tmp_path: Path) -> None:
    package = _package(tmp_path, "0.4.9")
    hass = _Hass()
    answer = _run(hass, _Client("0.5.0", _new("0.5.0")), package, "0.5.0")

    assert (package / "core" / "new.py").read_text() == "new"
    assert not (package / "old.py").exists()
    # Прежний пакет остаётся рядом — откат без выезда.
    assert (package.parent / ".mega_home.prev" / "old.py").read_text() == "old"
    assert not (package.parent / ".mega_home.new").exists()
    # ⚠ Перезапуск — отложенной задачей, а не прямо здесь: ответ менеджеру
    # обязан уйти раньше, чем HA закроет канал.
    assert ("homeassistant", "restart") not in hass.calls
    assert hass.tasks == ["mega_home self-update restart"]
    assert answer["installing"] is True and answer["restarting"] is True
    assert (answer["installed"], answer["latest"], answer["onDisk"]) == ("0.4.9", "0.5.0", "0.5.0")


def test_манифест_главнее_подсказки_менеджера(tmp_path: Path) -> None:
    package = _package(tmp_path, "0.4.9")
    answer = _run(_Hass(), _Client("0.5.1", _new("0.5.1")), package, "0.5.0")
    assert answer["target"] == "0.5.1" and answer["installing"] is True


def test_на_диске_уже_та_версия_в_памяти_старая_только_перезапуск(tmp_path: Path) -> None:
    """Файлы уже положили, HA не перезапускали — лечится ровно перезапуском."""
    package = _package(tmp_path, "9.9.9")
    hass = _Hass()
    client = _Client("9.9.9", _new("9.9.9"))
    answer = _run(hass, client, package)
    assert client.fetched == [] and (package / "old.py").exists()
    assert hass.tasks and answer["installing"] is False and answer["restarting"] is True


def test_всё_совпадает_ни_скачивания_ни_перезапуска(tmp_path: Path) -> None:
    """Живой объект 2026-09-20: перезапуск «на всякий случай» — минута без дома ни за что."""
    package = _package(tmp_path, ha_update.INTEGRATION_VERSION)
    hass = _Hass()
    answer = _run(hass, _Client(ha_update.INTEGRATION_VERSION, {}), package)
    assert hass.tasks == [] and answer["restarting"] is False and answer["installing"] is False


def test_битая_контрольная_сумма_пакет_не_тронут_перезапуска_нет(tmp_path: Path) -> None:
    package = _package(tmp_path, "0.4.9")
    hass = _Hass()
    with pytest.raises(OpError, match="контрольная сумма"):
        _run(hass, _Client("0.5.0", _new("0.5.0"), broken="core/new.py"), package)
    assert (package / "old.py").exists() and not (package / "core").exists()
    assert not (package.parent / ".mega_home.new").exists()
    assert hass.tasks == []


def test_путь_вне_пакета_отвергается(tmp_path: Path) -> None:
    package = _package(tmp_path, "0.4.9")
    files = {"../evil.py": b"x", "manifest.json": b"{}"}
    with pytest.raises(OpError, match="за каталог"):
        _run(_Hass(), _Client("0.5.0", files), package)
    assert not (package.parent / "evil.py").exists()


def test_менеджер_не_отдал_манифест_понятный_отказ(tmp_path: Path) -> None:
    package = _package(tmp_path, "0.4.9")
    hass = _Hass()
    with pytest.raises(OpError, match="не отдал"):
        _run(hass, _Client("unavailable", {}), package)
    assert hass.tasks == []


def test_ядро_зовёт_то_что_дал_адаптер() -> None:
    class _Coordinator:
        data = {"tiles": []}
        source = FakeSource()

        async def self_update(self, wanted: str | None = None) -> dict:
            return {"restarting": True, "wanted": wanted}

    assert asyncio.run(ops.run(_Coordinator(), "self-update", None)) == {
        "restarting": True,
        "wanted": None,
    }
    assert asyncio.run(ops.run(_Coordinator(), "self-update", {"version": "0.4.1"}))["wanted"] == "0.4.1"
    _Coordinator.self_update = None  # type: ignore[assignment]
    with pytest.raises(OpError, match="не умеет"):
        asyncio.run(ops.run(_Coordinator(), "self-update", None))
