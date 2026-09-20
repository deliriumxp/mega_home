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
        self.gate: asyncio.Event | None = None

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
        if self.gate is not None:
            await self.gate.wait()
        self.fetched.append(path)
        return b"garbage" if path == self.broken else self.files[path]


def _package(tmp_path: Path, version: str) -> Path:
    package = tmp_path / "config" / "custom_components" / "mega_home"
    package.mkdir(parents=True)
    (package / "manifest.json").write_text(json.dumps({"version": version}), "utf-8")
    (package / "__init__.py").write_text("old", "utf-8")
    return package


def _new(version: str, init: str = "new") -> dict[str, bytes]:
    return {
        "manifest.json": json.dumps({"version": version}).encode(),
        "__init__.py": init.encode(),
        "core/new.py": b"new",
    }


def _run(hass: _Hass, client: _Client, package: Path, wanted: str | None = None) -> dict:
    return asyncio.run(ha_update.async_self_update(hass, client, wanted, package))


def test_новая_версия_скачивается_встаёт_на_место_и_перезапускает(tmp_path: Path) -> None:
    package = _package(tmp_path, "0.4.9")
    hass = _Hass()
    answer = _run(hass, _Client("0.5.0", _new("0.5.0")), package, "0.5.0")

    assert (package / "core" / "new.py").read_text() == "new"
    assert (package / "__init__.py").read_text() == "new"
    # Прежний пакет остаётся — откат без выезда, — но ВНЕ custom_components:
    # загрузчик HA берёт любой каталог с manifest.json, точка его не останавливает.
    config = package.parent.parent
    assert (config / ".mega_home_update" / "prev" / "__init__.py").read_text() == "old"
    assert [p.name for p in (config / "custom_components").iterdir()] == ["mega_home"]
    assert not (config / ".mega_home_update" / "new").exists()
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


def test_та_же_версия_но_другое_содержимое_ставится(tmp_path: Path) -> None:
    """Правка без подъёма номера иначе не доезжала бы никогда (ревью 2026-09-20)."""
    package = _package(tmp_path, "0.5.0")
    hass = _Hass()
    answer = _run(hass, _Client("0.5.0", _new("0.5.0", init="fixed")), package)
    assert (package / "__init__.py").read_text() == "fixed"
    assert answer["installing"] is True and hass.tasks


def test_на_диске_ровно_то_что_раздаёт_менеджер_в_памяти_старая_только_перезапуск(tmp_path: Path) -> None:
    """Файлы уже положили, HA не перезапускали — лечится ровно перезапуском."""
    files = _new("9.9.9")
    package = _package(tmp_path, "9.9.9")
    for path, data in files.items():
        (package / path).parent.mkdir(exist_ok=True)
        (package / path).write_bytes(data)
    hass = _Hass()
    client = _Client("9.9.9", files)
    answer = _run(hass, client, package)
    assert client.fetched == []
    assert hass.tasks and answer["installing"] is False and answer["restarting"] is True


def test_всё_совпадает_ни_скачивания_ни_перезапуска(tmp_path: Path) -> None:
    """Живой объект 2026-09-20: перезапуск «на всякий случай» — минута без дома ни за что."""
    files = _new(ha_update.INTEGRATION_VERSION)
    package = _package(tmp_path, ha_update.INTEGRATION_VERSION)
    for path, data in files.items():
        (package / path).parent.mkdir(exist_ok=True)
        (package / path).write_bytes(data)
    hass = _Hass()
    answer = _run(hass, _Client(ha_update.INTEGRATION_VERSION, files), package)
    assert hass.tasks == [] and answer["restarting"] is False and answer["installing"] is False


def test_на_диске_новее_чем_у_менеджера_понижения_нет(tmp_path: Path) -> None:
    """«Применить 0.5.1» не должно откатывать дом на 0.5.0, которую раздаёт менеджер."""
    package = _package(tmp_path, "9.9.9")
    hass = _Hass()
    client = _Client("0.5.0", _new("0.5.0"))
    answer = _run(hass, client, package, "9.9.9")
    assert client.fetched == [] and (package / "__init__.py").read_text() == "old"
    # Загруженная версия (константа) от 9.9.9 отличается — перезапуск нужен.
    assert answer["installing"] is False and answer["restarting"] is True and answer["onDisk"] == "9.9.9"


def test_битая_контрольная_сумма_пакет_не_тронут_перезапуска_нет(tmp_path: Path) -> None:
    package = _package(tmp_path, "0.4.9")
    hass = _Hass()
    with pytest.raises(OpError, match="контрольная сумма"):
        _run(hass, _Client("0.5.0", _new("0.5.0"), broken="core/new.py"), package)
    assert (package / "__init__.py").read_text() == "old" and not (package / "core").exists()
    assert not (package.parent.parent / ".mega_home_update" / "new").exists()
    assert hass.tasks == []


def test_манифест_без_обязательных_файлов_отвергается(tmp_path: Path) -> None:
    """Менеджер собирает список обходом каталога в момент запроса: во время
    `git pull` он согласован, но неполон. Пакет без `__init__.py` — не интеграция."""
    package = _package(tmp_path, "0.4.9")
    hass = _Hass()
    files = {"manifest.json": json.dumps({"version": "0.5.0"}).encode(), "core/new.py": b"x"}
    with pytest.raises(OpError, match="__init__.py"):
        _run(hass, _Client("0.5.0", files), package)
    assert (package / "__init__.py").read_text() == "old" and hass.tasks == []


def test_скачанный_манифест_другой_версии_отвергается(tmp_path: Path) -> None:
    package = _package(tmp_path, "0.4.9")
    with pytest.raises(OpError, match="объявлена 0.5.0"):
        _run(_Hass(), _Client("0.5.0", _new("0.4.9")), package)
    assert (package / "__init__.py").read_text() == "old"


def test_путь_вне_пакета_отвергается(tmp_path: Path) -> None:
    package = _package(tmp_path, "0.4.9")
    files = {**_new("0.5.0"), "../evil.py": b"x"}
    with pytest.raises(OpError, match="за каталог"):
        _run(_Hass(), _Client("0.5.0", files), package)
    assert not (package.parent.parent / ".mega_home_update" / "evil.py").exists()


def test_менеджер_не_отдал_манифест_понятный_отказ(tmp_path: Path) -> None:
    package = _package(tmp_path, "0.4.9")
    hass = _Hass()
    with pytest.raises(OpError, match="не отдал"):
        _run(hass, _Client("unavailable", {}), package)
    assert hass.tasks == []


def test_второе_нажатие_во_время_скачивания_отказ_а_не_вторая_подмена(tmp_path: Path) -> None:
    """Две загрузки в один черновик сносили бы файлы друг друга (ревью 2026-09-20)."""
    package = _package(tmp_path, "0.4.9")
    hass = _Hass()

    async def scenario() -> tuple[dict, OpError]:
        client = _Client("0.5.0", _new("0.5.0"))
        client.gate = asyncio.Event()
        first = asyncio.create_task(ha_update.async_self_update(hass, client, None, package))
        await asyncio.sleep(0)
        with pytest.raises(OpError, match="уже идёт") as second:
            await ha_update.async_self_update(hass, client, None, package)
        client.gate.set()
        return await first, second.value

    answer, second = asyncio.run(scenario())
    assert answer["installing"] is True and second.status == 409
    assert hass.tasks == ["mega_home self-update restart"]


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
