"""Опрос как страховка: он обязан проверять И конфиг, И бандл интерфейса.

Дефект, который эти тесты закрывают, случился на реальном объекте. Менеджер
опубликовал новый интерфейс; состав квартиры при этом не менялся, поэтому версия
конфига совпадала, и `_async_update_data` выходил на первом же `if` — ДО проверки
бандла. Новый интерфейс мог доехать только push-кадром по живому каналу, а канал
лежал. Снаружи это выглядело нормой: приложение работает, в журнале тишина, в
диагностике пусто — разбираться пришлось сравнением скриншотов.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

from mega_home.coordinator import MegaHomeCoordinator


class _Config:
    def __init__(self, root: Path) -> None:
        self._root = root

    def path(self, *parts: str) -> str:
        return str(self._root.joinpath(*parts))


class FakeHass:
    def __init__(self, root: Path) -> None:
        self.config = _Config(root)

    async def async_add_executor_job(self, func: Any, *args: Any) -> Any:
        return func(*args)


class FakeClient:
    """Менеджер: отдаёт версию и конфиг, считает походы за конфигом."""

    def __init__(self, version: str = "v1") -> None:
        self.version = version
        self.config_calls = 0

    async def async_version(self) -> str:
        return self.version

    async def async_config(self) -> dict[str, Any]:
        self.config_calls += 1
        return {"version": self.version, "scenarios": []}


class FakeBundle:
    """Хранилище бандла: считает проверки и умеет «не смогло»."""

    def __init__(self, error: str | None = None) -> None:
        self.syncs = 0
        self.version = "sha256-old"
        self.last_error = error

    async def async_sync(self, version: str | None = None) -> bool:
        self.syncs += 1
        return False


def _coordinator(tmp_path: Path, client: FakeClient) -> MegaHomeCoordinator:
    return MegaHomeCoordinator(FakeHass(tmp_path), object(), client)


def test_бандл_проверяется_даже_когда_состав_не_менялся(tmp_path: Path) -> None:
    client = FakeClient()
    coordinator = _coordinator(tmp_path, client)
    bundle = FakeBundle()
    coordinator.bundle = bundle

    # Первый проход: конфига ещё нет, идём за ним целиком.
    asyncio.run(coordinator._async_update_data())
    coordinator.data = {"version": "v1"}
    assert client.config_calls == 1
    assert bundle.syncs == 1

    # Второй проход — тот самый частый случай: версия совпала, конфиг не
    # запрашивается. Бандл всё равно обязан быть проверен.
    asyncio.run(coordinator._async_update_data())
    assert client.config_calls == 1, "конфиг незачем тянуть, версия та же"
    assert bundle.syncs == 2, "а вот бандл проверить обязаны — иначе интерфейс не доедет"


def test_причина_неудачи_видна_в_диагностике(tmp_path: Path) -> None:
    client = FakeClient()
    coordinator = _coordinator(tmp_path, client)
    coordinator.bundle = FakeBundle(error="manifest unavailable: HTTP 404")

    asyncio.run(coordinator._async_update_data())

    assert coordinator.app_error == "manifest unavailable: HTTP 404"
    assert coordinator.app_checked_at is not None


def test_починилось_ошибка_снимается(tmp_path: Path) -> None:
    client = FakeClient()
    coordinator = _coordinator(tmp_path, client)
    bundle = FakeBundle(error="download failed: timeout")
    coordinator.bundle = bundle

    asyncio.run(coordinator._async_update_data())
    assert coordinator.app_error == "download failed: timeout"

    bundle.last_error = None
    coordinator.data = {"version": "v1"}
    asyncio.run(coordinator._async_update_data())
    assert coordinator.app_error is None


def test_хранилище_бандла_есть_сразу_после_создания(tmp_path: Path) -> None:
    """Проверка бандла не должна зависеть от порядка вызовов в `async_setup_entry`.

    Именно на этом и обожглись: хранилище создавалось в setup ПОСЛЕ первого
    опроса, поэтому на старте `self.bundle` был None и проверка молча
    пропускалась. Диагностика с живого объекта показала `app_checked_at: null`
    при `last_update_success: true` — «не пробовало», а не «не смогло».
    """
    coordinator = _coordinator(tmp_path, FakeClient())
    assert coordinator.bundle is not None
    # Раздавать что-то надо и до первой синхронизации: это копия из релиза.
    assert coordinator.bundle.active_dir.name == "www"


def test_первый_же_опрос_проверяет_бандл(tmp_path: Path) -> None:
    client = FakeClient()
    coordinator = _coordinator(tmp_path, client)
    bundle = FakeBundle()
    coordinator.bundle = bundle

    # Ровно то, что делает старт Home Assistant: опрос на пустом кэше.
    asyncio.run(coordinator._async_update_data())

    assert bundle.syncs == 1, "новый интерфейс обязан доехать на старте, а не через 15 минут"
    assert coordinator.app_checked_at is not None


def test_опрос_включается_явно_иначе_его_нет_вовсе(tmp_path: Path) -> None:
    """⚠ Регрессия, найденная на живом объекте: за десять часов ни одного опроса.

    `DataUpdateCoordinator` заводит таймер, только пока у него есть слушатели, а
    слушатели — это сущности; интеграция их не создаёт (`PLATFORMS` пуст).
    Поэтому опрос включается явно, и снимается он вместе с записью конфигурации —
    иначе «опрос как страховка» остаётся только на бумаге, и объект с лежащим
    каналом живёт на кэше до перезапуска Home Assistant.
    """
    coordinator = _coordinator(tmp_path, FakeClient())
    unloads: list[object] = []
    entry = type("Entry", (), {"async_on_unload": lambda self, cb: unloads.append(cb)})()

    coordinator.keep_polling(entry)

    assert len(coordinator.listeners) == 1
    assert len(unloads) == 1
    # Снятие подписки возвращает координатор в исходное состояние.
    unloads[0]()
    assert coordinator.listeners == []
