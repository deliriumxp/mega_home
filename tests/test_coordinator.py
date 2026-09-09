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

from mega_home.api import ManagerError
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
        # Комнаты конфига и походы за их фонами — для проверки синхронизации
        # заготовок инсталлятора.
        self.rooms: list[dict[str, Any]] = []
        self.tiles: list[dict[str, Any]] = []
        # Общий канал файлов: что назвал менеджер, за тем дом и сходил.
        self.assets: dict[str, Any] = {}
        self.asset_calls: list[str] = []
        self.photo_fails: set[str] = set()

    async def async_version(self) -> str:
        return self.version

    async def async_config(self) -> dict[str, Any]:
        self.config_calls += 1
        return {
            "version": self.version,
            "scenarios": [],
            "rooms": self.rooms,
            "tiles": self.tiles,
            "assets": self.assets,
        }

    async def async_asset(self, key: str) -> bytes:
        self.asset_calls.append(key)
        if key in self.photo_fails:
            raise ManagerError("HTTP 404")
        return b"asset:" + key.encode()


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
    # ⚠ До первой синхронизации раздавать НЕЧЕГО: копии интерфейса в релизе нет,
    # и это не поломка, а заглушка «Подключаюсь к менеджеру…» из `http.py`.
    assert coordinator.bundle.active_dir is None


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


# --- зеркала фонов БОЛЬШЕ НЕТ ---
#
# ⚠ Спеки на «заготовки инсталлятора» сняты вместе с самим зеркалом (0.2.20):
# фоны комнат и плиток ездят ОБЩИМ каналом файлов, и всё, что они проверяли —
# качать только недостающее, выбрасывать снятое, переживать недоступный файл —
# проверяется ниже на нём же. Возвращать второй водопровод не надо: фон плитки
# он качал ДВАЖДЫ, обоими путями сразу.


# --- ОБЩИЙ канал файлов (0.1.17) ---
#
# Ради него всё и делалось: дом должен уметь забрать из менеджера ЛЮБОЙ файл,
# ничего о нём не зная. Проверяем именно это — что цикл не знает про фото,
# звуки и шрифты, а идёт по манифесту, и что снятое из манифеста исчезает с
# диска. Знание о видах файлов здесь = следующий релиз HACS ради каждой мелочи.
def test_дом_забирает_любой_файл_по_манифесту(tmp_path: Path) -> None:
    client = FakeClient()
    client.assets = {
        "photo/tile/ha:light.lamp1": {"v": "a1", "type": "image/jpeg"},
        "sound/doorbell": {"v": "s1", "type": "audio/mpeg"},
        "font/inter": {"v": "", "type": "font/woff2"},  # без версии — не файл
    }
    coordinator = _coordinator(tmp_path, client)
    coordinator.bundle = FakeBundle()

    asyncio.run(coordinator._async_update_data())

    # Звук интеграции незнаком ровно так же, как и фон, — и это правильно.
    assert client.asset_calls == ["photo/tile/ha:light.lamp1", "sound/doorbell"]
    assert coordinator.assets.has("sound/doorbell", "s1")
    assert coordinator.assets.path("sound/doorbell", "s1").read_bytes() == b"asset:sound/doorbell"

    # Второй опрос: те же версии уже на диске — качать нечего.
    coordinator.data = None
    asyncio.run(coordinator._async_update_data())
    assert client.asset_calls == ["photo/tile/ha:light.lamp1", "sound/doorbell"]


def test_снятый_из_манифеста_файл_исчезает_из_дома(tmp_path: Path) -> None:
    client = FakeClient()
    client.assets = {"sound/doorbell": {"v": "s1", "type": "audio/mpeg"}}
    coordinator = _coordinator(tmp_path, client)
    coordinator.bundle = FakeBundle()
    asyncio.run(coordinator._async_update_data())
    assert coordinator.assets.has("sound/doorbell", "s1")

    # Менеджер заменил один файл и снял другой.
    client.assets = {"sound/doorbell": {"v": "s2", "type": "audio/mpeg"}}
    coordinator.data = None
    asyncio.run(coordinator._async_update_data())

    assert coordinator.assets.has("sound/doorbell", "s2")
    assert not coordinator.assets.has("sound/doorbell", "s1"), "старая версия — мусор на диске"

    client.assets = {}
    coordinator.data = None
    asyncio.run(coordinator._async_update_data())
    assert coordinator.assets.count() == 0


def test_недоступный_файл_не_роняет_синхронизацию(tmp_path: Path) -> None:
    client = FakeClient()
    client.assets = {
        "sound/doorbell": {"v": "s1", "type": "audio/mpeg"},
        "photo/tile/x": {"v": "p1", "type": "image/jpeg"},
    }
    client.photo_fails = {"sound/doorbell"}
    coordinator = _coordinator(tmp_path, client)
    coordinator.bundle = FakeBundle()

    asyncio.run(coordinator._async_update_data())

    assert not coordinator.assets.has("sound/doorbell", "s1")
    assert coordinator.assets.has("photo/tile/x", "p1"), "соседний файл обязан доехать"
