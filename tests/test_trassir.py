"""Лента событий объекта: учётка, дедупликация и трим.

Дефекты, которые здесь заперты, все родом с живого регистратора:

* `/events` — очередь СЕССИИ, и новая сессия приносит до сотни уже отданных
  событий заново. Без дедупликации лента жильца удваивалась бы при каждом
  переподключении, а не «иногда»;
* метка времени TRASSIR сдвинута на пояс сервера, поэтому возраст события
  считается в ЕГО шкале, а не по нашим часам;
* учётку нельзя тянуть при каждом опросе (лишний поход к менеджеру) и нельзя не
  тянуть никогда (работа сменённым паролем до перезапуска Home Assistant) —
  ориентир один: отпечаток в конфиге.
"""

from __future__ import annotations

import asyncio
from pathlib import Path
from typing import Any

from mega_home.api import ManagerError
from mega_home.trassir import TrassirGateway


class _Config:
    def __init__(self, root: Path) -> None:
        self._root = root

    def path(self, *parts: str) -> str:
        return str(self._root.joinpath(*parts))


class FakeHass:
    def __init__(self, root: Path) -> None:
        self.config = _Config(root)
        self.tasks: list[Any] = []

    def async_create_background_task(self, coro: Any, name: str) -> Any:
        # Опрос в спеках не крутим: он проверяется вызовом одного прохода.
        coro.close()
        self.tasks.append(name)
        return _Task()


class _Task:
    def done(self) -> bool:
        return False

    def cancel(self) -> None:
        pass


class FakeManager:
    def __init__(self, creds: dict[str, str] | None = None) -> None:
        self.creds = creds or {
            "username": "megahome",
            "password": "s3cret",
            "sdkPassword": "sdk-s3cret",
        }
        self.calls = 0
        self.fail = False

    async def async_trassir_credentials(self) -> dict[str, str]:
        self.calls += 1
        if self.fail:
            raise ManagerError("менеджер недоступен")
        return dict(self.creds)


class FakeClient:
    """Регистратор: отдаёт заданные события и каналы."""

    def __init__(self, events: list[dict[str, Any]] | None = None) -> None:
        self.events_queue = events or []
        self.channels_data = [{"guid": "cam1", "name": "Вход", "rights": "8975"}]
        self.channel_calls = 0

    async def async_events(self) -> list[dict[str, Any]]:
        return list(self.events_queue)

    async def async_channels(self) -> list[dict[str, Any]]:
        self.channel_calls += 1
        return list(self.channels_data)


def gateway(tmp_path: Path, manager: FakeManager | None = None) -> TrassirGateway:
    return TrassirGateway(FakeHass(tmp_path), manager or FakeManager(), object())


def config(fingerprint: str = "fp1", host: str = "192.168.1.50") -> dict[str, Any]:
    return {
        "version": "v1",
        "trassir": {
            "host": host,
            "port": 8080,
            "rtspPort": 555,
            "clipSeconds": 60,
            "credentials": fingerprint,
        },
    }


def test_object_without_trassir_asks_the_manager_for_nothing(tmp_path: Path) -> None:
    manager = FakeManager()
    gate = gateway(tmp_path, manager)

    asyncio.run(gate.async_apply({"version": "v1"}))

    assert not gate.configured
    assert manager.calls == 0


def test_credentials_are_fetched_once_per_fingerprint(tmp_path: Path) -> None:
    manager = FakeManager()
    gate = gateway(tmp_path, manager)

    async def scenario() -> None:
        await gate.async_apply(config("fp1"))
        await gate.async_apply(config("fp1"))  # ничего не менялось
        await gate.async_apply(config("fp2"))  # пароль сменили в менеджере

    asyncio.run(scenario())

    assert manager.calls == 2
    assert gate.configured


def test_unreachable_manager_keeps_the_previous_credentials(tmp_path: Path) -> None:
    manager = FakeManager()
    gate = gateway(tmp_path, manager)

    async def scenario() -> None:
        await gate.async_apply(config("fp1"))
        manager.fail = True
        await gate.async_apply(config("fp2"))

    asyncio.run(scenario())

    # Связь с менеджером пропадает регулярно, пароль меняют редко: остановиться
    # означало бы гасить видеонаблюдение при каждом обрыве интернета.
    assert gate.configured


def test_first_credentials_failure_leaves_the_gateway_idle(tmp_path: Path) -> None:
    manager = FakeManager()
    manager.fail = True
    gate = gateway(tmp_path, manager)

    asyncio.run(gate.async_apply(config()))

    assert not gate.configured, "без учётки к регистратору идти не с чем"


def test_events_are_deduplicated_across_relogins(tmp_path: Path) -> None:
    raw = [
        {"timestamp": "1788960515770731", "type": "Motion Start", "origin": "cam1"},
        {"timestamp": "1788960523369911", "type": "Motion Stop", "origin": "cam1"},
    ]
    gate = gateway(tmp_path)
    client = FakeClient(raw)

    async def scenario() -> None:
        await gate.async_apply(config())
        gate._client = client  # noqa: SLF001 — подменяем регистратор целиком
        await gate._async_poll_once()  # noqa: SLF001
        await gate._async_poll_once()  # noqa: SLF001 — как после перелогина

    asyncio.run(scenario())

    rows = gate.events()
    assert len(rows) == 2, "повторно отданные события не должны удваивать ленту"
    assert rows[0]["timestampUs"] == 1788960523369911, "новые сверху"
    assert rows[0]["cameraName"] == "Вход", "имя камеры подставляется из каналов"


def test_unknown_camera_keeps_its_guid_as_a_name(tmp_path: Path) -> None:
    gate = gateway(tmp_path)
    client = FakeClient([{"timestamp": "1", "type": "Motion Start", "origin": "ghost"}])

    async def scenario() -> None:
        await gate.async_apply(config())
        gate._client = client  # noqa: SLF001
        await gate._async_poll_once()  # noqa: SLF001

    asyncio.run(scenario())

    assert gate.events()[0]["cameraName"] == "ghost"


def test_feed_is_trimmed_by_age_in_trassir_scale(tmp_path: Path) -> None:
    newest = 1_788_960_000_000_000
    old = newest - 8 * 24 * 3600 * 1_000_000  # старше недели
    gate = gateway(tmp_path)
    client = FakeClient(
        [
            {"timestamp": str(old), "type": "Motion Start", "origin": "cam1"},
            {"timestamp": str(newest), "type": "Motion Start", "origin": "cam1"},
        ]
    )

    async def scenario() -> None:
        await gate.async_apply(config())
        gate._client = client  # noqa: SLF001
        await gate._async_poll_once()  # noqa: SLF001

    asyncio.run(scenario())

    kept = gate.events()
    assert [row["timestampUs"] for row in kept] == [newest]


def test_events_can_be_filtered_by_camera_and_paged(tmp_path: Path) -> None:
    gate = gateway(tmp_path)
    client = FakeClient(
        [
            {"timestamp": "100", "type": "Motion Start", "origin": "cam1"},
            {"timestamp": "200", "type": "Motion Start", "origin": "cam2"},
            {"timestamp": "300", "type": "Motion Stop", "origin": "cam1"},
        ]
    )

    async def scenario() -> None:
        await gate.async_apply(config())
        gate._client = client  # noqa: SLF001
        await gate._async_poll_once()  # noqa: SLF001

    asyncio.run(scenario())

    assert [row["timestampUs"] for row in gate.events(guid="cam1")] == [300, 100]
    assert [row["timestampUs"] for row in gate.events(before=300)] == [200, 100]
    assert gate.event(gate.events()[0]["id"])["timestampUs"] == 300


def test_one_page_of_the_feed_is_capped(tmp_path: Path) -> None:
    """Потолок страницы держит шлюз, а не вежливость приложения."""
    gate = gateway(tmp_path)
    client = FakeClient(
        [
            {"timestamp": str(1000 + i), "type": "Motion Start", "origin": "cam1"}
            for i in range(300)
        ]
    )

    async def scenario() -> None:
        await gate.async_apply(config())
        gate._client = client  # noqa: SLF001
        await gate._async_poll_once()  # noqa: SLF001

    asyncio.run(scenario())

    assert len(gate.events(limit=100000)) == 200
    assert len(gate.events(limit=0)) == 1, "бессмысленный предел не должен ронять ленту"


def test_cameras_carry_the_codec_and_the_archive_flag(tmp_path: Path) -> None:
    gate = gateway(tmp_path)
    client = FakeClient()
    client.channels_data = [
        {"guid": "cam1", "name": "Вход", "rights": "8975", "codec": "h264"},
        {"guid": "cam2", "name": "Склад", "rights": "8973", "codec": "h265"},
        {"guid": "cam3", "name": "Без прав", "codec": "h264"},
    ]

    async def scenario() -> list[dict[str, Any]]:
        await gate.async_apply(config())
        gate._client = client  # noqa: SLF001
        # Карта «guid → плитка» приходит снаружи: её собирает `ops` по АДРЕСУ
        # потока камеры, а не по имени.
        return await gate.async_cameras({"cam1": "tile-1"})

    cameras = asyncio.run(scenario())

    assert cameras[0] == {
        "guid": "cam1",
        "name": "Вход",
        "codec": "h264",
        "hasArchive": True,
        "tile": "tile-1",
    }
    assert cameras[1]["hasArchive"] is False, "бит архива снят — так и показываем"
    # ⚠ Неизвестная маска = «архив есть»: на живом регистраторе раскладка битов
    # не совпала с документированной, и запрет по ней спрятал бы работающую камеру.
    assert cameras[2]["hasArchive"] is True


def test_stored_feed_survives_a_restart(tmp_path: Path) -> None:
    gate = gateway(tmp_path)
    client = FakeClient([{"timestamp": "5", "type": "Motion Start", "origin": "cam1"}])

    async def scenario() -> None:
        await gate.async_apply(config())
        gate._client = client  # noqa: SLF001
        await gate._async_poll_once()  # noqa: SLF001

    asyncio.run(scenario())
    stored = gate._store  # noqa: SLF001

    revived = gateway(tmp_path)
    revived._store = stored  # noqa: SLF001 — то же хранилище, другой запуск
    asyncio.run(revived.async_load())

    assert [row["timestampUs"] for row in revived.events()] == [5]
    assert revived._creds.get("sdkPassword") == "sdk-s3cret"  # noqa: SLF001
