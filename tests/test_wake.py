"""Включение по сети на чужую команду «включи» (`core/wake.py`).

Подписка по плиткам с `wake` и `wakeTrigger`; срабатывание шлёт вызов `connect` плитки;
переподписка только на смену триггера; неудача повторяется не чаще `RETRY_S`.
"""

from __future__ import annotations

import asyncio
from typing import Any

from mega_home.core.ops_base import OpError
from mega_home.core.wake import RETRY_S, WakeOnRequest, wake_tiles

WAKE = {"kind": "udp", "host": "192.168.1.255", "port": 9, "sendBase64": "AA==", "readUntil": {"deadline": 0}}


def config(trigger: str = "webostv.turn_on", wake: dict | None = None) -> dict[str, Any]:
    return {
        "tiles": [
            {"id": "t-tv", "entityId": "media_player.tv", "domain": "media_player", "wake": wake or WAKE, "wakeTrigger": trigger},
            # Без триггера — пакет шлёт только приложение, дом не подписывается.
            {"id": "t-tv2", "entityId": "media_player.tv2", "domain": "media_player", "wake": WAKE},
            {"id": "t-light", "entityId": "light.a", "domain": "light"},
        ]
    }


class Source:
    """Источник с подпиской «просят включить»: запоминает подписки, отписки и отказы."""

    def __init__(self, fail: bool = False) -> None:
        self.fail = fail
        self.attached: list[tuple[str, str]] = []
        self.removed: list[str] = []
        self.actions: dict[str, Any] = {}

    async def on_turn_on(self, entity_id: str, trigger: str, action: Any) -> Any:
        self.attached.append((entity_id, trigger))
        if self.fail:
            return None
        self.actions[entity_id] = action
        return lambda: self.removed.append(entity_id)


class Clock:
    def __init__(self) -> None:
        self.now = 1000.0

    def __call__(self) -> float:
        return self.now


def run(coro: Any) -> Any:
    return asyncio.run(coro)


def make(source: Source, sent: list, clock: Clock | None = None, raises: Exception | None = None) -> WakeOnRequest:
    async def send(payload: dict[str, Any]) -> dict[str, Any]:
        sent.append(payload)
        if raises:
            raise raises
        return {"bodyBase64": ""}

    return WakeOnRequest(source, send, clock or Clock())


def test_плитки_только_с_триггером_и_вызовом() -> None:
    assert wake_tiles(config()) == {"media_player.tv": ("webostv.turn_on", WAKE)}
    assert wake_tiles(None) == {}
    assert wake_tiles({"tiles": [{"entityId": "media_player.tv", "wakeTrigger": "x", "wake": "не объект"}]}) == {}


def test_просьба_включить_шлёт_пакет_плитки() -> None:
    source, sent = Source(), []
    wake = make(source, sent)

    async def go() -> None:
        await wake.track(config())
        await source.actions["media_player.tv"]()

    run(go())
    assert source.attached == [("media_player.tv", "webostv.turn_on")]
    assert sent == [WAKE]
    assert wake.summary["attached"] == {"media_player.tv": "webostv.turn_on"}
    assert wake.summary["fired"] == 1


def test_опрос_не_переподписывает_а_смена_пакета_доезжает_без_переподписки() -> None:
    source, sent = Source(), []
    wake = make(source, sent)
    other = {**WAKE, "host": "10.0.0.255"}

    async def go() -> None:
        await wake.track(config())
        await wake.track(config())
        await wake.track(config(wake=other))
        await source.actions["media_player.tv"]()

    run(go())
    assert source.attached == [("media_player.tv", "webostv.turn_on")]
    assert source.removed == []
    assert sent == [other]


def test_смена_триггера_и_пропажа_плитки_снимают_подписку() -> None:
    source, sent = Source(), []
    wake = make(source, sent)

    async def go() -> None:
        await wake.track(config())
        await wake.track(config(trigger="samsungtv.turn_on"))
        await wake.track({"tiles": []})

    run(go())
    assert source.attached == [("media_player.tv", "webostv.turn_on"), ("media_player.tv", "samsungtv.turn_on")]
    assert source.removed == ["media_player.tv", "media_player.tv"]
    assert wake.summary["attached"] == {}


def test_срабатывание_снятой_плитки_ничего_не_шлёт() -> None:
    source, sent = Source(), []
    wake = make(source, sent)

    async def go() -> None:
        await wake.track(config())
        action = source.actions["media_player.tv"]
        await wake.track({"tiles": []})
        await action()

    run(go())
    assert sent == []


def test_неудача_подписки_повторяется_не_чаще_паузы() -> None:
    source, sent, clock = Source(fail=True), [], Clock()
    wake = make(source, sent, clock)

    async def go() -> None:
        await wake.track(config())
        clock.now += RETRY_S - 1
        await wake.track(config())
        assert len(source.attached) == 1
        assert wake.summary["failed"] == {"media_player.tv": "webostv.turn_on"}
        source.fail = False
        clock.now += 1
        await wake.track(config())

    run(go())
    assert len(source.attached) == 2
    assert wake.summary["failed"] == {}
    assert wake.summary["attached"] == {"media_player.tv": "webostv.turn_on"}


def test_новый_триггер_не_ждёт_паузы_прежней_неудачи() -> None:
    source, sent = Source(fail=True), []
    wake = make(source, sent)

    async def go() -> None:
        await wake.track(config())
        source.fail = False
        await wake.track(config(trigger="samsungtv.turn_on"))

    run(go())
    assert source.attached[-1] == ("media_player.tv", "samsungtv.turn_on")


def test_отказ_транспорта_не_роняет_и_виден_в_диагностике() -> None:
    source, sent = Source(), []
    wake = make(source, sent, raises=OpError("host вне локальной сети объекта"))

    async def go() -> None:
        await wake.track(config())
        await source.actions["media_player.tv"]()

    run(go())
    assert wake.summary["last_error"] == "media_player.tv: host вне локальной сети объекта"


def test_выгрузка_снимает_подписки_и_глушит_запоздавшую() -> None:
    source, sent = Source(), []
    wake = make(source, sent)

    async def go() -> None:
        await wake.track(config())
        wake.stop()
        await wake.track(config())

    run(go())
    assert source.removed == ["media_player.tv"]
    assert len(source.attached) == 1


def test_источник_без_подписки_молчит() -> None:
    sent: list = []
    wake = make(object(), sent)  # type: ignore[arg-type]
    run(wake.track(config()))
    assert wake.summary["attached"] == {}


def test_исключение_подписки_не_роняет_проход_и_ждёт_паузу() -> None:
    """Сбой подписки одного прибора — как отказ: пауза `RETRY_S`, остальные плитки подписываются."""

    class Flaky(Source):
        async def on_turn_on(self, entity_id: str, trigger: str, action: Any) -> Any:
            if entity_id == "media_player.tv":
                self.attached.append((entity_id, trigger))
                raise RuntimeError("интеграция плеера упала")
            return await super().on_turn_on(entity_id, trigger, action)

    cfg = {
        "tiles": [
            {"id": "t-tv", "entityId": "media_player.tv", "wake": WAKE, "wakeTrigger": "webostv.turn_on"},
            {"id": "t-tv3", "entityId": "media_player.tv3", "wake": WAKE, "wakeTrigger": "webostv.turn_on"},
        ]
    }
    source, clock = Flaky(), Clock()
    wake = make(source, [], clock)
    run(wake.track(cfg))
    assert wake.summary["attached"] == {"media_player.tv3": "webostv.turn_on"}
    assert wake.summary["failed"] == {"media_player.tv": "webostv.turn_on"}
    run(wake.track(cfg))
    assert source.attached.count(("media_player.tv", "webostv.turn_on")) == 1
    clock.now += RETRY_S
    run(wake.track(cfg))
    assert source.attached.count(("media_player.tv", "webostv.turn_on")) == 2
