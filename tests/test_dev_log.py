"""Лог разработки (`core/dev_log.py`) — спеки шага 4 `docs/plan-dev-logs.md`.

Фильтр не пускает ниже уровня; очередь не растёт сверх потолка; неподтверждённое
уходит повторно в исходном порядке; секреты не попадают в запись.
"""

from __future__ import annotations

import asyncio
import logging

from fake_host import FakeHost
from mega_home.core import dev_log as dl


def _log() -> dl.DevLog:
    log = dl.DevLog(FakeHost(), lambda: {"homeVersion": "0.5.6"})
    asyncio.run(log.async_load())
    return log


def _emit(log: dl.DevLog, level: int, message: str) -> None:
    """Запись через настоящий `logging` — ровно так пишет код интеграции."""

    async def go() -> None:
        log._loop = asyncio.get_running_loop()  # noqa: SLF001
        logger = logging.getLogger("custom_components.mega_home.test")
        logger.propagate = False
        logger.setLevel(logging.DEBUG)
        logger.addHandler(log.handler)
        try:
            logger.log(level, message)
            await asyncio.sleep(0)
        finally:
            logger.removeHandler(log.handler)

    asyncio.run(go())


def test_фильтр_не_пускает_ниже_уровня() -> None:
    log = _log()
    log.apply({"devLog": {"level": "warn"}})
    _emit(log, logging.DEBUG, "шум")
    _emit(log, logging.INFO, "шум")
    _emit(log, logging.WARNING, "go2rtc не поднялся")
    assert [r["message"] for r in log._queue] == ["go2rtc не поднялся"]  # noqa: SLF001
    assert log._queue[0]["source"] == "home" and log._queue[0]["level"] == "warn"  # noqa: SLF001


def test_без_ключа_в_конфиге_warn() -> None:
    log = _log()
    log.apply({})
    assert log.level == logging.WARNING


def test_секреты_не_попадают_в_запись() -> None:
    log = _log()
    _emit(log, logging.WARNING, "src rtsp://admin:s3cret@10.0.0.5/live, pass=hunter2")
    log.add_app([{"message": "x", "data": {"password": "p", "url": "http://u:p@h/", "ok": 1}}])
    text = str(log._queue)  # noqa: SLF001
    for secret in ("s3cret", "hunter2", "u:p@"):
        assert secret not in text
    assert log._queue[-1]["data"]["ok"] == 1  # noqa: SLF001


def test_очередь_не_растёт_сверх_потолка() -> None:
    log = _log()
    log.add_app([{"message": str(i)} for i in range(dl.MAX_APP_BATCH)] * 10)
    for index in range(dl.QUEUE_LIMIT + 30):
        log.add("app", "error", "x", str(index))
    assert len(log._queue) == dl.QUEUE_LIMIT  # noqa: SLF001
    assert log._queue[-1]["message"] == str(dl.QUEUE_LIMIT + 29)  # noqa: SLF001


def test_неподтверждённое_уходит_повторно_по_порядку() -> None:
    log = _log()
    sent: list[dict] = []
    log.add("app", "error", "a", "первая")
    log.add("app", "error", "b", "вторая")
    frames = log.attach(sent.append)
    assert [f["record"]["message"] for f in frames] == ["первая", "вторая"]

    log.ack(frames[0]["id"])
    log.detach()
    again = log.attach(sent.append)
    assert [f["record"]["message"] for f in again] == ["вторая"]
    log.add("app", "error", "c", "третья")
    assert sent[-1]["t"] == "dev-log" and sent[-1]["record"]["message"] == "третья"
