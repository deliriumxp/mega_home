"""Вложения к событиям (`core/event_files.py`) — спеки `docs/plan-event-attachments.md`.

Правило исполняется один раз на событие; не-GET отвергается; файл сверх потолка
не сохраняется; уборка сносит старое; отдача — одним маршрутизатором локально и
переносом.
"""

from __future__ import annotations

import asyncio
import base64
import os
import time
from pathlib import Path
from typing import Any

from fake_host import FakeHost
from mega_home.core.host import PlainHost
from mega_home.core import event_files as ef
from mega_home.core import relay_api

RULE = {
    "on": {"access": "intercom", "source": "sip-bridge", "event": "call"},
    "request": {"kind": "http", "host": "go2rtc", "path": "/api/frame.jpeg?src=panel"},
    "type": "image/jpeg",
}


class _Journal:
    def __init__(self) -> None:
        self.marks: list[tuple] = []

    def mark(self, access: Any, event_id: str, file: dict) -> None:
        self.marks.append((access, event_id, file))


def _files(tmp_path: Path, monkeypatch, body: bytes = b"\xff\xd8jpeg", status: int = 200):
    calls: list[dict] = []

    async def resource(request: dict):
        calls.append(request)
        return status, "image/jpeg", body, "private"

    monkeypatch.setattr(ef.connect, "resource", resource)
    journal = _Journal()
    files = ef.EventFiles(PlainHost(tmp_path), journal)
    files.apply({"attachments": [RULE]})
    return files, journal, calls


def _publish(files: ef.EventFiles, frame: dict) -> None:
    async def go() -> None:
        files.on_event(frame)
        await asyncio.gather(*files._env._tasks)  # noqa: SLF001

    asyncio.run(go())


CALL = {"id": "e1", "access": "intercom", "source": "sip-bridge", "event": "call"}


def test_правило_исполняется_один_раз_на_событие_и_помечает_журнал(tmp_path, monkeypatch) -> None:
    files, journal, calls = _files(tmp_path, monkeypatch)
    _publish(files, CALL)
    _publish(files, {**CALL, "id": "e2", "event": "cancel"})  # не то событие

    assert len(calls) == 1
    assert journal.marks == [("intercom", "e1", {"type": "image/jpeg", "bytes": 6})]
    found = files.find("e1")
    assert found is not None and found[1] == "image/jpeg" and found[0].read_bytes() == b"\xff\xd8jpeg"


def test_не_get_отвергается(tmp_path) -> None:
    files = ef.EventFiles(FakeHost(tmp_path))
    files.apply({"attachments": [{**RULE, "request": {**RULE["request"], "method": "POST"}}]})
    assert files._rules == []  # noqa: SLF001


def test_файл_сверх_потолка_не_сохраняется(tmp_path, monkeypatch) -> None:
    files, journal, _ = _files(tmp_path, monkeypatch, body=b"x" * (ef.MAX_BYTES + 1))
    _publish(files, CALL)
    assert files.find("e1") is None and journal.marks == []


def test_уборка_сносит_старое_и_лишнее(tmp_path, monkeypatch) -> None:
    files, _, _ = _files(tmp_path, monkeypatch)
    monkeypatch.setattr(ef, "MAX_FILES", 2)
    for index in range(3):
        _publish(files, {**CALL, "id": f"e{index}"})
    assert files.find("e0") is None and files.find("e2") is not None

    old = files.find("e1")[0]
    stale = time.time() - ef.TTL_S - 10
    os.utime(old, (stale, stale))
    _publish(files, {**CALL, "id": "e3"})
    assert files.find("e1") is None


def test_отдача_одним_маршрутизатором_и_переносом(tmp_path, monkeypatch) -> None:
    files, _, _ = _files(tmp_path, monkeypatch)
    _publish(files, CALL)

    class _Coordinator:
        data = {"tiles": []}
        env = FakeHost(tmp_path)
        event_files = files

    answer = asyncio.run(
        relay_api.handle(_Coordinator(), {"method": "GET", "path": "/api/event-file?id=e1"})
    )
    assert answer["status"] == 200 and answer["contentType"] == "image/jpeg"
    assert base64.b64decode(answer["body"]) == b"\xff\xd8jpeg"
