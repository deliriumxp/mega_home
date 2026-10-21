"""Темы дома (`core/topics.py`): путь записи поднимает номер, поток несёт сигнал, снимок — карту.

⚠ Предмет — что второй телефон УЗНАЁТ о записи первого: сигналом в открытом потоке, а после
обрыва — по карте номеров в снимке `states`. Сигнал без данных: данные читаются прежним запросом.
"""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from typing import Any

import pytest

from fake_host import FakeHost, FakeSource
from mega_home.core import ops
from mega_home.core.events import StateStream
from mega_home.core.relay_api import dispatch
from mega_home.core.shared import SharedDocs
from mega_home.core.topics import Topics


class _Coordinator:
    def __init__(self, tmp: Path) -> None:
        self.data = {"version": "sha256:abc", "tiles": []}
        self.env = FakeHost(tmp)
        self.shared = SharedDocs(self.env)
        self.topics = Topics()
        self.source = FakeSource()
        self.bundle = None

    @property
    def version(self) -> str:
        return self.data["version"]

    def async_add_listener(self, callback: Any) -> Any:
        return lambda: None


def call(coordinator, method: str, path: str, payload=None):
    body = json.dumps(payload).encode() if payload is not None else b""
    _, _, raw, _ = asyncio.run(dispatch(coordinator, method, path, body, {}))
    return json.loads(raw)


def drain(stream: StateStream) -> list[tuple[str, Any]]:
    items = []
    while not stream.queue.empty():
        items.append(stream.queue.get_nowait())
    return items


@pytest.fixture()
def coordinator(tmp_path: Path) -> _Coordinator:
    return _Coordinator(tmp_path)


def test_номер_темы_растёт_и_слушатель_узнаёт():
    topics = Topics()
    seen: list[tuple[str, int]] = []
    unsubscribe = topics.subscribe(lambda topic, rev: seen.append((topic, rev)))
    topics.bump("alerts")
    topics.bump("alerts")
    unsubscribe()
    topics.bump("alerts")
    assert seen == [("alerts", 1), ("alerts", 2)]
    assert topics.revs() == {"alerts": 3}


def test_принятая_запись_документа_сигналит_всем_потокам(coordinator):
    stream = StateStream(coordinator)
    stream.start()
    call(coordinator, "POST", "api/shared/radio-sent", {"rev": 0, "data": {"players": {}}})
    assert drain(stream) == [("changed", {"topic": "shared/radio-sent", "rev": 1})]
    # Отказ по ревизии ничего не поменял — и сигнала нет.
    call(coordinator, "POST", "api/shared/radio-sent", {"rev": 0, "data": {"players": {}}})
    assert drain(stream) == []
    stream.stop()
    call(coordinator, "POST", "api/shared/radio-sent", {"rev": 1, "data": None})
    assert drain(stream) == []


def test_снимок_несёт_номера_тем_для_сверки_после_обрыва(coordinator):
    assert ops.states(coordinator)["revs"] == {}
    call(coordinator, "POST", "api/shared/radio", {"rev": 0, "data": {"favorites": ["1"]}})
    assert ops.states(coordinator)["revs"] == {"shared/radio": 1}


def test_бандл_сменился_тема_app(tmp_path: Path):
    from mega_home.core.bundle import BundleStore

    bundle = BundleStore(FakeHost(tmp_path), None)  # type: ignore[arg-type]
    topics = Topics()
    bundle.on_swap = lambda: topics.bump("app")
    assert bundle.on_swap is not None
    bundle.on_swap()
    assert topics.revs() == {"app": 1}
