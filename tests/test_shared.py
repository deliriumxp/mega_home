"""Общие данные жильцов дома (`core/shared.py`) и их путь `api/shared/<имя>`.

⚠ Предмет — ревизия и пределы: два телефона пишут один документ, и молча потерять отметку
одного из них нельзя; диск объекта не забивается чем попало.
"""

from __future__ import annotations

import asyncio
import base64
import json
from pathlib import Path

import pytest

from fake_host import FakeHost
from mega_home.core import ops
from mega_home.core.relay_api import dispatch, handle
from mega_home.core.shared import MAX_DOCS, SharedDocs


class _Coordinator:
    def __init__(self, tmp: Path) -> None:
        self.data = {"version": "sha256:abc", "tiles": []}
        self.env = FakeHost(tmp)
        self.shared = SharedDocs(self.env)


def call(coordinator, method: str, path: str, payload=None):
    body = json.dumps(payload).encode() if payload is not None else b""
    _, _, raw, _ = asyncio.run(dispatch(coordinator, method, path, body, {}))
    return json.loads(raw)


@pytest.fixture()
def coordinator(tmp_path: Path) -> _Coordinator:
    return _Coordinator(tmp_path)


def test_неписанный_документ_пуст_а_не_ошибка(coordinator):
    assert call(coordinator, "GET", "api/shared/radio") == {"rev": 0, "data": None}


def test_запись_по_ревизии_и_отказ_устаревшей(coordinator):
    first = call(coordinator, "POST", "api/shared/radio", {"rev": 0, "data": {"favorites": ["1"]}})
    assert first == {"accepted": True, "rev": 1, "data": {"favorites": ["1"]}}
    # Второй телефон читал ДО первой записи: его копия не затирает чужую отметку.
    stale = call(coordinator, "POST", "api/shared/radio", {"rev": 0, "data": {"favorites": ["2"]}})
    assert stale == {"accepted": False, "rev": 1, "data": {"favorites": ["1"]}}


def test_снятый_документ_не_откатывает_ревизию(coordinator):
    call(coordinator, "POST", "api/shared/radio", {"rev": 0, "data": {"a": 1}})
    assert call(coordinator, "POST", "api/shared/radio", {"rev": 1, "data": None})["accepted"]
    assert call(coordinator, "GET", "api/shared/radio") == {"rev": 2, "data": None}
    assert not call(coordinator, "POST", "api/shared/radio", {"rev": 0, "data": {"a": 1}})["accepted"]


def test_документ_переживает_перезапуск(coordinator):
    call(coordinator, "POST", "api/shared/radio", {"rev": 0, "data": {"favorites": ["7"]}})
    again = SharedDocs(coordinator.env)
    asyncio.run(again.async_load())
    assert again.get("radio") == {"rev": 1, "data": {"favorites": ["7"]}}


def test_пределы_имени_размера_и_числа(coordinator):
    with pytest.raises(ops.OpError) as bad_name:
        call(coordinator, "GET", "api/shared/Bad Name")
    assert bad_name.value.status == 404
    with pytest.raises(ops.OpError) as big:
        call(coordinator, "POST", "api/shared/big", {"rev": 0, "data": "x" * 70_000})
    assert big.value.status == 413
    for n in range(MAX_DOCS):
        call(coordinator, "POST", f"api/shared/d{n}", {"rev": 0, "data": n})
    with pytest.raises(ops.OpError) as full:
        call(coordinator, "POST", "api/shared/one-more", {"rev": 0, "data": 1})
    assert full.value.status == 507
    # Снятый документ места не занимает: после снятия новое заводится.
    call(coordinator, "POST", "api/shared/d0", {"rev": 1, "data": None})
    assert call(coordinator, "POST", "api/shared/one-more", {"rev": 0, "data": 1})["accepted"]


def test_снятие_несуществующего_записи_не_заводит(coordinator):
    assert call(coordinator, "POST", "api/shared/ghost", {"rev": 0, "data": None}) == {"accepted": True, "rev": 0, "data": None}
    assert "ghost" not in coordinator.shared._docs


def test_снаружи_тем_же_путём(coordinator):
    payload = {
        "method": "POST",
        "path": "api/shared/radio",
        "body": base64.b64encode(json.dumps({"rev": 0, "data": [1]}).encode()).decode(),
    }
    answer = asyncio.run(handle(coordinator, payload))
    assert json.loads(base64.b64decode(answer["body"]))["rev"] == 1
