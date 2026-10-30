"""Язык условий правил дома (`core/conditions.py`): один на уведомления и сторожа.

Предмет — что новое условие задаётся ДАННЫМИ (путь, сравнение, значение), а не кодом: «между»,
«не равно», атрибут вместо состояния, поле ответа задания (ревью заказчика 2026-10-30).
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any

from fake_host import FakeHost, FakeSource
from mega_home.core import agent, alerts, conditions


@dataclass
class EntityState:
    state: str
    attributes: dict[str, Any] = field(default_factory=dict)


def holds(tree: Any, path: str, op: str, value: Any = None) -> bool:
    cond = conditions.valid({"path": path, "op": op, **({"value": value} if value is not None else {})})
    assert cond is not None, (path, op, value)
    return conditions.holds(tree, cond)


def test_сравнения_числом_и_строкой() -> None:
    tree = {"state": "21.5", "attributes": {"battery": 9, "mode": "cool"}}
    assert holds(tree, "state", "gt", 21) and holds(tree, "state", "le", "21.5")
    assert holds(tree, "state", "between", [20, 22]) and not holds(tree, "state", "between", [22, 30])
    assert holds(tree, "attributes.battery", "lt", 10)
    assert holds(tree, "attributes.mode", "ne", "heat") and holds(tree, "attributes.mode", "in", ["cool", "dry"])
    assert holds(tree, "attributes.mode", "exists") and not holds(tree, "attributes.nope", "exists")
    # Нет поля — условие не выполнено, а не исключение.
    assert not holds(tree, "attributes.nope", "gt", 1)


def test_путь_раскрывает_json_тела_и_списки() -> None:
    tree = {"results": [{"ok": True, "status": 200, "body": '{"director": {"state": "connected", "load": 0.7}}'}]}
    assert holds(tree, "results.0.body.director.state", "eq", "connected")
    assert holds(tree, "results.0.body.director.load", "lt", 0.9)
    assert holds(tree, "results.0.body", "contains", "connected")
    assert not holds(tree, "results.5.status", "eq", 200)


def test_испорченное_условие_не_исполняется() -> None:
    assert conditions.valid({"op": "gt", "value": "много"}) is None
    assert conditions.valid({"op": "between", "value": [1]}) is None
    assert conditions.valid({"op": "regex", "value": ".*"}) is None
    assert conditions.valid({"op": "eq", "value": True}) is None


def test_уведомление_по_атрибуту_и_новому_сравнению() -> None:
    rule = alerts.rule_valid({"op": "between", "value": [18, 26], "path": "attributes.current_temperature", "holdSec": 0, "cooldownSec": 600, "enabled": True})
    assert rule is not None and rule["path"] == "attributes.current_temperature"
    assert alerts.matches(rule, EntityState("heat", {"current_temperature": 21})) is True
    assert alerts.matches(rule, EntityState("heat", {"current_temperature": 30})) is False


def test_прежняя_форма_уведомления_не_меняется() -> None:
    # Сохранённое правило прежней формы обязано остаться равным своей проверке (`async_load`).
    old = {"op": "gt", "value": 25.0, "holdSec": 0, "cooldownSec": 900, "enabled": True}
    assert alerts.rule_valid(old) == old
    on = {"op": "on", "value": None, "holdSec": 30, "cooldownSec": 900, "enabled": True}
    assert alerts.rule_valid(on) == on


def test_действие_уведомления_вызовом_connect() -> None:
    # «Протечка — закрыть кран»: действие правила — описание вызова, исполняет дом сам.
    calls: list[dict[str, Any]] = []
    spawned: list[Any] = []

    async def act(request: dict[str, Any]) -> dict[str, Any]:
        calls.append(request)
        return {"status": 200}

    class Host(FakeHost):
        def spawn(self, coro, name: str):  # noqa: ANN001, ANN201
            spawned.append(coro)

    async def scenario() -> None:
        source = FakeSource({"binary_sensor.leak": EntityState("off")})
        box = alerts.Alerts(Host(), source, lambda tile, data: True, act=act)
        config = {"tiles": [{"id": "leak", "entityId": "binary_sensor.leak"}]}
        box.track(config)
        valve = {"kind": "http", "host": "192.168.1.5", "port": 80, "path": "/valve/close"}
        await box.save("leak", alerts.rule_valid({"op": "on", "holdSec": 0, "cooldownSec": 600, "enabled": True, "do": [valve]}))
        box.track(config)
        source.states["binary_sensor.leak"] = EntityState("on")
        source.subscriptions[-1][1]("binary_sensor.leak", EntityState("on"))
        for coro in spawned:
            await coro

    asyncio.run(scenario())
    assert calls == [{"kind": "http", "host": "192.168.1.5", "port": 80, "path": "/valve/close"}]


def test_сторож_проверяет_общим_языком_и_действует_вызовом_connect(monkeypatch) -> None:
    sent: list[dict[str, Any]] = []

    async def perform(request: dict[str, Any], bundle: bool = False) -> dict[str, Any]:
        sent.append(request)
        return {"status": 200, "body": '{"load": 0.95}'}

    monkeypatch.setattr(agent.connect, "perform", perform)
    results = [{"ok": True, "status": 200, "body": '{"load": 0.95}'}]
    assert agent._check(results, {"path": "results.0.body.load", "op": "gt", "value": 0.9})  # noqa: SLF001
    assert agent._check(results, {"probe": 0, "status": 200})  # noqa: SLF001 — прежняя форма жива
    rule = {
        "id": "r", "requests": [{"kind": "mqtt", "host": "192.168.1.9", "port": 1883}], "healthy": [{"op": "exists", "path": "results.0.status"}],
        "everySec": 60, "failForSec": 0, "action": {"requests": [{"kind": "udp", "host": "192.168.1.255", "port": 9}], "cooldownSec": 0, "maxPerDay": 1},
    }
    assert agent._valid(rule)  # noqa: SLF001

    class Runner(agent.AgentRunner):
        def __init__(self) -> None:  # без хозяина и менеджера: проверяется только `_tasks`
            self._env = None

    done = asyncio.run(Runner()._tasks(rule["action"]))  # noqa: SLF001
    assert done == [{"ok": True, "status": 200, "body": '{"load": 0.95}'}]
    assert sent == rule["action"]["requests"]
