"""Сторож объекта (`agent.py`).

⚠ Предмет этих тестов — ИСПОЛНЕНИЕ правил и их ограничители, а не смысл правил.
Ни одного теста вида «зависший директор перезагружается» здесь быть не должно:
такой тест означал бы, что толкование ответов снова уехало в Python, за который
платят релизом HACS и перезапуском Home Assistant на каждом объекте. Что
означает ответ, знает менеджер — у него и спека
(`c4-health/director-watchdog.util.spec.ts`).

⚠ Второй предмет — АВТОНОМНОСТЬ: сторож обязан работать из кэша, когда менеджера
нет, и не терять отчёты, пока он не вернётся. Ради этого и заводился.
"""

from __future__ import annotations

import asyncio

from mega_home import agent as agent_module
from mega_home.agent import AgentRunner
from mega_home.api import ManagerError


def run(coro):
    return asyncio.run(coro)


def rule(**over):
    base = {
        "id": "r1",
        "everySec": 1,
        "probes": [{"kind": "http", "url": "https://device/status"}],
        "healthy": [{"probe": 0, "ok": True, "json": "director", "equals": "connected"}],
        "failForSec": 0,
        "action": {
            "probes": [{"kind": "tcp", "host": "device", "port": 5810, "send": "reboot\n"}],
            "cooldownSec": 600,
            "maxPerDay": 2,
        },
        "note": "правило",
    }
    base.update(over)
    return base


class FakeClient:
    """Менеджер: отдаёт правила, принимает отчёты. Может быть недоступен."""

    def __init__(self, answer=None, fail=False):
        self.answer = answer or {"version": "v1", "rules": [rule()]}
        self.fail = fail
        self.received: list[dict] = []
        self.calls = 0

    async def async_agent(self, version, reports):
        self.calls += 1
        if self.fail:
            raise ManagerError("менеджер недоступен")
        self.received.extend(reports)
        return self.answer


def make(monkeypatch, client, results):
    """Сторож с подменённой пробой: она возвращает заданные результаты."""
    calls: list[list[dict]] = []

    async def fake_probe(hass, payload):
        calls.append(payload["probes"])
        answer = results[min(len(calls) - 1, len(results) - 1)]
        return {"results": answer}

    import mega_home.probe as probe_module

    monkeypatch.setattr(probe_module, "run", fake_probe)
    return AgentRunner(None, client), calls


HEALTHY = [{"ok": True, "ms": 5, "status": 200, "body": '{"director": "connected"}'}]
SICK = [{"ok": True, "ms": 5, "status": 200, "body": '{"director": "initializing"}'}]


def test_rules_survive_an_unreachable_manager(monkeypatch, tmp_path):
    # ⚠ Ради этого сторож и заводился: объект, потерявший связь с менеджером,
    # обязан продолжать сторожить себя — именно тогда чинить его некому.
    client = FakeClient()
    runner, _ = make(monkeypatch, client, [HEALTHY])
    run(runner.async_start())
    run(runner.async_sync())
    saved = runner.summary
    assert saved["rules"] == ["r1"]

    offline = FakeClient(fail=True)
    revived, _ = make(monkeypatch, offline, [HEALTHY])
    revived._store = runner._store  # тот же диск
    run(revived.async_start())
    run(revived.async_sync())
    assert revived.summary["rules"] == ["r1"]


def test_action_runs_only_after_the_grace_period(monkeypatch):
    client = FakeClient(answer={"version": "v1", "rules": [rule(failForSec=3600)]})
    runner, calls = make(monkeypatch, client, [SICK])
    run(runner.async_start())
    run(runner.async_sync())
    run(runner._async_tick())
    # Проверка выполнена, действие — нет: одиночный неудачный тик не авария.
    assert len(calls) == 1


def test_cooldown_keeps_the_device_from_being_hit_while_it_recovers(monkeypatch):
    client = FakeClient()
    runner, calls = make(monkeypatch, client, [SICK])
    run(runner.async_start())
    run(runner.async_sync())
    run(runner._async_tick())
    assert len(calls) == 2  # проверка + действие
    runner._state["r1"]["ranAt"] = 0
    run(runner._async_tick())
    # Второе действие не ушло: устройство после первого ещё поднимается.
    assert len(calls) == 3


def test_daily_limit_blocks_and_says_so_once(monkeypatch):
    # Объект с мёртвым железом обязан упереться в потолок и позвать инженера, а
    # не перезагружаться вечно.
    client = FakeClient(answer={"version": "v1", "rules": [rule(action={
        "probes": [{"kind": "tcp", "host": "device", "port": 5810, "send": "reboot\n"}],
        "cooldownSec": 0,
        "maxPerDay": 1,
    })]})
    runner, calls = make(monkeypatch, client, [SICK])
    run(runner.async_start())
    run(runner.async_sync())
    run(runner._async_tick())
    for _ in range(3):
        runner._state["r1"]["ranAt"] = 0
        run(runner._async_tick())
    acted = [r for r in runner._pending if r["event"] == "acted"]
    blocked = [r for r in runner._pending if r["event"] == "blocked"]
    assert len(acted) == 1
    assert len(blocked) == 1


def test_reports_wait_for_the_manager_and_are_not_lost(monkeypatch):
    offline = FakeClient(fail=True)
    runner, _ = make(monkeypatch, offline, [SICK])
    run(runner.async_start())
    runner._rules = [rule()]
    run(runner._async_tick())
    assert runner._pending, "событие обязано встать в очередь"
    pending = len(runner._pending)

    online = FakeClient()
    runner._client = online
    run(runner.async_sync())
    assert len(online.received) == pending
    assert runner._pending == []


def test_recovery_is_reported_only_after_a_reported_failure(monkeypatch):
    client = FakeClient()
    runner, _ = make(monkeypatch, client, [SICK, HEALTHY])
    run(runner.async_start())
    runner._rules = [rule(action={
        "probes": [{"kind": "tcp", "host": "d", "port": 1, "send": "x"}],
        "cooldownSec": 600,
        "maxPerDay": 0 or 1,
    })]
    run(runner._async_tick())
    runner._state["r1"]["ranAt"] = 0
    run(runner._async_tick())
    events = [r["event"] for r in runner._pending]
    assert "fail" in events and "recovered" in events


def test_broken_rule_does_not_take_the_others_down(monkeypatch):
    # Сторож нужен целиком, а не до первого испорченного правила.
    client = FakeClient(answer={"version": "v1", "rules": [{"id": "bad"}, rule(id="good")]})
    runner, calls = make(monkeypatch, client, [HEALTHY])
    run(runner.async_start())
    run(runner.async_sync())
    assert runner.summary["rules"] == ["good"]
    run(runner._async_tick())
    assert len(calls) == 1


def test_checks_read_json_fields_and_nothing_more():
    # Язык условий беден намеренно: богаче — значит толкование ответов
    # переехало сюда, а ему сюда нельзя.
    results = [{"ok": True, "status": 200, "body": '{"a": {"b": "yes"}}'}]
    assert agent_module._check(results, {"probe": 0, "json": "a.b", "equals": "yes"})
    assert not agent_module._check(results, {"probe": 0, "json": "a.c", "equals": "yes"})
    assert not agent_module._check(results, {"probe": 0, "json": "a.b", "equals": "no"})
    assert not agent_module._check(results, {"probe": 5, "ok": True})
    assert agent_module._check(results, {"probe": 0, "contains": "yes"})
