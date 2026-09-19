"""Исходящий опрос (`listeners_out.poll`): повторный вход при мёртвой сессии.

⚠ `_request` подменяется целиком: настоящий сетевой поход здесь не нужен,
проверяется управляющий поток `poll` — когда он входит заново и когда шлёт
событие. `aiohttp.ClientSession()` создаётся по-настоящему (это просто объект
с `__aenter__`/`__aexit__`, запроса он не делает), чтобы не городить вторую
заглушку под контекстный менеджер.
"""

from __future__ import annotations

import asyncio
import json

import pytest

from mega_home.core import listeners_out as lo
from mega_home.core.devices import AuthSpec, DeviceDescriptor


class _Done(Exception):
    """Опрос сделал всё, что проверяет тест — прерываем бесконечный цикл."""


class _Ctx:
    def __init__(self) -> None:
        self.events: list[dict] = []

    def emit(self, descriptor, source, spec, kind, payload) -> None:  # noqa: ANN001
        self.events.append(payload)


def _descriptor() -> DeviceDescriptor:
    return DeviceDescriptor(id="trassir", host="10.0.0.1", port=8080, auth=AuthSpec())


def test_resetup_входит_заново_и_шлёт_одно_событие(monkeypatch) -> None:
    """Первый ответ `no session` → `setup` повторён, второй запрос — с новым
    `carry.sid`, событие одно (отказ сессии сам событием не публикуется)."""
    spec = {
        "setup": {"path": "/login", "carry": {"sid": "sid"}},
        "path": "/events",
        "resetup": {"path": "error_code", "equals": "no session"},
        "pause": 5,
    }
    calls: list[str | None] = []
    setups = 0

    async def fake_request(session, descriptor, block, values):  # noqa: ANN001
        nonlocal setups
        if block is spec["setup"]:
            setups += 1
            return 200, "application/json", json.dumps({"sid": f"sid-{setups}"}).encode()
        calls.append(values.get("carry.sid"))
        if len(calls) == 1:
            return 200, "application/json", json.dumps({"error_code": "no session"}).encode()
        return 200, "application/json", json.dumps({"ok": True}).encode()

    async def fake_sleep(_delay: float) -> None:
        raise _Done

    monkeypatch.setattr(lo, "_request", fake_request)
    monkeypatch.setattr(asyncio, "sleep", fake_sleep)

    ctx = _Ctx()
    with pytest.raises(_Done):
        asyncio.run(lo.poll(ctx, _descriptor(), "device-events", spec))

    assert calls == ["sid-1", "sid-2"]
    assert setups == 2
    assert len(ctx.events) == 1
    assert json.loads(ctx.events[0]["text"]) == {"ok": True}


def test_вход_не_чаще_pause(monkeypatch) -> None:
    """Сессия умирает снова сразу после входа — второй вход ждёт `pause`, не долбит."""
    spec = {
        "setup": {"path": "/login", "carry": {"sid": "sid"}},
        "path": "/events",
        "resetup": {"path": "error_code", "equals": "no session"},
        "pause": 5,
    }
    setups = 0
    main_calls = 0

    async def fake_request(session, descriptor, block, values):  # noqa: ANN001
        nonlocal setups, main_calls
        if block is spec["setup"]:
            setups += 1
            return 200, "application/json", json.dumps({"sid": f"sid-{setups}"}).encode()
        main_calls += 1
        return 200, "application/json", json.dumps({"error_code": "no session"}).encode()

    slept: list[float] = []

    async def fake_sleep(delay: float) -> None:
        slept.append(delay)
        if len(slept) >= 2:
            raise _Done

    monkeypatch.setattr(lo, "_request", fake_request)
    monkeypatch.setattr(asyncio, "sleep", fake_sleep)

    ctx = _Ctx()
    with pytest.raises(_Done):
        asyncio.run(lo.poll(ctx, _descriptor(), "device-events", spec))

    # Начальный вход + один повторный (право есть сразу после старта) — а
    # дальше та же мёртвая сессия ждёт `pause`, не долбит входом на каждый ответ.
    assert setups == 2
    assert slept and all(delay == spec["pause"] for delay in slept)
    assert ctx.events == []


def test_сессия_к_устройству_с_tls_без_проверки_сертификата() -> None:
    """Регистратор на 8080/https с самоподписанным сертификатом: без `ssl=False`
    `setup` падал на первом запросе и источник молча перезапускался
    (живой объект 2026-09-20). Правило то же, что у `connect.py`."""
    import asyncio

    from mega_home.core.devices import DeviceDescriptor, AuthSpec
    from mega_home.core import listeners_out as out

    async def scenario() -> tuple[bool, bool]:
        tls = out._session(DeviceDescriptor(id="d", host="192.168.1.10", port=8080, tls=True, timeout=5.0, auth=AuthSpec(type="none", user="", password="p"), events=[]))
        plain = out._session(DeviceDescriptor(id="d", host="192.168.1.10", port=80, tls=False, timeout=5.0, auth=AuthSpec(type="none", user="", password=""), events=[]))
        try:
            return tls.connector._ssl is False, plain.connector._ssl is not False  # noqa: SLF001
        finally:
            await tls.close()
            await plain.close()

    insecure, checked = asyncio.run(scenario())
    assert insecure and checked
