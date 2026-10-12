"""Наблюдатель (`core/observe.py`): счёт переживает перезапуск, журнал ограничен."""

from __future__ import annotations

import asyncio

import pytest
from fake_host import FakeHost
from mega_home.core import observe
from mega_home.core.ops_base import OpError

TARGET = {"id": "d1", "request": {"kind": "ping", "host": "192.168.1.20"}, "everySec": 30, "downAfterSec": 120}


def _observer(host: FakeHost | None = None) -> observe.Observer:
    observer = observe.Observer(host or FakeHost())
    asyncio.run(observer.async_start())
    return observer


def _pulse(observer: observe.Observer, ok: bool, at_s: float) -> None:
    observer._apply(TARGET, ok, {"method": "arp"}, int(at_s * 1000))


def test_задание_по_версии_и_needTargets() -> None:
    observer = _observer()
    assert observer.read({"version": "v1"})["needTargets"] is True
    answer = observer.read({"version": "v1", "targets": [TARGET]})
    assert answer["needTargets"] is False and answer["version"] == "v1"


def test_больше_предела_целей_отказ_целиком() -> None:
    observer = _observer()
    many = [dict(TARGET, id=f"d{i}") for i in range(observe.MAX_TARGETS + 1)]
    with pytest.raises(OpError):
        observer.read({"version": "v2", "targets": many})


def test_битые_и_публичные_цели_отбрасываются() -> None:
    observer = _observer()
    observer.read({"version": "v1", "targets": [
        TARGET,
        dict(TARGET, id="pub", request={"kind": "ping", "host": "8.8.8.8"}),
        dict(TARGET, id="fast", everySec=1),
        dict(TARGET, id="str", everySec="30"),
    ]})
    assert [t["id"] for t in observer._targets] == ["d1"]


def test_обрыв_с_последнего_подтверждения_а_не_с_выдержки() -> None:
    observer = _observer()
    observer.read({"version": "v1", "targets": [TARGET]})
    _pulse(observer, True, 1000)
    for at in (1030, 1060, 1090):
        _pulse(observer, False, at)
    assert observer._spans["d1"]["state"] == "up"
    _pulse(observer, False, 1120)
    span = observer._spans["d1"]
    assert span["state"] == "down" and span["since"] == 1000 * 1000
    assert [e["state"] for e in observer._log] == ["up", "down"]


def test_перезапуск_дома_не_сбрасывает_счёт_а_копит_провал() -> None:
    host = FakeHost()
    observer = _observer(host)
    observer.read({"version": "v1", "targets": [TARGET]})
    _pulse(observer, True, 1000)
    _pulse(observer, True, 1030)
    asyncio.run(observer.async_stop())

    # Дом лежал пять минут (обновление): после старта тот же интервал, провал — в gapMs.
    again = _observer(host)
    assert again.read({"version": "v1"})["needTargets"] is False
    again._apply(TARGET, True, {"method": "arp"}, 1330 * 1000)
    span = again._spans["d1"]
    assert span["since"] == 1000 * 1000
    assert span["gapMs"] == (1330 - 1030 - 30) * 1000


def test_журнал_смен_ограничен_числом_и_сроком_и_чистится_подтверждением() -> None:
    observer = _observer()
    observer.read({"version": "v1", "targets": [TARGET]})
    for index in range(observe.MAX_LOG + 50):
        observer._change("d1", observer._spans.setdefault("d1", {}), "up", 10_000_000 + index)
    observer._trim(10_000 + 100)
    assert len(observer._log) == observe.MAX_LOG
    assert observer._log[0]["seq"] == 51
    observer._trim(10_000 + observe.LOG_TTL_S + 10_000)
    assert observer._log == []

    observer._change("d1", observer._spans["d1"], "down", 99)
    seq = observer._log[-1]["seq"]
    assert observer.read({"version": "v1", "ack": seq})["log"] == []


def test_выгрузка_записи_дописывает_счёт_сразу() -> None:
    host = FakeHost()
    observer = _observer(host)
    observer.read({"version": "v1", "targets": [TARGET]})
    _pulse(observer, True, 1000)
    asyncio.run(observer.async_stop())
    assert host.stores[observe.STORE_KEY].data["spans"]["d1"]["state"] == "up"


def test_снятая_цель_уносит_свой_счёт() -> None:
    observer = _observer()
    observer.read({"version": "v1", "targets": [TARGET]})
    _pulse(observer, True, 1000)
    observer.read({"version": "v2", "targets": []})
    assert observer._spans == {}
