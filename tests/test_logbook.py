"""Подпись Mega Home в журнале HA (`logbook.py`): кто — в имени, что — в тексте."""

from __future__ import annotations

from types import SimpleNamespace

from mega_home.logbook import async_describe_events, describe


# ⚠ «С помощью: …» строки прибора журнал собирает только из ИМЕНИ, поэтому кто —
# там; действие и прибор — в тексте, который виден лишь в строке события.
def test_кто_в_имени_что_в_тексте() -> None:
    described: dict = {}
    async_describe_events(None, lambda domain, event, fn: described.update({(domain, event): fn}))
    fn = described[("mega_home", "mega_home_command")]
    data = {"name": "Ноутбук", "via": "Локально", "action": "Включение", "target": "Люстра"}
    assert fn(SimpleNamespace(data=data)) == {
        "name": "Mega Home: Ноутбук, Локально",
        "message": "— Включение — Люстра",
    }


def test_недостающие_части_не_оставляют_пустых_разделителей() -> None:
    assert describe({"via": "Временный доступ №5", "action": "Сценарий", "target": ""}) == {
        "name": "Mega Home: Временный доступ №5",
        "message": "— Сценарий",
    }
    assert describe(None) == {"name": "Mega Home"}
