"""Подпись Mega Home в журнале HA (`logbook.py`): кто скомандовал, и только он."""

from __future__ import annotations

from types import SimpleNamespace

from mega_home.logbook import async_describe_events, describe


def test_платформа_описывает_событие_команды_именем_и_источником() -> None:
    described: dict = {}
    async_describe_events(None, lambda domain, event, fn: described.update({(domain, event): fn}))
    fn = described[("mega_home", "mega_home_command")]
    entry = fn(SimpleNamespace(data={"name": "Иван", "via": "удалённое приложение"}))
    assert entry == {"name": "Mega Home", "message": "Иван, удалённое приложение"}


def test_без_имени_остаётся_источник() -> None:
    assert describe({"name": "", "via": "локальное приложение"}) == "локальное приложение"
    assert describe({"via": "временный доступ №5"}) == "временный доступ №5"
    assert describe(None) == ""
