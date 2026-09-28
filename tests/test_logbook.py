"""Подпись Mega Home в журнале HA (`logbook.py`): кто скомандовал, и только он."""

from __future__ import annotations

from types import SimpleNamespace

from mega_home.logbook import async_describe_events, describe


# ⚠ Вся подпись — в ИМЕНИ: «С помощью: …» строки прибора журнал собирает только
# из него, а текст записи туда не попадает.
def test_подпись_целиком_в_имени_без_текста() -> None:
    described: dict = {}
    async_describe_events(None, lambda domain, event, fn: described.update({(domain, event): fn}))
    fn = described[("mega_home", "mega_home_command")]
    entry = fn(SimpleNamespace(data={"name": "Иван", "via": "удалённое приложение"}))
    assert entry == {"name": "Mega Home — Иван, удалённое приложение"}


def test_без_имени_остаётся_источник() -> None:
    assert describe({"name": "", "via": "локальное приложение"}) == "Mega Home — локальное приложение"
    assert describe({"via": "временный доступ №5"}) == "Mega Home — временный доступ №5"
    assert describe(None) == "Mega Home"
