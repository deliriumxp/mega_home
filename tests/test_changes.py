"""С какого момента прибор в своём состоянии (`core/changes.py`).

Время двигает только смена значения; `unavailable`/`unknown` и перезапуск HA (тот же `on` с новым
`last_changed`) его не сбрасывают; память переживает перезапуск через хранилище.
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from fake_host import FakeHost, FakeSource
from mega_home.core import ops
from mega_home.core.changes import STORE_KEY, Changes


@dataclass
class S:
    state: str
    last_changed: datetime
    attributes: dict[str, Any] = field(default_factory=dict)

    @property
    def last_updated(self) -> datetime:
        return self.last_changed


def at(hour: int) -> datetime:
    return datetime(2026, 10, 12, hour, tzinfo=timezone.utc)


def ms(hour: int) -> int:
    return int(at(hour).timestamp() * 1000)


def _changes(host: FakeHost, source: FakeSource) -> Changes:
    changes = Changes(host, source)
    asyncio.run(changes.async_load())
    return changes


def test_время_двигает_только_смена_значения() -> None:
    source = FakeSource({"light.a": S("on", at(1))})
    changes = _changes(FakeHost(), source)
    changes.track(["light.a"])
    assert changes.since("light.a", source.get("light.a")) == ms(1)

    # Атрибуты сменились, значение то же — время прежнее.
    assert changes.since("light.a", S("on", at(2))) == ms(1)
    assert changes.since("light.a", S("off", at(3))) == ms(3)


def test_недоступность_не_сбрасывает_время() -> None:
    source = FakeSource({"light.a": S("on", at(1))})
    changes = _changes(FakeHost(), source)
    changes.track(["light.a"])
    _, on_change = source.subscriptions[-1]
    on_change("light.a", S("unavailable", at(2)))
    on_change("light.a", S("on", at(3)))
    assert changes.since("light.a", S("on", at(3))) == ms(1)


def test_перезапуск_ha_не_сбрасывает_время() -> None:
    host = FakeHost()
    first = _changes(host, FakeSource({"light.a": S("on", at(1))}))
    first.track(["light.a"])

    # Новый процесс: хранилище то же, HA записал `on` заново со временем старта.
    source = FakeSource({"light.a": S("on", at(5))})
    second = _changes(host, source)
    second.track(["light.a"])
    assert second.since("light.a", source.get("light.a")) == ms(1)


def test_снятые_с_состава_приборы_не_копятся() -> None:
    host = FakeHost()
    source = FakeSource({"light.a": S("on", at(1)), "light.b": S("on", at(1))})
    changes = _changes(host, source)
    changes.track(["light.a", "light.b"])
    changes.track(["light.a"])
    assert set(host.stores[STORE_KEY].data) == {"light.a"}


def test_ответ_дома_несёт_changedAt() -> None:
    source = FakeSource({"light.a": S("on", at(1))})
    changes = _changes(FakeHost(), source)
    tile = {"id": "t1", "domain": "light", "entityId": "light.a"}
    assert ops.entity_view(tile, source.get("light.a"), None, changes)["changedAt"] == ms(1)
    # Без памяти (старый вызов) — поле есть, но пустое.
    assert ops.entity_view(tile, source.get("light.a"))["changedAt"] is None


def test_flush_пишет_отложенное_сейчас() -> None:
    """Выгрузка записи: отложенная запись не должна пережить экземпляр (`__init__.async_unload_entry`)."""
    host = FakeHost()
    changes = _changes(host, FakeSource({"light.a": S("on", at(1))}))
    changes.track(["light.a"])
    store = host.stores[STORE_KEY]
    assert store._pending is not None  # noqa: SLF001 — запись отложена
    asyncio.run(changes.flush())
    assert store._pending is None and store._data["light.a"]["at"] == ms(1)  # noqa: SLF001
