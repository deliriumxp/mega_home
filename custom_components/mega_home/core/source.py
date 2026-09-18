"""Источник состояний и команд: откуда дом знает, что включено, и чем включает.

Зачем. Второе, что Home Assistant даёт дому, кроме хозяина среды (`host.py`):
сущности. Операции жильца (`ops.py`), поток состояний и перенос запросов
читают и командуют через этот интерфейс, а не через `hass`, — поэтому рядом с
HA (`ha_source.py`) может встать другой источник той же формы (Wirenboard по
MQTT, HA по токену), не трогая операций (`docs/plan-core-without-ha.md`).

⚠ Модуль в ядре без HA (`tests/test_core_without_ha.py`).

⚠ Форма состояния — ровно то, что читает `ops.entity_view`: сырое значение,
атрибуты целиком, время изменения. `State` Home Assistant ей уже отвечает, так
что HA-источник отдаёт его как есть, без копирования.
"""

from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from datetime import datetime
from typing import Any, Protocol


class EntityState(Protocol):
    """Состояние одной сущности."""

    state: str
    attributes: Mapping[str, Any]
    last_updated: datetime


@dataclass(frozen=True)
class PlainState:
    """`EntityState` источника без своего класса состояния."""

    state: str
    attributes: Mapping[str, Any] = field(default_factory=dict)
    last_updated: datetime = field(default_factory=datetime.now)


class CommandUnknown(Exception):
    """Источник не умеет такую команду (в HA — нет службы)."""


class CommandRejected(Exception):
    """Команда есть, но аргументы ей не подошли."""


class Cameras(Protocol):
    """Камеры ИСТОЧНИКА (в HA — сущности `camera.*`).

    ⚠ Не путать со своей go2rtc (`go2rtc_session.py`): та — наш тракт видео и
    есть у дома всегда, а камеры источника бывают не у каждого источника.
    """

    async def negotiate(
        self, entity_id: str, sdp: str, remote: bool, trickle: bool
    ) -> dict[str, Any]: ...

    def close(self, entity_id: str, session_id: str) -> dict[str, Any]: ...

    async def snapshot(self, entity_id: str) -> tuple[str, bytes]: ...

    def warm(self, entity_id: str) -> None: ...

    async def stream_source(self, entity_id: str) -> str | None: ...


class StateSource(Protocol):
    """Сущности дома: прочитать, скомандовать, подписаться."""

    # Камеры источника; None — у источника их нет.
    cameras: Cameras | None

    def get(self, entity_id: str) -> EntityState | None: ...

    async def call(self, domain: str, service: str, data: dict[str, Any]) -> None:
        """Выполнить команду и дождаться её: ответ жильцу несёт НОВОЕ состояние.

        Отказы — `CommandUnknown`/`CommandRejected`, а не исключения источника.
        """

    def subscribe(
        self,
        entity_ids: list[str],
        on_change: Callable[[str, EntityState | None], None],
    ) -> Callable[[], None]:
        """Звать `on_change(entity_id, новое состояние)`; возвращает отписку."""
