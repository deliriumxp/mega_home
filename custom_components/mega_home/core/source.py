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
    """Камеры ИСТОЧНИКА (в HA — сущности `camera.*`): прогрев и кадр плитки.

    ⚠ Переговоров WebRTC здесь больше нет (`docs/plan-thin-gateway.md`): бандл
    ведёт их с go2rtc сам, через `connect` к службе «go2rtc» — свой тракт видео
    дом для этого больше не подставляет. `snapshot` остаётся: снаружи у
    приложения нет ни одного адреса Home Assistant (`state.picture` — это
    `/api/camera_proxy/...` самого HA, наружу недостижим), и кадр плитки едет
    тем же переносом, что и остальной API (`api/camera-frame/<tileId>`).
    """

    def warm(self, entity_id: str) -> None: ...

    async def snapshot(self, entity_id: str) -> tuple[str, bytes]: ...


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
