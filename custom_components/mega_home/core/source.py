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

    async def snapshot(self, entity_id: str, fresh: bool = False) -> tuple[str, bytes]:
        """Кадр камеры. Плитке годится и недавний из памяти; `fresh` — только снятый сейчас
        (вложение к событию: кадр обязан показать момент срабатывания, а не пять минут назад)."""
        ...

    async def stream_source(self, entity_id: str) -> str | None:
        """Адрес живого потока камеры источника (у HA — `camera.stream_source()`).

        ⚠ У приложения снаружи нет адреса Home Assistant, но БАНДЛ живёт внутри
        дома и ведёт переговоры с ЕГО go2rtc сам (`docs/plan-thin-gateway.md`,
        часть B) — ему и нужен этот адрес, обычно RTSP с учёткой внутри строки
        (пароли — поля описания, а не отдельный секрет). Нет потока — `None`,
        а не отказ: у камеры без потока (например, только снимки) это не ошибка.
        """
        ...


class StateSource(Protocol):
    """Сущности дома: прочитать, скомандовать, подписаться."""

    # Камеры источника; None — у источника их нет.
    cameras: Cameras | None

    def get(self, entity_id: str) -> EntityState | None: ...

    async def call(
        self,
        domain: str,
        service: str,
        data: dict[str, Any],
        response: bool = False,
        by: dict[str, str] | None = None,
    ) -> Any:
        """Выполнить команду и дождаться её выполнения (не нового состояния —
        его приносит `subscribe`).

        `response` — вернуть ОТВЕТ службы (прогноз, события календаря, пункты
        списка дел есть только в нём); без него — `None`. Отказы —
        `CommandUnknown`/`CommandRejected`, а не исключения источника.

        `by` — кто скомандовал (`{name, via}`, `ops_base.actor`): источник с
        журналом подписывает им изменения; без журнала — пропускает.
        """

    def subscribe(
        self,
        entity_ids: list[str],
        on_change: Callable[[str, EntityState | None], None],
    ) -> Callable[[], None]:
        """Звать `on_change(entity_id, новое состояние)`; возвращает отписку."""

    # Необязательно: `registry(entity_ids) -> dict` — реестры источника (области, этажи, метки,
    # устройства, записи сущностей; у HA — `ha_registry.read`). Источник без реестров метода не
    # имеет, и `ops.registry` отвечает пустым (`docs/superpowers/specs/2026-10-10-vacuum-design.md`).
    #
    # Необязательно: `async on_turn_on(entity_id, trigger, action) -> отписка | None` — звать
    # `action()`, когда источник просят включить прибор, который сам включиться не умеет (у HA —
    # триггер интеграции прибора, `webostv.turn_on`). `None` — подписаться не вышло. Источник без
    # метода включения по сети на чужую команду не даёт (`wake.py`).
