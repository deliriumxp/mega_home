"""Штора на паре импульсных реле без обратной связи — логика хода, без HA.

Перенос интеграции `pulse_cover` (github.com/deliriumxp/pulse_cover) внутрь дома
(решение заказчика 2026-09-19: одна интеграция на объекте, штора приходит
конфигом объекта, а не записью мастера настройки HA). Физика и её обоснование —
`docs/ha-integration.md` менеджера, раздел «Шторы на паре импульсных реле».

Коротко: привод трогается по ФРОНТУ короткого замыкания реле нужного
направления и встаёт по импульсу, который задаёт «способ остановки» (1:1 со
свойством `Stop Method` драйвера Control4 `blinds_generic_2_relay`). Положение —
ОЦЕНКА по времени хода, не измерение. Полный ход всегда идёт на полное время плюс
запас и тем самым «доезжает до упора» — самокоррекция накопленной ошибки.

⚠ Реле зовутся через `call(domain, service, data)` — это источник состояний дома
(`source.py`), в HA — `switch.turn_on/off`. Сущность `cover` для жильца и шины
KNX (виртуальная позиция) — забота адаптера (`cover.py`).
"""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from time import monotonic
from typing import Any, Awaitable, Callable

OPEN, CLOSE = "open", "close"
STOP_METHODS = ("pulse_both", "pulse_last", "pulse_opposite", "pulse_up", "pulse_down", "none")
# Как часто пересчитывается оценка положения на ходу (ползунок у жильца).
# ⚠ Раз в секунду, а не 4 раза (решение заказчика 2026-09-22): каждый пересчёт —
# новое состояние сущности, то есть запись в историю HA и кадр в поток жильца;
# при 0.25 с один ход шторы давал ~80 записей. Ползунок идёт шагами, и это принято.
POSITION_UPDATE_S = 1.0

Call = Callable[[str, str, dict[str, Any]], Awaitable[None]]


@dataclass
class BlindSpec:
    """Штора из конфига объекта (`covers[]`, собирает менеджер)."""

    id: str
    name: str
    open_switch: str
    close_switch: str
    entity_id: str = ""
    travel_up: float = 20.0
    travel_down: float = 20.0
    pulse_move_ms: int = 500
    pulse_stop_ms: int = 500
    failsafe: float = 3.0
    stop_method: str = "pulse_both"
    # Групповой адрес KNX (DPT 5.001) общей с Control4 `knx_pulse_blind` позиции.
    position_address: str = ""


def spec_of(block: Any) -> BlindSpec | None:
    """Штора из блока конфига; мусор — «шторы нет», а не падение."""
    if not isinstance(block, dict):
        return None
    try:
        spec = BlindSpec(
            id=str(block["id"]),
            name=str(block.get("name") or block["id"]),
            open_switch=str(block["open"]),
            close_switch=str(block["close"]),
            entity_id=str(block.get("entityId") or ""),
            travel_up=max(float(block.get("travelUp") or 20), 0.5),
            travel_down=max(float(block.get("travelDown") or 20), 0.5),
            pulse_move_ms=max(int(block.get("pulseMove") or 500), 50),
            pulse_stop_ms=max(int(block.get("pulseStop") or 500), 50),
            failsafe=max(float(block.get("failsafe") if block.get("failsafe") is not None else 3), 0.0),
            stop_method=str(block.get("stopMethod") or "pulse_both"),
            position_address=str(block.get("positionAddress") or "").strip(),
        )
    except (KeyError, TypeError, ValueError):
        return None
    if spec.stop_method not in STOP_METHODS:
        spec.stop_method = "pulse_both"
    return spec


class PulseBlind:
    """Одна штора: команды, ход, оценка положения."""

    def __init__(
        self,
        spec: BlindSpec,
        call: Call,
        on_change: Callable[[], None],
        on_settled: Callable[[], None] | None = None,
    ) -> None:
        self.spec = spec
        self._call = call
        self._on_change = on_change
        # Положение УСТАНОВИЛОСЬ нашим ходом или калибровкой — его публикуют на
        # общий адрес. ⚠ Не после чужого доклада: вернуть его в шину — эхо.
        self._on_settled = on_settled or (lambda: None)
        self.position: int | None = None
        self.opening = False
        self.closing = False
        self.calibrated = False
        self.last_direction: str | None = None
        self._move: asyncio.Task[None] | None = None
        self._cancel = asyncio.Event()
        self._lock = asyncio.Lock()

    @property
    def moving(self) -> bool:
        return self._move is not None and not self._move.done()

    # --- команды ------------------------------------------------------------

    async def open(self) -> None:
        await self._start(OPEN, None)

    async def close(self) -> None:
        await self._start(CLOSE, None)

    async def set_position(self, target: int) -> None:
        target = min(max(int(target), 0), 100)
        # Никогда не калиброванная — середина как лучшее предположение.
        current = self.position if self.position is not None else 50
        if target != current:
            await self._start(OPEN if target > current else CLOSE, target)

    async def stop(self) -> None:
        if self.moving:
            self._cancel.set()
            await self._move  # type: ignore[misc]
        else:
            # Стоп без хода — штору могли тронуть руками, а жилец хочет гарантию,
            # что мотор стоит. Не мешаем.
            await self._stop_pulse()

    def calibrate(self, position: int) -> bool:
        """Сказать, где штора на самом деле, без движения. На ходу — отказ."""
        if self.moving:
            return False
        self.position = min(max(int(position), 0), 100)
        self.calibrated = True
        self._on_change()
        self._on_settled()
        return True

    def external_position(self, value: float) -> None:
        """Положение, сообщённое другой стороной (Control4 по шине). Только показ."""
        if self.moving:
            # Чужой доклад посреди своего хода испортил бы точку отсчёта оценки;
            # наш итог хода будет опубликован следом.
            return
        self.position = round(float(value))
        self.opening = self.closing = False
        self.calibrated = False
        self._on_change()

    async def shutdown(self) -> None:
        if self.moving:
            self._cancel.set()
            await asyncio.gather(self._move, return_exceptions=True)  # type: ignore[arg-type]

    # --- ход ---------------------------------------------------------------

    async def _start(self, direction: str, target: int | None) -> None:
        # ⚠ Без «уже едем туда — пропустить»: цель частичного хода и полный ход —
        # разные длительности; пропуск оставил бы штору не доехавшей. Текущий ход
        # отменяется и ДОЖИДАЕТСЯ своего стоп-импульса — это и есть пауза перед
        # новым стартом (третьего тайминга у эталонного драйвера нет).
        # ⚠ Под замком: две команды подряд (открыть, затем закрыть в окне
        # стоп-импульса) обе ждали бы один ход и обе заводили бы новый — первый
        # осиротел бы и ехал параллельно второму.
        async with self._lock:
            if self.moving:
                self._cancel.set()
                await self._move  # type: ignore[misc]
            self._cancel = asyncio.Event()
            self._move = asyncio.ensure_future(self._run(direction, target))

    async def _run(self, direction: str, target: int | None) -> None:
        full = target is None
        travel = self.spec.travel_up if direction == OPEN else self.spec.travel_down
        start_position = self.position
        if full:
            duration, final = travel + self.spec.failsafe, (100 if direction == OPEN else 0)
        else:
            current = start_position if start_position is not None else 50
            duration, final = travel * abs(target - current) / 100.0, target
        if duration <= 0:
            return
        await self._pulse([self._relay(direction)], self.spec.pulse_move_ms)
        self.opening, self.closing = direction == OPEN, direction == CLOSE
        self._on_change()
        # Видимый ход идёт по РЕАЛЬНОМУ времени, без запаса: иначе ползунок
        # стоял бы «почти у края» весь запас.
        visual = travel if full else duration
        started = monotonic()
        interrupted = False
        try:
            while monotonic() - started < duration:
                try:
                    await asyncio.wait_for(self._cancel.wait(), POSITION_UPDATE_S)
                    interrupted = True
                    break
                except asyncio.TimeoutError:
                    pass
                if start_position is not None:
                    self.position = _between(start_position, final, (monotonic() - started) / visual)
                    self._on_change()
        finally:
            if interrupted and start_position is not None:
                self.position = _between(start_position, final, (monotonic() - started) / visual)
            elif not interrupted:
                self.position = final
                # Калибрует ТОЛЬКО полный ход, доведённый до конца.
                self.calibrated = full
            self.last_direction = direction
            self.opening = self.closing = False
            await self._stop_pulse()
            self._on_change()
            self._on_settled()

    def _relay(self, direction: str) -> str:
        return self.spec.open_switch if direction == OPEN else self.spec.close_switch

    async def _stop_pulse(self) -> None:
        method = self.spec.stop_method
        if method == "none":
            return
        both = [self.spec.open_switch, self.spec.close_switch]
        if method == "pulse_up":
            relays = [self.spec.open_switch]
        elif method == "pulse_down":
            relays = [self.spec.close_switch]
        elif method in ("pulse_last", "pulse_opposite") and self.last_direction is not None:
            direction = self.last_direction
            if method == "pulse_opposite":
                direction = CLOSE if direction == OPEN else OPEN
            relays = [self._relay(direction)]
        else:
            # `pulse_both` и «направление ещё неизвестно» — оба реле.
            relays = both
        await self._pulse(relays, self.spec.pulse_stop_ms)

    async def _pulse(self, relays: list[str], duration_ms: int) -> None:
        # ⚠ Отпускание — ВСЕГДА: упавший `turn_on` одного реле или отмена посреди
        # импульса иначе оставляли бы второе реле замкнутым.
        try:
            await asyncio.gather(*(self._call("switch", "turn_on", {"entity_id": r}) for r in relays))
            await asyncio.sleep(duration_ms / 1000)
        finally:
            await asyncio.gather(
                *(self._call("switch", "turn_off", {"entity_id": r}) for r in relays), return_exceptions=True
            )


def _between(start: int, final: int, fraction: float) -> int:
    return round(start + (final - start) * min(max(fraction, 0.0), 1.0))
