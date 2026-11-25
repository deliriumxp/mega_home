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
# ⚠ Перечень ПОЛОН, а не «пока»: у пары реле остановить можно только импульсом на оба, на одно (вверх, вниз,
# последнее, противоположное) или ничем — других вариантов у двух реле нет (ревью 2026-10-30).
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
    # Класс HA (`CoverDeviceClass`): `curtain` — шторы/жалюзи в Яндексе (`openable.curtain`).
    # Пусто — без класса, как было до выбора. Проверяет класс адаптер HA (`cover.py`), ядро — нет:
    # `core` не знает `homeassistant`.
    device_class: str = ""


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
            device_class=str(block.get("deviceClass") or "").strip().lower(),
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
    #
    # ⚠ Каждая команда — СВОЯ задача под общим замком (`_command`), и вызывающий ждёт её через `shield`.
    # Вызывающий — служба HA, а за ней автоматизация клавиши в `mode: restart`: следующая телеграмма
    # отменяет идущее действие. Прежде команда сама ждала ход, и отмена вызывающего обрывала ход посреди
    # стоп-импульса до записи состояния: HA навсегда оставалась с «Закрывается», и каждое следующее
    # нажатие шло в «стоп» (объект 11, штора в промежуточном положении). Теперь отмена вызывающего
    # отменяет только его ожидание. Замок общий и для «стопа»: прежде «стоп» шёл мимо него и мог
    # разминуться с разворотом.

    async def open(self) -> None:
        await self._command(lambda: self._start(OPEN, None))

    async def close(self) -> None:
        await self._command(lambda: self._start(CLOSE, None))

    async def set_position(self, target: int) -> None:
        await self._command(lambda: self._set_position(target))

    async def stop(self) -> None:
        await self._command(self._stop)

    async def toggle(self) -> None:
        """Нажатие клавиши: решает САМА штора по своему ходу, а не шаблон по состоянию в HA.

        Едет — стоп; стоит — в сторону, противоположную последнему ходу; хода ещё не было — открыть
        закрытую, иначе закрыть. ⚠ Прежде это решал шаблон автоматизации по `states(...)` HA, то есть по
        записи, которая могла отстать от шторы, — второй владелец одного решения.
        """
        await self._command(self._toggle)

    async def _command(self, action: Callable[[], Awaitable[None]]) -> None:
        async def locked() -> None:
            async with self._lock:
                await action()

        await asyncio.shield(asyncio.ensure_future(locked()))

    async def _set_position(self, target: int) -> None:
        target = min(max(int(target), 0), 100)
        if self.position is not None:
            if target != self.position:
                await self._start(
                    OPEN if target > self.position else CLOSE,
                    None if target in (0, 100) else target,
                )
            return
        # ⚠ Положение неизвестно (штора ещё ни разу не доезжала до упора и никто его
        # не сообщил). Оценка «середина» годится только для выбора направления, а
        # доля от неё — выдумка: «поставить 50 %» не сдвинуло бы штору вовсе (цель
        # совпала бы с предположением), а любая другая доля встала бы мимо на
        # неизвестную величину. Поэтому сначала доезжаем до ближайшего упора — он же
        # и калибрует оценку, — а от него уже отсчитываем долю.
        if target in (0, 100):
            await self._start(OPEN if target == 100 else CLOSE, None)
            return
        await self._start(OPEN if target > 50 else CLOSE, None, then=target)

    async def _stop(self) -> None:
        if self.moving:
            self._cancel.set()
            await self._move  # type: ignore[misc]
            return
        # Хода нет, а HA могла остаться с «едет»: сбросить и записать заново.
        self.opening = self.closing = False
        self._on_change()

    async def _toggle(self) -> None:
        if self.moving:
            await self._stop()
        elif self.last_direction is not None:
            await self._start(CLOSE if self.last_direction == OPEN else OPEN, None)
        else:
            await self._start(OPEN if self.position == 0 else CLOSE, None)

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

    async def _start(
        self,
        direction: str,
        target: int | None,
        then: int | None = None,
    ) -> None:
        """Новый ход; зовётся только под замком (`_command`).

        ⚠ Без «уже едем туда — пропустить»: цель частичного хода и полный ход — разные длительности;
        пропуск оставил бы штору не доехавшей. Текущий ход отменяется и ДОЖИДАЕТСЯ своего стоп-импульса —
        это и есть пауза перед новым стартом (третьего тайминга у эталонного драйвера нет). Замок не даёт
        двум командам подряд (открыть, затем закрыть в окне стоп-импульса) ждать один ход и обеим заводить
        новый — первый осиротел бы и ехал параллельно второму.
        """
        if self.moving:
            self._cancel.set()
            await self._move  # type: ignore[misc]
        self._cancel = asyncio.Event()
        self._move = asyncio.ensure_future(self._run(direction, target, then))

    async def _run(self, direction: str, target: int | None, then: int | None = None) -> None:
        full = target is None
        travel = self.spec.travel_up if direction == OPEN else self.spec.travel_down
        # ⚠ Неизвестное положение — не повод молчать на ходу: оценка идёт от
        # «середины», как у эталонного драйвера Control4 (`gLevel or 50`). Иначе у
        # жильца ползунок стоял бы на месте весь ход неоткалиброванной шторы и
        # прыгал бы в конце — ровно то, на что жаловались.
        start_position = self.position if self.position is not None else 50
        if full:
            duration, final = travel + self.spec.failsafe, (100 if direction == OPEN else 0)
        else:
            duration, final = travel * abs(target - start_position) / 100.0, target
        if duration <= 0:
            return
        # Видимый ход идёт по РЕАЛЬНОМУ времени, без запаса: иначе ползунок
        # стоял бы «почти у края» весь запас.
        if not await self._travel(
            direction,
            start_position,
            final,
            duration,
            travel if full else duration,
            calibrate=full,
        ):
            # Прервали: оценку `_travel` уже выставил, осталось опубликовать итог.
            self._on_settled()
            return
        # Дошли до упора, а целью была доля от него — второй ход от точной точки.
        if then is not None and then != final and not self._cancel.is_set():
            await self._second_leg(then)
        self._on_settled()

    async def _second_leg(self, target: int) -> None:
        """Ход от упора к промежуточной цели: упор дал точку отсчёта, а не «середину»."""
        start = self.position if self.position is not None else 50
        if target == start:
            return
        direction = OPEN if target > start else CLOSE
        travel = self.spec.travel_up if direction == OPEN else self.spec.travel_down
        if target in (0, 100):
            duration, final, visual, calibrate = travel + self.spec.failsafe, target, travel, True
        else:
            duration = travel * abs(target - start) / 100.0
            final, visual, calibrate = target, duration, False
        if duration <= 0:
            return
        await self._travel(direction, start, final, duration, visual, calibrate)

    async def _travel(
        self,
        direction: str,
        start: int,
        final: int,
        duration: float,
        visual: float,
        calibrate: bool,
    ) -> bool:
        """Один ход: стартовый импульс, оценка раз в секунду. `False` — прервали стопом."""
        await self._pulse([self._relay(direction)], self.spec.pulse_move_ms)
        self.opening, self.closing = direction == OPEN, direction == CLOSE
        self._on_change()
        started = monotonic()
        interrupted = False
        try:
            while monotonic() - started < duration:
                try:
                    remaining = max(0.0, duration - (monotonic() - started))
                    await asyncio.wait_for(self._cancel.wait(), min(POSITION_UPDATE_S, remaining))
                    interrupted = True
                    break
                except asyncio.TimeoutError:
                    pass
                self.position = _between(start, final, (monotonic() - started) / visual)
                self._on_change()
        finally:
            if interrupted:
                self.position = _between(start, final, (monotonic() - started) / visual)
            else:
                self.position = final
                # Калибрует ТОЛЬКО полный ход, доведённый до конца.
                self.calibrated = calibrate
            self.last_direction = direction
            self.opening = self.closing = False
            try:
                # На концевике мотор уже стоит; лишний импульс может запустить его обратно.
                if interrupted or not calibrate:
                    await self._stop_pulse()
            finally:
                # Состояние — при любом исходе стоп-импульса (отказ реле, отмена): без записи HA
                # оставалась с «едет».
                self._on_change()
        return not interrupted

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
