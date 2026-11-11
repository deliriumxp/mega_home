"""Датчики по событиям устройств — раздел `sensors[]` конфига (дом 0.6.23+).

Зачем. Реакции, уведомления и сценарии строятся на сущностях HA, а событие устройства — человек в
кадре своей детекции камеры, движение от её подписки, показание, присланное устройством само (MQTT,
вебхук, поток), — только кадр концентратора (`device_events.py`). Датчик держит его СОСТОЯНИЕ. Что в
событии что значит — описание менеджера; дом смысла не знает, и новый датчик из событий — правка
менеджера, а не релиз. Видов два, как у HA — `binary_sensor` и `sensor`:
  * `binary` (умолчание) — слово «началось» включает, «кончилось» выключает; словарь и разбор те же, что
    у записи по движению камеры (`event_words.py`): `on[]`, `off[]`, `split`, `require[]`, `skip[]`;
  * `value` — показание: поле данных события по пути через точку (`path`, JSON внутри текста тоже —
    `conditions.value_at`) или первая группа выражения по тексту (`pattern`, для XML и строк), числом
    (`valueType: number`, умножается на `scale`) или текстом (`text`). Событие без значения состояние не
    меняет, кроме слова «кончилось» (`off[]`, дом 0.6.24+): оно ставит `offValue` или «неизвестно» — у
    уверенности сети ушедший человек — 0, а не последнее значение. Единица, класс и `stateClass` — слова HA.

Общее: `{id, entityId, name, deviceClass, access, source, event?, where?, require?, skip?, offAfterSec?}`. `event` —
имя события источника, если значимо не каждое; `where` — `{путь: подстрока}` по полям данных события (тема MQTT,
адрес вебхука: они не в тексте, по которому ищут `require`/`skip`). `offAfterSec` — тишина, после которой двоичный датчик выключается
(источник шлёт только «началось»), а показание становится неизвестным (устарело); каждое событие отсчитывает
заново.

⚠ Доступность — жизнь ИСТОЧНИКА (`EventHub.alive`), а не «давно не было событий»: молчащий датчик —
норма, а неработающий источник (конвейер камеры встал, подписка не поднялась) — «недоступен», а не
«нет». Источник встал — состояние сбрасывается: «есть», пришедшее до остановки, иначе висело бы вечно —
конца источник уже не пришлёт.
"""

from __future__ import annotations

import asyncio
import re
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any

from . import event_words
from .conditions import _MISSING, number, value_at
from .device_events import add_listener

# Состояние HA — строка до 255 знаков; длиннее HA отверг бы запись.
MAX_TEXT = 255


@dataclass(frozen=True)
class SensorSpec:
    id: str
    kind: str
    entity_id: str
    name: str
    device_class: str
    access: str
    source: str
    event: str
    on: tuple[str, ...]
    off: tuple[str, ...]
    split: str
    require: tuple[str, ...]
    skip: tuple[str, ...]
    off_after: float | None
    path: str = ""
    pattern: str = ""
    numeric: bool = True
    scale: float = 1.0
    unit: str = ""
    state_class: str = ""
    where: tuple[tuple[str, str], ...] = ()
    off_value: float | None = None


def spec_of(raw: Any) -> SensorSpec | None:
    if not isinstance(raw, dict) or not raw.get("id") or not raw.get("access"):
        return None
    kind = "value" if raw.get("kind") == "value" else "binary"
    pattern = str(raw.get("pattern") or "")
    if kind == "binary" and not raw.get("on") or kind == "value" and not (raw.get("path") or pattern):
        return None
    try:
        re.compile(pattern)
    except re.error:
        return None
    words = lambda key: tuple(str(s) for s in raw.get(key) or [] if s)  # noqa: E731
    after, scale = number(raw.get("offAfterSec")), number(raw.get("scale"))
    return SensorSpec(
        id=str(raw["id"]),
        kind=kind,
        entity_id=str(raw.get("entityId") or ""),
        name=str(raw.get("name") or raw["id"]),
        device_class=str(raw.get("deviceClass") or ""),
        access=str(raw["access"]),
        source=str(raw.get("source") or ""),
        event=str(raw.get("event") or ""),
        on=words("on"),
        off=words("off"),
        split=str(raw.get("split") or ""),
        require=words("require"),
        skip=words("skip"),
        off_after=after if after and after > 0 else None,
        path=str(raw.get("path") or ""),
        pattern=pattern,
        numeric=raw.get("valueType") != "text",
        scale=scale if scale is not None else 1.0,
        unit=str(raw.get("unit") or ""),
        state_class=str(raw.get("stateClass") or ""),
        where=tuple((str(k), str(v)) for k, v in raw["where"].items()) if isinstance(raw.get("where"), dict) else (),
        off_value=number(raw.get("offValue")),
    )


def reading_of(spec: SensorSpec, frame: dict[str, Any]) -> Any:
    """Показание из события; `_MISSING` — в событии его нет (состояние не меняется)."""
    text = event_words.text_of(frame)
    found: Any = _MISSING
    for message in text.split(spec.split) if spec.split else [text]:
        if spec.require and not any(word in message for word in spec.require) or any(word in message for word in spec.skip):
            continue
        if spec.pattern:
            match = re.search(spec.pattern, message)
            found = (match.group(1) if match.re.groups else match.group(0)) if match else found
        else:
            found = value_at(frame.get("data"), spec.path)
    if found is _MISSING or found is None:
        return _MISSING
    if spec.numeric:
        value = number(found)
        return _MISSING if value is None else value * spec.scale
    return str(found)[:MAX_TEXT]


class EventSensors:
    """Датчики объекта: состояние по событиям, доступность по жизни источника."""

    def __init__(self, alive: Callable[[str, str], bool]) -> None:
        self._alive = alive
        self.specs: dict[str, SensorSpec] = {}
        self._state: dict[str, Any] = {}
        self._timers: dict[str, asyncio.TimerHandle] = {}
        self._listeners: list[Callable[[str], None]] = []
        self._spec_listeners: list[Callable[[], None]] = []

    def apply(self, config: dict[str, Any] | None) -> None:
        raw = (config or {}).get("sensors")
        specs = {s.id: s for s in (spec_of(item) for item in raw) if s} if isinstance(raw, list) else {}
        if specs == self.specs:
            return
        for sensor_id in [s for s in self.specs if specs.get(s) != self.specs[s]]:
            # Описание сменилось — состояние начинается заново: слова прежнего описания к новому не относятся.
            self._drop(sensor_id)
        self.specs = specs
        for listener in list(self._spec_listeners):
            listener()

    def on_specs(self, listener: Callable[[], None]) -> Callable[[], None]:
        """Сменился состав датчиков: адаптер заводит и снимает сущности."""
        return add_listener(self._spec_listeners, listener)

    def listen(self, listener: Callable[[str], None]) -> Callable[[], None]:
        """Сменилось состояние или доступность датчика (его id)."""
        return add_listener(self._listeners, listener)

    def is_on(self, sensor_id: str) -> bool:
        return self._state.get(sensor_id) is True

    def value(self, sensor_id: str) -> Any:
        """Показание датчика вида `value`; `None` — неизвестно."""
        return self._state.get(sensor_id)

    def available(self, sensor_id: str) -> bool:
        spec = self.specs.get(sensor_id)
        return spec is not None and self._alive(spec.access, spec.source)

    def on_event(self, frame: dict[str, Any]) -> None:
        for spec in self.specs.values():
            if frame.get("access") != spec.access or frame.get("source") != spec.source:
                continue
            if spec.event and frame.get("event") != spec.event:
                continue
            if any(word not in str(value_at(frame.get("data"), path)) for path, word in spec.where):
                continue
            if spec.kind == "value":
                state = reading_of(spec, frame)
                if state is _MISSING and spec.off and False in event_words.steps(
                    event_words.text_of(frame), (), spec.off, spec.split, spec.require, spec.skip,
                ):
                    # «Кончилось» без значения — `offValue` (уверенность ушедшего человека — 0) или «неизвестно».
                    state = spec.off_value
                if state is _MISSING:
                    continue
            else:
                changes = event_words.steps(event_words.text_of(frame), spec.on, spec.off, spec.split, spec.require, spec.skip)
                if not changes:
                    continue
                # Последняя смена — состояние после события: окно, открытое и закрытое одним ответом, датчик
                # не держит включённым (запись по движению тот же ответ разбирает по шагам — ей важен каждый).
                state = changes[-1]
            if spec.off_after and state is not False and state is not None:
                self._arm(spec)
            else:
                self._disarm(spec.id)
            self._set(spec.id, state)

    def on_alive(self, access: str, source: str, alive: bool) -> None:
        for spec in self.specs.values():
            if spec.access != access or spec.source != source:
                continue
            if not alive:
                self._drop(spec.id)
            self._notify(spec.id)

    def stop(self) -> None:
        for sensor_id in list(self._timers):
            self._disarm(sensor_id)

    def _arm(self, spec: SensorSpec) -> None:
        self._disarm(spec.id)
        assert spec.off_after is not None
        self._timers[spec.id] = asyncio.get_running_loop().call_later(spec.off_after, self._expire, spec.id)

    def _disarm(self, sensor_id: str) -> None:
        timer = self._timers.pop(sensor_id, None)
        if timer is not None:
            timer.cancel()

    def _expire(self, sensor_id: str) -> None:
        self._timers.pop(sensor_id, None)
        spec = self.specs.get(sensor_id)
        self._set(sensor_id, None if spec is not None and spec.kind == "value" else False)

    def _drop(self, sensor_id: str) -> None:
        self._disarm(sensor_id)
        self._state.pop(sensor_id, None)

    def _set(self, sensor_id: str, state: Any) -> None:
        if self._state.get(sensor_id) == state and (sensor_id in self._state or state is None):
            return
        self._state[sensor_id] = state
        self._notify(sensor_id)

    def _notify(self, sensor_id: str) -> None:
        for listener in list(self._listeners):
            listener(sensor_id)
