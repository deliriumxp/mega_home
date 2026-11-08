"""Прибор с состоянием по опросу — описание из конфига, без HA и без вендора.

Зачем. Устройство, у которого нет ни подписки, ни интеграции HA (контроллер
вентиляции с HTTP-API веб-интерфейса), дом заводит сам: опрашивает его тем же
`connect`, раскладывает ответ по полям описания и отдаёт значения адаптеру,
который публикует сущности в HA (`climate.py`). Как у импульсных штор
(`pulse_blind.py`): логика — здесь, сущность — у адаптера, описание — от менеджера.

⚠ Смысла полей дом НЕ знает: «где уставка и во сколько раз она меньше» —
строки и числа описания, их пишет генератор менеджера под вендора. Новое
устройство того же класса — правка менеджера, а не выпуск дома
(`docs/home-gateway.md`, «Устройства объекта: форма, а не перечень классов»).

⚠ Опрос, а не слушатель `poll` (`listeners_out.py`): кадры слушателя — СОБЫТИЯ,
они уходят в ленту устройства и менеджеру; состояние раз в N секунд забивало бы
ленту, а обрыв связи не становился бы «недоступен». Подписки у таких устройств нет —
их собственный веб-интерфейс сам опрашивает состояние.
"""

from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any

from .templating import dig, render

# Чаще раза в 2 с устройство опрашивать незачем: состояние климата меняется
# минутами, а слабые контроллеры теряют кадры под нагрузкой.
MIN_EVERY_S = 2.0
DEFAULT_EVERY_S = 10.0
# Сколько отказов подряд — «недоступен». ⚠ Не первый: контроллер в шумной сети
# теряет отдельные кадры, и сущность мигала бы «недоступно» на каждой потере
# (так падал Modbus TCP офисной вентиляции, 2026-11-08).
MISSES_UNAVAILABLE = 3

Perform = Callable[[dict[str, Any]], Awaitable[dict[str, Any]]]


@dataclass(frozen=True)
class FieldSpec:
    """Поле состояния: путь в JSON-ответе и множитель к числу."""

    path: tuple[str, ...]
    scale: float = 1.0


@dataclass(frozen=True)
class WriteSpec:
    """Запись поля: ключ подставляется в тело запроса записи как `{key}`, значение — `{value}`."""

    key: str
    scale: float = 1.0


@dataclass(frozen=True)
class DeviceSpec:
    """Прибор из конфига объекта (собирает менеджер)."""

    id: str
    name: str
    access: str
    read_path: str
    read_method: str = "GET"
    every: float = DEFAULT_EVERY_S
    fields: dict[str, FieldSpec] = field(default_factory=dict)
    write_path: str = ""
    write_method: str = "POST"
    write_body: str = "{key}={value}"
    writes: dict[str, WriteSpec] = field(default_factory=dict)
    # Остальное описание (`climate`, `sensors`, …) — адаптеру: ядру его смысл не нужен.
    extra: dict[str, Any] = field(default_factory=dict)


def _path(value: Any) -> tuple[str, ...]:
    if isinstance(value, list):
        return tuple(str(step) for step in value)
    return tuple(step for step in str(value or "").split(".") if step)


def _scale(value: Any) -> float:
    try:
        number = float(value if value is not None else 1)
    except (TypeError, ValueError):
        return 1.0
    return number if number else 1.0


def spec_of(block: Any) -> DeviceSpec | None:
    """Прибор из блока конфига; мусор — «прибора нет», а не падение."""
    if not isinstance(block, dict):
        return None
    read = block.get("read") if isinstance(block.get("read"), dict) else {}
    write = block.get("write") if isinstance(block.get("write"), dict) else {}
    if not block.get("id") or not block.get("access") or not str(read.get("path") or "").startswith("/"):
        return None
    try:
        every = max(float(block.get("every") or DEFAULT_EVERY_S), MIN_EVERY_S)
    except (TypeError, ValueError):
        every = DEFAULT_EVERY_S
    raw_fields = block.get("fields") if isinstance(block.get("fields"), dict) else {}
    raw_writes = block.get("writes") if isinstance(block.get("writes"), dict) else {}
    known = {"id", "name", "access", "read", "write", "every", "fields", "writes"}
    return DeviceSpec(
        id=str(block["id"]),
        name=str(block.get("name") or block["id"]),
        access=str(block["access"]),
        read_path=str(read["path"]),
        read_method=str(read.get("method") or "GET").upper(),
        every=every,
        fields={
            str(name): FieldSpec(_path(item.get("path")), _scale(item.get("scale")))
            for name, item in raw_fields.items() if isinstance(item, dict) and _path(item.get("path"))
        },
        write_path=str(write.get("path") or ""),
        write_method=str(write.get("method") or "POST").upper(),
        write_body=str(write.get("body") if write.get("body") is not None else "{key}={value}"),
        writes={
            str(name): WriteSpec(str(item["key"]), _scale(item.get("scale")))
            for name, item in raw_writes.items() if isinstance(item, dict) and item.get("key")
        },
        extra={k: v for k, v in block.items() if k not in known},
    )


def _number(value: Any, scale: float) -> Any:
    """Число с множителем; не число — как пришло (строка режима, флаг)."""
    if isinstance(value, bool) or not isinstance(value, (int, float)) or scale == 1.0:
        # ⚠ Без множителя — как пришло: `2 * 1.0` дало бы `2.0`, и скорость «2» не нашлась бы в списке
        # скоростей, а код состояния «4» — в таблице действий (сравнение идёт строкой).
        return value
    scaled = value * scale
    return round(scaled, 6) if isinstance(scaled, float) else scaled


def _wire(value: Any, scale: float) -> str:
    """Значение для тела записи: число с множителем без хвоста «.0» (уставка 18.0 × 10 → «180»)."""
    if isinstance(value, bool):
        return "1" if value else "0"
    if isinstance(value, (int, float)):
        scaled = round(float(value) * scale, 6)
        return str(int(scaled)) if scaled.is_integer() else str(scaled)
    return str(value)


class PolledDevice:
    """Один прибор: опрос, значения полей, запись."""

    def __init__(
        self, spec: DeviceSpec, request: Callable[[], dict[str, Any] | None], perform: Perform,
        on_change: Callable[[], None],
    ) -> None:
        self.spec = spec
        # Адрес и учётка — описание доступа (`accesses[]`), его берут при КАЖДОМ запросе:
        # новый конфиг меняет адрес без пересоздания прибора.
        self._request = request
        self._perform = perform
        self._on_change = on_change
        self.values: dict[str, Any] = {}
        self.available = False
        self.why = ""
        self._misses = 0
        self._task: asyncio.Task[None] | None = None
        self._wake = asyncio.Event()

    def start(self) -> None:
        if self._task is None or self._task.done():
            self._task = asyncio.ensure_future(self._loop())

    async def shutdown(self) -> None:
        task, self._task = self._task, None
        if task is not None and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)

    async def write(self, name: str, value: Any) -> None:
        """Записать поле; сразу за записью — внеочередной опрос, чтобы сущность не ждала цикла."""
        target = self.spec.writes.get(name)
        if target is None or not self.spec.write_path:
            raise ValueError(f"у прибора {self.spec.id} нет записи «{name}»")
        body = render(self.spec.write_body, {"key": target.key, "value": _wire(value, target.scale)})
        answer = await self._call(self.spec.write_method, self.spec.write_path, body)
        if int(answer.get("status") or 0) >= 400:
            raise ConnectionError(f"прибор {self.spec.id} отказал в записи ({answer.get('status')})")
        self._wake.set()

    async def refresh(self) -> None:
        """Один опрос: значения полей или счёт отказов."""
        try:
            answer = await self._call(self.spec.read_method, self.spec.read_path, None)
            if int(answer.get("status") or 0) >= 400:
                raise ConnectionError(f"ответ {answer.get('status')}")
            data = json.loads(answer.get("body") or "null")
        except asyncio.CancelledError:
            raise
        except Exception as err:  # noqa: BLE001 — любой отказ опроса — «не ответил», не падение дома
            self._misses += 1
            # Текст ошибки не пишем: в нём бывает адрес с учёткой (как у слушателей).
            self.why = type(err).__name__
            if self.available and self._misses >= MISSES_UNAVAILABLE:
                self.available = False
                self._on_change()
            return
        values = {name: _number(dig(data, item.path), item.scale) for name, item in self.spec.fields.items()}
        changed = values != self.values or not self.available
        self.values, self.available, self._misses, self.why = values, True, 0, ""
        if changed:
            self._on_change()

    async def _loop(self) -> None:
        while True:
            await self.refresh()
            self._wake.clear()
            try:
                await asyncio.wait_for(self._wake.wait(), self.spec.every)
            except asyncio.TimeoutError:
                pass

    async def _call(self, method: str, path: str, body: str | None) -> dict[str, Any]:
        request = self._request()
        if request is None:
            raise LookupError(f"нет доступа «{self.spec.access}» в конфиге")
        payload = {**request, "kind": "http", "method": method, "path": path}
        if body is not None:
            payload["body"] = body
        return await self._perform(payload)
