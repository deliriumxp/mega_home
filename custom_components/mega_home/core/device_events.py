"""События устройств: один концентратор, два получателя.

Источники — SIP-мост (вызов, отмена, ответ, конец), входящие вызовы устройств
(webhook, Action URL), долгий опрос, подписки MQTT, строки TCP (`listeners.py`).
Дом их НЕ толкует: несёт как есть, с именем источника (`docs/home-gateway.md`).

⚠ Получателей два, и оба обязательны:
  * менеджер — кадр `event` по живому каналу БЕЗ подписки: звонок в дверь
    приходит, когда жилец ничего не смотрит, а `watch` живёт, только пока он
    смотрит; из события менеджер делает push;
  * локальный поток приложения (`events.py`) — настенная панель без интернета
    обязана узнать о звонке в дверь.

⚠ Буфер на время обрыва: канал переподключается секундами, а событие, потерянное
в эту секунду, — пропущенный звонок. Буфер короткий по числу и по сроку: событие
старше минуты для push бесполезно, а память Home Assistant не склад.
"""

from __future__ import annotations

from collections import deque
from collections.abc import Callable
from time import time
from typing import Any
from uuid import uuid4

from .const import LOGGER

BUFFER = 100
BUFFER_TTL_S = 60.0
MAX_DATA_BYTES = 64 * 1024

Listener = Callable[[dict[str, Any]], None]


class EventHub:
    """Раздаёт событие всем подписчикам; менеджеру — через буфер."""

    def __init__(self) -> None:
        self._listeners: list[Listener] = []
        self._pending: deque[dict[str, Any]] = deque(maxlen=BUFFER)
        self._sender: Callable[[dict[str, Any]], bool] | None = None
        self.recent: deque[dict[str, Any]] = deque(maxlen=20)

    def publish(self, access: str, source: str, event: str, data: Any = None) -> dict[str, Any]:
        """Событие устройства. `access` — доступ (или служба дома), `source` — его источник."""
        frame = {
            "t": "event",
            "id": uuid4().hex,
            "at": round(time(), 3),
            "access": access,
            "source": source,
            "event": event,
            "data": _bounded(data),
        }
        self.recent.append({k: frame[k] for k in ("at", "access", "source", "event")})
        for listener in list(self._listeners):
            try:
                listener(frame)
            except Exception:  # noqa: BLE001 — подписчик не роняет источник
                LOGGER.warning("Событие устройства: подписчик упал", exc_info=True)
        if self._sender is None or not self._sender(frame):
            self._pending.append(frame)
        return frame

    def subscribe(self, listener: Listener) -> Callable[[], None]:
        self._listeners.append(listener)

        def unsubscribe() -> None:
            if listener in self._listeners:
                self._listeners.remove(listener)

        return unsubscribe

    def attach(self, sender: Callable[[dict[str, Any]], bool]) -> list[dict[str, Any]]:
        """Канал к менеджеру поднялся: отдать отложенное свежее, дальше слать сразу.

        `sender` возвращает False, если кадр не ушёл — тогда он ляжет в буфер.
        """
        self._sender = sender
        now = time()
        fresh = [f for f in self._pending if now - f["at"] <= BUFFER_TTL_S]
        self._pending.clear()
        return fresh

    def detach(self) -> None:
        self._sender = None

    def requeue(self, frame: dict[str, Any]) -> None:
        self._pending.append(frame)

    def state(self) -> dict[str, Any]:
        return {"pending": len(self._pending), "recent": list(self.recent)}


def _bounded(data: Any) -> Any:
    """Данные события не больше потолка: дверь событий не превращается в выкачивание."""
    import json

    try:
        size = len(json.dumps(data, default=str))
    except (TypeError, ValueError):
        return None
    if size > MAX_DATA_BYTES:
        return {"truncated": True, "size": size}
    return data
