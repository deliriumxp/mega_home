"""События устройств: один концентратор, два получателя.

Источники — SIP-мост (вызов, отмена, ответ, конец), входящие вызовы устройств,
долгий опрос, поток, подписки MQTT, WebSocket, TCP, UDP (`listeners.py`). Дом их
НЕ толкует: несёт как есть, с именем доступа и источника (`docs/home-gateway.md`).

⚠ Получателей два:
  * менеджер — кадр `event` по живому каналу БЕЗ подписки: звонок в дверь
    приходит, когда жилец ничего не смотрит; из события менеджер делает push;
  * локальный поток приложения (`events.py`) — настенная панель без интернета
    обязана узнать о звонке в дверь. ⚠ Туда идёт ТОЛЬКО событие с `local`:
    локальный контур без аутентификации, а тело опроса уровня менеджера или
    токен в запросе вебхука не должны становиться видны любому в Wi-Fi.

⚠ Доставка менеджеру — С ПОДТВЕРЖДЕНИЕМ (`event-ack {id}`). Отправка в сокет
не значит доставку: кадр ложится в буфер TCP мёртвого соединения до срабатывания
heartbeat и пропадает. Поэтому кадр живёт здесь до подтверждения и повторяется
после переподключения В ИСХОДНОМ ПОРЯДКЕ; менеджер отбрасывает повторы по `id`.
Срок и число ограничены: событие старше минуты для push бесполезно.
"""

from __future__ import annotations

from collections import OrderedDict, deque
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
    """Раздаёт событие подписчикам; менеджеру — до подтверждения."""

    def __init__(self, store: Any = None) -> None:
        self._listeners: list[Listener] = []
        self._unacked: OrderedDict[str, dict[str, Any]] = OrderedDict()
        self._sender: Callable[[dict[str, Any]], bool] | None = None
        self.recent: deque[dict[str, Any]] = deque(maxlen=20)
        self.dropped = 0
        # Хранилище ленты устройств (`device_store.DeviceEventStore`) — часть F:
        # опционально, чтобы демон без диска (`docs/plan-core-without-ha.md`)
        # и тесты этого модуля обходились без него.
        self._store = store
        # Каждое опубликованное событие — правилам вложений (`event_files.py`).
        self.on_published: Listener | None = None

    def publish(
        self, access: str, source: str, event: str, data: Any = None, local: bool = False
    ) -> dict[str, Any]:
        """Событие устройства. `local` — можно ли показать его в локальном контуре.

        ⚠ Умолчание — НЕ показывать: локальный контур без аутентификации, и
        новый источник не должен попадать туда по забывчивости. Ровно тот же
        признак решает, ложится ли событие в хранилище ленты (`_store`) — как
        до 0.4.0, история устройства видна без менеджера, а его тело наружу не
        уходило и не уходит.
        """
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
        if local:
            for listener in list(self._listeners):
                try:
                    listener(frame)
                except Exception:  # noqa: BLE001 — подписчик не роняет источник
                    LOGGER.warning("Событие устройства: подписчик упал", exc_info=True)
            if self._store is not None:
                self._store.add(frame)
        self._keep(frame)
        if self._sender is not None:
            self._sender(frame)
        # ⚠ ПОСЛЕ отправки: вложение (`event_files.py`) догоняет событие, а не
        # задерживает его.
        if self.on_published is not None:
            self.on_published(frame)
        return frame

    def subscribe(self, listener: Listener) -> Callable[[], None]:
        self._listeners.append(listener)

        def unsubscribe() -> None:
            if listener in self._listeners:
                self._listeners.remove(listener)

        return unsubscribe

    def attach(self, sender: Callable[[dict[str, Any]], bool]) -> list[dict[str, Any]]:
        """Канал поднялся: вернуть неподтверждённое свежее (в порядке событий)."""
        self._sender = sender
        self._expire()
        return list(self._unacked.values())

    def detach(self) -> None:
        self._sender = None

    def ack(self, frame_id: Any) -> None:
        self._unacked.pop(str(frame_id), None)

    def state(self) -> dict[str, Any]:
        return {"unacked": len(self._unacked), "dropped": self.dropped, "recent": list(self.recent)}

    def _keep(self, frame: dict[str, Any]) -> None:
        self._expire()
        self._unacked[frame["id"]] = frame
        while len(self._unacked) > BUFFER:
            self._unacked.popitem(last=False)
            self.dropped += 1
            LOGGER.warning("События устройств: буфер полон, старейшее не доставлено менеджеру")

    def _expire(self) -> None:
        now = time()
        for frame_id in [i for i, f in self._unacked.items() if now - f["at"] > BUFFER_TTL_S]:
            del self._unacked[frame_id]


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
